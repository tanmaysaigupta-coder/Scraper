"""The multi-tier extraction engine.

Given raw text + a target Pydantic model, return a validated instance.

Control flow per call:

    for tier in chain:                      # e.g. Gemini Flash -> Groq -> DeepSeek
        build provider (skip tier on AuthError)
        chunk doc to this tier's budget
        for attempt in 1..attempts_per_tier:
            call provider
            on RateLimited     -> sleep(retry_after or backoff+jitter), retry
            on PayloadTooLarge -> plan.shrink(), re-chunk, retry
            on ProviderError   -> backoff+jitter, retry
            on ContentError    -> retry, then next tier
            on success         -> parse JSON -> validate -> return
        (tier exhausted -> fall through to next tier)
    raise ExtractionFailed  # every tier exhausted

Nothing here fabricates data: if no tier produces schema-valid JSON the call
fails loudly and the record is dropped (and logged), never invented.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass, field
from typing import TypeVar

from pydantic import BaseModel, ValidationError

from src.config import get_pipeline_config
from src.llm.chunking import ChunkPlan, chunk_document, estimate_tokens, pick_primary_chunk
from src.llm.errors import (
    AuthError,
    ContentError,
    LLMError,
    PayloadTooLarge,
    ProviderError,
    RateLimited,
)
from src.llm.providers import Provider, build_provider
from src.logging_setup import get_logger

log = get_logger("extract")

T = TypeVar("T", bound=BaseModel)


class ExtractionFailed(RuntimeError):
    def __init__(self, message: str, *, tried: list[str]):
        super().__init__(message)
        self.tried = tried


@dataclass
class ExtractionResult:
    model: BaseModel
    provider: str
    tier_model: str
    attempts: int
    chunks: int
    elapsed_s: float
    fell_back: bool


@dataclass
class _Tier:
    provider: str
    model: str
    max_input_tokens: int


@dataclass
class LLMOrchestrator:
    _tiers: list[_Tier] = field(default_factory=list)
    _providers: dict[str, Provider | None] = field(default_factory=dict)
    attempts_per_tier: int = 3
    backoff_base_s: float = 2.0
    backoff_max_s: float = 45.0
    _plan: ChunkPlan | None = None
    cache: object | None = None  # ExtractionCache | None

    @classmethod
    def from_config(cls) -> LLMOrchestrator:
        cfg = get_pipeline_config()["llm"]
        tiers = [
            _Tier(t["provider"], t["model"], int(t.get("max_input_tokens", 100_000)))
            for t in cfg["chain"]
        ]
        ck = cfg["chunk"]
        plan = ChunkPlan(
            chars_per_token=int(ck.get("chars_per_token", 4)),
            safety_ratio=float(ck.get("safety_ratio", 0.8)),
            overlap_chars=int(ck.get("overlap_chars", 400)),
        )
        return cls(
            _tiers=tiers,
            attempts_per_tier=int(cfg.get("attempts_per_tier", 3)),
            backoff_base_s=float(cfg.get("backoff_base_s", 2.0)),
            backoff_max_s=float(cfg.get("backoff_max_s", 45.0)),
            _plan=plan,
            cache=_default_cache(),
        )

    # --------------------------------------------------------------------- #
    def _provider(self, name: str) -> Provider | None:
        """Lazily build; cache `None` for a tier whose creds are missing."""
        if name not in self._providers:
            try:
                self._providers[name] = build_provider(name)
            except AuthError as exc:
                log.warning("tier_unavailable", provider=name, reason=str(exc))
                self._providers[name] = None
        return self._providers[name]

    def _sleep_for(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return retry_after
        expo = min(self.backoff_max_s, self.backoff_base_s * (2 ** (attempt - 1)))
        return expo * (0.5 + random.random())  # full jitter

    # --------------------------------------------------------------------- #
    async def extract(
        self,
        *,
        raw_text: str,
        target: type[T],
        instructions: str,
        context: dict | None = None,
    ) -> ExtractionResult:
        started = time.monotonic()
        tried: list[str] = []
        schema_json = json.dumps(target.model_json_schema(), indent=2)
        system = (
            "You are a precise information-extraction engine. Return ONLY a JSON "
            "object matching the given JSON Schema. Use null for anything not "
            "explicitly present in the source text. Never invent values.\n\n"
            f"JSON Schema:\n{schema_json}\n\n{instructions}"
        )

        if self.cache is not None:
            hit = self.cache.get(target.__name__, instructions, raw_text)
            if hit is not None:
                return ExtractionResult(
                    model=target.model_validate(hit), provider="cache", tier_model="cache",
                    attempts=0, chunks=0, elapsed_s=round(time.monotonic() - started, 3),
                    fell_back=False,
                )

        for idx, tier in enumerate(self._tiers):
            provider = self._provider(tier.provider)
            if provider is None:
                continue
            tried.append(f"{tier.provider}:{tier.model}")
            plan = self._plan or ChunkPlan(4, 0.8, 400)

            try:
                model_obj, attempts, n_chunks = await self._run_tier(
                    provider, tier, plan, system, raw_text, target, context or {}
                )
            except _TierExhausted as exc:
                log.warning("tier_exhausted", provider=tier.provider, model=tier.model, cause=str(exc))
                continue

            if self.cache is not None:
                self.cache.put(target.__name__, instructions, raw_text,
                               model_obj.model_dump(mode="json"), tier.provider)

            return ExtractionResult(
                model=model_obj,
                provider=tier.provider,
                tier_model=tier.model,
                attempts=attempts,
                chunks=n_chunks,
                elapsed_s=round(time.monotonic() - started, 3),
                fell_back=idx > 0,
            )

        raise ExtractionFailed("all LLM tiers exhausted", tried=tried)

    # --------------------------------------------------------------------- #
    async def _run_tier(
        self, provider, tier, plan, system, raw_text, target, context
    ) -> tuple[BaseModel, int, int]:
        est = estimate_tokens(raw_text, plan.chars_per_token)
        log.debug("tier_start", provider=tier.provider, est_tokens=est, budget=tier.max_input_tokens)

        for attempt in range(1, self.attempts_per_tier + 1):
            chunks = chunk_document(raw_text, tier.max_input_tokens, plan)
            payload = pick_primary_chunk(chunks)
            user = _render_user(payload, context)
            try:
                text = await provider.complete(system, user, model=tier.model)
                model_obj = _parse(text, target)
                return model_obj, attempt, len(chunks)

            except RateLimited as exc:
                wait = self._sleep_for(attempt, exc.retry_after)
                log.info("rate_limited", provider=tier.provider, attempt=attempt, sleep_s=round(wait, 2))
                await asyncio.sleep(wait)

            except PayloadTooLarge as exc:
                plan = plan.shrink()
                log.info("payload_too_large", provider=tier.provider, attempt=attempt,
                         new_safety_ratio=plan.safety_ratio, detail=str(exc)[:160])

            except ProviderError as exc:
                wait = self._sleep_for(attempt, None)
                log.info("provider_error", provider=tier.provider, attempt=attempt,
                         sleep_s=round(wait, 2), detail=str(exc)[:160])
                await asyncio.sleep(wait)

            except (ContentError, ValidationError, json.JSONDecodeError) as exc:
                log.info("bad_content", provider=tier.provider, attempt=attempt, detail=str(exc)[:200])

            except asyncio.CancelledError:
                raise

            except Exception as exc:  # noqa: BLE001 - never let one odd error kill the whole chain
                wait = self._sleep_for(attempt, None)
                log.warning("tier_unexpected_error", provider=tier.provider, attempt=attempt,
                            err=f"{type(exc).__name__}: {str(exc)[:160]}", sleep_s=round(wait, 2))
                await asyncio.sleep(wait)

        raise _TierExhausted(f"{tier.provider} gave no valid result in {self.attempts_per_tier} attempts")


class _TierExhausted(LLMError):
    pass


def _default_cache():
    try:
        from src.llm.cache import ExtractionCache

        return ExtractionCache()
    except Exception as exc:  # noqa: BLE001 - cache is an optimization, never fatal
        log.warning("extraction_cache_unavailable", err=str(exc)[:160])
        return None


def _render_user(payload: str, context: dict) -> str:
    ctx = ""
    if context:
        ctx = "Known context (authoritative, do not contradict):\n" + json.dumps(context, default=str) + "\n\n"
    return f"{ctx}Source text:\n\"\"\"\n{payload}\n\"\"\""


def _parse(text: str, target: type[T]) -> T:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = text[text.find("{") : text.rfind("}") + 1]
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1:
        raise ContentError(f"no JSON object in response: {text[:120]!r}")
    data = json.loads(text[start : end + 1])
    return target.model_validate(data)
