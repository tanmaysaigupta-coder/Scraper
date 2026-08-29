"""Provider adapters.

Each adapter exposes the same coroutine:

    async def complete(system: str, user: str, *, model: str, temperature: float) -> str

and translates provider-native errors into `src.llm.errors` types. SDKs are
imported lazily so a missing optional dependency only breaks the tier that
needs it, not the whole process.

Adding a provider = add a subclass + register it in `BUILDERS`. The model id
comes from config (`llm.chain[*].model`), so swapping models is a config edit.
"""

from __future__ import annotations

import abc
import re
from collections.abc import Callable

from src.config import get_settings
from src.llm.errors import AuthError, ContentError, PayloadTooLarge, ProviderError, RateLimited

_CONTEXT_HINTS = re.compile(
    r"(maximum context length|context window|too many tokens|reduce the length|"
    r"payload too large|request entity too large|string too long)",
    re.I,
)


def _classify_generic(exc: Exception, provider: str) -> Exception:
    msg = str(exc)
    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if status == 429 or "rate limit" in msg.lower() or "429" in msg:
        retry_after = None
        m = re.search(r"retry(?:-| )after[^0-9]*([0-9.]+)", msg, re.I)
        if m:
            retry_after = float(m.group(1))
        return RateLimited(msg, provider=provider, retry_after=retry_after)
    if status == 413 or _CONTEXT_HINTS.search(msg):
        return PayloadTooLarge(msg, provider=provider, status=413)
    if status in (401, 403) or "api key" in msg.lower() or "unauthorized" in msg.lower():
        return AuthError(msg, provider=provider, status=status)
    return ProviderError(msg, provider=provider, status=status)


class Provider(abc.ABC):
    name: str

    @abc.abstractmethod
    async def complete(self, system: str, user: str, *, model: str, temperature: float = 0.0) -> str:
        ...


class GeminiProvider(Provider):
    name = "gemini"
    _API = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    def __init__(self) -> None:
        key = get_settings().gemini_api_key
        if not key:
            raise AuthError("GEMINI_API_KEY not set", provider=self.name, status=401)
        self._key = key

    async def complete(self, system, user, *, model, temperature=0.0) -> str:
        # Direct REST call. The google-generativeai SDK defaults to a gRPC
        # transport that is minutes-per-call slow on some networks; REST is
        # 10-30x faster and genuinely async here (no thread pool).

        import aiohttp

        payload = {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": [{"text": user}]}],
            "generationConfig": {
                "temperature": temperature,
                "responseMimeType": "application/json",
            },
        }
        url = self._API.format(model=model)
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    url, params={"key": self._key}, json=payload,
                    timeout=aiohttp.ClientTimeout(total=120),
                ) as resp:
                    body = await resp.text()
                    if resp.status == 429:
                        raise RateLimited(body[:200], provider=self.name)
                    if resp.status in (400, 413) and _CONTEXT_HINTS.search(body):
                        raise PayloadTooLarge(body[:200], provider=self.name, status=413)
                    if resp.status in (401, 403):
                        raise AuthError(body[:200], provider=self.name, status=resp.status)
                    if resp.status >= 400:
                        raise ProviderError(f"HTTP {resp.status}: {body[:200]}",
                                            provider=self.name, status=resp.status)
                    data = __import__("json").loads(body)
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise ProviderError(f"{type(exc).__name__}: {exc}", provider=self.name) from exc

        try:
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError) as exc:
            reason = data.get("promptFeedback") or data
            raise ContentError(f"no text in response: {str(reason)[:200]}",
                               provider=self.name) from exc


class _OpenAICompatProvider(Provider):
    """Groq, OpenRouter, DeepSeek — all speak the OpenAI chat API."""

    base_url: str
    env_attr: str

    def __init__(self) -> None:
        key = getattr(get_settings(), self.env_attr)
        if not key:
            raise AuthError(f"{self.env_attr} not set", provider=self.name, status=401)
        from openai import AsyncOpenAI  # lazy

        self._client = AsyncOpenAI(api_key=key, base_url=self.base_url)

    async def complete(self, system, user, *, model, temperature=0.0) -> str:
        try:
            resp = await self._client.chat.completions.create(
                model=model,
                temperature=temperature,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
        except Exception as exc:  # noqa: BLE001
            raise _classify_generic(exc, self.name) from exc
        content = resp.choices[0].message.content
        if not content:
            raise ContentError("empty completion", provider=self.name)
        return content


class GroqProvider(_OpenAICompatProvider):
    name = "groq"
    base_url = "https://api.groq.com/openai/v1"
    env_attr = "groq_api_key"


class OpenRouterProvider(_OpenAICompatProvider):
    name = "openrouter"
    base_url = "https://openrouter.ai/api/v1"
    env_attr = "openrouter_api_key"


class DeepSeekProvider(_OpenAICompatProvider):
    name = "deepseek"
    base_url = "https://api.deepseek.com/v1"
    env_attr = "deepseek_api_key"


BUILDERS: dict[str, Callable[[], Provider]] = {
    "gemini": GeminiProvider,
    "groq": GroqProvider,
    "openrouter": OpenRouterProvider,
    "deepseek": DeepSeekProvider,
}


def build_provider(name: str) -> Provider:
    try:
        return BUILDERS[name]()
    except KeyError:
        raise ValueError(f"unknown LLM provider '{name}'") from None
