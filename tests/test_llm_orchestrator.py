"""Fallback-chain behaviour with fake providers (no network, no keys)."""

import asyncio

import pytest

from src.llm import orchestrator as orch_mod
from src.llm.errors import PayloadTooLarge, RateLimited
from src.llm.orchestrator import ExtractionFailed, LLMOrchestrator, _Tier
from src.schemas import JobContent


class FakeProvider:
    def __init__(self, name, script):
        self.name = name
        self._script = list(script)
        self.calls = 0

    async def complete(self, system, user, *, model, temperature=0.0):
        self.calls += 1
        action = self._script.pop(0) if self._script else "ok"
        if isinstance(action, Exception):
            raise action
        return '{"company": "OpenAI", "role_family": "Engineering", "is_remote": true}'


def _orch(providers, monkeypatch):
    o = LLMOrchestrator(
        _tiers=[_Tier(p.name, f"{p.name}-model", 50_000) for p in providers],
        attempts_per_tier=3, backoff_base_s=0, backoff_max_s=0,
    )
    o._providers = {p.name: p for p in providers}

    async def _no_sleep(*_a, **_k):
        return None

    monkeypatch.setattr(orch_mod.asyncio, "sleep", _no_sleep)
    return o


def test_first_tier_succeeds(monkeypatch):
    p1 = FakeProvider("gemini", ["ok"])
    p2 = FakeProvider("groq", [])
    o = _orch([p1, p2], monkeypatch)
    res = asyncio.run(o.extract(raw_text="hiring backend engineer", target=JobContent,
                                instructions="x"))
    assert res.provider == "gemini" and not res.fell_back
    assert p2.calls == 0
    assert res.model.company == "OpenAI"


def test_falls_back_after_rate_limit_exhaustion(monkeypatch):
    p1 = FakeProvider("gemini", [RateLimited("429"), RateLimited("429"), RateLimited("429")])
    p2 = FakeProvider("groq", ["ok"])
    o = _orch([p1, p2], monkeypatch)
    res = asyncio.run(o.extract(raw_text="x", target=JobContent, instructions="x"))
    assert res.provider == "groq" and res.fell_back
    assert p1.calls == 3


def test_payload_too_large_triggers_reshrink_then_success(monkeypatch):
    p1 = FakeProvider("gemini", [PayloadTooLarge("413"), "ok"])
    o = _orch([p1], monkeypatch)
    res = asyncio.run(o.extract(raw_text="x" * 5000, target=JobContent, instructions="x"))
    assert res.provider == "gemini" and res.attempts == 2


def test_all_tiers_exhausted_raises(monkeypatch):
    p1 = FakeProvider("gemini", [RateLimited("429")] * 3)
    p2 = FakeProvider("groq", [RateLimited("429")] * 3)
    o = _orch([p1, p2], monkeypatch)
    with pytest.raises(ExtractionFailed) as ei:
        asyncio.run(o.extract(raw_text="x", target=JobContent, instructions="x"))
    assert ei.value.tried == ["gemini:gemini-model", "groq:groq-model"]
