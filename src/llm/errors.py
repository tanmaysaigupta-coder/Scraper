"""Typed LLM failure modes, normalized across providers.

Each provider adapter maps its native SDK/HTTP errors onto these so the
orchestrator can react uniformly:

  RateLimited      -> HTTP 429; retry same tier with exponential backoff + jitter
  PayloadTooLarge  -> HTTP 413 / context-length errors; re-chunk smaller, retry
  ProviderError    -> 5xx / transient; retry same tier a bounded number of times
  AuthError        -> 401/403; skip this tier entirely (bad/missing key)
  ContentError     -> model returned unusable output (non-JSON); retry then fall
"""

from __future__ import annotations


class LLMError(Exception):
    def __init__(self, message: str, *, provider: str | None = None, status: int | None = None):
        super().__init__(message)
        self.provider = provider
        self.status = status


class RateLimited(LLMError):
    def __init__(self, message: str, *, provider=None, status=429, retry_after: float | None = None):
        super().__init__(message, provider=provider, status=status)
        self.retry_after = retry_after


class PayloadTooLarge(LLMError):
    """413, or any 'maximum context length' / 'too many tokens' style error."""


class ProviderError(LLMError):
    """Transient upstream failure (5xx, connection reset, timeout)."""


class AuthError(LLMError):
    """Missing or rejected credentials — tier is unusable, do not retry it."""


class ContentError(LLMError):
    """Response received but not parseable into the requested JSON shape."""
