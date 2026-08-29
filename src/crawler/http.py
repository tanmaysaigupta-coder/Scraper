"""Async HTTP fetcher: the one place outbound requests happen.

Guarantees:
  * global concurrency cap (asyncio.Semaphore)
  * per-host rate limiting (token bucket via aiolimiter)
  * retry with exponential backoff + jitter on 429/5xx/timeout/conn-reset
  * honours Retry-After on 429
  * optional rotating proxy (PROXY_URL) for anti-bot (Phase V)
  * single shared session, explicit lifecycle via async context manager

Escalation to a real browser (Playwright) for JS-rendered / Cloudflare pages
lives in `src/crawler/browser.py`; this module is the cheap path tried first.
"""

from __future__ import annotations

import asyncio
import random
from types import TracebackType
from typing import Any

import aiohttp
from aiolimiter import AsyncLimiter
from yarl import URL

from src.config import get_pipeline_config, get_settings
from src.logging_setup import get_logger

log = get_logger("crawl")

_RETRY_STATUS = {429, 500, 502, 503, 504}
_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
]


class FetchError(RuntimeError):
    def __init__(self, message: str, *, url: str, status: int | None = None):
        super().__init__(message)
        self.url = url
        self.status = status


class AsyncFetcher:
    def __init__(self) -> None:
        cfg = get_pipeline_config()["crawler"]
        self._max_conc: int = int(cfg["max_concurrency"])
        self._timeout = aiohttp.ClientTimeout(total=float(cfg["request_timeout_s"]))
        self._max_retries = int(cfg["max_retries"])
        self._backoff_base = float(cfg["backoff_base_s"])
        self._backoff_max = float(cfg["backoff_max_s"])
        self._per_host_rps = float(cfg["per_host_rps"])
        self._default_ua = cfg.get("user_agent") or _UA_POOL[0]
        self._proxy = get_settings().proxy_url or None

        self._sem = asyncio.Semaphore(self._max_conc)
        self._host_limiters: dict[str, AsyncLimiter] = {}
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> AsyncFetcher:
        self._session = aiohttp.ClientSession(
            timeout=self._timeout,
            headers={"User-Agent": self._default_ua, "Accept-Language": "en-US,en;q=0.9"},
            trust_env=True,
        )
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                        tb: TracebackType | None) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def _limiter(self, host: str) -> AsyncLimiter:
        if host not in self._host_limiters:
            # max_rate over a 1s period == requests/second for that host
            self._host_limiters[host] = AsyncLimiter(max(1.0, self._per_host_rps), 1.0)
        return self._host_limiters[host]

    def _sleep_for(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return retry_after
        expo = min(self._backoff_max, self._backoff_base * (2 ** attempt))
        return expo * (0.5 + random.random())

    async def fetch_text(self, url: str, **kw: Any) -> str:
        _, body = await self._request("GET", url, **kw)
        return body

    async def fetch_json(self, url: str, *, headers: dict | None = None, **kw: Any) -> Any:
        import json

        merged = {"Accept": "application/json"}
        if headers:
            merged.update(headers)
        _, body = await self._request("GET", url, headers=merged, **kw)
        return json.loads(body)

    async def post_json(self, url: str, *, json_body: Any, headers: dict | None = None,
                        **kw: Any) -> Any:
        import json as _json

        merged = {"Accept": "application/json", "Content-Type": "application/json"}
        if headers:
            merged.update(headers)
        _, body = await self._request("POST", url, headers=merged, data=_json.dumps(json_body), **kw)
        return _json.loads(body)

    async def _request(self, method: str, url: str, *, headers: dict | None = None,
                       params: dict | None = None, data: str | None = None) -> tuple[int, str]:
        if self._session is None:
            raise RuntimeError("AsyncFetcher used outside 'async with'")
        host = URL(url).host or ""
        merged = {"User-Agent": random.choice(_UA_POOL)}
        if headers:
            merged.update(headers)

        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                async with self._sem, self._limiter(host):
                    async with self._session.request(
                        method, url, headers=merged, params=params, data=data,
                        proxy=self._proxy, allow_redirects=True,
                    ) as resp:
                        text = await resp.text()
                        if resp.status in _RETRY_STATUS:
                            ra = resp.headers.get("Retry-After")
                            wait = self._sleep_for(attempt, float(ra) if ra and ra.isdigit() else None)
                            log.info("retry_status", url=url, status=resp.status,
                                     attempt=attempt, sleep_s=round(wait, 2))
                            await asyncio.sleep(wait)
                            last_exc = FetchError("retryable status", url=url, status=resp.status)
                            continue
                        if resp.status >= 400:
                            raise FetchError(f"HTTP {resp.status}", url=url, status=resp.status)
                        return resp.status, text
            except (TimeoutError, aiohttp.ClientError) as exc:
                wait = self._sleep_for(attempt, None)
                log.info("retry_exc", url=url, attempt=attempt, err=str(exc)[:120], sleep_s=round(wait, 2))
                await asyncio.sleep(wait)
                last_exc = exc

        raise FetchError(f"exhausted retries: {last_exc}", url=url,
                         status=getattr(last_exc, "status", None))
