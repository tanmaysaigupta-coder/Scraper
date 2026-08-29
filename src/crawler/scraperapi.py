"""ScraperAPI escalation path (Phase V).

When a direct request hits an anti-bot wall (Cloudflare / DataDome 403, JS-only
shell), the fetch is retried through ScraperAPI, which rotates residential IPs
and — with `render=true` — runs a real browser server-side. This is the
concrete "handle high-value sources that aggressively block automated requests"
answer.

JS rendering legitimately takes 40-90s per page, so this client uses its own
long timeout and retry budget rather than the crawler's fast defaults. Each
render call spends more ScraperAPI credits, so callers escalate only on a real
block, and results are cached upstream (raw HTML -> extraction cache).
"""

from __future__ import annotations

import asyncio
from urllib.parse import urlencode

import aiohttp

from src.config import get_settings
from src.logging_setup import get_logger

log = get_logger("crawl")

_ENDPOINT = "https://api.scraperapi.com/"


class ScraperAPIError(RuntimeError):
    pass


def available() -> bool:
    return bool(get_settings().scraper_api_key)


async def scraper_get(
    url: str,
    *,
    render: bool = False,
    country: str | None = None,
    timeout_s: float = 120.0,
    max_retries: int = 3,
) -> str:
    key = get_settings().scraper_api_key
    if not key:
        raise ScraperAPIError("SCRAPER_API_KEY not set")

    params = {"api_key": key, "url": url}
    if render:
        params["render"] = "true"
    if country:
        params["country_code"] = country
    endpoint = f"{_ENDPOINT}?{urlencode(params)}"

    last: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(
                    endpoint, timeout=aiohttp.ClientTimeout(total=timeout_s)
                ) as resp:
                    body = await resp.text()
                    if resp.status == 200:
                        return body
                    if resp.status in (429, 500, 502, 503, 504):
                        last = ScraperAPIError(f"HTTP {resp.status}")
                        await asyncio.sleep(2 ** attempt * 3)
                        continue
                    raise ScraperAPIError(f"HTTP {resp.status}: {body[:160]}")
        except (aiohttp.ClientError, TimeoutError) as exc:
            last = exc
            log.info("scraperapi_retry", url=url, attempt=attempt, err=f"{type(exc).__name__}")
            await asyncio.sleep(2 ** attempt * 3)

    raise ScraperAPIError(f"exhausted retries for {url}: {last}")
