"""Playwright escalation path for JS-rendered / Cloudflare-protected sources
(Phase V).

The cheap `AsyncFetcher` is always tried first. A source is escalated here only
when it returns a challenge page, an empty shell, or a 403 from the anti-bot
edge. Strategy (also written up in architecture.pdf):

  * real headless Chromium with a realistic UA, viewport, locale, timezone
  * `navigator.webdriver` patched out; plausible `languages` / `plugins`
  * human-ish pacing: randomized delays, mouse move before click
  * one browser context per proxy identity; rotate proxy on challenge
  * exponential backoff on repeated challenges; never solve CAPTCHAs — park the
    domain and alert instead
  * cache rendered HTML aggressively so a domain is hit as little as possible

This module is intentionally thin at trial scale; the docstring + architecture
doc carry the "how would you do this at 500k" answer.
"""

from __future__ import annotations

import asyncio
import random
from contextlib import asynccontextmanager

from src.config import get_pipeline_config, get_settings
from src.logging_setup import get_logger

log = get_logger("crawl")

_CHALLENGE_MARKERS = (
    "cf-browser-verification", "cf_chl_opt", "Just a moment...",
    "captcha-delivery.com", "DataDome", "px-captcha", "Access to this page has been denied",
)


def looks_like_challenge(html: str, status: int) -> bool:
    if status in (403, 429, 503) and len(html) < 4000:
        return True
    return any(m in html for m in _CHALLENGE_MARKERS)


@asynccontextmanager
async def browser_page(*, proxy: str | None = None):
    from playwright.async_api import async_playwright

    cfg = get_pipeline_config()["crawler"]
    proxy = proxy or get_settings().proxy_url
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = await browser.new_context(
            user_agent=cfg.get("user_agent"),
            locale="en-US",
            timezone_id="America/New_York",
            viewport={"width": 1366, "height": 900},
            proxy={"server": proxy} if proxy else None,
        )
        await context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        )
        page = await context.new_page()
        try:
            yield page
        finally:
            await context.close()
            await browser.close()


class BrowserUnavailable(RuntimeError):
    """Playwright/Chromium could not be launched in this environment."""


async def render(url: str, *, proxy: str | None = None, wait_selector: str | None = None) -> str:
    try:
        async with browser_page(proxy=proxy) as page:
            await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            await asyncio.sleep(random.uniform(1.2, 3.0))
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=15_000)
                except Exception:  # noqa: BLE001
                    log.info("render_selector_timeout", url=url, selector=wait_selector)
            html = await page.content()
            if looks_like_challenge(html, 200):
                log.warning("render_challenged", url=url)
            return html
    except ImportError as exc:
        raise BrowserUnavailable("playwright not installed") from exc
    except Exception as exc:  # noqa: BLE001 - launch failures ("spawn UNKNOWN"), sandbox denials
        msg = str(exc)
        if "spawn" in msg or "launch" in msg.lower() or "executable" in msg.lower():
            raise BrowserUnavailable(f"cannot launch browser: {msg[:160]}") from exc
        raise
