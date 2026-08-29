"""RSS / JSON-API acquisition for news + jobs with a hard 24h freshness gate.

For each configured source:
  * pull the feed (RSS via feedparser, or a JSON API shape we normalize)
  * parse each item's date with `dates.parse_date`
  * keep it only if BOTH:
      - `within_window(date, hours=24)`  (hard gate from the brief), OR
        the source has no reliable date AND `FreshnessState.is_new` says so
        (heuristic fallback)
      - `seen_store.add_if_new(url)`     (never process the same item twice)
  * for news, fetch the article page and extract full text
  * update `FreshnessState` high-water mark

Returns raw dicts; the pipeline hands them to the LLM layer for schema-shaping.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import feedparser

from src.config import get_pipeline_config, get_sources_config
from src.crawler.dates import FreshnessState, parse_date, within_window
from src.crawler.extract_text import extract_main_text
from src.crawler.http import AsyncFetcher, FetchError
from src.crawler.seen_store import SeenStore, build_seen_store
from src.logging_setup import get_logger

log = get_logger("crawl")


def _rss_items(raw: str) -> list[dict]:
    parsed = feedparser.parse(raw)
    items = []
    for e in parsed.entries:
        date_raw = e.get("published") or e.get("updated") or e.get("pubDate") or ""
        items.append({
            "title": e.get("title", ""),
            "url": e.get("link", ""),
            "date_raw": date_raw,
            "author": e.get("author", ""),
            "summary": e.get("summary", ""),
        })
    return items


def _api_job_items(payload: Any, source_name: str) -> list[dict]:
    """Normalize the common remote-job API shapes (Remotive / RemoteOK / Jobicy / Arbeitnow)."""
    if isinstance(payload, dict) and "jobs" in payload:
        rows = payload["jobs"]
    elif isinstance(payload, list):
        rows = [r for r in payload if isinstance(r, dict) and r.get("id") != "legal"]
    elif isinstance(payload, dict):
        rows = payload.get("results") or payload.get("data") or []
    else:
        rows = []

    items = []
    for r in rows:
        items.append({
            "title": r.get("title") or r.get("position") or r.get("jobTitle") or "",
            "url": (r.get("url") or r.get("apply_url") or r.get("application_link")
                    or r.get("job_url") or ""),
            "date_raw": (r.get("publication_date") or r.get("date") or r.get("pubDate")
                         or r.get("created_at") or r.get("epoch")
                         or r.get("created") or ""),
            "company": (r.get("company_name") or r.get("company") or r.get("companyName") or ""),
            "is_remote": True if "remote" in source_name.lower() else r.get("remote", True),
            "location": (r.get("candidate_required_location") or r.get("location")
                         or r.get("jobGeo") or r.get("job_geo") or ""),
            "summary": (r.get("description") or r.get("jobExcerpt") or r.get("jobDescription")
                        or "")[:4000],
        })
    return items


async def _load_source(fetcher: AsyncFetcher, source: dict, kind_hint: str) -> list[dict]:
    kind = source.get("kind")
    try:
        if kind == "rss":
            raw = await fetcher.fetch_text(source["feed_url"])
            return _rss_items(raw)
        if kind == "api":
            payload = await fetcher.fetch_json(source["base_url"], params=source.get("params"))
            return _api_job_items(payload, source["name"])
    except (FetchError, ValueError) as exc:
        log.warning("source_load_failed", source=source["name"], err=str(exc)[:160])
    return []


async def crawl_feed_vertical(vertical: str, *, seen: SeenStore | None = None) -> list[dict]:
    """vertical in {'news', 'jobs'}."""
    assert vertical in ("news", "jobs")
    window_h = int(get_pipeline_config()["freshness"]["window_hours"])
    state = FreshnessState(get_pipeline_config()["freshness"]["state_path"])
    own_seen = seen is None
    seen = seen or build_seen_store()
    now = datetime.now(UTC)
    out: list[dict] = []

    try:
        async with AsyncFetcher() as fetcher:
            sources = get_sources_config()[vertical]
            per_source = await asyncio.gather(
                *(_load_source(fetcher, s, vertical) for s in sources)
            )

            for source, items in zip(sources, per_source, strict=False):
                kept = 0
                for it in items:
                    dt = parse_date(it["date_raw"], now=now)
                    fresh = within_window(dt, hours=window_h, now=now)
                    if not fresh and not state.is_new(source["name"], dt):
                        continue
                    if not it["url"] or not seen.add_if_new(it["url"], source=source["name"]):
                        continue

                    record = {
                        "source_name": source["name"],
                        "url": it["url"],
                        "title": it["title"],
                        "published_date": dt.isoformat() if dt else None,
                        "author": it.get("author", ""),
                        "raw_summary": it.get("summary", ""),
                    }
                    if vertical == "news":
                        record["full_text"] = await _fetch_fulltext(fetcher, it["url"])
                    else:
                        record.update({
                            "company": it.get("company", ""),
                            "is_remote": it.get("is_remote"),
                            "location": it.get("location", ""),
                        })
                    out.append(record)
                    state.observe(source["name"], dt)
                    kept += 1
                log.info("source_done", source=source["name"], fetched=len(items), kept=kept)

        state.save()
    finally:
        if own_seen:
            seen.close()
    return out


async def _fetch_fulltext(fetcher: AsyncFetcher, url: str) -> str:
    from src.crawler.browser import looks_like_challenge, render
    from src.crawler.scraperapi import ScraperAPIError, scraper_get
    from src.crawler.scraperapi import available as scraperapi_available

    html = ""
    try:
        html = await fetcher.fetch_text(url)
        if not looks_like_challenge(html, 200):
            text = extract_main_text(html)
            if len(text) > 200:
                return text
    except FetchError as exc:
        if exc.status not in (403, 429, 503):
            return ""

    # anti-bot edge / thin render -> ScraperAPI (Phase V), then Playwright
    if scraperapi_available():
        try:
            text = extract_main_text(await scraper_get(url, render=False))
            if len(text) > 200:
                return text
        except ScraperAPIError as exc:
            log.info("fulltext_scraperapi_failed", url=url, err=str(exc)[:140])

    try:
        return extract_main_text(await render(url))
    except Exception as exc:  # noqa: BLE001 - BrowserUnavailable in sandboxes, or launch errors
        log.info("fulltext_browser_failed", url=url, err=str(exc)[:140])
        return extract_main_text(html) if html else ""
