"""Generic directory crawler for the Startup + Product verticals (Phase I).

Design: the crawler is source-agnostic. A source in `sources.yaml` provides:

    kind: html
    base_url: "..."
    list_pages:  { pattern: "...?page={n}", start: 1, max_pages: 60 }
    selectors:   { item: "div.card", link: "a.name::attr(href)" }
    render: false            # true -> route through Playwright (Phase V)

For each item page we fetch raw HTML, reduce it to main text, and hand it to
the LLM extraction engine with the Startup/Product schema. The resolver
canonicalizes names before the record is emitted.

The two default sources (YC companies, There's An AI For That) are marked
PROPOSED in config and still need their `selectors` block filled in + an
anti-bot check — that is a deliberate open decision, not an oversight.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from selectolax.parser import HTMLParser

from src.crawler.browser import looks_like_challenge, render
from src.crawler.extract_text import extract_main_text
from src.crawler.http import AsyncFetcher, FetchError
from src.crawler.seen_store import SeenStore, build_seen_store
from src.llm.orchestrator import ExtractionFailed, LLMOrchestrator
from src.logging_setup import get_logger
from src.resolver.entity_resolver import EntityResolver
from src.schemas import (
    ProductContent,
    ProductRecord,
    Source,
    StartupContent,
    StartupData,
    StartupRecord,
)

log = get_logger("crawl")


async def _get_html(fetcher: AsyncFetcher, url: str, *, want_render: bool) -> str:
    if not want_render:
        try:
            html = await fetcher.fetch_text(url)
            if not looks_like_challenge(html, 200):
                return html
            log.info("escalate_to_browser", url=url)
        except FetchError as exc:
            if exc.status not in (403, 429, 503):
                raise
            log.info("escalate_to_browser", url=url, status=exc.status)
    return await render(url)


def _iter_list_urls(source: dict) -> list[str]:
    lp = source.get("list_pages") or {}
    pattern = lp.get("pattern")
    if not pattern:
        return [source["base_url"]]
    start = int(lp.get("start", 1))
    max_pages = int(lp.get("max_pages", 20))
    return [source["base_url"].rstrip("/") + "/" + pattern.format(n=n)
            for n in range(start, start + max_pages)]


async def _item_links(fetcher: AsyncFetcher, source: dict, list_url: str) -> list[str]:
    sel = source.get("selectors") or {}
    item_sel, link_sel = sel.get("item"), sel.get("link")
    if not item_sel or not link_sel:
        log.warning("directory_selectors_missing", source=source["name"],
                    msg="fill sources.yaml selectors.item / selectors.link")
        return []
    html = await _get_html(fetcher, list_url, want_render=source.get("render", False))
    tree = HTMLParser(html)
    attr = None
    if "::attr(" in link_sel:
        link_sel, attr = link_sel.split("::attr(")
        attr = attr.rstrip(")")
    links: list[str] = []
    for item in tree.css(item_sel):
        a = item.css_first(link_sel)
        if not a:
            continue
        href = a.attributes.get(attr) if attr else a.attributes.get("href")
        if href:
            links.append(_absolutize(source["base_url"], href))
    return links


def _absolutize(base: str, href: str) -> str:
    from urllib.parse import urljoin

    return urljoin(base, href)


async def crawl_directory(
    vertical: str,
    *,
    max_records: int,
    resolver: EntityResolver,
    seen: SeenStore | None = None,
) -> AsyncIterator[StartupRecord | ProductRecord]:
    """vertical in {'startups', 'products'}."""
    from src.config import get_sources_config

    assert vertical in ("startups", "products")
    sources = get_sources_config()[vertical]
    own_seen = seen is None
    seen = seen or build_seen_store()
    llm = LLMOrchestrator.from_config()
    emitted = 0

    try:
        async with AsyncFetcher() as fetcher:
            for source in sources:
                for list_url in _iter_list_urls(source):
                    if emitted >= max_records:
                        return
                    links = await _item_links(fetcher, source, list_url)
                    if not links:
                        break
                    sem = asyncio.Semaphore(8)

                    async def _one(url: str, *, _src=source, _sem=sem):
                        nonlocal emitted
                        if emitted >= max_records or not seen.add_if_new(url, source=_src["name"]):
                            return None
                        async with _sem:
                            rec = await _extract_item(fetcher, llm, resolver, _src, vertical, url)
                        if rec is not None:
                            emitted += 1
                            return rec
                        return None

                    for coro in asyncio.as_completed([_one(u) for u in links]):
                        rec = await coro
                        if rec is not None:
                            yield rec
    finally:
        if own_seen:
            seen.close()


async def _extract_item(fetcher, llm: LLMOrchestrator, resolver, source, vertical, url):
    try:
        html = await _get_html(fetcher, url, want_render=source.get("render", False))
    except FetchError as exc:
        log.info("item_fetch_failed", url=url, status=exc.status)
        return None
    text = extract_main_text(html)
    if len(text) < 120:
        return None

    try:
        if vertical == "startups":
            res = await llm.extract(
                raw_text=text, target=StartupContent,
                instructions="Extract the startup's name (entityName) and employeeCount if stated.",
                context={"source_url": url},
            )
            content: StartupContent = res.model  # type: ignore[assignment]
            content = StartupContent(
                entityName=resolver.resolve_str(content.entityName),
                data=StartupData(employeeCount=content.data.employeeCount),
            )
            return StartupRecord(source=Source(name=source["name"], url=url), content=content)

        res = await llm.extract(
            raw_text=text, target=ProductContent,
            instructions=("Extract the owning startup's name (startupName) and pricingModel "
                          "as one of FREE, FREEMIUM, PAID, ENTERPRISE if determinable."),
            context={"source_url": url},
        )
        content = res.model  # type: ignore[assignment]
        content = ProductContent(
            startupName=resolver.resolve_str(content.startupName),
            pricingModel=content.pricingModel,
        )
        return ProductRecord(source=Source(name=source["name"], url=url), content=content)
    except ExtractionFailed as exc:
        log.warning("item_extract_failed", url=url, tried=exc.tried)
        return None
