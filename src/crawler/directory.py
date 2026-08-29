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

from src.crawler.browser import BrowserUnavailable, looks_like_challenge, render
from src.crawler.extract_text import extract_main_text
from src.crawler.http import AsyncFetcher, FetchError
from src.crawler.scraperapi import ScraperAPIError, scraper_get
from src.crawler.scraperapi import available as scraperapi_available
from src.crawler.seen_store import SeenStore, build_seen_store
from src.llm.orchestrator import ExtractionFailed, LLMOrchestrator
from src.logging_setup import get_logger
from src.resolver.entity_resolver import EntityResolver
from src.schemas import (
    ProductContent,
    ProductDraft,
    ProductRecord,
    Source,
    StartupContent,
    StartupData,
    StartupDraft,
    StartupRecord,
)

log = get_logger("crawl")


async def _get_html(fetcher: AsyncFetcher, url: str, *, want_render: bool) -> str:
    """Escalation ladder, cheapest first:
      1. direct fetch
      2. ScraperAPI without JS render (residential IPs; ~1 credit)
      3. ScraperAPI with render=true  (server-side browser; ~10-25 credits)
      4. local Playwright
    `want_render=True` skips straight to step 3 (the page needs JS to populate).
    """
    if not want_render:
        try:
            html = await fetcher.fetch_text(url)
            if not looks_like_challenge(html, 200):
                return html
            log.info("escalate", url=url, reason="challenge")
        except FetchError as exc:
            if exc.status not in (403, 429, 503):
                raise
            log.info("escalate", url=url, status=exc.status)

        if scraperapi_available():
            try:
                html = await scraper_get(url, render=False)
                if not looks_like_challenge(html, 200):
                    return html
            except ScraperAPIError as exc:
                log.info("scraperapi_norender_failed", url=url, err=str(exc)[:120])

    if scraperapi_available():
        try:
            return await scraper_get(url, render=True, timeout_s=180)
        except ScraperAPIError as exc:
            log.warning("scraperapi_failed", url=url, err=str(exc)[:160])

    try:
        return await render(url)  # local Playwright, last resort
    except BrowserUnavailable as exc:
        raise FetchError(f"all escalation paths exhausted: {exc}", url=url, status=None) from exc


def _iter_list_urls(source: dict) -> list[str]:
    # explicit list of listing URLs wins over a {n} pattern
    explicit = source.get("list_urls")
    if explicit:
        base = source["base_url"].rstrip("/")
        return [u if u.startswith("http") else f"{base}/{u.lstrip('/')}" for u in explicit]

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
    item_sel = sel.get("item")
    link_sel = sel.get("link", "")
    if not item_sel:
        log.warning("directory_selectors_missing", source=source["name"],
                    msg="fill sources.yaml selectors.item")
        return []
    html = await _get_html(fetcher, list_url, want_render=source.get("render", False))
    tree = HTMLParser(html)

    attr = "href"
    if "::attr(" in link_sel:
        link_sel, attr = link_sel.split("::attr(")
        attr = attr.rstrip(")")
    link_sel = link_sel.strip()

    seen: set[str] = set()
    links: list[str] = []
    for item in tree.css(item_sel):
        # link_sel empty or "self" -> the matched item is itself the <a>
        a = item if link_sel in ("", "self") else item.css_first(link_sel)
        if not a:
            continue
        href = a.attributes.get(attr)
        if href:
            absolute = _absolutize(source["base_url"], href)
            if absolute not in seen:
                seen.add(absolute)
                links.append(absolute)
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
                    # only take as many new links as we still need (+ margin for
                    # extraction failures) so we never over-crawl the target
                    fresh = [u for u in links if seen.add_if_new(u, source=source["name"])]
                    need = max_records - emitted
                    fresh = fresh[: need + need // 2 + 5]
                    sem = asyncio.Semaphore(8)

                    async def _one(url: str, *, _src=source, _sem=sem):
                        async with _sem:
                            return await _extract_item(fetcher, llm, resolver, _src, vertical, url)

                    for coro in asyncio.as_completed([_one(u) for u in fresh]):
                        rec = await coro
                        if rec is not None:
                            emitted += 1
                            yield rec
                            if emitted >= max_records:
                                return
    finally:
        if own_seen:
            seen.close()


async def _extract_item(fetcher, llm: LLMOrchestrator, resolver, source, vertical, url):
    # detail pages default to no JS render (cheaper); override with detail_render
    detail_render = source.get("detail_render", False)
    try:
        html = await _get_html(fetcher, url, want_render=detail_render)
    except FetchError as exc:
        log.info("item_fetch_failed", url=url, status=exc.status)
        return None
    text = extract_main_text(html)
    if len(text) < 120:
        return None
    page_name = _page_title(html)

    try:
        if vertical == "startups":
            res = await llm.extract(
                raw_text=text, target=StartupDraft,
                instructions="entityName = the startup's name. employeeCount = integer if stated.",
                context={"source_url": url, "page_title": page_name},
            )
            d: StartupDraft = res.model  # type: ignore[assignment]
            name = (d.entityName or page_name or "").strip()
            if not name:
                return None
            return StartupRecord(
                source=Source(name=source["name"], url=url),
                content=StartupContent(
                    entityName=resolver.resolve_str(name),
                    data=StartupData(employeeCount=d.employeeCount),
                ),
            )

        res = await llm.extract(
            raw_text=text, target=ProductDraft,
            instructions=(
                "startupName = the company/brand behind this product; if none is named "
                "distinctly, use the product's own name. Only use a name present in the text. "
                "pricingModel = FREE, FREEMIUM, PAID or ENTERPRISE only when the text makes "
                "it clear, else null."
            ),
            context={"source_url": url, "page_title": page_name},
        )
        d = res.model  # type: ignore[assignment]
        name = (d.startupName or page_name or "").strip()
        if not name:
            return None
        return ProductRecord(
            source=Source(name=source["name"], url=url),
            content=ProductContent(
                startupName=resolver.resolve_str(name),
                pricingModel=d.pricingModel,
            ),
        )
    except ExtractionFailed:
        # extraction unusable -> still emit from the page title alone, no fabrication
        if not page_name:
            return None
        if vertical == "startups":
            return StartupRecord(
                source=Source(name=source["name"], url=url),
                content=StartupContent(entityName=resolver.resolve_str(page_name)),
            )
        return ProductRecord(
            source=Source(name=source["name"], url=url),
            content=ProductContent(startupName=resolver.resolve_str(page_name)),
        )


def _page_title(html: str) -> str:
    tree = HTMLParser(html)
    node = tree.css_first("h1") or tree.css_first("title")
    if not node:
        return ""
    t = node.text(strip=True)
    # trim common " - AI Tool For X" / " | Product Hunt" style suffixes
    for sep in (" - ", " | ", " – "):
        if sep in t:
            t = t.split(sep)[0]
    return t.strip()[:120]
