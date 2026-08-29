"""arXiv acquisition via the public Atom API (no key required).

Paginates newest-first through the configured CS/ML categories and yields a
lightweight dict per paper. GitHub correlation + star counts happen in
`papers.py`; this module only touches arXiv.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from selectolax.parser import HTMLParser

from src.crawler.http import AsyncFetcher
from src.logging_setup import get_logger

log = get_logger("crawl")

_NS = {"atom": "http://www.w3.org/2005/Atom"}


def _text(node, sel: str) -> str:
    found = node.css_first(sel)
    return found.text(strip=True) if found else ""


async def iter_arxiv(
    fetcher: AsyncFetcher,
    *,
    base_url: str,
    categories: list[str],
    page_size: int = 200,
    max_records: int = 1000,
) -> AsyncIterator[dict]:
    cat_query = " OR ".join(f"cat:{c}" for c in categories)
    emitted = 0
    start = 0

    while emitted < max_records:
        params = {
            "search_query": cat_query,
            "start": str(start),
            "max_results": str(min(page_size, max_records - emitted)),
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
        xml = await fetcher.fetch_text(base_url, params=params)
        tree = HTMLParser(xml)
        entries = tree.css("entry")
        if not entries:
            log.info("arxiv_end", start=start)
            return

        for e in entries:
            arxiv_id = _text(e, "id").rsplit("/", 1)[-1]
            links = e.css("link")
            pdf_url = next(
                (ln.attributes.get("href", "") for ln in links
                 if ln.attributes.get("title") == "pdf"),
                _text(e, "id"),
            )
            published_raw = _text(e, "published")
            try:
                published = datetime.fromisoformat(published_raw.replace("Z", "+00:00"))
            except ValueError:
                published = None
            comment_node = e.css_first("arxiv\\:comment")
            yield {
                "arxiv_id": arxiv_id,
                "title": " ".join(_text(e, "title").split()),
                "authors": [a.text(strip=True) for a in e.css("author name")],
                "summary": _text(e, "summary"),
                "comment": comment_node.text(strip=True) if comment_node else "",
                "paper_url": _text(e, "id") or pdf_url,
                "pdf_url": pdf_url,
                "published_date": published.astimezone(UTC) if published else None,
                "primary_category": (
                    e.css_first("arxiv\\:primary_category").attributes.get("term")
                    if e.css_first("arxiv\\:primary_category") else None
                ),
            }
            emitted += 1
            if emitted >= max_records:
                return

        start += len(entries)
        await asyncio.sleep(3.0)  # arXiv asks for >=3s between calls
