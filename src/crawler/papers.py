"""Research-paper vertical: arXiv -> GitHub correlation -> star counts -> record.

Pipeline per paper:
  1. arXiv Atom API gives title/authors/abstract/date/url.
  2. GitHub URL discovery, cheapest first:
       a. regex over the abstract text
       b. Papers-with-Code API lookup by arxiv_id (official_implementation)
  3. GitHubStars fills `github_stars` (current, live).
  4. Assemble a ResearchPaperRecord (already schema-valid — this vertical is
     structured enough that the LLM layer is optional; the abstract can still be
     sent through extraction for enrichment if desired).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from src.crawler.arxiv import iter_arxiv
from src.crawler.github import GitHubStars, extract_repo
from src.crawler.http import AsyncFetcher, FetchError
from src.logging_setup import get_logger
from src.schemas import ResearchPaperContent, ResearchPaperRecord, Source

log = get_logger("crawl")

_PWC = "https://paperswithcode.com/api/v1/papers/{arxiv_id}/repositories/"


async def _github_from_pwc(fetcher: AsyncFetcher, arxiv_id: str) -> str | None:
    try:
        data = await fetcher.fetch_json(_PWC.format(arxiv_id=arxiv_id))
    except (FetchError, ValueError):
        return None
    results = data.get("results") or []
    official = [r for r in results if r.get("is_official")] or results
    if not official:
        return None
    official.sort(key=lambda r: r.get("stars", 0), reverse=True)
    return official[0].get("url")


async def _discover_github(fetcher: AsyncFetcher, paper: dict) -> str | None:
    if extract_repo(paper.get("summary", "")):
        m = extract_repo(paper["summary"])
        return f"https://github.com/{m[0]}/{m[1]}"
    return await _github_from_pwc(fetcher, paper["arxiv_id"])


async def crawl_research_papers(
    *, max_records: int = 1000
) -> AsyncIterator[ResearchPaperRecord]:
    from src.config import get_sources_config

    src_cfg = next(
        s for s in get_sources_config()["research_papers"] if s["name"] == "arXiv"
    )

    async with AsyncFetcher() as fetcher:
        stars = GitHubStars(fetcher)
        buffer: list[dict] = []

        async for paper in iter_arxiv(
            fetcher,
            base_url=src_cfg["base_url"],
            categories=src_cfg["categories"],
            page_size=src_cfg.get("page_size", 200),
            max_records=max_records,
        ):
            buffer.append(paper)
            if len(buffer) >= 50:
                async for rec in _flush(fetcher, stars, buffer):
                    yield rec
                buffer.clear()

        async for rec in _flush(fetcher, stars, buffer):
            yield rec


async def _flush(fetcher, stars: GitHubStars, papers: list[dict]) -> AsyncIterator[ResearchPaperRecord]:
    gh_urls = await asyncio.gather(*(_discover_github(fetcher, p) for p in papers))
    star_counts = await stars.stars_batch(gh_urls)

    for paper, gh_url, star in zip(papers, gh_urls, star_counts, strict=False):
        try:
            yield ResearchPaperRecord(
                source=Source(name="arXiv", url=paper["paper_url"]),
                content=ResearchPaperContent(
                    title=paper["title"],
                    authors=paper["authors"],
                    paper_url=paper["paper_url"],
                    github_url=gh_url,
                    github_stars=star,
                    published_date=paper["published_date"],
                ),
            )
        except Exception as exc:  # noqa: BLE001 - never let one bad record kill the run
            log.warning("paper_record_invalid", arxiv_id=paper.get("arxiv_id"), err=str(exc)[:160])
