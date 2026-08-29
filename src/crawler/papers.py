"""Research-paper vertical: arXiv -> GitHub correlation -> star counts -> record.

Pipeline per paper:
  1. arXiv Atom API gives title/authors/abstract/comment/date/url.
  2. GitHub URL discovery, cheapest first:
       a. regex over the abstract text + the arXiv `comment` field
          (authors very often write "Code: https://github.com/..." there)
       b. regex over the arXiv full-text HTML render (arxiv.org/html/{id}),
          minus a denylist of arXiv's own infra repos
  3. GitHubStars fills `github_stars` (current, live).
  4. Assemble a ResearchPaperRecord (already schema-valid — this vertical is
     structured enough that the LLM layer is optional; the abstract can still be
     sent through extraction for enrichment if desired).

Papers-with-Code was retired in 2025 (its API now returns non-JSON), so that
path was removed.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator

from src.crawler.arxiv import iter_arxiv
from src.crawler.github import GitHubStars, extract_repo
from src.crawler.http import AsyncFetcher, FetchError
from src.logging_setup import get_logger
from src.schemas import ResearchPaperContent, ResearchPaperRecord, Source

log = get_logger("crawl")

_GH_RE = re.compile(r"github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)", re.I)
# arXiv's own footer / infra repos that appear on every HTML render
_DENY_OWNERS = {"arxiv", "brucemiller"}
_DENY_REPOS = {"html_feedback", "latexml"}


def _clean_repo(owner: str, repo: str) -> str | None:
    repo = repo.rstrip(".").removesuffix(".git")
    if owner.lower() in _DENY_OWNERS or repo.lower() in _DENY_REPOS:
        return None
    if extract_repo(f"https://github.com/{owner}/{repo}") is None:
        return None
    return f"https://github.com/{owner}/{repo}"


def _from_text(text: str) -> str | None:
    for owner, repo in _GH_RE.findall(text or ""):
        url = _clean_repo(owner, repo)
        if url:
            return url
    return None


_CODE_CUE = re.compile(
    r"(our code|code is (?:available|released)|we release|code and (?:data|models)|"
    r"implementation is available|source code|project page|\brepo\b|reproduce)",
    re.I,
)
# very popular repos that papers cite as tooling/baselines, not as their own code
_MEGA_REPOS = {
    "huggingface/transformers", "pytorch/pytorch", "tatsu-lab/stanford_alpaca",
    "wan-video/wan2.2", "openai/clip", "facebookresearch/detectron2",
    "vllm-project/vllm", "meta-llama/llama", "lm-sys/fastchat", "hiyouga/llama-factory",
}


def _title_tokens(title: str) -> set[str]:
    return {w.lower() for w in re.findall(r"[A-Za-z]{4,}", title)}


async def _github_from_html(fetcher: AsyncFetcher, arxiv_id: str, title: str) -> str | None:
    html = ""
    for ver in ("", "v2", "v1"):
        try:
            html = await fetcher.fetch_text(f"https://arxiv.org/html/{arxiv_id}{ver}")
            break
        except FetchError:
            continue
    if not html:
        return None

    ttoks = _title_tokens(title)
    scored: dict[str, int] = {}
    for m in _GH_RE.finditer(html):
        owner, repo = m.group(1), m.group(2)
        url = _clean_repo(owner, repo)
        if url is None:
            continue
        slug = f"{owner}/{repo}".lower().removesuffix(".git")
        if slug in _MEGA_REPOS:
            continue
        window = html[max(0, m.start() - 200): m.end() + 200]
        score = 1
        if _CODE_CUE.search(window):
            score += 3
        if _title_tokens(f"{owner} {repo}") & ttoks:
            score += 3
        scored[url] = max(scored.get(url, 0), score) + (1 if url in scored else 0)

    if not scored:
        return None
    best = max(scored, key=scored.get)
    # require at least one real signal (cue or title match), not just a bare mention
    return best if scored[best] >= 2 else None


async def _discover_github(fetcher: AsyncFetcher, paper: dict) -> str | None:
    hit = _from_text(paper.get("comment", "")) or _from_text(paper.get("summary", ""))
    if hit:
        return hit
    return await _github_from_html(fetcher, paper["arxiv_id"], paper.get("title", ""))


async def crawl_research_papers(
    *, max_records: int = 1000, require_github: bool = True, scan_cap: int = 8000
) -> AsyncIterator[ResearchPaperRecord]:
    """Yield `max_records` papers.

    With `require_github=True` (the brief wants "papers WITH GitHub metrics"),
    only papers that resolved to a repo with a live star count are counted
    toward the target; arXiv is scanned newest-first up to `scan_cap` papers to
    fill the quota. `scan_cap` bounds the work if the hit-rate is low.
    """
    from src.config import get_sources_config

    src_cfg = next(
        s for s in get_sources_config()["research_papers"] if s["name"] == "arXiv"
    )

    emitted = 0
    scanned = 0
    async with AsyncFetcher() as fetcher:
        stars = GitHubStars(fetcher)
        buffer: list[dict] = []

        async for paper in iter_arxiv(
            fetcher,
            base_url=src_cfg["base_url"],
            categories=src_cfg["categories"],
            page_size=src_cfg.get("page_size", 200),
            max_records=scan_cap if require_github else max_records,
        ):
            buffer.append(paper)
            scanned += 1
            if len(buffer) >= 50:
                async for rec in _flush(fetcher, stars, buffer):
                    if require_github and rec.content.github_stars is None:
                        continue
                    yield rec
                    emitted += 1
                    if emitted >= max_records:
                        return
                buffer.clear()

        async for rec in _flush(fetcher, stars, buffer):
            if require_github and rec.content.github_stars is None:
                continue
            yield rec
            emitted += 1
            if emitted >= max_records:
                return

    log.info("papers_scan_done", emitted=emitted, scanned=scanned, require_github=require_github)


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
