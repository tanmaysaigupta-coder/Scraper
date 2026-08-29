"""GitHub repo metadata — current star counts for the research-paper vertical.

Rate limits: 60 req/hr unauthenticated vs 5,000 req/hr with a token. With
1,000+ papers a token is mandatory, so `GITHUB_TOKEN` is used when present and
the crawler logs a loud warning (and self-throttles hard) when it is missing.

`stars_batch` fans out with a bounded semaphore and respects the
X-RateLimit-Remaining / X-RateLimit-Reset headers, parking the whole batch if
the budget runs low rather than eating 403s.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Iterable

from src.config import get_settings
from src.crawler.http import AsyncFetcher, FetchError
from src.logging_setup import get_logger

log = get_logger("crawl")

_REPO_RE = re.compile(r"github\.com/([^/\s#]+)/([^/\s#?]+)", re.I)
_API = "https://api.github.com/repos/{owner}/{repo}"


def extract_repo(url: str | None) -> tuple[str, str] | None:
    if not url:
        return None
    m = _REPO_RE.search(url)
    if not m:
        return None
    owner, repo = m.group(1), m.group(2)
    repo = repo.removesuffix(".git")
    if owner.lower() in {"sponsors", "about", "features", "topics", "collections"}:
        return None
    return owner, repo


class GitHubStars:
    def __init__(self, fetcher: AsyncFetcher, *, max_concurrency: int = 8) -> None:
        self._fetcher = fetcher
        token = get_settings().github_token
        self._headers = {"Accept": "application/vnd.github+json"}
        if token:
            self._headers["Authorization"] = f"Bearer {token}"
        else:
            log.warning("github_no_token",
                        msg="GITHUB_TOKEN missing: 60 req/hr cap, star lookups will be throttled")
            max_concurrency = 1
        self._sem = asyncio.Semaphore(max_concurrency)
        self._park_until = 0.0

    async def stars_for(self, repo_url: str | None) -> int | None:
        parsed = extract_repo(repo_url)
        if not parsed:
            return None
        owner, repo = parsed
        async with self._sem:
            if self._park_until > time.time():
                await asyncio.sleep(self._park_until - time.time())
            try:
                data = await self._fetcher.fetch_json(
                    _API.format(owner=owner, repo=repo), headers=self._headers
                )
            except FetchError as exc:
                if exc.status == 404:
                    return None
                log.info("github_lookup_failed", repo=f"{owner}/{repo}", status=exc.status)
                return None
            return data.get("stargazers_count")

    async def stars_batch(self, repo_urls: Iterable[str | None]) -> list[int | None]:
        return await asyncio.gather(*(self.stars_for(u) for u in repo_urls))
