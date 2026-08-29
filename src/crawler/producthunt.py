"""Product Hunt (Products vertical, Phase I).

Product Hunt's v2 GraphQL API is the primary products source: every post is a
launched product with a stable producthunt.com URL (traceable), a rich
description, and topic tags. We page through the "artificial-intelligence"
topic (plus a few adjacent AI topics) newest-first.

The API rate-limits on query *complexity* (~6250 points / 15 min), so we keep
the selection set lean, page in blocks of 20, and back off on 429 /
`rate_limited`.

`startupName` and `pricingModel` are not structured fields on a post, so the
pipeline sends each product's description through the LLM extraction engine to
derive them (then the resolver canonicalizes the company). That is derivation
from the source text, not fabrication.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from src.config import get_settings
from src.crawler.http import AsyncFetcher, FetchError
from src.logging_setup import get_logger

log = get_logger("crawl")

_ENDPOINT = "https://api.producthunt.com/v2/api/graphql"

# AI + adjacent topics on Product Hunt (slugs)
AI_TOPICS = [
    "artificial-intelligence",
    "developer-tools",
    "saas",
    "productivity",
    "design-tools",
]

_QUERY = """
query($topic:String!, $after:String) {
  posts(order: NEWEST, first: 20, topic: $topic, after: $after) {
    pageInfo { endCursor hasNextPage }
    edges {
      node {
        id name tagline description slug url website
        votesCount createdAt
        makers { name username }
        topics { edges { node { name } } }
      }
    }
  }
}
"""


async def _page(fetcher: AsyncFetcher, token: str, topic: str, after: str | None) -> dict:
    for attempt in range(5):
        try:
            r = await fetcher.post_json(
                _ENDPOINT,
                json_body={"query": _QUERY, "variables": {"topic": topic, "after": after}},
                headers={"Authorization": f"Bearer {token}"},
            )
        except FetchError as exc:
            if exc.status == 429:
                wait = 20 * (attempt + 1)
                log.info("ph_rate_limited", topic=topic, sleep_s=wait)
                await asyncio.sleep(wait)
                continue
            raise
        if "errors" in r:
            msg = str(r["errors"])
            if "rate" in msg.lower():
                await asyncio.sleep(30)
                continue
            raise RuntimeError(f"Product Hunt GraphQL error: {msg[:200]}")
        return r["data"]["posts"]
    raise RuntimeError(f"Product Hunt: exhausted retries for topic {topic}")


async def iter_producthunt(
    fetcher: AsyncFetcher, *, max_records: int = 1000
) -> AsyncIterator[dict]:
    token = get_settings().producthunt_token
    if not token:
        log.warning("ph_no_token", msg="PRODUCTHUNT_TOKEN not set; products vertical skipped")
        return

    seen: set[str] = set()
    emitted = 0

    for topic in AI_TOPICS:
        after: str | None = None
        while emitted < max_records:
            data = await _page(fetcher, token, topic, after)
            for edge in data["edges"]:
                n = edge["node"]
                if n["id"] in seen:
                    continue
                seen.add(n["id"])
                topics = [t["node"]["name"] for t in n["topics"]["edges"]]
                if topic == "artificial-intelligence" or "Artificial Intelligence" in topics:
                    yield {
                        "id": n["id"],
                        "name": n["name"].strip(),
                        "tagline": n["tagline"],
                        "description": n["description"] or "",
                        "url": n["url"].split("?")[0],
                        "website": n.get("website", ""),
                        "topics": topics,
                        "makers": [m["name"] for m in n.get("makers", [])],
                        "votes": n.get("votesCount"),
                        "created_at": n.get("createdAt"),
                    }
                    emitted += 1
                    if emitted >= max_records:
                        return
            if not data["pageInfo"]["hasNextPage"]:
                break
            after = data["pageInfo"]["endCursor"]
            await asyncio.sleep(1.5)  # stay well under the complexity budget
