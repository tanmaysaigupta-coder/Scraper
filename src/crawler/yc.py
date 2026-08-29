"""Y Combinator company directory (Startups vertical, Phase I).

ycombinator.com/companies is a JS app backed by Algolia. Rather than drive a
headless browser through infinite scroll, we call the same Algolia index the
page uses. The public search key is embedded in the page as
`window.AlgoliaOpts`; we scrape it live each run (with a known-good fallback) so
the connector survives key rotation.

Each Algolia hit already carries everything the Startup schema needs
(`name` -> entityName, `team_size` -> employeeCount), so this vertical needs no
LLM call — a direct, deterministic mapping with zero hallucination surface.

Algolia caps pagination at 1,000 hits per query, so to go beyond that we shard
the query by batch.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator

from src.crawler.http import AsyncFetcher, FetchError
from src.logging_setup import get_logger

log = get_logger("crawl")

_OPTS_RE = re.compile(r'window\.AlgoliaOpts\s*=\s*(\{.*?\})\s*;', re.S)
_FALLBACK_APP = "45BWZJ1SGC"
_FALLBACK_KEY = (
    "NzllNTY5MzJiZGM2OTY2ZTQwMDEzOTNhYWZiZGRjODlhYzVkNjBmOGRjNzJiMWM4ZTU0"
    "ZDlhYTZjOTJiMjlhMWFuYWx5dGljc1RhZ3M9eWNkYyZyZXN0cmljdEluZGljZXM9WUND"
    "b21wYW55X3Byb2R1Y3Rpb24lMkNZQ0NvbXBhbnlfQnlfTGF1bmNoX0RhdGVfcHJvZHVj"
    "dGlvbiZ0YWdGaWx0ZXJzPSU1QiUyMnljZGNfcHVibGljJTIyJTVE"
)
_INDEX = "YCCompany_production"

# AI-relevant YC tags — union of these is the AI/venture slice we want.
AI_TAGS = [
    "AI", "Generative AI", "Machine Learning", "AI Assistant", "Computer Vision",
    "AIOps", "Conversational AI", "Deep Learning", "NLP", "ML",
    "AI-Enhanced Learning", "AI-powered Drug Discovery",
]


async def _algolia_creds(fetcher: AsyncFetcher) -> tuple[str, str]:
    try:
        html = await fetcher.fetch_text("https://www.ycombinator.com/companies")
        m = _OPTS_RE.search(html)
        if m:
            import json

            opts = json.loads(m.group(1))
            return opts["app"], opts["key"]
    except (FetchError, ValueError, KeyError):
        pass
    log.info("yc_algolia_creds_fallback")
    return _FALLBACK_APP, _FALLBACK_KEY


async def _query_page(fetcher, app, key, *, facet_filters, page, hits_per_page=1000) -> dict:
    url = f"https://{app.lower()}-dsn.algolia.net/1/indexes/*/queries"
    payload = {
        "requests": [{
            "indexName": _INDEX,
            "query": "",
            "page": page,
            "hitsPerPage": hits_per_page,
            "facetFilters": facet_filters,
            "attributesToRetrieve": [
                "name", "former_names", "one_liner", "long_description", "team_size",
                "batch", "industries", "subindustry", "tags", "website", "slug",
                "all_locations", "regions", "status", "stage", "launched_at",
            ],
        }]
    }
    headers = {"x-algolia-application-id": app, "x-algolia-api-key": key}
    r = await fetcher.post_json(url, json_body=payload, headers=headers)
    return r["results"][0]


async def iter_yc_companies(
    fetcher: AsyncFetcher, *, max_records: int = 1000
) -> AsyncIterator[dict]:
    app, key = await _algolia_creds(fetcher)
    # one inner list = OR across those AI tags
    facet_filters = [[f"tags:{t}" for t in AI_TAGS]]
    seen_slugs: set[str] = set()
    emitted = 0

    first = await _query_page(fetcher, app, key, facet_filters=facet_filters, page=0)
    n_pages = min(first["nbPages"], (max_records // first["hitsPerPage"]) + 1)
    log.info("yc_start", nb_hits=first["nbHits"], nb_pages=first["nbPages"])

    for page in range(n_pages):
        res = first if page == 0 else await _query_page(
            fetcher, app, key, facet_filters=facet_filters, page=page
        )
        for hit in res["hits"]:
            slug = hit.get("slug") or hit.get("name", "")
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            yield {
                "name": hit.get("name", "").strip(),
                "former_names": hit.get("former_names") or [],
                "one_liner": hit.get("one_liner", ""),
                "description": hit.get("long_description", ""),
                "team_size": hit.get("team_size"),
                "batch": hit.get("batch"),
                "industries": hit.get("industries") or [],
                "tags": hit.get("tags") or [],
                "website": hit.get("website", ""),
                "locations": hit.get("all_locations") or hit.get("regions") or [],
                "status": hit.get("status"),
                "source_url": f"https://www.ycombinator.com/companies/{slug}",
            }
            emitted += 1
            if emitted >= max_records:
                return
