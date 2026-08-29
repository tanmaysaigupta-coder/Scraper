"""Top-level orchestration: crawl -> extract -> resolve -> sink.

Each vertical is a coroutine so verticals run concurrently. Within a vertical,
records stream through a bounded `asyncio.gather` worker pool for the LLM step
so one slow extraction never stalls the batch.

Run everything:      python -m src.pipeline all
One vertical:        python -m src.pipeline papers
"""

from __future__ import annotations

import asyncio
import re
import sys

from src.config import get_pipeline_config, get_sources_config
from src.crawler.directory import crawl_directory
from src.crawler.feeds import crawl_feed_vertical
from src.crawler.papers import crawl_research_papers
from src.crawler.seen_store import build_seen_store
from src.llm.orchestrator import ExtractionFailed, LLMOrchestrator
from src.logging_setup import configure_logging, get_logger
from src.resolver.entity_resolver import EntityResolver
from src.schemas import (
    JobContent,
    JobRecord,
    NewsContent,
    NewsRecord,
    Source,
)
from src.sinks.jsonl_sink import JsonlSink
from src.sinks.sheets_sink import SheetsSink
from src.sinks.xlsx_sink import XlsxSink

log = get_logger("pipeline")


class _TabularSinks:
    """Fan a vertical's tab out to every configured tabular sink (xlsx always,
    Google Sheets when a service-account key is available)."""

    def __init__(self, *sinks) -> None:
        self._sinks = [s for s in sinks if s is not None]

    def write_records(self, *a, **kw) -> None:
        for s in self._sinks:
            try:
                s.write_records(*a, **kw)
            except Exception as exc:  # noqa: BLE001 - one sink failing must not lose the others
                log.warning("sink_write_failed", sink=type(s).__name__, err=str(exc)[:160])

    def write_rows(self, *a, **kw) -> None:
        for s in self._sinks:
            try:
                s.write_rows(*a, **kw)
            except Exception as exc:  # noqa: BLE001
                log.warning("sink_write_failed", sink=type(s).__name__, err=str(exc)[:160])

PAPER_COLUMNS = [
    "schemaVersion", "recordType", "source.name", "source.url",
    "content.title", "content.authors", "content.paper_url",
    "content.github_url", "content.github_stars", "content.published_date",
    "collectedAt",
]
JOB_COLUMNS = [
    "schemaVersion", "recordType", "source.name", "source.url",
    "content.company", "content.title", "content.date", "content.is_remote",
    "content.role_family", "collectedAt",
]
NEWS_COLUMNS = [
    "schemaVersion", "recordType", "source.name", "source.url",
    "content.title", "content.author", "content.published_date",
    "content.full_text", "collectedAt",
]
STARTUP_COLUMNS = [
    "schemaVersion", "recordType", "source.name", "source.url",
    "content.entityName", "content.data.employeeCount", "collectedAt",
]
PRODUCT_COLUMNS = [
    "schemaVersion", "recordType", "source.name", "source.url",
    "content.startupName", "content.pricingModel", "collectedAt",
]
MAPPING_COLUMNS = ["raw", "canonical", "method", "score", "is_new_entity"]


async def run_papers(jsonl: JsonlSink, sheets: SheetsSink) -> None:
    target = get_pipeline_config()["targets"]["research_papers"]
    records = []
    async for rec in crawl_research_papers(max_records=target):
        records.append(rec)
    jsonl.write("RESEARCH_PAPER", records)
    sheets.write_records("research_papers", records, PAPER_COLUMNS)
    log.info("papers_done", count=len(records))


async def run_startups(jsonl: JsonlSink, sheets: SheetsSink, resolver: EntityResolver) -> None:
    """YC directory via Algolia -> direct deterministic map (no LLM needed)."""
    from src.crawler.http import AsyncFetcher
    from src.crawler.yc import iter_yc_companies
    from src.schemas import StartupContent, StartupData, StartupRecord

    target = get_pipeline_config()["targets"]["startups"]
    records: list[StartupRecord] = []
    async with AsyncFetcher() as fetcher:
        async for c in iter_yc_companies(fetcher, max_records=target):
            for fn in c.get("former_names") or []:
                resolver.register_alias(fn, c["name"])
            canonical = resolver.resolve_str(c["name"])
            try:
                records.append(StartupRecord(
                    source=Source(name="Y Combinator", url=c["source_url"]),
                    content=StartupContent(
                        entityName=canonical,
                        data=StartupData(employeeCount=_as_int(c.get("team_size"))),
                    ),
                ))
            except Exception as exc:  # noqa: BLE001
                log.warning("startup_record_invalid", name=c.get("name"), err=str(exc)[:140])
    jsonl.write("STARTUP", records)
    sheets.write_records("startups", records, STARTUP_COLUMNS)
    log.info("startups_done", count=len(records))


_COMPANY_FROM_TEXT = re.compile(
    r"\b(?:by|from|made by|built by|powered by|a product of|part of)\s+"
    r"([A-Z][A-Za-z0-9][A-Za-z0-9.&' ]{1,28}?)"
    r"(?=[.,;:)\n]| — | - |\s+(?:is|was|helps|makes|offers|provides|lets|the)\b|$)"
)


def _company_from_text(*texts: str) -> str | None:
    for t in texts:
        for m in _COMPANY_FROM_TEXT.finditer(t or ""):
            cand = m.group(1).strip().rstrip(".")
            low = cand.lower()
            if 2 <= len(cand) <= 30 and low not in {
                "the", "our team", "us", "me", "a team", "two", "a group",
                "a solo", "an indie", "the makers", "the team",
            }:
                return cand
    return None


async def run_products(jsonl: JsonlSink, sheets: SheetsSink, resolver: EntityResolver) -> None:
    """Product Hunt GraphQL primary + TheresAnAIForThat (ScraperAPI) top-up.

    Two-pass extraction:
      1. **Deterministic** — `startupName` = the product's brand (a
         "by <Company>" phrase upgrades it to the parent); `pricingModel` from a
         keyword scan of the product's own website. Quota-free, reproducible.
      2. **Batched LLM enrichment** — ~15 products per call through the
         multi-tier chain (`extract_many`) to recover a parent company / pricing
         the keyword pass missed. One call per ~15 items keeps a 1,000-row run
         inside free-tier quota. The LLM answer is used only if the company name
         actually appears in the source; otherwise pass 1 stands.

    Nothing is invented — every field traces to the source text.
    """
    from src.crawler.extract_text import extract_main_text
    from src.crawler.http import AsyncFetcher
    from src.crawler.pricing import classify_pricing
    from src.crawler.producthunt import iter_producthunt
    from src.schemas import PricingModel, ProductContent, ProductDraft, ProductRecord

    target = get_pipeline_config()["targets"]["products"]
    llm = LLMOrchestrator.from_config()
    fetch_sem = asyncio.Semaphore(16)
    batch_sem = asyncio.Semaphore(4)
    BATCH = 15

    async def _site_text(fetcher: AsyncFetcher, url: str) -> str:
        if not url:
            return ""
        try:
            html = await fetcher.fetch_text(url)
            return extract_main_text(html)[:6000]
        except Exception:  # noqa: BLE001 - a single bad site must not stop the run
            return ""

    async def _prep(fetcher: AsyncFetcher, item: dict) -> dict:
        try:
            async with fetch_sem:
                site = await _site_text(fetcher, item.get("website", ""))
        except Exception:  # noqa: BLE001
            site = ""
        blurb = f"{item['name']} — {item.get('tagline', '')}\n{item.get('description', '')}"
        parent = _company_from_text(item.get("tagline", ""), item.get("description", ""))
        return {
            "item": item,
            "blurb": blurb,
            "site": site,
            "det_name": parent if (parent and parent.lower() in blurb.lower()) else item["name"],
            "det_pricing": classify_pricing(blurb, site),
        }

    def _emit(prep: dict, name: str, pricing) -> ProductRecord | None:
        try:
            return ProductRecord(
                source=Source(name="Product Hunt", url=prep["item"]["url"]),
                content=ProductContent(
                    startupName=resolver.resolve_str(name),
                    pricingModel=pricing if isinstance(pricing, PricingModel) else None,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("product_record_invalid", url=prep["item"]["url"], err=str(exc)[:140])
            return None

    async def _enrich_batch(preps: list[dict]) -> list[ProductRecord]:
        blocks = [
            f"{p['blurb']}\n[website] {p['site'][:1200]}"
            for p in preps
        ]
        async with batch_sem:
            drafts = await llm.extract_many(
                blocks=blocks, item_target=ProductDraft,
                instructions=(
                    "For each item: startupName = the company/brand that publishes the "
                    "product (a name that literally appears in the text; else the product's "
                    "own name — never invented). pricingModel = FREE, FREEMIUM, PAID or "
                    "ENTERPRISE only when the text makes it clear, else null."
                ),
            )
        out: list[ProductRecord] = []
        for p, d in zip(preps, drafts, strict=False):
            name, pricing = p["det_name"], p["det_pricing"]
            if isinstance(d, ProductDraft):
                cand = (d.startupName or "").strip()
                src = f"{p['blurb']}\n{p['site']}".lower()
                if cand and (cand.lower() in src or resolver.looks_known(cand)):
                    name = cand
                pricing = d.pricingModel or pricing
            rec = _emit(p, name, pricing)
            if rec:
                out.append(rec)
        return out

    records: list[ProductRecord] = []
    async with AsyncFetcher() as fetcher:
        pending: list[dict] = []
        items: list[dict] = []
        async for item in iter_producthunt(fetcher, max_records=target):
            items.append(item)
        log.info("products_fetched", count=len(items))

        preps = await asyncio.gather(*(_prep(fetcher, it) for it in items))
        for i in range(0, len(preps), BATCH):
            pending.append(list(preps[i : i + BATCH]))

        results = await asyncio.gather(*(_enrich_batch(b) for b in pending))
        for r in results:
            records.extend(r)
        log.info("products_progress", done=len(records), batches=len(pending))

    # secondary top-up from TheresAnAIForThat if PH under target
    if len(records) < target:
        remaining = target - len(records)
        log.info("products_topup_taaft", remaining=remaining)
        async for rec in crawl_directory("products", max_records=remaining, resolver=resolver):
            records.append(rec)

    jsonl.write("PRODUCT", records)
    sheets.write_records("products", records, PRODUCT_COLUMNS)
    log.info("products_done", count=len(records))


def _as_int(v) -> int | None:
    try:
        return int(v) if v not in (None, "", 0) else None
    except (TypeError, ValueError):
        return None


async def run_feed_vertical(
    vertical: str, jsonl: JsonlSink, sheets: SheetsSink, resolver: EntityResolver
) -> None:
    seen = build_seen_store()
    try:
        raw_items = await crawl_feed_vertical(vertical, seen=seen)
    finally:
        seen.close()

    llm = LLMOrchestrator.from_config()
    records: list = []

    sem = asyncio.Semaphore(8)

    async def _shape(item: dict):
        async with sem:
            try:
                if vertical == "news":
                    rec = await _shape_news(llm, item)
                else:
                    rec = await _shape_job(llm, item, resolver)
                if rec:
                    records.append(rec)
            except ExtractionFailed as exc:
                log.warning("extract_failed", vertical=vertical, url=item.get("url"),
                            tried=exc.tried)

    await asyncio.gather(*(_shape(it) for it in raw_items))

    rt = "NEWS" if vertical == "news" else "JOB"
    jsonl.write(rt, records)
    if vertical == "news":
        sheets.write_records("news", records, NEWS_COLUMNS)
    else:
        sheets.write_records("jobs", records, JOB_COLUMNS)
    log.info("feed_vertical_done", vertical=vertical, count=len(records))


async def _shape_news(llm: LLMOrchestrator, item: dict) -> NewsRecord | None:
    if not item.get("full_text"):
        return None
    content = NewsContent(
        title=item["title"],
        published_date=item.get("published_date"),
        author=item.get("author") or None,
        full_text=item["full_text"],
        url=item["url"],
    )
    return NewsRecord(source=Source(name=item["source_name"], url=item["url"]), content=content)


_ROLE_FAMILY_RULES = [
    ("Engineering", ("engineer", "developer", "programmer", "sre", "devops", "backend",
                     "frontend", "full stack", "full-stack", "software", "platform", "infra")),
    ("Data / ML", ("machine learning", " ml ", "data scientist", "data engineer", "mlops",
                   "ai researcher", "nlp", "computer vision", "research scientist")),
    ("Product", ("product manager", "product owner", "program manager", " pm ", "product lead")),
    ("Design", ("designer", "ux", "ui ", "product design", "brand")),
    ("Sales / GTM", ("sales", "account executive", "account manager", "business development",
                     "gtm", "revenue", "partnerships")),
    ("Marketing", ("marketing", "growth", "seo", "content", "demand gen", "community")),
    ("Data / Analytics", ("analyst", "analytics", "bi ")),
    ("Operations", ("operations", " ops", "recruiter", "people ", "hr ", "finance",
                    "accounting", "legal", "counsel", "support", "customer success")),
    ("Research", ("research",)),
]


def _role_family(title: str) -> str | None:
    t = f" {title.lower()} "
    for family, kws in _ROLE_FAMILY_RULES:
        if any(k in t for k in kws):
            return family
    return None


async def _shape_job(llm: LLMOrchestrator, item: dict, resolver: EntityResolver) -> JobRecord | None:
    # Jobs already come with company + date + remote from the feed APIs; role_family
    # is a cheap deterministic classification. The LLM is only a fallback when the
    # company name is missing -- keeps ~1 LLM call per job off the rate-limited tiers.
    company_raw = item.get("company") or ""
    role_family = _role_family(item.get("title", ""))

    if not company_raw:
        text = f"{item.get('title','')}\n\n{item.get('raw_summary','')}".strip()
        if text:
            try:
                res = await llm.extract(
                    raw_text=text, target=JobContent,
                    instructions="Extract only the hiring company name into `company`.",
                    context={"source_url": item["url"]},
                )
                company_raw = (res.model.company or "").strip()  # type: ignore[attr-defined]
            except ExtractionFailed:
                pass

    canonical = resolver.resolve_str(company_raw) if company_raw else ""
    content = JobContent(
        company=canonical or company_raw or "Unknown",
        title=item.get("title") or None,
        date=item.get("published_date"),
        is_remote=item.get("is_remote"),
        role_family=role_family,
        url=item["url"],
    )
    return JobRecord(source=Source(name=item["source_name"], url=item["url"]), content=content)


async def main(which: str) -> None:
    configure_logging()
    jsonl = JsonlSink()
    xlsx = XlsxSink()
    gsheets = SheetsSink()
    sheets = _TabularSinks(xlsx, gsheets if gsheets.enabled else None)
    resolver = EntityResolver.from_seed_file(
        get_sources_config()["entity_seed_file"]
    )

    tasks = []
    if which in ("all", "papers"):
        tasks.append(run_papers(jsonl, sheets))
    if which in ("all", "startups"):
        tasks.append(run_startups(jsonl, sheets, resolver))
    if which in ("all", "products"):
        tasks.append(run_products(jsonl, sheets, resolver))
    if which in ("all", "news"):
        tasks.append(run_feed_vertical("news", jsonl, sheets, resolver))
    if which in ("all", "jobs"):
        tasks.append(run_feed_vertical("jobs", jsonl, sheets, resolver))

    if not tasks:
        print(f"unknown target '{which}'. use: all | papers | news | jobs | startups | products")
        return

    await asyncio.gather(*tasks)

    # Entity mapping log (deliverable tab #6)
    mapping_rows = [
        {"raw": r.raw, "canonical": r.canonical, "method": r.method,
         "score": r.score, "is_new_entity": r.is_new_entity}
        for r in resolver.mapping_log
    ]
    if mapping_rows:
        jsonl.write_dicts("entity_mapping_log", mapping_rows)
        sheets.write_rows("mapping_log", mapping_rows, MAPPING_COLUMNS)

    workbook = xlsx.save()
    log.info("pipeline_complete", counts=jsonl.counts, workbook=str(workbook))
    print(f"\nWorkbook written: {workbook}\n"
          f"Upload it to Google Drive and 'Open with Google Sheets', then Share -> "
          f"Anyone with the link -> Viewer to get the public link for submission.")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "all"))
