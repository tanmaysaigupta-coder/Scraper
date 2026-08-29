# GraphOne / FrontierAtlas — Intelligence Graph Ingestion Pipeline

A scalable, fault-tolerant ingestion pipeline that acquires the AI / venture
ecosystem (startups, products, research papers, jobs, news), structures it into
a canonical JSON schema with a multi-tier LLM engine, and canonicalizes
entities — designed to scale from a few thousand records to 500k+ by adding
infrastructure, not changing code.

> Trial-scale targets: 1,000 startups, 1,000 products, 1,000 research papers
> (each with a live GitHub star count), plus every news item and job published
> in the last 24 h.

---

## Architecture at a glance

```
   config/*.yaml ─▶  Acquisition (Phase I/II)
                     ├ YC Algolia API .......... startups   (direct map, no LLM)
                     ├ Product Hunt GraphQL .... products
                     ├ arXiv + GitHub API ...... research papers (+ live stars)
                     ├ 5 RSS news feeds ........ news
                     └ 5 job APIs/feeds ........ jobs
                     async aiohttp · per-host rate-limit · 429/5xx backoff+jitter
                            │
                     Anti-bot escalation (Phase V)
                     direct → ScraperAPI (residential) → ScraperAPI render → Playwright
                            │
                     Freshness + Dedupe
                     relative-date parsing · 24 h window · SeenStore (SQLite→Redis)
                     · per-source high-water mark
                            │
                     LLM extraction engine (Phase III)
                     Groq → Gemini Flash → DeepSeek/OpenRouter
                     · 413-aware chunking w/ runtime shrink · 429 backoff+jitter
                     · on-disk result cache (resumable under quota)
                            │
                     Entity resolution (Phase IV)
                     normalize → exact → alias → fuzzy → mint · every decision logged
                            │
                     Sinks
                     JSONL (source of truth) + .xlsx workbook (6 tabs)
                     [+ Google Sheets API when a service-account key is available]
```

Full design rationale, the 500k-scale plan, and the 413 / 429 / freshness /
storage answers: **[`docs/architecture.md`](docs/architecture.md)**
(rendered: [`architecture.pdf`](architecture.pdf)).

---

## Layout

| Path | What |
|---|---|
| `src/schemas.py` | Canonical pydantic models for every record type (the LLM↔sink contract); lenient `*Draft` models are the LLM extraction targets |
| `src/config.py` | Env/secret settings + YAML config loading |
| `src/crawler/http.py` | The one outbound-request path: semaphore, per-host RPS, retry/backoff, `post_json` |
| `src/crawler/yc.py` | Startups — YC's Algolia index (public key scraped live), direct deterministic map |
| `src/crawler/producthunt.py` | Products — Product Hunt v2 GraphQL, AI topics |
| `src/crawler/arxiv.py`, `papers.py` | Research papers — arXiv API → GitHub repo discovery (abstract / comment / HTML) → live stars; `require_github` keeps only rows with metrics |
| `src/crawler/feeds.py` | News + jobs — RSS/JSON APIs, 24 h freshness, dedupe, full-text |
| `src/crawler/directory.py` | Generic config-driven directory crawler (TheresAnAIForThat) |
| `src/crawler/scraperapi.py` | Anti-bot escalation: residential IPs + server-side JS render |
| `src/crawler/browser.py` | Playwright last-resort render; degrades cleanly where Chromium can't launch |
| `src/crawler/dates.py` | Date normalization (ISO, RFC-822, "2 h ago", epoch ints…) + freshness heuristic |
| `src/crawler/seen_store.py` | Dedupe store: `SeenStore` interface, SQLite + Redis backends |
| `src/llm/` | Multi-tier engine: `providers.py`, `chunking.py`, `orchestrator.py`, `cache.py` |
| `src/resolver/` | Deterministic entity resolution + full mapping log |
| `src/sinks/` | `jsonl_sink.py` (SoT) · `xlsx_sink.py` (6-tab workbook) · `sheets_sink.py` (Sheets API) |
| `src/pipeline.py` | Top-level `crawl → extract → resolve → sink` orchestration |
| `config/*.yaml` | Every source URL + every tuning knob. Add a source = edit YAML. |
| `tests/` | Unit tests for the tricky logic (chain fallback, dates, resolver, chunking, dedupe) |

---

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows;  source .venv/bin/activate on *nix
pip install -r requirements.txt
python -m playwright install chromium   # optional — only the last-resort render path

cp .env.example .env               # then fill in the keys below
```

### Credentials (`.env`)

| Var | For | Notes |
|---|---|---|
| `GROQ_API_KEY` | LLM tier 1 | console.groq.com, free |
| `GEMINI_API_KEY` | LLM tier 2 (900k context) | aistudio.google.com, free |
| `OPENROUTER_API_KEY` | LLM tier 3 (`deepseek/deepseek-chat`) | openrouter.ai |
| `GITHUB_TOKEN` | research-paper star counts | classic token, no scopes; 60→5000 req/hr |
| `PRODUCTHUNT_TOKEN` | products vertical | producthunt.com/v2/oauth/applications → Developer Token |
| `SCRAPER_API_KEY` | anti-bot escalation (Phase V) | scraperapi.com, 5000 free credits |
| `GOOGLE_SERVICE_ACCOUNT_JSON` + `GSHEET_ID` | *optional* direct Sheets push | only if your Google org allows service-account keys |

Missing a key degrades gracefully: crawlers + resolver still run and write JSONL
+ the `.xlsx`; the LLM tier logs `tier_unavailable` and is skipped.

---

## Run

```bash
python -m src.pipeline all         # every vertical, concurrently
python -m src.pipeline papers      # one vertical: papers | startups | products | news | jobs
```

Output:
- `data/output/*.jsonl` — append-only source of truth, one file per record type
- `data/output/graphone_intelligence.xlsx` — the 6-tab deliverable
  (**Startups · Products · Research Papers · Jobs · News · Entity Mapping Log**)
- Google Sheet tabs too, if a service-account key is configured

**To publish the deliverable:** upload `graphone_intelligence.xlsx` to Google
Drive → *Open with Google Sheets* → *Share → Anyone with the link → Viewer* →
that link is the submission.

```bash
pytest -q                          # unit tests, no network/keys required
ruff check src tests
```

---

## Data integrity

Per the brief, **hallucinated data is disqualifying.** Every record carries a
required, validated `source.url`. The LLM system prompt forbids inventing values
(null for anything not present); an LLM-proposed company name is kept only if it
literally appears in the source text or resolves to a known canonical entity,
else the pipeline falls back to the item's own name — never a guess. A record
that fails schema validation on every tier is dropped and logged, never
fabricated.
