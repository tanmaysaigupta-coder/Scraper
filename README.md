# GraphOne / FrontierAtlas — Intelligence Graph Ingestion Pipeline

A scalable, fault-tolerant ingestion pipeline that scrapes the AI/venture
ecosystem (startups, products, research papers, jobs, news), structures raw HTML
into a canonical JSON schema with a multi-tier LLM engine, and canonicalizes
entities — designed to scale from a few thousand records to 500k+ by adding
infrastructure, not changing code.

> Trial-scale run targets: 1,000 startups, 1,000 products, 1,000 research papers
> (with live GitHub stars), plus all news/jobs published in the last 24h.

---

## Architecture at a glance

```
                 ┌──────────────┐
   sources.yaml ─▶│  Acquisition │  async aiohttp + per-host rate limit + 429/5xx
                 │   (Phase I/II)│  backoff+jitter ; Playwright escalation (Phase V)
                 └──────┬───────┘
                        │ raw HTML / feed items
                 ┌──────▼───────┐
                 │  Freshness   │  relative-date parsing, 24h window,
                 │  + Dedupe    │  SeenStore (SQLite→Redis), FreshnessState hi-water
                 └──────┬───────┘
                        │
                 ┌──────▼───────┐
                 │ LLM Extraction│  fallback chain Gemini→Groq→DeepSeek
                 │  (Phase III)  │  chunking guard (413), backoff+jitter (429)
                 └──────┬───────┘
                        │ schema-valid pydantic records
                 ┌──────▼───────┐
                 │ Entity Resolve│  normalize → exact → alias → fuzzy → mint
                 │  (Phase IV)   │  every decision logged
                 └──────┬───────┘
                        │
             ┌──────────▼──────────┐
             │  Sinks: JSONL (SoT) │
             │  + Google Sheets    │  6 tabs = the deliverable
             └─────────────────────┘
```

Full design rationale and the 500k-scale answer: [`architecture.pdf`](architecture.pdf)
(source: [`docs/architecture.md`](docs/architecture.md)).

---

## Layout

| Path | What |
|---|---|
| `src/schemas.py` | Canonical pydantic models for all record types (the LLM↔sink contract) |
| `src/config.py` | Env/secret settings + YAML config loading |
| `src/crawler/http.py` | The one outbound-request path: semaphore, per-host RPS, retry/backoff |
| `src/crawler/arxiv.py`, `papers.py` | Research-paper vertical: arXiv API → GitHub correlation → live stars |
| `src/crawler/feeds.py` | News + jobs: RSS/JSON APIs, 24h freshness, dedupe, full-text |
| `src/crawler/directory.py` | Generic startup/product directory crawler (config-driven selectors) |
| `src/crawler/dates.py` | Date normalization + freshness heuristic |
| `src/crawler/seen_store.py` | Dedupe store: `SeenStore` interface, SQLite + Redis backends |
| `src/crawler/browser.py` | Playwright escalation for Cloudflare/JS pages (Phase V) |
| `src/llm/` | Multi-tier extraction engine: providers, chunking, orchestrator |
| `src/resolver/` | Deterministic entity resolution + mapping log |
| `src/sinks/` | JSONL (always) + Google Sheets (deliverable) |
| `src/pipeline.py` | Top-level `crawl → extract → resolve → sink` orchestration |
| `config/*.yaml` | Every source URL + every tuning knob. Add a source = edit YAML. |
| `tests/` | Unit tests for the parts with tricky logic (chain fallback, dates, resolver, chunking, dedupe) |

---

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows;  source .venv/bin/activate on *nix
pip install -r requirements.txt
python -m playwright install chromium   # only if you need the Phase V browser path

cp .env.example .env               # then fill in the keys below
```

### Credentials (`.env`)

| Var | Needed for | Notes |
|---|---|---|
| `GEMINI_API_KEY` | LLM tier 1 | aistudio.google.com, free tier |
| `GROQ_API_KEY` | LLM tier 2 | console.groq.com, free tier |
| `OPENROUTER_API_KEY` | LLM tier 3 (`deepseek/deepseek-chat`) | openrouter.ai — one key, many models |
| `GITHUB_TOKEN` | research-paper star counts | classic token, no scopes; 60→5000 req/hr |
| `GOOGLE_SERVICE_ACCOUNT_JSON` + `GSHEET_ID` | Sheets deliverable | share the sheet with the SA email as Editor |
| `PROXY_URL` *(optional)* | Phase V anti-bot | rotating/residential proxy endpoint |

Without keys: the crawlers and resolver still run and write JSONL; the LLM step
logs `tier_unavailable` and skips, Sheets logs `sheets_disabled`.

---

## Run

```bash
python -m src.pipeline all         # every vertical, concurrently
python -m src.pipeline papers      # just research papers
python -m src.pipeline news        # just 24h-fresh news
python -m src.pipeline jobs
python -m src.pipeline startups
python -m src.pipeline products
```

Output: `data/output/*.jsonl` locally, and (if configured) the 6 Sheet tabs
`Startups / Products / Research Papers / Jobs / News / Entity Mapping Log`.

```bash
pytest -q                          # 20 tests, no network/keys required
ruff check src tests
```

---

## Data integrity

Per the brief: **hallucinated data is disqualifying.** Every record carries a
required, validated `source.url`. The LLM system prompt forbids inventing
values (null for anything not present), output is parsed and pydantic-validated,
and a record that fails validation on every tier is **dropped and logged**,
never fabricated.

---

## Status / open items

See [`docs/OPEN_ITEMS.md`](docs/OPEN_ITEMS.md) — mainly: confirm the specific
startup/product directory sources + fill their CSS selectors, and confirm the
5 news / 5 job feeds against each site's ToS and anti-bot posture.
