# Open items / decisions needed

Things deliberately left as decisions rather than guessed during scaffolding.

## 1. Startup + product directory sources (Phase I)
`config/sources.yaml` lists YC Companies + There's An AI For That as
`status: PROPOSED` with **no `selectors` block**. To activate:
- confirm the sites are acceptable to scrape (ToS / rate posture)
- fill `selectors.item` and `selectors.link` for the list pages
- set `list_pages.pattern` / `max_pages`
- set `render: true` if the listing is JS-only (routes through Playwright)
Alternatives if those two are off the table: Crunchbase (paid API), Product
Hunt API, an arXiv-affiliation-derived company list, a curated CSV seed.

## 2. News (5) + job boards (5) — Phase II
Current picks (all `PROPOSED`): VentureBeat AI, TechCrunch AI, The Verge AI, MIT
Tech Review AI, Ars Technica AI / ai-jobs.net, Remotive, RemoteOK, We Work
Remotely, Himalayas. Confirm each against ToS; some (VentureBeat) 429 on article
pages and will fall back to the browser path. Swap any that are unacceptable.

## 3. LLM chain
Config default: `gemini-1.5-flash` → `llama-3.3-70b-versatile` (Groq) →
`deepseek/deepseek-chat` (OpenRouter). Adjust models/order in
`config/settings.yaml` → `llm.chain`. Needs `GEMINI_API_KEY`, `GROQ_API_KEY`,
`OPENROUTER_API_KEY`.

## 4. Google Sheet
Create the spreadsheet, share it with the service-account email as Editor, put
its id in `GSHEET_ID`, and drop the SA JSON at
`config/gcp-service-account.json`. Tabs are auto-created.

## 5. GitHub token
`GITHUB_TOKEN` — without it the paper vertical is throttled to 60 req/hr and
most `github_stars` come back null.

## 6. Proxy (optional, Phase V)
`PROXY_URL` for a rotating/residential endpoint if we want to actually
demonstrate (not just document) scraping a Cloudflare-hard source.
