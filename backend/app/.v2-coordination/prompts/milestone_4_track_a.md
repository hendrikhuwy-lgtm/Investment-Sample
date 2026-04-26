# TRACK A — MILESTONE 4: Tier 1B Sources (News + Macro)

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-a
Branch: track/a

Complete ALL tasks below before marking the gate file as done.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `backend/app/v2/sources/` — all Tier 1A adapters from M2/M3
- `backend/app/v2/sources/registry.py` — source registry from M3
- `backend/app/v2/sources/freshness_registry.py` — freshness tracking
- `backend/app/v2/translators/market_signal_translator.py` — M3 implementation
- `backend/app/services/ingest_mcp.py` — existing MCP ingestion
- `backend/app/services/mcp_policy.py` — MCP policy
- `mcp/` directory — all MCP connectors
- `docs/v2_mcp_rationalization.md` — Tier 1B connectors identified in M1
- `docs/v2_source_secrets_policy.md` — credentials policy

---

## TASK 2: IMPLEMENT TIER 1B NEWS ADAPTER

Create `backend/app/v2/sources/news_adapter.py`:

- Check `mcp/` for any existing news/GDELT/Finnhub news connector
- If a Finnhub news connector exists and credentials are configured: wrap it
- If GDELT is available (free, no key): implement a simple GDELT API fetch for top financial headlines
- Otherwise: implement a stub that returns `{"error": "news_unavailable", "items": []}`
- `fetch(limit: int = 10) -> list[dict]` — returns list of `{headline, source, published_utc, url}`
- `freshness_state() -> FreshnessState`
- `source_tier: str = "1B"`
- `fallback() -> list[dict]` — returns empty list

---

## TASK 3: IMPLEMENT TIER 1B MACRO ADAPTER

Create `backend/app/v2/sources/macro_adapter.py`:

- Check `mcp/` and `backend/app/services/` for any existing FRED/macro connector
- If FRED API key is configured (`FRED_API_KEY` in env or config): implement real FRED fetch for:
  - DGS10 (10-year Treasury yield)
  - FEDFUNDS (Fed Funds Rate)
  - CPIAUCSL (CPI)
  - SP500 (S&P 500 index)
- If no key: read from `backend/outbox/live_cache/` (FRED CSVs are already there: DGS10.csv, SP500.csv, BAMLH0A0HYM2.csv)
- `fetch(series_id: str) -> dict` — returns `{series_id, value, date, unit}`
- `fetch_all() -> list[dict]` — returns all available series
- `freshness_state() -> FreshnessState`
- `source_tier: str = "1B"`
- `fallback(series_id: str) -> dict` — returns `{"value": None, "error": "macro_unavailable"}`

---

## TASK 4: IMPLEMENT TRANSLATORS

Create `backend/app/v2/translators/macro_signal_translator.py`:
- Input: raw dict from `macro_adapter.fetch(series_id)`
- Returns `MacroTruth` from `backend/app/v2/core/domain_objects.py`
- Map: `indicator_id = series_id`, `name`, `current_value`, `previous_value` (if available), `unit`, `regime_signal = None` (set by interpretation engine), `freshness_state`, `source_id = "macro_adapter"`

Create `backend/app/v2/translators/news_signal_translator.py`:
- Input: raw dict from `news_adapter.fetch()`
- Returns list of `MarketSeriesTruth` event entries (one per headline)
- Map: `series_id = f"news:{item['source']}:{hash}"`, `ticker = item.get('source')`, `current_value = None`, `one_day_change_pct = None`, `regime_label = item.get('headline')`, `freshness_state`, `source_id = "news_adapter"`

---

## TASK 5: REGISTER TIER 1B IN SOURCE REGISTRY

Update `backend/app/v2/sources/registry.py` to add:
- `_news_adapter = NewsAdapter()`
- `_macro_adapter = MacroAdapter()`
- Register both in `_freshness_registry`
- Expose `get_news_adapter()` and `get_macro_adapter()` functions

---

## TASK 6: WRITE TESTS

Add to `tests/v2/test_tier1b_sources.py` (create file):
- Test that both adapters import without error
- Test that `macro_adapter.fetch_all()` returns a non-empty list (reads from CSV cache)
- Test that `macro_signal_translator.translate({})` returns `MacroTruth` without crashing
- Test that `news_signal_translator.translate([])` returns empty list without crashing

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_4_track_a.json`

```json
{
  "tier1b_news_adapter_done": true,
  "tier1b_macro_adapter_done": true,
  "tier1b_translators_done": true
}
```
