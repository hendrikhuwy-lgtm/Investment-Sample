# TRACK A — MILESTONE 2: Tier 1A Adapters + Real Translators

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-a
Branch: track/a

Complete ALL tasks below before marking the gate file as done.

---

## TASK 1: READ CONTEXT FIRST

Read these files to understand existing infrastructure:
- `backend/app/config/etf_doc_registry.json` — 25-instrument ETF registry
- `backend/app/services/ingest_etf_data.py` — ETF ingestion
- `backend/app/services/etf_doc_parser.py` — ETF document parsing
- `backend/app/services/blueprint_benchmark_registry.py` — benchmark mapping
- `backend/app/services/blueprint_candidate_truth.py` — candidate truth primitives
- `backend/app/services/provider_cache.py` — provider cache
- `backend/app/v2/donors/` — existing donor stubs from M1
- `backend/app/v2/translators/` — existing translator stubs from M1
- `docs/v2_mcp_rationalization.md` — Tier classification from M1
- `docs/v2_source_tier_table.md` — Tier table from M1

---

## TASK 2: IMPLEMENT TIER 1A SOURCE ADAPTERS

Create `backend/app/v2/sources/__init__.py` (empty)

Create these 4 adapter files in `backend/app/v2/sources/`:

**`backend/app/v2/sources/issuer_factsheet_adapter.py`**
- Wraps `backend/app/services/ingest_etf_data.py` and `etf_doc_parser.py`
- Reads from `backend/app/config/etf_doc_registry.json` (25 instruments)
- `fetch(ticker: str) -> dict` — returns raw factsheet fields for a ticker
- `fetch_all() -> list[dict]` — returns all 25 instruments
- `freshness_state() -> FreshnessState` — from `backend/app/v2/donors/source_freshness.py`
- `source_tier: str = "1A"`
- `fallback() -> dict` — returns empty dict with `{"error": "factsheet_unavailable"}`

**`backend/app/v2/sources/benchmark_truth_adapter.py`**
- Wraps `backend/app/services/blueprint_benchmark_registry.py`
- `fetch(benchmark_id: str) -> dict` — returns raw benchmark fields
- `fetch_all() -> list[dict]` — returns all known benchmarks
- `freshness_state() -> FreshnessState`
- `source_tier: str = "1A"`
- `fallback() -> dict`

**`backend/app/v2/sources/market_price_adapter.py`**
- Wraps whichever provider is already configured and returning data: check `backend/app/services/provider_cache.py` and `backend/app/config/` for Finnhub or Alpha Vantage credentials
- If no live provider is available, wrap the existing provider cache (last known values)
- `fetch(ticker: str) -> dict` — returns `{price, change_pct_1d, currency, as_of_utc}`
- `fetch_batch(tickers: list[str]) -> dict[str, dict]`
- `freshness_state() -> FreshnessState`
- `source_tier: str = "1A"`
- `fallback(ticker: str) -> dict` — returns `{"price": None, "error": "price_unavailable"}`

**`backend/app/v2/sources/freshness_registry.py`**
- Tracks per-source freshness state
- `register_source(source_id: str, adapter) -> None`
- `get_freshness(source_id: str) -> FreshnessState`
- `get_all_freshness() -> dict[str, FreshnessState]`
- Uses in-memory dict; no persistence required in M2

---

## TASK 3: IMPLEMENT REAL TRANSLATOR LOGIC

Implement real logic (not stubs) for these 2 translators:

**`backend/app/v2/translators/instrument_truth_translator.py`**
Replace the stub with real translation logic:
- Input: raw dict from `issuer_factsheet_adapter.fetch(ticker)`
- Map fields: ticker, name (from factsheet), asset_class (from registry classification), issuer, benchmark_id, ter, aum_usd, inception_date
- `freshness_state`: read from `freshness_registry` for source_id `"issuer_factsheet"`
- `source_id`: `"issuer_factsheet_adapter"`
- Returns `InstrumentTruth` from `backend/app/v2/core/domain_objects.py`
- Handle missing fields gracefully (None, not crash)

**`backend/app/v2/translators/benchmark_truth_translator.py`**
Replace the stub with real translation logic:
- Input: raw dict from `benchmark_truth_adapter.fetch(benchmark_id)`
- Map fields: benchmark_id, name, current_value, ytd_return_pct, one_year_return_pct
- `freshness_state`: read from `freshness_registry` for source_id `"benchmark_truth"`
- `source_id`: `"benchmark_truth_adapter"`
- Returns `BenchmarkTruth` from `backend/app/v2/core/domain_objects.py`

---

## TASK 4: WRITE TESTS

Create `tests/v2/__init__.py` (empty, if not exists)

Create `tests/v2/test_tier1a_sources.py`:
```python
# Test that all 3 Tier 1A adapters can be imported without error
# Test that issuer_factsheet_adapter.fetch_all() returns a non-empty list
# Test that fetch() on a known ticker (e.g. "IWDA" or first in etf_doc_registry) returns a dict
# Test that benchmark_truth_adapter.fetch_all() returns a non-empty list
# Test that instrument_truth_translator.translate({}) returns InstrumentTruth (with None fields, not crash)
# Test that benchmark_truth_translator.translate({}) returns BenchmarkTruth (with None fields, not crash)
# Mark all live-network tests with @pytest.mark.skip(reason="live network") unless mock data available
```

---

## GATE OUTPUT

When all 4 tasks are complete, write:
`backend/app/.v2-coordination/gates/milestone_2_track_a.json`

```json
{
  "tier1a_adapters_done": true,
  "translators_implemented": true
}
```

If any task is incomplete, set that field to `false` and add a `"notes"` field.
