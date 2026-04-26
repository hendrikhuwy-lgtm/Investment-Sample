# TRACK A — MILESTONE 3: Tier 1A Live + Source Registry

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-a
Branch: track/a

Complete ALL tasks below before marking the gate file as done.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `backend/app/v2/sources/` — all adapters from M2
- `backend/app/v2/translators/instrument_truth_translator.py` — M2 implementation
- `backend/app/v2/translators/benchmark_truth_translator.py` — M2 implementation
- `backend/app/v2/translators/market_signal_translator.py` — still a stub
- `backend/app/v2/donors/source_freshness.py` — freshness state
- `backend/app/config/etf_doc_registry.json` — 25 instruments
- `docs/v2_mcp_rationalization.md` — tier classification

---

## TASK 2: IMPLEMENT MARKET SIGNAL TRANSLATOR (real logic)

Replace the stub in `backend/app/v2/translators/market_signal_translator.py`:

- Input: raw dict from `market_price_adapter.fetch(ticker)`
- Map fields to `MarketSeriesTruth`:
  - `series_id`: f"market:{ticker}"
  - `ticker`: from input
  - `current_value`: price field (try "price", "close", "c" — whatever the adapter returns)
  - `one_day_change_pct`: change_pct_1d or compute from open/close if available
  - `one_week_change_pct`: None if not available
  - `regime_label`: None (populated by interpretation engine)
  - `freshness_state`: from freshness_registry for source_id "market_price"
  - `source_id`: "market_price_adapter"
- Returns `MarketSeriesTruth` from `backend/app/v2/core/domain_objects.py`
- Handle missing fields gracefully (None, not crash)

---

## TASK 3: BUILD SOURCE REGISTRY

Create `backend/app/v2/sources/registry.py` (replace if exists):

```python
from backend.app.v2.sources.issuer_factsheet_adapter import IssuerFactsheetAdapter
from backend.app.v2.sources.benchmark_truth_adapter import BenchmarkTruthAdapter
from backend.app.v2.sources.market_price_adapter import MarketPriceAdapter
from backend.app.v2.sources.freshness_registry import FreshnessRegistry

_freshness_registry = FreshnessRegistry()

_issuer_adapter = IssuerFactsheetAdapter()
_benchmark_adapter = BenchmarkTruthAdapter()
_market_adapter = MarketPriceAdapter()

_freshness_registry.register_source("issuer_factsheet", _issuer_adapter)
_freshness_registry.register_source("benchmark_truth", _benchmark_adapter)
_freshness_registry.register_source("market_price", _market_adapter)

def get_issuer_adapter() -> IssuerFactsheetAdapter:
    return _issuer_adapter

def get_benchmark_adapter() -> BenchmarkTruthAdapter:
    return _benchmark_adapter

def get_market_adapter() -> MarketPriceAdapter:
    return _market_adapter

def get_freshness_registry() -> FreshnessRegistry:
    return _freshness_registry
```

Adjust class names to match whatever was actually created in M2.

---

## TASK 4: END-TO-END SMOKE TEST

Add to `tests/v2/test_tier1a_sources.py`:

```python
def test_e2e_issuer_to_instrument_truth():
    """issuer factsheet fetch → translator → InstrumentTruth object"""
    from backend.app.v2.sources.registry import get_issuer_adapter
    from backend.app.v2.translators.instrument_truth_translator import translate
    adapter = get_issuer_adapter()
    raw = adapter.fetch_all()[0]  # first instrument
    truth = translate(raw)
    assert truth.candidate_id or truth.ticker  # at minimum one ID field populated
    assert truth.source_id == "issuer_factsheet_adapter"
```

Run `pytest tests/v2/test_tier1a_sources.py -x` — fix any failures before marking gate done.

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_3_track_a.json`

```json
{
  "tier1a_live": true,
  "source_registry_done": true
}
```
