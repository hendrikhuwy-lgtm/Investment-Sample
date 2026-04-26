# TRACK B — MILESTONE 4: Daily Brief V2 Route

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-b
Branch: track/b

Complete ALL tasks below before marking the gate file as done.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `backend/app/v2/core/domain_objects.py` — all 18 domain objects
- `backend/app/v2/core/interpretation_engine.py` — M2/M3 implementation
- `backend/app/v2/core/mandate_rubric.py` — M2/M3 implementation
- `backend/app/v2/core/holdings_overlay.py` — M2 implementation
- `backend/app/v2/doctrine/doctrine_evaluator.py` — M2 implementation
- `backend/app/v2/router.py` — existing router with blueprint routes
- `shared/v2_surface_contracts.ts` — contracts you own; DailyBriefContract must be complete
- `frontend/src/pages/daily_brief.tsx` — current legacy page (read only, understand structure)
- `backend/app/services/daily_brief_execution_contract.py` — existing daily brief data (donor, read only)
- `backend/app/services/daily_brief_fact_pack.py` — fact pack donor
- `backend/app/services/brief_grounding_state.py` — grounding state donor
- `backend/app/.v2-coordination/fixtures/` — existing fixtures

---

## TASK 2: ENSURE DailyBriefContract IS COMPLETE IN v2_surface_contracts.ts

Verify `shared/v2_surface_contracts.ts` contains a complete `DailyBriefContract`:

```typescript
export interface DailyBriefContract extends V2ContractBase {
  what_changed: SignalCardV2[];
  why_it_matters_economically: string;
  why_it_matters_here: string;
  review_posture: string;
  what_confirms_or_breaks: string;
  evidence_and_trust: EvidenceSummary;
  portfolio_overlay: PortfolioOverlaySummary | null;
}
```

All referenced types (`SignalCardV2`, `EvidenceSummary`, `PortfolioOverlaySummary`) must be present.
DO NOT import from `canonical_frontend_contract.ts`.

---

## TASK 3: BUILD DAILY BRIEF CONTRACT BUILDER

Create `backend/app/v2/surfaces/daily_brief/__init__.py` (empty)

Create `backend/app/v2/surfaces/daily_brief/contract_builder.py`:

Output priority order (must be reflected in contract structure):
1. `what_changed` — array of `SignalCardV2` from macro + market signals
2. `why_it_matters_economically` — interpretation string from `InterpretationCard`
3. `why_it_matters_here` — sleeve/candidate impact summary from `SleeveAssessment`
4. `review_posture` — from `VisibleDecisionState`
5. `what_confirms_or_breaks` — from `ConstraintSummary.doctrine_annotations`
6. `evidence_and_trust` — `EvidenceSummary` with `freshness_state`, `source_count`, `completeness_score`
7. `portfolio_overlay` — nullable `PortfolioOverlaySummary`

Call path:
1. Read macro signals from `macro_adapter` (via source registry) → `macro_signal_translator` → `MacroTruth`
2. Read market signals from `market_price_adapter` → `market_signal_translator` → `MarketSeriesTruth`
3. Call `interpretation_engine.interpret()` for each significant signal
4. Aggregate into `SignalCardV2` array (top 3–5 signals by magnitude)
5. Call `doctrine_evaluator.evaluate()` + `mandate_rubric.apply_rubric()` for `what_confirms_or_breaks`
6. Call `holdings_overlay.apply_overlay(base, holdings=None)`
7. Return `DailyBriefContract`

NEVER import from `blueprint_payload_assembler`, `cortex_blueprint_presentation`, or any legacy route handler.

If Tier 1B adapters are not yet live (Track A still in M4), use fallback values:
- `what_changed`: empty array with `freshness_state: "degraded_monitoring_mode"`
- All other fields: sensible defaults

---

## TASK 4: ADD ROUTE TO ROUTER

Add to `backend/app/v2/router.py`:

```python
@router.get("/surfaces/daily-brief")
async def daily_brief():
    from backend.app.v2.surfaces.daily_brief.contract_builder import build
    return build()
```

---

## TASK 5: WRITE CONTRACT TESTS

Add `tests/v2/test_daily_brief_contract.py`:
- Test: GET `/api/v2/surfaces/daily-brief` returns 200
- Test: response contains `what_changed`, `why_it_matters_economically`, `review_posture`, `evidence_and_trust`
- Test: `portfolio_overlay` is null (no holdings wired yet)
- Test: `holdings_overlay_present` is False
- CRITICAL: assert `blueprint_payload_assembler` not in imports used by the route
- Test: banned fields not present: `gate_result`, `review_intensity_decision`, `prompt_schema`, `retry_count`

Run `python3 -m pytest tests/v2/test_daily_brief_contract.py -x` before marking gate done.

---

## TASK 6: PRODUCE DAILY BRIEF FIXTURE

Write `backend/app/.v2-coordination/fixtures/daily_brief_contract_sample.json`:
- Complete `DailyBriefContract` JSON with realistic values
- `what_changed`: 2–3 `SignalCardV2` entries (macro signals)
- `freshness_state`: "stored_valid_context"
- `portfolio_overlay`: null

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_4_track_b.json`

```json
{
  "daily_brief_route_live": true,
  "contract_tests_pass": true,
  "fixture_produced": true
}
```
