# TRACK B — MILESTONE 5: Portfolio V2 Route

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-b
Branch: track/b

Complete ALL tasks below before marking the gate file as done.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `backend/app/v2/core/domain_objects.py` — PortfolioTruth, PortfolioPressure, SleeveAssessment
- `backend/app/v2/core/holdings_overlay.py` — null-safe overlay
- `backend/app/v2/core/interpretation_engine.py` — interpretation engine
- `backend/app/v2/core/mandate_rubric.py` — rubric
- `backend/app/v2/router.py` — existing routes
- `shared/v2_surface_contracts.ts` — PortfolioContract must be complete
- `frontend/src/pages/portfolio.tsx` — current legacy page (read only)
- `backend/app/services/portfolio_state.py` — portfolio data donor
- `backend/app/v2/donors/portfolio_truth.py` — donor stub
- `backend/app/.v2-coordination/fixtures/` — existing fixtures
- `docs/v2_core_surface_dependency_map.md` — M1 audit findings for portfolio.tsx

---

## TASK 2: ENSURE PortfolioContract IS COMPLETE IN v2_surface_contracts.ts

Verify `shared/v2_surface_contracts.ts` contains a complete `PortfolioContract`:

```typescript
export interface PortfolioContract extends V2ContractBase {
  account_id: string;
  mandate_state: string;
  what_matters_now: string;
  action_posture: string;
  sleeve_drift_summary: SleeveDriftRow[];
  blueprint_consequence: string | null;
  daily_brief_consequence: string | null;
}

export interface SleeveDriftRow {
  sleeve_id: SleeveId;
  current_pct: number;
  target_pct: number;
  drift_pct: number;
  status: 'on_target' | 'needs_review' | 'off_target';
}
```

DO NOT import from `canonical_frontend_contract.ts`.

---

## TASK 3: IMPLEMENT PORTFOLIO TRUTH DONOR

Replace stub in `backend/app/v2/donors/portfolio_truth.py`:

- Wrap `backend/app/services/portfolio_state.py`
- Read current holdings if available; return empty PortfolioTruth if no holdings data
- `get_portfolio_truth(account_id: str) -> PortfolioTruth`
- Handle missing/None holdings gracefully

---

## TASK 4: BUILD PORTFOLIO CONTRACT BUILDER

Create `backend/app/v2/surfaces/portfolio/__init__.py` (empty)

Create `backend/app/v2/surfaces/portfolio/contract_builder.py`:

Single coherent contract — no page-side stitching across endpoints.

Call path:
1. Call `portfolio_truth_donor.get_portfolio_truth(account_id)` → `PortfolioTruth`
2. For each sleeve in holdings: compute drift from target allocation
3. Call `interpretation_engine.interpret()` for each sleeve with significant drift → `PortfolioPressure`
4. Call `mandate_rubric.apply_rubric()` → `ConstraintSummary` for `mandate_state`
5. Derive `blueprint_consequence` — 1-sentence string if any sleeve is off_target, else null
6. Derive `daily_brief_consequence` — 1-sentence string if any macro signal affects holdings, else null
7. Call `holdings_overlay.apply_overlay(base_contract, holdings)` — pass real holdings here
8. Return `PortfolioContract`

NEVER import from `blueprint_payload_assembler` or any legacy route handler.
If no holdings data: return contract with empty `sleeve_drift_summary`, `mandate_state: "no_data"`.

---

## TASK 5: ADD ROUTE TO ROUTER

Add to `backend/app/v2/router.py`:

```python
@router.get("/surfaces/portfolio")
async def portfolio(account_id: str = "default"):
    from backend.app.v2.surfaces.portfolio.contract_builder import build
    return build(account_id)
```

---

## TASK 6: WRITE CONTRACT TESTS

Create `tests/v2/test_portfolio_contract.py`:
- Test: GET `/api/v2/surfaces/portfolio` returns 200
- Test: response contains `account_id`, `mandate_state`, `sleeve_drift_summary`, `holdings_overlay_present`
- Test: `blueprint_consequence` and `daily_brief_consequence` are either string or null (not missing)
- Test: no banned fields (`gate_result`, `review_intensity_decision`, `prompt_schema`)
- CRITICAL: assert no `blueprint_payload_assembler` import in call stack

Run `python3 -m pytest tests/v2/test_portfolio_contract.py -x` before marking gate done.

---

## TASK 7: PRODUCE PORTFOLIO FIXTURE

Write `backend/app/.v2-coordination/fixtures/portfolio_contract_sample.json`:
- Complete `PortfolioContract` JSON
- 2–3 `SleeveDriftRow` entries with realistic sleeve names
- `holdings_overlay_present`: true (portfolio is the surface where holdings are wired)
- `mandate_state`: "on_mandate" or "drifted"

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_5_track_b.json`

```json
{
  "portfolio_route_live": true,
  "contract_tests_pass": true
}
```
