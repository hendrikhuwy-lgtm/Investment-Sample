# TRACK B — MILESTONE 3: Blueprint + Candidate V2 Routes

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-b
Branch: track/b

Complete ALL tasks below before marking the gate file as done.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `backend/app/v2/core/domain_objects.py` — all 18 domain objects
- `backend/app/v2/core/interpretation_engine.py` — M2 implementation
- `backend/app/v2/core/mandate_rubric.py` — M2 implementation
- `backend/app/v2/core/holdings_overlay.py` — M2 implementation
- `backend/app/v2/doctrine/doctrine_evaluator.py` — M2 implementation
- `backend/app/v2/router.py` — existing router scaffold
- `backend/app/.v2-coordination/fixtures/blueprint_explorer_contract_sample.json`
- `backend/app/.v2-coordination/fixtures/candidate_report_contract_sample.json`
- `shared/v2_surface_contracts.ts` — contracts you own
- `backend/app/services/blueprint_candidate_truth.py` — candidate data donor
- `backend/app/services/blueprint_benchmark_registry.py` — benchmark donor
- `docs/v2_m2_contract_validation.md` — Track C's validation findings (fix any noted mismatches)

---

## TASK 2: FIX v2_surface_contracts.ts

Track C's M2 validation found that `V2ContractBase`, `BlueprintExplorerContract`, and `CandidateReportContract` may be missing or incomplete in `shared/v2_surface_contracts.ts`.

Ensure `shared/v2_surface_contracts.ts` contains ALL of:
- `V2ContractBase` interface with: `surface_id`, `generated_at`, `freshness_state`, `holdings_overlay_present`
- `BlueprintExplorerContract extends V2ContractBase` with: `sleeves`, `market_state_summary`, `review_posture`
- `BlueprintSleeveRow` with: `sleeve_id`, `sleeve_purpose`, `lead_candidate_id`, `lead_candidate_name`, `visible_state`, `implication_summary`, `why_it_leads`, `main_limit`
- `CandidateReportContract extends V2ContractBase` with: `candidate_id`, `sleeve_id`, `name`, `investment_case`, `current_implication`, `main_tradeoffs`, `baseline_comparisons`, `evidence_depth`, `mandate_boundary`, `holdings_overlay`
- All other contracts from M1 (`DailyBriefContract`, `PortfolioContract`) must remain intact

DO NOT import from `canonical_frontend_contract.ts`.

---

## TASK 3: BUILD BLUEPRINT EXPLORER CONTRACT BUILDER

Create `backend/app/v2/surfaces/blueprint/__init__.py` (empty if not exists)

Create `backend/app/v2/surfaces/blueprint/explorer_contract_builder.py`:

Call path (strictly in order — no shortcuts):
1. For each sleeve: call donor interfaces to get `InstrumentTruth` for lead candidate
2. Call `market_price_adapter` (via source registry) to get `MarketSeriesTruth`
3. Call `interpretation_engine.interpret(truth, market)` → `(SignalPacket, InterpretationCard)`
4. Call `doctrine_evaluator.evaluate(candidate_assessment)` → `list[FrameworkRestraint]`
5. Call `mandate_rubric.apply_rubric(assessment, boundary, restraints)` → `ConstraintSummary`
6. Call `holdings_overlay.apply_overlay(base_contract, holdings=None)` (holdings always None for now)
7. Return `BlueprintExplorerContract`

NEVER import from:
- `blueprint_payload_assembler`
- `cortex_blueprint_presentation`
- Any `/api/platform/*` or `/api/cortex/*` route handler

Use real data from donor interfaces where available; fall back to None/empty gracefully.

---

## TASK 4: BUILD CANDIDATE REPORT CONTRACT BUILDER

Create `backend/app/v2/surfaces/blueprint/report_contract_builder.py`:

Call path:
1. Get `InstrumentTruth` for the candidate_id
2. Get `BenchmarkTruth` for each baseline comparison
3. Call `interpretation_engine.interpret()` for current implication
4. Call `doctrine_evaluator.evaluate()` + `mandate_rubric.apply_rubric()` for mandate_boundary
5. Call `holdings_overlay.apply_overlay(base_contract, holdings=None)`
6. Return `CandidateReportContract`

---

## TASK 5: ADD ROUTES TO ROUTER

Update `backend/app/v2/router.py` to add (or confirm already present):

```python
from fastapi import APIRouter
router = APIRouter(prefix="/api/v2")

@router.get("/surfaces/blueprint/explorer")
async def blueprint_explorer():
    from backend.app.v2.surfaces.blueprint.explorer_contract_builder import build
    return build()

@router.get("/surfaces/candidates/{candidate_id}/report")
async def candidate_report(candidate_id: str):
    from backend.app.v2.surfaces.blueprint.report_contract_builder import build
    return build(candidate_id)
```

Both routes must be mounted in `main.py` via `app.include_router(v2_router)` (already done in M1).

---

## TASK 6: WRITE CONTRACT TESTS

Create `tests/v2/test_blueprint_contracts.py`:

```python
# Test: GET /api/v2/surfaces/blueprint/explorer returns 200
# Test: response contains surface_id, freshness_state, holdings_overlay_present, sleeves
# Test: holdings_overlay_present is False (no holdings wired yet)
# Test: GET /api/v2/surfaces/candidates/{id}/report returns 200 for a known candidate_id
# CRITICAL: assert "blueprint_payload_assembler" not in str(inspect.stack()) during call
# Test: response does NOT contain fields: gate_result, review_intensity_decision, deep_review_result
# Test: stable IDs — two consecutive calls return same candidate_id values in sleeves
```

Use FastAPI `TestClient`. Run `pytest tests/v2/test_blueprint_contracts.py -x` before marking gate done.

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_3_track_b.json`

```json
{
  "explorer_route_live": true,
  "report_route_live": true,
  "contract_tests_pass": true
}
```
