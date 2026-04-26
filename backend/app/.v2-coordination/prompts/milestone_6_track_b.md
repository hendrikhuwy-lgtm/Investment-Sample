# TRACK B — MILESTONE 6: Compare + Changes Routes + Change Ledger

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-b
Branch: track/b

Complete ALL tasks below before marking the gate file as done.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `backend/app/v2/core/domain_objects.py` — CompareAssessment, ChangeEvent, DecisionDiff, TrustDiff, SurfaceChangeSummary
- `backend/app/v2/core/change_ledger.py` — stub with SQLAlchemy schema from M2
- `backend/app/v2/core/interpretation_engine.py`
- `backend/app/v2/core/mandate_rubric.py`
- `backend/app/v2/router.py` — existing routes
- `shared/v2_surface_contracts.ts` — CompareContract and ChangesContract must be complete
- `backend/app/services/blueprint_candidate_truth.py` — candidate data donor
- `backend/app/services/blueprint_benchmark_registry.py` — benchmark donor

---

## TASK 2: ENSURE COMPARE + CHANGES CONTRACTS IN v2_surface_contracts.ts

Verify/add to `shared/v2_surface_contracts.ts`:

```typescript
export interface CompareContract extends V2ContractBase {
  candidate_a_id: CandidateId;
  candidate_b_id: CandidateId;
  candidate_a_name: string;
  candidate_b_name: string;
  who_leads: CandidateId;
  why_leads: string;
  where_loser_wins: string | null;
  what_would_change_comparison: string | null;
  dimensions: CompareDimension[];
}

export interface CompareDimension {
  dimension: string;
  a_value: string;
  b_value: string;
  winner: CandidateId | 'tie';
}

export interface ChangesContract extends V2ContractBase {
  surface_id: string;
  change_events: ChangeEventRow[];
  net_impact: 'material' | 'minor' | 'none';
  since_utc: string | null;
}

export interface ChangeEventRow {
  event_id: string;
  event_type: string;
  summary: string;
  changed_at_utc: string;
}
```

---

## TASK 3: PROMOTE CHANGE LEDGER FROM STUB TO REAL

Replace stubs in `backend/app/v2/core/change_ledger.py`:

- Use SQLite via SQLAlchemy (same DB as existing `storage/investment_agent.sqlite3` if it exists, else create `storage/v2_changes.sqlite3`)
- Implement `record_change(event_type, surface_id, summary) -> str` — inserts a `ChangeEventRecord`, returns `event_id`
- Implement `get_diffs(surface_id, since_utc=None) -> list[ChangeEventRow]` — queries by surface_id, optionally filtered by `changed_at_utc >= since_utc`
- Create table on first use (use `Base.metadata.create_all`)
- Handle DB errors gracefully (log, return empty list — don't crash route)

---

## TASK 4: BUILD COMPARE CONTRACT BUILDER

Create `backend/app/v2/surfaces/compare/__init__.py` (empty)
Create `backend/app/v2/surfaces/compare/contract_builder.py`:

- Takes `candidate_a_id` and `candidate_b_id`
- Gets `InstrumentTruth` for each candidate
- Calls `interpretation_engine.interpret()` for each
- Builds `CompareAssessment` — who leads, why, where loser wins
- Calls `mandate_rubric.apply_rubric()` for boundary checks
- Returns `CompareContract`
- NEVER import from `blueprint_payload_assembler`

---

## TASK 5: BUILD CHANGES CONTRACT BUILDER

Create `backend/app/v2/surfaces/changes/__init__.py` (empty)
Create `backend/app/v2/surfaces/changes/contract_builder.py`:

- Takes `surface_id` and optional `since_utc`
- Calls `change_ledger.get_diffs(surface_id, since_utc)`
- Derives `net_impact` from event types: "material" if any `truth_change` or `boundary_change`, "minor" if only `interpretation_change`, "none" if empty
- Returns `ChangesContract`

---

## TASK 6: ADD ROUTES TO ROUTER

Add to `backend/app/v2/router.py`:

```python
@router.get("/surfaces/compare")
async def compare(candidate_a: str, candidate_b: str):
    from backend.app.v2.surfaces.compare.contract_builder import build
    return build(candidate_a, candidate_b)

@router.get("/surfaces/changes")
async def changes(surface_id: str, since_utc: str | None = None):
    from backend.app.v2.surfaces.changes.contract_builder import build
    return build(surface_id, since_utc)
```

---

## TASK 7: WRITE TESTS

Create `tests/v2/test_compare_changes_contracts.py`:
- Test: GET `/api/v2/surfaces/compare?candidate_a=X&candidate_b=Y` returns 200
- Test: response contains `who_leads`, `why_leads`, `dimensions`
- Test: GET `/api/v2/surfaces/changes?surface_id=blueprint_explorer` returns 200
- Test: response contains `change_events` (may be empty list), `net_impact`
- Test: `record_change()` + `get_diffs()` round-trip works
- CRITICAL: no `blueprint_payload_assembler` import in call stack

Run `python3 -m pytest tests/v2/test_compare_changes_contracts.py -x` before marking gate done.

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_6_track_b.json`

```json
{
  "compare_route_live": true,
  "changes_route_live": true,
  "change_ledger_live": true
}
```
