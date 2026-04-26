# TRACK C — MILESTONE 3: Rebind Blueprint Explorer + Candidate Report

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-c
Branch: track/c

**BLOCKED until Track B gate passes. Check before starting:**
`backend/app/.v2-coordination/gates/milestone_3_track_b.json` — all fields must be `true`.
If not, stop and do not proceed.

---

## CONTEXT: Candidate Report page does not exist

From the M1 audit: `blueprint_candidate_report.tsx` does NOT exist as a standalone page.
Candidate report is currently rendered as a component (`CandidateReportContent.tsx`) embedded
inside `blueprint.tsx` and other pages.

**For M3, you will CREATE a new page file** that calls the V2 candidate report route directly.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `frontend/src/pages/blueprint.tsx` — current blueprint page
- `frontend/src/components/CandidateReportContent.tsx` (or similar path) — candidate report component
- `shared/v2_surface_contracts.ts` — types you consume (DO NOT EDIT)
- `shared/v2_ids.ts` — ID types (DO NOT EDIT)
- `frontend/src/lib/v2_api_client.ts` — the stub from M2 (update to real fetch calls)
- `docs/v2_core_surface_dependency_map.md` — M1 audit findings
- `backend/app/.v2-coordination/fixtures/blueprint_explorer_contract_sample.json`
- `backend/app/.v2-coordination/fixtures/candidate_report_contract_sample.json`

---

## TASK 2: UPDATE v2_api_client.ts WITH REAL FETCH CALLS

Update `frontend/src/lib/v2_api_client.ts`:

```typescript
import type {
  BlueprintExplorerContract,
  CandidateReportContract,
} from '../../shared/v2_surface_contracts';

export async function fetchBlueprintExplorer(): Promise<BlueprintExplorerContract> {
  const res = await fetch('/api/v2/surfaces/blueprint/explorer');
  if (!res.ok) throw new Error(`Blueprint explorer fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchCandidateReport(
  candidateId: string
): Promise<CandidateReportContract> {
  const res = await fetch(`/api/v2/surfaces/candidates/${candidateId}/report`);
  if (!res.ok) throw new Error(`Candidate report fetch failed: ${res.status}`);
  return res.json();
}
```

---

## TASK 3: REBIND blueprint.tsx

Rewrite `frontend/src/pages/blueprint.tsx` to:
- Call `fetchBlueprintExplorer()` from `v2_api_client.ts`
- Use types from `shared/v2_surface_contracts.ts` only
- Render: market_state_summary, review_posture, sleeves array (sleeve_purpose, lead_candidate_name, visible_state, implication_summary)
- REMOVE all calls to `/api/platform/*` and `/api/cortex/*`
- REMOVE all semantic reconstruction (no computing display labels from raw state codes)
- Keep it renderer-only: display what the contract says, nothing more

---

## TASK 4: CREATE frontend/src/pages/blueprint_candidate_report.tsx (NEW FILE)

Create a new standalone page that:
- Reads `candidateId` from route params
- Calls `fetchCandidateReport(candidateId)` from `v2_api_client.ts`
- Uses types from `shared/v2_surface_contracts.ts` only
- Renders: name, investment_case, current_implication, main_tradeoffs, baseline_comparisons, evidence_depth, mandate_boundary
- Zero calls to legacy routes
- Zero semantic reconstruction
- If `holdings_overlay` is present: render it; if null: render nothing (null-safe)

Register the new route in your frontend router (React Router or equivalent) at a path like `/blueprint/candidates/:candidateId/report`.

---

## TASK 5: SMOKE TEST VERIFICATION

Verify manually or via test:
1. Blueprint Explorer page loads without 500s
2. Candidate Report page loads for a known candidate_id
3. `grep -r "api/platform" frontend/src/pages/blueprint.tsx` → zero results
4. `grep -r "api/cortex" frontend/src/pages/blueprint.tsx` → zero results
5. `grep -r "canonical_frontend_contract" frontend/src/` → zero results

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_3_track_c.json`

```json
{
  "blueprint_rebound": true,
  "candidate_report_rebound": true,
  "smoke_test_pass": true
}
```

Set each to `true` only after confirmed. Add `"notes"` for anything partial.
