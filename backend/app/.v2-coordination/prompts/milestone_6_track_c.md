# TRACK C — MILESTONE 6: Rebind Compare + Changes

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-c
Branch: track/c

**BLOCKED until Track B gate passes. Check before starting:**
`backend/app/.v2-coordination/gates/milestone_6_track_b.json` — all fields must be `true`.
If not, stop and do not proceed.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `frontend/src/pages/` — find any compare or changes page files
- `shared/v2_surface_contracts.ts` — DO NOT EDIT; CompareContract, ChangesContract
- `shared/v2_ids.ts` — DO NOT EDIT
- `frontend/src/lib/v2_api_client.ts` — extend with compare + changes fetches

---

## TASK 2: ADD COMPARE + CHANGES TO v2_api_client.ts

Add to `frontend/src/lib/v2_api_client.ts`:

```typescript
import type { CompareContract, ChangesContract } from '../../shared/v2_surface_contracts';

export async function fetchCompare(
  candidateAId: string,
  candidateBId: string
): Promise<CompareContract> {
  const res = await fetch(
    `/api/v2/surfaces/compare?candidate_a=${candidateAId}&candidate_b=${candidateBId}`
  );
  if (!res.ok) throw new Error(`Compare fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchChanges(
  surfaceId: string,
  sinceUtc?: string
): Promise<ChangesContract> {
  const url = sinceUtc
    ? `/api/v2/surfaces/changes?surface_id=${surfaceId}&since_utc=${sinceUtc}`
    : `/api/v2/surfaces/changes?surface_id=${surfaceId}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Changes fetch failed: ${res.status}`);
  return res.json();
}
```

---

## TASK 3: REBIND OR CREATE COMPARE PAGE

Check if a compare page exists (e.g. `frontend/src/pages/compare.tsx` or similar).
- If it exists: rebind to `fetchCompare()`, remove all legacy routes
- If it does not exist: create `frontend/src/pages/compare.tsx` that:
  - Reads `candidateAId` and `candidateBId` from URL params or query string
  - Calls `fetchCompare()`
  - Renders: `who_leads`, `why_leads`, `where_loser_wins`, `what_would_change_comparison`, `dimensions` table
  - Register route at `/compare` in frontend router

---

## TASK 4: REBIND OR CREATE CHANGES PAGE

Check if a changes page exists (e.g. `frontend/src/pages/changes.tsx` or similar).
- If it exists: rebind to `fetchChanges()`, remove all legacy routes
- If it does not exist: create `frontend/src/pages/changes.tsx` that:
  - Reads `surfaceId` from URL params
  - Calls `fetchChanges(surfaceId)`
  - Renders: `net_impact` badge, `change_events` list (event_type, summary, changed_at_utc)
  - Register route at `/changes` in frontend router

---

## TASK 5: SMOKE TEST CHECKS

Verify:
1. No legacy route references in the compare/changes pages
2. `grep -r "canonical_frontend_contract" frontend/src/` → zero results
3. Frontend build passes

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_6_track_c.json`

```json
{
  "compare_rebound": true,
  "changes_rebound": true,
  "smoke_test_pass": true
}
```
