# TRACK C — MILESTONE 4: Rebind Daily Brief

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-c
Branch: track/c

**BLOCKED until Track B gate passes. Check before starting:**
`backend/app/.v2-coordination/gates/milestone_4_track_b.json` — all fields must be `true`.
If not, stop and do not proceed.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `frontend/src/pages/daily_brief.tsx` — current legacy page
- `shared/v2_surface_contracts.ts` — DO NOT EDIT; consume only
- `shared/v2_ids.ts` — DO NOT EDIT
- `frontend/src/lib/v2_api_client.ts` — extend with daily brief fetch
- `backend/app/.v2-coordination/fixtures/daily_brief_contract_sample.json`
- `docs/v2_core_surface_dependency_map.md` — M1 audit for daily_brief.tsx

---

## TASK 2: ADD DAILY BRIEF TO v2_api_client.ts

Add to `frontend/src/lib/v2_api_client.ts`:

```typescript
import type { DailyBriefContract } from '../../shared/v2_surface_contracts';

export async function fetchDailyBrief(): Promise<DailyBriefContract> {
  const res = await fetch('/api/v2/surfaces/daily-brief');
  if (!res.ok) throw new Error(`Daily brief fetch failed: ${res.status}`);
  return res.json();
}
```

---

## TASK 3: REBIND daily_brief.tsx

Rewrite `frontend/src/pages/daily_brief.tsx` to:
- Call `fetchDailyBrief()` from `v2_api_client.ts`
- Use types from `shared/v2_surface_contracts.ts` only
- Render in this order (matching contract output priority):
  1. `what_changed` — list of signal cards (what_changed, magnitude, direction, affected_sleeves)
  2. `why_it_matters_economically`
  3. `why_it_matters_here`
  4. `review_posture`
  5. `what_confirms_or_breaks`
  6. `evidence_and_trust` (freshness_state, source_count)
  7. `portfolio_overlay` — render only if non-null
- REMOVE all calls to `/api/daily-brief/*`, `/api/platform/*`, `/api/cortex/*`
- REMOVE all semantic reconstruction
- Renderer-only: display what the contract says

---

## TASK 4: CHECK FOR daily_brief_reader.tsx

Check if `frontend/src/pages/daily_brief_reader.tsx` exists. If it does:
- Rebind it the same way as `daily_brief.tsx`
- Remove all legacy route calls

---

## TASK 5: SMOKE TEST CHECKS

Verify:
1. `grep -r "api/daily-brief\|api/platform\|api/cortex" frontend/src/pages/daily_brief.tsx` → zero results
2. `grep -r "canonical_frontend_contract" frontend/src/` → zero results
3. Frontend build passes (`npm run build` in `frontend/`)

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_4_track_c.json`

```json
{
  "daily_brief_rebound": true,
  "smoke_test_pass": true
}
```
