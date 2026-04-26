# TRACK C — MILESTONE 5: Rebind Portfolio

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-c
Branch: track/c

**BLOCKED until Track B gate passes. Check before starting:**
`backend/app/.v2-coordination/gates/milestone_5_track_b.json` — all fields must be `true`.
If not, stop and do not proceed.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `frontend/src/pages/portfolio.tsx` — current legacy page (multi-endpoint, page-side stitching)
- `shared/v2_surface_contracts.ts` — DO NOT EDIT; PortfolioContract, SleeveDriftRow
- `shared/v2_ids.ts` — DO NOT EDIT
- `frontend/src/lib/v2_api_client.ts` — extend with portfolio fetch
- `backend/app/.v2-coordination/fixtures/portfolio_contract_sample.json`
- `docs/v2_core_surface_dependency_map.md` — M1 audit: portfolio.tsx does multi-endpoint joins

---

## TASK 2: ADD PORTFOLIO TO v2_api_client.ts

Add to `frontend/src/lib/v2_api_client.ts`:

```typescript
import type { PortfolioContract } from '../../shared/v2_surface_contracts';

export async function fetchPortfolio(accountId: string = 'default'): Promise<PortfolioContract> {
  const res = await fetch(`/api/v2/surfaces/portfolio?account_id=${accountId}`);
  if (!res.ok) throw new Error(`Portfolio fetch failed: ${res.status}`);
  return res.json();
}
```

---

## TASK 3: REBIND portfolio.tsx

Rewrite `frontend/src/pages/portfolio.tsx` to:
- Call `fetchPortfolio()` from `v2_api_client.ts` — ONE call, ONE contract
- Use types from `shared/v2_surface_contracts.ts` only
- Render:
  - `mandate_state` + `action_posture` — top-level status
  - `what_matters_now` — primary message
  - `sleeve_drift_summary` — table of sleeve rows (sleeve_id, current_pct, target_pct, drift_pct, status)
  - `holdings_overlay` — if present, render overlay details
  - `blueprint_consequence` — if non-null, show as a callout
  - `daily_brief_consequence` — if non-null, show as a callout
- REMOVE all multi-endpoint joins (portfolio.tsx was calling multiple legacy endpoints and stitching on the page)
- REMOVE all calls to `/api/portfolio/*`, `/api/platform/*`, `/api/cortex/*`
- REMOVE all semantic reconstruction
- Renderer-only: no computing meaning from raw fields

---

## TASK 4: SMOKE TEST CHECKS

Verify:
1. `grep -r "api/portfolio\|api/platform\|api/cortex" frontend/src/pages/portfolio.tsx` → zero results
2. `grep -r "canonical_frontend_contract" frontend/src/` → zero results
3. Frontend build passes (`npm run build` in `frontend/`)

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_5_track_c.json`

```json
{
  "portfolio_rebound": true,
  "smoke_test_pass": true
}
```
