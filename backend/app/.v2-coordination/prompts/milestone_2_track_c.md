# TRACK C — MILESTONE 2: Contract Validation Against Fixtures

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-c
Branch: track/c

**THIS IS A VALIDATION-ONLY TASK. Do NOT modify page files, v2_surface_contracts.ts, or v2_ids.ts.**

---

## PREREQUISITE

Before starting: confirm that Track B's gate file exists and all fields are true:
`backend/app/.v2-coordination/gates/milestone_2_track_b.json`

If it does not exist or any field is false, stop and do not proceed.

---

## TASK 1: VALIDATE v2_surface_contracts.ts AGAINST FIXTURES

Read:
- `shared/v2_surface_contracts.ts` — the TypeScript contracts Track B owns
- `backend/app/.v2-coordination/fixtures/blueprint_explorer_contract_sample.json`
- `backend/app/.v2-coordination/fixtures/candidate_report_contract_sample.json`

For each fixture, check that every top-level field in the JSON is present in the corresponding TypeScript interface.
Check that field types are compatible (string → string, boolean → boolean, array → array).
Check that `holdings_overlay_present` is present in `V2ContractBase`.

Report all mismatches — do NOT edit `v2_surface_contracts.ts`. Only report.

---

## TASK 2: WRITE V2 API CLIENT STUB

Create `frontend/src/lib/v2_api_client.ts`:

```typescript
// V2 API client stub — returns mock data from B's fixtures in development.
// Replace with real fetch calls when V2 routes are live (M3).

import type {
  BlueprintExplorerContract,
  CandidateReportContract,
} from '../../shared/v2_surface_contracts';

export async function fetchBlueprintExplorer(): Promise<BlueprintExplorerContract> {
  // In dev: return fixture data
  // In prod: fetch('/api/v2/surfaces/blueprint/explorer')
  throw new Error('v2_api_client: not yet connected to live route');
}

export async function fetchCandidateReport(
  candidateId: string
): Promise<CandidateReportContract> {
  // In dev: return fixture data
  // In prod: fetch(`/api/v2/surfaces/candidates/${candidateId}/report`)
  throw new Error('v2_api_client: not yet connected to live route');
}
```

---

## TASK 3: DOCUMENT VALIDATION FINDINGS

Write `docs/v2_m2_contract_validation.md`:

```markdown
# V2 M2 Contract Validation

## Blueprint Explorer Contract
- Fixture fields checked: [list]
- TypeScript interface coverage: [pass/fail per field]
- Mismatches found: [list or "none"]

## Candidate Report Contract
- Fixture fields checked: [list]
- TypeScript interface coverage: [pass/fail per field]
- Mismatches found: [list or "none"]

## Stable ID Usage
- candidate_id: [found in fixture / missing]
- sleeve_id: [found in fixture / missing]

## Recommendation
[Pass / Needs Track B fix — list what needs fixing]
```

---

## GATE OUTPUT

Write:
`backend/app/.v2-coordination/gates/milestone_2_track_c.json`

```json
{
  "contract_validation_done": true
}
```

Set to `true` only after validation doc is written. Add `"notes"` field describing any mismatches found.
