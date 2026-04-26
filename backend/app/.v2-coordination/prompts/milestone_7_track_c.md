# TRACK C — MILESTONE 7: Final Cutover + Legacy Demotion

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-c
Branch: track/c

**BLOCKED until Track B gate passes. Check before starting:**
`backend/app/.v2-coordination/gates/milestone_7_track_b.json` — all fields must be `true`.
If not, stop and do not proceed.

---

## TASK 1: ADD NOTEBOOK + EVIDENCE TO v2_api_client.ts

Add to `frontend/src/lib/v2_api_client.ts`:

```typescript
import type { NotebookContract, EvidenceWorkspaceContract } from '../../shared/v2_surface_contracts';

export async function fetchNotebook(candidateId: string): Promise<NotebookContract> {
  const res = await fetch(`/api/v2/surfaces/candidates/${candidateId}/notebook`);
  if (!res.ok) throw new Error(`Notebook fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchEvidenceWorkspace(candidateId: string): Promise<EvidenceWorkspaceContract> {
  const res = await fetch(`/api/v2/surfaces/candidates/${candidateId}/evidence`);
  if (!res.ok) throw new Error(`Evidence workspace fetch failed: ${res.status}`);
  return res.json();
}
```

---

## TASK 2: CREATE OR REBIND NOTEBOOK + EVIDENCE WORKSPACE PAGES

Check `frontend/src/pages/` for any existing notebook or evidence workspace pages.
- If they exist: rebind to V2 API client, remove legacy routes
- If they do not exist: create renderer-only pages:
  - `frontend/src/pages/notebook.tsx` — renders `evidence_sections`, `evidence_depth`
  - `frontend/src/pages/evidence_workspace.tsx` — renders `source_citations`, `completeness_score`, `evidence_pack`
  - Register both routes in frontend router

---

## TASK 3: FINAL LEGACY DEMOTION SWEEP

Run these checks across the entire `frontend/src/` directory:

```bash
grep -r "api/platform" frontend/src
grep -r "api/cortex" frontend/src
grep -r "api/daily-brief" frontend/src
grep -r "api/portfolio" frontend/src
grep -r "canonical_frontend_contract" frontend/src
```

For every file with remaining legacy references:
- If it is one of the 4 core surfaces (blueprint, candidate report, daily brief, portfolio): this is a regression — fix it
- If it is a shared utility or non-surface file: add a comment `// LEGACY — migration pending` and leave it
- Document every remaining legacy reference in `docs/v2_legacy_demotion_report.md`

---

## TASK 4: WRITE DEMOTION REPORT

Write `docs/v2_legacy_demotion_report.md`:

```markdown
# V2 Legacy Demotion Report — Milestone 7

## Core surfaces fully rebound (zero legacy routes)
- blueprint.tsx ✓
- blueprint_candidate_report.tsx ✓
- daily_brief.tsx ✓
- portfolio.tsx ✓
- compare.tsx ✓
- changes.tsx ✓
- notebook.tsx ✓
- evidence_workspace.tsx ✓

## Remaining legacy references (non-surface files)
| File | Reference | Status |
|---|---|---|
[list any found]

## Final grep results
[paste output of grep commands from Task 3]
```

---

## TASK 5: FRONTEND BUILD FINAL CHECK

Run `npm run build` in `frontend/` — must pass with zero errors from V2 files.

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_7_track_c.json`

```json
{
  "final_cutover_done": true,
  "legacy_demotion_done": true
}
```

Set `legacy_demotion_done: true` when all core surfaces are clean and demotion report is written.
