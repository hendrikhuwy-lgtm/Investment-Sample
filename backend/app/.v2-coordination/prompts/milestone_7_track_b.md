# TRACK B — MILESTONE 7: Notebook + Evidence Workspace Routes

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-b
Branch: track/b

Complete ALL tasks below before marking the gate file as done.

---

## TASK 1: READ CONTEXT FIRST

Read these files:
- `backend/app/v2/core/domain_objects.py` — EvidencePack, InstrumentTruth
- `backend/app/v2/donors/evidence_pack.py` — donor stub
- `backend/app/v2/router.py` — all existing routes
- `shared/v2_surface_contracts.ts` — add Notebook + EvidenceWorkspace contracts
- `backend/app/services/blueprint_candidate_truth.py` — candidate data donor
- `frontend/src/pages/` — check for any existing notebook or evidence workspace pages

---

## TASK 2: ADD NOTEBOOK + EVIDENCE CONTRACTS TO v2_surface_contracts.ts

Add to `shared/v2_surface_contracts.ts`:

```typescript
export interface NotebookContract extends V2ContractBase {
  candidate_id: CandidateId;
  name: string;
  investment_case: string;
  evidence_sections: EvidenceSection[];
  evidence_depth: string;
  last_updated_utc: string | null;
}

export interface EvidenceSection {
  section_id: string;
  title: string;
  body: string;
  source_refs: string[];
  freshness_state: FreshnessClass;
}

export interface EvidenceWorkspaceContract extends V2ContractBase {
  candidate_id: CandidateId;
  evidence_pack: EvidencePackSummary;
  source_citations: SourceCitation[];
  completeness_score: number | null;
}

export interface EvidencePackSummary {
  source_count: number;
  freshness_state: FreshnessClass;
  completeness_score: number | null;
}

export interface SourceCitation {
  source_id: string;
  title: string;
  url: string | null;
  retrieved_utc: string | null;
  reliability: 'high' | 'medium' | 'low';
}
```

---

## TASK 3: IMPLEMENT EVIDENCE PACK DONOR

Replace stub in `backend/app/v2/donors/evidence_pack.py`:
- Wrap `backend/app/services/blueprint_candidate_truth.py` — read any citation/evidence fields
- `build_evidence_pack(candidate_id: str) -> EvidencePack`
- If no evidence data available: return `EvidencePack` with empty sources and `completeness_score=0.0`

---

## TASK 4: BUILD NOTEBOOK CONTRACT BUILDER

Create `backend/app/v2/surfaces/notebook/__init__.py` (empty)
Create `backend/app/v2/surfaces/notebook/contract_builder.py`:
- Gets `InstrumentTruth` for candidate_id
- Calls `build_evidence_pack(candidate_id)`
- Builds `NotebookContract` with evidence sections derived from available source data
- If no data: return contract with empty `evidence_sections`
- NEVER import from `blueprint_payload_assembler`

---

## TASK 5: BUILD EVIDENCE WORKSPACE CONTRACT BUILDER

Create `backend/app/v2/surfaces/evidence_workspace/__init__.py` (empty)
Create `backend/app/v2/surfaces/evidence_workspace/contract_builder.py`:
- Gets `EvidencePack` for candidate_id
- Maps source citations
- Returns `EvidenceWorkspaceContract`

---

## TASK 6: ADD ROUTES TO ROUTER

Add to `backend/app/v2/router.py`:

```python
@router.get("/surfaces/candidates/{candidate_id}/notebook")
async def notebook(candidate_id: str):
    from backend.app.v2.surfaces.notebook.contract_builder import build
    return build(candidate_id)

@router.get("/surfaces/candidates/{candidate_id}/evidence")
async def evidence_workspace(candidate_id: str):
    from backend.app.v2.surfaces.evidence_workspace.contract_builder import build
    return build(candidate_id)
```

---

## TASK 7: WRITE TESTS

Create `tests/v2/test_notebook_evidence_contracts.py`:
- Test: GET `/api/v2/surfaces/candidates/{id}/notebook` returns 200
- Test: response contains `candidate_id`, `evidence_sections`, `evidence_depth`
- Test: GET `/api/v2/surfaces/candidates/{id}/evidence` returns 200
- Test: response contains `evidence_pack`, `source_citations`, `completeness_score`
- CRITICAL: no `blueprint_payload_assembler` import in call stack

Run `python3 -m pytest tests/v2/test_notebook_evidence_contracts.py -x` before marking done.

---

## GATE OUTPUT

Write: `backend/app/.v2-coordination/gates/milestone_7_track_b.json`

```json
{
  "notebook_route_live": true,
  "evidence_workspace_live": true
}
```
