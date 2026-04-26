# Framework Phase 2 Cleanup

## Objective

Phase one moved semantic ownership into the canonical backend decision path.

Phase two removes residue that could steal that meaning back.

## File inventory and classification

| File or module | Classification | Status |
|---|---|---|
| `backend/app/services/framework_constitution.py` | canonical keeper | kept |
| `backend/app/services/blueprint_pipeline.py` | canonical keeper | kept |
| `backend/app/services/blueprint_canonical_decision.py` | canonical keeper | kept |
| `backend/app/services/framework_lenses/*` | canonical keeper | added |
| `backend/app/services/blueprint_payload_assembler.py` | payload assembly / candidate for future split | kept |
| `backend/app/services/portfolio_blueprint.py` | orchestration only | shrunk to thin wrapper |
| `backend/app/services/daily_brief_execution_contract.py` | canonical keeper | kept |
| `shared/canonical_frontend_contract.ts` | canonical keeper | kept |
| `investment-agent-demo/src/lib/transformers.ts` | renderer-side normalization only | hardened |
| `investment-agent-demo/src/lib/api.ts` | official API bridge | hardened |
| `investment-agent-demo/src/lib/types.ts` | official renderer types | pruned |

## Split and restructure

### Split completed

- `portfolio_blueprint.py` no longer contains the large implementation body.
- Large assembly logic now lives in `blueprint_payload_assembler.py`.
- Public imports remain stable through the thin wrapper:
  - `BLUEPRINT_PAYLOAD_INTEGRITY_VERSION`
  - `build_portfolio_blueprint_payload`
  - `_candidate_truth_state`

### Compatibility retired

- Official Blueprint payloads no longer expose `detail_explanation` or `thesis_sections`.
- The shared canonical frontend contract now requires `canonical_decision`.
- Legacy detail projection has been removed from the live path instead of being preserved behind another adapter.

## Deleted now

### Backend

- `backend/app/services/blueprint_candidate_explanation_fact_pack.py`
- `backend/app/services/blueprint_candidate_explanation_formatter.py`
- `backend/app/services/blueprint_candidate_explanation_schema.py`
- `backend/app/services/blueprint_candidate_explanation_validator.py`
- `backend/app/services/blueprint_candidate_explanation_writer.py`
- `backend/app/services/blueprint_thesis.py`
- `backend/app/services/blueprint_thesis_sections.py`
- `backend/app/services/blueprint_legacy_adapter.py`
- `backend/tests/test_blueprint_candidate_explanation.py`

### Frontend

- `investment-agent-demo/src/lib/mockData.ts`
- `investment-agent-demo/src/components/patterns/DataSourceBadge.tsx`
- `investment-agent-demo/src/components/patterns/ExplanationGrid.tsx`
- `investment-agent-demo/src/components/patterns/BoundaryStatement.tsx`
- `investment-agent-demo/src/components/blueprint/thesis/ActionSection.tsx`
- `investment-agent-demo/src/components/blueprint/thesis/EvidencePoint.tsx`
- `investment-agent-demo/src/components/blueprint/thesis/EvidenceSection.tsx`
- `investment-agent-demo/src/components/blueprint/thesis/PastBehaviorSection.tsx`

## Transitional files still allowed

### `backend/app/services/blueprint_payload_assembler.py`

Why it still exists:
- payload assembly and supporting detail payloads are still broad

Why it is allowed:
- semantic ownership has been removed from it

Deletion condition:
- split further if payload assembly needs additional maintainability work later

### `lens_review_set`

Status:
- removed from the official live payload path
- removed from runtime artifact persistence

Replacement:
- `lens_assessment`
- `lens_fusion_result`
- `framework_judgment`

## Frontend hardening completed

- mock fallback removed from official runtime
- canonical decision fallback removed from official renderer
- canonical summary fallback removed from official renderer
- Daily Brief renderer no longer reads `signal.explanation` compatibility fields
- action states reduced to `ignore | monitor | review`
- official app now depends on canonical backend truth rather than local semantic patching

## Rules after cleanup

1. Do not add new explanation builders ahead of `blueprint_canonical_decision.py`.
2. Do not put recommendation semantics back into `portfolio_blueprint.py`.
3. Do not reintroduce mock or fallback meaning into `investment-agent-demo`.
4. If compatibility is needed, derive it from canonical fields only.
5. Lens fusion precedence must remain gate-first and downgrade-only.
6. Do not reintroduce `lens_review_set` or any equivalent shadow lens summary into the official path.
