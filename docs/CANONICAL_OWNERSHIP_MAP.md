# Canonical Ownership Map

## Backend meaning path

1. `backend/app/services/framework_constitution.py`
   - Owns doctrine only.
   - Hard rules: investor doctrine, promotion ladder, blockers, benchmark/tax/forecast boundaries, manual approval.

2. `backend/app/services/blueprint_pipeline.py`
   - Owns canonical gate, completeness, incumbent comparison, and portfolio consequence inputs.
   - Runs deterministic framework lens assessment and fusion after base gates/scoring and before canonical decision finalization.
   - Produces the semantic inputs consumed by the canonical decision object.

3. `backend/app/services/blueprint_canonical_decision.py`
   - Owns final candidate decision meaning.
   - This is the single authoritative owner of:
     - promotion state
     - base promotion state
     - readiness state
     - blockers
     - unresolved limits
     - lens fusion effects
     - framework judgment
     - action boundary
     - report sections
     - plain-English summary

4. `backend/app/services/blueprint_payload_assembler.py`
   - Owns payload assembly only.
   - Must not become a semantic owner again.

5. `backend/app/services/portfolio_blueprint.py`
   - Thin orchestration entry point only.
   - Re-exports the public payload builder from the assembler.

## Daily Brief meaning path

1. `backend/app/services/daily_brief_execution_contract.py`
   - Canonical interpretation and monitoring-first action state owner.
   - Lens context may modify explanation tone and review intensity only.
   - Lens context may not alter `ignore | monitor | review` action-state selection.

2. `backend/app/v2/surfaces/daily_brief/contract_builder.py`
   - V2 Daily Brief surface assembly and loading contract owner.
   - Must preserve the monitoring-first ceiling.

## Frontend meaning path

1. `shared/canonical_frontend_contract.ts`
   - Official cross-repo contract.
   - Requires canonical Blueprint decision meaning directly.

2. `investment-agent-demo/src/lib/transformers.ts`
   - Normalizes canonical backend payloads into page models.
   - Must fail loudly if canonical decision meaning or canonical summary is missing.
   - Must not synthesize recommendation meaning locally.
   - Must not reintroduce fallback `summary_line`, `promotion_state`, or action semantics outside canonical fields.

3. `investment-agent-demo/src/components/...`
   - Renderer only.
   - Components may group, format, and disclose.
   - Components may not reinterpret recommendation, trust, or action boundaries.

## Git roots and commit boundaries

1. Main framework repo
   - Git root: `/Users/huwenyihendrik/Projects/investment-agent`
   - Owns backend, shared contract, and framework docs.

2. Official frontend repo
   - Git root: `/Users/huwenyihendrik/Projects/investment-agent-demo`
   - Owns the renderer-only official frontend path.

Do not commit official frontend changes from the home-directory git root.

## Removed historical paths

These paths were retired from the repo and are not authoritative:

- `investment-agent/frontend/`
- `investment-agent/frontend_snapshots/`

## Read-only historical paths

These paths may still describe stale semantic shapes, but they are not authoritative:

- `docs/audits/`

## Retired semantic owners

- `backend/app/services/blueprint_candidate_explanation_fact_pack.py`
- `backend/app/services/blueprint_candidate_explanation_formatter.py`
- `backend/app/services/blueprint_candidate_explanation_schema.py`
- `backend/app/services/blueprint_candidate_explanation_validator.py`
- `backend/app/services/blueprint_candidate_explanation_writer.py`
- `backend/app/services/blueprint_thesis.py`
- `backend/app/services/blueprint_thesis_sections.py`
- `backend/app/services/blueprint_legacy_adapter.py`
- `investment-agent-demo/src/lib/mockData.ts`
- `investment-agent-demo/src/components/blueprint/thesis/*`
- `investment-agent-demo/src/components/patterns/DataSourceBadge.tsx`
- `investment-agent-demo/src/components/patterns/ExplanationGrid.tsx`
- `investment-agent-demo/src/components/patterns/BoundaryStatement.tsx`

## Future contributor rule

If a change needs investor-facing recommendation meaning, it must go through:

`framework_constitution.py` -> `blueprint_pipeline.py` -> `blueprint_canonical_decision.py`

Do not add new semantic owners in payload assembly or frontend render code.

## Lens-specific guardrails

1. Lens outputs must stay deterministic and rule-bounded.
2. No lens may create or upgrade a recommendation state.
3. `implementation_reality` and `fragility_red_team` may only preserve or downgrade.
4. `marks_cycle_risk` and `dalio_regime_transmission` are explanatory and review-intensity lenses only.
5. `lens_review_set` must not be reintroduced in the official path.

`lens_review_set` has now been removed from the official live payload path and exists only as an anti-drift test assertion.
