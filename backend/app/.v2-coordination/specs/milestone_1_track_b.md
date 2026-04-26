# Track B Milestone 1

This milestone establishes the Track B-owned V2 package boundary for layers 2 through 8.

Implemented scope:
- `backend/app/v2/core/domain_objects.py` defines the required truth, interpretation, policy, and change objects.
- `backend/app/v2/doctrine/` contains the milestone-1 doctrine corpus, principle registry, evaluator, and explanation renderer.
- `backend/app/v2/core/interpretation_engine.py`, `mandate_rubric.py`, `holdings_overlay.py`, and `change_ledger.py` provide the first orchestration layer.
- `backend/app/v2/surfaces/*/contract_builder.py` emits backend-owned product contracts for `portfolio_overview` and `compare`.
- `backend/app/v2/router.py` exposes thin `/api/v2` demo routes and is mounted in `backend/app/main.py`.
- `shared/v2_ids.ts` and `shared/v2_surface_contracts.ts` define the Track B-owned frontend contract boundary.
- `backend/app/.v2-coordination/fixtures/` holds canonical JSON fixtures for the initial surfaces.

Milestone 1 intentionally leaves several areas stubbed:
- The doctrine engine constrains conviction but does not yet ingest the full donor framework output set.
- `ChangeLedger` is in-memory only.
- The router returns demo contracts instead of live database-backed payloads.

Guardrails:
- No V2 module imports `blueprint_payload_assembler`, `cortex_blueprint_presentation`, or legacy `/api/platform/*` and `/api/cortex/*` handlers.
- Existing donor services are wrapped or translated only through Track B-owned modules.

