# Framework Contributor Guardrails

## Official ownership path

Official framework meaning must flow through:

`framework_constitution.py` -> `blueprint_pipeline.py` -> `blueprint_canonical_decision.py`

Official Daily Brief action meaning must flow through:

`daily_brief_execution_contract.py`

Official frontend rendering must flow through:

`shared/canonical_frontend_contract.ts` -> `investment-agent-demo/src/lib/transformers.ts` -> official renderer components

## Hard rules

1. Official frontend surfaces are renderer-only.
2. `transformers.ts` may normalize shape only. It must not invent recommendation meaning.
3. `canonical_decision` is the only official owner of candidate decision meaning.
4. Daily Brief action states are limited to `ignore`, `monitor`, and `review`.
5. Framework lenses are deterministic, bounded, and downgrade-only.
6. Do not reintroduce `detail_explanation`, `thesis_sections`, or `lens_review_set` into the official path.
7. Do not add parallel recommendation states in payload builders, frontend helpers, or components.

## Removed and non-authoritative surfaces

These paths were retired from the repo and are not authoritative for the official framework runtime:

1. `investment-agent/frontend/`
   - historical in-repo frontend path
   - removed after Cortex/V2 became the active runtime
   - do not use it to define official framework semantics

2. `investment-agent/frontend_snapshots/`
   - removed generated UI snapshots
   - did not own current framework semantics

3. `docs/audits/`
   - historical analysis
   - may describe retired owners or pre-upgrade architecture

## Safe commit boundaries

Expected git roots:

1. Main framework repo
   - `/Users/huwenyihendrik/Projects/investment-agent`

2. Official frontend repo
   - `/Users/huwenyihendrik/Projects/investment-agent-demo`

Commit framework changes in the repo that owns the files you touched.
Do not commit frontend framework files from the home-directory git root.

## Deletion conditions

If a deprecated field or helper is no longer referenced by:

1. the canonical backend path
2. the shared frontend contract
3. the official `investment-agent-demo` renderer path

then it should be deleted rather than preserved as a convenience fallback.
