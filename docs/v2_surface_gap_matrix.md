# Cortex Reference Delta Matrix

This is the Wave 4A working matrix. It is intentionally narrow.

Authority:
- reference: `/Users/huwenyihendrik/Projects/investment-samples/01-cortex.html`
- live target: `frontend-cortex` hydrated by V2

Goal:
- identify the remaining reference deltas
- bind and hydrate immediately after the audit
- keep the Cortex structure fixed while making V2 sections feel live and investor-readable

## Evidence Workspace

| Delta Type | Reference | Live Before Wave 4A | Fix Direction |
| --- | --- | --- | --- |
| Section fidelity | Support map with summary counts, per-object support, documents, mappings, tax assumptions, and gaps | Correct sections exist, but content still reads like a raw workspace/data grid | Keep sections; emit cleaner object families, stronger derived mappings, better document labels, and explicit gap families |
| Hierarchy | Summary -> object support -> documents -> mappings -> tax -> gaps | Same order, but object groups and documents feel too mechanical | Make groups object-first and investor-readable; reduce raw fallback language |
| Backend distortion | Support items feel curated | Claims and documents still echo storage rows and donor placeholders too directly | Normalize labels, derive benchmark/baseline objects, and emit clearer support/gap summaries |
| Degraded dominance | Missing evidence still preserves the map | Degraded text is correct but visually over-dominant | Keep degraded states, but move them into object-level rows instead of letting them define the whole page |
| Truth gap | Documents and mappings feel purposeful | Real claims, benchmark links, and tax assumptions are still thin | Hydrate more from truth + stored workspace; derive missing object links and gap rows |

## Blueprint And Candidate Report

| Delta Type | Reference | Live Before Wave 4A | Fix Direction |
| --- | --- | --- | --- |
| Section fidelity | Sleeve-first capital map with cleaner candidate workflow | Correct workflow exists, but summary chips and candidate detail still feel backend-shaped | Keep sleeve-first structure; shorten top-line chips and sharpen candidate rows |
| Hierarchy | Sleeve priority first, then competition, then report drilldown | Present, but too much summary text competes before the table | Compress top summaries; let the candidate table and active sleeve lead |
| Backend distortion | Candidate detail reads like a decision surface | Forecast/meta text is stacked into too many strings; weights are placeholder-heavy | Derive real sleeve weights when possible and separate main limit vs what changes the view cleanly |
| Report hydration | Report tabs feel differentiated | Tabs exist, but market/scenario/evidence sections still repeat themselves | Emit richer regime windows, clearer baseline context, and stronger evidence/source wording |
| Truth gap | Funding path, incumbent context, and support families feel grounded | Still partial because holdings and baseline truth are thin | Use available portfolio truth for sleeve weights and cleaner baseline summaries |

## Daily Brief

| Delta Type | Reference | Live Before Wave 4A | Fix Direction |
| --- | --- | --- | --- |
| Section fidelity | Tight investor triage surface | Full structure exists, but it still exposes diagnostics too early | Keep all sections; move low-level diagnostics later and reduce chip verbosity |
| Hierarchy | Bottom line -> signals -> what changes the view -> portfolio consequence | Same material exists, but too much contract detail appears up front | Shorten status bar and keep per-signal “why here” local rather than repeating global copy |
| Backend distortion | Signal cards feel investor-ordered | Several rows reuse broad contract strings across every signal | Bind per-signal summaries and local consequence text instead of repeating page-level text |
| Degraded dominance | Degraded mode preserves the page | Correct, but the degraded framing can dominate the first screen | Preserve degraded grammar while making the primary signal stack lead the page |
| Truth gap | Concise but data-rich | Source depth exists, but output still feels diagnostic | Tighten adapter mapping and keep evidence/diagnostics as support sections |

## Portfolio

| Delta Type | Reference | Live Before Wave 4A | Fix Direction |
| --- | --- | --- | --- |
| Section fidelity | Live book with allocation, holdings, health, Blueprint relevance, Daily Brief linkage, upload/sync | Correct sections exist | Keep structure and improve upload/mapping truth presentation |
| Empty state behavior | Empty book still feels intentional | Empty holdings state can dominate the page | Keep empty sections but strengthen upload/sync and mapping messages so the page still feels operational |
| Backend distortion | Upload/sync feels like portfolio control | Current messaging is still a little control-plane shaped | Emit cleaner source state, mapping quality, and recent upload context |
| Truth gap | Holdings and mapping make the page feel alive | Real live holdings/mapping truth may still be absent | Bind the available upload/mapping contract more clearly and preserve typed empty states |

## Notebook

| Delta Type | Reference | Live Before Wave 4A | Fix Direction |
| --- | --- | --- | --- |
| Urgency | Lower than Evidence / Blueprint / Brief / Portfolio | Mostly structurally correct | Patch only if a higher-priority hydration change exposes a continuity gap |

## Implementation Order

1. Evidence Workspace
2. Blueprint and Candidate Report
3. Daily Brief
4. Portfolio
5. Notebook only if continuity requires it
