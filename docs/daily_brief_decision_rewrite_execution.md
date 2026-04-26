# Daily Brief Decision Rewrite Execution

Scope for this session only:

1. Move `What Matters Now` from ranked market-signal monitoring to backend-owned decision synthesis.
2. Keep the current live route and forecast stack operational.
3. Add richer contract fields so Cortex renders investor-readable explanations without inventing financial meaning.
4. Keep Yahoo/FRED/news truth inputs intact; do not add broad new source waves in this pass.
5. Avoid weak fallbacks and avoid polished-but-false actionability.

Implementation checkpoints:

1. Add daily-brief effect classification and explanation builders.
2. Add decision-driver synthesis and contingent-driver synthesis.
3. Expand the daily-brief contract for richer signal, contingent-driver, and chart-callout fields.
4. Rewire the daily-brief contract builder to use decision relevance instead of raw magnitude ranking alone.
5. Rewire Cortex adapters and rendering to consume the richer backend contract directly.
6. Verify the live `8001` route and targeted tests.
