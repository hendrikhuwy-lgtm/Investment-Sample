# V2 Source Tier Table

Reference classification: `docs/v2_mcp_rationalization.md`

| Tier | Sources | When required | Rationale |
| --- | --- | --- | --- |
| `Tier 1A` | Issuer factsheets and filings, benchmark truth, primary price/history providers | Required for the Blueprint first slice and any run that emits investor-visible instrument truth | These sources establish the minimum truth backbone for instrument identity, benchmark mapping, and price history. |
| `Tier 1B` | News feed, official macro/policy releases, finance event feeds | Required for Daily Brief runs and any short-horizon monitoring surface | These sources anchor event and policy commentary to current, citeable upstreams. |
| `Tier 2` | Holdings pricing, additional benchmarks, ETF metadata, broader macro and positioning data | Required for portfolio-aware surfaces, regime expansion, and secondary validation | These sources broaden coverage after the core truth spine is stable. |
| `Tier 3` | Richer market-data and factor/risk providers | Required only for compare/changes, advanced diagnostics, and later analytics | These providers deepen analysis quality but should not block the first V2 slice. |
| `Tier 4 Optional` | Premium wire services, research feeds, supplementary sentiment | Never required for the base truth path | These feeds are additive and must not become single points of failure. |
| `Migration-only` | Generic MCP tooling, discovery, publishing, and support-sidecars | Kept only while the current platform still depends on them operationally | These connectors may assist migration or operations but are not V2 truth sources. |
| `Demote` | Noisy, irrelevant, test-only, or non-truth-grade connectors | Never required | These connectors should not influence investor-visible product truth. |
