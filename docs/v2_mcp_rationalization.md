# V2 MCP Rationalization

`backend/app/v2/sources/registry.py` is the canonical table for MCP connector tiering. The current inventory contains 45 unique connectors across:

- `mcp/connectors/current_registry_priorities.json`
- `mcp/connectors/financial_intelligence_candidates.json`
- `mcp/registry_snapshot.json`

## Rollout Order

- Tier `1A`: Blueprint slice connectors that materially improve issuer or instrument truth.
- Tier `1B`: Daily Brief connectors needed for operational macro, news, and market context.
- Tier `2`: strong follow-on connectors for redundancy or deeper historical coverage.
- Tier `3`: research and model-adjacent enrichment.
- Tier `4-Optional`: useful but non-core or access-constrained connectors.
- `Migration-only`: temporary workflow or discovery infrastructure kept only during the V2 transition.
- `Demote`: irrelevant, noisy, redundant, or test-only connectors.

## Summary By Tier

- `1A`
  - `local.sec-edgar/filings`
- `1B`
  - `local.ecb/data-api`
  - `local.cftc/cot`
  - `local.central-bank/regulatory-releases`
  - `local.reuters/news-wire`
  - `local.news-sentiment/financial-news`
  - `local.google-news/macro-aggregator`
  - `local.fmp/market-data`
  - `local.finnhub/market-data`
- `2`
  - `local.intrinio/risk-factor-data`
  - `local.quandl/historical-data`
  - `local.eodhd/historical-data`
  - `local.alpha-vantage/market-data`
  - `local.polygon/market-data`
  - `local.tiingo/market-data`
  - `local.twelve-data/market-data`
  - `local.world-bank/indicators`
- `3`
  - `local.quantconnect/factor-monitor`
  - `local.arxiv/macro-research`
  - `local.ssrn/macro-research`
- `4-Optional`
  - `local.bb-browser/browser-discovery`
  - `local.bloomberg/news-wire`
  - `local.alt-sentiment/social-macro`
  - `local.openbb/provider-bridge`
- `Migration-only`
  - `ai.auteng/docs`
  - `ai.auteng/mcp`
  - `ai.com.mcp/registry`
  - `ai.com.mcp/openai-tools`
  - `ai.com.mcp/contabo`
  - `ai.exa/exa`
- `Demote`
  - `ai.aliengiraffe/spotdb`
  - `ai.alpic.test/test-mcp-server`
  - `ai.appdeploy/deploy-app`
  - `ai.autoblocks/contextlayer-mcp`
  - `ai.autoblocks/ctxl`
  - `ai.autoblocks/ctxl-mcp`
  - `ai.cirra/salesforce-mcp`
  - `ai.com.mcp/hapi-mcp`
  - `ai.com.mcp/lenny-rachitsky-podcast`
  - `ai.com.mcp/petstore`
  - `ai.com.mcp/skills-search`
  - `ai.explorium/mcp-explorium`
  - `ai.filegraph/document-processing`
  - `ai.gomarble/mcp-api`
  - `ai.gossiper/shopify-admin-mcp`

## Operating Decision

Track A should only wire Tier `1A` and `1B` during Milestone 1 implementation. Tier `2` and below can be documented and registered without being activated in product flows.

