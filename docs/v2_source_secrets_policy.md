# V2 Source Secrets Policy

## Tier 1A

- Free/public providers: `local.sec-edgar/filings`
- Credentialed providers: `local.fmp/market-data`, `local.eodhd/historical-data`
- Rate limit expectation: public filings must use a conservative fair-use profile and explicit user-agent; credentialed APIs default to conservative throttling until contract limits are documented.
- Caching policy: issuer filings/facts `24h`, benchmark mappings `7d`, primary end-of-day price/history `6h` during the trading day and `24h` after market close.
- Failure and fallback policy: if the primary paid provider fails, use a pre-approved secondary truth provider or prior valid context within TTL; otherwise mark the run `degraded_monitoring_mode` and do not synthesize truth from ad hoc scraping.

## Tier 1B

- Free/public providers: `local.central-bank/regulatory-releases`
- Credentialed providers: `local.news-sentiment/financial-news`, `local.reuters/news-wire`, `local.finnhub/market-data`
- Rate limit expectation: official release feeds should be polled lightly; credentialed news and event feeds should respect contract limits with default low-burst polling during market hours.
- Caching policy: official policy releases `6h` on release day then `24h`; finance news and event feeds `15m` to `60m` depending on market session.
- Failure and fallback policy: official macro releases may fall back only to prior official releases within TTL; news feeds may fail over to another approved wire or structured feed, but not to noisy social or broad headline aggregation for investor-visible claims.

## Tier 2

- Free/public providers: `local.ecb/data-api`, `local.world-bank/indicators`, `local.cftc/cot`, public-backed paths through `local.openbb/provider-bridge`
- Credentialed providers: `local.quandl/historical-data`, `local.alpha-vantage/market-data`, `local.tiingo/market-data`, `local.twelve-data/market-data`
- Rate limit expectation: macro and positioning endpoints are generally low-frequency; market-data fallbacks should use provider-specific pacing with batching where possible.
- Caching policy: holdings pricing `15m` to `1d` depending on the surface, ETF metadata `24h`, structural macro indicators `7d` to `30d`, positioning data `1d` to `7d`.
- Failure and fallback policy: prefer secondary approved providers or stored valid context; if holdings pricing exceeds its TTL, portfolio-aware surfaces must show degraded status instead of pretending to be current.

## Tier 3

- Free/public providers: `local.quantconnect/factor-monitor` where public access is sufficient
- Credentialed providers: `local.intrinio/risk-factor-data`, `local.polygon/market-data`
- Rate limit expectation: use contract-aware throttling and batch retrieval; do not allow advanced analytics fetches to starve Tier 1 runs.
- Caching policy: factor and risk datasets `1d`, market microstructure snapshots `5m` to `15m`, richer historical backfills `1d` to `7d`.
- Failure and fallback policy: Tier 3 failures should usually suppress optional analytics only; they should not block Tier 1A or Tier 1B truth publication.

## Tier 4 Optional

- Free/public providers: `local.arxiv/macro-research`, `local.ssrn/macro-research`
- Credentialed providers: `local.bloomberg/news-wire`, `local.alt-sentiment/social-macro`
- Rate limit expectation: respect license constraints and keep polling infrequent unless a surface explicitly requests the feed.
- Caching policy: premium wires `15m` to `60m`, research feeds `7d` to `30d`, sentiment overlays `15m` to `60m`.
- Failure and fallback policy: optional feeds may be dropped without blocking the run; no investor-visible statement may depend solely on Tier 4 data.

## Migration-only

- Free/public providers: generic MCP registry, publishing, infra, and document-processing utilities
- Credentialed providers: any migration connector that later introduces credentials must remain operational-only until separately approved
- Rate limit expectation: no investor-facing SLA or freshness guarantee applies
- Caching policy: operational only; do not mix migration-only caches into product truth state
- Failure and fallback policy: failures may affect tooling or migration workflows, but must not affect V2 truth grading

## Demote

- Free/public providers: test, generic, noisy, or irrelevant connectors
- Credentialed providers: none should receive new source secrets allocation for V2
- Rate limit expectation: no production polling budget should be assigned
- Caching policy: no product caching policy, because these connectors are outside the V2 source spine
- Failure and fallback policy: demoted connectors are not valid fallbacks for investor-visible truth
