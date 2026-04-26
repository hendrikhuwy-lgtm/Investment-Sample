# V2 Freshness And Run State Policy

## Freshness classes

1. `fresh_full_rebuild`
   A full source fetch and recomputation completed in the current run.
2. `fresh_partial_rebuild`
   A subset of required sources was refreshed in the current run and the remaining dependencies were reused from still-valid cache.
3. `stored_valid_context`
   No new fetch was required because the stored context remained inside the approved freshness window and the prior run completed successfully.
4. `degraded_monitoring_mode`
   A required source degraded or became unavailable, a fallback path is active, and the investor must be informed that freshness or completeness is reduced.
5. `execution_failed_or_incomplete`
   The run did not finish correctly; stored context from that run may not be reused as investor-visible truth.

## Per-tier freshness expectations

| Tier | Freshness expectation |
| --- | --- |
| `Tier 1A` | Rebuild on each Blueprint run when the relevant market day or issuer context has changed. Reuse is allowed only within a valid window: issuer facts up to 24 hours, benchmark mappings up to 7 days, end-of-day price/history up to the latest completed market close. |
| `Tier 1B` | Rebuild on each Daily Brief run. Official policy releases and event feeds should normally be no older than 15 to 60 minutes during active monitoring windows; same-day reuse is acceptable only when the prior run completed successfully and the source TTL has not expired. |
| `Tier 2` | Rebuild when a portfolio-aware surface needs the data and the relevant TTL has expired. Holdings pricing should normally be refreshed intraday or at least once per market day; macro or metadata feeds may reuse 1 to 30 day context depending on source cadence. |
| `Tier 3` | Rebuild on demand for advanced diagnostics and compare/change workflows. Reuse within 1 to 7 days is acceptable if the use case is analytical rather than investor-operational. |
| `Tier 4 Optional` | Opportunistic refresh only. Optional feeds may be stale or absent without blocking the run, but must never be labeled as fresh if the TTL is expired. |

## Rebuild vs reuse rules

- A run qualifies for `fresh_full_rebuild` only when every required source for that surface was fetched or recomputed in the current execution.
- A run qualifies for `fresh_partial_rebuild` only when reused dependencies come from a previously successful run and remain inside the approved TTL.
- `stored_valid_context` is allowed only when:
  - the prior run completed successfully,
  - the source TTL is still valid,
  - no higher-priority upstream change has been detected,
  - and the surface contract does not require same-run recomputation.
- Any state produced by an `execution_failed_or_incomplete` run is ineligible for reuse as investor-visible truth.
- Required tiers may not fall back to uncited web scraping or noisy aggregation simply to preserve a green status.

## Degraded-mode behavior

- `degraded_monitoring_mode` is valid only when a defined fallback exists and the surface can still communicate bounded, truthful output.
- When degraded mode is active, the product must expose that condition to the investor or operator instead of silently presenting the surface as fully fresh.
- Tier 1A and Tier 1B degraded mode should prefer prior valid context plus explicit staleness markers over speculative replacement sources.
- Tier 2 degraded mode may use secondary providers when mappings and semantics are already validated by the anti-corruption translator layer.
- Tier 3 and Tier 4 failures should normally suppress optional enrichments rather than downgrade the whole run unless those enrichments were explicitly requested.
