# V2 Source Freshness Policy

## Purpose

Layer 1 must expose freshness as data, not as an implicit caller-side convention. The registry attaches a `FreshnessPolicy` to each source definition, and translators evaluate freshness into one of:

- `fresh`
- `aging`
- `stale`
- `expired`
- `unknown`
- `not_applicable`

## Rules

- Use `observed_at` when the upstream payload contains a market or publication timestamp.
- Fall back to `retrieved_at` when only ingest time is available.
- `fresh` means the source is within the preferred operating window.
- `aging` means the source is still usable but should not be silently treated as current.
- `stale` means the source should be demoted or clearly disclosed to downstream layers.
- `expired` means the source should not anchor a product decision without an explicit fallback path.
- `unknown` means the adapter could not recover a timestamp and the caller should assume degraded trust.

## Milestone 1 Policies

- Blueprint registry and benchmark mapping data can age in days to weeks.
- ETF factsheet and holdings proofs can age in weeks to months.
- Blueprint market snapshots should usually remain within a few days.
- Daily Brief provider context should usually remain intraday and should expire within a few days.
- Portfolio holdings and portfolio snapshots are internal sources and should refresh on the user’s working cadence, not on exchange cadence.

## Enforcement

- Registry definitions own the source-specific windows.
- Translator helpers compute `FreshnessEvaluation` for every emitted `SourceRecord`.
- Higher layers should branch on `record.freshness.state`, not by re-implementing timestamp math.

