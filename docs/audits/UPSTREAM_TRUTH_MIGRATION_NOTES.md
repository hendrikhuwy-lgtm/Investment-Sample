# Upstream Truth Migration Notes

## Preserved Systems

- `source_truth_registry.py`
- `provider_registry.py`
- `provider_activation.py`
- `candidate_field_observations`
- `candidate_field_current`
- `candidate_completeness_snapshots`
- existing `direct_holdings` / `factsheet_summary` / `html_summary` / `structure-first` distinctions

## Repaired Systems

- ETF source sync now persists explicit parser routing into `etf_data_sources.parser_type`
- priority ETF source entries now carry explicit parser/type + truth-family purpose metadata
- `get_etf_holdings_profile()` now preserves:
  - `direct_holdings_available`
  - `summary_support_available`
  - `best_available_source_class`
  - `directness_class`
  - `authority_class`
  - `fallback_state`
  - `structural_limitations`
  - `downgrade_reasons`
- Blueprint payloads now carry canonical upstream truth fields:
  - `source_directness`
  - `coverage_class`
  - `authority_class`
  - `fallback_state`
  - `claim_limit_class`
  - `evidence_density_class`
  - `upstream_truth_contract`
- Daily Brief payloads now carry canonical run-mode fields:
  - `run_mode`
  - `holdings_mapping_directness`
  - `source_density_class`
  - `chart_evidence_state`
  - `portfolio_relevance_basis`
  - `action_authority_class`
  - `run_mode_downgrade_reasons`
- Blueprint provider refresh breadth now covers the active decision set rather than the first 4 symbols only
- doc-registry parity repaired for `A35` with curated HTML factsheet support
- source-state de-flattening now preserves differentiated investor-facing states such as:
  - `source_validated_issuer_structured_summary_backed`
  - `source_validated_html_summary_backed`
  - `source_validated_proxy_backed`
  - `source_validated_structure_first`
- parity is no longer report-only:
  - fatal parity gaps for decision-relevant symbols now raise at Blueprint build / refresh entry points
  - optional or placeholder gaps are downgraded with explicit severity instead of passing silently
- Blueprint truth summaries now preserve both:
  - base `source_state_counts` for compatibility and governance rollups
  - precise `display_source_state_counts` for directness-aware diagnostics
- legacy Blueprint explanation compatibility shapes are now recursively claim-bounded, not only their top-level summary strings
- Daily Brief payloads now enforce run-mode claim boundaries mechanically across the final reader payload
- SGX `A35` market-data handling is now explicit support-only runtime behavior rather than an ambiguous stub
- health-report classification now separates:
  - directness weakness
  - coverage weakness
  - freshness weakness
  - structural limitations
- health-report parity enforcement no longer relies on a shadowed helper definition
- structure-first products are now treated as healthy when their structure-first evidence is intact, instead of being penalized for missing fake holdings completeness

## Downgraded Authorities

- summary-backed holdings truth is no longer treated as equivalent to direct holdings
- bootstrap Daily Brief runs are now explicitly `bootstrap_review_only`
- SGX `A35` market-data support remains partial and is labeled through `sgx_market_stub_not_implemented`
- proxy or fallback-driven paths now feed `claim_limit_class` rather than disappearing into generic “validated” language
- proxy benchmark support now constrains claim limits instead of being allowed to read like fully direct comparative support
- text-fallback and proxy-mapped Daily Brief runs now clamp action language in the final payload

## Structurally Weak Truths Accepted For Now

- `HMCH`
- `XCHA`
- `SGLN`
- `A35`
- SGX live market-data path for Singapore-listed ETF microstructure
- ETF microstructure families without a robust public direct provider
- benchmark-history sleeves that remain proxy-supported

These remain usable only with explicit claim limits and lower authority.

## Stronger After Completion Pass

- `VEVE`
- `VAGU`
- `BIL`
- `BILS`
- `HMCH`
- `XCHA`
- `SGLN`
- `A35`

These symbols now surface more precise truth states and authority classes even when they remain partial or structurally constrained. Weakness is now explicit rather than flattened behind generic validated language.

## Selective Paid Escalation Candidates

Do not escalate before the current engineering fixes are exhausted. Revisit only if:

- SGX market-data support remains materially decision-blocking after public-source engineering work
- issuer-specific HTML/PDF extraction for Vanguard / SSGA / HSBC / DWS still leaves decision-relevant symbols summary-only
- benchmark-history or ETF microstructure support remains weak enough to suppress investor-safe recommendation confidence after routing and parser fixes

Do not escalate before exhausting:

- issuer-specific Vanguard / SSGA / HSBC / DWS parser upgrades
- SGX public-path engineering and explicit support-only downgrade handling
- benchmark-history proxy calibration
- parity and claim-limit enforcement already completed in code

## Operational Reading

The intended reading order after this migration is:

1. source config and parser routing
2. holdings/factsheet/market raw truth
3. canonical upstream truth contract
4. Blueprint and Daily Brief payload claim limits
5. health report for remediation decisions
