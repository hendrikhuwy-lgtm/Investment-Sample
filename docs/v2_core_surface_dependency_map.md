# V2 Core Surface Dependency Map

Audit date: 2026-04-01

Note: `frontend/src/pages/blueprint_candidate_report.tsx` is not present in this branch. The section below records that absence and the nearby orphaned candidate-report renderer context without inventing a replacement page.

## Blueprint Explorer (blueprint.tsx)

### API Calls
| Endpoint | Method | Fields consumed | Legacy family |
|---|---|---|---|
| `/api/blueprints` | `GET` | Wrapper returns `items`; page uses `registry.length` and passes registry rows through to `BlueprintAdminPanel` without page-level field destructuring. | other (`/api/blueprints/*`) |
| `/api/blueprints/current/comparison` | `GET` | `blueprint.version`; `blueprint.sleeves[].sleeve_id`, `sleeve_key`, `sleeve_name`; `comparison_rows[].sleeve_key`, `sleeve_name`, `current_weight`, `target_weight`, `band_min`, `band_max`, `benchmark_reference`, `rebalance_candidate`, `breach_severity`, `rebalance_diagnostics.summary`; `summary.benchmark_gap_count`. | other (`/api/blueprints/*`) |
| `/api/platform/portfolio-blueprint` | `GET` | `blueprint_meta.version`, `base_currency`, `profile_type`, `data_quality.*`, `verification_summary.*`, `truth_summary.*`, `refresh_monitor.*`, `replacement_opportunities[]`, `citations[]`; `sleeves[].sleeve_key`, `name`, `purpose`, `recommendation.*`, `candidates[]`; candidate renderer consumes many nested fields including `symbol`, `name`, `domicile`, `accumulation_or_distribution`, `expense_ratio`, `source_state`, `freshness_state`, `benchmark_assignment.*`, `decision_state.*`, `investment_quality.*`, `investment_lens.*`, `detail_explanation.*`, `citations[]`. | `/api/platform/*` |
| `/api/blueprints` | `POST` | Response ignored; page reloads registry/comparison/payload after draft creation. | other (`/api/blueprints/*`) |
| `/api/blueprints/{blueprintId}/activate` | `POST` | Response ignored; page reloads registry/comparison/payload after activation. | other (`/api/blueprints/*`) |
| `/api/platform/portfolio-blueprint/history?limit={limit}` | `GET` | `items[].snapshot_id`, `created_at`, `note`, `actor_id`, `blueprint_hash`; also seeds selected snapshot IDs. Conditional inside implementation history drawer. | `/api/platform/*` |
| `/api/platform/portfolio-blueprint/recommendation-history?limit={limit}` | `GET` | `items[].event_id`, `created_at`, `sleeve_key`, `candidate_symbol`, `prior_rank`, `new_rank`, `prior_badge`, `new_badge`, `score_version`, `detail.change_driver.driver`, `detail.change_driver.changed_dimensions[]`, `detail.recommendation_diff.what_changed[]`. Conditional inside implementation history drawer. | `/api/platform/*` |
| `/api/platform/portfolio-blueprint/comparison-history?limit={limit}` | `GET` | `items[].comparison_snapshot_id`, `created_at`, `sleeve_key`, `candidate_symbols[]`; also passed to `CandidateComparisonDrawer`. Conditional inside implementation history drawer. | `/api/platform/*` |
| `/api/platform/portfolio-blueprint/citation-health?force_refresh=1` | `GET` | No response fields consumed directly; used as a refresh side effect before reloading `/api/platform/portfolio-blueprint`. | `/api/platform/*` |
| `/api/platform/portfolio-blueprint/decisions` | `POST` | Request payload uses `sleeve_key`, `candidate_symbol`, `status`, `note`, `actor_id`, `override_reason`; response ignored and surface reloads. | `/api/platform/*` |
| `/api/platform/portfolio-blueprint/history` | `POST` | Request payload uses `note`, `actor_id`; response ignored and history reloads. | `/api/platform/*` |
| `/api/platform/portfolio-blueprint/history/{snapshotId}` | `GET` | `created_at`, `payload.blueprint_meta.version`, `payload.sleeves.length`, `decision_artifacts.length`. Conditional inside implementation history drawer. | `/api/platform/*` |
| `/api/platform/portfolio-blueprint/history/diff?snapshot_a=...&snapshot_b=...` | `GET` | `snapshot_a.created_at`, `snapshot_b.created_at`; `diff.added_candidates[]`, `removed_candidates[]`, `weight_range_changes[].sleeve_key/before/after`, `policy_changes[]`, `verification_status_changes[].candidate/before/after`, `risk_control_changes[].candidate/risk_control_status.before/after/liquidity_status.before/after`, `recommendation_changes[].candidate/rank.before/after/recommendation_state.before/after`, `sleeve_pick_changes[].sleeve_key/before/after`. Conditional inside implementation history drawer. | `/api/platform/*` |
| `/api/platform/portfolio-blueprint/compare-candidates` | `POST` | Request payload uses `sleeve_key`, `candidate_symbols[]`; response consumes `comparison.rows[]`, `comparison.summary`, `comparison_snapshot_id`. Conditional when 2+ candidates are selected for compare. | `/api/platform/*` |

### Stable IDs
- `candidate_id`: inconsistent. Present only in nested candidate subobjects such as `decision_record`, `approval_memo`, `rejection_memo`, `decision_thesis`; renderer state keys candidates by `symbol` and local `${sleeve_key}::${symbol}` strings.
- `sleeve_id`: inconsistent. Present in `comparison.blueprint.sleeves[]`, but page/renderers mostly use `sleeve_key`.
- `report_id`: not found.
- `report_tab_id`: not found.
- `section_id`: not found.

### Semantic Reconstruction Found
Yes. `blueprint.tsx` computes `sectionLabel`, `truthSummaryLabel`, and load orchestration by section. `BlueprintOverviewPanel` aggregates benchmark/readiness/pressure counts from raw candidate arrays. `PortfolioBlueprint.tsx` reconstructs verification state, trust labels, source-state language, local eligibility filters, comparison tables, history summaries, and candidate semantics from raw payload objects instead of consuming pre-shaped renderer data.

### V2 Replacement
Route: `/api/v2/surfaces/blueprint/explorer`

## Candidate Report (blueprint_candidate_report.tsx)

### API Calls
| Endpoint | Method | Fields consumed | Legacy family |
|---|---|---|---|
| File missing in branch: `frontend/src/pages/blueprint_candidate_report.tsx` | `n/a` | No page file exists to map. No route in `frontend/src/main.tsx` points to a candidate-report page. Related orphan renderer context exists in `frontend/src/components/portfolio/CandidateReportContent.tsx`, but it makes no direct `/api/*` calls. | `n/a` |

### Stable IDs
- `candidate_id`: inconsistent in related renderer context. Optional nested candidate fields exist, but `CandidateReportContent` keys the surface by `candidate.symbol`.
- `sleeve_id`: not found in related renderer context; it uses sleeve labels/`sleeve_key`-derived text instead.
- `report_id`: not found.
- `report_tab_id`: not found.
- `section_id`: not found. The related renderer uses hard-coded anchor keys such as `why-this-fund`, `key-facts`, and `performance-path`.

### Semantic Reconstruction Found
Yes. Although the requested page file is missing, the nearby orphan renderer `CandidateReportContent.tsx` and `candidatePresentation.ts` build investor-facing narratives, benchmark explanations, cost realism text, decision-change conditions, lens reviews, and peer-comparison judgments from raw `PortfolioBlueprintCandidate` objects rather than consuming a contract-shaped candidate-report surface.

### V2 Replacement
Route: `/api/v2/surfaces/blueprint/candidate-report`

## Daily Brief (daily_brief.tsx)

### API Calls
| Endpoint | Method | Fields consumed | Legacy family |
|---|---|---|---|
| `/api/daily-brief/latest?brief_mode=...&audience_preset=...` | `GET` | `brief_run_id` is used to seed the selected run and drive follow-on detail/chart/IPS/replay fetches. | `/api/daily-brief/*` |
| `/api/daily-brief/history?limit=80` | `GET` | `history[].brief_run_id`, `generated_at`, `delivery_state`, `brief_mode`, `audience_preset`, `status`, `summary`. | `/api/daily-brief/*` |
| `/api/daily-brief/analytics` | `GET` | `total_runs`, `by_mode`, `by_audience`, `delivery_states`. | `/api/daily-brief/*` |
| `/api/daily-brief/scenarios` | `GET` | `generated_at`; `scenarios[].scenario_id`, `scenario_name/name`, `status/scenario_version`, `confidence_rating/confidence_label`, `active_version.probability_weight`, `last_reviewed_at`, `source_rationale/policy_notes/reviewed_by`; `stress_history.length`; `comparison_history.length`. | `/api/daily-brief/*` |
| `/api/benchmarks/current` | `GET` | `benchmark_name`, `version`, `assumption_date`, `rationale`, `components[].component_key/component_name`, `weight`, `rationale`. | other |
| `/api/cma/current` | `GET` | `version`, `assumptions[].cma_id/sleeve_key`, `sleeve_name`, `expected_return_min`, `expected_return_max`, `worst_year_loss_min`, `worst_year_loss_max`, `confidence_label`. | other |
| `/api/daily-brief/policy-status` | `GET` | `latest_brief_run_id`, `trust_banner.label/guidance_ready`, `policy_truth_state`, `policy_labels[]`, `policy_citation_health.overall_status/sourced_count`, `assumptions[]`, `benchmark_profiles[]`, `stress_methodologies[]`, `regime_methodology.items[]`. | `/api/daily-brief/*` |
| `/api/daily-brief/methodology/regime` | `GET` | `version`, `items[]`; merged into `policyStatus.regime_methodology` in page code. | `/api/daily-brief/*` |
| `/api/daily-brief/{briefRunId}` | `GET` | `brief_run_id`, `summary`, `brief_mode`, `audience_preset`, `delivery_state`, `generated_at`, `approval_required`, `approval.*`, `ack_state.ack_state`, `engagement_summary.variant_label/sections[]`, `versions.*`, `history_compare.changes[]`, `scenario_comparison_history[].scenario_id/current_impact_pct/impact_delta_pct`, `artifact_links.html_path/md_path`, `ips_snapshot.ips_snapshot_id/payload`, `policy_pack.*`. | `/api/daily-brief/*` |
| `/api/daily-brief/{briefRunId}/charts` | `GET` | `charts[].chart_key`, `title`, `source_as_of`, `freshness_note`, `svg`. | `/api/daily-brief/*` |
| `/api/daily-brief/{briefRunId}/ips` | `GET` | `payload.profile_id`, `owner_label`, `risk_tolerance`, `time_horizon_years`, `objectives`, `liquidity_needs`, `tax_context`, `benchmark.benchmark_name`, `caveat`, `constraints`, `target_allocation[].sleeve/target_weight/min_band/max_band/calendar_rebalance_frequency/rebalance_priority`, `rebalancing_rules.calendar_review_cadence/small_trade_threshold/tax_aware_note`, `cma_version`. | `/api/daily-brief/*` |
| `/api/daily-brief/{briefRunId}/replay` | `GET` | `brief_run_id`, `generated_at`, `summary`, `delivery_state`, `policy_versions.*`, `state_capture.regime_state.*`, `state_capture.signal_methodology.version`, `state_capture.implication_summary[]`, `state_capture.action_guidance_summary[]`, `state_capture.series_state.*.latest_value/change_5obs`. | `/api/daily-brief/*` |
| `/api/daily-brief/{briefRunId}/review?...` | `POST` | Response fields are not rendered directly; page refetches run detail and analytics after review. | `/api/daily-brief/*` |
| `/api/daily-brief/{briefRunId}/approve?...` | `POST` | Response fields are not rendered directly; page refetches run detail and analytics after approval. | `/api/daily-brief/*` |
| `/api/daily-brief/{briefRunId}/reject?...` | `POST` | Response fields are not rendered directly; page refetches run detail and analytics after rejection. | `/api/daily-brief/*` |
| `/api/daily-brief/{briefRunId}/acknowledge?...` | `POST` | Response fields are not rendered directly; page refetches run detail and analytics after acknowledge/archive. | `/api/daily-brief/*` |
| `/api/daily-brief/policy-bootstrap` | `POST` | `policy_status` is consumed; `status`, `actor`, `result` are ignored in page render. | `/api/daily-brief/*` |
| `/api/daily-brief/policy-pack/import` | `POST` | `policy_status` is consumed; `status`, `result` are ignored in page render. | `/api/daily-brief/*` |
| `/api/daily-brief/policy-observation` | `POST` | Request payload uses `assumption_key`, `assumption_family`, `value_json`, provenance/source metadata; response ignored and page refetches policy status/analytics/replay. | `/api/daily-brief/*` |
| `/api/daily-brief/benchmark-profile` | `POST` | Request payload uses `profile_key`, `sleeve_key`, `target_weight`, weight bands, provenance/source metadata; response ignored and page refetches policy status/analytics/replay. | `/api/daily-brief/*` |
| `/api/daily-brief/stress-methodology` | `POST` | Request payload uses `scenario_key`, `shock_definition_json`, provenance/source metadata; response ignored and page refetches policy status/analytics/replay. | `/api/daily-brief/*` |
| `/api/daily-brief/methodology/regime/{metricKey}` | `PATCH` | Request payload uses `metric_key`, thresholds, methodology/source metadata; response ignored and page refetches policy status/analytics/replay. | `/api/daily-brief/*` |

### Stable IDs
- `candidate_id`: not found.
- `sleeve_id`: not found. Operator payloads use `sleeve_key`, not `sleeve_id`.
- `report_id`: not found. Primary run identity is `brief_run_id`.
- `report_tab_id`: not found.
- `section_id`: not found.

### Semantic Reconstruction Found
Yes. The page merges `/policy-status` with `/methodology/regime`, filters history client-side by cadence/audience, and uses many helper panels and utility functions (`asRecord`, `asArray`, `friendlyKey`, `formatBand`, `truthyLabel`, `textValue`) to translate raw generic objects into narrative cards, tables, and labels.

### V2 Replacement
Route: `/api/v2/surfaces/daily-brief`

## Portfolio (portfolio.tsx)

### API Calls
| Endpoint | Method | Fields consumed | Legacy family |
|---|---|---|---|
| `/api/portfolio/control[?account_id=...]` | `GET` | `summary.position_count`, `total_value`, `holdings_as_of_date`, `price_as_of_date`, `stale_price_count`, `mapping_issue_count`, `top_positions[]`; `delta.summary.new_count/exited_count`; `delta.new_positions[]`, `exited_positions[]`, `increased_positions[]`, `reduced_positions[]`; full payload also passed to child panels. | `/api/portfolio/*` |
| `/api/blueprints/current/comparison[?account_id=...]` | `GET` | `comparison_rows[].sleeve_key`, `sleeve_name`, `current_weight`, `target_weight`, `band_min`, `band_max`, `rebalance_candidate`, `breach_severity`; full payload also passed to `BenchmarkContextStrip`. | other (`/api/blueprints/*`) |
| `/api/portfolio/exposures/latest[?account_id=...]` | `GET` | No page-level destructuring; full `ExposureSection` is passed to exposure summary and currency panels. | `/api/portfolio/*` |
| `/api/accounts` | `GET` | No page-level destructuring; full `AccountSummary[]` is passed to `AccountSelector` and `AccountExposurePanel`. | other |
| `/api/portfolio/uploads?include_deleted=0|1` | `GET` | `uploads`, `active_run_id`. | `/api/portfolio/*` |
| `/api/portfolio/limit-breaches/latest[?account_id=...]` | `GET` | `summary.breach_count`, `summary.warning_count`; full payload also passed to `ExposureSummaryPanel`. | `/api/portfolio/*` |
| `/api/limits/current` | `GET` | `profiles[]`. | other |
| `/api/portfolio/change-attribution/latest[?account_id=...]` | `GET` | No page-level destructuring; full payload is passed to `ChangeAttributionPanel`. | `/api/portfolio/*` |
| `/api/portfolio/dimensions/latest[?account_id=...]` | `GET` | No page-level destructuring; full payload is passed to region/sector/factor panels. | `/api/portfolio/*` |
| `/api/stress/liquidity[?account_id=...]` | `GET` | No page-level destructuring; full payload is passed to `LiquiditySummaryPanel`. | other |
| `/api/reviews[?account_id=...]` | `GET` | `items[].review_id`, `severity`, `category`, `notes`; page displays first six items. | other |
| `/api/portfolio/uploads/{runId}` | `GET` | No page-level destructuring; full `PortfolioUploadDetail` is passed to `PortfolioImportDetailModal`. | `/api/portfolio/*` |
| `/api/portfolio/holdings/import-csv` | `POST` | `created_count`, `errors[]`, `run_id`; page uses `errors[0]`, `errors.length`, and `run_id` for status/error messaging. | `/api/portfolio/*` |
| `/api/portfolio/uploads/{runId}/activate` | `POST` | Response fields not rendered directly; page refetches monitoring data. | `/api/portfolio/*` |
| `/api/portfolio/uploads/{runId}?reason=...` | `DELETE` | Response fields not rendered directly; page refetches monitoring data. | `/api/portfolio/*` |

### Stable IDs
- `candidate_id`: not found.
- `sleeve_id`: inconsistent. Available in the blueprint comparison type, but portfolio rendering keys rows by `sleeve_key` and other portfolio records by `run_id`, `snapshot_id`, `review_id`, `breach_id`, and `limit_id`.
- `report_id`: not found.
- `report_tab_id`: not found.
- `section_id`: not found.

### Semantic Reconstruction Found
Yes. The page assembles a two-stage monitoring model (`refreshMonitoringPrimary` and `refreshMonitoringSecondary`), computes upload/load warnings from partial failures, formats snapshot headers and currency strings, falls back through `normalized_symbol || security_name || security_key` in `ChangeBucket`, and combines unrelated truth sources (portfolio, blueprint comparison, limits, reviews, liquidity) into one renderer.

### V2 Replacement
Route: `/api/v2/surfaces/portfolio`

## Shared Legacy Utilities in `frontend/src/lib/api.ts`

- `requestPlatform(path, fallback?, init?)`: shared wrapper for most audited legacy reads/writes, including `/api/platform/*`, `/api/daily-brief/*`, `/api/portfolio/*`, `/api/blueprints/*`, `/api/reviews`, `/api/limits/current`, `/api/accounts`, `/api/benchmarks/current`, `/api/cma/current`, and `/api/stress/liquidity`.
- `request(path, init?, fallback?)`: still wraps legacy non-`requestPlatform` calls used by the audited scope, notably `importHoldingsCsv()` -> `/api/portfolio/holdings/import-csv`.
- `withAccount(path, accountId?)`: appends `account_id` query parameters to legacy account-scoped routes in portfolio and blueprint surfaces.
