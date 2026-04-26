PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS schema_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  schema_version INTEGER NOT NULL,
  applied_at TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_meta (id, schema_version, applied_at)
VALUES (1, 0, CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS source_records (
  source_id TEXT NOT NULL,
  url TEXT NOT NULL,
  publisher TEXT NOT NULL,
  published_at TEXT,
  retrieved_at TEXT NOT NULL,
  topic TEXT NOT NULL,
  credibility_tier TEXT NOT NULL,
  raw_hash TEXT NOT NULL,
  source_type TEXT NOT NULL,
  PRIMARY KEY (source_id, retrieved_at)
);

CREATE TABLE IF NOT EXISTS insight_records (
  insight_id TEXT PRIMARY KEY,
  theme TEXT NOT NULL,
  summary TEXT NOT NULL,
  stance TEXT NOT NULL,
  confidence REAL NOT NULL,
  time_horizon TEXT NOT NULL,
  citations_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS portfolio_signals (
  signal_id TEXT PRIMARY KEY,
  metric TEXT NOT NULL,
  value REAL NOT NULL,
  threshold REAL NOT NULL,
  state TEXT NOT NULL,
  portfolio_impact_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tax_profiles (
  profile_id TEXT PRIMARY KEY,
  tax_residency TEXT NOT NULL,
  base_currency TEXT NOT NULL,
  dta_flags_json TEXT NOT NULL,
  estate_risk_flags_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS instrument_tax_profiles (
  instrument_id TEXT PRIMARY KEY,
  domicile TEXT NOT NULL,
  us_dividend_exposure INTEGER NOT NULL,
  expected_withholding_rate REAL NOT NULL,
  us_situs_risk_flag INTEGER NOT NULL,
  expense_ratio REAL NOT NULL,
  liquidity_score REAL NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS convex_sleeve_states (
  date TEXT PRIMARY KEY,
  nav_weight REAL NOT NULL,
  carry_cost_annualized REAL NOT NULL,
  protection_ratio REAL NOT NULL,
  max_loss_known INTEGER NOT NULL,
  margin_required INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alert_events (
  alert_id TEXT PRIMARY KEY,
  severity TEXT NOT NULL,
  trigger_reason TEXT NOT NULL,
  citations_json TEXT NOT NULL,
  sent_channel TEXT NOT NULL,
  ack_state TEXT NOT NULL,
  created_at TEXT NOT NULL,
  run_id TEXT
);

CREATE TABLE IF NOT EXISTS metric_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  asof_ts TEXT NOT NULL,
  run_id TEXT,
  metric_key TEXT,
  metric_id TEXT NOT NULL,
  metric_name TEXT,
  value REAL NOT NULL,
  observed_at TEXT,
  retrieved_at TEXT,
  lag_days INTEGER,
  lag_class TEXT,
  lag_cause TEXT,
  delta_1d REAL,
  window_5_change REAL,
  window_20_change REAL,
  window_60_range_low REAL,
  window_60_range_high REAL,
  percentile_60 REAL,
  prev_percentile_60 REAL,
  percentile_shift REAL,
  stddev_60 REAL,
  state_short TEXT,
  days_in_state_short INTEGER,
  citations_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_metric_snapshots_metric_asof
ON metric_snapshots (metric_id, asof_ts DESC);

CREATE TABLE IF NOT EXISTS series_observations (
  series_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  metric_key TEXT NOT NULL,
  observation_date TEXT NOT NULL,
  observation_value REAL,
  retrieved_at TEXT NOT NULL,
  lag_days INTEGER,
  lag_class TEXT,
  lag_cause TEXT,
  retrieval_succeeded INTEGER,
  raw_hash TEXT NOT NULL,
  PRIMARY KEY (series_id, retrieved_at)
);

CREATE INDEX IF NOT EXISTS ix_series_observations_metric
ON series_observations (metric_key, retrieved_at DESC);

CREATE TABLE IF NOT EXISTS regime_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  asof_ts TEXT NOT NULL,
  horizon TEXT NOT NULL,
  state TEXT NOT NULL,
  days_in_state INTEGER NOT NULL DEFAULT 1,
  confidence REAL NOT NULL,
  contributors_json TEXT NOT NULL,
  citations_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_regime_snapshots_horizon_asof
ON regime_snapshots (horizon, asof_ts DESC);

CREATE TABLE IF NOT EXISTS alert_events_v2 (
  alert_id TEXT PRIMARY KEY,
  asof_ts TEXT NOT NULL,
  horizon TEXT NOT NULL,
  metric_id TEXT NOT NULL,
  prev_state TEXT NOT NULL,
  curr_state TEXT NOT NULL,
  lifecycle TEXT NOT NULL,
  severity INTEGER NOT NULL,
  threshold_name TEXT NOT NULL,
  threshold_value REAL NOT NULL,
  current_value REAL NOT NULL,
  delta_value REAL NOT NULL,
  delta_window TEXT NOT NULL,
  percentile_60 REAL,
  days_in_state INTEGER NOT NULL,
  impact_score REAL NOT NULL,
  impact_map_json TEXT NOT NULL,
  narrative_md TEXT NOT NULL,
  citations_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_alert_events_v2_asof
ON alert_events_v2 (asof_ts DESC);

CREATE INDEX IF NOT EXISTS ix_alert_events_v2_metric
ON alert_events_v2 (metric_id, horizon, asof_ts DESC);

CREATE TABLE IF NOT EXISTS mcp_servers (
  server_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  url TEXT,
  publisher TEXT,
  topic TEXT,
  status TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mcp_server_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  server_id TEXT NOT NULL,
  server_name TEXT NOT NULL,
  endpoint_url TEXT,
  retrieved_at TEXT NOT NULL,
  cached INTEGER NOT NULL,
  raw_hash TEXT NOT NULL,
  snapshot_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mcp_server_capabilities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  server_id TEXT NOT NULL,
  retrieved_at TEXT NOT NULL,
  raw_hash TEXT NOT NULL,
  capabilities_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mcp_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  server_id TEXT NOT NULL,
  mcp_server_id TEXT,
  run_id TEXT,
  item_id TEXT,
  retrieved_at TEXT NOT NULL,
  raw_hash TEXT NOT NULL,
  content_hash TEXT,
  item_type TEXT NOT NULL,
  uri TEXT,
  url TEXT,
  title TEXT,
  published_at TEXT,
  snippet TEXT,
  item_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mcp_connectivity_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  server_id TEXT NOT NULL,
  server_name TEXT NOT NULL,
  endpoint_url TEXT NOT NULL,
  endpoint_type TEXT NOT NULL,
  endpoint_host TEXT,
  endpoint_port INTEGER,
  ip_family_attempted TEXT,
  connectable INTEGER NOT NULL,
  status TEXT NOT NULL,
  error_class TEXT,
  error_detail TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS report_runs (
  run_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  long_state TEXT NOT NULL,
  short_state TEXT NOT NULL,
  summary_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS email_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_date_sgt TEXT NOT NULL,
  run_slot_key TEXT,
  run_slot_label TEXT,
  attempted_at TEXT NOT NULL,
  status TEXT NOT NULL,
  recipient TEXT NOT NULL,
  subject TEXT,
  run_id TEXT,
  md_path TEXT,
  html_path TEXT,
  cached_used INTEGER NOT NULL DEFAULT 0,
  mcp_connected_count INTEGER,
  mcp_total_count INTEGER,
  citations_count INTEGER,
  error TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_email_runs_slot_sent
ON email_runs (run_slot_key)
WHERE status = 'sent' AND run_slot_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS portfolio_holdings (
  holding_id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  name TEXT NOT NULL,
  quantity REAL NOT NULL,
  cost_basis REAL NOT NULL,
  currency TEXT NOT NULL,
  sleeve TEXT NOT NULL,
  account_type TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_upload_runs (
  run_id TEXT PRIMARY KEY,
  uploaded_at TEXT NOT NULL,
  holdings_as_of_date TEXT NOT NULL,
  filename TEXT,
  source_name TEXT,
  status TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 0,
  is_deleted INTEGER NOT NULL DEFAULT 0,
  deleted_at TEXT,
  deleted_reason TEXT,
  snapshot_id TEXT,
  raw_row_count INTEGER NOT NULL DEFAULT 0,
  parsed_row_count INTEGER NOT NULL DEFAULT 0,
  normalized_position_count INTEGER NOT NULL DEFAULT 0,
  total_market_value REAL NOT NULL DEFAULT 0,
  stale_price_count INTEGER NOT NULL DEFAULT 0,
  mapping_issue_count INTEGER NOT NULL DEFAULT 0,
  warning_count INTEGER NOT NULL DEFAULT 0,
  warnings_json TEXT NOT NULL DEFAULT '[]',
  errors_json TEXT NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS ix_portfolio_upload_runs_uploaded_at
ON portfolio_upload_runs (uploaded_at DESC);

CREATE INDEX IF NOT EXISTS ix_portfolio_upload_runs_active
ON portfolio_upload_runs (is_active, is_deleted, uploaded_at DESC);

CREATE TABLE IF NOT EXISTS portfolio_holding_snapshots (
  snapshot_row_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  uploaded_at TEXT NOT NULL,
  holdings_as_of_date TEXT NOT NULL,
  price_as_of_date TEXT,
  account_id TEXT NOT NULL,
  security_key TEXT NOT NULL,
  raw_symbol TEXT NOT NULL,
  normalized_symbol TEXT NOT NULL,
  security_name TEXT NOT NULL,
  asset_type TEXT NOT NULL,
  currency TEXT NOT NULL,
  quantity REAL NOT NULL,
  cost_basis REAL NOT NULL,
  market_price REAL,
  market_value REAL,
  fx_rate_to_base REAL,
  base_currency TEXT NOT NULL DEFAULT 'SGD',
  sleeve TEXT,
  mapping_status TEXT NOT NULL DEFAULT 'unmapped',
  price_source TEXT,
  price_stale INTEGER NOT NULL DEFAULT 0,
  venue TEXT,
  identifier_isin TEXT
);

CREATE INDEX IF NOT EXISTS ix_portfolio_holding_snapshots_run
ON portfolio_holding_snapshots (run_id, account_id, security_key);

CREATE INDEX IF NOT EXISTS ix_portfolio_holding_snapshots_asof
ON portfolio_holding_snapshots (holdings_as_of_date DESC, uploaded_at DESC);

CREATE TABLE IF NOT EXISTS portfolio_mapping_overrides (
  override_id TEXT PRIMARY KEY,
  account_id TEXT,
  security_key TEXT NOT NULL,
  normalized_symbol TEXT,
  target_sleeve TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'manual_override',
  note TEXT,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_portfolio_mapping_override_key
ON portfolio_mapping_overrides (account_id, security_key);

CREATE TABLE IF NOT EXISTS portfolio_mapping_issues (
  issue_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  account_id TEXT,
  security_key TEXT,
  issue_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  detail TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_portfolio_mapping_issues_run
ON portfolio_mapping_issues (run_id, severity);

CREATE TABLE IF NOT EXISTS exposure_snapshots (
  exposure_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  snapshot_id TEXT,
  exposure_type TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  label TEXT NOT NULL,
  market_value REAL NOT NULL DEFAULT 0,
  weight REAL NOT NULL DEFAULT 0,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_exposure_snapshots_run_type
ON exposure_snapshots (run_id, exposure_type, weight DESC);

CREATE TABLE IF NOT EXISTS limit_profiles (
  limit_id TEXT PRIMARY KEY,
  blueprint_id TEXT,
  strategy_id TEXT,
  limit_type TEXT NOT NULL,
  scope TEXT NOT NULL,
  threshold_value REAL NOT NULL,
  warning_threshold REAL,
  breach_severity TEXT NOT NULL DEFAULT 'medium',
  enabled INTEGER NOT NULL DEFAULT 1,
  effective_from TEXT,
  effective_to TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_limit_profiles_scope
ON limit_profiles (COALESCE(blueprint_id, ''), COALESCE(strategy_id, ''), limit_type, scope);

CREATE TABLE IF NOT EXISTS limit_breaches (
  breach_id TEXT PRIMARY KEY,
  limit_id TEXT NOT NULL,
  snapshot_id TEXT,
  run_id TEXT,
  scope_key TEXT,
  label TEXT,
  current_value REAL NOT NULL,
  threshold_value REAL NOT NULL,
  warning_threshold REAL,
  severity TEXT NOT NULL,
  breach_status TEXT NOT NULL,
  first_detected_at TEXT NOT NULL,
  last_detected_at TEXT NOT NULL,
  resolved_at TEXT,
  linked_review_id TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_limit_breaches_open
ON limit_breaches (limit_id, COALESCE(run_id, ''), COALESCE(scope_key, ''), breach_status)
WHERE breach_status IN ('warning', 'breached');

CREATE INDEX IF NOT EXISTS ix_limit_breaches_run
ON limit_breaches (run_id, severity, last_detected_at DESC);

CREATE TABLE IF NOT EXISTS review_events (
  review_event_id TEXT PRIMARY KEY,
  review_id TEXT NOT NULL,
  prior_status TEXT,
  new_status TEXT NOT NULL,
  actor TEXT NOT NULL,
  reason TEXT,
  occurred_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_review_events_review
ON review_events (review_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS escalation_rules (
  rule_id TEXT PRIMARY KEY,
  category TEXT NOT NULL,
  severity TEXT NOT NULL,
  overdue_hours INTEGER,
  persistence_runs INTEGER,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
  audit_event_id TEXT PRIMARY KEY,
  actor TEXT NOT NULL,
  action_type TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_id TEXT,
  before_json TEXT,
  after_json TEXT,
  source_ip TEXT,
  user_agent TEXT,
  occurred_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_audit_events_occurred_at
ON audit_events (occurred_at DESC, action_type);

CREATE TABLE IF NOT EXISTS audit_exports (
  export_id TEXT PRIMARY KEY,
  export_scope TEXT NOT NULL,
  generated_at TEXT NOT NULL,
  generated_by TEXT NOT NULL,
  filters_json TEXT NOT NULL DEFAULT '{}',
  file_path TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS retention_policies (
  policy_id TEXT PRIMARY KEY,
  object_type TEXT NOT NULL,
  retention_days INTEGER NOT NULL,
  soft_delete_only INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staleness_sla_policies (
  policy_id TEXT PRIMARY KEY,
  asset_class TEXT NOT NULL,
  source_type TEXT NOT NULL,
  max_lag_days_warning INTEGER NOT NULL,
  max_lag_days_breach INTEGER NOT NULL,
  nav_blocking INTEGER NOT NULL DEFAULT 0,
  escalation_enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS availability_history (
  history_id TEXT PRIMARY KEY,
  service_domain TEXT NOT NULL,
  status TEXT NOT NULL,
  issue_count INTEGER NOT NULL DEFAULT 0,
  entered_at TEXT NOT NULL,
  exited_at TEXT,
  duration_seconds INTEGER,
  root_cause TEXT,
  run_id TEXT
);

CREATE INDEX IF NOT EXISTS ix_availability_history_domain
ON availability_history (service_domain, entered_at DESC);

CREATE TABLE IF NOT EXISTS change_attribution_records (
  attribution_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  security_key TEXT,
  normalized_symbol TEXT,
  attribution_type TEXT NOT NULL,
  confidence TEXT NOT NULL,
  trade_date TEXT,
  settlement_date TEXT,
  pending_settlement INTEGER NOT NULL DEFAULT 0,
  detail TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_change_attribution_run
ON change_attribution_records (run_id, attribution_type);

CREATE TABLE IF NOT EXISTS blueprint_versions (
  version_id TEXT PRIMARY KEY,
  blueprint_id TEXT NOT NULL,
  version_label TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 0,
  archived_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sub_sleeve_mappings (
  sub_sleeve_id TEXT PRIMARY KEY,
  blueprint_id TEXT NOT NULL,
  parent_sleeve_key TEXT NOT NULL,
  child_sleeve_key TEXT NOT NULL,
  child_sleeve_name TEXT NOT NULL,
  target_weight REAL NOT NULL,
  min_band REAL NOT NULL,
  max_band REAL NOT NULL,
  benchmark_reference TEXT,
  region TEXT,
  sector TEXT,
  factor_hint TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_sub_sleeve_mappings_blueprint
ON sub_sleeve_mappings (blueprint_id, parent_sleeve_key);

CREATE TABLE IF NOT EXISTS liquidity_snapshots (
  liquidity_id TEXT PRIMARY KEY,
  run_id TEXT,
  security_key TEXT,
  normalized_symbol TEXT,
  liquidity_bucket TEXT NOT NULL,
  trading_volume_proxy REAL,
  days_to_exit_proxy REAL,
  confidence_flag TEXT NOT NULL,
  source_name TEXT,
  source_observed_at TEXT,
  provenance_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_liquidity_snapshots_run
ON liquidity_snapshots (run_id, liquidity_bucket);

CREATE TABLE IF NOT EXISTS account_entities (
  account_id TEXT PRIMARY KEY,
  account_name TEXT,
  custodian_name TEXT,
  base_currency TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  account_type TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dashboard_refresh_metadata (
  section_name TEXT PRIMARY KEY,
  last_refreshed_at TEXT,
  refresh_mode TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stress_scenario_history (
  scenario_record_id TEXT PRIMARY KEY,
  as_of_ts TEXT NOT NULL,
  scenario_id TEXT NOT NULL,
  scenario_name TEXT NOT NULL,
  scenario_probability_weight REAL,
  estimated_impact_pct REAL NOT NULL,
  convex_contribution_pct REAL,
  ex_convex_impact_pct REAL,
  scenario_version TEXT NOT NULL DEFAULT '1.0'
);

CREATE INDEX IF NOT EXISTS ix_stress_scenario_history_asof
ON stress_scenario_history (as_of_ts DESC, scenario_id);

CREATE TABLE IF NOT EXISTS security_classifications (
  classification_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  security_key TEXT NOT NULL,
  normalized_symbol TEXT NOT NULL,
  issuer_key TEXT,
  issuer_name TEXT,
  country TEXT,
  region TEXT,
  sector TEXT,
  industry TEXT,
  classification_source TEXT NOT NULL,
  confidence TEXT NOT NULL,
  provenance_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_security_classifications_run_security
ON security_classifications (run_id, security_key);

CREATE TABLE IF NOT EXISTS issuer_classifications (
  issuer_classification_id TEXT PRIMARY KEY,
  issuer_key TEXT NOT NULL,
  issuer_name TEXT,
  country TEXT,
  region TEXT,
  sector TEXT,
  industry TEXT,
  classification_source TEXT NOT NULL,
  confidence TEXT NOT NULL,
  provenance_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_issuer_classifications_key
ON issuer_classifications (issuer_key);

CREATE TABLE IF NOT EXISTS factor_exposure_snapshots (
  factor_snapshot_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  factor_name TEXT NOT NULL,
  exposure_value REAL NOT NULL,
  exposure_type TEXT NOT NULL,
  confidence TEXT NOT NULL,
  provenance_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_factor_exposure_snapshots_run
ON factor_exposure_snapshots (run_id, factor_name);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  snapshot_date TEXT,
  holdings_as_of_date TEXT,
  price_as_of_date TEXT,
  upload_run_id TEXT,
  total_value REAL NOT NULL,
  sleeve_weights_json TEXT NOT NULL,
  concentration_metrics_json TEXT NOT NULL,
  convex_coverage_ratio REAL NOT NULL,
  tax_drag_estimate REAL NOT NULL,
  stale_price_count INTEGER NOT NULL DEFAULT 0,
  mapping_issue_count INTEGER NOT NULL DEFAULT 0,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS daily_logs (
  log_id TEXT PRIMARY KEY,
  run_id TEXT,
  created_at TEXT NOT NULL,
  macro_state_summary TEXT NOT NULL,
  short_term_alert_state TEXT NOT NULL,
  portfolio_snapshot_id TEXT,
  regime_classification TEXT NOT NULL,
  top_risk_flags_json TEXT NOT NULL,
  top_opportunity_flags_json TEXT NOT NULL,
  personal_alignment_score REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS journal_entries (
  entry_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  thesis TEXT NOT NULL,
  concerns TEXT,
  mistakes_avoided TEXT,
  lessons TEXT
);

CREATE TABLE IF NOT EXISTS ips_profile (
  profile_id TEXT PRIMARY KEY,
  payload_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_sleeve_overrides (
  symbol TEXT PRIMARY KEY,
  sleeve TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_price_cache (
  symbol TEXT NOT NULL,
  quote_currency TEXT NOT NULL,
  price REAL NOT NULL,
  as_of TEXT NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY (symbol, as_of)
);

CREATE TABLE IF NOT EXISTS market_price_snapshots (
  price_id TEXT PRIMARY KEY,
  security_key TEXT NOT NULL,
  normalized_symbol TEXT NOT NULL,
  raw_symbol TEXT,
  quote_currency TEXT NOT NULL,
  market_price REAL NOT NULL,
  fx_rate_to_base REAL NOT NULL,
  base_currency TEXT NOT NULL DEFAULT 'SGD',
  source TEXT NOT NULL,
  source_as_of TEXT,
  stale_flag INTEGER NOT NULL DEFAULT 0,
  retrieved_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_market_price_snapshots_symbol
ON market_price_snapshots (security_key, retrieved_at DESC);

CREATE TABLE IF NOT EXISTS fx_rates_cache (
  pair TEXT NOT NULL,
  rate REAL NOT NULL,
  as_of TEXT NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY (pair, as_of)
);

CREATE TABLE IF NOT EXISTS outbox_artifacts (
  artifact_id TEXT PRIMARY KEY,
  run_id TEXT,
  artifact_type TEXT NOT NULL,
  artifact_path TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_outbox_artifacts_run_id
ON outbox_artifacts (run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_outbox_artifacts_created
ON outbox_artifacts (created_at DESC);

CREATE TABLE IF NOT EXISTS blueprints (
  blueprint_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  base_currency TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  benchmark_reference TEXT,
  rebalance_frequency TEXT,
  rebalance_logic TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blueprint_sleeves (
  sleeve_id TEXT PRIMARY KEY,
  blueprint_id TEXT NOT NULL,
  sleeve_key TEXT NOT NULL,
  sleeve_name TEXT NOT NULL,
  target_weight REAL NOT NULL,
  min_band REAL NOT NULL,
  max_band REAL NOT NULL,
  core_satellite TEXT NOT NULL DEFAULT 'core',
  benchmark_reference TEXT,
  notes TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_sleeves_key
ON blueprint_sleeves (blueprint_id, sleeve_key);

CREATE TABLE IF NOT EXISTS blueprint_benchmarks (
  benchmark_id TEXT PRIMARY KEY,
  blueprint_id TEXT NOT NULL,
  sleeve_key TEXT,
  benchmark_name TEXT NOT NULL,
  benchmark_symbol TEXT,
  notes TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blueprint_mapping_rules (
  rule_id TEXT PRIMARY KEY,
  blueprint_id TEXT NOT NULL,
  match_type TEXT NOT NULL,
  match_value TEXT NOT NULL,
  target_sleeve TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 1.0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_blueprint_mapping_rules_match
ON blueprint_mapping_rules (blueprint_id, match_type, match_value);

CREATE TABLE IF NOT EXISTS blueprint_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  blueprint_id TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  note TEXT,
  blueprint_hash TEXT NOT NULL,
  portfolio_settings_hash TEXT NOT NULL,
  candidate_list_hash TEXT NOT NULL,
  sleeve_settings_hash TEXT NOT NULL,
  ips_version TEXT,
  governance_summary_json TEXT,
  market_state_snapshot_json TEXT,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_blueprint_snapshots_blueprint_created
ON blueprint_snapshots (blueprint_id, created_at DESC);

CREATE TABLE IF NOT EXISTS blueprint_decision_artifacts (
  artifact_id TEXT PRIMARY KEY,
  snapshot_id TEXT NOT NULL,
  sleeve_key TEXT,
  candidate_symbol TEXT,
  artifact_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_blueprint_decision_artifacts_snapshot
ON blueprint_decision_artifacts (snapshot_id, artifact_type, sleeve_key, candidate_symbol);

CREATE TABLE IF NOT EXISTS blueprint_runtime_cycles (
  cycle_id TEXT PRIMARY KEY,
  blueprint_id TEXT NOT NULL,
  refresh_run_id TEXT,
  evaluation_mode TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  generated_at TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_blueprint_runtime_cycles_created
ON blueprint_runtime_cycles (created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_runtime_cycles_identity
ON blueprint_runtime_cycles (payload_hash, COALESCE(refresh_run_id, ''), evaluation_mode, COALESCE(generated_at, ''));

CREATE TABLE IF NOT EXISTS blueprint_runtime_cycle_artifacts (
  artifact_id TEXT PRIMARY KEY,
  cycle_id TEXT NOT NULL,
  sleeve_key TEXT,
  candidate_symbol TEXT,
  artifact_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_blueprint_runtime_cycle_artifacts_cycle
ON blueprint_runtime_cycle_artifacts (cycle_id, artifact_type, sleeve_key, candidate_symbol);

CREATE TABLE IF NOT EXISTS sleeve_recommendations (
  recommendation_id TEXT PRIMARY KEY,
  snapshot_id TEXT,
  sleeve_key TEXT NOT NULL,
  our_pick_symbol TEXT,
  top_candidates_json TEXT NOT NULL,
  acceptable_candidates_json TEXT NOT NULL,
  caution_candidates_json TEXT NOT NULL,
  why_this_pick_wins TEXT,
  what_would_change_the_pick TEXT,
  missing_data_json TEXT NOT NULL,
  score_version TEXT NOT NULL,
  as_of_date TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recommendation_events (
  event_id TEXT PRIMARY KEY,
  snapshot_id TEXT,
  sleeve_key TEXT NOT NULL,
  candidate_symbol TEXT NOT NULL,
  prior_rank INTEGER,
  new_rank INTEGER,
  prior_badge TEXT,
  new_badge TEXT,
  score_version TEXT NOT NULL,
  ips_version TEXT,
  governance_summary_json TEXT,
  market_state_snapshot_json TEXT,
  detail_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blueprint_candidate_decisions (
  decision_id TEXT PRIMARY KEY,
  sleeve_key TEXT NOT NULL,
  candidate_symbol TEXT NOT NULL,
  status TEXT NOT NULL,
  note TEXT,
  override_reason TEXT,
  actor_id TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_candidate_decisions_symbol
ON blueprint_candidate_decisions (sleeve_key, candidate_symbol);

CREATE TABLE IF NOT EXISTS blueprint_candidate_decision_events (
  event_id TEXT PRIMARY KEY,
  decision_id TEXT NOT NULL,
  sleeve_key TEXT NOT NULL,
  candidate_symbol TEXT NOT NULL,
  prior_status TEXT,
  new_status TEXT NOT NULL,
  note TEXT,
  score_snapshot_json TEXT,
  recommendation_snapshot_json TEXT,
  governance_summary_json TEXT,
  ips_version TEXT,
  market_state_snapshot_json TEXT,
  actor_id TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_items (
  review_id TEXT PRIMARY KEY,
  review_key TEXT NOT NULL,
  category TEXT NOT NULL,
  severity TEXT NOT NULL,
  account_id TEXT,
  owner TEXT,
  due_date TEXT,
  status TEXT NOT NULL DEFAULT 'open',
  notes TEXT,
  linked_object_type TEXT,
  linked_object_id TEXT,
  source_run_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_review_items_key
ON review_items (review_key);

CREATE INDEX IF NOT EXISTS ix_review_items_status
ON review_items (status, severity, updated_at DESC);

CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  email TEXT,
  password_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL,
  last_active_at TEXT
);

CREATE TABLE IF NOT EXISTS roles (
  role_id TEXT PRIMARY KEY,
  role_name TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_roles (
  user_role_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  role_name TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_roles_user_role
ON user_roles (user_id, role_name);

CREATE TABLE IF NOT EXISTS auth_sessions (
  session_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  session_token_hash TEXT NOT NULL UNIQUE,
  issued_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  revoked_at TEXT,
  source_ip TEXT,
  user_agent TEXT
);

CREATE INDEX IF NOT EXISTS ix_auth_sessions_user
ON auth_sessions (user_id, expires_at DESC);

CREATE TABLE IF NOT EXISTS daily_brief_runs (
  brief_run_id TEXT PRIMARY KEY,
  source_run_id TEXT,
  generated_at TEXT NOT NULL,
  status TEXT NOT NULL,
  brief_mode TEXT NOT NULL DEFAULT 'daily',
  audience_preset TEXT NOT NULL DEFAULT 'pm',
  delivery_state TEXT NOT NULL DEFAULT 'generated',
  approval_required INTEGER NOT NULL DEFAULT 0,
  summary TEXT,
  diagnostics_json TEXT NOT NULL DEFAULT '{}',
  content_version TEXT,
  policy_pack_version TEXT,
  benchmark_definition_version TEXT,
  cma_version TEXT,
  chart_version TEXT
);

CREATE TABLE IF NOT EXISTS daily_brief_items (
  brief_item_id TEXT PRIMARY KEY,
  brief_run_id TEXT NOT NULL,
  rank_order INTEGER NOT NULL,
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  relevance_type TEXT NOT NULL,
  affects_portfolio INTEGER NOT NULL DEFAULT 0,
  affects_blueprint INTEGER NOT NULL DEFAULT 0,
  action_needed INTEGER NOT NULL DEFAULT 0,
  citations_json TEXT NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS ix_daily_brief_items_run
ON daily_brief_items (brief_run_id, rank_order);

CREATE TABLE IF NOT EXISTS daily_brief_impact_links (
  impact_link_id TEXT PRIMARY KEY,
  brief_item_id TEXT NOT NULL,
  link_type TEXT NOT NULL,
  target_key TEXT NOT NULL,
  target_label TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 1.0
);

CREATE INDEX IF NOT EXISTS ix_daily_brief_impact_links_item
ON daily_brief_impact_links (brief_item_id, link_type);

CREATE TABLE IF NOT EXISTS cma_assumptions (
  cma_id TEXT PRIMARY KEY,
  sleeve_key TEXT NOT NULL,
  sleeve_name TEXT NOT NULL,
  expected_return_min REAL NOT NULL,
  expected_return_max REAL NOT NULL,
  confidence_label TEXT NOT NULL,
  worst_year_loss_min REAL,
  worst_year_loss_max REAL,
  scenario_notes TEXT,
  assumption_date TEXT NOT NULL,
  version_label TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_cma_assumptions_active
ON cma_assumptions (sleeve_key, version_label);

CREATE TABLE IF NOT EXISTS benchmark_definitions (
  benchmark_definition_id TEXT PRIMARY KEY,
  context_key TEXT NOT NULL,
  benchmark_name TEXT NOT NULL,
  version_label TEXT NOT NULL,
  components_json TEXT NOT NULL DEFAULT '[]',
  rationale TEXT,
  assumption_date TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_benchmark_definitions_context_version
ON benchmark_definitions (context_key, version_label);

CREATE TABLE IF NOT EXISTS ips_snapshots (
  ips_snapshot_id TEXT PRIMARY KEY,
  brief_run_id TEXT NOT NULL,
  profile_id TEXT NOT NULL,
  benchmark_definition_id TEXT,
  cma_version TEXT,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_ips_snapshots_brief
ON ips_snapshots (brief_run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS dca_policies (
  dca_policy_id TEXT PRIMARY KEY,
  profile_id TEXT NOT NULL,
  policy_name TEXT NOT NULL,
  cadence TEXT NOT NULL,
  routing_mode TEXT NOT NULL,
  neutral_routing_json TEXT NOT NULL DEFAULT '[]',
  drift_routing_json TEXT NOT NULL DEFAULT '[]',
  stress_routing_json TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dca_policies_profile_name
ON dca_policies (profile_id, policy_name);

CREATE TABLE IF NOT EXISTS daily_brief_approvals (
  approval_id TEXT PRIMARY KEY,
  brief_run_id TEXT NOT NULL,
  approval_status TEXT NOT NULL,
  reviewed_by TEXT,
  approved_by TEXT,
  reviewed_at TEXT,
  approved_at TEXT,
  rejection_reason TEXT,
  notes TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_daily_brief_approvals_run
ON daily_brief_approvals (brief_run_id);

CREATE TABLE IF NOT EXISTS brief_ack_events (
  ack_event_id TEXT PRIMARY KEY,
  brief_run_id TEXT NOT NULL,
  recipient TEXT NOT NULL,
  ack_state TEXT NOT NULL,
  actor TEXT,
  occurred_at TEXT NOT NULL,
  details_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS ix_brief_ack_events_run
ON brief_ack_events (brief_run_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS brief_versions (
  version_id TEXT PRIMARY KEY,
  brief_run_id TEXT NOT NULL,
  content_version TEXT NOT NULL,
  policy_pack_version TEXT NOT NULL,
  benchmark_definition_version TEXT NOT NULL,
  cma_version TEXT NOT NULL,
  chart_version TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_brief_versions_run
ON brief_versions (brief_run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS daily_brief_regeneration_jobs (
  job_id TEXT PRIMARY KEY,
  requested_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  status TEXT NOT NULL,
  requested_by TEXT,
  brief_mode TEXT NOT NULL DEFAULT 'daily',
  audience_preset TEXT NOT NULL DEFAULT 'pm',
  force_cache_only INTEGER NOT NULL DEFAULT 0,
  contract_version_target TEXT,
  contract_version_persisted TEXT,
  brief_run_id TEXT,
  verifier_result_json TEXT NOT NULL DEFAULT '{}',
  proof_json TEXT NOT NULL DEFAULT '{}',
  stage_reports_json TEXT NOT NULL DEFAULT '[]',
  failure_reason TEXT,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_daily_brief_regeneration_jobs_requested
ON daily_brief_regeneration_jobs (requested_at DESC);

CREATE INDEX IF NOT EXISTS ix_daily_brief_regeneration_jobs_status
ON daily_brief_regeneration_jobs (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS chart_artifacts (
  chart_artifact_id TEXT PRIMARY KEY,
  brief_run_id TEXT NOT NULL,
  chart_key TEXT NOT NULL,
  title TEXT NOT NULL,
  artifact_path TEXT NOT NULL,
  artifact_format TEXT NOT NULL DEFAULT 'svg',
  source_as_of TEXT,
  freshness_note TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_chart_artifacts_run
ON chart_artifacts (brief_run_id, chart_key, created_at DESC);

CREATE TABLE IF NOT EXISTS regime_history (
  regime_history_id TEXT PRIMARY KEY,
  brief_run_id TEXT NOT NULL,
  as_of_ts TEXT NOT NULL,
  long_state TEXT NOT NULL,
  short_state TEXT NOT NULL,
  change_summary TEXT,
  confidence_label TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_regime_history_asof
ON regime_history (as_of_ts DESC, brief_run_id);

CREATE TABLE IF NOT EXISTS scenario_registry (
  scenario_id TEXT PRIMARY KEY,
  scenario_name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  source_rationale TEXT,
  policy_notes TEXT,
  created_at TEXT NOT NULL,
  approved_at TEXT,
  retired_at TEXT
);

CREATE TABLE IF NOT EXISTS policy_assumption_observations (
  observation_id TEXT PRIMARY KEY,
  assumption_key TEXT NOT NULL,
  assumption_family TEXT NOT NULL,
  value_json TEXT NOT NULL,
  source_name TEXT,
  source_url TEXT,
  observed_at TEXT,
  ingested_at TEXT NOT NULL,
  methodology_note TEXT,
  provenance_level TEXT NOT NULL DEFAULT 'developer_seed',
  confidence_label TEXT NOT NULL DEFAULT 'low',
  overwrite_priority INTEGER NOT NULL DEFAULT 0,
  is_current INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_policy_assumption_observations_key
ON policy_assumption_observations (assumption_family, assumption_key, is_current, ingested_at DESC);

CREATE TABLE IF NOT EXISTS policy_assumption_current (
  assumption_key TEXT PRIMARY KEY,
  assumption_family TEXT NOT NULL,
  resolved_value_json TEXT NOT NULL,
  source_name TEXT,
  source_url TEXT,
  observed_at TEXT,
  ingested_at TEXT NOT NULL,
  methodology_note TEXT,
  provenance_level TEXT NOT NULL DEFAULT 'developer_seed',
  confidence_label TEXT NOT NULL DEFAULT 'low',
  last_resolved_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS benchmark_policy_profiles (
  profile_row_id TEXT PRIMARY KEY,
  profile_key TEXT NOT NULL,
  sleeve_key TEXT NOT NULL,
  target_weight REAL NOT NULL,
  min_weight REAL,
  max_weight REAL,
  source_name TEXT,
  source_url TEXT,
  methodology_note TEXT,
  observed_at TEXT,
  provenance_level TEXT NOT NULL DEFAULT 'developer_seed',
  confidence_label TEXT NOT NULL DEFAULT 'low',
  is_current INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS ix_benchmark_policy_profiles_key
ON benchmark_policy_profiles (profile_key, is_current, sleeve_key);

CREATE TABLE IF NOT EXISTS stress_methodology_registry (
  scenario_key TEXT PRIMARY KEY,
  shock_definition_json TEXT NOT NULL,
  methodology_source_name TEXT,
  methodology_source_url TEXT,
  observed_at TEXT,
  provenance_level TEXT NOT NULL DEFAULT 'developer_seed',
  confidence_label TEXT NOT NULL DEFAULT 'low',
  notes TEXT
);

CREATE TABLE IF NOT EXISTS scenario_versions (
  scenario_version_id TEXT PRIMARY KEY,
  scenario_id TEXT NOT NULL,
  version_label TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  probability_weight REAL,
  confidence_rating TEXT NOT NULL DEFAULT 'medium',
  review_cadence_days INTEGER,
  last_reviewed_at TEXT,
  reviewed_by TEXT,
  shocks_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scenario_review_events (
  scenario_review_event_id TEXT PRIMARY KEY,
  scenario_id TEXT NOT NULL,
  scenario_version_id TEXT,
  actor TEXT NOT NULL,
  event_type TEXT NOT NULL,
  note TEXT,
  occurred_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scenario_comparison_snapshots (
  comparison_id TEXT PRIMARY KEY,
  scenario_id TEXT NOT NULL,
  scenario_version_id TEXT,
  current_run_id TEXT,
  prior_run_id TEXT,
  current_impact_pct REAL,
  prior_impact_pct REAL,
  impact_delta_pct REAL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candidate_quality_scores (
  score_id TEXT PRIMARY KEY,
  snapshot_id TEXT,
  symbol TEXT NOT NULL,
  sleeve_key TEXT NOT NULL,
  eligibility_state TEXT NOT NULL,
  eligibility_blockers_json TEXT NOT NULL,
  data_confidence TEXT NOT NULL,
  performance_score REAL,
  risk_adjusted_score REAL,
  cost_score REAL,
  liquidity_score REAL,
  structure_score REAL,
  tax_score REAL,
  governance_confidence_score REAL,
  composite_score REAL,
  rank_in_sleeve INTEGER,
  percentile_in_sleeve REAL,
  badge TEXT NOT NULL,
  recommendation_state TEXT NOT NULL,
  investment_thesis TEXT,
  role_in_portfolio TEXT,
  key_advantages_json TEXT NOT NULL DEFAULT '[]',
  key_risks_json TEXT NOT NULL DEFAULT '[]',
  comparison_vs_peers_json TEXT NOT NULL DEFAULT '{}',
  score_provenance_json TEXT NOT NULL DEFAULT '{}',
  score_version TEXT NOT NULL,
  as_of_date TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_candidate_quality_scores_snapshot
ON candidate_quality_scores (snapshot_id, sleeve_key, rank_in_sleeve);

CREATE TABLE IF NOT EXISTS sleeve_recommendations (
  recommendation_id TEXT PRIMARY KEY,
  snapshot_id TEXT,
  sleeve_key TEXT NOT NULL,
  our_pick_symbol TEXT,
  top_candidates_json TEXT NOT NULL DEFAULT '[]',
  acceptable_candidates_json TEXT NOT NULL DEFAULT '[]',
  caution_candidates_json TEXT NOT NULL DEFAULT '[]',
  why_this_pick_wins TEXT NOT NULL,
  what_would_change_the_pick TEXT NOT NULL,
  missing_data_json TEXT NOT NULL DEFAULT '[]',
  common_blockers_json TEXT NOT NULL DEFAULT '[]',
  score_version TEXT NOT NULL,
  as_of_date TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_sleeve_recommendations_snapshot
ON sleeve_recommendations (snapshot_id, sleeve_key);

CREATE TABLE IF NOT EXISTS recommendation_events (
  event_id TEXT PRIMARY KEY,
  snapshot_id TEXT,
  sleeve_key TEXT NOT NULL,
  candidate_symbol TEXT NOT NULL,
  prior_rank INTEGER,
  new_rank INTEGER,
  prior_badge TEXT,
  new_badge TEXT,
  prior_recommendation_state TEXT,
  new_recommendation_state TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_recommendation_events_snapshot
ON recommendation_events (snapshot_id, sleeve_key, created_at DESC);

CREATE TABLE IF NOT EXISTS blueprint_candidate_registry (
  registry_id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  name TEXT NOT NULL,
  sleeve_key TEXT NOT NULL,
  issuer TEXT,
  asset_class TEXT,
  instrument_type TEXT NOT NULL,
  domicile TEXT NOT NULL,
  share_class TEXT,
  benchmark_key TEXT,
  replication_method TEXT,
  expense_ratio REAL,
  expected_withholding_drag_estimate REAL,
  estate_risk_flag INTEGER NOT NULL DEFAULT 0,
  liquidity_proxy TEXT,
  liquidity_score REAL,
  rationale TEXT,
  citation_keys_json TEXT NOT NULL DEFAULT '[]',
  source_links_json TEXT NOT NULL DEFAULT '[]',
  source_state TEXT NOT NULL DEFAULT 'manual_static',
  factsheet_asof TEXT,
  market_data_asof TEXT,
  verification_metadata_json TEXT NOT NULL DEFAULT '{}',
  manual_provenance_note TEXT,
  extra_json TEXT NOT NULL DEFAULT '{}',
  effective_at TEXT NOT NULL,
  retired_at TEXT,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_candidate_registry_symbol_active
ON blueprint_candidate_registry (symbol, sleeve_key, effective_at);

CREATE TABLE IF NOT EXISTS candidate_field_observations (
  observation_id TEXT PRIMARY KEY,
  candidate_symbol TEXT NOT NULL,
  sleeve_key TEXT NOT NULL,
  field_name TEXT NOT NULL,
  value_json TEXT,
  value_type TEXT NOT NULL,
  source_name TEXT,
  source_url TEXT,
  observed_at TEXT,
  ingested_at TEXT NOT NULL,
  provenance_level TEXT NOT NULL,
  confidence_label TEXT,
  parser_method TEXT,
  overwrite_priority INTEGER NOT NULL,
  missingness_reason TEXT NOT NULL,
  is_current INTEGER NOT NULL DEFAULT 0,
  override_annotation_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS ix_candidate_field_observations_lookup
ON candidate_field_observations (candidate_symbol, sleeve_key, field_name, ingested_at DESC);

CREATE TABLE IF NOT EXISTS candidate_field_current (
  candidate_symbol TEXT NOT NULL,
  sleeve_key TEXT NOT NULL,
  field_name TEXT NOT NULL,
  resolved_value_json TEXT,
  value_type TEXT NOT NULL,
  source_name TEXT,
  source_url TEXT,
  observed_at TEXT,
  ingested_at TEXT,
  provenance_level TEXT NOT NULL,
  confidence_label TEXT,
  parser_method TEXT,
  overwrite_priority INTEGER NOT NULL,
  missingness_reason TEXT NOT NULL,
  override_annotation_json TEXT NOT NULL DEFAULT '{}',
  last_resolved_at TEXT NOT NULL,
  PRIMARY KEY (candidate_symbol, sleeve_key, field_name)
);

CREATE TABLE IF NOT EXISTS candidate_required_field_matrix (
  sleeve_key TEXT NOT NULL,
  field_name TEXT NOT NULL,
  critical_flag INTEGER NOT NULL DEFAULT 0,
  applicability_rule TEXT NOT NULL DEFAULT 'always',
  readiness_tier TEXT NOT NULL DEFAULT 'review',
  PRIMARY KEY (sleeve_key, field_name)
);

CREATE TABLE IF NOT EXISTS candidate_completeness_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  candidate_symbol TEXT NOT NULL,
  sleeve_key TEXT NOT NULL,
  required_fields_total INTEGER NOT NULL,
  required_fields_populated INTEGER NOT NULL,
  critical_required_fields_missing_json TEXT NOT NULL DEFAULT '[]',
  fetchable_missing_count INTEGER NOT NULL DEFAULT 0,
  source_gap_missing_count INTEGER NOT NULL DEFAULT 0,
  proxy_only_count INTEGER NOT NULL DEFAULT 0,
  stale_required_count INTEGER NOT NULL DEFAULT 0,
  readiness_level TEXT NOT NULL,
  computed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_candidate_completeness_snapshots_lookup
ON candidate_completeness_snapshots (candidate_symbol, sleeve_key, computed_at DESC);

CREATE TABLE IF NOT EXISTS sleeve_no_pick_reasons (
  snapshot_id TEXT NOT NULL,
  sleeve_key TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  reason_text TEXT NOT NULL,
  nearest_passing_candidate TEXT,
  blocking_fields_json TEXT NOT NULL DEFAULT '[]',
  evidence_json TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY (snapshot_id, sleeve_key)
);

CREATE TABLE IF NOT EXISTS regime_methodology_registry (
  metric_key TEXT PRIMARY KEY,
  watch_threshold REAL,
  alert_threshold REAL,
  methodology_note TEXT NOT NULL,
  threshold_kind TEXT NOT NULL DEFAULT 'observational',
  source_name TEXT,
  source_url TEXT,
  observed_at TEXT,
  provenance_level TEXT NOT NULL DEFAULT 'developer_seed',
  confidence_label TEXT NOT NULL DEFAULT 'low',
  methodology_version TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_usage_ledger (
  ledger_id TEXT PRIMARY KEY,
  provider_name TEXT NOT NULL,
  endpoint_family TEXT NOT NULL,
  date_bucket TEXT NOT NULL,
  month_bucket TEXT NOT NULL,
  call_count INTEGER NOT NULL DEFAULT 1,
  estimated_cost_unit REAL NOT NULL DEFAULT 1.0,
  success INTEGER NOT NULL DEFAULT 1,
  triggered_by_job TEXT,
  triggered_by_surface TEXT,
  cache_hit INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_provider_usage_provider_month
ON provider_usage_ledger (provider_name, month_bucket, endpoint_family);

CREATE TABLE IF NOT EXISTS provider_budget_policy (
  provider_name TEXT PRIMARY KEY,
  daily_soft_budget REAL NOT NULL,
  monthly_soft_budget REAL NOT NULL,
  monthly_hard_budget REAL NOT NULL,
  reserve_percentage REAL NOT NULL DEFAULT 0.15,
  critical_use_only_threshold REAL NOT NULL DEFAULT 0.85,
  blocked_threshold REAL NOT NULL DEFAULT 1.0,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_surface_budget_policy (
  provider_name TEXT NOT NULL,
  surface_name TEXT NOT NULL,
  daily_reserved_budget REAL NOT NULL DEFAULT 0,
  monthly_reserved_budget REAL NOT NULL DEFAULT 0,
  importance_weight REAL NOT NULL DEFAULT 1.0,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (provider_name, surface_name)
);

CREATE TABLE IF NOT EXISTS provider_health_state (
  provider_name TEXT PRIMARY KEY,
  last_successful_fetch_at TEXT,
  last_failure_at TEXT,
  failure_streak INTEGER NOT NULL DEFAULT 0,
  current_mode TEXT NOT NULL DEFAULT 'normal',
  quota_state TEXT NOT NULL DEFAULT 'normal',
  last_error TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_surface_snapshot_versions (
  surface_name TEXT NOT NULL,
  family_name TEXT NOT NULL,
  snapshot_version TEXT NOT NULL,
  latest_observed_at TEXT,
  latest_fetched_at TEXT,
  provider_mix_json TEXT NOT NULL DEFAULT '[]',
  updated_at TEXT NOT NULL,
  PRIMARY KEY (surface_name, family_name)
);

CREATE TABLE IF NOT EXISTS provider_family_events (
  event_id TEXT PRIMARY KEY,
  provider_name TEXT NOT NULL,
  surface_name TEXT NOT NULL,
  family_name TEXT NOT NULL,
  identifier TEXT,
  target_universe_json TEXT NOT NULL DEFAULT '[]',
  success INTEGER NOT NULL DEFAULT 0,
  error_class TEXT,
  cache_hit INTEGER NOT NULL DEFAULT 0,
  freshness_state TEXT,
  fallback_used INTEGER NOT NULL DEFAULT 0,
  age_seconds REAL,
  triggered_by_job TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_provider_family_events_lookup
ON provider_family_events (provider_name, surface_name, family_name, created_at);

CREATE TABLE IF NOT EXISTS provider_family_success (
  provider_name TEXT NOT NULL,
  surface_name TEXT NOT NULL,
  family_name TEXT NOT NULL,
  target_universe_json TEXT NOT NULL DEFAULT '[]',
  success_count INTEGER NOT NULL DEFAULT 0,
  failure_count INTEGER NOT NULL DEFAULT 0,
  empty_response_count INTEGER NOT NULL DEFAULT 0,
  endpoint_blocked_count INTEGER NOT NULL DEFAULT 0,
  plan_limited_count INTEGER NOT NULL DEFAULT 0,
  symbol_gap_count INTEGER NOT NULL DEFAULT 0,
  stale_snapshot_count INTEGER NOT NULL DEFAULT 0,
  current_snapshot_count INTEGER NOT NULL DEFAULT 0,
  median_freshness_seconds REAL,
  last_successful_family_refresh TEXT,
  last_failed_family_refresh TEXT,
  reliability_score REAL NOT NULL DEFAULT 0,
  current_tier TEXT NOT NULL DEFAULT 'backup_only',
  last_error_class TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (provider_name, surface_name, family_name)
);

CREATE TABLE IF NOT EXISTS provider_cache_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  provider_name TEXT NOT NULL,
  endpoint_family TEXT NOT NULL,
  cache_key TEXT NOT NULL,
  surface_name TEXT,
  payload_json TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  expires_at TEXT,
  freshness_state TEXT NOT NULL,
  confidence_tier TEXT,
  source_ref TEXT,
  cache_status TEXT NOT NULL,
  fallback_used INTEGER NOT NULL DEFAULT 0,
  error_state TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_provider_cache_key
ON provider_cache_snapshots (provider_name, endpoint_family, cache_key, surface_name);

CREATE TABLE IF NOT EXISTS public_upstream_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  provider_key TEXT NOT NULL,
  family_name TEXT NOT NULL,
  surface_usage_json TEXT NOT NULL DEFAULT '[]',
  payload_json TEXT NOT NULL DEFAULT '{}',
  source_url TEXT,
  observed_at TEXT,
  fetched_at TEXT NOT NULL,
  freshness_state TEXT NOT NULL,
  error_state TEXT,
  snapshot_version TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_public_upstream_snapshots_lookup
ON public_upstream_snapshots (provider_key, family_name, fetched_at DESC);

CREATE TABLE IF NOT EXISTS symbol_resolution_registry (
  canonical_symbol TEXT NOT NULL,
  provider_name TEXT NOT NULL,
  endpoint_family TEXT NOT NULL,
  provider_symbol TEXT NOT NULL,
  exchange_suffix TEXT,
  asset_type TEXT,
  region TEXT,
  primary_listing TEXT,
  fallback_aliases_json TEXT NOT NULL DEFAULT '[]',
  resolution_confidence REAL NOT NULL DEFAULT 0.5,
  resolution_reason TEXT,
  last_verified_at TEXT NOT NULL,
  PRIMARY KEY (canonical_symbol, provider_name, endpoint_family)
);

CREATE TABLE IF NOT EXISTS symbol_resolution_failures (
  canonical_symbol TEXT NOT NULL,
  provider_name TEXT NOT NULL,
  endpoint_family TEXT NOT NULL,
  provider_symbol TEXT NOT NULL,
  error_class TEXT NOT NULL,
  failure_count INTEGER NOT NULL DEFAULT 1,
  first_failed_at TEXT NOT NULL,
  last_failed_at TEXT NOT NULL,
  last_success_at TEXT,
  disabled_until TEXT,
  PRIMARY KEY (canonical_symbol, provider_name, endpoint_family, provider_symbol, error_class)
);

CREATE TABLE IF NOT EXISTS candidate_market_identities (
  identity_id TEXT PRIMARY KEY,
  candidate_id TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  provider_symbol TEXT NOT NULL,
  provider_asset_class TEXT,
  exchange_mic TEXT,
  quote_currency TEXT,
  series_role TEXT NOT NULL,
  adjustment_mode TEXT NOT NULL,
  timezone TEXT NOT NULL,
  primary_interval TEXT NOT NULL,
  preferred_lookback_days INTEGER NOT NULL,
  forecast_eligibility TEXT NOT NULL,
  proxy_relationship TEXT,
  resolution_method TEXT,
  resolution_confidence REAL,
  resolved_from TEXT,
  last_verified_at TEXT,
  forecast_driving_series INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(candidate_id, series_role, primary_interval)
);

CREATE INDEX IF NOT EXISTS ix_candidate_market_identities_candidate
ON candidate_market_identities (candidate_id, forecast_driving_series DESC, series_role);

CREATE TABLE IF NOT EXISTS candidate_price_series (
  row_id TEXT PRIMARY KEY,
  candidate_id TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  series_role TEXT NOT NULL,
  timestamp_utc TEXT NOT NULL,
  interval TEXT NOT NULL,
  open REAL NOT NULL,
  high REAL NOT NULL,
  low REAL NOT NULL,
  close REAL NOT NULL,
  volume REAL,
  amount REAL,
  provider TEXT NOT NULL,
  provider_symbol TEXT NOT NULL,
  adjusted_flag INTEGER NOT NULL DEFAULT 0,
  freshness_ts TEXT NOT NULL,
  quality_flags_json TEXT NOT NULL DEFAULT '[]',
  series_quality_summary_json TEXT NOT NULL DEFAULT '{}',
  ingest_run_id TEXT NOT NULL,
  UNIQUE(candidate_id, series_role, interval, timestamp_utc)
);

CREATE INDEX IF NOT EXISTS ix_candidate_price_series_candidate
ON candidate_price_series (candidate_id, series_role, interval, timestamp_utc DESC);

CREATE TABLE IF NOT EXISTS candidate_price_series_runs (
  ingest_run_id TEXT PRIMARY KEY,
  run_type TEXT NOT NULL,
  candidate_id TEXT NOT NULL,
  series_role TEXT NOT NULL,
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  bars_written INTEGER NOT NULL DEFAULT 0,
  failure_class TEXT,
  details_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS ix_candidate_price_series_runs_candidate
ON candidate_price_series_runs (candidate_id, series_role, started_at DESC);

CREATE TABLE IF NOT EXISTS candidate_market_forecast_runs (
  forecast_run_id TEXT PRIMARY KEY,
  candidate_id TEXT NOT NULL,
  series_role TEXT NOT NULL,
  model_name TEXT NOT NULL,
  model_version TEXT NOT NULL,
  input_series_version TEXT NOT NULL,
  run_status TEXT NOT NULL,
  usefulness_label TEXT NOT NULL,
  suppression_reason TEXT,
  generated_at TEXT NOT NULL,
  details_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS ix_candidate_market_forecast_runs_candidate
ON candidate_market_forecast_runs (candidate_id, generated_at DESC);

CREATE TABLE IF NOT EXISTS candidate_market_forecast_artifacts (
  artifact_id TEXT PRIMARY KEY,
  forecast_run_id TEXT NOT NULL,
  candidate_id TEXT NOT NULL,
  input_series_version TEXT NOT NULL,
  market_path_support_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_candidate_market_forecast_artifacts_candidate
ON candidate_market_forecast_artifacts (candidate_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_candidate_market_forecast_artifacts_series_version
ON candidate_market_forecast_artifacts (candidate_id, input_series_version);
