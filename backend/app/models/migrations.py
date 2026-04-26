from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.services.blueprint_decisions import ensure_blueprint_decision_tables
from app.services.blueprint_benchmark_registry import ensure_benchmark_registry_tables
from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables
from app.services.blueprint_candidate_compare import ensure_candidate_comparison_tables
from app.services.blueprint_candidate_truth import ensure_candidate_truth_tables, seed_required_field_matrix
from app.services.blueprint_investment_quality import ensure_quality_tables
from app.services.blueprint_recommendations import ensure_recommendation_tables
from app.services.policy_assumptions import ensure_policy_assumption_tables
from app.services.provider_budget import ensure_provider_budget_tables
from app.services.provider_family_success import ensure_provider_family_success_tables
from app.services.public_upstream_snapshots import ensure_public_upstream_snapshot_tables
from app.services.daily_brief_regeneration import ensure_daily_brief_regeneration_tables
from app.services.regime_methodology import ensure_regime_methodology_tables
from app.services.symbol_resolution import ensure_symbol_resolution_tables
from app.v2.blueprint_market.series_store import ensure_blueprint_market_tables

SCHEMA_VERSION = 36


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    if not table_exists(conn, table_name):
        return False
    rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return any(str(row[1]) == column_name for row in rows)


def _ensure_schema_meta_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          schema_version INTEGER NOT NULL,
          applied_at TEXT NOT NULL
        )
        """
    )
    row = conn.execute("SELECT schema_version FROM schema_meta WHERE id = 1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO schema_meta (id, schema_version, applied_at) VALUES (1, 0, ?)",
            (_now_iso(),),
        )
    conn.commit()


def _current_schema_version(conn: sqlite3.Connection) -> int:
    _ensure_schema_meta_table(conn)
    row = conn.execute("SELECT schema_version FROM schema_meta WHERE id = 1").fetchone()
    if row is None:
        return 0
    try:
        return int(row[0])
    except Exception:  # noqa: BLE001
        return 0


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "UPDATE schema_meta SET schema_version = ?, applied_at = ? WHERE id = 1",
        (int(version), _now_iso()),
    )
    conn.commit()


def _ensure_run_id_columns(conn: sqlite3.Connection) -> None:
    for table_name in (
        "email_runs",
        "daily_logs",
        "mcp_connectivity_runs",
        "mcp_items",
        "metric_snapshots",
        "alert_events",
    ):
        if table_exists(conn, table_name) and not column_exists(conn, table_name, "run_id"):
            conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN run_id TEXT')
    conn.commit()


def _ensure_email_run_slot_columns(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "email_runs"):
        return
    if not column_exists(conn, "email_runs", "run_slot_key"):
        conn.execute('ALTER TABLE "email_runs" ADD COLUMN run_slot_key TEXT')
    if not column_exists(conn, "email_runs", "run_slot_label"):
        conn.execute('ALTER TABLE "email_runs" ADD COLUMN run_slot_label TEXT')
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_email_runs_slot_sent
        ON email_runs (run_slot_key)
        WHERE status = 'sent' AND run_slot_key IS NOT NULL
        """
    )
    conn.commit()


def _ensure_outbox_artifacts_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS outbox_artifacts (
          artifact_id TEXT PRIMARY KEY,
          run_id TEXT,
          artifact_type TEXT NOT NULL,
          artifact_path TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_outbox_artifacts_run_id
        ON outbox_artifacts (run_id, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_outbox_artifacts_created
        ON outbox_artifacts (created_at DESC)
        """
    )
    conn.commit()


def _ensure_optional_indexes(conn: sqlite3.Connection) -> None:
    if (
        table_exists(conn, "metric_snapshots")
        and column_exists(conn, "metric_snapshots", "run_id")
        and column_exists(conn, "metric_snapshots", "metric_key")
    ):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_metric_snapshots_run_metric
            ON metric_snapshots (run_id, metric_key)
            """
        )

    if (
        table_exists(conn, "mcp_items")
        and column_exists(conn, "mcp_items", "run_id")
        and column_exists(conn, "mcp_items", "retrieved_at")
    ):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_mcp_items_run_id
            ON mcp_items (run_id, retrieved_at DESC)
            """
        )
    conn.commit()


def _ensure_symbol_resolution(conn: sqlite3.Connection) -> None:
    ensure_symbol_resolution_tables(conn)
    conn.commit()


def _ensure_portfolio_control_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_upload_runs (
          run_id TEXT PRIMARY KEY,
          uploaded_at TEXT NOT NULL,
          holdings_as_of_date TEXT NOT NULL,
          source_name TEXT,
          status TEXT NOT NULL,
          raw_row_count INTEGER NOT NULL DEFAULT 0,
          parsed_row_count INTEGER NOT NULL DEFAULT 0,
          warning_count INTEGER NOT NULL DEFAULT 0,
          warnings_json TEXT NOT NULL DEFAULT '[]',
          errors_json TEXT NOT NULL DEFAULT '[]'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_portfolio_upload_runs_uploaded_at
        ON portfolio_upload_runs (uploaded_at DESC)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_portfolio_holding_snapshots_run
        ON portfolio_holding_snapshots (run_id, account_id, security_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_portfolio_holding_snapshots_asof
        ON portfolio_holding_snapshots (holdings_as_of_date DESC, uploaded_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_mapping_overrides (
          override_id TEXT PRIMARY KEY,
          account_id TEXT,
          security_key TEXT NOT NULL,
          normalized_symbol TEXT,
          target_sleeve TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'manual_override',
          note TEXT,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_portfolio_mapping_override_key
        ON portfolio_mapping_overrides (account_id, security_key)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_mapping_issues (
          issue_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          account_id TEXT,
          security_key TEXT,
          issue_type TEXT NOT NULL,
          severity TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_portfolio_mapping_issues_run
        ON portfolio_mapping_issues (run_id, severity)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_market_price_snapshots_symbol
        ON market_price_snapshots (security_key, retrieved_at DESC)
        """
    )
    for column_name, definition in (
        ("snapshot_date", "TEXT"),
        ("holdings_as_of_date", "TEXT"),
        ("price_as_of_date", "TEXT"),
        ("upload_run_id", "TEXT"),
        ("stale_price_count", "INTEGER NOT NULL DEFAULT 0"),
        ("mapping_issue_count", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if table_exists(conn, "portfolio_snapshots") and not column_exists(conn, "portfolio_snapshots", column_name):
            conn.execute(f'ALTER TABLE "portfolio_snapshots" ADD COLUMN "{column_name}" {definition}')
    conn.commit()


def _ensure_blueprint_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_sleeves_key
        ON blueprint_sleeves (blueprint_id, sleeve_key)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_benchmarks (
          benchmark_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          sleeve_key TEXT,
          benchmark_name TEXT NOT NULL,
          benchmark_symbol TEXT,
          notes TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_mapping_rules (
          rule_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          match_type TEXT NOT NULL,
          match_value TEXT NOT NULL,
          target_sleeve TEXT NOT NULL,
          confidence REAL NOT NULL DEFAULT 1.0,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_mapping_rules_match
        ON blueprint_mapping_rules (blueprint_id, match_type, match_value)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_snapshots_blueprint_created
        ON blueprint_snapshots (blueprint_id, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_decision_artifacts (
          artifact_id TEXT PRIMARY KEY,
          snapshot_id TEXT NOT NULL,
          sleeve_key TEXT,
          candidate_symbol TEXT,
          artifact_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_decision_artifacts_snapshot
        ON blueprint_decision_artifacts (snapshot_id, artifact_type, sleeve_key, candidate_symbol)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_runtime_cycles (
          cycle_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          refresh_run_id TEXT,
          evaluation_mode TEXT NOT NULL,
          payload_hash TEXT NOT NULL,
          payload_json TEXT NOT NULL DEFAULT '{}',
          generated_at TEXT,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_runtime_cycles_created
        ON blueprint_runtime_cycles (created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_runtime_cycles_identity
        ON blueprint_runtime_cycles (payload_hash, COALESCE(refresh_run_id, ''), evaluation_mode, COALESCE(generated_at, ''))
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_runtime_cycle_artifacts (
          artifact_id TEXT PRIMARY KEY,
          cycle_id TEXT NOT NULL,
          sleeve_key TEXT,
          candidate_symbol TEXT,
          artifact_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_runtime_cycle_artifacts_cycle
        ON blueprint_runtime_cycle_artifacts (cycle_id, artifact_type, sleeve_key, candidate_symbol)
        """
    )
    conn.commit()


def _ensure_review_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS review_items (
          review_id TEXT PRIMARY KEY,
          review_key TEXT NOT NULL,
          category TEXT NOT NULL,
          severity TEXT NOT NULL,
          owner TEXT,
          due_date TEXT,
          status TEXT NOT NULL DEFAULT 'open',
          notes TEXT,
          linked_object_type TEXT,
          linked_object_id TEXT,
          source_run_id TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_review_items_key
        ON review_items (review_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_review_items_status
        ON review_items (status, severity, updated_at DESC)
        """
    )
    conn.commit()


def _ensure_daily_brief_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_brief_runs (
          brief_run_id TEXT PRIMARY KEY,
          source_run_id TEXT,
          generated_at TEXT NOT NULL,
          status TEXT NOT NULL,
          summary TEXT,
          diagnostics_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_daily_brief_items_run
        ON daily_brief_items (brief_run_id, rank_order)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_brief_impact_links (
          impact_link_id TEXT PRIMARY KEY,
          brief_item_id TEXT NOT NULL,
          link_type TEXT NOT NULL,
          target_key TEXT NOT NULL,
          target_label TEXT NOT NULL,
          confidence REAL NOT NULL DEFAULT 1.0
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_daily_brief_impact_links_item
        ON daily_brief_impact_links (brief_item_id, link_type)
        """
    )
    conn.commit()


def _ensure_portfolio_upload_registry_columns(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "portfolio_upload_runs"):
        return
    for column_name, definition in (
        ("filename", "TEXT"),
        ("is_active", "INTEGER NOT NULL DEFAULT 0"),
        ("is_deleted", "INTEGER NOT NULL DEFAULT 0"),
        ("deleted_at", "TEXT"),
        ("deleted_reason", "TEXT"),
        ("snapshot_id", "TEXT"),
        ("normalized_position_count", "INTEGER NOT NULL DEFAULT 0"),
        ("total_market_value", "REAL NOT NULL DEFAULT 0"),
        ("stale_price_count", "INTEGER NOT NULL DEFAULT 0"),
        ("mapping_issue_count", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if not column_exists(conn, "portfolio_upload_runs", column_name):
            conn.execute(f'ALTER TABLE "portfolio_upload_runs" ADD COLUMN "{column_name}" {definition}')
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_portfolio_upload_runs_active
        ON portfolio_upload_runs (is_active, is_deleted, uploaded_at DESC)
        """
    )
    conn.commit()


def _ensure_limit_and_exposure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_exposure_snapshots_run_type
        ON exposure_snapshots (run_id, exposure_type, weight DESC)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_limit_profiles_scope
        ON limit_profiles (COALESCE(blueprint_id, ''), COALESCE(strategy_id, ''), limit_type, scope)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_limit_breaches_open
        ON limit_breaches (limit_id, COALESCE(run_id, ''), COALESCE(scope_key, ''), breach_status)
        WHERE breach_status IN ('warning', 'breached')
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_limit_breaches_run
        ON limit_breaches (run_id, severity, last_detected_at DESC)
        """
    )
    conn.commit()


def _ensure_review_workflow_extensions(conn: sqlite3.Connection) -> None:
    for column_name, definition in (
        ("assignee_name", "TEXT"),
        ("assignee_role", "TEXT"),
        ("assigned_at", "TEXT"),
        ("assigned_by", "TEXT"),
        ("resolution_type", "TEXT"),
        ("resolution_summary", "TEXT"),
        ("resolution_notes", "TEXT"),
        ("resolved_by", "TEXT"),
        ("resolved_at", "TEXT"),
        ("acknowledged_at", "TEXT"),
        ("escalated_at", "TEXT"),
    ):
        if table_exists(conn, "review_items") and not column_exists(conn, "review_items", column_name):
            conn.execute(f'ALTER TABLE "review_items" ADD COLUMN "{column_name}" {definition}')
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS review_events (
          review_event_id TEXT PRIMARY KEY,
          review_id TEXT NOT NULL,
          prior_status TEXT,
          new_status TEXT NOT NULL,
          actor TEXT NOT NULL,
          reason TEXT,
          occurred_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS escalation_rules (
          rule_id TEXT PRIMARY KEY,
          category TEXT NOT NULL,
          severity TEXT NOT NULL,
          overdue_hours INTEGER,
          persistence_runs INTEGER,
          enabled INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _ensure_audit_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_exports (
          export_id TEXT PRIMARY KEY,
          export_scope TEXT NOT NULL,
          generated_at TEXT NOT NULL,
          generated_by TEXT NOT NULL,
          filters_json TEXT NOT NULL DEFAULT '{}',
          file_path TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS retention_policies (
          policy_id TEXT PRIMARY KEY,
          object_type TEXT NOT NULL,
          retention_days INTEGER NOT NULL,
          soft_delete_only INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _ensure_sla_and_availability_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.commit()


def _ensure_monitoring_extension_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )
    conn.commit()


def _ensure_blueprint_extension_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_versions (
          version_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          version_label TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 0,
          archived_at TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.commit()


def _ensure_liquidity_and_account_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS liquidity_snapshots (
          liquidity_id TEXT PRIMARY KEY,
          run_id TEXT,
          security_key TEXT,
          normalized_symbol TEXT,
          liquidity_bucket TEXT NOT NULL,
          trading_volume_proxy REAL,
          days_to_exit_proxy REAL,
          confidence_flag TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS account_entities (
          account_id TEXT PRIMARY KEY,
          custodian_name TEXT,
          base_currency TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dashboard_refresh_metadata (
          section_name TEXT PRIMARY KEY,
          last_refreshed_at TEXT,
          refresh_mode TEXT,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _ensure_stress_history_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_stress_scenario_history_asof
        ON stress_scenario_history (as_of_ts DESC, scenario_id)
        """
    )
    conn.commit()


def _ensure_classification_precision_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_security_classifications_run_security
        ON security_classifications (run_id, security_key)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_issuer_classifications_key
        ON issuer_classifications (issuer_key)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS factor_exposure_snapshots (
          factor_snapshot_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          factor_name TEXT NOT NULL,
          exposure_value REAL NOT NULL,
          exposure_type TEXT NOT NULL,
          confidence TEXT NOT NULL,
          provenance_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_factor_exposure_snapshots_run
        ON factor_exposure_snapshots (run_id, factor_name)
        """
    )
    for column_name, definition in (
        ("source_name", "TEXT"),
        ("source_observed_at", "TEXT"),
        ("provenance_json", "TEXT NOT NULL DEFAULT '{}'"),
    ):
        if table_exists(conn, "liquidity_snapshots") and not column_exists(conn, "liquidity_snapshots", column_name):
            conn.execute(f'ALTER TABLE "liquidity_snapshots" ADD COLUMN "{column_name}" {definition}')
    conn.commit()


def _ensure_scenario_registry_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_registry (
          scenario_id TEXT PRIMARY KEY,
          scenario_name TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          source_rationale TEXT,
          policy_notes TEXT,
          created_at TEXT NOT NULL,
          approved_at TEXT,
          retired_at TEXT
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_review_events (
          scenario_review_event_id TEXT PRIMARY KEY,
          scenario_id TEXT NOT NULL,
          scenario_version_id TEXT,
          actor TEXT NOT NULL,
          event_type TEXT NOT NULL,
          note TEXT,
          occurred_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.commit()


def _ensure_daily_brief_upgrade_tables(conn: sqlite3.Connection) -> None:
    if table_exists(conn, "daily_brief_runs"):
        for column_name, definition in (
            ("brief_mode", "TEXT NOT NULL DEFAULT 'daily'"),
            ("audience_preset", "TEXT NOT NULL DEFAULT 'pm'"),
            ("delivery_state", "TEXT NOT NULL DEFAULT 'generated'"),
            ("approval_required", "INTEGER NOT NULL DEFAULT 0"),
            ("content_version", "TEXT"),
            ("policy_pack_version", "TEXT"),
            ("benchmark_definition_version", "TEXT"),
            ("cma_version", "TEXT"),
            ("chart_version", "TEXT"),
        ):
            if not column_exists(conn, "daily_brief_runs", column_name):
                conn.execute(f'ALTER TABLE "daily_brief_runs" ADD COLUMN "{column_name}" {definition}')
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_cma_assumptions_active
        ON cma_assumptions (sleeve_key, version_label)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_benchmark_definitions_context_version
        ON benchmark_definitions (context_key, version_label)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ips_snapshots (
          ips_snapshot_id TEXT PRIMARY KEY,
          brief_run_id TEXT NOT NULL,
          profile_id TEXT NOT NULL,
          benchmark_definition_id TEXT,
          cma_version TEXT,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_ips_snapshots_brief
        ON ips_snapshots (brief_run_id, created_at DESC)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_dca_policies_profile_name
        ON dca_policies (profile_id, policy_name)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_daily_brief_approvals_run
        ON daily_brief_approvals (brief_run_id)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS brief_ack_events (
          ack_event_id TEXT PRIMARY KEY,
          brief_run_id TEXT NOT NULL,
          recipient TEXT NOT NULL,
          ack_state TEXT NOT NULL,
          actor TEXT,
          occurred_at TEXT NOT NULL,
          details_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_brief_ack_events_run
        ON brief_ack_events (brief_run_id, occurred_at DESC)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_brief_versions_run
        ON brief_versions (brief_run_id, created_at DESC)
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_chart_artifacts_run
        ON chart_artifacts (brief_run_id, chart_key, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_history (
          regime_history_id TEXT PRIMARY KEY,
          brief_run_id TEXT NOT NULL,
          as_of_ts TEXT NOT NULL,
          long_state TEXT NOT NULL,
          short_state TEXT NOT NULL,
          change_summary TEXT,
          confidence_label TEXT,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_regime_history_asof
        ON regime_history (as_of_ts DESC, brief_run_id)
        """
    )
    conn.commit()


def _ensure_auth_and_account_scope_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
          user_id TEXT PRIMARY KEY,
          username TEXT NOT NULL UNIQUE,
          display_name TEXT NOT NULL,
          email TEXT,
          password_hash TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          created_at TEXT NOT NULL,
          last_active_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS roles (
          role_id TEXT PRIMARY KEY,
          role_name TEXT NOT NULL UNIQUE,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_roles (
          user_role_id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          role_name TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_user_roles_user_role
        ON user_roles (user_id, role_name)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_sessions (
          session_id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          session_token_hash TEXT NOT NULL UNIQUE,
          issued_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          revoked_at TEXT,
          source_ip TEXT,
          user_agent TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_auth_sessions_user
        ON auth_sessions (user_id, expires_at DESC)
        """
    )
    for table_name, columns in (
        (
            "account_entities",
            (
                ("account_name", "TEXT"),
                ("status", "TEXT NOT NULL DEFAULT 'active'"),
                ("account_type", "TEXT"),
                ("is_active", "INTEGER NOT NULL DEFAULT 1"),
            ),
        ),
        (
            "review_items",
            (("account_id", "TEXT"),),
        ),
    ):
        for column_name, definition in columns:
            if table_exists(conn, table_name) and not column_exists(conn, table_name, column_name):
                conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {definition}')
    conn.commit()


def _ensure_policy_assumption_governance_tables(conn: sqlite3.Connection) -> None:
    ensure_policy_assumption_tables(conn)


def _ensure_regime_methodology_registry(conn: sqlite3.Connection) -> None:
    ensure_regime_methodology_tables(conn)


def _ensure_provider_routing_tables(conn: sqlite3.Connection) -> None:
    ensure_provider_budget_tables(conn)


def _apply_migration(conn: sqlite3.Connection, version: int) -> None:
    if version == 1:
        # schema_meta bootstrap version.
        return
    if version == 2:
        _ensure_run_id_columns(conn)
        _ensure_outbox_artifacts_table(conn)
        return
    if version == 3:
        _ensure_optional_indexes(conn)
        return
    if version == 4:
        _ensure_portfolio_control_tables(conn)
        return
    if version == 5:
        _ensure_blueprint_tables(conn)
        return
    if version == 6:
        _ensure_review_tables(conn)
        return
    if version == 7:
        _ensure_daily_brief_tables(conn)
        return
    if version == 8:
        _ensure_portfolio_upload_registry_columns(conn)
        return
    if version == 9:
        _ensure_limit_and_exposure_tables(conn)
        return
    if version == 10:
        _ensure_review_workflow_extensions(conn)
        return
    if version == 11:
        _ensure_audit_tables(conn)
        return
    if version == 12:
        _ensure_sla_and_availability_tables(conn)
        return
    if version == 13:
        _ensure_monitoring_extension_tables(conn)
        return
    if version == 14:
        _ensure_blueprint_extension_tables(conn)
        return
    if version == 15:
        _ensure_liquidity_and_account_tables(conn)
        return
    if version == 16:
        _ensure_liquidity_and_account_tables(conn)
        return
    if version == 17:
        _ensure_stress_history_tables(conn)
        return
    if version == 18:
        _ensure_classification_precision_tables(conn)
        return
    if version == 19:
        _ensure_scenario_registry_tables(conn)
        return
    if version == 20:
        _ensure_daily_brief_upgrade_tables(conn)
        return
    if version == 21:
        _ensure_auth_and_account_scope_tables(conn)
        return
    if version == 22:
        ensure_blueprint_decision_tables(conn)
        ensure_quality_tables(conn)
        ensure_recommendation_tables(conn)
        return
    if version == 23:
        ensure_benchmark_registry_tables(conn)
        ensure_candidate_comparison_tables(conn)
        return
    if version == 24:
        ensure_candidate_registry_tables(conn)
        return
    if version == 25:
        ensure_candidate_truth_tables(conn)
        seed_required_field_matrix(conn)
        return
    if version == 26:
        _ensure_policy_assumption_governance_tables(conn)
        return
    if version == 27:
        _ensure_regime_methodology_registry(conn)
        return
    if version == 28:
        _ensure_provider_routing_tables(conn)
        return
    if version == 29:
        _ensure_symbol_resolution(conn)
        return
    if version == 30:
        ensure_provider_budget_tables(conn)
        return
    if version == 31:
        ensure_provider_family_success_tables(conn)
        ensure_public_upstream_snapshot_tables(conn)
        return
    if version == 32:
        ensure_provider_family_success_tables(conn)
        ensure_symbol_resolution_tables(conn)
        return
    if version == 33:
        # Daily Brief grounding/support fields are runtime-level and do not require
        # a schema change beyond ensuring the latest upstream support tables exist.
        ensure_provider_family_success_tables(conn)
        ensure_symbol_resolution_tables(conn)
        return
    if version == 34:
        ensure_daily_brief_regeneration_tables(conn)
        return
    if version == 35:
        ensure_blueprint_market_tables(conn)
        return
    if version == 36:
        _ensure_email_run_slot_columns(conn)
        return
    raise ValueError(f"Unsupported migration version: {version}")


def apply_schema_migrations(
    conn: sqlite3.Connection,
    target_version: int = SCHEMA_VERSION,
) -> dict[str, Any]:
    _ensure_schema_meta_table(conn)
    before = _current_schema_version(conn)
    applied: list[int] = []

    if before >= int(target_version):
        return {"before": before, "after": before, "applied": applied}

    for version in range(before + 1, int(target_version) + 1):
        _apply_migration(conn, version)
        _set_schema_version(conn, version)
        applied.append(version)

    after = _current_schema_version(conn)
    return {"before": before, "after": after, "applied": applied}
