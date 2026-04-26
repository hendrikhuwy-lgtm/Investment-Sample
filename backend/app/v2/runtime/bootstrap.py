from __future__ import annotations

from pathlib import Path

from app.config import get_db_path
from app.models.db import connect, init_db
from app.services.blueprint_decisions import ensure_blueprint_decision_tables
from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, seed_default_candidate_registry
from app.services.blueprint_refresh_monitor import ensure_blueprint_refresh_tables
from app.services.blueprint_store import ensure_blueprint_tables
from app.services.history_log import ensure_history_tables
from app.services.ingest_etf_data import ensure_etf_tables, sync_configured_etf_data_sources
from app.services.portfolio_ingest import ensure_portfolio_control_tables
from app.services.portfolio_pricing import ensure_pricing_tables
from app.services.portfolio_state import ensure_portfolio_tables
from app.services.provider_budget import ensure_provider_budget_tables
from app.services.provider_family_success import ensure_provider_family_success_tables
from app.services.public_upstream_snapshots import ensure_public_upstream_snapshot_tables
from app.services.symbol_resolution import ensure_symbol_resolution_tables


_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "storage" / "schema.sql"


def ensure_v2_runtime_bootstrap() -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn, _SCHEMA_PATH)
        ensure_portfolio_tables(conn)
        ensure_history_tables(conn)
        ensure_pricing_tables(conn)
        ensure_portfolio_control_tables(conn)
        ensure_blueprint_tables(conn)
        ensure_candidate_registry_tables(conn)
        seed_default_candidate_registry(conn)
        ensure_etf_tables(conn)
        sync_configured_etf_data_sources(conn)
        ensure_blueprint_refresh_tables(conn)
        ensure_blueprint_decision_tables(conn)
        ensure_provider_budget_tables(conn)
        ensure_provider_family_success_tables(conn)
        ensure_public_upstream_snapshot_tables(conn)
        ensure_symbol_resolution_tables(conn)
    finally:
        conn.close()
