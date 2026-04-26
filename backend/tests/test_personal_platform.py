from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from app.config import Settings
from app.models.db import connect, init_db
from app.services.history_log import ensure_history_tables, generate_daily_log, list_daily_logs
from app.services.personal_analysis import compute_allocation_drift
from app.services.personal_analysis import build_personal_portfolio_diagnostic
from app.services.portfolio_state import ensure_portfolio_tables, list_holdings, upsert_holding


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "personal.sqlite3"),
        mcp_live_required=False,
        refresh_live_cache_on_brief=False,
    )


def test_personal_diagnostic_builds(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    db_path = settings.resolved_db_path(Path(__file__).resolve().parents[2])
    conn = connect(db_path)
    try:
        init_db(conn, Path(__file__).resolve().parents[1] / "app" / "storage" / "schema.sql")
        ensure_portfolio_tables(conn)
        ensure_history_tables(conn)
        upsert_holding(
            conn,
            {
                "symbol": "CSPX",
                "name": "UCITS S&P 500",
                "quantity": 10,
                "cost_basis": 600,
                "currency": "USD",
                "sleeve": "global_equity",
                "account_type": "broker",
            },
        )
        upsert_holding(
            conn,
            {
                "symbol": "A35",
                "name": "SG Bond ETF",
                "quantity": 20,
                "cost_basis": 1.2,
                "currency": "SGD",
                "sleeve": "ig_bond",
                "account_type": "broker",
            },
        )
        holdings = list_holdings(conn)
        diagnostic = build_personal_portfolio_diagnostic(
            holdings,
            macro_context={"long_state": "Watch", "short_state": "Normal", "graph_metadata": []},
        )
    finally:
        conn.close()

    assert diagnostic.total_value > 0
    assert "global_equity" in diagnostic.actual_weights
    assert len(diagnostic.stress_scenarios) == 5
    assert 0 <= diagnostic.regime_alignment_score <= 100


def test_nonempty_holdings_yield_nonzero_totals(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    db_path = settings.resolved_db_path(Path(__file__).resolve().parents[2])
    conn = connect(db_path)
    try:
        init_db(conn, Path(__file__).resolve().parents[1] / "app" / "storage" / "schema.sql")
        ensure_portfolio_tables(conn)
        upsert_holding(
            conn,
            {
                "symbol": "VTI",
                "name": "US Equity ETF",
                "quantity": 12,
                "cost_basis": 250,
                "currency": "USD",
                "sleeve": "global_equity",
                "account_type": "broker",
            },
        )
        holdings = list_holdings(conn)
        allocation = compute_allocation_drift(holdings)
    finally:
        conn.close()
    assert allocation["total_value"] > 0
    assert any(float(value) > 0 for value in allocation["actual_weights"].values())


def test_generate_daily_log_persists(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    db_path = settings.resolved_db_path(Path(__file__).resolve().parents[2])
    conn = connect(db_path)
    init_db(conn, Path(__file__).resolve().parents[1] / "app" / "storage" / "schema.sql")
    ensure_portfolio_tables(conn)
    ensure_history_tables(conn)
    upsert_holding(
        conn,
        {
            "symbol": "DBMF",
            "name": "Managed Futures",
            "quantity": 5,
            "cost_basis": 25,
            "currency": "USD",
            "sleeve": "convex",
            "account_type": "broker",
        },
    )

    md_path = tmp_path / "sample.md"
    html_path = tmp_path / "sample.html"
    md_path.write_text("# sample", encoding="utf-8")
    html_path.write_text("<html>sample</html>", encoding="utf-8")

    def fake_macro(settings: Settings, force_cache_only: bool = False) -> dict:
        return {
            "subject": "SG Macro and Markets Brief, 2026-02-17 16:00 SGT, Signals {Long: Watch} {Short: Watch}",
            "run_id": "run_test",
            "md_path": str(md_path),
            "html_path": str(html_path),
            "long_state": "Watch",
            "short_state": "Watch",
            "alert_count": 1,
            "opportunity_count": 2,
            "citations_count": 10,
            "cached_used": True,
            "mcp_connected_count": 0,
            "mcp_total_count": 30,
            "mcp_connectable_count": 26,
            "signals_summary": {"graph_metadata": []},
            "errors": [],
        }

    monkeypatch.setattr("app.services.history_log.generate_mcp_omni_email_brief", fake_macro)
    payload = generate_daily_log(settings=settings, force_cache_only=True, conn=conn)
    logs = list_daily_logs(conn, limit=5)
    conn.close()

    assert payload["daily_log"]["regime_classification"] == "Watch"
    assert logs
    assert logs[0].macro_state_summary.startswith("SG Macro and Markets Brief")
