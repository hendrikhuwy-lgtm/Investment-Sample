from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import sqlite3

from app.config import Settings, get_db_path
from app.models.db import connect, init_db
from app.models.types import DailyLog, JournalEntry, PortfolioSnapshot
from app.services.personal_analysis import build_personal_portfolio_diagnostic
from app.services.ips import get_ips, policy_weights_from_ips
from app.services.portfolio_state import ensure_portfolio_tables, list_holdings, save_snapshot
from app.services.real_email_brief import generate_mcp_omni_email_brief
from app.services.delta_engine import build_and_persist_delta_state, ensure_delta_tables


PROJECT_ROOT = Path(__file__).resolve().parents[3]
BACKEND_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = BACKEND_ROOT / "app" / "storage" / "schema.sql"


def ensure_history_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )
    daily_log_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(daily_logs)").fetchall()}
    if "run_id" not in daily_log_cols:
        conn.execute("ALTER TABLE daily_logs ADD COLUMN run_id TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS journal_entries (
          entry_id TEXT PRIMARY KEY,
          created_at TEXT NOT NULL,
          thesis TEXT NOT NULL,
          concerns TEXT,
          mistakes_avoided TEXT,
          lessons TEXT
        )
        """
    )
    conn.commit()


def _daily_log_from_row(row: sqlite3.Row | Any) -> DailyLog:
    return DailyLog(
        log_id=str(row["log_id"]),
        created_at=datetime.fromisoformat(str(row["created_at"])),
        macro_state_summary=str(row["macro_state_summary"]),
        short_term_alert_state=str(row["short_term_alert_state"]),
        portfolio_snapshot_id=str(row["portfolio_snapshot_id"]) if row["portfolio_snapshot_id"] is not None else None,
        regime_classification=str(row["regime_classification"]),
        top_risk_flags=list(json.loads(str(row["top_risk_flags_json"]))),
        top_opportunity_flags=list(json.loads(str(row["top_opportunity_flags_json"]))),
        personal_alignment_score=float(row["personal_alignment_score"]),
    )


def list_daily_logs(conn: sqlite3.Connection, limit: int = 60) -> list[DailyLog]:
    ensure_history_tables(conn)
    rows = conn.execute(
        """
        SELECT log_id, created_at, macro_state_summary, short_term_alert_state, portfolio_snapshot_id,
               regime_classification, top_risk_flags_json, top_opportunity_flags_json, personal_alignment_score
        FROM daily_logs
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (max(1, limit),),
    ).fetchall()
    return [_daily_log_from_row(row) for row in rows]


def latest_daily_log(conn: sqlite3.Connection) -> DailyLog | None:
    logs = list_daily_logs(conn, limit=1)
    return logs[0] if logs else None


def _journal_from_row(row: sqlite3.Row | Any) -> JournalEntry:
    return JournalEntry(
        entry_id=str(row["entry_id"]),
        created_at=datetime.fromisoformat(str(row["created_at"])),
        thesis=str(row["thesis"]),
        concerns=str(row["concerns"]) if row["concerns"] is not None else None,
        mistakes_avoided=str(row["mistakes_avoided"]) if row["mistakes_avoided"] is not None else None,
        lessons=str(row["lessons"]) if row["lessons"] is not None else None,
    )


def list_journal_entries(conn: sqlite3.Connection, limit: int = 200) -> list[JournalEntry]:
    ensure_history_tables(conn)
    rows = conn.execute(
        """
        SELECT entry_id, created_at, thesis, concerns, mistakes_avoided, lessons
        FROM journal_entries
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (max(1, limit),),
    ).fetchall()
    return [_journal_from_row(row) for row in rows]


def add_journal_entry(
    conn: sqlite3.Connection,
    *,
    thesis: str,
    concerns: str | None = None,
    mistakes_avoided: str | None = None,
    lessons: str | None = None,
) -> JournalEntry:
    ensure_history_tables(conn)
    entry_id = f"journal_{uuid.uuid4().hex[:12]}"
    created_at = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO journal_entries (entry_id, created_at, thesis, concerns, mistakes_avoided, lessons)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (entry_id, created_at, thesis, concerns, mistakes_avoided, lessons),
    )
    conn.commit()
    return JournalEntry(
        entry_id=entry_id,
        created_at=datetime.fromisoformat(created_at),
        thesis=thesis,
        concerns=concerns,
        mistakes_avoided=mistakes_avoided,
        lessons=lessons,
    )


def _persist_daily_log(
    conn: sqlite3.Connection,
    *,
    run_id: str | None,
    macro_state_summary: str,
    short_term_alert_state: str,
    regime_classification: str,
    personal_alignment_score: float,
    top_risk_flags: list[str],
    top_opportunity_flags: list[str],
    portfolio_snapshot_id: str | None,
) -> DailyLog:
    ensure_history_tables(conn)
    log_id = f"log_{uuid.uuid4().hex[:12]}"
    created_at = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO daily_logs (
            log_id, run_id, created_at, macro_state_summary, short_term_alert_state, portfolio_snapshot_id,
            regime_classification, top_risk_flags_json, top_opportunity_flags_json, personal_alignment_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            log_id,
            run_id,
            created_at,
            macro_state_summary,
            short_term_alert_state,
            portfolio_snapshot_id,
            regime_classification,
            json.dumps(top_risk_flags),
            json.dumps(top_opportunity_flags),
            float(personal_alignment_score),
        ),
    )
    conn.commit()
    return DailyLog(
        log_id=log_id,
        created_at=datetime.fromisoformat(created_at),
        macro_state_summary=macro_state_summary,
        short_term_alert_state=short_term_alert_state,
        portfolio_snapshot_id=portfolio_snapshot_id,
        regime_classification=regime_classification,
        top_risk_flags=top_risk_flags,
        top_opportunity_flags=top_opportunity_flags,
        personal_alignment_score=float(personal_alignment_score),
    )


def generate_daily_log(
    settings: Settings | None = None,
    *,
    force_cache_only: bool = False,
    conn: sqlite3.Connection | None = None,
    brief_mode: str | None = None,
    audience_preset: str | None = None,
) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    own_conn = conn is None
    if conn is None:
        db_path = get_db_path(settings=settings)
        conn = connect(db_path)
        init_db(conn, SCHEMA_PATH)

    ensure_portfolio_tables(conn)
    ensure_history_tables(conn)
    ensure_delta_tables(conn)

    macro = generate_mcp_omni_email_brief(
        settings=settings,
        force_cache_only=force_cache_only,
        brief_mode=brief_mode,
        audience_preset=audience_preset,
    )

    holdings = list_holdings(conn)
    ips_profile = get_ips(conn)
    policy_weights = policy_weights_from_ips(ips_profile)
    macro_context = {
        "long_state": macro.get("long_state"),
        "short_state": macro.get("short_state"),
        "graph_metadata": macro.get("signals_summary", {}).get("graph_metadata", []),
    }
    diagnostic = build_personal_portfolio_diagnostic(
        holdings,
        macro_context=macro_context,
        policy_weights=policy_weights,
    )
    delta_state = build_and_persist_delta_state(
        conn,
        macro_result=macro,
        sleeve_weights=diagnostic.actual_weights,
        holdings_available=bool(holdings),
        asof_ts=datetime.now(UTC),
    )
    snapshot: PortfolioSnapshot = save_snapshot(
        conn,
        total_value=diagnostic.total_value,
        sleeve_weights=diagnostic.actual_weights,
        concentration_metrics=diagnostic.concentration_metrics,
        convex_coverage_ratio=float(diagnostic.convex_coverage.get("hedge_coverage_pct", 0.0)) / 100.0,
        tax_drag_estimate=diagnostic.tax_drag_estimate,
        notes="Auto-generated daily portfolio snapshot.",
    )

    top_risk_flags: list[str] = []
    top_opportunity_flags: list[str] = []
    if str(macro.get("long_state", "")).lower() in {"elevated", "stress_emerging", "stress_regime"}:
        top_risk_flags.append("Long-horizon regime is elevated versus neutral baseline.")
    if str(macro.get("short_state", "")).lower() in {"elevated", "stress_emerging", "stress_regime"}:
        top_risk_flags.append("Short-horizon monitoring state is elevated.")
    if int(macro.get("alert_count", 0)) > 0:
        top_risk_flags.append("Alert events were generated in current macro context.")
    if (
        (not bool(macro.get("mcp_live_gate_passed", False)))
        and bool(macro.get("cached_used", False))
        and int(macro.get("mcp_connected_count", 0)) < max(1, int(macro.get("mcp_connectable_count", 1)))
    ):
        top_risk_flags.append("MCP live coverage below full connectable scope; fallback labels should be monitored.")
    if int(delta_state["daily_state_change_summary"].get("escalated_count", 0)) > 0:
        top_risk_flags.append("Escalated state-change alerts were observed versus the prior snapshot.")

    if int(macro.get("opportunity_count", 0)) > 0:
        top_opportunity_flags.append("Opportunity observations were logged for monitoring.")
    if diagnostic.regime_alignment_score >= 70:
        top_opportunity_flags.append("Portfolio alignment score is within monitoring comfort band.")
    if float(diagnostic.convex_coverage.get("hedge_coverage_pct", 0.0)) >= 2.0:
        top_opportunity_flags.append("Convex sleeve coverage remains present in current snapshot.")

    daily_log = _persist_daily_log(
        conn,
        run_id=str(macro.get("run_id") or "") or None,
        macro_state_summary=str(macro.get("subject", "Macro context snapshot")),
        short_term_alert_state=str(macro.get("short_state", "Normal")),
        regime_classification=str(macro.get("long_state", "Normal")),
        personal_alignment_score=diagnostic.regime_alignment_score,
        top_risk_flags=top_risk_flags[:5],
        top_opportunity_flags=top_opportunity_flags[:5],
        portfolio_snapshot_id=snapshot.snapshot_id,
    )

    if own_conn:
        conn.close()

    return {
        "daily_log": daily_log.model_dump(mode="json"),
        "portfolio_snapshot": snapshot.model_dump(mode="json"),
        "personal_diagnostic": diagnostic.model_dump(mode="json"),
        "delta_state": delta_state,
        "macro_result": {
            "subject": macro.get("subject"),
            "run_id": macro.get("run_id"),
            "long_state": macro.get("long_state"),
            "short_state": macro.get("short_state"),
            "alert_count": macro.get("alert_count", 0),
            "opportunity_count": macro.get("opportunity_count", 0),
            "citations_count": macro.get("citations_count", 0),
            "md_path": macro.get("md_path"),
            "html_path": macro.get("html_path"),
            "cached_used": macro.get("cached_used", False),
            "mcp_connected_count": macro.get("mcp_connected_count", 0),
            "mcp_total_count": macro.get("mcp_total_count", 0),
            "mcp_connectable_count": macro.get("mcp_connectable_count", 0),
        },
    }
