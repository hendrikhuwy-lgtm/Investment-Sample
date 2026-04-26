from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.config import Settings, get_db_path
from app.models.db import connect, init_db
from app.services.alerts_email import send_narrated_brief_email
from app.services.brief_delivery_state import can_send_brief
from app.services.daily_brief_slots import current_slot_info, settings_slot_hours
from app.services.history_log import generate_daily_log


BACKEND_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = BACKEND_ROOT / "app" / "storage" / "schema.sql"
CHINA_TZ = ZoneInfo("Asia/Shanghai")


def _ensure_email_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(email_runs)").fetchall()}
    if "run_slot_key" not in columns:
        conn.execute("ALTER TABLE email_runs ADD COLUMN run_slot_key TEXT")
    if "run_slot_label" not in columns:
        conn.execute("ALTER TABLE email_runs ADD COLUMN run_slot_label TEXT")
    conn.execute("DROP INDEX IF EXISTS ux_email_runs_date_sent")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_email_runs_slot_sent
        ON email_runs (run_slot_key)
        WHERE status = 'sent' AND run_slot_key IS NOT NULL
        """
    )
    conn.commit()


def _has_sent_for_slot(conn: sqlite3.Connection, run_slot_key: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM email_runs WHERE run_slot_key = ? AND status = 'sent' LIMIT 1",
        (run_slot_key,),
    ).fetchone()
    return row is not None


def _insert_email_run(conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO email_runs (
            run_date_sgt, run_slot_key, run_slot_label, attempted_at, status, recipient, subject, run_id, md_path, html_path,
            cached_used, mcp_connected_count, mcp_total_count, citations_count, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(payload.get("run_date_sgt", "")),
            str(payload.get("run_slot_key", "")) or None,
            str(payload.get("run_slot_label", "")) or None,
            str(payload.get("attempted_at", datetime.now(UTC).isoformat())),
            str(payload.get("status", "unknown")),
            str(payload.get("recipient", "")),
            payload.get("subject"),
            payload.get("run_id"),
            payload.get("md_path"),
            payload.get("html_path"),
            1 if bool(payload.get("cached_used", False)) else 0,
            payload.get("mcp_connected_count"),
            payload.get("mcp_total_count"),
            payload.get("citations_count"),
            payload.get("error"),
        ),
    )
    conn.commit()


def run_daily_brief_once(
    settings: Settings | None = None,
    *,
    now_utc: datetime | None = None,
    force_send: bool = False,
    send_email: bool = True,
    force_cache_only: bool = False,
    brief_mode: str | None = None,
    audience_preset: str | None = None,
) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    now_utc = now_utc or datetime.now(UTC)
    slot_info = current_slot_info(now_utc, hours=settings_slot_hours(settings))
    run_date_sgt = str(slot_info.get("slot_date_china") or now_utc.astimezone(CHINA_TZ).date().isoformat())
    run_slot_key = str(slot_info.get("slot_key") or "")
    run_slot_label = str(slot_info.get("slot_label") or "")
    attempted_at = now_utc.isoformat()

    db_path = get_db_path(settings=settings)
    conn = connect(db_path)
    init_db(conn, SCHEMA_PATH)
    _ensure_email_runs_table(conn)

    try:
        already_sent_for_slot = _has_sent_for_slot(conn, run_slot_key)
        run_settings = replace(settings, mcp_live_required=settings.daily_brief_mcp_live_required)
        result = generate_daily_log(
            settings=run_settings,
            force_cache_only=force_cache_only,
            brief_mode=brief_mode,
            audience_preset=audience_preset,
        )
        macro_result = result.get("macro_result", {})

        md_path = str(macro_result.get("md_path", ""))
        html_path = str(macro_result.get("html_path", ""))
        md_body = Path(md_path).read_text(encoding="utf-8")
        html_body = Path(html_path).read_text(encoding="utf-8")
        alert_count = int(macro_result.get("alert_count", 0))
        run_id = str(macro_result.get("run_id") or "")
        freshness_ok = bool(macro_result.get("freshness_ok", True))
        citations_ok = int(macro_result.get("citations_count", 0)) > 0
        run_complete = bool(run_id and md_path and html_path)
        requested_audience = (audience_preset or settings.daily_brief_default_audience or "pm").strip().lower()
        delivery_profile = (
            "investment_guidance" if requested_audience in {"client", "client_friendly"} else "market_monitoring"
        )
        send_gate_ok, send_gate_reason = can_send_brief(
            conn,
            brief_run_id=run_id,
            approval_required=bool(settings.daily_brief_require_approval_before_send),
            citations_ok=citations_ok,
            freshness_ok=freshness_ok,
            run_complete=run_complete,
            policy_guidance_ready=bool(macro_result.get("policy_guidance_ready", False)),
            delivery_profile=delivery_profile,
            force_override=force_send,
        )
        should_send = send_email and send_gate_ok and (force_send or (not already_sent_for_slot and alert_count > 0))

        if should_send:
            send_narrated_brief_email(
                settings=settings,
                subject=str(macro_result.get("subject", "Investment Agent Daily Brief")),
                markdown_body=md_body,
                html_body=html_body,
            )
            if run_id:
                conn.execute(
                    "UPDATE daily_brief_runs SET delivery_state = 'sent' WHERE brief_run_id = ?",
                    (run_id,),
                )
                conn.execute(
                    "UPDATE daily_brief_approvals SET approval_status = 'sent', updated_at = ? WHERE brief_run_id = ? AND approval_status = 'approved'",
                    (datetime.now(UTC).isoformat(), run_id),
                )
                conn.commit()
            status = "sent"
        else:
            status = "generated_no_send"
            if run_id:
                delivery_state = "generated"
                if settings.daily_brief_require_approval_before_send and send_gate_reason == "approval_required_absent":
                    delivery_state = "reviewed"
                conn.execute(
                    "UPDATE daily_brief_runs SET delivery_state = ? WHERE brief_run_id = ?",
                    (delivery_state, run_id),
                )
                conn.commit()

        payload = {
            "status": status,
            "run_date_sgt": run_date_sgt,
            "run_slot_key": run_slot_key,
            "run_slot_label": run_slot_label,
            "attempted_at": attempted_at,
            "run_date_china": run_date_sgt,
            "attempted_at_china": now_utc.astimezone(CHINA_TZ).isoformat(),
            "recipient": settings.alert_to,
            "subject": macro_result.get("subject"),
            "run_id": run_id,
            "md_path": md_path,
            "html_path": html_path,
            "cached_used": bool(macro_result.get("cached_used", False)),
            "mcp_connected_count": macro_result.get("mcp_connected_count"),
            "mcp_total_count": macro_result.get("mcp_total_count"),
            "citations_count": macro_result.get("citations_count"),
            "send_gate_reason": send_gate_reason,
            "delivery_profile": delivery_profile,
        }
        if already_sent_for_slot and not force_send:
            payload["reason"] = "already_sent_for_slot"
        elif send_email and not send_gate_ok:
            payload["reason"] = send_gate_reason
        elif send_email and not should_send and not force_send:
            payload["reason"] = "no_alert_trigger"
        _insert_email_run(conn, payload)
        conn.close()
        return payload
    except Exception as exc:
        payload = {
            "status": "failed",
            "run_date_sgt": run_date_sgt,
            "attempted_at": attempted_at,
            "recipient": settings.alert_to,
            "error": str(exc),
        }
        _insert_email_run(conn, payload)
        conn.close()
        raise


def run_nightly_storage_cleanup(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    db_path = get_db_path(settings=settings)
    conn = connect(db_path)
    init_db(conn, SCHEMA_PATH)
    _ensure_email_runs_table(conn)
    conn.close()
    return {
        "status": "completed",
        "cleaned_at": datetime.now(UTC).isoformat(),
        "scope": "minimal_runtime_housekeeping",
    }
