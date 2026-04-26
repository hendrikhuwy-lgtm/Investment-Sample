from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.services.brief_approval import get_approval


VALID_ACK_STATES = {"unopened", "opened", "acknowledged", "archived"}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_delivery_tables(conn: sqlite3.Connection) -> None:
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
    conn.commit()


def record_ack_event(
    conn: sqlite3.Connection,
    *,
    brief_run_id: str,
    recipient: str,
    ack_state: str,
    actor: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_delivery_tables(conn)
    occurred_at = _now_iso()
    conn.execute(
        """
        INSERT INTO brief_ack_events (
          ack_event_id, brief_run_id, recipient, ack_state, actor, occurred_at, details_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"brief_ack_{uuid.uuid4().hex[:12]}",
            brief_run_id,
            recipient,
            ack_state,
            actor,
            occurred_at,
            json.dumps(details or {}),
        ),
    )
    conn.commit()
    return {
        "brief_run_id": brief_run_id,
        "recipient": recipient,
        "ack_state": ack_state,
        "actor": actor,
        "occurred_at": occurred_at,
        "details": details or {},
    }


def latest_ack_state(conn: sqlite3.Connection, brief_run_id: str, recipient: str | None = None) -> dict[str, Any] | None:
    ensure_delivery_tables(conn)
    if recipient:
        row = conn.execute(
            """
            SELECT *
            FROM brief_ack_events
            WHERE brief_run_id = ? AND recipient = ?
            ORDER BY occurred_at DESC
            LIMIT 1
            """,
            (brief_run_id, recipient),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT *
            FROM brief_ack_events
            WHERE brief_run_id = ?
            ORDER BY occurred_at DESC
            LIMIT 1
            """,
            (brief_run_id,),
        ).fetchone()
    if row is None:
        return None
    payload = dict(row)
    payload["details"] = json.loads(str(payload.get("details_json") or "{}"))
    return payload


def record_brief_versions(
    conn: sqlite3.Connection,
    *,
    brief_run_id: str,
    content_version: str,
    policy_pack_version: str,
    benchmark_definition_version: str,
    cma_version: str,
    chart_version: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_delivery_tables(conn)
    created_at = _now_iso()
    conn.execute(
        """
        INSERT INTO brief_versions (
          version_id, brief_run_id, content_version, policy_pack_version,
          benchmark_definition_version, cma_version, chart_version, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"brief_version_{uuid.uuid4().hex[:12]}",
            brief_run_id,
            content_version,
            policy_pack_version,
            benchmark_definition_version,
            cma_version,
            chart_version,
            json.dumps(payload or {}),
            created_at,
        ),
    )
    conn.commit()
    return {
        "brief_run_id": brief_run_id,
        "content_version": content_version,
        "policy_pack_version": policy_pack_version,
        "benchmark_definition_version": benchmark_definition_version,
        "cma_version": cma_version,
        "chart_version": chart_version,
        "created_at": created_at,
    }


def latest_brief_versions(conn: sqlite3.Connection, brief_run_id: str) -> dict[str, Any] | None:
    ensure_delivery_tables(conn)
    row = conn.execute(
        """
        SELECT *
        FROM brief_versions
        WHERE brief_run_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (brief_run_id,),
    ).fetchone()
    if row is None:
        return None
    payload = dict(row)
    payload["payload"] = json.loads(str(payload.get("payload_json") or "{}"))
    return payload


def can_send_brief(
    conn: sqlite3.Connection,
    *,
    brief_run_id: str,
    approval_required: bool,
    citations_ok: bool,
    freshness_ok: bool,
    run_complete: bool,
    policy_guidance_ready: bool = False,
    delivery_profile: str = "market_monitoring",
    force_override: bool = False,
) -> tuple[bool, str]:
    if not citations_ok:
        return False, "citations_failed_validation"
    if delivery_profile == "investment_guidance" and not policy_guidance_ready and not force_override:
        return False, "policy_sections_unsourced_for_guidance_mode"
    if approval_required and not force_override:
        approval = get_approval(conn, brief_run_id)
        if str((approval or {}).get("approval_status") or "") != "approved":
            return False, "approval_required_absent"
    if not freshness_ok and not force_override:
        return False, "critical_data_freshness_sla_breached"
    if not run_complete:
        return False, "run_incomplete"
    return True, "ok"
