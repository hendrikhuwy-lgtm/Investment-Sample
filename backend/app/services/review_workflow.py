from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any


WORKFLOW_STATUSES = {
    "open",
    "acknowledged",
    "assigned",
    "in_progress",
    "pending_approval",
    "resolved",
    "dismissed",
    "escalated",
    "archived",
}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_review_workflow_tables(conn: sqlite3.Connection) -> None:
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
        rows = conn.execute('PRAGMA table_info("review_items")').fetchall()
        if not any(str(row[1]) == column_name for row in rows):
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
        CREATE INDEX IF NOT EXISTS ix_review_events_review
        ON review_events (review_id, occurred_at DESC)
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
    existing = conn.execute("SELECT 1 FROM escalation_rules LIMIT 1").fetchone()
    if existing is None:
        now = _now_iso()
        defaults = [
            ("critical", "critical", 4, 1),
            ("high", "high", 24, 2),
            ("stale_price", "medium", 24, 2),
        ]
        for category, severity, overdue_hours, persistence_runs in defaults:
            conn.execute(
                """
                INSERT INTO escalation_rules (
                  rule_id, category, severity, overdue_hours, persistence_runs, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (f"escalation_{uuid.uuid4().hex[:12]}", category, severity, overdue_hours, persistence_runs, now, now),
            )
    conn.commit()


def _event(conn: sqlite3.Connection, *, review_id: str, prior_status: str | None, new_status: str, actor: str, reason: str | None) -> None:
    conn.execute(
        """
        INSERT INTO review_events (review_event_id, review_id, prior_status, new_status, actor, reason, occurred_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (f"review_event_{uuid.uuid4().hex[:12]}", review_id, prior_status, new_status, actor, reason, _now_iso()),
    )


def get_review_item_detail(conn: sqlite3.Connection, review_id: str) -> dict[str, Any] | None:
    ensure_review_workflow_tables(conn)
    row = conn.execute("SELECT * FROM review_items WHERE review_id = ? LIMIT 1", (review_id,)).fetchone()
    if row is None:
        return None
    payload = dict(row)
    payload["events"] = list_review_events(conn, review_id)
    return payload


def list_review_events(conn: sqlite3.Connection, review_id: str) -> list[dict[str, Any]]:
    ensure_review_workflow_tables(conn)
    rows = conn.execute(
        """
        SELECT review_event_id, review_id, prior_status, new_status, actor, reason, occurred_at
        FROM review_events
        WHERE review_id = ?
        ORDER BY occurred_at DESC
        """,
        (review_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def assign_review_item(
    conn: sqlite3.Connection,
    *,
    review_id: str,
    assignee_name: str,
    assignee_role: str | None,
    actor: str,
) -> dict[str, Any]:
    ensure_review_workflow_tables(conn)
    row = conn.execute("SELECT status FROM review_items WHERE review_id = ? LIMIT 1", (review_id,)).fetchone()
    if row is None:
        raise ValueError("Review item not found.")
    prior = str(row["status"])
    now = _now_iso()
    conn.execute(
        """
        UPDATE review_items
        SET status = ?, owner = ?, assignee_name = ?, assignee_role = ?, assigned_at = ?, assigned_by = ?, updated_at = ?
        WHERE review_id = ?
        """,
        ("assigned", assignee_name, assignee_name, assignee_role, now, actor, now, review_id),
    )
    _event(conn, review_id=review_id, prior_status=prior, new_status="assigned", actor=actor, reason=f"Assigned to {assignee_name}.")
    conn.commit()
    return get_review_item_detail(conn, review_id) or {}


def update_review_status(
    conn: sqlite3.Connection,
    *,
    review_id: str,
    new_status: str,
    actor: str,
    reason: str | None = None,
) -> dict[str, Any]:
    ensure_review_workflow_tables(conn)
    if new_status not in WORKFLOW_STATUSES:
        raise ValueError("Unsupported review status.")
    row = conn.execute("SELECT status FROM review_items WHERE review_id = ? LIMIT 1", (review_id,)).fetchone()
    if row is None:
        raise ValueError("Review item not found.")
    prior = str(row["status"])
    now = _now_iso()
    extra_sql = ""
    extra_values: list[Any] = []
    if new_status == "acknowledged":
        extra_sql = ", acknowledged_at = ?"
        extra_values.append(now)
    if new_status == "escalated":
        extra_sql = ", escalated_at = ?"
        extra_values.append(now)
    if new_status in {"resolved", "dismissed", "archived"}:
        extra_sql = ", resolved_at = ?"
        extra_values.append(now)
    conn.execute(
        f"UPDATE review_items SET status = ?, updated_at = ?{extra_sql} WHERE review_id = ?",
        tuple([new_status, now, *extra_values, review_id]),
    )
    _event(conn, review_id=review_id, prior_status=prior, new_status=new_status, actor=actor, reason=reason)
    conn.commit()
    return get_review_item_detail(conn, review_id) or {}


def resolve_review_item_structured(
    conn: sqlite3.Connection,
    *,
    review_id: str,
    resolution_type: str,
    resolution_summary: str,
    resolution_notes: str | None,
    actor: str,
) -> dict[str, Any]:
    ensure_review_workflow_tables(conn)
    row = conn.execute("SELECT status FROM review_items WHERE review_id = ? LIMIT 1", (review_id,)).fetchone()
    if row is None:
        raise ValueError("Review item not found.")
    prior = str(row["status"])
    now = _now_iso()
    conn.execute(
        """
        UPDATE review_items
        SET status = 'resolved',
            resolution_type = ?,
            resolution_summary = ?,
            resolution_notes = ?,
            resolved_by = ?,
            resolved_at = ?,
            updated_at = ?
        WHERE review_id = ?
        """,
        (resolution_type, resolution_summary, resolution_notes, actor, now, now, review_id),
    )
    _event(conn, review_id=review_id, prior_status=prior, new_status="resolved", actor=actor, reason=resolution_summary)
    conn.commit()
    return get_review_item_detail(conn, review_id) or {}


def apply_escalation_rules(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_review_workflow_tables(conn)
    rules = conn.execute(
        """
        SELECT category, severity, overdue_hours
        FROM escalation_rules
        WHERE enabled = 1
        """
    ).fetchall()
    now = datetime.now(UTC)
    escalated: list[dict[str, Any]] = []
    for row in rules:
        overdue_hours = int(row["overdue_hours"] or 0)
        if overdue_hours <= 0:
            continue
        cutoff = (now - timedelta(hours=overdue_hours)).date().isoformat()
        items = conn.execute(
            """
            SELECT review_id, status, category, severity, due_date
            FROM review_items
            WHERE status NOT IN ('resolved', 'dismissed', 'archived')
              AND due_date IS NOT NULL
              AND due_date <= ?
              AND (category = ? OR severity = ?)
            """,
            (cutoff, str(row["category"]), str(row["severity"])),
        ).fetchall()
        for item in items:
            detail = update_review_status(
                conn,
                review_id=str(item["review_id"]),
                new_status="escalated",
                actor="system",
                reason="Automatic escalation due to overdue review item.",
            )
            escalated.append(detail)
    return escalated
