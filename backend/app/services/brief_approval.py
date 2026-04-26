from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


VALID_APPROVAL_STATES = {"generated", "reviewed", "approved", "sent", "rejected"}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_approval_tables(conn: sqlite3.Connection) -> None:
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
    conn.commit()


def create_or_refresh_approval(
    conn: sqlite3.Connection,
    *,
    brief_run_id: str,
    status: str = "generated",
    notes: str | None = None,
) -> dict[str, Any]:
    ensure_approval_tables(conn)
    now = _now_iso()
    row = conn.execute(
        "SELECT approval_id FROM daily_brief_approvals WHERE brief_run_id = ? LIMIT 1",
        (brief_run_id,),
    ).fetchone()
    if row is None:
        approval_id = f"brief_approval_{uuid.uuid4().hex[:12]}"
        conn.execute(
            """
            INSERT INTO daily_brief_approvals (
              approval_id, brief_run_id, approval_status, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (approval_id, brief_run_id, status, notes, now, now),
        )
        conn.commit()
        return get_approval(conn, brief_run_id) or {}
    conn.execute(
        """
        UPDATE daily_brief_approvals
        SET approval_status = ?, notes = COALESCE(?, notes), updated_at = ?
        WHERE brief_run_id = ?
        """,
        (status, notes, now, brief_run_id),
    )
    conn.commit()
    return get_approval(conn, brief_run_id) or {}


def get_approval(conn: sqlite3.Connection, brief_run_id: str) -> dict[str, Any] | None:
    ensure_approval_tables(conn)
    row = conn.execute(
        "SELECT * FROM daily_brief_approvals WHERE brief_run_id = ? LIMIT 1",
        (brief_run_id,),
    ).fetchone()
    return dict(row) if row is not None else None


def mark_reviewed(conn: sqlite3.Connection, brief_run_id: str, reviewed_by: str, notes: str | None = None) -> dict[str, Any]:
    create_or_refresh_approval(conn, brief_run_id=brief_run_id)
    now = _now_iso()
    conn.execute(
        """
        UPDATE daily_brief_approvals
        SET approval_status = 'reviewed', reviewed_by = ?, reviewed_at = ?, notes = COALESCE(?, notes), updated_at = ?
        WHERE brief_run_id = ?
        """,
        (reviewed_by, now, notes, now, brief_run_id),
    )
    conn.commit()
    return get_approval(conn, brief_run_id) or {}


def approve_brief(conn: sqlite3.Connection, brief_run_id: str, approved_by: str, notes: str | None = None) -> dict[str, Any]:
    create_or_refresh_approval(conn, brief_run_id=brief_run_id)
    now = _now_iso()
    conn.execute(
        """
        UPDATE daily_brief_approvals
        SET approval_status = 'approved', approved_by = ?, approved_at = ?, notes = COALESCE(?, notes), updated_at = ?
        WHERE brief_run_id = ?
        """,
        (approved_by, now, notes, now, brief_run_id),
    )
    conn.commit()
    return get_approval(conn, brief_run_id) or {}


def reject_brief(conn: sqlite3.Connection, brief_run_id: str, actor: str, reason: str) -> dict[str, Any]:
    create_or_refresh_approval(conn, brief_run_id=brief_run_id)
    now = _now_iso()
    conn.execute(
        """
        UPDATE daily_brief_approvals
        SET approval_status = 'rejected',
            reviewed_by = COALESCE(reviewed_by, ?),
            reviewed_at = COALESCE(reviewed_at, ?),
            rejection_reason = ?,
            updated_at = ?
        WHERE brief_run_id = ?
        """,
        (actor, now, reason, now, brief_run_id),
    )
    conn.commit()
    return get_approval(conn, brief_run_id) or {}


def is_approved(conn: sqlite3.Connection, brief_run_id: str) -> bool:
    row = get_approval(conn, brief_run_id)
    return str((row or {}).get("approval_status") or "") in {"approved", "sent"}
