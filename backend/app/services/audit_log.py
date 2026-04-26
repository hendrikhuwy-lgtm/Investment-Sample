from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_audit_tables(conn: sqlite3.Connection) -> None:
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
        CREATE INDEX IF NOT EXISTS ix_audit_events_occurred_at
        ON audit_events (occurred_at DESC, action_type)
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
    existing = conn.execute("SELECT 1 FROM retention_policies LIMIT 1").fetchone()
    if existing is None:
        now = _now_iso()
        defaults = [
            ("audit_events", 3650),
            ("audit_exports", 3650),
            ("review_items", 1825),
            ("portfolio_upload_runs", 3650),
        ]
        for object_type, retention_days in defaults:
            conn.execute(
                """
                INSERT INTO retention_policies (policy_id, object_type, retention_days, soft_delete_only, created_at, updated_at)
                VALUES (?, ?, ?, 1, ?, ?)
                """,
                (f"retention_{uuid.uuid4().hex[:12]}", object_type, retention_days, now, now),
            )
    conn.commit()


def log_audit_event(
    conn: sqlite3.Connection,
    *,
    actor: str,
    action_type: str,
    object_type: str,
    object_id: str | None = None,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    source_ip: str | None = None,
    user_agent: str | None = None,
) -> str:
    ensure_audit_tables(conn)
    audit_event_id = f"audit_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO audit_events (
          audit_event_id, actor, action_type, object_type, object_id, before_json, after_json,
          source_ip, user_agent, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            audit_event_id,
            actor,
            action_type,
            object_type,
            object_id,
            json.dumps(before or {}) if before is not None else None,
            json.dumps(after or {}) if after is not None else None,
            source_ip,
            user_agent,
            _now_iso(),
        ),
    )
    conn.commit()
    return audit_event_id


def list_audit_events(conn: sqlite3.Connection, *, limit: int = 200) -> list[dict[str, Any]]:
    ensure_audit_tables(conn)
    rows = conn.execute(
        """
        SELECT audit_event_id, actor, action_type, object_type, object_id, before_json, after_json, source_ip, user_agent, occurred_at
        FROM audit_events
        ORDER BY occurred_at DESC
        LIMIT ?
        """,
        (max(1, limit),),
    ).fetchall()
    return [
        {
            "audit_event_id": str(row["audit_event_id"]),
            "actor": str(row["actor"]),
            "action_type": str(row["action_type"]),
            "object_type": str(row["object_type"]),
            "object_id": row["object_id"],
            "before_json": json.loads(str(row["before_json"])) if row["before_json"] else None,
            "after_json": json.loads(str(row["after_json"])) if row["after_json"] else None,
            "source_ip": row["source_ip"],
            "user_agent": row["user_agent"],
            "occurred_at": row["occurred_at"],
        }
        for row in rows
    ]
