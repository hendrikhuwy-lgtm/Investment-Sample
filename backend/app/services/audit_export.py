from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.services.audit_log import ensure_audit_tables, log_audit_event


OUTBOX_DIR = Path(__file__).resolve().parents[3] / "outbox" / "audit"


def create_audit_export(
    conn: sqlite3.Connection,
    *,
    payload: dict[str, Any],
    generated_by: str,
    export_scope: str = "dashboard_state",
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_audit_tables(conn)
    OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
    export_id = f"audit_export_{uuid.uuid4().hex[:12]}"
    generated_at = datetime.now(UTC).isoformat()
    file_path = OUTBOX_DIR / f"{export_id}.json"
    file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    conn.execute(
        """
        INSERT INTO audit_exports (export_id, export_scope, generated_at, generated_by, filters_json, file_path)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (export_id, export_scope, generated_at, generated_by, json.dumps(filters or {}), str(file_path)),
    )
    conn.commit()
    log_audit_event(
        conn,
        actor=generated_by,
        action_type="audit_export_run",
        object_type="audit_export",
        object_id=export_id,
        after={"export_scope": export_scope, "file_path": str(file_path)},
    )
    return {
        "export_id": export_id,
        "generated_at": generated_at,
        "generated_by": generated_by,
        "export_scope": export_scope,
        "filters": filters or {},
        "file_path": str(file_path),
    }


def list_audit_exports(conn: sqlite3.Connection, *, limit: int = 100) -> list[dict[str, Any]]:
    ensure_audit_tables(conn)
    rows = conn.execute(
        """
        SELECT export_id, export_scope, generated_at, generated_by, filters_json, file_path
        FROM audit_exports
        ORDER BY generated_at DESC
        LIMIT ?
        """,
        (max(1, limit),),
    ).fetchall()
    return [
        {
            "export_id": str(row["export_id"]),
            "export_scope": str(row["export_scope"]),
            "generated_at": str(row["generated_at"]),
            "generated_by": str(row["generated_by"]),
            "filters": json.loads(str(row["filters_json"] or "{}")),
            "file_path": str(row["file_path"]),
        }
        for row in rows
    ]
