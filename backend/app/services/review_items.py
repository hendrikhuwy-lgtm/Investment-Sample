from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_review_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS review_items (
          review_id TEXT PRIMARY KEY,
          review_key TEXT NOT NULL,
          category TEXT NOT NULL,
          severity TEXT NOT NULL,
          account_id TEXT,
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
    cols = conn.execute('PRAGMA table_info("review_items")').fetchall()
    if not any(str(row[1]) == "account_id" for row in cols):
        conn.execute('ALTER TABLE "review_items" ADD COLUMN "account_id" TEXT')
    conn.commit()


def upsert_review_item(
    conn: sqlite3.Connection,
    *,
    review_key: str,
    category: str,
    severity: str,
    linked_object_type: str | None,
    linked_object_id: str | None,
    source_run_id: str | None,
    notes: str,
    owner: str | None = None,
    account_id: str | None = None,
    due_in_days: int = 1,
) -> None:
    ensure_review_tables(conn)
    now = _now_iso()
    due_date = (datetime.now(UTC) + timedelta(days=max(0, due_in_days))).date().isoformat()
    existing = conn.execute(
        "SELECT review_id, status FROM review_items WHERE review_key = ?",
        (review_key,),
    ).fetchone()
    review_id = str(existing["review_id"]) if existing is not None else f"review_{uuid.uuid4().hex[:12]}"
    status = str(existing["status"]) if existing is not None else "open"
    conn.execute(
        """
        INSERT INTO review_items (
          review_id, review_key, category, severity, account_id, owner, due_date, status, notes,
          linked_object_type, linked_object_id, source_run_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(review_key) DO UPDATE SET
          category=excluded.category,
          severity=excluded.severity,
          account_id=excluded.account_id,
          owner=excluded.owner,
          due_date=excluded.due_date,
          notes=excluded.notes,
          linked_object_type=excluded.linked_object_type,
          linked_object_id=excluded.linked_object_id,
          source_run_id=excluded.source_run_id,
          updated_at=excluded.updated_at
        """,
        (
            review_id,
            review_key,
            category,
            severity,
            account_id,
            owner,
            due_date,
            status,
            notes,
            linked_object_type,
            linked_object_id,
            source_run_id,
            now,
            now,
        ),
    )
    conn.commit()


def list_review_items(conn: sqlite3.Connection, *, status: str | None = None, account_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    ensure_review_tables(conn)
    params: list[Any] = []
    clauses: list[str] = []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if account_id:
        clauses.append("(account_id = ? OR account_id IS NULL)")
        params.append(account_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(max(1, limit))
    rows = conn.execute(
        f"""
        SELECT review_id, review_key, category, severity, account_id, owner, due_date, status, notes,
               linked_object_type, linked_object_id, source_run_id, created_at, updated_at
        FROM review_items
        {where}
        ORDER BY
          CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
          updated_at DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [
        {
            "review_id": str(row["review_id"]),
            "review_key": str(row["review_key"]),
            "category": str(row["category"]),
            "severity": str(row["severity"]),
            "account_id": row["account_id"],
            "owner": row["owner"],
            "due_date": row["due_date"],
            "status": str(row["status"]),
            "notes": row["notes"],
            "linked_object_type": row["linked_object_type"],
            "linked_object_id": row["linked_object_id"],
            "source_run_id": row["source_run_id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def resolve_review_item(conn: sqlite3.Connection, *, review_key: str, resolution_note: str | None = None) -> None:
    ensure_review_tables(conn)
    existing = conn.execute(
        "SELECT review_id, notes FROM review_items WHERE review_key = ? LIMIT 1",
        (review_key,),
    ).fetchone()
    if existing is None:
        return
    note_parts = [str(existing["notes"] or "").strip()]
    if resolution_note:
        note_parts.append(str(resolution_note).strip())
    notes = " ".join(part for part in note_parts if part)
    conn.execute(
        """
        UPDATE review_items
        SET status = 'resolved',
            notes = ?,
            updated_at = ?
        WHERE review_key = ?
        """,
        (notes, _now_iso(), review_key),
    )
    conn.commit()
