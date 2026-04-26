from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.config import get_db_path


_VALID_STATUS = {"draft", "finalized", "archived"}
_MUTABLE_FIELDS = {
    "title",
    "thesis",
    "assumptions",
    "invalidation",
    "watch_items",
    "reflections",
    "next_review_date",
    "linked_object_type",
    "linked_object_id",
    "linked_object_label",
}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_notebook_entries (
          entry_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          linked_object_type TEXT NOT NULL,
          linked_object_id TEXT NOT NULL,
          linked_object_label TEXT NOT NULL,
          status TEXT NOT NULL,
          title TEXT NOT NULL,
          thesis TEXT NOT NULL,
          assumptions TEXT NOT NULL,
          invalidation TEXT NOT NULL,
          watch_items TEXT NOT NULL,
          reflections TEXT NOT NULL,
          next_review_date TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          finalized_at TEXT,
          archived_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_v2_notebook_entries_candidate
        ON v2_notebook_entries (candidate_id, status, updated_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_notebook_entry_history (
          revision_id TEXT PRIMARY KEY,
          entry_id TEXT NOT NULL,
          action TEXT NOT NULL,
          snapshot_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_v2_notebook_entry_history_entry
        ON v2_notebook_entry_history (entry_id, created_at DESC)
        """
    )
    conn.commit()


def _row_to_entry(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    return {
        "entry_id": str(item.get("entry_id") or ""),
        "candidate_id": str(item.get("candidate_id") or ""),
        "linked_object_type": str(item.get("linked_object_type") or "candidate"),
        "linked_object_id": str(item.get("linked_object_id") or ""),
        "linked_object_label": str(item.get("linked_object_label") or ""),
        "status": str(item.get("status") or "draft"),
        "title": str(item.get("title") or ""),
        "thesis": str(item.get("thesis") or ""),
        "assumptions": str(item.get("assumptions") or ""),
        "invalidation": str(item.get("invalidation") or ""),
        "watch_items": str(item.get("watch_items") or ""),
        "reflections": str(item.get("reflections") or ""),
        "next_review_date": item.get("next_review_date"),
        "created_at": str(item.get("created_at") or ""),
        "updated_at": str(item.get("updated_at") or ""),
        "finalized_at": item.get("finalized_at"),
        "archived_at": item.get("archived_at"),
    }


def _history_snapshot(entry: dict[str, Any], action: str) -> dict[str, Any]:
    return {
        "action": action,
        "entry": entry,
    }


def _record_history(conn: sqlite3.Connection, entry: dict[str, Any], action: str) -> None:
    conn.execute(
        """
        INSERT INTO v2_notebook_entry_history (
          revision_id, entry_id, action, snapshot_json, created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            f"notebook_revision_{uuid.uuid4().hex}",
            entry["entry_id"],
            action,
            json.dumps(_history_snapshot(entry, action), ensure_ascii=True),
            _now_iso(),
        ),
    )


def _fetch_entry(conn: sqlite3.Connection, entry_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM v2_notebook_entries WHERE entry_id = ?",
        (entry_id,),
    ).fetchone()
    return _row_to_entry(row) if row is not None else None


def list_entries(candidate_id: str) -> list[dict[str, Any]]:
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM v2_notebook_entries
            WHERE candidate_id = ?
            ORDER BY
              CASE status
                WHEN 'draft' THEN 0
                WHEN 'finalized' THEN 1
                ELSE 2
              END,
              updated_at DESC,
              created_at DESC
            """,
            (candidate_id,),
        ).fetchall()
        return [_row_to_entry(row) for row in rows]


def list_history(*, candidate_id: str | None = None, entry_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    capped_limit = max(1, min(int(limit or 100), 500))
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT revision_id, entry_id, action, snapshot_json, created_at
            FROM v2_notebook_entry_history
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (max(capped_limit * 4, 100),),
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            try:
                snapshot = json.loads(str(item.get("snapshot_json") or "{}"))
            except Exception:
                snapshot = {}
            entry = dict(snapshot.get("entry") or {})
            if entry_id is not None and str(item.get("entry_id") or "") != str(entry_id):
                continue
            if candidate_id is not None and str(entry.get("candidate_id") or "") != str(candidate_id):
                continue
            items.append(
                {
                    "revision_id": str(item.get("revision_id") or ""),
                    "entry_id": str(item.get("entry_id") or ""),
                    "action": str(item.get("action") or ""),
                    "created_at": str(item.get("created_at") or ""),
                    "candidate_id": str(entry.get("candidate_id") or ""),
                    "status": str(entry.get("status") or ""),
                    "title": str(entry.get("title") or ""),
                    "snapshot": entry,
                }
            )
            if len(items) >= capped_limit:
                break
        return items


def create_entry(
    candidate_id: str,
    *,
    linked_object_type: str,
    linked_object_id: str,
    linked_object_label: str,
    title: str,
    thesis: str,
    assumptions: str,
    invalidation: str,
    watch_items: str,
    reflections: str,
    next_review_date: str | None,
) -> dict[str, Any]:
    now = _now_iso()
    entry = {
        "entry_id": f"notebook_entry_{uuid.uuid4().hex}",
        "candidate_id": candidate_id,
        "linked_object_type": linked_object_type or "candidate",
        "linked_object_id": linked_object_id or candidate_id,
        "linked_object_label": linked_object_label or candidate_id,
        "status": "draft",
        "title": title,
        "thesis": thesis,
        "assumptions": assumptions,
        "invalidation": invalidation,
        "watch_items": watch_items,
        "reflections": reflections,
        "next_review_date": next_review_date,
        "created_at": now,
        "updated_at": now,
        "finalized_at": None,
        "archived_at": None,
    }
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO v2_notebook_entries (
              entry_id, candidate_id, linked_object_type, linked_object_id, linked_object_label,
              status, title, thesis, assumptions, invalidation, watch_items, reflections,
              next_review_date, created_at, updated_at, finalized_at, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry["entry_id"],
                entry["candidate_id"],
                entry["linked_object_type"],
                entry["linked_object_id"],
                entry["linked_object_label"],
                entry["status"],
                entry["title"],
                entry["thesis"],
                entry["assumptions"],
                entry["invalidation"],
                entry["watch_items"],
                entry["reflections"],
                entry["next_review_date"],
                entry["created_at"],
                entry["updated_at"],
                entry["finalized_at"],
                entry["archived_at"],
            ),
        )
        _record_history(conn, entry, "created")
        conn.commit()
    return entry


def ensure_seed_entry(
    candidate_id: str,
    *,
    linked_object_type: str,
    linked_object_id: str,
    linked_object_label: str,
    title: str,
    thesis: str,
    assumptions: str,
    invalidation: str,
    watch_items: str,
    reflections: str,
    next_review_date: str | None,
) -> tuple[dict[str, Any], bool]:
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM v2_notebook_entries
            WHERE candidate_id = ?
            ORDER BY
              CASE status
                WHEN 'draft' THEN 0
                WHEN 'finalized' THEN 1
                ELSE 2
              END,
              updated_at DESC,
              created_at DESC
            """,
            (candidate_id,),
        ).fetchall()
        existing = [_row_to_entry(row) for row in rows]
    draft = next((entry for entry in existing if entry["status"] == "draft"), None)
    if draft is not None:
        return draft, False
    created = create_entry(
        candidate_id,
        linked_object_type=linked_object_type,
        linked_object_id=linked_object_id,
        linked_object_label=linked_object_label,
        title=title,
        thesis=thesis,
        assumptions=assumptions,
        invalidation=invalidation,
        watch_items=watch_items,
        reflections=reflections,
        next_review_date=next_review_date,
    )
    return created, True


def update_entry(entry_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    with _connection() as conn:
        entry = _fetch_entry(conn, entry_id)
        if entry is None:
            return None
        for field in _MUTABLE_FIELDS:
            if field in updates:
                entry[field] = str(updates.get(field) or "") if field != "next_review_date" else updates.get(field)
        entry["updated_at"] = _now_iso()
        conn.execute(
            """
            UPDATE v2_notebook_entries
            SET linked_object_type = ?, linked_object_id = ?, linked_object_label = ?,
                title = ?, thesis = ?, assumptions = ?, invalidation = ?, watch_items = ?,
                reflections = ?, next_review_date = ?, updated_at = ?
            WHERE entry_id = ?
            """,
            (
                entry["linked_object_type"],
                entry["linked_object_id"],
                entry["linked_object_label"],
                entry["title"],
                entry["thesis"],
                entry["assumptions"],
                entry["invalidation"],
                entry["watch_items"],
                entry["reflections"],
                entry["next_review_date"],
                entry["updated_at"],
                entry_id,
            ),
        )
        _record_history(conn, entry, "updated")
        conn.commit()
        return entry


def set_status(entry_id: str, status: str) -> dict[str, Any] | None:
    normalized = str(status or "").strip().lower()
    if normalized not in _VALID_STATUS:
        raise ValueError(f"Unsupported notebook status: {status}")

    with _connection() as conn:
        entry = _fetch_entry(conn, entry_id)
        if entry is None:
            return None
        entry["status"] = normalized
        entry["updated_at"] = _now_iso()
        if normalized == "finalized":
            entry["finalized_at"] = entry["updated_at"]
        if normalized == "archived":
            entry["archived_at"] = entry["updated_at"]
        conn.execute(
            """
            UPDATE v2_notebook_entries
            SET status = ?, updated_at = ?, finalized_at = ?, archived_at = ?
            WHERE entry_id = ?
            """,
            (
                entry["status"],
                entry["updated_at"],
                entry["finalized_at"],
                entry["archived_at"],
                entry_id,
            ),
        )
        _record_history(conn, entry, f"status:{normalized}")
        conn.commit()
        return entry


def delete_entry(entry_id: str) -> bool:
    with _connection() as conn:
        entry = _fetch_entry(conn, entry_id)
        if entry is None:
            return False
        _record_history(conn, entry, "deleted")
        conn.execute("DELETE FROM v2_notebook_entries WHERE entry_id = ?", (entry_id,))
        conn.commit()
        return True
