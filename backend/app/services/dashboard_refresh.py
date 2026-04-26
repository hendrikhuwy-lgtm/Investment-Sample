from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def select_dashboard_sections(payload: dict[str, Any], names: list[str]) -> dict[str, Any]:
    out = {
        "generated_at": payload.get("generated_at"),
        "generated_at_china": payload.get("generated_at_china"),
    }
    for name in names:
        if name in payload:
            out[name] = payload.get(name)
    return out


def mark_sections_refreshed(conn: sqlite3.Connection, names: list[str], *, refresh_mode: str) -> None:
    now = _now_iso()
    for name in names:
        conn.execute(
            """
            INSERT INTO dashboard_refresh_metadata (section_name, last_refreshed_at, refresh_mode, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(section_name) DO UPDATE SET
              last_refreshed_at = excluded.last_refreshed_at,
              refresh_mode = excluded.refresh_mode,
              updated_at = excluded.updated_at
            """,
            (name, now, refresh_mode, now),
        )
    conn.commit()


def list_refresh_metadata(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT section_name, last_refreshed_at, refresh_mode, updated_at
        FROM dashboard_refresh_metadata
        ORDER BY section_name ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]
