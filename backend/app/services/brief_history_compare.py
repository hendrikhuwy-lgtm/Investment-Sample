from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_regime_history_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_history (
          regime_history_id TEXT PRIMARY KEY,
          brief_run_id TEXT NOT NULL,
          as_of_ts TEXT NOT NULL,
          long_state TEXT NOT NULL,
          short_state TEXT NOT NULL,
          change_summary TEXT,
          confidence_label TEXT,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_regime_history_asof
        ON regime_history (as_of_ts DESC, brief_run_id)
        """
    )
    conn.commit()


def record_regime_history(
    conn: sqlite3.Connection,
    *,
    brief_run_id: str,
    as_of_ts: str,
    long_state: str,
    short_state: str,
    change_summary: str | None,
    confidence_label: str = "medium",
) -> dict[str, Any]:
    ensure_regime_history_tables(conn)
    created_at = _now_iso()
    conn.execute(
        """
        INSERT INTO regime_history (
          regime_history_id, brief_run_id, as_of_ts, long_state, short_state, change_summary, confidence_label, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"regime_history_{uuid.uuid4().hex[:12]}",
            brief_run_id,
            as_of_ts,
            long_state,
            short_state,
            change_summary,
            confidence_label,
            created_at,
        ),
    )
    conn.commit()
    return {
        "brief_run_id": brief_run_id,
        "as_of_ts": as_of_ts,
        "long_state": long_state,
        "short_state": short_state,
        "change_summary": change_summary,
        "confidence_label": confidence_label,
        "created_at": created_at,
    }


def list_regime_history(conn: sqlite3.Connection, limit: int = 20) -> list[dict[str, Any]]:
    ensure_regime_history_tables(conn)
    rows = conn.execute(
        """
        SELECT brief_run_id, as_of_ts, long_state, short_state, change_summary, confidence_label, created_at
        FROM regime_history
        ORDER BY as_of_ts DESC
        LIMIT ?
        """,
        (max(1, limit),),
    ).fetchall()
    return [dict(row) for row in rows]


def build_history_compare(conn: sqlite3.Connection, *, current_run_id: str) -> dict[str, Any]:
    history = list_regime_history(conn, limit=12)
    current = next((item for item in history if str(item.get("brief_run_id")) == current_run_id), None)
    prior = next((item for item in history if str(item.get("brief_run_id")) != current_run_id), None)
    if current is None:
        return {"history": history, "current": None, "prior": None, "changes": []}
    changes: list[str] = []
    if prior is not None:
        if str(current.get("long_state")) != str(prior.get("long_state")):
            changes.append(
                f"Long horizon moved from {prior.get('long_state')} to {current.get('long_state')}."
            )
        if str(current.get("short_state")) != str(prior.get("short_state")):
            changes.append(
                f"Short horizon moved from {prior.get('short_state')} to {current.get('short_state')}."
            )
        if not changes:
            changes.append("Regime labels were stable versus the prior brief.")
    else:
        changes.append("No prior brief history is available yet.")
    return {"history": history, "current": current, "prior": prior, "changes": changes}
