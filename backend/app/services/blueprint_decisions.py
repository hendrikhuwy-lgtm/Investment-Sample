from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
import json
from typing import Any


DECISION_STATES = {"draft", "proposed", "approved", "rejected", "manual_override"}
DECISION_STATES_REQUIRING_RATIONALE = {"approved", "rejected", "manual_override"}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_blueprint_decision_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_candidate_decisions (
          decision_id TEXT PRIMARY KEY,
          sleeve_key TEXT NOT NULL,
          candidate_symbol TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT,
          override_reason TEXT,
          actor_id TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_candidate_decisions_symbol
        ON blueprint_candidate_decisions (sleeve_key, candidate_symbol)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_candidate_decision_events (
          event_id TEXT PRIMARY KEY,
          decision_id TEXT NOT NULL,
          sleeve_key TEXT NOT NULL,
          candidate_symbol TEXT NOT NULL,
          prior_status TEXT,
          new_status TEXT NOT NULL,
          note TEXT,
          score_snapshot_json TEXT,
          recommendation_snapshot_json TEXT,
          governance_summary_json TEXT,
          ips_version TEXT,
          market_state_snapshot_json TEXT,
          actor_id TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    _ensure_column(conn, "blueprint_candidate_decision_events", "score_snapshot_json", "TEXT")
    _ensure_column(conn, "blueprint_candidate_decision_events", "recommendation_snapshot_json", "TEXT")
    _ensure_column(conn, "blueprint_candidate_decision_events", "governance_summary_json", "TEXT")
    _ensure_column(conn, "blueprint_candidate_decision_events", "ips_version", "TEXT")
    _ensure_column(conn, "blueprint_candidate_decision_events", "market_state_snapshot_json", "TEXT")
    conn.commit()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def list_candidate_decisions(conn: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    ensure_blueprint_decision_tables(conn)
    rows = conn.execute(
        """
        SELECT decision_id, sleeve_key, candidate_symbol, status, note, override_reason, actor_id, updated_at, created_at
        FROM blueprint_candidate_decisions
        ORDER BY updated_at DESC
        """
    ).fetchall()
    return {
        (str(row["sleeve_key"]), str(row["candidate_symbol"]).upper()): dict(row)
        for row in rows
    }


def list_candidate_decision_events(
    conn: sqlite3.Connection,
    *,
    limit_per_candidate: int = 5,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    ensure_blueprint_decision_tables(conn)
    rows = conn.execute(
        """
        SELECT event_id, decision_id, sleeve_key, candidate_symbol, prior_status, new_status, note,
               score_snapshot_json, recommendation_snapshot_json, governance_summary_json, ips_version,
               market_state_snapshot_json, actor_id, created_at
        FROM blueprint_candidate_decision_events
        ORDER BY created_at DESC
        """
    ).fetchall()
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (str(row["sleeve_key"]), str(row["candidate_symbol"]).upper())
        bucket = grouped.setdefault(key, [])
        if len(bucket) >= limit_per_candidate:
            continue
        item = dict(row)
        for field in (
            "score_snapshot_json",
            "recommendation_snapshot_json",
            "governance_summary_json",
            "market_state_snapshot_json",
        ):
            raw_value = str(item.get(field) or "{}")
            try:
                item[field[:-5]] = json.loads(raw_value)
            except Exception:
                item[field[:-5]] = {}
            item[field] = raw_value
        bucket.append(item)
    return grouped


def upsert_candidate_decision(
    conn: sqlite3.Connection,
    *,
    sleeve_key: str,
    candidate_symbol: str,
    status: str,
    note: str | None,
    actor_id: str,
    override_reason: str | None = None,
    score_snapshot: dict[str, Any] | None = None,
    recommendation_snapshot: dict[str, Any] | None = None,
    governance_summary: dict[str, Any] | None = None,
    ips_version: str | None = None,
    market_state_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_blueprint_decision_tables(conn)
    if status not in DECISION_STATES:
        raise ValueError("Unsupported decision state.")
    rationale = str(override_reason or note or "").strip()
    if status in DECISION_STATES_REQUIRING_RATIONALE and not rationale:
        raise ValueError(f"{status.replace('_', ' ').title()} requires rationale.")
    key_symbol = candidate_symbol.upper()
    prior = conn.execute(
        """
        SELECT decision_id, status
        FROM blueprint_candidate_decisions
        WHERE sleeve_key = ? AND candidate_symbol = ?
        LIMIT 1
        """,
        (sleeve_key, key_symbol),
    ).fetchone()
    now = _now_iso()
    if prior is None:
        decision_id = f"blueprint_decision_{uuid.uuid4().hex[:12]}"
        conn.execute(
            """
            INSERT INTO blueprint_candidate_decisions (
              decision_id, sleeve_key, candidate_symbol, status, note, override_reason, actor_id, updated_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (decision_id, sleeve_key, key_symbol, status, note, override_reason, actor_id, now, now),
        )
        prior_status = None
    else:
        decision_id = str(prior["decision_id"])
        prior_status = str(prior["status"])
        conn.execute(
            """
            UPDATE blueprint_candidate_decisions
            SET status = ?, note = ?, override_reason = ?, actor_id = ?, updated_at = ?
            WHERE decision_id = ?
            """,
            (status, note, override_reason, actor_id, now, decision_id),
        )
    conn.execute(
        """
        INSERT INTO blueprint_candidate_decision_events (
          event_id, decision_id, sleeve_key, candidate_symbol, prior_status, new_status, note,
          score_snapshot_json, recommendation_snapshot_json, governance_summary_json, ips_version,
          market_state_snapshot_json, actor_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"blueprint_decision_event_{uuid.uuid4().hex[:12]}",
            decision_id,
            sleeve_key,
            key_symbol,
            prior_status,
            status,
            note or override_reason,
            json.dumps(score_snapshot or {}, sort_keys=True),
            json.dumps(recommendation_snapshot or {}, sort_keys=True),
            json.dumps(governance_summary or {}, sort_keys=True),
            ips_version,
            json.dumps(market_state_snapshot or {}, sort_keys=True),
            actor_id,
            now,
        ),
    )
    conn.commit()
    return dict(
        conn.execute(
            """
            SELECT decision_id, sleeve_key, candidate_symbol, status, note, override_reason, actor_id, updated_at, created_at
            FROM blueprint_candidate_decisions
            WHERE decision_id = ?
            """,
            (decision_id,),
        ).fetchone()
    )
