from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.config import get_db_path


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
        CREATE TABLE IF NOT EXISTS v2_surface_snapshots (
          snapshot_id TEXT PRIMARY KEY,
          surface_id TEXT NOT NULL,
          object_id TEXT NOT NULL,
          snapshot_kind TEXT NOT NULL,
          state_label TEXT,
          data_confidence TEXT,
          decision_confidence TEXT,
          generated_at TEXT NOT NULL,
          contract_json TEXT NOT NULL,
          input_summary_json TEXT NOT NULL,
          decision_inputs_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL
        )
        """
    )
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(v2_surface_snapshots)")}
    if "decision_inputs_json" not in columns:
        conn.execute(
            """
            ALTER TABLE v2_surface_snapshots
            ADD COLUMN decision_inputs_json TEXT NOT NULL DEFAULT '{}'
            """
        )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_v2_surface_snapshots_lookup
        ON v2_surface_snapshots (surface_id, object_id, created_at DESC)
        """
    )
    conn.commit()


def _decode_snapshot_row(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["contract"] = json.loads(str(item.pop("contract_json") or "{}"))
    item["input_summary"] = json.loads(str(item.pop("input_summary_json") or "{}"))
    item["decision_inputs"] = json.loads(str(item.pop("decision_inputs_json") or "{}"))
    return item


def _report_binding_tuple(contract: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(contract.get("candidate_id") or "").strip(),
        str(contract.get("sleeve_key") or contract.get("sleeve_id") or "").strip(),
        str(contract.get("bound_source_snapshot_id") or "").strip(),
        str(contract.get("source_contract_version") or "").strip(),
    )


def _dedupe_normalized_contract(value: Any) -> Any:
    if isinstance(value, dict):
        volatile = {
            "surface_snapshot_id",
            "generated_at",
            "report_generated_at",
            "report_source_snapshot_at",
            "route_cache_state",
            "report_loading_hint",
        }
        return {
            key: _dedupe_normalized_contract(item)
            for key, item in sorted(value.items())
            if key not in volatile
        }
    if isinstance(value, list):
        return [_dedupe_normalized_contract(item) for item in value]
    return value


def _candidate_report_duplicate(conn: sqlite3.Connection, *, object_id: str, contract: dict[str, Any]) -> str | None:
    binding = _report_binding_tuple(contract)
    for row in conn.execute(
        """
        SELECT snapshot_id, contract_json
        FROM v2_surface_snapshots
        WHERE surface_id = 'candidate_report' AND object_id = ?
        ORDER BY created_at DESC
        LIMIT 20
        """,
        (object_id,),
    ):
        existing = json.loads(str(row["contract_json"] or "{}"))
        if _report_binding_tuple(existing) != binding:
            continue
        if _dedupe_normalized_contract(existing) == _dedupe_normalized_contract(contract):
            return str(row["snapshot_id"] or "")
    return None


def _prune_candidate_report_snapshots(conn: sqlite3.Connection, *, object_id: str, keep: int = 80) -> None:
    conn.execute(
        """
        DELETE FROM v2_surface_snapshots
        WHERE surface_id = 'candidate_report'
          AND object_id = ?
          AND snapshot_id NOT IN (
            SELECT snapshot_id
            FROM v2_surface_snapshots
            WHERE surface_id = 'candidate_report' AND object_id = ?
            ORDER BY created_at DESC
            LIMIT ?
          )
        """,
        (object_id, object_id, keep),
    )


def record_surface_snapshot(
    *,
    surface_id: str,
    object_id: str,
    snapshot_kind: str,
    state_label: str | None,
    data_confidence: str | None,
    decision_confidence: str | None,
    generated_at: str | None,
    contract: dict[str, Any],
    input_summary: dict[str, Any],
    decision_inputs: dict[str, Any] | None = None,
) -> str:
    snapshot_id = f"surface_snapshot_{uuid.uuid4().hex[:16]}"
    snapshot_contract = dict(contract)
    snapshot_contract.setdefault("surface_snapshot_id", snapshot_id)
    with _connection() as conn:
        if surface_id == "candidate_report":
            existing_snapshot_id = _candidate_report_duplicate(conn, object_id=object_id, contract=snapshot_contract)
            if existing_snapshot_id:
                return existing_snapshot_id
        conn.execute(
            """
            INSERT INTO v2_surface_snapshots (
              snapshot_id, surface_id, object_id, snapshot_kind, state_label,
              data_confidence, decision_confidence, generated_at, contract_json,
              input_summary_json, decision_inputs_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                surface_id,
                object_id,
                snapshot_kind,
                state_label,
                data_confidence,
                decision_confidence,
                generated_at or _now_iso(),
                json.dumps(snapshot_contract, ensure_ascii=True, sort_keys=True),
                json.dumps(input_summary, ensure_ascii=True, sort_keys=True),
                json.dumps(decision_inputs or input_summary, ensure_ascii=True, sort_keys=True),
                _now_iso(),
            ),
        )
        if surface_id == "candidate_report":
            _prune_candidate_report_snapshots(conn, object_id=object_id)
        conn.commit()
    return snapshot_id


def get_surface_snapshot(snapshot_id: str) -> dict[str, Any] | None:
    with _connection() as conn:
        row = conn.execute(
            """
            SELECT snapshot_id, surface_id, object_id, snapshot_kind, state_label,
                   data_confidence, decision_confidence, generated_at, created_at,
                   contract_json, input_summary_json, decision_inputs_json
            FROM v2_surface_snapshots
            WHERE snapshot_id = ?
            LIMIT 1
            """,
            (snapshot_id,),
        ).fetchone()
    if row is None:
        return None
    return _decode_snapshot_row(row)


def list_surface_snapshots(*, surface_id: str, object_id: str, limit: int = 50) -> list[dict[str, Any]]:
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT snapshot_id, surface_id, object_id, snapshot_kind, state_label,
                   data_confidence, decision_confidence, generated_at, created_at,
                   contract_json, input_summary_json, decision_inputs_json
            FROM v2_surface_snapshots
            WHERE surface_id = ? AND object_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (surface_id, object_id, int(limit)),
        ).fetchall()
    return [_decode_snapshot_row(row) for row in rows]


def latest_surface_snapshot(*, surface_id: str, object_id: str) -> dict[str, Any] | None:
    with _connection() as conn:
        row = conn.execute(
            """
            SELECT snapshot_id, surface_id, object_id, snapshot_kind, state_label,
                   data_confidence, decision_confidence, generated_at, created_at,
                   contract_json, input_summary_json, decision_inputs_json
            FROM v2_surface_snapshots
            WHERE surface_id = ? AND object_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (surface_id, object_id),
        ).fetchone()
    if row is None:
        return None
    return _decode_snapshot_row(row)


def previous_surface_snapshot(*, surface_id: str, object_id: str) -> dict[str, Any] | None:
    with _connection() as conn:
        row = conn.execute(
            """
            SELECT snapshot_id, surface_id, object_id, snapshot_kind, state_label,
                   data_confidence, decision_confidence, generated_at, created_at,
                   contract_json, input_summary_json, decision_inputs_json
            FROM v2_surface_snapshots
            WHERE surface_id = ? AND object_id = ?
            ORDER BY created_at DESC
            LIMIT 1 OFFSET 1
            """,
            (surface_id, object_id),
        ).fetchone()
    if row is None:
        return None
    return _decode_snapshot_row(row)
