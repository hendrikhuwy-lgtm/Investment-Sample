from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

from app.services.ips import get_ips


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def ensure_ips_snapshot_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ips_snapshots (
          ips_snapshot_id TEXT PRIMARY KEY,
          brief_run_id TEXT NOT NULL,
          profile_id TEXT NOT NULL,
          benchmark_definition_id TEXT,
          cma_version TEXT,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_ips_snapshots_brief
        ON ips_snapshots (brief_run_id, created_at DESC)
        """
    )
    conn.commit()


def build_ips_snapshot(
    conn: sqlite3.Connection,
    *,
    benchmark: dict[str, Any] | None = None,
    rebalancing_policy: dict[str, Any] | None = None,
    cma_version: str | None = None,
    profile_id: str = "primary",
) -> dict[str, Any]:
    profile = get_ips(conn, profile_id=profile_id)
    benchmark_components = list((benchmark or {}).get("components") or [])
    benchmark_summary = {
        "benchmark_name": (benchmark or {}).get("benchmark_name"),
        "benchmark_definition_id": (benchmark or {}).get("benchmark_definition_id"),
        "assumption_date": (benchmark or {}).get("assumption_date"),
        "components": benchmark_components,
    }
    allocation = [
        {
            "sleeve": item.sleeve,
            "target_weight": float(item.target_weight),
            "min_band": float(item.min_band),
            "max_band": float(item.max_band),
            "calendar_rebalance_frequency": item.calendar_rebalance_frequency,
            "drift_threshold_absolute": float(item.drift_threshold_absolute),
            "drift_threshold_relative": float(item.drift_threshold_relative),
            "rebalance_priority": item.rebalance_priority,
        }
        for item in profile.allocations
    ]
    return {
        "profile_id": profile.profile_id,
        "owner_label": profile.owner_label,
        "objectives": f"Target policy return range of {profile.target_return_min:.0%} to {profile.target_return_max:.0%} over a multi-year horizon.",
        "risk_tolerance": profile.risk_tier.replace("_", " "),
        "time_horizon_years": int(profile.horizon_years),
        "liquidity_needs": "Maintain a reserve sleeve and stage deployment through policy-aware DCA or rebalance reviews.",
        "tax_context": "Singapore resident lens. Net-after-tax implementation differences matter when exposures are otherwise similar.",
        "constraints": profile.constraints.model_dump(mode="json"),
        "target_allocation": allocation,
        "benchmark": benchmark_summary,
        "rebalancing_rules": rebalancing_policy or {},
        "cma_version": cma_version,
        "generated_at": _now_iso(),
        "caveat": "IPS snapshot is a policy summary for the Daily Brief. It is not individualized advice or a prediction.",
    }


def persist_ips_snapshot(
    conn: sqlite3.Connection,
    *,
    brief_run_id: str,
    snapshot: dict[str, Any],
    benchmark_definition_id: str | None,
    cma_version: str | None,
) -> dict[str, Any]:
    ensure_ips_snapshot_tables(conn)
    ips_snapshot_id = f"ips_snapshot_{uuid.uuid4().hex[:12]}"
    created_at = _now_iso()
    conn.execute(
        """
        INSERT INTO ips_snapshots (
          ips_snapshot_id, brief_run_id, profile_id, benchmark_definition_id, cma_version, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ips_snapshot_id,
            brief_run_id,
            str(snapshot.get("profile_id") or "primary"),
            benchmark_definition_id,
            cma_version,
            json.dumps(_jsonable(snapshot)),
            created_at,
        ),
    )
    conn.commit()
    return {
        "ips_snapshot_id": ips_snapshot_id,
        "created_at": created_at,
        "payload": snapshot,
    }


def latest_ips_snapshot(conn: sqlite3.Connection, brief_run_id: str) -> dict[str, Any] | None:
    ensure_ips_snapshot_tables(conn)
    row = conn.execute(
        """
        SELECT *
        FROM ips_snapshots
        WHERE brief_run_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (brief_run_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "ips_snapshot_id": str(row["ips_snapshot_id"]),
        "brief_run_id": str(row["brief_run_id"]),
        "profile_id": str(row["profile_id"]),
        "benchmark_definition_id": row["benchmark_definition_id"],
        "cma_version": row["cma_version"],
        "payload": json.loads(str(row["payload_json"] or "{}")),
        "created_at": str(row["created_at"]),
    }
