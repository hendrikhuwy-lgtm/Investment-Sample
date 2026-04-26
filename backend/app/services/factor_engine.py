from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_factor_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS factor_exposure_snapshots (
          factor_snapshot_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          factor_name TEXT NOT NULL,
          exposure_value REAL NOT NULL,
          exposure_type TEXT NOT NULL,
          confidence TEXT NOT NULL,
          provenance_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_factor_exposure_snapshots_run
        ON factor_exposure_snapshots (run_id, factor_name)
        """
    )
    conn.commit()


def build_factor_snapshot(
    conn: sqlite3.Connection,
    *,
    run_id: str | None,
    holdings: list[dict[str, Any]],
    classifications: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    ensure_factor_tables(conn)
    if not run_id:
        return []

    total_value = sum(float(item.get("market_value") or 0.0) for item in holdings)
    if total_value <= 0:
        conn.execute("DELETE FROM factor_exposure_snapshots WHERE run_id = ?", (run_id,))
        conn.commit()
        return []

    classification_by_security = {str(item.get("security_key")): item for item in classifications}
    factor_map: dict[str, dict[str, Any]] = {
        "style_growth": {"score": 0.0, "observed": 0.0, "inferred": 0.0},
        "style_value": {"score": 0.0, "observed": 0.0, "inferred": 0.0},
        "style_quality": {"score": 0.0, "observed": 0.0, "inferred": 0.0},
        "style_momentum": {"score": 0.0, "observed": 0.0, "inferred": 0.0},
        "size_large_cap": {"score": 0.0, "observed": 0.0, "inferred": 0.0},
        "duration": {"score": 0.0, "observed": 0.0, "inferred": 0.0},
        "credit": {"score": 0.0, "observed": 0.0, "inferred": 0.0},
        "inflation_sensitivity": {"score": 0.0, "observed": 0.0, "inferred": 0.0},
    }

    for row in holdings:
        market_value = float(row.get("market_value") or 0.0)
        weight = market_value / total_value if total_value > 0 else 0.0
        sleeve = str(row.get("sleeve") or "").lower()
        asset_type = str(row.get("asset_type") or "").lower()
        classification = classification_by_security.get(str(row.get("security_key")), {})
        sector = str(classification.get("sector") or "").lower()
        confidence = str(classification.get("confidence") or "low")
        observed_multiplier = 1.0 if confidence in {"high", "medium"} else 0.0
        inferred_multiplier = 1.0 if confidence == "low" else 0.25

        if sleeve == "global_equity":
            factor_map["size_large_cap"]["score"] += weight * 0.8
            factor_map["size_large_cap"]["observed"] += weight * 0.8
            factor_map["style_quality"]["score"] += weight * 0.2
            factor_map["style_quality"]["inferred"] += weight * 0.2
        if sector == "technology":
            factor_map["style_growth"]["score"] += weight * (0.9 if observed_multiplier else 0.6)
            factor_map["style_growth"]["observed"] += weight * 0.9 * observed_multiplier
            factor_map["style_growth"]["inferred"] += weight * 0.6 * inferred_multiplier
            factor_map["style_momentum"]["score"] += weight * 0.4
            factor_map["style_momentum"]["inferred"] += weight * 0.4
        if sector in {"financials", "energy", "materials"}:
            factor_map["style_value"]["score"] += weight * 0.6
            factor_map["style_value"]["inferred"] += weight * 0.6
        if sector in {"health care", "fixed income", "cash"}:
            factor_map["style_quality"]["score"] += weight * 0.3
            factor_map["style_quality"]["inferred"] += weight * 0.3
        if sleeve == "ig_bond" or asset_type == "bond":
            factor_map["duration"]["score"] += weight
            factor_map["duration"]["observed"] += weight * 0.75
            factor_map["duration"]["inferred"] += weight * 0.25
            factor_map["credit"]["score"] += weight * 0.55
            factor_map["credit"]["observed"] += weight * 0.4
            factor_map["credit"]["inferred"] += weight * 0.15
        if sleeve == "real_asset":
            factor_map["inflation_sensitivity"]["score"] += weight * 0.8
            factor_map["inflation_sensitivity"]["inferred"] += weight * 0.8
        if sleeve == "cash":
            factor_map["duration"]["score"] += weight * 0.1
            factor_map["duration"]["observed"] += weight * 0.1
            factor_map["credit"]["score"] += weight * 0.05
            factor_map["credit"]["observed"] += weight * 0.05

    conn.execute("DELETE FROM factor_exposure_snapshots WHERE run_id = ?", (run_id,))
    created_at = _now_iso()
    output: list[dict[str, Any]] = []
    for factor_name, totals in factor_map.items():
        score = round(float(totals["score"]), 6)
        observed = round(float(totals["observed"]), 6)
        inferred = round(float(totals["inferred"]), 6)
        exposure_type = "observed" if observed > inferred and observed > 0 else "inferred" if inferred > 0 else "mixed"
        confidence = "high" if observed >= max(inferred, 0.2) else "medium" if observed > 0 else "low"
        payload = {
            "factor_snapshot_id": f"factor_{uuid.uuid4().hex[:12]}",
            "run_id": run_id,
            "factor_name": factor_name,
            "exposure_value": score,
            "exposure_type": exposure_type,
            "confidence": confidence,
            "provenance_json": {
                "observed_component": observed,
                "inferred_component": inferred,
                "methodology": "portfolio_factor_proxy_v1",
            },
            "created_at": created_at,
        }
        conn.execute(
            """
            INSERT INTO factor_exposure_snapshots (
              factor_snapshot_id, run_id, factor_name, exposure_value, exposure_type,
              confidence, provenance_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["factor_snapshot_id"],
                payload["run_id"],
                payload["factor_name"],
                payload["exposure_value"],
                payload["exposure_type"],
                payload["confidence"],
                json.dumps(payload["provenance_json"]),
                payload["created_at"],
            ),
        )
        output.append(payload)
    conn.commit()
    return output


def list_factor_snapshot(conn: sqlite3.Connection, *, run_id: str | None) -> list[dict[str, Any]]:
    ensure_factor_tables(conn)
    if not run_id:
        return []
    rows = conn.execute(
        """
        SELECT factor_snapshot_id, run_id, factor_name, exposure_value, exposure_type,
               confidence, provenance_json, created_at
        FROM factor_exposure_snapshots
        WHERE run_id = ?
        ORDER BY exposure_value DESC, factor_name ASC
        """,
        (run_id,),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["provenance_json"] = json.loads(str(item.get("provenance_json") or "{}"))
        items.append(item)
    return items
