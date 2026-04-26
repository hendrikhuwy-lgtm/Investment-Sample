from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


DEFAULT_POLICY_NAME = "policy_default"

DEFAULT_NEUTRAL = [
    {"sleeve_key": "global_equity", "weight": 0.45},
    {"sleeve_key": "ig_bond", "weight": 0.20},
    {"sleeve_key": "cash", "weight": 0.10},
    {"sleeve_key": "real_asset", "weight": 0.10},
    {"sleeve_key": "alt", "weight": 0.07},
    {"sleeve_key": "convex", "weight": 0.03},
]
DEFAULT_DRIFT = [
    {"sleeve_key": "underweight_sleeves", "weight": 0.70},
    {"sleeve_key": "policy_pro_rata", "weight": 0.30},
]
DEFAULT_STRESS = [
    {"sleeve_key": "cash", "weight": 0.35},
    {"sleeve_key": "ig_bond", "weight": 0.25},
    {"sleeve_key": "convex", "weight": 0.15},
    {"sleeve_key": "staged_risk_add", "weight": 0.25},
]


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_dca_policy_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dca_policies (
          dca_policy_id TEXT PRIMARY KEY,
          profile_id TEXT NOT NULL,
          policy_name TEXT NOT NULL,
          cadence TEXT NOT NULL,
          routing_mode TEXT NOT NULL,
          neutral_routing_json TEXT NOT NULL DEFAULT '[]',
          drift_routing_json TEXT NOT NULL DEFAULT '[]',
          stress_routing_json TEXT NOT NULL DEFAULT '[]',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_dca_policies_profile_name
        ON dca_policies (profile_id, policy_name)
        """
    )
    conn.commit()


def seed_default_dca_policies(conn: sqlite3.Connection, profile_id: str = "primary") -> None:
    ensure_dca_policy_tables(conn)
    row = conn.execute(
        """
        SELECT dca_policy_id
        FROM dca_policies
        WHERE profile_id = ? AND policy_name = ?
        LIMIT 1
        """,
        (profile_id, DEFAULT_POLICY_NAME),
    ).fetchone()
    if row is not None:
        return
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO dca_policies (
          dca_policy_id, profile_id, policy_name, cadence, routing_mode,
          neutral_routing_json, drift_routing_json, stress_routing_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"dca_{uuid.uuid4().hex[:12]}",
            profile_id,
            DEFAULT_POLICY_NAME,
            "monthly",
            "drift_correcting",
            json.dumps(DEFAULT_NEUTRAL),
            json.dumps(DEFAULT_DRIFT),
            json.dumps(DEFAULT_STRESS),
            now,
            now,
        ),
    )
    conn.commit()


def get_current_dca_policy(conn: sqlite3.Connection, profile_id: str = "primary") -> dict[str, Any]:
    seed_default_dca_policies(conn, profile_id=profile_id)
    row = conn.execute(
        """
        SELECT *
        FROM dca_policies
        WHERE profile_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (profile_id,),
    ).fetchone()
    if row is None:
        return {
            "policy_name": DEFAULT_POLICY_NAME,
            "cadence": "monthly",
            "routing_mode": "drift_correcting",
            "neutral_routing": list(DEFAULT_NEUTRAL),
            "drift_routing": list(DEFAULT_DRIFT),
            "stress_routing": list(DEFAULT_STRESS),
        }
    return {
        "dca_policy_id": str(row["dca_policy_id"]),
        "profile_id": str(row["profile_id"]),
        "policy_name": str(row["policy_name"]),
        "cadence": str(row["cadence"]),
        "routing_mode": str(row["routing_mode"]),
        "neutral_routing": list(json.loads(str(row["neutral_routing_json"] or "[]"))),
        "drift_routing": list(json.loads(str(row["drift_routing_json"] or "[]"))),
        "stress_routing": list(json.loads(str(row["stress_routing_json"] or "[]"))),
    }


def build_dca_guidance(policy: dict[str, Any]) -> dict[str, Any]:
    return {
        "policy_name": str(policy.get("policy_name") or DEFAULT_POLICY_NAME),
        "cadence": str(policy.get("cadence") or "monthly"),
        "routing_mode": str(policy.get("routing_mode") or "drift_correcting"),
        "neutral_conditions": list(policy.get("neutral_routing") or []),
        "drift_conditions": list(policy.get("drift_routing") or []),
        "stress_conditions": list(policy.get("stress_routing") or []),
        "distribution_logic": "Distribution logic remains optional. Current default assumes accumulation unless policy is overridden.",
        "caveat": "DCA routing is policy guidance for review and staging, not an execution directive.",
    }
