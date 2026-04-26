from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

from app.models.types import Constraints, InvestorProfile, PolicyAllocation


def _now() -> datetime:
    return datetime.now(UTC)


def default_ips_profile() -> InvestorProfile:
    updated_at = _now()
    return InvestorProfile(
        profile_id="primary",
        owner_label="Personal Investor",
        risk_tier="moderate_growth",
        target_return_min=0.06,
        target_return_max=0.10,
        horizon_years=10,
        rebalance_frequency="monthly",
        allocations=[
            PolicyAllocation(sleeve="global_equity", target_weight=0.50, min_band=0.45, max_band=0.55),
            PolicyAllocation(sleeve="ig_bond", target_weight=0.20, min_band=0.15, max_band=0.25),
            PolicyAllocation(sleeve="cash", target_weight=0.10, min_band=0.05, max_band=0.15),
            PolicyAllocation(sleeve="real_asset", target_weight=0.10, min_band=0.05, max_band=0.15),
            PolicyAllocation(sleeve="alt", target_weight=0.07, min_band=0.04, max_band=0.10),
            PolicyAllocation(sleeve="convex", target_weight=0.03, min_band=0.02, max_band=0.04),
        ],
        constraints=Constraints(),
        updated_at=updated_at,
    )


def ensure_ips_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ips_profile (
          profile_id TEXT PRIMARY KEY,
          payload_json TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def get_ips(conn: sqlite3.Connection, profile_id: str = "primary") -> InvestorProfile:
    ensure_ips_table(conn)
    row = conn.execute(
        "SELECT payload_json FROM ips_profile WHERE profile_id = ?",
        (profile_id,),
    ).fetchone()
    if row is None:
        profile = default_ips_profile()
        put_ips(conn, profile)
        return profile
    payload = json.loads(str(row["payload_json"]))
    return InvestorProfile(**payload)


def put_ips(conn: sqlite3.Connection, profile: InvestorProfile) -> InvestorProfile:
    ensure_ips_table(conn)
    normalized = profile.model_copy(update={"updated_at": _now()})
    conn.execute(
        """
        INSERT INTO ips_profile (profile_id, payload_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(profile_id) DO UPDATE SET
            payload_json=excluded.payload_json,
            updated_at=excluded.updated_at
        """,
        (
            normalized.profile_id,
            json.dumps(normalized.model_dump(mode="json")),
            normalized.updated_at.isoformat(),
        ),
    )
    conn.commit()
    return normalized


def policy_weights_from_ips(profile: InvestorProfile) -> dict[str, float]:
    return {item.sleeve: float(item.target_weight) for item in profile.allocations}


def policy_bands_from_ips(profile: InvestorProfile) -> dict[str, tuple[float, float]]:
    return {item.sleeve: (float(item.min_band), float(item.max_band)) for item in profile.allocations}


def ips_version_token(profile: InvestorProfile) -> str:
    return f"{profile.profile_id}@{profile.updated_at.date().isoformat()}"


def blueprint_policy_from_ips(profile: InvestorProfile) -> dict[str, dict[str, float]]:
    constraints = profile.constraints
    return {
        "global_equity_core": {
            "target": float(constraints.blueprint_global_equity_core_target),
            "min": float(constraints.blueprint_global_equity_core_min),
            "max": float(constraints.blueprint_global_equity_core_max),
        },
        "developed_ex_us_optional": {
            "target": float(constraints.blueprint_developed_ex_us_optional_target),
            "min": float(constraints.blueprint_developed_ex_us_optional_min),
            "max": float(constraints.blueprint_developed_ex_us_optional_max),
        },
        "emerging_markets": {
            "target": float(constraints.blueprint_emerging_markets_target),
            "min": float(constraints.blueprint_emerging_markets_min),
            "max": float(constraints.blueprint_emerging_markets_max),
        },
        "china_satellite": {
            "target": float(constraints.blueprint_china_satellite_target),
            "min": float(constraints.blueprint_china_satellite_min),
            "max": float(constraints.blueprint_china_satellite_max),
        },
        "ig_bonds": {
            "target": float(constraints.blueprint_ig_bonds_target),
            "min": float(constraints.blueprint_ig_bonds_min),
            "max": float(constraints.blueprint_ig_bonds_max),
        },
        "cash_bills": {
            "target": float(constraints.blueprint_cash_bills_target),
            "min": max(float(constraints.blueprint_cash_bills_min), float(constraints.blueprint_cash_floor)),
            "max": float(constraints.blueprint_cash_bills_max),
        },
        "real_assets": {
            "target": float(constraints.blueprint_real_assets_target),
            "min": float(constraints.blueprint_real_assets_min),
            "max": float(constraints.blueprint_real_assets_max),
        },
        "alternatives": {
            "target": float(constraints.blueprint_alternatives_target),
            "min": float(constraints.blueprint_alternatives_min),
            "max": float(constraints.blueprint_alternatives_max),
        },
        "convex": {
            "target": float(constraints.convex_target_total),
            "min": float(constraints.convex_target_total),
            "max": float(constraints.convex_target_total),
        },
    }
