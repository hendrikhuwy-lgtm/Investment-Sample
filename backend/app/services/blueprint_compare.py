from __future__ import annotations

import sqlite3
from typing import Any

from app.services.blueprint_rebalance import build_rebalance_policy, evaluate_rebalance_diagnostics
from app.services.blueprint_store import get_current_blueprint


def compare_latest_portfolio_to_blueprint(
    conn: sqlite3.Connection,
    *,
    latest_run_id: str | None,
    account_id: str | None = None,
) -> dict[str, Any]:
    blueprint = get_current_blueprint(conn)
    sleeves = list(blueprint.get("sleeves") or [])
    total_row = None
    if latest_run_id:
        total_row = conn.execute(
            """
            SELECT COALESCE(SUM(market_value), 0) AS total_value
            FROM portfolio_holding_snapshots
            WHERE run_id = ?
              AND (? IS NULL OR account_id = ?)
            """,
            (latest_run_id, account_id, account_id),
        ).fetchone()
    total_value = float(total_row["total_value"]) if total_row is not None else 0.0
    actual_by_sleeve: dict[str, float] = {}
    unmapped_value = 0.0
    mapping_breakdown = {"auto_matched": 0, "low_confidence": 0, "unmapped": 0, "manual_override": 0}
    if latest_run_id:
        rows = conn.execute(
            """
            SELECT sleeve, mapping_status, COALESCE(SUM(market_value), 0) AS market_value
            FROM portfolio_holding_snapshots
            WHERE run_id = ?
              AND (? IS NULL OR account_id = ?)
            GROUP BY sleeve, mapping_status
            """,
            (latest_run_id, account_id, account_id),
        ).fetchall()
        for row in rows:
            sleeve = str(row["sleeve"] or "").strip()
            status = str(row["mapping_status"] or "unmapped")
            mapping_breakdown[status] = mapping_breakdown.get(status, 0) + 1
            value = float(row["market_value"] or 0.0)
            if sleeve:
                actual_by_sleeve[sleeve] = actual_by_sleeve.get(sleeve, 0.0) + value
            else:
                unmapped_value += value

    comparison_rows: list[dict[str, Any]] = []
    breach_count = 0
    related_weights = {
        str(sleeve["sleeve_key"]): (
            float(actual_by_sleeve.get(str(sleeve["sleeve_key"]), 0.0)) / total_value if total_value > 0 else 0.0
        )
        for sleeve in sleeves
    }
    for sleeve in sleeves:
        current_value = float(actual_by_sleeve.get(str(sleeve["sleeve_key"]), 0.0))
        current_weight = (current_value / total_value) if total_value > 0 else 0.0
        target_weight = float(sleeve["target_weight"])
        min_band = float(sleeve["min_band"])
        max_band = float(sleeve["max_band"])
        rebalance_policy = build_rebalance_policy(
            sleeve_key=str(sleeve["sleeve_key"]),
            target_weight=target_weight,
            min_band=min_band,
            max_band=max_band,
        )
        rebalance_diagnostics = evaluate_rebalance_diagnostics(
            policy=rebalance_policy,
            actual_weight=current_weight if total_value > 0 else None,
            related_weights=related_weights,
        )
        deviation = current_weight - target_weight
        if current_weight < min_band:
            breach_severity = "high" if (min_band - current_weight) >= 0.03 else "medium"
        elif current_weight > max_band:
            breach_severity = "high" if (current_weight - max_band) >= 0.03 else "medium"
        else:
            breach_severity = "none"
        rebalance_candidate = breach_severity != "none"
        if rebalance_candidate:
            breach_count += 1
        comparison_rows.append(
            {
                "sleeve_key": sleeve["sleeve_key"],
                "sleeve_name": sleeve["sleeve_name"],
                "current_weight": round(current_weight, 6),
                "target_weight": target_weight,
                "band_min": min_band,
                "band_max": max_band,
                "deviation": round(deviation, 6),
                "breach_severity": breach_severity,
                "rebalance_candidate": rebalance_candidate,
                "rebalance_diagnostics": rebalance_diagnostics,
                "rebalance_policy": rebalance_policy.model_dump(mode="json"),
                "benchmark_reference": sleeve.get("benchmark_reference"),
                "core_satellite": sleeve.get("core_satellite"),
            }
        )

    mapped_value = sum(actual_by_sleeve.values())
    mapping_coverage = (mapped_value / total_value) if total_value > 0 else 0.0
    benchmark_gap_count = sum(1 for row in comparison_rows if not row.get("benchmark_reference"))
    return {
        "blueprint": blueprint,
        "account_id": account_id,
        "comparison_rows": comparison_rows,
        "summary": {
            "portfolio_total_value": round(total_value, 2),
            "mapped_value": round(mapped_value, 2),
            "unmapped_value": round(unmapped_value, 2),
            "mapping_coverage": round(mapping_coverage, 6),
            "mapping_breakdown": mapping_breakdown,
            "breach_count": breach_count,
            "benchmark_gap_count": benchmark_gap_count,
        },
    }
