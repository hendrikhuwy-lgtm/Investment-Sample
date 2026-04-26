from __future__ import annotations

import math
from statistics import mean, pstdev
from typing import Any


def detect_outlier_flags(values: list[float], *, z_threshold: float = 3.0) -> list[str]:
    if len(values) < 8:
        return ["limited_history_for_outlier_detection"]
    sigma = pstdev(values)
    if sigma == 0:
        return []
    mu = mean(values)
    flags = []
    for idx, value in enumerate(values):
        z = abs((value - mu) / sigma)
        if z >= z_threshold:
            flags.append(f"outlier_at_index_{idx}_z_{z:.2f}")
    return flags


def evaluate_data_integrity(
    *,
    holdings_count: int,
    snapshots_count: int,
    valuation_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = valuation_rows or []
    blocked_sections: list[str] = []
    warnings: list[str] = []

    if holdings_count <= 0:
        warnings.append("no_holdings")
        blocked_sections.extend(["risk", "stress", "performance", "rebalance"])
    if snapshots_count < 2:
        warnings.append("insufficient_snapshot_history")
        blocked_sections.extend(["risk", "performance"])

    fx_fallback = sum(1 for item in rows if str(item.get("fx_source", "")) == "fallback")
    price_fallback = sum(1 for item in rows if str(item.get("price_source", "")) == "fallback")
    if rows:
        fallback_ratio = (fx_fallback + price_fallback) / (2 * len(rows))
        if fallback_ratio > 0.4:
            warnings.append("high_pricing_fallback_ratio")
            blocked_sections.append("performance")

    values = [float(item.get("market_value_sgd", 0.0)) for item in rows if float(item.get("market_value_sgd", 0.0)) > 0]
    outlier_flags = detect_outlier_flags(values)
    for flag in outlier_flags:
        if flag != "limited_history_for_outlier_detection":
            warnings.append(flag)

    blocked_unique = sorted(set(blocked_sections))
    return {
        "warnings": sorted(set(warnings)),
        "blocked_sections": blocked_unique,
        "status": "ok" if not blocked_unique else "partial",
        "completeness_score": round(max(0.0, 1.0 - (len(blocked_unique) * 0.12)), 2),
    }
