from __future__ import annotations

from typing import Any

from app.models.types import PortfolioHolding
from app.services.portfolio_state import holdings_total_value


def _convex_bucket_values(holdings: list[PortfolioHolding]) -> dict[str, float]:
    buckets = {"managed_futures": 0.0, "tail_hedge": 0.0, "long_put": 0.0, "other": 0.0}
    for item in holdings:
        if item.sleeve != "convex":
            continue
        value = float(item.quantity) * float(item.cost_basis)
        symbol = item.symbol.upper()
        if symbol in {"DBMF", "KMLM"}:
            buckets["managed_futures"] += value
        elif symbol in {"TAIL", "CAOS", "BTAL"}:
            buckets["tail_hedge"] += value
        elif "PUT" in symbol:
            buckets["long_put"] += value
        else:
            buckets["other"] += value
    return buckets


def compute_convex_kpi_panel(
    holdings: list[PortfolioHolding],
    stress_output: dict[str, Any] | None = None,
) -> dict[str, Any]:
    total_value = holdings_total_value(holdings)
    if total_value <= 0:
        return {
            "carry_estimate_pct_nav": 0.0,
            "hedge_behavior_score": 0.0,
            "convex_kpi_panel": {},
            "diagnostic": "No holdings available for convex effectiveness monitoring.",
        }

    buckets = _convex_bucket_values(holdings)
    convex_total = sum(buckets.values())
    managed_w = buckets["managed_futures"] / total_value
    tail_w = buckets["tail_hedge"] / total_value
    put_w = buckets["long_put"] / total_value

    # Carry estimate proxy: managed + tail ongoing cost + long-put premium decay.
    carry_estimate = (managed_w * 0.0035) + (tail_w * 0.0045) + (put_w * 0.012)
    stress_scenarios = list((stress_output or {}).get("scenarios", []))
    convex_contrib = [float(item.get("convex_contribution_pct", 0.0)) for item in stress_scenarios]
    avg_convex_support = sum(convex_contrib) / max(len(convex_contrib), 1)

    hedge_behavior_score = max(
        0.0,
        min(100.0, 60.0 + (avg_convex_support * 10.0) - (carry_estimate * 1000.0)),
    )
    return {
        "carry_estimate_pct_nav": round(carry_estimate * 100.0, 3),
        "hedge_behavior_score": round(hedge_behavior_score, 2),
        "convex_kpi_panel": {
            "convex_weight_pct": round((convex_total / total_value) * 100.0, 2),
            "managed_futures_weight_pct": round(managed_w * 100.0, 2),
            "tail_hedge_weight_pct": round(tail_w * 100.0, 2),
            "long_put_weight_pct": round(put_w * 100.0, 2),
            "avg_stress_convex_contribution_pct": round(avg_convex_support, 2),
        },
        "diagnostic": (
            "Convex KPI panel summarizes carry and stress-behavior diagnostics under scenario analogs."
        ),
    }
