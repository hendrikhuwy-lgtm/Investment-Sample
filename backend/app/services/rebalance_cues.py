from __future__ import annotations

from typing import Any


def evaluate_drift_alerts(
    actual_weights: dict[str, float],
    policy_weights: dict[str, float],
    policy_bands: dict[str, tuple[float, float]],
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for sleeve, target in policy_weights.items():
        actual = float(actual_weights.get(sleeve, 0.0))
        band = policy_bands.get(sleeve, (target - 0.05, target + 0.05))
        lower, upper = float(band[0]), float(band[1])
        if actual < lower or actual > upper:
            direction = "above" if actual > upper else "below"
            alerts.append(
                {
                    "sleeve": sleeve,
                    "target": target,
                    "actual": actual,
                    "lower_band": lower,
                    "upper_band": upper,
                    "status": "drift_out_of_band",
                    "diagnostic": f"{sleeve} is {direction} policy band.",
                }
            )
    return alerts


def _risk_impact_estimate(weights: dict[str, float]) -> dict[str, float]:
    eq = float(weights.get("global_equity", 0.0))
    convex = float(weights.get("convex", 0.0))
    ig = float(weights.get("ig_bond", 0.0))
    risk_score = (eq * 100.0) - (convex * 80.0) - (ig * 25.0)
    return {
        "risk_score": round(risk_score, 2),
        "equity_beta_proxy": round(eq * 1.0 - convex * 0.25, 3),
        "duration_balance_proxy": round(ig - eq * 0.2, 3),
    }


def generate_rebalance_options(
    actual_weights: dict[str, float],
    policy_weights: dict[str, float],
    drift_alerts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not drift_alerts:
        return [
            {
                "option_id": "maintain_monitoring",
                "label": "Maintain current profile with ongoing drift monitoring",
                "impact": _risk_impact_estimate(actual_weights),
                "diagnostic": "No sleeve currently outside policy bands.",
            }
        ]

    mid_weights = dict(actual_weights)
    for item in drift_alerts:
        sleeve = str(item["sleeve"])
        lower = float(item["lower_band"])
        upper = float(item["upper_band"])
        mid_weights[sleeve] = (lower + upper) / 2.0

    policy_like = dict(policy_weights)

    options = [
        {
            "option_id": "monitor_only",
            "label": "Keep current allocations and monitor next diagnostic cycle",
            "impact": _risk_impact_estimate(actual_weights),
            "diagnostic": "Preserves current exposures while drift remains observable.",
        },
        {
            "option_id": "band_midpoint_alignment",
            "label": "Bring out-of-band sleeves toward policy-band midpoints",
            "impact": _risk_impact_estimate(mid_weights),
            "diagnostic": "Reduces drift magnitude while retaining implementation flexibility.",
        },
        {
            "option_id": "policy_reference_alignment",
            "label": "Recenter sleeves to policy reference profile",
            "impact": _risk_impact_estimate(policy_like),
            "diagnostic": "Minimizes policy drift and standardizes risk budget reference point.",
        },
    ]
    return options
