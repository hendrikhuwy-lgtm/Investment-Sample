from __future__ import annotations

from typing import Any

from app.models.types import PolicyAllocation, RebalanceDiagnostics


def build_rebalance_policy(*, sleeve_key: str, target_weight: float, min_band: float, max_band: float) -> PolicyAllocation:
    priority = "core"
    calendar = "monthly"
    drift_abs = 0.05
    drift_rel = 0.20
    interdependency_rules: list[str] = []

    if sleeve_key in {"real_assets", "alternatives", "convex", "cash"}:
        priority = "satellite" if sleeve_key != "cash" else "defensive"
        calendar = "quarterly" if sleeve_key in {"real_assets", "alternatives"} else "monthly"
        drift_abs = 0.02 if sleeve_key == "convex" else 0.03
        drift_rel = 0.15
    elif sleeve_key == "ig_bonds":
        priority = "defensive"
        calendar = "monthly"
        drift_abs = 0.04
        drift_rel = 0.15

    if sleeve_key == "convex":
        interdependency_rules.append("Maintain convex target at or above 6% of combined equity target weight.")

    return PolicyAllocation(
        sleeve=_map_sleeve_key(sleeve_key),
        target_weight=float(target_weight),
        min_band=float(min_band),
        max_band=float(max_band),
        calendar_rebalance_frequency=calendar,
        drift_threshold_absolute=drift_abs,
        drift_threshold_relative=drift_rel,
        rebalance_priority=priority,
        interdependency_rules=interdependency_rules,
    )


def evaluate_rebalance_diagnostics(
    *,
    policy: PolicyAllocation,
    actual_weight: float | None = None,
    related_weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    if actual_weight is None:
        return RebalanceDiagnostics(
            status="data_incomplete",
            summary="Holdings snapshot is required before band-breach or drift diagnostics can be evaluated.",
            blockers=["current sleeve weight missing"],
            triggered_rules=[],
            provenance=["model-derived from policy only; holdings snapshot unavailable"],
        ).model_dump(mode="json")

    triggered_rules: list[str] = []
    blockers: list[str] = []
    status = "no_action"
    summary = "Sleeve remains within policy bands."

    if actual_weight < policy.min_band or actual_weight > policy.max_band:
        status = "band_breach"
        summary = "Observed sleeve weight breaches the configured policy band."
        triggered_rules.append("band_breach")
    elif abs(actual_weight - policy.target_weight) >= policy.drift_threshold_absolute:
        status = "calendar_review_due"
        summary = "Observed sleeve weight remains inside the band but is large enough to justify scheduled review."
        triggered_rules.append("drift_threshold_absolute")

    if policy.interdependency_rules:
        equity_weight = float((related_weights or {}).get("global_equity_core", 0.0))
        convex_weight = float((related_weights or {}).get("convex", 0.0))
        if equity_weight > 0:
            minimum_convex = equity_weight * 0.06
            if convex_weight < minimum_convex and _map_sleeve_key_back(policy.sleeve) == "convex":
                status = "interdependency_warning"
                summary = "Convex allocation is below the minimum ratio versus current equity exposure."
                triggered_rules.append("convex_vs_equity_ratio")
        elif _map_sleeve_key_back(policy.sleeve) == "convex":
            blockers.append("equity sleeve weight missing")

    return RebalanceDiagnostics(
        status=status,
        summary=summary,
        blockers=blockers,
        triggered_rules=triggered_rules or (["calendar_frequency"] if status == "no_action" else []),
        provenance=["model-derived from target/min/max bands and interdependency rules"],
    ).model_dump(mode="json")


def _map_sleeve_key(sleeve_key: str) -> str:
    return {
        "global_equity_core": "global_equity",
        "developed_ex_us_optional": "global_equity",
        "emerging_markets": "global_equity",
        "china_satellite": "global_equity",
        "ig_bonds": "ig_bond",
        "cash_bills": "cash",
        "real_assets": "real_asset",
        "alternatives": "alt",
        "convex": "convex",
    }.get(sleeve_key, "global_equity")


def _map_sleeve_key_back(sleeve: str) -> str:
    return {
        "global_equity": "global_equity_core",
        "ig_bond": "ig_bonds",
        "cash": "cash_bills",
        "real_asset": "real_assets",
        "alt": "alternatives",
        "convex": "convex",
    }.get(sleeve, "global_equity_core")
