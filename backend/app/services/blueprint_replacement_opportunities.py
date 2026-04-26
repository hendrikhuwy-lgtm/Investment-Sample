from __future__ import annotations

from typing import Any


def build_replacement_opportunities(*, sleeves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    opportunities: list[dict[str, Any]] = []
    thresholds = {
        "global_equity_core": 10.0,
        "developed_ex_us_optional": 8.0,
        "emerging_markets": 9.0,
        "china_satellite": 9.0,
        "ig_bonds": 7.0,
        "cash_bills": 6.0,
        "real_assets": 8.0,
        "alternatives": 8.0,
        "convex": 7.0,
    }
    for sleeve in sleeves:
        candidates = list(sleeve.get("candidates") or [])
        ranked = [
            item
            for item in candidates
            if str(dict(item.get("investment_quality") or {}).get("recommendation_state") or "") in {
                "recommended_primary",
                "recommended_backup",
                "watchlist_only",
                "research_only",
                "rejected_inferior_to_selected",
            }
        ]
        if len(ranked) < 2:
            continue
        best = ranked[0]
        best_quality = dict(best.get("investment_quality") or {})
        if str(best_quality.get("eligibility_state") or "") not in {"eligible", "eligible_with_caution"}:
            continue
        for candidate in ranked[1:]:
            quality = dict(candidate.get("investment_quality") or {})
            if str(quality.get("eligibility_state") or "") not in {"eligible", "eligible_with_caution"}:
                continue
            if str(candidate.get("decision_state", {}).get("status") or "") not in {"approved", "proposed", "manual_override"} and not candidate.get("starred"):
                continue
            score_gap = float(best_quality.get("composite_score") or 0.0) - float(quality.get("composite_score") or 0.0)
            frictions = _estimate_switching_frictions(current=candidate, preferred=best)
            net_score_gap = score_gap - frictions["total_penalty_points"]
            minimum_threshold = thresholds.get(str(sleeve.get("sleeve_key") or ""), 8.0)
            if net_score_gap < minimum_threshold:
                continue
            stable_edges = []
            compare = dict(best_quality.get("comparison_vs_peers") or {})
            if compare.get("cost_position") == "stronger":
                stable_edges.append("lower cost")
            if compare.get("liquidity_position") == "stronger":
                stable_edges.append("stronger liquidity")
            if compare.get("structure_position") == "stronger":
                stable_edges.append("cleaner structure")
            if compare.get("tax_position") == "stronger":
                stable_edges.append("stronger SG tax fit")
            if compare.get("performance_position") == "stronger":
                stable_edges.append("stronger benchmark-relative performance")
            if not stable_edges:
                continue
            confidence = str(best_quality.get("data_confidence") or "medium")
            if frictions["tax_friction_label"] == "elevated" or frictions["switching_cost_bps"] >= 25:
                confidence = "medium" if confidence == "high" else confidence
            readiness = "watch"
            if net_score_gap >= minimum_threshold + 8 and confidence == "high" and frictions["transaction_confidence"] in {"high", "medium"}:
                readiness = "recommend"
            elif net_score_gap >= minimum_threshold + 3:
                readiness = "review"
            opportunities.append(
                {
                    "sleeve_key": sleeve.get("sleeve_key"),
                    "current_symbol": candidate.get("symbol"),
                    "preferred_symbol": best.get("symbol"),
                    "recommendation_state": "replacement_candidate",
                    "screening_state": "possible_upgrade",
                    "replacement_readiness": readiness,
                    "reason_for_upgrade": ", ".join(stable_edges[:3]),
                    "confidence": confidence,
                    "score_gap": round(score_gap, 2),
                    "net_score_gap": round(net_score_gap, 2),
                    "switching_cost_bps": frictions["switching_cost_bps"],
                    "minimum_net_improvement_threshold": minimum_threshold,
                    "good_enough_to_hold": False,
                    "tax_friction_label": frictions["tax_friction_label"],
                    "tax_friction_note": frictions["tax_friction_note"],
                    "liquidity_confidence": frictions["liquidity_confidence"],
                    "transaction_confidence": frictions["transaction_confidence"],
                    "implementation_uncertainty": frictions["implementation_uncertainty"],
                    "frictions_considered": frictions["frictions_considered"],
                    "what_data_would_confirm": [
                        "Additional verified benchmark-relative performance history would strengthen confirmation.",
                        "Observed execution spreads and tax-friction estimates would improve confidence.",
                    ],
                    "conservative_note": "Screening only. No automatic replacement is performed and marginal score gaps are treated as good enough to hold.",
                }
            )
    return opportunities


def _estimate_switching_frictions(*, current: dict[str, Any], preferred: dict[str, Any]) -> dict[str, Any]:
    current_spread = _safe_float(current.get("bid_ask_spread_proxy")) or 10.0
    preferred_spread = _safe_float(preferred.get("bid_ask_spread_proxy")) or 10.0
    switching_cost_bps = round(current_spread + preferred_spread, 2)

    current_wht = _safe_float(current.get("expected_withholding_drag_estimate")) or 0.0
    preferred_wht = _safe_float(preferred.get("expected_withholding_drag_estimate")) or 0.0
    drag_delta = abs(current_wht - preferred_wht)

    tax_label = "low"
    tax_note = "No material SG tax-friction difference identified from current metadata."
    penalty_points = switching_cost_bps / 10.0
    frictions_considered = [f"estimated round-trip spread {switching_cost_bps:.2f} bps"]
    if drag_delta >= 0.003:
        tax_label = "elevated"
        tax_note = "The preferred candidate changes expected withholding drag materially under the SG lens."
        penalty_points += 4.0
        frictions_considered.append("withholding drag delta")
    elif drag_delta >= 0.001:
        tax_label = "moderate"
        tax_note = "The preferred candidate changes expected withholding drag modestly under the SG lens."
        penalty_points += 2.0
        frictions_considered.append("modest withholding drag delta")

    current_decision = str((current.get("decision_state") or {}).get("status") or "draft")
    if current_decision == "approved":
        penalty_points += 1.5
        frictions_considered.append("approved candidate review burden")

    liquidity_confidence = "high" if all(_safe_float(item.get("bid_ask_spread_proxy")) is not None for item in (current, preferred)) else "medium"
    transaction_confidence = "high" if switching_cost_bps <= 15 else ("medium" if switching_cost_bps <= 30 else "low")
    implementation_uncertainty = "elevated" if any(str(item.get("source_state") or "unknown") in {"manual_seed", "stale_live", "broken_source", "strategy_placeholder", "policy_placeholder", "unknown"} for item in (current, preferred)) else "contained"

    return {
        "switching_cost_bps": switching_cost_bps,
        "tax_friction_label": tax_label,
        "tax_friction_note": tax_note,
        "liquidity_confidence": liquidity_confidence,
        "transaction_confidence": transaction_confidence,
        "implementation_uncertainty": implementation_uncertainty,
        "frictions_considered": frictions_considered,
        "total_penalty_points": round(penalty_points, 2),
    }


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None
