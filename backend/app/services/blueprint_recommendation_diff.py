from __future__ import annotations

from typing import Any


def build_recommendation_diff(previous_record: dict[str, Any] | None, current_record: dict[str, Any]) -> dict[str, Any]:
    previous = dict(previous_record or {})
    current = dict(current_record or {})
    previous_state = str(previous.get("recommendation_state") or "none")
    current_state = str(current.get("recommendation_state") or "none")
    changes: list[str] = []
    supporting_reasons: list[str] = []
    if previous_state != current_state:
        changes.append(f"recommendation state changed from {previous_state} to {current_state}")
        supporting_reasons.append("recommendation classification changed")
    if str(previous.get("benchmark_fit_type") or "") != str(current.get("benchmark_fit_type") or ""):
        changes.append("benchmark support changed materially")
        supporting_reasons.append("benchmark authority or fit changed")
    if str(previous.get("readiness_level") or "") != str(current.get("readiness_level") or ""):
        changes.append("readiness level changed")
        supporting_reasons.append("data quality or policy readiness changed")
    if float(previous.get("composite_score") or -1e9) != float(current.get("composite_score") or -1e9):
        changes.append("composite score changed")
        supporting_reasons.append("composite score inputs changed")
    if str(previous.get("recommendation_confidence") or "") != str(current.get("recommendation_confidence") or ""):
        changes.append("recommendation confidence changed")
        supporting_reasons.append("recommendation confidence changed")
    if bool(previous.get("candidate_universe_changed")) != bool(current.get("candidate_universe_changed")) or str(previous.get("candidate_universe_reason") or "") != str(current.get("candidate_universe_reason") or ""):
        changes.append("candidate universe context changed")
        supporting_reasons.append("candidate universe changed around the recommendation")

    if str(previous.get("benchmark_fit_type") or "") != str(current.get("benchmark_fit_type") or ""):
        driver = "benchmark_change"
    elif str(previous.get("readiness_level") or "") != str(current.get("readiness_level") or ""):
        driver = "data_or_policy_change"
    elif bool(previous.get("candidate_universe_changed")) != bool(current.get("candidate_universe_changed")) or str(previous.get("candidate_universe_reason") or "") != str(current.get("candidate_universe_reason") or ""):
        driver = "candidate_universe_change"
    elif previous_state != current_state:
        driver = "recommendation_reclassification"
    else:
        driver = "score_change"

    resolved_rejection_reasons = [
        item for item in list(previous.get("rejection_reasons") or [])
        if item and item not in list(current.get("rejection_reasons") or [])
    ]

    dominant_reason = {
        "benchmark_change": "Benchmark support changed enough to affect recommendation authority.",
        "data_or_policy_change": "Data completeness or policy-gate conditions changed enough to alter recommendation status.",
        "candidate_universe_change": "The candidate universe changed around the recommendation, affecting relative standing.",
        "recommendation_reclassification": "Recommendation state changed even without a single dominant benchmark or data trigger.",
        "score_change": "Underlying score dimensions moved without changing the broader decision context.",
    }.get(driver, "Recommendation context changed.")

    return {
        "what_changed": changes,
        "why_it_changed": driver,
        "material_dimensions_changed": [
            name for name in (
                "benchmark_fit_type",
                "readiness_level",
                "composite_score",
                "recommendation_state",
                "recommendation_confidence",
                "candidate_universe_reason",
            )
            if previous.get(name) != current.get(name)
        ],
        "change_driver_type": driver,
        "dominant_reason_for_change": dominant_reason,
        "supporting_reasons": list(dict.fromkeys(supporting_reasons))[:5],
        "candidate_universe_change_effect": str(current.get("candidate_universe_reason") or previous.get("candidate_universe_reason") or ""),
        "prior_rejection_reasons_resolved": resolved_rejection_reasons,
        "implementation_quality_direction": "increased" if current_state in {"recommended_primary", "recommended_backup"} and previous_state not in {"recommended_primary", "recommended_backup"} else "reduced" if previous_state in {"recommended_primary", "recommended_backup"} and current_state not in {"recommended_primary", "recommended_backup"} else "unchanged",
    }
