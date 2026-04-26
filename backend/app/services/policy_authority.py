"""Compatibility stub — policy_authority module was removed; stubs preserve import-ability."""
from __future__ import annotations

from typing import Any


def evaluate_challenger_promotion(
    *,
    incumbent_score: float = 0.0,
    challenger_score: float = 0.0,
    challenger_dimensions: dict[str, Any] | None = None,
    incumbent_dimensions: dict[str, Any] | None = None,
    incumbent_truth_class: str = "",
    challenger_truth_class: str = "",
) -> dict[str, Any]:
    """Stub — returns replacement_incomplete so pipeline can continue safely."""
    return {
        "policy_replacement_authority": "replacement_incomplete",
        "challenger_truth_class": challenger_truth_class,
        "incumbent_truth_class": incumbent_truth_class,
    }


def build_policy_authority_record(
    *,
    directness_class: str = "unknown",
    authority_class: str = "support_grade",
    fallback_state: str = "none",
    claim_limit_class: str = "review_only",
    coverage_class: str = "missing",
    evidence_density_class: str = "thin",
    benchmark_support_class: str = "unknown",
    sleeve_key: str = "",
    eligibility_state: str = "data_incomplete",
    readiness_level: str = "research_visible",
    data_quality_state: str = "",
) -> dict[str, Any]:
    blocked_actions: list[str] = []
    restriction_codes: list[str] = []
    if authority_class not in {"truth_grade", "direct", "strong"}:
        blocked_actions.append("approve")
        restriction_codes.append("bounded_authority")
    if coverage_class in {"missing", "partial"} or fallback_state not in {"none", "cache_continuity"}:
        blocked_actions.append("replace_incumbent")
        restriction_codes.append("coverage_gap")
    allowed_actions = [action for action in ["monitor", "review", "compare", "approve"] if action not in set(blocked_actions)]
    policy_action_class = (
        "bounded_review"
        if blocked_actions
        else "promotion_eligible"
    )
    policy_authority_grade = (
        "bounded"
        if blocked_actions
        else "elevated"
    )
    return {
        "policy_authority_grade": policy_authority_grade,
        "policy_action_class": policy_action_class,
        "policy_allowed_actions": allowed_actions,
        "policy_blocked_actions": blocked_actions,
        "policy_restriction_codes": restriction_codes,
        "policy_benchmark_authority": benchmark_support_class or "unknown",
        "policy_replacement_authority": (
            "replacement_allowed"
            if "replace_incumbent" not in blocked_actions and eligibility_state not in {"blocked", "data_incomplete"}
            else "replacement_incomplete"
        ),
        "policy_truth_limit_summary": (
            f"{sleeve_key or 'sleeve'} remains {policy_action_class} because authority={authority_class}, coverage={coverage_class}, "
            f"fallback={fallback_state}, readiness={readiness_level}, data_quality={data_quality_state or 'unknown'}."
        ),
        "policy_escalation_allowed": "approve" not in blocked_actions,
        "directness_class": directness_class,
        "claim_limit_class": claim_limit_class,
        "evidence_density_class": evidence_density_class,
    }
