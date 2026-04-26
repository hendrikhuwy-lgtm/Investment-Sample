from __future__ import annotations

from copy import deepcopy
from typing import Any


BENCHMARK_FIT_TYPES = (
    "strong_fit",
    "acceptable_proxy",
    "weak_proxy",
    "mismatched",
    "unknown",
)
POLICY_GATE_STATES = ("pass", "fail", "partial", "not_evaluated")
DATA_QUALITY_STATES = ("pass", "partial", "failed", "unknown_due_to_missing_inputs")
SCORING_STATES = ("valid", "blocked_by_missing_input", "not_run")
LIQUIDITY_STATES = ("strong", "adequate", "weak", "limited_evidence", "unknown")
BENCHMARK_AUTHORITY_LEVELS = ("strong", "moderate", "limited", "insufficient")
PRESSURE_TYPES = (
    "benchmark",
    "data",
    "structure",
    "liquidity",
    "tax_wrapper",
    "performance_evidence",
    "readiness",
    "replacement",
)
PRESSURE_TRENDS = ("emerging", "stable", "worsening", "improving", "resolved")
PRESSURE_PERSISTENCE = ("isolated", "repeated", "persistent", "structural")
RECOMMENDATION_STABILITY_LEVELS = ("robust", "watch_stable", "fragile", "unstable")
REVIEW_ESCALATION_LEVELS = ("informational", "watch", "review", "urgent_review")
READINESS_LEVELS = ("research_visible", "review_ready", "shortlist_ready", "recommendation_ready")
USER_FACING_DECISION_STATES = (
    "fully_clean_recommendable",
    "best_available_with_limits",
    "research_ready_but_not_recommendable",
    "blocked_by_policy",
    "blocked_by_missing_required_evidence",
    "blocked_by_unresolved_gate",
)

_SEVERITY_ORDER = {"critical": 3, "important": 2, "informational": 1}
_READINESS_ORDER = {
    "research_visible": 0,
    "review_ready": 1,
    "shortlist_ready": 2,
    "recommendation_ready": 3,
}
_ESCALATION_ORDER = {
    "informational": 0,
    "watch": 1,
    "review": 2,
    "urgent_review": 3,
}
_BENCHMARK_AUTHORITY_ORDER = {
    "insufficient": 0,
    "limited": 1,
    "moderate": 2,
    "strong": 3,
}

_LEGACY_BENCHMARK_FIT_MAP = {
    "exact_fit": "strong_fit",
    "structurally_acceptable_fit": "strong_fit",
    "proxy_fit": "acceptable_proxy",
    "weak_fit": "weak_proxy",
    "benchmark_inappropriate": "mismatched",
    "strong_fit": "strong_fit",
    "acceptable_proxy": "acceptable_proxy",
    "weak_proxy": "weak_proxy",
    "mismatched": "mismatched",
    "unknown": "unknown",
}

SLEEVE_EXPRESSIONS: dict[str, dict[str, Any]] = {
    "global_equity_core": {
        "sleeve_purpose": "Core global equity anchor with broad implementation credibility and clean benchmark comparability.",
        "constraints": ["verified_live_vehicle", "ucits_preferred", "tax_wrapper_clean", "no_leverage", "broad_diversification"],
        "benchmark_role": "core_comparison_anchor",
        "implementation_priorities": ["benchmark_truth", "structure", "tax_wrapper", "cost", "liquidity"],
        "acceptable_tradeoffs": ["proxy_benchmark_for_research_only", "medium_liquidity_if_rest_strong"],
        "unacceptable_weaknesses": ["benchmark_inappropriate", "weak_structure", "critical_tax_wrapper_pressure", "critical_data_pressure"],
    },
    "developed_ex_us_optional": {
        "sleeve_purpose": "Developed-market split sleeve used when the policy wants more explicit regional control than a single global fund provides.",
        "constraints": ["verified_live_vehicle", "ucits_preferred", "no_leverage"],
        "benchmark_role": "core_comparison_anchor",
        "implementation_priorities": ["benchmark_truth", "structure", "cost", "tax_wrapper", "liquidity"],
        "acceptable_tradeoffs": ["proxy_benchmark_for_research_only", "slightly_higher_fee_for_better_regional_expression"],
        "unacceptable_weaknesses": ["benchmark_inappropriate", "critical_structure_pressure"],
    },
    "emerging_markets": {
        "sleeve_purpose": "Emerging-markets participation sleeve where broad exposure, implementation discipline, and benchmark comparability still matter.",
        "constraints": ["verified_live_vehicle", "ucits_preferred", "no_leverage"],
        "benchmark_role": "core_comparison_anchor",
        "implementation_priorities": ["benchmark_truth", "structure", "liquidity", "tax_wrapper", "cost"],
        "acceptable_tradeoffs": ["proxy_benchmark_for_research_only", "higher_tracking_noise_if_structure_clean"],
        "unacceptable_weaknesses": ["benchmark_inappropriate", "critical_liquidity_pressure", "critical_data_pressure"],
    },
    "china_satellite": {
        "sleeve_purpose": "China satellite sleeve where exposure precision matters more than broad global comparability, but benchmark discipline still matters.",
        "constraints": ["verified_live_vehicle", "no_leverage"],
        "benchmark_role": "supporting_anchor",
        "implementation_priorities": ["benchmark_truth", "structure", "liquidity", "cost"],
        "acceptable_tradeoffs": ["proxy_benchmark_if_exposure_is_structurally_close"],
        "unacceptable_weaknesses": ["benchmark_inappropriate", "critical_structure_pressure"],
    },
    "ig_bonds": {
        "sleeve_purpose": "Investment-grade ballast sleeve where duration, credit quality, and benchmark comparability are central to implementation trust.",
        "constraints": ["verified_live_vehicle", "duration_support", "credit_quality_support", "no_leverage"],
        "benchmark_role": "core_comparison_anchor",
        "implementation_priorities": ["benchmark_truth", "structure", "liquidity", "performance_evidence", "cost"],
        "acceptable_tradeoffs": ["proxy_benchmark_for_research_only"],
        "unacceptable_weaknesses": ["benchmark_inappropriate", "critical_performance_evidence_pressure", "critical_data_pressure"],
    },
    "cash_bills": {
        "sleeve_purpose": "Cash and bills sleeve where capital stability, implementation cleanliness, and liquidity certainty dominate.",
        "constraints": ["verified_live_vehicle", "liquidity_clean", "no_leverage"],
        "benchmark_role": "supporting_anchor",
        "implementation_priorities": ["structure", "liquidity", "benchmark_truth", "tax_wrapper"],
        "acceptable_tradeoffs": ["proxy_benchmark_if_maturity_role_is_clean"],
        "unacceptable_weaknesses": ["critical_liquidity_pressure", "critical_structure_pressure"],
    },
    "real_assets": {
        "sleeve_purpose": "Real-assets diversifier sleeve where implementation role, inflation linkage, and structure matter more than a single exact benchmark.",
        "constraints": ["verified_live_vehicle", "role_clarity"],
        "benchmark_role": "supporting_anchor",
        "implementation_priorities": ["structure", "liquidity", "benchmark_truth", "tax_wrapper"],
        "acceptable_tradeoffs": ["proxy_benchmark_if_role_is_clear"],
        "unacceptable_weaknesses": ["critical_structure_pressure", "critical_data_pressure"],
    },
    "alternatives": {
        "sleeve_purpose": "Alternative diversifier sleeve where scenario role and implementation constraints matter more than benchmark purity.",
        "constraints": ["role_clarity", "governance_clean"],
        "benchmark_role": "context_only",
        "implementation_priorities": ["structure", "readiness", "liquidity", "benchmark_truth"],
        "acceptable_tradeoffs": ["benchmark_not_decisive", "proxy_benchmark_for_context_only"],
        "unacceptable_weaknesses": ["critical_structure_pressure", "critical_readiness_pressure"],
    },
    "convex": {
        "sleeve_purpose": "Convex protection sleeve where downside-response role and governance constraints dominate benchmark comparison.",
        "constraints": ["role_clarity", "max_loss_known_or_explicit", "governance_clean"],
        "benchmark_role": "not_decisive",
        "implementation_priorities": ["structure", "readiness", "scenario_role", "liquidity"],
        "acceptable_tradeoffs": ["benchmark_not_decisive"],
        "unacceptable_weaknesses": ["critical_structure_pressure", "critical_readiness_pressure"],
    },
}


def get_sleeve_expression_definition(sleeve_key: str) -> dict[str, Any]:
    base = SLEEVE_EXPRESSIONS.get(
        str(sleeve_key),
        {
            "sleeve_purpose": "Blueprint sleeve with explicit implementation review requirements.",
            "constraints": ["verified_live_vehicle"],
            "benchmark_role": "supporting_anchor",
            "implementation_priorities": ["benchmark_truth", "structure", "liquidity"],
            "acceptable_tradeoffs": [],
            "unacceptable_weaknesses": ["critical_structure_pressure", "critical_data_pressure"],
        },
    )
    return deepcopy(base)


def normalize_benchmark_fit_type(value: Any) -> str:
    return _LEGACY_BENCHMARK_FIT_MAP.get(str(value or "").strip(), "unknown")


def classify_benchmark_truth(*, assignment: dict[str, Any], sleeve_key: str) -> dict[str, Any]:
    benchmark_role = str(get_sleeve_expression_definition(sleeve_key).get("benchmark_role") or "supporting_anchor")
    validation_status = str(assignment.get("validation_status") or "unassigned")
    confidence = str(assignment.get("benchmark_confidence") or "unknown")
    kind = str(assignment.get("benchmark_kind") or "unassigned")
    allowed_proxy = bool(assignment.get("allowed_proxy_flag", True))

    if benchmark_role in {"context_only", "not_decisive"} and not assignment.get("benchmark_key"):
        fit_type = "mismatched"
        authority = "insufficient"
        fair_comparison = False
    elif validation_status == "matched" and kind == "direct" and confidence == "high":
        fit_type = "strong_fit"
        authority = "strong"
        fair_comparison = True
    elif validation_status == "matched" and kind in {"direct", "sleeve_default"} and confidence in {"high", "medium"}:
        fit_type = "strong_fit"
        authority = "moderate"
        fair_comparison = True
    elif validation_status == "proxy_matched" and allowed_proxy:
        fit_type = "acceptable_proxy"
        authority = "limited"
        fair_comparison = benchmark_role not in {"core_comparison_anchor"}
    elif validation_status in {"mismatch", "proxy_disallowed"} or confidence == "low":
        fit_type = "weak_proxy"
        authority = "limited"
        fair_comparison = False
    elif validation_status in {"unassigned", "assigned_no_metrics"}:
        fit_type = "weak_proxy" if benchmark_role in {"core_comparison_anchor", "supporting_anchor"} else "mismatched"
        authority = "insufficient"
        fair_comparison = False
    else:
        fit_type = "weak_proxy"
        authority = "limited"
        fair_comparison = False

    if fit_type == "strong_fit":
        recommendation_effect = "Benchmark truth materially strengthens recommendation confidence and fair peer comparison."
    elif fit_type == "acceptable_proxy":
        recommendation_effect = "Proxy benchmark support is acceptable for research and shortlist work, but remains weaker authority for recommendation use and is not fully decisive."
    elif fit_type == "weak_proxy":
        recommendation_effect = "Weak benchmark fit limits peer-comparison authority and materially reduces recommendation confidence."
    else:
        recommendation_effect = "Benchmark comparison is not decisive for this sleeve, so recommendation trust must come from structure, role, and governance instead."

    return {
        "benchmark_fit_type": fit_type,
        "benchmark_authority_level": authority,
        "supports_fair_comparison": fair_comparison,
        "benchmark_role": benchmark_role,
        "benchmark_truth_summary": recommendation_effect,
    }


def evaluate_sleeve_expression(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    benchmark_assignment: dict[str, Any],
    pressures: list[dict[str, Any]] | None,
    readiness_level: str,
) -> dict[str, Any]:
    definition = get_sleeve_expression_definition(sleeve_key)
    pressures = list(pressures or [])
    benchmark_fit = normalize_benchmark_fit_type(benchmark_assignment.get("benchmark_fit_type"))
    pressure_types = {str(item.get("pressure_type") or "") for item in pressures}
    critical_pressure_types = {
        str(item.get("pressure_type") or "")
        for item in pressures
        if str(item.get("severity") or "") == "critical"
    }
    fit_strengths: list[str] = []
    compromises: list[str] = []

    if benchmark_fit == "strong_fit":
        fit_strengths.append("Benchmark role is aligned closely enough to support fair sleeve comparison.")
    elif benchmark_fit == "acceptable_proxy":
        compromises.append("Benchmark support is proxy-based, so sleeve comparison remains useful but not fully authoritative.")
    else:
        compromises.append("Benchmark support is weak for the sleeve’s intended comparison role.")

    if "structure" not in pressure_types and "tax_wrapper" not in critical_pressure_types:
        fit_strengths.append("Structure and wrapper profile are not the dominant current sleeve concern.")
    if "liquidity" not in critical_pressure_types:
        fit_strengths.append("Liquidity is not the main reason this sleeve fit is constrained.")
    if readiness_level in {"shortlist_ready", "recommendation_ready"}:
        fit_strengths.append("Current readiness is high enough for live implementation review.")
    else:
        compromises.append(f"Current readiness is only {readiness_level.replace('_', ' ')}, so fit remains conditional.")

    if "data" in pressure_types:
        compromises.append("Data pressure still weakens how confidently the sleeve expression can be judged.")
    if "benchmark" in pressure_types and benchmark_fit != "strong_fit":
        compromises.append("Benchmark weakness is part of the remaining sleeve-fit compromise.")

    if critical_pressure_types.intersection({"structure", "readiness"}):
        fit_type = "mismatch"
    elif benchmark_fit == "mismatched" and str(definition.get("benchmark_role")) == "core_comparison_anchor":
        fit_type = "mismatch"
    elif critical_pressure_types or readiness_level == "review_ready":
        fit_type = "partial_fit"
    elif compromises:
        fit_type = "qualified_fit"
    else:
        fit_type = "direct_fit"

    serves_best = {
        "global_equity_core": "broad global equity anchor role",
        "developed_ex_us_optional": "regional implementation precision",
        "emerging_markets": "broad EM implementation role",
        "china_satellite": "focused China sleeve expression",
        "ig_bonds": "defensive ballast and benchmarkable duration role",
        "cash_bills": "liquidity and capital-stability role",
        "real_assets": "diversifier and inflation-sensitive role",
        "alternatives": "diversifier role under looser benchmark dependence",
        "convex": "portfolio protection role",
    }.get(sleeve_key, "current sleeve role")

    return {
        **definition,
        "fit_type": fit_type,
        "serves_best": serves_best,
        "strengths": fit_strengths[:4],
        "compromises": list(dict.fromkeys(compromises))[:4],
        "summary": (
            f"{definition['sleeve_purpose']} "
            f"This candidate is a {fit_type.replace('_', ' ')} against the sleeve expression and serves best as {serves_best}."
        ).strip(),
    }


def enrich_pressures_with_state(
    *,
    pressures: list[dict[str, Any]],
    candidate_history: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    history = list(candidate_history or [])
    prior_map = _extract_prior_pressure_map(history[:5])
    enriched: list[dict[str, Any]] = []
    for pressure in pressures:
        pressure_type = str(pressure.get("pressure_type") or "")
        severity = str(pressure.get("severity") or "informational")
        prior = prior_map.get(pressure_type)
        occurrences = sum(1 for event in history[:5] if pressure_type in _history_pressure_types(event))
        prior_severity = str(prior.get("severity") or "") if prior else ""
        if not prior:
            trend = "emerging"
        elif _SEVERITY_ORDER.get(severity, 1) > _SEVERITY_ORDER.get(prior_severity, 1):
            trend = "worsening"
        elif _SEVERITY_ORDER.get(severity, 1) < _SEVERITY_ORDER.get(prior_severity, 1):
            trend = "improving"
        else:
            trend = "stable"
        if occurrences >= 3:
            persistence = "structural" if pressure_type in {"benchmark", "structure", "tax_wrapper"} else "persistent"
        elif occurrences == 2:
            persistence = "persistent"
        elif occurrences == 1:
            persistence = "repeated"
        else:
            persistence = "isolated"
        review_relevance = "high" if severity == "critical" or trend == "worsening" else "medium" if severity == "important" or persistence in {"persistent", "structural"} else "low"
        enriched.append(
            {
                **pressure,
                "trend": trend,
                "persistence": persistence,
                "review_relevance": review_relevance,
            }
        )
    return enriched


def build_investor_consequence_summary(
    *,
    sleeve_expression: dict[str, Any],
    benchmark_assignment: dict[str, Any],
    pressures: list[dict[str, Any]],
    readiness_level: str,
) -> dict[str, Any]:
    pressure_types = {str(item.get("pressure_type") or "") for item in pressures}
    highest_severity = max((_SEVERITY_ORDER.get(str(item.get("severity") or "informational"), 1) for item in pressures), default=0)
    benchmark_fit = str(benchmark_assignment.get("benchmark_fit_type") or "")
    implementation = "Implementation quality is currently supported." if highest_severity < 3 else "Implementation quality is constrained by at least one critical pressure."
    if "liquidity" in pressure_types or "structure" in pressure_types:
        implementation = "Implementation quality is affected mainly by structure or liquidity constraints."
    benchmark_comparison = str(benchmark_assignment.get("benchmark_truth_summary") or "Benchmark comparison support is limited.")
    trust = (
        "Recommendation trust is reduced mainly by data or benchmark weakness."
        if pressure_types.intersection({"data", "benchmark"})
        else "Recommendation trust depends mainly on true candidate quality rather than missing evidence."
    )
    issue_nature = (
        "data_issue"
        if pressure_types and pressure_types.issubset({"data", "benchmark", "performance_evidence"})
        else "candidate_issue"
        if pressure_types.intersection({"structure", "liquidity", "tax_wrapper"})
        else "mixed"
    )
    actionability = (
        "changes_actionability"
        if readiness_level in {"research_visible", "review_ready"} or highest_severity >= 3
        else "confidence_only"
    )
    summary = (
        f"{sleeve_expression.get('fit_type', 'qualified_fit').replace('_', ' ')} for the sleeve. "
        f"{implementation} {benchmark_comparison}"
    ).strip()
    return {
        "implementation_quality_effect": implementation,
        "benchmark_comparison_effect": benchmark_comparison,
        "investment_trust_effect": trust,
        "issue_nature": issue_nature,
        "actionability_effect": actionability,
        "summary": summary,
    }


def build_review_escalation(
    *,
    pressures: list[dict[str, Any]],
    benchmark_assignment: dict[str, Any],
    readiness_level: str,
    confidence_history: dict[str, Any] | None,
    recommendation_context: dict[str, Any] | None,
) -> dict[str, Any]:
    level = "informational"
    reasons: list[str] = []
    if any(str(item.get("trend") or "") == "worsening" and str(item.get("severity") or "") == "critical" for item in pressures):
        level = "urgent_review"
        reasons.append("Critical pressure is worsening.")
    if any(str(item.get("persistence") or "") in {"persistent", "structural"} and str(item.get("pressure_type") or "") == "benchmark" for item in pressures):
        level = _max_escalation(level, "review")
        reasons.append("Benchmark weakness is repeated or structural.")
    if str((confidence_history or {}).get("trust_direction") or "") == "worsening":
        level = _max_escalation(level, "review")
        reasons.append("Confidence trend is deteriorating.")
    if str((confidence_history or {}).get("readiness_trend") or "") == "worsening":
        level = _max_escalation(level, "review")
        reasons.append("Readiness has deteriorated.")
    if str((recommendation_context or {}).get("stability") or "") in {"fragile", "unstable"}:
        level = _max_escalation(level, "review" if str((recommendation_context or {}).get("stability")) == "fragile" else "urgent_review")
        reasons.append("Recommendation lead is not stable.")
    fit_type = normalize_benchmark_fit_type(benchmark_assignment.get("benchmark_fit_type"))
    if fit_type in {"weak_proxy", "mismatched"} and readiness_level == "recommendation_ready":
        level = _max_escalation(level, "review")
        reasons.append("Benchmark authority is too weak for an otherwise strong recommendation.")
    if not reasons and any(str(item.get("severity") or "") == "important" for item in pressures):
        level = "watch"
        reasons.append("Important pressure exists but is not yet urgent.")
    return {
        "level": level,
        "reasons": reasons[:4],
        "summary": reasons[0] if reasons else "No immediate review escalation is triggered.",
    }


def build_confidence_snapshot(
    *,
    candidate: dict[str, Any],
    recommendation_context: dict[str, Any] | None = None,
    review_escalation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    pressures = list(dict(candidate.get("eligibility") or {}).get("pressures") or [])
    readiness = str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "research_visible")
    highest = "informational"
    if any(str(item.get("severity") or "") == "critical" for item in pressures):
        highest = "critical"
    elif any(str(item.get("severity") or "") == "important" for item in pressures):
        highest = "important"
    return {
        "recommendation_confidence": str(quality.get("recommendation_confidence") or "medium"),
        "benchmark_authority_level": str(benchmark.get("benchmark_authority_level") or "insufficient"),
        "benchmark_fit_type": normalize_benchmark_fit_type(benchmark.get("benchmark_fit_type")),
        "pressure_count": len(pressures),
        "highest_pressure_severity": highest,
        "readiness_level": readiness,
        "winner_stability": str((recommendation_context or {}).get("stability") or "watch_stable"),
        "review_escalation_level": str((review_escalation or {}).get("level") or "informational"),
        "pressure_snapshot": [
            {
                "pressure_type": item.get("pressure_type"),
                "severity": item.get("severity"),
                "trend": item.get("trend"),
                "persistence": item.get("persistence"),
            }
            for item in pressures[:6]
        ],
    }


def build_confidence_history_summary(
    *,
    current_snapshot: dict[str, Any],
    candidate_history: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    history = list(candidate_history or [])
    previous = _extract_latest_confidence_snapshot(history)
    if not previous:
        return {
            "current": current_snapshot,
            "previous": None,
            "trust_direction": "stable",
            "readiness_trend": "stable",
            "driver": "no_prior_history",
            "summary": "No prior recommendation-confidence history is available yet.",
        }
    current_conf = _confidence_rank(str(current_snapshot.get("recommendation_confidence") or "medium"))
    previous_conf = _confidence_rank(str(previous.get("recommendation_confidence") or "medium"))
    current_benchmark = _BENCHMARK_AUTHORITY_ORDER.get(str(current_snapshot.get("benchmark_authority_level") or "insufficient"), 0)
    previous_benchmark = _BENCHMARK_AUTHORITY_ORDER.get(str(previous.get("benchmark_authority_level") or "insufficient"), 0)
    current_readiness = _READINESS_ORDER.get(str(current_snapshot.get("readiness_level") or "research_visible"), 0)
    previous_readiness = _READINESS_ORDER.get(str(previous.get("readiness_level") or "research_visible"), 0)

    if current_conf > previous_conf or current_benchmark > previous_benchmark or current_readiness > previous_readiness:
        trust_direction = "improving"
    elif current_conf < previous_conf or current_benchmark < previous_benchmark or current_readiness < previous_readiness:
        trust_direction = "worsening"
    else:
        trust_direction = "stable"

    if current_readiness > previous_readiness:
        readiness_trend = "improving"
    elif current_readiness < previous_readiness:
        readiness_trend = "worsening"
    else:
        readiness_trend = "stable"

    if current_benchmark != previous_benchmark:
        driver = "benchmark_driven"
    elif current_readiness != previous_readiness or int(current_snapshot.get("pressure_count") or 0) != int(previous.get("pressure_count") or 0):
        driver = "data_or_readiness_driven"
    elif current_conf != previous_conf:
        driver = "investment_case_driven"
    else:
        driver = "stable"

    return {
        "current": current_snapshot,
        "previous": previous,
        "trust_direction": trust_direction,
        "readiness_trend": readiness_trend,
        "driver": driver,
        "summary": (
            f"Confidence is {trust_direction} versus the latest stored recommendation history. "
            f"Primary driver: {driver.replace('_', ' ')}."
        ),
    }


def evaluate_hard_policy_gates(*, candidate: dict[str, Any], sleeve_key: str) -> dict[str, Any]:
    sleeve_expression = dict(candidate.get("sleeve_expression") or get_sleeve_expression_definition(sleeve_key))
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    pressures = list(dict(candidate.get("eligibility") or {}).get("pressures") or [])
    liquidity_status = str(dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {}).get("liquidity_status") or "unknown")
    risk_controls = list(dict(candidate.get("investment_lens") or {}).get("risk_controls") or [])
    gates: list[dict[str, Any]] = []

    def add_gate(gate_name: str, state: str, reason: str, *, missing_inputs: list[str] | None = None) -> None:
        gates.append(
            {
                "gate_name": gate_name,
                "state": state,
                "reason": reason,
                "missing_inputs": list(missing_inputs or []),
            }
        )

    structural_pass = not any(bool(candidate.get(key)) for key in ("margin_required", "short_options")) and candidate.get("max_loss_known") is not False
    add_gate(
        "structural_ineligibility",
        "pass" if structural_pass else "fail",
        "No prohibited leverage, margin, short-option, or undefined-loss structure detected." if structural_pass else "Candidate violates structural safety constraints.",
    )

    sleeve_fit_type = str(sleeve_expression.get("fit_type") or "")
    if not sleeve_fit_type:
        add_gate(
            "sleeve_constitution_fit",
            "not_evaluated",
            "Sleeve constitution fit could not be evaluated because sleeve-expression fit type is missing.",
            missing_inputs=["sleeve_expression.fit_type"],
        )
    else:
        sleeve_pass = sleeve_fit_type != "mismatch"
        add_gate(
            "sleeve_constitution_fit",
            "pass" if sleeve_pass else "fail",
            "Candidate does not breach the sleeve constitution." if sleeve_pass else "Candidate breaches sleeve constitution or unacceptable weaknesses.",
        )

    domicile = str(candidate.get("domicile") or "").strip().upper()
    if not domicile:
        add_gate(
            "prohibited_domicile_or_tax_state",
            "not_evaluated",
            "Domicile and tax-state gate could not be fully evaluated because domicile is missing.",
            missing_inputs=["candidate.domicile"],
        )
    else:
        domicile_pass = not (sleeve_key in {"global_equity_core", "developed_ex_us_optional", "emerging_markets", "ig_bonds"} and domicile == "US")
        add_gate(
            "prohibited_domicile_or_tax_state",
            "pass" if domicile_pass else "fail",
            "No prohibited domicile or tax posture was triggered." if domicile_pass else "Candidate domicile or tax posture breaches sleeve policy.",
        )

    if liquidity_status in {"unknown", ""}:
        add_gate(
            "critical_liquidity_failure",
            "not_evaluated",
            "Critical liquidity failure cannot be ruled out because liquidity evidence is absent.",
            missing_inputs=["investment_lens.liquidity_profile.liquidity_status"],
        )
    elif liquidity_status == "limited_evidence":
        add_gate(
            "critical_liquidity_failure",
            "partial",
            "Liquidity evidence is only partial, so critical liquidity failure cannot be fully ruled out.",
            missing_inputs=["candidate.liquidity_score"],
        )
    else:
        liquidity_pass = liquidity_status != "weak"
        add_gate(
            "critical_liquidity_failure",
            "pass" if liquidity_pass else "fail",
            "Liquidity is not in critical failure." if liquidity_pass else "Critical liquidity weakness blocks deliverable recommendation use.",
        )

    governance_failures = [
        control
        for control in risk_controls
        if str(control.get("status") or "") == "fail"
    ]
    governance_pass = str(dict(candidate.get("decision_state") or {}).get("status") or "draft") != "rejected" and not governance_failures
    add_gate(
        "critical_governance_failure",
        "pass" if governance_pass else "fail",
        "No critical governance failure is active." if governance_pass else "Governance or risk-control failure blocks recommendation use.",
    )

    benchmark_fit_type = normalize_benchmark_fit_type(benchmark.get("benchmark_fit_type"))
    benchmark_role = str(sleeve_expression.get("benchmark_role") or "")
    validation_status = str(benchmark.get("validation_status") or "unassigned")
    if benchmark_role in {"context_only", "not_decisive"}:
        add_gate(
            "required_benchmark_support",
            "pass",
            "Benchmark support is context only for this sleeve, so benchmark weakness does not block structural review.",
        )
    elif benchmark_fit_type in {"", "unknown"}:
        add_gate(
            "required_benchmark_support",
            "not_evaluated",
            "Benchmark support could not be evaluated because no benchmark fit evidence is available.",
            missing_inputs=["benchmark_assignment.benchmark_fit_type"],
        )
    else:
        if benchmark_fit_type == "strong_fit":
            benchmark_state = "pass"
            benchmark_reason = "Benchmark support is adequate for the sleeve role."
        elif benchmark_fit_type == "acceptable_proxy":
            benchmark_state = "pass"
            benchmark_reason = "Proxy benchmark support is acceptable for this sleeve role, but still lowers recommendation authority."
        elif benchmark_fit_type == "weak_proxy" and validation_status in {"assigned_no_metrics", "unassigned"}:
            benchmark_state = "not_evaluated"
            benchmark_reason = "Benchmark support is still incomplete because validation or metrics are missing."
        else:
            benchmark_state = "fail"
            benchmark_reason = "Required benchmark support is missing or too weak."
        add_gate(
            "required_benchmark_support",
            benchmark_state,
            benchmark_reason,
            missing_inputs=["benchmark_assignment.validation_status", "performance_metrics.benchmark_history"] if benchmark_state == "not_evaluated" else None,
        )

    bounded_loss_required = sleeve_key == "convex"
    bounded_loss_pass = not bounded_loss_required or candidate.get("max_loss_known") is not False
    add_gate(
        "bounded_loss_requirement",
        "pass" if bounded_loss_pass else "fail",
        "Bounded-loss requirement is satisfied." if bounded_loss_pass else "Bounded-loss requirement is not satisfied.",
    )

    leverage_pass = not bool(candidate.get("leverage_used"))
    add_gate(
        "leverage_prohibited",
        "pass" if leverage_pass else "fail",
        "No prohibited leverage is in use." if leverage_pass else "Leverage is prohibited for this sleeve.",
    )

    failed = [gate for gate in gates if gate["state"] == "fail"]
    partial = [gate for gate in gates if gate["state"] in {"partial", "not_evaluated"}]
    if failed:
        overall_state = "fail"
    elif partial and len(partial) == len(gates):
        overall_state = "not_evaluated"
    elif partial:
        overall_state = "partial"
    else:
        overall_state = "pass"
    return {
        "state": overall_state,
        "failed_gates": failed,
        "gates": gates,
        "failed_gate_names": [gate["gate_name"] for gate in failed],
        "partial_gate_names": [gate["gate_name"] for gate in partial],
    }


def required_gate_names_for_sleeve(*, sleeve_key: str) -> list[str]:
    benchmark_role = str(get_sleeve_expression_definition(sleeve_key).get("benchmark_role") or "supporting_anchor")
    required = [
        "structural_ineligibility",
        "sleeve_constitution_fit",
        "prohibited_domicile_or_tax_state",
        "critical_liquidity_failure",
        "critical_governance_failure",
        "bounded_loss_requirement",
        "leverage_prohibited",
    ]
    if benchmark_role in {"core_comparison_anchor", "supporting_anchor"}:
        required.append("required_benchmark_support")
    return required


def resolve_user_facing_decision_state(
    *,
    policy_gate_state: str,
    required_gate_resolution_state: str,
    data_quality_state: str,
    scoring_state: str,
    recommendation_state: str,
    readiness_level: str,
    recommendation_confidence: str,
) -> str:
    if policy_gate_state == "fail":
        return "blocked_by_policy"
    if required_gate_resolution_state == "unresolved":
        return "blocked_by_unresolved_gate"
    if data_quality_state in {"failed", "unknown_due_to_missing_inputs"} or scoring_state == "blocked_by_missing_input":
        return "blocked_by_missing_required_evidence"
    if recommendation_state == "recommended_primary" and readiness_level == "recommendation_ready" and recommendation_confidence == "high":
        return "fully_clean_recommendable"
    if recommendation_state in {"recommended_primary", "recommended_backup"}:
        return "best_available_with_limits"
    if readiness_level in {"review_ready", "shortlist_ready", "recommendation_ready"}:
        return "research_ready_but_not_recommendable"
    return "blocked_by_missing_required_evidence" if recommendation_state == "rejected_data_insufficient" else "research_ready_but_not_recommendable"


def build_blueprint_decision_record(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    evaluation_mode: str,
) -> dict[str, Any]:
    sleeve_expression = dict(candidate.get("sleeve_expression") or get_sleeve_expression_definition(sleeve_key))
    completeness = dict(candidate.get("data_completeness") or {})
    quality = dict(candidate.get("investment_quality") or {})
    policy_gates = evaluate_hard_policy_gates(candidate=candidate, sleeve_key=sleeve_key)
    required_gate_names = required_gate_names_for_sleeve(sleeve_key=sleeve_key)
    unresolved_required_gates = [
        gate
        for gate in list(policy_gates.get("gates") or [])
        if str(gate.get("gate_name") or "") in required_gate_names and str(gate.get("state") or "") == "not_evaluated"
    ]
    fit_type = str(sleeve_expression.get("fit_type") or "")
    readiness_level = str(completeness.get("readiness_level") or "")
    mandate_fit_state = (
        "pass"
        if fit_type in {"direct_fit", "qualified_fit", "partial_fit"}
        else "fail"
        if fit_type == "mismatch"
        else "not_evaluated"
    )
    sleeve_fit_state = (
        "pass"
        if fit_type in {"direct_fit", "qualified_fit"}
        else "partial"
        if fit_type == "partial_fit"
        else "fail"
        if fit_type == "mismatch"
        else "not_evaluated"
    )
    critical_missing = int(completeness.get("critical_required_fields_missing_count") or 0)
    required_complete = int(completeness.get("required_fields_complete_count") or 0)
    if not completeness:
        data_quality_state = "not_evaluated"
    elif readiness_level == "research_visible" and required_complete == 0:
        data_quality_state = "unknown_due_to_missing_inputs"
    elif bool(quality.get("composite_score_valid")) is False or readiness_level == "research_visible":
        data_quality_state = "failed"
    elif critical_missing > 0:
        data_quality_state = "partial"
    else:
        data_quality_state = "pass"
    if not quality:
        scoring_state = "not_run"
    elif bool(quality.get("composite_score_valid", True)) and quality.get("composite_score") is not None:
        scoring_state = "valid"
    elif readiness_level == "research_visible" or critical_missing > 0 or list(quality.get("unknown_dimensions") or []):
        scoring_state = "blocked_by_missing_input"
    else:
        scoring_state = "not_run"
    rejection_reasons: list[str] = []
    caution_reasons: list[str] = []
    approval_reasons: list[str] = []
    if policy_gates["state"] == "fail":
        rejection_reasons.extend(str(item["reason"]) for item in policy_gates["failed_gates"])
    if unresolved_required_gates:
        rejection_reasons.extend(
            str(item.get("reason") or "A required policy gate is still unresolved.")
            for item in unresolved_required_gates
        )
    if data_quality_state == "failed":
        rejection_reasons.append("Data completeness is too weak for a valid composite score or deliverable recommendation.")
    if sleeve_fit_state == "partial":
        caution_reasons.extend(str(item) for item in list(sleeve_expression.get("compromises") or [])[:3])
    if mandate_fit_state == "pass":
        approval_reasons.append("Candidate serves a defined sleeve expression.")
    if policy_gates["state"] == "pass":
        approval_reasons.append("Hard policy gates pass.")
    if scoring_state == "valid":
        approval_reasons.append("Composite scoring is allowed.")

    if policy_gates["state"] == "fail":
        final_state = "rejected_policy_failure"
        final_reason = rejection_reasons[0] if rejection_reasons else "Hard policy gate failed."
    elif unresolved_required_gates:
        final_state = "blocked_by_unresolved_gate"
        final_reason = rejection_reasons[0] if rejection_reasons else "Required gate logic is still unresolved."
    elif data_quality_state == "failed":
        final_state = "blocked_by_missing_required_evidence"
        final_reason = rejection_reasons[0] if rejection_reasons else "Data completeness is insufficient."
    elif data_quality_state == "unknown_due_to_missing_inputs":
        final_state = "blocked_by_missing_required_evidence"
        final_reason = "Required evidence is still too incomplete to support a clean recommendation state."
    else:
        final_state = "research_only"
        final_reason = "Candidate passes pre-score policy review and remains eligible for ranking."

    explanations = {
        "mandate_fit": _mandate_fit_explanation(mandate_fit_state, sleeve_key=sleeve_key, fit_type=fit_type),
        "sleeve_fit": _sleeve_fit_explanation(sleeve_fit_state, sleeve_expression=sleeve_expression),
        "policy_gates": _policy_gate_explanation(policy_gates),
        "data_quality": _data_quality_explanation(data_quality_state, completeness=completeness),
        "scoring": _scoring_explanation(scoring_state, quality=quality, completeness=completeness),
    }
    missing_inputs = {
        "policy_gates": sorted({missing for gate in list(policy_gates.get("gates") or []) for missing in list(gate.get("missing_inputs") or [])}),
        "data_quality": _data_quality_missing_inputs(completeness),
        "scoring": _scoring_missing_inputs(quality=quality, completeness=completeness),
    }

    return {
        "candidate_id": str(candidate.get("symbol") or ""),
        "sleeve": sleeve_key,
        "evaluation_mode": evaluation_mode,
        "mandate_fit_state": mandate_fit_state,
        "sleeve_fit_state": sleeve_fit_state,
        "policy_gate_state": policy_gates["state"],
        "policy_gates": policy_gates,
        "required_gate_names": required_gate_names,
        "required_gate_resolution_state": "unresolved" if unresolved_required_gates else "resolved",
        "unresolved_required_gates": unresolved_required_gates,
        "data_quality_state": data_quality_state,
        "scoring_state": scoring_state,
        "final_decision_state": final_state,
        "final_decision_reason": final_reason,
        "rejection_reason": {
            "primary_reason": final_reason,
            "secondary_reasons": list(dict.fromkeys(rejection_reasons[1:]))[:4],
            "root_cause_class": (
                "structural"
                if final_state == "rejected_policy_failure"
                else "unresolved_gate"
                if final_state == "blocked_by_unresolved_gate"
                else "evidence_based"
                if final_state == "blocked_by_missing_required_evidence"
                else "mixed"
            ),
            "blocking_layer": (
                "policy"
                if final_state == "rejected_policy_failure"
                else "gate_resolution"
                if final_state == "blocked_by_unresolved_gate"
                else "data_quality"
                if final_state == "blocked_by_missing_required_evidence"
                else "ranking"
            ),
            "issue_type": (
                "structural mismatch"
                if final_state == "rejected_policy_failure"
                else "temporary review issue"
                if final_state == "blocked_by_unresolved_gate"
                else "evidence weakness"
                if final_state == "blocked_by_missing_required_evidence"
                else "implementation weakness"
            ),
        },
        "rejection_reasons": list(dict.fromkeys(rejection_reasons))[:6],
        "caution_reasons": list(dict.fromkeys(caution_reasons))[:6],
        "approval_reasons": list(dict.fromkeys(approval_reasons))[:6],
        "explanations": explanations,
        "evidence_basis": {
            "mandate_fit": ["candidate.sleeve_expression.fit_type", "sleeve_expression_definition"],
            "sleeve_fit": ["candidate.sleeve_expression", "candidate.eligibility.pressures"],
            "policy_gates": ["decision_semantics.hard_policy_gates", "candidate.investment_lens.liquidity_profile", "candidate.benchmark_assignment"],
            "data_quality": ["candidate.data_completeness", "candidate.investment_quality"],
            "scoring": ["candidate.investment_quality.composite_score", "candidate.investment_quality.composite_score_valid"],
        },
        "missing_inputs": missing_inputs,
    }


def finalize_decision_record(
    *,
    decision_record: dict[str, Any],
    recommendation_state: str,
    reason: str | None = None,
) -> dict[str, Any]:
    out = dict(decision_record or {})
    if out.get("policy_gate_state") == "fail":
        out["final_decision_state"] = "rejected_policy_failure"
    elif out.get("required_gate_resolution_state") == "unresolved":
        out["final_decision_state"] = "blocked_by_unresolved_gate"
    elif out.get("data_quality_state") == "failed":
        out["final_decision_state"] = "blocked_by_missing_required_evidence"
    elif out.get("data_quality_state") == "unknown_due_to_missing_inputs":
        out["final_decision_state"] = "blocked_by_missing_required_evidence"
    else:
        out["final_decision_state"] = recommendation_state or "research_only"
    out["final_decision_reason"] = reason or out.get("final_decision_reason") or ""
    return out


def _mandate_fit_explanation(state: str, *, sleeve_key: str, fit_type: str) -> str:
    if state == "pass":
        return f"Mandate fit is supported because the candidate still fits the {sleeve_key.replace('_', ' ')} sleeve expression ({fit_type.replace('_', ' ')})."
    if state == "fail":
        return "Mandate fit fails because the sleeve-expression assessment indicates a mismatch with the intended sleeve role."
    return "Mandate fit has not been fully evaluated because sleeve-expression fit evidence is incomplete."


def _sleeve_fit_explanation(state: str, *, sleeve_expression: dict[str, Any]) -> str:
    if state == "pass":
        return "Sleeve fit is strong enough for direct implementation review."
    if state == "partial":
        compromises = list(sleeve_expression.get("compromises") or [])
        if compromises:
            return f"Sleeve fit is only partial because {compromises[0]}"
        return "Sleeve fit is only partial because some implementation compromises remain."
    if state == "fail":
        return "Sleeve fit fails because the candidate breaches the sleeve constitution or unacceptable-weakness rules."
    return "Sleeve fit has not been evaluated because sleeve-expression evidence is incomplete."


def _policy_gate_explanation(policy_gates: dict[str, Any]) -> str:
    state = str(policy_gates.get("state") or "unknown")
    if state == "pass":
        return "Hard policy gates pass across the currently evaluated structural, benchmark, governance, and bounded-loss checks."
    if state == "fail":
        failed = list(policy_gates.get("failed_gates") or [])
        first = dict(failed[0]) if failed else {}
        return str(first.get("reason") or "At least one hard policy gate failed.")
    if state == "partial":
        partial_names = [str(name).replace("_", " ") for name in list(policy_gates.get("partial_gate_names") or [])]
        return f"Policy review is only partial because some checks still depend on incomplete evidence: {', '.join(partial_names)}." if partial_names else "Policy review is only partial because some checks still depend on incomplete evidence."
    return "Policy gates are not fully resolved yet because key policy-check inputs are still incomplete."


def _data_quality_explanation(state: str, *, completeness: dict[str, Any]) -> str:
    readiness = str(completeness.get("readiness_level") or "unknown").replace("_", " ")
    critical_missing = int(completeness.get("critical_required_fields_missing_count") or 0)
    if state == "pass":
        return f"Data quality is strong enough for the current stage, with readiness at {readiness}."
    if state == "partial":
        return f"Evidence quality is only partial because {critical_missing} critical fields still need confirmation."
    if state == "failed":
        return f"Evidence quality still blocks stronger recommendation states because readiness remains {readiness}."
    if state == "unknown_due_to_missing_inputs":
        return "Evidence quality could not be judged cleanly because too many required completeness inputs are still missing."
    return "Evidence quality has not been evaluated yet."


def _scoring_explanation(state: str, *, quality: dict[str, Any], completeness: dict[str, Any]) -> str:
    if state == "valid":
        return "Composite scoring is valid because the current evidence set supports a scored comparison."
    if state == "blocked_by_missing_input":
        unknown_dimensions = list(quality.get("unknown_dimensions") or [])
        if unknown_dimensions:
            return f"Composite scoring is not shown because important evidence is still missing in: {', '.join(unknown_dimensions[:4])}."
        readiness = str(completeness.get("readiness_level") or "unknown").replace("_", " ")
        return f"Composite scoring is not shown because the current readiness level is only {readiness}."
    return "Composite scoring has not been run yet."


def _data_quality_missing_inputs(completeness: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for requirement in list(completeness.get("requirements") or []):
        status = str(dict(requirement).get("status") or "")
        if status in {"missing_but_fetchable", "missing_requires_source_expansion"}:
            missing.append(str(dict(requirement).get("key") or "unknown_requirement"))
    return missing[:8]


def _scoring_missing_inputs(*, quality: dict[str, Any], completeness: dict[str, Any]) -> list[str]:
    missing = [str(item) for item in list(quality.get("unknown_dimensions") or []) if str(item).strip()]
    if not missing and str(completeness.get("readiness_level") or "") == "research_visible":
        missing.extend(_data_quality_missing_inputs(completeness))
    return missing[:8]


def _history_pressure_types(event: dict[str, Any]) -> set[str]:
    snapshot = _extract_latest_confidence_snapshot([event]) or {}
    return {
        str(item.get("pressure_type") or "")
        for item in list(snapshot.get("pressure_snapshot") or [])
        if str(item.get("pressure_type") or "").strip()
    }


def _extract_prior_pressure_map(history: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    for event in history:
        snapshot = _extract_latest_confidence_snapshot([event]) or {}
        pressure_snapshot = list(snapshot.get("pressure_snapshot") or [])
        if pressure_snapshot:
            return {
                str(item.get("pressure_type") or ""): dict(item)
                for item in pressure_snapshot
                if str(item.get("pressure_type") or "").strip()
            }
    return {}


def _extract_latest_confidence_snapshot(history: list[dict[str, Any]]) -> dict[str, Any] | None:
    for event in history:
        detail = dict(event.get("detail") or {})
        confidence_snapshot = dict(detail.get("confidence_snapshot") or {})
        after = dict(confidence_snapshot.get("after") or {})
        if after:
            return after
        explanation_after = dict(dict(detail.get("explanation_snapshot") or {}).get("after") or {})
        if explanation_after:
            return explanation_after
    return None


def _confidence_rank(value: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}.get(str(value), 1)


def _max_escalation(left: str, right: str) -> str:
    return left if _ESCALATION_ORDER.get(left, 0) >= _ESCALATION_ORDER.get(right, 0) else right
