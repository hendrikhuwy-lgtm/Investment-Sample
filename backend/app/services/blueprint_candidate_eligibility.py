from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.services.blueprint_data_quality import classify_freshness
from app.services.blueprint_decision_semantics import enrich_pressures_with_state, normalize_benchmark_fit_type
from app.services.upstream_truth_contract import normalize_source_state_base


CRITICAL_DECISION_BLOCKERS = {"rejected"}
CRITICAL_RISK_BLOCKERS = {"fail"}
CAUTION_RISK_STATES = {"warn", "unknown"}
CAUTION_LIQUIDITY_STATES = {"adequate", "weak", "unknown"}
CRITICAL_LIQUIDITY_BLOCKERS = {"weak"}
CORE_PASSIVE_SLEEVES = {"global_equity_core", "developed_ex_us_optional", "emerging_markets", "china_satellite", "ig_bonds", "cash_bills"}
EQUITY_COMPOSITION_SLEEVES = {"global_equity_core", "developed_ex_us_optional", "emerging_markets", "china_satellite"}
BOND_LIKE_SLEEVES = {"ig_bonds"}
CASH_LIKE_SLEEVES = {"cash_bills"}
SOFT_RISK_FAILURES: dict[str, set[str]] = {
    "emerging_markets": {"em weight band"},
    "ig_bonds": {"top-10 concentration", "sector concentration proxy"},
    "cash_bills": {"top-10 concentration", "sector concentration proxy"},
}
MIN_SOURCE_STATE_FOR_RECOMMENDATION = {"source_validated", "aging"}
PRESSURE_ORDER = {
    "readiness": 0,
    "benchmark": 1,
    "data": 2,
    "structure": 3,
    "liquidity": 4,
    "tax_wrapper": 5,
    "performance_evidence": 6,
    "replacement": 7,
}


def evaluate_candidate_eligibility(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    settings: Settings,
    now: datetime,
    candidate_history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    verification_status = str(candidate.get("verification_status") or "unverified")
    decision_state = str(dict(candidate.get("decision_state") or {}).get("status") or "draft")
    source_gaps = [str(item) for item in list(dict(candidate.get("investment_lens") or {}).get("source_gap_highlights") or []) if str(item).strip()]
    unknowns = [str(item) for item in list(dict(candidate.get("investment_lens") or {}).get("unknowns_that_matter") or []) if str(item).strip()]
    risk_summary = str(dict(dict(candidate.get("investment_lens") or {}).get("risk_control_summary") or {}).get("status") or "unknown")
    risk_controls = list(dict(candidate.get("investment_lens") or {}).get("risk_controls") or [])
    liquidity_profile = dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {})
    liquidity_status = str(liquidity_profile.get("liquidity_status") or "unknown")
    spread_status = str(liquidity_profile.get("spread_status") or "unknown")
    source_state = normalize_source_state_base(str(candidate.get("display_source_state") or candidate.get("source_state") or "unknown"))
    freshness_state = str(candidate.get("freshness_state") or "unknown")
    performance_support_state = str(candidate.get("performance_support_state") or "partial_due_to_missing_metrics")
    benchmark_assignment = dict(candidate.get("benchmark_assignment") or {})
    benchmark_confidence = str(benchmark_assignment.get("benchmark_confidence") or "unknown")
    benchmark_validation = str(benchmark_assignment.get("validation_status") or "unassigned")
    benchmark_fit_type = normalize_benchmark_fit_type(benchmark_assignment.get("benchmark_fit_type"))
    tax_score = (dict(candidate.get("sg_lens") or {})).get("score")
    field_truth = dict(candidate.get("field_truth") or {})
    factsheet_freshness = classify_freshness(
        observed_at=candidate.get("factsheet_asof") or candidate.get("last_verified_at"),
        now=now,
        fresh_hours=float(settings.blueprint_factsheet_max_age_days * 24),
        aging_hours=float(settings.blueprint_factsheet_max_age_days * 24 * 1.5),
        stale_hours=float(settings.blueprint_factsheet_max_age_days * 24 * 2),
    )

    blockers: list[str] = []
    cautions: list[str] = []
    provenance = [
        "candidate.verification_status",
        "candidate.decision_state",
        "investment_lens.risk_control_summary",
        "investment_lens.liquidity_profile",
        "candidate.factsheet_asof",
        "candidate.last_verified_at",
        "investment_lens.source_gap_highlights",
        "investment_lens.unknowns_that_matter",
        "candidate.source_state",
        "candidate.freshness_state",
        "candidate.performance_support_state",
        "candidate.benchmark_assignment",
        "candidate.sg_lens.score",
    ]

    if decision_state in CRITICAL_DECISION_BLOCKERS:
        blockers.append(f"governance state is {decision_state}")
    if verification_status == "unverified":
        blockers.append("candidate is unverified")
    elif verification_status == "partially_verified":
        cautions.append("candidate is partially verified")
    if source_state == "strategy_placeholder":
        blockers.append("candidate is a strategy placeholder, not a live instrument")
    elif source_state == "policy_placeholder":
        blockers.append("candidate is a policy placeholder, not a live instrument")
    elif sleeve_key in CORE_PASSIVE_SLEEVES and source_state == "source_linked_not_validated":
        blockers.append("candidate is source linked but not validated for a core sleeve")
    elif sleeve_key in CORE_PASSIVE_SLEEVES and source_state == "manual_seed":
        blockers.append("candidate source state is manual seeded for a core sleeve")
    elif sleeve_key in CORE_PASSIVE_SLEEVES and source_state == "broken_source":
        blockers.append("candidate source linkage is broken for a core sleeve")
    elif sleeve_key in CORE_PASSIVE_SLEEVES and source_state == "stale_live":
        blockers.append("candidate live source state is stale for a core sleeve")
    elif source_state == "broken_source":
        blockers.append("candidate source linkage is broken")
    elif source_state == "stale_live":
        blockers.append("candidate live source state is stale")
    elif source_state not in MIN_SOURCE_STATE_FOR_RECOMMENDATION:
        cautions.append(f"candidate source state is {source_state}")
    if factsheet_freshness == "quarantined":
        blockers.append("factsheet freshness is quarantined")
    elif factsheet_freshness in {"stale", "aging"}:
        cautions.append(f"factsheet freshness is {factsheet_freshness}")
    if freshness_state in {"quarantined", "stale"}:
        blockers.append(f"candidate freshness state is {freshness_state}")
    elif freshness_state == "aging":
        cautions.append("candidate freshness state is aging")
    hard_risk_failures: list[str] = []
    soft_risk_failures: list[str] = []
    for control in risk_controls:
        status = str(control.get("status") or "unknown")
        metric_name = str(control.get("metric_name") or "").strip()
        rationale = str(control.get("rationale") or metric_name or "Risk control issue")
        metric_key = metric_name.lower()
        if status == "fail":
            if metric_key in SOFT_RISK_FAILURES.get(sleeve_key, set()):
                soft_risk_failures.append(rationale)
            else:
                hard_risk_failures.append(rationale)
        elif status in {"warn", "unknown"} and rationale:
            soft_risk_failures.append(rationale)

    if hard_risk_failures:
        blockers.append("risk controls failed")
        cautions.extend(hard_risk_failures[:2])
    elif soft_risk_failures or risk_summary in CAUTION_RISK_STATES:
        cautions.append(f"risk controls are {risk_summary}")
        cautions.extend(soft_risk_failures[:2])
    if liquidity_status in CRITICAL_LIQUIDITY_BLOCKERS and spread_status == "wide":
        blockers.append("liquidity is weak and spreads are wide")
    elif liquidity_status in CAUTION_LIQUIDITY_STATES or spread_status in {"wide", "unknown"}:
        cautions.append(f"liquidity is {liquidity_status} and spread status is {spread_status}")

    if bool(candidate.get("margin_required")):
        blockers.append("margin is required")
    if candidate.get("max_loss_known") is False:
        blockers.append("max loss is not known")
    if bool(candidate.get("short_options")):
        blockers.append("short options are present")
    if bool(candidate.get("leverage_used")):
        blockers.append("leverage is used")

    if source_gaps:
        cautions.append(f"source gaps remain: {', '.join(source_gaps[:3])}")
    comparison_readiness = dict(dict(candidate.get("investment_lens") or {}).get("comparison_readiness") or {})
    if str(comparison_readiness.get("status") or "") == "incomplete":
        for item in list(comparison_readiness.get("blockers") or []):
            issue = str(item or "").strip()
            if not issue:
                continue
            classification = _classify_comparison_issue(issue=issue, sleeve_key=sleeve_key)
            if classification == "hard_blocker":
                blockers.append(issue)
            else:
                cautions.append(issue)
    if sleeve_key in CORE_PASSIVE_SLEEVES:
        if benchmark_validation in {"unassigned", "mismatch"} or benchmark_fit_type in {"weak_proxy", "mismatched"}:
            blockers.append("benchmark mapping is missing or invalid for a core sleeve")
        elif benchmark_validation in {"assigned_no_metrics", "proxy_matched"} or benchmark_fit_type == "acceptable_proxy":
            cautions.append(f"benchmark validation is {benchmark_validation}")
        if benchmark_confidence in {"unknown", "low"}:
            blockers.append("benchmark confidence is too low for a core sleeve")
    else:
        if benchmark_validation in {"unassigned", "mismatch"} or benchmark_fit_type == "weak_proxy":
            cautions.append(f"benchmark validation is {benchmark_validation}")
        elif benchmark_confidence in {"unknown", "low"}:
            cautions.append(f"benchmark confidence is {benchmark_confidence}")
    if performance_support_state == "unsupported":
        blockers.append("performance scoring support is unavailable")
    elif performance_support_state in {"partial_due_to_missing_metrics", "benchmark_proxy_only"}:
        cautions.append(f"performance support is {performance_support_state.replace('_', ' ')}")
    if liquidity_status == "unknown" or spread_status == "unknown":
        cautions.append("liquidity inputs are incomplete")
    if tax_score in {None, ""} and sleeve_key in CORE_PASSIVE_SLEEVES:
        blockers.append("tax data is too incomplete for sleeve type")

    exposure_fields = ("us_weight", "top_10_concentration", "holdings_count")
    exposure_gaps = [
        str(dict(field_truth.get(name) or {}).get("missingness_reason") or "")
        for name in exposure_fields
    ]
    if sleeve_key in EQUITY_COMPOSITION_SLEEVES and exposure_gaps and all(
        gap in {"fetchable_from_current_sources", "blocked_by_parser_gap", "blocked_by_source_gap"} for gap in exposure_gaps
    ):
        blockers.append("not yet usable because holdings-source coverage is still missing for key exposure fields")
    elif sleeve_key in BOND_LIKE_SLEEVES | CASH_LIKE_SLEEVES and exposure_gaps and any(
        gap in {"fetchable_from_current_sources", "blocked_by_parser_gap", "blocked_by_source_gap"} for gap in exposure_gaps
    ):
        cautions.append("holdings-source coverage is still incomplete for supplemental exposure fields")
    trading_currency_gap = str(dict(field_truth.get("primary_trading_currency") or {}).get("missingness_reason") or "")
    resolved_trading_currency = str(
        dict(field_truth.get("primary_trading_currency") or {}).get("resolved_value")
        or candidate.get("primary_trading_currency")
        or candidate.get("trading_currency")
        or ""
    ).strip()
    if (
        sleeve_key in CORE_PASSIVE_SLEEVES
        and not resolved_trading_currency
        and trading_currency_gap in {"fetchable_from_current_sources", "blocked_by_parser_gap", "blocked_by_source_gap"}
    ):
        blockers.append("not yet usable because primary trading currency evidence is still missing")
    listing_gap = str(dict(field_truth.get("primary_listing_exchange") or {}).get("missingness_reason") or "")
    if sleeve_key in CORE_PASSIVE_SLEEVES and listing_gap in {"fetchable_from_current_sources", "blocked_by_parser_gap", "blocked_by_source_gap"}:
        cautions.append("listing evidence is still incomplete for liquidity and implementation review")
    tracking_gap = str(dict(field_truth.get("tracking_difference_1y") or {}).get("missingness_reason") or "")
    if sleeve_key in EQUITY_COMPOSITION_SLEEVES and tracking_gap in {"fetchable_from_current_sources", "blocked_by_parser_gap", "blocked_by_source_gap"}:
        cautions.append("tracking-difference history is still incomplete")
    elif sleeve_key in BOND_LIKE_SLEEVES | CASH_LIKE_SLEEVES and tracking_gap in {"fetchable_from_current_sources", "blocked_by_parser_gap", "blocked_by_source_gap"}:
        cautions.append("tracking-difference history is still incomplete")

    blockers = list(dict.fromkeys(blockers))
    cautions = list(dict.fromkeys(cautions))
    blocker_details = [_classify_eligibility_reason(reason, candidate=candidate, field_truth=field_truth, severity="critical") for reason in blockers]
    caution_details = [_classify_eligibility_reason(reason, candidate=candidate, field_truth=field_truth, severity="important") for reason in cautions]
    pressures = _classify_pressures(
        candidate=candidate,
        sleeve_key=sleeve_key,
        blockers=blockers,
        cautions=cautions,
        benchmark_assignment=benchmark_assignment,
        performance_support_state=performance_support_state,
        factsheet_freshness=factsheet_freshness,
        source_state=source_state,
        freshness_state=freshness_state,
        liquidity_status=liquidity_status,
        spread_status=spread_status,
        tax_score=tax_score,
    )
    pressures = enrich_pressures_with_state(pressures=pressures, candidate_history=candidate_history)
    confidence = "high"
    if blockers:
        confidence = "low"
    elif cautions or unknowns or source_gaps:
        confidence = "medium"

    if blockers and any(
        blocker in blockers
        for blocker in (
            "candidate is unverified",
            "factsheet freshness is quarantined",
            "risk controls failed",
            "liquidity is weak and spreads are wide",
            "margin is required",
            "max loss is not known",
            "short options are present",
            "leverage is used",
        )
    ):
        state = "ineligible"
    elif blockers:
        state = "data_incomplete"
    elif cautions:
        state = "eligible_with_caution"
    else:
        state = "eligible"

    role = _infer_role_in_portfolio(candidate=candidate, sleeve_key=sleeve_key)
    return {
        "eligibility_state": state,
        "eligibility_blockers": blockers,
        "eligibility_cautions": cautions,
        "eligibility_blocker_details": blocker_details,
        "eligibility_caution_details": caution_details,
        "data_confidence": confidence,
        "factsheet_freshness": factsheet_freshness,
        "role_in_portfolio": role,
        "provenance": provenance,
        "pressures": pressures,
        "primary_pressure_type": pressures[0]["pressure_type"] if pressures else None,
        "secondary_pressure_type": pressures[1]["pressure_type"] if len(pressures) > 1 else None,
    }


def _classify_eligibility_reason(
    reason: str,
    *,
    candidate: dict[str, Any],
    field_truth: dict[str, Any],
    severity: str,
) -> dict[str, Any]:
    lowered = str(reason or "").lower()
    category = "missing_required_evidence"
    root_cause = "missing_required_evidence"
    field_name = None
    if any(token in lowered for token in {"margin", "max loss", "short options", "leverage", "risk controls failed", "governance state"}):
        category = "genuine_policy_failure"
        root_cause = "policy_failure"
    elif any(token in lowered for token in {"manual seeded", "source linked but not validated", "broken source", "missing holdings source coverage"}):
        category = "ingest_configuration_gap"
        root_cause = "source_coverage_gap"
    elif any(token in lowered for token in {"liquidity is", "spread status", "benchmark validation is", "performance support is", "listing evidence is incomplete"}):
        category = "weak_but_usable_evidence"
        root_cause = "weak_supporting_evidence"
    if "primary trading currency" in lowered:
        field_name = "primary_trading_currency"
        root_cause = "missing_primary_trading_currency"
    elif "tracking-difference history" in lowered:
        field_name = "tracking_difference_1y"
        root_cause = "incomplete_tracking_difference_history"
    elif "holdings source coverage" in lowered:
        field_name = "us_weight"
    elif "benchmark" in lowered:
        field_name = "benchmark_key"
    elif "tax data" in lowered:
        field_name = "withholding_tax_posture"
    source_type = str(dict(field_truth.get(field_name) or {}).get("source_type") or "") if field_name else None
    return {
        "category": category,
        "severity": severity,
        "reason": reason,
        "root_cause": root_cause,
        "field_name": field_name,
        "source_type": source_type,
    }


def _classify_comparison_issue(*, issue: str, sleeve_key: str) -> str:
    lowered = issue.lower()
    if "primary trading currency" in lowered:
        return "hard_blocker"
    if "holdings source coverage" in lowered:
        return "hard_blocker" if sleeve_key in EQUITY_COMPOSITION_SLEEVES else "confidence_reducer"
    if "share-class" in lowered:
        return "upgrade_condition"
    if "tracking" in lowered:
        return "confidence_reducer"
    if "yield proxy" in lowered or "weighted average maturity" in lowered or "underlying currency" in lowered or "redemption settlement" in lowered:
        return "confidence_reducer"
    return "hard_blocker" if sleeve_key in EQUITY_COMPOSITION_SLEEVES else "confidence_reducer"


def _infer_role_in_portfolio(*, candidate: dict[str, Any], sleeve_key: str) -> str:
    role_line = str(dict(candidate.get("investment_lens") or {}).get("role_line") or "").strip()
    if role_line:
        return role_line
    mapping = {
        "global_equity_core": "Core global equity implementation.",
        "developed_ex_us_optional": "Optional developed-market split sleeve.",
        "emerging_markets": "Emerging-markets satellite sleeve.",
        "china_satellite": "China satellite sleeve.",
        "ig_bonds": "Investment-grade defensive ballast sleeve.",
        "cash_bills": "Cash and bills liquidity sleeve.",
        "real_assets": "Real-assets diversifier sleeve.",
        "alternatives": "Alternative diversifier sleeve.",
        "convex": "Convex protection sleeve.",
    }
    return mapping.get(str(sleeve_key), "Portfolio implementation sleeve.")


def _classify_pressures(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    blockers: list[str],
    cautions: list[str],
    benchmark_assignment: dict[str, Any],
    performance_support_state: str,
    factsheet_freshness: str,
    source_state: str,
    freshness_state: str,
    liquidity_status: str,
    spread_status: str,
    tax_score: Any,
) -> list[dict[str, Any]]:
    pressure_map: dict[str, dict[str, Any]] = {}

    def add_pressure(
        pressure_type: str,
        *,
        severity: str,
        label: str,
        detail: str,
        evidence: list[str] | None = None,
        recommendation_effect: str | None = None,
    ) -> None:
        existing = pressure_map.get(pressure_type)
        severity_rank = {"critical": 0, "important": 1, "informational": 2}
        payload = {
            "pressure_type": pressure_type,
            "severity": severity,
            "label": label,
            "detail": detail,
            "evidence": list(evidence or []),
            "recommendation_effect": recommendation_effect or detail,
        }
        if existing is None or severity_rank[severity] < severity_rank[str(existing.get("severity") or "informational")]:
            pressure_map[pressure_type] = payload

    benchmark_effect = str(benchmark_assignment.get("benchmark_effect_type") or "")
    benchmark_kind = str(benchmark_assignment.get("benchmark_kind") or "")
    if benchmark_effect in {"benchmark_fit_weak", "benchmark_data_incomplete"}:
        add_pressure(
            "benchmark",
            severity="critical" if sleeve_key in CORE_PASSIVE_SLEEVES else "important",
            label="Benchmark pressure",
            detail=str(benchmark_assignment.get("benchmark_explanation") or "Benchmark comparison support is still weak or incomplete."),
            evidence=[
                str(benchmark_assignment.get("benchmark_key") or ""),
                str(benchmark_assignment.get("validation_status") or ""),
                str(benchmark_assignment.get("benchmark_confidence") or ""),
            ],
            recommendation_effect=str(benchmark_assignment.get("recommendation_confidence_effect") or ""),
        )
    elif benchmark_effect == "benchmark_fit_proxy_acceptable":
        add_pressure(
            "benchmark",
            severity="important",
            label="Benchmark pressure",
            detail=str(benchmark_assignment.get("proxy_usage_explanation") or "A proxy benchmark is usable here, but it still limits confidence."),
            evidence=[benchmark_kind, str(benchmark_assignment.get("benchmark_proxy_symbol") or "")],
            recommendation_effect=str(benchmark_assignment.get("recommendation_confidence_effect") or ""),
        )

    data_evidence = []
    if source_state not in MIN_SOURCE_STATE_FOR_RECOMMENDATION:
        data_evidence.append(f"source_state={source_state}")
    if factsheet_freshness in {"aging", "stale", "quarantined"}:
        data_evidence.append(f"factsheet_freshness={factsheet_freshness}")
    if freshness_state in {"aging", "stale", "quarantined"}:
        data_evidence.append(f"freshness_state={freshness_state}")
    if any("unverified" in item or "source gap" in item or "freshness" in item for item in [*blockers, *cautions]):
        add_pressure(
            "data",
            severity="critical" if any("unverified" in item or "quarantined" in item for item in blockers) else "important",
            label="Data pressure",
            detail="Data freshness, direct verification, or source support still limits how confidently this candidate can be used.",
            evidence=data_evidence or blockers[:2] or cautions[:2],
            recommendation_effect="This keeps the candidate in review until stronger direct evidence is available.",
        )

    if any(item in blockers for item in ["margin is required", "max loss is not known", "short options are present", "leverage is used"]) or str(candidate.get("source_state") or "") in {"strategy_placeholder", "policy_placeholder"}:
        add_pressure(
            "structure",
            severity="critical",
            label="Structure pressure",
            detail="Vehicle structure or implementation mechanics do not fit the sleeve's current constraints cleanly enough.",
            evidence=[item for item in blockers if item in {"margin is required", "max loss is not known", "short options are present", "leverage is used"}],
            recommendation_effect="This is treated as a hard stop for recommendation use.",
        )
    elif any("structure" in item.lower() or "wrapper" in item.lower() for item in cautions):
        add_pressure(
            "structure",
            severity="important",
            label="Structure pressure",
            detail="Wrapper or structural fit is usable for review, but not as clean as the strongest peers.",
            evidence=cautions[:2],
        )

    if liquidity_status in {"weak", "unknown"} or spread_status in {"wide", "unknown"}:
        add_pressure(
            "liquidity",
            severity="critical" if liquidity_status == "weak" and spread_status == "wide" else "important",
            label="Liquidity pressure",
            detail=f"Liquidity support is {liquidity_status} and spread support is {spread_status}.",
            evidence=[f"liquidity_status={liquidity_status}", f"spread_status={spread_status}"],
            recommendation_effect="This can keep a candidate in review even when the broader investment case looks attractive.",
        )

    if tax_score in {None, ""} or any("tax" in item.lower() for item in [*blockers, *cautions]):
        add_pressure(
            "tax_wrapper",
            severity="critical" if tax_score in {None, ""} and sleeve_key in CORE_PASSIVE_SLEEVES else "important",
            label="Tax or wrapper pressure",
            detail="SG tax posture or wrapper mechanics still limit implementation confidence.",
            evidence=[f"tax_score={tax_score}", str(candidate.get("domicile") or ""), str(candidate.get("instrument_type") or "")],
            recommendation_effect="This lowers implementation confidence until the tax and wrapper posture is clearer.",
        )

    if performance_support_state in {"unsupported", "partial_due_to_missing_metrics", "benchmark_proxy_only"}:
        add_pressure(
            "performance_evidence",
            severity="critical" if performance_support_state == "unsupported" else "important",
            label="Performance evidence pressure",
            detail=f"Benchmark-relative performance support is {performance_support_state.replace('_', ' ')}.",
            evidence=[performance_support_state],
            recommendation_effect="This prevents historical benchmark-relative evidence from being fully decisive.",
        )

    if blockers or cautions:
        add_pressure(
            "readiness",
            severity="critical" if blockers else "important",
            label="Readiness pressure",
            detail="The candidate is not yet ready for active selection because important issues are still unresolved.",
            evidence=blockers[:3] or cautions[:3],
            recommendation_effect="This keeps the candidate below recommendation-ready until the key issues clear.",
        )

    best_alternative = dict(dict(candidate.get("decision_readiness") or {}).get("best_alternative") or {})
    if best_alternative.get("symbol"):
        add_pressure(
            "replacement",
            severity="informational",
            label="Replacement pressure",
            detail=f"Peer {best_alternative.get('symbol')} currently clears more of the selection case.",
            evidence=[str(best_alternative.get("symbol") or ""), str(best_alternative.get("readiness_level") or "")],
            recommendation_effect="This matters if the peer remains stronger while this candidate still cannot clear the key requirements.",
        )

    return sorted(
        pressure_map.values(),
        key=lambda item: (
            {"critical": 0, "important": 1, "informational": 2}.get(str(item.get("severity") or "informational"), 3),
            PRESSURE_ORDER.get(str(item.get("pressure_type") or ""), 99),
            str(item.get("label") or ""),
        ),
    )
