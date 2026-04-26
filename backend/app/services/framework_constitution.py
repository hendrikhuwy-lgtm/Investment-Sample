from __future__ import annotations

from typing import Any


INVESTOR_PROFILE = {
    "jurisdiction": "singapore_retail",
    "horizon": "long_horizon_accumulation",
    "style": "passive_first",
    "risk_bias": "moderate_growth_resilience_biased",
    "turnover_preference": "low_turnover",
    "manual_approval_required_for_non_rebalance_changes": True,
}

PROMOTION_LADDER = (
    "research_only",
    "acceptable",
    "near_decision_ready",
    "buyable",
)

PROMOTION_ORDER = {state: index for index, state in enumerate(PROMOTION_LADDER)}

CORE_SLEEVES = {
    "global_equity_core",
    "developed_ex_us_optional",
    "emerging_markets",
    "ig_bonds",
    "cash_bills",
}

SATELLITE_SLEEVES = {
    "china_satellite",
    "real_assets",
    "alternatives",
    "convex",
}

NON_BUYABLE_BLOCKER_CODES = (
    "core_holdings_incomplete",
    "current_holding_comparison_incomplete",
    "nearest_rival_comparison_weak",
    "significant_tax_uncertainty",
    "benchmark_authority_too_weak",
    "implementation_friction_unclear",
    "structure_too_complex",
    "mandate_complexity_violation",
    "switch_cost_unclear",
    "forecast_dependent_case",
)

ACTION_BOUNDARY_STATES = (
    "no_change",
    "monitor_only",
    "secondary_review_only",
    "compare_in_main_path",
    "manual_review_required",
    "rebalance_only",
    "blocked",
)

LENS_IDS = (
    "marks_cycle_risk",
    "buffett_munger_quality",
    "dalio_regime_transmission",
    "implementation_reality",
    "fragility_red_team",
)

LENS_STATUSES = (
    "supportive",
    "neutral",
    "cautious",
    "constraining",
    "blocking",
    "explanatory_only",
)

LENS_PROMOTION_CAPS = (
    "none",
    "acceptable",
    "near_decision_ready",
)

LENS_REVIEW_INTENSITY_MODIFIERS = (
    "none",
    "raise_to_universal",
    "raise_to_deep",
)

LENS_CONFIDENCE_MODIFIERS = (
    "none",
    "soften",
    "materially_soften",
)

LENS_ACTION_TONE_CONSTRAINTS = (
    "none",
    "restrained",
    "monitoring_only",
)

LENS_OVERALL_POSTURES = (
    "supportive_with_restraint",
    "mixed_but_constructive",
    "caution_dominant",
    "promotion_constrained",
    "blocked_by_fragility",
    "explanatory_only",
)

BENCHMARK_PERFORMANCE_ALLOWED = {"direct"}
BENCHMARK_STRUCTURAL_ALLOWED = {"direct", "acceptable_proxy"}
TAX_DECISIVE_ALLOWED = {"SUFFICIENT"}
FORECAST_MAIN_PATH_ALLOWED = {"SOFT_SCENARIO_ONLY", "FULLY_DISPLAYABLE"}


def is_core_sleeve(sleeve_key: str) -> bool:
    return str(sleeve_key or "") in CORE_SLEEVES


def is_satellite_sleeve(sleeve_key: str) -> bool:
    return str(sleeve_key or "") in SATELLITE_SLEEVES


def requires_manual_approval(*, action_type: str, is_rebalance: bool = False) -> bool:
    if is_rebalance:
        return False
    return str(action_type or "").upper() in {"ADD", "REPLACE", "TRIM"}


def benchmark_performance_language_allowed(support_class: str) -> bool:
    return str(support_class or "") in BENCHMARK_PERFORMANCE_ALLOWED


def benchmark_structural_language_allowed(support_class: str) -> bool:
    return str(support_class or "") in BENCHMARK_STRUCTURAL_ALLOWED


def decisive_tax_language_allowed(assumption_grade: str, tax_confidence: str) -> bool:
    return str(assumption_grade or "") in TAX_DECISIVE_ALLOWED and str(tax_confidence or "") == "high"


def forecast_may_influence_main_path(display_grade: str) -> bool:
    return str(display_grade or "") in FORECAST_MAIN_PATH_ALLOWED


def manual_approval_boundary_text(action_type: str) -> str:
    action = str(action_type or "").upper()
    if action == "REPLACE":
        return "Do not switch automatically. Escalate this through manual approval before any holding change."
    if action == "ADD":
        return "Do not allocate automatically. Treat this as a manual approval candidate only."
    if action == "TRIM":
        return "Do not trim automatically. Review manually before changing the live portfolio."
    return "No manual approval action is currently required."


def clamp_promotion_state(base_promotion_state: str, promotion_cap: str) -> str:
    base = str(base_promotion_state or "research_only")
    cap = str(promotion_cap or "none")
    if cap == "none":
        return base
    if base not in PROMOTION_ORDER:
        return base
    target = cap if cap in PROMOTION_ORDER else base
    if PROMOTION_ORDER[base] > PROMOTION_ORDER[target]:
        return target
    return base


def lens_outputs_explanatory_only(*, gate_summary: dict[str, Any]) -> bool:
    return bool(gate_summary.get("buyable_blocked")) or str(gate_summary.get("gate_overall_status") or "") == "fail"


def classify_complexity(candidate: dict[str, Any], sleeve_key: str) -> dict[str, Any]:
    instrument_type = str(candidate.get("instrument_type") or "")
    leverage_used = bool(candidate.get("leverage_used"))
    max_loss_known = candidate.get("max_loss_known")
    derivatives_usage = str(candidate.get("factsheet_summary", {}).get("derivatives_usage") or candidate.get("derivatives_usage") or "")
    structure_first = str(candidate.get("evidence_depth_class") or "") == "structure_first"

    violates_mandate = leverage_used or max_loss_known is False or "extensive" in derivatives_usage.lower()
    simply_explainable = (
        instrument_type in {"etf_ucits", "etf_us", "t_bill_sg", "money_market_fund_sg", "cash_account_sg"}
        and not violates_mandate
        and not structure_first
    )
    if structure_first and str(sleeve_key or "") in {"alternatives", "convex"}:
        simply_explainable = False
    return {
        "simply_explainable": simply_explainable,
        "violates_mandate": violates_mandate,
        "summary": (
            "Structure is simple enough for the investor mandate."
            if simply_explainable
            else "Structure requires explicit explanation and cannot be treated as a simple passive default."
        ),
    }
