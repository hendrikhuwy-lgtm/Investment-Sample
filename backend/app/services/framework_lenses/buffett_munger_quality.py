from __future__ import annotations

from typing import Any

from app.services.framework_constitution import classify_complexity
from app.services.framework_lenses.contracts import build_lens_judgment


def evaluate_buffett_munger_quality(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    current_holding_record: dict[str, Any],
    tax_assumption_status: dict[str, Any],
    gate_summary: dict[str, Any],
) -> dict[str, Any]:
    complexity = classify_complexity(candidate, sleeve_key)
    cautions: list[str] = []
    supports: list[str] = []
    promotion_cap = "none"
    posture = "neutral"

    if complexity.get("violates_mandate"):
        cautions.append("The structure is outside a clean owner-like mandate because the complexity cannot be defended simply.")
        posture = "blocking"
        promotion_cap = "acceptable"
    elif not complexity.get("simply_explainable"):
        cautions.append("The structure is harder to hold with long-horizon discipline than a simple passive default.")
        posture = "constraining"
        promotion_cap = "near_decision_ready"

    if bool(current_holding_record.get("current_symbol")) and bool(current_holding_record.get("status") == "matched_to_current" or current_holding_record.get("practical_edge", {}).get("status") != "sufficient"):
        supports.append("The incumbent still looks good enough that unnecessary churn should stay hard to justify.")
        posture = "supportive" if posture == "neutral" else posture

    if (
        str(tax_assumption_status.get("tax_confidence") or "") == "high"
        and not gate_summary.get("buyable_blocked")
        and complexity.get("simply_explainable")
    ):
        supports.append("The case is cleaner because structure, tax, and explainability remain aligned.")
        posture = "supportive" if posture == "neutral" else posture

    return build_lens_judgment(
        lens_id="buffett_munger_quality",
        lens_status=posture,
        confidence="high" if supports and not cautions else "medium",
        promotion_cap=promotion_cap,
        supports=supports,
        cautions=cautions,
        claim_constraints=["Do not treat structural neatness as enough to bypass evidence, tax, or comparison blockers."],
        what_changes_view=["A clearer practical edge over the incumbent would matter more than a merely interesting structure."],
        investor_summary=supports[0] if supports and not cautions else cautions[0] if cautions else "Structural quality is acceptable, but it does not by itself justify disturbing the portfolio.",
    )
