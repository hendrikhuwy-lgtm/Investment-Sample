from __future__ import annotations

from typing import Any

from app.services.framework_lenses.contracts import build_lens_judgment


def evaluate_marks_cycle_risk(
    *,
    recommendation_result: dict[str, Any],
    source_integrity_result: dict[str, Any],
    forecast_defensibility_status: dict[str, Any],
    benchmark_support_status: dict[str, Any],
    portfolio_consequence_summary: dict[str, Any],
) -> dict[str, Any]:
    cautions: list[str] = []
    supports: list[str] = []
    posture = "neutral"
    confidence_modifier = "none"
    review_modifier = "none"
    action_tone = "none"

    if bool(recommendation_result.get("no_change_is_best")):
        supports.append("The base engine already prefers patience over churn, which fits a cycle-aware discipline.")

    if str(source_integrity_result.get("overall_caution_level") or "") in {"material", "critical"}:
        cautions.append("Current evidence quality is still fragile enough that standards should tighten rather than loosen.")
        posture = "cautious"
        confidence_modifier = "soften"
        action_tone = "restrained"

    if str(forecast_defensibility_status.get("display_grade") or "") == "HIDE":
        cautions.append("Forecast support is not defensible enough to carry a cycle-sensitive conclusion.")
        posture = "cautious"
        review_modifier = "raise_to_universal"
        action_tone = "restrained"

    if str(benchmark_support_status.get("support_class") or "") in {"weak_proxy", "unavailable"}:
        cautions.append("Benchmark support is too weak for performance-led cycle conclusions.")
        posture = "cautious"
        action_tone = "restrained"

    portfolio_summary = str(portfolio_consequence_summary.get("summary") or "").strip()
    if portfolio_summary:
        supports.append("Cycle interpretation remains subordinate to the sleeve's long-run portfolio job.")

    return build_lens_judgment(
        lens_id="marks_cycle_risk",
        lens_status=posture,
        confidence="medium",
        review_intensity_modifier=review_modifier,
        confidence_modifier=confidence_modifier,
        action_tone_constraint=action_tone,
        supports=supports,
        cautions=cautions,
        claim_constraints=["Do not let cycle framing create a direct trade recommendation."],
        what_changes_view=["Fresher direct evidence and stronger benchmark authority would reduce cycle-related caution."],
        investor_summary=supports[0] if supports and not cautions else cautions[0] if cautions else "Cycle context is informative, but it should remain a restraint lens rather than an action trigger.",
    )
