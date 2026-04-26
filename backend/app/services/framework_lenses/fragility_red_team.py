from __future__ import annotations

from typing import Any

from app.services.framework_lenses.contracts import build_lens_judgment


def evaluate_fragility_red_team(
    *,
    gate_summary: dict[str, Any],
    source_integrity_result: dict[str, Any],
    forecast_defensibility_status: dict[str, Any],
    current_holding_record: dict[str, Any],
) -> dict[str, Any]:
    cautions: list[str] = []
    supports: list[str] = []
    promotion_cap = "none"
    posture = "neutral"
    confidence_modifier = "none"
    action_tone = "none"

    unresolved_limits = list(gate_summary.get("unresolved_limits") or [])
    if unresolved_limits:
        cautions.extend([str(item) for item in unresolved_limits[:2]])
        posture = "constraining"
        promotion_cap = "near_decision_ready"
        confidence_modifier = "soften"
        action_tone = "restrained"

    if str(source_integrity_result.get("overall_caution_level") or "") in {"material", "critical"}:
        cautions.append("Source integrity still carries enough caution that the thesis should not be treated as robust.")
        posture = "blocking"
        promotion_cap = "acceptable"
        confidence_modifier = "materially_soften"
        action_tone = "monitoring_only"

    if str(forecast_defensibility_status.get("display_grade") or "") == "HIDE":
        cautions.append("Forecast support is not defensible enough to rescue the case.")
        posture = "constraining" if posture != "blocking" else posture

    if bool(current_holding_record.get("current_symbol")) and bool(current_holding_record.get("practical_edge", {}).get("status") == "sufficient"):
        supports.append("Practical edge is visible, so fragility is less likely to rest on a single flattering datapoint.")

    return build_lens_judgment(
        lens_id="fragility_red_team",
        lens_status=posture,
        confidence="high" if cautions else "medium",
        promotion_cap=promotion_cap,
        confidence_modifier=confidence_modifier,
        action_tone_constraint=action_tone,
        supports=supports,
        cautions=cautions,
        blocker_flags=cautions if posture == "blocking" else [],
        claim_constraints=["Do not let missing or thin evidence hide behind smooth recommendation language."],
        what_changes_view=["Direct fresher evidence, a cleaner rival comparison, and fewer unresolved limits would reduce fragility pressure."],
        investor_summary=cautions[0] if cautions else supports[0] if supports else "Fragility review does not currently add a decisive objection, but it remains a restraint lens.",
    )
