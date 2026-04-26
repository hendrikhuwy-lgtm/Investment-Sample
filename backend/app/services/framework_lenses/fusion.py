from __future__ import annotations

from typing import Any

from app.services.framework_constitution import clamp_promotion_state, lens_outputs_explanatory_only
from app.services.framework_lenses.contracts import build_lens_fusion_result_payload


def _order(value: str, ordered: tuple[str, ...]) -> int:
    try:
        return ordered.index(value)
    except ValueError:
        return -1


def build_lens_fusion_result(
    *,
    gate_summary: dict[str, Any],
    base_promotion_state: str,
    lens_judgments: dict[str, dict[str, Any]],
    recommendation_result: dict[str, Any],
) -> dict[str, Any]:
    # Framework lenses are bounded and downgrade-only. This fusion step may preserve or cap the
    # base promotion state, but it must never create a stronger recommendation than the base engine.
    if lens_outputs_explanatory_only(gate_summary=gate_summary):
        return build_lens_fusion_result_payload(
            overall_lens_posture="explanatory_only",
            promotion_cap=clamp_promotion_state(base_promotion_state, "acceptable") if base_promotion_state in {"near_decision_ready", "buyable"} else "none",
            confidence_modifier="materially_soften",
            review_intensity_modifier="raise_to_universal",
            action_tone_constraint="monitoring_only",
            explanatory_only=True,
            dominant_supports=[],
            dominant_cautions=list(gate_summary.get("unresolved_limits") or [])[:4],
            applied_by_lens={lens_id: {"lens_status": "explanatory_only"} for lens_id in lens_judgments},
        )

    promotion_cap = "none"
    confidence_modifier = "none"
    review_modifier = "none"
    action_tone = "none"
    dominant_supports: list[str] = []
    dominant_cautions: list[str] = []
    applied: dict[str, Any] = {}
    blocking_fragility = False

    for lens_id, judgment in lens_judgments.items():
        applied[lens_id] = {
            "lens_status": judgment.get("lens_status"),
            "promotion_cap": judgment.get("promotion_cap"),
            "confidence_modifier": judgment.get("confidence_modifier"),
            "review_intensity_modifier": judgment.get("review_intensity_modifier"),
            "action_tone_constraint": judgment.get("action_tone_constraint"),
            "investor_summary": judgment.get("investor_summary"),
        }
        if lens_id == "fragility_red_team" and str(judgment.get("lens_status") or "") == "blocking":
            blocking_fragility = True
        cap = str(judgment.get("promotion_cap") or "none")
        if cap == "acceptable" or (cap == "near_decision_ready" and promotion_cap == "none"):
            promotion_cap = cap
        modifier = str(judgment.get("confidence_modifier") or "none")
        if _order(modifier, ("none", "soften", "materially_soften")) > _order(confidence_modifier, ("none", "soften", "materially_soften")):
            confidence_modifier = modifier
        review = str(judgment.get("review_intensity_modifier") or "none")
        if _order(review, ("none", "raise_to_universal", "raise_to_deep")) > _order(review_modifier, ("none", "raise_to_universal", "raise_to_deep")):
            review_modifier = review
        tone = str(judgment.get("action_tone_constraint") or "none")
        if _order(tone, ("none", "restrained", "monitoring_only")) > _order(action_tone, ("none", "restrained", "monitoring_only")):
            action_tone = tone
        dominant_supports.extend([str(item) for item in list(judgment.get("supports") or [])[:2]])
        dominant_cautions.extend([str(item) for item in list(judgment.get("cautions") or [])[:2]])

    if bool(recommendation_result.get("no_change_is_best")) and promotion_cap == "none":
        action_tone = "restrained" if action_tone == "none" else action_tone

    if blocking_fragility:
        posture = "blocked_by_fragility"
    elif promotion_cap != "none":
        posture = "promotion_constrained"
    elif len(dominant_cautions) >= 2:
        posture = "caution_dominant"
    elif dominant_supports and not dominant_cautions:
        posture = "supportive_with_restraint"
    else:
        posture = "mixed_but_constructive"

    return build_lens_fusion_result_payload(
        overall_lens_posture=posture,
        promotion_cap=promotion_cap,
        confidence_modifier=confidence_modifier,
        review_intensity_modifier=review_modifier,
        action_tone_constraint=action_tone,
        explanatory_only=False,
        dominant_supports=dominant_supports,
        dominant_cautions=dominant_cautions,
        applied_by_lens=applied,
    )
