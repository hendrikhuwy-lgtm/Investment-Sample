from __future__ import annotations

from typing import Any

from app.v2.core.domain_objects import FrameworkRestraint


def translate_framework_judgment(judgment: dict[str, Any]) -> FrameworkRestraint:
    lens_id = str(judgment.get("lens_id") or "unknown_lens")
    return FrameworkRestraint(
        restraint_id=f"restraint_{lens_id}",
        framework_id=lens_id,
        label=lens_id.replace("_", " ").title(),
        posture=str(judgment.get("lens_status") or "neutral"),
        promotion_cap=str(judgment.get("promotion_cap") or "none"),
        review_intensity_modifier=str(judgment.get("review_intensity_modifier") or "none"),
        confidence_modifier=str(judgment.get("confidence_modifier") or "none"),
        action_tone_constraint=str(judgment.get("action_tone_constraint") or "none"),
        rationale=str(judgment.get("investor_summary") or "Translated from donor lens output."),
        supports=[str(item) for item in judgment.get("supports") or []],
        cautions=[str(item) for item in judgment.get("cautions") or []],
        claim_constraints=[str(item) for item in judgment.get("claim_constraints") or []],
        what_changes_view=[str(item) for item in judgment.get("what_changes_view") or []],
    )

