from __future__ import annotations

from typing import Any

from app.services.framework_constitution import (
    LENS_ACTION_TONE_CONSTRAINTS,
    LENS_CONFIDENCE_MODIFIERS,
    LENS_IDS,
    LENS_OVERALL_POSTURES,
    LENS_PROMOTION_CAPS,
    LENS_REVIEW_INTENSITY_MODIFIERS,
    LENS_STATUSES,
)


def _unique_text(items: list[Any], *, limit: int = 6) -> list[str]:
    seen: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.append(text)
        if len(seen) >= limit:
            break
    return seen


def build_lens_judgment(
    *,
    lens_id: str,
    lens_status: str,
    confidence: str = "medium",
    promotion_cap: str = "none",
    review_intensity_modifier: str = "none",
    confidence_modifier: str = "none",
    action_tone_constraint: str = "none",
    supports: list[Any] | None = None,
    cautions: list[Any] | None = None,
    blocker_flags: list[Any] | None = None,
    portfolio_relevance: Any = "",
    claim_constraints: list[Any] | None = None,
    what_changes_view: list[Any] | None = None,
    investor_summary: Any = "",
) -> dict[str, Any]:
    assert lens_id in LENS_IDS
    assert lens_status in LENS_STATUSES
    assert confidence in {"high", "medium", "low"}
    assert promotion_cap in LENS_PROMOTION_CAPS
    assert review_intensity_modifier in LENS_REVIEW_INTENSITY_MODIFIERS
    assert confidence_modifier in LENS_CONFIDENCE_MODIFIERS
    assert action_tone_constraint in LENS_ACTION_TONE_CONSTRAINTS
    return {
        "lens_id": lens_id,
        "lens_status": lens_status,
        "confidence": confidence,
        "promotion_cap": promotion_cap,
        "review_intensity_modifier": review_intensity_modifier,
        "confidence_modifier": confidence_modifier,
        "action_tone_constraint": action_tone_constraint,
        "supports": _unique_text(list(supports or [])),
        "cautions": _unique_text(list(cautions or [])),
        "blocker_flags": _unique_text(list(blocker_flags or [])),
        "portfolio_relevance": str(portfolio_relevance or "").strip(),
        "claim_constraints": _unique_text(list(claim_constraints or [])),
        "what_changes_view": _unique_text(list(what_changes_view or [])),
        "investor_summary": str(investor_summary or "").strip(),
    }


def identity_fusion_result() -> dict[str, Any]:
    return {
        "overall_lens_posture": "mixed_but_constructive",
        "promotion_cap": "none",
        "confidence_modifier": "none",
        "review_intensity_modifier": "none",
        "action_tone_constraint": "none",
        "explanatory_only": False,
        "dominant_supports": [],
        "dominant_cautions": [],
        "applied_by_lens": {},
    }


def build_lens_fusion_result_payload(
    *,
    overall_lens_posture: str,
    promotion_cap: str,
    confidence_modifier: str,
    review_intensity_modifier: str,
    action_tone_constraint: str,
    explanatory_only: bool,
    dominant_supports: list[Any],
    dominant_cautions: list[Any],
    applied_by_lens: dict[str, Any],
) -> dict[str, Any]:
    assert overall_lens_posture in LENS_OVERALL_POSTURES
    assert promotion_cap in LENS_PROMOTION_CAPS
    assert confidence_modifier in LENS_CONFIDENCE_MODIFIERS
    assert review_intensity_modifier in LENS_REVIEW_INTENSITY_MODIFIERS
    assert action_tone_constraint in LENS_ACTION_TONE_CONSTRAINTS
    return {
        "overall_lens_posture": overall_lens_posture,
        "promotion_cap": promotion_cap,
        "confidence_modifier": confidence_modifier,
        "review_intensity_modifier": review_intensity_modifier,
        "action_tone_constraint": action_tone_constraint,
        "explanatory_only": bool(explanatory_only),
        "dominant_supports": _unique_text(list(dominant_supports or []), limit=5),
        "dominant_cautions": _unique_text(list(dominant_cautions or []), limit=6),
        "applied_by_lens": applied_by_lens,
    }
