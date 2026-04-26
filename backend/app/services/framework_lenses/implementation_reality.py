from __future__ import annotations

from typing import Any

from app.services.framework_lenses.contracts import build_lens_judgment


def evaluate_implementation_reality(
    *,
    gate_summary: dict[str, Any],
    tax_assumption_status: dict[str, Any],
    portfolio_completeness_status: dict[str, Any],
    current_holding_record: dict[str, Any],
    cost_realism_summary: dict[str, Any],
) -> dict[str, Any]:
    cautions: list[str] = []
    supports: list[str] = []
    promotion_cap = "none"
    posture = "neutral"
    confidence_modifier = "none"
    action_tone = "none"

    if str(gate_summary.get("liquidity_bucket_state") or "") in {"missing", "proxy_only"}:
        cautions.append("Liquidity and implementation support remain too weak for a clean buyability call.")
        posture = "blocking"
        promotion_cap = "acceptable"
        confidence_modifier = "materially_soften"
        action_tone = "restrained"

    if str(gate_summary.get("tax_confidence") or "") != "high":
        cautions.append("Tax clarity is not strong enough to treat implementation as clean and settled.")
        posture = "constraining" if posture != "blocking" else posture
        promotion_cap = "acceptable" if promotion_cap == "none" else promotion_cap
        confidence_modifier = "materially_soften" if posture == "blocking" else "soften"
        action_tone = "restrained"

    if not bool(portfolio_completeness_status.get("switch_cost_estimated")):
        cautions.append("Switch friction is not estimated tightly enough to support a clean replacement call.")
        posture = "blocking"
        promotion_cap = "acceptable"
        confidence_modifier = "materially_soften"
        action_tone = "restrained"

    if str(dict(current_holding_record.get("practical_edge") or {}).get("status") or "") == "sufficient":
        supports.append("Practical edge is visible after switching friction, which supports implementation realism.")

    cost_summary = str(cost_realism_summary.get("summary") or "").strip()
    if cost_summary and not cautions:
        supports.append(cost_summary)
        posture = "supportive"

    return build_lens_judgment(
        lens_id="implementation_reality",
        lens_status=posture,
        confidence="high" if supports and not cautions else "medium",
        promotion_cap=promotion_cap,
        confidence_modifier=confidence_modifier,
        action_tone_constraint=action_tone,
        supports=supports,
        cautions=cautions,
        blocker_flags=cautions if posture == "blocking" else [],
        claim_constraints=["Do not let structural appeal outrun tax, liquidity, wrapper, or friction reality."],
        what_changes_view=["Cleaner tax clarity, direct liquidity support, and a quantified switch path would strengthen this lens materially."],
        investor_summary=supports[0] if supports and not cautions else cautions[0] if cautions else "Implementation looks acceptable, but it should remain subordinate to the full policy stack.",
    )
