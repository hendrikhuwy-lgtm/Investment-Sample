from __future__ import annotations

from typing import Any

from app.services.framework_lenses.contracts import build_lens_judgment


def evaluate_dalio_regime_transmission(
    *,
    sleeve_key: str,
    portfolio_consequence_summary: dict[str, Any],
    benchmark_support_status: dict[str, Any],
    current_holding_record: dict[str, Any],
) -> dict[str, Any]:
    supports: list[str] = []
    cautions: list[str] = []
    review_modifier = "none"
    action_tone = "none"
    posture = "neutral"

    portfolio_summary = str(portfolio_consequence_summary.get("summary") or "").strip()
    portfolio_effect = str(dict(portfolio_consequence_summary.get("portfolio_effect") or {}).get("summary") or "").strip()
    if portfolio_effect or portfolio_summary:
        supports.append(portfolio_effect or portfolio_summary)
        posture = "supportive"

    if str(benchmark_support_status.get("support_class") or "") in {"weak_proxy", "unavailable"}:
        cautions.append("Regime transmission is still useful for sleeve framing, but weak benchmark authority limits how far the read should travel.")
        posture = "cautious" if not supports else "neutral"
        action_tone = "restrained"

    if sleeve_key in {"ig_bonds", "cash_bills", "real_assets", "alternatives", "convex"}:
        review_modifier = "raise_to_universal"

    if bool(current_holding_record.get("current_symbol")):
        supports.append("Regime framing should be read through portfolio role and diversification, not as a stand-alone trading call.")

    return build_lens_judgment(
        lens_id="dalio_regime_transmission",
        lens_status=posture,
        confidence="medium",
        review_intensity_modifier=review_modifier,
        action_tone_constraint=action_tone,
        supports=supports,
        cautions=cautions,
        claim_constraints=["Do not let regime context override the no-forecast and no-tactical-action doctrine."],
        what_changes_view=["A cleaner causal link between market regime and sleeve consequence would strengthen this lens."],
        investor_summary=supports[0] if supports and not cautions else cautions[0] if cautions else "Regime context helps explain sleeve role, but it should not dictate action by itself.",
    )
