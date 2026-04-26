from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from app.services.daily_brief_fact_pack import build_signal_fact_pack


def _text(value: Any) -> str:
    return str(value or "").strip()


def _float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _lower_first(value: str) -> str:
    text = _text(value)
    if not text:
        return ""
    return text[:1].lower() + text[1:]


def _with_period(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    if text.endswith((".", "!", "?")):
        return text
    return f"{text}."


def _join_first(*values: Any) -> str:
    for value in values:
        if isinstance(value, list):
            for item in value:
                text = _text(item)
                if text:
                    return text
            continue
        text = _text(value)
        if text:
            return text
    return ""


def _follow_through(claim: Any, implication: Any, *, connector: str = "which means") -> str:
    base = _text(claim)
    practical = _text(implication)
    if not base:
        return _with_period(practical)
    lowered = base.lower()
    if "which means" in lowered or "in practice" in lowered or "that matters because" in lowered:
        return _with_period(base)
    if not practical:
        return _with_period(base)
    return _with_period(f"{base.rstrip('.')} , {connector} {_lower_first(practical)}".replace(" ,", ","))


def _trust_level_text(confidence_label: str, freshness_status: str, watch_condition: str) -> str:
    freshness = freshness_status.lower()
    confidence = confidence_label.lower()
    if freshness == "refresh_failed_used_cache":
        return "Trust this cautiously because the latest refresh failed and the brief is leaning on cached data."
    if freshness in {"latest_available_source_lag", "refresh_skipped_policy"}:
        return "Trust this cautiously because the source is lagged, which means the signal may still matter but same-day conviction should stay lower."
    if freshness in {"stale_demoted", "stale_excluded"}:
        return "Trust this only as background context because the reading is no longer fresh enough for a strong lead conclusion."
    if confidence in {"low", "insufficient"}:
        return "Trust this cautiously because the evidence set is still thin enough that the interpretation should stay provisional."
    if watch_condition:
        return "Trust this as a current monitoring signal, not as a stand-alone portfolio conclusion."
    return "Trust this as a current market signal, while keeping the conclusion proportional to the available evidence."


def _do_not_overread_text(freshness_status: str, action_tag: str, portfolio_relevance: str) -> str:
    freshness = freshness_status.lower()
    action = action_tag.lower()
    if freshness == "refresh_failed_used_cache":
        return "Do not read this as a confirmed fresh turn in the market until the next successful refresh."
    if freshness in {"latest_available_source_lag", "refresh_skipped_policy"}:
        return "Do not read this as a same-day trade signal because the source itself is lagged."
    if "holdings unavailable" in portfolio_relevance.lower() or "proxy" in portfolio_relevance.lower():
        return "Do not read this as a confirmed holdings-level conclusion yet."
    if action in {"review", "monitor"}:
        return "Do not read this as a trade instruction or a thesis break yet."
    return "Do not overread this beyond the current evidence set."


def _what_to_do_now_text(action_tag: str, watch_condition: str) -> str:
    action = action_tag.lower()
    if watch_condition:
        if action in {"review", "urgent_review", "escalate"}:
            return _with_period(f"Review this next: {_lower_first(watch_condition)}")
        if action in {"monitor", "scenario_watch"}:
            return _with_period(f"Monitor this next: {_lower_first(watch_condition)}")
    if action in {"urgent_review", "escalate"}:
        return "Review this promptly rather than leaving it in background monitoring."
    if action == "review":
        return "Keep the affected sleeves under review rather than treating this as a trade decision."
    if action in {"monitor", "scenario_watch"}:
        return "Monitor the next refresh before reading this as a stronger portfolio signal."
    return "Keep this in the background unless fresher evidence makes it more important."


def _canonical_monitoring_action_state(
    *,
    action_tag: str,
    signal_trust_status: dict[str, Any],
    interpretive_strength_status: dict[str, Any],
    portfolio_mapping_directness_status: dict[str, Any],
) -> str:
    # This is the only official owner of Daily Brief action-state selection.
    # Lens context and frontend renderers may explain this result, but they may not widen it.
    trust = _text(signal_trust_status.get("overall_trust_level"))
    interpretation = _text(interpretive_strength_status.get("interpretation_strength_grade"))
    mapping_strength = _text(portfolio_mapping_directness_status.get("mapping_strength"))
    action = _text(action_tag).lower()
    if action in {"urgent_review", "escalate"}:
        return "review"
    if action == "review":
        return "review" if interpretation in {"STRONG", "USEFUL_BUT_LIMITED"} else "monitor"
    if action in {"monitor", "scenario_watch"}:
        return "monitor"
    if interpretation in {"BACKGROUND_ONLY", "DO_NOT_LEAD"} or trust in {"LOW", "VERY_LOW"}:
        return "ignore"
    return "monitor"


def _scenario_or_default(*values: Any, fallback: str) -> str:
    for value in values:
        text = _text(value)
        if text:
            return _with_period(text)
    return _with_period(fallback)


def classify_daily_brief_modules() -> list[dict[str, str]]:
    return [
        {
            "module_path": "backend/app/services/signals.py",
            "current_responsibility": "Market ingestion and signal extraction",
            "target_layer": "Layer 1 / Layer 2",
            "classification": "KEEP_AND_REFACTOR",
            "reason": "Already owns signal collection but needs to feed change-first signal packets rather than prose fragments.",
            "migration_note": "Preserve numeric extraction; route output through canonical SignalPacket builders.",
        },
        {
            "module_path": "backend/app/services/daily_brief_fact_pack.py",
            "current_responsibility": "Structured fact-pack generation from reader cards",
            "target_layer": "Layer 2 / Layer 7 bridge",
            "classification": "KEEP_AND_REFACTOR",
            "reason": "Already provides the correct deterministic ownership boundary for synthesis.",
            "migration_note": "Promote as canonical fact source for explanation cards and top-path synthesis.",
        },
        {
            "module_path": "backend/app/services/brief_consequence_rules.py",
            "current_responsibility": "Signal-to-sleeve consequence semantics",
            "target_layer": "Layer 3 / Layer 4",
            "classification": "KEEP_AND_REFACTOR",
            "reason": "Useful economic mechanism registry, but should feed structured explanation rather than stitched prose.",
            "migration_note": "Preserve family semantics and portfolio transmission rules.",
        },
        {
            "module_path": "backend/app/services/brief_holdings_mapper.py",
            "current_responsibility": "Signal-to-holdings and sleeve impact mapping",
            "target_layer": "Layer 4",
            "classification": "KEEP_AND_REFACTOR",
            "reason": "Already performs portfolio relevance work consistent with the target architecture.",
            "migration_note": "Continue to drive direct / indirect / proxy relevance flags.",
        },
        {
            "module_path": "backend/app/services/daily_brief_explanation_writer.py",
            "current_responsibility": "Investor-facing synthesis writing",
            "target_layer": "Layer 7",
            "classification": "KEEP_AND_REFACTOR",
            "reason": "Correct location for LLM synthesis once limited to approved fact packs.",
            "migration_note": "Keep as synthesis writer only; not fact generator.",
        },
        {
            "module_path": "backend/app/services/narrative_engine.py",
            "current_responsibility": "Legacy narrative shaping",
            "target_layer": "Layer 7",
            "classification": "TEMPORARY_ADAPTER",
            "reason": "Still bridges old and new explanation paths but should not remain the primary investor-facing architecture.",
            "migration_note": "Use only where needed for compatibility during migration.",
        },
        {
            "module_path": "backend/app/services/brief_fund_rationale.py",
            "current_responsibility": "Blueprint-adjacent policy rationale",
            "target_layer": "Out of Daily Brief core",
            "classification": "RETIRE",
            "reason": "Daily Brief should not rank or rationalize ETF candidates directly.",
            "migration_note": "Keep available for non-brief contexts only.",
        },
        {
            "module_path": "backend/app/services/brief_tax_location.py",
            "current_responsibility": "Tax and implementation commentary",
            "target_layer": "Optional appendix",
            "classification": "TEMPORARY_ADAPTER",
            "reason": "May remain as appendix context, but should not shape the main morning-note path.",
            "migration_note": "Demote to supporting appendix material only.",
        },
        {
            "module_path": "backend/app/services/brief_dca.py",
            "current_responsibility": "DCA guidance output",
            "target_layer": "Out of Daily Brief core",
            "classification": "RETIRE",
            "reason": "Daily Brief should not behave like a portfolio construction or pacing engine.",
            "migration_note": "Do not surface in main brief output.",
        },
        {
            "module_path": "backend/app/services/real_email_brief.py",
            "current_responsibility": "Run assembly and persisted brief generation",
            "target_layer": "Layer 7 / runtime orchestration",
            "classification": "KEEP_AND_REFACTOR",
            "reason": "Owns runtime generation and persistence, but should assemble around change-first structured objects.",
            "migration_note": "Persist canonical structured outputs and audit trace.",
        },
    ]


def build_signal_packet(card: dict[str, Any]) -> dict[str, Any]:
    fact_pack = build_signal_fact_pack(card)
    raw = dict(fact_pack.get("raw_metrics") or {})
    current_value = raw.get("metric_value")
    short_window_change = raw.get("metric_delta")
    previous_value = None
    current_numeric = _float(current_value)
    if current_numeric is not None and short_window_change is not None:
        previous_value = round(current_numeric - float(short_window_change), 4)
    signal_packet = {
        "signal_id": fact_pack.get("signal_id"),
        "as_of_date": fact_pack.get("observation_date"),
        "signal_name": fact_pack.get("signal_title"),
        "signal_family": fact_pack.get("signal_family"),
        "signal_type": fact_pack.get("signal_type"),
        "current_value": current_value,
        "previous_value": previous_value,
        "one_day_change": card.get("one_day_change"),
        "short_window_change": short_window_change,
        "medium_window_change": card.get("medium_window_change"),
        "z_score_or_percentile": raw.get("percentile_value"),
        "regime_state": card.get("persistence_state") or card.get("regime_state") or "stable",
        "urgency_state": card.get("severity_label") or "background",
        "confirming_indicators": list(card.get("confirming_indicators") or []),
        "conflicting_indicators": list(card.get("conflicting_indicators") or []),
        "source_refs": list(fact_pack.get("source_refs") or []),
        "freshness_status": fact_pack.get("freshness_state"),
        "quality_flags": list(fact_pack.get("limitations") or []),
        "signal_role": fact_pack.get("signal_role"),
        "evidence_classification": fact_pack.get("evidence_classification"),
        "grounding_type": fact_pack.get("grounding_type"),
    }
    return signal_packet


def build_signal_trust_status(card: dict[str, Any]) -> dict[str, Any]:
    confidence = dict(card.get("confidence") or {})
    freshness_state = _text(card.get("freshness_reason_code") or card.get("source_freshness_state") or "latest_available_source_lag")
    source_support_quality = (
        "high"
        if _text(confidence.get("evidence_confidence")) == "high"
        else "moderate"
        if _text(confidence.get("evidence_confidence")) == "medium"
        else "low"
    )
    holdings_grounding_quality = (
        "direct"
        if _text(card.get("mapping_mode")) == "live_holding_direct"
        else "sleeve_proxy"
        if _text(card.get("mapping_mode")) == "live_sleeve_exposure"
        else "benchmark_proxy"
        if _text(card.get("mapping_mode")) in {"benchmark_watch_proxy", "target_sleeve_proxy"}
        else "macro_only"
    )
    benchmark_support_quality = (
        "high"
        if _text(confidence.get("benchmark_confidence")) == "high"
        else "moderate"
        if _text(confidence.get("benchmark_confidence")) == "medium"
        else "low"
    )
    persistence_strength = (
        "high"
        if _text(card.get("persistence_state")) in {"persisting", "broadening"}
        else "moderate"
        if _text(card.get("persistence_state")) in {"stabilizing", "fading"}
        else "low"
    )
    freshness_strength = (
        "high"
        if freshness_state in {"refreshed_current", "fresh", "current"}
        else "moderate"
        if freshness_state in {"latest_available_source_lag", "refresh_skipped_policy", "aging"}
        else "low"
        if freshness_state not in {"refresh_failed_used_cache", "stale_demoted", "stale_excluded"}
        else "very_low"
    )
    if freshness_strength == "high" and source_support_quality == "high" and holdings_grounding_quality == "direct":
        overall = "HIGH"
    elif freshness_strength in {"high", "moderate"} and source_support_quality in {"high", "moderate"}:
        overall = "MODERATE"
    elif freshness_strength == "very_low":
        overall = "VERY_LOW"
    else:
        overall = "LOW"
    explanation = (
        "Direct fresh support and direct holdings mapping make this one of the more trustworthy current signals."
        if overall == "HIGH"
        else "The signal is useful, but trust should stay moderated by lag, proxy support, or indirect portfolio mapping."
        if overall == "MODERATE"
        else "The signal still helps with monitoring, but evidence, freshness, or mapping limits reduce how strongly it should be read."
        if overall == "LOW"
        else "Treat this mainly as background context because the read is relying on lagged, fallback, or weakly grounded evidence."
    )
    return {
        "freshness_strength": freshness_strength,
        "source_support_quality": source_support_quality,
        "holdings_grounding_quality": holdings_grounding_quality,
        "benchmark_support_quality": benchmark_support_quality,
        "persistence_strength": persistence_strength,
        "overall_trust_level": overall,
        "practical_trust_explanation": explanation,
    }


def build_interpretive_strength_status(card: dict[str, Any]) -> dict[str, Any]:
    trust = dict(card.get("signal_trust_status") or build_signal_trust_status(card))
    mapping = _text(card.get("mapping_mode"))
    freshness_state = _text(card.get("freshness_reason_code") or "")
    narrative_confidence = _text(dict(card.get("confidence") or {}).get("label") or card.get("narrative_confidence") or "low")
    directness = (
        "direct"
        if mapping == "live_holding_direct"
        else "sleeve_grounded"
        if mapping == "live_sleeve_exposure"
        else "proxy_grounded"
        if mapping in {"target_sleeve_proxy", "benchmark_watch_proxy"}
        else "macro_only"
    )
    proxy_dependence = "high" if "proxy" in mapping or mapping == "macro_only" else "low"
    if freshness_state in {"stale_excluded"}:
        grade = "DO_NOT_LEAD"
    elif freshness_state in {"stale_demoted"} or _text(card.get("signal_role")) == "background_signal":
        grade = "BACKGROUND_ONLY"
    elif freshness_state in {"refresh_failed_used_cache"}:
        grade = "STORED_CONTEXT_ONLY"
    elif trust.get("overall_trust_level") in {"HIGH", "MODERATE"} and narrative_confidence in {"high", "medium", "moderate"}:
        grade = "STRONG" if directness in {"direct", "sleeve_grounded"} else "USEFUL_BUT_LIMITED"
    else:
        grade = "USEFUL_BUT_LIMITED"
    action_boundary = (
        "Review next, but do not treat this as a trade signal."
        if _text(card.get("action_tag")) in {"review", "urgent_review", "escalate"}
        else "Monitor next, not act."
    )
    overread_warning = (
        "Do not overread this beyond sleeve-level monitoring because the mapping remains indirect."
        if proxy_dependence == "high"
        else "Do not overread this as a confirmed trend break without fresh follow-through."
    )
    return {
        "directness_of_support": directness,
        "proxy_dependence": proxy_dependence,
        "narrative_confidence": narrative_confidence,
        "action_boundary": action_boundary,
        "overread_warning": overread_warning,
        "interpretation_strength_grade": grade,
    }


def build_portfolio_mapping_directness_status(card: dict[str, Any]) -> dict[str, Any]:
    mapping_mode = _text(card.get("mapping_mode") or "macro_only")
    direct_holdings_grounded = mapping_mode == "live_holding_direct"
    sleeve_proxy_grounded = mapping_mode in {"live_sleeve_exposure", "target_sleeve_proxy"}
    benchmark_proxy_grounded = mapping_mode == "benchmark_watch_proxy"
    if direct_holdings_grounded:
        mapping_strength = "direct"
        warning = "Portfolio wording can be more direct because named holdings are actually mapped."
    elif sleeve_proxy_grounded:
        mapping_strength = "sleeve_proxy"
        warning = "Portfolio wording should stay at sleeve level because direct holding-level mapping is incomplete."
    elif benchmark_proxy_grounded:
        mapping_strength = "benchmark_proxy"
        warning = "Portfolio wording should stay benchmark- or sleeve-level because the mapping is proxy-based."
    else:
        mapping_strength = "macro_only"
        warning = "Portfolio wording should stay cautious because no direct portfolio mapping is confirmed."
    return {
        "direct_holdings_grounded": direct_holdings_grounded,
        "sleeve_proxy_grounded": sleeve_proxy_grounded,
        "benchmark_proxy_grounded": benchmark_proxy_grounded,
        "mapping_strength": mapping_strength,
        "practical_mapping_warning": warning,
    }


def build_refresh_strength_status(card: dict[str, Any]) -> dict[str, Any]:
    freshness_reason = _text(card.get("freshness_reason_code") or card.get("source_freshness_state") or "latest_available_source_lag")
    lag_days = _float(dict(card.get("source_freshness") or {}).get("lag_days"))
    fallback_used = freshness_reason in {"refresh_failed_used_cache", "refresh_skipped_policy"}
    if freshness_reason in {"refreshed_current", "fresh", "current"}:
        refresh_mode = "full_fresh_rebuild"
        effect = "Read as a current market interpretation."
    elif freshness_reason == "latest_available_source_lag":
        refresh_mode = "lagged_refresh"
        effect = "Read as useful but weaker than a same-day fresh interpretation."
    elif freshness_reason in {"refresh_failed_used_cache", "refresh_skipped_policy"}:
        refresh_mode = "fallback_or_cached"
        effect = "Read as stored-context monitoring, not a fresh market call."
    else:
        refresh_mode = "verified_stored_context"
        effect = "Read as background or stored-context interpretation only."
    return {
        "refresh_mode": refresh_mode,
        "refresh_recency": freshness_reason,
        "lag_present": lag_days is not None and lag_days > 0,
        "fallback_used": fallback_used,
        "freshness_policy_result": freshness_reason,
        "practical_effect_on_read": effect,
    }


def _build_daily_brief_lens_context(
    *,
    signal_trust_status: dict[str, Any],
    interpretive_strength_status: dict[str, Any],
    portfolio_mapping_directness_status: dict[str, Any],
    refresh_strength_status: dict[str, Any],
    action_state: str,
    why_it_matters_here: str,
) -> dict[str, Any]:
    trust = _text(signal_trust_status.get("overall_trust_level"))
    mapping_strength = _text(portfolio_mapping_directness_status.get("mapping_strength"))
    refresh_mode = _text(refresh_strength_status.get("refresh_mode"))
    interpretation_grade = _text(interpretive_strength_status.get("interpretation_strength_grade"))

    marks_summary = (
        "Cycle and risk posture stays restrained because trust, freshness, or directness is not strong enough for a louder read."
        if trust in {"LOW", "VERY_LOW"} or refresh_mode in {"fallback_or_cached", "verified_stored_context"}
        else "Cycle and risk posture allows monitoring, but still does not justify action beyond review."
    )
    dalio_summary = (
        "Regime and transmission context is useful mainly for sleeve framing because direct portfolio mapping is limited."
        if mapping_strength in {"benchmark_proxy", "macro_only", "sleeve_proxy"}
        else "Regime and transmission context helps explain why the mapped sleeves deserve attention."
    )
    fragility_summary = (
        "Fragility remains high because this interpretation is still limited by proxy support or indirect mapping."
        if interpretation_grade in {"BACKGROUND_ONLY", "DO_NOT_LEAD", "STORED_CONTEXT_ONLY"} or mapping_strength in {"benchmark_proxy", "macro_only"}
        else "Fragility stays bounded as long as this remains a monitoring or review signal rather than an action call."
    )
    implementation_summary = (
        "Implementation reality stays secondary here because the Daily Brief is not a trading instruction surface."
        if action_state in {"ignore", "monitor"}
        else "Implementation reality matters only as a review context, not as direct execution guidance."
    )

    review_intensity = "raise_to_universal" if action_state == "review" else "none"
    overall_posture = "monitoring_only" if action_state in {"ignore", "monitor"} else "review_only"

    return {
        "overall_posture": overall_posture,
        "review_intensity_modifier": review_intensity,
        "marks_cycle_risk": {"summary": marks_summary},
        "dalio_regime_transmission": {"summary": dalio_summary},
        "fragility_red_team": {"summary": fragility_summary},
        "implementation_reality": {"summary": implementation_summary},
        "portfolio_frame": _with_period(why_it_matters_here),
    }


def build_interpretation_card(card: dict[str, Any]) -> dict[str, Any]:
    fact_pack = build_signal_fact_pack(card)
    investor = dict(card.get("investor_explanation") or {})
    implication = dict(card.get("portfolio_implication") or {})
    signal_trust_status = dict(card.get("signal_trust_status") or build_signal_trust_status(card))
    interpretive_strength_status = dict(card.get("interpretive_strength_status") or build_interpretive_strength_status({**card, "signal_trust_status": signal_trust_status}))
    portfolio_mapping_directness_status = dict(card.get("portfolio_mapping_directness_status") or build_portfolio_mapping_directness_status(card))
    refresh_strength_status = dict(card.get("refresh_strength_status") or build_refresh_strength_status(card))
    what_happened = _join_first(card.get("observation"), investor.get("signal"), fact_pack.get("signal_summary_facts"))
    economic_mechanism = _join_first(card.get("mechanism"), card.get("transmission_path"), investor.get("boundary"))
    portfolio_relevance = _join_first(
        implication.get("current_holdings_relevance"),
        implication.get("policy_relevance"),
        investor.get("investment_implication"),
    )
    watch_condition = _join_first(card.get("financial_review_question"), investor.get("review_action"))
    action_relevance = _text(card.get("action_tag") or "monitor")
    confidence_label = _text(dict(card.get("confidence") or {}).get("label") or "low")
    freshness_status = _text(fact_pack.get("freshness_state") or "latest_available_source_lag")
    canonical_action_state = _canonical_monitoring_action_state(
        action_tag=action_relevance,
        signal_trust_status=signal_trust_status,
        interpretive_strength_status=interpretive_strength_status,
        portfolio_mapping_directness_status=portfolio_mapping_directness_status,
    )
    what_that_usually_means = _follow_through(
        _join_first(card.get("why_it_matters"), investor.get("meaning")),
        economic_mechanism or portfolio_relevance,
        connector="which usually means",
    )
    practical_portfolio_tail = _join_first(
        implication.get("policy_relevance"),
        implication.get("current_holdings_relevance") if "?" not in _text(implication.get("current_holdings_relevance")) else "",
        investor.get("review_action") if "?" not in _text(investor.get("review_action")) else "",
    )
    why_it_matters_here = _follow_through(
        portfolio_relevance or investor.get("investment_implication"),
        practical_portfolio_tail or "the affected sleeves deserve closer review than background-only issues",
        connector="that matters because",
    )
    lens_context = _build_daily_brief_lens_context(
        signal_trust_status=signal_trust_status,
        interpretive_strength_status=interpretive_strength_status,
        portfolio_mapping_directness_status=portfolio_mapping_directness_status,
        refresh_strength_status=refresh_strength_status,
        action_state=canonical_action_state,
        why_it_matters_here=why_it_matters_here,
    )
    return {
        "signal_id": fact_pack.get("signal_id"),
        "what_changed": _text(what_happened),
        "what_happened": _with_period(what_happened),
        "why_it_matters": _text(card.get("why_it_matters") or investor.get("meaning")),
        "what_that_usually_means": what_that_usually_means,
        "economic_mechanism": _text(economic_mechanism),
        "portfolio_relevance": _text(portfolio_relevance),
        "why_it_matters_here": why_it_matters_here,
        "watch_condition": _text(watch_condition),
        "action_state": canonical_action_state,
        "trust_level": _with_period(_join_first(signal_trust_status.get("practical_trust_explanation"), _trust_level_text(confidence_label, freshness_status, watch_condition))),
        "do_not_overread": _with_period(_join_first(interpretive_strength_status.get("overread_warning"), _do_not_overread_text(freshness_status, action_relevance, portfolio_relevance))),
        "what_to_do_next": _with_period(_join_first(interpretive_strength_status.get("action_boundary"), _what_to_do_now_text(action_relevance, watch_condition))),
        "scenario_pathways": {
            "if_persists": _text(card.get("if_persists")),
            "if_worsens": _text(card.get("if_worsens") or investor.get("scenario_if_worsens")),
            "if_stabilizes": _text(card.get("if_normalizes") or investor.get("scenario_if_stabilizes")),
            "if_reverses": _text(card.get("if_reverses") or investor.get("scenario_if_reverses")),
        },
        "what_would_confirm": _scenario_or_default(
            card.get("if_persists"),
            card.get("if_worsens"),
            investor.get("scenario_if_worsens"),
            investor.get("strengthen_read"),
            fallback="A fresher confirming move in the same direction would strengthen this read",
        ),
        "what_would_break": _scenario_or_default(
            card.get("if_reverses"),
            card.get("if_normalizes"),
            investor.get("scenario_if_reverses"),
            investor.get("weaken_read"),
            fallback="A reversal or cleaner contradictory signal would weaken this read",
        ),
        "action_relevance": action_relevance,
        "affected_sleeves": list(implication.get("affected_sleeves") or []),
        "affected_markets": list(fact_pack.get("affected_markets") or []),
        "confidence_label": confidence_label,
        "freshness_status": freshness_status,
        "signal_trust_status": signal_trust_status,
        "interpretive_strength_status": interpretive_strength_status,
        "portfolio_mapping_directness_status": portfolio_mapping_directness_status,
        "refresh_strength_status": refresh_strength_status,
        "lens_context": lens_context,
        "review_intensity_context": {
            "review_intensity_modifier": lens_context.get("review_intensity_modifier"),
            "summary": "Review intensity stays bounded by the monitoring-first contract.",
        },
    }


def build_explanation_card(card: dict[str, Any]) -> dict[str, Any]:
    # Compatibility alias while the final reader migrates to InterpretationCard naming.
    return build_interpretation_card(card)


def build_portfolio_impact(card: dict[str, Any]) -> dict[str, Any]:
    implication = dict(card.get("portfolio_implication") or {})
    sleeves = [str(item) for item in list(implication.get("affected_sleeves") or []) if str(item)]
    direction = "unclear"
    lowered = " ".join(
        [
            _text(card.get("consequence_type")),
            _text(card.get("why_it_matters")),
            _text(card.get("consequence_summary")),
        ]
    ).lower()
    if any(token in lowered for token in ("pressure", "widen", "stress", "weaker", "deteriorat")):
        direction = "headwind"
    elif any(token in lowered for token in ("support", "relevance increases", "improves", "firm")):
        direction = "tailwind"
    return {
        "signal_id": _text(card.get("signal_id") or card.get("metric_code") or card.get("title")),
        "affected_sleeves": sleeves,
        "impact_direction_by_sleeve": {sleeve: direction for sleeve in sleeves},
        "impact_confidence_by_sleeve": {
            sleeve: _text(dict(card.get("confidence") or {}).get("label") or "low") for sleeve in sleeves
        },
        "affected_assumptions": list(card.get("assumption_tags") or []),
        "direct_or_indirect_flag": _text(card.get("mapping_mode") or "macro_only"),
        "relevance_summary": _text(implication.get("current_holdings_relevance") or implication.get("policy_relevance")),
    }


def build_thesis_drift(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    assumption_votes: dict[str, dict[str, Any]] = defaultdict(lambda: {"supporting": [], "contradicting": [], "reviews": []})
    mapping = {
        "rates and inflation": "duration_ballast_still_functioning",
        "credit and liquidity": "credit_conditions_benign",
        "volatility and stress": "convex_sleeve_still_relevant",
        "equity breadth and risk appetite": "equity_concentration_manageable",
        "cross asset summary": "real_asset_sleeve_still_diversifying",
        "fx and singapore context": "sgd_allocator_hurdle_stable",
        "em and china context": "em_and_china_pacing_stable",
    }
    for card in cards:
        family = _text(card.get("category") or card.get("signal_family")).lower()
        assumption = mapping.get(family)
        if not assumption:
            continue
        severity = _text(card.get("severity_label")).lower()
        observation = _text(card.get("observation"))
        if severity in {"critical", "high"} or _text(card.get("action_tag")) in {"review", "urgent_review", "escalate"}:
            assumption_votes[assumption]["contradicting"].append(observation)
            assumption_votes[assumption]["reviews"].append(_text(card.get("financial_review_question")))
        else:
            assumption_votes[assumption]["supporting"].append(observation)
    drift_items: list[dict[str, Any]] = []
    for assumption, vote in assumption_votes.items():
        supporting = [item for item in vote["supporting"] if item]
        contradicting = [item for item in vote["contradicting"] if item]
        if contradicting and supporting:
            drift_state = "weakening"
        elif contradicting:
            drift_state = "broken" if len(contradicting) >= 2 else "weakening"
        elif supporting:
            drift_state = "strengthening"
        else:
            drift_state = "stable"
        drift_items.append(
            {
                "assumption_name": assumption,
                "supporting_signals": supporting[:3],
                "contradicting_signals": contradicting[:3],
                "current_drift_state": drift_state,
                "confidence": "high" if len(contradicting) + len(supporting) >= 2 else "medium",
                "review_implication": next((item for item in vote["reviews"] if item), "Monitor whether current evidence changes the assumption materially."),
            }
        )
    return drift_items


def classify_action(card: dict[str, Any]) -> str:
    explicit = _text(card.get("action_state")).lower()
    if explicit in {"ignore", "monitor", "review"}:
        return explicit.upper()
    tag = _text(card.get("action_tag")).lower()
    if tag in {"urgent_review", "escalate"}:
        return "REVIEW"
    if tag in {"review"}:
        return "REVIEW"
    if tag in {"monitor", "scenario_watch"}:
        return "MONITOR"
    return "IGNORE"


def build_brief_action_state(card: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal_id": _text(card.get("signal_id") or card.get("metric_code") or card.get("title")),
        "action_state": classify_action(card),
        "action_reason": _text(card.get("action_tag") or card.get("financial_review_question") or card.get("follow_up") or "monitor"),
        "review_question": _text(card.get("financial_review_question") or card.get("follow_up")),
        "urgency": _text(card.get("severity_label") or "background"),
    }


def build_blueprint_triggers(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    triggers: list[dict[str, Any]] = []
    for card in cards:
        action = classify_action(card)
        if action not in {"REVIEW", "ACTION"}:
            continue
        implication = dict(card.get("portfolio_implication") or {})
        sleeves = [str(item) for item in list(implication.get("affected_sleeves") or []) if str(item)]
        if not sleeves:
            continue
        for sleeve in sleeves[:2]:
            triggers.append(
                {
                    "trigger_id": f"{_text(card.get('signal_id') or card.get('metric_code') or card.get('title'))}:{sleeve}",
                    "as_of_date": _text(dict(card.get("evidence") or {}).get("observed_at")),
                    "sleeve": sleeve,
                    "trigger_reason": _text(card.get("why_it_matters") or card.get("consequence_summary")),
                    "urgency": action.lower(),
                    "source_signals": [_text(card.get("title") or card.get("metric_code"))],
                    "review_question": _text(card.get("financial_review_question") or "Does this change require a Blueprint review?"),
                }
            )
    return triggers


def build_overlay_notes(cards: list[dict[str, Any]]) -> list[dict[str, str]]:
    notes: list[dict[str, str]] = []
    if any(_text(card.get("category")).lower() == "volatility and stress" for card in cards):
        notes.append(
            {
                "overlay_name": "Cycle and psychology lens",
                "commentary": "Stress-sensitive signals suggest caution should remain higher than the headline market narrative alone might imply.",
            }
        )
    if any(_text(card.get("category")).lower() == "rates and inflation" for card in cards):
        notes.append(
            {
                "overlay_name": "Regime and macro transmission lens",
                "commentary": "Rates and inflation changes are still the cleanest transmission layer for discount-rate and reserve-hurdle interpretation.",
            }
        )
    if any(_text(card.get("category")).lower() == "equity breadth and risk appetite" for card in cards):
        notes.append(
            {
                "overlay_name": "Discipline and noise filter lens",
                "commentary": "Breadth and participation matter more than isolated headline index moves when separating durable change from market noise.",
            }
        )
    return notes[:3]


def build_overlay_insight_set(cards: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "primary_overlay": notes[0]["overlay_name"] if (notes := build_overlay_notes(cards)) else "none",
        "insights": notes,
    }


def build_monitoring_and_evidence(cards: list[dict[str, Any]], evidence_appendix: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    monitors = [_text(card.get("financial_review_question")) for card in cards if classify_action(card) == "MONITOR" and _text(card.get("financial_review_question"))]
    stabilizing = [_text(card.get("if_normalizes") or card.get("if_persists")) for card in cards if _text(card.get("persistence_state")) == "stabilizing"]
    reversing = [_text(card.get("if_reverses")) for card in cards if _text(card.get("if_reverses"))]
    source_summary = Counter(_text(dict(card.get("evidence") or {}).get("source") or "unknown") for card in cards)
    return {
        "monitor_list": monitors[:6],
        "stabilizing_signals": [item for item in stabilizing if item][:5],
        "reversing_signals": [item for item in reversing if item][:5],
        "source_summary": [{"source": source, "count": count} for source, count in source_summary.items()],
        "evidence_items": list(evidence_appendix or [])[:8],
    }


def build_supporting_evidence_summary(cards: list[dict[str, Any]], evidence_appendix: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return build_monitoring_and_evidence(cards, evidence_appendix)


def build_daily_brief_audit_trace(
    signal_packets: list[dict[str, Any]],
    explanation_cards: list[dict[str, Any]],
    portfolio_impacts: list[dict[str, Any]],
    thesis_drift: list[dict[str, Any]],
    triggers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {"stage": "ingestion_completion", "status": "completed", "detail": f"{len(signal_packets)} signal packets normalized."},
        {"stage": "signal_packet_generation", "status": "completed", "detail": f"{len(signal_packets)} canonical change packets built."},
        {"stage": "signal_prioritization", "status": "completed", "detail": f"{sum(1 for item in signal_packets if _text(item.get('signal_role')) == 'dominant_signal')} dominant signals elevated."},
        {"stage": "explanation_generation", "status": "completed", "detail": f"{len(explanation_cards)} explanation cards assembled under change -> meaning -> relevance -> watch sequencing."},
        {"stage": "portfolio_mapping", "status": "completed", "detail": f"{sum(1 for item in portfolio_impacts if item.get('affected_sleeves'))} signals mapped to sleeves."},
        {"stage": "thesis_drift_decision", "status": "completed", "detail": f"{len(thesis_drift)} portfolio assumptions evaluated."},
        {"stage": "action_classification", "status": "completed", "detail": f"{sum(1 for card in explanation_cards if _text(card.get('action_relevance')))} signal actions classified."},
        {"stage": "blueprint_trigger_decision", "status": "completed", "detail": f"{len(triggers)} Blueprint review triggers generated."},
        {"stage": "final_brief_assembly", "status": "completed", "detail": "Daily Brief payload assembled from structured change-detection layers."},
    ]
