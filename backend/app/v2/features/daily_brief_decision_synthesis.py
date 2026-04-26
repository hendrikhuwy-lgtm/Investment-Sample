from __future__ import annotations

from typing import Any

from app.v2.features.chart_payload_builders import build_daily_brief_chart_payload
from app.v2.features.daily_brief_contingent_driver_builder import build_contingent_drivers
from app.v2.features.daily_brief_effect_classifier import classify_effect
from app.v2.features.daily_brief_explanation_builder import build_decision_explanation, build_source_context
from app.v2.features.daily_brief_relevance_gate import assess_driver_relevance, build_signal_stack_groups
from app.v2.features.forecast_feature_service import build_signal_support_bundle


_MAGNITUDE_WEIGHTS = {
    "significant": 16,
    "moderate": 11,
    "minor": 6,
    "unknown": 0,
}

_SUFFICIENCY_WEIGHTS = {
    "direct": 16,
    "sufficient": 12,
    "bounded": 8,
    "thin": 2,
    "insufficient": 0,
}

_FORECAST_INFLUENCE_BY_BUCKET = {
    "duration": 1.0,
    "credit": 1.0,
    "dollar_fx": 0.95,
    "growth": 0.88,
    "market": 0.82,
    "inflation": 0.7,
    "energy": 0.65,
    "real_assets": 0.65,
    "policy": 0.5,
    "volatility": 0.75,
    "liquidity": 0.8,
}


def synthesize_daily_brief_decisions(
    signal_cards: list[dict[str, Any]],
    *,
    review_posture: str,
    why_here: str,
    holdings_overlay_present: bool,
    primary_limit: int = 5,
    contingent_limit: int = 5,
) -> dict[str, Any]:
    drivers: list[dict[str, Any]] = []
    for signal in signal_cards:
        runtime = dict(signal.get("runtime_provenance") or {})
        effect = classify_effect(signal)
        signal["affected_sleeves"] = effect["mapped_sleeves"]
        source_context = build_source_context(
            signal,
            effect,
            holdings_overlay_present=holdings_overlay_present,
        )
        preliminary_explanation = build_decision_explanation(
            signal,
            effect,
            holdings_overlay_present=holdings_overlay_present,
            source_context=source_context,
        )
        sufficiency_state = _sufficiency_state(signal, runtime, effect)
        decision_action = _next_action(
            signal=signal,
            review_posture=review_posture,
            effect=effect,
            sufficiency_state=sufficiency_state,
            source_context=source_context,
        )
        portfolio_consequence = preliminary_explanation["portfolio_consequence"]
        support_signal = {
            **signal,
            "source_context": source_context,
            "why_it_matters_macro": preliminary_explanation["why_it_matters_macro"],
            "why_it_matters_micro": preliminary_explanation["why_it_matters_micro"],
            "why_it_matters_short_term": preliminary_explanation["why_it_matters_short_term"],
            "why_it_matters_long_term": preliminary_explanation["why_it_matters_long_term"],
            "effect_type": effect["effect_type"],
            "primary_effect_bucket": effect["primary_effect_bucket"],
            "source_kind": effect["source_kind"],
            "affected_sleeves": effect["mapped_sleeves"],
            "affected_candidates": effect["affected_candidates"],
            "sufficiency_state": sufficiency_state,
        }
        support = build_signal_support_bundle(
            support_signal,
            why_here=preliminary_explanation["why_it_matters_long_term"] or why_here,
            portfolio_consequence=portfolio_consequence,
            next_action=decision_action,
        )
        monitoring = dict(support["monitoring_condition"])
        near_term_trigger = str(monitoring.get("near_term_trigger") or signal.get("confirms") or "")
        thesis_trigger = str(monitoring.get("thesis_trigger") or signal.get("implication") or "")
        confidence_class = _confidence_class(signal, runtime, support)
        support_class = str((support["bundle"].support.support_strength or "bounded")).strip().lower() or "bounded"
        source_provenance_summary = _source_provenance_summary(signal, runtime, effect)
        path_risk = _path_risk(signal=signal, support=support, source_context=source_context)
        gating = _gating_metrics(
            signal=signal,
            effect=effect,
            source_context=source_context,
            support=support,
            sufficiency_state=sufficiency_state,
            confidence_class=confidence_class,
            next_action=decision_action,
        )
        relevance = assess_driver_relevance(
            {
                **signal,
                "source_context": source_context,
                "monitoring_condition": monitoring,
                "decision_change_potential": gating.get("decision_change_potential"),
                "actionability_class": gating.get("actionability_class"),
                "primary_effect_bucket": effect["primary_effect_bucket"],
                "affected_sleeves": effect["mapped_sleeves"],
                "affected_candidates": effect["affected_candidates"],
                "affected_holdings": list(signal.get("affected_holdings") or []),
                "next_action": decision_action,
                "sufficiency_state": sufficiency_state,
                "prominence_score": gating.get("prominence_score"),
            }
        )
        source_context = {
            **source_context,
            "novelty_class": relevance["novelty_class"],
            "reactivation_reason": relevance["reactivation_reason"],
            "threshold_state": relevance["threshold_state"],
            "freshness_age_days": relevance["freshness_age_days"],
            "freshness_relevance_score": relevance["freshness_relevance_score"],
            "lead_lane": relevance["lead_lane"],
            "current_action_delta": relevance["current_action_delta"],
            "portfolio_read_delta": relevance["portfolio_read_delta"],
        }
        explanation = build_decision_explanation(
            signal,
            effect,
            holdings_overlay_present=holdings_overlay_present,
            source_context=source_context,
            support=support,
        )
        portfolio_consequence = explanation["portfolio_consequence"]
        score = _decision_relevance_score(
            signal=signal,
            effect=effect,
            support=support,
            sufficiency_state=sufficiency_state,
            confidence_class=confidence_class,
            gating={**gating, **relevance},
        )
        decision_title = _decision_title(
            signal=signal,
            effect=effect,
            next_action=decision_action,
            source_context=source_context,
        )
        signal_label = _signal_label(
            signal=signal,
            effect=effect,
            source_context=source_context,
        )
        evidence_class = _evidence_class_code(
            signal=signal,
            source_context=source_context,
        )
        market_confirmation_state = _market_confirmation_state(source_context)
        freshness_state, freshness_label = _freshness_contract(source_context)
        evidence_title = _evidence_title(
            signal=signal,
            effect=effect,
            source_context=source_context,
            signal_label=signal_label,
            freshness_label=freshness_label,
        )
        interpretation_subtitle = _interpretation_subtitle(
            signal=signal,
            effect=effect,
            source_context=source_context,
            explanation=explanation,
        )
        card_family = _card_family(
            effect=effect,
            source_context=source_context,
        )
        decision_status = _decision_status(
            visibility_role=str(relevance.get("lead_lane") or ""),
            next_action=decision_action,
            source_context=source_context,
            support=support,
            gating={**gating, **relevance},
        )
        action_posture = _action_posture(
            next_action=decision_action,
            signal=signal,
            effect=effect,
            source_context=source_context,
        )
        short_title = _short_title(
            signal=signal,
            effect=effect,
            source_context=source_context,
        )
        short_subtitle = _short_subtitle(
            signal=signal,
            effect=effect,
            source_context=source_context,
            explanation=explanation,
        )
        driver = {
            **signal,
            "card_id": str(signal.get("signal_id") or ""),
            "card_family": card_family,
            "event_cluster_id": source_context.get("event_cluster_id"),
            "event_family": source_context.get("event_family"),
            "event_subtype": source_context.get("event_subtype"),
            "event_region": source_context.get("event_region"),
            "event_entities": list(source_context.get("event_entities") or []),
            "market_channels": list(source_context.get("market_channels") or []),
            "confirmation_assets": list(source_context.get("confirmation_assets") or []),
            "event_trigger_summary": source_context.get("event_trigger_summary"),
            "event_title": source_context.get("event_title"),
            "event_fingerprint": source_context.get("event_fingerprint"),
            "signal_label": signal_label,
            "evidence_title": evidence_title,
            "interpretation_subtitle": interpretation_subtitle,
            "sleeve_tags": [_humanize_primary_sleeve([sleeve]) or str(sleeve) for sleeve in list(effect.get("mapped_sleeves") or [])],
            "instrument_tags": list(effect.get("affected_candidates") or []),
            "evidence_class": evidence_class,
            "freshness_state": freshness_state,
            "freshness_label": freshness_label,
            "decision_status": decision_status,
            "action_posture": action_posture,
            "support_label": _support_label(
                support_class=support_class,
                confidence_class=confidence_class,
                sufficiency_state=sufficiency_state,
            ),
            "confidence_label": confidence_class,
            "market_confirmation_state": market_confirmation_state,
            "decision_title": decision_title,
            "short_title": short_title,
            "short_subtitle": short_subtitle,
            "source_kind": effect["source_kind"],
            "effect_type": effect["effect_type"],
            "primary_effect_bucket": effect["primary_effect_bucket"],
            "aspect_bucket": _aspect_bucket(
                bucket=str(effect["primary_effect_bucket"] or "market"),
                source_context=source_context,
            ),
            "why_it_matters_macro": explanation["why_it_matters_macro"],
            "why_it_matters_micro": explanation["why_it_matters_micro"],
            "why_it_matters_short_term": explanation["why_it_matters_short_term"],
            "why_it_matters_long_term": explanation["why_it_matters_long_term"],
            "what_changed_today": explanation["what_changed_today"],
            "why_this_could_be_wrong": explanation["why_this_could_be_wrong"],
            "why_now_not_before": explanation["why_now_not_before"],
            "implementation_sensitivity": explanation["implementation_sensitivity"],
            "what_changed": explanation["what_changed"],
            "event_context_delta": explanation.get("event_context_delta"),
            "why_it_matters": explanation["why_it_matters"],
            "why_it_matters_economically": explanation.get("why_it_matters_economically"),
            "portfolio_meaning": explanation["portfolio_meaning"],
            "portfolio_and_sleeve_meaning": explanation.get("portfolio_and_sleeve_meaning"),
            "confirm_condition": explanation["confirm_condition"],
            "weaken_condition": explanation["weaken_condition"],
            "break_condition": explanation["break_condition"],
            "scenario_support": explanation.get("scenario_support"),
            "do_not_overread": explanation["overread_reason"],
            "source_and_validity": explanation["source_and_validity"],
            "market_confirmation": explanation.get("market_confirmation"),
            "news_to_market_confirmation": explanation["news_to_market_confirmation"],
            "near_term_trigger": near_term_trigger,
            "thesis_trigger": thesis_trigger,
            "portfolio_consequence": portfolio_consequence,
            "next_action": decision_action,
            "affected_candidates": effect["affected_candidates"],
            "implementation_set": explanation.get("implementation_set"),
            "decision_relevance_score": round(score, 1),
            "confidence_class": confidence_class,
            "sufficiency_state": sufficiency_state,
            "signal_support_class": support_class,
            "path_risk_note": path_risk["note"],
            "source_provenance_summary": source_provenance_summary,
            "monitoring_condition": {
                **monitoring,
                "affected_sleeve": _affected_sleeve(effect["mapped_sleeves"]),
                "affected_candidates": effect["affected_candidates"],
                "effect_type": effect["effect_type"],
                "source_kind": effect["source_kind"],
                "confidence_class": confidence_class,
                "sufficiency_state": sufficiency_state,
                "path_risk_note": path_risk["note"],
            },
            "scenario_block": support["scenario_block"],
            "forecast_support": monitoring.get("forecast_support"),
            "support_bundle": support,
            "review_lane": _review_lane(decision_action),
            "summary": _investor_summary(signal, explanation, effect),
            "source_context": source_context,
            "path_risk": path_risk,
            **gating,
            **relevance,
        }
        driver["scenarios"] = list(driver["scenario_block"].get("scenarios") or [])
        drivers.append(driver)

    ranked = sorted(
        drivers,
        key=lambda item: (
            float(item.get("lead_relevance_score") or 0.0),
            float(item.get("decision_relevance_score") or 0.0),
        ),
        reverse=True,
    )
    primary = _select_primary_drivers(ranked, limit=primary_limit)
    if not primary and ranked:
        primary = ranked[:1]
    primary = [
        _upgrade_significant_driver_scenarios(
            driver,
            why_here=why_here,
            holdings_overlay_present=holdings_overlay_present,
        )
        for driver in primary
    ]
    _assign_visibility(primary, visibility_role="lead", coverage_reason="lead_qualified")
    support_drivers = _select_support_drivers(ranked, primary_drivers=primary)
    regime_context_drivers = _select_regime_context_drivers(
        ranked,
        primary_drivers=primary,
        support_drivers=support_drivers,
    )
    _assign_visibility(regime_context_drivers, visibility_role="backdrop", coverage_reason="regime_context")
    _attach_chart_payloads(primary)
    _attach_chart_payloads(support_drivers)
    _attach_chart_payloads(regime_context_drivers)
    signal_stack_groups = build_signal_stack_groups(support_drivers)
    used_primary = {driver["signal_id"] for driver in primary}
    active_groups = {str(driver.get("duplication_group") or "") for driver in primary}
    contingent_source = [
        driver
        for driver in ranked
        if driver["signal_id"] not in used_primary
        and not driver.get("is_regime_context")
        and _eligible_contingent(driver, primary_groups=active_groups, primary_drivers=primary)
    ]
    contingent = build_contingent_drivers(contingent_source, limit=contingent_limit)
    return {
        "drivers": ranked,
        "primary_drivers": primary,
        "support_drivers": support_drivers,
        "regime_context_drivers": regime_context_drivers,
        "signal_stack_groups": signal_stack_groups,
        "contingent_drivers": contingent,
    }


def _attach_chart_payloads(drivers: list[dict[str, Any]]) -> None:
    for driver in drivers:
        driver["chart_payload"] = build_daily_brief_chart_payload(driver)


def _upgrade_significant_driver_scenarios(
    driver: dict[str, Any],
    *,
    why_here: str,
    holdings_overlay_present: bool,
) -> dict[str, Any]:
    source_context = dict(driver.get("source_context") or {})
    effect_profile = {
        "primary_effect_bucket": str(driver.get("primary_effect_bucket") or "market"),
        "mapped_sleeves": list(driver.get("affected_sleeves") or []),
    }
    support_signal = {
        **driver,
        "source_context": source_context,
        "why_it_matters_macro": driver.get("why_it_matters_macro"),
        "why_it_matters_micro": driver.get("why_it_matters_micro"),
        "why_it_matters_short_term": driver.get("why_it_matters_short_term"),
        "why_it_matters_long_term": driver.get("why_it_matters_long_term"),
        "effect_type": driver.get("effect_type"),
        "primary_effect_bucket": driver.get("primary_effect_bucket"),
        "source_kind": driver.get("source_kind"),
        "affected_sleeves": list(driver.get("affected_sleeves") or []),
        "affected_candidates": list(driver.get("affected_candidates") or []),
        "sufficiency_state": driver.get("sufficiency_state"),
        "confidence_class": driver.get("confidence_class"),
        "signal_support_class": driver.get("signal_support_class"),
    }
    support = build_signal_support_bundle(
        support_signal,
        why_here=str(driver.get("why_it_matters_long_term") or why_here),
        portfolio_consequence=str(driver.get("portfolio_consequence") or ""),
        next_action=str(driver.get("next_action") or "Monitor"),
        scenario_depth="significant",
    )
    explanation = build_decision_explanation(
        support_signal,
        effect_profile,
        holdings_overlay_present=holdings_overlay_present,
        source_context=source_context,
        support=support,
    )
    support_class = str((support["bundle"].support.support_strength or "bounded")).strip().lower() or "bounded"
    confidence_class = str(driver.get("confidence_class") or "medium")
    sufficiency_state = str(driver.get("sufficiency_state") or "bounded")
    monitoring = dict(support.get("monitoring_condition") or {})
    updated = {
        **driver,
        "why_it_matters_macro": explanation["why_it_matters_macro"],
        "why_it_matters_micro": explanation["why_it_matters_micro"],
        "why_it_matters_short_term": explanation["why_it_matters_short_term"],
        "why_it_matters_long_term": explanation["why_it_matters_long_term"],
        "what_changed_today": explanation["what_changed_today"],
        "why_this_could_be_wrong": explanation["why_this_could_be_wrong"],
        "why_now_not_before": explanation["why_now_not_before"],
        "implementation_sensitivity": explanation["implementation_sensitivity"],
        "what_changed": explanation["what_changed"],
        "event_context_delta": explanation.get("event_context_delta"),
        "why_it_matters": explanation["why_it_matters"],
        "why_it_matters_economically": explanation.get("why_it_matters_economically"),
        "portfolio_meaning": explanation["portfolio_meaning"],
        "portfolio_and_sleeve_meaning": explanation.get("portfolio_and_sleeve_meaning"),
        "confirm_condition": explanation["confirm_condition"],
        "weaken_condition": explanation["weaken_condition"],
        "break_condition": explanation["break_condition"],
        "scenario_support": explanation.get("scenario_support"),
        "do_not_overread": explanation["overread_reason"],
        "source_and_validity": explanation["source_and_validity"],
        "market_confirmation": explanation.get("market_confirmation"),
        "news_to_market_confirmation": explanation["news_to_market_confirmation"],
        "support_bundle": support,
        "scenario_block": support["scenario_block"],
        "scenarios": list(support["scenario_block"].get("scenarios") or []),
        "monitoring_condition": {
            **driver.get("monitoring_condition", {}),
            **monitoring,
        },
        "forecast_support": monitoring.get("forecast_support"),
        "path_risk_note": str(monitoring.get("path_risk_note") or driver.get("path_risk_note") or ""),
        "signal_support_class": support_class,
        "support_label": _support_label(
            support_class=support_class,
            confidence_class=confidence_class,
            sufficiency_state=sufficiency_state,
        ),
    }
    return updated


def _sufficiency_state(signal: dict[str, Any], runtime: dict[str, Any], effect: dict[str, Any]) -> str:
    usable_truth = runtime.get("usable_truth")
    mapping_scope = str(effect.get("mapping_scope") or "market")
    if usable_truth is False:
        return "insufficient"
    if mapping_scope == "holding":
        return "direct"
    if mapping_scope == "sleeve":
        return "sufficient"
    if usable_truth:
        return "bounded"
    return "thin"


def _confidence_class(signal: dict[str, Any], runtime: dict[str, Any], support: dict[str, Any]) -> str:
    if runtime.get("usable_truth") is False:
        return "low"
    support_summary = getattr(support["bundle"], "support", None)
    support_strength = str(getattr(support_summary, "support_strength", "") or "").lower()
    persistence_score = float(getattr(support_summary, "persistence_score", 0.0) or 0.0)
    cross_asset_confirmation_score = float(getattr(support_summary, "cross_asset_confirmation_score", 0.0) or 0.0)
    if support_strength in {"strong", "tight_interval_support"} and persistence_score >= 0.56 and signal.get("signal_kind") != "news":
        return "high"
    if cross_asset_confirmation_score >= 0.56 and signal.get("signal_kind") != "news":
        return "medium"
    if signal.get("signal_kind") == "news":
        return "low"
    return "medium"


def _next_action(
    *,
    signal: dict[str, Any],
    review_posture: str,
    effect: dict[str, Any],
    sufficiency_state: str,
    source_context: dict[str, Any],
) -> str:
    mapping_scope = str(effect.get("mapping_scope") or "market")
    bucket = str(effect.get("primary_effect_bucket") or "market")
    magnitude = str(signal.get("magnitude") or "minor")
    source_class = str(source_context.get("source_class") or "market_series")
    market_confirmation = str(source_context.get("market_confirmation") or "limited")
    if sufficiency_state in {"thin", "insufficient"}:
        return "Do not act yet"
    if mapping_scope == "holding" and "review" in review_posture.lower():
        return "Review now"
    if mapping_scope == "holding":
        return "Monitor"
    if source_class in {"policy_event", "geopolitical_news"} and market_confirmation != "strong":
        return "Monitor"
    if mapping_scope == "sleeve":
        if bucket == "duration" and str(signal.get("label") or "").lower() == "30y mortgage" and magnitude == "significant":
            return "Review now"
        if bucket in {"inflation", "credit", "policy"} and magnitude == "significant":
            return "Review now"
        if bucket in {"duration", "inflation", "credit", "policy", "dollar_fx", "energy", "real_assets", "volatility", "growth", "liquidity"}:
            return "Monitor"
        return "Monitor"
    if bucket in {"policy", "inflation", "duration", "dollar_fx", "credit"}:
        return "Monitor"
    return "Do not act yet"


def _decision_relevance_score(
    *,
    signal: dict[str, Any],
    effect: dict[str, Any],
    support: dict[str, Any],
    sufficiency_state: str,
    confidence_class: str,
    gating: dict[str, Any],
) -> float:
    magnitude = str(signal.get("magnitude") or "minor")
    history = list(signal.get("history") or [])
    support_summary = getattr(support["bundle"], "support", None)
    support_strength = str(getattr(support_summary, "support_strength", "") or "").lower()
    score = 0.0
    score += float(effect.get("effect_priority") or 8)
    score += float(_MAGNITUDE_WEIGHTS.get(magnitude, 0))
    score += float(_SUFFICIENCY_WEIGHTS.get(sufficiency_state, 0))
    score += 10.0 if effect.get("mapped_sleeves") else 0.0
    score += 8.0 if signal.get("affected_holdings") else 0.0
    score += 6.0 if len(history) >= 3 else 3.0 if len(history) >= 2 else 0.0
    score += 6.0 if support_strength in {"strong", "tight_interval_support"} else 3.0 if support_strength else 0.0
    score += 5.0 if confidence_class == "high" else 3.0 if confidence_class == "medium" else 0.0
    score += float(gating.get("decision_change_potential") or 0.0)
    score += float(gating.get("breadth_of_consequence") or 0.0)
    score += float(gating.get("transmission_clarity") or 0.0)
    score += float(gating.get("evidence_sufficiency") or 0.0)
    score += float(gating.get("freshness_relevance_score") or 0.0)
    score += float(gating.get("lead_relevance_score") or 0.0) * 0.2
    actionability_class = str(gating.get("actionability_class") or "")
    if actionability_class == "sleeve_decision":
        score += 8.0
    elif actionability_class == "holding_instruction":
        score += 10.0
    elif actionability_class == "contextual_monitor":
        score -= 2.0
    elif actionability_class == "evidence_only":
        score -= 8.0
    score += _forecast_intelligence_bonus(
        signal=signal,
        effect=effect,
        source_context=dict(signal.get("source_context") or {}),
        support_summary=support_summary,
        sufficiency_state=sufficiency_state,
        actionability_class=actionability_class,
    )
    if signal.get("signal_kind") == "news":
        score -= 8.0
    return score


def _source_provenance_summary(signal: dict[str, Any], runtime: dict[str, Any], effect: dict[str, Any]) -> str:
    provider = str(runtime.get("provider_used") or "declared provider")
    family = str(runtime.get("source_family") or effect.get("source_kind") or "source")
    freshness = str(runtime.get("freshness") or "current")
    return f"{provider} · {family.replace('_', ' ')} · {freshness}"


def _eligible_primary(driver: dict[str, Any]) -> bool:
    sufficiency = str(driver.get("sufficiency_state") or "")
    if sufficiency in {"thin", "insufficient"}:
        return False
    if str(driver.get("lead_lane") or "") != "daily_lead":
        return False
    if str(driver.get("next_action") or "") == "Do not act yet" and not driver.get("affected_sleeves"):
        return False
    if str(driver.get("actionability_class") or "") in {"contextual_monitor", "evidence_only"}:
        return False
    if float(driver.get("lead_relevance_score") or 0.0) < 64.0:
        return False
    return True


def _select_primary_drivers(ranked: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    eligible = [driver for driver in ranked if _eligible_primary(driver)]
    if not eligible:
        return []

    selected: list[dict[str, Any]] = []
    used_ids: set[str] = set()
    used_groups: set[str] = set()
    used_event_clusters: set[str] = set()

    for driver in eligible:
        group = str(driver.get("duplication_group") or _duplication_group(str(driver.get("primary_effect_bucket") or "market")))
        event_cluster = _event_cluster_id(driver)
        if event_cluster and event_cluster in used_event_clusters:
            continue
        if group in used_groups:
            continue
        selected.append(driver)
        used_ids.add(str(driver.get("signal_id")))
        used_groups.add(group)
        if event_cluster:
            used_event_clusters.add(event_cluster)

    return selected


def _select_support_drivers(
    ranked: list[dict[str, Any]],
    *,
    primary_drivers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not ranked:
        return []

    selected: list[dict[str, Any]] = []
    primary_ids = {str(driver.get("signal_id") or "") for driver in primary_drivers}
    selected_ids: set[str] = set()
    selected_signatures = {_consequence_signature(driver) for driver in primary_drivers}
    selected_event_clusters = {_event_cluster_id(driver) for driver in primary_drivers if _event_cluster_id(driver)}
    score_floor = _support_score_floor(ranked, primary_drivers=primary_drivers)
    ranked_positions = {str(driver.get("signal_id") or ""): index for index, driver in enumerate(ranked)}

    for driver in ranked:
        signal_id = str(driver.get("signal_id") or "")
        if signal_id in selected_ids or signal_id in primary_ids:
            continue
        if not _eligible_support(driver, score_floor=score_floor):
            continue
        event_cluster = _event_cluster_id(driver)
        if event_cluster and event_cluster in selected_event_clusters:
            continue
        signature = _consequence_signature(driver)
        if signature in selected_signatures:
            continue
        if any(_is_repetitive_neighbor(driver, prior) for prior in selected):
            continue
        selected.append(driver)
        selected_ids.add(signal_id)
        selected_signatures.add(signature)
        if event_cluster:
            selected_event_clusters.add(event_cluster)

    active_buckets = _active_aspect_buckets(ranked, primary_ids=primary_ids)
    primary_buckets = {_aspect_bucket_of(driver) for driver in primary_drivers if _aspect_bucket_of(driver)}
    selected_buckets = {_aspect_bucket_of(driver) for driver in selected if _aspect_bucket_of(driver)}
    for bucket in active_buckets:
        if bucket in primary_buckets or bucket in selected_buckets:
            continue
        candidate = next(
            (
                driver
                for driver in ranked
                if str(driver.get("signal_id") or "") not in primary_ids
                and str(driver.get("signal_id") or "") not in selected_ids
                and (not driver.get("is_regime_context") or _fresh_policy_or_news_candidate(driver))
                and _aspect_bucket_of(driver) == bucket
                and _eligible_category_floor(driver)
            ),
            None,
        )
        if candidate is None:
            continue
        event_cluster = _event_cluster_id(candidate)
        if event_cluster and event_cluster in selected_event_clusters:
            continue
        signature = _consequence_signature(candidate)
        if signature in selected_signatures or any(_is_repetitive_neighbor(candidate, prior) for prior in selected):
            continue
        selected.append(candidate)
        selected_ids.add(str(candidate.get("signal_id") or ""))
        selected_signatures.add(signature)
        if event_cluster:
            selected_event_clusters.add(event_cluster)
        selected_buckets.add(bucket)

    selected.sort(key=lambda driver: ranked_positions.get(str(driver.get("signal_id") or ""), 10_000))
    _assign_secondary_visibility(selected, primary_drivers=primary_drivers)
    return selected


def _select_regime_context_drivers(
    ranked: list[dict[str, Any]],
    *,
    primary_drivers: list[dict[str, Any]],
    support_drivers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    primary_ids = {str(driver.get("signal_id") or "") for driver in primary_drivers}
    support_ids = {str(driver.get("signal_id") or "") for driver in support_drivers}
    selected: list[dict[str, Any]] = []
    used_groups: set[str] = set()
    used_event_clusters = {
        cluster
        for cluster in (_event_cluster_id(driver) for driver in [*primary_drivers, *support_drivers])
        if cluster
    }
    group_counts: dict[str, int] = {}

    for driver in ranked:
        signal_id = str(driver.get("signal_id") or "")
        if signal_id in primary_ids or signal_id in support_ids:
            continue
        if not driver.get("is_regime_context"):
            continue
        event_cluster = _event_cluster_id(driver)
        if event_cluster and event_cluster in used_event_clusters:
            continue
        group = str(driver.get("duplication_group") or "")
        group_limit = 2 if group in {"geopolitical_global_news", "policy_release"} else 1
        if group_counts.get(group, 0) >= group_limit and str(driver.get("novelty_class") or "") == "continuation":
            continue
        selected.append(driver)
        if group:
            used_groups.add(group)
            group_counts[group] = group_counts.get(group, 0) + 1
        if event_cluster:
            used_event_clusters.add(event_cluster)

    return selected


def _eligible_contingent(
    driver: dict[str, Any],
    *,
    primary_groups: set[str],
    primary_drivers: list[dict[str, Any]],
) -> bool:
    if str(driver.get("sufficiency_state") or "") in {"thin", "insufficient"}:
        return False
    if float(driver.get("prominence_score") or 0.0) < 24.0:
        return False
    if str(driver.get("actionability_class") or "") == "evidence_only":
        return False
    forecast_support = dict(driver.get("forecast_support") or {})
    trigger_pressure = float(forecast_support.get("trigger_pressure") or 0.0)
    cross_asset_confirmation_score = float(forecast_support.get("cross_asset_confirmation_score") or 0.0)
    threshold_state = str(
        ((driver.get("monitoring_condition") or {}).get("trigger_support") or {}).get("threshold_state")
        or driver.get("threshold_state")
        or ""
    ).lower()
    novelty_class = str(driver.get("novelty_class") or "").lower()
    group = str(driver.get("duplication_group") or "")
    if group not in primary_groups:
        return (
            trigger_pressure >= 0.28
            or threshold_state in {"watch", "breached"}
            or novelty_class in {"new", "threshold_break", "reactivated", "reversal"}
            or cross_asset_confirmation_score >= 0.36
        )
    driver_sleeves = set(driver.get("affected_sleeves") or [])
    driver_source_class = str((driver.get("source_context") or {}).get("source_class") or "")
    if not driver_sleeves:
        return trigger_pressure >= 0.55 and cross_asset_confirmation_score >= 0.42
    for primary in primary_drivers:
        if str(primary.get("duplication_group") or "") != group:
            continue
        primary_sleeves = set(primary.get("affected_sleeves") or [])
        primary_source_class = str((primary.get("source_context") or {}).get("source_class") or "")
        if (
            driver_sleeves - primary_sleeves
            and driver_source_class
            and primary_source_class
            and driver_source_class != primary_source_class
            and float(driver.get("decision_change_potential") or 0.0) >= 10.0
        ):
            return True
        if threshold_state in {"watch", "breached"} and trigger_pressure >= 0.55 and cross_asset_confirmation_score >= 0.42:
            return True
    return False


def _support_score_floor(ranked: list[dict[str, Any]], *, primary_drivers: list[dict[str, Any]]) -> float:
    if primary_drivers:
        primary_scores = sorted(
            float(driver.get("lead_relevance_score") or driver.get("decision_relevance_score") or 0.0)
            for driver in primary_drivers
        )
        anchor = primary_scores[0]
        return max(40.0, anchor - 14.0)
    top_score = float(ranked[0].get("lead_relevance_score") or ranked[0].get("decision_relevance_score") or 0.0) if ranked else 0.0
    return max(40.0, top_score - 18.0)


def _eligible_support(driver: dict[str, Any], *, score_floor: float) -> bool:
    if str(driver.get("sufficiency_state") or "") in {"thin", "insufficient"}:
        return False
    if str(driver.get("lead_lane") or "") == "regime_context" and not _fresh_policy_or_news_candidate(driver):
        return False
    if str(driver.get("actionability_class") or "") == "evidence_only":
        return False
    score = float(driver.get("lead_relevance_score") or driver.get("decision_relevance_score") or 0.0)
    member_floor = max(34.0, score_floor - 8.0)
    if score < member_floor:
        if "high" not in str(driver.get("path_risk_note") or "").lower():
            return False
    if float(driver.get("prominence_score") or 0.0) < 24.0:
        return False
    if str(driver.get("novelty_class") or "") == "continuation" and float(driver.get("freshness_relevance_score") or 0.0) < 8.0:
        return False
    return True


def _consequence_signature(driver: dict[str, Any]) -> tuple[str, str, tuple[str, ...]]:
    group = str(
        driver.get("aspect_bucket")
        or driver.get("duplication_group")
        or _duplication_group(str(driver.get("primary_effect_bucket") or "market"))
    )
    event_cluster = _event_cluster_id(driver)
    holdings = tuple(sorted(str(item).strip().upper() for item in list(driver.get("affected_holdings") or []) if str(item).strip()))
    candidates = tuple(sorted(str(item).strip().upper() for item in list(driver.get("affected_candidates") or []) if str(item).strip()))
    sleeves = tuple(sorted(str(item).strip().lower() for item in list(driver.get("affected_sleeves") or []) if str(item).strip()))
    source_class = str((driver.get("source_context") or {}).get("source_class") or "").strip().lower() or "market_series"
    if event_cluster:
        return group, f"event:{source_class}", (event_cluster,)
    if holdings:
        return group, "holding", holdings[:3]
    if candidates:
        return group, f"candidate:{source_class}", candidates[:3]
    if sleeves:
        return group, f"sleeve:{source_class}", sleeves[:2]
    return group, source_class, (str(driver.get("primary_effect_bucket") or "market"),)


def _is_repetitive_neighbor(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_cluster = _event_cluster_id(left)
    right_cluster = _event_cluster_id(right)
    if left_cluster and left_cluster == right_cluster:
        return True
    left_group = str(left.get("aspect_bucket") or left.get("duplication_group") or "")
    right_group = str(right.get("aspect_bucket") or right.get("duplication_group") or "")
    if left_group != right_group:
        return False
    left_tokens = _normalized_tokens(
        " ".join(
            [
                str(left.get("decision_title") or ""),
                str(left.get("summary") or ""),
                str(left.get("portfolio_consequence") or ""),
            ]
        )
    )
    right_tokens = _normalized_tokens(
        " ".join(
            [
                str(right.get("decision_title") or ""),
                str(right.get("summary") or ""),
                str(right.get("portfolio_consequence") or ""),
            ]
        )
    )
    if not left_tokens or not right_tokens:
        return False
    overlap = len(left_tokens & right_tokens)
    return overlap / max(len(left_tokens), len(right_tokens)) >= 0.72


def _event_cluster_id(driver: dict[str, Any]) -> str:
    source_context = dict(driver.get("source_context") or {})
    return str(driver.get("event_cluster_id") or source_context.get("event_cluster_id") or "").strip()


def _normalized_tokens(text: str) -> set[str]:
    cleaned = "".join(char.lower() if char.isalnum() else " " for char in str(text or ""))
    stop = {
        "the",
        "and",
        "for",
        "with",
        "this",
        "that",
        "into",
        "current",
        "brief",
        "read",
        "level",
        "today",
        "risk",
        "sleeve",
    }
    return {token for token in cleaned.split() if len(token) > 2 and token not in stop}


def _review_lane(action: str) -> str:
    normalized = str(action or "").lower()
    if "review" in normalized:
        return "review_now"
    if "monitor" in normalized:
        return "monitor"
    return "do_not_act_yet"


def _affected_sleeve(sleeves: list[str]) -> str | None:
    if not sleeves:
        return None
    return str(sleeves[0]).replace("sleeve_", "").replace("_", " ").title()


def _investor_summary(signal: dict[str, Any], explanation: dict[str, Any], effect: dict[str, Any]) -> str:
    short_term = str(explanation.get("why_it_matters_short_term") or "").strip()
    consequence = str(explanation.get("portfolio_consequence") or "").strip()
    parts = [part for part in [short_term, consequence] if part]
    if parts:
        return " ".join(dict.fromkeys(parts))
    label = str(signal.get("label") or "Signal")
    bucket = str(effect.get("primary_effect_bucket") or "market").replace("_", " ")
    return f"{label} is the active {bucket} driver today."


def _effect_group(bucket: str) -> str:
    return _duplication_group(bucket)


def _aspect_bucket(*, bucket: str, source_context: dict[str, Any]) -> str:
    source_class = str(source_context.get("source_class") or "market_series")
    if source_class == "geopolitical_news":
        return "geopolitical_global_news"
    if bucket == "volatility":
        return "volatility_risk_regime"
    if bucket == "duration":
        return "rates_duration"
    if bucket in {"inflation", "energy", "real_assets"}:
        return "inflation_real_assets"
    if bucket in {"credit", "liquidity"}:
        return "credit_liquidity"
    if bucket == "dollar_fx":
        return "fx_dollar"
    if bucket == "policy" or source_class in {"macro_release", "policy_event"}:
        return "policy_release"
    if bucket in {"growth", "market"}:
        return "growth_equity_risk"
    return "implementation_execution"


def _aspect_bucket_of(driver: dict[str, Any]) -> str:
    return str(driver.get("aspect_bucket") or driver.get("duplication_group") or "implementation_execution")


def _active_aspect_buckets(
    ranked: list[dict[str, Any]],
    *,
    primary_ids: set[str],
) -> list[str]:
    buckets: list[str] = []
    seen: set[str] = set()
    for driver in ranked:
        signal_id = str(driver.get("signal_id") or "")
        if signal_id in primary_ids:
            continue
        if not _eligible_aspect_bucket(driver):
            continue
        bucket = _aspect_bucket_of(driver)
        if bucket in seen:
            continue
        buckets.append(bucket)
        seen.add(bucket)
    return buckets


def _eligible_aspect_bucket(driver: dict[str, Any]) -> bool:
    if str(driver.get("sufficiency_state") or "") in {"thin", "insufficient"}:
        return False
    if str(driver.get("lead_lane") or "") == "regime_context" and not _fresh_policy_or_news_candidate(driver):
        return False
    if str(driver.get("actionability_class") or "") == "evidence_only":
        return False
    if float(driver.get("prominence_score") or 0.0) < 24.0:
        return False
    if (
        float(driver.get("lead_relevance_score") or 0.0) < 34.0
        and str(driver.get("threshold_state") or "") not in {"watch", "breached"}
        and not _fresh_policy_or_news_candidate(driver)
    ):
        return False
    if str(driver.get("novelty_class") or "") == "continuation" and float(driver.get("freshness_relevance_score") or 0.0) < 8.0:
        return False
    return True


def _eligible_category_floor(driver: dict[str, Any]) -> bool:
    if not _eligible_aspect_bucket(driver):
        return False
    if float(driver.get("prominence_score") or 0.0) < 20.0:
        return False
    if float(driver.get("decision_change_potential") or 0.0) < 8.0:
        return False
    return True


def _assign_visibility(
    drivers: list[dict[str, Any]],
    *,
    visibility_role: str,
    coverage_reason: str,
) -> None:
    for driver in drivers:
        driver["visibility_role"] = visibility_role
        driver["prominence_class"] = visibility_role
        driver["coverage_reason"] = coverage_reason
        if visibility_role == "backdrop":
            driver["decision_status"] = "backdrop"


def _assign_secondary_visibility(
    drivers: list[dict[str, Any]],
    *,
    primary_drivers: list[dict[str, Any]],
) -> None:
    primary_buckets = {_aspect_bucket_of(driver) for driver in primary_drivers if _aspect_bucket_of(driver)}
    seen_buckets: set[str] = set()
    for driver in drivers:
        bucket = _aspect_bucket_of(driver)
        if bucket not in seen_buckets:
            driver["visibility_role"] = "representative"
            driver["prominence_class"] = "support"
            driver["coverage_reason"] = "grouped_support" if bucket in primary_buckets else "category_floor"
            driver.setdefault("decision_status", "monitor")
            seen_buckets.add(bucket)
        else:
            driver["visibility_role"] = "support"
            driver["prominence_class"] = "support"
            driver["coverage_reason"] = "grouped_support"
            driver.setdefault("decision_status", "monitor")


def _decision_title(
    *,
    signal: dict[str, Any],
    effect: dict[str, Any],
    next_action: str,
    source_context: dict[str, Any],
) -> str:
    label = str(signal.get("label") or "Signal")
    bucket = str(effect.get("primary_effect_bucket") or "market")
    sleeve = _humanize_primary_sleeve(list(effect.get("mapped_sleeves") or []))
    source_class = str(source_context.get("source_class") or "market_series")
    if source_class == "policy_event":
        return f"{_event_display_title(source_context, fallback='Policy headlines')} is shifting the risk map"
    if source_class == "geopolitical_news":
        return f"{_event_display_title(source_context, fallback='Global headline risk')} is back in the brief"
    if bucket == "duration":
        return f"{sleeve or 'Bond sleeve'} rate pressure is changing the decision frame"
    if bucket == "inflation":
        return "Inflation path is resetting the bond and real-asset read"
    if bucket == "credit":
        return "Credit conditions are changing the carry and risk budget read"
    if bucket == "policy":
        return "Policy path is steering the current portfolio read"
    if bucket == "dollar_fx":
        return "Dollar pressure is changing the hurdle on global risk"
    if bucket == "energy":
        return "Energy shock risk is changing the real-assets read"
    if bucket == "real_assets":
        return "Real-assets sleeve is carrying the inflation-hedge message"
    if bucket == "volatility":
        return "Volatility regime is testing risk tolerance"
    if bucket == "growth":
        return f"{sleeve or 'Equity sleeve'} tone remains the main growth read"
    if "review" in next_action.lower():
        return f"{label} is active enough to reopen the brief today"
    return f"{label} remains one of the drivers shaping the brief"


def _short_title(
    *,
    signal: dict[str, Any],
    effect: dict[str, Any],
    source_context: dict[str, Any],
) -> str:
    label = str(signal.get("label") or "Signal")
    lowered = label.lower()
    bucket = str(effect.get("primary_effect_bucket") or "market")
    source_class = str(source_context.get("source_class") or "market_series")
    if source_class == "policy_event":
        subtype = str(source_context.get("event_subtype") or "")
        if subtype == "trade_tariff_policy" or "tariff" in lowered:
            return "Tariff risk stays in play"
        if subtype == "central_bank_policy" or any(term in lowered for term in ("fed", "ecb", "boj", "pboc", "policy", "treasury")):
            return "Policy path stays in focus"
        if subtype == "fiscal_political_policy":
            return "Fiscal policy risk stays in focus"
        return "Policy risk stays in focus"
    if source_class == "geopolitical_news":
        subtype = str(source_context.get("event_subtype") or "")
        if subtype == "middle_east_security" or any(term in lowered for term in ("iran", "israel", "hormuz", "ceasefire")):
            return "Middle East risk stays live"
        if subtype == "shipping_energy_supply" or any(term in lowered for term in ("shipping", "oil", "sanction", "missile", "strike")):
            return "Global supply risk stays live"
        if subtype == "china_taiwan_security":
            return "China/Taiwan risk stays live"
        if subtype == "russia_ukraine_energy":
            return "Europe energy risk stays live"
        return "Global headline risk stays live"
    if bucket == "duration":
        if lowered == "30y mortgage":
            return "Duration timing stays constrained"
        if "real yield" in lowered:
            return "Real yields still pressure duration"
        return "Duration still faces a hurdle"
    if bucket == "inflation":
        return "Inflation still blocks easy duration"
    if bucket == "credit":
        return "Credit still drives the risk budget"
    if bucket == "policy":
        return "Policy still anchors timing"
    if bucket == "dollar_fx":
        return "Dollar strength raises the hurdle"
    if bucket == "energy":
        return "Energy pressure keeps hedges active"
    if bucket == "real_assets":
        return "Real assets keep hedge support"
    if bucket == "volatility":
        return "Volatility is back on watch"
    if bucket == "growth":
        return "Equity risk still needs selectivity"
    if bucket == "liquidity":
        return "Liquidity still shapes risk appetite"
    return label[:72]


def _short_subtitle(
    *,
    signal: dict[str, Any],
    effect: dict[str, Any],
    source_context: dict[str, Any],
    explanation: dict[str, Any],
) -> str:
    bucket = str(effect.get("primary_effect_bucket") or "market")
    source_class = str(source_context.get("source_class") or "market_series")
    market_confirmation = str(source_context.get("market_confirmation") or "limited")
    event_status = str(source_context.get("event_status") or "developing")
    if source_class == "policy_event":
        trigger = _event_trigger_summary(source_context)
        if market_confirmation in {"strong", "moderate"}:
            return f"Policy headlines are moving markets; {trigger}" if trigger else "Policy headlines are moving rates, cash, and risk appetite again."
        return trigger or "Policy headlines matter, but action still needs clearer market follow-through."
    if source_class == "geopolitical_news":
        trigger = _event_trigger_summary(source_context)
        if market_confirmation in {"strong", "moderate"}:
            return f"Headline risk is spilling into markets; {trigger}" if trigger else "Headline risk is spilling into oil, FX, and broader risk sentiment."
        if event_status == "confirmed":
            return f"The event is confirmed; {trigger}" if trigger else "The event is confirmed, but portfolio changes still need price validation."
        return trigger or "Headline risk is active, but it is not yet a stand-alone portfolio call."
    if bucket == "duration":
        if str(signal.get("label") or "").lower() == "30y mortgage":
            return "Higher financing costs keep bond adds patient."
        return "Higher yields keep duration extensions selective."
    if bucket == "inflation":
        return "Sticky price pressure still favors patience on duration and support for hedges."
    if bucket == "credit":
        return "Spread direction still governs carry, funding conditions, and risk budget."
    if bucket == "policy":
        return "The latest policy reading still guides bond and cash timing."
    if bucket == "dollar_fx":
        return "Dollar firmness keeps global risk sizing selective."
    if bucket == "energy":
        return "Oil pressure keeps the inflation-hedge case alive."
    if bucket == "real_assets":
        return "Hedge demand still looks healthier than broad risk appetite."
    if bucket == "volatility":
        return "Higher volatility keeps equity risk and cash posture on watch."
    if bucket == "growth":
        return "Breadth and leadership still decide how much equity risk deserves capital."
    if bucket == "liquidity":
        return "Liquidity-sensitive assets still frame how much risk the market will tolerate."
    return str(
        explanation.get("why_it_matters_short_term")
        or explanation.get("portfolio_consequence")
        or signal.get("summary")
        or ""
    ).strip()


def _event_display_title(source_context: dict[str, Any], *, fallback: str) -> str:
    title = str(source_context.get("event_title") or "").strip()
    return title or fallback


def _event_trigger_summary(source_context: dict[str, Any]) -> str:
    summary = str(source_context.get("event_trigger_summary") or "").strip()
    if summary:
        return summary
    assets = [str(item).strip() for item in list(source_context.get("confirmation_assets") or []) if str(item).strip()]
    if assets:
        return f"Watch {', '.join(assets[:4])} for confirmation before changing portfolio posture."
    return ""


def _fresh_policy_or_news_candidate(driver: dict[str, Any]) -> bool:
    source_class = str((driver.get("source_context") or {}).get("source_class") or "")
    if source_class not in {"policy_event", "geopolitical_news"}:
        return False
    age_days = float(driver.get("freshness_age_days") or 99.0)
    if age_days > 1.5:
        return False
    if float(driver.get("prominence_score") or 0.0) < 18.0:
        return False
    if float(driver.get("decision_change_potential") or 0.0) < 6.0:
        return False
    return True


def _humanize_primary_sleeve(sleeves: list[str]) -> str | None:
    if not sleeves:
        return None
    raw = str(sleeves[0]).replace("sleeve_", "").replace("_", " ").strip()
    if raw.lower() == "ig bonds":
        return "Bond sleeve"
    if raw.lower() == "global equity core":
        return "Global equity sleeve"
    if raw.lower() == "real assets":
        return "Real-assets sleeve"
    if raw.lower() == "cash bills":
        return "Cash sleeve"
    return raw.title() if raw else None


def _signal_label(
    *,
    signal: dict[str, Any],
    effect: dict[str, Any],
    source_context: dict[str, Any],
) -> str:
    label = str(signal.get("label") or signal.get("symbol") or "Signal").strip()
    source_class = str(source_context.get("source_class") or "market_series")
    bucket = str(effect.get("primary_effect_bucket") or "market")
    if source_class == "geopolitical_news":
        cleaned = label.split(" - ")[0].strip()
        lowered = cleaned.lower()
        if "israel" in lowered and "hezbollah" in lowered:
            return "Israel Hezbollah escalation"
        if any(term in lowered for term in {"iran", "hormuz", "ceasefire"}):
            return "Middle East escalation"
        return cleaned
    if source_class == "policy_event":
        cleaned = label.split(" - ")[0].strip()
        if any(term in cleaned.lower() for term in {"fed", "ecb", "boj", "pboc"}):
            return "Policy event"
        return cleaned
    if bucket == "credit":
        return "US credit spread"
    if bucket == "dollar_fx":
        return "Dollar index"
    if bucket == "duration" and "real yield" in label.lower():
        return "US 10Y real yield"
    if bucket == "duration" and label.lower() == "rates":
        return "US 10Y yield"
    if bucket == "duration" and label.lower() == "ust 30y":
        return "US 30Y yield"
    if label.lower() == "30y mortgage":
        return "US 30Y mortgage rate"
    if label.lower() == "inflation":
        return "US inflation"
    return label


def _card_family(
    *,
    effect: dict[str, Any],
    source_context: dict[str, Any],
) -> str:
    bucket = str(effect.get("primary_effect_bucket") or "market")
    source_class = str(source_context.get("source_class") or "market_series")
    if source_class == "policy_event" or bucket == "policy":
        return "policy"
    if source_class == "geopolitical_news":
        return "geopolitics"
    if source_class == "benchmark_move":
        return "benchmark"
    if bucket in {"duration"}:
        return "rates"
    if bucket in {"inflation"}:
        return "macro"
    if bucket in {"credit", "liquidity"}:
        return "credit"
    if bucket in {"dollar_fx"}:
        return "fx"
    if bucket in {"energy", "real_assets"}:
        return "commodity"
    if bucket in {"growth", "volatility", "market"}:
        return "equity"
    return "macro"


def _market_confirmation_state(source_context: dict[str, Any]) -> str | None:
    source_class = str(source_context.get("source_class") or "market_series")
    if source_class not in {"policy_event", "geopolitical_news"}:
        return None
    market_confirmation = str(source_context.get("market_confirmation") or "limited")
    return {
        "unconfirmed": "none",
        "limited": "limited",
        "moderate": "partial",
        "strong": "broad",
    }.get(market_confirmation, "limited")


def _evidence_class_code(
    *,
    signal: dict[str, Any],
    source_context: dict[str, Any],
) -> str:
    source_class = str(source_context.get("source_class") or "market_series")
    market_confirmation = str(source_context.get("market_confirmation") or "limited")
    authority = str(
        (signal.get("runtime_provenance") or {}).get("source_authority_tier")
        or (signal.get("runtime_provenance") or {}).get("provenance_strength")
        or source_context.get("source_authority_tier")
        or ""
    ).strip().lower()
    if source_class == "macro_release":
        return "official_release"
    if source_class in {"policy_event", "geopolitical_news"}:
        return "market_confirmed_news" if market_confirmation in {"moderate", "strong"} else "reported_event_unconfirmed_market_read"
    if "licensed" in authority:
        return "licensed_cross_check_gap"
    if authority in {"public_verified_close", "public verified close"} or str(signal.get("source_type") or "") == "market_close":
        return "public_verified_close"
    if source_class == "benchmark_move":
        return "bounded_inference"
    return "bounded_inference"


def _freshness_contract(source_context: dict[str, Any]) -> tuple[str, str]:
    source_class = str(source_context.get("source_class") or "market_series")
    lead_lane = str(source_context.get("lead_lane") or "").strip().lower()
    age_days = float(source_context.get("freshness_age_days") or 99.0)
    if lead_lane == "regime_context":
        return "backdrop_valid", "still valid but outside active market session"
    if source_class == "macro_release":
        if age_days > 2.0:
            return "latest_valid", "latest valid official print"
        return "fresh", "fresh in current brief window"
    if source_class in {"policy_event", "geopolitical_news"}:
        if age_days > 1.0:
            return "latest_valid", "latest valid reported event"
        return "fresh", "fresh in current brief window"
    return "fresh", "fresh in current brief window"


def _decision_status(
    *,
    visibility_role: str,
    next_action: str,
    source_context: dict[str, Any],
    support: dict[str, Any],
    gating: dict[str, Any],
) -> str:
    lead_lane = str(visibility_role or source_context.get("lead_lane") or "").lower()
    source_class = str(source_context.get("source_class") or "market_series")
    market_confirmation = str(source_context.get("market_confirmation") or "limited").lower()
    freshness_age_days = float(source_context.get("freshness_age_days") or 99.0)
    threshold_state = str(source_context.get("threshold_state") or "").lower()
    support_summary = getattr(support.get("bundle"), "support", None)
    trigger_pressure = float(getattr(support_summary, "trigger_pressure", 0.0) or 0.0)
    cross_asset_confirmation_score = float(getattr(support_summary, "cross_asset_confirmation_score", 0.0) or 0.0)
    decision_change_potential = float(gating.get("decision_change_potential") or 0.0)
    evidence_sufficiency = float(gating.get("evidence_sufficiency") or 0.0)
    actionability_class = str(gating.get("actionability_class") or "")
    if lead_lane == "regime_context":
        return "backdrop"
    if threshold_state == "breached" and (
        cross_asset_confirmation_score >= 0.46
        or decision_change_potential >= 11.0
        or (freshness_age_days <= 2.0 and trigger_pressure >= 0.58)
    ):
        return "triggered"
    if source_class in {"policy_event", "geopolitical_news"} and market_confirmation not in {"moderate", "strong"}:
        return "near_trigger" if trigger_pressure >= 0.58 or threshold_state in {"watch", "breached"} else "do_not_act_yet"
    if (
        "review" in next_action.lower()
        and freshness_age_days <= 2.0
        and decision_change_potential >= 12.0
        and evidence_sufficiency >= 11.0
        and (
            trigger_pressure >= 0.62
            or cross_asset_confirmation_score >= 0.56
            or (lead_lane == "daily_lead" and actionability_class != "contextual_monitor")
        )
    ):
        return "review_now"
    if (
        threshold_state in {"watch", "breached"}
        and trigger_pressure >= 0.56
        and (decision_change_potential >= 10.0 or cross_asset_confirmation_score >= 0.4 or freshness_age_days <= 2.0)
    ):
        return "near_trigger"
    if evidence_sufficiency < 10.0 or actionability_class == "contextual_monitor":
        return "do_not_act_yet" if trigger_pressure < 0.45 and cross_asset_confirmation_score < 0.46 else "monitor"
    return "monitor"


def _action_posture(
    *,
    next_action: str,
    signal: dict[str, Any],
    effect: dict[str, Any],
    source_context: dict[str, Any],
) -> str:
    normalized = str(next_action or "").lower()
    if str(source_context.get("lead_lane") or "").lower() == "regime_context":
        return "monitor_only"
    if signal.get("affected_holdings"):
        return "portfolio_review_only"
    if "review" in normalized:
        return "sleeve_timing_live"
    if effect.get("mapped_sleeves"):
        return "sleeve_tilt_watch"
    return "monitor_only"


def _support_label(*, support_class: str, confidence_class: str, sufficiency_state: str) -> str | None:
    support = str(support_class or "").strip().lower()
    confidence = str(confidence_class or "").strip().lower()
    sufficiency = str(sufficiency_state or "").strip().lower()
    parts: list[str] = []
    if support:
        if support in {"strong", "tight_interval_support"}:
            parts.append("strong path support")
        elif support in {"moderate", "support_only", "benchmark", "bounded"}:
            parts.append("usable path support")
        else:
            parts.append("bounded path support")
    if confidence == "high":
        parts.append("high clarity")
    elif confidence == "medium":
        parts.append("medium clarity")
    elif confidence == "low":
        parts.append("low clarity")
    if sufficiency in {"thin", "insufficient"}:
        parts.append("thin evidence")
    elif sufficiency == "bounded":
        parts.append("bounded evidence")
    elif sufficiency:
        parts.append("sufficient evidence")
    if not parts:
        return None
    return " · ".join(dict.fromkeys(parts))


def _evidence_title(
    *,
    signal: dict[str, Any],
    effect: dict[str, Any],
    source_context: dict[str, Any],
    signal_label: str,
    freshness_label: str,
) -> str:
    source_class = str(source_context.get("source_class") or "market_series")
    current_value = source_context.get("current_state")
    previous_value = source_context.get("previous_state")
    bucket = str(effect.get("primary_effect_bucket") or "market")
    source_type = str(source_context.get("source_type") or "").lower()
    freshness_anchor = _compact_freshness_anchor(source_context, freshness_label)
    if source_class in {"policy_event", "geopolitical_news"}:
        confirmation = _market_confirmation_state(source_context) or "limited"
        event_anchor = "reported today" if freshness_anchor == "fresh in current brief window" else "reported"
        return f"{signal_label} {event_anchor}, market confirmation still {confirmation}"
    value_text = _reading_text(signal_label=signal_label, current_value=current_value, bucket=bucket)
    delta_text = _delta_text(
        current_value=current_value,
        previous_value=previous_value,
        bucket=bucket,
        source_class=source_class,
        source_type=source_type,
    )
    return f"{value_text}, {delta_text} · {freshness_anchor}"


def _interpretation_subtitle(
    *,
    signal: dict[str, Any],
    effect: dict[str, Any],
    source_context: dict[str, Any],
    explanation: dict[str, Any],
) -> str:
    bucket = str(effect.get("primary_effect_bucket") or "market")
    source_class = str(source_context.get("source_class") or "market_series")
    market_confirmation_state = _market_confirmation_state(source_context)
    if source_class in {"policy_event", "geopolitical_news"}:
        trigger = _event_trigger_summary(source_context)
        if market_confirmation_state in {"partial", "broad"}:
            suffix = f" {trigger}" if trigger else ""
            return f"The headline is starting to spill into markets, but it still belongs at sleeve-monitor level until confirmation broadens further.{suffix}"
        suffix = f" {trigger}" if trigger else ""
        return f"Treat this as a live policy or geopolitical input, not a portfolio move, until cross-asset confirmation broadens.{suffix}"
    if bucket == "duration":
        return "Rates are still high enough that adding more long bonds should stay selective for now."
    if bucket == "inflation":
        return "Inflation is still firm enough that easy bond relief looks less credible for now."
    if bucket == "credit":
        return "Credit still says funding conditions are tight, so safer bond and cash exposure stays easier to justify than adding more risk."
    if bucket == "dollar_fx":
        return "The dollar is still firm enough that broad global risk adds look harder to justify for now."
    if bucket == "energy":
        return "Oil is still high enough to keep inflation protection relevant, but not enough on its own to justify a stand-alone commodity trade."
    if bucket == "real_assets":
        return "Protection demand is still firm enough that real assets stay relevant, but not enough yet for a bigger portfolio shift."
    if bucket in {"growth", "volatility", "market"}:
        return "The move is helping risk appetite, but broader confirmation is still needed before bigger equity adds look credible."
    return str(
        explanation.get("portfolio_meaning")
        or explanation.get("why_it_matters")
        or signal.get("summary")
        or ""
    ).strip()


def _reading_text(*, signal_label: str, current_value: Any, bucket: str) -> str:
    value = None
    try:
        value = float(current_value)
    except (TypeError, ValueError):
        value = None
    if value is None:
        return signal_label
    if bucket in {"duration", "credit", "inflation", "policy"} or "yield" in signal_label.lower() or "rate" in signal_label.lower() or "spread" in signal_label.lower():
        return f"{signal_label} {value:.2f}%"
    if value >= 1000:
        return f"{signal_label} {value:,.2f}" if value < 10000 else f"{signal_label} {value:,.0f}"
    return f"{signal_label} {value:.2f}"


def _delta_text(
    *,
    current_value: Any,
    previous_value: Any,
    bucket: str,
    source_class: str,
    source_type: str,
) -> str:
    try:
        current = float(current_value)
        previous = float(previous_value)
    except (TypeError, ValueError):
        anchor = "official print" if source_class == "macro_release" else "close" if source_type == "market_close" else "reading"
        return f"latest valid {anchor}"
    delta = current - previous
    direction = "up" if delta > 0 else "down" if delta < 0 else "unchanged"
    rate_like = bucket in {"duration", "credit", "inflation", "policy"} or current < 25.0
    anchor = "prior official print" if source_class == "macro_release" else "prior close" if source_type == "market_close" else "prior reading"
    if rate_like:
        bps = abs(delta) * 100.0
        if direction == "unchanged":
            return f"unchanged versus {anchor}"
        return f"{direction} {bps:.0f} bps from {anchor}"
    if previous == 0:
        return f"versus {anchor}"
    change_pct = abs(((current - previous) / abs(previous)) * 100.0)
    if direction == "unchanged":
        return f"unchanged on the day"
    if source_type == "market_close":
        return f"{direction} {change_pct:.2f}% on the day"
    return f"{direction} {change_pct:.2f}% from {anchor}"


def _compact_freshness_anchor(source_context: dict[str, Any], freshness_label: str) -> str:
    source_class = str(source_context.get("source_class") or "market_series")
    lead_lane = str(source_context.get("lead_lane") or "").strip().lower()
    if lead_lane == "regime_context":
        return "still valid but outside the active session"
    if source_class == "macro_release":
        return "fresh official print" if "fresh" in freshness_label.lower() else "latest valid official print"
    if source_class in {"policy_event", "geopolitical_news"}:
        return "fresh in current brief window" if "fresh" in freshness_label.lower() else "latest valid event"
    return "fresh in current brief window" if "fresh" in freshness_label.lower() else "latest valid reading"


def _gating_metrics(
    *,
    signal: dict[str, Any],
    effect: dict[str, Any],
    source_context: dict[str, Any],
    support: dict[str, Any],
    sufficiency_state: str,
    confidence_class: str,
    next_action: str,
) -> dict[str, Any]:
    duplication_group = _duplication_group(str(effect.get("primary_effect_bucket") or "market"))
    source_class = str(source_context.get("source_class") or "market_series")
    magnitude = str(signal.get("magnitude") or "unknown")
    support_strength = str(support["bundle"].support.support_strength or "").lower()
    sleeves = list(effect.get("mapped_sleeves") or [])
    holdings = list(signal.get("affected_holdings") or [])
    decision_change_potential = float(effect.get("effect_priority") or 8)
    if source_class == "macro_release":
        decision_change_potential += 5.0
    elif source_class in {"policy_event", "geopolitical_news"}:
        decision_change_potential -= 2.0
    if magnitude == "significant":
        decision_change_potential += 5.0
    elif magnitude == "moderate":
        decision_change_potential += 2.0

    breadth_of_consequence = 6.0 if len(sleeves) >= 2 else 3.0 if sleeves else 0.0
    breadth_of_consequence += 4.0 if holdings else 0.0

    transmission_clarity = 12.0 if source_class == "macro_release" else 10.0 if source_class == "market_series" else 7.0 if source_class == "benchmark_move" else 5.0
    if str(source_context.get("market_confirmation") or "") == "strong":
        transmission_clarity += 2.0
    elif str(source_context.get("market_confirmation") or "") == "limited":
        transmission_clarity -= 2.0

    evidence_sufficiency = float(_SUFFICIENCY_WEIGHTS.get(sufficiency_state, 0))
    if confidence_class == "high":
        evidence_sufficiency += 4.0
    elif confidence_class == "medium":
        evidence_sufficiency += 2.0
    if support_strength in {"strong", "tight_interval_support"}:
        evidence_sufficiency += 3.0

    actionability_class = _actionability_class(
        effect=effect,
        source_context=source_context,
        next_action=next_action,
        sufficiency_state=sufficiency_state,
        magnitude=magnitude,
    )
    prominence_score = decision_change_potential + breadth_of_consequence + transmission_clarity + evidence_sufficiency
    if actionability_class == "holding_instruction":
        prominence_score += 5.0
    elif actionability_class == "sleeve_decision":
        prominence_score += 3.0
    elif actionability_class == "contextual_monitor":
        prominence_score -= 4.0
    elif actionability_class == "evidence_only":
        prominence_score -= 10.0

    return {
        "decision_change_potential": round(decision_change_potential, 1),
        "breadth_of_consequence": round(breadth_of_consequence, 1),
        "transmission_clarity": round(transmission_clarity, 1),
        "evidence_sufficiency": round(evidence_sufficiency, 1),
        "duplication_group": duplication_group,
        "actionability_class": actionability_class,
        "prominence_score": round(prominence_score, 1),
    }


def _duplication_group(bucket: str) -> str:
    if bucket in {"duration"}:
        return "rates_duration"
    if bucket in {"inflation", "energy", "real_assets"}:
        return "inflation_real_assets"
    if bucket in {"dollar_fx"}:
        return "fx_dollar_hurdle"
    if bucket in {"credit", "liquidity"}:
        return "credit_liquidity"
    if bucket in {"policy"}:
        return "policy_release"
    if bucket in {"growth", "volatility", "market"}:
        return "growth_equity_risk"
    return "implementation_execution"


def _actionability_class(
    *,
    effect: dict[str, Any],
    source_context: dict[str, Any],
    next_action: str,
    sufficiency_state: str,
    magnitude: str,
) -> str:
    mapping_scope = str(effect.get("mapping_scope") or "market")
    source_class = str(source_context.get("source_class") or "market_series")
    market_confirmation = str(source_context.get("market_confirmation") or "limited")
    bucket = str(effect.get("primary_effect_bucket") or "market")
    if sufficiency_state in {"thin", "insufficient"}:
        return "evidence_only"
    if mapping_scope == "holding":
        return "holding_instruction"
    if bucket in {"volatility", "liquidity"}:
        return "contextual_monitor"
    if bucket == "policy" and (source_class != "macro_release" or magnitude in {"minor", "unknown"}):
        return "contextual_monitor"
    if mapping_scope == "sleeve":
        if source_class in {"policy_event", "geopolitical_news"} and market_confirmation != "strong":
            return "contextual_monitor"
        if "review" in next_action.lower():
            return "sleeve_decision"
        if "monitor" in next_action.lower() and bucket in {"duration", "inflation", "credit", "dollar_fx", "energy", "real_assets", "growth"}:
            return "sleeve_decision"
        if "monitor" in next_action.lower():
            return "contextual_monitor"
    if "monitor" in next_action.lower():
        return "contextual_monitor"
    return "evidence_only"


def _path_risk(
    *,
    signal: dict[str, Any],
    support: dict[str, Any],
    source_context: dict[str, Any],
) -> dict[str, Any]:
    bundle = support["bundle"]
    result = getattr(bundle, "result", None)
    support_summary = getattr(bundle, "support", None)
    near_term = dict((support.get("monitoring_condition") or {}).get("trigger_support") or {})
    support_strength = str(getattr(support_summary, "support_strength", "") or "").lower()
    confidence_summary = str(getattr(support_summary, "confidence_summary", "") or "").strip()
    anomaly = float(getattr(result, "anomaly_score", 0.0) or 0.0)
    direction = str(getattr(result, "direction", "mixed") or "mixed")
    confidence_band = str(getattr(result, "confidence_band", "") or "").lower()
    persistence_score = float(getattr(support_summary, "persistence_score", 0.0) or 0.0)
    fade_risk = float(getattr(support_summary, "fade_risk", 0.0) or 0.0)
    trigger_pressure = float(getattr(support_summary, "trigger_pressure", 0.0) or 0.0)
    regime_alignment_score = float(getattr(support_summary, "regime_alignment_score", 0.0) or 0.0)
    cross_asset_confirmation_score = float(getattr(support_summary, "cross_asset_confirmation_score", 0.0) or 0.0)
    scenario_support_strength = str(getattr(support_summary, "scenario_support_strength", "") or support_strength or "available")
    source_class = str(source_context.get("source_class") or "market_series")
    market_confirmation = str(source_context.get("market_confirmation") or "limited")
    bucket = str(source_context.get("bucket") or "market")
    if anomaly >= 0.8 or str(near_term.get("threshold_state") or "") == "breached":
        breach_risk = "high"
    elif trigger_pressure <= 0.32 and support_strength in {"strong", "tight_interval_support"} and "tight" in confidence_band:
        breach_risk = "low"
    else:
        breach_risk = "moderate"
    thesis_risk = (
        "high"
        if fade_risk >= 0.62 or anomaly >= 0.8
        else "moderate"
        if persistence_score < 0.68
        else "low"
    )
    asymmetry = "downside skew" if direction == "negative" else "upside skew" if direction == "positive" else "balanced path"
    band_stability = "stable bands" if "tight" in confidence_band or "tight" in confidence_summary.lower() else "wide bands"
    pressure_label = "rising trigger pressure" if trigger_pressure >= 0.66 else "low trigger pressure" if trigger_pressure <= 0.33 else "building trigger pressure"
    confirmation_label = (
        "broad cross-asset confirmation"
        if cross_asset_confirmation_score >= 0.66
        else "partial cross-asset confirmation"
        if cross_asset_confirmation_score >= 0.42
        else "light cross-asset confirmation"
    )
    regime_label = (
        "regime-relevant path"
        if regime_alignment_score >= 0.66
        else "partly regime-relevant path"
        if regime_alignment_score >= 0.42
        else "isolated path"
    )
    persistence_label = (
        "durable follow-through"
        if persistence_score >= 0.72
        else "fragile follow-through"
        if fade_risk >= 0.62
        else "two-way follow-through"
    )
    support_label = scenario_support_strength or support_strength or "available"
    if source_class in {"policy_event", "geopolitical_news"}:
        if market_confirmation != "strong":
            note = (
                f"The headline is real, but market confirmation is still {market_confirmation}. "
                f"Forecast support is {support_label}, so the read can fade quickly if price follow-through does not broaden."
            )
        else:
            note = (
                f"Prices are confirming the event, but persistence still matters. "
                f"Forecast support is {support_label}, persistence looks {persistence_label}, and {pressure_label} with {confirmation_label}."
            )
    elif bucket in {"energy", "real_assets"}:
        note = (
            f"The move is usable, but the hedge read still needs persistence beyond the latest session. "
            f"Forecast support is {support_label}; near-term breach risk is {breach_risk}, the path shows {persistence_label}, "
            f"and it carries {pressure_label}, {confirmation_label}, and a {regime_label} with {band_stability}."
        )
    elif bucket == "duration":
        note = (
            f"The timing read is only as good as its follow-through. "
            f"Forecast support is {support_label}; near-term breach risk is {breach_risk}, the path shows {persistence_label}, "
            f"and it carries {pressure_label}, {confirmation_label}, and a {regime_label} with {band_stability}."
        )
    else:
        note = (
            f"Forecast support is {support_label}; near-term breach risk is {breach_risk}, "
            f"thesis-break risk is {thesis_risk}, and the current path carries {persistence_label}, {pressure_label}, {confirmation_label}, and a {regime_label} with {band_stability}."
        )
    return {
        "near_term_breach_risk": breach_risk,
        "thesis_break_risk": thesis_risk,
        "path_asymmetry": asymmetry,
        "band_stability": band_stability,
        "note": note,
    }


def _forecast_influence_weight(*, effect: dict[str, Any], source_context: dict[str, Any]) -> float:
    bucket = str(effect.get("primary_effect_bucket") or "market")
    source_class = str(source_context.get("source_class") or "market_series")
    base = float(_FORECAST_INFLUENCE_BY_BUCKET.get(bucket, 0.72))
    if source_class == "macro_release":
        return min(base, 0.55)
    if source_class == "policy_event":
        return min(base, 0.45)
    if source_class == "geopolitical_news":
        return min(base, 0.35)
    return base


def _forecast_intelligence_bonus(
    *,
    signal: dict[str, Any],
    effect: dict[str, Any],
    source_context: dict[str, Any],
    support_summary: Any,
    sufficiency_state: str,
    actionability_class: str,
) -> float:
    if support_summary is None or sufficiency_state in {"thin", "insufficient"}:
        return 0.0
    weight = _forecast_influence_weight(effect=effect, source_context=source_context)
    persistence_score = float(getattr(support_summary, "persistence_score", 0.0) or 0.0)
    fade_risk = float(getattr(support_summary, "fade_risk", 0.0) or 0.0)
    trigger_pressure = float(getattr(support_summary, "trigger_pressure", 0.0) or 0.0)
    regime_alignment_score = float(getattr(support_summary, "regime_alignment_score", 0.0) or 0.0)
    cross_asset_confirmation_score = float(getattr(support_summary, "cross_asset_confirmation_score", 0.0) or 0.0)
    escalation_flag = bool(getattr(support_summary, "escalation_flag", False))
    raw = (
        (persistence_score * 4.0)
        + (trigger_pressure * 3.2)
        + (regime_alignment_score * 2.4)
        + (cross_asset_confirmation_score * 2.4)
        - (fade_risk * 2.6)
        + (1.2 if escalation_flag else 0.0)
    ) * weight
    if actionability_class == "evidence_only":
        return 0.0
    if signal.get("signal_kind") == "news":
        raw = min(raw, 2.0)
    if not effect.get("mapped_sleeves") and not signal.get("affected_holdings"):
        raw = min(raw, 3.0)
    return max(-2.0, min(7.5, raw))
