from __future__ import annotations

import re
from typing import Any

from app.v2.features.daily_brief_event_cluster import build_event_cluster


_GEOPOLITICAL_TERMS = {
    "iran",
    "israel",
    "hormuz",
    "ceasefire",
    "strike",
    "war",
    "conflict",
    "missile",
    "shipping",
    "sanction",
    "oil route",
}

_POLICY_TERMS = {
    "fed",
    "ecb",
    "boj",
    "pboc",
    "treasury",
    "budget",
    "tariff",
    "fiscal",
    "policy",
    "central bank",
    "rate decision",
}


def build_source_context(
    signal_card: dict[str, Any],
    effect_profile: dict[str, Any],
    *,
    holdings_overlay_present: bool,
) -> dict[str, Any]:
    label = str(signal_card.get("label") or signal_card.get("symbol") or "Signal")
    symbol = str(signal_card.get("symbol") or "").strip().upper()
    signal_kind = str(signal_card.get("signal_kind") or "market")
    bucket = str(effect_profile.get("primary_effect_bucket") or "market")
    runtime = dict(signal_card.get("runtime_provenance") or {})
    truth_envelope = dict(signal_card.get("truth_envelope") or runtime.get("truth_envelope") or {})
    source_type = str(signal_card.get("source_type") or "").strip().lower() or None
    metric_definition = str(signal_card.get("metric_definition") or "").strip() or None
    reference_period = str(signal_card.get("reference_period") or truth_envelope.get("reference_period") or "").strip() or None
    release_date = str(signal_card.get("release_date") or truth_envelope.get("release_date") or "").strip() or None
    availability_date = str(signal_card.get("availability_date") or truth_envelope.get("availability_date") or "").strip() or None
    revision_state = str(truth_envelope.get("revision_state") or "").strip() or None
    release_semantics_state = str(truth_envelope.get("release_semantics_state") or "").strip() or None
    source_origin = (
        str(runtime.get("provider_used") or signal_card.get("provider_used") or "").strip()
        or _source_origin_from_label(label)
        or "declared source"
    )
    source_authority_tier = str(
        runtime.get("source_authority_tier")
        or runtime.get("provenance_strength")
        or signal_card.get("source_authority_tier")
        or ""
    ).strip() or None
    current_value = _safe_float(signal_card.get("current_value"))
    history = [_safe_float(value) for value in list(signal_card.get("history") or [])]
    history = [value for value in history if value is not None]
    previous_value = history[-2] if len(history) >= 2 else None
    change_pct = _change_pct(previous_value, current_value)
    magnitude = str(signal_card.get("magnitude") or "unknown")
    source_class = _source_class(signal_card, effect_profile)
    evidence_type = _evidence_type(source_class)
    significance_delta = abs(change_pct or 0.0)
    market_confirmation = _market_confirmation(source_class=source_class, change_pct=change_pct, magnitude=magnitude)
    regime_context = _regime_context(bucket=bucket)
    event_metadata = build_event_cluster(
        label=label,
        source_class=source_class,
        bucket=bucket,
        seed_metadata=signal_card,
    )
    sleeves = [_humanize_sleeve(item) for item in list(effect_profile.get("mapped_sleeves") or [])]
    affected_candidates = [str(item).strip().upper() for item in list(signal_card.get("affected_candidates") or []) if str(item).strip()]
    event_status = _event_status(source_class=source_class, label=label, market_confirmation=market_confirmation)
    priced_state = _priced_state(source_class=source_class, market_confirmation=market_confirmation, change_pct=change_pct)
    source_support_map = {
        "macro": [source_origin, evidence_type, regime_context, metric_definition or reference_period or release_date],
        "micro": [
            ", ".join(sleeves) or "portfolio context",
            "holdings overlay active" if holdings_overlay_present else "sleeve-level consequence",
            ", ".join(affected_candidates[:3]) or "implementation set not yet narrowed",
        ],
        "short_term": [str(signal_card.get("confirms") or "").strip(), event_status],
        "long_term": [str(signal_card.get("breaks") or "").strip(), priced_state],
    }
    return {
        "source_class": source_class,
        "source_type": source_type,
        "source_origin": source_origin,
        "what_changed": _what_changed(
            label=label,
            current_value=current_value,
            previous_value=previous_value,
            change_pct=change_pct,
            magnitude=magnitude,
            source_class=source_class,
            source_type=source_type,
            reference_period=reference_period,
            release_date=release_date,
            metric_definition=metric_definition,
        ),
        "previous_state": previous_value,
        "current_state": current_value,
        "change_pct": change_pct,
        "significance_delta": significance_delta,
        "market_confirmation": market_confirmation,
        "regime_context": regime_context,
        "sleeve_exposure_path": sleeves,
        "affected_candidates": affected_candidates,
        "confidence_class": signal_card.get("confidence_class"),
        "evidence_type": evidence_type,
        "event_status": event_status,
        "priced_state": priced_state,
        "has_holdings_overlay": holdings_overlay_present,
        "source_support_map": source_support_map,
        "metric_definition": metric_definition,
        "reference_period": reference_period,
        "release_date": release_date,
        "availability_date": availability_date,
        "revision_state": revision_state,
        "release_semantics_state": release_semantics_state,
        "source_authority_tier": source_authority_tier,
        "novelty_class": str(signal_card.get("novelty_class") or "").strip().lower() or None,
        "reactivation_reason": str(signal_card.get("reactivation_reason") or "").strip() or None,
        "threshold_state": str(signal_card.get("threshold_state") or "").strip().lower() or None,
        "freshness_age_days": _safe_float(signal_card.get("freshness_age_days")),
        "freshness_relevance_score": _safe_float(signal_card.get("freshness_relevance_score")),
        "lead_lane": str(signal_card.get("lead_lane") or "").strip().lower() or None,
        "current_action_delta": str(signal_card.get("current_action_delta") or "").strip() or None,
        "portfolio_read_delta": str(signal_card.get("portfolio_read_delta") or "").strip() or None,
        "symbol": symbol,
        "bucket": bucket,
        **event_metadata,
    }


def build_decision_explanation(
    signal_card: dict[str, Any],
    effect_profile: dict[str, Any],
    *,
    holdings_overlay_present: bool,
    source_context: dict[str, Any] | None = None,
    support: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = source_context or build_source_context(
        signal_card,
        effect_profile,
        holdings_overlay_present=holdings_overlay_present,
    )
    label = str(signal_card.get("label") or signal_card.get("symbol") or "Signal")
    bucket = str(effect_profile.get("primary_effect_bucket") or "market")
    sleeves = list(effect_profile.get("mapped_sleeves") or [])
    holdings = list(signal_card.get("affected_holdings") or [])
    candidates = list(signal_card.get("affected_candidates") or [])

    clauses = {
        "what_changed_today": _what_changed_today_clause(label=label, context=context),
        "why_now_not_before": _why_now_not_before_clause(label=label, context=context),
        "evidence_class": _evidence_class_clause(label=label, bucket=bucket, context=context, support=support),
        "why_it_matters_macro": _macro_clause(label=label, bucket=bucket, context=context),
        "why_it_matters_micro": _micro_clause(
            label=label,
            bucket=bucket,
            sleeves=sleeves,
            holdings=holdings,
            candidates=candidates,
            context=context,
        ),
        "why_it_matters_short_term": _short_term_clause(
            label=label,
            bucket=bucket,
            sleeves=sleeves,
            holdings=holdings,
            context=context,
        ),
        "why_it_matters_long_term": _long_term_clause(
            label=label,
            bucket=bucket,
            sleeves=sleeves,
            context=context,
        ),
        "overread_reason": _overread_clause(
            label=label,
            bucket=bucket,
            holdings=holdings,
            context=context,
        ),
        "why_this_could_be_wrong": _why_this_could_be_wrong_clause(
            label=label,
            bucket=bucket,
            context=context,
        ),
        "implementation_sensitivity": _implementation_sensitivity_clause(
            label=label,
            bucket=bucket,
            sleeves=sleeves,
            holdings=holdings,
            candidates=candidates,
            context=context,
        ),
        "portfolio_consequence": _decision_frame(
            bucket=bucket,
            sleeves=sleeves,
            holdings=holdings,
            candidates=candidates,
            context=context,
        ),
    }
    clauses = _anti_repetition_pass(clauses, context=context, bucket=bucket, sleeves=sleeves, holdings=holdings)
    show_what_changed = _should_render_what_changed(context=context)
    why_it_matters_economically = _why_it_matters_economically_block(
        macro=clauses["why_it_matters_macro"],
        short_term=clauses["why_it_matters_short_term"],
        why_now=clauses["why_now_not_before"],
        context=context,
    )
    portfolio_and_sleeve_meaning = _portfolio_and_sleeve_meaning_block(
        micro=clauses["why_it_matters_micro"],
        portfolio_consequence=clauses["portfolio_consequence"],
        implementation_sensitivity=clauses["implementation_sensitivity"],
        overread_reason=clauses["overread_reason"],
        context=context,
    )
    market_confirmation = _market_confirmation_block(context=context)
    scenario_support = _scenario_support_block(support=support, context=context)
    what_changed_block = _what_changed_block(label=label, context=context) if show_what_changed else None
    return {
        **clauses,
        "what_changed": what_changed_block,
        "event_context_delta": what_changed_block if market_confirmation else None,
        "why_it_matters_economically": why_it_matters_economically,
        "why_it_matters": why_it_matters_economically,
        "portfolio_and_sleeve_meaning": portfolio_and_sleeve_meaning,
        "portfolio_meaning": portfolio_and_sleeve_meaning,
        "confirm_condition": _confirm_condition(label=label, bucket=bucket, context=context),
        "weaken_condition": _weaken_condition(label=label, bucket=bucket, context=context),
        "break_condition": _break_condition(label=label, bucket=bucket, context=context),
        "scenario_support": scenario_support,
        "source_and_validity": _source_and_validity_block(context=context),
        "market_confirmation": market_confirmation,
        "news_to_market_confirmation": _news_to_market_confirmation_block(context=context),
        "implementation_set": list(context.get("affected_candidates") or []) or None,
    }


def _macro_clause(*, label: str, bucket: str, context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    market_confirmation = str(context.get("market_confirmation") or "limited")
    novelty_class = str(context.get("novelty_class") or "")
    reactivation_reason = str(context.get("reactivation_reason") or "").strip()
    lead_lane = str(context.get("lead_lane") or "")
    if lead_lane == "regime_context":
        return f"{label} is still part of the broader backdrop, but it is not the fresh driver of today’s brief."
    if novelty_class == "reactivated" and reactivation_reason:
        return f"{label} matters again because {reactivation_reason[0].lower() + reactivation_reason[1:]}. The key question now is whether related markets start moving the same way."
    if source_class == "macro_release":
        if bucket == "inflation":
            return "The release resets the inflation outlook behind rate expectations, real yields, and margin pressure. If that shift is real, rates, inflation markets, and growth-sensitive stocks should start moving the same way."
        if bucket in {"duration", "policy"}:
            return "The official reading changes financing conditions and what investors will pay for risk more than it changes a single print. If it matters beyond the release, rates, credit, and rate-sensitive stocks should all respect the same tighter backdrop."
        if bucket == "credit":
            return "The official spread reading changes the funding baseline and how much extra return investors demand for risk. If the move is more than a one-off print, breadth should stay narrow and bond relief should stay limited."
        return "The latest official reading changes the baseline investors are using for the next policy and growth decision. What matters now is whether nearby markets confirm the same read or brush it off."
    if source_class in {"policy_event", "geopolitical_news"}:
        if bucket == "energy":
            return f"The event matters only if it lifts energy costs, shipping costs, and higher prices in other markets. Oil, FX, rates, or volatility need to confirm that the headline is becoming a real market channel; right now market confirmation is {market_confirmation}."
        if bucket in {"policy", "duration", "credit"}:
            return f"The event matters only if it changes rate expectations, financing pressure, or funding pressure. Rates, credit, and the dollar need to confirm that the headline is becoming a market channel; right now confirmation is {market_confirmation}."
        return f"The event matters only if the market reaction turns the headline into a lasting signal for growth, inflation, or risk appetite. Right now confirmation is {market_confirmation}."
    if bucket == "duration" and label.lower() == "30y mortgage":
        return "30Y Mortgage tracks financing conditions more than policy itself. When it stays firm, buying longer bonds stays harder to justify, refinancing relief stays limited, and rate-sensitive assets get less support."
    if bucket == "duration":
        return "Rates shape borrowing costs and what investors are willing to pay for bonds and stocks. When rates stay high, price support weakens and financing-sensitive assets get less relief."
    if bucket == "credit":
        return "Credit spreads show how much extra return investors want for taking risk. When they widen, owning lower-quality credit and equities gets harder to justify because funding conditions are getting tighter."
    if bucket == "dollar_fx":
        return "Dollar moves change foreign earnings, imported prices, and the hurdle for global risk. A firmer dollar can tighten conditions even when headline risk assets are trying to rally."
    if bucket == "energy":
        return "Energy prices can push higher prices into the rest of the market through fuel costs, input costs, and geopolitics. If the move persists, inflation worries and demand for protection can both rise."
    if bucket == "real_assets":
        return "Real-assets performance changes demand for protection when inflation or market stress stays high. The read matters most when other inflation-sensitive markets start moving the same way."
    if bucket == "growth":
        return "Equity leadership changes the market's read on breadth, growth leadership, and broad risk tolerance. To matter beyond the headline tape, credit, rates, and FX should stop resisting the move."
    if bucket == "volatility":
        return "Volatility changes how fragile the market mood looks, not just the price of one hedge. If the stress is real, breadth, carry, and risk appetite should all get weaker."
    return f"The latest move in {label} changes the macro backdrop investors are using to set capital priorities."


def _micro_clause(
    *,
    label: str,
    bucket: str,
    sleeves: list[str],
    holdings: list[str],
    candidates: list[str],
    context: dict[str, Any],
) -> str:
    sleeve_text = _sleeve_text(sleeves)
    source_class = str(context.get("source_class") or "market_series")
    candidate_suffix = (
        f" The main ETF choices are {', '.join(candidates[:3])} if the sleeve read strengthens."
        if candidates and not holdings
        else ""
    )
    if holdings:
        return f"The mapped holdings are now in scope, so judge {label} first through {', '.join(holdings[:2])} rather than through the market in general."
    if source_class in {"policy_event", "geopolitical_news"}:
        return (
            f"For {sleeve_text}, the question is not the headline itself. The question is whether market confirmation "
            f"is strong enough to change sleeve timing or make funding pressure matter more.{candidate_suffix}"
        )
    if bucket == "duration" and label.lower() == "30y mortgage":
        return (
            f"For {sleeve_text}, firmer mortgage conditions argue against treating the bond sleeve as ready for adding much more long bond exposure."
            f"{candidate_suffix}"
        )
    if bucket == "inflation":
        return f"For {sleeve_text}, the release changes whether real assets or bonds are the better response to higher inflation.{candidate_suffix}"
    if bucket == "credit":
        return f"For {sleeve_text}, spread direction changes whether investors are being paid enough to stay exposed.{candidate_suffix}"
    if bucket == "dollar_fx":
        return f"For {sleeve_text}, the dollar read changes the hurdle for non-domestic risk and the case for staying closer to cash.{candidate_suffix}"
    if bucket == "energy":
        return f"For {sleeve_text}, crude is part of the sleeve decision, not a stand-alone trade. It matters only if the energy move strengthens the case for inflation protection.{candidate_suffix}"
    if bucket == "real_assets":
        return f"For {sleeve_text}, the move changes the case for inflation protection and diversification rather than forcing a holdings instruction.{candidate_suffix}"
    if bucket == "growth":
        return f"For {sleeve_text}, the move changes which kind of equity risk the market is rewarding rather than whether risk should be added broadly.{candidate_suffix}"
    return f"For {sleeve_text}, keep the read at sleeve level until the signal clearly changes timing.{candidate_suffix}"


def _short_term_clause(
    *,
    label: str,
    bucket: str,
    sleeves: list[str],
    holdings: list[str],
    context: dict[str, Any],
) -> str:
    sleeve_text = _sleeve_text(sleeves)
    source_class = str(context.get("source_class") or "market_series")
    market_confirmation = str(context.get("market_confirmation") or "limited")
    lead_lane = str(context.get("lead_lane") or "")
    novelty_class = str(context.get("novelty_class") or "")
    reactivation_reason = str(context.get("reactivation_reason") or "").strip()
    current_action_delta = str(context.get("current_action_delta") or "").strip()
    if holdings:
        return f"Near term, review the mapped holdings first. The decision frame is specific enough to inspect positions before broad sleeve changes."
    if lead_lane == "regime_context":
        return f"Today this stays in background context for {sleeve_text}. It informs the frame, but it does not reopen a fresh sleeve decision by itself."
    if novelty_class == "reactivated" and reactivation_reason:
        return f"Near term, this older theme matters again because {reactivation_reason[0].lower() + reactivation_reason[1:]} {current_action_delta or ''}".strip()
    if source_class in {"policy_event", "geopolitical_news"}:
        return f"Near term, keep this on watch until price confirmation lasts. A developing event with {market_confirmation} confirmation is not enough for a fresh sleeve move."
    if bucket == "duration" and label.lower() == "30y mortgage":
        return f"Near term, the bond decision still depends on whether financing conditions stop tightening. Treat this as a timing check, not a policy verdict."
    if bucket == "inflation":
        return f"Near term, decide whether inflation pressure is strong enough to keep bonds under pressure and real assets supported."
    if bucket == "credit":
        return f"Near term, watch whether spreads keep moving in the same direction. That decides whether the bond sleeve read changes or merely stays cautious."
    if bucket == "dollar_fx":
        return f"Near term, size global risk against the dollar hurdle instead of chasing one session."
    if bucket == "energy":
        return f"Near term, use {label} to judge whether real assets still deserve space in the brief or whether inflation pressure is fading."
    return f"Near term, treat {label} as a guide for {sleeve_text}, not as a stand-alone move."


def _long_term_clause(
    *,
    label: str,
    bucket: str,
    sleeves: list[str],
    context: dict[str, Any],
) -> str:
    sleeve_text = _sleeve_text(sleeves)
    source_class = str(context.get("source_class") or "market_series")
    lead_lane = str(context.get("lead_lane") or "")
    novelty_class = str(context.get("novelty_class") or "")
    if lead_lane == "regime_context" or novelty_class == "continuation":
        return f"Longer term, this stays part of the broader backdrop for {sleeve_text}; it only moves back to the front of the brief if fresh evidence brings it back."
    if source_class in {"policy_event", "geopolitical_news"}:
        return f"Longer term, this matters only if the event keeps showing up in prices, funding conditions, or rate expectations. Without that, it stays background context."
    if bucket == "duration" and label.lower() == "30y mortgage":
        return f"If mortgage pressure persists, the bond sleeve stays slower to add longer bonds instead of reopening a clean extension trade."
    if bucket == "inflation":
        return f"If inflation stays sticky, the long-term sleeve mix leans away from easy bond adds and toward stronger inflation protection."
    if bucket == "credit":
        return f"If spreads keep moving the same way, the long-term case for {sleeve_text} changes because investors are no longer being paid for risk the same way."
    if bucket == "dollar_fx":
        return f"If the dollar move persists, the long-term hurdle for global risk and foreign earnings stays materially higher."
    if bucket == "energy":
        return "Only if the energy move persists and broader inflation markets confirm it does it strengthen the long-run case for real-assets protection."
    if bucket == "real_assets":
        return f"Only sustained cross-asset confirmation would make this a longer-lasting upgrade for {sleeve_text} rather than a short-term protection signal."
    if bucket == "growth":
        return f"Only persistent breadth and cross-market confirmation would turn this into a longer-lasting change in the equity read."
    return f"If this persists, it changes the strategic pacing for {sleeve_text}."


def _overread_clause(*, label: str, bucket: str, holdings: list[str], context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    novelty_class = str(context.get("novelty_class") or "")
    lead_lane = str(context.get("lead_lane") or "")
    if holdings:
        return f"Even with mapped holdings, {label} is not enough by itself to justify a holdings-level change. It still needs confirmation from related markets before it becomes something to act on."
    if lead_lane == "regime_context" or novelty_class == "continuation":
        return f"{label} is not a fresh daily driver. It does not justify a new portfolio move unless fresh confirmation or a clear break pulls it back into the active brief."
    if source_class in {"policy_event", "geopolitical_news"}:
        return f"A headline in {label} is not yet enough for a portfolio change. It still needs confirmation across the markets most exposed to the event before it becomes something to act on."
    if bucket == "duration" and label.lower() == "30y mortgage":
        return "One mortgage reading is not enough to justify buying a lot more long bonds or cutting risk broadly. It still needs confirmation from yields, credit, and inflation-sensitive markets."
    if bucket == "duration":
        return "This rate move is not enough by itself to justify buying a lot more long bonds or pulling back broadly on equities. It still needs confirmation from credit, inflation, and other markets under rate pressure."
    if bucket == "credit":
        return "One official spread print does not establish a lasting shift in funding conditions. It is not enough by itself to justify a broad equity add or a full defensive reset. It still needs confirmation from breadth, rates, and credit."
    if bucket == "dollar_fx":
        return "One dollar move does not settle the global risk read or justify a broad ex-US repositioning. It still needs confirmation from rates, earnings-sensitive stocks, and broader liquidity conditions."
    if bucket == "energy":
        return f"This move is not enough by itself to justify a stand-alone commodity trade or rebuilding protection across the portfolio. It still needs persistence and confirmation from inflation pricing, real yields, and other real-assets signals."
    if bucket == "real_assets":
        return "One protection-friendly move does not by itself justify a lasting real-assets overweight. It still needs persistence and broader inflation-sensitive confirmation."
    if bucket == "growth":
        return f"One equity move does not establish a lasting risk-on read or justify a broad equity add. It still needs breadth, credit, and rates to stop resisting the move."
    return f"{label} can change the brief without proving a full allocation shift. It still needs broader confirmation before it justifies a larger portfolio move."


def _decision_frame(*, bucket: str, sleeves: list[str], holdings: list[str], candidates: list[str], context: dict[str, Any]) -> str:
    sleeve_text = _sleeve_text(sleeves)
    source_class = str(context.get("source_class") or "market_series")
    lead_lane = str(context.get("lead_lane") or "")
    action_delta = str(context.get("current_action_delta") or "").strip()
    portfolio_delta = str(context.get("portfolio_read_delta") or "").strip()
    candidate_suffix = (
        f" The main ETF choices are {', '.join(candidates[:3])} if the sleeve consequence strengthens."
        if candidates and not holdings
        else ""
    )
    if holdings:
        return f"Keep the consequence at holding level only if the mapped names keep confirming the current read. Otherwise step back to sleeve review."
    if lead_lane == "regime_context":
        return f"Keep this in broader context. It still informs {portfolio_delta or sleeve_text}, but it is not changing today’s action frame.{candidate_suffix}"
    if source_class in {"policy_event", "geopolitical_news"}:
        return f"Keep this at {sleeve_text} context level until price confirmation lasts.{candidate_suffix}"
    if bucket == "duration":
        return f"Keep this at bond-sleeve timing level. This is about whether conditions for adding longer bonds are opening up or staying tight.{candidate_suffix}"
    if bucket == "inflation":
        return f"Keep this at the bond-versus-real-assets sleeve decision. The signal changes which protection gets priority, not one specific ticker.{candidate_suffix}"
    if bucket == "credit":
        return f"Keep this at bond-sleeve and funding level. This changes the risk-versus-reward trade-off, not one direct trade.{candidate_suffix}"
    if bucket == "dollar_fx":
        return f"Keep this at cross-border risk and cash level. This changes the hurdle for global risk, not a direct trade by itself.{candidate_suffix}"
    if bucket in {"energy", "real_assets"}:
        return f"Keep this at real-assets sleeve level. This changes the case for inflation protection more than it authorizes a holdings switch.{candidate_suffix}"
    return f"Keep this at {sleeve_text} level until the timing becomes clearer. {action_delta or ''}{candidate_suffix}".strip()


def _what_changed_today_clause(*, label: str, context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    source_type = str(context.get("source_type") or "").strip().lower()
    current_value = _safe_float(context.get("current_state"))
    previous_value = _safe_float(context.get("previous_state"))
    change_pct = _safe_float(context.get("change_pct"))
    release_date = str(context.get("release_date") or "").strip()
    novelty_class = str(context.get("novelty_class") or "").strip().lower()
    market_confirmation = str(context.get("market_confirmation") or "limited")
    what_changed = str(context.get("what_changed") or label).strip()

    if source_class == "macro_release":
        if current_value is not None and previous_value is not None:
            return f"The latest official reading printed {current_value:.2f} versus {previous_value:.2f} on the prior release."
        return what_changed
    if source_class in {"policy_event", "geopolitical_news"}:
        freshness_note = "today" if release_date else "in the current brief window"
        return f"A fresh headline entered the brief {freshness_note}; market confirmation is currently {market_confirmation}."
    if change_pct is not None:
        direction = "higher" if change_pct > 0 else "lower" if change_pct < 0 else "flat"
        session_word = "closed" if source_type == "market_close" else "printed"
        comparator = "prior close" if source_type == "market_close" else "prior reading"
        sentence = f"{label} {session_word} {abs(change_pct):.2f}% {direction} versus the {comparator}."
        if novelty_class == "threshold_break":
            sentence += " That move crossed a watched threshold."
        elif novelty_class == "escalation":
            sentence += " The move is large enough to reopen the daily read."
        elif novelty_class == "reversal":
            sentence += " The direction flipped the previous read."
        return sentence
    if current_value is not None:
        return f"{label} is currently reading {current_value:.2f} in the latest valid observation."
    return what_changed


def _what_changed_block(*, label: str, context: dict[str, Any]) -> str:
    base = _what_changed_today_clause(label=label, context=context)
    source_class = str(context.get("source_class") or "market_series")
    freshness_label = _freshness_label(context)
    if source_class == "macro_release":
        return f"{base} This is the {freshness_label.lower()}."
    if source_class in {"policy_event", "geopolitical_news"}:
        confirmation = str(context.get("market_confirmation") or "limited")
        return f"{base} Market confirmation is still {confirmation}."
    return f"{base} This is the {freshness_label.lower()}."


def _why_now_not_before_clause(*, label: str, context: dict[str, Any]) -> str:
    novelty_class = str(context.get("novelty_class") or "").strip().lower()
    reactivation_reason = str(context.get("reactivation_reason") or "").strip()
    threshold_state = str(context.get("threshold_state") or "").strip().lower()
    lead_lane = str(context.get("lead_lane") or "").strip().lower()
    action_delta = str(context.get("current_action_delta") or "").strip()
    freshness_age_days = _safe_float(context.get("freshness_age_days"))

    if lead_lane == "regime_context":
        return f"{label} is still visible because it shapes the backdrop, but it did not generate a fresh decision change today."
    if novelty_class == "threshold_break":
        return "It surfaces now because a watched threshold changed the daily decision map."
    if novelty_class == "reactivated" and reactivation_reason:
        return f"It matters again today because {reactivation_reason[0].lower() + reactivation_reason[1:]}"
    if novelty_class == "escalation":
        return f"It matters now because the latest move escalated enough to change today’s hurdle. {action_delta}".strip()
    if novelty_class == "reversal":
        return "It matters now because the latest move reversed the previous read and reopened the brief."
    if novelty_class == "new":
        return "It matters now because this is a fresh input in the current brief window."
    if threshold_state == "watch":
        return "It is still visible because the signal is leaning on a watched level, even without a clean break."
    if freshness_age_days is not None and freshness_age_days > 1.0:
        return "It remains in view mainly as context, not because it generated a fresh catalyst today."
    return f"It remains in the brief because it still changes today’s timing and pacing. {action_delta}".strip()


def _evidence_class_clause(
    *,
    label: str,
    bucket: str,
    context: dict[str, Any],
    support: dict[str, Any] | None,
) -> str:
    source_class = str(context.get("source_class") or "market_series")
    market_confirmation = str(context.get("market_confirmation") or "limited")
    support_strength = _support_strength(support)
    if source_class == "macro_release":
        if market_confirmation == "strong":
            return "Official release with visible market follow-through; the portfolio consequence is still an inference, not a direct instruction."
        return "Official release first; the portfolio consequence still depends on whether markets keep confirming it."
    if source_class in {"policy_event", "geopolitical_news"}:
        if market_confirmation == "strong":
            return "Reported event with clear price follow-through; the portfolio consequence still needs persistence."
        if market_confirmation == "moderate":
            return "Reported event with partial market confirmation; the portfolio consequence remains provisional."
        return "Reported event only; the portfolio consequence stays tentative until prices confirm it."
    if source_class == "benchmark_move":
        return "Benchmark move with inferred portfolio meaning; use it as confirmation, not as stand-alone proof."
    forecast_note = (
        " Forecast support is constructive."
        if support_strength in {"strong", "tight_interval_support"}
        else " Forecast support is bounded."
        if support_strength
        else ""
    )
    if bucket in {"energy", "real_assets", "dollar_fx", "growth", "credit", "duration", "inflation"}:
        return (
            "Direct market move with usable price confirmation; the portfolio consequence remains a sleeve-level inference."
            f"{forecast_note}"
        ).strip()
    return f"Direct market move in {label} with usable confirmation; the portfolio consequence remains bounded.{forecast_note}".strip()


def _why_this_could_be_wrong_clause(*, label: str, bucket: str, context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    market_confirmation = str(context.get("market_confirmation") or "limited")
    if source_class in {"policy_event", "geopolitical_news"}:
        if market_confirmation == "strong":
            return "The headline can still fade if price follow-through does not persist beyond the initial reaction."
        return "The interpretation can fail quickly if the headline never broadens into durable price confirmation."
    if bucket == "duration" and label.lower() == "30y mortgage":
        return "Mortgage rates can stay sticky for spread or technical reasons even if the broader duration backdrop is improving."
    if bucket == "inflation":
        return "One release can overstate the trend if later prints and rate pricing do not confirm it."
    if bucket == "credit":
        return "One session of spread relief does not guarantee easier financing conditions or durable risk appetite."
    if bucket == "dollar_fx":
        return "Dollar strength can fade quickly if rate expectations and broader risk tone stop confirming it."
    if bucket == "energy":
        return f"{label} can mean-revert quickly; without confirmation from inflation pricing, real yields, or broader risk assets, the hedge read can fade."
    if bucket == "real_assets":
        return "The hedge case can weaken if cross-asset confirmation never extends beyond the initial move."
    if bucket == "growth":
        return "A short-lived leadership move can fade before it becomes a durable signal for equity allocation."
    return f"The current read can fail if {label} does not get follow-through or threshold confirmation."


def _implementation_sensitivity_clause(
    *,
    label: str,
    bucket: str,
    sleeves: list[str],
    holdings: list[str],
    candidates: list[str],
    context: dict[str, Any],
) -> str:
    sleeve_text = _sleeve_text(sleeves)
    if holdings:
        return "Implementation is holding-led here: review mapped positions before changing broader sleeve posture."
    candidate_note = f" Watchlist sensitivity is highest in {', '.join(candidates[:3])} if the sleeve case keeps confirming." if candidates else ""
    if bucket == "duration" and label.lower() == "30y mortgage":
        return f"This is mainly a timing and pacing signal for {sleeve_text}, not a binary allocation switch.{candidate_note}"
    if bucket == "inflation":
        return f"Implementation changes the bond-versus-real-assets mix before it changes security selection.{candidate_note}"
    if bucket == "credit":
        return f"Sizing and funding discipline matter more than swapping instruments on a single spread move.{candidate_note}"
    if bucket == "dollar_fx":
        return f"Use it to set the hurdle for non-domestic risk and cash deployment, not as a direct FX trade.{candidate_note}"
    if bucket in {"energy", "real_assets"}:
        return f"Treat this as hedge timing and sequencing, not as a stand-alone commodity call.{candidate_note}"
    if candidates:
        return f"Timing and sequencing matter most here.{candidate_note}"
    return f"Keep implementation at {sleeve_text} pacing level until confirmation strengthens."


def _why_it_matters_block(*, macro: str, micro: str) -> str:
    parts = [part.strip() for part in [macro, micro] if str(part).strip()]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if _similarity(parts[0], parts[1]) >= 0.72:
        return parts[0]
    return f"{parts[0]} {parts[1]}"


def _portfolio_meaning_block(*, portfolio_consequence: str, implementation_sensitivity: str) -> str:
    primary = str(portfolio_consequence or "").strip()
    implementation = str(implementation_sensitivity or "").strip()
    if primary and implementation and _similarity(primary, implementation) < 0.75:
        return f"{primary} {implementation}"
    return primary or implementation


def _should_render_what_changed(*, context: dict[str, Any]) -> bool:
    source_class = str(context.get("source_class") or "market_series")
    novelty_class = str(context.get("novelty_class") or "").strip().lower()
    revision_state = str(context.get("revision_state") or "").strip().lower()
    release_semantics_state = str(context.get("release_semantics_state") or "").strip().lower()
    standard_revision_states = {"none", "unrevised", "unknown", "latest_observation", "current_observation"}
    standard_release_semantics = {"normal", "standard", "current", "fred_latest", "latest"}
    if source_class in {"policy_event", "geopolitical_news"}:
        return True
    if source_class == "macro_release":
        if revision_state and revision_state not in standard_revision_states:
            return True
        if release_semantics_state and release_semantics_state not in standard_release_semantics:
            return True
        if novelty_class in {"threshold_break", "reactivated", "reversal"}:
            return True
        return False
    return False


def _why_it_matters_economically_block(
    *,
    macro: str,
    short_term: str,
    why_now: str,
    context: dict[str, Any],
) -> str:
    parts = [
        _economic_mechanism_line(context=context),
        _economic_why_line(context=context),
        _economic_confirmation_line(context=context),
        _economic_effect_line(context=context),
    ]
    cleaned = [part for part in parts if str(part or "").strip()]
    return _dedupe_sentences(" ".join(cleaned))


def _economic_mechanism_line(*, context: dict[str, Any]) -> str:
    bucket = str(context.get("bucket") or "market")
    source_class = str(context.get("source_class") or "market_series")
    if source_class in {"policy_event", "geopolitical_news"}:
        return "The mechanism here is whether the headline starts moving actual market prices rather than staying in the news flow."
    if bucket == "duration" and str(context.get("symbol") or "").upper() == "MORTGAGE30US":
        return "Financing costs are still the main mechanism moving here, not just one rate print."
    return {
        "duration": "Rate pressure is still the mechanism moving here.",
        "credit": "Funding conditions are still moving through the credit market.",
        "dollar_fx": "The dollar is still acting as the main hurdle for global risk.",
        "growth": "Breadth and leadership are still the mechanism that decides whether this equity move is broadening.",
        "inflation": "Inflation pressure is still feeding through rates and real returns.",
        "energy": "Oil is still the main channel that can keep inflation pressure alive.",
        "real_assets": "Protection demand is still the channel keeping real assets relevant.",
        "volatility": "Risk hedging demand is still the mechanism showing how defensive the market feels.",
    }.get(bucket, "The market mechanism is still changing here, not just the headline level.")


def _economic_why_line(*, context: dict[str, Any]) -> str:
    bucket = str(context.get("bucket") or "market")
    source_class = str(context.get("source_class") or "market_series")
    market_confirmation = str(context.get("market_confirmation") or "limited")
    if source_class in {"policy_event", "geopolitical_news"}:
        return (
            "That matters economically only if the headline starts changing energy costs, funding conditions, or risk pricing. "
            f"Right now market confirmation is still {market_confirmation}."
        )
    if bucket == "duration" and str(context.get("symbol") or "").upper() == "MORTGAGE30US":
        return "When mortgage rates stay firm, financing relief stays limited and longer-duration assets get less help."
    return {
        "duration": "Higher rates keep financing conditions tight and keep the bar high for duration and valuation-sensitive assets.",
        "credit": "Wider spreads raise the price of risk and limit how much carry or equity exposure the market can absorb comfortably.",
        "dollar_fx": "A firmer dollar tightens conditions for global risk, makes foreign earnings translation harder, and keeps cross-border exposure under pressure.",
        "growth": "If breadth stays narrow, the move is still a headline rally rather than a durable broadening in risk appetite.",
        "inflation": "Sticky inflation keeps rates and real yields from easing cleanly and keeps inflation protection relevant.",
        "energy": "Higher oil can keep inflation worries alive and extend demand for portfolio protection.",
        "real_assets": "Stronger protection demand keeps inflation-sensitive assets relevant and delays easy relief elsewhere in the portfolio.",
        "volatility": "Higher stress makes investors pay more for protection and take less balance-sheet risk elsewhere.",
    }.get(bucket, "That matters economically because it changes the backdrop investors are using to price risk.")


def _economic_confirmation_line(*, context: dict[str, Any]) -> str:
    bucket = str(context.get("bucket") or "market")
    source_class = str(context.get("source_class") or "market_series")
    market_confirmation = str(context.get("market_confirmation") or "limited")
    if source_class in {"policy_event", "geopolitical_news"}:
        return (
            "If the read is real, oil, rates, volatility, breadth, or the dollar should start carrying the same message. "
            f"Right now confirmation remains {market_confirmation}."
        )
    return {
        "duration": "If the read is real, credit, equity sensitivity, and real yields should keep respecting the same higher hurdle.",
        "credit": "If the read is real, breadth should stay narrow, rates should not ease cleanly, and lower-quality risk should keep struggling.",
        "dollar_fx": "If the read is real, real yields, ex-US risk assets, and commodities should keep moving in line with the dollar.",
        "growth": "If the read is real, breadth should broaden and credit, rates, and FX should stop leaning against the move.",
        "inflation": "If the read is real, rates, real yields, and inflation-sensitive assets should keep moving the same way.",
        "energy": "If the read is real, inflation markets, the dollar, and hedge proxies should keep reflecting the same pressure.",
        "real_assets": "If the read is real, inflation-sensitive markets and hedge proxies should keep confirming the move.",
        "volatility": "If the read is real, breadth, carry, and demand for protection should all keep turning more defensive.",
    }.get(bucket, "If the read is real, nearby markets should start confirming the same message.")


def _economic_follow_through_line(*, context: dict[str, Any]) -> str | None:
    bucket = str(context.get("bucket") or "market")
    source_class = str(context.get("source_class") or "market_series")
    market_confirmation = str(context.get("market_confirmation") or "limited")
    if source_class in {"policy_event", "geopolitical_news"}:
        return (
            "This only becomes an economic signal if exposed markets start moving with the headline. "
            f"Right now that market follow-through is only {market_confirmation}."
        )
    if bucket == "duration":
        return "If this is real, credit and stock valuations should keep acting like rates are still a hurdle."
    if bucket == "credit":
        return "If this is real, stock breadth should stay narrow and easier rate conditions should stay out of reach."
    if bucket == "dollar_fx":
        return "If this is real, real yields, non-US risk assets, and commodities should keep moving in line with the dollar."
    if bucket == "growth":
        return "If this is real, breadth should widen, leadership should hold, and credit, rates, and FX should stop pushing back."
    if bucket == "inflation":
        return "If this is real, real yields, inflation-sensitive assets, and demand for inflation protection should keep moving the same way."
    if bucket in {"energy", "real_assets"}:
        return "If this is real, inflation markets, the dollar, and demand for protection should keep backing it up."
    if bucket == "volatility":
        return "If this is real, breadth, carry, and demand for safer assets should all start acting more defensive."
    return "If this is real, related markets should start confirming the same message instead of brushing it off."


def _economic_freshness_line(*, context: dict[str, Any]) -> str | None:
    source_class = str(context.get("source_class") or "market_series")
    freshness_age_days = _safe_float(context.get("freshness_age_days"))
    market_confirmation = str(context.get("market_confirmation") or "limited")
    if source_class == "macro_release":
        return "It matters now because this is the latest official baseline feeding the current brief rather than a stale backdrop print."
    if source_class in {"policy_event", "geopolitical_news"}:
        return f"It matters now because the headline is fresh, but its economic meaning still depends on {market_confirmation} market confirmation."
    if freshness_age_days is not None and freshness_age_days <= 1.0:
        return "It matters now because the move is recent enough to test whether the market is broadening the signal or fading it."
    if freshness_age_days is not None and freshness_age_days > 1.0:
        return "It remains relevant only if related markets keep carrying the same message despite the older observation."
    return None


def _economic_effect_line(*, context: dict[str, Any]) -> str | None:
    bucket = str(context.get("bucket") or "market")
    source_class = str(context.get("source_class") or "market_series")
    if source_class in {"policy_event", "geopolitical_news"}:
        return "If this holds, the headline becomes a real market issue instead of just a news event."
    if bucket == "duration":
        return "If this holds, adding more long bonds stays harder to justify than staying with safer bond holdings."
    if bucket == "credit":
        return "If this holds, safer ballast stays easier to justify than adding more risk."
    if bucket == "dollar_fx":
        return "If this holds, broad global risk adds stay harder to justify."
    if bucket == "growth":
        return "If this holds, broader equity adds become more credible."
    if bucket == "inflation":
        return "If this holds, inflation protection stays more relevant than easy bond adds."
    if bucket in {"energy", "real_assets"}:
        return "If this holds, inflation protection stays relevant longer than a quick return to easier bond conditions."
    if bucket == "volatility":
        return "If this holds, safer ballast stays more useful than aggressive risk adds."
    return "If this holds, the current market read becomes more credible than a quick reversal."


def _portfolio_and_sleeve_meaning_block(
    *,
    micro: str,
    portfolio_consequence: str,
    implementation_sensitivity: str,
    overread_reason: str,
    context: dict[str, Any],
) -> str:
    source_class = str(context.get("source_class") or "market_series")
    bucket = str(context.get("bucket") or "market")
    sleeve_text = _joined_sleeve_text(list(context.get("sleeve_exposure_path") or []))
    candidates = [str(item).strip().upper() for item in list(context.get("affected_candidates") or []) if str(item).strip()]
    consequence = str(portfolio_consequence or "").strip()
    micro_text = str(micro or "").strip()
    implementation = str(implementation_sensitivity or "").strip()
    parts = [
        _portfolio_sleeve_line(bucket=bucket, source_class=source_class, sleeve_text=sleeve_text, fallback=consequence or micro_text),
        _portfolio_more_usable_line(bucket=bucket, source_class=source_class, candidates=candidates),
        _portfolio_less_usable_line(bucket=bucket, source_class=source_class),
        _portfolio_not_yet_line(
            bucket=bucket,
            source_class=source_class,
            overread_reason=overread_reason,
            fallback=implementation,
        ),
    ]
    cleaned = [part for part in parts if str(part or "").strip()]
    return _dedupe_sentences(" ".join(cleaned))


def _portfolio_sleeve_line(*, bucket: str, source_class: str, sleeve_text: str, fallback: str) -> str:
    if source_class in {"policy_event", "geopolitical_news"}:
        return f"For {sleeve_text}, this stays a watch point first rather than a direct trade call."
    mapping = {
        "duration": f"For {sleeve_text}, safer bond exposure is still easier to justify than adding a lot more long duration.",
        "credit": f"For {sleeve_text}, tighter funding conditions still keep the risk budget under pressure.",
        "dollar_fx": f"For {sleeve_text}, the dollar still keeps the hurdle high for broad global risk.",
        "growth": f"For {sleeve_text}, the equity view still depends on whether the move broadens beyond the headline gain.",
        "inflation": f"For {sleeve_text}, the choice between bond exposure and inflation protection is still active.",
        "energy": f"For {sleeve_text}, oil still matters because it can keep the inflation-hedge case alive.",
        "real_assets": f"For {sleeve_text}, real assets still matter as portfolio protection.",
        "volatility": f"For {sleeve_text}, safer holdings stay easier to justify while stress is still active.",
    }
    return mapping.get(bucket, fallback or f"For {sleeve_text}, the current read still affects sleeve posture.")


def _portfolio_more_usable_line(*, bucket: str, source_class: str, candidates: list[str]) -> str:
    candidate_text = f" {', '.join(candidates[:3])} become the main ETF choices if confirmation improves." if candidates else ""
    if source_class in {"policy_event", "geopolitical_news"}:
        return f"If confirmation improves, a sleeve-level response becomes more usable.{candidate_text}"
    mapping = {
        "duration": f"If confirmation improves, safer bond holdings become more usable.{candidate_text}",
        "credit": f"If confirmation improves, higher-quality ballast and selective carry become more usable.{candidate_text}",
        "dollar_fx": f"If confirmation improves, selective global exposure and cash-like flexibility become more usable.{candidate_text}",
        "growth": f"If confirmation improves, selective equity exposure becomes more usable.{candidate_text}",
        "inflation": f"If confirmation improves, inflation protection becomes more usable.{candidate_text}",
        "energy": f"If confirmation improves, real assets become more usable as protection.{candidate_text}",
        "real_assets": f"If confirmation improves, real assets become more usable as protection.{candidate_text}",
        "volatility": f"If confirmation improves, safer holdings become more usable.{candidate_text}",
    }
    return mapping.get(bucket, f"If confirmation improves, the preferred sleeve move becomes more usable.{candidate_text}")


def _portfolio_less_usable_line(*, bucket: str, source_class: str) -> str:
    if source_class in {"policy_event", "geopolitical_news"}:
        return "Until then, reacting to the headline alone should stay delayed."
    mapping = {
        "duration": "Until then, larger long-bond adds should stay delayed.",
        "credit": "Until then, broad risk adds stay less usable than safer ballast.",
        "dollar_fx": "Until then, broad international risk adds should stay delayed.",
        "growth": "Until then, broad equity adds should stay delayed.",
        "inflation": "Until then, easy duration relief stays less likely than ongoing inflation pressure.",
        "energy": "Until then, quick bond relief stays less likely than keeping inflation protection in place.",
        "real_assets": "Until then, treating this as a stand-alone commodity bet stays less useful than keeping portfolio protection.",
        "volatility": "Until then, aggressive risk adds stay less useful than safer holdings.",
    }
    return mapping.get(bucket, "Until then, broader risk deployment should stay delayed.")


def _portfolio_not_yet_line(*, bucket: str, source_class: str, overread_reason: str, fallback: str) -> str:
    if source_class in {"policy_event", "geopolitical_news"}:
        return "This still is not enough for a portfolio move on the headline alone."
    mapping = {
        "duration": "This still is not a full green light for aggressive duration extension.",
        "credit": "This still is not enough for a broad portfolio shift on its own.",
        "dollar_fx": "This still is not enough for a full portfolio pivot.",
        "growth": "This still is not enough for a broad equity risk reset.",
        "inflation": "This still is not enough to force a single trade before broader confirmation arrives.",
        "energy": "This still is not a stand-alone oil trade call.",
        "real_assets": "This still is not enough to justify a big long-term shift into real assets.",
        "volatility": "This still is not enough for a full defensive reset on its own.",
    }
    default_line = mapping.get(bucket)
    if default_line:
        return default_line
    boundary = _boundary_from_overread(overread_reason)
    if boundary:
        return boundary
    return fallback or "This still should not be treated as a full portfolio instruction."


def _scenario_support_block(*, support: dict[str, Any] | None, context: dict[str, Any]) -> str | None:
    if not support:
        return None
    scenario_block = dict(support.get("scenario_block") or {})
    summary = str(scenario_block.get("summary") or "").strip()
    if summary:
        return summary
    source_class = str(context.get("source_class") or "market_series")
    if source_class in {"policy_event", "geopolitical_news"}:
        return "Scenario support stays conditional on broader market confirmation."
    return "Scenario support is available, but the current read still depends on follow-through."


def _boundary_from_overread(text: str | None) -> str | None:
    raw = str(text or "").strip()
    lowered = raw.lower()
    if not raw:
        return None
    if "headline force alone" in lowered:
        return "This still does not justify treating the headline as a durable portfolio fact."
    if "direct commodity trade call" in lowered:
        return "This still does not justify a stand-alone commodity trade."
    if "full allocation instruction" in lowered:
        return "This still does not justify a full allocation instruction."
    if "execution instruction" in lowered:
        return "This still does not justify a direct execution instruction."
    if "holdings" in lowered and "confirmation" in lowered:
        return "This still does not justify a holdings-level move without stronger confirmation."
    return None


def _append_distinct_part(parts: list[str], text: str | None, *, threshold: float) -> None:
    candidate = str(text or "").strip()
    if not candidate:
        return
    for existing in parts:
        if candidate == existing:
            return
        if candidate in existing or existing in candidate:
            return
        if _similarity(existing, candidate) >= threshold:
            return
    parts.append(candidate)


def _dedupe_sentences(text: str) -> str:
    sentences = [segment.strip() for segment in re.split(r"(?<=[.!?])\s+", str(text or "").strip()) if segment.strip()]
    kept: list[str] = []
    signatures: list[str] = []
    for sentence in sentences:
        signature = _sentence_signature(sentence)
        duplicate = False
        for existing, existing_signature in zip(kept, signatures):
            if signature == existing_signature:
                duplicate = True
                break
            if sentence == existing or sentence in existing or existing in sentence or _similarity(existing, sentence) >= 0.72:
                duplicate = True
                break
        if not duplicate:
            kept.append(sentence)
            signatures.append(signature)
    return " ".join(kept)


def _sentence_signature(sentence: str) -> str:
    normalized = " ".join(str(sentence or "").strip().lower().split())
    if normalized.startswith("the live implementation set is "):
        head = normalized.split(" if ", 1)[0]
        return head.rstrip(".")
    return normalized


def _confirm_condition(*, label: str, bucket: str, context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    market_confirmation = str(context.get("market_confirmation") or "limited")
    if source_class in {"policy_event", "geopolitical_news"}:
        return f"Look for oil, rates, volatility, breadth, or the dollar to move with the headline. That would turn it into a real market signal. Right now confirmation is {market_confirmation}."
    if bucket == "duration":
        return "Look for rates to stay firm while credit and rate-sensitive equities keep respecting the higher hurdle. That would confirm duration pressure is still active."
    if bucket == "credit":
        return "Look for spreads to stay wide while breadth stays narrow and rates do not ease. That would confirm funding conditions are still tight."
    if bucket in {"energy", "real_assets"}:
        return "Look for inflation markets, the dollar, and hedge assets to keep backing the move. That would confirm the protection case is strengthening."
    if bucket == "dollar_fx":
        return "Look for real yields, non-US risk assets, and commodities to keep lining up with the dollar. That would confirm the global hurdle is still real."
    if bucket == "growth":
        return "Look for breadth to widen, leadership to hold, and credit, rates, and FX to stop pushing back. That would confirm risk appetite is really broadening."
    if bucket == "inflation":
        return "Look for inflation-sensitive assets, real yields, and protection assets to keep moving the same way. That would confirm price pressure is still active."
    return "Look for related markets to keep moving the same way. That would show the current read is strengthening instead of fading."


def _weaken_condition(*, label: str, bucket: str, context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    if source_class in {"policy_event", "geopolitical_news"}:
        return "If the headline stays live but markets absorb it quickly, the read weakens and stays at watch level."
    if bucket == "duration":
        return "If real yields ease and rate-sensitive assets absorb the move, the case for waiting on longer bonds weakens."
    if bucket == "credit":
        return "If spreads retrace while breadth improves and rates stop tightening, the tight funding read weakens."
    if bucket in {"energy", "real_assets"}:
        return "If the move retraces and inflation markets stop backing it up, the case for keeping protection prominent weakens."
    if bucket == "dollar_fx":
        return "If the dollar eases and global markets stop respecting it, the global hurdle weakens."
    if bucket == "growth":
        return "If breadth narrows again or credit, rates, and FX keep resisting the move, the broader risk-on case weakens."
    if bucket == "inflation":
        return "If real yields, broader prices, and protection assets stop confirming the move, the case for keeping bonds constrained weakens."
    return "If the move partly retraces and related markets stop confirming it, the current read weakens."


def _break_condition(*, label: str, bucket: str, context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    if source_class in {"policy_event", "geopolitical_news"}:
        return "If the price response fades across exposed markets, the event falls back into headline noise rather than portfolio reality."
    if bucket == "duration":
        return "If rates and real yields fall back while credit and equities reopen, the case for waiting on longer bonds breaks."
    if bucket == "credit":
        return "If spreads fully reverse while breadth broadens and rates stop leaning against risk, the tight funding read breaks."
    if bucket in {"energy", "real_assets"}:
        return "If the move fully reverses and inflation markets stop backing it up, the case for extra protection breaks."
    if bucket == "dollar_fx":
        return "If the dollar fully reverses and other markets stop lining up with it, the global hurdle breaks."
    if bucket == "growth":
        return "If the rally reverses and breadth, credit, and rates all turn less supportive, the case for adding equity risk breaks."
    if bucket == "inflation":
        return "If inflation pressure fades across rates, broader prices, and protection assets, the case against adding more bond exposure breaks."
    return "If the move fully reverses and related markets stop backing it up, the current read breaks."


def _source_and_validity_block(context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    freshness_label = _freshness_label(context)
    source_authority_tier = str(context.get("source_authority_tier") or "").strip().lower()
    authority_label = _authority_label(source_class=source_class, authority_tier=source_authority_tier)
    if source_class == "macro_release":
        return f"Official release. Status: {freshness_label.lower()}."
    if source_class in {"policy_event", "geopolitical_news"}:
        confirmation = str(context.get("market_confirmation") or "limited")
        return f"Reported event with {confirmation} market confirmation. Status: {freshness_label.lower()}."
    return f"{authority_label}. Status: {freshness_label.lower()}."


def _news_to_market_confirmation_block(context: dict[str, Any]) -> str | None:
    source_class = str(context.get("source_class") or "market_series")
    if source_class not in {"policy_event", "geopolitical_news"}:
        return None
    return _market_confirmation_block(context=context)


def _market_confirmation_block(*, context: dict[str, Any]) -> str | None:
    source_class = str(context.get("source_class") or "market_series")
    if source_class not in {"policy_event", "geopolitical_news"}:
        return None
    event_status = str(context.get("event_status") or "developing")
    market_confirmation = str(context.get("market_confirmation") or "limited")
    priced_state = str(context.get("priced_state") or "").strip()
    if market_confirmation == "broad":
        return "Market confirmation is broad. The event is transmitting across the main affected channels rather than staying headline-only."
    if market_confirmation == "partial" or market_confirmation == "moderate":
        return "Market confirmation is partial. Some exposed markets are reacting, but the event is not yet a fully confirmed portfolio fact."
    if market_confirmation in {"limited", "unconfirmed", "none"}:
        return (
            f"Market confirmation is limited. The event is {event_status}, but the price response is still bounded"
            f"{f' and only {priced_state}' if priced_state else ''}."
        )
    return f"Market confirmation is {market_confirmation}. Treat the portfolio consequence as bounded until follow-through broadens."


def _anti_repetition_pass(
    clauses: dict[str, str],
    *,
    context: dict[str, Any],
    bucket: str,
    sleeves: list[str],
    holdings: list[str],
) -> dict[str, str]:
    macro = clauses["why_it_matters_macro"]
    micro = clauses["why_it_matters_micro"]
    short_term = clauses["why_it_matters_short_term"]
    long_term = clauses["why_it_matters_long_term"]
    source_class = str(context.get("source_class") or "market_series")

    if _similarity(macro, micro) >= 0.74:
        clauses["why_it_matters_micro"] = _micro_fallback(bucket=bucket, sleeves=sleeves, holdings=holdings)
    if _similarity(clauses["why_it_matters_macro"], clauses["why_it_matters_micro"]) >= 0.8:
        clauses["why_it_matters_micro"] = ""
    if _similarity(short_term, long_term) >= 0.74:
        clauses["why_it_matters_long_term"] = _long_term_fallback(bucket=bucket, sleeves=sleeves, context=context)
    if _similarity(clauses["why_it_matters_short_term"], clauses["why_it_matters_long_term"]) >= 0.8:
        clauses["why_it_matters_long_term"] = ""
    if source_class in {"policy_event", "geopolitical_news"} and _similarity(clauses["why_it_matters_short_term"], clauses["why_it_matters_long_term"]) >= 0.6:
        clauses["why_it_matters_long_term"] = ""
    if _similarity(clauses["why_it_matters_micro"], clauses["portfolio_consequence"]) >= 0.82:
        clauses["portfolio_consequence"] = _decision_frame(
            bucket=bucket,
            sleeves=sleeves,
            holdings=holdings,
            candidates=list(context.get("affected_candidates") or []),
            context=context,
        )
    if _similarity(clauses["what_changed_today"], clauses["why_now_not_before"]) >= 0.78:
        clauses["why_now_not_before"] = _why_now_fallback(context=context)
    if _similarity(clauses["why_this_could_be_wrong"], clauses["overread_reason"]) >= 0.82:
        clauses["why_this_could_be_wrong"] = _failure_mode_fallback(bucket=bucket, context=context)
    if _similarity(clauses["implementation_sensitivity"], clauses["portfolio_consequence"]) >= 0.82:
        clauses["implementation_sensitivity"] = _implementation_fallback(
            bucket=bucket,
            sleeves=sleeves,
            holdings=holdings,
            candidates=list(context.get("affected_candidates") or []),
        )
    if _similarity(clauses["evidence_class"], clauses["what_changed_today"]) >= 0.82:
        clauses["evidence_class"] = _evidence_fallback(context=context)
    return clauses


def _micro_fallback(*, bucket: str, sleeves: list[str], holdings: list[str]) -> str:
    if holdings:
        return f"Use the mapped holding set as the first implementation lens: {', '.join(holdings[:2])}."
    sleeve_text = _sleeve_text(sleeves)
    if bucket == "duration":
        return f"For {sleeve_text}, this changes timing discipline more than target allocation."
    if bucket in {"energy", "real_assets", "inflation"}:
        return f"For {sleeve_text}, this changes the hedge quality rather than forcing a direct trade."
    return f"For {sleeve_text}, keep the consequence at sleeve level."


def _long_term_fallback(*, bucket: str, sleeves: list[str], context: dict[str, Any]) -> str:
    sleeve_text = _sleeve_text(sleeves)
    source_class = str(context.get("source_class") or "market_series")
    if source_class in {"policy_event", "geopolitical_news"}:
        return "Longer term, this matters only if market confirmation persists beyond the current headline window."
    if bucket == "duration":
        return f"If the signal persists, {sleeve_text} stays in a slower, higher-hurdle regime."
    return f"Only persistence would make this a structural change for {sleeve_text}."


def _why_now_fallback(*, context: dict[str, Any]) -> str:
    lead_lane = str(context.get("lead_lane") or "").strip().lower()
    if lead_lane == "regime_context":
        return "It remains visible because it still shapes the backdrop, not because it produced a fresh catalyst."
    return "It matters now because it still affects the current decision window."


def _failure_mode_fallback(*, bucket: str, context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    if source_class in {"policy_event", "geopolitical_news"}:
        return "The interpretation fails first if price confirmation fades."
    if bucket in {"energy", "real_assets"}:
        return "The interpretation fails first if the move reverses before other inflation-sensitive markets confirm it."
    if bucket == "duration":
        return "The interpretation fails first if financing pressure eases without broader rate confirmation."
    return "The interpretation fails first if follow-through does not arrive."


def _implementation_fallback(*, bucket: str, sleeves: list[str], holdings: list[str], candidates: list[str]) -> str:
    if holdings:
        return "Implementation should stay position-specific until the read strengthens."
    sleeve_text = _sleeve_text(sleeves)
    if candidates:
        return f"Treat this as a pacing signal for {sleeve_text}; candidate selection comes later."
    return f"Treat this as a pacing signal for {sleeve_text}."


def _evidence_fallback(*, context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    if source_class == "macro_release":
        return "Official release with bounded portfolio inference."
    if source_class in {"policy_event", "geopolitical_news"}:
        return "Reported event with bounded market confirmation."
    return "Direct market move with bounded portfolio inference."


def _freshness_label(context: dict[str, Any]) -> str:
    source_class = str(context.get("source_class") or "market_series")
    lead_lane = str(context.get("lead_lane") or "").strip().lower()
    age_days = _safe_float(context.get("freshness_age_days"))
    if lead_lane == "regime_context":
        return "Still valid but outside the active decision window"
    if source_class == "macro_release":
        if age_days is not None and age_days > 2.0:
            return "Latest valid official print"
        return "Fresh official input in the current brief window"
    if source_class in {"policy_event", "geopolitical_news"}:
        if age_days is not None and age_days > 1.0:
            return "Latest valid reported event"
        return "Fresh reported event in the current brief window"
    return "Fresh in the current brief window"


def _humanize_authority(value: str) -> str:
    raw = str(value or "").strip().replace("_", " ")
    if not raw:
        return ""
    return raw


def _humanize_source_origin(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Declared source"
    return raw.replace("_", " ").strip().title()


def _authority_label(*, source_class: str, authority_tier: str) -> str:
    if source_class == "macro_release":
        return "Official release"
    if source_class in {"policy_event", "geopolitical_news"}:
        return "Reported event"
    if "licensed" in authority_tier and "gap" in authority_tier:
        return "Public close with licensed cross-check gap"
    if "public" in authority_tier and "verified" in authority_tier:
        return "Public verified close"
    if "official" in authority_tier:
        return _humanize_authority(authority_tier)
    if authority_tier:
        return _humanize_authority(authority_tier)
    return "Latest valid market reading"


def _source_class(signal_card: dict[str, Any], effect_profile: dict[str, Any]) -> str:
    signal_kind = str(signal_card.get("signal_kind") or "market")
    bucket = str(effect_profile.get("primary_effect_bucket") or "market")
    label = str(signal_card.get("label") or signal_card.get("symbol") or "").lower()
    symbol = str(signal_card.get("symbol") or "").upper()
    runtime = dict(signal_card.get("runtime_provenance") or {})
    truth_envelope = dict(signal_card.get("truth_envelope") or runtime.get("truth_envelope") or {})
    source_type = str(signal_card.get("source_type") or "").strip().lower()
    source_family = str(runtime.get("source_family") or "").strip().lower()
    if signal_kind == "news":
        if bucket == "policy" or any(term in label for term in _POLICY_TERMS):
            return "policy_event"
        if any(term in label for term in _GEOPOLITICAL_TERMS):
            return "geopolitical_news"
        return "geopolitical_news"
    if signal_kind == "macro":
        return "macro_release"
    if source_type == "official_release":
        return "macro_release"
    if source_family in {"macro_market_state", "macro_series"}:
        return "macro_release"
    if truth_envelope.get("release_date"):
        return "macro_release"
    if symbol in {"NASDAQNCPAG"}:
        return "benchmark_move"
    return "market_series"


def _evidence_type(source_class: str) -> str:
    if source_class == "macro_release":
        return "official_macro_print"
    if source_class in {"policy_event", "geopolitical_news"}:
        return "reported_event"
    if source_class == "benchmark_move":
        return "inferred_market_read"
    return "direct_price_move"


def _source_origin_from_label(label: str) -> str | None:
    lowered = label.lower()
    if "reuters" in lowered:
        return "Reuters"
    if "fred" in lowered:
        return "FRED"
    return None


def _market_confirmation(*, source_class: str, change_pct: float | None, magnitude: str) -> str:
    if source_class in {"policy_event", "geopolitical_news"}:
        if change_pct is None:
            return "unconfirmed"
        if abs(change_pct) >= 1.0 or magnitude == "significant":
            return "strong"
        if abs(change_pct) >= 0.35 or magnitude == "moderate":
            return "moderate"
        return "limited"
    if magnitude == "significant":
        return "strong"
    if magnitude == "moderate":
        return "moderate"
    return "limited"


def _event_status(*, source_class: str, label: str, market_confirmation: str) -> str:
    if source_class not in {"policy_event", "geopolitical_news"}:
        return "observed"
    lowered = label.lower()
    if any(term in lowered for term in {"confirmed", "official", "agrees", "announces"}):
        return "confirmed"
    if market_confirmation == "strong":
        return "reported and market-confirmed"
    return "developing"


def _priced_state(*, source_class: str, market_confirmation: str, change_pct: float | None) -> str:
    if source_class in {"policy_event", "geopolitical_news"}:
        if market_confirmation == "strong":
            return "market already reacting"
        if market_confirmation == "moderate":
            return "partly reflected in price action"
        return "headline not yet strongly confirmed by market prices"
    if change_pct is None:
        return "price confirmation limited"
    if abs(change_pct) >= 1.0:
        return "materially priced today"
    if abs(change_pct) >= 0.35:
        return "partly priced today"
    return "only lightly priced today"


def _regime_context(*, bucket: str) -> str:
    return {
        "duration": "financing conditions and duration hurdle",
        "inflation": "inflation path and hedge mix",
        "credit": "spread compensation and refinancing conditions",
        "dollar_fx": "external hurdle and global risk translation",
        "energy": "energy inflation impulse and cost shock risk",
        "real_assets": "inflation hedge quality and diversification",
        "growth": "equity leadership and breadth",
        "policy": "official policy path",
        "volatility": "risk regime fragility",
    }.get(bucket, "portfolio context")


def _what_changed(
    *,
    label: str,
    current_value: float | None,
    previous_value: float | None,
    change_pct: float | None,
    magnitude: str,
    source_class: str,
    source_type: str | None,
    reference_period: str | None,
    release_date: str | None,
    metric_definition: str | None,
) -> str:
    if source_class == "macro_release":
        if current_value is not None and previous_value is not None:
            suffix = f" for {reference_period}" if reference_period else ""
            return f"{label} latest release is {current_value:.2f} versus {previous_value:.2f} prior{suffix}"
        if current_value is not None:
            release_note = f" released on {release_date[:10]}" if release_date else ""
            return f"{label} latest official release is {current_value:.2f}{release_note}"
        if metric_definition:
            return metric_definition
    if current_value is None and change_pct is None:
        return f"{label} remains the active source-backed input"
    if change_pct is None:
        return f"{label} is printing near {current_value:.2f}"
    direction = "higher" if change_pct > 0 else "lower" if change_pct < 0 else "flat"
    session_word = "closed" if str(source_type or "").strip().lower() == "market_close" else "printed"
    return f"{label} {session_word} {abs(change_pct):.2f}% {direction}"


def _support_strength(support: dict[str, Any] | None) -> str:
    if not support:
        return ""
    bundle = support.get("bundle")
    support_summary = getattr(bundle, "support", None)
    return str(getattr(support_summary, "support_strength", "") or "").strip().lower()


def _change_pct(previous_value: float | None, current_value: float | None) -> float | None:
    if previous_value in {None, 0.0} or current_value is None:
        return None
    return ((current_value - previous_value) / abs(previous_value)) * 100.0


def _sleeve_text(sleeves: list[str]) -> str:
    names = []
    for item in sleeves[:2]:
        raw = str(item or "").strip()
        if not raw:
            continue
        names.append(raw if raw.lower().startswith("the ") else _humanize_sleeve(raw))
    filtered = [name for name in names if name]
    return ", ".join(filtered) if filtered else "the relevant sleeve mix"


def _joined_sleeve_text(sleeves: list[str]) -> str:
    names = [str(item or "").strip() for item in sleeves if str(item or "").strip()]
    if not names:
        return "the relevant sleeve mix"
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def _humanize_sleeve(value: str) -> str:
    text = str(value or "").replace("sleeve_", "").replace("_", " ").strip()
    if text.lower() == "ig bonds":
        return "the bond sleeve"
    if text.lower() == "global equity core":
        return "the global equity sleeve"
    if text.lower() == "real assets":
        return "the real-assets sleeve"
    if text.lower() == "cash bills":
        return "the cash sleeve"
    return f"the {text}" if text else "the portfolio context"


def _similarity(left: str, right: str) -> float:
    left_tokens = _normalized_tokens(left)
    right_tokens = _normalized_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    return overlap / max(len(left_tokens), len(right_tokens))


def _normalized_tokens(text: str) -> set[str]:
    cleaned = "".join(char.lower() if char.isalnum() else " " for char in str(text or ""))
    stop = {"the", "and", "for", "into", "with", "this", "that", "than", "only", "keep", "level", "until", "still"}
    return {token for token in cleaned.split() if len(token) > 2 and token not in stop}


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
