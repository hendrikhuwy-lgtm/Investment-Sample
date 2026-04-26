from __future__ import annotations

from typing import Any


def _text(value: Any) -> str:
    return str(value or "").strip()


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _friendly_family(value: str) -> str:
    return value.replace("_", " ").strip().lower()


def _dedupe(values: list[str]) -> list[str]:
    cleaned: list[str] = []
    for value in values:
        text = _text(value)
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _friendly_signal_type(card: dict[str, Any]) -> str:
    for candidate in (
        card.get("signal_type"),
        card.get("signal_family"),
        card.get("category"),
        card.get("consequence_type"),
    ):
        text = _text(candidate)
        if text:
            return text
    return "general_market_signal"


def _evidence_classification(card: dict[str, Any]) -> str:
    if bool(card.get("live_holdings_used")):
        return "holdings_grounded"
    grounding_mode = _text(card.get("grounding_mode"))
    if grounding_mode == "target_proxy":
        return "sleeve_inferred"
    if grounding_mode == "benchmark_proxy":
        return "proxy_supported"
    return "direct_or_proxy_mixed"


def _signal_role(card: dict[str, Any]) -> str:
    severity = _text(card.get("severity_label")).lower()
    family = _text(card.get("signal_family")).lower()
    if severity in {"critical", "high"}:
        return "dominant_signal"
    if family in {"fx", "rates", "credit", "volatility", "equity_breadth"}:
        return "supporting_signal"
    if family in {"research_dataset", "structural_macro", "positioning"}:
        return "contextual_signal"
    return "background_signal"


def _lag_reason(card: dict[str, Any]) -> str:
    freshness = dict(dict(card.get("evidence") or {}).get("freshness") or {})
    lag_cause = _text(freshness.get("lag_cause"))
    if lag_cause == "expected_publication_lag":
        return "source publication cadence is slower than the Daily Brief run cadence"
    if lag_cause == "unexpected_ingestion_lag":
        return "upstream refresh failed and the prior cache had to be reused"
    return _text(card.get("lag_reason") or "")


def _directness(card: dict[str, Any]) -> str:
    grounding_mode = _text(card.get("grounding_mode"))
    if bool(card.get("live_holdings_used")):
        return "holdings_based"
    if grounding_mode == "target_proxy":
        return "sleeve_based"
    if grounding_mode == "benchmark_proxy":
        return "proxy"
    return "direct"


def build_signal_fact_pack(card: dict[str, Any]) -> dict[str, Any]:
    evidence = dict(card.get("evidence") or {})
    freshness = dict(evidence.get("freshness") or {})
    implication = dict(card.get("portfolio_implication") or {})
    confidence = dict(card.get("confidence") or {})
    benchmark_context = _text(card.get("benchmark_effect_family") or card.get("benchmark_issue_type") or card.get("benchmark_effect"))
    issue_type = _text(card.get("consequence_type") or "watch only")
    primary_sleeves = [str(item) for item in list(implication.get("primary_affected_sleeves") or []) if str(item)]
    secondary_sleeves = [str(item) for item in list(implication.get("secondary_affected_sleeves") or []) if str(item)]
    affected_sleeves = _dedupe([*primary_sleeves, *secondary_sleeves])
    affected_markets = _dedupe([str(item) for item in list(card.get("affected_markets") or []) if str(item)])
    evidence_items: list[dict[str, Any]] = []
    observed_at = _text(evidence.get("observed_at"))
    observation = _text(card.get("observation"))
    evidence_source = _text(evidence.get("source") or "unknown")
    lag_class = _text(freshness.get("lag_class") or "unknown")
    refresh_reason_code = _text(card.get("freshness_reason_code") or freshness.get("reason_code") or "")
    if observation:
        evidence_items.append(
            {
                "evidence_id": "reading",
                "label": _text(card.get("title") or card.get("metric_code") or "Observed reading"),
                "value_text": observation,
                "observed_date": observed_at or None,
                "source_ref": evidence_source,
                "meaning_class": "observed_reading",
                "evidence_type": "direct",
                "freshness_state": lag_class,
                "freshness_reason_code": refresh_reason_code,
                "footnote_template_key": "observed_reading",
            }
        )
    delta = _float(evidence.get("delta_5obs"))
    if delta is not None:
        evidence_items.append(
            {
                "evidence_id": "delta_5obs",
                "label": "Five-observation change",
                "value_text": f"{delta:+.2f} over 5 observations",
                "observed_date": observed_at or None,
                "source_ref": evidence_source,
                "meaning_class": "recent_change",
                "evidence_type": "direct",
                "freshness_state": lag_class,
                "freshness_reason_code": refresh_reason_code,
                "footnote_template_key": "recent_change",
            }
        )
    percentile = _float(evidence.get("percentile"))
    if percentile is not None:
        evidence_items.append(
            {
                "evidence_id": "percentile",
                "label": "Recent percentile",
                "value_text": f"{percentile:.0f}th percentile",
                "observed_date": observed_at or None,
                "source_ref": evidence_source,
                "meaning_class": "percentile_position",
                "evidence_type": "direct",
                "freshness_state": lag_class,
                "freshness_reason_code": refresh_reason_code,
                "footnote_template_key": "high_percentile" if percentile >= 80 else "percentile_general",
            }
        )
    if affected_sleeves:
        evidence_items.append(
            {
                "evidence_id": "sleeve_mapping",
                "label": "Most affected sleeves",
                "value_text": ", ".join(affected_sleeves),
                "observed_date": observed_at or None,
                "source_ref": "daily_brief_relevance_engine",
                "meaning_class": "sleeve_relevance",
                "evidence_type": "holdings_direct" if bool(card.get("live_holdings_used")) else "sleeve_proxy",
                "freshness_state": lag_class,
                "freshness_reason_code": refresh_reason_code,
                "footnote_template_key": "holdings_direct" if bool(card.get("live_holdings_used")) else "sleeve_proxy_only",
            }
        )
    if benchmark_context:
        evidence_items.append(
            {
                "evidence_id": "benchmark_context",
                "label": "Benchmark context",
                "value_text": benchmark_context,
                "observed_date": observed_at or None,
                "source_ref": "daily_brief_benchmark_logic",
                "meaning_class": "benchmark_context",
                "evidence_type": "benchmark",
                "freshness_state": lag_class,
                "freshness_reason_code": refresh_reason_code,
                "footnote_template_key": "benchmark_context_only",
            }
        )
    limitations = [str(item) for item in list(card.get("unknowns") or []) if str(item)]
    benchmark_support_facts = _dedupe(
        [
            benchmark_context,
            _text(card.get("benchmark_effect")),
            _text(card.get("benchmark_driver")),
            _text(implication.get("affected_benchmark_context")),
        ]
    )
    holdings_support_facts = _dedupe(
        [
            _text(card.get("holdings_effect")),
            _text(card.get("holdings_reason")),
            _text(implication.get("current_holdings_relevance")),
        ]
    )
    uncertainty_facts = _dedupe(
        [
            _text(confidence.get("reason")),
            *limitations,
            _lag_reason(card),
        ]
    )
    scenario_branch_facts = {
        "if_worsens": _text(card.get("if_worsens")),
        "if_stabilizes": _text(card.get("if_normalizes") or card.get("if_persists")),
        "if_reverses": _text(card.get("if_reverses")),
    }
    strengthen_read_facts = _dedupe(
        [
            _text(card.get("what_would_strengthen")),
            _text(card.get("supporting_data")),
            *[str(item) for item in list(card.get("strengthen_read_facts") or []) if str(item)],
        ]
    )
    weaken_read_facts = _dedupe(
        [
            _text(card.get("what_would_weaken")),
            *limitations,
            *[str(item) for item in list(card.get("weaken_read_facts") or []) if str(item)],
        ]
    )
    why_it_matters_facts = _dedupe(
        [
            _text(card.get("why_it_matters")),
            _text(card.get("interpretation")),
            _text(card.get("mechanism")),
            _text(card.get("transmission_path")),
        ]
    )
    implication_facts = _dedupe(
        [
            _text(card.get("consequence_summary")),
            _text(card.get("consequence_detail")),
            _text(card.get("immediate_effect")),
            _text(card.get("risk_consequence")),
            _text(card.get("opportunity_consequence")),
        ]
    )
    review_facts = _dedupe(
        [
            _text(card.get("follow_up")),
            _text(card.get("financial_review_question")),
        ]
    )
    boundary_facts = _dedupe(
        [
            _text(card.get("long_term_effect")),
            _text(card.get("boundary")),
            "This remains a sleeve-level read because live holdings are unavailable today."
            if _text(card.get("grounding_mode")) != "live_holding_grounded"
            else "This supports review, not an automatic allocation change.",
        ]
    )
    signal_summary_facts = _dedupe(
        [
            observation,
            _text(card.get("title") or card.get("metric_code")),
            _text(card.get("recency_note")),
        ]
    )
    return {
        "signal_id": _text(card.get("signal_id") or card.get("metric_code") or card.get("title") or "signal"),
        "signal_title": _text(card.get("title") or card.get("metric_code") or "Signal"),
        "signal_family": _friendly_family(_text(card.get("signal_family") or card.get("category") or "general")),
        "signal_type": _friendly_signal_type(card),
        "signal_role": _signal_role(card),
        "affected_markets": affected_markets,
        "affected_sleeves": affected_sleeves,
        "primary_affected_sleeves": primary_sleeves,
        "secondary_affected_sleeves": secondary_sleeves,
        "raw_metrics": {
            "metric_name": _text(card.get("title") or card.get("metric_code") or "Signal"),
            "metric_value": _text(card.get("observation")),
            "metric_delta": delta,
            "delta_window": 5 if delta is not None else None,
            "percentile_state": "extreme" if percentile is not None and (percentile >= 80 or percentile <= 20) else ("available" if percentile is not None else "unavailable"),
            "percentile_value": percentile,
        },
        "observation_date": observed_at or None,
        "observed_date": observed_at or None,
        "freshness_state": _text(freshness.get("lag_class") or card.get("source_freshness_state") or "unknown"),
        "freshness_reason_code": refresh_reason_code,
        "source_type": evidence_source,
        "source_priority": _text(card.get("source_priority") or "primary"),
        "lag_reason": _lag_reason(card),
        "evidence_classification": _evidence_classification(card),
        "grounding_type": _directness(card),
        "confidence_level": _text(confidence.get("label") or card.get("narrative_confidence") or "low"),
        "confidence_basis": _text(confidence.get("reason") or ""),
        "consequence_type": issue_type,
        "route_type": _text(card.get("action_tag") or card.get("follow_up") or "monitor"),
        "signal_summary_facts": signal_summary_facts,
        "why_it_matters_facts": why_it_matters_facts,
        "likely_investment_implication_facts": implication_facts,
        "boundary_facts": boundary_facts,
        "review_action_facts": review_facts,
        "benchmark_support_facts": benchmark_support_facts,
        "holdings_support_facts": holdings_support_facts,
        "uncertainty_facts": uncertainty_facts,
        "scenario_branch_facts": scenario_branch_facts,
        "strengthen_read_facts": strengthen_read_facts,
        "weaken_read_facts": weaken_read_facts,
        "holdings_grounding_mode": _text(card.get("grounding_mode") or "macro_only"),
        "benchmark_context_class": benchmark_context,
        "local_context_class": _text(card.get("benchmark_driver") or ""),
        "evidence_items": evidence_items,
        "source_refs": [str(item) for item in list(card.get("source_ids") or []) if str(item)] or [evidence_source or "unknown"],
        "limitations": limitations,
        "forbidden_claims": {
            "no_holdings_level_claim": _text(card.get("grounding_mode")) != "live_holding_grounded",
            "no_extra_sleeves": True,
            "no_unsupported_dates": True,
            "no_stronger_confidence_than_allowed": True,
        },
    }
