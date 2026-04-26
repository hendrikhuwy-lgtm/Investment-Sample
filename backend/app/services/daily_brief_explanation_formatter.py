from __future__ import annotations

from typing import Any


def _sentence(text: str) -> str:
    text = " ".join(str(text or "").split()).strip()
    if not text:
        return ""
    return text if text.endswith(".") else f"{text}."


def _friendly(values: list[str], *, fallback: str) -> str:
    cleaned = [str(value).replace("_", " ") for value in values if str(value)]
    return ", ".join(cleaned) if cleaned else fallback


def _first(values: list[str], fallback: str) -> str:
    for value in values:
        text = " ".join(str(value or "").split()).strip()
        if text:
            return text
    return fallback


def format_signal_explanation(fact_pack: dict[str, Any], schema: dict[str, Any]) -> dict[str, str]:
    raw_metrics = dict(fact_pack.get("raw_metrics") or {})
    observed = str(raw_metrics.get("metric_value") or "").rstrip(".")
    date = str(fact_pack.get("observed_date") or "").strip()
    change = (
        f"{raw_metrics.get('metric_delta'):+.2f} over {raw_metrics.get('delta_window')} observations"
        if raw_metrics.get("metric_delta") is not None and raw_metrics.get("delta_window")
        else ""
    )
    freshness_state = str(fact_pack.get("freshness_state") or "unknown").replace("_", " ")
    prefix = {
        "refreshed_current": "Today",
        "current": "Today",
        "fresh": "Today",
        "latest_available_source_lag": "Latest available",
        "aging": "Latest available",
        "stale_demoted": "Lagged support",
        "stale_background_only": "Background only",
        "refresh_failed_used_cache": "Cached prior reading",
    }.get(freshness_state.lower(), "Latest available")
    signal = f"{prefix}: {observed or fact_pack.get('signal_title') or fact_pack.get('signal_id')}"
    if date:
        signal += f" on {date}"
    if change:
        signal += f", {change}"
    signal = _sentence(signal)

    meaning = _sentence(
        _first(
            list(fact_pack.get("why_it_matters_facts") or []),
            f"This changes the current backdrop for {_friendly(list(fact_pack.get('affected_sleeves') or []), fallback='background conditions')}.",
        )
    )
    investment_implication = _sentence(
        _first(
            list(fact_pack.get("likely_investment_implication_facts") or []),
            f"This mainly changes review urgency for {_friendly(list(fact_pack.get('affected_sleeves') or []), fallback='benchmark assumptions')}.",
        )
    )
    boundary = _sentence(
        _first(
            list(fact_pack.get("boundary_facts") or []),
            "This supports review, not an automatic allocation conclusion on its own.",
        )
    )
    review_action = _sentence(
        _first(
            list(fact_pack.get("review_action_facts") or []),
            "Monitor only.",
        )
    )
    analyst_synthesis = _sentence(
        _first(
            [
                " ".join(
                    [
                        _first(list(fact_pack.get("signal_summary_facts") or []), ""),
                        _first(list(fact_pack.get("why_it_matters_facts") or []), ""),
                    ]
                ).strip(),
                _first(list(fact_pack.get("likely_investment_implication_facts") or []), ""),
            ],
            "Current conditions remain worth monitoring, but the evidence is not strong enough to imply a larger conclusion yet.",
        )
    )
    system_relevance = _sentence(
        _first(
            [
                _first(list(fact_pack.get("holdings_support_facts") or []), ""),
                _first(list(fact_pack.get("benchmark_support_facts") or []), ""),
                f"This is most relevant for {_friendly(list(fact_pack.get('affected_sleeves') or []), fallback='benchmark assumptions')}.",
            ],
            "This remains mainly a sleeve-level review question rather than a holdings-level instruction.",
        )
    )
    scenario_facts = dict(fact_pack.get("scenario_branch_facts") or {})
    scenario_if_worsens = _sentence(_first([scenario_facts.get("if_worsens")], "If this worsens, review whether the current risk read needs to become more defensive."))
    scenario_if_stabilizes = _sentence(_first([scenario_facts.get("if_stabilizes")], "If this stabilizes, the current caution can remain contained rather than broadening."))
    scenario_if_reverses = _sentence(_first([scenario_facts.get("if_reverses")], "If this reverses cleanly, current pressure would likely fade back into background monitoring."))
    strengthen_read = _sentence(_first(list(fact_pack.get("strengthen_read_facts") or []), "A cleaner follow-through in the same direction would strengthen this read."))
    weaken_read = _sentence(_first(list(fact_pack.get("weaken_read_facts") or []), "A reversal or better confirming evidence elsewhere would weaken this read."))
    top_strip_summary = _sentence(
        _first(
            [
                f"{_first(list(fact_pack.get('signal_summary_facts') or []), fact_pack.get('signal_title') or 'The main signal')} is the clearest market development today.",
                analyst_synthesis,
            ],
            "No single development is strong enough to dominate the current brief.",
        )
    )
    top_strip_implication = _sentence(
        _first(
            list(fact_pack.get("likely_investment_implication_facts") or []),
            investment_implication,
        )
    )
    top_strip_review = _sentence(
        _first(
            list(fact_pack.get("review_action_facts") or []),
            "Monitor only.",
        )
    )
    top_strip_boundary = _sentence(
        _first(
            list(fact_pack.get("boundary_facts") or []),
            "This does not yet justify a stronger portfolio conclusion.",
        )
    )
    return {
        "signal": signal,
        "meaning": meaning,
        "investment_implication": investment_implication,
        "boundary": boundary,
        "review_action": review_action,
        "analyst_synthesis": analyst_synthesis,
        "system_relevance": system_relevance,
        "scenario_if_worsens": scenario_if_worsens,
        "scenario_if_stabilizes": scenario_if_stabilizes,
        "scenario_if_reverses": scenario_if_reverses,
        "strengthen_read": strengthen_read,
        "weaken_read": weaken_read,
        "top_strip_summary": top_strip_summary,
        "top_strip_implication": top_strip_implication,
        "top_strip_review": top_strip_review,
        "top_strip_boundary": top_strip_boundary,
    }
