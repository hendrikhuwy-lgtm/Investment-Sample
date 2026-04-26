from __future__ import annotations

from typing import Any


def _top(values: list[str], count: int = 3) -> list[str]:
    return [str(item) for item in values if str(item)][:count]


def build_signal_explanation_schema(fact_pack: dict[str, Any]) -> dict[str, Any]:
    sleeves = [str(item).replace("_", " ") for item in list(fact_pack.get("affected_sleeves") or []) if str(item)]
    markets = [str(item).replace("_", " ") for item in list(fact_pack.get("affected_markets") or []) if str(item)]
    raw_metrics = dict(fact_pack.get("raw_metrics") or {})
    metric_name = str(raw_metrics.get("metric_name") or fact_pack.get("signal_title") or "signal")
    freshness_state = str(fact_pack.get("freshness_state") or "unknown")
    lag_reason = str(fact_pack.get("lag_reason") or "")
    return {
        "output_contract": "signal_synthesis_v2",
        "signal_sentence": {
            "metric": metric_name,
            "value": raw_metrics.get("metric_value"),
            "date": fact_pack.get("observed_date"),
            "change": (
                f"{raw_metrics.get('metric_delta'):+.2f} over {raw_metrics.get('delta_window')} observations"
                if raw_metrics.get("metric_delta") is not None and raw_metrics.get("delta_window")
                else None
            ),
            "freshness_state": freshness_state,
            "freshness_reason_code": fact_pack.get("freshness_reason_code"),
        },
        "top_strip_spec": {
            "task": "Write one concise investor summary line, one implication line, one review line, and one boundary line.",
            "allowed_focus": [
                "what changed",
                "why it matters",
                "what deserves review now",
                "what does not change yet",
            ],
        },
        "collapsed_card_spec": {
            "meaning_must_describe": "what the signal says about current conditions",
            "implication_must_describe": "what changes for review pacing, risk tolerance, protection relevance, or sleeve monitoring",
            "boundary_must_describe": "what this does not mean yet",
            "review_action_must_describe": "the concrete review question or monitoring task",
        },
        "expanded_analysis_spec": {
            "analyst_synthesis_must_add": "one analytical explanation of why this matters now",
            "system_relevance_must_add": "one portfolio-system paragraph tying this to sleeves, pacing, or review discipline",
            "scenario_branches": ["if_worsens", "if_stabilizes", "if_reverses"],
            "strengthen_and_weaken": True,
        },
        "prompt_constraints": {
            "plain_investor_language": True,
            "institutional_tone": True,
            "no_internal_ontology_vocabulary": True,
            "no_metric_invention": True,
            "no_date_invention": True,
            "no_holdings_claim_if_unavailable": True,
            "distinguish_meaning_from_implication": True,
            "state_uncertainty_directly": True,
        },
        "fact_pack_summary": {
            "signal_family": fact_pack.get("signal_family"),
            "signal_type": fact_pack.get("signal_type"),
            "signal_role": fact_pack.get("signal_role"),
            "affected_sleeves": _top(sleeves, 4),
            "affected_markets": _top(markets, 4),
            "signal_summary_facts": _top(list(fact_pack.get("signal_summary_facts") or []), 4),
            "why_it_matters_facts": _top(list(fact_pack.get("why_it_matters_facts") or []), 4),
            "investment_implication_facts": _top(list(fact_pack.get("likely_investment_implication_facts") or []), 4),
            "boundary_facts": _top(list(fact_pack.get("boundary_facts") or []), 3),
            "review_action_facts": _top(list(fact_pack.get("review_action_facts") or []), 3),
            "benchmark_support_facts": _top(list(fact_pack.get("benchmark_support_facts") or []), 3),
            "holdings_support_facts": _top(list(fact_pack.get("holdings_support_facts") or []), 3),
            "uncertainty_facts": _top(list(fact_pack.get("uncertainty_facts") or []), 4),
            "strengthen_read_facts": _top(list(fact_pack.get("strengthen_read_facts") or []), 3),
            "weaken_read_facts": _top(list(fact_pack.get("weaken_read_facts") or []), 3),
            "lag_reason": lag_reason,
        },
        "forbidden_claims": {
            **dict(fact_pack.get("forbidden_claims") or {}),
            "forbidden_phrases": [
                "stress-transmission evidence first",
                "target-sleeve proxy",
                "support family",
                "evidence basis",
                "current enough",
                "policy dependence reduces authority",
                "benchmark-relative interpretation becomes less clean",
                "routing unavailable",
                "optional holdings effect",
                "ontology",
                "framework vocabulary",
            ],
        },
        "grounding_constraints": {
            "holdings_grounding_mode": fact_pack.get("holdings_grounding_mode"),
            "freshness_state": freshness_state,
            "lag_reason": lag_reason,
            "evidence_classification": fact_pack.get("evidence_classification"),
        },
    }
