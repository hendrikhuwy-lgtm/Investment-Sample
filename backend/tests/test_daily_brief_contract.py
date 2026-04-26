from __future__ import annotations

from app.services.daily_brief_contract import (
    DAILY_BRIEF_CONTRACT_VERSION,
    build_reader_contract_snapshot,
    verify_reader_contract_snapshot,
)
from app.services.daily_brief_execution_contract import build_explanation_card, build_interpretation_card
from app.services.daily_brief_prompt_builders import build_prompt_family_bundle


def _sample_fact_pack() -> dict:
    return {
        "signal_id": "signal_1",
        "signal_title": "US 10Y yield pressure",
        "signal_family": "rates",
        "signal_type": "macro_regime",
        "signal_role": "dominant_signal",
        "observation_date": "2026-03-12",
        "freshness_state": "refreshed_current",
        "freshness_reason_code": "refreshed_current",
        "affected_sleeves": ["ig_bonds", "cash_bills"],
        "affected_markets": ["rates"],
        "raw_metrics": {"metric_value": "4.37", "metric_delta": 0.18, "delta_window": 5},
        "signal_summary_facts": ["US 10Y yields moved higher again into the daily brief run."],
        "why_it_matters_facts": ["Higher long-end rates keep ballast and cash decisions under review."],
        "likely_investment_implication_facts": ["Rate-sensitive sleeves deserve a cautious near-term review."],
        "boundary_facts": ["This does not break the long-horizon ballast role by itself."],
        "review_action_facts": ["Review whether ballast assumptions still match current rate pressure."],
        "holdings_support_facts": ["Live holdings are unavailable, so this remains sleeve-level."],
        "benchmark_support_facts": ["Benchmark proxies moved in the same direction."],
        "scenario_branch_facts": {
            "if_worsens": "If yields keep rising, ballast pressure broadens.",
            "if_stabilizes": "If yields stabilize, the current caution can stay contained.",
            "if_reverses": "If yields reverse lower, current pressure becomes less urgent.",
        },
        "strengthen_read_facts": ["A second rates confirmation would strengthen this read."],
        "weaken_read_facts": ["A quick reversal lower would weaken this read."],
    }


def _sample_schema() -> dict:
    return {
        "forbidden_claims": {"no_holdings_level_claim": True},
    }


def _compliant_reader_payload() -> dict:
    investor_explanation = {
        "signal": "US 10Y yields rose again on 2026-03-12.",
        "meaning": "Rate pressure remains elevated.",
        "investment_implication": "Ballast and cash review deserve more attention.",
        "boundary": "This does not break the long-horizon ballast role by itself.",
        "review_action": "Review whether ballast assumptions still match current rate pressure.",
        "analyst_synthesis": "Rates remain the clearest market pressure today and now matter more than background inflation noise.",
        "system_relevance": "This matters most for IG bonds and cash sleeves because it changes near-term ballast expectations.",
        "scenario_if_worsens": "If yields keep rising, ballast pressure broadens.",
        "scenario_if_stabilizes": "If yields stabilize, the current caution can stay contained.",
        "scenario_if_reverses": "If yields reverse lower, current pressure becomes less urgent.",
        "strengthen_read": "A second rates confirmation would strengthen this read.",
        "weaken_read": "A quick reversal lower would weaken this read.",
        "top_strip_summary": "Rates pressure is the main market development today.",
        "top_strip_implication": "Ballast assumptions deserve closer review, not a larger allocation change.",
        "top_strip_review": "Review IG bonds and cash posture.",
        "top_strip_boundary": "This is a review signal, not a thesis break.",
    }
    return {
        "generated_at": "2026-03-12T08:00:00+00:00",
        "daily_conclusion_block": {"investor_explanation": investor_explanation},
        "top_developments": [
            {
                "metric_code": "DGS10",
                "title": "US 10Y yield pressure",
                "freshness_reason_code": "refreshed_current",
                "investor_explanation": investor_explanation,
            }
        ],
        "what_deserves_review_now": {
            "summary": "Review rate-sensitive sleeves first because rates pressure is still the clearest live market development.",
            "items": ["Review IG bonds and cash posture."],
        },
        "what_stays_in_background": {
            "summary": "Broader macro noise stays in background unless rates pressure starts to spill into other sleeves.",
            "items": ["Keep lower-priority macro noise in background."],
        },
    }


def test_prompt_family_bundle_separates_surface_contracts() -> None:
    bundle = build_prompt_family_bundle(_sample_fact_pack(), _sample_schema())

    assert bundle["top_strip"]["purpose"] == "top_strip_synthesis"
    assert bundle["card"]["purpose"] == "card_synthesis"
    assert bundle["expanded"]["purpose"] == "expanded_synthesis"
    assert bundle["top_strip"]["constraints"]["brevity"] == "very concise"
    assert bundle["card"]["constraints"]["task"].startswith("Write collapsed card synthesis")
    assert bundle["expanded"]["constraints"]["task"].startswith("Write additive deep-read analysis")


def test_interpretation_card_is_the_canonical_daily_brief_meaning_object() -> None:
    card = {
        "signal_id": "signal_1",
        "observation": "US 10Y yields moved higher again.",
        "why_it_matters": "Higher yields keep duration-sensitive sleeves under pressure.",
        "mechanism": "Higher discount rates tighten financing conditions and pressure long-duration assets.",
        "financial_review_question": "Review whether ballast assumptions still hold.",
        "action_tag": "review",
        "portfolio_implication": {"affected_sleeves": ["ig_bonds"], "policy_relevance": "Duration ballast review."},
        "confidence": {"label": "medium"},
    }

    interpretation = build_interpretation_card(card)
    legacy = build_explanation_card(card)

    assert interpretation == legacy
    assert interpretation["what_changed"]
    assert interpretation["what_happened"]
    assert interpretation["why_it_matters"]
    assert interpretation["what_that_usually_means"]
    assert interpretation["economic_mechanism"]
    assert interpretation["portfolio_relevance"]
    assert interpretation["why_it_matters_here"]
    assert interpretation["trust_level"]
    assert interpretation["do_not_overread"]
    assert interpretation["what_to_do_next"]
    assert interpretation["what_would_confirm"]
    assert interpretation["what_would_break"]
    assert interpretation["signal_trust_status"]["overall_trust_level"] in {"HIGH", "MODERATE", "LOW", "VERY_LOW"}
    assert interpretation["interpretive_strength_status"]["interpretation_strength_grade"] in {
        "STRONG",
        "USEFUL_BUT_LIMITED",
        "STORED_CONTEXT_ONLY",
        "BACKGROUND_ONLY",
        "DO_NOT_LEAD",
    }
    assert interpretation["portfolio_mapping_directness_status"]["mapping_strength"]
    assert interpretation["refresh_strength_status"]["refresh_mode"]
    assert interpretation["watch_condition"]
    assert "which" in interpretation["what_that_usually_means"].lower() or "in practice" in interpretation["what_that_usually_means"].lower()


def test_reader_contract_snapshot_verification_requires_new_fields() -> None:
    snapshot = build_reader_contract_snapshot(_compliant_reader_payload())
    compliance = verify_reader_contract_snapshot(snapshot)

    assert compliance["status"] == "ok"
    broken = dict(snapshot)
    broken["what_stays_in_background"] = {}
    broken_compliance = verify_reader_contract_snapshot(broken)
    assert broken_compliance["status"] == "failed_contract_compliance"
    assert "missing_background_summary" in broken_compliance["failures"]


def test_daily_brief_action_state_downgrades_urgent_paths_to_review() -> None:
    interpretation = build_interpretation_card(
        {
            "signal_id": "signal_review",
            "observation": "Rates moved sharply higher.",
            "why_it_matters": "Duration-sensitive sleeves need a calm review.",
            "mechanism": "Higher discount rates raise ballast pressure.",
            "financial_review_question": "Review ballast assumptions, do not trade automatically.",
            "action_tag": "urgent_review",
            "portfolio_implication": {"affected_sleeves": ["ig_bonds"], "policy_relevance": "Ballast review."},
            "confidence": {"label": "medium"},
        }
    )

    assert interpretation["action_state"] == "review"


def test_daily_brief_card_exposes_monitoring_first_language() -> None:
    interpretation = build_interpretation_card(
        {
            "signal_id": "signal_monitor",
            "observation": "FX moved modestly.",
            "why_it_matters": "FX is worth monitoring, but it does not justify portfolio churn.",
            "mechanism": "Currency moves can affect translation and implementation context.",
            "financial_review_question": "Monitor whether FX pressure persists.",
            "action_tag": "monitor",
            "portfolio_implication": {"affected_sleeves": ["global_equity_core"], "policy_relevance": "Monitoring only."},
            "confidence": {"label": "low"},
        }
    )

    assert interpretation["action_state"] in {"ignore", "monitor"}
    assert "do not overread" in str(interpretation["do_not_overread"]).lower()
    assert any(word in str(interpretation["what_to_do_next"]).lower() for word in {"monitor", "review"})


def test_daily_brief_lens_context_is_explanatory_only_and_preserves_review_ceiling() -> None:
    interpretation = build_interpretation_card(
        {
            "signal_id": "signal_rates",
            "observation": "Rates moved higher.",
            "why_it_matters": "Duration pressure is back in focus.",
            "mechanism": "Higher yields tighten financing conditions.",
            "financial_review_question": "Review ballast posture, but do not trade automatically.",
            "action_tag": "urgent_review",
            "mapping_mode": "benchmark_watch_proxy",
            "freshness_reason_code": "refresh_failed_used_cache",
            "portfolio_implication": {"affected_sleeves": ["ig_bonds"], "policy_relevance": "Ballast review."},
            "confidence": {"label": "medium"},
        }
    )

    lens_context = dict(interpretation.get("lens_context") or {})
    review_context = dict(interpretation.get("review_intensity_context") or {})

    assert interpretation["action_state"] == "review"
    assert lens_context.get("overall_posture") in {"monitoring_only", "review_only"}
    assert "Cycle and risk posture" in str(dict(lens_context.get("marks_cycle_risk") or {}).get("summary") or "")
    assert "Regime and transmission context" in str(dict(lens_context.get("dalio_regime_transmission") or {}).get("summary") or "")
    assert review_context.get("review_intensity_modifier") in {"none", "raise_to_universal"}
