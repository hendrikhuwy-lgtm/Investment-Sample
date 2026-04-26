from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from app.v2.features.daily_brief_contingent_driver_builder import build_contingent_drivers
from app.v2.features.daily_brief_effect_classifier import classify_effect
from app.v2.features.daily_brief_explanation_builder import build_source_context
from app.v2.features.daily_brief_decision_synthesis import synthesize_daily_brief_decisions


def _signal(
    symbol: str,
    label: str,
    magnitude: str,
    direction: str,
    *,
    source_family: str = "market_close",
    source_type: str | None = None,
    current_value: float = 102.0,
    previous_value: float = 100.0,
    as_of: str = "2026-04-08T00:00:00+00:00",
) -> dict[str, object]:
    return {
        "signal_id": f"signal_{symbol}",
        "label": label,
        "symbol": symbol,
        "signal_kind": "market",
        "direction": direction,
        "magnitude": magnitude,
        "summary": f"{label} moved.",
        "implication": f"{label} matters.",
        "confirms": f"{label} confirms.",
        "breaks": f"{label} breaks.",
        "mapping_directness": "sleeve-proxy",
        "affected_sleeves": [],
        "affected_holdings": [],
        "affected_candidates": [],
        "current_value": current_value,
        "as_of": as_of,
        "history": [previous_value, current_value],
        "runtime_provenance": {
            "provider_used": "yahoo_finance",
            "source_family": source_family,
            "freshness": "current",
            "usable_truth": True,
        },
        "source_type": source_type,
    }


def _news_signal(
    label: str,
    *,
    as_of: str = "2026-04-09T00:00:00+00:00",
) -> dict[str, object]:
    return {
        "signal_id": f"signal_news_{label[:12]}",
        "label": label,
        "symbol": "",
        "signal_kind": "news",
        "direction": "up",
        "magnitude": "moderate",
        "summary": f"{label} matters.",
        "implication": f"{label} changes the brief.",
        "confirms": "The theme confirms if follow-through persists.",
        "breaks": "The theme breaks if markets fade the headline.",
        "mapping_directness": "macro-only",
        "affected_sleeves": [],
        "affected_holdings": [],
        "affected_candidates": [],
        "current_value": None,
        "as_of": as_of,
        "history": [],
        "runtime_provenance": {
            "provider_used": "Reuters",
            "source_family": "news",
            "freshness": "current",
            "usable_truth": True,
        },
    }


def test_decision_synthesis_prefers_distinct_effect_groups_and_emits_titles(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    signals = [
        _signal("CL=F", "WTI Crude", "significant", "down"),
        _signal("BZ=F", "Brent Crude", "significant", "down"),
        _signal("DXY", "FX / USD", "significant", "down"),
        _signal("CPI_YOY", "Inflation", "moderate", "up"),
        _signal("BAMLH0A0HYM2", "Credit", "moderate", "down"),
    ]

    decision_bundle = synthesize_daily_brief_decisions(
        signals,
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
        primary_limit=4,
        contingent_limit=3,
    )

    primary = decision_bundle["primary_drivers"]
    assert primary
    assert len({item["duplication_group"] for item in primary}) == len(primary)
    assert primary[0]["decision_title"]
    assert primary[0]["short_title"]
    assert primary[0]["short_subtitle"]
    assert primary[0]["why_it_matters_macro"]
    assert primary[0]["why_it_matters_short_term"]
    assert any(item["duplication_group"] == "inflation_real_assets" for item in decision_bundle["drivers"])
    assert any(item["primary_effect_bucket"] == "credit" for item in decision_bundle["drivers"])
    assert any(item["primary_effect_bucket"] == "dollar_fx" for item in decision_bundle["drivers"])


def test_energy_driver_stays_monitor_without_holdings_overlay(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    decision_bundle = synthesize_daily_brief_decisions(
        [_signal("CL=F", "WTI Crude", "significant", "down")],
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
        primary_limit=3,
        contingent_limit=2,
    )

    assert decision_bundle["primary_drivers"][0]["next_action"] == "Monitor"


def test_primary_drivers_are_rebuilt_with_significant_wave_b_support(monkeypatch) -> None:
    call_depths: list[str] = []

    def _fake_support(*args, **kwargs):
        scenario_depth = str(kwargs.get("scenario_depth") or "coverage")
        call_depths.append(scenario_depth)
        summary = f"{scenario_depth} scenario summary"
        return {
            "monitoring_condition": {
                "near_term_trigger": f"{scenario_depth} near-term trigger",
                "thesis_trigger": f"{scenario_depth} thesis trigger",
                "forecast_support": {"scenario_support_strength": "strong"},
                "path_risk_note": f"{scenario_depth} path risk",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": summary,
                "scenarios": [
                    {
                        "scenario_name": "Bull",
                        "scenario_likelihood_pct": 34 if scenario_depth == "significant" else 33,
                        "support_strength": summary,
                    }
                ],
                "forecast_support": {"scenario_support_strength": "strong"},
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                    scenario_support_strength="strong",
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    decision_bundle = synthesize_daily_brief_decisions(
        [
            _signal("BAMLH0A0HYM2", "Credit", "significant", "down"),
            _signal("^990100-USD-STRD", "World Equity", "significant", "up"),
        ],
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
        primary_limit=2,
        contingent_limit=2,
    )

    primary = decision_bundle["primary_drivers"]
    assert primary
    assert "significant" in call_depths
    assert any(depth == "coverage" for depth in call_depths)
    assert all(item["scenario_block"]["summary"] == "significant scenario summary" for item in primary)
    assert all(item["scenarios"][0]["scenario_likelihood_pct"] == 34 for item in primary)


def test_contingent_drivers_use_two_pass_selection_and_investor_status_labels() -> None:
    contingent = build_contingent_drivers(
        [
            {
                "signal_id": "signal_duration_watch",
                "decision_title": "Rates watch",
                "label": "Rates watch",
                "effect_type": "rates_duration_effect",
                "interpretation_subtitle": "Rates remain central to the brief.",
                "near_term_trigger": "10Y real yield pushes through the current watch line.",
                "thesis_trigger": "The broader duration hurdle keeps tightening.",
                "breaks": "The duration hurdle breaks if rates stop confirming.",
                "portfolio_consequence": "Bond adds stay patient.",
                "next_action": "Monitor",
                "confidence_class": "medium",
                "sufficiency_state": "sufficient",
                "source_kind": "official_release",
                "duplication_group": "rates_duration",
                "path_risk_note": "Forecast support is strong and threshold pressure is rising.",
                "primary_effect_bucket": "duration",
                "magnitude": "moderate",
                "signal_support_class": "strong",
                "actionability_class": "contextual_monitor",
                "affected_sleeves": ["sleeve_ig_bonds"],
                "affected_candidates": ["A35", "AGGU", "VAGU"],
                "forecast_support": {
                    "trigger_pressure": 0.82,
                    "persistence_score": 0.74,
                    "cross_asset_confirmation_score": 0.71,
                    "scenario_support_strength": "strong",
                    "uncertainty_width_label": "moderate",
                    "escalation_flag": True,
                },
                "monitoring_condition": {
                    "trigger_support": {
                        "threshold_state": "watch",
                    }
                },
            },
            {
                "signal_id": "signal_fx_break",
                "decision_title": "Dollar watch",
                "label": "Dollar watch",
                "effect_type": "fx_effect",
                "interpretation_subtitle": "Dollar firmness is starting to matter for global risk again.",
                "near_term_trigger": "Dollar index clears the current review band.",
                "thesis_trigger": "The global hurdle stays high for cross-border risk.",
                "breaks": "The dollar read breaks if rates and breadth stop confirming.",
                "portfolio_consequence": "Global risk adds stay selective.",
                "next_action": "Monitor",
                "confidence_class": "medium",
                "sufficiency_state": "bounded",
                "source_kind": "market_close",
                "duplication_group": "dollar_fx",
                "path_risk_note": "Threshold is through the line but broader confirmation is still light.",
                "primary_effect_bucket": "dollar_fx",
                "magnitude": "moderate",
                "signal_support_class": "moderate",
                "actionability_class": "contextual_monitor",
                "affected_sleeves": ["sleeve_global_equity_core"],
                "forecast_support": {
                    "trigger_pressure": 0.71,
                    "persistence_score": 0.58,
                    "fade_risk": 0.32,
                    "cross_asset_confirmation_score": 0.22,
                    "scenario_support_strength": "moderate",
                    "uncertainty_width_label": "wide",
                    "escalation_flag": False,
                },
                "monitoring_condition": {
                    "trigger_support": {
                        "threshold_state": "breached",
                    }
                },
            },
            {
                "signal_id": "signal_credit_watch",
                "decision_title": "Credit watch",
                "label": "Credit watch",
                "effect_type": "credit_effect",
                "interpretation_subtitle": "Funding conditions could still tighten further if spreads keep widening.",
                "near_term_trigger": "Credit holds near current levels while breadth stays narrow.",
                "thesis_trigger": "Carry remains selective and risk appetite stays constrained.",
                "breaks": "The credit restraint breaks if spreads retrace and breadth broadens.",
                "portfolio_consequence": "Carry stays selective and the risk budget remains tighter.",
                "next_action": "Monitor",
                "confidence_class": "medium",
                "sufficiency_state": "sufficient",
                "source_kind": "official_release",
                "duplication_group": "credit_liquidity",
                "path_risk_note": "Pressure is building but not yet decisive.",
                "primary_effect_bucket": "credit",
                "magnitude": "moderate",
                "signal_support_class": "moderate",
                "actionability_class": "contextual_monitor",
                "affected_sleeves": ["sleeve_ig_bonds", "sleeve_cash_bills"],
                "forecast_support": {
                    "trigger_pressure": 0.44,
                    "persistence_score": 0.53,
                    "fade_risk": 0.34,
                    "cross_asset_confirmation_score": 0.47,
                    "scenario_support_strength": "moderate",
                    "uncertainty_width_label": "moderate",
                    "escalation_flag": False,
                },
                "monitoring_condition": {
                    "trigger_support": {
                        "threshold_state": "watch",
                    }
                },
            },
        ],
        limit=5,
    )

    assert len(contingent) == 3
    assert contingent[0]["current_status"] == "near_trigger"
    assert contingent[0]["support_label"] == "rising trigger pressure"
    assert contingent[0]["why_it_matters_now"] == "Higher short term yields make it less attractive to buy longer bonds right now."
    assert contingent[0]["what_changes_if_confirmed"].startswith(
        "If this holds, safer bond holdings in Ig Bonds stay more attractive"
    )
    assert contingent[0]["portfolio_consequence"].startswith("For Ig Bonds, keep bond posture patient")
    assert contingent[0]["portfolio_consequence"].count("A35, AGGU, VAGU") == 1
    assert contingent[1]["current_status"] == "triggered_but_unconfirmed"
    assert contingent[1]["support_label"] == "breached but not yet confirmed"
    assert contingent[2]["current_status"] == "near_trigger"
    assert contingent[2]["what_to_watch_next"] == (
        "Look for credit spreads and stock breadth to confirm; treat it as fading if spreads retrace quickly."
    )


def test_contingent_drivers_fold_near_duplicate_rate_cards_into_supporting_lines() -> None:
    contingent = build_contingent_drivers(
        [
            {
                "signal_id": "signal_duration_primary",
                "decision_title": "Rates watch",
                "label": "Rates watch",
                "effect_type": "rates_duration_effect",
                "interpretation_subtitle": "Rates remain central to the brief.",
                "near_term_trigger": "10Y real yield pushes through the current watch line.",
                "thesis_trigger": "The broader duration hurdle keeps tightening.",
                "breaks": "The duration hurdle fades if rates stop confirming.",
                "portfolio_consequence": "Bond adds stay patient.",
                "implementation_sensitivity": "Favor higher-quality ballast over aggressive extension.",
                "next_action": "Monitor",
                "confidence_class": "medium",
                "sufficiency_state": "sufficient",
                "source_kind": "official_release",
                "duplication_group": "rates_duration",
                "path_risk_note": "Forecast support is strong and threshold pressure is rising.",
                "primary_effect_bucket": "duration",
                "magnitude": "moderate",
                "signal_support_class": "strong",
                "actionability_class": "contextual_monitor",
                "affected_sleeves": ["sleeve_ig_bonds"],
                "forecast_support": {
                    "trigger_pressure": 0.82,
                    "persistence_score": 0.74,
                    "cross_asset_confirmation_score": 0.71,
                    "scenario_support_strength": "strong",
                    "uncertainty_width_label": "moderate",
                    "escalation_flag": True,
                },
                "monitoring_condition": {"trigger_support": {"threshold_state": "watch"}},
            },
            {
                "signal_id": "signal_duration_related",
                "decision_title": "Real yield watch",
                "label": "Real yield watch",
                "effect_type": "rates_duration_effect",
                "interpretation_subtitle": "Rates remain central to the brief.",
                "near_term_trigger": "30Y real yield pushes through the current watch line.",
                "thesis_trigger": "The broader duration hurdle keeps tightening.",
                "breaks": "The duration hurdle fades if rates stop confirming.",
                "portfolio_consequence": "Bond adds stay patient.",
                "implementation_sensitivity": "Favor higher-quality ballast over aggressive extension.",
                "next_action": "Monitor",
                "confidence_class": "medium",
                "sufficiency_state": "sufficient",
                "source_kind": "official_release",
                "duplication_group": "rates_duration",
                "path_risk_note": "Forecast support is strong and threshold pressure is rising.",
                "primary_effect_bucket": "duration",
                "magnitude": "moderate",
                "signal_support_class": "strong",
                "actionability_class": "contextual_monitor",
                "affected_sleeves": ["sleeve_ig_bonds"],
                "forecast_support": {
                    "trigger_pressure": 0.78,
                    "persistence_score": 0.7,
                    "cross_asset_confirmation_score": 0.68,
                    "scenario_support_strength": "strong",
                    "uncertainty_width_label": "moderate",
                    "escalation_flag": False,
                },
                "monitoring_condition": {"trigger_support": {"threshold_state": "watch"}},
            },
        ],
        limit=5,
    )

    assert len(contingent) == 1
    assert contingent[0]["trigger_title"] == "Bond-yield pressure is near the line"
    assert contingent[0]["supporting_lines"] == ["Real yield watch: more convincing if yields keep holding the move."]


def test_contingent_titles_use_family_led_watch_labels() -> None:
    contingent = build_contingent_drivers(
        [
            {
                "signal_id": "signal_duration_watch",
                "decision_title": "Bond sleeve rate pressure is changing the decision frame",
                "label": "UST 2Y",
                "effect_type": "rates_duration_effect",
                "interpretation_subtitle": "Rates remain central to the brief.",
                "near_term_trigger": "UST 2Y tests the watch line.",
                "thesis_trigger": "Short rates keep the hurdle high.",
                "breaks": "The hurdle breaks if rates stop confirming.",
                "portfolio_consequence": "Bond adds stay delayed.",
                "next_action": "Monitor",
                "confidence_class": "medium",
                "sufficiency_state": "sufficient",
                "source_kind": "official_release",
                "duplication_group": "rates_duration",
                "path_risk_note": "Forecast support is strong and threshold pressure is rising.",
                "primary_effect_bucket": "duration",
                "magnitude": "moderate",
                "signal_support_class": "strong",
                "actionability_class": "contextual_monitor",
                "affected_sleeves": ["sleeve_ig_bonds"],
                "forecast_support": {
                    "trigger_pressure": 0.78,
                    "persistence_score": 0.7,
                    "cross_asset_confirmation_score": 0.68,
                    "scenario_support_strength": "strong",
                    "uncertainty_width_label": "moderate",
                    "escalation_flag": False,
                },
                "monitoring_condition": {"trigger_support": {"threshold_state": "watch"}},
            },
            {
                "signal_id": "signal_policy_watch",
                "decision_title": "Policy path is steering the current portfolio read",
                "label": "Fed Funds",
                "effect_type": "rates_duration_effect",
                "interpretation_subtitle": "Policy remains central to the brief.",
                "near_term_trigger": "Fed Funds tests the watch line.",
                "thesis_trigger": "Policy keeps relief delayed.",
                "breaks": "The policy hurdle breaks if relief arrives.",
                "portfolio_consequence": "Risk adds stay patient.",
                "next_action": "Monitor",
                "confidence_class": "medium",
                "sufficiency_state": "sufficient",
                "source_kind": "official_release",
                "duplication_group": "policy_path",
                "path_risk_note": "Forecast support is moderate and threshold pressure is rising.",
                "primary_effect_bucket": "policy",
                "magnitude": "moderate",
                "signal_support_class": "moderate",
                "actionability_class": "contextual_monitor",
                "affected_sleeves": ["sleeve_cash_bills"],
                "forecast_support": {
                    "trigger_pressure": 0.74,
                    "persistence_score": 0.62,
                    "cross_asset_confirmation_score": 0.55,
                    "scenario_support_strength": "moderate",
                    "uncertainty_width_label": "moderate",
                    "escalation_flag": False,
                },
                "monitoring_condition": {"trigger_support": {"threshold_state": "watch"}},
            },
            {
                "signal_id": "signal_volatility_watch",
                "decision_title": "Volatility regime is testing risk tolerance",
                "label": "VIX",
                "effect_type": "market_effect",
                "interpretation_subtitle": "Volatility remains central to the brief.",
                "near_term_trigger": "VIX tests the watch line.",
                "thesis_trigger": "Stress broadens across markets.",
                "breaks": "The stress read breaks if volatility fades.",
                "portfolio_consequence": "Risk adds stay selective.",
                "next_action": "Monitor",
                "confidence_class": "medium",
                "sufficiency_state": "sufficient",
                "source_kind": "market_close",
                "duplication_group": "volatility_watch",
                "path_risk_note": "Forecast support is moderate and threshold pressure is rising.",
                "primary_effect_bucket": "volatility",
                "magnitude": "moderate",
                "signal_support_class": "moderate",
                "actionability_class": "contextual_monitor",
                "affected_sleeves": ["sleeve_global_equity_core"],
                "forecast_support": {
                    "trigger_pressure": 0.72,
                    "persistence_score": 0.6,
                    "cross_asset_confirmation_score": 0.51,
                    "scenario_support_strength": "moderate",
                    "uncertainty_width_label": "moderate",
                    "escalation_flag": False,
                },
                "monitoring_condition": {"trigger_support": {"threshold_state": "watch"}},
            },
        ],
        limit=5,
    )

    titles = [item["trigger_title"] for item in contingent]
    assert "Short-term rate pressure is near the line" in titles
    assert "Rate relief is near the line" in titles
    assert "Market stress is near the line" in titles


def test_support_stack_deduplicates_same_consequence_group(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    signals = [
        _signal("MORTGAGE30US", "30Y Mortgage", "significant", "up", source_type="official_release", source_family="macro_market_state"),
        _signal("DGS2", "UST 2Y", "moderate", "up", source_type="official_release", source_family="macro_market_state"),
        _signal("^TNX", "Rates", "moderate", "up"),
        _signal("CPI_YOY", "Inflation", "moderate", "up", source_type="official_release", source_family="macro_market_state"),
        _signal("DXY", "FX / USD", "moderate", "down"),
    ]

    decision_bundle = synthesize_daily_brief_decisions(
        signals,
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
        primary_limit=5,
        contingent_limit=3,
    )

    support = decision_bundle["support_drivers"]
    assert support
    assert len(support) < len(decision_bundle["drivers"])
    duration_titles = [item["decision_title"] for item in support if item["duplication_group"] == "rates_duration"]
    assert len(duration_titles) == 1


def test_macro_release_context_uses_release_metadata_even_if_signal_kind_is_market() -> None:
    signal = _signal(
        "CPI_YOY",
        "Inflation",
        "moderate",
        "down",
        source_family="macro_market_state",
        source_type="official_release",
        current_value=2.43,
        previous_value=2.66,
    )
    signal["reference_period"] = "2026-02"
    signal["release_date"] = "2026-03-12"
    effect = classify_effect(signal)

    context = build_source_context(
        signal,
        effect,
        holdings_overlay_present=False,
    )

    assert context["source_class"] == "macro_release"
    assert context["release_date"] == "2026-03-12"
    assert "2.43" in context["what_changed"]
    assert "2.66" in context["what_changed"]


def test_effect_classifier_preserves_candidate_mapping_when_holdings_absent() -> None:
    signal = _signal("MORTGAGE30US", "30Y Mortgage", "significant", "up")
    signal["affected_candidates"] = ["AGGU", "VAGU"]

    effect = classify_effect(signal)

    assert effect["mapping_scope"] == "sleeve"
    assert effect["affected_candidates"] == ["AGGU", "VAGU"]


def test_wti_explanation_adds_current_timing_evidence_and_failure_mode(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
                "trigger_support": {"threshold_state": "watch"},
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="weak",
                    provider="chronos",
                    degraded_state=None,
                    confidence_summary="bounded",
                ),
                result=SimpleNamespace(
                    anomaly_score=0.6,
                    direction="negative",
                    confidence_band="wide",
                ),
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    signal = _signal(
        "CL=F",
        "WTI Crude",
        "significant",
        "down",
        source_type="market_close",
        current_value=97.74,
        previous_value=112.95,
    )
    signal["affected_candidates"] = ["CMOD", "IWDP", "SGLN"]

    decision_bundle = synthesize_daily_brief_decisions(
        [signal],
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
        primary_limit=3,
        contingent_limit=2,
    )

    driver = decision_bundle["primary_drivers"][0]
    assert "WTI Crude closed" in driver["what_changed_today"]
    assert driver["evidence_class"] == "public_verified_close"
    assert "WTI Crude 97.74" in driver["evidence_title"]
    assert "fresh in current brief window" in driver["evidence_title"]
    assert "It matters again today because" in driver["why_now_not_before"]
    assert "mean-revert quickly" in driver["why_this_could_be_wrong"]
    assert "hedge timing and sequencing" in driver["implementation_sensitivity"]
    assert "Only if the energy move persists" in driver["why_it_matters_long_term"]
    assert "Forecast support is weak" in driver["path_risk_note"]
    assert driver["do_not_overread"].startswith("This move is not enough by itself")
    assert "inflation pass-through" not in driver["why_it_matters_economically"].lower()
    assert "hedge exposure" not in driver["why_it_matters_economically"].lower()
    assert driver["interpretation_subtitle"] == (
        "Oil is still high enough to keep inflation protection relevant, but not enough on its own to justify a stand-alone commodity trade."
    )
    assert driver["why_it_matters_economically"].endswith(
        "If this holds, inflation protection stays relevant longer than a quick return to easier bond conditions."
    )
    assert "CMOD, IWDP, SGLN become the main ETF choices if confirmation improves." in driver["portfolio_and_sleeve_meaning"]
    assert "real assets become more usable as protection" in driver["portfolio_and_sleeve_meaning"].lower()
    assert "quick bond relief stays less likely" in driver["portfolio_and_sleeve_meaning"].lower()
    assert "still is not a stand-alone oil trade call" in driver["portfolio_and_sleeve_meaning"].lower()
    assert driver["decision_status"] != "active_read"
    assert driver["decision_status"] != "watch_trigger"


def test_equity_explanation_uses_investor_language_and_explicit_cross_asset_conditions(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Escalate review if world equity reaches the near-term threshold.",
                "thesis_trigger": "Reset the view if breadth fails to confirm.",
                "trigger_support": {"threshold_state": "breached"},
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="moderate",
                    provider="chronos",
                    degraded_state=None,
                    confidence_summary="usable",
                ),
                result=SimpleNamespace(
                    anomaly_score=0.34,
                    direction="positive",
                    confidence_band="wide",
                ),
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    signal = _signal(
        "^990100-USD-STRD",
        "World Equity",
        "significant",
        "up",
        current_value=4458.4,
        previous_value=4327.7,
    )
    signal["affected_candidates"] = ["CSPX", "IWDA", "SSAC"]

    decision_bundle = synthesize_daily_brief_decisions(
        [signal],
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
        primary_limit=3,
        contingent_limit=2,
    )

    driver = decision_bundle["primary_drivers"][0]
    economic = driver["why_it_matters_economically"].lower()
    portfolio = driver["portfolio_and_sleeve_meaning"]
    assert "changed the daily decision map" not in economic
    assert "watched threshold" not in economic
    assert "breadth" in economic
    assert "credit" in economic
    assert "rates" in economic
    assert "if this holds" in economic
    assert driver["interpretation_subtitle"] == (
        "The move is helping risk appetite, but broader confirmation is still needed before bigger equity adds look credible."
    )
    assert "CSPX, IWDA, SSAC become the main ETF choices if confirmation improves." in portfolio
    assert "selective equity exposure becomes more usable" in portfolio.lower()
    assert "broad equity adds should stay delayed" in portfolio.lower()
    assert "still is not enough for a broad equity risk reset" in portfolio.lower()
    assert driver["confirm_condition"].startswith("Look for breadth to widen")
    assert "credit, rates, and fx" in driver["confirm_condition"].lower()
    assert driver["weaken_condition"].startswith("If breadth narrows again")
    assert driver["break_condition"].startswith("If the rally reverses")
    assert "regime" not in economic


def test_macro_release_explanation_keeps_fact_and_inference_separate(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="bounded",
                    provider="chronos",
                    degraded_state=None,
                    confidence_summary="bounded",
                ),
                result=SimpleNamespace(
                    anomaly_score=0.2,
                    direction="mixed",
                    confidence_band="bounded",
                ),
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    signal = _signal(
        "CPI_YOY",
        "Inflation",
        "moderate",
        "down",
        source_family="macro_market_state",
        source_type="official_release",
        current_value=2.43,
        previous_value=2.66,
    )
    signal["reference_period"] = "2026-02"
    signal["release_date"] = "2026-03-12"

    decision_bundle = synthesize_daily_brief_decisions(
        [signal],
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
        primary_limit=3,
        contingent_limit=2,
    )

    driver = decision_bundle["primary_drivers"][0]
    assert "official reading printed 2.43 versus 2.66" in driver["what_changed_today"]
    assert driver["evidence_class"] == "official_release"
    assert "latest valid official print" in driver["evidence_title"]
    assert driver["source_and_validity"] == "Official release. Status: latest valid official print."


def test_short_display_fields_are_concise_and_action_relevant(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    decision_bundle = synthesize_daily_brief_decisions(
        [
            _signal(
                "MORTGAGE30US",
                "30Y Mortgage",
                "significant",
                "up",
                source_type="official_release",
                source_family="macro_market_state",
            )
        ],
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
        primary_limit=3,
        contingent_limit=2,
    )

    lead = decision_bundle["primary_drivers"][0]
    assert lead["short_title"] == "Duration timing stays constrained"
    assert lead["short_subtitle"] == "Higher financing costs keep bond adds patient."


def test_fresh_policy_and_global_news_get_first_class_group_visibility(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    signals = [
        _signal(
            "MORTGAGE30US",
            "30Y Mortgage",
            "significant",
            "up",
            source_type="official_release",
            source_family="macro_market_state",
        ),
        _signal(
            "BAMLH0A0HYM2",
            "Credit",
            "moderate",
            "down",
            source_type="official_release",
            source_family="macro_market_state",
        ),
        _news_signal("US tariff headlines pressure global trade expectations"),
        _news_signal("Iran shipping risk keeps oil markets on edge"),
    ]

    decision_bundle = synthesize_daily_brief_decisions(
        signals,
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
        primary_limit=5,
        contingent_limit=3,
    )

    groups = {group["group_id"]: group for group in decision_bundle["signal_stack_groups"]}
    assert groups == {}
    regime = {item["aspect_bucket"]: item for item in decision_bundle["regime_context_drivers"]}
    assert regime["policy_release"]["visibility_role"] == "backdrop"
    assert regime["geopolitical_global_news"]["visibility_role"] == "backdrop"
    assert regime["policy_release"]["short_title"] == "Tariff risk stays in play"
    assert regime["geopolitical_global_news"]["short_title"] == "Middle East risk stays live"
    assert "reported, market confirmation still none" in regime["policy_release"]["evidence_title"]
    assert "reported, market confirmation still none" in regime["geopolitical_global_news"]["evidence_title"]


def test_old_macro_release_moves_to_regime_context_until_reactivated(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    stale_inflation = _signal(
        "CPI_YOY",
        "Inflation",
        "moderate",
        "up",
        source_family="macro_market_state",
        source_type="official_release",
        current_value=2.43,
        previous_value=2.66,
    )
    stale_inflation["release_date"] = "2026-03-12"
    stale_inflation["availability_date"] = "2026-03-12"
    stale_inflation["reference_period"] = "2026-02"

    fresh_credit = _signal("BAMLH0A0HYM2", "Credit", "significant", "down")

    decision_bundle = synthesize_daily_brief_decisions(
        [stale_inflation, fresh_credit],
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
    )

    assert all(item["label"] != "Inflation" for item in decision_bundle["primary_drivers"])
    assert any(item["label"] == "Inflation" for item in decision_bundle["regime_context_drivers"])


def test_signal_stack_groups_emit_remaining_secondary_drivers(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    signals = [
        _signal("MORTGAGE30US", "30Y Mortgage", "significant", "up", source_type="official_release", source_family="macro_market_state"),
        _signal("DXY", "FX / USD", "moderate", "down"),
        _signal("BAMLH0A0HYM2", "Credit", "moderate", "down"),
        _signal("CL=F", "WTI Crude", "moderate", "up"),
        _signal("CPI_YOY", "Inflation", "moderate", "up", source_type="official_release", source_family="macro_market_state"),
    ]

    decision_bundle = synthesize_daily_brief_decisions(
        signals,
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
    )

    primary_ids = {item["signal_id"] for item in decision_bundle["primary_drivers"]}
    support_ids = {item["signal_id"] for item in decision_bundle["support_drivers"]}
    assert support_ids
    assert not (primary_ids & support_ids)
    assert decision_bundle["signal_stack_groups"]
    first_group = decision_bundle["signal_stack_groups"][0]
    assert first_group["representative"] is not None
    assert first_group["count"] == len(first_group["signals"])
    assert all(signal.get("visibility_role") for signal in first_group["signals"])
    assert all(signal.get("aspect_bucket") for signal in first_group["signals"])


def test_decision_synthesis_emits_short_titles_and_subtitles(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    decision_bundle = synthesize_daily_brief_decisions(
        [_signal("MORTGAGE30US", "30Y Mortgage", "significant", "up", source_type="official_release", source_family="macro_market_state")],
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
    )

    driver = decision_bundle["primary_drivers"][0]
    assert driver["short_title"] == "Duration timing stays constrained"
    assert "bond" in driver["short_subtitle"].lower() or "duration" in driver["short_subtitle"].lower()


def test_category_floor_surfaces_missing_active_aspect_as_representative(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    signals = [
        _signal("MORTGAGE30US", "30Y Mortgage", "significant", "up", source_type="official_release", source_family="macro_market_state"),
        _signal("BAMLH0A0HYM2", "Credit", "significant", "down"),
        _signal("^VIX", "VIX", "moderate", "up"),
    ]

    decision_bundle = synthesize_daily_brief_decisions(
        signals,
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
    )

    backdrop = decision_bundle["regime_context_drivers"]
    vix_backdrop = next(item for item in backdrop if item["label"] == "VIX")
    assert vix_backdrop["aspect_bucket"] == "volatility_risk_regime"
    assert vix_backdrop["visibility_role"] == "backdrop"
    assert vix_backdrop["coverage_reason"] == "regime_context"


def test_fresh_geopolitical_news_surfaces_as_first_class_aspect(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="bounded",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    signals = [
        _signal("MORTGAGE30US", "30Y Mortgage", "significant", "up", source_type="official_release", source_family="macro_market_state"),
        _signal("BAMLH0A0HYM2", "Credit", "significant", "down"),
        _news_signal("Dollar wobbles as fragile US-Iran ceasefire keeps markets on edge - Reuters"),
    ]

    decision_bundle = synthesize_daily_brief_decisions(
        signals,
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
    )

    news_backdrop = next(item for item in decision_bundle["regime_context_drivers"] if item["aspect_bucket"] == "geopolitical_global_news")
    assert news_backdrop["visibility_role"] == "backdrop"
    assert news_backdrop["short_title"] == "Middle East risk stays live"
    assert news_backdrop["evidence_title"].endswith("market confirmation still none")


def test_news_event_cluster_metadata_carries_confirmation_triggers(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="bounded",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    now = datetime.now(timezone.utc).isoformat()
    decision_bundle = synthesize_daily_brief_decisions(
        [_news_signal("Iran shipping risk keeps oil markets on edge - Reuters", as_of=now)],
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
    )

    visible = [
        *decision_bundle["primary_drivers"],
        *decision_bundle["support_drivers"],
        *decision_bundle["regime_context_drivers"],
    ]
    news_driver = next(item for item in visible if item.get("event_cluster_id"))
    assert news_driver["event_subtype"] == "middle_east_security"
    assert "Brent/WTI" in news_driver["confirmation_assets"]
    assert "Escalates" in news_driver["short_subtitle"]


def test_news_event_cluster_is_assigned_to_one_visible_slot(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="bounded",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    now = datetime.now(timezone.utc).isoformat()
    decision_bundle = synthesize_daily_brief_decisions(
        [
            _signal("MORTGAGE30US", "30Y Mortgage", "significant", "up", source_type="official_release", source_family="macro_market_state", as_of=now),
            _news_signal("Iran shipping risk keeps oil markets on edge - Reuters", as_of=now),
            _news_signal("Iran ceasefire uncertainty keeps risk assets cautious - Reuters", as_of=now),
        ],
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
    )

    visible = [
        *decision_bundle["primary_drivers"],
        *decision_bundle["support_drivers"],
        *decision_bundle["regime_context_drivers"],
    ]
    clusters = [str(item.get("event_cluster_id") or "") for item in visible if item.get("event_cluster_id")]
    assert clusters
    assert len(clusters) == 1
    assert len(clusters) == len(set(clusters))


def test_secondary_visibility_prefers_qualified_aspect_coverage_over_generic_market_leftovers(monkeypatch) -> None:
    def _fake_support(*args, **kwargs):
        return {
            "monitoring_condition": {
                "near_term_trigger": "Near-term trigger",
                "thesis_trigger": "Thesis trigger",
            },
            "scenario_block": {
                "label": "Scenario watch",
                "summary": "Scenario summary",
                "scenarios": [],
            },
            "bundle": SimpleNamespace(
                support=SimpleNamespace(
                    support_strength="strong",
                    provider="chronos",
                    degraded_state=None,
                )
            ),
        }

    monkeypatch.setattr(
        "app.v2.features.daily_brief_decision_synthesis.build_signal_support_bundle",
        _fake_support,
    )

    signals = [
        _signal("MORTGAGE30US", "30Y Mortgage", "significant", "up", source_type="official_release", source_family="macro_market_state"),
        _signal("SPY", "S&P 500", "moderate", "down"),
        _signal("^VIX", "VIX", "moderate", "up"),
        _signal("DXY", "FX / USD", "moderate", "up"),
    ]

    decision_bundle = synthesize_daily_brief_decisions(
        signals,
        review_posture="Review: manual review remains available.",
        why_here="Portfolio context.",
        holdings_overlay_present=False,
    )

    support_labels = {item["label"] for item in decision_bundle["support_drivers"]}
    assert not support_labels
    assert "S&P 500" not in support_labels
    assert any(item["label"] == "VIX" for item in decision_bundle["regime_context_drivers"])
