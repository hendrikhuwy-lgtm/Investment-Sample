from __future__ import annotations

from types import SimpleNamespace

import pytest


class _DictNamespace(SimpleNamespace):
    def to_dict(self) -> dict[str, object]:
        return dict(self.__dict__)


def _fake_bundle(
    *,
    direction: str = "positive",
    confidence_band: str = "tight",
    anomaly_score: float = 0.22,
    support_strength: str = "strong",
) -> object:
    support = _DictNamespace(
        provider="chronos",
        model_name="chronos-bolt",
        horizon=10,
        support_strength=support_strength,
        confidence_summary="Tight interval support",
        degraded_state=None,
        generated_at="2026-04-09T00:00:00+00:00",
    )
    result = SimpleNamespace(
        confidence_band=confidence_band,
        anomaly_score=anomaly_score,
        direction=direction,
    )
    scenario_support = SimpleNamespace(
        bull_case={"label": "Bull case", "summary": "Supportive follow-through stays available.", "expected_path": "Supportive follow-through"},
        base_case={"label": "Base case", "summary": "The current read stays bounded.", "expected_path": "Bounded hold"},
        bear_case={"label": "Bear case", "summary": "A break path weakens the read.", "expected_path": "Break lower"},
        what_confirms="Hold the near-term band.",
        what_breaks="Lose the near-term band.",
    )
    trigger_support = [
        _DictNamespace(
            object_id="signal_demo",
            trigger_type="near_term",
            threshold="105.0",
            source_family="benchmark_proxy",
            provider="chronos",
            current_distance_to_trigger="1.20",
            next_action_if_hit="Escalate review if the near-term threshold is reached.",
            next_action_if_broken="Reset the brief support if the near-term band fails.",
            threshold_state="watch",
            support_strength=support_strength,
            confidence_summary="Tight interval support",
            degraded_state=None,
            generated_at="2026-04-09T00:00:00+00:00",
        ),
        _DictNamespace(
            object_id="signal_demo",
            trigger_type="thesis",
            threshold="109.0",
            source_family="benchmark_proxy",
            provider="chronos",
            current_distance_to_trigger="3.80",
            next_action_if_hit="Recheck the thesis if the horizon threshold is reached.",
            next_action_if_broken="Treat the directional support as broken if the horizon path fails.",
            threshold_state="watch",
            support_strength=support_strength,
            confidence_summary="Tight interval support",
            degraded_state=None,
            generated_at="2026-04-09T00:00:00+00:00",
        ),
    ]
    return SimpleNamespace(
        support=support,
        result=result,
        scenario_support=scenario_support,
        trigger_support=trigger_support,
    )


def test_signal_support_bundle_emits_path_specific_scenarios(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.features.forecast_feature_service as feature_service

    monkeypatch.setattr(feature_service, "build_request", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(
        feature_service,
        "build_forecast_bundle",
        lambda **kwargs: _fake_bundle(direction="positive", confidence_band="tight", anomaly_score=0.2),
    )

    payload = feature_service.build_signal_support_bundle(
        {
            "signal_id": "signal_duration",
            "label": "30Y Mortgage",
            "symbol": "MORTGAGE30US",
            "signal_kind": "market",
            "mapping_directness": "sleeve-proxy",
            "effect_type": "rates_duration_effect",
            "primary_effect_bucket": "duration",
            "source_kind": "official_release",
            "source_context": {
                "source_class": "macro_release",
                "market_confirmation": "strong",
            },
            "why_it_matters_macro": "Higher financing costs keep the macro backdrop restrictive.",
            "why_it_matters_micro": "Bond adds need patience while financing conditions stay tight.",
            "why_it_matters_short_term": "Keep the current watch line active.",
            "why_it_matters_long_term": "Persistence would keep the duration hurdle elevated.",
            "sufficiency_state": "sufficient",
            "affected_sleeves": ["sleeve_ig_bonds"],
            "affected_candidates": ["AGGU"],
            "current_value": 6.46,
            "history": [6.31, 6.41, 6.46],
            "timestamps": [
                "2026-04-07T00:00:00+00:00",
                "2026-04-08T00:00:00+00:00",
                "2026-04-09T00:00:00+00:00",
            ],
            "summary": "Mortgage rates remain elevated.",
            "implication": "Duration timing stays constrained.",
        },
        why_here="Bond sleeve timing remains patient.",
        portfolio_consequence="Keep bond adds patient.",
        next_action="Monitor",
    )

    scenarios = payload["scenario_block"]["scenarios"]
    assert len(scenarios) == 3
    bull, base, bear = scenarios

    assert bull["lead_sentence"] != base["lead_sentence"]
    assert base["lead_sentence"] != bear["lead_sentence"]
    assert bull["action_consequence"] != bear["action_consequence"]
    assert bull["path_meaning"] != bear["path_meaning"]
    assert bull["path_statement"] != base["path_statement"]
    assert base["path_statement"] != bear["path_statement"]
    assert bull["confirmation_note"] != base["confirmation_note"]
    assert base["confirmation_note"] != bear["confirmation_note"]
    assert bull["support_strength"] != base["support_strength"]
    assert base["support_strength"] != bear["support_strength"]
    assert bull["confirm_probability"] != bear["confirm_probability"]
    assert bull["threshold_breach_risk"] != bear["threshold_breach_risk"]
    assert bull["path_bias"] != base["path_bias"]
    assert bull["regime_note"]
    assert base["regime_note"]
    assert bear["regime_note"]
    assert all(isinstance(scenario.get("scenario_likelihood_pct"), int) for scenario in scenarios)
    assert sum(int(scenario.get("scenario_likelihood_pct") or 0) for scenario in scenarios) == 100
    assert payload["scenario_block"]["forecast_support"]["persistence_score"] is not None
    assert payload["scenario_block"]["forecast_support"]["trigger_pressure"] is not None
    assert payload["scenario_block"]["forecast_support"]["cross_asset_confirmation_score"] is not None
    assert payload["scenario_block"]["forecast_support"]["scenario_support_strength"] in {"strong", "moderate", "bounded"}
    summary = payload["scenario_block"]["summary"]
    assert summary.startswith("The move is still")
    assert "Near-term confidence" in summary
    assert "follow-through" not in summary.lower()
    assert "the path is still" not in summary.lower()
    assert "Across the active horizon" not in summary
    visible_scenario_text = " ".join(
        str(value or "")
        for scenario in scenarios
        for value in (
            scenario.get("path_statement"),
            scenario.get("lead_sentence"),
            scenario.get("action_consequence"),
            scenario.get("path_meaning"),
            scenario.get("support_strength"),
            scenario.get("regime_note"),
            scenario.get("confirmation_note"),
            scenario.get("macro"),
            scenario.get("micro"),
            scenario.get("short_term"),
            scenario.get("long_term"),
        )
    ).lower()
    assert "chronos" not in visible_scenario_text
    assert "bounded upside only" not in visible_scenario_text
    assert "central path" not in visible_scenario_text
    assert "right now the path" not in visible_scenario_text
    bull_path = bull["path_statement"].lower()
    assert (
        "broadens enough" in bull_path
        or "stops widening" in bull_path
        or "stop tightening further" in bull_path
        or "stops worsening" in bull_path
    )
    assert "shifts toward bull" in base["path_statement"].lower()
    assert "gains weight" in bear["path_statement"].lower()
    assert "stops short" in bull["path_statement"].lower() or "still needs" in bull["support_strength"].lower()
    assert bull["confirmation_note"]


def test_news_scenarios_stay_cautious_when_market_confirmation_is_weak(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.features.forecast_feature_service as feature_service

    monkeypatch.setattr(feature_service, "build_request", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(
        feature_service,
        "build_forecast_bundle",
        lambda **kwargs: _fake_bundle(direction="mixed", confidence_band="wide", anomaly_score=0.54, support_strength="moderate"),
    )

    payload = feature_service.build_signal_support_bundle(
        {
            "signal_id": "signal_news",
            "label": "Iran shipping risk keeps oil markets on edge",
            "symbol": "",
            "signal_kind": "news",
            "mapping_directness": "macro-only",
            "effect_type": "global_news_effect",
            "primary_effect_bucket": "market",
            "source_kind": "news_context",
            "source_context": {
                "source_class": "geopolitical_news",
                "market_confirmation": "limited",
                "event_status": "confirmed",
            },
            "why_it_matters_macro": "Headline spillover into energy and inflation remains possible.",
            "why_it_matters_micro": "Real-assets and cash sleeves need confirmation before action.",
            "why_it_matters_short_term": "The next confirmation session matters more than the headline alone.",
            "why_it_matters_long_term": "Only persistent confirmation would make this a structural shift.",
            "sufficiency_state": "bounded",
            "affected_sleeves": ["sleeve_real_assets", "sleeve_cash_bills"],
            "affected_candidates": [],
            "summary": "Headline risk remains live.",
            "implication": "Monitor the cross-market response, not just the event.",
        },
        why_here="Keep the portfolio response conditional on broader confirmation.",
        portfolio_consequence="No portfolio action yet; keep the event on watch.",
        next_action="Monitor",
    )

    bull, base, bear = payload["scenario_block"]["scenarios"]

    assert "actionable" in bull["lead_sentence"].lower()
    assert "monitored risk" in base["lead_sentence"].lower()
    assert "backdrop" in bear["lead_sentence"].lower()
    assert base["macro"] is None
    assert "no portfolio action yet" in base["action_consequence"].lower()
    assert bull["path_meaning"] != bear["path_meaning"]
    assert bull["threshold_breach_risk"] != bear["threshold_breach_risk"]
    assert bull["regime_note"]
    assert "confirmation" in bull["path_statement"].lower()
    assert "market confirmation" in bull["confirmation_note"].lower()


def test_bounded_support_can_still_emit_wide_honest_likelihoods(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.features.forecast_feature_service as feature_service

    monkeypatch.setattr(feature_service, "build_request", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(
        feature_service,
        "build_forecast_bundle",
        lambda **kwargs: _fake_bundle(direction="mixed", confidence_band="wide", anomaly_score=0.34, support_strength="moderate"),
    )

    payload = feature_service.build_signal_support_bundle(
        {
            "signal_id": "signal_fx",
            "label": "FX / USD",
            "symbol": "DXY",
            "signal_kind": "market",
            "mapping_directness": "sleeve-proxy",
            "effect_type": "fx_effect",
            "primary_effect_bucket": "dollar_fx",
            "source_kind": "market_close",
            "source_context": {
                "source_class": "market_series",
                "market_confirmation": "moderate",
                "freshness_age_days": 0.4,
                "threshold_state": "watch",
            },
            "sufficiency_state": "sufficient",
            "affected_sleeves": ["sleeve_global_equity_core", "sleeve_cash_bills"],
            "affected_candidates": ["BIL", "IB01"],
            "current_value": 99.0,
            "history": [101.4, 101.0, 100.6, 100.1, 99.8, 99.6, 99.3, 99.0],
            "timestamps": [
                "2026-04-01T00:00:00+00:00",
                "2026-04-02T00:00:00+00:00",
                "2026-04-03T00:00:00+00:00",
                "2026-04-04T00:00:00+00:00",
                "2026-04-05T00:00:00+00:00",
                "2026-04-06T00:00:00+00:00",
                "2026-04-07T00:00:00+00:00",
                "2026-04-08T00:00:00+00:00",
            ],
            "summary": "Dollar pressure still matters for the global hurdle.",
            "implication": "Keep global risk sizing selective.",
            "why_it_matters_macro": "Dollar firmness still feeds through global financial conditions.",
            "why_it_matters_micro": "Global risk sizing stays selective while the hurdle remains high.",
            "why_it_matters_short_term": "Keep the watch line active.",
            "why_it_matters_long_term": "Only persistence would make this a stronger regime hurdle.",
        },
        why_here="Global risk still needs confirmation.",
        portfolio_consequence="Keep global risk sizing selective.",
        next_action="Monitor",
    )

    scenarios = payload["scenario_block"]["scenarios"]
    likelihoods = [int(scenario.get("scenario_likelihood_pct") or 0) for scenario in scenarios]
    assert sum(likelihoods) == 100
    assert all(scenario.get("scenario_likelihood_pct") is not None for scenario in scenarios)
    assert max(likelihoods) - min(likelihoods) <= 15


def test_short_usable_history_can_still_emit_wide_scenario_likelihoods(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.features.forecast_feature_service as feature_service

    monkeypatch.setattr(feature_service, "build_request", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(
        feature_service,
        "build_forecast_bundle",
        lambda **kwargs: _fake_bundle(direction="positive", confidence_band="wide", anomaly_score=0.28, support_strength="moderate"),
    )

    payload = feature_service.build_signal_support_bundle(
        {
            "signal_id": "signal_equity_short_history",
            "label": "World Equity",
            "symbol": "^990100-USD",
            "signal_kind": "market",
            "mapping_directness": "sleeve-proxy",
            "effect_type": "equity_effect",
            "primary_effect_bucket": "growth",
            "source_kind": "market_close",
            "source_context": {
                "source_class": "market_series",
                "market_confirmation": "strong",
                "freshness_age_days": 0.3,
                "threshold_state": "breached",
            },
            "sufficiency_state": "sufficient",
            "affected_sleeves": ["sleeve_global_equity_core"],
            "affected_candidates": ["CSPX", "IWDA"],
            "current_value": 4458.4,
            "history": [4327.7, 4458.4],
            "timestamps": [
                "2026-04-08T00:00:00+00:00",
                "2026-04-09T00:00:00+00:00",
            ],
            "summary": "Equity breadth is trying to broaden.",
            "implication": "Keep equity adds selective while confirmation builds.",
            "why_it_matters_macro": "Broader equity strength only matters if credit, rates, and FX stop resisting it.",
            "why_it_matters_micro": "Global equity exposure can stay engaged, but only with broader confirmation.",
            "why_it_matters_short_term": "The next session needs to confirm the breadth move.",
            "why_it_matters_long_term": "Only persistence would turn this into a more durable regime shift.",
            "related_series": [
                {"label": "Credit", "latest_change": -0.15, "latest_change_pct": -0.5, "direction": "down"},
                {"label": "Dollar Index", "latest_change": -0.22, "latest_change_pct": -0.2, "direction": "down"},
            ],
        },
        why_here="Risk appetite has improved, but the move still needs confirmation.",
        portfolio_consequence="Keep equity adds selective while confirmation builds.",
        next_action="Monitor",
    )

    scenarios = payload["scenario_block"]["scenarios"]
    likelihoods = [scenario.get("scenario_likelihood_pct") for scenario in scenarios]
    assert all(isinstance(value, int) for value in likelihoods)
    assert sum(int(value or 0) for value in likelihoods) == 100
    assert max(int(value or 0) for value in likelihoods) - min(int(value or 0) for value in likelihoods) <= 20


def test_significant_scenarios_use_full_wave_b_request_context(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.features.forecast_feature_service as feature_service

    captured_request: dict[str, object] = {}

    class _Point(SimpleNamespace):
        pass

    class _Truth(SimpleNamespace):
        pass

    def _capture_request(**kwargs):
        captured_request.update(kwargs)
        return SimpleNamespace(**kwargs)

    def _fake_truth(symbol: str, **kwargs):
        return _Truth(
            points=[
                _Point(value=100.0, timestamp="2026-04-06T00:00:00+00:00"),
                _Point(value=101.0, timestamp="2026-04-07T00:00:00+00:00"),
                _Point(value=102.0, timestamp="2026-04-08T00:00:00+00:00"),
                _Point(value=103.0, timestamp="2026-04-09T00:00:00+00:00"),
            ]
        )

    monkeypatch.setattr(feature_service, "build_request", _capture_request)
    monkeypatch.setattr(feature_service, "load_surface_market_truth", _fake_truth)
    monkeypatch.setattr(
        feature_service,
        "build_forecast_bundle",
        lambda **kwargs: _fake_bundle(direction="positive", confidence_band="moderate", anomaly_score=0.24, support_strength="strong"),
    )

    feature_service.build_signal_support_bundle(
        {
            "signal_id": "signal_equity_wave_b",
            "label": "World Equity",
            "symbol": "^990100-USD-STRD",
            "signal_kind": "market",
            "mapping_directness": "sleeve-proxy",
            "effect_type": "equity_effect",
            "primary_effect_bucket": "growth",
            "source_kind": "market_close",
            "source_context": {
                "source_class": "market_series",
                "market_confirmation": "strong",
                "freshness_age_days": 0.2,
                "threshold_state": "breached",
            },
            "sufficiency_state": "sufficient",
            "affected_sleeves": ["sleeve_global_equity_core"],
            "affected_candidates": ["CSPX", "IWDA"],
            "current_value": 4458.4,
            "history": [4327.7, 4381.1, 4458.4],
            "timestamps": [
                "2026-04-07T00:00:00+00:00",
                "2026-04-08T00:00:00+00:00",
                "2026-04-09T00:00:00+00:00",
            ],
            "summary": "Global equity breadth is improving.",
            "implication": "Keep equity adds selective while confirmation builds.",
            "why_it_matters_macro": "Equity breadth matters if credit, rates, and FX stop resisting the move.",
            "why_it_matters_micro": "Global equity can stay engaged while breadth improves.",
            "why_it_matters_short_term": "The next session needs to confirm the breadth move.",
            "why_it_matters_long_term": "Only persistence would make this a more durable regime shift.",
        },
        why_here="Risk appetite has improved, but it still needs confirmation.",
        portfolio_consequence="Keep equity adds selective while confirmation builds.",
        next_action="Monitor",
        scenario_depth="significant",
    )

    assert captured_request["covariates"]["scenario_depth"] == "significant"
    assert int(captured_request["history_target_points"]) == 16
    assert "wave_b_significant_flag" in captured_request["future_covariates"]
    assert "breadth_flag" in captured_request["future_covariates"]
    assert "wave_b_significant_flag" in captured_request["past_covariates"]
    assert captured_request["grouped_context_series"]
    assert "breadth_measures" in captured_request["grouped_context_series"]


def test_scenario_likelihoods_are_suppressed_when_forecast_support_is_weak(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.features.forecast_feature_service as feature_service

    monkeypatch.setattr(feature_service, "build_request", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(
        feature_service,
        "build_forecast_bundle",
        lambda **kwargs: _fake_bundle(direction="negative", confidence_band="wide", anomaly_score=0.64, support_strength="weak"),
    )

    payload = feature_service.build_signal_support_bundle(
        {
            "signal_id": "signal_credit",
            "label": "Credit",
            "symbol": "BAMLH0A0HYM2",
            "signal_kind": "market",
            "mapping_directness": "sleeve-proxy",
            "effect_type": "credit_effect",
            "primary_effect_bucket": "credit",
            "source_kind": "official_release",
            "source_context": {
                "source_class": "macro_release",
                "market_confirmation": "strong",
            },
            "why_it_matters_macro": "Funding conditions are tightening again.",
            "why_it_matters_micro": "Carry stays selective while spreads are widening.",
            "why_it_matters_short_term": "Keep the current review range active.",
            "why_it_matters_long_term": "Only persistence would make this structural.",
            "sufficiency_state": "sufficient",
            "affected_sleeves": ["sleeve_ig_bonds"],
            "affected_candidates": ["AGGU"],
            "summary": "Credit remains restrictive.",
            "implication": "Risk budget stays selective.",
        },
        why_here="Funding conditions still matter for the bond sleeve.",
        portfolio_consequence="Keep the bond sleeve patient.",
        next_action="Monitor",
    )

    scenarios = payload["scenario_block"]["scenarios"]
    assert len(scenarios) == 3
    assert all(scenario.get("scenario_likelihood_pct") is None for scenario in scenarios)
