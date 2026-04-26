from __future__ import annotations

from types import SimpleNamespace

from app.v2.features.chart_payload_builders import build_daily_brief_chart_payload


class _DictNamespace(SimpleNamespace):
    def to_dict(self) -> dict[str, object]:
        return dict(self.__dict__)


def _trigger(
    *,
    trigger_type: str,
    threshold: str,
    next_action_if_hit: str,
    next_action_if_broken: str,
) -> _DictNamespace:
    return _DictNamespace(
        trigger_type=trigger_type,
        threshold=threshold,
        next_action_if_hit=next_action_if_hit,
        next_action_if_broken=next_action_if_broken,
        threshold_state="watch",
    )


def test_wti_chart_payload_uses_threshold_line_and_confirmation_strip() -> None:
    signal = {
        "signal_id": "signal_wti",
        "label": "WTI Crude",
        "signal_label": "WTI Crude",
        "symbol": "CL=F",
        "signal_kind": "market",
        "direction": "up",
        "primary_effect_bucket": "energy",
        "evidence_title": "WTI Crude 98.58, up 4.42% on the day · fresh in current brief window",
        "interpretation_subtitle": "The move matters through inflation and hedge transmission, so keep the real-assets sleeve on watch rather than turning it into a direct commodity call.",
        "freshness_label": "fresh in current brief window",
        "evidence_class": "public_verified_close",
        "source_and_validity": "Public verified close. Status: fresh in current brief window.",
        "history": [91.2, 92.8, 94.1, 95.6, 98.58],
        "timestamps": [
            "2026-04-04T00:00:00+00:00",
            "2026-04-05T00:00:00+00:00",
            "2026-04-06T00:00:00+00:00",
            "2026-04-07T00:00:00+00:00",
            "2026-04-08T00:00:00+00:00",
        ],
        "as_of": "2026-04-08T00:00:00+00:00",
        "source_context": {"source_class": "market_series"},
        "support_bundle": {
            "bundle": SimpleNamespace(
                trigger_support=[
                    _trigger(
                        trigger_type="review",
                        threshold="96.0",
                        next_action_if_hit="Escalate review if WTI keeps holding above the review range.",
                        next_action_if_broken="Treat the oil move as fading if WTI falls back through the recent range.",
                    ),
                    _trigger(
                        trigger_type="confirm",
                        threshold="100.0",
                        next_action_if_hit="Keep inflation hedges active if oil pushes through the inflation-pressure line.",
                        next_action_if_broken="Treat the inflation impulse as unconfirmed if oil fails to extend.",
                    ),
                ],
                request=SimpleNamespace(
                    covariates={
                        "related_series": [
                            {
                                "symbol": "CPI_YOY",
                                "relation": "same",
                                "channel": "inflation_context",
                                "history": [2.2, 2.3, 2.4, 2.45],
                                "timestamps": [
                                    "2026-01-01T00:00:00+00:00",
                                    "2026-02-01T00:00:00+00:00",
                                    "2026-03-01T00:00:00+00:00",
                                    "2026-04-01T00:00:00+00:00",
                                ],
                                "latest_direction": "up",
                                "latest_change": 0.05,
                                "latest_change_pct": 2.08,
                            },
                            {
                                "symbol": "DXY",
                                "relation": "inverse",
                                "channel": "dollar_context",
                                "history": [101.4, 101.2, 100.9, 100.3],
                                "timestamps": [
                                    "2026-04-04T00:00:00+00:00",
                                    "2026-04-05T00:00:00+00:00",
                                    "2026-04-06T00:00:00+00:00",
                                    "2026-04-07T00:00:00+00:00",
                                ],
                                "latest_direction": "down",
                                "latest_change": -0.6,
                                "latest_change_pct": -0.6,
                            },
                            {
                                "symbol": "DFII10",
                                "relation": "same",
                                "channel": "rates_context",
                                "history": [1.82, 1.87, 1.91, 1.96],
                                "timestamps": [
                                    "2026-04-04T00:00:00+00:00",
                                    "2026-04-05T00:00:00+00:00",
                                    "2026-04-06T00:00:00+00:00",
                                    "2026-04-07T00:00:00+00:00",
                                ],
                                "latest_direction": "up",
                                "latest_change": 0.05,
                                "latest_change_pct": 2.62,
                            },
                            {
                                "symbol": "GC=F",
                                "relation": "same",
                                "channel": "hedge_demand",
                                "history": [2360.0, 2375.0, 2382.0, 2390.0],
                                "timestamps": [
                                    "2026-04-04T00:00:00+00:00",
                                    "2026-04-05T00:00:00+00:00",
                                    "2026-04-06T00:00:00+00:00",
                                    "2026-04-07T00:00:00+00:00",
                                ],
                                "latest_direction": "up",
                                "latest_change": 8.0,
                                "latest_change_pct": 0.34,
                            },
                        ]
                    }
                ),
                result=SimpleNamespace(
                    point_path=[99.0, 99.6, 100.4, 100.8, 101.2],
                    quantiles={"0.1": [97.6, 98.0, 98.4, 98.7, 99.0], "0.9": [100.1, 100.9, 101.8, 102.2, 102.6]},
                ),
                support=SimpleNamespace(
                    support_strength="moderate",
                    scenario_support_strength="moderate",
                    degraded_state=None,
                    horizon=10,
                    uncertainty_width_label="moderate",
                    trigger_pressure=0.62,
                ),
            )
        },
    }

    payload = build_daily_brief_chart_payload(signal)

    assert payload["chart_kind"] == "threshold_line"
    assert payload["chart_density_profile"] in {"rich_line", "compact_line"}
    assert payload["chart_question"].startswith("Is oil still holding")
    assert payload["chart_suppressed_reason"] is None
    assert payload["observed_path"]["object_role"] == "observed_path"
    assert payload["observed_path"]["object_type"] == "observed_timeseries"
    assert payload["forecast_path"]["object_role"] == "forecast_path"
    assert payload["forecast_path"]["object_type"] == "forecast_timeseries"
    assert payload["forecast_path"]["forecast_start_timestamp"] == "2026-04-09T00:00:00+00:00"
    assert payload["forecast_path"]["forecast_end_timestamp"] == "2026-04-13T00:00:00+00:00"
    assert payload["review_context"]["object_role"] == "review_context"
    assert payload["review_context"]["object_type"] == "range_zone"
    assert payload["review_context"]["lower_bound"] == payload["review_context"]["min"]
    assert payload["review_context"]["upper_bound"] == payload["review_context"]["max"]
    assert payload["thresholds"]["review_line"]["label"] == "Review range"
    assert payload["thresholds"]["confirm_line"]["label"] == "Inflation pressure line"
    assert payload["thresholds"]["break_line"]["label"] == "Fade back into context"
    assert payload["observed_series"]["plain_language_meaning"] == "Observed path shows the actual oil path so far."
    assert payload["forecast_series"]["plain_language_meaning"].startswith("Forecast path shows where oil may go next")
    assert payload["review_band"]["plain_language_meaning"] == "Review range marks the range where the inflation or hedge case stays under review."
    assert payload["threshold_lines"][0]["semantic_role"] in {"strengthen_line", "hold_line"}
    assert payload["threshold_lines"][1]["semantic_role"] == "fade_line"
    assert payload["threshold_lines"][0]["render_mode"] == "line"
    assert payload["threshold_lines"][1]["render_mode"] == "dashed_line"
    assert payload["decision_references"][0]["object_role"] == "decision_reference"
    assert payload["decision_references"][0]["object_type"] == "reference_line"
    assert payload["decision_references"][0]["visible_in_overview"] is True
    assert payload["decision_references"][0]["visible_in_focus"] is True
    assert payload["decision_references"][0]["hover_enabled"] is True
    assert payload["decision_references"][0]["legend_enabled"] is True
    assert payload["inspectable_series_order"]
    assert payload["distance_to_thresholds"]
    assert payload["inspection_points"]
    assert payload["hover_payload_by_timestamp"]
    hover_payload = next(item for item in payload["hover_payload_by_timestamp"] if item["timestamp"] == "2026-04-08T00:00:00+00:00")
    assert hover_payload["observed_value"] == 98.58
    assert hover_payload["forecast_value"] == 99.0
    assert len(hover_payload["relation_statements"]) <= 2
    assert hover_payload["reference_values"][0]["label"] == "Inflation pressure line"
    assert payload["active_comparison_enabled"] is True
    assert payload["threshold_overlap_mode"] in {"separate_lines", "hide_secondary_line_from_plot_show_in_legend", "merge_to_zone"}
    assert payload["threshold_legend"]
    assert payload["compact_chart_summary"]
    assert payload["path_state"]
    assert payload["chart_guide_items"]
    assert payload["chart_guide_items"][0]["label"] == "Observed"
    assert payload["chart_guide_items"][0]["text"] == "actual oil path so far"
    assert payload["focusable_threshold_groups"]
    assert payload["focus_default_group"] == "decision_lines_group"
    assert payload["focus_y_domain"]
    assert payload["focus_modes"]
    assert payload["focus_modes"][0]["mode_id"] == "overview"
    assert payload["focus_modes"][1]["mode_id"] == "observed_forecast_group"
    assert payload["focus_modes"][2]["mode_id"] == "decision_lines_group"
    assert payload["inspectable_thresholds"] == ["confirm_line", "break_line"]
    assert payload["forecast_focus_ready"] is True
    assert payload["focus_reason"]
    assert payload["current_vs_thresholds"]
    assert payload["forecast_vs_thresholds"]
    assert payload["nearest_threshold"]["threshold_id"] == "confirm_line"
    assert payload["relation_priority_order"]
    assert payload["forecast_visibility_mode"] == "emphasized"
    assert payload["forecast_strength_label"]
    assert payload["forecast_comparison_label"]
    assert payload["focus_split_available"] is False
    assert payload["chart_explainer_lines"]
    assert payload["chart_explainer_lines"][0] == "actual oil path so far"
    assert payload["chart_explainer_lines"][1] == "projected oil path if current pressure holds"
    assert any("range where the inflation or hedge case stays under review" == line for line in payload["chart_explainer_lines"])
    assert any("level where the inflation or hedge case strengthens" == line for line in payload["chart_explainer_lines"])
    assert any("level where the move stops adding to the hedge case" == line for line in payload["chart_explainer_lines"])
    assert payload["chart_takeaway"].startswith("Oil is ")
    assert "Forecast drifting higher" in payload["chart_takeaway"]
    assert "inflation pressure line" in payload["chart_takeaway"]
    assert payload["confirmation_strip"] is not None
    assert len(payload["confirmation_strip"]["items"]) >= 3
    assert payload["forecast_overlay"] is not None
    assert payload["forecast_overlay"]["forecast_start_timestamp"] == "2026-04-09T00:00:00+00:00"
    assert payload["forecast_overlay"]["forecast_end_timestamp"] == "2026-04-13T00:00:00+00:00"
    assert payload["forecast_overlay"]["visible_by_default"] is True


def test_event_chart_payload_suppresses_when_market_reaction_is_thin() -> None:
    signal = {
        "signal_id": "signal_news",
        "label": "Regional escalation reported",
        "signal_label": "Regional escalation",
        "symbol": "",
        "signal_kind": "news",
        "direction": "up",
        "primary_effect_bucket": "policy",
        "evidence_title": "Regional escalation reported, market confirmation still limited",
        "freshness_label": "fresh in current brief window",
        "evidence_class": "reported_event_unconfirmed_market_read",
        "source_and_validity": "Reported event with limited market confirmation.",
        "history": [],
        "timestamps": [],
        "as_of": "2026-04-08T00:00:00+00:00",
        "source_context": {
            "source_class": "geopolitical_news",
            "market_confirmation": "limited",
        },
        "support_bundle": {
            "bundle": SimpleNamespace(
                request=SimpleNamespace(
                    covariates={
                        "related_series": [
                            {
                                "symbol": "DXY",
                                "relation": "same",
                                "channel": "fx_spillover",
                                "history": [100.0, 100.0, 100.0],
                                "timestamps": [
                                    "2026-04-06T00:00:00+00:00",
                                    "2026-04-07T00:00:00+00:00",
                                    "2026-04-08T00:00:00+00:00",
                                ],
                                "latest_direction": "flat",
                                "latest_change": 0.0,
                                "latest_change_pct": 0.0,
                            }
                        ]
                    }
                )
            )
        },
    }

    payload = build_daily_brief_chart_payload(signal)

    assert payload["chart_kind"] == "event_reaction_strip"
    assert payload["chart_density_profile"] == "suppressed"
    assert payload["chart_suppressed_reason"]
    assert payload["event_reaction_strip"] is None
    assert payload["chart_takeaway"] == "Market confirmation is still too thin to treat the event as a priced channel."


def test_close_thresholds_merge_to_zone_in_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.v2.features.chart_payload_builders._primary_history_points",
        lambda signal_card, *, target_points: (
            [
                {"timestamp": "2026-04-08T00:00:00+00:00", "value": 3.06},
                {"timestamp": "2026-04-09T00:00:00+00:00", "value": 3.08},
            ],
            None,
        ),
    )
    signal = {
        "signal_id": "signal_credit_overlap",
        "label": "Credit",
        "signal_label": "Credit",
        "symbol": "BAMLH0A0HYM2",
        "signal_kind": "market",
        "direction": "up",
        "primary_effect_bucket": "credit",
        "evidence_title": "Credit 3.08%, up 2 bps from prior official print · fresh official print",
        "interpretation_subtitle": "Credit still says funding conditions are tight, so safer ballast stays easier to justify than adding more risk.",
        "freshness_label": "fresh official print",
        "evidence_class": "official_release",
        "source_and_validity": "Official release. Status: fresh official print.",
        "history": [3.06, 3.08],
        "timestamps": [
            "2026-04-08T00:00:00+00:00",
            "2026-04-09T00:00:00+00:00",
        ],
        "as_of": "2026-04-09T00:00:00+00:00",
        "source_context": {"source_class": "macro_release"},
        "support_bundle": {
            "bundle": SimpleNamespace(
                trigger_support=[
                    _trigger(
                        trigger_type="review",
                        threshold="3.00",
                        next_action_if_hit="Review credit if spreads hold the move.",
                        next_action_if_broken="Treat the move as fading if spreads fall back.",
                    ),
                    _trigger(
                        trigger_type="confirm",
                        threshold="3.061",
                        next_action_if_hit="Funding pressure keeps tightening if spreads extend.",
                        next_action_if_broken="Treat the move as unconfirmed if spreads fail to hold.",
                    ),
                ],
                support=SimpleNamespace(
                    support_strength="moderate",
                    scenario_support_strength="moderate",
                    degraded_state=None,
                    horizon=10,
                    uncertainty_width_label="moderate",
                    trigger_pressure=0.61,
                ),
                result=SimpleNamespace(point_path=[3.07, 3.08, 3.09], quantiles={}),
            )
        },
    }

    payload = build_daily_brief_chart_payload(signal)

    assert payload["threshold_overlap_mode"] == "merge_to_zone"
    assert payload["thresholds"]["trigger_zone"]["label"] == "Decision zone"
    assert payload["threshold_lines"]
    assert payload["decision_references"]
    assert all(item["visible_by_default"] is False for item in payload["threshold_lines"])
    assert all(item["render_mode"] == "merged_zone" for item in payload["threshold_lines"])
    assert all(item["visible_in_overview"] is False for item in payload["decision_references"])
    assert all(item["visible_in_focus"] is True for item in payload["decision_references"])
    assert any(item["label"] == "Decision zone" for item in payload["chart_guide_items"])
    assert payload["focus_split_available"] is True
    assert payload["focusable_threshold_groups"]
    decision_group = next(item for item in payload["focusable_threshold_groups"] if item["group_id"] == "decision_lines_group")
    assert decision_group["can_split_from_zone"] is True
    assert "confirm_line" in decision_group["member_line_ids"]
    assert "break_line" in decision_group["member_line_ids"]
    focus_mode = next(item for item in payload["focus_modes"] if item["mode_id"] == "decision_lines_group")
    assert "confirm_line" in focus_mode["visible_object_ids"]
    assert "break_line" in focus_mode["visible_object_ids"]


def test_credit_chart_can_fall_back_to_strip_only_when_path_shape_is_too_thin(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.v2.features.chart_payload_builders._primary_history_points",
        lambda signal_card, *, target_points: (
            [
                {"timestamp": "2026-04-07T00:00:00+00:00", "value": 3.05},
                {"timestamp": "2026-04-08T00:00:00+00:00", "value": 3.12},
            ],
            None,
        ),
    )
    signal = {
        "signal_id": "signal_credit",
        "label": "Credit",
        "signal_label": "US Credit Spread",
        "symbol": "BAMLH0A0HYM2",
        "signal_kind": "market",
        "direction": "up",
        "primary_effect_bucket": "credit",
        "evidence_title": "US credit spread 3.12%, up 7 bps from prior official print",
        "freshness_label": "fresh official print",
        "evidence_class": "official_release",
        "source_and_validity": "Official release. Status: fresh official print.",
        "history": [3.05, 3.12],
        "timestamps": ["2026-04-07T00:00:00+00:00", "2026-04-08T00:00:00+00:00"],
        "as_of": "2026-04-08T00:00:00+00:00",
        "source_context": {"source_class": "market_series"},
        "support_bundle": {
            "bundle": SimpleNamespace(
                trigger_support=[
                    _trigger(
                        trigger_type="review",
                        threshold="3.10",
                        next_action_if_hit="Escalate review if spreads hold above the review line.",
                        next_action_if_broken="Treat the move as fading if spreads slip back below the review line.",
                    ),
                    _trigger(
                        trigger_type="confirm",
                        threshold="3.20",
                        next_action_if_hit="Funding stress is confirming.",
                        next_action_if_broken="Funding stress is fading.",
                    ),
                ],
                request=SimpleNamespace(
                    covariates={
                        "related_series": [
                            {
                                "symbol": "^SPXEW",
                                "relation": "inverse",
                                "channel": "equity_breadth",
                                "history": [8060.0, 8025.0],
                                "timestamps": ["2026-04-07T00:00:00+00:00", "2026-04-08T00:00:00+00:00"],
                                "latest_direction": "down",
                                "latest_change": -35.0,
                                "latest_change_pct": -0.43,
                            },
                            {
                                "symbol": "DFII10",
                                "relation": "same",
                                "channel": "rates_context",
                                "history": [1.90, 1.96],
                                "timestamps": ["2026-04-07T00:00:00+00:00", "2026-04-08T00:00:00+00:00"],
                                "latest_direction": "up",
                                "latest_change": 0.06,
                                "latest_change_pct": 3.15,
                            },
                            {
                                "symbol": "DXY",
                                "relation": "same",
                                "channel": "dollar_context",
                                "history": [98.6, 98.9],
                                "timestamps": ["2026-04-07T00:00:00+00:00", "2026-04-08T00:00:00+00:00"],
                                "latest_direction": "up",
                                "latest_change": 0.3,
                                "latest_change_pct": 0.3,
                            },
                        ]
                    }
                ),
                support=SimpleNamespace(
                    support_strength="moderate",
                    scenario_support_strength="moderate",
                    degraded_state=None,
                    horizon=10,
                    uncertainty_width_label="wide",
                    trigger_pressure=0.66,
                ),
                result=SimpleNamespace(point_path=[3.15, 3.18]),
            )
        },
    }

    payload = build_daily_brief_chart_payload(signal)

    assert payload["chart_kind"] == "threshold_line"
    assert payload["chart_density_profile"] == "strip_only"
    assert payload["confirmation_strip"] is not None
    assert len(payload["confirmation_strip"]["items"]) >= 3
