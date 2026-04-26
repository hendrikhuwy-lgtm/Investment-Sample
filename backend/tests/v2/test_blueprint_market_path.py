from __future__ import annotations

import sqlite3

import numpy as np

from app.services.blueprint_benchmark_registry import resolve_benchmark_assignment
from app.v2.blueprint_market.kronos_input_adapter import build_kronos_input
from app.v2.blueprint_market.blueprint_candidate_forecast_service import (
    _artifact_requires_upgrade,
    _build_support_from_samples,
    _choose_identity_with_series,
    _candidate_fragility_semantics,
    _candidate_implication,
    _decorate_last_good_artifact,
    _is_retriable_kronos_error,
    _path_quality_semantics,
    _recalibrate_usefulness_label,
    _threshold_drift_direction,
    _usefulness_label,
    compact_forecast_support_from_market_path,
    run_market_forecast_refresh_lane,
)
from app.v2.blueprint_market.series_refresh_service import _series_quality_summary, detect_candidate_series_gaps
from app.v2.surfaces.blueprint.report_contract_builder import _kronos_market_setup, _kronos_path_support_label


def _row(timestamp: str, close: float) -> dict[str, object]:
    return {
        "timestamp_utc": f"{timestamp}T00:00:00+00:00",
        "open": close,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": 1000.0,
    }


def test_series_quality_summary_marks_proxy_and_corporate_action_uncertainty():
    rows = [_row("2026-03-02", 100.0), _row("2026-03-03", 101.0), _row("2026-03-06", 102.0)]
    summary = _series_quality_summary(
        rows=rows,
        series_role="approved_proxy",
        adjustment_mode="provider_native",
    )
    assert summary["uses_proxy_series"] is True
    assert summary["has_corporate_action_uncertainty"] is True
    assert summary["bars_present"] == 3


def test_detect_candidate_series_gaps_flags_missing_business_days():
    rows = [_row("2026-03-02", 100.0), _row("2026-03-03", 101.0), _row("2026-03-06", 102.0)]
    gaps = detect_candidate_series_gaps(rows)
    assert "gap:2026-03-04" in gaps
    assert "gap:2026-03-05" in gaps


def test_yahoo_secondary_candidates_add_sgx_suffix_for_a35():
    from app.v2.blueprint_market.series_refresh_service import _yahoo_secondary_candidates

    identity = {"provider_symbol": "A35", "exchange_mic": "XSES"}
    assert _yahoo_secondary_candidates(identity, "A35") == ["A35", "A35.SI"]


def test_build_kronos_input_uses_exchange_sessions_and_price_only_mode_for_spy():
    rows = []
    for index, day in enumerate(
        [
            "2025-04-01",
            "2025-04-02",
            "2025-04-03",
            "2025-04-04",
            "2025-04-07",
            "2025-04-08",
        ]
    ):
        rows.append(
            {
                "timestamp_utc": f"{day}T00:00:00+00:00",
                "open": 100.0 + index,
                "high": 101.0 + index,
                "low": 99.0 + index,
                "close": 100.5 + index,
                "volume": None,
                "amount": None,
            }
        )
    payload = build_kronos_input(
        identity={
            "provider_symbol": "SPY",
            "symbol": "SPY",
            "series_role": "direct",
            "exchange_mic": "NYSEARCA",
            "provider_asset_class": "etf",
            "forecast_eligibility": "eligible",
        },
        rows=rows,
        horizon=3,
        interval="1day",
        max_context=20,
        min_history_bars=6,
    )
    assert payload["supported"] is True
    assert payload["liquidity_feature_mode"] == "price_only"
    assert payload["volume_available"] is False
    assert payload["amount_available"] is False
    assert payload["output_timestamps"][0].startswith("2025-04-09")


def test_candidate_implication_stays_deterministic_and_proxy_aware():
    implication = _candidate_implication(
        usefulness_label="usable",
        path_stability="stable",
        volatility_outlook="contained",
        threshold_map=[
            {"threshold_id": "downside_case", "relation": "below"},
            {"threshold_id": "stress_case", "relation": "below"},
        ],
        uses_proxy_series=True,
    )
    assert "Proxy series is being used" in implication
    assert "contained" in implication.lower()


def test_artifact_requires_upgrade_when_truth_manifest_fields_are_missing():
    assert _artifact_requires_upgrade(
        {
            "usefulness_label": "usable",
            "path_quality_label": "balanced",
            "path_quality_score": 72.0,
            "candidate_fragility_label": "watchful",
            "candidate_fragility_score": 34.0,
            "threshold_drift_direction": "balanced",
            "scenario_takeaways": {},
        }
    )


def test_artifact_requires_upgrade_when_timing_enum_is_missing():
    assert _artifact_requires_upgrade(
        {
            "usefulness_label": "usable",
            "market_setup_state": "direct_usable",
            "freshness_state": "fresh",
            "driving_symbol": "SPY",
            "driving_series_role": "direct",
            "output_timestamps": ["2026-04-27T00:00:00+00:00"],
            "sampling_summary": {"sample_path_count": 16},
            "scenario_endpoint_summary": [],
            "liquidity_feature_mode": "price_only",
            "path_quality_label": "balanced",
            "path_quality_score": 72.0,
            "candidate_fragility_label": "watchful",
            "candidate_fragility_score": 34.0,
            "threshold_drift_direction": "balanced",
            "scenario_takeaways": {},
        }
    )


def test_strong_proxy_history_no_longer_forces_alias_review_when_resolution_is_stable(monkeypatch):
    from app.v2.blueprint_market import coverage

    monkeypatch.setattr(
        coverage,
        "resolve_provider_identifiers",
        lambda *args, **kwargs: {
            "provider_symbol": "VWRA.LSE",
            "exchange_qualified_symbol": "VWRA.LSE",
            "fallback_aliases": ["VWRA"],
            "resolution_confidence": 0.88,
        },
    )
    monkeypatch.setattr(coverage, "ensure_candidate_market_identities", lambda *args, **kwargs: [{"series_role": "approved_proxy"}])
    monkeypatch.setattr(coverage, "check_candidate_series_freshness", lambda *args, **kwargs: {"series_quality_summary": None})
    monkeypatch.setattr(coverage, "load_price_series", lambda *args, **kwargs: [{}] * 300)

    summary = coverage.build_candidate_coverage_summary(
        sqlite3.connect(":memory:"),
        {"symbol": "VWRA", "sleeve_key": "global_equity_core", "domicile": "IRELAND"},
        {"failure_class_summary": {}, "data_quality": {}, "source_integrity_summary": {}, "blocker_category": None},
        candidate_id="candidate_instrument_vwra",
    )

    workflow = dict(summary["coverage_workflow_summary"])
    assert workflow["status"] == "proxy_ready"
    assert summary["alias_review_needed"] is False


def test_exchange_qualified_proxy_route_can_clear_alias_review_with_deep_history(monkeypatch):
    from app.v2.blueprint_market import coverage

    monkeypatch.setattr(
        coverage,
        "resolve_provider_identifiers",
        lambda *args, **kwargs: {
            "provider_symbol": "IWDP.LSE",
            "exchange_qualified_symbol": "IWDP.LSE",
            "fallback_aliases": ["IWDP", "IWDP.SW"],
            "resolution_confidence": 0.8,
            "resolution_reason": "route_candidate_promoted",
        },
    )
    monkeypatch.setattr(coverage, "ensure_candidate_market_identities", lambda *args, **kwargs: [{"series_role": "approved_proxy"}])
    monkeypatch.setattr(coverage, "check_candidate_series_freshness", lambda *args, **kwargs: {"series_quality_summary": None})
    monkeypatch.setattr(coverage, "load_price_series", lambda *args, **kwargs: [{}] * 2960)

    summary = coverage.build_candidate_coverage_summary(
        sqlite3.connect(":memory:"),
        {"symbol": "IWDP", "sleeve_key": "real_assets", "domicile": "IE"},
        {"failure_class_summary": {}, "data_quality": {}, "source_integrity_summary": {}, "blocker_category": None},
        candidate_id="candidate_instrument_iwdp",
    )

    workflow = dict(summary["coverage_workflow_summary"])
    assert workflow["status"] == "proxy_ready"
    assert summary["alias_review_needed"] is False


def test_stable_exchange_qualified_route_with_failed_direct_attempt_is_missing_history(monkeypatch):
    from app.v2.blueprint_market import coverage

    monkeypatch.setattr(
        coverage,
        "resolve_provider_identifiers",
        lambda *args, **kwargs: {
            "provider_symbol": "CSPX.LSE",
            "exchange_qualified_symbol": "CSPX.LSE",
            "fallback_aliases": ["CSPX"],
            "resolution_confidence": 0.92,
            "resolution_reason": "ucits_exchange_qualified_preferred",
        },
    )
    monkeypatch.setattr(coverage, "ensure_candidate_market_identities", lambda *args, **kwargs: [{"series_role": "direct"}])
    monkeypatch.setattr(coverage, "load_price_series", lambda *args, **kwargs: [])
    monkeypatch.setattr(coverage, "check_candidate_series_freshness", lambda *args, **kwargs: {"series_quality_summary": {}})
    monkeypatch.setattr(
        coverage,
        "_latest_series_run",
        lambda *args, **kwargs: {
            "provider_name": "twelve_data",
            "status": "failed",
            "failure_class": "no_data_for_symbol",
        }
        if kwargs.get("series_role") == "direct"
        else {},
    )

    summary = coverage.build_candidate_coverage_summary(
        sqlite3.connect(":memory:"),
        {"symbol": "CSPX", "name": "CSPX", "sleeve_key": "global_equity_core"},
        {},
        candidate_id="candidate_instrument_cspx",
    )

    assert summary["alias_review_needed"] is False
    assert summary["coverage_status"] == "missing_history"
    workflow = summary["coverage_workflow_summary"]
    assert workflow["direct_history_attempted"] is True
    assert workflow["direct_history_attempt_status"] == "failed"
    assert workflow["direct_history_failure_class"] == "no_data_for_symbol"


def test_verified_benchmark_lineage_no_longer_keeps_proxy_ready_row_stuck_as_benchmark_weak(monkeypatch):
    from app.v2.blueprint_market import coverage

    monkeypatch.setattr(
        coverage,
        "resolve_provider_identifiers",
        lambda *args, **kwargs: {
            "provider_symbol": "IWDA.LSE",
            "exchange_qualified_symbol": "IWDA.LSE",
            "fallback_aliases": ["IWDA"],
            "resolution_confidence": 0.92,
            "resolution_reason": "ucits_exchange_qualified_preferred",
        },
    )
    monkeypatch.setattr(coverage, "ensure_candidate_market_identities", lambda *args, **kwargs: [{"series_role": "approved_proxy"}])
    monkeypatch.setattr(coverage, "check_candidate_series_freshness", lambda *args, **kwargs: {"series_quality_summary": None})
    monkeypatch.setattr(coverage, "load_price_series", lambda *args, **kwargs: [{}] * 1200)

    truth_context = {
        "reconciliation_report": [
            {"field_name": "benchmark_key", "status": "verified"},
            {"field_name": "benchmark_name", "status": "verified"},
        ],
        "failure_class_summary": {
            "confidence_drag_classes": ["bounded_proxy_support"],
        },
        "data_quality": {},
        "source_integrity_summary": {},
        "blocker_category": None,
    }

    summary = coverage.build_candidate_coverage_summary(
        sqlite3.connect(":memory:"),
        {"symbol": "IWDA", "sleeve_key": "global_equity_core", "benchmark_key": "MSCI_WORLD"},
        truth_context,
        candidate_id="candidate_instrument_iwda",
    )

    assert summary["coverage_status"] == "proxy_ready"
    assert summary["coverage_workflow_summary"]["status"] == "proxy_ready"


def test_usefulness_ladder_relaxes_into_usable_with_caution_when_signal_is_moderate():
    usefulness = _usefulness_label(
        series_quality_summary={
            "bars_present": 320,
            "stale_days": 6,
            "missing_bar_ratio": 0.04,
            "quality_label": "watch",
            "uses_proxy_series": True,
        },
        uncertainty_width=0.09,
        path_stability="balanced",
        volatility_outlook="stable",
    )
    assert usefulness == "usable_with_caution"


def test_path_quality_and_fragility_scores_remain_bounded_and_deterministic():
    threshold_map = [
        {"threshold_id": "base_case", "relation": "above", "delta_pct": 4.2},
        {"threshold_id": "downside_case", "relation": "below", "delta_pct": -2.8},
        {"threshold_id": "stress_case", "relation": "below", "delta_pct": -7.5},
    ]
    path_quality_label, path_quality_score = _path_quality_semantics(
        series_quality_summary={"stale_days": 2, "missing_bar_ratio": 0.01, "uses_proxy_series": False},
        threshold_map=threshold_map,
        uncertainty_width=0.05,
        path_stability="balanced",
        volatility_outlook="stable",
    )
    fragility_label, fragility_score = _candidate_fragility_semantics(
        series_quality_summary={"stale_days": 2, "missing_bar_ratio": 0.01, "uses_proxy_series": False},
        threshold_map=threshold_map,
        uncertainty_width=0.05,
        path_stability="balanced",
        volatility_outlook="stable",
    )
    assert path_quality_label in {"clean", "balanced", "noisy", "fragile"}
    assert 0.0 <= path_quality_score <= 100.0
    assert fragility_label in {"resilient", "watchful", "fragile", "acute"}
    assert 0.0 <= fragility_score <= 100.0


def test_threshold_drift_direction_is_toward_weakening_when_stress_breaks_support():
    direction = _threshold_drift_direction(
        base_threshold={"delta_pct": -0.5},
        downside_threshold={"delta_pct": -3.2},
        stress_threshold={"delta_pct": -7.1},
        path_stability="fragile",
        volatility_outlook="elevated",
    )
    assert direction == "toward_weakening"


def test_compact_forecast_support_is_bounded_and_escalates_on_unstable_output():
    compact = compact_forecast_support_from_market_path(
        {
            "provider_source": "twelve_data+kronos",
            "usefulness_label": "unstable",
            "suppression_reason": None,
            "candidate_implication": "Weak support only.",
            "generated_at": "2026-04-07T00:00:00+00:00",
            "forecast_horizon": 21,
            "series_quality_summary": {"quality_label": "watch"},
            "model_metadata": {"model_name": "kronos", "uncertainty_width_score": 0.12},
            "path_quality_score": 52.0,
            "candidate_fragility_score": 61.0,
            "current_distance_to_strengthening": 4.2,
            "current_distance_to_weakening": -2.4,
        }
    )
    assert compact is not None
    assert compact["provider"] == "twelve_data+kronos"
    assert compact["support_strength"] == "weak"
    assert compact["escalation_flag"] is True


def test_compact_forecast_support_marks_last_good_artifact_usage_as_degraded_state():
    compact = compact_forecast_support_from_market_path(
        {
            "provider_source": "twelve_data+kronos",
            "usefulness_label": "usable",
            "suppression_reason": None,
            "candidate_implication": "Last good support is still usable.",
            "generated_at": "2026-04-13T00:00:00+00:00",
            "forecast_horizon": 21,
            "quality_flags": ["last_good_artifact_served"],
            "series_quality_summary": {"quality_label": "good"},
            "model_metadata": {"model_name": "kronos", "uncertainty_width_score": 0.04},
            "path_quality_score": 74.0,
            "candidate_fragility_score": 34.0,
        }
    )
    assert compact is not None
    assert compact["degraded_state"] == "last_good_artifact_served"


def test_build_support_from_samples_uses_retained_paths_not_pointwise_min(monkeypatch):
    import app.v2.blueprint_market.blueprint_candidate_forecast_service as forecast_service

    monkeypatch.setattr(
        forecast_service,
        "list_blueprint_market_candidates",
        lambda conn, candidate_id=None: [{"candidate_id": candidate_id, "symbol": "CSPX", "sleeve_key": "global_equity_core"}],
    )
    monkeypatch.setattr(
        forecast_service,
        "resolve_benchmark_assignment",
        lambda conn, candidate, sleeve_key: {"benchmark_key": "SP500", "benchmark_label": "S&P 500 ETF proxy"},
    )
    support = _build_support_from_samples(
        conn=sqlite3.connect(":memory:"),
        candidate_id="candidate_instrument_cspx",
        identity={"symbol": "CSPX", "provider_symbol": "SPY", "series_role": "approved_proxy", "proxy_relationship": "benchmark_proxy:SPY"},
        rows=[_row("2026-03-02", 100.0), _row("2026-03-03", 101.0), _row("2026-03-04", 102.0)] * 120,
        sample_paths=[
            {"path_index": 0, "seed": 101, "close": np.asarray([104.0, 105.0, 106.0])},
            {"path_index": 1, "seed": 102, "close": np.asarray([101.0, 102.0, 103.0])},
            {"path_index": 2, "seed": 103, "close": np.asarray([98.0, 99.0, 100.0])},
            {"path_index": 3, "seed": 104, "close": np.asarray([97.0, 98.0, 99.0])},
        ],
        adapter_payload={
            "output_timestamps": [
                "2026-03-05T00:00:00+00:00",
                "2026-03-06T00:00:00+00:00",
                "2026-03-09T00:00:00+00:00",
            ],
            "input_timestamps": [
                "2026-03-02T00:00:00+00:00",
                "2026-03-03T00:00:00+00:00",
                "2026-03-04T00:00:00+00:00",
            ],
            "liquidity_feature_mode": "full_liquidity",
            "volume_available": True,
            "amount_available": True,
        },
        series_quality_summary={
            "quality_label": "good",
            "stale_days": 0,
            "missing_bar_ratio": 0.0,
            "uses_proxy_series": True,
        },
    )
    stress = next(item for item in support["scenario_summary"] if item["scenario_type"] == "stress")
    base = next(item for item in support["scenario_summary"] if item["scenario_type"] == "base")
    assert [point["value"] for point in base["path"]] == [101.0, 102.0, 103.0]
    assert [point["value"] for point in stress["path"]] == [97.0, 98.0, 99.0]
    assert [point["value"] for point in stress["path"]] != [97.0, 98.0, 100.0]
    assert support["sampling_summary"]["sampling_mode"] == "seeded_single_path_ensemble"
    assert support["sampling_summary"]["summary_method"] == "retained_percentile_paths"
    assert support["timing_state"] in {"timing_review", "timing_constrained", "timing_fragile", "timing_ready", "timing_unavailable"}
    assert support["timing_label"].startswith("Timing ")
    assert support["timing_artifact_valid"] is True


def test_kronos_market_setup_uses_route_truth_for_cspx():
    setup = _kronos_market_setup(
        sleeve_key="global_equity_core",
        benchmark_full_name="S&P 500 Index",
        exposure_label="Broad U.S. large-cap equity",
        market_path_support={
            "market_setup_state": "proxy_usable",
            "scope_key": "us_large_cap",
            "scope_label": "broad U.S. large-cap equity",
            "driving_symbol": "SPY",
            "driving_series_role": "approved_proxy",
            "proxy_symbol": "SPY",
            "freshness_state": "fresh",
            "output_timestamps": [f"2026-04-{day:02d}T00:00:00+00:00" for day in range(21, 42)],
            "forecast_horizon": 21,
            "usefulness_label": "usable",
            "threshold_drift_direction": "balanced",
            "scenario_endpoint_summary": [{"scenario_type": "base", "endpoint_delta_pct": 1.8}],
            "series_quality_summary": {"uses_proxy_series": True, "stale_days": 0},
            "model_metadata": {"confidence_label": "medium"},
            "generated_at": "2026-04-20T00:00:00+00:00",
            "observed_series": [{"timestamp": "2026-04-18T00:00:00+00:00", "value": 100.0}],
            "projected_series": [{"timestamp": "2026-04-21T00:00:00+00:00", "value": 101.0}],
            "scenario_summary": [{"scenario_type": "base", "summary": "Base path"}],
        },
        status_state="eligible",
        decision_reasons=[],
    )
    assert setup is not None
    assert setup["route_label"] == "Using SPY proxy for broad U.S. large-cap equity market setup"
    assert setup["horizon_label"] == "21 trading days"
    assert setup["path_support_label"] == "Base path mildly supportive"


def test_kronos_market_setup_uses_validated_scope_for_sgln_proxy():
    setup = _kronos_market_setup(
        sleeve_key="real_assets",
        benchmark_full_name="FTSE BRIC 50",
        exposure_label="Something stale and wrong",
        market_path_support={
            "market_setup_state": "proxy_usable",
            "scope_key": "gold",
            "scope_label": "gold",
            "driving_symbol": "GLD",
            "driving_series_role": "approved_proxy",
            "proxy_symbol": "GLD",
            "freshness_state": "fresh",
            "output_timestamps": [f"2026-04-{day:02d}T00:00:00+00:00" for day in range(21, 42)],
            "forecast_horizon": 21,
            "usefulness_label": "usable_with_caution",
            "threshold_drift_direction": "balanced",
            "scenario_endpoint_summary": [{"scenario_type": "base", "endpoint_delta_pct": -0.8}],
            "series_quality_summary": {"uses_proxy_series": True, "stale_days": 0},
            "model_metadata": {"confidence_label": "medium"},
            "generated_at": "2026-04-20T00:00:00+00:00",
            "observed_series": [{"timestamp": "2026-04-18T00:00:00+00:00", "value": 100.0}],
            "projected_series": [{"timestamp": "2026-04-21T00:00:00+00:00", "value": 99.2}],
            "scenario_summary": [{"scenario_type": "base", "summary": "Base path"}],
        },
        status_state="eligible",
        decision_reasons=[],
    )
    assert setup is not None
    assert setup["scope_label"] == "gold"
    assert setup["route_label"] == "Using GLD proxy for gold market setup"


def test_dbmf_negative_base_endpoint_cannot_render_mild_support():
    label = _kronos_path_support_label(
        {
            "market_setup_state": "direct_usable",
            "usefulness_label": "usable",
            "threshold_drift_direction": "balanced",
            "candidate_fragility_label": "watchful",
            "scenario_takeaways": {"stress_breaks_candidate_support": False},
            "scenario_endpoint_summary": [{"scenario_type": "base", "endpoint_delta_pct": -6.2}],
        }
    )
    assert label == "Base path adverse"


def test_proxy_policy_caps_global_equity_proxy_support_at_usable_when_proxy_is_clean():
    usefulness, flags = _recalibrate_usefulness_label(
        base_usefulness_label="strong",
        series_quality_summary={"uses_proxy_series": True},
        sleeve_keys={"global_equity_core"},
        path_quality_label="balanced",
        candidate_fragility_label="watchful",
        threshold_drift_direction="balanced",
        volatility_outlook="stable",
        scenario_takeaways={
            "favorable_case_is_narrow": False,
            "stress_breaks_candidate_support": False,
            "downside_damage_is_contained": True,
        },
    )
    assert usefulness == "usable"
    assert "proxy_strength_capped" in flags


def test_proxy_policy_keeps_caution_only_when_proxy_support_is_not_clean_enough():
    usefulness, flags = _recalibrate_usefulness_label(
        base_usefulness_label="strong",
        series_quality_summary={"uses_proxy_series": True},
        sleeve_keys={"cash_bills"},
        path_quality_label="balanced",
        candidate_fragility_label="watchful",
        threshold_drift_direction="toward_weakening",
        volatility_outlook="contained",
        scenario_takeaways={
            "favorable_case_is_narrow": False,
            "stress_breaks_candidate_support": False,
            "downside_damage_is_contained": True,
        },
    )
    assert usefulness == "usable_with_caution"
    assert "proxy_strength_capped" in flags


def test_proxy_ig_bond_exception_preserves_strong_when_guardrails_hold():
    usefulness, flags = _recalibrate_usefulness_label(
        base_usefulness_label="strong",
        series_quality_summary={"uses_proxy_series": True},
        sleeve_keys={"ig_bonds"},
        path_quality_label="clean",
        candidate_fragility_label="resilient",
        threshold_drift_direction="balanced",
        volatility_outlook="contained",
        scenario_takeaways={
            "favorable_case_is_narrow": False,
            "stress_breaks_candidate_support": False,
            "downside_damage_is_contained": True,
        },
    )
    assert usefulness == "strong"
    assert "proxy_strength_capped" not in flags


def test_recalibration_uses_unstable_when_proxy_support_is_narrow_and_breakable():
    usefulness, flags = _recalibrate_usefulness_label(
        base_usefulness_label="strong",
        series_quality_summary={"uses_proxy_series": True},
        sleeve_keys={"global_equity_core"},
        path_quality_label="noisy",
        candidate_fragility_label="fragile",
        threshold_drift_direction="toward_weakening",
        volatility_outlook="elevated",
        scenario_takeaways={
            "favorable_case_is_narrow": True,
            "stress_breaks_candidate_support": True,
            "downside_damage_is_contained": False,
        },
    )
    assert usefulness == "unstable"
    assert "support_unstable" in flags


def test_retriable_kronos_error_detects_tensor_shape_mismatch():
    assert _is_retriable_kronos_error(
        RuntimeError("The size of tensor a (400) must match the size of tensor b (420) at non-singleton dimension 2")
    )
    assert not _is_retriable_kronos_error(RuntimeError("Pandas is unavailable"))


def test_last_good_artifact_annotation_stays_explicit():
    support = _decorate_last_good_artifact(
        {
            "usefulness_label": "usable",
            "quality_flags": ["approved_proxy_series"],
            "series_quality_summary": {"quality_label": "good"},
            "model_metadata": {"model_name": "kronos"},
        },
        error_message="shape mismatch",
        series_quality_summary={"quality_label": "good", "uses_proxy_series": True},
    )
    assert "last_good_artifact_served" in support["quality_flags"]
    assert support["model_metadata"]["last_good_artifact_served"] is True
    assert support["model_metadata"]["last_model_error"] == "shape mismatch"


def test_forecast_refresh_lane_counts_last_good_artifact_served(monkeypatch):
    import app.v2.blueprint_market.blueprint_candidate_forecast_service as forecast_service

    monkeypatch.setattr(
        forecast_service,
        "_connection",
        lambda: sqlite3.connect(":memory:"),
    )
    monkeypatch.setattr(
        forecast_service,
        "list_blueprint_market_candidates",
        lambda conn, candidate_id=None, sleeve_key=None: [{"candidate_id": "candidate_instrument_tail", "symbol": "TAIL"}],
    )
    monkeypatch.setattr(
        forecast_service,
        "ensure_candidate_market_identities",
        lambda conn, cid: [{"candidate_id": "candidate_instrument_tail", "series_role": "direct", "primary_interval": "1day"}],
    )
    monkeypatch.setattr(
        forecast_service,
        "_choose_identity_with_series",
        lambda conn, candidate_id: ({"candidate_id": candidate_id, "series_role": "direct", "primary_interval": "1day"}, []),
    )
    monkeypatch.setattr(
        forecast_service,
        "latest_series_version",
        lambda conn, *, candidate_id, series_role, interval: "series_v1",
    )
    monkeypatch.setattr(
        forecast_service,
        "latest_forecast_artifact",
        lambda conn, *, candidate_id: {
            "input_series_version": "series_v0",
            "market_path_support": {"model_metadata": {"support_semantics_version": "2026-04-13-coverage-recovery-v1"}},
        },
    )
    monkeypatch.setattr(
        forecast_service,
        "build_candidate_market_path_support",
        lambda *args, **kwargs: {
            "usefulness_label": "usable",
            "suppression_reason": None,
            "path_quality_label": "balanced",
            "candidate_fragility_label": "watchful",
            "threshold_drift_direction": "balanced",
            "model_metadata": {"last_good_artifact_served": True, "last_model_error": "shape mismatch"},
        },
    )

    result = run_market_forecast_refresh_lane(candidate_id="candidate_instrument_tail", stale_only=False)

    assert result["served_last_good_count"] == 1
    assert result["refreshed_count"] == 0
    assert result["items"][0]["status"] == "served_last_good"
    assert result["items"][0]["last_model_error"] == "shape mismatch"


def test_choose_identity_prefers_proxy_when_direct_history_is_thin(monkeypatch):
    import app.v2.blueprint_market.blueprint_candidate_forecast_service as forecast_service

    direct_identity = {
        "candidate_id": "candidate_instrument_ssac",
        "series_role": "direct",
        "primary_interval": "1day",
        "forecast_eligibility": "eligible",
    }
    proxy_identity = {
        "candidate_id": "candidate_instrument_ssac",
        "series_role": "approved_proxy",
        "primary_interval": "1day",
        "forecast_eligibility": "eligible",
    }
    direct_rows = [_row("2026-03-02", 100.0) for _ in range(30)]
    proxy_rows = [_row("2025-01-02", 100.0 + index) for index in range(320)]

    monkeypatch.setattr(
        forecast_service,
        "ensure_candidate_market_identities",
        lambda conn, candidate_id: [direct_identity, proxy_identity],
    )
    monkeypatch.setattr(
        forecast_service,
        "load_price_series",
        lambda conn, *, candidate_id, series_role, interval, ascending=True: direct_rows if series_role == "direct" else proxy_rows,
    )
    monkeypatch.setattr(
        forecast_service,
        "check_candidate_series_freshness",
        lambda conn, *, candidate_id, series_role: {
            "series_quality_summary": {
                "bars_present": len(direct_rows if series_role == "direct" else proxy_rows),
                "stale_days": 1,
                "missing_bar_ratio": 0.0,
                "quality_label": "thin" if series_role == "direct" else "good",
            }
        },
    )
    monkeypatch.setattr(forecast_service, "set_forecast_driving_series", lambda *args, **kwargs: None)

    identity, rows = _choose_identity_with_series(sqlite3.connect(":memory:"), candidate_id="candidate_instrument_ssac")
    assert identity is not None
    assert identity["series_role"] == "approved_proxy"
    assert rows == proxy_rows


def test_twelvedata_client_tries_resolved_aliases_before_failing(monkeypatch):
    import app.v2.blueprint_market.twelvedata_price_client as td_client
    from app.services.provider_adapters import ProviderAdapterError

    attempted: list[str] = []

    monkeypatch.setattr(
        td_client,
        "resolve_provider_identifiers",
        lambda conn, *, provider_name, endpoint_family, identifier: {
            "provider_symbol": "IWDA",
            "fallback_aliases": ["IWDA.LSE"],
            "resolution_confidence": 0.85,
        },
    )
    monkeypatch.setattr(td_client, "record_resolution_success", lambda *args, **kwargs: None)
    monkeypatch.setattr(td_client, "record_resolution_failure", lambda *args, **kwargs: None)

    def _fetch(provider_name: str, endpoint_family: str, identifier: str):
        attempted.append(identifier)
        if identifier == "IWDA":
            raise ProviderAdapterError(provider_name, endpoint_family, "No history returned", error_class="empty_response")
        return {"series": [{"datetime": "2026-04-10", "close": "100.0"}]}

    monkeypatch.setattr(td_client, "fetch_provider_data", _fetch)

    client = td_client.TwelveDataPriceClient()
    payload = client.fetch_daily_ohlcv("IWDA")

    assert attempted == ["IWDA", "IWDA.LSE"]
    assert payload["provider_symbol"] == "IWDA.LSE"


def test_default_benchmark_assignment_uses_fetchable_sp500_proxy_and_short_bill_proxy():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    cspx = resolve_benchmark_assignment(
        conn,
        candidate={"symbol": "CSPX"},
        sleeve_key="global_equity_core",
    )
    bils = resolve_benchmark_assignment(
        conn,
        candidate={"symbol": "BILS"},
        sleeve_key="cash_bills",
    )

    assert cspx["benchmark_proxy_symbol"] == "SPY"
    assert cspx["benchmark_source_type"] == "proxy_etf"
    assert bils["benchmark_proxy_symbol"] == "SHV"
