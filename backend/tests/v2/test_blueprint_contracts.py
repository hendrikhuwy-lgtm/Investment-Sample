"""
Integration tests for Blueprint Explorer and Candidate Report contracts.

These tests verify that the enriched contract shapes contain the required
Cortex-facing fields. They call the contract builders directly (no HTTP).
"""
from __future__ import annotations

import sqlite3

import pytest


def _walk_strings(value):
    if isinstance(value, dict):
        for item in value.values():
            yield from _walk_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_strings(item)
    elif isinstance(value, str):
        yield value


def _explorer_contract() -> dict:
    from app.v2.surfaces.blueprint.explorer_contract_builder import build
    return build(snapshot_override_reason="test_force_rebuild")


def _report_contract(candidate_id: str = "VWRA") -> dict:
    from app.v2.surfaces.blueprint.report_contract_builder import build
    return build(candidate_id)


def _compare_contract(candidate_ids: list[str] | None = None) -> dict:
    from app.v2.surfaces.blueprint.compare_contract_builder import build
    return build(candidate_ids or ["candidate_instrument_cmod", "candidate_instrument_sgln"])


def _current_timing_support(
    *,
    timing_state: str = "timing_review",
    timing_artifact_valid: bool = True,
    timing_reasons: list[str] | None = None,
) -> dict:
    return {
        "usefulness_label": "usable",
        "timing_state": timing_state,
        "timing_label": {
            "timing_review": "Timing review",
            "timing_fragile": "Timing fragile",
            "timing_constrained": "Timing constrained",
        }.get(timing_state, "Timing review"),
        "timing_reasons": timing_reasons or ["direct_series_current_and_usable"],
        "timing_artifact_valid": timing_artifact_valid,
        "timing_artifact_schema_status": "current_schema" if timing_artifact_valid else "old_schema_artifact",
    }


# ---------------------------------------------------------------------------
# Blueprint Explorer contract shape tests
# ---------------------------------------------------------------------------

class TestBlueprintExplorerShape:
    def test_top_level_fields_present(self):
        contract = _explorer_contract()
        required = {
            "contract_version", "surface_id", "generated_at",
            "freshness_state", "header_badges", "sleeves", "market_state_summary", "review_posture", "surface_state", "section_states",
        }
        for field in required:
            assert field in contract, f"Missing top-level field: {field}"

    def test_header_badges_are_backend_owned(self):
        contract = _explorer_contract()
        expected = {
            "no_data": {"label": "No holdings loaded", "tone": "warn"},
            "fresh_full_rebuild": {"label": "Fresh full rebuild", "tone": "good"},
            "fresh_partial_rebuild": {"label": "Fresh partial rebuild", "tone": "good"},
            "stored_valid_context": {"label": "Stored valid context", "tone": "neutral"},
            "degraded_monitoring_mode": {"label": "Degraded monitoring mode", "tone": "warn"},
            "execution_failed_or_incomplete": {"label": "Execution failed or incomplete", "tone": "bad"},
        }
        assert contract["header_badges"] == [expected[contract["freshness_state"]]]

    def test_sleeves_is_list(self):
        contract = _explorer_contract()
        assert isinstance(contract["sleeves"], list)

    def test_sleeve_has_required_fields(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned — candidate registry may be empty.")
        sleeve = sleeves[0]
        required_sleeve_fields = {
            "sleeve_id", "sleeve_purpose", "pressure_level", "capital_memo",
            "reopen_condition", "lead_candidate_id", "lead_candidate_name",
            "visible_state", "implication_summary", "why_it_leads", "main_limit",
            "candidates", "priority_rank", "candidate_count", "support_pillars", "funding_path",
            "sleeve_role_statement", "cycle_sensitivity", "base_allocation_rationale",
            "sleeve_name", "target_pct", "target_display", "min_pct", "max_pct",
            "sort_midpoint_pct", "is_nested", "parent_sleeve_id", "parent_sleeve_name",
            "counts_as_top_level_total", "target_label", "range_label",
            "sleeve_actionability_state", "sleeve_reviewability_state", "sleeve_block_reason_summary",
            "blocked_count", "reviewable_count", "bounded_count", "ready_count",
            "active_support_candidate_count", "leader_is_blocked_but_sleeve_still_reviewable",
            "failure_class_summary",
        }
        for field in required_sleeve_fields:
            assert field in sleeve, f"Sleeve missing field: {field}"

    def test_sleeves_follow_ips_rank_order(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        ranks = [int(sleeve["priority_rank"]) for sleeve in sleeves]
        assert ranks == sorted(ranks)

    def test_nested_sleeves_emit_parent_and_non_additive_flags(self):
        contract = _explorer_contract()
        nested = [sleeve for sleeve in contract["sleeves"] if sleeve.get("is_nested")]
        if not nested:
            pytest.skip("No nested sleeves returned.")
        sample = nested[0]
        assert sample["parent_sleeve_name"] == "Global Equity Core"
        assert sample["counts_as_top_level_total"] is False

    def test_sleeve_has_candidates_array(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        sleeve = sleeves[0]
        assert isinstance(sleeve["candidates"], list)
        assert len(sleeve["candidates"]) >= 1, "Each sleeve should have at least one candidate."

    def test_candidate_has_enriched_fields(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        candidate = sleeves[0]["candidates"][0]
        required_candidate_fields = {
            "candidate_id", "sleeve_key", "symbol", "name",
            "instrument_quality", "portfolio_fit_now", "capital_priority_now",
            "gate_state", "status_label", "why_now", "what_blocks_action", "what_changes_view",
            "action_boundary", "funding_source", "visible_decision_state",
            "implication_summary", "score", "issuer", "expense_ratio", "aum",
            "implementation_profile", "recommendation_gate", "reconciliation_status",
            "source_authority_fields", "reconciliation_report", "data_quality_summary",
            "research_support_summary", "market_path_support",
            "candidate_row_summary", "candidate_supporting_factors", "candidate_penalizing_factors",
            "report_summary_strip", "source_confidence_label", "coverage_status",
            "coverage_workflow_summary", "score_rubric", "market_path_objective", "market_path_case_note",
            "benchmark_full_name", "exposure_summary", "ter_bps", "spread_proxy_bps",
            "aum_usd", "aum_state", "sg_tax_posture", "distribution_policy",
            "replication_risk_note", "current_weight_pct", "weight_state",
            "investor_decision_state", "source_integrity_summary",
            "failure_class_summary", "score_decomposition", "identity_state", "blocker_category",
        }
        for field in required_candidate_fields:
            assert field in candidate, f"Candidate missing field: {field}"

    def test_candidate_rich_fields_are_non_null_in_live_explorer_rows(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        for sleeve in sleeves:
            for candidate in sleeve["candidates"]:
                assert candidate["sleeve_key"]
                assert candidate["gate_state"]
                assert isinstance(candidate["coverage_workflow_summary"], dict)
                assert "market_path_support" in candidate

    def test_snapshot_guard_rejects_thinner_harsher_blueprint_snapshot(self):
        from app.v2.surfaces.blueprint.explorer_contract_builder import _blueprint_snapshot_guard_reason

        previous = {
            "sleeves": [
                {
                    "candidates": [
                        {
                            "candidate_id": "candidate_instrument_vwra",
                            "status_label": "Alternative candidate",
                            "visible_decision_state": {"state": "review"},
                            "sleeve_key": "global_equity_core",
                            "gate_state": "review_only",
                            "coverage_workflow_summary": {"status": "proxy_ready"},
                            "market_path_support": _current_timing_support(),
                        }
                    ]
                }
            ]
        }
        candidate = {
            "sleeves": [
                {
                    "candidates": [
                        {
                            "candidate_id": "candidate_instrument_vwra",
                            "status_label": "Blocked candidate",
                            "visible_decision_state": {"state": "blocked"},
                            "sleeve_key": None,
                            "gate_state": None,
                            "coverage_workflow_summary": None,
                            "market_path_support": None,
                        }
                    ]
                }
            ]
        }
        reason = _blueprint_snapshot_guard_reason(
            previous_contract=previous,
            candidate_contract=candidate,
        )
        assert reason is not None

    def test_snapshot_guard_rejects_source_completion_metric_regression(self):
        from app.v2.surfaces.blueprint.explorer_contract_builder import _blueprint_snapshot_guard_reason

        previous = {
            "sleeves": [
                {
                    "candidates": [
                        {
                            "candidate_id": "candidate_instrument_vwra",
                            "status_label": "Alternative candidate",
                            "visible_decision_state": {"state": "review"},
                            "sleeve_key": "global_equity_core",
                            "gate_state": "review_only",
                            "coverage_workflow_summary": {"status": "proxy_ready"},
                            "market_path_support": _current_timing_support(),
                            "source_completion_summary": {
                                "state": "complete",
                                "critical_fields_completed": 14,
                                "critical_fields_total": 14,
                                "weak_fields": [],
                                "stale_fields": [],
                                "conflict_fields": [],
                            },
                        }
                    ]
                }
            ]
        }
        candidate = {
            "sleeves": [
                {
                    "candidates": [
                        {
                            "candidate_id": "candidate_instrument_vwra",
                            "status_label": "Alternative candidate",
                            "visible_decision_state": {"state": "review"},
                            "sleeve_key": "global_equity_core",
                            "gate_state": "review_only",
                            "coverage_workflow_summary": {"status": "proxy_ready"},
                            "market_path_support": _current_timing_support(),
                            "source_completion_summary": {
                                "state": "complete",
                                "critical_fields_completed": 14,
                                "critical_fields_total": 14,
                                "weak_fields": ["aum"],
                                "stale_fields": [],
                                "conflict_fields": [],
                            },
                        }
                    ]
                }
            ]
        }
        reason = _blueprint_snapshot_guard_reason(
            previous_contract=previous,
            candidate_contract=candidate,
        )
        assert reason == "rejected_blueprint_snapshot_weak_fields_regressed:candidate_instrument_vwra:0->1"

    def test_explorer_cached_old_schema_artifact_becomes_current_route_history_assessment(self, monkeypatch):
        import app.v2.surfaces.blueprint.explorer_contract_builder as explorer_builder

        old_artifact_support = {
            "eligibility_state": "eligible",
            "usefulness_label": "unstable",
            "suppression_reason": None,
            "observed_series": [],
            "projected_series": [],
            "candidate_implication": "Old proxy artifact said the path was fragile.",
            "generated_at": "2026-04-01T00:00:00+00:00",
            "provider_source": "twelve_data+approved_proxy+kronos",
            "forecast_horizon": 21,
            "forecast_interval": "1day",
            "path_quality_label": "noisy",
            "candidate_fragility_label": "acute",
            "threshold_drift_direction": "toward_weakening",
        }

        monkeypatch.setattr(
            explorer_builder,
            "latest_forecast_artifact",
            lambda *args, **kwargs: {
                "artifact_id": "artifact_old_proxy",
                "created_at": "2026-04-01T00:00:00+00:00",
                "market_path_support": old_artifact_support,
            },
        )
        monkeypatch.setattr(
            explorer_builder,
            "_latest_forecast_run",
            lambda *args, **kwargs: {
                "run_status": "failed",
                "generated_at": "2026-04-20T00:00:00+00:00",
                "details": {"error": "Predicted bar geometry is invalid."},
            },
        )
        monkeypatch.setattr(
            explorer_builder,
            "_series_quality_for_role",
            lambda *args, **kwargs: {
                "quality_label": "good",
                "bars_present": 400,
                "stale_days": 1,
                "uses_proxy_series": kwargs.get("series_role") != "direct",
            }
            if kwargs.get("series_role") == "direct"
            else {
                "quality_label": "degraded",
                "bars_present": 400,
                "stale_days": 14,
                "uses_proxy_series": True,
            },
        )

        result = explorer_builder._try_load_cached_forecast_support(
            sqlite3.connect(":memory:"),
            candidate_id="candidate_instrument_ssac",
            label="SSAC",
            symbol="SSAC",
        )

        support = result["market_path_support"]
        assert support["timing_state"] == "timing_review"
        assert support["timing_artifact_valid"] is True
        assert support["timing_artifact_schema_status"] == "current_schema"
        assert support["raw_artifact_schema_status"] == "old_schema_artifact"
        assert "old_artifact_replaced" in support["timing_reasons"]
        assert "forecast_output_invalid_quarantined" in support["timing_reasons"]
        assert "direct_series_current_and_usable" in support["timing_reasons"]
        assert support["validation_status"] == "forecast_output_invalid_quarantined"
        assert support["usefulness_label"] == "usable_with_caution"

    def test_explorer_timing_usefulness_contract_is_current_schema_and_meaningful(self):
        contract = _explorer_contract()
        rows = [
            candidate
            for sleeve in contract["sleeves"]
            for candidate in sleeve["candidates"]
        ]
        if not rows:
            pytest.skip("No candidates returned.")
        states = {
            dict(candidate.get("market_path_support") or {}).get("timing_state")
            for candidate in rows
        }
        for candidate in rows:
            support = dict(candidate.get("market_path_support") or {})
            assert support.get("timing_artifact_schema_status") == "current_schema"
            assert support.get("timing_artifact_valid") is True
            assert support.get("latest_forecast_failure_reason") != "forecast_model_missing_dependency"
            assert support.get("validation_status")
            if support.get("direct_series_status") in {"good", "watch"} and int(support.get("direct_series_last_bar_age_days") or 999) <= 7:
                assert support.get("timing_state") in {"timing_ready", "timing_review"}
        assert states != {"timing_constrained"}

    def test_explorer_proxy_dependent_cmod_has_explicit_proxy_review_state(self):
        contract = _explorer_contract()
        cmod_rows = [
            candidate
            for sleeve in contract["sleeves"]
            for candidate in sleeve["candidates"]
            if candidate.get("symbol") == "CMOD"
        ]
        if not cmod_rows:
            pytest.skip("CMOD not in current registry.")
        for candidate in cmod_rows:
            support = dict(candidate.get("market_path_support") or {})
            assert support.get("timing_state") == "timing_review"
            assert support.get("driving_series_role") == "approved_proxy"
            assert "proxy_series_fresh_and_approved" in set(support.get("timing_reasons") or [])

    def test_snapshot_guard_rejects_noncanonical_timing_fragile(self):
        from app.v2.surfaces.blueprint.explorer_contract_builder import _blueprint_snapshot_guard_reason

        candidate = {
            "sleeves": [
                {
                    "candidates": [
                        {
                            "candidate_id": "candidate_instrument_ssac",
                            "status_label": "Alternative candidate",
                            "visible_decision_state": {"state": "review"},
                            "sleeve_key": "global_equity_core",
                            "gate_state": "review_only",
                            "coverage_workflow_summary": {"status": "direct_ready"},
                            "source_completion_summary": {
                                "state": "complete",
                                "critical_fields_completed": 14,
                                "critical_fields_total": 14,
                                "weak_fields": [],
                                "stale_fields": [],
                                "conflict_fields": [],
                            },
                            "market_path_support": _current_timing_support(
                                timing_state="timing_fragile",
                                timing_artifact_valid=False,
                                timing_reasons=["old_schema_artifact"],
                            ),
                        }
                    ]
                }
            ]
        }

        assert _blueprint_snapshot_guard_reason(previous_contract=None, candidate_contract=candidate) == (
            "rejected_blueprint_snapshot_noncurrent_timing_artifact:candidate_instrument_ssac:old_schema_artifact"
        )

    def test_explorer_score_decomposition_exposes_explicit_rubric_fields(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        candidate = sleeves[0]["candidates"][0]
        score = candidate["score_decomposition"]
        assert score["score_model_version"] == "recommendation_score_v3"
        assert score["total_score"] == score["recommendation_score"]
        assert "source_integrity_score" in score
        assert "benchmark_fidelity_score" in score
        assert "long_horizon_quality_score" in score
        assert "market_path_support_score" in score
        assert "instrument_quality_score" in score
        assert "portfolio_fit_score" in score
        assert "recommendation_score" in score
        assert "recommendation_merit_score" in score
        assert "investment_merit_score" in score
        assert "deployability_score" in score
        assert "truth_confidence_score" in score
        assert "deployment_score" in score
        assert "optimality_score" in score
        assert "readiness_score" in score
        assert "admissibility_identity_score" in score
        component_ids = {component["component_id"] for component in score["components"]}
        assert "benchmark_fidelity" in component_ids
        assert "long_horizon_quality" in component_ids
        assert "market_path_support" in component_ids
        assert "instrument_quality" in component_ids
        assert "portfolio_fit" in component_ids

    def test_explorer_source_completion_summary_exposes_clean_contract_fields(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        candidate = sleeves[0]["candidates"][0]
        summary = candidate["source_completion_summary"]
        assert "weak_fields" in summary
        assert "stale_fields" in summary
        assert "conflict_fields" in summary
        assert "authority_clean" in summary
        assert "freshness_clean" in summary
        assert "conflict_clean" in summary
        assert "completeness_clean" in summary
        assert "completion_reasons" in summary

    def test_explorer_sleeve_recommendation_score_uses_recommendation_basis(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        recommendation = sleeves[0]["recommendation_score"]
        assert recommendation["score_basis"] == "recommendation_score"
        assert recommendation["factor_count_used"] == 4
        assert "leader_candidate_recommendation_score" in recommendation
        assert "leader_truth_confidence_score" in recommendation
        assert "leader_candidate_deployability_score" in recommendation
        assert "leader_candidate_investment_merit_score" in recommendation
        assert "leader_candidate_deployment_score" in recommendation
        assert "depth_score" in recommendation
        assert "blocker_burden_score" in recommendation

    def test_visible_decision_state_is_structured(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        vds = sleeves[0]["candidates"][0]["visible_decision_state"]
        assert isinstance(vds, dict), "visible_decision_state must be a dict, not a string."
        assert "state" in vds
        assert "allowed_action" in vds
        assert "rationale" in vds

    def test_sleeve_visible_state_tracks_aggregate_posture(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        mapping = {
            "ready": "eligible",
            "reviewable": "review",
            "bounded": "watch",
            "blocked": "blocked",
        }
        for sleeve in sleeves:
            assert sleeve["visible_state"] == mapping[str(sleeve["sleeve_actionability_state"])]

    def test_pressure_level_valid(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        valid_levels = {"low", "medium", "high"}
        for sleeve in sleeves:
            assert sleeve["pressure_level"] in valid_levels, (
                f"Unexpected pressure_level: {sleeve['pressure_level']}"
            )

    def test_instrument_quality_valid(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        valid = {"High", "Moderate", "Low"}
        for sleeve in sleeves:
            for candidate in sleeve["candidates"]:
                assert candidate["instrument_quality"] in valid, (
                    f"Unexpected instrument_quality: {candidate['instrument_quality']}"
                )

    def test_capital_priority_now_valid(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        valid = {"First call on next dollar", "Second choice", "No new capital"}
        for sleeve in sleeves:
            for candidate in sleeve["candidates"]:
                assert candidate["capital_priority_now"] in valid, (
                    f"Unexpected capital_priority_now: {candidate['capital_priority_now']}"
                )

    def test_candidate_id_is_stable_format(self):
        contract = _explorer_contract()
        sleeves = contract["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        for sleeve in sleeves:
            for candidate in sleeve["candidates"]:
                assert str(candidate["candidate_id"]).startswith("candidate_"), (
                    f"Unstable candidate_id format: {candidate['candidate_id']}"
                )

    def test_live_candidate_scope_excludes_placeholders(self):
        contract = _explorer_contract()
        blocked = {
            "UCITS_MMF_PLACEHOLDER",
            "SG_TBILL_POLICY",
            "SGD_MMF_POLICY",
            "SGD_CASH_RESERVE",
            "SPX_LONG_PUT",
        }
        for sleeve in contract["sleeves"]:
            for candidate in sleeve["candidates"]:
                assert candidate["symbol"] not in blocked

    def test_explorer_prefers_cached_candidate_forecast_support_when_available(self, monkeypatch):
        import app.v2.surfaces.blueprint.report_contract_builder as report_builder

        monkeypatch.setattr(
            report_builder,
            "_try_load_cached_forecast_support",
            lambda candidate_id, *, label: {
                "forecast_support": {
                    "provider": "chronos",
                    "model_name": "chronos",
                    "horizon": 21,
                    "support_strength": "supported",
                    "confidence_summary": f"{label} scenario support sourced from cached forecast run.",
                    "degraded_state": None,
                    "generated_at": "2026-04-07T00:00:00+00:00",
                },
                "scenario_blocks": [],
                "market_path_support": {
                    "eligibility_state": "eligible",
                    "usefulness_label": "moderate",
                    "suppression_reason": None,
                    "observed_series": [],
                    "projected_series": [{"timestamp": "2026-04-28T00:00:00+00:00", "value": 101.2}],
                    "uncertainty_band": None,
                    "volatility_outlook": "stable",
                    "path_stability": "balanced",
                    "threshold_map": [],
                    "scenario_summary": [],
                    "candidate_implication": "Bounded support only.",
                    "generated_at": "2026-04-07T00:00:00+00:00",
                    "provider_source": "twelve_data+kronos",
                    "forecast_horizon": 21,
                    "forecast_interval": "1day",
                    "quality_flags": [],
                    "series_quality_summary": {"bars_expected": 300, "bars_present": 300, "missing_bar_ratio": 0.0, "stale_days": 1, "has_corporate_action_uncertainty": False, "uses_proxy_series": False, "quality_label": "good"},
                    "model_metadata": {"model_name": "kronos"},
                },
            },
        )
        contract = _explorer_contract()
        candidate = contract["sleeves"][0]["candidates"][0]
        assert candidate["forecast_support"]["provider"] == "chronos"
        assert candidate["market_path_support"]["provider_source"] == "twelve_data+kronos"
        assert "Market path support is available" in candidate["scenario_readiness_note"]

    def test_explorer_research_summary_stays_lightweight(self, monkeypatch):
        import app.v2.features.research_support as research_support

        monkeypatch.setattr(
            research_support,
            "build_research_support_pack",
            lambda **kwargs: (_ for _ in ()).throw(AssertionError("full research pack should not run in explorer")),
        )

        contract = _explorer_contract()
        candidate = contract["sleeves"][0]["candidates"][0]
        assert "research_support_summary" in candidate

    def test_instrument_truth_cache_only_mode_skips_live_provider_refresh(self, monkeypatch):
        import app.v2.donors.instrument_truth as instrument_truth

        monkeypatch.setattr(instrument_truth, "routed_provider_candidates", lambda *args, **kwargs: ["yahoo_finance"])
        monkeypatch.setattr(instrument_truth, "provider_support_status", lambda *args, **kwargs: (True, None))
        monkeypatch.setattr(instrument_truth, "get_cached_provider_snapshot", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            instrument_truth,
            "fetch_routed_family",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("live provider refresh should not run")),
        )

        assert instrument_truth._cached_routed_payload(
            sqlite3.connect(":memory:"),
            surface_name="blueprint",
            endpoint_family="quote_latest",
            identifier="CMOD",
            allow_live_fetch=False,
        ) == {}

    def test_explorer_instrument_truth_cache_only_mode_skips_live_adapters(self, monkeypatch):
        import app.v2.donors.instrument_truth as instrument_truth

        class DummyConnection:
            row_factory = None

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeDonor:
            def __init__(self, conn):
                self.conn = conn

            def list_candidates(self):
                return [
                    {
                        "symbol": "CMOD",
                        "name": "CMOD ETF",
                        "asset_class": "real_assets",
                        "instrument_type": "ETF",
                        "sleeve_key": "real_assets",
                        "extra": {},
                    }
                ]

            def resolve_benchmark_assignment(self, *, candidate, sleeve_key):
                return {"benchmark_key": "commodity_broad", "benchmark_label": "Commodities", "benchmark_confidence": "high"}

        cache_only_calls: list[bool] = []

        def fake_cached_payload(*args, allow_live_fetch=True, **kwargs):
            cache_only_calls.append(allow_live_fetch)
            return {}

        monkeypatch.setattr(instrument_truth, "_connection", lambda: DummyConnection())
        monkeypatch.setattr(instrument_truth, "ensure_candidate_registry_tables", lambda conn: None)
        monkeypatch.setattr(instrument_truth, "SQLiteBlueprintDonor", FakeDonor)
        monkeypatch.setattr(instrument_truth, "_cached_routed_payload", fake_cached_payload)
        monkeypatch.setattr(
            instrument_truth,
            "get_issuer_adapter",
            lambda: (_ for _ in ()).throw(AssertionError("issuer adapter should not run")),
        )
        monkeypatch.setattr(
            instrument_truth,
            "get_market_adapter",
            lambda: (_ for _ in ()).throw(AssertionError("market adapter should not run")),
        )

        truth = instrument_truth.get_instrument_truth("candidate_instrument_cmod", allow_live_fetch=False)

        assert truth.metrics["sleeve_key"] == "real_assets"
        assert cache_only_calls
        assert set(cache_only_calls) == {False}

    def test_explorer_contract_does_not_leak_local_fixture_paths(self):
        contract = _explorer_contract()
        leaked = [
            value for value in _walk_strings(contract)
            if "/Users/" in value or "tests/fixtures" in value
        ]
        assert leaked == []

    def test_blocked_investor_state_stays_blocked_in_visible_state(self):
        contract = _explorer_contract()
        for sleeve in contract["sleeves"]:
            for candidate in sleeve["candidates"]:
                if candidate.get("investor_decision_state") == "blocked":
                    assert candidate["visible_decision_state"]["state"] == "blocked"
                    return
        pytest.skip("No blocked live candidate emitted in current registry snapshot.")


# ---------------------------------------------------------------------------
# Candidate Report contract shape tests
# ---------------------------------------------------------------------------

class TestCandidateReportShape:
    def test_top_level_fields_present(self):
        contract = _report_contract()
        required = {
            "contract_version", "surface_id", "generated_at", "freshness_state",
            "candidate_id", "sleeve_id", "name", "investment_case",
            "benchmark_full_name", "exposure_summary", "ter_bps", "spread_proxy_bps",
            "aum_usd", "aum_state", "sg_tax_posture", "distribution_policy",
            "replication_risk_note", "current_weight_pct", "weight_state",
            "investor_decision_state", "source_integrity_summary", "failure_class_summary", "score_decomposition",
            "identity_state", "blocker_category",
            "candidate_row_summary", "candidate_supporting_factors", "candidate_penalizing_factors",
            "report_summary_strip", "source_confidence_label", "coverage_status",
            "coverage_workflow_summary", "score_rubric", "market_path_objective", "market_path_case_note",
            "report_cache_state", "report_generated_at", "report_source_snapshot_at", "report_loading_hint",
            "current_implication", "action_boundary", "what_changes_view",
            "visible_decision_state", "upgrade_condition", "downgrade_condition",
            "kill_condition", "main_tradeoffs", "baseline_comparisons",
            "evidence_depth", "mandate_boundary", "doctrine_annotations",
            "report_tabs", "holdings_overlay", "surface_state", "section_states",
            "market_history_block", "scenario_blocks", "risk_blocks", "competition_blocks",
            "evidence_sources", "decision_thresholds", "implementation_profile",
            "recommendation_gate", "reconciliation_status", "source_authority_fields",
            "reconciliation_report", "data_quality_summary", "surface_snapshot_id",
            "market_path_support",
        }
        for field in required:
            assert field in contract, f"Candidate report missing field: {field}"

    def test_report_market_path_support_contract_block_when_service_available(self, monkeypatch):
        import app.v2.surfaces.blueprint.report_contract_builder as report_builder

        sample_support = {
            "eligibility_state": "eligible",
            "usefulness_label": "moderate",
            "suppression_reason": None,
            "observed_series": [{"timestamp": "2026-04-01T00:00:00+00:00", "value": 100.0}],
            "projected_series": [{"timestamp": "2026-04-28T00:00:00+00:00", "value": 104.0}],
            "uncertainty_band": None,
            "volatility_outlook": "stable",
            "path_stability": "balanced",
            "threshold_map": [{"threshold_id": "base_case", "label": "Base path", "value": 104.0, "relation": "above", "delta_pct": 4.0, "note": "Base path ends above the current anchor."}],
            "scenario_summary": [{"scenario_type": "base", "label": "Base", "summary": "Bounded base case.", "path": [{"timestamp": "2026-04-28T00:00:00+00:00", "value": 104.0}]}],
            "candidate_implication": "Projected path stays bounded.",
            "generated_at": "2026-04-07T00:00:00+00:00",
            "provider_source": "twelve_data+kronos",
            "forecast_horizon": 21,
            "forecast_interval": "1day",
            "quality_flags": [],
            "series_quality_summary": {"bars_expected": 300, "bars_present": 300, "missing_bar_ratio": 0.0, "stale_days": 1, "has_corporate_action_uncertainty": False, "uses_proxy_series": False, "quality_label": "good"},
            "model_metadata": {"model_name": "kronos"},
        }

        monkeypatch.setattr(report_builder, "latest_forecast_artifact", lambda *args, **kwargs: {"market_path_support": sample_support})
        monkeypatch.setattr(
            report_builder,
            "compact_forecast_support_from_market_path",
            lambda support: {
                "provider": "twelve_data+kronos",
                "model_name": "kronos",
                "horizon": 21,
                "support_strength": "moderate",
                "confidence_summary": "Projected path stays bounded.",
                "degraded_state": None,
                "generated_at": "2026-04-07T00:00:00+00:00",
            },
        )

        contract = _report_contract()
        assert contract["market_path_support"]["provider_source"] == "twelve_data+kronos"
        assert contract["forecast_support"]["provider"] == "twelve_data+kronos"

    def test_report_contract_does_not_leak_local_fixture_paths(self):
        contract = _report_contract()
        leaked = [
            value for value in _walk_strings(contract)
            if "/Users/" in value or "tests/fixtures" in value
        ]
        assert leaked == []

    def test_report_emits_admissibility_and_implementation_context(self):
        contract = _report_contract()
        gate = contract["recommendation_gate"]
        profile = contract["implementation_profile"]
        reconciliation = contract["reconciliation_status"]
        assert isinstance(gate, dict)
        assert "gate_state" in gate
        assert "blocked_reasons" in gate
        assert isinstance(profile, dict)
        assert "execution_suitability" in profile
        assert "summary" in profile
        assert isinstance(reconciliation, dict)
        assert "status" in reconciliation
        assert "summary" in reconciliation

    def test_report_emits_research_support_pack(self):
        contract = _report_contract()
        assert "research_support" in contract
        assert isinstance(contract["research_support"], dict)

    def test_market_history_block_exposes_field_provenance(self):
        contract = _report_contract()
        block = contract["market_history_block"]
        assert isinstance(block, dict)
        provenance = block.get("field_provenance")
        assert isinstance(provenance, dict)
        assert "instrument.price" in provenance

    def test_visible_decision_state_is_structured(self):
        contract = _report_contract()
        vds = contract["visible_decision_state"]
        assert isinstance(vds, dict), "visible_decision_state must be a structured dict."
        assert "state" in vds
        assert "allowed_action" in vds
        assert "rationale" in vds

    def test_report_and_explorer_share_candidate_visible_state(self):
        explorer = _explorer_contract()
        sleeves = explorer["sleeves"]
        if not sleeves:
            pytest.skip("No sleeves returned.")
        candidate = sleeves[0]["candidates"][0]
        report = _report_contract(candidate["candidate_id"])
        assert report["visible_decision_state"]["state"] == candidate["visible_decision_state"]["state"]

    def test_report_builder_reads_requested_explorer_snapshot_row(self, monkeypatch):
        import app.v2.surfaces.blueprint.report_contract_builder as report_builder

        monkeypatch.setattr(
            report_builder,
            "get_surface_snapshot",
            lambda snapshot_id: {
                "snapshot_id": snapshot_id,
                "contract": {
                    "sleeves": [
                        {
                            "candidates": [
                                {
                                    "candidate_id": "candidate_instrument_vwra",
                                    "visible_decision_state": {
                                        "state": "blocked",
                                        "allowed_action": "monitor",
                                        "rationale": "Bound snapshot row wins.",
                                    },
                                }
                            ]
                        }
                    ]
                },
            },
        )

        row = report_builder._bound_explorer_candidate_row("candidate_instrument_vwra", "surface_snapshot_bound")

        assert row["visible_decision_state"]["rationale"] == "Bound snapshot row wins."

    def test_report_score_decomposition_exposes_explicit_rubric_fields(self):
        contract = _report_contract()
        score = contract["score_decomposition"]
        assert score["score_model_version"] == "recommendation_score_v3"
        assert score["total_score"] == score["recommendation_score"]
        assert "source_integrity_score" in score
        assert "benchmark_fidelity_score" in score
        assert "long_horizon_quality_score" in score
        assert "market_path_support_score" in score
        assert "instrument_quality_score" in score
        assert "portfolio_fit_score" in score
        assert "recommendation_score" in score
        assert "investment_merit_score" in score
        assert "deployability_score" in score
        assert "deployment_score" in score
        assert "optimality_score" in score
        assert "readiness_score" in score
        assert "admissibility_identity_score" in score
        component_ids = {component["component_id"] for component in score["components"]}
        assert "benchmark_fidelity" in component_ids
        assert "long_horizon_quality" in component_ids
        assert "market_path_support" in component_ids
        assert "instrument_quality" in component_ids
        assert "portfolio_fit" in component_ids

    def test_report_emits_score_rubric_and_loading_metadata(self):
        contract = _report_contract()
        rubric = contract["score_rubric"]
        assert isinstance(rubric, dict)
        assert rubric["dimension_priority_order"]
        assert rubric["families"]
        loading = contract["report_loading_hint"]
        assert isinstance(loading, dict)
        assert loading["route_cache_state"] == contract["report_cache_state"]
        assert isinstance(contract["route_cache_state"], dict)

    def test_report_emits_coverage_workflow_summary(self):
        contract = _report_contract()
        coverage = contract["coverage_workflow_summary"]
        assert isinstance(coverage, dict)
        assert "status" in coverage
        assert "checklist" in coverage
        assert "symbol_alias_registry" in coverage
        assert contract["coverage_status"] == coverage["status"]

    def test_baseline_comparisons_includes_cash_and_incumbent(self):
        contract = _report_contract()
        labels = [bc["label"] for bc in contract["baseline_comparisons"]]
        assert any("cash" in label.lower() for label in labels), (
            "baseline_comparisons must include a cash/do-nothing entry."
        )
        assert any("incumbent" in label.lower() for label in labels), (
            "baseline_comparisons must include an incumbent entry."
        )

    def test_report_tabs_present_and_non_empty(self):
        contract = _report_contract()
        tabs = contract["report_tabs"]
        assert isinstance(tabs, list)
        assert len(tabs) >= 6, "report_tabs should expose the Cortex report tab family."
        assert "investment_case" in tabs
        assert "market_history" in tabs
        assert "scenarios" in tabs
        assert "risks" in tabs
        assert "competition" in tabs
        assert "evidence" in tabs

    def test_doctrine_annotations_is_list(self):
        contract = _report_contract()
        assert isinstance(contract["doctrine_annotations"], list)

    def test_what_changes_view_is_string(self):
        contract = _report_contract()
        assert isinstance(contract["what_changes_view"], str)

    def test_upgrade_downgrade_kill_conditions_are_strings_or_none(self):
        contract = _report_contract()
        for field in ("upgrade_condition", "downgrade_condition", "kill_condition"):
            value = contract[field]
            assert value is None or isinstance(value, str), (
                f"{field} must be str or None, got {type(value)}"
            )

    def test_action_boundary_is_string_or_none(self):
        contract = _report_contract()
        ab = contract["action_boundary"]
        assert ab is None or isinstance(ab, str)

    def test_candidate_id_stable_format(self):
        contract = _report_contract()
        assert str(contract["candidate_id"]).startswith("candidate_")


class TestBlueprintCompareShape:
    def test_compare_contract_exposes_backend_owned_semantics(self):
        contract = _compare_contract()
        required = {
            "compare_ids",
            "sleeve_name",
            "candidates",
            "leader_candidate_id",
            "compare_readiness_state",
            "compare_readiness_note",
            "substitution_verdict",
            "substitution_rationale",
            "substitution_answer",
            "winner_for_sleeve_job",
            "loser_weakness_summary",
            "change_the_read_summary",
            "compare_investor_summary",
            "compare_dimensions",
            "dimension_groups",
            "dimension_priority_order",
            "discriminating_dimension_ids",
            "insufficient_dimensions",
            "path_asymmetry",
            "downside_asymmetry",
            "stability_advantage",
            "market_path_compare_note",
            "compare_decision",
        }
        for field in required:
            assert field in contract, f"Compare contract missing field: {field}"

    def test_compare_decision_includes_backend_decision_read_model(self):
        contract = _compare_contract(["candidate_instrument_cspx", "candidate_instrument_iwda"])
        decision = contract["compare_decision"]

        required = {
            "compare_id",
            "sleeve_id",
            "candidate_a_id",
            "candidate_b_id",
            "substitution_assessment",
            "winner_summary",
            "decision_rule",
            "delta_table",
            "portfolio_consequence",
            "scenario_winners",
            "flip_conditions",
            "evidence_diff",
        }
        for field in required:
            assert field in decision, f"Compare decision missing field: {field}"
        assert decision["candidate_a_id"] == "candidate_instrument_cspx"
        assert decision["candidate_b_id"] == "candidate_instrument_iwda"
        assert decision["sleeve_id"] == "sleeve_global_equity_core"

    def test_compare_decision_identifies_partial_substitutes(self):
        contract = _compare_contract(["candidate_instrument_cspx", "candidate_instrument_iwda"])
        substitution = contract["compare_decision"]["substitution_assessment"]

        assert substitution["status"] == "partial_substitutes"
        assert substitution["are_true_substitutes"] is False
        assert "not perfect substitutes" in substitution["summary"].lower()

    def test_compare_decision_delta_table_covers_required_investor_fields(self):
        contract = _compare_contract(["candidate_instrument_cspx", "candidate_instrument_iwda"])
        delta_rows = contract["compare_decision"]["delta_table"]
        row_ids = {row["row_id"] for row in delta_rows}

        expected = {
            "sleeve_job",
            "exposure_scope",
            "benchmark",
            "diversification",
            "ter",
            "spread",
            "aum",
            "tracking_difference",
            "domicile",
            "trading_currency",
            "listing_exchange",
            "distribution_type",
            "replication_method",
            "source_confidence",
            "timing_state",
            "deployability_posture",
            "portfolio_fit",
        }
        assert expected.issubset(row_ids)
        for row in delta_rows:
            assert {"row_id", "label", "candidate_a_value", "candidate_b_value", "winner", "implication"}.issubset(row)

    def test_compare_decision_separates_winners_by_use_case(self):
        contract = _compare_contract(["candidate_instrument_cspx", "candidate_instrument_iwda"])
        winners = contract["compare_decision"]["winner_summary"]

        assert winners["best_overall"] in {"candidate_a", "candidate_b", "tie", "depends", "no_clear_winner"}
        assert winners["investment_winner"] in {"candidate_a", "candidate_b", "tie", "depends", "no_clear_winner"}
        assert winners["deployment_winner"] in {"candidate_a", "candidate_b", "tie", "depends", "no_clear_winner"}
        assert winners["evidence_winner"] in {"candidate_a", "candidate_b", "tie", "depends", "no_clear_winner"}
        assert winners["timing_winner"] in {"candidate_a", "candidate_b", "tie", "depends", "no_clear_winner"}
        assert isinstance(winners["summary"], str)

    def test_compare_decision_portfolio_consequence_handles_missing_holdings_overlay(self):
        contract = _compare_contract(["candidate_instrument_cspx", "candidate_instrument_iwda"])
        consequence = contract["compare_decision"]["portfolio_consequence"]

        for side in ("candidate_a", "candidate_b"):
            assert consequence[side]["candidate_id"].startswith("candidate_")
            assert isinstance(consequence[side]["portfolio_effect"], str)
            assert isinstance(consequence[side]["funding_path_effect"], str)
            assert consequence[side]["confidence"] in {"high", "medium", "low"}

    def test_compare_decision_scenario_winners_flip_conditions_and_evidence_diff_are_backend_owned(self):
        contract = _compare_contract(["candidate_instrument_cspx", "candidate_instrument_iwda"])
        decision = contract["compare_decision"]

        assert len(decision["scenario_winners"]) >= 4
        for row in decision["scenario_winners"]:
            assert {"scenario", "candidate_a_effect", "candidate_b_effect", "winner", "why"}.issubset(row)
        assert len(decision["flip_conditions"]) >= 3
        for row in decision["flip_conditions"]:
            assert {"condition", "current_state", "flips_toward", "threshold_or_trigger"}.issubset(row)
        assert {"stronger_evidence", "unresolved_fields", "evidence_needed_to_decide"}.issubset(decision["evidence_diff"])

    def test_compare_dimensions_include_recommendation_aware_fields(self):
        contract = _compare_contract()
        dimension_ids = {row.get("dimension_id") for row in contract["compare_dimensions"]}
        assert "benchmark_identity" in dimension_ids
        assert "benchmark_fidelity" in dimension_ids
        assert "decision_state" in dimension_ids
        assert "source_integrity" in dimension_ids
        assert "market_path_provenance" in dimension_ids
        assert "score_total" in dimension_ids

    def test_compare_promotes_investor_dimensions_first(self):
        contract = _compare_contract()
        dimension_ids = [row.get("dimension_id") for row in contract["compare_dimensions"][:6]]
        assert dimension_ids[:6] == [
            "benchmark_identity",
            "exposure_type",
            "sleeve_fit",
            "benchmark_fidelity",
            "decision_state",
            "source_integrity",
        ]

    def test_compare_emits_grouped_dimension_semantics(self):
        contract = _compare_contract()
        assert contract["dimension_groups"]
        assert contract["dimension_priority_order"]
        assert isinstance(contract["compare_investor_summary"], str)

    def test_compare_candidates_stay_in_blueprint_candidate_space(self):
        contract = _compare_contract()
        for candidate in contract["candidates"]:
            assert str(candidate["candidate_id"]).startswith("candidate_")
            assert candidate["symbol"] in {"CMOD", "SGLN"}

    def test_compare_adds_market_path_enrichment_only_when_both_candidates_are_usable(self, monkeypatch):
        import app.v2.surfaces.blueprint.compare_contract_builder as compare_builder

        def _support(candidate_id: str, *, allow_refresh: bool = False):
            if "cmod" in candidate_id.lower():
                return {
                    "eligibility_state": "eligible",
                    "usefulness_label": "usable",
                    "suppression_reason": None,
                    "forecast_interval": "1day",
                    "forecast_horizon": 21,
                    "path_quality_score": 71.0,
                    "candidate_fragility_score": 34.0,
                    "threshold_map": [
                        {"threshold_id": "base_case", "delta_pct": 4.0},
                        {"threshold_id": "downside_case", "delta_pct": -2.0},
                    ],
                    "series_quality_summary": {"quality_label": "good"},
                }
            return {
                "eligibility_state": "eligible",
                "usefulness_label": "strong",
                "suppression_reason": None,
                "forecast_interval": "1day",
                "forecast_horizon": 21,
                "path_quality_score": 82.0,
                "candidate_fragility_score": 28.0,
                "threshold_map": [
                    {"threshold_id": "base_case", "delta_pct": 2.0},
                    {"threshold_id": "downside_case", "delta_pct": -4.5},
                ],
                "series_quality_summary": {"quality_label": "good"},
            }

        enrichment = compare_builder._compare_market_path_enrichment(
            [
                {
                    "candidate_id": "candidate_instrument_cmod",
                    "symbol": "CMOD",
                    "market_path_support": _support("candidate_instrument_cmod"),
                },
                {
                    "candidate_id": "candidate_instrument_sgln",
                    "symbol": "SGLN",
                    "market_path_support": _support("candidate_instrument_sgln"),
                },
            ],
            readiness_state="ready",
            verdict="direct_substitutes",
        )

        assert enrichment is not None
        assert enrichment["path_asymmetry"] is not None
        assert enrichment["downside_asymmetry"] is not None
        assert enrichment["stability_advantage"] in {
            "candidate_instrument_cmod",
            "candidate_instrument_sgln",
            "tie",
        }
        assert isinstance(enrichment["market_path_compare_note"], str)

    def test_compare_suppresses_market_path_enrichment_when_one_candidate_is_suppressed(self, monkeypatch):
        import app.v2.surfaces.blueprint.compare_contract_builder as compare_builder

        def _support(candidate_id: str):
            return {
                "eligibility_state": "eligible",
                "usefulness_label": "suppressed" if "cmod" in candidate_id.lower() else "usable",
                "suppression_reason": "output_suppressed" if "cmod" in candidate_id.lower() else None,
                "forecast_interval": "1day",
                "forecast_horizon": 21,
                "threshold_map": [
                    {"threshold_id": "base_case", "delta_pct": 2.0},
                    {"threshold_id": "downside_case", "delta_pct": -2.0},
                ],
                "series_quality_summary": {"quality_label": "good"},
            }

        enrichment = compare_builder._compare_market_path_enrichment(
            [
                {
                    "candidate_id": "candidate_instrument_cmod",
                    "symbol": "CMOD",
                    "market_path_support": _support("candidate_instrument_cmod"),
                },
                {
                    "candidate_id": "candidate_instrument_sgln",
                    "symbol": "SGLN",
                    "market_path_support": _support("candidate_instrument_sgln"),
                },
            ],
            readiness_state="ready",
            verdict="direct_substitutes",
        )

        assert enrichment is None
