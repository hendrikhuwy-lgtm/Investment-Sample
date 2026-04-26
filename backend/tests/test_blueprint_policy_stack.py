from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

from app.config import Settings
from app.services.blueprint_candidate_eligibility import evaluate_candidate_eligibility
from app.services.blueprint_candidate_universe import build_candidate_universe_diff
from app.services.blueprint_decision_semantics import (
    BENCHMARK_FIT_TYPES,
    DATA_QUALITY_STATES,
    POLICY_GATE_STATES,
    SCORING_STATES,
    build_blueprint_decision_record,
    evaluate_sleeve_expression,
)
from app.services.blueprint_deliverable_candidates import (
    build_deliverable_candidates,
    build_deliverable_candidates_diff,
)
from app.services.blueprint_investment_quality import build_investment_quality_score
from app.services.blueprint_payload_integrity import blueprint_payload_supports_candidate_detail
from app.services.blueprint_recommendation_diff import build_recommendation_diff
from app.services.blueprint_store import (
    create_blueprint_snapshot,
    get_blueprint_runtime_cycle,
    get_blueprint_snapshot,
    list_blueprint_runtime_cycles,
    persist_blueprint_runtime_cycle,
)
from app.services.portfolio_blueprint import build_portfolio_blueprint_payload
import sqlite3


def _global_core_payload() -> tuple[dict, dict, list[dict]]:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    return payload, sleeve, list(sleeve.get("candidates") or [])


def test_candidate_with_unacceptable_sleeve_weakness_is_blocked_before_recommendation() -> None:
    _, _, candidates = _global_core_payload()
    candidate = deepcopy(next(item for item in candidates if item["symbol"] == "VWRA"))
    candidate["leverage_used"] = True
    now = datetime.now(UTC)
    eligibility = evaluate_candidate_eligibility(
        candidate=candidate,
        sleeve_key="global_equity_core",
        settings=Settings(),
        now=now,
    )
    candidate["eligibility"] = eligibility
    candidate["sleeve_expression"] = evaluate_sleeve_expression(
        candidate=candidate,
        sleeve_key="global_equity_core",
        benchmark_assignment=dict(candidate.get("benchmark_assignment") or {}),
        pressures=list(eligibility.get("pressures") or []),
        readiness_level=str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "research_visible"),
    )
    quality = build_investment_quality_score(
        candidate=candidate,
        sleeve_key="global_equity_core",
        sleeve_candidates=candidates,
        eligibility=eligibility,
        performance_metrics=dict(candidate.get("performance_metrics") or {}),
        settings=Settings(),
    )
    candidate["investment_quality"] = quality
    decision = build_blueprint_decision_record(
        candidate=candidate,
        sleeve_key="global_equity_core",
        evaluation_mode="design_only",
    )
    assert decision["policy_gate_state"] == "fail"
    assert "leverage_prohibited" in decision["policy_gates"]["failed_gate_names"]
    assert decision["final_decision_state"] == "rejected_policy_failure"


def test_policy_stack_emits_specific_explanations_and_missing_inputs() -> None:
    _, _, candidates = _global_core_payload()
    candidate = deepcopy(next(item for item in candidates if item["symbol"] == "VWRA"))
    candidate["domicile"] = ""
    candidate.setdefault("investment_lens", {})["liquidity_profile"] = {
        "liquidity_status": "limited_evidence",
        "spread_status": "tight",
    }
    candidate["data_completeness"] = {
        "readiness_level": "research_visible",
        "critical_required_fields_missing_count": 2,
        "required_fields_complete_count": 0,
        "requirements": [
            {"key": "tracking_difference_1y", "status": "missing_but_fetchable"},
            {"key": "benchmark_name", "status": "missing_requires_source_expansion"},
        ],
    }
    candidate["investment_quality"] = {
        "composite_score_valid": False,
        "composite_score": None,
        "unknown_dimensions": ["performance_evidence", "liquidity"],
    }
    candidate["sleeve_expression"] = {"fit_type": "partial_fit", "compromises": ["benchmark fit remains proxy-based"]}
    candidate["benchmark_assignment"] = {}
    decision = build_blueprint_decision_record(
        candidate=candidate,
        sleeve_key="global_equity_core",
        evaluation_mode="design_only",
    )
    assert decision["policy_gate_state"] in {"fail", "partial", "not_evaluated"}
    assert decision["data_quality_state"] in {"failed", "unknown_due_to_missing_inputs"}
    assert decision["scoring_state"] == "blocked_by_missing_input"
    assert decision["explanations"]["policy_gates"]
    assert "performance_evidence" in set(decision["missing_inputs"]["scoring"])


def test_unresolved_required_gate_blocks_candidate_before_recommendation() -> None:
    _, _, candidates = _global_core_payload()
    candidate = deepcopy(next(item for item in candidates if item["symbol"] == "VWRA"))
    candidate["benchmark_assignment"] = {}
    candidate["sleeve_expression"] = {"fit_type": "qualified_fit", "compromises": []}
    candidate["data_completeness"] = {
        "readiness_level": "shortlist_ready",
        "critical_required_fields_missing_count": 0,
        "required_fields_complete_count": 10,
        "requirements": [],
    }
    candidate["investment_quality"] = {
        "composite_score_valid": True,
        "composite_score": 72.0,
        "unknown_dimensions": [],
        "recommendation_confidence": "medium",
    }
    decision = build_blueprint_decision_record(
        candidate=candidate,
        sleeve_key="global_equity_core",
        evaluation_mode="design_only",
    )
    assert decision["required_gate_resolution_state"] == "unresolved"
    assert decision["final_decision_state"] == "blocked_by_unresolved_gate"
    assert decision["rejection_reason"]["root_cause_class"] == "unresolved_gate"


def test_excessive_unknown_weighted_share_suppresses_composite_score() -> None:
    _, _, candidates = _global_core_payload()
    candidate = deepcopy(next(item for item in candidates if item["symbol"] == "VWRA"))
    candidate["expense_ratio"] = None
    candidate["sg_lens"] = {}
    candidate.setdefault("investment_lens", {})["liquidity_profile"] = {"liquidity_status": "unknown", "spread_status": "unknown"}
    settings = Settings(
        blueprint_max_unknown_dimensions=1,
        blueprint_max_unknown_weight_share=0.20,
        blueprint_require_liquidity_dimension=True,
    )
    quality = build_investment_quality_score(
        candidate=candidate,
        sleeve_key="global_equity_core",
        sleeve_candidates=candidates,
        eligibility=dict(candidate.get("eligibility") or {}),
        performance_metrics=dict(candidate.get("performance_metrics") or {}),
        settings=settings,
    )
    assert quality["composite_score"] is None
    assert quality["composite_score_valid"] is False
    assert "liquidity" in set(quality["unknown_dimensions"])


def test_rejection_memo_exists_for_every_non_primary_candidate() -> None:
    payload, sleeve, candidates = _global_core_payload()
    _ = payload
    non_primary = [
        item
        for item in candidates
        if str(dict(item.get("investment_quality") or {}).get("recommendation_state") or "") != "recommended_primary"
    ]
    assert non_primary
    for candidate in non_primary:
        memo = dict(candidate.get("rejection_memo") or {})
        assert memo["candidate_id"] == candidate["symbol"]
        assert memo["rejection_type"]


def test_blueprint_runs_in_design_only_mode_without_holdings_dependency() -> None:
    payload, _, candidates = _global_core_payload()
    assert payload["blueprint_meta"]["recommendation_summary"]
    assert all(
        str(dict(candidate.get("decision_record") or {}).get("evaluation_mode") or "") in {"design_only", "market_context_refreshed"}
        for candidate in candidates
    )


def test_payload_candidates_include_decision_record_and_enriched_benchmark_fields() -> None:
    payload, _, candidates = _global_core_payload()
    _ = payload
    candidate = next(item for item in candidates if item["symbol"] == "VWRL")
    decision_record = dict(candidate.get("decision_record") or {})
    benchmark_assignment = dict(candidate.get("benchmark_assignment") or {})
    assert decision_record.get("mandate_fit_state") in {"pass", "fail", "not_evaluated"}
    assert decision_record.get("policy_gate_state") in POLICY_GATE_STATES
    assert decision_record.get("data_quality_state") in DATA_QUALITY_STATES
    assert decision_record.get("scoring_state") in SCORING_STATES
    assert decision_record.get("explanations")
    assert benchmark_assignment.get("benchmark_kind")
    assert benchmark_assignment.get("benchmark_role")
    assert benchmark_assignment.get("benchmark_fit_type") in BENCHMARK_FIT_TYPES
    assert benchmark_assignment.get("benchmark_explanation")
    assert isinstance(benchmark_assignment.get("evidence_basis"), list)
    assert candidate.get("field_truth_surface")
    assert candidate.get("score_honesty")
    assert candidate.get("benchmark_dependency_diagnostics")
    assert candidate.get("upgrade_path")


def test_deliverable_candidates_can_expand_and_contract_with_explicit_reasons() -> None:
    previous_payload = {
        "sleeves": [
            {
                "sleeve_key": "global_equity_core",
                "candidates": [
                    {"symbol": "VWRA", "name": "VWRA", "investment_quality": {"recommendation_state": "recommended_primary", "recommendation_confidence": "high"}},
                    {"symbol": "IWDA", "name": "IWDA", "investment_quality": {"recommendation_state": "research_only", "recommendation_confidence": "medium"}},
                ],
            }
        ]
    }
    current_payload = {
        "sleeves": [
            {
                "sleeve_key": "global_equity_core",
                "candidates": [
                    {"symbol": "VWRA", "name": "VWRA", "investment_quality": {"recommendation_state": "recommended_backup", "recommendation_confidence": "high"}},
                    {"symbol": "SSAC", "name": "SSAC", "investment_quality": {"recommendation_state": "watchlist_only", "recommendation_confidence": "medium"}},
                ],
            }
        ]
    }
    deliverable = build_deliverable_candidates(current_payload=current_payload, previous_payload=previous_payload)
    diff = build_deliverable_candidates_diff(current_payload=current_payload, previous_payload=previous_payload)
    assert deliverable["summary"]["current_deliverable_count"] == 2
    assert diff["summary"]["entered_count"] == 1
    assert diff["summary"]["exited_count"] == 1
    assert diff["entered"][0]["change_reasons"]
    assert diff["exited"][0]["change_reasons"]


def test_recommendation_diff_explains_why_winner_changed() -> None:
    diff = build_recommendation_diff(
        {
            "recommendation_state": "recommended_primary",
            "benchmark_fit_type": "acceptable_proxy",
            "readiness_level": "shortlist_ready",
            "composite_score": 65.0,
            "recommendation_confidence": "medium",
            "candidate_universe_changed": False,
            "candidate_universe_reason": "",
            "rejection_reasons": ["benchmark support is weak"],
        },
        {
            "recommendation_state": "recommended_backup",
            "benchmark_fit_type": "strong_fit",
            "readiness_level": "recommendation_ready",
            "composite_score": 74.0,
            "recommendation_confidence": "high",
            "candidate_universe_changed": True,
            "candidate_universe_reason": "candidate universe changed around the sleeve",
            "rejection_reasons": [],
        },
    )
    assert diff["what_changed"]
    assert diff["change_driver_type"] in {"benchmark_change", "data_or_policy_change", "recommendation_reclassification", "score_change"}
    assert "benchmark_fit_type" in set(diff["material_dimensions_changed"])
    assert diff["dominant_reason_for_change"]
    assert diff["candidate_universe_change_effect"]


def test_primary_and_watchlist_states_are_not_conflated() -> None:
    payload, _, candidates = _global_core_payload()
    states = {str(dict(candidate.get("investment_quality") or {}).get("recommendation_state") or "") for candidate in candidates}
    assert "recommended_primary" in states
    assert states.isdisjoint({"primary_pick", "secondary_pick", "watchlist"})


def test_payload_exposes_portfolio_governance_summary() -> None:
    payload, _, _ = _global_core_payload()
    governance = dict(payload["blueprint_meta"].get("portfolio_governance") or {})
    assert governance["candidate_state_counts"]
    assert "unknown_share_distribution" in governance
    assert "sleeve_coverage_gap_analysis" in governance
    assert "highest_leverage_fix_candidates" in governance


def test_historical_like_output_preserves_approval_and_rejection_rationale() -> None:
    _, _, candidates = _global_core_payload()
    primary = next(item for item in candidates if str(dict(item.get("investment_quality") or {}).get("recommendation_state") or "") == "recommended_primary")
    non_primary = next(item for item in candidates if str(dict(item.get("investment_quality") or {}).get("recommendation_state") or "") != "recommended_primary")
    assert list(dict(primary.get("approval_memo") or {}).get("approval_reasons") or [])
    assert dict(non_primary.get("rejection_memo") or {}).get("rejection_reasons") is not None


def test_research_only_and_removed_from_deliverable_set_are_distinct() -> None:
    payload = {
        "sleeves": [
            {
                "sleeve_key": "alternatives",
                "candidates": [
                    {"symbol": "AAA", "name": "AAA", "investment_quality": {"recommendation_state": "research_only", "recommendation_confidence": "low"}},
                    {"symbol": "BBB", "name": "BBB", "investment_quality": {"recommendation_state": "removed_from_deliverable_set", "recommendation_confidence": "low"}},
                ],
            }
        ]
    }
    deliverable = build_deliverable_candidates(current_payload=payload, previous_payload=None)
    symbols = {
        item["candidate_symbol"]: item["change_state"]
        for item in list(deliverable.get("by_sleeve", {}).get("alternatives", []))
    }
    assert symbols["AAA"] in {"added", "under_review", "retained"}
    assert "BBB" not in symbols


def test_decision_artifacts_are_persisted_with_snapshot() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        payload = build_portfolio_blueprint_payload()
        created = create_blueprint_snapshot(conn, blueprint_payload=payload, note="artifact coverage")
        loaded = get_blueprint_snapshot(conn, created["snapshot_id"])
        assert loaded is not None
        artifact_types = {str(item.get("artifact_type") or "") for item in list(loaded.get("decision_artifacts") or [])}
        assert "deliverable_candidates" in artifact_types
        assert "deliverable_candidates_diff" in artifact_types
        assert "decision_record" in artifact_types
        assert "rejection_memo" in artifact_types
        assert "candidate_universe_diff" in artifact_types
    finally:
        conn.close()


def test_runtime_cycle_artifacts_are_persisted_outside_snapshot_save() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        payload = build_portfolio_blueprint_payload()
        persisted = persist_blueprint_runtime_cycle(conn, blueprint_payload=payload)
        cycles = list_blueprint_runtime_cycles(conn)
        assert cycles
        loaded = get_blueprint_runtime_cycle(conn, str(persisted["cycle_id"]))
        assert loaded is not None
        artifact_types = {str(item.get("artifact_type") or "") for item in list(loaded.get("artifacts") or [])}
        assert "decision_record" in artifact_types
        assert "deliverable_candidates_diff" in artifact_types
        assert "candidate_universe_diff" in artifact_types
    finally:
        conn.close()


def test_candidate_universe_diff_tracks_review_and_exit_states() -> None:
    previous_payload = {
        "sleeves": [
            {
                "sleeve_key": "global_equity_core",
                "candidates": [
                    {"symbol": "VWRA", "name": "VWRA", "investment_quality": {"recommendation_state": "recommended_primary", "recommendation_confidence": "high"}},
                    {"symbol": "IWDA", "name": "IWDA", "investment_quality": {"recommendation_state": "watchlist_only", "recommendation_confidence": "medium"}},
                ],
            }
        ]
    }
    current_payload = {
        "sleeves": [
            {
                "sleeve_key": "global_equity_core",
                "candidates": [
                    {"symbol": "VWRA", "name": "VWRA", "investment_quality": {"recommendation_state": "recommended_primary", "recommendation_confidence": "high"}},
                    {"symbol": "SSAC", "name": "SSAC", "investment_quality": {"recommendation_state": "research_only", "recommendation_confidence": "medium"}},
                ],
            }
        ]
    }
    diff = build_candidate_universe_diff(current_payload=current_payload, previous_payload=previous_payload)
    assert diff["summary"]["entered_count"] == 1
    assert diff["summary"]["exited_count"] == 1
    assert diff["summary"]["under_review_count"] == 1


def test_payload_integrity_rejects_cached_payloads_missing_enriched_fields() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = next(item for sleeve in payload["sleeves"] for item in sleeve["candidates"] if item["symbol"] == "VWRL")
    payload["blueprint_meta"]["payload_integrity_version"] = 0
    candidate["decision_record"] = {}
    supports_detail, reason = blueprint_payload_supports_candidate_detail(payload)
    assert supports_detail is False
    assert reason == "payload_integrity_version_mismatch"


def test_payload_integrity_rejects_missing_decision_explanations_for_partial_states() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = next(item for sleeve in payload["sleeves"] for item in sleeve["candidates"] if item["symbol"] == "VWRL")
    candidate["decision_record"]["sleeve_fit_state"] = "partial"
    candidate["decision_record"]["explanations"] = {}
    supports_detail, reason = blueprint_payload_supports_candidate_detail(payload)
    assert supports_detail is False
    assert reason == "missing_decision_explanations"


def test_thin_evidence_candidate_degrades_specifically_not_vaguely() -> None:
    _, _, candidates = _global_core_payload()
    candidate = deepcopy(next(item for item in candidates if item["symbol"] == "VWRA"))
    candidate["domicile"] = ""
    candidate["benchmark_assignment"] = {
        "benchmark_kind": "proxy",
        "benchmark_fit_type": "acceptable_proxy",
        "benchmark_authority_level": "limited",
        "benchmark_role": "supporting_anchor",
        "benchmark_effect_label": "Benchmark fit acceptable but proxy-based",
    }
    candidate.setdefault("investment_lens", {})["liquidity_profile"] = {
        "liquidity_status": "limited_evidence",
        "spread_status": "unknown",
        "explanation": "Liquidity could only be assessed with limited evidence because qualitative proxy fields are present while direct score evidence is missing.",
        "evidence_basis": ["candidate.liquidity_proxy"],
        "missing_inputs": ["liquidity_score", "bid_ask_spread_proxy"],
    }
    candidate["data_completeness"] = {
        "readiness_level": "research_visible",
        "critical_required_fields_missing_count": 1,
        "required_fields_complete_count": 0,
        "requirements": [{"key": "tracking_difference_1y", "status": "missing_but_fetchable"}],
    }
    candidate["investment_quality"] = {
        "composite_score_valid": False,
        "unknown_dimensions": ["performance_evidence"],
        "composite_score": None,
    }
    candidate["sleeve_expression"] = {"fit_type": "partial_fit", "compromises": ["benchmark support is proxy-based"]}
    decision = build_blueprint_decision_record(candidate=candidate, sleeve_key="global_equity_core", evaluation_mode="design_only")
    assert decision["policy_gate_state"] in {"partial", "not_evaluated"}
    assert decision["data_quality_state"] == "unknown_due_to_missing_inputs"
    assert decision["scoring_state"] == "blocked_by_missing_input"
    assert decision["explanations"]["scoring"]
    assert decision["missing_inputs"]["scoring"]


def test_cache_integrity_accepts_current_payload_contract() -> None:
    payload = build_portfolio_blueprint_payload()
    supports_detail, reason = blueprint_payload_supports_candidate_detail(payload)
    assert supports_detail is True
    assert reason is None
