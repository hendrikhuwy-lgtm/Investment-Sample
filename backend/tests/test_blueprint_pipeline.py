import sqlite3

from app.services.blueprint_pipeline import build_candidate_pipeline
from app.services.blueprint_store import list_blueprint_runtime_cycle_artifacts, persist_blueprint_runtime_cycle
from app.services.portfolio_blueprint import build_portfolio_blueprint_payload


def _find_candidate(payload: dict, symbol: str) -> dict:
    target = symbol.upper()
    for sleeve in payload.get("sleeves", []):
        for candidate in sleeve.get("candidates", []):
            if str(candidate.get("symbol") or "").upper() == target:
                return candidate
    raise AssertionError(f"Candidate {symbol} not found")


def _find_candidate_in_sleeve(payload: dict, *, sleeve_key: str, symbol: str) -> dict:
    target = symbol.upper()
    for sleeve in payload.get("sleeves", []):
        if str(sleeve.get("sleeve_key") or "") != sleeve_key:
            continue
        for candidate in sleeve.get("candidates", []):
            if str(candidate.get("symbol") or "").upper() == target:
                return candidate
    raise AssertionError(f"Candidate {symbol} not found in sleeve {sleeve_key}")


def test_blueprint_meta_exposes_canonical_architecture_summary() -> None:
    payload = build_portfolio_blueprint_payload()

    architecture = dict(payload.get("blueprint_meta", {}).get("architecture") or {})
    layers = list(architecture.get("target_layers") or [])
    module_classification = list(architecture.get("module_classification") or [])
    pipeline_summary = dict(architecture.get("pipeline_summary") or {})

    assert len(layers) == 7
    assert "recommendation_engine" in layers
    assert any(
        str(item.get("module_path") or "") == "app/services/portfolio_blueprint.py"
        and str(item.get("classification") or "") == "ORCHESTRATION_ONLY"
        for item in module_classification
    )
    assert any(
        str(item.get("module_path") or "") == "app/services/blueprint_canonical_decision.py"
        and str(item.get("classification") or "") == "CANONICAL_KEEPER"
        for item in module_classification
    )
    assert "candidate_status_counts" in pipeline_summary
    assert "review_intensity_counts" in pipeline_summary


def test_candidates_include_canonical_pipeline_objects() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")

    assert isinstance(candidate.get("candidate_record"), dict)
    assert isinstance(candidate.get("evidence_pack"), dict)
    assert isinstance(candidate.get("source_integrity_result"), dict)
    assert isinstance(candidate.get("gate_result"), dict)
    assert isinstance(candidate.get("review_intensity_decision"), dict)
    assert isinstance(candidate.get("universal_review_result"), dict)
    assert isinstance(candidate.get("scoring_result"), dict)
    assert isinstance(candidate.get("recommendation_result"), dict)
    assert isinstance(candidate.get("decision_completeness_status"), dict)
    assert isinstance(candidate.get("portfolio_completeness_status"), dict)
    assert isinstance(candidate.get("investor_recommendation_status"), dict)
    assert isinstance(candidate.get("benchmark_support_status"), dict)
    assert isinstance(candidate.get("gate_summary"), dict)
    assert isinstance(candidate.get("base_promotion_state"), str)
    assert isinstance(candidate.get("lens_assessment"), dict)
    assert isinstance(candidate.get("lens_fusion_result"), dict)
    assert isinstance(candidate.get("decision_thesis"), dict)
    assert isinstance(candidate.get("forecast_visual_model"), dict)
    assert isinstance(candidate.get("forecast_defensibility_status"), dict)
    assert isinstance(candidate.get("tax_assumption_status"), dict)
    assert isinstance(candidate.get("cost_realism_summary"), dict)
    assert isinstance(candidate.get("portfolio_consequence_summary"), dict)
    assert isinstance(candidate.get("decision_change_set"), dict)
    assert isinstance(candidate.get("supporting_metadata_summary"), dict)
    assert isinstance(candidate.get("memo_result"), dict)
    assert isinstance(candidate.get("audit_log_entries"), list)
    assert isinstance(candidate.get("baseline_reference"), dict)
    assert isinstance(candidate.get("bucket_support"), dict)
    assert str(candidate.get("evidence_depth_class") or "")
    assert isinstance(dict(candidate.get("evidence_pack") or {}).get("bucket_support"), dict)


def test_global_equity_candidate_has_baseline_reference() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")

    baseline = dict(candidate.get("baseline_reference") or {})

    assert baseline.get("baseline_symbol")
    assert "baseline" in str(baseline.get("baseline_reason") or "").lower()


def test_current_holding_comparison_and_practical_edge_shape_recommendation_result() -> None:
    current_candidate = {
        "symbol": "CSPX",
        "name": "Current Holding",
        "instrument_type": "etf_ucits",
        "issuer": "Issuer",
        "source_state": "source_validated",
        "field_truth_surface": {"fields": [{"field_name": "expense_ratio", "label": "Expense ratio", "value_state": "resolved", "decision_critical": True}]},
        "field_truth": {
            "expense_ratio": {"resolved_value": 0.07, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "bid_ask_spread_proxy": {"resolved_value": 8.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
            "aum": {"resolved_value": 1800000000.0, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "volume_30d_avg": {"resolved_value": 250000.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
            "share_class": {"resolved_value": "accumulating", "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "holdings_count": {"resolved_value": 500, "completeness_state": "complete", "source_name": "etf_holdings", "evidence_class": "verified_official"},
            "us_weight": {"resolved_value": 65.0, "completeness_state": "complete", "source_name": "etf_holdings", "evidence_class": "verified_official"},
        },
        "evidence_buckets": {
            "identity_wrapper": {"state": "complete"},
            "holdings_exposure": {"state": "complete"},
            "expense_and_cost": {"state": "complete"},
            "liquidity_and_aum": {"state": "complete"},
            "benchmark_support": {"state": "complete"},
            "performance_relative_support": {"state": "complete"},
            "tax_posture": {"state": "complete", "confidence": "high"},
        },
        "benchmark_assignment": {"benchmark_key": "acwi", "benchmark_label": "MSCI ACWI", "benchmark_fit_type": "strong_fit", "benchmark_authority_level": "direct", "benchmark_kind": "direct"},
        "investment_lens": {"liquidity_profile": {"liquidity_status": "strong", "spread_status": "supported", "explanation": "Liquidity is usable."}},
        "sg_lens": {"score": 75},
        "tax_truth": {"tax_score": 78.0, "tax_confidence": "high", "policy_buckets": {}, "advisory_boundary": "good_sg_retail_default", "evidence_strength": "supported"},
        "performance_metrics": {},
        "eligibility": {"pressures": []},
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Current holding remains acceptable."},
        "decision_readiness": {"what_must_change": []},
        "recommendation_context": {"lead_summary": "Current holding is behind the preferred candidate."},
        "score_honesty": {"comparability": "fully_comparable", "evidence_coverage_band": "high", "unknown_share_band": "low", "unknown_dimensions": []},
        "investment_quality": {
            "user_facing_state": "best_available_with_limits",
            "recommendation_state": "watchlist_only",
            "composite_score": 79.0,
            "composite_score_valid": True,
            "cost_score": 70.0,
            "liquidity_score": 72.0,
            "structure_score": 75.0,
            "tax_score": 73.0,
            "performance_score": 69.0,
            "risk_adjusted_score": 68.0,
            "governance_confidence_score": 71.0,
            "sg_rank": 70.0,
            "structured_summary": "Current holding remains acceptable.",
            "investment_thesis": "Current holding delivers acceptable exposure.",
        },
    }
    preferred_candidate = {
        **current_candidate,
        "symbol": "VWRA",
        "name": "Preferred Candidate",
        "field_truth": {
            "expense_ratio": {"resolved_value": 0.22},
            "bid_ask_spread_proxy": {"resolved_value": 9.0},
        },
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Preferred candidate leads on net quality."},
        "recommendation_context": {"lead_summary": "Preferred candidate leads on net quality after policy checks."},
        "investment_quality": {
            **dict(current_candidate["investment_quality"]),
            "user_facing_state": "fully_clean_recommendable",
            "recommendation_state": "recommended_primary",
            "composite_score": 95.0,
            "investment_thesis": "Preferred candidate improves sleeve implementation.",
            "structured_summary": "Preferred candidate improves sleeve implementation.",
        },
    }

    pipeline = build_candidate_pipeline(
        candidate=preferred_candidate,
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        sleeve_candidates=[preferred_candidate, current_candidate],
        winner_candidate=preferred_candidate,
        current_holdings=[
            {
                "symbol": "CSPX",
                "name": "Current Holding",
                "quantity": 10.0,
                "cost_basis": 100.0,
                "currency": "USD",
                "sleeve": "global_equity",
                "account_type": "broker",
            }
        ],
    )

    current_holding = dict(pipeline.get("current_holding_record") or {})
    recommendation = dict(pipeline.get("recommendation_result") or {})

    assert str(current_holding.get("status") or "") == "different_from_current"
    assert str(current_holding.get("current_symbol") or "") == "CSPX"
    assert str(dict(current_holding.get("practical_edge") or {}).get("status") or "") == "sufficient"
    assert str(recommendation.get("decision_type") or "") == "REPLACE"
    assert bool(recommendation.get("no_change_is_best")) is False
    assert str(recommendation.get("recommendation_tier") or "") == "actionable"


def test_alternative_or_convex_candidates_trigger_deep_review() -> None:
    payload = build_portfolio_blueprint_payload()

    candidate = None
    for symbol in ("DBMF", "CAOS"):
        try:
            candidate = _find_candidate(payload, symbol)
            break
        except AssertionError:
            continue

    assert candidate is not None, "Expected an alternatives or convex candidate in Blueprint payload"
    review_intensity = dict(candidate.get("review_intensity_decision") or {})
    deep_review = candidate.get("deep_review_result")

    assert str(review_intensity.get("review_intensity") or "") == "level_2_deep"
    assert isinstance(deep_review, dict)
    assert deep_review.get("status") == "completed"


def test_audit_trace_covers_pipeline_steps() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")

    steps = [str(item.get("step") or "") for item in list(candidate.get("audit_log_entries") or [])]

    assert "evidence_pack_build" in steps
    assert "source_integrity_checks" in steps
    assert "gate_outcomes" in steps
    assert "review_intensity_decision" in steps
    assert "universal_review_completion" in steps
    assert "scoring_completion" in steps
    assert "recommendation_decision" in steps
    assert "memo_generation" in steps


def test_recommendation_result_exposes_actionability_and_winner_context() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")

    recommendation = dict(candidate.get("recommendation_result") or {})

    assert str(recommendation.get("candidate_status") or "")
    assert str(recommendation.get("decision_type") or "") in {"ADD", "REPLACE", "HOLD", "TRIM", "REJECT", "RESEARCH"}
    assert str(recommendation.get("recommendation_tier") or "") in {"actionable", "non_actionable"}
    assert "candidate" in str(recommendation.get("why_this_candidate") or "").lower()


def test_summary_backed_candidate_exposes_bucket_support_with_claim_limits() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = None
    for symbol in ("HMCH", "XCHA", "VEVE", "VAGU"):
        try:
            candidate = _find_candidate(payload, symbol)
            break
        except AssertionError:
            continue

    assert candidate is not None, "Expected a summary-backed candidate in Blueprint payload"
    assert str(candidate.get("evidence_depth_class") or "") in {"structured_summary_strong", "summary_backed_limited"}
    holdings_support = dict(dict(candidate.get("bucket_support") or {}).get("holdings_exposure") or {})
    interpretation = dict(holdings_support.get("interpretation_summary") or {})

    assert str(holdings_support.get("bucket_state") or "") in {"partial", "proxy_only", "complete", "missing"}
    assert isinstance(list(holdings_support.get("claim_limits") or []), list)
    assert str(interpretation.get("supports") or "")
    assert str(interpretation.get("does_not_support") or "")


def test_structure_first_candidate_uses_structure_first_evidence_depth() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = None
    for symbol in ("DBMF", "CAOS"):
        try:
            candidate = _find_candidate(payload, symbol)
            break
        except AssertionError:
            continue

    assert candidate is not None, "Expected a structure-first candidate in Blueprint payload"
    assert str(candidate.get("evidence_depth_class") or "") == "structure_first"
    holdings_support = dict(dict(candidate.get("bucket_support") or {}).get("holdings_exposure") or {})
    claim_limits = " ".join(list(holdings_support.get("claim_limits") or []))
    assert "holdings-style" in claim_limits.lower() or "structure" in claim_limits.lower()


def test_backend_authority_objects_separate_reliability_completeness_and_status() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")

    source_integrity = dict(candidate.get("source_integrity_result") or {})
    decision_completeness = dict(candidate.get("decision_completeness_status") or {})
    portfolio_completeness = dict(candidate.get("portfolio_completeness_status") or {})
    investor_status = dict(candidate.get("investor_recommendation_status") or {})

    assert source_integrity.get("overall_source_status") is not None
    assert decision_completeness.get("data_completeness_grade") in {"INCOMPLETE", "PARTIAL", "SUFFICIENT"}
    assert isinstance(decision_completeness.get("missing_but_fetchable") or [], list)
    assert isinstance(decision_completeness.get("missing_requires_source_expansion") or [], list)
    assert portfolio_completeness.get("completeness_grade") in {"INCOMPLETE", "PARTIAL", "SUFFICIENT"}
    assert investor_status.get("investor_status") in {
        "NOT_DECISION_READY",
        "RESEARCH_CANDIDATE",
        "WATCHLIST_CANDIDATE",
        "DECISION_READY",
        "ACTIONABLE_RECOMMENDATION",
        "DO_NOT_USE",
    }
    assert isinstance(investor_status.get("blocked_reasons") or [], list)
    assert isinstance(investor_status.get("evidence_blockers") or [], list)
    assert isinstance(investor_status.get("portfolio_blockers") or [], list)


def test_missing_current_holding_keeps_portfolio_completeness_incomplete() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")
    pipeline = build_candidate_pipeline(
        candidate=candidate,
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        sleeve_candidates=[candidate],
        winner_candidate=candidate,
        current_holdings=[],
    )

    completeness = dict(pipeline.get("portfolio_completeness_status") or {})
    investor_status = dict(pipeline.get("investor_recommendation_status") or {})

    assert completeness.get("current_holding_known") is False
    assert completeness.get("portfolio_state_status") == "portfolio_state_missing"
    assert completeness.get("completeness_grade") == "INCOMPLETE"
    assert investor_status.get("investor_status") == "RESEARCH_CANDIDATE"
    assert "Current holding is not recorded" in " ".join(investor_status.get("portfolio_blockers") or [])


def test_field_truth_aum_support_counts_toward_decision_completeness() -> None:
    candidate = {
        "symbol": "TEST",
        "field_truth": {
            "aum": {"missingness_reason": "populated", "resolved_value": 1_000_000_000.0},
            "volume_30d_avg": {"missingness_reason": "populated", "resolved_value": 150000.0},
        },
        "performance_metrics": {},
        "benchmark_assignment": {"benchmark_fit_type": "strong_fit"},
        "accumulation_or_distribution": "accumulating",
        "recommendation_context": {"challenger": {"symbol": "ALT"}},
    }
    candidate_full = {
        **candidate,
        "name": "Test Candidate",
        "issuer": "Issuer",
        "instrument_type": "etf_ucits",
        "source_state": "source_validated",
        "field_truth_surface": {"fields": []},
        "investment_lens": {"liquidity_profile": {"liquidity_status": "supported"}},
        "sg_lens": {"score": 80},
        "eligibility": {"pressures": []},
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Eligible."},
        "decision_readiness": {"what_must_change": []},
        "score_honesty": {"comparability": "fully_comparable", "evidence_coverage_band": "high", "unknown_share_band": "low", "unknown_dimensions": []},
        "investment_quality": {
            "user_facing_state": "research_ready_but_not_recommendable",
            "recommendation_state": "recommended_primary",
            "composite_score": 90.0,
            "composite_score_valid": True,
            "structured_summary": "Candidate is usable for comparison.",
            "investment_thesis": "Candidate is usable for comparison.",
        },
    }

    completeness = build_candidate_pipeline(
        candidate=candidate_full,
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        sleeve_candidates=[candidate_full],
        winner_candidate=candidate_full,
        current_holdings=[],
    ).get("decision_completeness_status") or {}

    assert "AUM support is still missing." not in list(completeness.get("material_gaps") or [])
    assert completeness.get("watchlist_eligible") is True


def test_forecast_and_tax_authority_objects_govern_display_and_tax_decisiveness() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")

    forecast = dict(candidate.get("forecast_defensibility_status") or {})
    tax = dict(candidate.get("tax_assumption_status") or {})

    assert forecast.get("display_grade") in {"HIDE", "ADVANCED_ONLY", "SOFT_SCENARIO_ONLY", "FULLY_DISPLAYABLE"}
    assert tax.get("assumption_completeness_grade") in {"INCOMPLETE", "PARTIAL", "SUFFICIENT"}
    assert isinstance(tax.get("decisive_tax_use_allowed"), bool)
    assert tax.get("tax_confidence") in {"low", "medium", "high"}
    assert isinstance(tax.get("policy_buckets"), dict)


def test_data_completeness_does_not_report_stale_aum_gap_when_authoritative_truth_is_populated() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRL")

    completeness = dict(candidate.get("data_completeness") or {})

    assert "aum" not in list(completeness.get("critical_required_fields_missing") or [])
    missing_requirements = {
        str(item.get("key") or ""): str(item.get("status") or "")
        for item in list(completeness.get("requirements") or [])
    }
    assert missing_requirements.get("aum") == "populated"


def test_supplemental_real_asset_gaps_do_not_dominate_material_gap_bucket() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate_in_sleeve(payload, sleeve_key="real_assets", symbol="SGLN")

    completeness = dict(candidate.get("decision_completeness_status") or {})
    material = list(completeness.get("material_gaps") or [])
    supplemental = list(completeness.get("supplemental_gaps") or [])

    assert "US weight can likely be filled from current sources, but it is not populated yet." not in material
    assert "Tracking difference 1Y is still blocked by a parser gap." not in material
    assert "AUM support is still missing." in material
    assert any("US weight" in item for item in supplemental)
    assert any("Tracking difference 1Y" in item for item in supplemental)


def test_convex_role_requirement_uses_candidate_role_fallback() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate_in_sleeve(payload, sleeve_key="convex", symbol="DBMF")

    completeness = dict(candidate.get("data_completeness") or {})

    assert "role_in_portfolio" not in list(completeness.get("critical_required_fields_missing") or [])


def test_runtime_cycle_persists_canonical_pipeline_artifacts() -> None:
    payload = build_portfolio_blueprint_payload()
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    cycle = persist_blueprint_runtime_cycle(conn, blueprint_payload=payload)
    artifacts = list_blueprint_runtime_cycle_artifacts(conn, cycle_id=str(cycle.get("cycle_id") or ""))
    artifact_types = {str(item.get("artifact_type") or "") for item in artifacts}

    assert "architecture" in artifact_types
    assert "portfolio_governance" in artifact_types
    assert "candidate_record" in artifact_types
    assert "evidence_pack" in artifact_types
    assert "source_integrity_result" in artifact_types
    assert "gate_result" in artifact_types
    assert "review_intensity_decision" in artifact_types
    assert "scoring_result" in artifact_types
    assert "current_holding_record" in artifact_types
    assert "recommendation_result" in artifact_types
    assert "decision_completeness_status" in artifact_types
    assert "portfolio_completeness_status" in artifact_types
    assert "investor_recommendation_status" in artifact_types
    assert "benchmark_support_status" in artifact_types
    assert "gate_summary" in artifact_types
    assert "base_promotion_state" in artifact_types
    assert "lens_assessment" in artifact_types
    assert "lens_fusion_result" in artifact_types
    assert "decision_thesis" in artifact_types
    assert "forecast_visual_model" in artifact_types
    assert "forecast_defensibility_status" in artifact_types
    assert "tax_assumption_status" in artifact_types
    assert "cost_realism_summary" in artifact_types
    assert "portfolio_consequence_summary" in artifact_types
    assert "decision_change_set" in artifact_types
    assert "supporting_metadata_summary" in artifact_types
    assert "memo_result" in artifact_types
    assert "audit_log_entries" in artifact_types


def test_partial_holdings_truth_softens_decision_semantics() -> None:
    candidate = {
        "symbol": "TESTH",
        "name": "Test Holdings",
        "issuer": "Issuer",
        "instrument_type": "etf_ucits",
        "source_state": "source_validated",
        "field_truth_surface": {"fields": []},
        "field_truth": {
            "expense_ratio": {"resolved_value": 0.15, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "aum": {"resolved_value": 500000000.0, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "volume_30d_avg": {"resolved_value": 100000.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
            "us_weight": {"resolved_value": 60.0, "completeness_state": "weak_or_partial", "source_name": "supplemental_candidate_metrics", "evidence_class": "manual_reviewed_override"},
            "holdings_count": {"resolved_value": 1000, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
        },
        "evidence_buckets": {
            "identity_wrapper": {"state": "complete"},
            "holdings_exposure": {"state": "partial"},
            "expense_and_cost": {"state": "complete"},
            "liquidity_and_aum": {"state": "complete"},
            "benchmark_support": {"state": "complete"},
            "performance_relative_support": {"state": "complete"},
            "tax_posture": {"state": "complete", "confidence": "high"},
        },
        "benchmark_assignment": {"benchmark_fit_type": "strong_fit", "benchmark_kind": "direct"},
        "investment_lens": {"liquidity_profile": {"liquidity_status": "strong", "spread_status": "supported"}},
        "sg_lens": {"score": 82},
        "tax_truth": {"tax_score": 82.0, "tax_confidence": "high", "policy_buckets": {}, "advisory_boundary": "good_sg_retail_default", "evidence_strength": "supported"},
        "performance_metrics": {"tracking_difference_1y": -0.1},
        "eligibility": {"pressures": []},
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Eligible."},
        "decision_readiness": {"what_must_change": []},
        "recommendation_context": {"challenger": {"symbol": "ALT"}},
        "score_honesty": {"comparability": "fully_comparable", "evidence_coverage_band": "high", "unknown_share_band": "low", "unknown_dimensions": []},
        "investment_quality": {
            "user_facing_state": "best_available_with_limits",
            "recommendation_state": "recommended_primary",
            "composite_score": 91.0,
            "composite_score_valid": True,
            "structured_summary": "Candidate remains usable with limits.",
            "investment_thesis": "Candidate remains usable with limits.",
        },
    }

    pipeline = build_candidate_pipeline(
        candidate=candidate,
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        sleeve_candidates=[candidate],
        winner_candidate=candidate,
        current_holdings=[],
    )

    completeness = dict(pipeline.get("decision_completeness_status") or {})
    investor = dict(pipeline.get("investor_recommendation_status") or {})
    assert completeness.get("claim_strength_boundary") == "structural_only"
    assert completeness.get("data_completeness_grade") == "PARTIAL"
    assert investor.get("investor_status") == "WATCHLIST_CANDIDATE"


def test_factsheet_summary_holdings_do_not_count_as_direct_holdings() -> None:
    candidate = {
        "symbol": "TESTHS",
        "name": "Test Holdings Summary",
        "issuer": "Issuer",
        "instrument_type": "etf_ucits",
        "source_state": "source_validated",
        "field_truth_surface": {"fields": []},
        "field_truth": {
            "expense_ratio": {"resolved_value": 0.15, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "aum": {"resolved_value": 500000000.0, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "volume_30d_avg": {"resolved_value": 100000.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
            "us_weight": {"resolved_value": 60.0, "completeness_state": "complete", "source_name": "etf_holdings_summary", "source_type": "issuer_factsheet_secondary", "evidence_class": "verified_official", "override_annotation": {"bucket_source_class": "factsheet_summary", "bucket_parse_confidence": "medium"}},
            "em_weight": {"resolved_value": 11.0, "completeness_state": "complete", "source_name": "etf_holdings_summary", "source_type": "issuer_factsheet_secondary", "evidence_class": "verified_official", "override_annotation": {"bucket_source_class": "factsheet_summary", "bucket_parse_confidence": "medium"}},
            "top_10_concentration": {"resolved_value": 18.0, "completeness_state": "complete", "source_name": "etf_holdings_summary", "source_type": "issuer_factsheet_secondary", "evidence_class": "verified_official", "override_annotation": {"bucket_source_class": "factsheet_summary", "bucket_parse_confidence": "medium"}},
            "holdings_count": {"resolved_value": 1500, "completeness_state": "complete", "source_name": "etf_holdings_summary", "source_type": "issuer_factsheet_secondary", "evidence_class": "verified_official", "override_annotation": {"bucket_source_class": "factsheet_summary", "bucket_parse_confidence": "medium"}},
        },
        "evidence_buckets": {
            "identity_wrapper": {"state": "complete"},
            "holdings_exposure": {"state": "partial", "source_class": "factsheet_summary"},
            "expense_and_cost": {"state": "complete"},
            "liquidity_and_aum": {"state": "partial"},
            "benchmark_support": {"state": "complete"},
            "performance_relative_support": {"state": "complete"},
            "tax_posture": {"state": "complete", "confidence": "high"},
        },
        "benchmark_assignment": {"benchmark_fit_type": "strong_fit", "benchmark_kind": "direct"},
        "investment_lens": {"liquidity_profile": {"liquidity_status": "strong", "spread_status": "supported"}},
        "sg_lens": {"score": 82},
        "tax_truth": {"tax_score": 82.0, "tax_confidence": "high", "policy_buckets": {}, "advisory_boundary": "good_sg_retail_default", "evidence_strength": "supported"},
        "performance_metrics": {"tracking_difference_1y": -0.1},
        "eligibility": {"pressures": []},
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Eligible."},
        "decision_readiness": {"what_must_change": []},
        "recommendation_context": {"challenger": {"symbol": "ALT"}},
        "score_honesty": {"comparability": "fully_comparable", "evidence_coverage_band": "high", "unknown_share_band": "low", "unknown_dimensions": []},
        "investment_quality": {
            "user_facing_state": "best_available_with_limits",
            "recommendation_state": "recommended_primary",
            "composite_score": 91.0,
            "composite_score_valid": True,
            "structured_summary": "Candidate remains usable with limits.",
            "investment_thesis": "Candidate remains usable with limits.",
        },
    }

    pipeline = build_candidate_pipeline(
        candidate=candidate,
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        sleeve_candidates=[candidate],
        winner_candidate=candidate,
        current_holdings=[],
    )

    completeness = dict(pipeline.get("decision_completeness_status") or {})
    assert completeness.get("claim_strength_boundary") == "structural_only"
    assert completeness.get("data_completeness_grade") == "PARTIAL"


def test_alternatives_strategy_is_not_blocked_by_missing_equity_style_holdings() -> None:
    candidate = {
        "symbol": "DBMF",
        "name": "Managed Futures Strategy ETF",
        "issuer": "Issuer",
        "instrument_type": "etf_us",
        "source_state": "source_validated",
        "field_truth_surface": {"fields": []},
        "field_truth": {
            "expense_ratio": {"resolved_value": 0.85, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "aum": {"resolved_value": 1000000000.0, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "volume_30d_avg": {"resolved_value": 250000.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
            "benchmark_confidence": {"resolved_value": "medium", "completeness_state": "complete", "source_name": "benchmark_registry", "evidence_class": "verified_nonissuer"},
        },
        "evidence_buckets": {
            "identity_wrapper": {"state": "complete"},
            "holdings_exposure": {"state": "missing", "source_class": "missing"},
            "expense_and_cost": {"state": "complete"},
            "liquidity_and_aum": {"state": "complete"},
            "benchmark_support": {"state": "partial"},
            "performance_relative_support": {"state": "partial"},
            "tax_posture": {"state": "complete", "confidence": "high"},
        },
        "benchmark_assignment": {"benchmark_fit_type": "acceptable_proxy", "benchmark_kind": "proxy_etf"},
        "investment_lens": {"liquidity_profile": {"liquidity_status": "strong", "spread_status": "supported"}},
        "sg_lens": {"score": 70},
        "tax_truth": {"tax_score": 70.0, "tax_confidence": "high", "policy_buckets": {}, "advisory_boundary": "acceptable_but_structurally_inferior", "evidence_strength": "supported"},
        "performance_metrics": {},
        "eligibility": {"pressures": []},
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Eligible."},
        "decision_readiness": {"what_must_change": []},
        "recommendation_context": {"challenger": {"symbol": "ALT"}},
        "score_honesty": {"comparability": "partially_comparable", "evidence_coverage_band": "medium", "unknown_share_band": "medium", "unknown_dimensions": []},
        "investment_quality": {
            "user_facing_state": "best_available_with_limits",
            "recommendation_state": "recommended_primary",
            "composite_score": 78.0,
            "composite_score_valid": True,
            "structured_summary": "Managed futures strategy remains usable with structure-first review.",
            "investment_thesis": "Managed futures strategy remains usable with structure-first review.",
        },
    }

    pipeline = build_candidate_pipeline(
        candidate=candidate,
        sleeve_key="alternatives",
        sleeve_name="Alternatives",
        sleeve_candidates=[candidate],
        winner_candidate=candidate,
        current_holdings=[],
    )

    completeness = dict(pipeline.get("decision_completeness_status") or {})
    assert completeness.get("data_completeness_grade") == "PARTIAL"
    assert completeness.get("claim_strength_boundary") != "review_only"


def test_weak_proxy_benchmark_blocks_strong_relative_claims() -> None:
    candidate = {
        "symbol": "TESTB",
        "name": "Test Benchmark",
        "issuer": "Issuer",
        "instrument_type": "etf_ucits",
        "source_state": "source_validated",
        "field_truth_surface": {"fields": []},
        "field_truth": {
            "expense_ratio": {"resolved_value": 0.15, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "aum": {"resolved_value": 500000000.0, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "volume_30d_avg": {"resolved_value": 100000.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
            "share_class": {"resolved_value": "accumulating", "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
        },
        "evidence_buckets": {
            "identity_wrapper": {"state": "complete"},
            "holdings_exposure": {"state": "complete"},
            "expense_and_cost": {"state": "complete"},
            "liquidity_and_aum": {"state": "complete"},
            "benchmark_support": {"state": "proxy_only"},
            "performance_relative_support": {"state": "proxy_only"},
            "tax_posture": {"state": "complete", "confidence": "high"},
        },
        "benchmark_assignment": {"benchmark_fit_type": "weak_proxy", "benchmark_kind": "proxy", "benchmark_source_type": "proxy_etf"},
        "investment_lens": {"liquidity_profile": {"liquidity_status": "strong", "spread_status": "supported"}},
        "sg_lens": {"score": 82},
        "tax_truth": {"tax_score": 82.0, "tax_confidence": "high", "policy_buckets": {}, "advisory_boundary": "good_sg_retail_default", "evidence_strength": "supported"},
        "performance_metrics": {"tracking_difference_1y": -0.1},
        "eligibility": {"pressures": []},
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Eligible."},
        "decision_readiness": {"what_must_change": []},
        "recommendation_context": {"challenger": {"symbol": "ALT"}},
        "score_honesty": {"comparability": "fully_comparable", "evidence_coverage_band": "high", "unknown_share_band": "low", "unknown_dimensions": []},
        "investment_quality": {
            "user_facing_state": "best_available_with_limits",
            "recommendation_state": "recommended_primary",
            "composite_score": 91.0,
            "composite_score_valid": True,
            "structured_summary": "Candidate remains usable with limits.",
            "investment_thesis": "Candidate remains usable with limits.",
        },
    }

    pipeline = build_candidate_pipeline(
        candidate=candidate,
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        sleeve_candidates=[candidate],
        winner_candidate=candidate,
        current_holdings=[],
    )

    benchmark = dict(pipeline.get("benchmark_support_status") or {})
    investor = dict(pipeline.get("investor_recommendation_status") or {})
    assert benchmark.get("performance_claims_allowed") is False
    assert benchmark.get("performance_claim_limit") == "no_strong_relative_performance_claims"
    assert investor.get("investor_status") == "WATCHLIST_CANDIDATE"


def test_tax_score_and_tax_confidence_can_diverge() -> None:
    candidate = {
        "symbol": "TESTT",
        "name": "Test Tax",
        "issuer": "Issuer",
        "instrument_type": "etf_ucits",
        "source_state": "source_validated",
        "field_truth_surface": {"fields": []},
        "field_truth": {
            "expense_ratio": {"resolved_value": 0.15, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "aum": {"resolved_value": 500000000.0, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "volume_30d_avg": {"resolved_value": 100000.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
            "share_class": {"resolved_value": "accumulating", "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
        },
        "evidence_buckets": {
            "identity_wrapper": {"state": "complete"},
            "holdings_exposure": {"state": "complete"},
            "expense_and_cost": {"state": "complete"},
            "liquidity_and_aum": {"state": "complete"},
            "benchmark_support": {"state": "complete"},
            "performance_relative_support": {"state": "complete"},
            "tax_posture": {"state": "partial", "confidence": "low"},
        },
        "benchmark_assignment": {"benchmark_fit_type": "strong_fit", "benchmark_kind": "direct"},
        "investment_lens": {"liquidity_profile": {"liquidity_status": "strong", "spread_status": "supported"}},
        "sg_lens": {"score": 88},
        "tax_truth": {"tax_score": 88.0, "tax_confidence": "low", "policy_buckets": {}, "advisory_boundary": "requires_case_specific_review", "evidence_strength": "thin"},
        "performance_metrics": {"tracking_difference_1y": -0.1},
        "eligibility": {"pressures": []},
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Eligible."},
        "decision_readiness": {"what_must_change": []},
        "recommendation_context": {"challenger": {"symbol": "ALT"}},
        "score_honesty": {"comparability": "fully_comparable", "evidence_coverage_band": "high", "unknown_share_band": "low", "unknown_dimensions": []},
        "investment_quality": {
            "user_facing_state": "best_available_with_limits",
            "recommendation_state": "recommended_primary",
            "composite_score": 91.0,
            "composite_score_valid": True,
            "structured_summary": "Candidate remains usable with limits.",
            "investment_thesis": "Tax edge remains tentative.",
        },
    }

    pipeline = build_candidate_pipeline(
        candidate=candidate,
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        sleeve_candidates=[candidate],
        winner_candidate=candidate,
        current_holdings=[],
    )

    tax = dict(pipeline.get("tax_assumption_status") or {})
    investor = dict(pipeline.get("investor_recommendation_status") or {})
    assert tax.get("tax_score") == 88.0
    assert tax.get("tax_confidence") == "low"
    assert tax.get("decisive_tax_use_allowed") is False
    assert investor.get("recommendation_confidence") in {"low", "moderate"}


def test_canonical_decision_blocks_buyable_when_core_holdings_truth_is_incomplete() -> None:
    candidate = {
        "symbol": "TESTCORE",
        "name": "Core Candidate",
        "issuer": "Issuer",
        "instrument_type": "etf_ucits",
        "source_state": "source_validated",
        "field_truth_surface": {"fields": []},
        "field_truth": {
            "expense_ratio": {"resolved_value": 0.10, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "aum": {"resolved_value": 900000000.0, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "volume_30d_avg": {"resolved_value": 220000.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
        },
        "evidence_buckets": {
            "identity_wrapper": {"state": "complete"},
            "holdings_exposure": {"state": "missing"},
            "expense_and_cost": {"state": "complete"},
            "liquidity_and_aum": {"state": "complete"},
            "benchmark_support": {"state": "complete"},
            "performance_relative_support": {"state": "complete"},
            "tax_posture": {"state": "complete", "confidence": "high"},
        },
        "benchmark_assignment": {"benchmark_fit_type": "strong_fit", "benchmark_kind": "direct"},
        "investment_lens": {"liquidity_profile": {"liquidity_status": "strong", "spread_status": "supported"}},
        "sg_lens": {"score": 85},
        "tax_truth": {"tax_score": 84.0, "tax_confidence": "high", "policy_buckets": {}, "advisory_boundary": "good_sg_retail_default", "evidence_strength": "supported"},
        "performance_metrics": {"tracking_difference_1y": -0.1},
        "eligibility": {"pressures": []},
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Eligible."},
        "decision_readiness": {"what_must_change": []},
        "recommendation_context": {"challenger": {"symbol": "ALT"}},
        "score_honesty": {"comparability": "fully_comparable", "evidence_coverage_band": "high", "unknown_share_band": "low", "unknown_dimensions": []},
        "investment_quality": {
            "user_facing_state": "fully_clean_recommendable",
            "recommendation_state": "recommended_primary",
            "composite_score": 94.0,
            "composite_score_valid": True,
            "structured_summary": "Candidate screens well but holdings truth is incomplete.",
            "investment_thesis": "Candidate screens well but holdings truth is incomplete.",
        },
    }

    pipeline = build_candidate_pipeline(
        candidate=candidate,
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        sleeve_candidates=[candidate],
        winner_candidate=candidate,
        current_holdings=[],
    )

    canonical = dict(pipeline.get("canonical_decision") or {})
    blockers = dict(canonical.get("eligibility_and_blockers") or {})

    assert canonical.get("promotion_state") != "buyable"
    assert "core_holdings_incomplete" in list(blockers.get("failed_blocker_codes") or [])
    assert str(dict(canonical.get("action_boundary") or {}).get("state") or "") in {"blocked", "monitor_only", "secondary_review_only"}


def test_canonical_decision_tax_uncertainty_blocks_buyable_promotion() -> None:
    candidate = {
        "symbol": "TESTTAX",
        "name": "Tax Uncertain Candidate",
        "issuer": "Issuer",
        "instrument_type": "etf_ucits",
        "source_state": "source_validated",
        "field_truth_surface": {"fields": []},
        "field_truth": {
            "expense_ratio": {"resolved_value": 0.10, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "aum": {"resolved_value": 900000000.0, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "volume_30d_avg": {"resolved_value": 220000.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
            "share_class": {"resolved_value": "accumulating", "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
        },
        "evidence_buckets": {
            "identity_wrapper": {"state": "complete"},
            "holdings_exposure": {"state": "complete"},
            "expense_and_cost": {"state": "complete"},
            "liquidity_and_aum": {"state": "complete"},
            "benchmark_support": {"state": "complete"},
            "performance_relative_support": {"state": "complete"},
            "tax_posture": {"state": "partial", "confidence": "low"},
        },
        "benchmark_assignment": {"benchmark_fit_type": "strong_fit", "benchmark_kind": "direct"},
        "investment_lens": {"liquidity_profile": {"liquidity_status": "strong", "spread_status": "supported"}},
        "sg_lens": {"score": 88},
        "tax_truth": {"tax_score": 92.0, "tax_confidence": "low", "policy_buckets": {}, "advisory_boundary": "requires_case_specific_review", "evidence_strength": "thin"},
        "performance_metrics": {"tracking_difference_1y": -0.1},
        "eligibility": {"pressures": []},
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Eligible."},
        "decision_readiness": {"what_must_change": []},
        "recommendation_context": {"challenger": {"symbol": "ALT"}},
        "score_honesty": {"comparability": "fully_comparable", "evidence_coverage_band": "high", "unknown_share_band": "low", "unknown_dimensions": []},
        "investment_quality": {
            "user_facing_state": "fully_clean_recommendable",
            "recommendation_state": "recommended_primary",
            "composite_score": 93.0,
            "composite_score_valid": True,
            "structured_summary": "Candidate is attractive but tax authority remains weak.",
            "investment_thesis": "Candidate is attractive but tax authority remains weak.",
        },
    }

    pipeline = build_candidate_pipeline(
        candidate=candidate,
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        sleeve_candidates=[candidate],
        winner_candidate=candidate,
        current_holdings=[],
    )

    canonical = dict(pipeline.get("canonical_decision") or {})
    blockers = dict(canonical.get("eligibility_and_blockers") or {})

    assert canonical.get("promotion_state") != "buyable"
    assert "significant_tax_uncertainty" in list(blockers.get("failed_blocker_codes") or [])


def test_blocked_candidate_report_stays_non_endorsement_like() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "SGOV")

    canonical = dict(candidate.get("canonical_decision") or {})
    blockers = dict(canonical.get("eligibility_and_blockers") or {})
    report = dict(canonical.get("report_sections") or {})
    action = dict(canonical.get("action_boundary") or {})
    summary = str(canonical.get("plain_english_summary") or "").lower()

    assert list(blockers.get("failed_blocker_codes") or [])
    assert action.get("state") in {"blocked", "secondary_review_only", "no_change"}
    assert "buyable" not in summary
    assert "recommended" not in summary
    assert "do not" in str(report.get("what_not_to_do_now") or "").lower()
    assert "do not change the portfolio" in str(report.get("what_to_do_now") or "").lower() or "keep the current holding" in str(report.get("what_to_do_now") or "").lower()


def test_weak_tax_authority_constrains_tax_language_in_report() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "SGOV")

    canonical = dict(candidate.get("canonical_decision") or {})
    tax = dict(canonical.get("tax_authority") or {})
    report = dict(canonical.get("report_sections") or {})

    assert tax.get("decisive_tax_use_allowed") is False
    assert "not be treated as a decisive edge" in str(report.get("tax_authority") or "").lower()


def test_unresolved_switch_friction_constrains_switch_language_in_report() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "BILS")

    canonical = dict(candidate.get("canonical_decision") or {})
    blockers = dict(canonical.get("eligibility_and_blockers") or {})
    report = dict(canonical.get("report_sections") or {})

    assert "implementation_friction_unclear" in list(blockers.get("failed_blocker_codes") or []) or "switch_cost_unclear" in list(blockers.get("failed_blocker_codes") or [])
    assert "not yet fully clear" in str(report.get("switch_cost_or_friction") or "").lower() or "too weak" in str(report.get("switch_cost_or_friction") or "").lower()
    assert dict(canonical.get("tax_authority") or {}).get("decisive_tax_use_allowed") is False


def test_canonical_decision_prefers_no_change_when_practical_edge_is_too_small() -> None:
    current_candidate = {
        "symbol": "CSPX",
        "name": "Current Holding",
        "instrument_type": "etf_ucits",
        "issuer": "Issuer",
        "source_state": "source_validated",
        "field_truth_surface": {"fields": [{"field_name": "expense_ratio", "label": "Expense ratio", "value_state": "resolved", "decision_critical": True}]},
        "field_truth": {
            "expense_ratio": {"resolved_value": 0.07, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "bid_ask_spread_proxy": {"resolved_value": 8.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
            "aum": {"resolved_value": 1800000000.0, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "volume_30d_avg": {"resolved_value": 250000.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
            "share_class": {"resolved_value": "accumulating", "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "holdings_count": {"resolved_value": 500, "completeness_state": "complete", "source_name": "etf_holdings", "evidence_class": "verified_official"},
            "us_weight": {"resolved_value": 65.0, "completeness_state": "complete", "source_name": "etf_holdings", "evidence_class": "verified_official"},
        },
        "evidence_buckets": {
            "identity_wrapper": {"state": "complete"},
            "holdings_exposure": {"state": "complete"},
            "expense_and_cost": {"state": "complete"},
            "liquidity_and_aum": {"state": "complete"},
            "benchmark_support": {"state": "complete"},
            "performance_relative_support": {"state": "complete"},
            "tax_posture": {"state": "complete", "confidence": "high"},
        },
        "benchmark_assignment": {"benchmark_key": "acwi", "benchmark_label": "MSCI ACWI", "benchmark_fit_type": "strong_fit", "benchmark_authority_level": "direct", "benchmark_kind": "direct"},
        "investment_lens": {"liquidity_profile": {"liquidity_status": "strong", "spread_status": "supported", "explanation": "Liquidity is usable."}},
        "sg_lens": {"score": 75},
        "tax_truth": {"tax_score": 78.0, "tax_confidence": "high", "policy_buckets": {}, "advisory_boundary": "good_sg_retail_default", "evidence_strength": "supported"},
        "performance_metrics": {},
        "eligibility": {"pressures": []},
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Current holding remains acceptable."},
        "decision_readiness": {"what_must_change": []},
        "recommendation_context": {"lead_summary": "Current holding remains acceptable."},
        "score_honesty": {"comparability": "fully_comparable", "evidence_coverage_band": "high", "unknown_share_band": "low", "unknown_dimensions": []},
        "investment_quality": {
            "user_facing_state": "best_available_with_limits",
            "recommendation_state": "watchlist_only",
            "composite_score": 79.0,
            "composite_score_valid": True,
            "cost_score": 70.0,
            "liquidity_score": 72.0,
            "structure_score": 75.0,
            "tax_score": 73.0,
            "performance_score": 69.0,
            "risk_adjusted_score": 68.0,
            "governance_confidence_score": 71.0,
            "sg_rank": 70.0,
            "structured_summary": "Current holding remains acceptable.",
            "investment_thesis": "Current holding delivers acceptable exposure.",
        },
    }
    preferred_candidate = {
        **current_candidate,
        "symbol": "VWRA",
        "name": "Preferred Candidate",
        "field_truth": {
            **dict(current_candidate["field_truth"]),
            "expense_ratio": {"resolved_value": 0.08, "completeness_state": "complete", "source_name": "issuer_doc_parser", "evidence_class": "verified_official"},
            "bid_ask_spread_proxy": {"resolved_value": 9.0, "completeness_state": "complete", "source_name": "etf_market_data", "evidence_class": "verified_official"},
        },
        "decision_record": {"policy_gates": {"gates": []}, "reason": "Preferred candidate leads marginally."},
        "recommendation_context": {"lead_summary": "Preferred candidate leads only marginally after policy checks."},
        "investment_quality": {
            **dict(current_candidate["investment_quality"]),
            "user_facing_state": "fully_clean_recommendable",
            "recommendation_state": "recommended_primary",
            "composite_score": 82.0,
            "investment_thesis": "Preferred candidate improves sleeve implementation only marginally.",
            "structured_summary": "Preferred candidate improves sleeve implementation only marginally.",
        },
    }

    pipeline = build_candidate_pipeline(
        candidate=preferred_candidate,
        sleeve_key="global_equity_core",
        sleeve_name="Global Equity Core",
        sleeve_candidates=[preferred_candidate, current_candidate],
        winner_candidate=preferred_candidate,
        current_holdings=[
            {
                "symbol": "CSPX",
                "name": "Current Holding",
                "quantity": 10.0,
                "cost_basis": 100.0,
                "currency": "USD",
                "sleeve": "global_equity",
                "account_type": "broker",
            }
        ],
    )

    canonical = dict(pipeline.get("canonical_decision") or {})
    incumbent = dict(canonical.get("incumbent_comparison_result") or {})
    action = dict(canonical.get("action_boundary") or {})

    assert incumbent.get("no_change_is_best") is True
    assert action.get("state") == "no_change"
    assert "keep the current holding" in str(action.get("do_now") or "").lower()


def test_canonical_decision_manual_approval_required_for_non_rebalance_replace() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")
    canonical = dict(candidate.get("canonical_decision") or {})
    action = dict(canonical.get("action_boundary") or {})

    if str(dict(canonical.get("recommendation_state") or {}).get("decision_type") or "") == "REPLACE":
        assert action.get("manual_approval_required") is True
        assert "manual approval" in str(action.get("manual_approval_note") or "").lower()


def test_canonical_decision_exposes_deterministic_lens_assessment_and_fusion() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")

    canonical = dict(candidate.get("canonical_decision") or {})
    lens_assessment = dict(canonical.get("lens_assessment") or {})
    fusion = dict(canonical.get("lens_fusion_result") or {})
    framework = dict(canonical.get("framework_judgment") or {})
    report = dict(canonical.get("report_sections") or {})

    per_lens = dict(lens_assessment.get("per_lens") or {})
    assert set(per_lens.keys()) == {
        "marks_cycle_risk",
        "buffett_munger_quality",
        "dalio_regime_transmission",
        "implementation_reality",
        "fragility_red_team",
    }
    for lens in per_lens.values():
        assert str(lens.get("lens_status") or "") in {"supportive", "neutral", "cautious", "constraining", "blocking", "explanatory_only"}
        assert str(lens.get("promotion_cap") or "") in {"none", "acceptable", "near_decision_ready"}
        assert str(lens.get("confidence_modifier") or "") in {"none", "soften", "materially_soften"}
    assert str(fusion.get("overall_lens_posture") or "") in {
        "supportive_with_restraint",
        "mixed_but_constructive",
        "caution_dominant",
        "promotion_constrained",
        "blocked_by_fragility",
        "explanatory_only",
    }
    assert str(fusion.get("promotion_cap") or "") in {"none", "acceptable", "near_decision_ready"}
    assert framework.get("summary")
    assert isinstance(report.get("lens_supports") or [], list)
    assert isinstance(report.get("lens_cautions") or [], list)


def test_lens_fusion_does_not_upgrade_base_promotion_state() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")

    canonical = dict(candidate.get("canonical_decision") or {})
    base = str(canonical.get("base_promotion_state") or "")
    final = str(canonical.get("promotion_state") or "")
    order = {"research_only": 0, "acceptable": 1, "near_decision_ready": 2, "buyable": 3}

    assert order[final] <= order[base]


def test_lens_fusion_enters_explanatory_only_when_buyable_blocked() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "BILS")

    canonical = dict(candidate.get("canonical_decision") or {})
    blockers = dict(canonical.get("eligibility_and_blockers") or {})
    fusion = dict(canonical.get("lens_fusion_result") or {})

    assert blockers.get("buyable_blocked") is True
    assert fusion.get("explanatory_only") is True
    assert str(fusion.get("action_tone_constraint") or "") == "monitoring_only"
