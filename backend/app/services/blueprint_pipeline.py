from __future__ import annotations

from copy import deepcopy
from typing import Any

from app.services.blueprint_canonical_decision import (
    build_base_promotion_state,
    build_canonical_decision_object,
    build_canonical_gate_completeness_result,
)
from app.services.framework_lenses import build_lens_assessment, build_lens_fusion_result
from app.services.blueprint_replacement_opportunities import _estimate_switching_frictions
from app.services.portfolio_state import build_portfolio_state_context
from app.services.policy_authority import evaluate_challenger_promotion
from app.services.blueprint_approval import check_policy_escalation_allowed


SOURCE_TIERS = ("tier_a", "tier_b", "tier_c", "tier_d", "tier_e")
SOURCE_QUALITY_FLAGS = (
    "fresh",
    "slightly_stale",
    "materially_stale",
    "conflicting",
    "incomplete",
    "proxy_based",
    "estimated_not_verified",
)
GATE_STATUSES = ("pass", "fail", "partial", "not_evaluated")
REVIEW_INTENSITIES = ("level_1_universal", "level_2_deep")
CANDIDATE_STATUSES = (
    "fully_clean_recommendable",
    "best_available_with_limits",
    "research_ready_but_not_recommendable",
    "blocked_by_policy",
    "blocked_by_missing_required_evidence",
    "blocked_by_unresolved_gate",
)
DECISION_TYPES = ("ADD", "REPLACE", "HOLD", "TRIM", "REJECT", "RESEARCH")
CAUTION_LEVELS = ("minor", "moderate", "material", "critical")
RECOMMENDATION_TIERS = ("actionable", "non_actionable")

MODULE_CLASSIFICATION = [
    {
        "module_path": "app/services/blueprint_candidate_truth.py",
        "current_responsibility": "Candidate truth and provenance resolution",
        "target_layer": "Layer 1 candidate sourcing and evidence assembly",
        "classification": "KEEP_AND_REFACTOR",
        "reason": "Already owns candidate truth and provenance. It should remain canonical for evidence inputs rather than be replaced.",
        "migration_note": "Expose its resolved truth through canonical EvidencePack instead of duplicating field fragments later.",
    },
    {
        "module_path": "app/services/blueprint_benchmark_registry.py",
        "current_responsibility": "Benchmark assignment, validation, and truth context",
        "target_layer": "Layer 2 source integrity and policy checks",
        "classification": "KEEP_AND_REFACTOR",
        "reason": "Benchmark integrity belongs in the canonical pipeline, but current assignment and validation logic is still valid.",
        "migration_note": "Use as the authoritative benchmark-input provider for SourceIntegrityResult and GateResult.",
    },
    {
        "module_path": "app/services/blueprint_candidate_eligibility.py",
        "current_responsibility": "Eligibility, blockers, cautions, and pressure extraction",
        "target_layer": "Layer 2 source integrity and policy checks / Layer 3 mandate and sleeve framework",
        "classification": "KEEP_AND_REFACTOR",
        "reason": "Current logic is useful but mixes gate and diagnostic meaning. It should feed canonical GateResult instead of owning public architecture.",
        "migration_note": "Treat as a deterministic input layer behind GateResult and UniversalReviewResult.",
    },
    {
        "module_path": "app/services/blueprint_investment_quality.py",
        "current_responsibility": "Score generation and performance support",
        "target_layer": "Layer 5 scoring and portfolio impact assessment",
        "classification": "KEEP_AND_REFACTOR",
        "reason": "Already the scoring backbone. It needs canonical wrapping, not parallel replacement.",
        "migration_note": "Surface one ScoringResult object and comparability context instead of multiple overlapping score views.",
    },
    {
        "module_path": "app/services/blueprint_recommendations.py",
        "current_responsibility": "Ranking, recommendation states, and recommendation context",
        "target_layer": "Layer 6 recommendation engine",
        "classification": "KEEP_AND_REFACTOR",
        "reason": "Current ranking logic is still useful, but RecommendationResult should become the canonical public object.",
        "migration_note": "Keep ranking and event persistence, but route final recommendation semantics through RecommendationResult.",
    },
    {
        "module_path": "app/services/blueprint_canonical_decision.py",
        "current_responsibility": "Canonical candidate decision meaning and report sections",
        "target_layer": "Layer 7 memo and explanation engine",
        "classification": "CANONICAL_KEEPER",
        "reason": "This is now the authoritative owner of investor-facing candidate meaning.",
        "migration_note": "Do not reintroduce semantic owners ahead of or beside the canonical decision object.",
    },
    {
        "module_path": "app/services/framework_lenses",
        "current_responsibility": "Deterministic bounded framework lens judgments and fusion",
        "target_layer": "Layer 6A framework restraint and pressure-testing",
        "classification": "CANONICAL_KEEPER",
        "reason": "Lens judgments refine caution, review intensity, and promotion caps without becoming parallel recommendation owners.",
        "migration_note": "Keep deterministic and subordinate to the canonical gate and decision path.",
    },
    {
        "module_path": "app/services/portfolio_blueprint.py",
        "current_responsibility": "Thin orchestration entry point",
        "target_layer": "Cross-layer orchestration only",
        "classification": "ORCHESTRATION_ONLY",
        "reason": "The live assembler now sits behind a thin entry point so semantic ownership does not live here.",
        "migration_note": "Keep this module thin and route payload shaping through the assembler and canonical decision path only.",
    },
]

_BASELINE_SYMBOLS = {
    "global_equity_core": "VWRA",
    "developed_ex_us_optional": "VEVE",
    "emerging_markets": "EIMI",
    "china_satellite": "HMCH",
    "ig_bonds": "AGGU",
    "cash_bills": "IB01",
    "real_assets": "SGLN",
    "alternatives": "DBMF",
    "convex": "CAOS",
}

_CURRENT_HOLDING_SLEEVE_MAP = {
    "global_equity_core": "global_equity",
    "developed_ex_us_optional": "global_equity",
    "emerging_markets": "global_equity",
    "china_satellite": "global_equity",
    "ig_bonds": "ig_bond",
    "cash_bills": "cash",
    "real_assets": "real_asset",
    "alternatives": "alt",
    "convex": "convex",
}

_REPLACEMENT_EDGE_THRESHOLDS = {
    "global_equity_core": 10.0,
    "developed_ex_us_optional": 8.0,
    "emerging_markets": 9.0,
    "china_satellite": 9.0,
    "ig_bonds": 7.0,
    "cash_bills": 6.0,
    "real_assets": 8.0,
    "alternatives": 8.0,
    "convex": 7.0,
}

_GATE_CODE_MAP = {
    "sleeve_constitution_fit": ("G1", "sleeve_role_fit"),
    "structural_ineligibility": ("G2", "structural_clarity"),
    "critical_liquidity_failure": ("G4", "implementation_viability"),
    "prohibited_domicile_or_tax_state": ("G6", "tax_and_jurisdiction_acceptability"),
    "required_benchmark_support": ("G7", "evidence_sufficiency"),
    "bounded_loss_requirement": ("G8", "no_hidden_fragility"),
    "leverage_prohibited": ("G8", "no_hidden_fragility"),
    "critical_governance_failure": ("G10", "recommendation_plausibility"),
}


def classify_blueprint_modules() -> list[dict[str, Any]]:
    return deepcopy(MODULE_CLASSIFICATION)


def _has_meaningful_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized not in {"", "n/a", "unknown", "none", "null"}
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _field_truth_resolved_value(candidate: dict[str, Any], field_name: str) -> Any:
    field_truth = dict(candidate.get("field_truth") or {})
    field = dict(field_truth.get(field_name) or {})
    if str(field.get("missingness_reason") or "") == "populated":
        return field.get("resolved_value")
    return None


def _normalize_gap_reason(reason: Any, *, field_name: Any = None, label: Any = None) -> str:
    raw = str(reason or "").strip()
    human_label = str(label or field_name or "This field").strip().replace("_", " ")
    if not raw:
        return ""
    if raw == "fetchable_from_current_sources":
        return f"{human_label} can likely be filled from current sources, but it is not populated yet."
    if raw == "blocked_by_parser_gap":
        return f"{human_label} is still blocked by a parser gap."
    if raw == "blocked_by_source_gap":
        return f"{human_label} still needs source expansion."
    return raw


def build_candidate_pipeline(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    sleeve_name: str,
    sleeve_candidates: list[dict[str, Any]],
    winner_candidate: dict[str, Any] | None,
    current_holdings: list[dict[str, Any]] | None = None,
    conn: Any | None = None,
    approval_required: bool = False,
) -> dict[str, Any]:
    candidate_record = _build_candidate_record(candidate=candidate, sleeve_key=sleeve_key, sleeve_name=sleeve_name)
    evidence_pack = _build_evidence_pack(candidate)
    source_integrity = _build_source_integrity_result(candidate)
    baseline_reference = _build_baseline_reference(sleeve_key=sleeve_key, sleeve_candidates=sleeve_candidates)
    gate_result = _build_gate_result(candidate=candidate, source_integrity=source_integrity)
    review_intensity = _build_review_intensity_decision(candidate=candidate, sleeve_key=sleeve_key)
    universal_review = _build_universal_review_result(candidate=candidate, sleeve_key=sleeve_key)
    deep_review = _build_deep_review_result(
        candidate=candidate,
        sleeve_key=sleeve_key,
        review_intensity=review_intensity,
        universal_review=universal_review,
    )
    scoring_result = _build_scoring_result(candidate=candidate, sleeve_candidates=sleeve_candidates, baseline_reference=baseline_reference)
    current_holding_record = _build_current_holding_record(
        candidate=candidate,
        sleeve_key=sleeve_key,
        sleeve_candidates=sleeve_candidates,
        current_holdings=current_holdings or [],
    )
    recommendation_result = _build_recommendation_result(
        candidate=candidate,
        sleeve_key=sleeve_key,
        winner_candidate=winner_candidate,
        baseline_reference=baseline_reference,
        current_holding_record=current_holding_record,
        source_integrity=source_integrity,
        gate_result=gate_result,
        scoring_result=scoring_result,
        conn=conn,
        approval_required=approval_required,
    )
    benchmark_support_status = _build_benchmark_support_status(candidate)
    decision_completeness_status = _build_decision_completeness_status(
        candidate=candidate,
        source_integrity=source_integrity,
        scoring_result=scoring_result,
        benchmark_support_status=benchmark_support_status,
        current_holding_record=current_holding_record,
        recommendation_result=recommendation_result,
    )
    portfolio_completeness_status = _build_portfolio_completeness_status(
        candidate=candidate,
        current_holding_record=current_holding_record,
    )
    decision_thesis = _build_decision_thesis(
        candidate=candidate,
        sleeve_key=sleeve_key,
        recommendation_result=recommendation_result,
        source_integrity=source_integrity,
        scoring_result=scoring_result,
        baseline_reference=baseline_reference,
    )
    forecast_visual_model = _build_forecast_visual_model(
        candidate=candidate,
        scoring_result=scoring_result,
        source_integrity=source_integrity,
    )
    forecast_defensibility_status = _build_forecast_defensibility_status(
        candidate=candidate,
        scoring_result=scoring_result,
        source_integrity=source_integrity,
        benchmark_support_status=benchmark_support_status,
        forecast_visual_model=forecast_visual_model,
    )
    tax_assumption_status = _build_tax_assumption_status(candidate)
    cost_realism_summary = _build_cost_realism_summary(
        candidate=candidate,
        current_holding_record=current_holding_record,
        recommendation_result=recommendation_result,
        tax_assumption_status=tax_assumption_status,
    )
    portfolio_consequence_summary = _build_portfolio_consequence_summary(
        candidate=candidate,
        baseline_reference=baseline_reference,
        current_holding_record=current_holding_record,
        recommendation_result=recommendation_result,
    )
    investor_recommendation_status = _build_investor_recommendation_status(
        candidate=candidate,
        recommendation_result=recommendation_result,
        decision_completeness_status=decision_completeness_status,
        portfolio_completeness_status=portfolio_completeness_status,
        benchmark_support_status=benchmark_support_status,
        forecast_defensibility_status=forecast_defensibility_status,
        tax_assumption_status=tax_assumption_status,
        source_integrity=source_integrity,
        current_holding_record=current_holding_record,
    )
    decision_change_set = _build_decision_change_set(
        candidate=candidate,
        recommendation_result=recommendation_result,
    )
    gate_summary = build_canonical_gate_completeness_result(
        sleeve_key=sleeve_key,
        gate_result=gate_result,
        decision_completeness_status=decision_completeness_status,
        portfolio_completeness_status=portfolio_completeness_status,
        benchmark_support_status=benchmark_support_status,
        tax_assumption_status=tax_assumption_status,
        forecast_defensibility_status=forecast_defensibility_status,
        source_integrity_result=source_integrity,
        evidence_pack=evidence_pack,
        current_holding_record=current_holding_record,
    )
    base_promotion_state = build_base_promotion_state(
        candidate=candidate,
        gate_summary=gate_summary,
        recommendation_result=recommendation_result,
        investor_recommendation_status=investor_recommendation_status,
        current_holding_record=current_holding_record,
    )
    lens_assessment = build_lens_assessment(
        candidate=candidate,
        sleeve_key=sleeve_key,
        gate_summary=gate_summary,
        source_integrity_result=source_integrity,
        benchmark_support_status=benchmark_support_status,
        tax_assumption_status=tax_assumption_status,
        forecast_defensibility_status=forecast_defensibility_status,
        portfolio_completeness_status=portfolio_completeness_status,
        current_holding_record=current_holding_record,
        recommendation_result=recommendation_result,
        portfolio_consequence_summary=portfolio_consequence_summary,
        cost_realism_summary=cost_realism_summary,
    )
    lens_fusion_result = build_lens_fusion_result(
        gate_summary=gate_summary,
        base_promotion_state=base_promotion_state,
        lens_judgments=dict(lens_assessment.get("per_lens") or {}),
        recommendation_result=recommendation_result,
    )
    canonical_decision = build_canonical_decision_object(
        candidate=candidate,
        sleeve_key=sleeve_key,
        evidence_pack=evidence_pack,
        source_integrity_result=source_integrity,
        gate_result=gate_result,
        decision_completeness_status=decision_completeness_status,
        portfolio_completeness_status=portfolio_completeness_status,
        benchmark_support_status=benchmark_support_status,
        tax_assumption_status=tax_assumption_status,
        forecast_defensibility_status=forecast_defensibility_status,
        current_holding_record=current_holding_record,
        recommendation_result=recommendation_result,
        investor_recommendation_status=investor_recommendation_status,
        portfolio_consequence_summary=portfolio_consequence_summary,
        decision_change_set=decision_change_set,
        gate_summary=gate_summary,
        base_promotion_state=base_promotion_state,
        lens_assessment=lens_assessment,
        lens_fusion_result=lens_fusion_result,
    )
    decision_thesis = {
        "thesis_summary": canonical_decision.get("plain_english_summary"),
        "key_allocation_reason": dict(canonical_decision.get("report_sections") or {}).get("why_attractive"),
        "key_reservation_reason": dict(canonical_decision.get("report_sections") or {}).get("main_tradeoff"),
    }
    supporting_metadata_summary = _build_supporting_metadata_summary(
        candidate=candidate,
        source_integrity=source_integrity,
        gate_result=gate_result,
        scoring_result=scoring_result,
        current_holding_record=current_holding_record,
        recommendation_result=recommendation_result,
    )
    memo_result = _build_memo_result(
        candidate=candidate,
        sleeve_key=sleeve_key,
        recommendation_result=recommendation_result,
        current_holding_record=current_holding_record,
        source_integrity=source_integrity,
        gate_result=gate_result,
        scoring_result=scoring_result,
    )
    audit_trace = _build_audit_trace(
        candidate=candidate,
        evidence_pack=evidence_pack,
        source_integrity=source_integrity,
        gate_result=gate_result,
        review_intensity=review_intensity,
        universal_review=universal_review,
        deep_review=deep_review,
        scoring_result=scoring_result,
        recommendation_result=recommendation_result,
        memo_result=memo_result,
    )
    return {
        "candidate_record": candidate_record,
        "evidence_pack": evidence_pack,
        "source_integrity_result": source_integrity,
        "gate_result": gate_result,
        "review_intensity_decision": review_intensity,
        "universal_review_result": universal_review,
        "deep_review_result": deep_review,
        "scoring_result": scoring_result,
        "current_holding_record": current_holding_record,
        "recommendation_result": recommendation_result,
        "decision_completeness_status": decision_completeness_status,
        "portfolio_completeness_status": portfolio_completeness_status,
        "investor_recommendation_status": investor_recommendation_status,
        "benchmark_support_status": benchmark_support_status,
        "gate_summary": gate_summary,
        "base_promotion_state": base_promotion_state,
        "lens_assessment": lens_assessment,
        "lens_fusion_result": lens_fusion_result,
        "decision_thesis": decision_thesis,
        "forecast_visual_model": forecast_visual_model,
        "forecast_defensibility_status": forecast_defensibility_status,
        "tax_assumption_status": tax_assumption_status,
        "cost_realism_summary": cost_realism_summary,
        "portfolio_consequence_summary": portfolio_consequence_summary,
        "decision_change_set": decision_change_set,
        "canonical_decision": canonical_decision,
        "supporting_metadata_summary": supporting_metadata_summary,
        "memo_result": memo_result,
        "audit_log_entries": audit_trace,
        "baseline_reference": baseline_reference,
    }


def _build_candidate_record(*, candidate: dict[str, Any], sleeve_key: str, sleeve_name: str) -> dict[str, Any]:
    return {
        "candidate_id": str(candidate.get("symbol") or ""),
        "candidate_symbol": str(candidate.get("symbol") or ""),
        "candidate_name": str(candidate.get("name") or ""),
        "candidate_type": str(candidate.get("instrument_type") or "unknown"),
        "sleeve_type": sleeve_key,
        "sleeve_name": sleeve_name,
        "issuer": str(candidate.get("issuer") or ""),
        "status": str(dict(candidate.get("investment_quality") or {}).get("user_facing_state") or "research_ready_but_not_recommendable"),
        "source_state": str(candidate.get("source_state") or "unknown"),
        "source_directness": str(candidate.get("source_directness") or "unknown"),
        "coverage_class": str(candidate.get("coverage_class") or "unknown"),
        "authority_class": str(candidate.get("authority_class") or "support_grade"),
        "fallback_state": str(candidate.get("fallback_state") or "none"),
        "claim_limit_class": str(candidate.get("claim_limit_class") or "review_only"),
        "factsheet_as_of": candidate.get("factsheet_asof"),
        "market_data_as_of": candidate.get("market_data_asof"),
    }


def _field_value(candidate: dict[str, Any], field_name: str) -> dict[str, Any]:
    field_truth = dict(candidate.get("field_truth") or {})
    return dict(field_truth.get(field_name) or {})


def _bucket_state(candidate: dict[str, Any], bucket_name: str) -> str:
    return str(dict(dict(candidate.get("evidence_buckets") or {}).get(bucket_name) or {}).get("state") or "missing")


def _bucket_confidence(candidate: dict[str, Any], bucket_name: str) -> str | None:
    value = dict(dict(candidate.get("evidence_buckets") or {}).get(bucket_name) or {}).get("confidence")
    return str(value or "").strip() or None


def _field_source_name(candidate: dict[str, Any], field_name: str) -> str:
    return str(_field_value(candidate, field_name).get("source_name") or "").strip()


def _pack_field(candidate: dict[str, Any], field_name: str, fallback: Any = None) -> dict[str, Any]:
    field = _field_value(candidate, field_name)
    value = field.get("resolved_value", fallback)
    return {
        "value": value,
        "source": field.get("source_name") or field.get("source_url"),
        "source_type": field.get("source_type"),
        "evidence_class": field.get("evidence_class"),
        "as_of": field.get("as_of"),
        "completeness_state": field.get("completeness_state"),
        "missingness_reason": field.get("missingness_reason"),
    }


def _bucket_support_summary_text(candidate: dict[str, Any]) -> str:
    bucket_support = dict(candidate.get("bucket_support") or {})
    if not bucket_support:
        return ""
    evidence_depth_class = str(candidate.get("evidence_depth_class") or "summary_backed_limited")
    priority_order = (
        "holdings_exposure",
        "benchmark_support",
        "liquidity_and_aum",
        "tax_posture",
    )
    summary_parts: list[str] = []
    for bucket_name in priority_order:
        support = dict(bucket_support.get(bucket_name) or {})
        interpretation = dict(support.get("interpretation_summary") or {})
        supports = str(interpretation.get("supports") or "").strip()
        unsupported = str(interpretation.get("does_not_support") or "").strip()
        if supports:
            summary_parts.append(supports)
        if unsupported and len(summary_parts) < 3:
            summary_parts.append(unsupported)
        if len(summary_parts) >= 3:
            break
    depth_text = {
        "direct_holdings_backed": "Evidence depth is direct-holdings-backed.",
        "structured_summary_strong": "Evidence depth is structured-summary-backed rather than direct-holdings-backed.",
        "summary_backed_limited": "Evidence depth is summary-backed and materially limited.",
        "structure_first": "Evidence depth is structure-first and should not be read through holdings-style logic.",
    }.get(evidence_depth_class, "")
    if depth_text:
        summary_parts.append(depth_text)
    return " ".join(part for part in summary_parts if part).strip()


def _build_evidence_pack(candidate: dict[str, Any]) -> dict[str, Any]:
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    liquidity = dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {})
    sg_lens = dict(candidate.get("sg_lens") or {})
    performance = dict(candidate.get("performance_metrics") or {})
    return {
        "core_identity": {
            "objective": str(candidate.get("rationale") or ""),
            "strategy": str(candidate.get("replication_method") or ""),
            "benchmark": {
                "benchmark_key": benchmark.get("benchmark_key"),
                "benchmark_label": benchmark.get("benchmark_label"),
                "benchmark_fit_type": benchmark.get("benchmark_fit_type"),
                "benchmark_authority_level": benchmark.get("benchmark_authority_level"),
            },
        },
        "portfolio_character": {
            "holdings_count": _pack_field(candidate, "holdings_count", candidate.get("holdings_count")),
            "top_10_concentration": _pack_field(candidate, "top_10_concentration", candidate.get("top10_concentration_pct")),
            "us_weight": _pack_field(candidate, "us_weight", candidate.get("us_weight_pct")),
            "em_weight": _pack_field(candidate, "em_weight", candidate.get("em_weight_pct")),
            "sector_concentration_proxy": _pack_field(candidate, "sector_concentration_proxy", candidate.get("tech_weight_pct")),
        },
        "implementation": {
            "fees": _pack_field(candidate, "expense_ratio", candidate.get("expense_ratio")),
            "aum": _pack_field(candidate, "aum", candidate.get("aum")),
            "liquidity": {
                "liquidity_status": liquidity.get("liquidity_status"),
                "spread_status": liquidity.get("spread_status"),
                "volume_30d_avg": _pack_field(candidate, "volume_30d_avg", candidate.get("volume_30d_avg")),
            },
            "tracking_difference_1y": _pack_field(candidate, "tracking_difference_1y", performance.get("tracking_difference_1y")),
            "tracking_difference_3y": _pack_field(candidate, "tracking_difference_3y", performance.get("tracking_difference_3y")),
        },
        "structure": {
            "distribution_type": _pack_field(candidate, "distribution_type", candidate.get("accumulation_or_distribution")),
            "wrapper": _pack_field(candidate, "wrapper_or_vehicle_type", candidate.get("instrument_type")),
            "domicile": _pack_field(candidate, "domicile", candidate.get("domicile")),
            "share_class_proven": _pack_field(candidate, "share_class_proven", candidate.get("share_class")),
            "derivatives_or_leverage": {
                "leverage_used": bool(candidate.get("leverage_used")),
                "max_loss_known": candidate.get("max_loss_known"),
                "short_options": bool(candidate.get("short_options")),
            },
        },
        "context": {
            "stress_behavior": dict(candidate.get("scenario_role") or {}),
            "overlap_metrics": dict(candidate.get("portfolio_overlap") or {}),
            "tax_metadata": {
                "sg_score": sg_lens.get("score"),
                "withholding_penalty": dict(sg_lens.get("breakdown") or {}).get("withholding_penalty"),
                "estate_risk_penalty": dict(sg_lens.get("breakdown") or {}).get("estate_risk_penalty"),
            },
            "implementation_metadata": {
                "primary_trading_currency": _pack_field(candidate, "primary_trading_currency", candidate.get("primary_trading_currency")),
                "primary_listing_exchange": _pack_field(candidate, "primary_listing_exchange", candidate.get("primary_listing_exchange")),
            },
        },
        "bucket_support": dict(candidate.get("bucket_support") or {}),
        "evidence_depth_class": str(candidate.get("evidence_depth_class") or ""),
        "evidence_summary": _bucket_support_summary_text(candidate),
    }


def _build_source_integrity_result(candidate: dict[str, Any]) -> dict[str, Any]:
    truth_surface = dict(candidate.get("field_truth_surface") or {})
    fields = list(truth_surface.get("fields") or [])
    critical_missing = [
        field for field in fields
        if bool(field.get("decision_critical")) and str(field.get("value_state") or "") not in {"resolved", "partial", "not_applicable"}
    ]
    material_missing = [
        field for field in fields
        if str(field.get("value_state") or "") not in {"resolved", "partial", "not_applicable"}
    ]
    source_state = str(candidate.get("source_state") or "unknown")
    freshness_state = str(candidate.get("freshness_state") or "unknown")
    proxy_fields = [
        field for field in fields
        if str(field.get("evidence_class") or "") in {"proxy_only", "proxy_only_last_resort"}
    ]
    conflict_fields = [
        field for field in fields
        if str(field.get("value_state") or "") == "conflicting"
    ]

    bucket_support = dict(candidate.get("bucket_support") or {})
    if conflict_fields:
        source_quality_flag = "conflicting"
    elif freshness_state in {"quarantined", "stale"}:
        source_quality_flag = "materially_stale"
    elif freshness_state == "aging":
        source_quality_flag = "slightly_stale"
    elif critical_missing:
        source_quality_flag = "incomplete"
    elif proxy_fields:
        source_quality_flag = "proxy_based"
    elif source_state in {"manual_seed", "refresh_failed_unvalidated"}:
        source_quality_flag = "estimated_not_verified"
    else:
        source_quality_flag = "fresh"

    caution_level = "minor"
    if conflict_fields or freshness_state == "quarantined":
        caution_level = "critical"
    elif critical_missing or freshness_state in {"stale", "quarantined"}:
        caution_level = "material"
    elif material_missing or proxy_fields:
        caution_level = "moderate"

    blocking_issues = [
        {
            "field_name": field.get("field_name"),
            "label": field.get("label"),
            "reason": _normalize_gap_reason(
                field.get("missingness_reason"),
                field_name=field.get("field_name"),
                label=field.get("label"),
            ),
        }
        for field in critical_missing[:8]
    ]
    material_issues = [
        {
            "field_name": field.get("field_name"),
            "label": field.get("label"),
            "reason": _normalize_gap_reason(
                field.get("missingness_reason"),
                field_name=field.get("field_name"),
                label=field.get("label"),
            ),
        }
        for field in material_missing[:10]
    ]
    minor_issues = [
        {
            "field_name": field.get("field_name"),
            "label": field.get("label"),
            "reason": _normalize_gap_reason(
                field.get("missingness_reason"),
                field_name=field.get("field_name"),
                label=field.get("label"),
            ),
        }
        for field in proxy_fields[:6]
    ]
    return {
        "critical_field_checks": blocking_issues,
        "missing_field_checks": material_issues,
        "freshness_checks": {
            "source_state": source_state,
            "freshness_state": freshness_state,
        },
        "conflict_checks": [
            {
                "field_name": field.get("field_name"),
                "label": field.get("label"),
                "reason": _normalize_gap_reason(
                    field.get("missingness_reason"),
                    field_name=field.get("field_name"),
                    label=field.get("label"),
                ),
            }
            for field in conflict_fields
        ],
        "overall_source_status": source_quality_flag,
        "overall_caution_level": caution_level,
        "blocking_issues": blocking_issues,
        "material_issues": material_issues,
        "minor_issues": minor_issues,
        "concise_source_summary": _bucket_support_summary_text(candidate)
        or _source_summary_text(
            source_quality_flag=source_quality_flag,
            critical_missing=critical_missing,
            freshness_state=freshness_state,
            proxy_fields=proxy_fields,
        ),
        "bucket_support_summary": {
            bucket_name: dict(dict(bucket_support.get(bucket_name) or {}).get("interpretation_summary") or {})
            for bucket_name in bucket_support
        },
    }


def _benchmark_support_quality(candidate: dict[str, Any]) -> str:
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    fit = str(benchmark.get("benchmark_fit_type") or "")
    if fit == "strong_fit":
        return "direct"
    if fit == "acceptable_proxy":
        return "acceptable_proxy"
    if fit in {"weak_proxy", "mismatched"}:
        return "weak_proxy"
    return "missing"


def _has_material_performance_support(candidate: dict[str, Any]) -> bool:
    performance = dict(candidate.get("performance_metrics") or {})
    return any(
        performance.get(key) is not None
        for key in (
            "return_1y",
            "return_3y",
            "return_5y",
            "benchmark_return_1y",
            "benchmark_return_3y",
            "tracking_difference_1y",
            "tracking_error_1y",
        )
    )


_SUPPLEMENTAL_DECISION_GAP_FIELDS: dict[str, set[str]] = {
    "ig_bonds": {"us_weight", "top_10_concentration", "holdings_count", "sector_concentration_proxy"},
    "cash_bills": {"us_weight", "top_10_concentration", "holdings_count", "sector_concentration_proxy"},
    "real_assets": {
        "us_weight",
        "em_weight",
        "top_10_concentration",
        "holdings_count",
        "sector_concentration_proxy",
        "tracking_difference_1y",
        "tracking_difference_3y",
        "tracking_difference_5y",
        "tracking_error_1y",
    },
    "alternatives": {
        "us_weight",
        "em_weight",
        "top_10_concentration",
        "holdings_count",
        "sector_concentration_proxy",
        "tracking_difference_1y",
        "tracking_difference_3y",
        "tracking_difference_5y",
        "tracking_error_1y",
    },
    "convex": {
        "us_weight",
        "em_weight",
        "top_10_concentration",
        "holdings_count",
        "sector_concentration_proxy",
        "tracking_difference_1y",
        "tracking_difference_3y",
        "tracking_difference_5y",
        "tracking_error_1y",
    },
}


def _candidate_field_fallback_value(candidate: dict[str, Any], field_name: str) -> Any:
    performance = dict(candidate.get("performance_metrics") or {})
    factsheet_history = dict(candidate.get("aum_history_summary") or {})
    benchmark_assignment = dict(candidate.get("benchmark_assignment") or {})
    if field_name == "aum":
        return (
            _field_truth_resolved_value(candidate, "aum")
            if _has_meaningful_value(_field_truth_resolved_value(candidate, "aum"))
            else performance.get("aum_usd_latest")
            if performance.get("aum_usd_latest") is not None
            else factsheet_history.get("latest_aum_usd")
            if factsheet_history.get("latest_aum_usd") is not None
            else candidate.get("aum_usd")
            if candidate.get("aum_usd") is not None
            else candidate.get("aum")
        )
    if field_name == "tracking_difference_1y":
        return (
            _field_truth_resolved_value(candidate, "tracking_difference_1y")
            if _has_meaningful_value(_field_truth_resolved_value(candidate, "tracking_difference_1y"))
            else performance.get("tracking_difference_1y")
            if performance.get("tracking_difference_1y") is not None
            else factsheet_history.get("tracking_difference_1y")
            if factsheet_history.get("tracking_difference_1y") is not None
            else candidate.get("tracking_difference_1y")
        )
    if field_name == "tracking_difference_3y":
        return (
            _field_truth_resolved_value(candidate, "tracking_difference_3y")
            if _has_meaningful_value(_field_truth_resolved_value(candidate, "tracking_difference_3y"))
            else performance.get("tracking_difference_3y")
            if performance.get("tracking_difference_3y") is not None
            else factsheet_history.get("tracking_difference_3y")
            if factsheet_history.get("tracking_difference_3y") is not None
            else candidate.get("tracking_difference_3y")
        )
    if field_name == "holdings_count":
        return _field_truth_resolved_value(candidate, "holdings_count") or candidate.get("holdings_count")
    if field_name == "top_10_concentration":
        return _field_truth_resolved_value(candidate, "top_10_concentration") or candidate.get("top10_concentration_pct")
    if field_name == "us_weight":
        return _field_truth_resolved_value(candidate, "us_weight") or candidate.get("us_weight_pct")
    if field_name == "benchmark_key":
        return _field_truth_resolved_value(candidate, "benchmark_key") or benchmark_assignment.get("benchmark_key")
    if field_name == "benchmark_confidence":
        return _field_truth_resolved_value(candidate, "benchmark_confidence") or benchmark_assignment.get("benchmark_confidence")
    if field_name == "role_in_portfolio":
        return (
            _field_truth_resolved_value(candidate, "role_in_portfolio")
            or dict(candidate.get("investment_quality") or {}).get("role_in_portfolio")
            or dict(candidate.get("sleeve_expression") or {}).get("summary")
            or dict(candidate.get("eligibility") or {}).get("role_in_portfolio")
        )
    return _field_truth_resolved_value(candidate, field_name)


def _is_supplemental_gap_for_sleeve(*, sleeve_key: str, field_name: str) -> bool:
    return field_name in _SUPPLEMENTAL_DECISION_GAP_FIELDS.get(sleeve_key, set())


def _build_benchmark_support_status(candidate: dict[str, Any]) -> dict[str, Any]:
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    fit = str(benchmark.get("benchmark_fit_type") or "unknown")
    kind = str(benchmark.get("benchmark_kind") or "")
    direct = fit == "strong_fit" and kind != "proxy"
    acceptable_proxy = fit == "acceptable_proxy"
    weak_proxy = fit in {"weak_proxy", "mismatched", "unknown"} or not fit
    support_class = "direct" if direct else "acceptable_proxy" if acceptable_proxy else "weak_proxy" if benchmark else "unavailable"
    distortion_risk = "low" if direct else "moderate" if acceptable_proxy else "high"
    if direct:
        explanation = "Benchmark comparison is direct enough to support a fair sleeve-level comparison."
    elif acceptable_proxy:
        explanation = "Benchmark comparison is directionally useful, but part of the comparison still depends on proxy mapping."
    else:
        explanation = "Benchmark comparison is weak or proxy-heavy, so relative ranking should be treated as provisional."
    performance_claims_allowed = direct
    performance_claim_limit = (
        "full_relative_performance_claims_allowed"
        if direct
        else "soft_relative_language_only"
        if acceptable_proxy
        else "no_strong_relative_performance_claims"
    )
    return {
        "benchmark_type": kind or "unassigned",
        "support_class": support_class,
        "direct_benchmark_match": direct,
        "acceptable_proxy": acceptable_proxy,
        "weak_proxy": weak_proxy,
        "comparison_distortion_risk": distortion_risk,
        "practical_explanation": explanation,
        "performance_claims_allowed": performance_claims_allowed,
        "performance_claim_limit": performance_claim_limit,
        "proxy_type": str(benchmark.get("benchmark_source_type") or benchmark.get("benchmark_kind") or "") or None,
        "proxy_reason": None if direct else str(benchmark.get("methodology_notes") or benchmark.get("proxy_usage_explanation") or "") or None,
        "comparative_claim_boundary": (
            "Structural comparison only."
            if support_class in {"weak_proxy", "unavailable"}
            else "Relative comparison is usable with proxy caveats."
            if support_class == "acceptable_proxy"
            else "Direct benchmark support allows normal sleeve-relative comparison."
        ),
    }


def _build_decision_completeness_status(
    *,
    candidate: dict[str, Any],
    source_integrity: dict[str, Any],
    scoring_result: dict[str, Any],
    benchmark_support_status: dict[str, Any],
    current_holding_record: dict[str, Any],
    recommendation_result: dict[str, Any],
) -> dict[str, Any]:
    sleeve_key = str(candidate.get("sleeve_key") or "")
    blocking = list(source_integrity.get("blocking_issues") or [])
    material = list(source_integrity.get("material_issues") or [])
    critical_gaps: list[str] = []
    material_gaps: list[str] = []
    supplemental_gaps: list[str] = []
    fixable_gaps: list[str] = []
    expansion_gaps: list[str] = []
    for item, target in [(entry, "critical") for entry in blocking] + [(entry, "material") for entry in material]:
        field_name = str(item.get("field_name") or "").strip()
        reason = _normalize_gap_reason(item.get("reason"), field_name=field_name, label=item.get("label"))
        if not reason:
            continue
        if field_name and _has_meaningful_value(_candidate_field_fallback_value(candidate, field_name)):
            continue
        lowered = reason.lower()
        if "can likely be filled from current sources" in lowered or "parser gap" in lowered:
            fixable_gaps.append(reason)
        elif "source expansion" in lowered:
            expansion_gaps.append(reason)
        if field_name and _is_supplemental_gap_for_sleeve(sleeve_key=sleeve_key, field_name=field_name):
            supplemental_gaps.append(reason)
        elif target == "critical":
            critical_gaps.append(reason)
        else:
            material_gaps.append(reason)
    performance = dict(candidate.get("performance_metrics") or {})
    factsheet_summary = dict(candidate.get("factsheet_summary") or {})
    recommendation_context = dict(candidate.get("recommendation_context") or {})
    runner_up = dict(recommendation_context.get("challenger") or recommendation_context.get("nearest_challenger") or {})
    runner_up_complete = bool(runner_up.get("symbol") or recommendation_result.get("runner_up_symbol"))
    comparison_complete = bool(scoring_result.get("validity_flag")) and _benchmark_support_quality(candidate) != "missing"
    benchmark_quality = _benchmark_support_quality(candidate)
    bucket_states = {
        bucket_name: _bucket_state(candidate, bucket_name)
        for bucket_name in (
            "identity_wrapper",
            "holdings_exposure",
            "expense_and_cost",
            "liquidity_and_aum",
            "benchmark_support",
            "performance_relative_support",
            "tax_posture",
        )
    }
    holdings_bucket = dict(dict(candidate.get("evidence_buckets") or {}).get("holdings_exposure") or {})
    holdings_source_class = str(holdings_bucket.get("source_class") or "missing")
    holdings_quality = (
        "direct_current_holding"
        if str(current_holding_record.get("status") or "") in {"matched_to_current", "different_from_current"}
        else "missing_current_holding"
        if str(current_holding_record.get("status") or "") == "unavailable"
        else "not_applicable"
    )
    has_aum = any(
        _has_meaningful_value(value)
        for value in (
            performance.get("aum_usd_latest"),
            factsheet_summary.get("latest_aum_usd"),
            dict(candidate.get("aum_history_summary") or {}).get("latest_aum_usd"),
            _field_truth_resolved_value(candidate, "aum"),
            candidate.get("aum_usd"),
            candidate.get("aum"),
        )
    )
    has_liquidity = any(
        _has_meaningful_value(value)
        for value in (
            performance.get("volume_30d_avg"),
            performance.get("spread_bps_latest"),
            dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {}).get("liquidity_status"),
            dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {}).get("spread_status"),
            _field_truth_resolved_value(candidate, "volume_30d_avg"),
            _field_truth_resolved_value(candidate, "bid_ask_spread_proxy"),
        )
    )
    share_class_known = any(
        _has_meaningful_value(value)
        for value in (
            candidate.get("accumulation_or_distribution"),
            candidate.get("share_class"),
            _field_truth_resolved_value(candidate, "share_class"),
            _field_truth_resolved_value(candidate, "distribution_type"),
        )
    )
    has_performance_support = _has_material_performance_support(candidate)
    exposure_dependent = sleeve_key in {
        "global_equity_core",
        "developed_ex_us_optional",
        "emerging_markets",
        "china_satellite",
        "real_assets",
    }
    strategy_structure_driven = sleeve_key in {"alternatives", "convex"}
    factsheet_row = _field_value(candidate, "expense_ratio")
    factsheet_support_weak = str(factsheet_row.get("completeness_state") or "").lower() in {"weak_or_partial", "incomplete"} or not str(candidate.get("factsheet_asof") or "").strip()
    if exposure_dependent and bucket_states["holdings_exposure"] in {"missing", "proxy_only"}:
        critical_gaps.append("Holdings and exposure support is not strong enough for exposure-complete structural conclusions.")
    elif exposure_dependent and holdings_source_class == "factsheet_summary":
        material_gaps.append("Exposure support relies on issuer factsheet summary rather than direct holdings, so structural conclusions must stay softened.")
    elif exposure_dependent and bucket_states["holdings_exposure"] == "partial":
        material_gaps.append("Holdings and exposure support is only partial, so structural conclusions must stay softened.")
    elif strategy_structure_driven and bucket_states["holdings_exposure"] in {"missing", "proxy_only"}:
        supplemental_gaps.append("Line-item holdings are limited, but strategy structure and mandate remain the more relevant evidence path for this sleeve.")
    if not has_aum:
        material_gaps.append("AUM support is still missing.")
    if not has_liquidity or bucket_states["liquidity_and_aum"] in {"missing", "proxy_only"}:
        material_gaps.append("Liquidity evidence is still incomplete.")
    if not runner_up_complete:
        material_gaps.append("Nearest runner-up comparison is still incomplete.")
    if bucket_states["benchmark_support"] in {"proxy_only", "missing"} or benchmark_quality in {"weak_proxy", "missing"}:
        material_gaps.append("Benchmark support is still proxy-dependent or incomplete.")
    elif bucket_states["benchmark_support"] == "partial":
        supplemental_gaps.append("Benchmark support is usable only with explicit proxy caveats.")
    if not share_class_known:
        material_gaps.append("Share class details are still incomplete.")
    if factsheet_support_weak:
        material_gaps.append("Official factsheet support is stale or incomplete, so wrapper and implementation claims stay constrained.")
    if not has_performance_support and sleeve_key in {
        "global_equity_core",
        "developed_ex_us_optional",
        "emerging_markets",
        "china_satellite",
        "ig_bonds",
        "cash_bills",
    }:
        material_gaps.append("Performance-relative support is still incomplete.")
    elif not has_performance_support:
        supplemental_gaps.append("Performance-relative support is still incomplete.")
    if bucket_states["tax_posture"] in {"missing", "partial", "proxy_only"}:
        supplemental_gaps.append("Tax posture remains partially modeled, so tax edge language must stay conditional.")
    if critical_gaps or benchmark_quality == "missing":
        grade = "INCOMPLETE"
    elif (
        not has_aum
        or not has_liquidity
        or not runner_up_complete
        or benchmark_quality == "weak_proxy"
        or (
            not has_performance_support
            and sleeve_key in {
                "global_equity_core",
                "developed_ex_us_optional",
                "emerging_markets",
                "china_satellite",
                "ig_bonds",
                "cash_bills",
            }
        )
        or not share_class_known
        or (not strategy_structure_driven and bucket_states["holdings_exposure"] in {"partial", "proxy_only"})
        or factsheet_support_weak
    ):
        grade = "PARTIAL"
    else:
        grade = "SUFFICIENT"
    watchlist_eligible = grade in {"PARTIAL", "SUFFICIENT"} and not critical_gaps and benchmark_quality != "missing"
    claim_strength_boundary = (
        "review_only"
        if critical_gaps
        else "structural_only"
        if bucket_states["benchmark_support"] in {"proxy_only", "missing"} or ((bucket_states["holdings_exposure"] in {"partial", "proxy_only"} or holdings_source_class == "factsheet_summary") and not strategy_structure_driven)
        else "implementation_limited"
        if bucket_states["liquidity_and_aum"] in {"missing", "proxy_only"} or factsheet_support_weak
        else "full_structural_and_implementation"
    )
    return {
        "critical_gaps": list(dict.fromkeys(critical_gaps))[:8],
        "material_gaps": list(dict.fromkeys(material_gaps))[:10],
        "supplemental_gaps": list(dict.fromkeys(supplemental_gaps))[:10],
        "missing_but_fetchable": list(dict.fromkeys(fixable_gaps))[:8],
        "missing_requires_source_expansion": list(dict.fromkeys(expansion_gaps))[:8],
        "comparison_complete": comparison_complete,
        "runner_up_comparison_complete": runner_up_complete,
        "benchmark_support_quality": benchmark_quality,
        "holdings_truth_quality": holdings_quality,
        "data_completeness_grade": grade,
        "watchlist_eligible": watchlist_eligible,
        "claim_strength_boundary": claim_strength_boundary,
        "bucket_states": bucket_states,
    }


def _build_portfolio_completeness_status(
    *,
    candidate: dict[str, Any],
    current_holding_record: dict[str, Any],
) -> dict[str, Any]:
    portfolio_state_status = str(current_holding_record.get("portfolio_state_status") or "portfolio_state_missing")
    current_holding_known = bool(current_holding_record.get("current_symbol"))
    replacement_path_known = str(current_holding_record.get("status") or "") in {"matched_to_current", "different_from_current"}
    switch = dict(current_holding_record.get("switching_friction") or {})
    switch_cost_estimated = bool(switch.get("estimated_total_bps")) or str(current_holding_record.get("status") or "") in {"matched_to_current", "not_applicable"}
    overlap_assessed = bool(candidate.get("portfolio_overlap")) or bool(candidate.get("recommendation_diff"))
    role_defined = bool(
        dict(candidate.get("investment_quality") or {}).get("role_in_portfolio")
        or dict(candidate.get("sleeve_expression") or {}).get("summary")
        or dict(candidate.get("investment_quality") or {}).get("fit_for_sleeve")
    )
    incremental_benefit_defined = bool(dict(candidate.get("investor_consequence_summary") or {}).get("implementation_quality_effect"))
    downside_tradeoff_defined = bool(
        list(dict(candidate.get("investment_quality") or {}).get("main_limitations") or [])
        or list(dict(candidate.get("investment_quality") or {}).get("key_risks") or [])
        or dict(candidate.get("investor_consequence_summary") or {}).get("investment_trust_effect")
    )
    mapped_sleeve = str(current_holding_record.get("sleeve_key") or "")
    if portfolio_state_status == "portfolio_state_missing":
        grade = "INCOMPLETE"
    elif mapped_sleeve and str(current_holding_record.get("status") or "") == "unavailable":
        grade = "INCOMPLETE"
    elif all(
        (
            current_holding_known or str(current_holding_record.get("status") or "") == "not_applicable",
            replacement_path_known or str(current_holding_record.get("status") or "") == "not_applicable",
            switch_cost_estimated,
            overlap_assessed,
            role_defined,
            incremental_benefit_defined,
            downside_tradeoff_defined,
        )
    ):
        grade = "SUFFICIENT"
    elif any((current_holding_known, replacement_path_known, role_defined, incremental_benefit_defined)):
        grade = "PARTIAL"
    else:
        grade = "INCOMPLETE"
    return {
        "portfolio_state_status": portfolio_state_status,
        "current_holding_known": current_holding_known,
        "replacement_path_known": replacement_path_known,
        "switch_cost_estimated": switch_cost_estimated,
        "overlap_assessed": overlap_assessed,
        "role_defined": role_defined,
        "incremental_benefit_defined": incremental_benefit_defined,
        "downside_tradeoff_defined": downside_tradeoff_defined,
        "completeness_grade": grade,
    }


def _build_forecast_defensibility_status(
    *,
    candidate: dict[str, Any],
    scoring_result: dict[str, Any],
    source_integrity: dict[str, Any],
    benchmark_support_status: dict[str, Any],
    forecast_visual_model: dict[str, Any],
) -> dict[str, Any]:
    methodology_explainable = bool(scoring_result.get("validity_flag")) and bool(forecast_visual_model.get("forecast_validity_summary"))
    unit_plain_english_clear = bool(forecast_visual_model.get("current_anchor_label"))
    horizon_clear = bool(forecast_visual_model.get("forecast_horizon"))
    hurdle_meaning_clear = False
    support_class = str(benchmark_support_status.get("support_class") or "unavailable")
    proxy_dependence_level = "high" if support_class in {"weak_proxy", "unavailable"} else "moderate" if support_class == "acceptable_proxy" else "low"
    caution = str(source_integrity.get("overall_caution_level") or "moderate")
    if not methodology_explainable or support_class == "unavailable":
        display_grade = "HIDE"
    elif not unit_plain_english_clear or not horizon_clear or support_class == "weak_proxy":
        display_grade = "ADVANCED_ONLY"
    elif caution in {"critical", "material"} or proxy_dependence_level == "high":
        display_grade = "SOFT_SCENARIO_ONLY"
    elif proxy_dependence_level == "moderate":
        display_grade = "ADVANCED_ONLY"
    else:
        display_grade = "FULLY_DISPLAYABLE"
    return {
        "methodology_explainable": methodology_explainable,
        "unit_plain_english_clear": unit_plain_english_clear,
        "horizon_clear": horizon_clear,
        "hurdle_meaning_clear": hurdle_meaning_clear,
        "proxy_dependence_level": proxy_dependence_level,
        "display_grade": display_grade,
        "defensibility_status": (
            "not_defensible"
            if display_grade == "HIDE"
            else "advanced_only"
            if display_grade == "ADVANCED_ONLY"
            else "soft_scenario_only"
            if display_grade == "SOFT_SCENARIO_ONLY"
            else "displayable"
        ),
        "main_path_allowed": display_grade in {"SOFT_SCENARIO_ONLY", "FULLY_DISPLAYABLE"},
    }


def _build_tax_assumption_status(candidate: dict[str, Any]) -> dict[str, Any]:
    tax_truth = dict(candidate.get("tax_truth") or {})
    tax_mechanics = dict(dict(candidate.get("investment_lens") or {}).get("tax_mechanics") or {})
    investor_tax_assumption_present = bool(candidate.get("sg_lens") or tax_mechanics.get("withholding_tax_exposure_note"))
    account_assumption_present = bool(candidate.get("instrument_type"))
    domicile_assumption_present = bool(candidate.get("domicile"))
    withholding_assumption_present = candidate.get("expected_withholding_drag_estimate") is not None or bool(tax_mechanics.get("withholding_tax_exposure_note"))
    estate_exposure_considered = "us_situs_estate_risk_flag" in candidate
    distribution_treatment_considered = bool(candidate.get("accumulation_or_distribution") or tax_mechanics.get("distribution_mechanics_note"))
    completeness_count = sum(
        1
        for flag in (
            investor_tax_assumption_present,
            account_assumption_present,
            domicile_assumption_present,
            withholding_assumption_present,
            estate_exposure_considered,
            distribution_treatment_considered,
        )
        if flag
    )
    tax_confidence = str(tax_truth.get("tax_confidence") or ("high" if completeness_count >= 5 else "medium" if completeness_count >= 3 else "low"))
    assumption_grade = "SUFFICIENT" if completeness_count >= 5 and tax_confidence == "high" else "PARTIAL" if completeness_count >= 3 else "INCOMPLETE"
    advisory_boundary = str(tax_truth.get("advisory_boundary") or ("requires_case_specific_review" if tax_confidence == "low" else "acceptable_but_structurally_inferior"))
    return {
        "investor_tax_assumption_present": investor_tax_assumption_present,
        "account_assumption_present": account_assumption_present,
        "domicile_assumption_present": domicile_assumption_present,
        "withholding_assumption_present": withholding_assumption_present,
        "estate_exposure_considered": estate_exposure_considered,
        "distribution_treatment_considered": distribution_treatment_considered,
        "assumption_completeness_grade": assumption_grade,
        "tax_score": tax_truth.get("tax_score"),
        "tax_confidence": tax_confidence,
        "policy_buckets": dict(tax_truth.get("policy_buckets") or {}),
        "advisory_boundary": advisory_boundary,
        "evidence_strength": str(tax_truth.get("evidence_strength") or ""),
        "decisive_tax_use_allowed": assumption_grade == "SUFFICIENT" and tax_confidence == "high",
    }


def _recommendation_confidence_label(
    *,
    source_integrity: dict[str, Any],
    decision_completeness_status: dict[str, Any],
    portfolio_completeness_status: dict[str, Any],
    recommendation_result: dict[str, Any],
    benchmark_support_status: dict[str, Any],
    tax_assumption_status: dict[str, Any],
) -> str:
    caution = str(source_integrity.get("overall_caution_level") or "moderate")
    lead_strength = str(recommendation_result.get("lead_strength") or "")
    if caution in {"critical", "material"}:
        return "low"
    decision_grade = str(decision_completeness_status.get("data_completeness_grade") or "")
    portfolio_grade = str(portfolio_completeness_status.get("completeness_grade") or "")
    if decision_grade == "INCOMPLETE":
        return "low"
    if str(benchmark_support_status.get("support_class") or "") in {"weak_proxy", "unavailable"}:
        return "low" if decision_grade != "SUFFICIENT" else "moderate"
    if str(tax_assumption_status.get("tax_confidence") or "") == "low":
        return "moderate" if decision_grade == "SUFFICIENT" else "low"
    if decision_grade == "PARTIAL" and bool(decision_completeness_status.get("watchlist_eligible")):
        return "moderate"
    if portfolio_grade != "SUFFICIENT":
        return "moderate"
    if lead_strength in {"robust", "watch_stable"}:
        return "high"
    return "moderate"


def _verbs_for_investor_status(status: str) -> list[str]:
    if status == "ACTIONABLE_RECOMMENDATION":
        return ["review", "compare", "add", "replace", "switch"]
    if status == "DECISION_READY":
        return ["review", "compare"]
    if status == "WATCHLIST_CANDIDATE":
        return ["monitor", "review", "compare", "watch"]
    if status == "RESEARCH_CANDIDATE":
        return ["research", "monitor", "review"]
    return []


def _build_investor_recommendation_status(
    *,
    candidate: dict[str, Any],
    recommendation_result: dict[str, Any],
    decision_completeness_status: dict[str, Any],
    portfolio_completeness_status: dict[str, Any],
    benchmark_support_status: dict[str, Any],
    forecast_defensibility_status: dict[str, Any],
    tax_assumption_status: dict[str, Any],
    source_integrity: dict[str, Any],
    current_holding_record: dict[str, Any],
) -> dict[str, Any]:
    blocked_reasons: list[str] = []
    policy_blockers: list[str] = []
    evidence_blockers: list[str] = []
    portfolio_blockers: list[str] = []
    candidate_status = str(recommendation_result.get("candidate_status") or "")
    if candidate_status == "blocked_by_policy":
        status = "DO_NOT_USE"
        readiness_grade = "NOT_READY"
        policy_blockers.append("Decisive policy or structural blockers still disqualify this candidate.")
    else:
        if list(decision_completeness_status.get("critical_gaps") or []):
            evidence_blockers.append("Critical evidence gaps still prevent stronger investor-facing recommendation language.")
        if str(portfolio_completeness_status.get("completeness_grade") or "") == "INCOMPLETE":
            portfolio_blockers.append("Current holding or portfolio-action context is still incomplete.")
        if str(portfolio_completeness_status.get("portfolio_state_status") or "") == "portfolio_state_missing":
            portfolio_blockers.append("No active portfolio-state snapshot is available, so action language must stay watchlist-level.")
        if not bool(decision_completeness_status.get("runner_up_comparison_complete")):
            evidence_blockers.append("Nearest runner-up comparison is still incomplete, so the lead remains provisional.")
        if str(decision_completeness_status.get("claim_strength_boundary") or "") == "review_only":
            evidence_blockers.append("Truth-quality gaps still block stronger structural or implementation conclusions.")
        elif str(decision_completeness_status.get("claim_strength_boundary") or "") == "structural_only":
            evidence_blockers.append("Current evidence supports only softened structural comparison, not stronger performance or implementation claims.")
        elif str(decision_completeness_status.get("claim_strength_boundary") or "") == "implementation_limited":
            evidence_blockers.append("Implementation evidence is incomplete, so stronger switch language remains constrained.")
        if bool(benchmark_support_status.get("weak_proxy")) or str(benchmark_support_status.get("support_class") or "") in {"weak_proxy", "unavailable"}:
            evidence_blockers.append("Benchmark support is still weak enough that relative-performance language must stay softened.")
        if not bool(tax_assumption_status.get("decisive_tax_use_allowed")) and any(
            "tax" in str(value).lower()
            for value in (
                recommendation_result.get("why_this_candidate"),
                dict(candidate.get("decision_thesis") or {}).get("key_allocation_reason"),
                " ".join(list(dict(candidate.get("investment_quality") or {}).get("better_than_peers") or [])),
            )
        ):
            evidence_blockers.append("Tax assumptions are still incomplete, so tax should remain a conditional note rather than a decisive edge.")
        if str(forecast_defensibility_status.get("display_grade") or "") in {"HIDE", "ADVANCED_ONLY"}:
            evidence_blockers.append("Forecast output is not strong enough to add decision authority on the main surface.")
        if str(current_holding_record.get("status") or "") == "unavailable":
            portfolio_blockers.append("Current holding is not recorded for this sleeve, so replacement language must stay constrained.")
        blocked_reasons = [*policy_blockers, *evidence_blockers, *portfolio_blockers]
        if (
            str(decision_completeness_status.get("data_completeness_grade") or "") == "SUFFICIENT"
            and str(portfolio_completeness_status.get("completeness_grade") or "") == "SUFFICIENT"
            and str(recommendation_result.get("recommendation_tier") or "") == "actionable"
            and str(benchmark_support_status.get("support_class") or "") == "direct"
            and not blocked_reasons
        ):
            status = "ACTIONABLE_RECOMMENDATION"
            readiness_grade = "ACTIONABLE_READY"
        elif (
            str(decision_completeness_status.get("data_completeness_grade") or "") == "SUFFICIENT"
            and str(portfolio_completeness_status.get("completeness_grade") or "") == "SUFFICIENT"
            and str(benchmark_support_status.get("support_class") or "") in {"direct", "acceptable_proxy"}
        ):
            status = "DECISION_READY"
            readiness_grade = "DECISION_READY"
        elif (
            bool(decision_completeness_status.get("watchlist_eligible"))
            and candidate_status not in {"blocked_by_missing_required_evidence", "blocked_by_unresolved_gate"}
        ):
            status = "WATCHLIST_CANDIDATE"
            readiness_grade = "WATCHLIST_READY"
        elif candidate_status in {"blocked_by_missing_required_evidence", "blocked_by_unresolved_gate"}:
            status = "NOT_DECISION_READY"
            readiness_grade = "NOT_READY"
        else:
            status = "RESEARCH_CANDIDATE"
            readiness_grade = "NOT_READY"
    rank = dict(candidate.get("investment_quality") or {}).get("rank_in_sleeve")
    if status == "ACTIONABLE_RECOMMENDATION":
        analytical_rank_softening = f"Decision rank #{rank}" if rank is not None else "Decision-ready rank"
    elif status == "DECISION_READY":
        analytical_rank_softening = f"Final comparison rank #{rank}" if rank is not None else "Final comparison ordering"
    elif status == "WATCHLIST_CANDIDATE" and rank == 1:
        analytical_rank_softening = "Current watchlist leader"
    elif rank == 1:
        analytical_rank_softening = "Provisional research leader"
    elif rank is not None:
        analytical_rank_softening = f"Provisional research order #{rank}"
    else:
        analytical_rank_softening = "Research ordering not established"
    explanation_boundary = (
        "Use for portfolio action only after implementation details still hold."
        if status == "ACTIONABLE_RECOMMENDATION"
        else "Use for final portfolio comparison, not immediate action."
        if status == "DECISION_READY"
        else "Keep on the watchlist and do not treat this as a switch decision yet."
        if status == "WATCHLIST_CANDIDATE"
        else "Keep in research and do not treat this as a portfolio action candidate yet."
    )
    return {
        "investor_status": status,
        "readiness_grade": readiness_grade,
        "blocked_reasons": blocked_reasons,
        "policy_blockers": policy_blockers,
        "evidence_blockers": evidence_blockers,
        "portfolio_blockers": portfolio_blockers,
        "allowed_action_verbs": _verbs_for_investor_status(status),
        "recommendation_confidence": _recommendation_confidence_label(
            source_integrity=source_integrity,
            decision_completeness_status=decision_completeness_status,
            portfolio_completeness_status=portfolio_completeness_status,
            recommendation_result=recommendation_result,
            benchmark_support_status=benchmark_support_status,
            tax_assumption_status=tax_assumption_status,
        ),
        "explanation_boundary": explanation_boundary,
        "analytical_rank_softening": analytical_rank_softening,
    }


def _source_summary_text(*, source_quality_flag: str, critical_missing: list[dict[str, Any]], freshness_state: str, proxy_fields: list[dict[str, Any]]) -> str:
    if source_quality_flag == "fresh":
        return "Core decision evidence is supported by direct current sources."
    if source_quality_flag == "slightly_stale":
        return "Core evidence is usable, but some source support is aging."
    if source_quality_flag == "materially_stale":
        return "Source support is materially stale, so recommendation authority must remain constrained."
    if source_quality_flag == "conflicting":
        return "Critical source inputs conflict, so the candidate requires verification before promotion."
    if source_quality_flag == "proxy_based":
        return "Some important evidence is still proxy-based rather than directly verified."
    if critical_missing:
        return "Critical source evidence is still missing, so the candidate is not fully recommendable."
    if freshness_state == "unknown" and proxy_fields:
        return "Evidence remains usable only with supporting estimates and incomplete freshness confirmation."
    return "Evidence support remains incomplete."


def _build_gate_result(*, candidate: dict[str, Any], source_integrity: dict[str, Any]) -> dict[str, Any]:
    decision = dict(candidate.get("decision_record") or {})
    policy_gates = dict(decision.get("policy_gates") or {})
    existing_gates = {
        str(gate.get("gate_name") or ""): dict(gate)
        for gate in list(policy_gates.get("gates") or [])
        if str(gate.get("gate_name") or "").strip()
    }
    universal_review = dict(candidate.get("decision_readiness") or {})
    recommendation_context = dict(candidate.get("recommendation_context") or {})
    candidate_status = str(dict(candidate.get("investment_quality") or {}).get("user_facing_state") or "")
    quality = dict(candidate.get("investment_quality") or {})

    def gate_entry(code: str, name: str, status: str, reason_text: str, decisive: bool, evidence_refs: list[str], reopen_conditions: list[str]) -> dict[str, Any]:
        return {
            "gate_code": code,
            "gate_name": name,
            "status": status,
            "decisive": decisive,
            "caution_level": _caution_from_gate_status(status),
            "evidence_refs": evidence_refs,
            "reason_text": reason_text,
            "reopen_conditions": reopen_conditions,
        }

    gates: list[dict[str, Any]] = []
    for legacy_name, (code, canonical_name) in _GATE_CODE_MAP.items():
        gate = existing_gates.get(legacy_name, {})
        status = str(gate.get("state") or "not_evaluated")
        reason = str(gate.get("reason") or f"{canonical_name.replace('_', ' ')} remains unresolved.")
        gates.append(
            gate_entry(
                code,
                canonical_name,
                status,
                reason,
                canonical_name not in {"recommendation_plausibility"},
                list(gate.get("missing_inputs") or []),
                list(gate.get("missing_inputs") or []),
            )
        )

    exposure_integrity_status = "pass"
    if list(source_integrity.get("blocking_issues") or []):
        exposure_integrity_status = "fail"
    elif list(source_integrity.get("material_issues") or []):
        exposure_integrity_status = "partial"
    gates.append(
        gate_entry(
            "G3",
            "exposure_integrity",
            exposure_integrity_status,
            "Holdings and exposure evidence is sufficiently clear." if exposure_integrity_status == "pass" else "Exposure evidence is incomplete or still conflicted.",
            True,
            [str(item.get("field_name") or "") for item in list(source_integrity.get("blocking_issues") or [])[:5]],
            [str(item.get("field_name") or "") for item in list(source_integrity.get("material_issues") or [])[:5]],
        )
    )

    cost_status = "pass"
    cost_score = quality.get("cost_score")
    if cost_score is None:
        cost_status = "not_evaluated"
    elif float(cost_score) < 35:
        cost_status = "partial"
    gates.append(
        gate_entry(
            "G5",
            "cost_acceptability",
            cost_status,
            "Cost remains acceptable for the sleeve role." if cost_status == "pass" else "Cost weakens the case relative to peers or is not fully observed.",
            False,
            ["investment_quality.cost_score"],
            ["official fee disclosure"],
        )
    )

    portfolio_usefulness_status = "pass"
    if candidate_status in {"blocked_by_policy", "blocked_by_missing_required_evidence", "blocked_by_unresolved_gate"}:
        portfolio_usefulness_status = "partial"
    elif str(recommendation_context.get("stability") or "") in {"fragile", "unstable"}:
        portfolio_usefulness_status = "partial"
    gates.append(
        gate_entry(
            "G9",
            "portfolio_usefulness",
            portfolio_usefulness_status,
            "Candidate improves the sleeve role relative to available alternatives." if portfolio_usefulness_status == "pass" else "Candidate usefulness is still conditional on unresolved issues or a fragile lead.",
            False,
            ["recommendation_context.stability", "decision_record.final_decision_state"],
            list(universal_review.get("what_must_change") or [])[:4],
        )
    )

    all_statuses = [str(item.get("status") or "not_evaluated") for item in gates]
    overall_status = "pass"
    if "fail" in all_statuses:
        overall_status = "fail"
    elif "not_evaluated" in all_statuses:
        overall_status = "not_evaluated"
    elif "partial" in all_statuses:
        overall_status = "partial"
    return {
        "overall_status": overall_status,
        "gates": gates,
        "decisive_failures": [item for item in gates if item["decisive"] and item["status"] == "fail"],
        "reopen_conditions": list(dict.fromkeys(condition for item in gates for condition in list(item.get("reopen_conditions") or [])))[:10],
    }


def _caution_from_gate_status(status: str) -> str:
    if status == "fail":
        return "critical"
    if status == "partial":
        return "material"
    if status == "not_evaluated":
        return "moderate"
    return "minor"


def _build_review_intensity_decision(*, candidate: dict[str, Any], sleeve_key: str) -> dict[str, Any]:
    triggers: list[str] = []
    instrument_type = str(candidate.get("instrument_type") or "")
    pressures = list(dict(candidate.get("eligibility") or {}).get("pressures") or [])
    quality = dict(candidate.get("investment_quality") or {})
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    if sleeve_key in {"alternatives", "convex"}:
        triggers.append("sleeve requires deep review because scenario role matters more than simple peer ranking")
    if instrument_type not in {"etf_ucits", "etf_us", "t_bill_sg", "money_market_fund_sg", "cash_account_sg"}:
        triggers.append("structure is non-standard for a simple passive sleeve")
    if any(str(item.get("severity") or "") == "critical" for item in pressures):
        triggers.append("material caution remains while the candidate is still in active consideration")
    if str(benchmark.get("benchmark_fit_type") or "") in {"acceptable_proxy", "weak_proxy", "mismatched"}:
        triggers.append("benchmark support is proxy-based or structurally weak")
    if str(quality.get("recommendation_state") or "") == "recommended_primary" and any(str(item.get("severity") or "") in {"critical", "important"} for item in pressures):
        triggers.append("current winner status still carries meaningful caution")
    level = "level_2_deep" if triggers else "level_1_universal"
    return {
        "review_intensity": level,
        "triggers": triggers[:6],
        "summary": "Deep review is required." if level == "level_2_deep" else "Universal review is sufficient for now.",
    }


def _build_universal_review_result(*, candidate: dict[str, Any], sleeve_key: str) -> dict[str, Any]:
    eligibility = dict(candidate.get("eligibility") or {})
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    liquidity = dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {})
    return {
        "structural_soundness": {
            "status": "pass" if not list(eligibility.get("eligibility_blockers") or []) else "conditional",
            "summary": str(dict(candidate.get("decision_record") or {}).get("explanations", {}).get("policy_gates") or "Structural review completed."),
        },
        "portfolio_overlap": {
            "status": "not_available",
            "summary": "Current holding overlap is not available in this evaluation run.",
        },
        "tax_and_implementation_handling": {
            "status": "pass" if (dict(candidate.get("sg_lens") or {}).get("score") is not None) else "conditional",
            "summary": "Tax and implementation support is present." if (dict(candidate.get("sg_lens") or {}).get("score") is not None) else "Tax or implementation support is still incomplete.",
        },
        "benchmark_suitability": {
            "status": "pass" if str(benchmark.get("benchmark_fit_type") or "") == "strong_fit" else "conditional",
            "summary": str(benchmark.get("benchmark_explanation") or benchmark.get("benchmark_truth_summary") or "Benchmark suitability reviewed."),
        },
        "fragility_and_hidden_risk": {
            "status": "pass" if not bool(candidate.get("leverage_used")) and candidate.get("max_loss_known") is not False else "conditional",
            "summary": "No hidden fragility flags dominate." if not bool(candidate.get("leverage_used")) and candidate.get("max_loss_known") is not False else "Fragility or bounded-loss support still needs caution.",
        },
        "role_fit_and_substitution_quality": {
            "status": "pass",
            "summary": str(dict(candidate.get("sleeve_expression") or {}).get("summary") or f"{sleeve_key} role reviewed."),
        },
        "liquidity_note": {
            "status": str(liquidity.get("liquidity_status") or "unknown"),
            "summary": str(liquidity.get("explanation") or "Liquidity reviewed."),
        },
    }


def _build_deep_review_result(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    review_intensity: dict[str, Any],
    universal_review: dict[str, Any],
) -> dict[str, Any] | None:
    if str(review_intensity.get("review_intensity") or "") != "level_2_deep":
        return None
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    pressures = list(dict(candidate.get("eligibility") or {}).get("pressures") or [])
    return {
        "status": "completed",
        "escalation_reasons": list(review_intensity.get("triggers") or []),
        "recommendation_relevant_findings": [
            str(dict(universal_review.get("benchmark_suitability") or {}).get("summary") or ""),
            *[str(item.get("detail") or item.get("label") or "") for item in pressures[:3]],
        ],
        "implications_for_scoring": "Deep review mainly affects confidence and recommendation authority for this sleeve.",
        "implications_for_memo_generation": "Memo should emphasize tradeoffs, scenario role, and non-standard implementation burden.",
        "sleeve_key": sleeve_key,
        "benchmark_context": {
            "fit_type": benchmark.get("benchmark_fit_type"),
            "authority_level": benchmark.get("benchmark_authority_level"),
        },
    }


def _average(values: list[float | None]) -> float | None:
    valid = [float(value) for value in values if value is not None]
    if not valid:
        return None
    return round(sum(valid) / float(len(valid)), 2)


def _build_scoring_result(*, candidate: dict[str, Any], sleeve_candidates: list[dict[str, Any]], baseline_reference: dict[str, Any]) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    honesty = dict(candidate.get("score_honesty") or {})
    section_a = _average([
        quality.get("cost_score"),
        quality.get("liquidity_score"),
        quality.get("structure_score"),
        quality.get("tax_score"),
    ])
    section_b = _average([
        quality.get("performance_score"),
        quality.get("risk_adjusted_score"),
        quality.get("governance_confidence_score"),
    ])
    section_c = _average([
        quality.get("sg_rank"),
    ])
    baseline_gap = None
    baseline_symbol = str(baseline_reference.get("baseline_symbol") or "")
    baseline_candidate = next(
        (item for item in sleeve_candidates if str(item.get("symbol") or "").upper() == baseline_symbol),
        None,
    )
    if baseline_candidate is not None and baseline_candidate is not candidate:
        baseline_score = dict(baseline_candidate.get("investment_quality") or {}).get("composite_score")
        if baseline_score is not None and quality.get("composite_score") is not None:
            baseline_gap = round(float(quality.get("composite_score") or 0.0) - float(baseline_score or 0.0), 2)
    return {
        "section_scores": {
            "common_candidate_quality": section_a,
            "sleeve_specific_fitness": section_b,
            "portfolio_impact": section_c,
        },
        "penalties": {
            "fees": quality.get("cost_penalty"),
            "spread": quality.get("spread_penalty"),
            "tax_drag": quality.get("tax_penalty"),
            "fragility": quality.get("fragility_penalty"),
            "complexity": quality.get("complexity_penalty"),
            "evidence_weakness": quality.get("evidence_penalty"),
        },
        "forecast_adjustment": quality.get("forecast_adjustment"),
        "final_score": quality.get("composite_score"),
        "validity_flag": bool(quality.get("composite_score_valid", True)),
        "caution_level": _caution_from_score(candidate),
        "confidence_or_comparability_notes": {
            "comparability": honesty.get("comparability"),
            "evidence_coverage_band": honesty.get("evidence_coverage_band"),
            "unknown_share_band": honesty.get("unknown_share_band"),
            "unknown_dimensions": honesty.get("unknown_dimensions"),
        },
        "score_explanation_summary": str(quality.get("score_explanation_summary") or honesty.get("composite_score_display") or ""),
        "baseline_gap": baseline_gap,
        "baseline_symbol": baseline_symbol or None,
    }


def _caution_from_score(candidate: dict[str, Any]) -> str:
    quality = dict(candidate.get("investment_quality") or {})
    if not bool(quality.get("composite_score_valid", True)):
        return "critical"
    band = str(dict(candidate.get("score_honesty") or {}).get("unknown_share_band") or "unknown")
    if band in {"high", "unknown"}:
        return "material"
    if band == "moderate":
        return "moderate"
    return "minor"


def _build_current_holding_record(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    sleeve_candidates: list[dict[str, Any]],
    current_holdings: list[dict[str, Any]],
) -> dict[str, Any]:
    portfolio_state = build_portfolio_state_context(current_holdings)
    mapped_sleeve = _CURRENT_HOLDING_SLEEVE_MAP.get(sleeve_key)
    if not mapped_sleeve:
        return {
            "status": "not_applicable",
            "portfolio_state_status": str(portfolio_state.get("coverage_state") or "portfolio_state_missing"),
            "current_holding_match_status": "not_applicable",
            "sleeve_key": sleeve_key,
            "current_symbol": None,
            "comparison_summary": "Current holding comparison is not applicable for this sleeve mapping.",
            "switching_friction": {
                "status": "not_applicable",
                "estimated_total_bps": None,
                "spread_cost_bps": None,
                "tax_cost_bps": None,
                "operational_friction": None,
                "summary": "Switching friction is not applicable for this sleeve mapping.",
            },
            "candidate_symbol": str(candidate.get("symbol") or ""),
        }

    sleeve_holdings = [
        dict(item)
        for item in current_holdings
        if str(item.get("sleeve") or "") == mapped_sleeve
    ]
    if not sleeve_holdings:
        portfolio_state_status = str(portfolio_state.get("coverage_state") or "portfolio_state_missing")
        return {
            "status": "unavailable",
            "portfolio_state_status": portfolio_state_status,
            "current_holding_match_status": "sleeve_unmapped_or_missing"
            if portfolio_state_status in {"direct_portfolio_state", "partial_portfolio_state"}
            else "portfolio_state_missing",
            "sleeve_key": sleeve_key,
            "current_symbol": None,
            "comparison_summary": "No current holding is recorded for this sleeve in the active portfolio state.",
            "switching_friction": {
                "status": "not_available",
                "estimated_total_bps": None,
                "spread_cost_bps": None,
                "tax_cost_bps": None,
                "operational_friction": "Current holding is not recorded, so switching friction cannot be estimated.",
                "summary": "Switching friction is not yet fully clear because the current holding is missing.",
            },
            "candidate_symbol": str(candidate.get("symbol") or ""),
        }

    candidate_symbol = str(candidate.get("symbol") or "").upper()
    matched = next((item for item in sleeve_holdings if str(item.get("symbol") or "").upper() == candidate_symbol), None)
    selected = matched or max(
        sleeve_holdings,
        key=lambda item: float(item.get("estimated_value") or (float(item.get("quantity") or 0.0) * float(item.get("cost_basis") or 0.0))),
    )
    current_symbol = str(selected.get("symbol") or "").upper()
    current_candidate = next(
        (item for item in sleeve_candidates if str(item.get("symbol") or "").upper() == current_symbol),
        None,
    )

    comparison_summary = (
        "Candidate matches the current sleeve holding."
        if matched
        else f"Current sleeve holding is {current_symbol}; replacement comparison uses the active holding as the practical baseline."
    )
    switching_friction: dict[str, Any] = {
        "status": "not_available",
        "estimated_total_bps": None,
        "spread_cost_bps": None,
        "tax_cost_bps": None,
        "operational_friction": "Switching cost cannot be estimated cleanly yet.",
        "summary": "Switching friction is not available because the active holding is not matched to a comparable Blueprint candidate.",
    }
    practical_edge = {
        "status": "unavailable",
        "net_score_gap": None,
        "minimum_required_gap": _REPLACEMENT_EDGE_THRESHOLDS.get(sleeve_key, 8.0),
        "reason": "Current holding comparison lacks a matched Blueprint candidate, so practical edge cannot be confirmed.",
    }
    if current_candidate is not None:
        frictions = _estimate_switching_frictions(current=current_candidate, preferred=candidate)
        switching_friction = {
            "status": str(frictions.get("transaction_confidence") or "not_available"),
            "estimated_total_bps": frictions.get("switching_cost_bps"),
            "spread_cost_bps": frictions.get("switching_cost_bps"),
            "tax_cost_bps": round(abs(float(current_candidate.get("expected_withholding_drag_estimate") or 0.0) - float(candidate.get("expected_withholding_drag_estimate") or 0.0)) * 10000.0, 2),
            "operational_friction": str(frictions.get("implementation_uncertainty") or ""),
            "summary": (
                f"Estimated switching friction is about {frictions.get('switching_cost_bps')} bps, "
                f"with {str(frictions.get('implementation_uncertainty') or 'contained').replace('_', ' ')} implementation uncertainty."
            ),
        }
        candidate_score = dict(candidate.get("investment_quality") or {}).get("composite_score")
        current_score = dict(current_candidate.get("investment_quality") or {}).get("composite_score")
        if candidate_score is not None and current_score is not None:
            net_gap = round(float(candidate_score) - float(current_score) - float(frictions.get("total_penalty_points") or 0.0), 2)
            min_gap = _REPLACEMENT_EDGE_THRESHOLDS.get(sleeve_key, 8.0)
            practical_edge = {
                "status": "sufficient" if net_gap >= min_gap else "marginal",
                "net_score_gap": net_gap,
                "minimum_required_gap": min_gap,
                "reason": (
                    "Candidate clears the practical replacement edge after estimated friction."
                    if net_gap >= min_gap
                    else "Candidate does not yet clear the minimum practical edge after friction."
                ),
            }
    return {
        "status": "matched_to_current" if matched else "different_from_current",
        "portfolio_state_status": str(portfolio_state.get("coverage_state") or "portfolio_state_missing"),
        "current_holding_match_status": "exact_match" if matched else "replacement_candidate",
        "sleeve_key": sleeve_key,
        "current_symbol": current_symbol or None,
        "current_name": selected.get("name"),
        "comparison_summary": comparison_summary,
        "switching_friction": switching_friction,
        "candidate_symbol": candidate.get("symbol"),
        "practical_edge": practical_edge,
        "holding_count_in_sleeve": len(sleeve_holdings),
    }


def _build_baseline_reference(*, sleeve_key: str, sleeve_candidates: list[dict[str, Any]]) -> dict[str, Any]:
    preferred_symbol = _BASELINE_SYMBOLS.get(sleeve_key)
    baseline = next(
        (item for item in sleeve_candidates if str(item.get("symbol") or "").upper() == str(preferred_symbol or "").upper()),
        sleeve_candidates[0] if sleeve_candidates else None,
    )
    if baseline is None:
        return {"baseline_symbol": None, "baseline_reason": "No baseline candidate is configured for the sleeve."}
    return {
        "baseline_symbol": baseline.get("symbol"),
        "baseline_name": baseline.get("name"),
        "baseline_reason": f"{baseline.get('symbol')} is the simplest acceptable baseline for {sleeve_key}.",
        "baseline_recommendation_state": dict(baseline.get("investment_quality") or {}).get("recommendation_state"),
    }


def _build_recommendation_result(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    winner_candidate: dict[str, Any] | None,
    baseline_reference: dict[str, Any],
    current_holding_record: dict[str, Any],
    source_integrity: dict[str, Any],
    gate_result: dict[str, Any],
    scoring_result: dict[str, Any],
    conn: Any | None = None,
    approval_required: bool = False,
) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    decision = dict(candidate.get("decision_record") or {})
    recommendation_context = dict(candidate.get("recommendation_context") or {})
    candidate_status = str(quality.get("user_facing_state") or "research_ready_but_not_recommendable")
    recommendation_state = str(quality.get("recommendation_state") or "research_only")
    candidate_symbol = str(candidate.get("symbol") or "")
    winner_symbol = str((winner_candidate or {}).get("symbol") or "")
    practical_edge = dict(current_holding_record.get("practical_edge") or {})
    practical_edge_status = str(practical_edge.get("status") or "unavailable")
    # Evaluate challenger promotion gate via policy authority
    _incumbent_score = float(dict(current_holding_record.get("incumbent_quality") or {}).get("composite_score") or 0.0)
    _challenger_score = float(quality.get("composite_score") or 0.0)
    _incumbent_exists = bool(str(current_holding_record.get("status") or "") == "different_from_current")
    _practical_edge_wins = practical_edge_status == "sufficient"
    _challenger_promotion = evaluate_challenger_promotion(
        incumbent_score=_incumbent_score,
        challenger_score=_challenger_score,
        challenger_dimensions={
            "practical_edge": {"challenger_wins": _practical_edge_wins},
        },
        incumbent_dimensions={},
        incumbent_truth_class="",
        challenger_truth_class="",
    ) if _incumbent_exists and _challenger_score > 0 and _incumbent_score > 0 else None
    _policy_replacement_authority = str((_challenger_promotion or {}).get("policy_replacement_authority") or "replacement_incomplete")
    actionable = recommendation_state == "recommended_primary" and candidate_status == "fully_clean_recommendable"
    _approval_gate_blocked = False
    if candidate_status == "blocked_by_policy":
        decision_type = "REJECT"
    elif candidate_status in {"blocked_by_missing_required_evidence", "blocked_by_unresolved_gate"}:
        decision_type = "RESEARCH"
    elif recommendation_state == "recommended_primary":
        current_symbol = str(current_holding_record.get("current_symbol") or "")
        if not current_symbol:
            decision_type = "ADD"
        elif current_symbol == candidate_symbol:
            decision_type = "HOLD"
        elif _policy_replacement_authority == "replacement_eligible":
            decision_type = "REPLACE"
            if conn is not None and approval_required:
                _escalation_result = check_policy_escalation_allowed(
                    conn,
                    entity_id=str(candidate.get("candidate_id") or ""),
                    sleeve_key=str(candidate.get("sleeve_key") or sleeve_key),
                    change_type="REPLACE",
                    directness_class=str((_challenger_promotion or {}).get("challenger_truth_class") or "unknown"),
                    authority_class="decision_grade",
                    policy_action_class="action_eligible",
                    policy_restriction_codes=[],
                    requires_approval=True,
                )
                if not _escalation_result.get("policy_escalation_allowed"):
                    decision_type = "HOLD"
                    _approval_gate_blocked = True
        elif _policy_replacement_authority == "replacement_incomplete":
            decision_type = "HOLD"
            actionable = False
        else:
            # replacement_blocked — preserve existing HOLD behavior
            decision_type = "HOLD"
            actionable = False
    elif recommendation_state == "recommended_backup":
        decision_type = "RESEARCH"
    else:
        decision_type = "RESEARCH"

    runner_up = dict(recommendation_context.get("challenger") or {})
    baseline_symbol = str(baseline_reference.get("baseline_symbol") or "")
    beats_baseline = None
    if baseline_symbol and baseline_symbol != candidate_symbol and scoring_result.get("baseline_gap") is not None:
        beats_baseline = bool(float(scoring_result.get("baseline_gap") or 0.0) > 0)
    why_wins = str(recommendation_context.get("lead_summary") or dict(candidate.get("decision_record") or {}).get("reason") or "")
    why_beats_runner_up = why_wins or "Current lead comes from the strongest mix of gates, score validity, and sleeve fit."
    if not winner_symbol or winner_symbol == candidate_symbol:
        why_this_candidate = "Candidate currently leads the sleeve under the active evidence and policy stack."
    else:
        why_this_candidate = "Candidate remains behind the current winner on the active evidence and policy stack."
    if baseline_symbol and baseline_symbol != candidate_symbol:
        if beats_baseline is True:
            why_beats_baseline = f"It currently clears the sleeve baseline {baseline_symbol} on net evidence and recommendation standing."
        elif beats_baseline is False:
            why_beats_baseline = f"It does not currently beat the sleeve baseline {baseline_symbol} on net recommendation usefulness."
        else:
            why_beats_baseline = f"Baseline comparison versus {baseline_symbol} remains incomplete."
    else:
        why_beats_baseline = "This candidate is the sleeve baseline or no separate baseline is configured."

    no_change_is_best = bool(
        recommendation_state == "recommended_primary"
        and str(current_holding_record.get("status") or "") == "different_from_current"
        and (
            practical_edge_status in {"marginal", "unavailable"}
            or _policy_replacement_authority == "replacement_blocked"
            or _approval_gate_blocked
        )
    )
    return {
        "candidate_status": candidate_status,
        "decision_type": decision_type,
        "recommendation_tier": "actionable" if actionable else "non_actionable",
        "why_this_candidate": why_this_candidate,
        "lead_strength": str(recommendation_context.get("stability") or "watch_stable"),
        "action_required_now": actionable,
        "current_holding_should_be_kept": bool(current_holding_record.get("status") == "matched_to_current" or no_change_is_best),
        "no_change_is_best": no_change_is_best,
        "winner_symbol": winner_symbol or None,
        "runner_up_symbol": runner_up.get("symbol"),
        "why_beats_runner_up": why_beats_runner_up,
        "why_beats_baseline": why_beats_baseline,
        "source_integrity_effect": source_integrity.get("concise_source_summary"),
        "gate_effect": gate_result.get("overall_status"),
        "score_validity_effect": scoring_result.get("validity_flag"),
        "switching_friction": current_holding_record.get("switching_friction"),
        "practical_edge": practical_edge,
        "practical_edge_required": recommendation_state == "recommended_primary",
        "portfolio_consequence": str(dict(candidate.get("investor_consequence_summary") or {}).get("summary") or ""),
        "upgrade_path_exists": bool(dict(candidate.get("upgrade_path") or {}).get("upgrade_path_valid")),
        "rejection_reason": dict(decision.get("rejection_reason") or {}),
    }


def _build_decision_thesis(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    recommendation_result: dict[str, Any],
    source_integrity: dict[str, Any],
    scoring_result: dict[str, Any],
    baseline_reference: dict[str, Any],
) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    consequence = dict(candidate.get("investor_consequence_summary") or {})
    recommendation_context = dict(candidate.get("recommendation_context") or {})
    main_strength = next(
        (
            item
            for item in list(quality.get("better_than_peers") or []) + list(quality.get("why_it_wins") or []) + list(quality.get("key_advantages") or [])
            if str(item).strip()
        ),
        "It is currently the cleanest usable way to express the sleeve role.",
    )
    main_limitation = next(
        (
            item
            for item in list(quality.get("main_limitations") or []) + list(quality.get("key_risks") or []) + list(source_integrity.get("blocking_issues") or [])
            if str(item if isinstance(item, str) else item.get("reason") or item.get("label") or "").strip()
        ),
        "Important implementation or evidence limits still need review.",
    )
    limitation_text = str(main_limitation if isinstance(main_limitation, str) else main_limitation.get("reason") or main_limitation.get("label") or "").strip()
    confidence_state = "moderate"
    caution = str(source_integrity.get("overall_caution_level") or "moderate")
    comparability = str(dict(scoring_result.get("confidence_or_comparability_notes") or {}).get("comparability") or "")
    if caution == "critical":
        confidence_state = "limited"
    elif caution == "minor" and comparability in {"fully comparable across peers", "full"}:
        confidence_state = "high"
    elif caution == "material":
        confidence_state = "limited"
    lead_strength = str(recommendation_result.get("lead_strength") or recommendation_context.get("stability") or "conditional")
    thesis_summary = (
        str(quality.get("investment_thesis") or "").strip()
        or str(quality.get("structured_summary") or "").strip()
        or str(recommendation_result.get("why_this_candidate") or "").strip()
    )
    if not thesis_summary:
        thesis_summary = "This candidate remains under active Blueprint review for its sleeve role."
    baseline_symbol = str(baseline_reference.get("baseline_symbol") or "")
    return {
        "candidate_id": str(candidate.get("symbol") or ""),
        "thesis_summary": thesis_summary,
        "lead_reason": str(recommendation_result.get("why_beats_runner_up") or recommendation_result.get("why_this_candidate") or ""),
        "hesitation_reason": limitation_text,
        "lead_strength": lead_strength,
        "confidence_state": confidence_state,
        "current_decision": str(recommendation_result.get("decision_type") or "RESEARCH"),
        "key_allocation_reason": str(main_strength),
        "key_reservation_reason": limitation_text,
        "rank": quality.get("rank_in_sleeve"),
        "sleeve": sleeve_key,
        "baseline_anchor": baseline_symbol or None,
        "comparability_quality": comparability or None,
    }


def _bounded_range(center: float, width: float) -> list[float]:
    lower = round(center - width, 2)
    upper = round(center + width, 2)
    return [lower, upper] if lower <= upper else [upper, lower]


def _build_forecast_visual_model(
    *,
    candidate: dict[str, Any],
    scoring_result: dict[str, Any],
    source_integrity: dict[str, Any],
) -> dict[str, Any]:
    performance = dict(candidate.get("performance_metrics") or {})
    quality = dict(candidate.get("investment_quality") or {})
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    return_1y = performance.get("return_1y")
    return_3y = performance.get("return_3y")
    volatility_1y = performance.get("volatility_1y")
    current_anchor = 100.0
    base_return = None
    if return_3y is not None:
        base_return = float(return_3y) / 3.0
    elif return_1y is not None:
        base_return = float(return_1y)
    width = float(volatility_1y or 0.12)
    if base_return is None:
        base_return = float((quality.get("forecast_adjustment") or 0.0)) / 100.0
    if base_return is None:
        base_return = 0.05
    base_center = current_anchor * (1.0 + base_return)
    base_case = _bounded_range(base_center, current_anchor * max(width * 0.5, 0.05))
    stronger_case = _bounded_range(base_center + current_anchor * 0.05, current_anchor * max(width * 0.45, 0.04))
    weaker_case = _bounded_range(base_center - current_anchor * 0.07, current_anchor * max(width * 0.55, 0.05))
    forecast_confidence = "moderate"
    validity = "usable_with_limits"
    caution = str(source_integrity.get("overall_caution_level") or "moderate")
    if not bool(scoring_result.get("validity_flag")):
        forecast_confidence = "disabled"
        validity = "disabled"
    elif caution in {"critical", "material"}:
        forecast_confidence = "low"
        validity = "partial"
    elif return_3y is not None and volatility_1y is not None:
        forecast_confidence = "moderate"
        validity = "direct_history_supported"
    elif return_1y is not None:
        forecast_confidence = "low"
        validity = "short_history_only"
    confidence_intervals = {
        "p50": _bounded_range(base_center, current_anchor * max(width * 0.35, 0.03)),
        "p80": base_case,
    }
    downside_probability = 0.45 if forecast_confidence == "low" else 0.3 if forecast_confidence == "moderate" else 0.15
    validity_notes = []
    if benchmark.get("benchmark_kind") == "proxy":
        validity_notes.append("Forecast context leans on proxy benchmark support rather than a clean direct benchmark.")
    if caution in {"critical", "material"}:
        validity_notes.append("Evidence quality still limits how much weight this forward-looking range deserves.")
    if return_3y is None and return_1y is None:
        validity_notes.append("Range is normalized from limited history and should be read as directional scenario framing only.")
    if not validity_notes:
        validity_notes.append("Range is derived from validated history support and should be read as a bounded path view, not a point forecast.")
    return {
        "current_anchor": current_anchor,
        "current_anchor_label": "Normalized current level",
        "forecast_horizon": "12 months",
        "base_case_range": base_case,
        "stronger_case_range": stronger_case,
        "weaker_case_range": weaker_case,
        "confidence_intervals": confidence_intervals,
        "downside_probability": round(downside_probability, 2),
        "forecast_validity_notes": validity_notes,
        "forecast_confidence": forecast_confidence,
        "forecast_validity_summary": validity,
        "evidence_basis": "direct" if return_3y is not None else "proxy_based_or_partial",
    }


def _build_cost_realism_summary(
    *,
    candidate: dict[str, Any],
    current_holding_record: dict[str, Any],
    recommendation_result: dict[str, Any],
    tax_assumption_status: dict[str, Any],
) -> dict[str, Any]:
    expense_ratio = float(candidate.get("expense_ratio") or 0.0)
    sg_lens = dict(candidate.get("sg_lens") or {})
    breakdown = dict(sg_lens.get("breakdown") or {})
    spread_bps = float(dict(candidate.get("performance_metrics") or {}).get("spread_bps_latest") or 0.0)
    switch = dict(current_holding_record.get("switching_friction") or {})
    switch_cost = float(switch.get("estimated_total_bps") or 0.0)
    tax_drag = abs(float(breakdown.get("withholding_penalty") or 0.0))
    total_cost_estimate = round(expense_ratio * 10000.0 / 100.0 + spread_bps + tax_drag, 2)
    wrapper_implications = (
        f"{candidate.get('domicile') or 'Unknown domicile'} {str(candidate.get('instrument_type') or 'wrapper').replace('_', ' ')}"
    ).strip()
    cost_summary_text = (
        "Owning this candidate is cheap enough to stay interesting on cost alone."
        if total_cost_estimate <= 25
        else "The cost case is usable, but implementation friction and tax drag still matter to the net edge."
        if total_cost_estimate <= 60
        else "Costs and friction are high enough that the case needs a stronger strategic edge."
    )
    if recommendation_result.get("no_change_is_best"):
        cost_summary_text += " The current holding remains preferable because the replacement edge does not yet clear switching cost."
    if not bool(tax_assumption_status.get("decisive_tax_use_allowed")):
        cost_summary_text += " Tax drag should still be read as conditional because the tax assumptions are incomplete."
    return {
        "expense_ratio": candidate.get("expense_ratio"),
        "total_cost_estimate_bps": total_cost_estimate,
        "trading_friction_bps": spread_bps,
        "tax_drag_bps": round(tax_drag, 2),
        "wrapper_implications": wrapper_implications,
        "switch_cost_estimate_bps": switch_cost or None,
        "cost_summary_text": cost_summary_text,
    }


def _build_portfolio_consequence_summary(
    *,
    candidate: dict[str, Any],
    baseline_reference: dict[str, Any],
    current_holding_record: dict[str, Any],
    recommendation_result: dict[str, Any],
) -> dict[str, Any]:
    consequence = dict(candidate.get("investor_consequence_summary") or {})
    improves: list[str] = []
    worsens: list[str] = []
    unchanged: list[str] = []
    implementation_effect = str(consequence.get("implementation_quality_effect") or "")
    benchmark_effect = str(consequence.get("benchmark_comparison_effect") or "")
    trust_effect = str(consequence.get("investment_trust_effect") or "")
    for text in (implementation_effect, benchmark_effect, trust_effect):
        lowered = text.lower()
        if not text:
            continue
        if any(token in lowered for token in ("improve", "cleaner", "better", "stronger", "more favorable")):
            improves.append(text)
        elif any(token in lowered for token in ("weaken", "fragile", "reduced", "worse", "limited")):
            worsens.append(text)
        else:
            unchanged.append(text)
    if not improves:
        improves.append("The main benefit is a potentially cleaner sleeve implementation if the current evidence holds.")
    if not worsens:
        worsens.append("The main risk is that recommendation confidence is still sensitive to unresolved evidence or benchmark limitations.")
    if not unchanged:
        unchanged.append("The sleeve’s strategic purpose remains broadly similar even if the implementation changes.")
    baseline_symbol = str(baseline_reference.get("baseline_symbol") or "")
    return {
        "improves": improves[:3],
        "worsens": worsens[:3],
        "unchanged": unchanged[:3],
        "current_holding_relative_effect": str(current_holding_record.get("comparison_summary") or "Current holding comparison is not yet available."),
        "baseline_relative_effect": str(recommendation_result.get("why_beats_baseline") or f"Baseline comparison versus {baseline_symbol or 'the sleeve baseline'} is incomplete."),
        "sleeve_role_effect": str(candidate.get("investment_quality", {}).get("fit_for_sleeve") or ""),
        "diversification_effect": "Diversification benefit depends on whether this candidate adds cleaner breadth or simply repackages the same sleeve exposure.",
        "implementation_effect": implementation_effect or "Implementation effect is still driven mainly by evidence quality and wrapper practicality.",
    }


def _build_decision_change_set(
    *,
    candidate: dict[str, Any],
    recommendation_result: dict[str, Any],
) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    recommendation_context = dict(candidate.get("recommendation_context") or {})
    upgrade_path = dict(candidate.get("upgrade_path") or {})
    strengthens = list(quality.get("confidence_improvers") or [])[:4]
    weakens = list(quality.get("recommendation_weakeners") or [])[:4]
    upgrade_conditions = list(upgrade_path.get("required_evidence_or_condition") or [])[:5]
    downgrade_conditions = list(recommendation_context.get("flip_conditions") or [])[:5]
    invalidation_conditions = []
    if recommendation_result.get("no_change_is_best"):
        invalidation_conditions.append("The practical edge versus the current holding remains too small after switching friction.")
    if str(recommendation_result.get("candidate_status") or "").startswith("blocked_by_"):
        invalidation_conditions.append("A decisive policy or evidence issue remains unresolved for this sleeve.")
    if not invalidation_conditions:
        invalidation_conditions.append("A cleaner runner-up, a weaker benchmark anchor, or deteriorating implementation evidence would invalidate the current lead.")
    return {
        "strengthens_case": strengthens,
        "weakens_case": weakens,
        "upgrade_conditions": upgrade_conditions,
        "downgrade_conditions": downgrade_conditions,
        "invalidation_conditions": invalidation_conditions,
    }


def _build_supporting_metadata_summary(
    *,
    candidate: dict[str, Any],
    source_integrity: dict[str, Any],
    gate_result: dict[str, Any],
    scoring_result: dict[str, Any],
    current_holding_record: dict[str, Any],
    recommendation_result: dict[str, Any],
) -> dict[str, Any]:
    bucket_support = dict(candidate.get("bucket_support") or {})
    upstream_truth = dict(candidate.get("upstream_truth_contract") or {})
    return {
        "admissibility": {
            "status": gate_result.get("overall_status"),
            "summary": "Candidate remains admissible only if the decisive gate stack continues to clear."
            if gate_result.get("overall_status") == "pass"
            else "Decisive gates or evidence still limit admissibility.",
        },
        "sleeve_fit": {
            "status": dict(candidate.get("decision_record") or {}).get("sleeve_fit_state"),
            "summary": str(dict(candidate.get("decision_record") or {}).get("explanations", {}).get("sleeve_fit") or ""),
        },
        "evidence": {
            "status": source_integrity.get("overall_source_status"),
            "summary": source_integrity.get("concise_source_summary"),
            "source_directness": candidate.get("source_directness"),
            "coverage_class": candidate.get("coverage_class"),
            "authority_class": candidate.get("authority_class"),
            "fallback_state": candidate.get("fallback_state"),
            "claim_limit_class": candidate.get("claim_limit_class"),
            "evidence_density_class": candidate.get("evidence_density_class"),
            "downgrade_reasons": list(upstream_truth.get("downgrade_reasons") or []),
            "bucket_support": {
                bucket_name: dict(dict(bucket_support.get(bucket_name) or {}).get("interpretation_summary") or {})
                for bucket_name in ("holdings_exposure", "benchmark_support", "liquidity_and_aum", "tax_posture")
                if bucket_support.get(bucket_name) is not None
            },
        },
        "comparison": {
            "status": dict(scoring_result.get("confidence_or_comparability_notes") or {}).get("comparability"),
            "summary": scoring_result.get("score_explanation_summary"),
        },
        "current_action": {
            "status": recommendation_result.get("decision_type"),
            "summary": str(current_holding_record.get("comparison_summary") or ""),
        },
        "citations": {
            "count": len(list(candidate.get("citations") or [])),
        },
        "audit_trace_refs": [str(item.get("step") or "") for item in list(candidate.get("audit_log_entries") or []) if str(item.get("step") or "").strip()],
    }


def _build_memo_result(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    recommendation_result: dict[str, Any],
    current_holding_record: dict[str, Any],
    source_integrity: dict[str, Any],
    gate_result: dict[str, Any],
    scoring_result: dict[str, Any],
) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    recommendation_state = str(recommendation_result.get("candidate_status") or "")
    limitations = list(quality.get("main_limitations") or quality.get("key_risks") or [])
    improve_confidence = list(quality.get("confidence_improvers") or [])
    strengthen_or_change = dict(candidate.get("upgrade_path") or {})
    bucket_support = dict(candidate.get("bucket_support") or {})
    return {
        "status_line": recommendation_state.replace("_", " ").title(),
        "investment_case": str(quality.get("investment_thesis") or quality.get("structured_summary") or ""),
        "why_ahead_or_behind": str(recommendation_result.get("why_this_candidate") or ""),
        "key_tradeoffs_and_risks": limitations[:5],
        "decision_change_conditions": {
            "what_would_strengthen": improve_confidence[:4],
            "what_would_weaken": list(quality.get("recommendation_weakeners") or [])[:4],
            "upgrade_path": strengthen_or_change,
        },
        "supporting_detail": {
            "benchmark_context": str(dict(candidate.get("benchmark_assignment") or {}).get("benchmark_explanation") or ""),
            "implementation_notes": list(dict(candidate.get("investment_lens") or {}).get("implementation_notes") or [])[:4],
            "evidence_summary": source_integrity.get("concise_source_summary"),
            "bucket_support": {
                bucket_name: dict(bucket_support.get(bucket_name) or {})
                for bucket_name in ("identity_wrapper", "holdings_exposure", "liquidity_and_aum", "benchmark_support", "tax_posture")
                if bucket_support.get(bucket_name) is not None
            },
            "current_holding_context": {
                "current_symbol": current_holding_record.get("current_symbol"),
                "switching_friction": recommendation_result.get("switching_friction"),
                "practical_edge": recommendation_result.get("practical_edge"),
                "no_change_is_best": recommendation_result.get("no_change_is_best"),
            },
            "technical_appendix": {
                "canonical_decision_available": bool(candidate.get("canonical_decision")),
                "gate_status": gate_result.get("overall_status"),
                "score_validity": scoring_result.get("validity_flag"),
                "sleeve_key": sleeve_key,
            },
        },
    }


def _build_audit_trace(
    *,
    candidate: dict[str, Any],
    evidence_pack: dict[str, Any],
    source_integrity: dict[str, Any],
    gate_result: dict[str, Any],
    review_intensity: dict[str, Any],
    universal_review: dict[str, Any],
    deep_review: dict[str, Any] | None,
    scoring_result: dict[str, Any],
    recommendation_result: dict[str, Any],
    memo_result: dict[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "step": "evidence_pack_build",
            "status": "completed",
            "reason": "Canonical EvidencePack assembled from field truth, benchmark assignment, performance support, and implementation fields.",
            "decisive_inputs": list(evidence_pack.keys()),
        },
        {
            "step": "source_integrity_checks",
            "status": source_integrity.get("overall_source_status"),
            "reason": source_integrity.get("concise_source_summary"),
            "decisive_inputs": [item.get("label") for item in list(source_integrity.get("blocking_issues") or [])[:5]],
        },
        {
            "step": "gate_outcomes",
            "status": gate_result.get("overall_status"),
            "reason": "Canonical gate stack evaluated.",
            "decisive_inputs": [item.get("gate_name") for item in list(gate_result.get("decisive_failures") or [])[:5]],
        },
        {
            "step": "review_intensity_decision",
            "status": review_intensity.get("review_intensity"),
            "reason": review_intensity.get("summary"),
            "decisive_inputs": list(review_intensity.get("triggers") or [])[:5],
        },
        {
            "step": "universal_review_completion",
            "status": "completed",
            "reason": "Universal review findings are available for every candidate.",
            "decisive_inputs": list(universal_review.keys()),
        },
        {
            "step": "deep_review_completion",
            "status": "completed" if deep_review else "not_triggered",
            "reason": str((deep_review or {}).get("implications_for_memo_generation") or "Deep review not required."),
            "decisive_inputs": list((deep_review or {}).get("escalation_reasons") or [])[:5],
        },
        {
            "step": "scoring_completion",
            "status": "completed" if scoring_result.get("validity_flag") else "suppressed",
            "reason": scoring_result.get("score_explanation_summary") or "Score pipeline evaluated.",
            "decisive_inputs": list(dict(scoring_result.get("confidence_or_comparability_notes") or {}).get("unknown_dimensions") or [])[:5],
        },
        {
            "step": "recommendation_decision",
            "status": recommendation_result.get("candidate_status"),
            "reason": recommendation_result.get("why_this_candidate"),
            "decisive_inputs": [
                recommendation_result.get("winner_symbol"),
                recommendation_result.get("runner_up_symbol"),
                dict(candidate.get("decision_record") or {}).get("final_decision_state"),
            ],
        },
        {
            "step": "memo_generation",
            "status": "completed",
            "reason": memo_result.get("investment_case"),
            "decisive_inputs": list(memo_result.keys()),
        },
    ]
