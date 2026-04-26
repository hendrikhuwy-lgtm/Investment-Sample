from __future__ import annotations

"""Blueprint payload assembler.

This module may remain large temporarily because it still carries broad payload
assembly and compatibility wiring. It is no longer the semantic owner of
recommendation meaning; canonical decision logic lives in the pipeline and
canonical decision modules.
"""

from functools import lru_cache
import json
import re
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from app.config import Settings, get_db_path
from app.models.db import connect
from app.models.types import InstrumentTaxProfile, TaxResidencyProfile
from app.services.data_lag import classify_lag_cause
from app.services.delta_engine import load_latest_delta_payload
from app.services.blueprint_liquidity import evaluate_liquidity_profile
from app.services.blueprint_candidate_compare import compare_candidates
from app.services.blueprint_candidate_registry import (
    AGING_STATE,
    BROKEN_SOURCE_STATE,
    LIVE_OBJECT_TYPE,
    MANUAL_SEED_STATE,
    POLICY_PLACEHOLDER_TYPE,
    SOURCE_VALIDATED_STATE,
    STALE_LIVE_STATE,
    STRATEGY_PLACEHOLDER_TYPE,
    ensure_candidate_registry_tables,
    export_candidate_registry,
    list_active_candidate_registry,
    refresh_registry_candidate_truth,
    seed_default_candidate_registry,
)
from app.services.blueprint_candidate_truth import (
    PROVENANCE_PRIORITIES,
    _json,
    _default_value_type,
    _field_stale_days,
    _safe_parse_dt,
    compute_candidate_completeness,
    ensure_candidate_truth_tables,
    get_candidate_field_current,
    list_required_fields,
    persist_sleeve_no_pick_reason,
    resolve_candidate_field_truth,
    seed_required_field_matrix,
    upsert_field_observation,
)
from app.services.blueprint_candidate_eligibility import evaluate_candidate_eligibility
from app.services.blueprint_benchmark_registry import (
    enrich_benchmark_assignment,
    ensure_benchmark_registry_tables,
    build_benchmark_registry_summary,
    normalize_benchmark_fit_type,
    resolve_benchmark_assignment,
    upsert_candidate_benchmark_assignment,
    validate_benchmark_assignment,
)
from app.services.blueprint_data_quality import summarize_blueprint_data_quality
from app.services.blueprint_decision_semantics import (
    build_confidence_history_summary,
    build_confidence_snapshot,
    build_blueprint_decision_record,
    build_investor_consequence_summary,
    build_review_escalation,
    evaluate_sleeve_expression,
    finalize_decision_record,
)
from app.services.blueprint_candidate_universe import (
    build_candidate_universe,
    build_candidate_universe_diff,
)
from app.services.blueprint_deliverable_candidates import (
    build_deliverable_candidates,
    build_deliverable_candidates_diff,
)
from app.services.blueprint_decisions import ensure_blueprint_decision_tables, list_candidate_decision_events, list_candidate_decisions
from app.services.blueprint_investment_quality import (
    V1_MODEL,
    V2_MODEL,
    build_investment_quality_score,
    ensure_quality_tables,
    get_latest_performance_metrics,
    list_score_models,
    maybe_refresh_performance_data,
)
from app.services.blueprint_rebalance import build_rebalance_policy, evaluate_rebalance_diagnostics
from app.services.blueprint_refresh_monitor import latest_blueprint_refresh_status
from app.services.blueprint_recommendations import (
    build_recommendation_events,
    ensure_recommendation_tables,
    list_recommendation_events,
    rank_sleeve_candidates,
)
from app.services.blueprint_pipeline import build_candidate_pipeline, classify_blueprint_modules
from app.services.blueprint_recommendation_diff import build_recommendation_diff
from app.services.blueprint_rejection_memo import build_rejection_memo
from app.services.blueprint_replacement_opportunities import build_replacement_opportunities
from app.services.blueprint_risk_controls import evaluate_concentration_controls, get_concentration_policy, summarize_concentration_status
from app.services.citation_health import ensure_citation_health_tables, summarize_citation_health
from app.services.provider_refresh import build_cached_external_upstream_payload
from app.services.regime_transition import get_regime_transition_context
from app.services.etf_doc_parser import fetch_candidate_docs
from app.services.portfolio_state import build_portfolio_state_context, list_holdings
from app.services.ingest_etf_data import (
    _ensure_etf_tables,
    configure_etf_data_source,
    fetch_ishares_holdings,
    get_etf_source_config,
    get_etf_holdings_profile,
    get_etf_factsheet_history_summary,
    get_latest_successful_etf_ingest_at,
    get_latest_etf_fetch_status,
    get_latest_etf_factsheet_metrics,
    get_preferred_latest_market_data,
    get_preferred_market_history_summary,
    list_etf_source_configs,
    refresh_etf_data,
)
from app.services.tax_engine import build_sg_tax_truth, evaluate_instrument_for_sg
from app.services.verification import verify_candidate_proofs
from app.services.ips import blueprint_policy_from_ips, get_ips, ips_version_token
from app.services.blueprint_store import persist_blueprint_runtime_cycle
from app.services.source_truth_registry import truth_family_registry
from app.services.upstream_health_report import enforce_registry_parity
from app.services.upstream_truth_contract import (
    apply_claim_boundary_from_truth,
    build_candidate_upstream_truth_contract,
    infer_candidate_directness,
    normalize_source_state_base,
    precise_source_state,
    classify_authority,
    classify_claim_limit,
    classify_directness,
)
from app.services.policy_authority import build_policy_authority_record


@lru_cache(maxsize=256)
def _fetch_candidate_docs_cached(symbol: str, use_fixtures: bool = True) -> dict[str, Any]:
    return fetch_candidate_docs(symbol, use_fixtures=use_fixtures)


BLUEPRINT_PAYLOAD_INTEGRITY_VERSION = 3
BLUEPRINT_REQUIRED_DETAIL_FIELDS = (
    "decision_record.mandate_fit_state",
    "decision_record.sleeve_fit_state",
    "decision_record.policy_gate_state",
    "decision_record.data_quality_state",
    "decision_record.scoring_state",
    "action_readiness",
    "usability_memo.state",
    "usability_memo.summary",
    "benchmark_assignment.benchmark_fit_type",
    "benchmark_assignment.benchmark_authority_level",
    "benchmark_assignment.benchmark_effect_label",
    "benchmark_assignment.benchmark_kind",
    "benchmark_assignment.benchmark_role",
    "investment_lens.liquidity_profile.liquidity_status",
    "investment_lens.liquidity_profile.explanation",
)


def _publisher_from_source(source_id: str) -> str:
    source = str(source_id or "").lower()
    if source.startswith("vanguard"):
        return "Vanguard"
    if source.startswith("ishares"):
        return "BlackRock iShares"
    if source.startswith("xtrackers"):
        return "DWS Xtrackers"
    if source.startswith("hsbc"):
        return "HSBC Asset Management"
    if source.startswith("nikko"):
        return "Nikko AM"
    if source.startswith("invesco"):
        return "Invesco"
    if source.startswith("spdr"):
        return "State Street SPDR"
    if source.startswith("mas"):
        return "Monetary Authority of Singapore"
    if source.startswith("fullerton"):
        return "Fullerton Fund Management"
    if source in {"dbmf", "kmlm", "tail", "caos"}:
        return "Issuer"
    if source == "cboe_spx":
        return "Cboe"
    if source == "irs_nra":
        return "IRS"
    if source == "iras_overseas_income":
        return "IRAS"
    return "Source"


def _purpose_from_source(source_id: str) -> str:
    source = str(source_id or "").lower()
    if source in {"irs_nra", "iras_overseas_income"}:
        return "Tax context"
    return "Identity"


def _citation(
    *,
    source_id: str,
    url: str,
    retrieved_at: datetime,
    importance: str,
    published_at: str | None = None,
    proof_identifier: str | None = None,
    proof_domicile: str | None = None,
    proof_share_class: str | None = None,
    proof_ter: float | None = None,
    factsheet_asof: str | None = None,
) -> dict[str, Any]:
    payload = {
        "source_id": source_id,
        "url": url,
        "retrieved_at": retrieved_at.isoformat(),
        "importance": importance,
        "tier": "primary",
        "publisher": _publisher_from_source(source_id),
        "title": importance,
        "purpose": _purpose_from_source(source_id),
        "published_at": published_at,
    }
    if proof_identifier is not None:
        payload["proof_identifier"] = str(proof_identifier)
    if proof_domicile is not None:
        payload["proof_domicile"] = str(proof_domicile)
    if proof_share_class is not None:
        payload["proof_share_class"] = str(proof_share_class)
    if proof_ter is not None:
        payload["proof_ter"] = float(proof_ter)
    if factsheet_asof is not None:
        payload["factsheet_asof"] = str(factsheet_asof)
    return payload


def _safe_parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        try:
            parsed = datetime.fromisoformat(f"{text}T00:00:00+00:00")
        except Exception:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _field_stale_days(field_name: str) -> int:
    key = str(field_name or "").strip().lower()
    if key in {"market_price", "quote", "volume", "aum", "tracking_difference_1y"}:
        return 7
    if key in {"expense_ratio", "domicile", "fund_name", "benchmark", "replication_method"}:
        return 180
    return 30


def _bucket_state(resolved: int, total: int, *, proxy_only: bool = False) -> str:
    if proxy_only and resolved <= 0:
        return "proxy_only"
    if resolved <= 0:
        return "missing"
    if resolved >= total:
        return "complete"
    return "partial"


def _has_truth(field_truth: dict[str, Any], field_name: str) -> bool:
    row = dict(field_truth.get(field_name) or {})
    value = row.get("resolved_value")
    return value not in {None, "", "unknown"}


_TRUTH_GRADE_SOURCE_NAMES = {
    "issuer_doc_parser",
    "etf_holdings",
    "etf_holdings_summary",
    "etf_factsheet_metrics",
    "benchmark_registry",
    "tax_engine",
    "canonical_instrument",
}
_SUPPORT_GRADE_SOURCE_NAMES = {
    "etf_market_data",
}
_FALLBACK_SOURCE_NAMES = {
    "candidate_registry",
    "supplemental_candidate_metrics",
}


def _field_quality(field_truth: dict[str, Any], field_name: str) -> dict[str, Any]:
    row = dict(field_truth.get(field_name) or {})
    value = row.get("resolved_value")
    source_name = str(row.get("source_name") or "").strip()
    completeness_state = str(row.get("completeness_state") or "").strip().lower()
    evidence_class = str(row.get("evidence_class") or "").strip().lower()
    override_annotation = dict(row.get("override_annotation") or {})
    return {
        "has_value": value not in {None, "", "unknown"},
        "source_name": source_name,
        "source_type": str(row.get("source_type") or "").strip().lower(),
        "completeness_state": completeness_state,
        "evidence_class": evidence_class,
        "truth_grade": source_name in _TRUTH_GRADE_SOURCE_NAMES and evidence_class in {"verified_official", "verified_nonissuer", "inferred"},
        "support_grade": source_name in _SUPPORT_GRADE_SOURCE_NAMES,
        "fallback": source_name in _FALLBACK_SOURCE_NAMES or "fallback" in str(row.get("source_type") or "").lower(),
        "stale_or_partial": completeness_state in {"weak_or_partial", "incomplete"},
        "bucket_source_class": str(override_annotation.get("bucket_source_class") or ""),
        "bucket_parse_confidence": str(override_annotation.get("bucket_parse_confidence") or ""),
        "bucket_failure_reason": str(override_annotation.get("bucket_failure_reason") or ""),
    }


def _resolved_field_count(
    field_truth: dict[str, Any],
    field_names: list[str],
    *,
    require_truth_grade: bool = False,
) -> int:
    count = 0
    for field_name in field_names:
        quality = _field_quality(field_truth, field_name)
        if not quality["has_value"]:
            continue
        if require_truth_grade and not quality["truth_grade"]:
            continue
        count += 1
    return count


def _candidate_tax_truth_profile(candidate: dict[str, Any]) -> dict[str, Any]:
    sg_lens = dict(candidate.get("sg_lens") or {})
    breakdown = dict(sg_lens.get("breakdown") or {})
    tax_mechanics = dict(dict(candidate.get("investment_lens") or {}).get("tax_mechanics") or {})
    factsheet_asof = _safe_parse_dt(candidate.get("factsheet_asof"))
    now = datetime.now(UTC)
    factsheet_fresh = False
    if factsheet_asof is not None:
        factsheet_fresh = (now - factsheet_asof).days <= 180

    withholding_rate = _safe_float(candidate.get("expected_withholding_drag_estimate"))
    domicile = str(candidate.get("domicile") or "").strip().upper()
    distribution = str(candidate.get("accumulation_or_distribution") or "").strip().lower()
    instrument_type = str(candidate.get("instrument_type") or "").strip().lower()
    withholding_note = str(candidate.get("withholding_tax_exposure_note") or tax_mechanics.get("withholding_tax_exposure_note") or "").strip()
    score = _safe_float(sg_lens.get("score"))

    policy_buckets = {
        "withholding_drag": {
            "mode": "observed" if withholding_rate is not None else "modeled_missing",
            "value": withholding_rate,
            "status": "supported" if withholding_rate is not None else "missing",
        },
        "estate_risk": {
            "mode": "observed" if "us_situs_estate_risk_flag" in candidate else "modeled_missing",
            "value": bool(candidate.get("us_situs_estate_risk_flag")),
            "status": "supported" if "us_situs_estate_risk_flag" in candidate else "missing",
        },
        "wrapper_efficiency": {
            "mode": "observed" if domicile else "modeled_missing",
            "value": "sg_retail_preferred" if domicile == "IE" and instrument_type == "etf_ucits" else "review_required" if domicile else None,
            "status": "supported" if domicile else "missing",
        },
        "distribution_tax_friction": {
            "mode": "observed" if distribution else "modeled_missing",
            "value": distribution or None,
            "status": "supported" if distribution else "missing",
        },
        "domicile_legal_friction": {
            "mode": "observed" if domicile else "modeled_missing",
            "value": domicile or None,
            "status": "supported" if domicile else "missing",
        },
        "account_implementation_compatibility": {
            "mode": "modeled" if instrument_type else "modeled_missing",
            "value": "compatible" if instrument_type in {"etf_ucits", "etf_us"} else None,
            "status": "supported" if instrument_type else "missing",
        },
    }
    supported_count = sum(1 for bucket in policy_buckets.values() if str(bucket.get("status") or "") == "supported")
    if score is None or supported_count < 4:
        confidence = "low"
    elif supported_count >= 6 and factsheet_fresh and "unverified" not in withholding_note.lower():
        confidence = "high"
    else:
        confidence = "medium"

    if score is None or confidence == "low":
        advisory_boundary = "requires_case_specific_review"
    elif domicile == "IE" and instrument_type == "etf_ucits" and not bool(candidate.get("us_situs_estate_risk_flag")):
        advisory_boundary = "good_sg_retail_default"
    else:
        advisory_boundary = "acceptable_but_structurally_inferior"

    return {
        "tax_score": score,
        "tax_confidence": confidence,
        "evidence_strength": "supported" if confidence == "high" else "mixed" if confidence == "medium" else "thin",
        "policy_buckets": policy_buckets,
        "advisory_boundary": advisory_boundary,
        "factsheet_fresh": factsheet_fresh,
        "withholding_note_verified": "unverified" not in withholding_note.lower() if withholding_note else False,
        "score_breakdown": breakdown,
    }


def _bucket_field_counts(field_truth: dict[str, Any], field_names: list[str]) -> dict[str, Any]:
    direct_count = 0
    summary_count = 0
    truth_count = 0
    fallback_count = 0
    failure_reasons: list[str] = []
    for field_name in field_names:
        quality = _field_quality(field_truth, field_name)
        if quality["bucket_failure_reason"]:
            failure_reasons.append(quality["bucket_failure_reason"])
        if not quality["has_value"]:
            continue
        if quality["truth_grade"]:
            truth_count += 1
        if quality["fallback"] or quality["support_grade"]:
            fallback_count += 1
        source_type = quality["source_type"]
        source_class = quality["bucket_source_class"]
        if source_type == "issuer_holdings_primary" or source_class == "direct_holdings":
            direct_count += 1
        elif source_type == "issuer_factsheet_secondary" or source_class in {"factsheet_summary", "html_summary"}:
            summary_count += 1
    return {
        "direct_count": direct_count,
        "summary_count": summary_count,
        "truth_count": truth_count,
        "fallback_count": fallback_count,
        "failure_reasons": list(dict.fromkeys(item for item in failure_reasons if item))[:6],
    }


_BUCKET_FIELD_SUPPORT_MAP: dict[str, tuple[str, ...]] = {
    "identity_wrapper": ("fund_name", "isin", "domicile", "wrapper_or_vehicle_type", "distribution_type", "share_class_proven", "primary_trading_currency"),
    "holdings_exposure": ("holdings_count", "top_10_concentration", "us_weight", "em_weight", "sector_concentration_proxy", "developed_market_exposure_summary", "emerging_market_exposure_summary"),
    "expense_and_cost": ("expense_ratio", "tracking_difference_1y", "tracking_difference_3y", "tracking_error_1y"),
    "liquidity_and_aum": ("aum", "bid_ask_spread_proxy", "volume_30d_avg", "primary_listing_exchange"),
    "benchmark_support": ("benchmark_name", "benchmark_key", "benchmark_confidence", "benchmark_assignment_method", "benchmark_assignment_proof"),
    "performance_relative_support": ("tracking_difference_1y", "tracking_difference_3y", "tracking_difference_5y", "tracking_error_1y"),
    "tax_posture": ("withholding_tax_posture", "estate_risk_posture", "tax_posture", "sg_suitability_note", "distribution_policy"),
}

_BUCKET_HUMAN_LABELS = {
    "identity_wrapper": "Identity and wrapper",
    "holdings_exposure": "Holdings and exposure",
    "expense_and_cost": "Expense and cost",
    "liquidity_and_aum": "Liquidity and AUM",
    "benchmark_support": "Benchmark support",
    "performance_relative_support": "Performance-relative support",
    "tax_posture": "Tax posture",
}


def _source_tier_for_field(row: dict[str, Any]) -> str:
    quality = _field_quality({str(row.get("field_name") or ""): row}, str(row.get("field_name") or ""))
    if quality["truth_grade"]:
        return "truth_grade"
    if quality["support_grade"]:
        return "support_grade"
    if quality["fallback"]:
        return "fallback"
    return "unclassified"


def _field_support_entry(field_name: str, row: dict[str, Any]) -> dict[str, Any]:
    override_annotation = dict(row.get("override_annotation") or {})
    return {
        "field_name": field_name,
        "value": row.get("resolved_value"),
        "source_name": row.get("source_name"),
        "source_url": row.get("source_url"),
        "source_kind": override_annotation.get("bucket_source_class") or row.get("source_type"),
        "source_tier": _source_tier_for_field({**row, "field_name": field_name}),
        "observed_at": row.get("observed_at"),
        "retrieved_at": row.get("ingested_at"),
        "extraction_method": row.get("parser_method"),
        "confidence": row.get("confidence_label"),
        "evidence_class": row.get("evidence_class"),
        "completeness_state": row.get("completeness_state"),
        "missingness_reason": row.get("missingness_reason"),
        "failure_reason": override_annotation.get("bucket_failure_reason"),
    }


def _support_sort_key(entry: dict[str, Any]) -> tuple[int, int, int, str]:
    source_tier = str(entry.get("source_tier") or "")
    completeness = str(entry.get("completeness_state") or "")
    confidence = str(entry.get("confidence") or "")
    tier_rank = 3
    if source_tier == "truth_grade":
        tier_rank = 0
    elif source_tier == "support_grade":
        tier_rank = 1
    elif source_tier == "fallback":
        tier_rank = 2
    completeness_rank = 0 if completeness == "complete" else 1 if completeness == "weak_or_partial" else 2
    confidence_rank = 0 if confidence == "high" else 1 if confidence == "medium" else 2
    observed = str(entry.get("observed_at") or "")
    return (tier_rank, completeness_rank, confidence_rank, observed)


def _candidate_evidence_depth_class(candidate: dict[str, Any]) -> str:
    sleeve_key = str(candidate.get("sleeve_key") or "")
    holdings_bucket = dict(dict(candidate.get("evidence_buckets") or {}).get("holdings_exposure") or {})
    holdings_state = str(holdings_bucket.get("state") or "missing")
    holdings_source_class = str(holdings_bucket.get("source_class") or "missing")
    identity_state = str(dict(dict(candidate.get("evidence_buckets") or {}).get("identity_wrapper") or {}).get("state") or "missing")
    benchmark_state = str(dict(dict(candidate.get("evidence_buckets") or {}).get("benchmark_support") or {}).get("state") or "missing")
    liquidity_state = str(dict(dict(candidate.get("evidence_buckets") or {}).get("liquidity_and_aum") or {}).get("state") or "missing")
    if sleeve_key in {"alternatives", "convex"}:
        return "structure_first"
    if holdings_source_class == "direct_holdings" and holdings_state in {"complete", "partial"}:
        return "direct_holdings_backed"
    if holdings_source_class in {"factsheet_summary", "html_summary"}:
        strong_support_count = sum(
            1
            for state in (identity_state, benchmark_state, liquidity_state)
            if state in {"complete", "partial"}
        )
        return "structured_summary_strong" if strong_support_count >= 2 else "summary_backed_limited"
    if holdings_state in {"partial", "proxy_only"}:
        return "summary_backed_limited"
    return "summary_backed_limited"


def _bucket_claim_limits(
    *,
    bucket_name: str,
    state: str,
    evidence_depth_class: str,
    source_class: str,
    tax_confidence: str | None = None,
) -> list[str]:
    if bucket_name == "holdings_exposure":
        if evidence_depth_class == "structure_first":
            return [
                "Do not force holdings-style interpretation onto this product.",
                "Use mandate, implementation mechanics, and role fit as the primary evidence path.",
            ]
        if source_class == "direct_holdings" and state == "complete":
            return ["Supports line-item and concentration conclusions with issuer-backed holdings."]
        if evidence_depth_class == "structured_summary_strong":
            return [
                "Supports high-level exposure framing from issuer summary evidence.",
                "Does not support line-item holdings conclusions.",
            ]
        return [
            "Supports only partial structural review.",
            "Does not support strong holdings-derived claims.",
        ]
    if bucket_name == "benchmark_support":
        if state == "complete":
            return ["Direct benchmark support allows normal sleeve-relative comparison."]
        if state == "partial":
            return ["Proxy-backed comparison is usable only with explicit caveats."]
        return ["Do not make strong relative-performance claims from this bucket."]
    if bucket_name == "tax_posture":
        if str(tax_confidence or "") == "high":
            return ["Tax language can contribute normally inside the SG retail lens."]
        return ["Tax language must stay conditional because tax confidence is incomplete."]
    if bucket_name == "liquidity_and_aum":
        if state == "complete":
            return ["Implementation claims can use AUM and liquidity evidence normally."]
        return ["Implementation claims must stay softened because AUM or liquidity evidence is incomplete."]
    return []


def _bucket_interpretation_summary(
    *,
    bucket_name: str,
    bucket_state: str,
    evidence_depth_class: str,
    supported_fields: list[str],
    missing_fields: list[str],
    source_class: str,
) -> dict[str, Any]:
    label = _BUCKET_HUMAN_LABELS.get(bucket_name, bucket_name.replace("_", " ").title())
    support_text = f"{label} is currently {bucket_state.replace('_', ' ')}."
    if bucket_name == "holdings_exposure":
        if evidence_depth_class == "direct_holdings_backed":
            support_text = "Issuer-backed holdings support structural exposure and concentration review."
        elif evidence_depth_class == "structured_summary_strong":
            support_text = "Issuer summary evidence supports high-level exposure framing."
        elif evidence_depth_class == "structure_first":
            support_text = "Strategy structure is the primary evidence path for this sleeve."
        else:
            support_text = "Only limited summary-backed exposure framing is supported."
    elif bucket_name == "benchmark_support":
        support_text = (
            "Benchmark support is direct enough for normal relative comparison."
            if bucket_state == "complete"
            else "Benchmark support is proxy-backed and should stay caveated."
            if bucket_state == "partial"
            else "Benchmark support is too weak for strong relative-performance claims."
        )
    elif bucket_name == "tax_posture":
        support_text = "Tax posture is modeled inside the SG retail lens with explicit confidence handling."
    unsupported_text = (
        "No strong unsupported claim boundary identified."
        if bucket_state == "complete" and evidence_depth_class != "structured_summary_strong"
        else "This bucket does not support stronger claims without clearer direct support."
    )
    if bucket_name == "holdings_exposure" and evidence_depth_class in {"structured_summary_strong", "summary_backed_limited"}:
        unsupported_text = "This bucket does not support line-item holdings conclusions."
    remaining = f"Missing support: {', '.join(missing_fields[:4])}." if missing_fields else "No major missing fields remain inside this bucket."
    return {
        "supports": support_text,
        "does_not_support": unsupported_text,
        "remains_missing": remaining,
        "supported_fields": supported_fields,
        "missing_fields": missing_fields,
    }


def _build_bucket_support(candidate: dict[str, Any]) -> dict[str, Any]:
    field_truth = dict(candidate.get("field_truth") or {})
    evidence_buckets = dict(candidate.get("evidence_buckets") or {})
    evidence_depth_class = _candidate_evidence_depth_class(candidate)
    bucket_support: dict[str, Any] = {}
    for bucket_name, field_names in _BUCKET_FIELD_SUPPORT_MAP.items():
        bucket = dict(evidence_buckets.get(bucket_name) or {})
        state = str(bucket.get("state") or "missing")
        source_class = str(bucket.get("source_class") or "")
        supported_entries: list[dict[str, Any]] = []
        missing_fields: list[str] = []
        failure_reasons: list[str] = list(bucket.get("failure_reasons") or [])
        for field_name in field_names:
            row = dict(field_truth.get(field_name) or {})
            if row and str(row.get("missingness_reason") or "") == "populated" and row.get("resolved_value") not in {None, "", "unknown"}:
                supported_entries.append(_field_support_entry(field_name, row))
            else:
                missing_fields.append(field_name)
                override_annotation = dict(row.get("override_annotation") or {})
                failure = str(override_annotation.get("bucket_failure_reason") or "")
                if failure:
                    failure_reasons.extend(part for part in failure.split(",") if part)
        supported_entries = sorted(supported_entries, key=_support_sort_key)
        primary_support = supported_entries[0] if supported_entries else None
        bucket_support[bucket_name] = {
            "bucket_name": bucket_name,
            "bucket_state": state,
            "evidence_depth_class": evidence_depth_class,
            "primary_source_url": (primary_support or {}).get("source_url"),
            "primary_source_name": (primary_support or {}).get("source_name"),
            "primary_source_kind": (primary_support or {}).get("source_kind") or source_class or None,
            "primary_source_tier": (primary_support or {}).get("source_tier"),
            "observed_at": (primary_support or {}).get("observed_at"),
            "retrieved_at": (primary_support or {}).get("retrieved_at"),
            "extraction_methods": list(dict.fromkeys(str(item.get("extraction_method") or "") for item in supported_entries if str(item.get("extraction_method") or "").strip()))[:4],
            "supported_fields": [str(item.get("field_name") or "") for item in supported_entries],
            "missing_fields": missing_fields,
            "field_confidence": {
                str(item.get("field_name") or ""): {
                    "confidence": item.get("confidence"),
                    "evidence_class": item.get("evidence_class"),
                    "completeness_state": item.get("completeness_state"),
                }
                for item in supported_entries
            },
            "field_support": supported_entries[:8],
            "claim_limits": _bucket_claim_limits(
                bucket_name=bucket_name,
                state=state,
                evidence_depth_class=evidence_depth_class,
                source_class=source_class,
                tax_confidence=str(dict(candidate.get("tax_truth") or {}).get("tax_confidence") or ""),
            ),
            "failure_reasons": list(dict.fromkeys(item for item in failure_reasons if item))[:6],
            "interpretation_summary": _bucket_interpretation_summary(
                bucket_name=bucket_name,
                bucket_state=state,
                evidence_depth_class=evidence_depth_class,
                supported_fields=[str(item.get("field_name") or "") for item in supported_entries],
                missing_fields=missing_fields,
                source_class=source_class,
            ),
        }
    return bucket_support


def _candidate_evidence_buckets(candidate: dict[str, Any]) -> dict[str, Any]:
    field_truth = dict(candidate.get("field_truth") or {})
    benchmark_assignment = dict(candidate.get("benchmark_assignment") or {})
    performance_metrics = dict(candidate.get("performance_metrics") or {})
    tax_truth = _candidate_tax_truth_profile(candidate)
    latest_fetch_status = dict(candidate.get("latest_fetch_status") or {})
    holdings_counts = _bucket_field_counts(
        field_truth,
        ["us_weight", "em_weight", "top_10_concentration", "holdings_count"],
    )
    holdings_direct_count = int(holdings_counts.get("direct_count") or 0)
    holdings_summary_count = int(holdings_counts.get("summary_count") or 0)
    holdings_any_count = holdings_direct_count + holdings_summary_count + int(holdings_counts.get("fallback_count") or 0)
    benchmark_fit = str(benchmark_assignment.get("benchmark_fit_type") or "").strip().lower()
    benchmark_authority = str(benchmark_assignment.get("benchmark_authority_level") or "").strip().lower()
    benchmark_source = str(benchmark_assignment.get("benchmark_source_type") or benchmark_assignment.get("benchmark_source") or "").strip().lower()
    benchmark_state = (
        "complete"
        if benchmark_fit == "strong_fit" and benchmark_authority in {"strong", "high", "direct"}
        else "partial"
        if benchmark_fit == "acceptable_proxy"
        else "proxy_only"
        if benchmark_fit in {"weak_proxy", "mismatched"} or "proxy" in benchmark_source
        else "missing"
    )
    performance_state = "missing"
    if performance_metrics:
        performance_state = "complete" if benchmark_state == "complete" else "partial" if benchmark_state == "partial" else "proxy_only"
    holdings_state = "missing"
    holdings_source_class = "missing"
    holdings_parse_confidence = "low"
    if holdings_direct_count >= 4:
        holdings_state = "complete"
        holdings_source_class = "direct_holdings"
        holdings_parse_confidence = "high"
    elif holdings_direct_count > 0:
        holdings_state = "partial"
        holdings_source_class = "direct_holdings"
        holdings_parse_confidence = "medium"
    elif holdings_summary_count >= 2:
        holdings_state = "partial"
        holdings_source_class = "factsheet_summary"
        holdings_parse_confidence = "medium"
    elif holdings_any_count > 0:
        holdings_state = "proxy_only"
        holdings_source_class = "fallback_or_proxy"
    liquidity_counts = _bucket_field_counts(field_truth, ["aum", "volume_30d_avg", "bid_ask_spread_proxy"])
    liquidity_direct = int(liquidity_counts.get("direct_count") or 0)
    liquidity_truth = int(liquidity_counts.get("truth_count") or 0)
    liquidity_support = int(liquidity_counts.get("fallback_count") or 0)
    liquidity_state = "missing"
    liquidity_source_class = "missing"
    if liquidity_direct >= 1 and liquidity_truth >= 2:
        liquidity_state = "complete"
        liquidity_source_class = "issuer_doc_plus_market"
    elif liquidity_truth >= 1:
        liquidity_state = "partial"
        liquidity_source_class = "issuer_or_truth_grade_partial"
    elif liquidity_support > 0:
        liquidity_state = "proxy_only"
        liquidity_source_class = "support_or_proxy_only"
    evidence_buckets = {
        "identity_wrapper": {
            "state": _bucket_state(
                _resolved_field_count(
                    field_truth,
                    ["isin", "domicile", "fund_name", "primary_trading_currency"],
                    require_truth_grade=True,
                ),
                4,
            ),
        },
        "holdings_exposure": {
            "state": holdings_state,
            "source_class": holdings_source_class,
            "parse_confidence": holdings_parse_confidence,
            "failure_reasons": list(holdings_counts.get("failure_reasons") or []),
        },
        "expense_and_cost": {
            "state": _bucket_state(
                _resolved_field_count(
                    field_truth,
                    ["expense_ratio", "tracking_difference_1y"],
                    require_truth_grade=True,
                ),
                2,
            ),
        },
        "liquidity_and_aum": {
            "state": liquidity_state,
            "source_class": liquidity_source_class,
            "failure_reasons": list(liquidity_counts.get("failure_reasons") or []),
        },
        "benchmark_support": {
            "state": benchmark_state,
        },
        "performance_relative_support": {
            "state": performance_state,
        },
        "tax_posture": {
            "state": (
                "complete"
                if tax_truth.get("tax_score") is not None and str(tax_truth.get("tax_confidence") or "") == "high"
                else "partial"
                if tax_truth.get("tax_score") is not None
                else "missing"
            ),
        },
    }
    if latest_fetch_status:
        evidence_buckets["source_refresh"] = {
            "state": "complete" if str(latest_fetch_status.get("status") or "").lower() == "success" else "partial"
        }
    evidence_buckets["tax_posture"]["confidence"] = tax_truth.get("tax_confidence")
    evidence_buckets["benchmark_support"]["support_class"] = benchmark_state
    evidence_buckets["performance_relative_support"]["support_class"] = performance_state
    return evidence_buckets


def _candidate_truth_quality_summary(candidate: dict[str, Any]) -> dict[str, Any]:
    evidence_buckets = dict(candidate.get("evidence_buckets") or {})
    evidence_depth_class = _candidate_evidence_depth_class(candidate)
    critical_buckets = [
        "identity_wrapper",
        "holdings_exposure",
        "expense_and_cost",
        "benchmark_support",
        "tax_posture",
    ]
    supporting_buckets = [
        "liquidity_and_aum",
        "performance_relative_support",
    ]
    missing_critical = [
        bucket_name
        for bucket_name in critical_buckets
        if str(dict(evidence_buckets.get(bucket_name) or {}).get("state") or "missing") in {"missing", "proxy_only"}
    ]
    partial_critical = [
        bucket_name
        for bucket_name in critical_buckets
        if str(dict(evidence_buckets.get(bucket_name) or {}).get("state") or "") == "partial"
    ]
    weak_support = [
        bucket_name
        for bucket_name in supporting_buckets
        if str(dict(evidence_buckets.get(bucket_name) or {}).get("state") or "missing") in {"missing", "proxy_only", "partial"}
    ]
    if not missing_critical and not partial_critical:
        overall_state = "truth_ready"
        summary = "Critical evidence buckets are complete enough for stronger recommendation interpretation."
    elif missing_critical:
        overall_state = "truth_gap"
        summary = (
            "Critical evidence buckets are still missing or proxy-only, so recommendation confidence should stay constrained."
        )
    else:
        overall_state = "review_limited"
        summary = "Critical evidence exists, but partial bucket coverage still limits stronger recommendation confidence."
    return {
        "overall_state": overall_state,
        "summary": summary,
        "evidence_depth_class": evidence_depth_class,
        "critical_buckets": critical_buckets,
        "supporting_buckets": supporting_buckets,
        "missing_critical_buckets": missing_critical,
        "partial_critical_buckets": partial_critical,
        "weak_support_buckets": weak_support,
    }


def _recommendation_state_priority(state: str) -> int:
    return {
        "recommended_primary": 0,
        "recommended_backup": 1,
        "watchlist_only": 2,
        "rejected_inferior_to_selected": 3,
        "research_only": 4,
        "rejected_data_insufficient": 5,
        "rejected_policy_failure": 6,
        "removed_from_deliverable_set": 7,
    }.get(str(state), 8)


def _recommendation_state_label(state: Any, *, fallback: Any = None) -> str:
    text = str(state or fallback or "unknown").strip()
    return text.replace("_", " ")


def _resolve_evaluation_mode(*, refresh_monitor: dict[str, Any]) -> str:
    status = str(refresh_monitor.get("status") or "never_run")
    stale_state = str(refresh_monitor.get("stale_state") or "stale")
    if status in {"succeeded", "partial"} and stale_state in {"fresh", "aging"} and refresh_monitor.get("last_success_at"):
        return "market_context_refreshed"
    return "design_only"


_SUPPLEMENTAL_CANDIDATE_METRICS: dict[str, dict[str, Any]] = {
    "VWRA": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged global developed and emerging equity basket",
        "us_weight_pct": 61.14,
        "top10_concentration_pct": 24.60,
        "holdings_count": 3797,
        "tech_weight_pct": 30.90,
        "tracking_difference_note": "1 year tracking difference: -0.08%; 3 year: -0.09%; since inception: -0.10%.",
        "citations": [
            {
                "source_id": "vanguard_vwra_market_profile",
                "url": "https://www.vanguard.co.uk/professional/product/etf/equity/9679/ftse-all-world-ucits-etf-usd-accumulating",
                "retrieved_at": "2026-03-06T15:00:00+00:00",
                "importance": "Market allocation and holdings summary",
                "published_at": "2026-01-31",
            },
            {
                "source_id": "vanguard_vwra_tracking_profile",
                "url": "https://fund-docs.vanguard.com/FTSE_All-World_UCITS_ETF_USD_Accumulating_9679_EU_INT_UK_EN.pdf",
                "retrieved_at": "2026-03-06T15:00:00+00:00",
                "importance": "Factsheet tracking-difference summary",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "VWRL": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged global developed and emerging equity basket",
        "us_weight_pct": 61.14,
        "top10_concentration_pct": 24.60,
        "holdings_count": 3797,
        "tech_weight_pct": 30.90,
        "tracking_difference_note": "Top-10 concentration and market-allocation profile sourced from the shared FTSE All-World fund-level holdings summary as of 31 Jan 2026.",
        "citations": [
            {
                "source_id": "vanguard_vwrl_market_profile",
                "url": "https://www.vanguard.co.uk/professional/product/etf/equity/9505/ftse-all-world-ucits-etf-usd-distributing",
                "retrieved_at": "2026-03-06T15:00:00+00:00",
                "importance": "Market allocation and holdings summary",
                "published_at": "2026-01-31",
            },
            {
                "source_id": "vanguard_vwrl_tracking_profile",
                "url": "https://fund-docs.vanguard.com/FTSE_All-World_UCITS_ETF_USD_Distributing_9505_EU_INT_UK_EN.pdf",
                "retrieved_at": "2026-03-06T15:00:00+00:00",
                "importance": "Factsheet holdings summary",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "IWDA": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged developed-market equity basket",
        "us_weight_pct": 71.17,
        "top10_concentration_pct": 25.39,
        "em_weight_pct": 0.00,
        "holdings_count": 1512,
        "tracking_difference_note": "1 year tracking difference: -0.05%; 3 year: -0.04%; 5 year: -0.03%.",
        "citations": [
            {
                "source_id": "ishares_iwda_holdings_profile",
                "url": "https://www.ishares.com/ch/individual/en/products/251882/ishares-core-msci-world-ucits-etf/1495092304805.ajax?dataType=fund&fileName=IWDA_holdings&fileType=csv",
                "retrieved_at": "2026-03-06T15:00:00+00:00",
                "importance": "Holdings download and country-weight profile",
                "published_at": "2026-03-04",
            }
        ],
    },
    "SSAC": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged global developed and emerging equity basket",
        "us_weight_pct": 63.09,
        "top10_concentration_pct": 23.35,
        "em_weight_pct": 8.38,
        "tracking_difference_note": "Official January 2026 factsheet performance table implies benchmark-relative spread of +0.07% over 1 year, +0.01% annualized over 3 years, +0.09% annualized over 5 years, and -0.14% annualized since inception.",
        "citations": [
            {
                "source_id": "ishares_ssac_holdings_profile",
                "url": "https://www.ishares.com/ch/individual/en/products/251850/ishares-msci-acwi-ucits-etf/1495092304805.ajax?dataType=fund&fileName=SSACCHF_holdings&fileType=csv",
                "retrieved_at": "2026-03-06T15:00:00+00:00",
                "importance": "Holdings download and country-weight profile",
                "published_at": "2026-03-04",
            },
            {
                "source_id": "ishares_ssac_factsheet_profile",
                "url": "https://www.ishares.com/uk/individual/en/literature/fact-sheet/ssac-ishares-msci-acwi-ucits-etf-fund-fact-sheet-en-gb.pdf?switchLocale=y&siteEntryPassthrough=true",
                "retrieved_at": "2026-03-07T09:00:00+00:00",
                "importance": "Official factsheet with benchmark-relative performance table",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "CSPX": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged US large-cap equity basket",
        "us_weight_pct": 100.0,
        "em_weight_pct": 0.0,
        "tracking_difference_note": "Official January 2026 factsheet performance table implies benchmark-relative spread of +0.15% over 1 year, +0.18% annualized over 3 years, +0.19% annualized over 5 years, and +0.25% annualized since inception.",
        "citations": [
            {
                "source_id": "ishares_cspx_factsheet_profile",
                "url": "https://www.ishares.com/uk/individual/en/literature/fact-sheet/cspx-ishares-core-s-p-500-ucits-etf-fund-fact-sheet-en-gb.pdf?switchLocale=y&siteEntryPassthrough=true",
                "retrieved_at": "2026-03-07T09:00:00+00:00",
                "importance": "Official factsheet with benchmark-relative performance table",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "VEVE": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged developed-market equity basket",
        "em_weight_pct": 0.0,
        "us_weight_pct": 68.25,
        "top10_concentration_pct": 26.90,
        "holdings_count": 2000,
        "tracking_difference_note": "Official factsheet benchmark-relative return spread as of 31 Jan 2026: 1 year +0.12%, 3 years annualized +0.16%, 5 years annualized +0.15%, since inception annualized +0.10%.",
        "citations": [
            {
                "source_id": "vanguard_veve_market_profile",
                "url": "https://www.vanguard.co.uk/professional/product/etf/equity/9675/ftse-developed-world-ucits-etf-usd-distributing",
                "retrieved_at": "2026-03-06T16:20:00+00:00",
                "importance": "Issuer page with country-weight and top-holdings summary",
                "published_at": "2026-01-31",
            },
            {
                "source_id": "vanguard_veve_factsheet_profile",
                "url": "https://fund-docs.vanguard.com/FTSE_Developed_World_UCITS_ETF_USD_Distributing_9527_CH_RET_EN.pdf",
                "retrieved_at": "2026-03-06T16:20:00+00:00",
                "importance": "Official factsheet with top-10 concentration and holdings count",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "EIMI": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged emerging-market equity basket",
        "us_weight_pct": 0.0,
        "em_weight_pct": 100.0,
        "tracking_difference_note": "Official January 2026 factsheet performance table implies benchmark-relative spread of +0.21% over 1 year, +0.08% annualized over 3 years, +0.06% annualized over 5 years, and -0.03% annualized since inception.",
        "citations": [
            {
                "source_id": "ishares_eimi_factsheet_profile",
                "url": "https://www.ishares.com/uk/individual/en/literature/fact-sheet/eimi-ishares-core-msci-em-imi-ucits-etf-fund-fact-sheet-en-gb.pdf?switchLocale=y&siteEntryPassthrough=true",
                "retrieved_at": "2026-03-07T09:00:00+00:00",
                "importance": "Official factsheet with benchmark-relative performance table",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "VFEA": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged emerging-market equity basket",
        "us_weight_pct": 0.0,
        "em_weight_pct": 100.0,
        "top10_concentration_pct": 28.40,
        "holdings_count": 2288,
        "tracking_difference_note": "Official factsheet benchmark-relative return spread as of 31 Jan 2026: 1 year -0.52%, 3 years annualized -0.50%, 5 years annualized -0.42%, since inception annualized -0.42%.",
        "citations": [
            {
                "source_id": "vanguard_vfea_factsheet_profile",
                "url": "https://fund-docs.vanguard.com/FTSE_Emerging_Markets_UCITS_ETF_USD_Accumulating_9678_EU_INT_UK_EN.pdf",
                "retrieved_at": "2026-03-06T16:20:00+00:00",
                "importance": "Official factsheet with top-10 concentration and holdings count",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "HMCH": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged China equity basket",
        "us_weight_pct": 0.0,
        "em_weight_pct": 100.0,
        "top10_concentration_pct": 46.53,
        "tracking_difference_note": "Tracking difference reported in the official factsheet: YTD -0.02%, 1 month -0.02%, 3 months -0.04%, 6 months -0.12%, 1 year -0.37%, 3 years annualized -0.30%, 5 years annualized -0.23%, 10 years annualized -0.45%.",
        "citations": [
            {
                "source_id": "hsbc_hmch_factsheet_profile",
                "url": "https://www.assetmanagement.hsbc.nl/api/v1/download/document/ie00b44t3h88/nl/en/factsheet",
                "retrieved_at": "2026-03-06T16:20:00+00:00",
                "importance": "Official factsheet with tracking-difference history and top-10 constituents",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "XCHA": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged China equity basket",
        "us_weight_pct": 0.0,
        "em_weight_pct": 100.0,
        "top10_concentration_pct": 46.91,
        "tracking_difference_note": "Official Xtrackers annual report for the reported year ended 2020 shows fund return of 17.80% versus index return of 17.68%, implying a +0.12% benchmark-relative spread for that period.",
        "citations": [
            {
                "source_id": "xtrackers_xcha_factsheet_profile",
                "url": "https://etf.dws.com/LUX/ENG/Download/Factsheet/LU0514695690/-/MSCI-China-UCITS-ETF",
                "retrieved_at": "2026-03-06T16:20:00+00:00",
                "importance": "Official factsheet with top-10 constituents",
                "published_at": "2026-01-30",
                "factsheet_asof": "2026-01-30",
            },
            {
                "source_id": "xtrackers_xcha_annual_report_profile",
                "url": "https://etf.dws.com/en-lu/LU0514695690-msci-china-ucits-etf-1c/",
                "retrieved_at": "2026-03-07T09:00:00+00:00",
                "importance": "Official issuer performance materials and annual report context",
                "published_at": "2020-12-31",
            },
        ],
    },
    "AGGU": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "unhedged global investment-grade bond basket",
        "tracking_difference_note": "Official January 2026 factsheet performance table shows fund returns trailing the benchmark by 0.71% over 1 month, 1.21% over 3 months, 1.02% over 6 months, 3.93% over 1 year, 1.03% annualized over 3 years, 2.02% annualized over 5 years, and 1.23% annualized since inception.",
        "citations": [
            {
                "source_id": "ishares_aggu_factsheet_profile",
                "url": "https://www.ishares.com/uk/individual/en/literature/fact-sheet/aggu-ishares-core-global-aggregate-bond-ucits-etf-fund-fact-sheet-en-gb.pdf?switchLocale=y&siteEntryPassthrough=true",
                "retrieved_at": "2026-03-07T09:00:00+00:00",
                "importance": "Official factsheet with benchmark-relative performance table",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "VAGU": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "global investment-grade bond basket with USD-hedged share-class implementation",
        "tracking_difference_note": "Official factsheet benchmark-relative return spread as of 31 Jan 2026: 1 year -0.09%, 3 years annualized -0.10%, 5 years annualized -0.09%, since inception annualized -0.08%.",
        "citations": [
            {
                "source_id": "vanguard_vagu_factsheet_profile",
                "url": "https://fund-docs.vanguard.com/Global_Aggregate_Bond_UCITS_ETF_USD_Hedged_Accumulating_9600_CB_INT_EN.pdf",
                "retrieved_at": "2026-03-06T16:20:00+00:00",
                "importance": "Official factsheet with benchmark-relative performance summary",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "A35": {
        "trading_currency": "SGD",
        "underlying_currency_exposure": "SGD investment-grade bond basket",
        "tracking_difference_note": "Official fund page performance table as of 31 Jan 2026 implies benchmark-relative spread of -0.33% over 1 year, -0.19% annualized over 3 years, -0.27% annualized over 5 years, and -0.29% annualized since inception.",
        "citations": [
            {
                "source_id": "amova_a35_fund_page_profile",
                "url": "https://sg.amova-am.com/general/funds/detail/abf-singapore-bond-index-fund",
                "retrieved_at": "2026-03-07T09:00:00+00:00",
                "importance": "Official fund page with benchmark-relative performance table and tracking-error disclosure",
                "published_at": "2026-01-31",
            },
        ],
    },
    "IB01": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "short-duration US Treasury bill basket",
        "holdings_count": 32,
        "tracking_difference_note": "Official factsheet benchmark-relative return spread as of 31 Jan 2026: 1 year +0.00%, 3 years annualized -0.01%, 5 years annualized -0.03%, since inception annualized -0.03%.",
        "citations": [
            {
                "source_id": "ishares_ib01_factsheet_profile",
                "url": "https://www.blackrock.com/americas-offshore/en/literature/fact-sheet/ib01-ishares-treasury-bond-0-1yr-ucits-etf-fund-fact-sheet-en-lm.pdf",
                "retrieved_at": "2026-03-06T16:20:00+00:00",
                "importance": "Official factsheet with holdings count and short-duration profile",
                "published_at": "2026-01-31",
                "factsheet_asof": "2026-01-31",
            },
        ],
    },
    "BIL": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "short-duration US Treasury bill basket",
        "tracking_difference_note": "Official total-return table as of 31 Dec 2025 implies benchmark-relative spread of -0.14% over 1 year, -0.14% annualized over 3 years, -0.15% annualized over 5 years, and -0.15% annualized over 10 years.",
        "citations": [
            {
                "source_id": "ssga_bil_factsheet_profile",
                "url": "https://www.ssga.com/library-content/products/factsheets/etfs/us/factsheet-us-en-bil.pdf",
                "retrieved_at": "2026-03-06T16:20:00+00:00",
                "importance": "Official factsheet with benchmark-relative total return table",
                "published_at": "2025-12-31",
                "factsheet_asof": "2025-12-31",
            },
        ],
    },
    "SGOV": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "short-duration US Treasury bill basket",
    },
    "BILS": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "short-duration US Treasury bill basket",
    },
    "SGLN": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "physically backed gold exposure",
    },
    "CMOD": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "broad commodity futures basket",
    },
    "IWDP": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "developed-market REIT equity basket",
        "em_weight_pct": 0.0,
    },
    "DBMF": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "multi-asset managed-futures strategy exposure",
    },
    "KMLM": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "multi-asset managed-futures strategy exposure",
    },
    "TAIL": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "tail-risk ETF with option-overlay and short-duration bond collateral",
    },
    "CAOS": {
        "trading_currency": "USD",
        "underlying_currency_exposure": "rules-based tail-risk strategy with US equity downside focus",
    },
}

_ISHARES_HOLDINGS_SOURCE_URLS: dict[str, str] = {
    "IWDA": "https://www.ishares.com/ch/individual/en/products/251882/ishares-core-msci-world-ucits-etf/1495092304805.ajax?dataType=fund&fileName=IWDA_holdings&fileType=csv",
    "SSAC": "https://www.ishares.com/ch/individual/en/products/251850/ishares-msci-acwi-ucits-etf/1495092304805.ajax?dataType=fund&fileName=SSACCHF_holdings&fileType=csv",
    "EIMI": "https://www.ishares.com/ch/individual/en/products/264659/ishares-core-msci-em-imi-ucits-etf/1495092304805.ajax?dataType=fund&fileName=EIMI_holdings&fileType=csv",
    "AGGU": "https://www.ishares.com/ch/individual/en/products/291773/ishares-core-global-aggregate-bond-ucits-etf/1495092304805.ajax?dataType=fund&fileName=AGGU_holdings&fileType=csv",
    "CSPX": "https://www.ishares.com/ch/individual/en/products/253743/ishares-core-sp-500-ucits-etf/1495092304805.ajax?dataType=fund&fileName=CSPX_holdings&fileType=csv",
    "IB01": "https://www.ishares.com/ch/individual/en/products/307243/ishares-treasury-bond-0-1yr-ucits-etf/1495092304805.ajax?dataType=fund&fileName=IB01_holdings&fileType=csv",
}
_EQUITY_COMPOSITION_SLEEVES = {
    "global_equity_core",
    "developed_ex_us_optional",
    "emerging_markets",
    "china_satellite",
}
_CURRENCY_COMPARISON_SLEEVES = _EQUITY_COMPOSITION_SLEEVES | {"ig_bonds", "cash_bills"}
_TRACKING_RELEVANT_INSTRUMENTS = {"etf_ucits", "etf_us"}
_TRACKING_COMPARISON_SLEEVES = _EQUITY_COMPOSITION_SLEEVES | {"ig_bonds", "cash_bills"}
_DEVELOPED_MARKET_LOCATIONS = {
    "Australia",
    "Austria",
    "Belgium",
    "Canada",
    "Denmark",
    "Finland",
    "France",
    "Germany",
    "Hong Kong",
    "Ireland",
    "Israel",
    "Italy",
    "Japan",
    "Netherlands",
    "New Zealand",
    "Norway",
    "Portugal",
    "Singapore",
    "Spain",
    "Sweden",
    "Switzerland",
    "United Kingdom",
    "United States",
}


def _apply_supplemental_candidate_metadata(candidate: dict[str, Any]) -> dict[str, Any]:
    symbol = str(candidate.get("symbol") or "").upper()
    supplement = dict(_SUPPLEMENTAL_CANDIDATE_METRICS.get(symbol) or {})
    if not supplement:
        return candidate

    for key in (
        "trading_currency",
        "primary_trading_currency",
        "primary_listing",
        "primary_listing_exchange",
        "underlying_currency_exposure",
        "us_weight_pct",
        "top10_concentration_pct",
        "em_weight_pct",
        "holdings_count",
        "tech_weight_pct",
        "tracking_difference_note",
        "factsheet_asof",
        "aum_usd",
    ):
        value = supplement.get(key)
        if value is not None:
            candidate[key] = value

    if not candidate.get("primary_trading_currency") and candidate.get("trading_currency") not in {None, "", "unknown"}:
        candidate["primary_trading_currency"] = candidate.get("trading_currency")
    if not candidate.get("primary_listing") and candidate.get("primary_listing_exchange"):
        candidate["primary_listing"] = candidate.get("primary_listing_exchange")

    tracking_note = str(candidate.get("tracking_difference_note") or "").strip()
    if tracking_note:
        patterns = {
            "tracking_difference_1y": [
                r"1 year[^-+0-9]*([+-]?\d+(?:\.\d+)?)%",
                r"over 1 year[^-+0-9]*([+-]?\d+(?:\.\d+)?)%",
            ],
            "tracking_difference_3y": [
                r"3 years annualized[^-+0-9]*([+-]?\d+(?:\.\d+)?)%",
                r"annualized over 3 years[^-+0-9]*([+-]?\d+(?:\.\d+)?)%",
            ],
            "tracking_difference_5y": [
                r"5 years annualized[^-+0-9]*([+-]?\d+(?:\.\d+)?)%",
                r"annualized over 5 years[^-+0-9]*([+-]?\d+(?:\.\d+)?)%",
            ],
        }
        for field_name, regexes in patterns.items():
            if candidate.get(field_name) is not None:
                continue
            for regex in regexes:
                match = re.search(regex, tracking_note, re.IGNORECASE)
                if match:
                    candidate[field_name] = float(match.group(1))
                    break

    existing_urls = {str(item.get("url") or "").strip() for item in list(candidate.get("citations") or [])}
    for entry in list(supplement.get("citations") or []):
        url = str(entry.get("url") or "").strip()
        if not url or url in existing_urls:
            continue
        candidate.setdefault("citations", []).append(
            _citation(
                source_id=str(entry.get("source_id") or f"{symbol.lower()}_supplemental"),
                url=url,
                retrieved_at=datetime.fromisoformat(str(entry.get("retrieved_at"))),
                importance=str(entry.get("importance") or "Supplemental candidate metadata"),
                published_at=str(entry.get("published_at") or "") or None,
                factsheet_asof=str(entry.get("factsheet_asof") or "") or None,
            )
        )
        existing_urls.add(url)

    candidate["primary_sources"] = list(candidate.get("citations") or [])
    return candidate


def _configure_supported_candidate_sources(conn: Any) -> None:
    _ensure_etf_tables(conn)
    for symbol, source_url in _ISHARES_HOLDINGS_SOURCE_URLS.items():
        try:
            configure_etf_data_source(
                conn,
                etf_symbol=symbol,
                data_type="holdings",
                source_id=f"ishares_{symbol.lower()}_holdings",
                source_url_template=source_url,
                fetch_method="csv_download",
                update_frequency="weekly",
            )
        except sqlite3.OperationalError:
            continue
    for source_config in list_etf_source_configs():
        symbol = str(source_config.get("etf_symbol") or "").strip().upper()
        if not symbol:
            continue
        data_sources = dict(source_config.get("data_sources") or {})
        factsheet = dict(data_sources.get("factsheet") or {})
        if factsheet.get("url") or factsheet.get("url_template"):
            try:
                configure_etf_data_source(
                    conn,
                    etf_symbol=symbol,
                    data_type="factsheet",
                    source_id=str(factsheet.get("citation_source_id") or f"{symbol.lower()}_factsheet"),
                    source_url_template=str(factsheet.get("url_template") or factsheet.get("url") or ""),
                    fetch_method=str(factsheet.get("method") or "api_call"),
                    update_frequency=str(factsheet.get("frequency") or "daily"),
                )
            except sqlite3.OperationalError:
                continue
        market = dict(data_sources.get("market_data") or {})
        if market.get("url") or market.get("url_template"):
            try:
                configure_etf_data_source(
                    conn,
                    etf_symbol=symbol,
                    data_type="market_data",
                    source_id=str(market.get("citation_source_id") or f"{symbol.lower()}_market"),
                    source_url_template=str(market.get("url_template") or market.get("url") or ""),
                    fetch_method=str(market.get("method") or "api_call"),
                    update_frequency=str(market.get("frequency") or "daily"),
                )
            except sqlite3.OperationalError:
                continue


def _latest_holdings_asof(conn: Any, symbol: str) -> str | None:
    row = conn.execute(
        "SELECT MAX(asof_date) AS asof_date FROM etf_holdings WHERE etf_symbol = ?",
        (symbol,),
    ).fetchone()
    value = str(row["asof_date"] or "").strip() if row else ""
    return value or None


def _has_resolved_candidate_truth(conn: Any, symbol: str) -> bool:
    ensure_candidate_truth_tables(conn)
    row = conn.execute(
        """
        SELECT 1
        FROM candidate_field_current
        WHERE candidate_symbol = ?
        LIMIT 1
        """,
        (str(symbol or "").strip().upper(),),
    ).fetchone()
    return row is not None


def _maybe_refresh_live_candidate_metadata(conn: Any, *, symbol: str, now: datetime) -> None:
    source_config = get_etf_source_config(symbol) or {}
    if symbol not in _ISHARES_HOLDINGS_SOURCE_URLS and not source_config:
        return
    if _has_resolved_candidate_truth(conn, symbol):
        # Read-path payload assembly should use persisted truth, not recompute it
        # candidate-by-candidate on every request. Background refresh and explicit
        # refresh endpoints own ongoing upkeep.
        return
    latest_dates = [
        _latest_holdings_asof(conn, symbol),
        str((get_etf_factsheet_history_summary(symbol, conn) or {}).get("latest_asof_date") or "").strip() or None,
        str((get_preferred_market_history_summary(symbol, conn) or {}).get("latest_asof_date") or "").strip() or None,
    ]
    latest_dates = [value for value in latest_dates if value]
    if latest_dates:
        try:
            latest_dt = datetime.fromisoformat(max(latest_dates))
            if latest_dt.date() >= (now - timedelta(days=5)).date():
                refresh_registry_candidate_truth(conn, symbol=symbol)
                return
        except ValueError:
            pass
    try:
        if symbol in _ISHARES_HOLDINGS_SOURCE_URLS:
            fetch_ishares_holdings(symbol, conn)
        refresh_etf_data(symbol, settings=Settings.from_env())
    except Exception:  # noqa: BLE001
        pass
    try:
        refresh_registry_candidate_truth(conn, symbol=symbol)
    except Exception:  # noqa: BLE001
        return


def _derive_live_candidate_metadata(conn: Any, *, symbol: str) -> dict[str, Any]:
    latest_asof = _latest_holdings_asof(conn, symbol)
    payload: dict[str, Any] = {}
    if latest_asof:
        rows = conn.execute(
            """
            SELECT security_name, weight_pct, sector, country, asset_class, source_url, retrieved_at
            FROM etf_holdings
            WHERE etf_symbol = ? AND asof_date = ?
            ORDER BY weight_pct DESC
            """,
            (symbol, latest_asof),
        ).fetchall()
        if rows:
            weights = [float(row["weight_pct"]) for row in rows]
            top10 = round(sum(weights[:10]), 2) if weights else None
            holdings_count = len(rows)
            us_weight = round(
                sum(float(row["weight_pct"]) for row in rows if str(row["country"] or "").strip() == "United States"),
                2,
            )
            tech_weight = round(
                sum(float(row["weight_pct"]) for row in rows if "information technology" in str(row["sector"] or "").lower()),
                2,
            )
            equity_rows = [row for row in rows if str(row["asset_class"] or "").strip().lower() in {"equity", ""}]
            em_weight: float | None = None
            if equity_rows and any(str(row["country"] or "").strip() for row in equity_rows):
                em_weight = round(
                    sum(
                        float(row["weight_pct"])
                        for row in equity_rows
                        if str(row["country"] or "").strip()
                        and str(row["country"] or "").strip() not in _DEVELOPED_MARKET_LOCATIONS
                    ),
                    2,
                )
            payload.update(
                {
                    "us_weight_pct": us_weight if us_weight > 0 or symbol in {"CSPX", "EIMI", "VFEA", "HMCH", "XCHA"} else None,
                    "top10_concentration_pct": top10,
                    "em_weight_pct": em_weight,
                    "holdings_count": holdings_count,
                    "tech_weight_pct": tech_weight if tech_weight > 0 else None,
                    "citations": [
                        _citation(
                            source_id=f"{symbol.lower()}_live_holdings",
                            url=str(rows[0]["source_url"]),
                            retrieved_at=datetime.fromisoformat(str(rows[0]["retrieved_at"])),
                            importance=f"Live holdings profile for {symbol}",
                            published_at=latest_asof,
                        )
                    ],
                }
            )

    factsheet = get_latest_etf_factsheet_metrics(symbol, conn)
    if factsheet:
        if factsheet.get("aum_usd") is not None:
            payload["aum_usd"] = float(factsheet["aum_usd"])
        tracking_bits = []
        for label, key in (("1 year", "tracking_difference_1y"), ("3 year", "tracking_difference_3y"), ("5 year", "tracking_difference_5y")):
            value = factsheet.get(key)
            if value is not None:
                tracking_bits.append(f"{label}: {float(value):+.2f}%")
        if tracking_bits:
            payload["tracking_difference_note"] = "Tracking difference " + "; ".join(tracking_bits) + "."
        payload.setdefault("citations", []).append(
            _citation(
                source_id=f"{symbol.lower()}_live_factsheet",
                url=str(dict(factsheet.get("citation") or {}).get("source_url") or ""),
                retrieved_at=datetime.fromisoformat(str(dict(factsheet.get("citation") or {}).get("retrieved_at"))),
                importance=f"Latest factsheet metrics for {symbol}",
                published_at=str(factsheet.get("asof_date") or "") or None,
                factsheet_asof=str(factsheet.get("asof_date") or "") or None,
            )
        )

    market = get_preferred_latest_market_data(symbol, conn)
    if market and market.get("bid_ask_spread_bps") is not None:
        payload["bid_ask_spread_proxy"] = float(market["bid_ask_spread_bps"])
        if market.get("volume_30d_avg") is not None:
            payload["volume_30d_avg"] = float(market["volume_30d_avg"])
        payload.setdefault("citations", []).append(
            _citation(
                source_id=f"{symbol.lower()}_live_market",
                url=str(dict(market.get("citation") or {}).get("source_url") or ""),
                retrieved_at=datetime.fromisoformat(str(dict(market.get("citation") or {}).get("retrieved_at"))),
                importance=f"Latest market microstructure metrics for {symbol}",
                published_at=str(market.get("asof_date") or "") or None,
            )
        )
    return payload


def _merge_candidate_metadata(candidate: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
    if not metadata:
        return candidate
    for key in (
        "trading_currency",
        "primary_trading_currency",
        "primary_listing",
        "primary_listing_exchange",
        "factsheet_asof",
        "market_data_asof",
        "underlying_currency_exposure",
        "us_weight_pct",
        "top10_concentration_pct",
        "em_weight_pct",
        "holdings_count",
        "tech_weight_pct",
        "tracking_difference_note",
        "tracking_difference_1y",
        "tracking_difference_3y",
        "tracking_difference_5y",
        "bid_ask_spread_proxy",
        "volume_30d_avg",
        "aum_usd",
        "effective_duration",
        "average_maturity",
        "yield_proxy",
        "credit_quality_mix",
        "government_vs_corporate_split",
        "interest_rate_sensitivity_proxy",
        "issuer_concentration_proxy",
        "weighted_average_maturity",
        "portfolio_quality_summary",
        "redemption_settlement_notes",
    ):
        value = metadata.get(key)
        if value is not None and value not in {"", "unknown"}:
            candidate[key] = value
    existing_urls = {str(item.get("url") or "").strip() for item in list(candidate.get("citations") or [])}
    for citation in list(metadata.get("citations") or []):
        url = str(citation.get("url") or "").strip()
        if url and url not in existing_urls:
            candidate.setdefault("citations", []).append(citation)
            existing_urls.add(url)
    candidate["primary_sources"] = list(candidate.get("citations") or [])
    return candidate


def _hydrate_blueprint_candidate_metadata(
    sleeves: list[dict[str, Any]],
    *,
    settings: Settings,
    now: datetime,
    conn: sqlite3.Connection,
) -> None:
    _configure_supported_candidate_sources(conn)
    refreshed_symbols: set[str] = set()
    for sleeve in sleeves:
        merged_candidates = []
        for candidate in list(sleeve.get("candidates") or []):
            merged = _apply_supplemental_candidate_metadata(dict(candidate))
            symbol = str(merged.get("symbol") or "").upper()
            object_type = str(merged.get("object_type") or LIVE_OBJECT_TYPE)
            if symbol and object_type == LIVE_OBJECT_TYPE and symbol not in refreshed_symbols:
                _maybe_refresh_live_candidate_metadata(conn, symbol=symbol, now=now)
                refreshed_symbols.add(symbol)
            merged = _merge_candidate_metadata(
                merged,
                _derive_live_candidate_metadata(conn, symbol=symbol),
            )
            merged_candidates.append(merged)
        sleeve["candidates"] = merged_candidates


def _load_previous_blueprint_payload(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT payload_json
        FROM blueprint_snapshots
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    try:
        return json.loads(str(row["payload_json"] or "{}"))
    except Exception:
        return None


def _find_payload_candidate(
    payload: dict[str, Any] | None,
    *,
    sleeve_key: str,
    symbol: str,
) -> dict[str, Any] | None:
    if not payload:
        return None
    for sleeve in list(payload.get("sleeves") or []):
        if str(sleeve.get("sleeve_key") or "") != sleeve_key:
            continue
        for candidate in list(sleeve.get("candidates") or []):
            if str(candidate.get("symbol") or "").upper() == symbol.upper():
                return dict(candidate)
    return None


def _sleeve_candidate_symbols(payload: dict[str, Any] | None, *, sleeve_key: str) -> set[str]:
    if not payload:
        return set()
    for sleeve in list(payload.get("sleeves") or []):
        if str(sleeve.get("sleeve_key") or "") != sleeve_key:
            continue
        return {
            str(candidate.get("symbol") or "").upper()
            for candidate in list(sleeve.get("candidates") or [])
            if str(candidate.get("symbol") or "").strip()
        }
    return set()


def _enrich_citation_with_etf_proofs(
    *,
    symbol: str,
    citation: dict[str, Any],
    candidate_domicile: str,  # Expected domicile from candidate definition (e.g., "IE")
    use_fixtures: bool = True,
) -> dict[str, Any]:
    """
    Enrich citation with ETF verification proofs from document parser.

    For ETF candidates, calls fetch_candidate_docs() to extract ISIN, domicile,
    TER, accumulating status from issuer factsheets, then adds those as proof
    fields to the citation.

    Normalizes extracted domicile to match candidate's domicile format for verification.
    Attempts enrichment for all symbols - fixture lookup will fail gracefully if not supported.
    """
    # Domicile normalization map: full name -> ISO code
    domicile_map = {
        "IRELAND": "IE",
        "LUXEMBOURG": "LU",
        "UNITED KINGDOM": "GB",
        "UNITED STATES": "US",
        "SINGAPORE": "SG",
    }

    # Permissive: attempt enrichment for all symbols
    # Fixture lookup will fail gracefully if symbol not supported
    try:
        doc_result = _fetch_candidate_docs_cached(symbol, use_fixtures=use_fixtures)

        if doc_result.get("status") == "failed":
            return citation

        extracted = doc_result.get("extracted", {})

        # Add proof fields to citation
        enriched = dict(citation)

        # Use identifier (ISIN or CUSIP)
        if extracted.get("identifier"):
            enriched["proof_identifier"] = str(extracted["identifier"])

        if extracted.get("domicile"):
            raw_domicile = str(extracted["domicile"]).strip().upper()
            # Normalize to match candidate's domicile format
            normalized = domicile_map.get(raw_domicile, candidate_domicile)
            enriched["proof_domicile"] = normalized

        if extracted.get("accumulating_status"):
            enriched["proof_share_class"] = str(extracted["accumulating_status"])

        if extracted.get("ter") is not None:
            enriched["proof_ter"] = float(extracted["ter"])

        if extracted.get("factsheet_date"):
            enriched["factsheet_asof"] = str(extracted["factsheet_date"])

        return enriched

    except Exception:  # noqa: BLE001
        # Fallback: return citation without enrichment if parser fails
        return citation


def _sg_score(*, symbol: str, domicile: str, expected_withholding_rate: float, expense_ratio: float, liquidity_score: float, us_situs_risk_flag: bool) -> dict[str, Any]:
    residency = TaxResidencyProfile(
        profile_id="sg_default_blueprint",
        tax_residency="SG",
        base_currency="SGD",
        dta_flags={"ireland_us_treaty_path": True},
        estate_risk_flags={"us_situs_cap_enabled": True},
    )
    instrument = InstrumentTaxProfile(
        instrument_id=symbol,
        domicile=domicile,
        us_dividend_exposure=True,
        expected_withholding_rate=expected_withholding_rate,
        us_situs_risk_flag=us_situs_risk_flag,
        expense_ratio=expense_ratio,
        liquidity_score=liquidity_score,
    )
    score = evaluate_instrument_for_sg(residency, instrument)
    return {
        "score": float(score.score),
        "breakdown": {
            "withholding_penalty": float(score.withholding_drag),
            "expense_penalty": round(expense_ratio * 100.0, 2),
            "liquidity_bonus": round(liquidity_score * 10.0, 2),
            "estate_risk_penalty": float(score.estate_risk_penalty),
        },
    }


def _candidate(
    *,
    symbol: str,
    name: str,
    instrument_type: str = "etf_ucits",
    domicile: str,
    accumulation: str,
    expense_ratio: float | None,
    liquidity_proxy: str,
    replication_method: str,
    us_situs_risk_flag: bool,
    expected_withholding_drag_estimate: float | None,
    rationale: str,
    citations: list[dict[str, Any]],
    liquidity_score: float | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Enrich ETF citations with verification proofs from document parser
    enriched_citations = []
    for i, citation in enumerate(citations):
        if i == 0 and instrument_type in {"etf_ucits", "etf_us"}:
            # First citation is typically issuer citation - enrich with proofs
            enriched = _enrich_citation_with_etf_proofs(
                symbol=symbol,
                citation=citation,
                candidate_domicile=domicile,  # Pass candidate's domicile for normalization
                use_fixtures=True,  # Use test fixtures for now
            )
            enriched_citations.append(enriched)
        else:
            enriched_citations.append(citation)

    sg_lens = None
    if (
        instrument_type in {"etf_ucits", "etf_us"}
        and expense_ratio is not None
        and expected_withholding_drag_estimate is not None
        and liquidity_score is not None
    ):
        sg_lens = _sg_score(
            symbol=symbol,
            domicile=domicile,
            expected_withholding_rate=expected_withholding_drag_estimate,
            expense_ratio=expense_ratio,
            liquidity_score=liquidity_score,
            us_situs_risk_flag=us_situs_risk_flag,
        )
    payload = {
        "symbol": symbol,
        "name": name,
        "instrument_type": instrument_type,
        "domicile": domicile,
        "accumulation_or_distribution": accumulation,
        "trading_currency": "unknown",
        "underlying_currency_exposure": "unknown",
        "hedged_share_class": False,
        "fx_hedged_flag": False,
        "jurisdiction_cost_model": "sg_ucits_default",
        "expense_ratio": expense_ratio,
        "liquidity_proxy": liquidity_proxy,
        "replication_method": replication_method,
        "securities_lending_policy": "unknown",
        "tracking_difference_note": None,
        "bid_ask_spread_proxy": None,
        "withholding_tax_exposure_note": "Unverified withholding-tax mechanics for this candidate.",
        "domicile_implication_note": "Unverified domicile implication details for this candidate.",
        "us_situs_estate_risk_flag": us_situs_risk_flag,
        "expected_withholding_drag_estimate": expected_withholding_drag_estimate,
        "rationale": rationale,
        "sg_lens": sg_lens,
        "citations": enriched_citations,
        "primary_sources": list(enriched_citations),
        "last_verified_at": datetime.now(UTC).isoformat(),
    }
    if instrument_type in {"etf_ucits", "etf_us"} and not payload.get("share_class_id"):
        payload["share_class_id"] = symbol
    if extra:
        payload.update(extra)
    return _apply_supplemental_candidate_metadata(payload)


_SLEEVE_TEMPLATES: dict[str, dict[str, Any]] = {
    "global_equity_core": {
        "name": "Global Equity Core",
        "purpose": "Long-horizon growth anchor. Default implementation is one global all-cap accumulating UCITS fund.",
        "constraints": [
            "Default pathway keeps a single-fund global all-cap core.",
            "Optional split implementation remains secondary and should preserve global diversification.",
            "Accumulating IE UCITS preference is applied where available.",
        ],
    },
    "developed_ex_us_optional": {
        "name": "Developed ex-US Optional Split",
        "purpose": "Optional secondary split sleeve for investors who track regional drift around a one-fund core.",
        "constraints": [
            "Optional sleeve only; default target remains zero.",
            "When used, sleeve should preserve broad developed-market coverage and avoid concentrated single-country tilts.",
        ],
    },
    "emerging_markets": {
        "name": "Emerging Markets",
        "purpose": "Satellite growth sleeve for broad EM exposure outside developed-market core holdings.",
        "constraints": [
            "Exposure should remain diversified across EM regions.",
            "China concentration should be monitored and coordinated with explicit China satellite sleeve.",
        ],
    },
    "china_satellite": {
        "name": "China Satellite",
        "purpose": "Limited tactical satellite for explicit China allocation transparency.",
        "constraints": [
            "China sleeve is capped and treated as satellite rather than core.",
            "Combined China exposure across all sleeves should remain within policy cap.",
        ],
    },
    "ig_bonds": {
        "name": "Investment Grade Bonds",
        "purpose": "Defensive ballast and duration sleeve for macro drawdown dampening.",
        "constraints": [
            "Preference for broad IG coverage with transparent duration profile.",
            "SGD implementation options may be used when base-currency stability is prioritized.",
        ],
    },
    "cash_bills": {
        "name": "Cash and Bills",
        "purpose": "Liquidity and optional deployment reserve with low duration risk.",
        "constraints": [
            "Focus remains on high-liquidity, short-maturity instruments.",
            "This sleeve functions as buffer capital, not return-maximization core.",
        ],
    },
    "real_assets": {
        "name": "Real Assets",
        "purpose": "Inflation-sensitive diversifier sleeve through gold, commodities, and property proxies.",
        "constraints": [
            "This sleeve is optional diversifier and should not displace core equity or bond policy anchors.",
            "Position sizing should account for commodity structure and tracking differences.",
        ],
    },
    "alternatives": {
        "name": "Alternatives",
        "purpose": "Conservative optional diversifier sleeve spanning gold, broad commodities, and global REIT pathways.",
        "constraints": [
            "Sleeve remains diagnostic and optional under the base policy.",
            "Instruments should preserve transparent max-loss profile and avoid margin-dependent structures.",
        ],
    },
    "convex": {
        "name": "Convex",
        "purpose": "Dedicated downside-convexity sleeve with predefined composition constraints.",
        "constraints": [
            "Managed futures bucket about 2.0%.",
            "Tail hedge bucket about 0.7%.",
            "Long put bucket about 0.3% with fallback to tail hedge if long puts are not permitted.",
            "No margin required, max loss known, no leverage.",
        ],
    },
}


def _citations_from_registry(row: dict[str, Any], sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key in list(row.get("citation_keys") or []):
        source = sources.get(str(key))
        if source:
            out.append(dict(source))
    for link in list(row.get("source_links") or []):
        if not isinstance(link, dict):
            continue
        url = str(link.get("url") or "").strip()
        if not url:
            continue
        out.append(
            _citation(
                source_id=str(link.get("source_id") or f"{str(row.get('symbol') or '').lower()}_registry"),
                url=url,
                retrieved_at=datetime.fromisoformat(str(link.get("retrieved_at"))),
                importance=str(link.get("importance") or "Registry-linked source"),
                published_at=str(link.get("published_at") or "") or None,
                factsheet_asof=str(link.get("factsheet_asof") or "") or None,
            )
        )
    return out


def _candidate_from_registry_row(row: dict[str, Any], sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    extra = dict(row.get("extra") or {})
    extra.setdefault("registry_id", row.get("registry_id"))
    extra.setdefault("benchmark_key", row.get("benchmark_key"))
    extra.setdefault("asset_class", row.get("asset_class"))
    candidate = _candidate(
        symbol=str(row.get("symbol") or ""),
        name=str(row.get("name") or ""),
        instrument_type=str(row.get("instrument_type") or "etf_ucits"),
        domicile=str(row.get("domicile") or "unknown"),
        accumulation=str(row.get("share_class") or "unknown"),
        expense_ratio=_safe_float(row.get("expense_ratio")),
        liquidity_proxy=str(row.get("liquidity_proxy") or "unknown"),
        replication_method=str(row.get("replication_method") or "unknown"),
        us_situs_risk_flag=bool(row.get("estate_risk_flag")),
        expected_withholding_drag_estimate=_safe_float(row.get("expected_withholding_drag_estimate")),
        rationale=str(row.get("rationale") or ""),
        citations=_citations_from_registry(row, sources),
        liquidity_score=_safe_float(row.get("liquidity_score")),
        extra=extra,
    )
    object_type = str(row.get("object_type") or LIVE_OBJECT_TYPE)
    candidate["source_state"] = str(row.get("source_state") or MANUAL_SEED_STATE)
    candidate["object_type"] = object_type
    candidate["policy_placeholder"] = object_type == POLICY_PLACEHOLDER_TYPE or bool(extra.get("policy_placeholder"))
    candidate["strategy_placeholder"] = object_type == STRATEGY_PLACEHOLDER_TYPE
    candidate["manual_provenance_note"] = str(row.get("manual_provenance_note") or "").strip() or None
    candidate["registry_updated_at"] = str(row.get("updated_at") or "").strip() or None
    candidate["registry_effective_at"] = str(row.get("effective_at") or "").strip() or None
    candidate["factsheet_asof"] = str(row.get("factsheet_asof") or "").strip() or None
    candidate["market_data_asof"] = str(row.get("market_data_asof") or "").strip() or None
    candidate["latest_fetch_status"] = {
        "status": str(row.get("latest_fetch_status") or "unknown"),
    }
    candidate["benchmark_key"] = row.get("benchmark_key")
    candidate["issuer"] = str(row.get("issuer") or "").strip() or None
    candidate["asset_class"] = str(row.get("asset_class") or "").strip() or None
    candidate["share_class"] = str(row.get("share_class") or "").strip() or None
    return candidate


def _build_registry_backed_sleeves(
    *,
    conn: sqlite3.Connection,
    sources: dict[str, dict[str, Any]],
    policy_ranges: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    ensure_candidate_registry_tables(conn)
    # Always upsert the controlled seed set so new live candidates and updated
    # source-link configs propagate into existing local databases.
    seed_default_candidate_registry(conn)
    rows = list_active_candidate_registry(conn)
    grouped: dict[str, list[dict[str, Any]]] = {}
    policy_grouped: dict[str, list[dict[str, Any]]] = {}
    strategy_grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        sleeve_key = str(row.get("sleeve_key") or "").strip()
        if not sleeve_key:
            continue
        candidate = _candidate_from_registry_row(row, sources)
        object_type = str(candidate.get("object_type") or LIVE_OBJECT_TYPE)
        if object_type == POLICY_PLACEHOLDER_TYPE:
            policy_grouped.setdefault(sleeve_key, []).append(candidate)
            continue
        if object_type == STRATEGY_PLACEHOLDER_TYPE:
            strategy_grouped.setdefault(sleeve_key, []).append(candidate)
            continue
        grouped.setdefault(sleeve_key, []).append(candidate)

    sleeves: list[dict[str, Any]] = []
    for sleeve_key, template in _SLEEVE_TEMPLATES.items():
        policy = dict(policy_ranges.get(sleeve_key) or {})
        sleeves.append(
            {
                "sleeve_key": sleeve_key,
                "name": template["name"],
                "policy_weight_range": {
                    "min": round(float(policy.get("min") or 0.0) * 100.0, 2),
                    "target": round(float(policy.get("target") or 0.0) * 100.0, 2),
                    "max": round(float(policy.get("max") or 0.0) * 100.0, 2),
                },
                "purpose": template["purpose"],
                "constraints": list(template["constraints"]),
                "candidates": _sort_candidates(grouped.get(sleeve_key, [])),
                "policy_placeholders": _sort_candidates(policy_grouped.get(sleeve_key, [])),
                "strategy_placeholders": _sort_candidates(strategy_grouped.get(sleeve_key, [])),
            }
        )
    return sleeves


def _verify_candidate(candidate: dict[str, Any], *, now: datetime, freshness_days_threshold: int) -> dict[str, Any]:
    citations = list(candidate.get("citations") or [])
    proofs = verify_candidate_proofs(
        candidate=candidate,
        citations=citations,
        now=now,
        freshness_days_threshold=freshness_days_threshold,
    )
    candidate["citations"] = citations
    candidate["primary_sources"] = list(proofs.get("primary_sources") or [])
    candidate["proof_isin"] = bool(proofs.get("proof_isin"))
    candidate["proof_domicile"] = bool(proofs.get("proof_domicile"))
    candidate["proof_share_class_match"] = bool(proofs.get("proof_share_class_match"))
    candidate["proof_accumulating"] = bool(proofs.get("proof_share_class_match"))  # Backward compat alias
    candidate["proof_ter"] = bool(proofs.get("proof_ter"))
    candidate["proof_factsheet_fresh"] = bool(proofs.get("proof_factsheet_fresh"))
    candidate["verification_missing"] = list(proofs.get("verification_missing") or [])
    candidate["verification_status"] = str(proofs.get("verification_status") or "partially_verified")
    candidate["factsheet_asof"] = proofs.get("factsheet_asof")
    candidate["last_verified_at"] = proofs.get("last_verified_at")
    candidate["required_citations_present"] = {
        "proof_isin": candidate["proof_isin"],
        "proof_domicile": candidate["proof_domicile"],
        "proof_share_class_match": candidate["proof_share_class_match"],
        "proof_accumulating": candidate["proof_accumulating"],  # Backward compat
        "proof_ter": candidate["proof_ter"],
        "proof_factsheet_fresh": candidate["proof_factsheet_fresh"],
    }
    return candidate


def _sort_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if any(isinstance(item.get("investment_quality"), dict) for item in candidates):
        return sorted(
            candidates,
            key=lambda item: (
                _quality_rank_order(item),
                -float(dict(item.get("investment_quality") or {}).get("composite_score") or -1e9),
                str(item.get("symbol") or ""),
            ),
        )
    order = {"verified": 0, "partially_verified": 1, "unverified": 2}

    def _score(item: dict[str, Any]) -> float:
        sg_lens = item.get("sg_lens")
        if isinstance(sg_lens, dict):
            score = sg_lens.get("score")
            if isinstance(score, (int, float)):
                return float(score)
        return -1e9

    ranked = sorted(
        candidates,
        key=lambda item: (order.get(str(item.get("verification_status")), 3), -_score(item)),
    )
    rank = 1
    for row in ranked:
        sg_lens = row.get("sg_lens")
        has_score = isinstance(sg_lens, dict) and isinstance(sg_lens.get("score"), (int, float))
        if str(row.get("verification_status")) == "unverified" or not has_score:
            row["sg_rank"] = None
            continue
        row["sg_rank"] = rank
        rank += 1
    return ranked


def _quality_rank_order(item: dict[str, Any]) -> int:
    quality = dict(item.get("investment_quality") or {})
    state = str(quality.get("eligibility_state") or "data_incomplete")
    return {
        "eligible": 0,
        "eligible_with_caution": 1,
        "data_incomplete": 2,
        "ineligible": 3,
    }.get(state, 4)


def _apply_claim_boundary_to_value(value: Any, truth_contract: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return apply_claim_boundary_from_truth(truth_contract, value)
    if isinstance(value, list):
        return [_apply_claim_boundary_to_value(item, truth_contract) for item in value]
    if isinstance(value, dict):
        return {key: _apply_claim_boundary_to_value(item, truth_contract) for key, item in value.items()}
    return value


def _candidate_truth_state(candidate: dict[str, Any], *, settings: Settings, now: datetime) -> dict[str, Any]:
    registry_source = str(candidate.get("source_state") or "unknown").strip().lower() or "unknown"
    object_type = str(candidate.get("object_type") or LIVE_OBJECT_TYPE).strip().lower() or LIVE_OBJECT_TYPE
    holdings_profile = dict(candidate.get("holdings_profile") or {})
    holdings_coverage_class = str(holdings_profile.get("coverage_class") or "")
    source_directness = infer_candidate_directness(candidate)
    fetch_status = str(dict(candidate.get("latest_fetch_status") or {}).get("status") or "unknown").lower()
    has_live_linkage = bool(list(candidate.get("citations") or []))
    latest_dates = [
        str(candidate.get("factsheet_asof") or "").strip(),
        str(dict(candidate.get("aum_history_summary") or {}).get("latest_asof_date") or "").strip(),
        str(dict(candidate.get("market_history_summary") or {}).get("latest_asof_date") or "").strip(),
        str(dict(candidate.get("performance_metrics") or {}).get("as_of_date") or "").strip(),
    ]
    latest_dates = [item for item in latest_dates if item]
    freshness_state = "unknown"
    if latest_dates:
        freshest = max(latest_dates)
        freshness_state = "fresh"
        try:
            observed = datetime.fromisoformat(freshest[:10])
            age_days = (now.date() - observed.date()).days
            if age_days <= 7:
                freshness_state = "fresh"
            elif age_days <= max(14, settings.blueprint_liquidity_proxy_freshness_days):
                freshness_state = "aging"
            elif age_days <= settings.blueprint_factsheet_max_age_days:
                freshness_state = "stale"
            else:
                freshness_state = "quarantined"
        except ValueError:
            freshness_state = "unknown"
    if object_type == STRATEGY_PLACEHOLDER_TYPE:
        source_state = STRATEGY_PLACEHOLDER_TYPE
    elif object_type == POLICY_PLACEHOLDER_TYPE:
        source_state = POLICY_PLACEHOLDER_TYPE
    elif registry_source == BROKEN_SOURCE_STATE:
        source_state = BROKEN_SOURCE_STATE
    elif has_live_linkage:
        if freshness_state == "stale":
            source_state = STALE_LIVE_STATE
        elif freshness_state == "aging":
            source_state = AGING_STATE
        elif freshness_state == "fresh":
            source_state = SOURCE_VALIDATED_STATE
        else:
            source_state = registry_source if registry_source not in {"unknown", "", MANUAL_SEED_STATE} else MANUAL_SEED_STATE
    else:
        source_state = MANUAL_SEED_STATE

    score_version = str(dict(candidate.get("investment_quality") or {}).get("score_version") or "")
    if score_version == V2_MODEL and dict(candidate.get("performance_metrics") or {}):
        score_mode = "V2 performance enabled"
    elif score_version == V1_MODEL:
        score_mode = "V1 non performance"
    else:
        score_mode = "partial due to missing metrics"

    performance_metrics = dict(candidate.get("performance_metrics") or {})
    performance_support_state = (
        "performance_enabled"
        if score_version == V2_MODEL and performance_metrics
        else ("partial_due_to_missing_metrics" if performance_metrics else "non_performance")
    )
    benchmark_confidence = str(dict(candidate.get("benchmark_assignment") or {}).get("benchmark_confidence") or "unknown")
    display_source_state = precise_source_state(source_state, source_directness)
    if fetch_status == "failed":
        if source_state == SOURCE_VALIDATED_STATE:
            display_source_state = "refresh_failed_using_last_validated"
        elif source_state == AGING_STATE:
            display_source_state = "refresh_failed_using_aging_validated"
        elif source_state == MANUAL_SEED_STATE:
            display_source_state = "manual_seed_refresh_failed"
        elif source_state == BROKEN_SOURCE_STATE:
            display_source_state = "refresh_failed_unvalidated"
        elif has_live_linkage:
            display_source_state = "refresh_failed_unvalidated"
    action_readiness = "review_only"
    quality = dict(candidate.get("investment_quality") or {})
    recommendation_state = str(quality.get("recommendation_state") or "")
    if (
        recommendation_state in {"recommended_primary", "recommended_backup"}
        and source_state in {SOURCE_VALIDATED_STATE, AGING_STATE}
        and freshness_state in {"fresh", "aging"}
        and fetch_status != "failed"
    ):
        action_readiness = "action_ready_for_shortlist"
    elif quality.get("eligibility_state") in {"ineligible", "data_incomplete"} or source_state in {STALE_LIVE_STATE, BROKEN_SOURCE_STATE, STRATEGY_PLACEHOLDER_TYPE, POLICY_PLACEHOLDER_TYPE}:
        action_readiness = "blocked_by_data_quality"

    if object_type == STRATEGY_PLACEHOLDER_TYPE:
        note_parts = ["strategy placeholder in blueprint policy design"]
    elif object_type == POLICY_PLACEHOLDER_TYPE:
        note_parts = ["policy placeholder in blueprint implementation design"]
    else:
        note_parts = [f"registry state {registry_source.replace('_', ' ')}"]
    if latest_dates:
        note_parts.append(f"freshness {freshness_state}")
    if benchmark_confidence and benchmark_confidence != "unknown":
        note_parts.append(f"benchmark confidence {benchmark_confidence}")
    if source_directness and source_directness != "unknown":
        note_parts.append(f"truth directness {source_directness.replace('_', ' ')}")
    if fetch_status == "failed" and source_state in {SOURCE_VALIDATED_STATE, AGING_STATE}:
        note_parts.append("latest ingest failed; currently using last successful validated source-backed data")
    elif fetch_status != "unknown":
        note_parts.append(f"latest ingest {fetch_status}")
    if candidate.get("manual_provenance_note"):
        note_parts.append(str(candidate.get("manual_provenance_note")))

    fallback_state = "none"
    if fetch_status == "failed":
        fallback_state = "refresh_failed"
    elif holdings_profile and not bool(holdings_profile.get("direct_holdings_available", False)) and bool(holdings_profile.get("summary_support_available", False)):
        fallback_state = "summary_only"
    elif source_directness in {"issuer_structured_summary_backed", "html_summary_backed"}:
        fallback_state = "summary_only"
    elif source_directness in {"proxy_backed", "support_only"}:
        fallback_state = "support_only"
    authority_class = classify_authority(
        source_directness,
        freshness_state=freshness_state,
        fallback_state=fallback_state,
        structure_first=bool(candidate.get("structure_first_by_design")),
    )
    claim_limit_class = classify_claim_limit(
        source_directness,
        authority_class,
        freshness_state=freshness_state,
        benchmark_support_class=str(
            dict(candidate.get("benchmark_assignment") or {}).get("benchmark_kind")
            or dict(candidate.get("benchmark_assignment") or {}).get("benchmark_fit_type")
            or ""
        ),
    )
    return {
        "source_state": source_state,
        "display_source_state": display_source_state,
        "freshness_state": freshness_state,
        "source_state_note": ". ".join(note_parts),
        "score_mode": score_version or "partial_due_to_missing_metrics",
        "score_mode_label": score_mode,
        "performance_support_state": performance_support_state,
        "action_readiness": action_readiness,
        "static_candidate_data": source_state == MANUAL_SEED_STATE,
        "source_directness": source_directness,
        "authority_class": authority_class,
        "fallback_state": fallback_state,
        "claim_limit_class": claim_limit_class,
    }


OPERATING_USABILITY_STATES = ("usable_now", "usable_with_limits", "review_only", "not_usable_now")
_EQUITY_IMPLEMENTATION_SLEEVES = {
    "global_equity_core",
    "developed_ex_us_optional",
    "emerging_markets",
    "china_satellite",
}
_BOND_AND_CASH_SLEEVES = {"ig_bonds", "cash_bills"}
_SOFT_RISK_FAILURES: dict[str, set[str]] = {
    "emerging_markets": {"em weight band"},
    "ig_bonds": {"top-10 concentration", "sector concentration proxy"},
    "cash_bills": {"top-10 concentration", "sector concentration proxy"},
}


def _candidate_operational_usability(candidate: dict[str, Any], *, sleeve_key: str) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    eligibility = dict(candidate.get("eligibility") or {})
    completeness = dict(candidate.get("data_completeness") or {})
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    liquidity_profile = dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {})
    field_truth = dict(candidate.get("field_truth") or {})
    verification_missing = {str(item) for item in list(candidate.get("verification_missing") or []) if str(item).strip()}
    recommendation_state = str(quality.get("recommendation_state") or "")
    readiness_level = str(completeness.get("readiness_level") or "research_visible")
    benchmark_fit = normalize_benchmark_fit_type(benchmark.get("benchmark_fit_type"))
    liquidity_status = str(liquidity_profile.get("liquidity_status") or "unknown")
    performance_support_state = str(candidate.get("performance_support_state") or "partial_due_to_missing_metrics")
    source_state = str(candidate.get("source_state") or "unknown")
    freshness_state = str(candidate.get("freshness_state") or "unknown")

    hard_blockers: list[str] = []
    confidence_reducers: list[str] = []
    upgrade_conditions: list[str] = []
    explanatory_only: list[str] = []

    for detail in list(eligibility.get("eligibility_blocker_details") or []):
        reason = str(detail.get("reason") or "").strip()
        root_cause = str(detail.get("root_cause") or "")
        lowered = reason.lower()
        if not reason:
            continue
        if root_cause == "policy_failure":
            if "risk controls failed" in lowered:
                # Risk-control severity is reclassified below from actual sleeve-aware control results.
                continue
            hard_blockers.append(reason)
        elif root_cause == "missing_primary_trading_currency":
            hard_blockers.append(reason)
        elif "holdings source coverage" in lowered:
            if sleeve_key in _EQUITY_IMPLEMENTATION_SLEEVES:
                hard_blockers.append(reason)
            else:
                upgrade_conditions.append(reason)
        elif root_cause == "incomplete_tracking_difference_history":
            upgrade_conditions.append(reason)
        else:
            hard_blockers.append(reason)

    for detail in list(eligibility.get("eligibility_caution_details") or []):
        reason = str(detail.get("reason") or "").strip()
        if reason:
            confidence_reducers.append(reason)

    risk_controls = list(dict(candidate.get("investment_lens") or {}).get("risk_controls") or [])
    for control in risk_controls:
        status = str(control.get("status") or "unknown")
        metric_name = str(control.get("metric_name") or "").strip()
        metric_key = metric_name.lower()
        rationale = str(control.get("rationale") or metric_name or "Risk control issue")
        if status == "fail":
            if metric_key in _SOFT_RISK_FAILURES.get(sleeve_key, set()):
                confidence_reducers.append(f"{metric_name} is elevated for this sleeve but treated as a review pressure rather than a hard block.")
            else:
                hard_blockers.append(rationale)
        elif status in {"warn", "unknown"} and metric_name:
            confidence_reducers.append(rationale)

    if benchmark_fit == "acceptable_proxy":
        confidence_reducers.append("Benchmark support is proxy-based, which reduces relative-comparison confidence.")
    elif benchmark_fit in {"weak_proxy", "mismatched"}:
        if sleeve_key == "convex":
            explanatory_only.append("Benchmark support is not decisive for the convex sleeve.")
        else:
            hard_blockers.append("Benchmark support is too weak for the sleeve's current implementation role.")

    if performance_support_state in {"partial_due_to_missing_metrics", "benchmark_proxy_only"}:
        confidence_reducers.append("Benchmark-relative performance support is incomplete and caps confidence.")
    elif performance_support_state == "unsupported" and sleeve_key in _EQUITY_IMPLEMENTATION_SLEEVES | {"ig_bonds"}:
        hard_blockers.append("Performance support is unavailable for a sleeve that requires benchmarkable comparison support.")

    if liquidity_status == "limited_evidence":
        confidence_reducers.append("Liquidity evidence is only partial, so implementation confidence remains capped.")
    elif liquidity_status == "unknown":
        upgrade_conditions.append("Liquidity evidence is missing and would benefit from spread or volume support.")
    elif liquidity_status == "weak":
        hard_blockers.append("Liquidity is too weak for current sleeve implementation.")

    if source_state in {BROKEN_SOURCE_STATE, STALE_LIVE_STATE, STRATEGY_PLACEHOLDER_TYPE, POLICY_PLACEHOLDER_TYPE} or freshness_state in {"stale", "quarantined"}:
        hard_blockers.append("Source freshness or truth state is too weak for operational use.")

    if "share_class_proven" in verification_missing:
        upgrade_conditions.append("Share-class proof remains incomplete.")

    if sleeve_key == "cash_bills":
        if str(dict(field_truth.get("yield_proxy") or {}).get("missingness_reason") or "") in {"fetchable_from_current_sources", "blocked_by_parser_gap", "blocked_by_source_gap"}:
            upgrade_conditions.append("Yield support is still incomplete for the cash sleeve.")
        if str(candidate.get("underlying_currency_exposure") or "unknown").strip().lower() in {"", "unknown"}:
            hard_blockers.append("Underlying currency exposure is still unresolved for the cash sleeve.")
        if str(dict(field_truth.get("weighted_average_maturity") or {}).get("missingness_reason") or "") in {"fetchable_from_current_sources", "blocked_by_parser_gap", "blocked_by_source_gap"}:
            upgrade_conditions.append("Weighted-average maturity support is still incomplete.")
        if str(candidate.get("redemption_settlement_notes") or "").strip() == "":
            explanatory_only.append("Settlement-note support is still thin but not treated as a hard block.")
    elif sleeve_key == "ig_bonds":
        if str(dict(field_truth.get("yield_proxy") or {}).get("missingness_reason") or "") in {"fetchable_from_current_sources", "blocked_by_parser_gap", "blocked_by_source_gap"}:
            upgrade_conditions.append("Yield support is still incomplete for this bond sleeve.")

    hard_blockers = list(dict.fromkeys(item for item in hard_blockers if item))
    confidence_reducers = [item for item in confidence_reducers if item not in hard_blockers]
    confidence_reducers = list(dict.fromkeys(confidence_reducers))
    upgrade_conditions = [item for item in upgrade_conditions if item not in hard_blockers]
    upgrade_conditions = list(dict.fromkeys(upgrade_conditions))
    explanatory_only = [item for item in explanatory_only if item not in hard_blockers and item not in confidence_reducers]
    explanatory_only = list(dict.fromkeys(explanatory_only))

    minimum_viable = _candidate_meets_minimum_viable_evidence(
        candidate,
        sleeve_key=sleeve_key,
        field_truth=field_truth,
        benchmark_fit=benchmark_fit,
        liquidity_status=liquidity_status,
    )
    if hard_blockers:
        state = "not_usable_now"
    elif recommendation_state in {"recommended_primary", "recommended_backup"}:
        state = "usable_with_limits" if confidence_reducers or upgrade_conditions else "usable_now"
    elif minimum_viable and str(quality.get("eligibility_state") or "") in {"eligible", "eligible_with_caution"}:
        state = "usable_with_limits" if confidence_reducers or upgrade_conditions or readiness_level == "review_ready" else "usable_now"
    else:
        state = "review_only"

    why_usable = (
        "Candidate clears the current hard policy and implementation gates."
        if state in {"usable_now", "usable_with_limits"}
        else "Candidate still needs more evidence or a blocker must be cleared before use."
    )
    if state == "usable_with_limits" and confidence_reducers:
        why_usable = "Candidate is implementable now, but remaining evidence gaps still cap confidence."

    summary = {
        "state": state,
        "hard_blockers": hard_blockers[:6],
        "confidence_reducers": confidence_reducers[:6],
        "upgrade_conditions": upgrade_conditions[:6],
        "explanatory_only": explanatory_only[:6],
        "minimum_viable_evidence": minimum_viable,
        "why_usable": why_usable,
        "limitations": confidence_reducers[:4] or upgrade_conditions[:4],
        "upgrade_triggers": upgrade_conditions[:4],
        "monitor_items": (confidence_reducers + explanatory_only)[:4],
        "summary": (
            "Usable now because hard blockers are cleared and evidence is strong enough for the sleeve."
            if state == "usable_now"
            else "Usable with limits because hard blockers are cleared but confidence-reducing gaps remain."
            if state == "usable_with_limits"
            else "Review only because the candidate is plausible but still under-evidenced."
            if state == "review_only"
            else "Not usable now because at least one hard blocker remains."
        ),
    }
    return summary


def _candidate_meets_minimum_viable_evidence(
    candidate: dict[str, Any],
    *,
    sleeve_key: str,
    field_truth: dict[str, Any],
    benchmark_fit: str,
    liquidity_status: str,
) -> bool:
    trading_currency = str(candidate.get("primary_trading_currency") or candidate.get("trading_currency") or "").strip().upper()
    source_state = str(candidate.get("source_state") or "unknown")
    if source_state in {BROKEN_SOURCE_STATE, STALE_LIVE_STATE, STRATEGY_PLACEHOLDER_TYPE, POLICY_PLACEHOLDER_TYPE}:
        return False
    if not trading_currency and sleeve_key not in {"convex"}:
        return False
    if liquidity_status == "weak":
        return False
    if sleeve_key in _EQUITY_IMPLEMENTATION_SLEEVES:
        return benchmark_fit in {"strong_fit", "acceptable_proxy"}
    if sleeve_key == "ig_bonds":
        has_duration = any(
            candidate.get(key) not in {None, "", "unknown"}
            for key in ("effective_duration", "average_maturity", "duration_years")
        )
        has_yield = any(
            candidate.get(key) not in {None, "", "unknown"}
            for key in ("yield_proxy", "distribution_yield")
        ) or str(dict(field_truth.get("yield_proxy") or {}).get("resolved_value") or "") not in {"", "unknown"}
        return benchmark_fit in {"strong_fit", "acceptable_proxy"} and has_duration and has_yield
    if sleeve_key == "cash_bills":
        underlying = str(candidate.get("underlying_currency_exposure") or "unknown").strip().lower()
        support_signals = 0
        for key in ("yield_proxy", "weighted_average_maturity", "tracking_difference_1y"):
            value = dict(field_truth.get(key) or {}).get("resolved_value")
            if value not in {None, "", "unknown"}:
                support_signals += 1
        return underlying not in {"", "unknown"} and support_signals >= 1
    return True


_CORE_EQUITY_REQUIRED_FIELDS: list[dict[str, Any]] = [
    {"key": "issuer", "label": "Issuer", "critical": True},
    {"key": "fund_name", "label": "Fund name", "critical": True},
    {"key": "identifier", "label": "Identifier", "critical": True},
    {"key": "domicile", "label": "Domicile", "critical": True},
    {"key": "wrapper", "label": "Wrapper", "critical": True},
    {"key": "distribution_type", "label": "Distribution type", "critical": True},
    {"key": "ter", "label": "TER", "critical": True},
    {"key": "benchmark", "label": "Benchmark", "critical": True},
    {"key": "benchmark_confidence", "label": "Benchmark confidence", "critical": True},
    {"key": "replication_method", "label": "Replication method", "critical": False},
    {"key": "aum", "label": "AUM", "critical": True},
    {"key": "primary_listing", "label": "Primary listing", "critical": False},
    {"key": "liquidity_proxy", "label": "Liquidity proxy", "critical": True},
    {"key": "spread_proxy", "label": "Spread proxy", "critical": False},
    {"key": "developed_em_exposure", "label": "Developed versus EM exposure", "critical": True},
    {"key": "us_weight", "label": "US weight", "critical": False},
    {"key": "top10_concentration", "label": "Top 10 concentration", "critical": False},
    {"key": "sector_concentration_proxy", "label": "Sector concentration proxy", "critical": False},
    {"key": "tracking_difference", "label": "Tracking difference", "critical": False},
    {"key": "withholding_tax_posture", "label": "Withholding tax posture", "critical": True},
    {"key": "estate_risk_posture", "label": "Estate risk posture", "critical": True},
    {"key": "factsheet_asof", "label": "Factsheet as of", "critical": True},
    {"key": "market_data_asof", "label": "Market data as of", "critical": True},
]

_IG_BOND_REQUIRED_FIELDS: list[dict[str, Any]] = _CORE_EQUITY_REQUIRED_FIELDS + [
    {"key": "duration", "label": "Duration", "critical": True},
    {"key": "yield_proxy", "label": "Yield proxy", "critical": True},
    {"key": "credit_quality_mix", "label": "Credit quality mix", "critical": True},
    {"key": "issuer_concentration_proxy", "label": "Issuer concentration proxy", "critical": False},
    {"key": "rate_sensitivity_inputs", "label": "Rate sensitivity inputs", "critical": True},
]

_CONVEX_REQUIRED_FIELDS: list[dict[str, Any]] = [
    {"key": "issuer", "label": "Issuer", "critical": True},
    {"key": "fund_name", "label": "Fund name", "critical": True},
    {"key": "identifier", "label": "Identifier", "critical": True},
    {"key": "structure_profile", "label": "Structure profile", "critical": True},
    {"key": "liquidity_proxy", "label": "Liquidity proxy", "critical": True},
    {"key": "margin_required", "label": "Margin required", "critical": True},
    {"key": "max_loss_known", "label": "Max loss known", "critical": True},
    {"key": "short_options", "label": "Short options", "critical": True},
    {"key": "factsheet_asof", "label": "Factsheet as of", "critical": False},
]

_CASH_REQUIRED_FIELDS: list[dict[str, Any]] = [
    {"key": "issuer", "label": "Issuer", "critical": True},
    {"key": "fund_name", "label": "Fund name", "critical": True},
    {"key": "identifier", "label": "Identifier", "critical": True},
    {"key": "wrapper", "label": "Wrapper", "critical": False},
    {"key": "liquidity_proxy", "label": "Liquidity proxy", "critical": True},
    {"key": "aum", "label": "AUM", "critical": False},
    {"key": "factsheet_asof", "label": "Factsheet as of", "critical": False},
    {"key": "market_data_asof", "label": "Market data as of", "critical": False},
]


def _approved_source_support(candidate: dict[str, Any], *, sleeve_key: str) -> set[str]:
    issuer = str(candidate.get("issuer") or "").strip().lower()
    instrument_type = str(candidate.get("instrument_type") or "").strip().lower()
    support: set[str] = {
        "issuer",
        "fund_name",
        "identifier",
        "domicile",
        "wrapper",
        "distribution_type",
        "ter",
        "benchmark",
        "benchmark_confidence",
        "replication_method",
        "liquidity_proxy",
        "withholding_tax_posture",
        "estate_risk_posture",
        "factsheet_asof",
        "market_data_asof",
    }
    if instrument_type in {"etf_ucits", "etf_us"}:
        support.update(
            {
                "aum",
                "spread_proxy",
                "developed_em_exposure",
                "us_weight",
                "top10_concentration",
                "sector_concentration_proxy",
                "tracking_difference",
                "primary_listing",
            }
        )
    if sleeve_key == "ig_bonds":
        support.update({"duration", "yield_proxy", "credit_quality_mix", "rate_sensitivity_inputs"})
    if sleeve_key == "convex":
        support.update({"structure_profile", "margin_required", "max_loss_known", "short_options"})
    if sleeve_key == "cash_bills":
        support.update({"aum"})
    if issuer in {"blackrock ishares", "vanguard", "dws xtrackers", "hsbc asset management", "nikko am", "invesco", "state street spdr", "fullerton fund management", "amova"}:
        support.update({"primary_listing", "spread_proxy", "tracking_difference"})
    return support


def _wrapper_label(candidate: dict[str, Any]) -> str | None:
    instrument_type = str(candidate.get("instrument_type") or "").lower()
    name = str(candidate.get("name") or "")
    if instrument_type == "etf_ucits" or "UCITS" in name.upper():
        return "UCITS"
    if instrument_type == "etf_us":
        return "US ETF"
    if instrument_type == "t_bill_sg":
        return "Singapore Treasury bill"
    if instrument_type == "money_market_fund_sg":
        return "SGD money market fund"
    if instrument_type == "cash_account_sg":
        return "SGD cash reserve"
    if instrument_type == "long_put_overlay_strategy":
        return "Policy overlay"
    return None


def _developed_em_summary(candidate: dict[str, Any], *, sleeve_key: str) -> str | None:
    if candidate.get("em_weight_pct") is not None:
        em_weight = float(candidate.get("em_weight_pct") or 0.0)
        if em_weight <= 0:
            return "Developed markets only"
        return f"Developed plus emerging ({em_weight:.1f}% EM)"
    if sleeve_key == "developed_ex_us_optional":
        return "Developed markets only"
    if sleeve_key == "emerging_markets":
        return "Emerging markets focused"
    if sleeve_key == "china_satellite":
        return "China focused satellite"
    if sleeve_key == "global_equity_core":
        return "Global developed plus emerging"
    return None


def _field_value_for_completeness(candidate: dict[str, Any], *, sleeve_key: str, field_key: str) -> Any:
    lens = dict(candidate.get("investment_lens") or {})
    benchmark_assignment = dict(candidate.get("benchmark_assignment") or {})
    performance_metrics = dict(candidate.get("performance_metrics") or {})
    liquidity_profile = dict(lens.get("liquidity_profile") or {})
    tax_mechanics = dict(lens.get("tax_mechanics") or {})
    name = str(candidate.get("name") or "").strip()
    if field_key == "issuer":
        return candidate.get("issuer")
    if field_key == "fund_name":
        return name
    if field_key == "identifier":
        return candidate.get("symbol") if str(candidate.get("symbol") or "").strip() else None
    if field_key == "domicile":
        return candidate.get("domicile")
    if field_key == "wrapper":
        return _wrapper_label(candidate)
    if field_key == "distribution_type":
        return candidate.get("accumulation_or_distribution")
    if field_key == "ter":
        return candidate.get("expense_ratio")
    if field_key == "benchmark":
        return benchmark_assignment.get("benchmark_label") or benchmark_assignment.get("benchmark_key") or candidate.get("benchmark_key")
    if field_key == "benchmark_confidence":
        return benchmark_assignment.get("benchmark_confidence")
    if field_key == "replication_method":
        return candidate.get("replication_method")
    if field_key == "aum":
        return (
            performance_metrics.get("aum_usd_latest")
            if performance_metrics.get("aum_usd_latest") is not None
            else candidate.get("aum_usd")
        )
    if field_key == "primary_listing":
        return candidate.get("primary_listing") or candidate.get("primary_listing_exchange")
    if field_key == "liquidity_proxy":
        return liquidity_profile.get("liquidity_status") or candidate.get("liquidity_proxy")
    if field_key == "spread_proxy":
        return performance_metrics.get("spread_bps_latest") if performance_metrics.get("spread_bps_latest") is not None else candidate.get("bid_ask_spread_proxy")
    if field_key == "developed_em_exposure":
        return _developed_em_summary(candidate, sleeve_key=sleeve_key)
    if field_key == "us_weight":
        return candidate.get("us_weight_pct")
    if field_key == "top10_concentration":
        return candidate.get("top10_concentration_pct")
    if field_key == "sector_concentration_proxy":
        return candidate.get("tech_weight_pct")
    if field_key == "tracking_difference":
        return (
            performance_metrics.get("tracking_difference_1y")
            if performance_metrics.get("tracking_difference_1y") is not None
            else (
                candidate.get("tracking_difference_1y")
                if candidate.get("tracking_difference_1y") is not None
                else candidate.get("tracking_difference_note")
            )
        )
    if field_key == "withholding_tax_posture":
        return tax_mechanics.get("withholding_tax_exposure_note") or candidate.get("expected_withholding_drag_estimate")
    if field_key == "estate_risk_posture":
        return "US situs risk" if candidate.get("us_situs_estate_risk_flag") else "No US situs flag"
    if field_key == "factsheet_asof":
        return candidate.get("factsheet_asof")
    if field_key == "market_data_asof":
        return candidate.get("market_data_asof") or dict(candidate.get("market_history_summary") or {}).get("latest_asof_date")
    if field_key == "duration":
        return (
            performance_metrics.get("duration_years")
            if performance_metrics.get("duration_years") is not None
            else candidate.get("effective_duration")
        )
    if field_key == "yield_proxy":
        return (
            performance_metrics.get("yield_to_maturity")
            if performance_metrics.get("yield_to_maturity") is not None
            else candidate.get("yield_proxy")
        )
    if field_key == "credit_quality_mix":
        return performance_metrics.get("credit_quality_mix") or candidate.get("credit_quality_mix")
    if field_key == "issuer_concentration_proxy":
        return candidate.get("issuer_concentration_proxy") or candidate.get("top10_concentration_pct")
    if field_key == "rate_sensitivity_inputs":
        return (
            performance_metrics.get("duration_years")
            if performance_metrics.get("duration_years") is not None
            else candidate.get("interest_rate_sensitivity_proxy")
        )
    if field_key == "structure_profile":
        return candidate.get("replication_method") or candidate.get("instrument_type")
    if field_key == "margin_required":
        return candidate.get("margin_required")
    if field_key == "max_loss_known":
        return candidate.get("max_loss_known")
    if field_key == "short_options":
        return candidate.get("short_options")
    return None


def _truth_observed_at(*values: Any) -> str | None:
    points = [str(value).strip() for value in values if str(value or "").strip()]
    return max(points) if points else None


def _truth_primary_citation(candidate: dict[str, Any]) -> dict[str, Any]:
    for citation in list(candidate.get("citations") or []):
        url = str(citation.get("url") or "").strip()
        if url:
            return dict(citation)
    return {}


def _candidate_truth_signature(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    benchmark_assignment: dict[str, Any],
    performance_metrics: dict[str, Any] | None,
    truth_state: dict[str, Any],
) -> dict[str, Any]:
    market_history_summary = dict(candidate.get("market_history_summary") or {})
    latest_fetch_status = dict(candidate.get("latest_fetch_status") or {})
    performance = dict(performance_metrics or {})
    return {
        "signature_version": 2,
        "symbol": str(candidate.get("symbol") or "").upper(),
        "sleeve_key": sleeve_key,
        "factsheet_asof": candidate.get("factsheet_asof"),
        "market_data_asof": candidate.get("market_data_asof") or market_history_summary.get("latest_asof_date"),
        "source_state": truth_state.get("source_state"),
        "freshness_state": truth_state.get("freshness_state"),
        "display_source_state": truth_state.get("display_source_state"),
        "benchmark_key": benchmark_assignment.get("benchmark_key"),
        "benchmark_confidence": benchmark_assignment.get("benchmark_confidence"),
        "benchmark_validation_status": benchmark_assignment.get("validation_status"),
        "performance_as_of": performance.get("as_of_date"),
        "tracking_difference_1y": performance.get("tracking_difference_1y"),
        "tracking_difference_3y": performance.get("tracking_difference_3y"),
        "tracking_difference_5y": performance.get("tracking_difference_5y"),
        "tracking_error_1y": performance.get("tracking_error_1y"),
        "aum_usd_latest": performance.get("aum_usd_latest"),
        "spread_bps_latest": performance.get("spread_bps_latest"),
        "volume_30d_avg": candidate.get("volume_30d_avg") or market_history_summary.get("latest_volume_30d_avg"),
        "share_class_proven": candidate.get("share_class_proven"),
        "underlying_currency_exposure": candidate.get("underlying_currency_exposure"),
        "latest_fetch_status": latest_fetch_status.get("status"),
        "latest_fetch_finished_at": latest_fetch_status.get("finished_at"),
        "latest_fetch_source": latest_fetch_status.get("source_url"),
    }


def _field_truth_without_runtime_signature(field_truth: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {key: value for key, value in field_truth.items() if key != "__runtime_signature__"}


def _upsert_candidate_truth_observations(
    *,
    conn: sqlite3.Connection,
    candidate: dict[str, Any],
    sleeve_key: str,
    benchmark_assignment: dict[str, Any],
    performance_metrics: dict[str, Any] | None,
    truth_state: dict[str, Any],
    now: datetime,
) -> dict[str, dict[str, Any]]:
    symbol = str(candidate.get("symbol") or "").upper()
    if not symbol:
        return {}
    candidate["sleeve_key"] = sleeve_key
    current_snapshot = get_candidate_field_current(conn, candidate_symbol=symbol, sleeve_key=sleeve_key)
    runtime_signature = _candidate_truth_signature(
        candidate=candidate,
        sleeve_key=sleeve_key,
        benchmark_assignment=benchmark_assignment,
        performance_metrics=performance_metrics,
        truth_state=truth_state,
    )
    existing_signature = dict(current_snapshot.get("__runtime_signature__") or {}).get("resolved_value")
    if current_snapshot and existing_signature == runtime_signature:
        return _field_truth_without_runtime_signature(current_snapshot)

    primary_citation = _truth_primary_citation(candidate)
    base_source_name = str(primary_citation.get("source_id") or "candidate_payload")
    base_source_url = str(primary_citation.get("url") or "") or None
    base_observed_at = _truth_observed_at(
        candidate.get("factsheet_asof"),
        dict(candidate.get("market_history_summary") or {}).get("latest_asof_date"),
        dict(candidate.get("performance_metrics") or {}).get("as_of_date"),
        primary_citation.get("factsheet_asof"),
        primary_citation.get("published_at"),
    )
    instrument_type = str(candidate.get("instrument_type") or "").lower()
    doc_result = _fetch_candidate_docs_cached(symbol, use_fixtures=True) if instrument_type in {"etf_ucits", "etf_us"} else None
    if instrument_type in {"etf_ucits", "etf_us"} and (
        not doc_result
        or not (
            str((doc_result or {}).get("status") or "") == "success"
            or str(dict((doc_result or {}).get("factsheet") or {}).get("status") or "") == "success"
        )
    ):
        live_doc_result = _fetch_candidate_docs_cached(symbol, use_fixtures=False)
        if live_doc_result:
            doc_result = live_doc_result
    doc_extracted = dict((doc_result or {}).get("extracted") or {})
    doc_available = bool(doc_result) and (
        str((doc_result or {}).get("status") or "") == "success"
        or str(dict((doc_result or {}).get("factsheet") or {}).get("status") or "") == "success"
    )
    doc_source_url = str(dict((doc_result or {}).get("factsheet") or {}).get("doc_url") or "") or base_source_url
    doc_observed_at = str(doc_extracted.get("factsheet_date") or base_observed_at or now.date().isoformat())
    holdings_profile = get_etf_holdings_profile(symbol, conn) if instrument_type in {"etf_ucits", "etf_us"} else None
    candidate["holdings_profile"] = holdings_profile
    holdings_available = holdings_profile is not None
    source_config = get_etf_source_config(symbol) or {}
    configured_sources = set(dict(source_config.get("data_sources") or {}).keys())
    factsheet_summary = get_etf_factsheet_history_summary(symbol, conn) if instrument_type in {"etf_ucits", "etf_us"} else None
    latest_factsheet_metrics = get_latest_etf_factsheet_metrics(symbol, conn) if instrument_type in {"etf_ucits", "etf_us"} else None
    latest_market_data = get_preferred_latest_market_data(symbol, conn) if instrument_type in {"etf_ucits", "etf_us"} else None
    market_summary = get_preferred_market_history_summary(symbol, conn) if instrument_type in {"etf_ucits", "etf_us"} else None
    ingest_success_at = get_latest_successful_etf_ingest_at(symbol, conn)
    tax_truth = build_sg_tax_truth(
        domicile=str(candidate.get("domicile") or ""),
        expected_withholding_rate=candidate.get("expected_withholding_drag_estimate"),
        us_situs_risk_flag=bool(candidate.get("us_situs_estate_risk_flag")),
        accumulation_or_distribution=str(candidate.get("accumulation_or_distribution") or ""),
        instrument_type=instrument_type,
    )
    current_rows_for_write: dict[str, dict[str, Any]] = {}
    current_observation_ids: dict[str, str] = {}

    def _observe(
        field_name: str,
        value: Any,
        *,
        provenance_level: str,
        source_name: str,
        source_url: str | None = None,
        observed_at: str | None = None,
        confidence_label: str | None = None,
        parser_method: str | None = None,
        missingness_reason: str | None = None,
        override_annotation: dict[str, Any] | None = None,
    ) -> None:
        row_id = upsert_field_observation(
            conn,
            candidate_symbol=symbol,
            sleeve_key=sleeve_key,
            field_name=field_name,
            value=value,
            source_name=source_name,
            source_url=source_url,
            observed_at=observed_at or base_observed_at or now.isoformat(),
            provenance_level=provenance_level,
            confidence_label=confidence_label,
            parser_method=parser_method or "portfolio_blueprint_runtime",
            missingness_reason=missingness_reason or ("populated" if value is not None else "blocked_by_source_gap"),
            override_annotation=override_annotation,
        )
        resolved_missingness = missingness_reason or ("populated" if value is not None else "blocked_by_source_gap")
        resolved_provenance = provenance_level if provenance_level in PROVENANCE_PRIORITIES else "seeded_fallback"
        resolved_value_type = _default_value_type(value, resolved_provenance, resolved_missingness)
        observed_value = observed_at or base_observed_at or now.isoformat()
        observed_dt = _safe_parse_dt(observed_value)
        if resolved_missingness == "populated" and observed_dt is not None:
            if (now - observed_dt).days > _field_stale_days(field_name):
                resolved_value_type = "stale"
        next_row = {
            "candidate_symbol": symbol,
            "sleeve_key": sleeve_key,
            "field_name": field_name,
            "resolved_value": value,
            "resolved_value_json": _json(value) if value is not None else None,
            "value_type": resolved_value_type,
            "source_name": source_name,
            "source_url": source_url,
            "observed_at": observed_value,
            "ingested_at": now.isoformat(),
            "provenance_level": resolved_provenance,
            "confidence_label": confidence_label,
            "parser_method": parser_method or "portfolio_blueprint_runtime",
            "overwrite_priority": PROVENANCE_PRIORITIES.get(resolved_provenance, 50),
            "missingness_reason": resolved_missingness,
            "override_annotation": override_annotation or {},
            "last_resolved_at": now.isoformat(),
        }
        existing_row = current_rows_for_write.get(field_name)
        if existing_row is not None:
            existing_priority = int(existing_row.get("overwrite_priority") or 0)
            next_priority = int(next_row.get("overwrite_priority") or 0)
            existing_missing = str(existing_row.get("missingness_reason") or "") != "populated"
            next_missing = str(next_row.get("missingness_reason") or "") != "populated"
            should_replace = False
            if existing_missing and not next_missing:
                should_replace = True
            elif next_priority > existing_priority:
                should_replace = True
            elif next_priority == existing_priority:
                if existing_missing == next_missing:
                    existing_observed = _safe_parse_dt(str(existing_row.get("observed_at") or ""))
                    next_observed = _safe_parse_dt(str(next_row.get("observed_at") or ""))
                    if next_observed and (existing_observed is None or next_observed >= existing_observed):
                        should_replace = True
            if should_replace:
                current_rows_for_write[field_name] = next_row
                current_observation_ids[field_name] = row_id
        else:
            current_rows_for_write[field_name] = next_row
            current_observation_ids[field_name] = row_id

    _observe(
        "__runtime_signature__",
        runtime_signature,
        provenance_level="seeded_fallback",
        source_name="portfolio_blueprint_runtime",
        source_url=None,
        observed_at=now.isoformat(),
        confidence_label="runtime_signature",
        parser_method="portfolio_blueprint_runtime_signature",
        missingness_reason="populated",
    )

    fetchable_fields = {
        "isin",
        "wrapper_or_vehicle_type",
        "distribution_type",
        "share_class_proven",
        "ucits_status",
        "primary_trading_currency",
        "primary_listing_exchange",
        "aum",
        "bid_ask_spread_proxy",
        "volume_30d_avg",
        "market_participation_proxy",
        "benchmark_assignment_method",
        "benchmark_validation_status",
        "tracking_difference_1y",
        "tracking_difference_3y",
        "tracking_difference_5y",
        "tracking_error_1y",
        "holdings_count",
        "top_10_concentration",
        "sector_concentration_proxy",
        "developed_market_exposure_summary",
        "emerging_market_exposure_summary",
        "us_weight",
        "em_weight",
        "withholding_tax_posture",
        "estate_risk_posture",
        "benchmark_assignment_proof",
        "effective_duration",
        "average_maturity",
        "yield_proxy",
        "credit_quality_mix",
        "government_vs_corporate_split",
        "issuer_concentration_proxy",
        "interest_rate_sensitivity_proxy",
        "weighted_average_maturity",
        "portfolio_quality_summary",
        "redemption_settlement_notes",
        "underlying_currency_exposure",
    }
    doc_backed_fields = {
        "isin",
        "fund_name",
        "wrapper_or_vehicle_type",
        "distribution_type",
        "share_class",
        "share_class_proven",
        "expense_ratio",
        "factsheet_as_of",
        "benchmark_name",
        "replication_method",
        "primary_listing_exchange",
        "primary_trading_currency",
        "ucits_status",
        "aum",
        "holdings_count",
        "top_10_concentration",
        "sector_concentration_proxy",
        "developed_market_exposure_summary",
        "emerging_market_exposure_summary",
        "us_weight",
        "em_weight",
        "tracking_difference_1y",
        "tracking_difference_3y",
        "tracking_difference_5y",
        "effective_duration",
        "average_maturity",
        "yield_proxy",
        "credit_quality_mix",
        "government_vs_corporate_split",
        "interest_rate_sensitivity_proxy",
        "weighted_average_maturity",
        "portfolio_quality_summary",
        "redemption_settlement_notes",
        "underlying_currency_exposure",
    }
    holdings_backed_fields = {
        "us_weight",
        "em_weight",
        "top_10_concentration",
        "sector_concentration_proxy",
        "developed_market_exposure_summary",
        "emerging_market_exposure_summary",
        "holdings_count",
    }
    market_backed_fields = {"bid_ask_spread_proxy", "volume_30d_avg", "primary_listing_exchange"}
    factsheet_metric_fields = {
        "tracking_difference_1y",
        "tracking_difference_3y",
        "tracking_difference_5y",
        "tracking_error_1y",
        "aum",
        "yield_proxy",
    }
    cash_support_fields = {
        "yield_proxy",
        "weighted_average_maturity",
        "portfolio_quality_summary",
        "redemption_settlement_notes",
        "underlying_currency_exposure",
    }

    def _has_meaningful(value: Any) -> bool:
        return value is not None and not (isinstance(value, str) and not value.strip())

    def _missingness_for_field(field_name: str, value: Any) -> str:
        if _has_meaningful(value):
            return "populated"
        if sleeve_key in {"alternatives", "convex"} and field_name in {
            "benchmark_name",
            "benchmark_key",
            "benchmark_assignment_method",
            "benchmark_confidence",
            "tracking_difference_1y",
            "tracking_difference_3y",
            "tracking_difference_5y",
            "tracking_error_1y",
            "holdings_count",
            "top_10_concentration",
            "sector_concentration_proxy",
            "ucits_status",
        }:
            return "not_applicable"
        if field_name in holdings_backed_fields:
            if holdings_available:
                return "blocked_by_parser_gap"
            if "holdings" in configured_sources or "factsheet" in configured_sources:
                return "fetchable_from_current_sources"
            return "blocked_by_source_gap"
        if field_name in market_backed_fields:
            if latest_market_data or market_summary:
                return "blocked_by_parser_gap"
            if "market_data" in configured_sources:
                return "fetchable_from_current_sources"
            return "blocked_by_source_gap"
        if field_name in factsheet_metric_fields:
            if latest_factsheet_metrics or factsheet_summary:
                return "blocked_by_parser_gap"
            if doc_available or "factsheet" in configured_sources:
                return "fetchable_from_current_sources"
            return "blocked_by_source_gap"
        if field_name in doc_backed_fields:
            if doc_available:
                return "blocked_by_parser_gap"
            if "factsheet" in configured_sources:
                return "fetchable_from_current_sources"
            return "blocked_by_source_gap"
        if sleeve_key == "cash_bills" and field_name in cash_support_fields:
            if doc_available or latest_factsheet_metrics:
                return "blocked_by_parser_gap"
            if "factsheet" in configured_sources:
                return "fetchable_from_current_sources"
            return "blocked_by_source_gap"
        if field_name in {"last_successful_ingest_at", "benchmark_assignment_proof"}:
            return "fetchable_from_current_sources" if configured_sources else "blocked_by_source_gap"
        return "blocked_by_source_gap"

    doc_field_values = {
        "fund_name": doc_extracted.get("fund_name"),
        "isin": doc_extracted.get("isin"),
        "share_class": doc_extracted.get("accumulating_status"),
        "share_class_proven": bool(doc_extracted.get("accumulating_status")) if doc_available else None,
        "wrapper_or_vehicle_type": doc_extracted.get("wrapper_or_vehicle_type"),
        "distribution_type": doc_extracted.get("accumulating_status"),
        "replication_method": doc_extracted.get("replication_method"),
        "expense_ratio": doc_extracted.get("ter"),
        "factsheet_as_of": doc_extracted.get("factsheet_date"),
        "benchmark_name": doc_extracted.get("benchmark_name"),
        "primary_listing_exchange": doc_extracted.get("primary_listing_exchange"),
        "primary_trading_currency": doc_extracted.get("primary_trading_currency"),
        "ucits_status": doc_extracted.get("ucits_status"),
        "aum": doc_extracted.get("aum_usd"),
        "holdings_count": doc_extracted.get("holdings_count"),
        "top_10_concentration": doc_extracted.get("top_10_concentration"),
        "sector_concentration_proxy": doc_extracted.get("sector_concentration_proxy"),
        "developed_market_exposure_summary": doc_extracted.get("developed_market_exposure_summary"),
        "emerging_market_exposure_summary": doc_extracted.get("emerging_market_exposure_summary"),
        "us_weight": doc_extracted.get("us_weight"),
        "em_weight": doc_extracted.get("em_weight"),
        "tracking_difference_1y": doc_extracted.get("tracking_difference_1y"),
        "tracking_difference_3y": doc_extracted.get("tracking_difference_3y"),
        "tracking_difference_5y": doc_extracted.get("tracking_difference_5y"),
        "effective_duration": doc_extracted.get("effective_duration"),
        "average_maturity": doc_extracted.get("average_maturity"),
        "yield_proxy": doc_extracted.get("yield_proxy"),
        "credit_quality_mix": doc_extracted.get("credit_quality_mix"),
        "government_vs_corporate_split": doc_extracted.get("government_vs_corporate_split"),
        "interest_rate_sensitivity_proxy": doc_extracted.get("interest_rate_sensitivity_proxy"),
        "weighted_average_maturity": doc_extracted.get("weighted_average_maturity"),
        "portfolio_quality_summary": doc_extracted.get("portfolio_quality_summary"),
        "redemption_settlement_notes": doc_extracted.get("redemption_settlement_notes"),
        "underlying_currency_exposure": doc_extracted.get("underlying_currency_exposure"),
    }
    factsheet_metric_values = {
        "aum": (latest_factsheet_metrics or {}).get("aum_usd"),
        "tracking_difference_1y": (latest_factsheet_metrics or {}).get("tracking_difference_1y"),
        "tracking_difference_3y": (latest_factsheet_metrics or {}).get("tracking_difference_3y"),
        "tracking_difference_5y": (latest_factsheet_metrics or {}).get("tracking_difference_5y"),
        "tracking_error_1y": (latest_factsheet_metrics or {}).get("tracking_error_1y"),
        "yield_proxy": (latest_factsheet_metrics or {}).get("dividend_yield"),
    }
    holdings_field_values = {
        "us_weight": (holdings_profile or {}).get("us_weight"),
        "em_weight": (holdings_profile or {}).get("em_weight"),
        "top_10_concentration": (holdings_profile or {}).get("top_10_concentration"),
        "sector_concentration_proxy": (holdings_profile or {}).get("sector_concentration_proxy"),
        "developed_market_exposure_summary": (holdings_profile or {}).get("developed_market_exposure_summary"),
        "emerging_market_exposure_summary": (holdings_profile or {}).get("emerging_market_exposure_summary"),
        "holdings_count": (holdings_profile or {}).get("holdings_count"),
    }
    market_field_values = {
        "primary_listing_exchange": (latest_market_data or {}).get("exchange"),
        "bid_ask_spread_proxy": (latest_market_data or {}).get("bid_ask_spread_bps"),
        "volume_30d_avg": (latest_market_data or {}).get("volume_30d_avg"),
    }

    base_fields = {
        "candidate_id": symbol,
        "symbol": symbol,
        "fund_name": doc_extracted.get("fund_name") or candidate.get("name"),
        "issuer": candidate.get("issuer"),
        "domicile": candidate.get("domicile"),
        "isin": doc_extracted.get("isin") or candidate.get("isin"),
        "share_class": doc_extracted.get("accumulating_status") or candidate.get("share_class_id") or candidate.get("accumulation_or_distribution"),
        "share_class_proven": bool(doc_extracted.get("accumulating_status")) if doc_available else candidate.get("share_class_proven"),
        "wrapper_or_vehicle_type": doc_extracted.get("wrapper_or_vehicle_type") or _wrapper_label(candidate),
        "distribution_type": doc_extracted.get("accumulating_status") or candidate.get("accumulation_or_distribution"),
        "replication_method": doc_extracted.get("replication_method") or candidate.get("replication_method"),
        "expense_ratio": doc_extracted.get("ter") if doc_extracted.get("ter") is not None else candidate.get("expense_ratio"),
        "source_state": truth_state.get("source_state"),
        "freshness_state": truth_state.get("freshness_state"),
        "factsheet_as_of": doc_extracted.get("factsheet_date") or candidate.get("factsheet_asof"),
        "market_data_as_of": candidate.get("market_data_asof") or (latest_market_data or {}).get("asof_date") or dict(candidate.get("market_history_summary") or {}).get("latest_asof_date"),
        "primary_listing_exchange": doc_extracted.get("primary_listing_exchange") or (latest_market_data or {}).get("exchange") or candidate.get("primary_listing"),
        "primary_trading_currency": doc_extracted.get("primary_trading_currency") or (candidate.get("trading_currency") if str(candidate.get("trading_currency") or "").strip() not in {"", "unknown"} else None),
        "liquidity_proxy": candidate.get("liquidity_proxy"),
        "bid_ask_spread_proxy": (
            (latest_market_data or {}).get("bid_ask_spread_bps")
            if (latest_market_data or {}).get("bid_ask_spread_bps") is not None
            else (performance_metrics or {}).get("spread_bps_latest", candidate.get("bid_ask_spread_proxy"))
        ),
        "volume_30d_avg": (
            (latest_market_data or {}).get("volume_30d_avg")
            if (latest_market_data or {}).get("volume_30d_avg") is not None
            else dict(candidate.get("market_history_summary") or {}).get("latest_volume_30d_avg")
            if dict(candidate.get("market_history_summary") or {}).get("latest_volume_30d_avg") is not None
            else candidate.get("volume_30d_avg")
        ),
        "aum": (
            doc_extracted.get("aum_usd")
            if doc_extracted.get("aum_usd") is not None
            else (
                (latest_factsheet_metrics or {}).get("aum_usd")
                if (latest_factsheet_metrics or {}).get("aum_usd") is not None
                else (performance_metrics or {}).get("aum_usd_latest", candidate.get("aum_usd"))
            )
        ),
        "benchmark_name": doc_extracted.get("benchmark_name") or benchmark_assignment.get("benchmark_label"),
        "benchmark_key": benchmark_assignment.get("benchmark_key"),
        "benchmark_assignment_method": benchmark_assignment.get("assignment_source"),
        "benchmark_assignment_proof": benchmark_assignment.get("benchmark_explanation") or benchmark_assignment.get("why_this_benchmark"),
        "benchmark_confidence": benchmark_assignment.get("benchmark_confidence"),
        "benchmark_validation_status": benchmark_assignment.get("validation_status"),
        "tracking_difference_1y": (
            doc_extracted.get("tracking_difference_1y")
            if doc_extracted.get("tracking_difference_1y") is not None
            else (
                (latest_factsheet_metrics or {}).get("tracking_difference_1y")
                if (latest_factsheet_metrics or {}).get("tracking_difference_1y") is not None
                else (
                    (performance_metrics or {}).get("tracking_difference_1y")
                    if (performance_metrics or {}).get("tracking_difference_1y") is not None
                    else candidate.get("tracking_difference_1y")
                )
            )
        ),
        "tracking_difference_3y": (
            doc_extracted.get("tracking_difference_3y")
            if doc_extracted.get("tracking_difference_3y") is not None
            else (
                (latest_factsheet_metrics or {}).get("tracking_difference_3y")
                if (latest_factsheet_metrics or {}).get("tracking_difference_3y") is not None
                else (
                    (performance_metrics or {}).get("tracking_difference_3y")
                    if (performance_metrics or {}).get("tracking_difference_3y") is not None
                    else candidate.get("tracking_difference_3y")
                )
            )
        ),
        "tracking_difference_5y": (
            doc_extracted.get("tracking_difference_5y")
            if doc_extracted.get("tracking_difference_5y") is not None
            else (
                (latest_factsheet_metrics or {}).get("tracking_difference_5y")
                if (latest_factsheet_metrics or {}).get("tracking_difference_5y") is not None
                else (
                    (performance_metrics or {}).get("tracking_difference_5y")
                    if (performance_metrics or {}).get("tracking_difference_5y") is not None
                    else candidate.get("tracking_difference_5y")
                )
            )
        ),
        "tracking_error_1y": (
            (latest_factsheet_metrics or {}).get("tracking_error_1y")
            if (latest_factsheet_metrics or {}).get("tracking_error_1y") is not None
            else (performance_metrics or {}).get("tracking_error_1y")
        ),
        "developed_market_exposure_summary": (holdings_profile or {}).get("developed_market_exposure_summary") or _developed_em_summary(candidate, sleeve_key=sleeve_key),
        "emerging_market_exposure_summary": (holdings_profile or {}).get("emerging_market_exposure_summary") or _developed_em_summary(candidate, sleeve_key=sleeve_key),
        "us_weight": (holdings_profile or {}).get("us_weight", candidate.get("us_weight_pct")),
        "em_weight": (holdings_profile or {}).get("em_weight", candidate.get("em_weight_pct")),
        "top_10_concentration": (holdings_profile or {}).get("top_10_concentration", candidate.get("top10_concentration_pct")),
        "sector_concentration_proxy": (holdings_profile or {}).get("sector_concentration_proxy", candidate.get("tech_weight_pct")),
        "holdings_count": doc_extracted.get("holdings_count") if doc_extracted.get("holdings_count") is not None else (holdings_profile or {}).get("holdings_count", candidate.get("holdings_count")),
        "withholding_tax_posture": tax_truth.get("withholding_tax_posture"),
        "estate_risk_posture": tax_truth.get("estate_risk_posture"),
        "ucits_status": doc_extracted.get("ucits_status") if doc_extracted.get("ucits_status") is not None else (candidate.get("instrument_type") == "etf_ucits" if str(candidate.get("instrument_type") or "") in {"etf_ucits", "etf_us"} else None),
        "securities_lending_policy": candidate.get("securities_lending_policy"),
        "last_successful_ingest_at": ingest_success_at or dict(candidate.get("latest_fetch_status") or {}).get("latest_success_at"),
        "effective_duration": (
            doc_extracted.get("effective_duration")
            if doc_extracted.get("effective_duration") is not None
            else (
                (performance_metrics or {}).get("duration_years")
                if (performance_metrics or {}).get("duration_years") is not None
                else candidate.get("effective_duration")
            )
        ),
        "average_maturity": (
            doc_extracted.get("average_maturity")
            if doc_extracted.get("average_maturity") is not None
            else (
                (performance_metrics or {}).get("average_maturity_years")
                if (performance_metrics or {}).get("average_maturity_years") is not None
                else candidate.get("average_maturity")
            )
        ),
        "yield_proxy": (
            doc_extracted.get("yield_proxy")
            if doc_extracted.get("yield_proxy") is not None
            else (
                (latest_factsheet_metrics or {}).get("dividend_yield")
                if (latest_factsheet_metrics or {}).get("dividend_yield") is not None
                else (
                (performance_metrics or {}).get("yield_to_maturity")
                or (performance_metrics or {}).get("dividend_yield")
                or candidate.get("yield_proxy")
                )
            )
        ),
        "credit_quality_mix": doc_extracted.get("credit_quality_mix") or (performance_metrics or {}).get("credit_quality_mix") or candidate.get("credit_quality_mix"),
        "government_vs_corporate_split": doc_extracted.get("government_vs_corporate_split") or (performance_metrics or {}).get("government_vs_corporate_split") or candidate.get("government_vs_corporate_split"),
        "issuer_concentration_proxy": (holdings_profile or {}).get("top_10_concentration", candidate.get("top10_concentration_pct")),
        "interest_rate_sensitivity_proxy": (
            doc_extracted.get("interest_rate_sensitivity_proxy")
            if doc_extracted.get("interest_rate_sensitivity_proxy") is not None
            else (
                (performance_metrics or {}).get("duration_years")
                if (performance_metrics or {}).get("duration_years") is not None
                else candidate.get("interest_rate_sensitivity_proxy")
            )
        ),
        "weighted_average_maturity": (
            doc_extracted.get("weighted_average_maturity")
            if doc_extracted.get("weighted_average_maturity") is not None
            else (
                doc_extracted.get("average_maturity")
                if doc_extracted.get("average_maturity") is not None
                else (
                (performance_metrics or {}).get("average_maturity_years")
                if (performance_metrics or {}).get("average_maturity_years") is not None
                else candidate.get("weighted_average_maturity") or candidate.get("average_maturity")
                )
            )
        ),
        "portfolio_quality_summary": doc_extracted.get("portfolio_quality_summary") or doc_extracted.get("credit_quality_mix") or (performance_metrics or {}).get("credit_quality_mix") or candidate.get("portfolio_quality_summary") or candidate.get("credit_quality_mix"),
        "redemption_settlement_notes": doc_extracted.get("redemption_settlement_notes") or candidate.get("settlement_notes") or candidate.get("redemption_settlement_notes"),
        "sg_suitability_note": tax_truth.get("wrapper_notes") or candidate.get("domicile_implication_note"),
        "asset_type_classification": candidate.get("asset_class") or candidate.get("instrument_type"),
        "inflation_linkage_rationale": candidate.get("rationale") if sleeve_key == "real_assets" else None,
        "underlying_currency_exposure": doc_extracted.get("underlying_currency_exposure") or candidate.get("underlying_currency_exposure"),
        "underlying_exposure_profile": doc_extracted.get("underlying_currency_exposure") or candidate.get("underlying_currency_exposure"),
        "distribution_policy": tax_truth.get("distribution_mechanics") or candidate.get("accumulation_or_distribution"),
        "tax_posture": tax_truth.get("wrapper_notes") or candidate.get("withholding_tax_exposure_note"),
        "instrument_type": candidate.get("instrument_type"),
        "role_in_portfolio": (
            dict(candidate.get("eligibility") or {}).get("role_in_portfolio")
            or dict(candidate.get("investment_quality") or {}).get("role_in_portfolio")
            or candidate.get("role_in_portfolio")
            or candidate.get("rationale")
        ),
        "implementation_method": candidate.get("replication_method") or candidate.get("instrument_type"),
        "cost_model": candidate.get("expense_ratio"),
        "liquidity_and_execution_constraints": candidate.get("liquidity_proxy"),
        "scenario_role": candidate.get("rationale"),
        "governance_conditions": "No margin, known max loss, and no short options required." if sleeve_key == "convex" else candidate.get("rationale"),
        "max_loss_known": candidate.get("max_loss_known"),
        "margin_required": candidate.get("margin_required"),
        "short_options": candidate.get("short_options"),
    }

    for field_name, value in base_fields.items():
        missingness = _missingness_for_field(field_name, value)
        provenance = "seeded_fallback"
        source_name = "candidate_payload"
        source_url = base_source_url
        confidence = "low"
        observed_at = base_observed_at
        parser_method = "portfolio_blueprint_runtime"
        if field_name in doc_backed_fields and _has_meaningful(doc_field_values.get(field_name)):
            provenance = "verified_official"
            source_name = "issuer_doc_parser"
            source_url = doc_source_url
            confidence = "high"
            observed_at = doc_observed_at
            parser_method = "fetch_candidate_docs"
        elif field_name in holdings_backed_fields and _has_meaningful(holdings_field_values.get(field_name)):
            provenance = "verified_official"
            source_name = "etf_holdings" if str((holdings_profile or {}).get("coverage_class") or "direct_holdings") == "direct_holdings" else "etf_holdings_summary"
            source_url = str(dict((holdings_profile or {}).get("citation") or {}).get("source_url") or "") or base_source_url
            confidence = "high" if str((holdings_profile or {}).get("coverage_class") or "") == "direct_holdings" else "medium"
            observed_at = str((holdings_profile or {}).get("asof_date") or base_observed_at)
            parser_method = "get_etf_holdings_profile"
        elif field_name in factsheet_metric_fields and _has_meaningful(factsheet_metric_values.get(field_name)):
            provenance = "issuer_documented"
            source_name = "etf_factsheet_metrics"
            source_url = str(dict((latest_factsheet_metrics or {}).get("citation") or {}).get("source_url") or "") or doc_source_url or base_source_url
            confidence = "high"
            observed_at = str((latest_factsheet_metrics or {}).get("asof_date") or base_observed_at)
            parser_method = "get_latest_etf_factsheet_metrics"
        elif field_name in {"tracking_difference_1y", "tracking_difference_3y", "tracking_difference_5y", "tracking_error_1y"} and _has_meaningful((performance_metrics or {}).get(field_name)):
            provenance = "derived_from_validated_history"
            source_name = str((performance_metrics or {}).get("source_name") or "validated_history_derivation")
            source_url = str((performance_metrics or {}).get("source_url") or "") or base_source_url
            confidence = "medium"
            observed_at = str((performance_metrics or {}).get("as_of_date") or base_observed_at)
            parser_method = "performance_metrics"
        elif field_name in market_backed_fields and _has_meaningful(market_field_values.get(field_name)):
            provenance = "verified_nonissuer"
            source_name = "etf_market_data"
            source_url = str(dict((latest_market_data or {}).get("citation") or {}).get("source_url") or "") or base_source_url
            confidence = "medium"
            observed_at = str((latest_market_data or {}).get("asof_date") or base_observed_at)
            parser_method = "get_preferred_latest_market_data"
        elif field_name.startswith("benchmark_"):
            provenance = "verified_nonissuer"
            source_name = "benchmark_registry"
            source_url = None
            confidence = str(benchmark_assignment.get("benchmark_confidence") or "medium")
            observed_at = (performance_metrics or {}).get("as_of_date") or base_observed_at
            parser_method = "benchmark_assignment"
        elif field_name in {"aum", "yield_proxy"} and _has_meaningful((performance_metrics or {}).get(field_name if field_name != "yield_proxy" else "yield_to_maturity")):
            provenance = "verified_nonissuer" if (performance_metrics or {}).get("source_url") else "proxy"
            source_name = str((performance_metrics or {}).get("source_name") or "performance_metrics")
            source_url = str((performance_metrics or {}).get("source_url") or "") or base_source_url
            confidence = "medium"
            observed_at = str((performance_metrics or {}).get("as_of_date") or base_observed_at)
            parser_method = "performance_metrics"
        elif field_name in {"bid_ask_spread_proxy", "volume_30d_avg"}:
            provenance = "verified_nonissuer" if dict(candidate.get("market_history_summary") or {}).get("citation") else "proxy"
            source_name = "market_history_summary"
            source_url = str(dict(dict(candidate.get("market_history_summary") or {}).get("citation") or {}).get("source_url") or "") or base_source_url
            confidence = "medium"
            observed_at = candidate.get("market_data_asof") or dict(candidate.get("market_history_summary") or {}).get("latest_asof_date") or base_observed_at
            parser_method = "market_history_summary"
        elif field_name in {"withholding_tax_posture", "estate_risk_posture", "tax_posture", "sg_suitability_note", "distribution_policy"}:
            provenance = "inferred"
            source_name = "tax_engine"
            source_url = None
            confidence = "medium"
            parser_method = "tax_engine"
        elif field_name in {"source_state", "freshness_state"}:
            provenance = "verified_nonissuer"
            source_name = "candidate_truth_state"
            source_url = None
            confidence = "medium"
            parser_method = "candidate_truth_state"
        _observe(
            field_name,
            value,
            provenance_level=provenance,
            source_name=source_name,
            source_url=source_url,
            observed_at=str(observed_at) if observed_at else None,
            confidence_label=confidence,
            parser_method=parser_method,
            missingness_reason=missingness,
            override_annotation={
                "bucket_source_class": (
                    str((holdings_profile or {}).get("coverage_class") or "")
                    if field_name in holdings_backed_fields
                    else "issuer_doc_parser"
                    if field_name in doc_backed_fields
                    else "provider_market_support"
                    if field_name in market_backed_fields
                    else ""
                ),
                "bucket_parse_confidence": confidence,
                "bucket_failure_reason": (
                    ",".join(list((holdings_profile or {}).get("quality_issues") or [])[:3])
                    if field_name in holdings_backed_fields
                    else ""
                ),
            },
        )

    supplement = dict(_SUPPLEMENTAL_CANDIDATE_METRICS.get(symbol) or {})
    if supplement:
        supplemental_citations = list(supplement.get("citations") or [])
        primary_supplemental = dict(supplemental_citations[0]) if supplemental_citations else {}
        supplemental_source_url = str(primary_supplemental.get("url") or "") or None
        supplemental_observed_at = str(
            primary_supplemental.get("factsheet_asof")
            or primary_supplemental.get("published_at")
            or base_observed_at
            or now.date().isoformat()
        )
        manual_annotation = {
            "actor": "repo_curated_metrics",
            "reason": "Curated supplemental candidate metrics backfill",
            "timestamp": now.isoformat(),
        }
        for field_name, key in (
            ("primary_trading_currency", "trading_currency"),
            ("primary_listing_exchange", "primary_listing_exchange"),
            ("factsheet_as_of", "factsheet_asof"),
            ("aum", "aum_usd"),
            ("us_weight", "us_weight_pct"),
            ("em_weight", "em_weight_pct"),
            ("top_10_concentration", "top10_concentration_pct"),
            ("holdings_count", "holdings_count"),
            ("sector_concentration_proxy", "tech_weight_pct"),
            ("tracking_difference_1y", "tracking_difference_1y"),
            ("tracking_difference_3y", "tracking_difference_3y"),
            ("tracking_difference_5y", "tracking_difference_5y"),
        ):
            value = supplement.get(key)
            if value is None:
                continue
            _observe(
                field_name,
                value,
                provenance_level="manual_reviewed_override",
                source_name="supplemental_candidate_metrics",
                source_url=supplemental_source_url,
                observed_at=supplemental_observed_at,
                confidence_label="medium",
                parser_method="supplemental_metrics_backfill",
                override_annotation=manual_annotation,
            )

    conn.execute(
        "UPDATE candidate_field_observations SET is_current = 0 WHERE candidate_symbol = ? AND sleeve_key = ?",
        (symbol, sleeve_key),
    )
    conn.execute(
        "DELETE FROM candidate_field_current WHERE candidate_symbol = ? AND sleeve_key = ?",
        (symbol, sleeve_key),
    )
    for field_name, row in current_rows_for_write.items():
        conn.execute(
            """
            INSERT OR REPLACE INTO candidate_field_current (
              candidate_symbol, sleeve_key, field_name, resolved_value_json, value_type,
              source_name, source_url, observed_at, ingested_at, provenance_level, confidence_label,
              parser_method, overwrite_priority, missingness_reason, override_annotation_json, last_resolved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                sleeve_key,
                field_name,
                row.get("resolved_value_json"),
                row.get("value_type"),
                row.get("source_name"),
                row.get("source_url"),
                row.get("observed_at"),
                row.get("ingested_at"),
                row.get("provenance_level"),
                row.get("confidence_label"),
                row.get("parser_method"),
                row.get("overwrite_priority"),
                row.get("missingness_reason"),
                _json(row.get("override_annotation") or {}),
                row.get("last_resolved_at"),
            ),
        )
        observation_id = current_observation_ids.get(field_name)
        if observation_id:
            conn.execute(
                "UPDATE candidate_field_observations SET is_current = 1 WHERE observation_id = ?",
                (observation_id,),
            )
    conn.commit()
    return _field_truth_without_runtime_signature(
        get_candidate_field_current(conn, candidate_symbol=symbol, sleeve_key=sleeve_key)
    )


def _build_data_completeness_from_truth(
    candidate: dict[str, Any],
    *,
    sleeve_key: str,
    conn: sqlite3.Connection,
    now: datetime,
) -> dict[str, Any]:
    def _fallback_value(field_name: str) -> Any:
        performance = dict(candidate.get("performance_metrics") or {})
        factsheet_history = dict(candidate.get("aum_history_summary") or {})
        benchmark_assignment = dict(candidate.get("benchmark_assignment") or {})
        if field_name == "aum":
            return (
                candidate.get("aum_usd")
                if candidate.get("aum_usd") is not None
                else performance.get("aum_usd_latest")
                if performance.get("aum_usd_latest") is not None
                else factsheet_history.get("latest_aum_usd")
            )
        if field_name == "tracking_difference_1y":
            return (
                candidate.get("tracking_difference_1y")
                if candidate.get("tracking_difference_1y") is not None
                else performance.get("tracking_difference_1y")
                if performance.get("tracking_difference_1y") is not None
                else factsheet_history.get("tracking_difference_1y")
            )
        if field_name == "tracking_difference_3y":
            return (
                candidate.get("tracking_difference_3y")
                if candidate.get("tracking_difference_3y") is not None
                else performance.get("tracking_difference_3y")
                if performance.get("tracking_difference_3y") is not None
                else factsheet_history.get("tracking_difference_3y")
            )
        if field_name == "holdings_count":
            return candidate.get("holdings_count")
        if field_name == "top_10_concentration":
            return candidate.get("top10_concentration_pct")
        if field_name == "us_weight":
            return candidate.get("us_weight_pct")
        if field_name == "benchmark_key":
            return benchmark_assignment.get("benchmark_key")
        if field_name == "benchmark_confidence":
            return benchmark_assignment.get("benchmark_confidence")
        if field_name == "role_in_portfolio":
            return (
                candidate.get("role_in_portfolio")
                or dict(candidate.get("investment_quality") or {}).get("role_in_portfolio")
                or dict(candidate.get("eligibility") or {}).get("role_in_portfolio")
                or candidate.get("rationale")
            )
        return None

    def _family_state(field_names: list[str], *, field_map: dict[str, Any]) -> str:
        applicable: list[dict[str, Any]] = []
        for name in field_names:
            field = dict(field_map.get(name) or {})
            if str(field.get("missingness_reason") or "") == "not_applicable":
                continue
            applicable.append(field)
        if not applicable:
            return "not_applicable"
        complete = sum(1 for field in applicable if str(field.get("completeness_state") or "") == "complete")
        weak = sum(1 for field in applicable if str(field.get("completeness_state") or "") == "weak_or_partial")
        incomplete = sum(1 for field in applicable if str(field.get("completeness_state") or "") == "incomplete")
        unavailable = sum(1 for field in applicable if str(field.get("completeness_state") or "") == "unavailable")
        total = len(applicable)
        if complete == total:
            return "complete"
        if complete and not unavailable and incomplete <= max(1, total // 3):
            return "partial"
        if weak or (complete and unavailable):
            return "weak"
        if incomplete or unavailable:
            return "missing"
        return "partial"

    snapshot = compute_candidate_completeness(conn, candidate={**candidate, "sleeve_key": sleeve_key}, now=now)
    current = get_candidate_field_current(conn, candidate_symbol=str(candidate.get("symbol") or "").upper(), sleeve_key=sleeve_key)
    labels = {
        "candidate_id": "Candidate ID",
        "symbol": "Symbol",
        "fund_name": "Fund name",
        "issuer": "Issuer",
        "isin": "ISIN",
        "domicile": "Domicile",
        "wrapper_or_vehicle_type": "Wrapper",
        "distribution_type": "Distribution type",
        "expense_ratio": "TER",
        "benchmark_name": "Benchmark",
        "benchmark_confidence": "Benchmark confidence",
        "primary_listing_exchange": "Primary listing",
        "primary_trading_currency": "Primary trading currency",
        "liquidity_proxy": "Liquidity proxy",
        "bid_ask_spread_proxy": "Spread proxy",
        "developed_market_exposure_summary": "Developed versus EM exposure",
        "us_weight": "US weight",
        "top_10_concentration": "Top 10 concentration",
        "sector_concentration_proxy": "Sector concentration proxy",
        "tracking_difference_1y": "Tracking difference",
        "withholding_tax_posture": "Withholding tax posture",
        "estate_risk_posture": "Estate risk posture",
        "factsheet_as_of": "Factsheet as of",
        "market_data_as_of": "Market data as of",
        "effective_duration": "Duration",
        "yield_proxy": "Yield proxy",
        "credit_quality_mix": "Credit quality mix",
        "issuer_concentration_proxy": "Issuer concentration proxy",
        "interest_rate_sensitivity_proxy": "Rate sensitivity inputs",
        "role_in_portfolio": "Role in portfolio",
        "implementation_method": "Implementation method",
        "cost_model": "Cost model",
        "liquidity_and_execution_constraints": "Liquidity and execution constraints",
        "scenario_role": "Scenario role",
        "governance_conditions": "Governance conditions",
        "max_loss_known": "Max loss known",
        "margin_required": "Margin required",
        "short_options": "Short options",
    }
    entries: list[dict[str, Any]] = []
    dominant_missing: dict[str, int] = {}
    status_counts = {
        "populated": 0,
        "missing_but_fetchable": 0,
        "missing_requires_source_expansion": 0,
        "not_applicable": 0,
    }
    corrected_critical_missing: list[str] = []
    fetchable_missing_count = 0
    source_gap_missing_count = 0
    populated_count = 0
    for requirement in list(snapshot.get("requirements") or []):
        field_name = str(requirement["field_name"])
        field = current.get(field_name)
        missingness = str(requirement.get("missingness_reason") or (field or {}).get("missingness_reason") or "blocked_by_source_gap")
        fallback_value = _fallback_value(field_name)
        if missingness != "populated" and _field_population_state(fallback_value) == "populated":
            missingness = "populated"
        value = fallback_value if _field_population_state(fallback_value) == "populated" else requirement.get("value")
        if missingness == "not_applicable":
            status = "not_applicable"
            value = None
        elif missingness == "populated":
            status = "populated"
            populated_count += 1
        elif missingness in {"fetchable_from_current_sources", "blocked_by_parser_gap"}:
            status = "missing_but_fetchable"
            value = None
            fetchable_missing_count += 1
            if bool(requirement["critical"]):
                corrected_critical_missing.append(field_name)
            dominant_missing[labels.get(field_name, field_name)] = dominant_missing.get(labels.get(field_name, field_name), 0) + 1
        else:
            status = "missing_requires_source_expansion"
            value = None
            source_gap_missing_count += 1
            if bool(requirement["critical"]):
                corrected_critical_missing.append(field_name)
            dominant_missing[labels.get(field_name, field_name)] = dominant_missing.get(labels.get(field_name, field_name), 0) + 1
        status_counts[status] = status_counts.get(status, 0) + 1
        entries.append(
            {
                "key": field_name,
                "label": labels.get(field_name, field_name.replace("_", " ").title()),
                "critical": bool(requirement["critical"]),
                "status": status,
                "value": value,
                "value_type": (field or {}).get("value_type"),
                "provenance_level": (field or {}).get("provenance_level"),
                "source_type": (field or {}).get("source_type"),
                "evidence_class": (field or {}).get("evidence_class"),
                "as_of": (field or {}).get("as_of"),
                "completeness_state": (field or {}).get("completeness_state"),
                "source_name": (field or {}).get("source_name"),
            }
        )
    family_completeness = {
        "holdings_exposure_completeness": _family_state(
            [
                "holdings_count",
                "top_10_concentration",
                "us_weight",
                "em_weight",
                "sector_concentration_proxy",
                "developed_market_exposure_summary",
                "emerging_market_exposure_summary",
            ],
            field_map=current,
        ),
        "implementation_truth_completeness": _family_state(
            [
                "primary_trading_currency",
                "primary_listing_exchange",
                "domicile",
                "wrapper_or_vehicle_type",
                "distribution_type",
                "share_class",
                "share_class_proven",
                "replication_method",
                "benchmark_assignment_method",
                "benchmark_assignment_proof",
            ],
            field_map=current,
        ),
        "benchmark_history_completeness": _family_state(
            [
                "benchmark_key",
                "benchmark_name",
                "benchmark_confidence",
                "tracking_difference_1y",
                "tracking_difference_3y",
                "tracking_error_1y",
            ],
            field_map=current,
        ),
        "performance_metric_completeness": _family_state(
            [
                "tracking_difference_1y",
                "tracking_difference_3y",
                "tracking_error_1y",
                "aum",
            ],
            field_map=current,
        ),
        "liquidity_evidence_completeness": _family_state(
            ["liquidity_proxy", "bid_ask_spread_proxy", "volume_30d_avg", "market_data_as_of"],
            field_map=current,
        ),
        "sleeve_specific_support_completeness": _family_state(
            (
                ["effective_duration", "average_maturity", "yield_proxy", "credit_quality_mix", "interest_rate_sensitivity_proxy"]
                if sleeve_key == "ig_bonds"
                else ["yield_proxy", "weighted_average_maturity", "portfolio_quality_summary", "redemption_settlement_notes", "sg_suitability_note"]
                if sleeve_key == "cash_bills"
                else ["asset_type_classification", "inflation_linkage_rationale", "underlying_exposure_profile", "tax_posture"]
                if sleeve_key == "real_assets"
                else ["role_in_portfolio", "implementation_method", "cost_model", "liquidity_and_execution_constraints", "scenario_role"]
                if sleeve_key in {"alternatives", "convex"}
                else ["developed_market_exposure_summary", "emerging_market_exposure_summary", "withholding_tax_posture", "estate_risk_posture"]
            ),
            field_map=current,
        ),
    }
    required_total = sum(1 for entry in entries if entry["status"] != "not_applicable")
    if corrected_critical_missing:
        readiness_level = "review_ready" if populated_count >= min(required_total, 6) else "research_visible"
    elif populated_count >= max(1, required_total - max(2, source_gap_missing_count)):
        readiness_level = "shortlist_ready"
    elif populated_count >= min(required_total, 6):
        readiness_level = "review_ready"
    else:
        readiness_level = "research_visible"
    return {
        "requirements": entries,
        "required_fields_complete_count": populated_count,
        "critical_required_fields_missing_count": len(corrected_critical_missing),
        "critical_required_fields_missing": corrected_critical_missing,
        "fetchable_missing_count": fetchable_missing_count,
        "source_gap_missing_count": source_gap_missing_count,
        "proxy_only_count": snapshot["proxy_only_count"],
        "stale_required_count": snapshot["stale_required_count"],
        "readiness_level": readiness_level,
        "dominant_missing_categories": [label for label, _ in sorted(dominant_missing.items(), key=lambda kv: (-kv[1], kv[0]))[:6]],
        "status_counts": status_counts,
        "truth_snapshot_id": snapshot["snapshot_id"],
        "family_completeness": family_completeness,
    }


def _required_field_matrix(candidate: dict[str, Any], *, sleeve_key: str) -> list[dict[str, Any]]:
    if sleeve_key == "ig_bonds":
        base = _IG_BOND_REQUIRED_FIELDS
    elif sleeve_key == "convex":
        base = _CONVEX_REQUIRED_FIELDS
    elif sleeve_key == "cash_bills":
        base = _CASH_REQUIRED_FIELDS
    elif sleeve_key in {"real_assets", "alternatives"}:
        base = [
            {"key": "issuer", "label": "Issuer", "critical": True},
            {"key": "fund_name", "label": "Fund name", "critical": True},
            {"key": "identifier", "label": "Identifier", "critical": True},
            {"key": "domicile", "label": "Domicile", "critical": False},
            {"key": "wrapper", "label": "Wrapper", "critical": False},
            {"key": "ter", "label": "TER", "critical": False},
            {"key": "liquidity_proxy", "label": "Liquidity proxy", "critical": True},
            {"key": "factsheet_asof", "label": "Factsheet as of", "critical": False},
        ]
    else:
        base = _CORE_EQUITY_REQUIRED_FIELDS
    return [dict(item) for item in base]


def _field_population_state(value: Any) -> str:
    if value is None:
        return "missing"
    if isinstance(value, str) and not value.strip():
        return "missing"
    return "populated"


def _build_data_completeness(
    candidate: dict[str, Any],
    *,
    sleeve_key: str,
    conn: sqlite3.Connection | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if conn is not None and now is not None:
        return _build_data_completeness_from_truth(candidate, sleeve_key=sleeve_key, conn=conn, now=now)

    requirements = _required_field_matrix(candidate, sleeve_key=sleeve_key)
    entries: list[dict[str, Any]] = []
    complete_count = 0
    critical_missing = 0
    for requirement in requirements:
        key = str(requirement["key"])
        value = _field_value_for_completeness(candidate, sleeve_key=sleeve_key, field_key=key)
        populated = _field_population_state(value) == "populated"
        if populated:
            complete_count += 1
        elif bool(requirement["critical"]):
            critical_missing += 1
        entries.append(
            {
                "key": key,
                "label": str(requirement["label"]),
                "critical": bool(requirement["critical"]),
                "status": "populated" if populated else "missing_requires_source_expansion",
                "value": value,
            }
        )
    readiness_level = "review_ready" if complete_count >= min(6, len(requirements)) else "research_visible"
    return {
        "requirements": entries,
        "required_fields_complete_count": complete_count,
        "critical_required_fields_missing_count": critical_missing,
        "readiness_level": readiness_level,
        "dominant_missing_categories": [],
        "status_counts": {
            "populated": complete_count,
            "missing_but_fetchable": 0,
            "missing_requires_source_expansion": max(0, len(requirements) - complete_count),
            "not_applicable": 0,
        },
    }


_HIGH_IMPACT_FIELD_TRUTH_CONFIG: list[dict[str, Any]] = [
    {"field_name": "us_weight", "label": "US weight", "decision_critical": False, "affects": ["confidence", "implementation"]},
    {"field_name": "top_10_concentration", "label": "Top 10 concentration", "decision_critical": False, "affects": ["confidence", "implementation"]},
    {"field_name": "holdings_count", "label": "Holdings count", "decision_critical": False, "affects": ["confidence", "implementation"]},
    {"field_name": "primary_trading_currency", "label": "Primary trading currency", "decision_critical": True, "affects": ["eligibility", "implementation"]},
    {"field_name": "tracking_difference_1y", "label": "Tracking difference 1Y", "decision_critical": False, "affects": ["scoring", "confidence"]},
    {"field_name": "tracking_difference_3y", "label": "Tracking difference 3Y", "decision_critical": False, "affects": ["scoring", "confidence"]},
    {"field_name": "volume_30d_avg", "label": "30d average volume", "decision_critical": False, "affects": ["confidence", "implementation"]},
    {"field_name": "benchmark_key", "label": "Benchmark key", "decision_critical": True, "affects": ["eligibility", "scoring", "confidence"]},
    {"field_name": "benchmark_confidence", "label": "Benchmark confidence", "decision_critical": True, "affects": ["eligibility", "confidence"]},
]


def _field_truth_value_state(field: dict[str, Any]) -> str:
    missingness = str(field.get("missingness_reason") or "")
    completeness_state = str(field.get("completeness_state") or "")
    if missingness == "not_applicable":
        return "not_applicable"
    if missingness == "populated" and field.get("resolved_value") not in {None, "", "unknown"}:
        if completeness_state == "weak_or_partial":
            return "partial"
        return "resolved"
    if missingness in {"fetchable_from_current_sources", "blocked_by_parser_gap"}:
        return "incomplete_but_fixable"
    if missingness == "blocked_by_source_gap":
        return "missing_source_coverage"
    return "not_yet_established"


def _band_from_pct(value: Any) -> str:
    try:
        numeric = float(value)
    except Exception:
        return "low"
    if numeric >= 85:
        return "high"
    if numeric >= 65:
        return "medium"
    return "low"


def _band_from_unknown_share(value: Any) -> str:
    try:
        numeric = float(value)
    except Exception:
        return "unknown"
    if numeric <= 0.10:
        return "low"
    if numeric <= 0.25:
        return "moderate"
    return "high"


def _build_field_truth_surface(candidate: dict[str, Any]) -> dict[str, Any]:
    field_truth = dict(candidate.get("field_truth") or {})
    liquidity_profile = dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {})
    performance = dict(candidate.get("performance_metrics") or {})
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    sg_lens = dict(candidate.get("sg_lens") or {})
    entries: list[dict[str, Any]] = []
    for config in _HIGH_IMPACT_FIELD_TRUTH_CONFIG:
        field_name = str(config["field_name"])
        field = dict(field_truth.get(field_name) or {})
        entries.append(
            {
                "field_name": field_name,
                "label": str(config["label"]),
                "value": field.get("resolved_value"),
                "value_state": _field_truth_value_state(field),
                "missingness_reason": field.get("missingness_reason") or "blocked_by_source_gap",
                "fetchability_classification": field.get("missingness_reason") or "blocked_by_source_gap",
                "decision_critical": bool(config["decision_critical"]),
                "affects": list(config["affects"]),
                "source_type": field.get("source_type"),
                "evidence_class": field.get("evidence_class"),
                "as_of": field.get("as_of"),
                "completeness_state": field.get("completeness_state"),
            }
        )
    entries.extend(
        [
            {
                "field_name": "liquidity_status",
                "label": "Liquidity status",
                "value": liquidity_profile.get("liquidity_status"),
                "value_state": "resolved" if liquidity_profile.get("liquidity_status") else "not_yet_established",
                "missingness_reason": "populated" if liquidity_profile.get("liquidity_status") else "fetchable_from_current_sources",
                "fetchability_classification": "fetchable_from_current_sources",
                "decision_critical": True,
                "affects": ["eligibility", "implementation", "confidence"],
                "source_type": "derived_runtime",
                "evidence_class": "derived_from_validated_history",
                "as_of": candidate.get("market_data_asof") or dict(candidate.get("market_history_summary") or {}).get("latest_asof_date"),
                "completeness_state": "complete" if liquidity_profile.get("liquidity_status") else "incomplete",
            },
            {
                "field_name": "spread_status",
                "label": "Spread support",
                "value": liquidity_profile.get("spread_status"),
                "value_state": "resolved" if liquidity_profile.get("spread_status") else "not_yet_established",
                "missingness_reason": "populated" if liquidity_profile.get("spread_status") else "fetchable_from_current_sources",
                "fetchability_classification": "fetchable_from_current_sources",
                "decision_critical": False,
                "affects": ["confidence", "implementation"],
                "source_type": "derived_runtime",
                "evidence_class": "derived_from_validated_history",
                "as_of": performance.get("retrieved_at"),
                "completeness_state": "complete" if liquidity_profile.get("spread_status") else "incomplete",
            },
            {
                "field_name": "sg_lens.score",
                "label": "Singapore lens score",
                "value": sg_lens.get("score"),
                "value_state": "resolved" if sg_lens.get("score") is not None else "not_yet_established",
                "missingness_reason": "populated" if sg_lens.get("score") is not None else "fetchable_from_current_sources",
                "fetchability_classification": "fetchable_from_current_sources",
                "decision_critical": True,
                "affects": ["eligibility", "confidence", "implementation"],
                "source_type": "derived_tax_lens",
                "evidence_class": "derived_from_validated_history",
                "as_of": candidate.get("factsheet_asof"),
                "completeness_state": "complete" if sg_lens.get("score") is not None else "incomplete",
            },
            {
                "field_name": "benchmark_validation",
                "label": "Benchmark validation",
                "value": benchmark.get("validation_status"),
                "value_state": "resolved" if benchmark.get("validation_status") else "not_yet_established",
                "missingness_reason": "populated" if benchmark.get("validation_status") else "fetchable_from_current_sources",
                "fetchability_classification": "fetchable_from_current_sources",
                "decision_critical": True,
                "affects": ["eligibility", "confidence"],
                "source_type": "benchmark_registry",
                "evidence_class": benchmark.get("benchmark_kind"),
                "as_of": performance.get("as_of_date"),
                "completeness_state": "complete" if benchmark.get("validation_status") else "incomplete",
            },
            {
                "field_name": "benchmark_fit_type",
                "label": "Benchmark fit type",
                "value": benchmark.get("benchmark_fit_type"),
                "value_state": "resolved" if benchmark.get("benchmark_fit_type") else "not_yet_established",
                "missingness_reason": "populated" if benchmark.get("benchmark_fit_type") else "fetchable_from_current_sources",
                "fetchability_classification": "fetchable_from_current_sources",
                "decision_critical": True,
                "affects": ["eligibility", "scoring", "confidence"],
                "source_type": "benchmark_registry",
                "evidence_class": benchmark.get("benchmark_kind"),
                "as_of": performance.get("as_of_date"),
                "completeness_state": "complete" if benchmark.get("benchmark_fit_type") else "incomplete",
            },
        ]
    )
    missing = [entry["label"] for entry in entries if str(entry.get("value_state") or "") not in {"resolved", "partial", "not_applicable"}]
    return {
        "fields": entries,
        "missing_fields": missing[:8],
        "critical_missing_fields": [entry["label"] for entry in entries if entry.get("decision_critical") and str(entry.get("value_state") or "") not in {"resolved", "partial", "not_applicable"}][:6],
    }


def _build_score_honesty_view(candidate: dict[str, Any], *, sleeve_candidates: list[dict[str, Any]]) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    score = quality.get("composite_score")
    candidate_score = float(score or 0.0) if score is not None else None
    valid_scores = sorted(
        [
            float(dict(peer.get("investment_quality") or {}).get("composite_score") or 0.0)
            for peer in sleeve_candidates
            if dict(peer.get("investment_quality") or {}).get("composite_score") is not None
        ],
        reverse=True,
    )
    leader_score = valid_scores[0] if valid_scores else None
    gap_to_leader = round((leader_score - candidate_score), 2) if leader_score is not None and candidate_score is not None else None
    gap_type = (
        "structural"
        if gap_to_leader is not None and gap_to_leader >= 8
        else "marginal"
        if gap_to_leader is not None and gap_to_leader <= 3
        else "not_decision_relevant"
        if gap_to_leader is not None
        else "not_available"
    )
    unknown_dimensions = list(quality.get("unknown_dimensions") or [])
    score_critical_unknowns = [item for item in unknown_dimensions if item in {"cost", "liquidity", "structure", "tax", "performance_evidence", "benchmark_support"}]
    confidence_only_unknowns = [item for item in unknown_dimensions if item not in score_critical_unknowns]
    weighted_completeness = quality.get("weighted_data_completeness_pct")
    weighted_unknown_share = quality.get("weighted_unknown_share")
    return {
        "composite_score": score,
        "composite_score_valid": bool(quality.get("composite_score_valid", True)),
        "composite_score_display": quality.get("composite_score_display"),
        "evidence_coverage_band": _band_from_pct(weighted_completeness),
        "unknown_share_band": _band_from_unknown_share(weighted_unknown_share),
        "scoring_model_version": quality.get("score_version"),
        "comparability": "fully_comparable" if bool(quality.get("composite_score_valid", True)) and (weighted_unknown_share or 0) <= 0.10 else "partially_comparable",
        "unknown_dimensions": unknown_dimensions,
        "weighted_unknown_share": weighted_unknown_share,
        "weighted_data_completeness_pct": weighted_completeness,
        "score_critical_unknown_dimensions": score_critical_unknowns,
        "confidence_reducing_only_dimensions": confidence_only_unknowns,
        "relative_context": {
            "rank_in_sleeve": quality.get("rank_in_sleeve"),
            "percentile_in_sleeve": quality.get("percentile_in_sleeve"),
            "distance_to_leader": gap_to_leader,
            "score_gap_type": gap_type,
        },
    }


def _build_benchmark_dependency_diagnostics(candidate: dict[str, Any]) -> dict[str, Any]:
    decision = dict(candidate.get("decision_record") or {})
    benchmark = dict(candidate.get("benchmark_assignment") or {})
    performance_support_state = str(candidate.get("performance_support_state") or "partial_due_to_missing_metrics")
    fit_type = str(benchmark.get("benchmark_fit_type") or "unknown")
    validation_status = str(benchmark.get("validation_status") or "unassigned")
    eligibility_block = "required_benchmark_support" in list(dict(decision.get("policy_gates") or {}).get("failed_gate_names") or [])
    unresolved = "required_benchmark_support" in list(dict(decision.get("policy_gates") or {}).get("partial_gate_names") or [])
    performance_block = performance_support_state in {"unsupported", "benchmark_proxy_only"}
    confidence_downgrade = fit_type in {"acceptable_proxy", "weak_proxy", "mismatched"} or str(benchmark.get("benchmark_authority_level") or "") in {"limited", "insufficient"}
    recommendation_downgrade = str(decision.get("final_decision_state") or "") in {"blocked_by_unresolved_gate", "blocked_by_missing_required_evidence"} and (eligibility_block or unresolved or confidence_downgrade)
    if fit_type == "mismatched":
        root = "benchmark truly inappropriate for sleeve"
    elif validation_status in {"unassigned", "assigned_no_metrics"}:
        root = "benchmark support present but incomplete"
    elif fit_type == "acceptable_proxy":
        root = "benchmark validation missing or proxy-based"
    elif performance_block:
        root = "benchmark metrics missing"
    else:
        root = "benchmark only needed as context, not as decisive hard gate"
    return {
        "benchmark_fit_type": fit_type,
        "benchmark_role": benchmark.get("benchmark_role"),
        "benchmark_authority_level": benchmark.get("benchmark_authority_level"),
        "validation_status": validation_status,
        "dependency_chain": {
            "eligibility_block": eligibility_block,
            "performance_scoring_block": performance_block,
            "confidence_downgrade": confidence_downgrade,
            "recommendation_downgrade": recommendation_downgrade,
            "unresolved_required_gate": unresolved,
        },
        "root_cause": root,
        "summary": str(benchmark.get("benchmark_truth_summary") or benchmark.get("benchmark_explanation") or ""),
    }


def _build_pressure_fix_mapping(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    mappings: list[dict[str, Any]] = []
    for pressure in list(dict(candidate.get("eligibility") or {}).get("pressures") or []):
        pressure_type = str(pressure.get("pressure_type") or "data")
        remediation = {
            "benchmark": "benchmark assignment or benchmark-history support",
            "data": "source or parser work",
            "structure": "candidate replacement rather than repair",
            "liquidity": "liquidity evidence or alternate vehicle",
            "tax_wrapper": "tax confirmation or wrapper validation",
            "performance_evidence": "validated history and internal metric derivation",
            "readiness": "required evidence completion",
            "replacement": "peer review and candidate rerank",
        }.get(pressure_type, "additional review")
        likely_effort = {
            "benchmark": "medium",
            "data": "medium",
            "structure": "high",
            "liquidity": "medium",
            "tax_wrapper": "medium",
            "performance_evidence": "medium",
            "readiness": "low",
            "replacement": "low",
        }.get(pressure_type, "medium")
        mappings.append(
            {
                "pressure_type": pressure_type,
                "investor_meaning": pressure.get("detail") or pressure.get("label"),
                "recommendation_effect": pressure.get("recommendation_effect"),
                "possible_remediation": remediation,
                "likely_effort_class": likely_effort,
                "can_change_ranking": pressure_type in {"benchmark", "liquidity", "performance_evidence", "replacement"},
            }
        )
    return mappings


def _build_upgrade_path(candidate: dict[str, Any]) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    decision = dict(candidate.get("decision_record") or {})
    readiness = dict(candidate.get("decision_readiness") or {})
    usability = dict(candidate.get("usability_memo") or {})
    user_state = str(quality.get("user_facing_state") or "")
    if user_state == "fully_clean_recommendable":
        return {
            "current_state": user_state,
            "next_meaningful_state": None,
            "smallest_change": "No upgrade path is required because the candidate is already fully clean for recommendation use.",
            "required_evidence_or_condition": [],
            "upgrade_dependency": "none",
            "upgrade_path_valid": False,
        }
    rejection_reason = dict(decision.get("rejection_reason") or {})
    if user_state == "blocked_by_policy":
        return {
            "current_state": user_state,
            "next_meaningful_state": None,
            "smallest_change": "No valid upgrade path exists inside this sleeve because the blocker is structural or policy-based.",
            "required_evidence_or_condition": list(dict(decision.get("policy_gates") or {}).get("failed_gate_names") or []),
            "upgrade_dependency": "genuine_policy_relaxation_not_permitted",
            "upgrade_path_valid": False,
        }
    blockers = list(readiness.get("what_must_change") or []) or list(usability.get("upgrade_conditions") or []) or list(dict(candidate.get("data_completeness") or {}).get("critical_required_fields_missing") or [])
    smallest_change = blockers[0] if blockers else "One material evidence or implementation gap still needs to be cleared."
    dependency = "upstream_data"
    lowered = smallest_change.lower()
    if "parser" in lowered:
        dependency = "parser_support"
    elif "benchmark" in lowered:
        dependency = "benchmark_assignment"
    elif "liquidity" in lowered or "spread" in lowered or "volume" in lowered:
        dependency = "liquidity_evidence"
    elif "tax" in lowered or "currency" in lowered or "share-class" in lowered:
        dependency = "tax_confirmation"
    next_state = {
        "blocked_by_missing_required_evidence": "research_ready_but_not_recommendable",
        "blocked_by_unresolved_gate": "best_available_with_limits",
        "research_ready_but_not_recommendable": "best_available_with_limits",
        "best_available_with_limits": "fully_clean_recommendable",
    }.get(user_state, "best_available_with_limits")
    if rejection_reason.get("root_cause_class") == "unresolved_gate":
        dependency = "upstream_data"
    return {
        "current_state": user_state or quality.get("recommendation_state"),
        "next_meaningful_state": next_state,
        "smallest_change": smallest_change,
        "required_evidence_or_condition": blockers[:4],
        "upgrade_dependency": dependency,
        "upgrade_path_valid": True,
    }


def _blocker_severity(blocker: str) -> str:
    text = str(blocker or "").lower()
    if any(token in text for token in ("benchmark", "quarantined", "risk controls failed", "manual static", "unverified", "liquidity is weak", "source state is stale", "freshness state is stale", "tax data is too incomplete")):
        return "critical"
    if any(token in text for token in ("spread", "liquidity inputs", "source gaps", "factsheet freshness", "aging", "performance support", "source state is")):
        return "important"
    return "informational"


def _ranked_blockers(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    eligibility = dict(candidate.get("eligibility") or {})
    completeness = dict(candidate.get("data_completeness") or {})
    items: list[dict[str, Any]] = []
    blocker_details = list(eligibility.get("eligibility_blocker_details") or [])
    if blocker_details:
        for blocker in blocker_details:
            items.append(
                {
                    "label": str(blocker.get("reason") or ""),
                    "severity": str(blocker.get("severity") or _blocker_severity(str(blocker.get("reason") or ""))),
                    "source": str(blocker.get("category") or "governance"),
                    "root_cause": blocker.get("root_cause"),
                }
            )
    else:
        for blocker in list(eligibility.get("eligibility_blockers") or []):
            items.append({"label": str(blocker), "severity": _blocker_severity(str(blocker)), "source": "governance"})
    for entry in list(completeness.get("requirements") or []):
        if str(entry.get("status")) != "populated":
            items.append(
                {
                    "label": f"{entry.get('label')} missing",
                    "severity": "critical" if bool(entry.get("critical")) else "important",
                    "source": str(entry.get("status")),
                }
            )
    unique: dict[str, dict[str, Any]] = {}
    order = {"critical": 0, "important": 1, "informational": 2}
    for item in items:
        key = str(item["label"])
        existing = unique.get(key)
        if existing is None or order[str(item["severity"])] < order[str(existing["severity"])]:
            unique[key] = item
    return sorted(unique.values(), key=lambda item: (order[str(item["severity"])], str(item["label"])))[:6]


def _dominant_implementation_trait(candidate: dict[str, Any]) -> str:
    if candidate.get("expense_ratio") is not None:
        return f"TER {(float(candidate.get('expense_ratio') or 0.0) * 100.0):.2f}%"
    if str(candidate.get("accumulation_or_distribution") or "").strip():
        return str(candidate.get("accumulation_or_distribution"))
    if str(candidate.get("replication_method") or "").strip():
        return str(candidate.get("replication_method"))
    return str(candidate.get("domicile") or "Unknown")


def _decision_readiness_summary(candidate: dict[str, Any], *, sleeve_candidates: list[dict[str, Any]]) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    completeness = dict(candidate.get("data_completeness") or {})
    eligibility = dict(candidate.get("eligibility") or {})
    recommendation_state = str(quality.get("recommendation_state") or "")
    pressures = list(eligibility.get("pressures") or [])
    blockers = _ranked_blockers(candidate)
    readiness_level = str(completeness.get("readiness_level") or "research_visible")
    nearest_peer = None
    passing_peers = [
        peer for peer in sleeve_candidates
        if str(peer.get("symbol") or "") != str(candidate.get("symbol") or "")
        and str(dict(peer.get("data_completeness") or {}).get("readiness_level") or "") in {"shortlist_ready", "recommendation_ready"}
        and str(dict(peer.get("investment_quality") or {}).get("eligibility_state") or "") in {"eligible", "eligible_with_caution"}
    ]
    if passing_peers:
        nearest = sorted(
            passing_peers,
            key=lambda peer: (
                _recommendation_state_priority(str(dict(peer.get("investment_quality") or {}).get("recommendation_state") or "")),
                int(dict(peer.get("investment_quality") or {}).get("rank_in_sleeve") or 9999),
                -float(dict(peer.get("investment_quality") or {}).get("composite_score") or -1e9),
            ),
        )[0]
        nearest_peer = {
            "symbol": nearest.get("symbol"),
            "name": nearest.get("name"),
            "recommendation_state": dict(nearest.get("investment_quality") or {}).get("recommendation_state"),
            "rank_in_sleeve": dict(nearest.get("investment_quality") or {}).get("rank_in_sleeve"),
            "readiness_level": dict(nearest.get("data_completeness") or {}).get("readiness_level"),
        }
    what_must_change = [str(item["label"]) for item in blockers if str(item["severity"]) in {"critical", "important"}][:4]
    primary_blocker = blockers[0]["label"] if blockers else None
    action_readiness = str(candidate.get("action_readiness") or "review_only")
    usability_memo = dict(candidate.get("usability_memo") or {})
    can_use_now = action_readiness in {"usable_now", "usable_with_limits"}
    return {
        "can_use_now": can_use_now,
        "current_status": recommendation_state or dict(candidate.get("eligibility") or {}).get("eligibility_state") or "research_visible",
        "action_readiness": action_readiness,
        "readiness_level": readiness_level,
        "top_blockers": blockers,
        "primary_blocker": primary_blocker,
        "best_alternative": nearest_peer,
        "what_must_change": what_must_change,
        "confidence_reducers": list(usability_memo.get("confidence_reducers") or [])[:4],
        "upgrade_conditions": list(usability_memo.get("upgrade_conditions") or [])[:4],
        "dominant_trust_state": str(candidate.get("display_source_state") or candidate.get("source_state") or "unknown"),
        "dominant_missing_categories": list(completeness.get("dominant_missing_categories") or []),
        "dominant_implementation_trait": _dominant_implementation_trait(candidate),
        "pressures": pressures,
        "primary_pressure_type": eligibility.get("primary_pressure_type"),
        "secondary_pressure_type": eligibility.get("secondary_pressure_type"),
    }


def _build_sleeve_coverage_gap_analysis(sleeves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    analysis: list[dict[str, Any]] = []
    for sleeve in sleeves:
        candidates = list(sleeve.get("candidates") or [])
        fully_clean = next(
            (
                candidate for candidate in candidates
                if str(dict(candidate.get("investment_quality") or {}).get("user_facing_state") or "") == "fully_clean_recommendable"
            ),
            None,
        )
        best_available = next(
            (
                candidate for candidate in candidates
                if str(dict(candidate.get("investment_quality") or {}).get("user_facing_state") or "") == "best_available_with_limits"
            ),
            None,
        )
        dominant_blockers: list[str] = []
        for candidate in candidates:
            dominant_blockers.extend(list(dict(candidate.get("decision_readiness") or {}).get("what_must_change") or [])[:1])
        dominant_blocker = dominant_blockers[0] if dominant_blockers else None
        state = (
            "fully_clean_candidate_available"
            if fully_clean
            else "best_available_only"
            if best_available
            else "no_recommendable_candidate"
        )
        analysis.append(
            {
                "sleeve_key": sleeve.get("sleeve_key"),
                "state": state,
                "fully_clean_symbol": fully_clean.get("symbol") if fully_clean else None,
                "best_available_symbol": best_available.get("symbol") if best_available else None,
                "dominant_blocker_class": dict((best_available or candidates[0] if candidates else {}).get("decision_record") or {}).get("rejection_reason", {}).get("root_cause_class") if candidates else None,
                "dominant_blocker": dominant_blocker,
            }
        )
    return analysis


def _build_portfolio_governance_summary(*, sleeves: list[dict[str, Any]], previous_payload: dict[str, Any] | None) -> dict[str, Any]:
    candidates = [candidate for sleeve in sleeves for candidate in list(sleeve.get("candidates") or [])]
    state_counts: dict[str, int] = {}
    root_cause_counts: dict[str, int] = {}
    score_validity_distribution = {"valid": 0, "invalid": 0}
    unknown_share_distribution = {"low": 0, "moderate": 0, "high": 0, "unknown": 0}
    benchmark_dependency_failures = 0
    top_missing_fields: dict[str, int] = {}
    top_source_coverage_gaps: dict[str, int] = {}
    top_parser_gaps: dict[str, int] = {}
    unresolved_required_gates = 0
    highest_leverage_fix_candidates: list[dict[str, Any]] = []
    stability_shift_counts = {"improving": 0, "stable": 0, "worsening": 0}

    for candidate in candidates:
        quality = dict(candidate.get("investment_quality") or {})
        decision = dict(candidate.get("decision_record") or {})
        user_state = str(quality.get("user_facing_state") or "research_ready_but_not_recommendable")
        state_counts[user_state] = state_counts.get(user_state, 0) + 1
        root = str(dict(decision.get("rejection_reason") or {}).get("root_cause_class") or "mixed")
        root_cause_counts[root] = root_cause_counts.get(root, 0) + 1
        if bool(quality.get("composite_score_valid", True)):
            score_validity_distribution["valid"] += 1
        else:
            score_validity_distribution["invalid"] += 1
        unknown_band = _band_from_unknown_share(quality.get("weighted_unknown_share"))
        unknown_share_distribution[unknown_band] = unknown_share_distribution.get(unknown_band, 0) + 1
        if bool(dict(candidate.get("benchmark_dependency_diagnostics") or {}).get("dependency_chain", {}).get("recommendation_downgrade")):
            benchmark_dependency_failures += 1
        if str(decision.get("required_gate_resolution_state") or "") == "unresolved":
            unresolved_required_gates += 1
        for field in list(dict(candidate.get("field_truth_surface") or {}).get("fields") or []):
            label = str(field.get("label") or field.get("field_name") or "")
            fetchability = str(field.get("fetchability_classification") or "")
            if str(field.get("value_state") or "") not in {"resolved", "partial", "not_applicable"}:
                top_missing_fields[label] = top_missing_fields.get(label, 0) + 1
            if fetchability == "blocked_by_source_gap":
                top_source_coverage_gaps[label] = top_source_coverage_gaps.get(label, 0) + 1
            if fetchability == "blocked_by_parser_gap":
                top_parser_gaps[label] = top_parser_gaps.get(label, 0) + 1
        confidence_history = dict(candidate.get("confidence_history") or {})
        trust_direction = str(confidence_history.get("trust_direction") or "stable")
        stability_shift_counts[trust_direction] = stability_shift_counts.get(trust_direction, 0) + 1
        upgrade_path = dict(candidate.get("upgrade_path") or {})
        if upgrade_path.get("upgrade_path_valid"):
            highest_leverage_fix_candidates.append(
                {
                    "symbol": candidate.get("symbol"),
                    "sleeve_key": candidate.get("sleeve_key"),
                    "reason_for_upgrade": upgrade_path.get("smallest_change"),
                    "next_state": upgrade_path.get("next_meaningful_state"),
                    "dependency": upgrade_path.get("upgrade_dependency"),
                }
            )

    highest_leverage_fix_candidates = highest_leverage_fix_candidates[:8]
    sleeve_coverage_gap_analysis = _build_sleeve_coverage_gap_analysis(sleeves)
    sleeves_lacking_fully_clean_candidates = [
        entry["sleeve_key"]
        for entry in sleeve_coverage_gap_analysis
        if entry["state"] != "fully_clean_candidate_available"
    ]
    primary_picks = [
        candidate
        for candidate in candidates
        if str(dict(candidate.get("investment_quality") or {}).get("recommendation_state") or "") == "recommended_primary"
    ]
    shared_weak_benchmark = [candidate.get("symbol") for candidate in primary_picks if str(dict(candidate.get("benchmark_assignment") or {}).get("benchmark_authority_level") or "") in {"limited", "insufficient"}]
    shared_evidence_gaps = [candidate.get("symbol") for candidate in primary_picks if str(dict(candidate.get("investment_quality") or {}).get("user_facing_state") or "") == "best_available_with_limits"]
    issuer_counts: dict[str, int] = {}
    for candidate in primary_picks:
        issuer = str(candidate.get("issuer") or "Unknown")
        issuer_counts[issuer] = issuer_counts.get(issuer, 0) + 1
    implementation_concentration = [issuer for issuer, count in issuer_counts.items() if count >= 2]

    return {
        "candidate_state_counts": state_counts,
        "primary_root_cause_counts": root_cause_counts,
        "unresolved_required_gate_count": unresolved_required_gates,
        "score_validity_distribution": score_validity_distribution,
        "unknown_share_distribution": unknown_share_distribution,
        "benchmark_dependency_failures": benchmark_dependency_failures,
        "top_missing_fields": [{"field": label, "count": count} for label, count in sorted(top_missing_fields.items(), key=lambda item: (-item[1], item[0]))[:8]],
        "top_source_coverage_gaps": [{"field": label, "count": count} for label, count in sorted(top_source_coverage_gaps.items(), key=lambda item: (-item[1], item[0]))[:8]],
        "top_parser_gaps": [{"field": label, "count": count} for label, count in sorted(top_parser_gaps.items(), key=lambda item: (-item[1], item[0]))[:8]],
        "highest_leverage_fix_candidates": highest_leverage_fix_candidates,
        "sleeves_lacking_fully_clean_candidates": sleeves_lacking_fully_clean_candidates,
        "recommendation_stability_shifts": stability_shift_counts,
        "sleeve_coverage_gap_analysis": sleeve_coverage_gap_analysis,
        "portfolio_level_gate_aggregation": {
            "shared_benchmark_weakness_symbols": shared_weak_benchmark,
            "shared_evidence_gap_symbols": shared_evidence_gaps,
            "implementation_concentration_issuers": implementation_concentration,
        },
        "cross_sleeve_impact_view": {
            "shared_benchmark_dependency_note": (
                f"Primary recommendations relying on weaker benchmark authority: {', '.join(shared_weak_benchmark)}."
                if shared_weak_benchmark
                else "Primary recommendations are not currently concentrated in weak benchmark authority."
            ),
            "shared_evidence_gap_note": (
                f"Current primary set still shares evidence-limited candidates: {', '.join(shared_evidence_gaps)}."
                if shared_evidence_gaps
                else "Current primary set is not mainly driven by shared evidence gaps."
            ),
        },
        "data_quality_sla_monitoring": {
            "freshness_deterioration": sum(1 for candidate in candidates if str(candidate.get("freshness_state") or "") in {"stale", "quarantined"}),
            "critical_field_missingness_trend": len([item for item in top_missing_fields if top_missing_fields[item] >= 2]),
            "benchmark_support_trend": benchmark_dependency_failures,
            "liquidity_evidence_trend": sum(
                1
                for candidate in candidates
                if str(dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {}).get("liquidity_status") or "") in {"weak", "limited_evidence", "unknown"}
            ),
        },
        "previous_payload_present": previous_payload is not None,
    }


def _candidate_universe_support(candidate: dict[str, Any]) -> dict[str, str]:
    quality = dict(candidate.get("investment_quality") or {})
    completeness = dict(candidate.get("data_completeness") or {})
    readiness_level = str(completeness.get("readiness_level") or "research_visible")
    recommendation_state = str(quality.get("recommendation_state") or "research_only")
    blockers = list(dict(candidate.get("decision_record") or {}).get("rejection_reasons") or [])
    usability_state = str(candidate.get("action_readiness") or "review_only")

    if usability_state in {"usable_now", "usable_with_limits"}:
        return {
            "candidate_universe_state": "active",
            "candidate_universe_reason": "usable now" if usability_state == "usable_now" else "usable with limits",
            "deliverable_readiness_state": usability_state,
        }
    if any("holdings source coverage" in str(item).lower() for item in blockers):
        return {
            "candidate_universe_state": "under_review",
            "candidate_universe_reason": "insufficient exposure truth",
            "deliverable_readiness_state": "review_only",
        }
    if any("benchmark" in str(item).lower() for item in blockers):
        return {
            "candidate_universe_state": "under_review",
            "candidate_universe_reason": "benchmark support incomplete",
            "deliverable_readiness_state": "review_only",
        }
    if usability_state == "review_only" or recommendation_state == "watchlist_only":
        return {
            "candidate_universe_state": "under_review",
            "candidate_universe_reason": "evidence improved",
            "deliverable_readiness_state": "review_only",
        }
    if recommendation_state == "research_only" or usability_state == "not_usable_now":
        return {
            "candidate_universe_state": "fallback_only",
            "candidate_universe_reason": "insufficient exposure truth" if blockers else "benchmark support incomplete",
            "deliverable_readiness_state": "not_usable_now",
        }
    return {
        "candidate_universe_state": "under_review",
        "candidate_universe_reason": "evidence improved",
        "deliverable_readiness_state": readiness_level if readiness_level != "research_visible" else "review_only",
    }


def _sleeve_readiness_summary(sleeve_key: str, candidates: list[dict[str, Any]], recommendation: dict[str, Any]) -> dict[str, Any]:
    counts = {"research_visible": 0, "review_ready": 0, "shortlist_ready": 0, "recommendation_ready": 0}
    usability_counts = {"usable_now": 0, "usable_with_limits": 0, "review_only": 0, "not_usable_now": 0}
    blocker_counts: dict[str, int] = {}
    missing_category_counts: dict[str, int] = {}
    pressure_counts: dict[str, int] = {}
    nearest = None
    for candidate in candidates:
        level = str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "research_visible")
        counts[level] = counts.get(level, 0) + 1
        usability = str(candidate.get("action_readiness") or "review_only")
        usability_counts[usability] = usability_counts.get(usability, 0) + 1
        for blocker in list(dict(candidate.get("decision_readiness") or {}).get("top_blockers") or []):
            label = str(blocker.get("label") or "")
            if label:
                blocker_counts[label] = blocker_counts.get(label, 0) + 1
        for category in list(dict(candidate.get("data_completeness") or {}).get("dominant_missing_categories") or []):
            name = str(category or "").strip()
            if name:
                missing_category_counts[name] = missing_category_counts.get(name, 0) + 1
        for pressure in list(dict(candidate.get("eligibility") or {}).get("pressures") or []):
            pressure_type = str(dict(pressure).get("pressure_type") or "").strip()
            if pressure_type:
                pressure_counts[pressure_type] = pressure_counts.get(pressure_type, 0) + 1
        if nearest is None and usability in {"usable_now", "usable_with_limits"}:
            nearest = {
                "symbol": candidate.get("symbol"),
                "name": candidate.get("name"),
                "recommendation_state": dict(candidate.get("investment_quality") or {}).get("recommendation_state"),
                "action_readiness": usability,
            }
    if recommendation.get("our_pick_symbol"):
        explanation = recommendation.get("why_this_pick_wins") or "Current pick is the strongest recommendation-ready candidate."
    else:
        main_reasons = [label for label, _ in sorted(blocker_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:3]]
        dominant_missing = [label for label, _ in sorted(missing_category_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:3]]
        explanation = (
            "No current pick because all candidates fail recommendation readiness."
            + (f" Main reasons: {', '.join(main_reasons)}." if main_reasons else "")
            + (f" Dominant missing categories: {', '.join(dominant_missing)}." if dominant_missing else "")
        )
    return {
        "readiness_counts": counts,
        "usability_counts": usability_counts,
        "no_pick_explanation": explanation,
        "nearest_passing_candidate": nearest,
        "dominant_missing_categories": [label for label, _ in sorted(missing_category_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:5]],
        "dominant_pressure_types": [label for label, _ in sorted(pressure_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:5]],
    }


def _build_sleeve_recommendation_presentation(
    sleeve: dict[str, Any],
    *,
    candidates: list[dict[str, Any]],
    recommendation: dict[str, Any],
    readiness_summary: dict[str, Any],
) -> dict[str, Any]:
    top_candidate = next(
        (
            candidate
            for candidate in candidates
            if str(candidate.get("symbol") or "").upper() == str(recommendation.get("our_pick_symbol") or "").upper()
        ),
        candidates[0] if candidates else None,
    )
    canonical_decision = dict(top_candidate.get("canonical_decision") or {}) if isinstance(top_candidate, dict) else {}
    report = dict(canonical_decision.get("report_sections") or {})
    action_boundary = dict(canonical_decision.get("action_boundary") or {})
    what_changes = dict(canonical_decision.get("what_changes_the_view") or {})
    portfolio_consequence = dict(top_candidate.get("portfolio_consequence_summary") or {}) if isinstance(top_candidate, dict) else {}
    review_escalation = dict(top_candidate.get("review_escalation") or {}) if isinstance(top_candidate, dict) else {}
    status_label = str(canonical_decision.get("promotion_state") or "").replace("_", " ").strip().title()
    status_explanation = str(report.get("current_view") or "")
    if not status_label:
        status_label = "Current sleeve lead available" if recommendation.get("our_pick_symbol") else "No clean sleeve lead yet"
    if not status_explanation:
        status_explanation = str(recommendation.get("no_current_pick_reason") or recommendation.get("why_this_pick_wins") or readiness_summary.get("no_pick_explanation") or "Sleeve review remains provisional.")
    next_review_action = str(
        recommendation.get("next_review_question")
        or (list(what_changes.get("conditions") or [])[:1] or [None])[0]
        or (list(dict(top_candidate.get("decision_readiness") or {}).get("what_must_change") or [])[:1] or [None])[0]
        or "Review the strongest evidence gap before changing the sleeve lead."
    )
    portfolio_effect = str(
        portfolio_consequence.get("current_holding_relative_effect")
        or portfolio_consequence.get("baseline_relative_effect")
        or dict(top_candidate.get("investor_consequence_summary") or {}).get("implementation_quality_effect")
        or "Portfolio consequence remains conditional until the sleeve lead is fully confirmed."
    )
    boundary = str(
        recommendation.get("what_would_change_the_pick")
        or report.get("main_tradeoff")
        or action_boundary.get("why_boundary_exists")
        or "Do not treat the current lead as final if evidence quality or implementation fit weakens."
    )
    visible_alternatives: list[dict[str, Any]] = []
    for candidate in candidates[1:4]:
        alternative_decision = dict(candidate.get("canonical_decision") or {})
        alternative_report = dict(alternative_decision.get("report_sections") or {})
        visible_alternatives.append(
            {
                "symbol": candidate.get("symbol"),
                "name": candidate.get("name"),
                "standing": str(
                    alternative_report.get("current_view")
                    or dict(candidate.get("investment_quality") or {}).get("investment_thesis")
                    or "Alternative remains in review."
                ),
            }
        )
    return {
        "status_label": status_label,
        "status_reason": status_explanation,
        "decision_title": str(recommendation.get("our_pick_symbol") or "No current pick"),
        "decision_summary": str(
            report.get("current_view")
            or recommendation.get("why_this_pick_wins")
            or recommendation.get("no_current_pick_reason")
            or "Current sleeve ordering remains provisional."
        ),
        "current_need": str(sleeve.get("purpose") or "Sleeve role is still being clarified."),
        "lead_candidate_symbol": top_candidate.get("symbol") if isinstance(top_candidate, dict) else recommendation.get("our_pick_symbol"),
        "lead_thesis": str(
            report.get("why_attractive")
            or dict(top_candidate.get("investment_quality") or {}).get("investment_thesis")
            or "Lead thesis is still being assembled."
        ),
        "portfolio_consequence": portfolio_effect,
        "next_review_action": next_review_action,
        "boundary": boundary,
        "review_priority": str(recommendation.get("review_escalation_level") or review_escalation.get("level") or "review"),
        "lead_strength": str(recommendation.get("winner_stability") or dict(top_candidate.get("decision_thesis") or {}).get("lead_strength") or "conditional"),
        "evidence_state": str(
            dict(top_candidate.get("investor_recommendation_status") or {}).get("recommendation_confidence")
            or dict(top_candidate.get("investment_quality") or {}).get("recommendation_confidence")
            or "moderate"
        ),
        "visible_alternatives": visible_alternatives,
    }


def _recommendation_history_by_candidate(conn: sqlite3.Connection, *, limit: int = 500) -> dict[str, list[dict[str, Any]]]:
    items = list_recommendation_events(conn, limit=limit)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        key = f"{str(item.get('sleeve_key') or '')}::{str(item.get('candidate_symbol') or '').upper()}"
        grouped.setdefault(key, []).append(item)
    return grouped


_CORE_LENS_METRIC_KEYS = ("DGS10", "T10YIE", "VIXCLS", "BAMLH0A0HYM2", "SP500")
_EXTENDED_LENS_METRIC_KEYS = (
    "REAL_YIELD_10Y",
    "USD_STRENGTH",
    "IG_CREDIT_SPREADS",
    "BOND_VOLATILITY",
    "GLOBAL_EQUITY_EX_US",
)
_LENS_METRIC_KEYS = _CORE_LENS_METRIC_KEYS + _EXTENDED_LENS_METRIC_KEYS

_LENS_METRIC_LABELS: dict[str, str] = {
    "DGS10": "US 10Y Treasury yield",
    "T10YIE": "US 10Y inflation expectations",
    "VIXCLS": "Market volatility (VIX)",
    "BAMLH0A0HYM2": "High yield credit spread",
    "SP500": "S&P 500 index",
    "REAL_YIELD_10Y": "US 10Y real yield",
    "USD_STRENGTH": "US dollar strength",
    "IG_CREDIT_SPREADS": "Investment grade credit spread",
    "BOND_VOLATILITY": "Bond market volatility",
    "GLOBAL_EQUITY_EX_US": "Global equities ex-US proxy",
}

_LENS_REGIME_THRESHOLDS: dict[str, dict[str, float]] = {
    "volatility": {
        "vix_elevated": 18.0,
        "vix_high": 25.0,
        "persistence_days": 3.0,
    },
    "rates": {
        "dgs10_1d_fast": 0.10,
        "dgs10_5d_fast": 0.20,
        "real_yield_1d_fast": 0.08,
        "real_yield_5d_fast": 0.15,
    },
    "credit": {
        "hy_elevated": 3.5,
        "hy_high": 5.0,
        "hy_5d_widening_elevated": 0.20,
        "hy_5d_widening_high": 0.40,
    },
}

_SENSITIVITY_RANK: dict[str, int] = {"high": 3, "medium": 2, "low": 1}


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _percentile_rank(values: list[float], current: float) -> float | None:
    if not values:
        return None
    count = sum(1 for value in values if value <= current)
    return round((count / len(values)) * 100.0, 1)


def _format_signed(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.2f}"


def _format_lag_cause(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "expected_publication_lag":
        return "expected publication lag"
    if normalized == "unexpected_ingestion_lag":
        return "unexpected ingestion lag"
    return "unknown"


def _format_metric_evidence(metric_key: str, metrics: dict[str, dict[str, Any]]) -> str:
    metric = dict(metrics.get(metric_key) or {})
    value = _safe_float(metric.get("value"))
    observed_at = str(metric.get("observed_at") or "unavailable")
    lag_cause = _format_lag_cause(str(metric.get("lag_cause") or "unknown"))
    lag_days = metric.get("lag_days")
    lag_part = f"{lag_cause}{f' ({lag_days}d)' if isinstance(lag_days, int) else ''}"
    if value is None:
        return (
            f"{metric_key} unavailable | 1d n/a | 5d n/a | 1m n/a | 1y pct n/a | "
            f"observed_at {observed_at} | lag {lag_part}"
        )
    pct_1y = _safe_float(metric.get("percentile_1y"))
    return (
        f"{metric_key} {value:.2f} | 1d {_format_signed(_safe_float(metric.get('delta_1d')))} | "
        f"5d {_format_signed(_safe_float(metric.get('delta_5d')))} | 1m {_format_signed(_safe_float(metric.get('delta_1m')))} | "
        f"1y pct {(f'{pct_1y:.1f}' if pct_1y is not None else 'n/a')} | observed_at {observed_at} | lag {lag_part}"
    )


def _metric_label(metric_key: str) -> str:
    return _LENS_METRIC_LABELS.get(metric_key, metric_key)


def _normalize_metric_row(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "value": _safe_float(item.get("value")),
        "delta_1d": _safe_float(item.get("delta_1d")),
        "delta_5d": _safe_float(item.get("window_5_change")),
        "delta_1m": _safe_float(item.get("window_20_change")),
        "state": str(item.get("curr_state") or item.get("state_short") or "").strip().lower() or None,
        "days_in_state": int(item.get("days_in_state_short") or 1),
        "observed_at": str(item.get("observed_at") or "").strip() or None,
        "retrieved_at": str(item.get("retrieved_at") or "").strip() or None,
        "lag_days": int(item.get("lag_days")) if item.get("lag_days") is not None else None,
        "lag_class": str(item.get("lag_class") or "").strip() or None,
        "lag_cause": str(item.get("lag_cause") or "").strip() or None,
        "percentile_1y": None,
    }


def _load_lens_regime_context(settings: Settings) -> dict[str, Any]:
    conn = None
    try:
        conn = connect(get_db_path(settings=settings))
        payload = load_latest_delta_payload(conn)
    except Exception:  # noqa: BLE001
        if conn is not None:
            conn.close()
        return {"available": False, "metrics": {}, "long_state": None, "short_state": None}

    try:
        regimes = dict(payload.get("regimes") or {})
        short_regime = dict(regimes.get("short") or {})
        long_regime = dict(regimes.get("long") or {})
        payload_metrics: dict[str, dict[str, Any]] = {}
        for item in list(payload.get("metrics") or []):
            key = str(item.get("metric_id") or item.get("metric_key") or "").strip().upper()
            if key in _LENS_METRIC_KEYS:
                payload_metrics[key] = dict(item)

        metrics: dict[str, dict[str, Any]] = {}
        for key in _LENS_METRIC_KEYS:
            row = conn.execute(
                """
                SELECT value, delta_1d, window_5_change, window_20_change, state_short, days_in_state_short,
                       observed_at, retrieved_at, lag_days, lag_class, lag_cause
                FROM metric_snapshots
                WHERE metric_id = ?
                ORDER BY asof_ts DESC
                LIMIT 1
                """,
                (key,),
            ).fetchone()
            merged = _normalize_metric_row(payload_metrics.get(key) or {})
            if row is not None:
                merged.update(
                    {
                        "value": _safe_float(row["value"]),
                        "delta_1d": _safe_float(row["delta_1d"]),
                        "delta_5d": _safe_float(row["window_5_change"]),
                        "delta_1m": _safe_float(row["window_20_change"]),
                        "state": str(row["state_short"] or merged.get("state") or "").strip().lower() or None,
                        "days_in_state": int(row["days_in_state_short"] or 1),
                        "observed_at": str(row["observed_at"] or "").strip() or merged.get("observed_at"),
                        "retrieved_at": str(row["retrieved_at"] or "").strip() or merged.get("retrieved_at"),
                        "lag_days": int(row["lag_days"]) if row["lag_days"] is not None else merged.get("lag_days"),
                        "lag_class": str(row["lag_class"] or "").strip() or merged.get("lag_class"),
                        "lag_cause": str(row["lag_cause"] or "").strip() or merged.get("lag_cause"),
                    }
                )
            lag_days = merged.get("lag_days")
            if isinstance(lag_days, int):
                merged["lag_cause"] = classify_lag_cause(
                    series_key=key,
                    observed_at=str(merged.get("observed_at") or ""),
                    retrieved_at=str(merged.get("retrieved_at") or ""),
                    lag_days=lag_days,
                    retrieval_succeeded=True,
                    cache_fallback_used=False,
                    latest_available_matches_observed=True,
                    previous_observed_at=None,
                )
            value = _safe_float(merged.get("value"))
            if value is not None:
                history_rows = conn.execute(
                    """
                    SELECT value
                    FROM metric_snapshots
                    WHERE metric_id = ?
                    ORDER BY asof_ts DESC
                    LIMIT 252
                    """,
                    (key,),
                ).fetchall()
                history = [float(item["value"]) for item in history_rows if item["value"] is not None]
                if len(history) >= 30:
                    merged["percentile_1y"] = _percentile_rank(history, float(value))
            if any(merged.get(field) is not None for field in ("value", "observed_at", "lag_cause")):
                metrics[key] = merged

        return {
            "available": bool(metrics),
            "metrics": metrics,
            "long_state": str(long_regime.get("state") or "").strip().lower() or None,
            "short_state": str(short_regime.get("state") or "").strip().lower() or None,
        }
    finally:
        conn.close()


def _date_from_observed(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    chunk = text[:10]
    try:
        return datetime.fromisoformat(chunk)
    except ValueError:
        return None


def _core_stale_reasons(*, metrics: dict[str, dict[str, Any]], now: datetime) -> list[str]:
    reasons: list[str] = []
    for key in _CORE_LENS_METRIC_KEYS:
        metric = dict(metrics.get(key) or {})
        observed = _date_from_observed(str(metric.get("observed_at") or ""))
        lag_days = metric.get("lag_days")
        lag_cause = str(metric.get("lag_cause") or "unknown")
        if observed is None:
            reasons.append(f"{key}: observed_at unavailable")
            continue
        age_days = (now.date() - observed.date()).days
        if age_days > 1:
            reasons.append(f"{key}: stale observed_at {observed.date().isoformat()} ({age_days}d)")
        if isinstance(lag_days, int) and lag_days > 1:
            reasons.append(f"{key}: lag {lag_days}d ({lag_cause})")
        if lag_cause == "unexpected_ingestion_lag":
            reasons.append(f"{key}: unexpected_ingestion_lag")
    return reasons


def _compute_data_freshness(*, metrics: dict[str, dict[str, Any]], now: datetime) -> str:
    dates: list[datetime] = []
    for key in _CORE_LENS_METRIC_KEYS:
        observed = _date_from_observed(str(dict(metrics.get(key) or {}).get("observed_at") or ""))
        if observed is not None:
            dates.append(observed)
    if not dates:
        return "unknown"
    latest = max(dates)
    lag_days = (now.date() - latest.date()).days
    if lag_days <= 1:
        return "fresh"
    if lag_days <= 3:
        return "recent"
    if lag_days <= 7:
        return "aging"
    return "stale"


def _build_regime_snapshot(*, context: dict[str, Any], now: datetime) -> dict[str, Any]:
    metrics = dict(context.get("metrics") or {})
    vol_cfg = dict(_LENS_REGIME_THRESHOLDS["volatility"])
    rates_cfg = dict(_LENS_REGIME_THRESHOLDS["rates"])
    credit_cfg = dict(_LENS_REGIME_THRESHOLDS["credit"])

    vix = dict(metrics.get("VIXCLS") or {})
    dgs10 = dict(metrics.get("DGS10") or {})
    real_yield = dict(metrics.get("REAL_YIELD_10Y") or {})
    hy = dict(metrics.get("BAMLH0A0HYM2") or {})

    vix_value = _safe_float(vix.get("value"))
    vix_days = int(vix.get("days_in_state") or 1)
    vol_partial = False
    if vix_value is None:
        volatility_label = "unknown"
        volatility_rule_summary = "Insufficient data: VIX value unavailable."
    elif vix_value >= vol_cfg["vix_high"] and vix_days >= int(vol_cfg["persistence_days"]):
        volatility_label = "stress"
        volatility_rule_summary = (
            f"Rule VIX>= {vol_cfg['vix_high']:.0f} for >= {int(vol_cfg['persistence_days'])}d; "
            f"now {vix_value:.2f} for {vix_days}d."
        )
    elif vix_value >= vol_cfg["vix_elevated"] and vix_days >= int(vol_cfg["persistence_days"]):
        volatility_label = "moderate"
        volatility_rule_summary = (
            f"Rule VIX>= {vol_cfg['vix_elevated']:.0f} for >= {int(vol_cfg['persistence_days'])}d; "
            f"now {vix_value:.2f} for {vix_days}d."
        )
    elif vix_value >= vol_cfg["vix_elevated"]:
        volatility_label = "moderate (partial)"
        vol_partial = True
        volatility_rule_summary = (
            f"Partial: VIX {vix_value:.2f} is above {vol_cfg['vix_elevated']:.0f}, "
            f"but persistence {vix_days}d is below {int(vol_cfg['persistence_days'])}d."
        )
    else:
        volatility_label = "low"
        volatility_rule_summary = (
            f"Rule VIX below elevated threshold {vol_cfg['vix_elevated']:.0f}; now {vix_value:.2f}."
        )

    dgs_1d = _safe_float(dgs10.get("delta_1d"))
    dgs_5d = _safe_float(dgs10.get("delta_5d"))
    real_1d = _safe_float(real_yield.get("delta_1d"))
    real_5d = _safe_float(real_yield.get("delta_5d"))
    has_dgs = any(value is not None for value in (dgs_1d, dgs_5d))
    has_real = any(value is not None for value in (real_1d, real_5d))
    rates_partial = False
    if not has_dgs and not has_real:
        rates_pressure_label = "unknown"
        rates_rule_summary = "Insufficient data: DGS10 and REAL_YIELD_10Y change fields unavailable."
    elif not has_real or not has_dgs:
        rates_pressure_label = "partial"
        rates_partial = True
        rates_rule_summary = (
            f"Partial: requires both DGS10 and REAL_YIELD_10Y. "
            f"DGS10 1d {_format_signed(dgs_1d)}, 5d {_format_signed(dgs_5d)}; "
            f"REAL_YIELD_10Y 1d {_format_signed(real_1d)}, 5d {_format_signed(real_5d)}."
        )
    else:
        rates_speed_high = (
            (dgs_1d is not None and dgs_1d >= rates_cfg["dgs10_1d_fast"])
            or (dgs_5d is not None and dgs_5d >= rates_cfg["dgs10_5d_fast"])
            or (real_1d is not None and real_1d >= rates_cfg["real_yield_1d_fast"])
            or (real_5d is not None and real_5d >= rates_cfg["real_yield_5d_fast"])
        )
        rates_speed_mod = (
            (dgs_1d is not None and dgs_1d > 0)
            or (dgs_5d is not None and dgs_5d > 0)
            or (real_1d is not None and real_1d > 0)
            or (real_5d is not None and real_5d > 0)
        )
        rates_pressure_label = "stress" if rates_speed_high else ("moderate" if rates_speed_mod else "low")
        rates_rule_summary = (
            f"Rule uses DGS10/REAL_YIELD speed; now DGS10 1d {_format_signed(dgs_1d)}, 5d {_format_signed(dgs_5d)}, "
            f"REAL_YIELD_10Y 1d {_format_signed(real_1d)}, 5d {_format_signed(real_5d)}."
        )

    hy_value = _safe_float(hy.get("value"))
    hy_5d = _safe_float(hy.get("delta_5d"))
    credit_partial = False
    if hy_value is None and hy_5d is None:
        credit_stress_label = "unknown"
        credit_rule_summary = "Insufficient data: HY level/widening fields unavailable."
    elif (
        (hy_value is not None and hy_value >= credit_cfg["hy_high"])
        or (hy_5d is not None and hy_5d >= credit_cfg["hy_5d_widening_high"])
    ):
        credit_stress_label = "stress"
        credit_rule_summary = f"Rule HY level/widening; now level {hy_value:.2f} and 5d {_format_signed(hy_5d)}."
    elif (
        (hy_value is not None and hy_value >= credit_cfg["hy_elevated"])
        or (hy_5d is not None and hy_5d >= credit_cfg["hy_5d_widening_elevated"])
    ):
        credit_stress_label = "moderate"
        credit_rule_summary = f"Rule HY level/widening; now level {hy_value:.2f} and 5d {_format_signed(hy_5d)}."
    else:
        credit_stress_label = "low"
        credit_rule_summary = f"Rule HY level/widening; now level {hy_value:.2f} and 5d {_format_signed(hy_5d)}."

    if volatility_label == "unknown" or credit_stress_label == "unknown":
        risk_appetite_label = "unknown"
    elif vol_partial or credit_partial:
        risk_appetite_label = "partial"
    elif volatility_label == "stress" or credit_stress_label == "stress":
        risk_appetite_label = "elevated_risk_off"
    elif volatility_label == "low" and credit_stress_label == "low":
        risk_appetite_label = "risk_on"
    else:
        risk_appetite_label = "normal"
    risk_appetite_rule_summary = "Rule combines volatility and credit-stress labels."

    volatility_evidence = _format_metric_evidence("VIXCLS", metrics)
    rates_evidence = f"{_format_metric_evidence('DGS10', metrics)}; {_format_metric_evidence('REAL_YIELD_10Y', metrics)}"
    credit_evidence = _format_metric_evidence("BAMLH0A0HYM2", metrics)
    risk_evidence_parts = []
    if "VIXCLS" in metrics:
        risk_evidence_parts.append(f"volatility={volatility_label}")
    if "BAMLH0A0HYM2" in metrics:
        risk_evidence_parts.append(f"credit={credit_stress_label}")
    risk_appetite_evidence = (
        ", ".join(risk_evidence_parts)
        if risk_evidence_parts
        else "Composite inputs unavailable (volatility and credit series missing)."
    )

    data_freshness = _compute_data_freshness(metrics=metrics, now=now)
    stale_core_series = _core_stale_reasons(metrics=metrics, now=now)
    missing_inputs: list[str] = []
    if vix_value is None:
        missing_inputs.append(_metric_label("VIXCLS"))
    if not has_dgs:
        missing_inputs.append(_metric_label("DGS10"))
    if not has_real:
        missing_inputs.append(_metric_label("REAL_YIELD_10Y"))
    if hy_value is None and hy_5d is None:
        missing_inputs.append(_metric_label("BAMLH0A0HYM2"))

    core_present = sum(1 for key in _CORE_LENS_METRIC_KEYS if key in metrics)
    if not context.get("available"):
        confidence = "low"
        confidence_reason = "Regime snapshot unavailable from current run."
    elif missing_inputs:
        confidence = "low"
        confidence_reason = f"Missing core inputs: {', '.join(missing_inputs[:3])}."
    elif stale_core_series:
        confidence = "low"
        confidence_reason = f"Core series stale/lagged: {', '.join(stale_core_series[:3])}."
    elif core_present >= 4:
        confidence = "high"
        confidence_reason = "Core series available with no stale/lagged flags."
    else:
        confidence = "medium"
        confidence_reason = "Core series partially available."

    rules = [
        {
            "rule_id": "volatility_vix_persistence",
            "description": "Volatility uses VIX level and persistence window.",
            "inputs_used": ["VIXCLS"],
            "satisfied": bool(vix_value is not None and vix_days >= int(vol_cfg["persistence_days"])),
            "notes": volatility_rule_summary,
        },
        {
            "rule_id": "rates_dgs10_real_yield",
            "description": "Rates pressure requires both nominal and real-yield speed inputs.",
            "inputs_used": ["DGS10", "REAL_YIELD_10Y"],
            "satisfied": bool(has_dgs and has_real),
            "notes": rates_rule_summary,
        },
        {
            "rule_id": "credit_hy_level_widening",
            "description": "Credit stress uses HY spread level and widening rate.",
            "inputs_used": ["BAMLH0A0HYM2"],
            "satisfied": bool(hy_value is not None or hy_5d is not None),
            "notes": credit_rule_summary,
        },
        {
            "rule_id": "risk_appetite_composite",
            "description": "Risk appetite combines volatility and credit-stress labels.",
            "inputs_used": ["VIXCLS", "BAMLH0A0HYM2"],
            "satisfied": risk_appetite_label not in {"unknown", "partial"},
            "notes": risk_appetite_rule_summary,
        },
    ]

    return {
        "risk_appetite_label": risk_appetite_label,
        "rates_pressure_label": rates_pressure_label,
        "credit_stress_label": credit_stress_label,
        "volatility_label": volatility_label,
        "confidence": confidence,
        "data_freshness": data_freshness,
        "confidence_reason": confidence_reason,
        "stale_core_series": stale_core_series,
        "missing_inputs": missing_inputs,
        "rule_summaries": {
            "volatility": volatility_rule_summary,
            "rates_pressure": rates_rule_summary,
            "credit_stress": credit_rule_summary,
            "risk_appetite": risk_appetite_rule_summary,
        },
        "rule_evidence": {
            "volatility": volatility_evidence,
            "rates_pressure": rates_evidence,
            "credit_stress": credit_evidence,
            "risk_appetite": risk_appetite_evidence,
        },
        "rules": rules,
    }


def _monitor_line(metric_key: str, context: dict[str, Any], fallback: str) -> str:
    metrics = dict(context.get("metrics") or {})
    metric = dict(metrics.get(metric_key) or {})
    value = _safe_float(metric.get("value"))
    if value is None:
        return (
            f"Monitoring now: {metric_key} level n/a | 1d n/a | 5d n/a | 1m n/a | 1y pct n/a | "
            f"observed_at unavailable | lag unknown. Context: {fallback}"
        )
    observed_at = str(metric.get("observed_at") or "n/a")
    lag_cause = str(metric.get("lag_cause") or "unknown")
    lag_cause_text = _format_lag_cause(lag_cause)
    lag_days = metric.get("lag_days")
    pct_1y = _safe_float(metric.get("percentile_1y"))
    return (
        f"Monitoring now: {metric_key} level {value:.2f} | 1d {_format_signed(_safe_float(metric.get('delta_1d')))} | "
        f"5d {_format_signed(_safe_float(metric.get('delta_5d')))} | 1m {_format_signed(_safe_float(metric.get('delta_1m')))} | "
        f"1y pct {(f'{pct_1y:.1f}' if pct_1y is not None else 'n/a')} | observed_at {observed_at} | "
        f"lag {lag_cause_text}{f' ({lag_days}d)' if isinstance(lag_days, int) else ''}."
    )


def _build_monitor_record(metric_key: str, context: dict[str, Any], fallback: str, group: str) -> dict[str, Any]:
    metrics = dict(context.get("metrics") or {})
    metric = dict(metrics.get(metric_key) or {})
    value = _safe_float(metric.get("value"))
    observed_at = str(metric.get("observed_at") or "").strip() or None
    lag_days = metric.get("lag_days")
    lag_cause_raw = str(metric.get("lag_cause") or "").strip() or None
    lag_cause = _format_lag_cause(lag_cause_raw)
    evidence = _format_metric_evidence(metric_key, metrics)
    return {
        "metric_key": metric_key,
        "metric_name": _metric_label(metric_key),
        "group": group,
        "label": fallback,
        "value": value,
        "observed_at": observed_at,
        "lag_days": int(lag_days) if isinstance(lag_days, int) else None,
        "lag_cause": lag_cause if lag_cause_raw else None,
        "evidence": evidence,
    }


def _monitor_is_all_null(monitor: dict[str, Any]) -> bool:
    return (
        monitor.get("value") is None
        and monitor.get("observed_at") is None
        and monitor.get("lag_days") is None
        and monitor.get("lag_cause") is None
    )


def _lens_monitor_fallbacks() -> dict[str, str]:
    return {
        "DGS10": "Monitor US 10Y Treasury yield for rates-pressure acceleration context.",
        "T10YIE": "Monitor US inflation expectations for regime context.",
        "VIXCLS": "Monitor market volatility for risk-appetite context.",
        "BAMLH0A0HYM2": "Monitor high-yield credit spread as a stress transmission proxy.",
        "SP500": "Monitor S&P 500 trend as a broad equity-condition proxy.",
        "REAL_YIELD_10Y": "Monitor US 10Y real yield where available for valuation-pressure context.",
        "USD_STRENGTH": "Monitor US dollar strength for global and EM liquidity sensitivity.",
        "IG_CREDIT_SPREADS": "Monitor investment-grade credit spread for financing-condition diagnostics.",
        "BOND_VOLATILITY": "Monitor bond volatility for cross-asset rate-shock persistence.",
        "GLOBAL_EQUITY_EX_US": "Monitor global equities ex-US proxy to avoid a purely US lens.",
    }


def _default_sensitivity_map(*, sleeve_key: str) -> dict[str, str]:
    if sleeve_key in {"global_equity_core", "developed_ex_us_optional"}:
        return {
            "DGS10": "medium",
            "T10YIE": "medium",
            "VIXCLS": "high",
            "BAMLH0A0HYM2": "medium",
            "SP500": "medium",
            "REAL_YIELD_10Y": "medium",
            "USD_STRENGTH": "low",
        }
    if sleeve_key in {"emerging_markets", "china_satellite"}:
        return {
            "DGS10": "medium",
            "T10YIE": "medium",
            "VIXCLS": "high",
            "BAMLH0A0HYM2": "high",
            "SP500": "medium",
            "REAL_YIELD_10Y": "medium",
            "USD_STRENGTH": "high",
        }
    if sleeve_key in {"ig_bonds", "cash_bills"}:
        return {
            "DGS10": "high",
            "T10YIE": "high",
            "VIXCLS": "medium",
            "BAMLH0A0HYM2": "medium",
            "SP500": "low",
            "REAL_YIELD_10Y": "high",
            "USD_STRENGTH": "low",
        }
    return {
        "DGS10": "low",
        "T10YIE": "medium",
        "VIXCLS": "high",
        "BAMLH0A0HYM2": "high",
        "SP500": "medium",
        "REAL_YIELD_10Y": "medium",
        "USD_STRENGTH": "medium",
    }


def _monitor_priority(key: str, sensitivity_map: dict[str, str], available_metrics: dict[str, dict[str, Any]]) -> tuple[int, int]:
    sensitivity = str(sensitivity_map.get(key) or "medium").lower()
    rank = _SENSITIVITY_RANK.get(sensitivity, 2)
    availability = 1 if key in available_metrics else 0
    return (rank, availability)


def _derived_monitors_for_axes(
    *,
    differentiation_axes: list[str],
    candidate_data: dict[str, Any],
) -> list[str]:
    out: list[str] = []
    axes = {str(item).strip().lower() for item in differentiation_axes}
    if "em_inclusion" in axes:
        out.extend(["USD_STRENGTH", "GLOBAL_EQUITY_EX_US"])
    if "distribution_policy" in axes and str(candidate_data.get("accumulation_or_distribution") or "").lower().startswith("distrib"):
        out.append("IG_CREDIT_SPREADS")
    if "index_scope" in axes:
        symbol = str(candidate_data.get("symbol") or "").upper()
        if symbol == "IWDA":
            out.append("DGS10")
    if "replication_method" in axes and str(candidate_data.get("replication_method") or "").lower() == "swap-based":
        out.append("BOND_VOLATILITY")
    return [key for key in dict.fromkeys(out) if key in _LENS_METRIC_KEYS]


def _build_instrument_diagnostics(
    *,
    candidate_data: dict[str, Any],
    selected: dict[str, Any],
) -> list[str]:
    scope_tag = "(cited)" if bool(candidate_data.get("proof_isin")) else "(partially verified)"
    wrapper_tag = "(cited)" if bool(candidate_data.get("proof_accumulating")) and bool(candidate_data.get("proof_domicile")) else "(partially verified)"
    scope = str(selected.get("index_scope") or "").strip()
    em_inclusion = str(selected.get("em_inclusion") or "").strip()
    if scope:
        scope_line = f"Index scope and EM inclusion: {scope}; EM inclusion: {em_inclusion or 'Unavailable'} {scope_tag}."
    else:
        scope_line = "Index scope and EM inclusion: Unavailable in current payload (Unverified)."
    distribution = str(candidate_data.get("accumulation_or_distribution") or "unknown")
    domicile = str(candidate_data.get("domicile") or "Unknown")
    wrapper_line = f"Distribution policy and wrapper: {distribution}; domicile wrapper: {domicile} {wrapper_tag}."
    concentration = str(selected.get("concentration_snapshot") or "").strip() or "Unavailable in current payload (Unverified)."
    replication = str(candidate_data.get("replication_method") or "unknown")
    lending = (str(selected.get("securities_lending_note") or "").strip() or "Unavailable (Unverified)").rstrip(".")
    tracking = (str(selected.get("tracking_proxy") or "").strip() or "Unavailable (Unverified)").rstrip(".")
    return [
        scope_line,
        wrapper_line,
        f"Concentration snapshot: {concentration}",
        f"Replication and securities lending: replication {replication}; securities lending note: {lending}.",
        f"Tracking difference or spread proxy: {tracking}.",
    ]


def _build_review_triggers(
    *,
    candidate_data: dict[str, Any],
    regime_snapshot: dict[str, Any],
) -> list[str]:
    triggers: list[str] = []
    stale = list(regime_snapshot.get("stale_core_series") or [])
    if stale:
        triggers.append("Review if data freshness stays stale or lag flags keep repeating.")
    else:
        triggers.append("Review if any core monitor shifts to stale status.")
    confidence = str(regime_snapshot.get("confidence") or "unknown")
    if confidence == "low":
        triggers.append("Review if regime confidence remains low for more than 3 days.")
    else:
        triggers.append("Review if confidence downgrades to low for more than 3 days.")
    factsheet_asof = str(candidate_data.get("factsheet_asof") or "").strip()
    if not factsheet_asof:
        triggers.append("Review if factsheet freshness remains unavailable.")
    missing = list(candidate_data.get("verification_missing") or [])
    if missing:
        triggers.append("Review if verification proofs remain incomplete.")
    triggers.append("Review if tracking-difference proxy deviates materially when available.")
    triggers.append("Review if index methodology, TER, or wrapper mechanics change.")
    return triggers[:6]


def _build_backdrop_summary(regime_snapshot: dict[str, Any]) -> str:
    volatility = str(regime_snapshot.get("volatility_label") or "unknown")
    credit = str(regime_snapshot.get("credit_stress_label") or "unknown")
    rates = str(regime_snapshot.get("rates_pressure_label") or "unknown")
    confidence = str(regime_snapshot.get("confidence") or "unknown")
    freshness = str(regime_snapshot.get("data_freshness") or "unknown")
    missing = list(regime_snapshot.get("missing_inputs") or [])
    reason = f"{freshness} data"
    if missing:
        reason = f"{freshness} data and missing inputs"
    return (
        f"Current backdrop: volatility {volatility}, credit stress {credit}, rates {rates}, "
        f"confidence {confidence} due to {reason}."
    )


def _build_currency_exposure(candidate_data: dict[str, Any]) -> dict[str, Any]:
    trading_currency = str(candidate_data.get("trading_currency") or "unknown")
    underlying = str(candidate_data.get("underlying_currency_exposure") or "unknown")
    hedged_share_class = bool(candidate_data.get("hedged_share_class", False))
    return {
        "trading_currency": trading_currency,
        "underlying_currency_exposure": underlying,
        "hedged_share_class": hedged_share_class,
        "fx_hedged_flag": bool(candidate_data.get("fx_hedged_flag", hedged_share_class)),
        "jurisdiction_cost_model": str(candidate_data.get("jurisdiction_cost_model") or "sg_ucits_default"),
        "note": "Trading currency is not underlying exposure.",
    }


def _build_concentration_composition(candidate_data: dict[str, Any]) -> dict[str, Any] | None:
    payload = {
        "us_weight_pct": _safe_float(candidate_data.get("us_weight_pct")),
        "top10_concentration_pct": _safe_float(candidate_data.get("top10_concentration_pct")),
        "sector_tilts_summary": str(candidate_data.get("sector_tilts_summary") or "").strip() or None,
        "em_weight_pct": _safe_float(candidate_data.get("em_weight_pct")),
    }
    return payload if any(value is not None for value in payload.values()) else None


def _build_implementation_risks(candidate_data: dict[str, Any]) -> dict[str, Any] | None:
    replication = str(candidate_data.get("replication_method") or "").strip().lower() or "unknown"
    if replication not in {"physical", "synthetic"}:
        replication = "unknown"
    payload = {
        "replication_method": replication,
        "securities_lending_policy": str(candidate_data.get("securities_lending_policy") or "unknown"),
        "tracking_difference_note": str(candidate_data.get("tracking_difference_note") or "").strip() or None,
        "liquidity_proxy": str(candidate_data.get("liquidity_proxy") or "").strip() or None,
        "bid_ask_spread_proxy": _safe_float(candidate_data.get("bid_ask_spread_proxy")),
    }
    return payload if any(value is not None and value != "unknown" for value in payload.values()) else None


def _build_tax_mechanics(candidate_data: dict[str, Any]) -> dict[str, Any]:
    wht = str(candidate_data.get("withholding_tax_exposure_note") or "").strip()
    domicile_note = str(candidate_data.get("domicile_implication_note") or "").strip()
    distribution = str(candidate_data.get("accumulation_or_distribution") or "unknown").strip() or "unknown"
    return {
        "withholding_tax_exposure_note": wht or "Unverified withholding-tax mechanics for this candidate.",
        "domicile_implication_note": domicile_note or "Unverified domicile implication details for this candidate.",
        "distribution_mechanics_note": f"Share class is {distribution}.",
    }


def _citation_ids(candidate_data: dict[str, Any]) -> list[str]:
    return [str(item.get("source_id")) for item in list(candidate_data.get("citations") or []) if str(item.get("source_id") or "").strip()]


def _status_tag(ok: bool, *, allow_inferred: bool = False) -> str:
    if ok:
        return "verified"
    return "inferred" if allow_inferred else "unverified"


def _status_to_confidence(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "verified":
        return "high"
    if normalized == "inferred":
        return "medium"
    return "low"


def _value_status(value: Any, verified: bool = False) -> str:
    if value is None or str(value).strip() == "":
        return "missing"
    return "verified" if verified else "unverified"


def _normalize_unknown_label(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    lowered = normalized.lower().rstrip(".")
    replacements = {
        "us weight missing": "US weight",
        "em weight missing": "EM weight",
        "top-10 concentration missing": "Top-10 concentration",
        "tracking-difference proxy is unavailable": "Tracking-difference proxy",
        "tracking difference proxy": "Tracking-difference proxy",
        "bid/ask spread proxy": "Bid/ask spread proxy",
        "underlying currency exposure is unavailable": "Underlying currency exposure",
        "share-class distribution policy is unavailable": "Share-class distribution policy",
        "ter is unavailable": "TER",
        "replication method": "Replication method",
    }
    if lowered in replacements:
        return replacements[lowered]
    return normalized.rstrip(".")


def _dedupe_unknowns(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in items:
        label = _normalize_unknown_label(raw)
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(label)
    return out


def _finalize_implication(item: dict[str, Any]) -> dict[str, Any]:
    implication = dict(item)
    evidence = [dict(entry) for entry in list(implication.get("evidence") or [])]
    blockers = [str(x).strip() for x in list(implication.get("blockers") or []) if str(x).strip()]
    status = str(implication.get("status") or "blocked").lower()

    has_verified_evidence_with_citations = any(
        str(entry.get("status") or "").lower() == "verified" and bool(list(entry.get("citation_ids") or []))
        for entry in evidence
    )
    has_non_missing_evidence = any(
        str(entry.get("status") or "").lower() in {"verified", "unverified"}
        for entry in evidence
    )

    if status == "verified" and not has_verified_evidence_with_citations:
        status = "inferred" if has_non_missing_evidence else "blocked"
    if status == "blocked" and not blockers:
        blockers = ["Required evidence is missing."]

    implication["status"] = status
    implication["confidence"] = _status_to_confidence(status)
    implication["evidence"] = evidence
    implication["blockers"] = blockers
    return implication


def buildImplications(candidate: dict[str, Any], lens_inputs: dict[str, Any]) -> list[dict[str, Any]]:
    selected = dict(lens_inputs.get("selected") or {})
    sleeve_key = str(lens_inputs.get("sleeve_key") or "")
    instrument_type = str(candidate.get("instrument_type") or "")
    citations = _citation_ids(candidate)
    implications: list[dict[str, Any]] = []

    em_inclusion = str(selected.get("em_inclusion") or "").strip()
    index_scope = str(selected.get("index_scope") or "").strip()
    exposure_blockers: list[str] = []
    if not em_inclusion:
        exposure_blockers.append("EM inclusion is unavailable.")
    if not index_scope:
        exposure_blockers.append("Index scope is unavailable.")
    if em_inclusion.lower().startswith("included"):
        exposure_text = "EM inclusion increases sensitivity to USD strength and emerging-market risk sentiment."
    elif em_inclusion.lower().startswith("excluded"):
        exposure_text = "EM exclusion reduces direct emerging-market cycle sensitivity in this candidate."
    else:
        exposure_text = "Exposure-structure implication is blocked because EM inclusion is not confirmed."
    exposure_status = "blocked" if exposure_blockers else ("verified" if bool(candidate.get("proof_isin")) else "inferred")
    implications.append(
        {
            "id": "exposure_structure",
            "text": exposure_text,
            "status": exposure_status,
            "confidence": _status_to_confidence(exposure_status),
            "driver_keys": ["index_scope", "em_inclusion", "USD_STRENGTH"],
            "evidence": [
                {
                    "key": "index_scope",
                    "label": "Index scope",
                    "value": index_scope or None,
                    "status": _value_status(index_scope, verified=bool(candidate.get("proof_isin"))),
                    "citation_ids": citations if bool(candidate.get("proof_isin")) else [],
                },
                {
                    "key": "em_inclusion",
                    "label": "EM inclusion",
                    "value": em_inclusion or None,
                    "status": _value_status(em_inclusion, verified=bool(candidate.get("proof_isin"))),
                    "citation_ids": citations if bool(candidate.get("proof_isin")) else [],
                },
            ],
            "blockers": exposure_blockers,
        }
    )

    distribution = str(candidate.get("accumulation_or_distribution") or "").strip().lower()
    dist_verified = bool(candidate.get("proof_accumulating"))
    share_class_blockers: list[str] = []
    if not distribution:
        share_class_blockers.append("Share-class distribution policy is unavailable.")
    if distribution.startswith("accum"):
        share_text = "Accumulating share class reduces dividend-cashflow handling versus distributing share classes."
    elif distribution.startswith("distrib"):
        share_text = "Distributing share class introduces dividend cashflow timing and reinvestment friction."
    else:
        share_text = "Share-class mechanics are blocked because distribution policy is not confirmed."
    share_status = "verified" if distribution and dist_verified else ("inferred" if distribution else "blocked")
    implications.append(
        {
            "id": "share_class_mechanics",
            "text": share_text,
            "status": share_status,
            "confidence": _status_to_confidence(share_status),
            "driver_keys": ["distribution_policy", "share_class"],
            "evidence": [
                {
                    "key": "share_class",
                    "label": "Share class",
                    "value": distribution or None,
                    "status": _value_status(distribution, verified=dist_verified),
                    "citation_ids": citations if dist_verified else [],
                }
            ],
            "blockers": share_class_blockers,
        }
    )

    ter = _safe_float(candidate.get("expense_ratio"))
    ter_verified = bool(candidate.get("proof_ter"))
    tracking_note = str(candidate.get("tracking_difference_note") or "").strip()
    cost_blockers: list[str] = []
    if ter is None:
        cost_blockers.append("TER is unavailable.")
    tracking_relevant = (
        instrument_type in _TRACKING_RELEVANT_INSTRUMENTS
        and not bool(candidate.get("policy_placeholder"))
        and sleeve_key in _TRACKING_COMPARISON_SLEEVES
    )
    if tracking_relevant and not tracking_note:
        cost_blockers.append("Tracking-difference proxy is unavailable.")
    if ter is None and instrument_type in _TRACKING_RELEVANT_INSTRUMENTS:
        cost_text = "Cost-and-friction implication is blocked because TER is unavailable."
        cost_status = "blocked"
    elif instrument_type not in _TRACKING_RELEVANT_INSTRUMENTS:
        cost_text = "Explicit TER is not the primary comparison field for this candidate type; implementation costs require instrument-specific review."
        cost_status = "inferred"
    else:
        cost_text = f"TER is {(ter * 100):.2f}%; realized tracking friction remains {'unconfirmed' if tracking_relevant and not tracking_note else 'available'}."
        cost_status = "verified" if ter_verified and (not tracking_relevant or bool(tracking_note)) else "inferred"
    implications.append(
        {
            "id": "cost_and_implementation",
            "text": cost_text,
            "status": cost_status,
            "confidence": _status_to_confidence(cost_status),
            "driver_keys": ["expense_ratio", "tracking_difference_proxy", "bid_ask_spread_proxy"],
            "evidence": [
                {
                    "key": "expense_ratio",
                    "label": "TER",
                    "value": round((ter or 0.0) * 100, 4) if ter is not None else None,
                    "status": _value_status(ter, verified=ter_verified),
                    "citation_ids": citations if ter_verified else [],
                },
                {
                    "key": "tracking_difference_note",
                    "label": "Tracking-difference proxy",
                    "value": tracking_note or None,
                    "status": _value_status(tracking_note, verified=bool(tracking_note)),
                    "citation_ids": citations if tracking_note else [],
                },
            ],
            "blockers": cost_blockers,
        }
    )

    if sleeve_key in _EQUITY_COMPOSITION_SLEEVES:
        us_weight = _safe_float(candidate.get("us_weight_pct"))
        top10 = _safe_float(candidate.get("top10_concentration_pct"))
        em_weight = _safe_float(candidate.get("em_weight_pct"))
        concentration_blockers: list[str] = []
        if us_weight is None:
            concentration_blockers.append("US weight missing.")
        if top10 is None:
            concentration_blockers.append("Top-10 concentration missing.")
        if concentration_blockers:
            concentration_text = "Concentration profile cannot be compared yet, limiting drawdown and factor interpretation."
            concentration_status = "blocked"
        else:
            em_fragment = f", EM weight {em_weight:.1f}%" if em_weight is not None else ""
            concentration_text = f"Concentration profile indicates US weight {us_weight:.1f}% and top-10 concentration {top10:.1f}%{em_fragment}."
            concentration_status = "inferred"
        implications.append(
            {
                "id": "concentration_and_composition",
                "text": concentration_text,
                "status": concentration_status,
                "confidence": _status_to_confidence(concentration_status),
                "driver_keys": ["us_weight", "top10_weight", "em_weight"],
                "evidence": [
                    {
                        "key": "us_weight",
                        "label": "US weight",
                        "value": us_weight,
                        "status": _value_status(us_weight, verified=us_weight is not None),
                        "citation_ids": citations if us_weight is not None else [],
                    },
                    {
                        "key": "top10_weight",
                        "label": "Top-10 concentration",
                        "value": top10,
                        "status": _value_status(top10, verified=top10 is not None),
                        "citation_ids": citations if top10 is not None else [],
                    },
                    {
                        "key": "em_weight",
                        "label": "EM weight",
                        "value": em_weight,
                        "status": _value_status(em_weight, verified=em_weight is not None),
                        "citation_ids": citations if em_weight is not None else [],
                    },
                ],
                "blockers": concentration_blockers,
            }
        )

    if sleeve_key in _CURRENCY_COMPARISON_SLEEVES:
        trading_currency = str(candidate.get("trading_currency") or "unknown").strip().upper()
        underlying_currency = str(candidate.get("underlying_currency_exposure") or "unknown").strip().lower()
        currency_blockers: list[str] = []
        if underlying_currency in {"", "unknown"}:
            currency_blockers.append("Underlying currency exposure is unavailable.")
        currency_status = "blocked" if currency_blockers else ("verified" if bool(candidate.get("proof_domicile")) else "inferred")
        currency_text = (
            "Trading currency is not underlying exposure; currency-specific comparison is blocked until underlying exposure is confirmed."
            if currency_status == "blocked"
            else f"Trading currency is {trading_currency}, while underlying currency exposure is {underlying_currency}; trading currency is not underlying exposure."
        )
        implications.append(
            {
                "id": "currency_exposure",
                "text": currency_text,
                "status": currency_status,
                "confidence": _status_to_confidence(currency_status),
                "driver_keys": ["trading_currency", "underlying_currency_exposure"],
                "evidence": [
                    {
                        "key": "trading_currency",
                        "label": "Trading currency",
                        "value": trading_currency or None,
                        "status": _value_status(trading_currency, verified=bool(trading_currency and trading_currency != "UNKNOWN")),
                        "citation_ids": citations if trading_currency and trading_currency != "UNKNOWN" else [],
                    },
                    {
                        "key": "underlying_currency_exposure",
                        "label": "Underlying currency exposure",
                        "value": underlying_currency or None,
                        "status": _value_status(
                            None if underlying_currency in {"", "unknown"} else underlying_currency,
                            verified=underlying_currency not in {"", "unknown"},
                        ),
                        "citation_ids": citations if underlying_currency not in {"", "unknown"} else [],
                    },
                ],
                "blockers": currency_blockers,
            }
        )

    return [_finalize_implication(item) for item in implications[:5]]


def _build_macro_implications(
    *,
    regime_snapshot: dict[str, Any],
    monitor_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key = {str(item.get("metric_key") or ""): item for item in monitor_records}
    items: list[dict[str, Any]] = []

    vix_label = str(regime_snapshot.get("volatility_label") or "unknown")
    vix_blockers: list[str] = []
    if vix_label == "unknown":
        vix_blockers.append("Volatility input is unavailable.")
    vix_status = "blocked" if vix_blockers else "inferred"
    items.append(
        {
            "id": "volatility_channel",
            "text": (
                "Volatility context is unavailable, so correlation and drawdown-depth diagnostics are blocked."
                if vix_status == "blocked"
                else f"Volatility is {vix_label}, which can raise correlation and drawdown depth when stress persists."
            ),
            "status": vix_status,
            "confidence": "low" if vix_status == "blocked" else "medium",
            "driver_keys": ["VIXCLS"],
            "evidence": [
                {
                    "key": "VIXCLS",
                    "label": _metric_label("VIXCLS"),
                    "value": by_key.get("VIXCLS", {}).get("value"),
                    "status": "verified" if by_key.get("VIXCLS", {}).get("value") is not None else "missing",
                    "citation_ids": [],
                }
            ],
            "blockers": vix_blockers,
        }
    )

    rates_label = str(regime_snapshot.get("rates_pressure_label") or "unknown")
    dgs_record = by_key.get("DGS10", {})
    real_record = by_key.get("REAL_YIELD_10Y", {})
    rates_blockers: list[str] = []
    if dgs_record.get("value") is None and real_record.get("value") is None:
        rates_blockers.append("Nominal and real-yield inputs are unavailable.")
        rates_status = "blocked"
    elif real_record.get("value") is None:
        rates_blockers.append("Real-yield input missing, so rates assessment is partial.")
        rates_status = "inferred"
    else:
        rates_status = "inferred"
    rates_text = (
        "Rates sensitivity assessment is blocked until nominal and real-yield inputs are available."
        if rates_status == "blocked"
        else (
            "Rates sensitivity assessment is partial because real-yield input is missing."
            if real_record.get("value") is None
            else f"Rates context is {rates_label}; rate repricing can influence valuation pressure."
        )
    )
    items.append(
        {
            "id": "rates_channel",
            "text": rates_text,
            "status": rates_status,
            "confidence": "low" if rates_status == "blocked" else ("medium" if real_record.get("value") is not None else "low"),
            "driver_keys": ["DGS10", "REAL_YIELD_10Y"],
            "evidence": [
                {
                    "key": "DGS10",
                    "label": _metric_label("DGS10"),
                    "value": dgs_record.get("value"),
                    "status": "verified" if dgs_record.get("value") is not None else "missing",
                    "citation_ids": [],
                },
                {
                    "key": "REAL_YIELD_10Y",
                    "label": _metric_label("REAL_YIELD_10Y"),
                    "value": real_record.get("value"),
                    "status": "verified" if real_record.get("value") is not None else "missing",
                    "citation_ids": [],
                },
            ],
            "blockers": rates_blockers,
        }
    )

    credit_label = str(regime_snapshot.get("credit_stress_label") or "unknown")
    credit_record = by_key.get("BAMLH0A0HYM2", {})
    credit_blockers: list[str] = []
    if credit_record.get("value") is None and credit_label == "unknown":
        credit_blockers.append("Credit-spread input is unavailable.")
    credit_status = "blocked" if credit_blockers else "inferred"
    items.append(
        {
            "id": "credit_channel",
            "text": (
                "Credit-stress channel is unavailable because high-yield spread input is missing."
                if credit_status == "blocked"
                else f"Credit context is {credit_label}; widening spreads often coincide with broader risk-off conditions."
            ),
            "status": credit_status,
            "confidence": "low" if credit_status == "blocked" else "medium",
            "driver_keys": ["BAMLH0A0HYM2"],
            "evidence": [
                {
                    "key": "BAMLH0A0HYM2",
                    "label": _metric_label("BAMLH0A0HYM2"),
                    "value": credit_record.get("value"),
                    "status": "verified" if credit_record.get("value") is not None else "missing",
                    "citation_ids": [],
                }
            ],
            "blockers": credit_blockers,
        }
    )

    freshness = str(regime_snapshot.get("data_freshness") or "unknown")
    stale = list(regime_snapshot.get("stale_core_series") or [])
    missing = list(regime_snapshot.get("missing_inputs") or [])
    if stale or missing or freshness in {"stale", "unknown"}:
        blockers = []
        if stale:
            blockers.append(f"stale inputs: {', '.join(stale[:3])}")
        if missing:
            blockers.append(f"missing inputs: {', '.join(missing[:3])}")
        items.append(
            {
                "id": "macro_data_quality",
                "text": f"Macro-confidence is reduced by {freshness} data and incomplete core inputs.",
                "status": "inferred",
                "confidence": "low",
                "driver_keys": ["data_freshness", "missing_core_inputs"],
                "evidence": [],
                "blockers": blockers,
            }
        )

    return items[:4]


def _build_investment_lens(
    *,
    sleeve_key: str,
    candidate_symbol: str,
    candidate_name: str,
    candidate_data: dict[str, Any],
    regime_context: dict[str, Any],
    profile_type: str,
    concentration_warning_buffer: float,
    target_weight_pct: float,
) -> dict[str, Any]:
    base_lens: dict[str, dict[str, Any]] = {
        "global_equity_core": {
            "role_line": "Long-horizon equity growth anchor aligned to a diversified global policy sleeve.",
            "index_scope": "Global all-cap developed + emerging coverage",
            "em_inclusion": "Included",
            "concentration_snapshot": "Unavailable in current payload (Unverified).",
            "securities_lending_note": "Unavailable in current payload (Unverified).",
            "tracking_proxy": "Unavailable in current payload (Unverified).",
            "differentiation_axes": [
                "index_scope",
                "em_inclusion",
                "distribution_policy",
                "domicile_wrapper",
                "replication_method",
                "fee_profile",
            ],
            "implementation_notes": [
                "Policy intent keeps one global all-cap accumulating UCITS fund as the default core pathway.",
                "Global sleeve diagnostics are interpreted with a shared regime layer plus candidate-specific sensitivities.",
                "Wrapper and distribution mechanics remain implementation details, not tactical signals.",
            ],
            "risk_drivers": [
                "Equity drawdown depth during risk-off regimes.",
                "Discount-rate sensitivity when DGS10 reprices higher.",
                "Volatility persistence when VIXCLS remains elevated.",
            ],
            "tends_to_help": [
                "Contained volatility regimes with stable VIXCLS behavior.",
                "Credit conditions that remain orderly without abrupt spread widening.",
                "Broad participation in global equity benchmarks.",
            ],
            "tends_to_hurt": [
                "Rapid volatility spikes and persistent risk-off sequences.",
                "Joint equity and credit stress episodes.",
                "Sharp discount-rate repricing that compresses valuation multiples.",
            ],
            "monitor_now": [
                "Monitoring now: SP500 path and state transitions for broad equity-risk tone.",
                "Monitoring now: VIXCLS persistence for drawdown-risk diagnostics.",
                "Monitoring now: DGS10 level shifts for discount-rate pressure context.",
            ],
            "candidate_specific_monitor_keys": ["GLOBAL_EQUITY_EX_US"],
        },
        "developed_ex_us_optional": {
            "role_line": "Optional developed-market split sleeve used for implementation review around the global core.",
            "index_scope": "Developed markets",
            "em_inclusion": "Excluded",
            "concentration_snapshot": "Unavailable in current payload (Unverified).",
            "securities_lending_note": "Unavailable in current payload (Unverified).",
            "tracking_proxy": "Unavailable in current payload (Unverified).",
            "differentiation_axes": ["index_scope", "distribution_policy", "replication_method", "fee_profile"],
            "implementation_notes": [
                "Sleeve is optional by policy with target 0%, used only as a secondary split implementation.",
                "Regional split diagnostics focus on concentration and overlap with global core holdings.",
                "Differences in benchmark composition can matter during divergence across developed regions.",
            ],
            "risk_drivers": [
                "Developed-market valuation sensitivity to rates and growth revisions.",
                "Regional concentration drift versus one-fund core policy.",
                "Volatility regime spillover into developed equities.",
            ],
            "tends_to_help": [
                "Stable cross-asset volatility conditions.",
                "Orderly credit backdrop that supports equity risk appetite.",
                "Disciplined alignment with the optional target range.",
            ],
            "tends_to_hurt": [
                "Extended volatility regimes with elevated risk premia.",
                "Concentration drift that diverges from policy intent.",
                "Abrupt risk-off repricing across developed benchmarks.",
            ],
            "monitor_now": [
                "Monitoring now: SP500 as a broad developed-equity proxy for risk tone.",
                "Monitoring now: VIXCLS for persistence of stress conditions.",
                "Monitoring now: BAMLH0A0HYM2 for cross-asset credit stress spillover.",
            ],
            "candidate_specific_monitor_keys": ["GLOBAL_EQUITY_EX_US"],
        },
        "emerging_markets": {
            "role_line": "Satellite growth sleeve for diversified emerging-market exposure under policy risk limits.",
            "implementation_notes": [
                "Sleeve complements developed-market core and is monitored for EM concentration and liquidity context.",
                "USD and global liquidity channels can affect EM sleeves more directly in stressed conditions.",
                "China overlap should be checked against explicit China satellite sizing constraints.",
            ],
            "risk_drivers": [
                "Global liquidity and funding conditions linked to DGS10 shifts.",
                "Risk sentiment sensitivity during volatility and credit stress regimes.",
                "Higher dispersion across country and sector exposures.",
            ],
            "tends_to_help": [
                "Stable global risk sentiment with contained VIXCLS behavior.",
                "Orderly credit-spread conditions across risk assets.",
                "Balanced inflation/rates backdrop without abrupt repricing shocks.",
            ],
            "tends_to_hurt": [
                "Volatility spikes with persistent risk aversion.",
                "Credit-spread widening that reduces risk appetite.",
                "Sharp global rate repricing episodes.",
            ],
            "monitor_now": [
                "Monitoring now: VIXCLS and BAMLH0A0HYM2 for risk-sentiment stress transmission.",
                "Monitoring now: DGS10 for global discount-rate and funding sensitivity context.",
                "Monitoring now: SP500 for broad risk-tone confirmation.",
            ],
            "candidate_specific_monitor_keys": ["USD_STRENGTH", "GLOBAL_EQUITY_EX_US"],
        },
        "china_satellite": {
            "role_line": "Capped China satellite sleeve used for transparency of explicit China exposure.",
            "implementation_notes": [
                "Policy intent is capped satellite exposure, not a core allocation pathway.",
                "Diagnostics emphasize concentration and correlation behavior during global stress states.",
                "China sleeve should be read alongside EM and global sleeves to avoid double-counting exposures.",
            ],
            "risk_drivers": [
                "Concentration risk from a narrow satellite allocation.",
                "Sensitivity to global risk sentiment and volatility persistence.",
                "Policy and macro-cycle uncertainty relative to core sleeves.",
            ],
            "tends_to_help": [
                "Contained global volatility and orderly credit conditions.",
                "Stable cross-asset risk sentiment with reduced stress spillover.",
                "Disciplined enforcement of satellite sizing limits.",
            ],
            "tends_to_hurt": [
                "High-volatility risk-off phases with broad de-risking.",
                "Credit stress spillovers that tighten global financial conditions.",
                "Regime shifts that increase equity risk premia.",
            ],
            "monitor_now": [
                "Monitoring now: VIXCLS for risk-off persistence affecting satellite sleeves.",
                "Monitoring now: BAMLH0A0HYM2 for cross-asset stress amplification.",
                "Monitoring now: SP500 as a broad risk-tone confirmation signal.",
            ],
            "candidate_specific_monitor_keys": ["USD_STRENGTH", "GLOBAL_EQUITY_EX_US"],
        },
        "ig_bonds": {
            "role_line": "Defensive ballast sleeve intended to support portfolio resilience in long-horizon policy construction.",
            "implementation_notes": [
                "Ballast diagnostics prioritize duration, spread behavior, and implementation quality of bond wrappers.",
                "Policy role is resilience support rather than directional return targeting.",
                "Currency and hedging conventions can materially affect realized sleeve behavior.",
            ],
            "risk_drivers": [
                "Duration sensitivity to DGS10 moves.",
                "Real-yield and inflation-expectation shifts linked to T10YIE.",
                "Credit spread transmission during stress regimes.",
            ],
            "tends_to_help": [
                "Orderly or declining rate volatility conditions.",
                "Contained inflation-expectation repricing.",
                "Stable credit-spread environment.",
            ],
            "tends_to_hurt": [
                "Abrupt rate repricing with persistent duration drag.",
                "Inflation-expectation instability and real-yield shocks.",
                "Credit spread widening that weakens ballast behavior.",
            ],
            "monitor_now": [
                "Monitoring now: DGS10 for duration-pressure context.",
                "Monitoring now: T10YIE for inflation-expectation drift.",
                "Monitoring now: BAMLH0A0HYM2 for credit-spread stress.",
            ],
            "candidate_specific_monitor_keys": ["REAL_YIELD_10Y", "IG_CREDIT_SPREADS", "BOND_VOLATILITY"],
        },
        "cash_bills": {
            "role_line": "Liquidity reserve sleeve for policy flexibility, drawdown resilience, and implementation optionality.",
            "implementation_notes": [
                "Sleeve emphasizes liquidity and operational certainty over return-seeking behavior.",
                "Non-ETF reserve instruments may appear as policy placeholders with partial verification labels.",
                "Monitoring focuses on rate and stress context rather than tactical timing.",
            ],
            "risk_drivers": [
                "Reinvestment-rate variability with changing front-end conditions.",
                "Opportunity-cost shifts relative to risk assets.",
                "Operational implementation differences across bill/cash vehicles.",
            ],
            "tends_to_help": [
                "Elevated uncertainty regimes where liquidity flexibility is valuable.",
                "Stable short-duration conditions with low mark-to-market variability.",
                "Policy discipline around reserve sizing.",
            ],
            "tends_to_hurt": [
                "Extended risk-on phases where reserve carry lags risk assets.",
                "Rapid policy-rate pivots that change reinvestment conditions.",
                "Over-allocation beyond policy range reducing sleeve balance.",
            ],
            "monitor_now": [
                "Monitoring now: DGS10 for broad rate-regime context.",
                "Monitoring now: VIXCLS for liquidity-demand pressure signals.",
                "Monitoring now: BAMLH0A0HYM2 for stress-sensitive reserve deployment context.",
            ],
            "candidate_specific_monitor_keys": ["BOND_VOLATILITY"],
        },
        "real_assets": {
            "role_line": "Real-assets diversifier sleeve used to monitor inflation-sensitive and real-asset pathways.",
            "implementation_notes": [
                "Sleeve is a diversifier and should be interpreted relative to the equity and bond core.",
                "Instrument structure (physical, swap, REIT equity) can affect tracking and stress behavior.",
                "Distribution mechanics vary by wrapper and can influence implementation fit.",
            ],
            "risk_drivers": [
                "Real-yield sensitivity when rates reprice.",
                "Inflation-expectation regime shifts linked to T10YIE.",
                "Commodity/property tracking dispersion across wrappers.",
            ],
            "tends_to_help": [
                "Inflation-sensitive regimes with diversified commodity support.",
                "Periods where diversification to equity/bond beta is additive.",
                "Stable implementation mechanics in underlying instruments.",
            ],
            "tends_to_hurt": [
                "Sharp real-yield rises that pressure duration-like real assets.",
                "Growth scares with weaker demand-sensitive commodity behavior.",
                "Volatility shocks that compress risk appetite.",
            ],
            "monitor_now": [
                "Monitoring now: T10YIE for inflation-expectation direction.",
                "Monitoring now: DGS10 for real-yield pressure context.",
                "Monitoring now: SP500 and VIXCLS for cross-asset risk-tone spillover.",
            ],
            "candidate_specific_monitor_keys": ["REAL_YIELD_10Y", "GLOBAL_EQUITY_EX_US"],
        },
        "alternatives": {
            "role_line": "Optional conservative diversifier sleeve for implementation review across non-core exposures.",
            "implementation_notes": [
                "Sleeve is optional and diagnostic under policy, not a daily allocation signal.",
                "Structure and liquidity differences can dominate behavior in stressed markets.",
                "Candidate comparisons focus on implementation fit, not directional forecasts.",
            ],
            "risk_drivers": [
                "Correlation instability across alternative wrappers.",
                "Liquidity and structure differences across instruments.",
                "Regime dependence of diversification behavior.",
            ],
            "tends_to_help": [
                "Mixed macro regimes where diversification pathways remain distinct.",
                "Contained credit stress and orderly cross-asset volatility.",
                "Policy-consistent sizing as a non-core sleeve.",
            ],
            "tends_to_hurt": [
                "Broad liquidation events where correlations rise abruptly.",
                "Persistent volatility spikes with reduced risk appetite.",
                "Structure-specific implementation frictions.",
            ],
            "monitor_now": [
                "Monitoring now: VIXCLS for volatility-regime persistence.",
                "Monitoring now: BAMLH0A0HYM2 for credit-stress confirmation.",
                "Monitoring now: SP500 for broad risk-tone context.",
            ],
            "candidate_specific_monitor_keys": ["IG_CREDIT_SPREADS", "GLOBAL_EQUITY_EX_US"],
        },
        "convex": {
            "role_line": "Dedicated convexity sleeve intended for drawdown-resilience diagnostics under non-linear stress.",
            "implementation_notes": [
                "Policy sleeve is bucketed across managed futures, tail hedge, and long-put overlay structure.",
                "Convex diagnostics remain safety-constrained: no leverage, no margin requirement, max loss known.",
                "Long-put overlay is a policy structure placeholder and may route to tail hedge when permissions are unavailable.",
            ],
            "risk_drivers": [
                "Carry drag in calm regimes for explicit hedge structures.",
                "Model/structure sensitivity across managed futures and tail hedges.",
                "Execution-quality dispersion across convex implementations.",
            ],
            "tends_to_help": [
                "Volatility regime expansion and abrupt risk repricing.",
                "Joint equity and credit stress episodes.",
                "Persistent stress states that reward convex protection.",
            ],
            "tends_to_hurt": [
                "Extended low-volatility environments with muted stress realization.",
                "Frequent reversals that reduce hedge efficiency.",
                "Overly narrow implementation breadth across convex buckets.",
            ],
            "monitor_now": [
                "Monitoring now: VIXCLS for volatility-regime escalation signals.",
                "Monitoring now: BAMLH0A0HYM2 for credit-stress confirmation.",
                "Monitoring now: SP500 drawdown behavior for hedge relevance diagnostics.",
            ],
            "candidate_specific_monitor_keys": ["BOND_VOLATILITY", "IG_CREDIT_SPREADS"],
        },
    }
    symbol = str(candidate_symbol or "").upper()
    candidate_overrides: dict[str, dict[str, Any]] = {
        "IWDA": {
            "role_line": "Developed market equity core sleeve for long-horizon growth exposure.",
            "index_scope": "MSCI World developed markets",
            "em_inclusion": "Excluded",
            "differentiation_axes": ["index_scope", "distribution_policy", "replication_method", "concentration_notes"],
            "implementation_notes": [
                "Developed markets only, with emerging markets intentionally excluded.",
                "Can be paired with a dedicated EM sleeve in optional split implementations.",
                "UCITS wrapper and accumulating share-class mechanics are implementation diagnostics.",
            ],
            "risk_drivers": [
                "Discount-rate repricing can compress developed-market equity multiples.",
                "Risk-off regimes can drive correlated drawdowns across developed equities.",
                "Concentration risk can rise when mega-cap leadership narrows.",
            ],
            "tends_to_help": [
                "Orderly rates environment without accelerating real-yield pressure.",
                "Contained volatility with stable equity risk-premium behavior.",
                "Stable credit conditions without abrupt spread widening.",
            ],
            "tends_to_hurt": [
                "Fast increases in real yields or abrupt term-premium repricing.",
                "Volatility spikes that tighten financial conditions.",
                "Credit-stress episodes that coincide with de-risking sentiment.",
            ],
            "sensitivity_map": {
                "DGS10": "high",
                "T10YIE": "medium",
                "VIXCLS": "high",
                "BAMLH0A0HYM2": "medium",
                "SP500": "medium",
                "REAL_YIELD_10Y": "high",
                "USD_STRENGTH": "low",
            },
            "candidate_specific_monitor_keys": ["GLOBAL_EQUITY_EX_US", "REAL_YIELD_10Y"],
        },
        "VWRA": {
            "role_line": "All-world equity core sleeve including emerging markets in an accumulating UCITS implementation.",
            "index_scope": "FTSE All-World developed + emerging",
            "em_inclusion": "Included",
            "differentiation_axes": ["index_scope", "em_inclusion", "distribution_policy", "replication_method", "fee_profile"],
            "implementation_notes": [
                "Developed plus emerging exposure in one-fund form under UCITS wrapper.",
                "Accumulating structure minimizes distribution-cashflow mechanics in implementation review.",
                "Broad country mix can reduce single-region dependence while increasing policy-divergence exposure.",
            ],
            "risk_drivers": [
                "USD strength and global liquidity tightening can pressure EM segments.",
                "Volatility spikes can raise cross-asset correlation and drawdown depth.",
                "Rates and real-yield repricing can compress global equity valuations.",
            ],
            "tends_to_help": [
                "Stable global liquidity conditions with contained funding stress.",
                "Moderate volatility with stable credit conditions.",
                "Orderly rates environment without sharp real-yield repricing.",
            ],
            "tends_to_hurt": [
                "Sharp USD-strength phases that coincide with EM stress.",
                "Rapid volatility spikes that trigger broad de-risking.",
                "Credit spread widening that signals tightening conditions.",
            ],
            "sensitivity_map": {
                "DGS10": "medium",
                "T10YIE": "medium",
                "VIXCLS": "high",
                "BAMLH0A0HYM2": "high",
                "SP500": "medium",
                "REAL_YIELD_10Y": "medium",
                "USD_STRENGTH": "high",
            },
            "candidate_specific_monitor_keys": ["USD_STRENGTH", "GLOBAL_EQUITY_EX_US", "REAL_YIELD_10Y"],
        },
        "VWRL": {
            "role_line": "All-world equity sleeve including emerging markets with distributing share-class mechanics.",
            "index_scope": "FTSE All-World developed + emerging",
            "em_inclusion": "Included",
            "differentiation_axes": ["index_scope", "em_inclusion", "distribution_policy", "replication_method", "fee_profile"],
            "implementation_notes": [
                "Includes emerging markets, so USD and liquidity channels matter in stressed regimes.",
                "Distributing structure introduces cashflow-timing diagnostics versus accumulating classes.",
                "Broader country mix can reduce single-region dependence while increasing global policy-divergence exposure.",
            ],
            "risk_drivers": [
                "USD strength and liquidity tightening can pressure EM risk assets.",
                "Volatility regimes can increase cross-asset correlation.",
                "Rates and real-yield repricing can compress valuations across global equities.",
            ],
            "tends_to_help": [
                "Stable global liquidity and contained funding stress.",
                "Moderate volatility with steady credit conditions.",
                "Orderly rates environment without sudden real-yield shocks.",
            ],
            "tends_to_hurt": [
                "Sharp USD strengthening that tightens conditions for EM components.",
                "Rapid volatility spikes that trigger broad global de-risking.",
                "Credit-spread widening that signals tighter financing conditions.",
            ],
            "sensitivity_map": {
                "DGS10": "medium",
                "T10YIE": "medium",
                "VIXCLS": "high",
                "BAMLH0A0HYM2": "high",
                "SP500": "medium",
                "REAL_YIELD_10Y": "medium",
                "USD_STRENGTH": "high",
            },
            "candidate_specific_monitor_keys": ["USD_STRENGTH", "GLOBAL_EQUITY_EX_US", "BOND_VOLATILITY"],
        },
        "SSAC": {
            "role_line": "ACWI-style global equity implementation used for one-fund core comparisons.",
            "index_scope": "MSCI ACWI developed + emerging",
            "em_inclusion": "Included",
            "differentiation_axes": ["index_scope", "em_inclusion", "distribution_policy", "replication_method", "concentration_notes"],
            "implementation_notes": [
                "Global developed plus emerging exposure with ACWI index methodology.",
                "Coverage and methodology can differ from other all-world benchmark constructions.",
                "Implementation quality diagnostics include wrapper, replication, and tracking behavior.",
            ],
            "risk_drivers": [
                "Global risk-off regimes can cause synchronized drawdowns across regions.",
                "USD strength and liquidity tightening can disproportionately affect EM segments.",
                "Implementation differences can appear as tracking divergence during stressed markets.",
            ],
            "tends_to_help": [
                "Contained volatility regimes with stable correlation structure.",
                "Stable or improving credit conditions with limited spread widening.",
                "Orderly rates backdrop without sudden real-yield repricing.",
            ],
            "tends_to_hurt": [
                "Volatility spikes with broad de-risking and rising correlations.",
                "Spread widening that signals tighter financial conditions.",
                "USD-strength shocks that coincide with EM outflow stress.",
            ],
            "sensitivity_map": {
                "DGS10": "medium",
                "T10YIE": "medium",
                "VIXCLS": "high",
                "BAMLH0A0HYM2": "high",
                "SP500": "medium",
                "REAL_YIELD_10Y": "medium",
                "USD_STRENGTH": "high",
            },
            "candidate_specific_monitor_keys": ["GLOBAL_EQUITY_EX_US", "IG_CREDIT_SPREADS", "REAL_YIELD_10Y"],
        },
    }

    selected = dict(base_lens.get(sleeve_key) or base_lens["global_equity_core"])
    override = dict(candidate_overrides.get(symbol) or {})
    if override:
        selected.update(override)

    sensitivity_map_raw = dict(selected.get("sensitivity_map") or _default_sensitivity_map(sleeve_key=sleeve_key))
    candidate_monitor_keys = [str(key) for key in list(selected.get("candidate_specific_monitor_keys") or []) if str(key)]
    monitor_fallback = _lens_monitor_fallbacks()
    regime_snapshot = _build_regime_snapshot(context=regime_context, now=datetime.now(UTC))
    shared_regime_keys = ["VIXCLS", "BAMLH0A0HYM2"]
    regime_monitors: list[str] = []

    candidate_monitor_keys_pool = [key for key in candidate_monitor_keys if key not in shared_regime_keys and key in monitor_fallback]
    if len(candidate_monitor_keys_pool) < 3:
        ranked_keys = sorted(
            [key for key in monitor_fallback.keys() if key not in shared_regime_keys and key not in candidate_monitor_keys_pool],
            key=lambda key: _monitor_priority(key, sensitivity_map_raw, dict(regime_context.get("metrics") or {})),
            reverse=True,
        )
        for key in ranked_keys:
            candidate_monitor_keys_pool.append(key)
            if len(candidate_monitor_keys_pool) >= 3:
                break
    if len(candidate_monitor_keys_pool) < 3:
        derived = _derived_monitors_for_axes(
            differentiation_axes=list(selected.get("differentiation_axes") or []),
            candidate_data=candidate_data,
        )
        for key in derived:
            if key not in shared_regime_keys and key not in candidate_monitor_keys_pool and key in monitor_fallback:
                candidate_monitor_keys_pool.append(key)
            if len(candidate_monitor_keys_pool) >= 3:
                break

    candidate_monitors: list[str] = []
    for key in candidate_monitor_keys_pool[:4]:
        line = _monitor_line(key, regime_context, monitor_fallback[key]) if regime_context.get("available") else monitor_fallback[key]
        if line not in candidate_monitors:
            candidate_monitors.append(line)
    while len(candidate_monitors) < 3:
        fallback_key = next(
            (
                key
                for key in ("DGS10", "REAL_YIELD_10Y", "USD_STRENGTH", "GLOBAL_EQUITY_EX_US", "SP500")
                if key not in shared_regime_keys and key in monitor_fallback and key not in candidate_monitor_keys_pool
            ),
            None,
        )
        if not fallback_key:
            break
        candidate_monitor_keys_pool.append(fallback_key)
        line = _monitor_line(fallback_key, regime_context, monitor_fallback[fallback_key]) if regime_context.get("available") else monitor_fallback[fallback_key]
        if line not in candidate_monitors:
            candidate_monitors.append(line)

    all_monitor_keys = list(dict.fromkeys(shared_regime_keys + candidate_monitor_keys_pool[:4]))
    sensitivity_map = {key: str(sensitivity_map_raw.get(key) or "medium") for key in all_monitor_keys if key in sensitivity_map_raw}
    dropped_sensitivity_keys = [key for key in sensitivity_map_raw if key not in sensitivity_map]

    monitor_records: list[dict[str, Any]] = []
    missing_data: list[str] = []
    for key in shared_regime_keys:
        if key not in monitor_fallback:
            continue
        record = _build_monitor_record(key, regime_context, monitor_fallback[key], "regime_shared")
        if _monitor_is_all_null(record):
            missing_data.append(_metric_label(key))
            continue
        monitor_records.append(record)
        regime_monitors.append(_monitor_line(key, regime_context, monitor_fallback[key]))

    candidate_monitors = []
    for key in candidate_monitor_keys_pool[:4]:
        if key not in monitor_fallback:
            continue
        record = _build_monitor_record(key, regime_context, monitor_fallback[key], "candidate_unique")
        if _monitor_is_all_null(record):
            missing_data.append(_metric_label(key))
            continue
        monitor_records.append(record)
        candidate_monitors.append(_monitor_line(key, regime_context, monitor_fallback[key]))

    monitor_now = regime_monitors[:2] + candidate_monitors[:4]
    instrument_diagnostics = _build_instrument_diagnostics(candidate_data=candidate_data, selected=selected)
    review_triggers = _build_review_triggers(candidate_data=candidate_data, regime_snapshot=regime_snapshot)
    currency_exposure = _build_currency_exposure(candidate_data)
    concentration_composition = _build_concentration_composition(candidate_data)
    implementation_risks = _build_implementation_risks(candidate_data)
    tax_mechanics = _build_tax_mechanics(candidate_data)
    concentration_policy = get_concentration_policy(profile_type=profile_type)
    concentration_controls = evaluate_concentration_controls(
        candidate=candidate_data,
        sleeve_key=sleeve_key,
        target_weight_pct=float(target_weight_pct),
        policy=concentration_policy,
        warning_buffer=float(concentration_warning_buffer),
    )
    concentration_summary = summarize_concentration_status(concentration_controls)
    liquidity_profile = evaluate_liquidity_profile(candidate=candidate_data, sleeve_key=sleeve_key)
    instrument_type = str(candidate_data.get("instrument_type") or "")
    policy_placeholder = bool(candidate_data.get("policy_placeholder"))
    if sleeve_key in _CURRENCY_COMPARISON_SLEEVES and currency_exposure["underlying_currency_exposure"] == "unknown":
        if not policy_placeholder and instrument_type not in {"t_bill_sg", "money_market_fund_sg", "cash_account_sg", "long_put_overlay_strategy"}:
            missing_data.append("Underlying currency exposure")
    if sleeve_key in _EQUITY_COMPOSITION_SLEEVES and concentration_composition is None:
        missing_data.extend(["US weight", "Top-10 concentration", "Sector tilts", "EM weight"])
    if (
        instrument_type in _TRACKING_RELEVANT_INSTRUMENTS
        and sleeve_key in _TRACKING_COMPARISON_SLEEVES
        and implementation_risks is None
        and not policy_placeholder
    ):
        missing_data.extend(["Tracking difference proxy", "Bid/ask spread proxy"])
    missing_data.extend(list(regime_snapshot.get("missing_inputs") or []))
    missing_data = list(dict.fromkeys([item for item in missing_data if str(item).strip()]))
    rules = list(regime_snapshot.get("rules") or [])
    backdrop_summary = _build_backdrop_summary(regime_snapshot)
    consistency_warnings: list[str] = []
    if dropped_sensitivity_keys:
        consistency_warnings.append(
            f"Sensitivity keys removed due monitor mismatch: {', '.join(dropped_sensitivity_keys)}"
        )
    micro_implications = buildImplications(
        candidate=candidate_data,
        lens_inputs={
            "sleeve_key": sleeve_key,
            "selected": selected,
            "regime_snapshot": regime_snapshot,
            "missing_data": missing_data,
        },
    )
    macro_implications = _build_macro_implications(
        regime_snapshot=regime_snapshot,
        monitor_records=monitor_records,
    )
    differentiators = [
        {
            "label": "EM included",
            "value": str(selected.get("em_inclusion") or "unknown"),
            "status": "verified" if bool(candidate_data.get("proof_isin")) else "partially_verified",
            "citation_ids": _citation_ids(candidate_data),
        },
        {
            "label": "Share class",
            "value": str(candidate_data.get("accumulation_or_distribution") or "unknown"),
            "status": "verified" if bool(candidate_data.get("proof_accumulating")) else "partially_verified",
            "citation_ids": _citation_ids(candidate_data),
        },
        {
            "label": "Domicile",
            "value": str(candidate_data.get("domicile") or "unknown"),
            "status": "verified" if bool(candidate_data.get("proof_domicile")) else "partially_verified",
            "citation_ids": _citation_ids(candidate_data),
        },
        {
            "label": "TER",
            "value": f"{(float(candidate_data.get('expense_ratio')) * 100):.2f}%"
            if isinstance(candidate_data.get("expense_ratio"), (int, float))
            else "unknown",
            "status": "verified" if bool(candidate_data.get("proof_ter")) else "partially_verified",
            "citation_ids": _citation_ids(candidate_data),
        },
    ]
    differentiators = [item for item in differentiators if str(item.get("value") or "").strip().lower() not in {"", "unknown"}][:4]
    implication_blockers = []
    for row in micro_implications:
        implication_blockers.extend([str(item) for item in list(row.get("blockers") or []) if str(item).strip()])
    raw_material_unknowns = _dedupe_unknowns(
        [
            item
            for item in (missing_data + implication_blockers + list(candidate_data.get("verification_missing") or []))
            if any(
                token in str(item).lower()
                for token in (
                    "us weight",
                    "em weight",
                    "top-10",
                    "tracking",
                    "underlying currency",
                    "share-class",
                    "ter",
                    "replication",
                )
            )
        ]
    )
    if policy_placeholder or instrument_type in {"t_bill_sg", "money_market_fund_sg", "cash_account_sg", "long_put_overlay_strategy"}:
        material_unknowns = [
            item for item in raw_material_unknowns
            if "ter" not in str(item).lower() and "underlying currency" not in str(item).lower()
        ]
    else:
        material_unknowns = raw_material_unknowns
    unknowns_that_matter = material_unknowns[:6]
    source_gap_highlights = [
        item for item in unknowns_that_matter
        if any(token in str(item).lower() for token in ("tracking", "top-10", "us weight", "em weight", "underlying currency"))
    ][:6]
    readiness_blockers = _dedupe_unknowns(unknowns_that_matter + list(candidate_data.get("verification_missing") or []))[:6]
    comparison_readiness = {
        "status": "ready" if not readiness_blockers else "incomplete",
        "blockers": readiness_blockers,
    }
    exposure_fingerprint = {
        "us_weight": {"value": _safe_float(candidate_data.get("us_weight_pct")), "status": "verified" if _safe_float(candidate_data.get("us_weight_pct")) is not None else "missing"},
        "em_weight": {"value": _safe_float(candidate_data.get("em_weight_pct")), "status": "verified" if _safe_float(candidate_data.get("em_weight_pct")) is not None else "missing"},
        "top10_weight": {"value": _safe_float(candidate_data.get("top10_concentration_pct")), "status": "verified" if _safe_float(candidate_data.get("top10_concentration_pct")) is not None else "missing"},
        "tech_weight": {"value": _safe_float(candidate_data.get("tech_weight_pct")), "status": "verified" if _safe_float(candidate_data.get("tech_weight_pct")) is not None else "missing"},
        "holdings_count": {"value": _safe_float(candidate_data.get("holdings_count")), "status": "verified" if _safe_float(candidate_data.get("holdings_count")) is not None else "missing"},
    }
    advanced = {
        "regime_labels": [
            {"label": "Volatility", "value": str(regime_snapshot.get("volatility_label") or "unknown")},
            {"label": "Rates pressure", "value": str(regime_snapshot.get("rates_pressure_label") or "unknown")},
            {"label": "Credit stress", "value": str(regime_snapshot.get("credit_stress_label") or "unknown")},
            {"label": "Risk appetite", "value": str(regime_snapshot.get("risk_appetite_label") or "unknown")},
        ],
        "rules": rules,
        "monitors": monitor_records,
        "rule_audit_trail": [
            {
                "label": "Volatility",
                "summary": str(dict(regime_snapshot.get("rule_summaries") or {}).get("volatility") or ""),
                "evidence": str(dict(regime_snapshot.get("rule_evidence") or {}).get("volatility") or ""),
            },
            {
                "label": "Rates pressure",
                "summary": str(dict(regime_snapshot.get("rule_summaries") or {}).get("rates_pressure") or ""),
                "evidence": str(dict(regime_snapshot.get("rule_evidence") or {}).get("rates_pressure") or ""),
            },
            {
                "label": "Credit stress",
                "summary": str(dict(regime_snapshot.get("rule_summaries") or {}).get("credit_stress") or ""),
                "evidence": str(dict(regime_snapshot.get("rule_evidence") or {}).get("credit_stress") or ""),
            },
            {
                "label": "Risk appetite",
                "summary": str(dict(regime_snapshot.get("rule_summaries") or {}).get("risk_appetite") or ""),
                "evidence": str(dict(regime_snapshot.get("rule_evidence") or {}).get("risk_appetite") or ""),
            },
        ],
        "review_triggers": review_triggers[:6],
        "consistency_warnings": consistency_warnings,
    }
    advanced_pack = {
        "micro_drivers": micro_implications,
        "macro_drivers": macro_implications,
        "risk_controls": concentration_controls,
        "liquidity_profile": liquidity_profile,
        "implementation_checks": [
            {
                "label": "Tracking-difference proxy",
                "value": implementation_risks.get("tracking_difference_note") if implementation_risks else None,
                "status": "verified" if implementation_risks and implementation_risks.get("tracking_difference_note") else "missing",
                "citation_ids": _citation_ids(candidate_data),
            },
            {
                "label": "Bid/ask spread proxy",
                "value": implementation_risks.get("bid_ask_spread_proxy") if implementation_risks else None,
                "status": "verified" if implementation_risks and implementation_risks.get("bid_ask_spread_proxy") is not None else "missing",
                "citation_ids": _citation_ids(candidate_data),
            },
            {
                "label": "Replication method",
                "value": implementation_risks.get("replication_method") if implementation_risks else None,
                "status": "verified" if implementation_risks and implementation_risks.get("replication_method") not in {None, "unknown"} else "unverified",
                "citation_ids": _citation_ids(candidate_data),
            },
            {
                "label": "Securities lending policy",
                "value": implementation_risks.get("securities_lending_policy") if implementation_risks else None,
                "status": "verified" if implementation_risks and implementation_risks.get("securities_lending_policy") not in {None, "unknown"} else "unverified",
                "citation_ids": _citation_ids(candidate_data),
            },
            {
                "label": "Primary listing / liquidity proxy",
                "value": str(candidate_data.get("liquidity_proxy") or "").strip() or None,
                "status": "verified" if str(candidate_data.get("liquidity_proxy") or "").strip() else "missing",
                "citation_ids": _citation_ids(candidate_data),
            },
        ],
        "tax_and_wrapper": [
            {
                "label": "Withholding-tax exposure note",
                "value": tax_mechanics.get("withholding_tax_exposure_note"),
                "status": "unverified" if "Unverified" in str(tax_mechanics.get("withholding_tax_exposure_note")) else "verified",
                "citation_ids": _citation_ids(candidate_data),
            },
            {
                "label": "Domicile implication note",
                "value": tax_mechanics.get("domicile_implication_note"),
                "status": "unverified" if "Unverified" in str(tax_mechanics.get("domicile_implication_note")) else "verified",
                "citation_ids": _citation_ids(candidate_data),
            },
            {
                "label": "Distribution mechanics",
                "value": tax_mechanics.get("distribution_mechanics_note"),
                "status": "verified" if str(candidate_data.get("accumulation_or_distribution") or "").strip() else "unverified",
                "citation_ids": _citation_ids(candidate_data),
            },
        ],
        "rules_and_labels": [
            {
                "label": "Volatility",
                "value": str(regime_snapshot.get("volatility_label") or "unknown"),
                "rule": str(dict(regime_snapshot.get("rule_summaries") or {}).get("volatility") or ""),
                "evidence": str(dict(regime_snapshot.get("rule_evidence") or {}).get("volatility") or ""),
            },
            {
                "label": "Rates pressure",
                "value": str(regime_snapshot.get("rates_pressure_label") or "unknown"),
                "rule": str(dict(regime_snapshot.get("rule_summaries") or {}).get("rates_pressure") or ""),
                "evidence": str(dict(regime_snapshot.get("rule_evidence") or {}).get("rates_pressure") or ""),
            },
            {
                "label": "Credit stress",
                "value": str(regime_snapshot.get("credit_stress_label") or "unknown"),
                "rule": str(dict(regime_snapshot.get("rule_summaries") or {}).get("credit_stress") or ""),
                "evidence": str(dict(regime_snapshot.get("rule_evidence") or {}).get("credit_stress") or ""),
            },
            {
                "label": "Risk appetite",
                "value": str(regime_snapshot.get("risk_appetite_label") or "unknown"),
                "rule": str(dict(regime_snapshot.get("rule_summaries") or {}).get("risk_appetite") or ""),
                "evidence": str(dict(regime_snapshot.get("rule_evidence") or {}).get("risk_appetite") or ""),
            },
        ],
        "monitor_evidence": monitor_records,
        "citations": list(candidate_data.get("citations") or []),
        "consistency_warnings": consistency_warnings,
        "review_triggers": review_triggers[:6],
        "verification_proof_gaps": [str(item) for item in list(candidate_data.get("verification_missing") or []) if str(item).strip()],
        "source_gap_highlights": source_gap_highlights,
    }
    backdrop = {
        "summary": backdrop_summary,
        "confidence": str(regime_snapshot.get("confidence") or "unknown"),
        "freshness": str(regime_snapshot.get("data_freshness") or "unknown"),
        "missing_core_inputs": list(regime_snapshot.get("missing_inputs") or []),
    }
    long_term_fit = list(selected.get("implementation_notes") or [])[:5]
    if len(long_term_fit) < 3:
        long_term_fit.extend(
            [
                "This candidate is assessed against long-horizon policy-sleeve fit, not short-term signals.",
                "Comparison focuses on structure, exposures, implementation quality, and verification depth.",
                "Regime diagnostics are used as context for monitoring consistency over time.",
            ]
        )
    long_term_fit = long_term_fit[:5]
    strengths = list(selected.get("tends_to_help") or [])[:4]
    risks = list(selected.get("risk_drivers") or [])[:4]

    return {
        "horizon": "long",
        "role_line": str(selected["role_line"]),
        "long_term_fit": long_term_fit,
        "micro_implications": micro_implications,
        "macro_implications": macro_implications,
        "strengths": strengths,
        "risks": risks,
        "unknowns_that_matter": unknowns_that_matter,
        "source_gap_highlights": source_gap_highlights,
        "advanced_pack": advanced_pack,
        "risk_controls": concentration_controls,
        "risk_control_summary": concentration_summary,
        "liquidity_profile": liquidity_profile,
        "backdrop": backdrop,
        "implications": micro_implications,
        "candidate_differentiators": differentiators,
        "comparison_readiness": comparison_readiness,
        "exposure_fingerprint": exposure_fingerprint,
        "advanced": advanced,
        "implementation_notes": list(selected.get("implementation_notes") or [])[:3],
        "instrument_diagnostics": instrument_diagnostics[:5],
        "risk_drivers": list(selected["risk_drivers"])[:4],
        "tends_to_help": list(selected["tends_to_help"])[:4],
        "tends_to_hurt": list(selected["tends_to_hurt"])[:4],
        "backdrop_summary": backdrop_summary,
        "confidence": str(regime_snapshot.get("confidence") or "unknown"),
        "freshness": str(regime_snapshot.get("data_freshness") or "unknown"),
        "missing_data": missing_data[:8],
        "currency_exposure": currency_exposure,
        "concentration_and_composition": concentration_composition,
        "implementation_risks": implementation_risks,
        "tax_mechanics": tax_mechanics,
        "monitors": monitor_records,
        "rules": rules,
        "monitor_now": monitor_now[:6],
        "regime_monitors": regime_monitors[:2],
        "candidate_monitors": candidate_monitors[:4],
        "review_triggers": review_triggers[:6],
        "sensitivity_map": {
            key: str(sensitivity_map.get(key) or "medium")
            for key in sensitivity_map
        },
        "candidate_specific_monitor_keys": candidate_monitor_keys_pool[:4],
        "regime_snapshot": regime_snapshot,
        "consistency_warnings": consistency_warnings,
        "lens_basis": "Diagnostic lens for monitoring regime context and implementation fit, not a return forecast or directive.",
        "candidate_name": candidate_name,
    }


def build_portfolio_blueprint_payload() -> dict[str, Any]:
    now = datetime.now(UTC)
    settings = Settings.from_env()
    freshness_days_threshold = int(settings.blueprint_factsheet_max_age_days)
    regime_context = _load_lens_regime_context(settings)
    conn = connect(get_db_path(settings=settings))
    enforce_registry_parity(conn, fail_on_fatal=True)
    current_holdings = [holding.model_dump(mode="json") for holding in list_holdings(conn)]
    portfolio_state_context = build_portfolio_state_context(current_holdings)
    ensure_benchmark_registry_tables(conn)
    ensure_citation_health_tables(conn)
    ensure_blueprint_decision_tables(conn)
    ensure_quality_tables(conn)
    ensure_recommendation_tables(conn)

    # Primary/official documentation sources used across sleeves.
    sources = {
        "vanguard_vwra": _citation(
            source_id="vanguard_vwra",
            url="https://www.vanguard.co.uk/professional/product/etf/equity/9679/ftse-all-world-ucits-etf-usd-accumulating",
            retrieved_at=now,
            importance="Issuer product page and KID access for global all-cap UCITS",
        ),
        "vanguard_vwrl": _citation(
            source_id="vanguard_vwrl",
            url="https://www.vanguard.co.uk/professional/product/etf/equity/9505/ftse-all-world-ucits-etf-usd-distributing",
            retrieved_at=now,
            importance="Issuer product page for distributing version of global all-cap UCITS",
        ),
        "ishares_ssac": _citation(
            source_id="ishares_ssac",
            url="https://www.ishares.com/uk/individual/en/products/308245/ishares-msci-acwi-ucits-etf",
            retrieved_at=now,
            importance="Issuer product page for ACWI UCITS implementation comparison",
        ),
        "vanguard_veve": _citation(
            source_id="vanguard_veve",
            url="https://www.vanguard.co.uk/professional/product/etf/equity/9675/ftse-developed-world-ucits-etf-usd-distributing",
            retrieved_at=now,
            importance="Issuer page for developed-market split sleeve comparisons",
        ),
        "ishares_iwda": _citation(
            source_id="ishares_iwda",
            url="https://www.ishares.com/uk/individual/en/products/251882/ishares-msci-world-ucits-etf-acc-fund",
            retrieved_at=now,
            importance="Issuer page for accumulating developed-market UCITS baseline",
        ),
        "ishares_cspx": _citation(
            source_id="ishares_cspx",
            url="https://www.ishares.com/uk/individual/en/products/253743/ishares-sp-500-b-ucits-etf-acc-fund",
            retrieved_at=now,
            importance="Issuer page for S&P 500 UCITS accumulating implementation",
        ),
        "ishares_eimi": _citation(
            source_id="ishares_eimi",
            url="https://www.ishares.com/uk/individual/en/products/264659/ishares-core-msci-em-imi-ucits-etf",
            retrieved_at=now,
            importance="Issuer page for emerging markets IMI UCITS exposure",
        ),
        "vanguard_vfea": _citation(
            source_id="vanguard_vfea",
            url="https://www.vanguard.co.uk/professional/product/etf/equity/9671/ftse-emerging-markets-ucits-etf-usd-accumulating",
            retrieved_at=now,
            importance="Issuer page for accumulating EM UCITS alternative",
        ),
        "hsbc_hmch": _citation(
            source_id="hsbc_hmch",
            url="https://www.assetmanagement.hsbc.com.sg/en/intermediary/fund-centre/etf/china-equity/ie00bk1pv551",
            retrieved_at=now,
            importance="Issuer page for China equity UCITS sleeve candidate",
        ),
        "xtrackers_china": _citation(
            source_id="xtrackers_china",
            url="https://etf.dws.com/en-gb/IE00BGHQ0G80-msci-china-ucits-etf-1c/",
            retrieved_at=now,
            importance="Issuer page for China UCITS comparison candidate",
        ),
        "ishares_aggu": _citation(
            source_id="ishares_aggu",
            url="https://www.ishares.com/uk/individual/en/products/251767/ishares-core-global-aggregate-bond-ucits-etf",
            retrieved_at=now,
            importance="Issuer page for global aggregate UCITS bond sleeve",
        ),
        "vanguard_vagu": _citation(
            source_id="vanguard_vagu",
            url="https://www.vanguard.co.uk/professional/product/etf/bond/9865/global-aggregate-bond-ucits-etf-usd-hedged-accumulating",
            retrieved_at=now,
            importance="Issuer page for accumulating global aggregate bond UCITS",
        ),
        "nikko_a35": _citation(
            source_id="nikko_a35",
            url="https://www.nikkoam.com.sg/etf/a35",
            retrieved_at=now,
            importance="Issuer page for SGD bond implementation reference",
        ),
        "ishares_ib01": _citation(
            source_id="ishares_ib01",
            url="https://www.ishares.com/uk/individual/en/products/307243/ishares-treasury-bond-0-1yr-ucits-etf",
            retrieved_at=now,
            importance="Issuer page for short-duration bills proxy",
        ),
        "spdr_sg_mmf": _citation(
            source_id="spdr_sg_mmf",
            url="https://www.ssga.com/sg/en_gb/institutional/etfs/funds/spdr-bloomberg-1-3-month-t-bill-etf-bil",
            retrieved_at=now,
            importance="Issuer page for short-bill liquidity comparison benchmark",
        ),
        "mas_tbill": _citation(
            source_id="mas_tbill",
            url="https://www.mas.gov.sg/bonds-and-bills/treasury-bills",
            retrieved_at=now,
            importance="Official Singapore Treasury Bills reference and auction information",
        ),
        "fullerton_sgd_cash": _citation(
            source_id="fullerton_sgd_cash",
            url="https://www.fullertonfund.com/funds/fullerton-sgd-cash-fund/",
            retrieved_at=now,
            importance="Official SGD money market fund page for implementation-review fallback",
        ),
        "ishares_sgln": _citation(
            source_id="ishares_sgln",
            url="https://www.ishares.com/uk/individual/en/products/258441/ishares-physical-gold-etc",
            retrieved_at=now,
            importance="Issuer page for physical gold ETC candidate",
        ),
        "invesco_cmod": _citation(
            source_id="invesco_cmod",
            url="https://www.invesco.com/uk/en/financial-products/etfs/invesco-bloomberg-commodity-ucits-etf.html",
            retrieved_at=now,
            importance="Issuer page for broad commodities UCITS candidate",
        ),
        "ishares_iwdp": _citation(
            source_id="ishares_iwdp",
            url="https://www.ishares.com/uk/individual/en/products/251908/ishares-developed-markets-property-yield-ucits-etf",
            retrieved_at=now,
            importance="Issuer page for developed-market REIT UCITS candidate",
        ),
        "dbmf": _citation(
            source_id="dbmf",
            url="https://www.im.natixis.com/en-us/etf/dbmf",
            retrieved_at=now,
            importance="Issuer page for managed futures implementation bucket",
        ),
        "kmlm": _citation(
            source_id="kmlm",
            url="https://kfafunds.com/kmlm/",
            retrieved_at=now,
            importance="Issuer page for managed futures alternative bucket",
        ),
        "tail": _citation(
            source_id="tail",
            url="https://www.cambriafunds.com/tail",
            retrieved_at=now,
            importance="Issuer page for tail-hedge bucket",
        ),
        "caos": _citation(
            source_id="caos",
            url="https://etfsite.alphaarchitect.com/caos/",
            retrieved_at=now,
            importance="Issuer page for tail-hedge fallback bucket",
        ),
        "cboe_spx": _citation(
            source_id="cboe_spx",
            url="https://www.cboe.com/tradable_products/sp_500/spx_options/specifications/",
            retrieved_at=now,
            importance="Primary options contract specification for long-put bucket",
        ),
        "irs_nra": _citation(
            source_id="irs_nra",
            url="https://www.irs.gov/individuals/international-taxpayers/federal-income-tax-withholding-and-reporting-on-other-kinds-of-us-source-income-paid-to-nonresident-aliens",
            retrieved_at=now,
            importance="Primary US withholding reference for SG lens comparisons",
        ),
        "iras_overseas_income": _citation(
            source_id="iras_overseas_income",
            url="https://www.iras.gov.sg/taxes/individual-income-tax/basics-of-individual-income-tax/what-is-taxable-what-is-not/income-received-from-overseas",
            retrieved_at=now,
            importance="Primary Singapore tax context for overseas income handling",
        ),
    }

    ips_profile = get_ips(conn)
    ips_policy = blueprint_policy_from_ips(ips_profile)
    registry_sleeves = _build_registry_backed_sleeves(
        conn=conn,
        sources=sources,
        policy_ranges=ips_policy,
    )

    sleeves: list[dict[str, Any]] = list(registry_sleeves or [])
    using_legacy_inline_fallback = False

    if (not sleeves) and settings.blueprint_allow_legacy_inline_fallback:
        using_legacy_inline_fallback = True
        sleeves = []

        sleeves.append(
            {
                "sleeve_key": "global_equity_core",
                "name": "Global Equity Core",
                "policy_weight_range": {"min": 40, "target": 45, "max": 55},
                "purpose": "Long-horizon growth anchor. Default implementation is one global all-cap accumulating UCITS fund.",
                "constraints": [
                    "Default pathway keeps a single-fund global all-cap core.",
                    "Optional split implementation remains secondary and should preserve global diversification.",
                    "Accumulating IE UCITS preference is applied where available.",
                ],
                "candidates": _sort_candidates(
                    [
                        _candidate(
                            symbol="VWRA",
                            name="Vanguard FTSE All-World UCITS ETF (USD Acc)",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0022,
                            liquidity_proxy="LSE primary listing with broad secondary market participation",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Single-fund global equity implementation with broad developed and emerging coverage and accumulating share class.",
                            citations=[sources["vanguard_vwra"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.88,
                            extra={"isin": "IE00BK5BQT80"},
                        ),
                        _candidate(
                            symbol="SSAC",
                            name="iShares MSCI ACWI UCITS ETF (Acc)",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0020,
                            liquidity_proxy="Large UCITS platform depth across EU venues",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Global ACWI implementation that can be used as an alternate one-fund core with comparable scope.",
                            citations=[sources["ishares_ssac"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.86,
                            extra={"isin": "IE00B6R52259"},
                        ),
                        _candidate(
                            symbol="IWDA",
                            name="iShares Core MSCI World UCITS ETF (Acc)",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0020,
                            liquidity_proxy="High turnover UCITS developed-market listing",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Developed-market anchor that can pair with a dedicated EM sleeve in optional split implementations.",
                            citations=[sources["ishares_iwda"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.87,
                            extra={"isin": "IE00B4L5Y983"},
                        ),
                        _candidate(
                            symbol="CSPX",
                            name="iShares Core S&P 500 UCITS ETF (Acc)",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0007,
                            liquidity_proxy="Highly liquid UCITS S&P 500 tracker with deep European market access",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="US large-cap focused alternative for investors preferring S&P 500 exposure over global diversification. Lower TER than global funds.",
                            citations=[sources["ishares_cspx"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.90,
                            extra={"isin": "IE00B5BMR087"},
                        ),
                        _candidate(
                            symbol="VWRL",
                            name="Vanguard FTSE All-World UCITS ETF (USD Dist)",
                            domicile="IE",
                            accumulation="distributing",
                            expense_ratio=0.0019,
                            liquidity_proxy="LSE primary listing with broad secondary market participation",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Distributing share class alternative to VWRA for investors requiring income distribution instead of accumulation.",
                            citations=[sources["vanguard_vwrl"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.87,
                            extra={"isin": "IE00B3RBWM25"},
                        ),
                    ]
                ),
            }
        )
    
        sleeves.append(
            {
                "sleeve_key": "developed_ex_us_optional",
                "name": "Developed ex-US Optional Split",
                "policy_weight_range": {"min": 0, "target": 0, "max": 10},
                "purpose": "Optional secondary split sleeve for investors who track regional drift around a one-fund core.",
                "constraints": [
                    "Optional sleeve only; default target remains zero.",
                    "When used, sleeve should preserve broad developed-market coverage and avoid concentrated single-country tilts.",
                ],
                "candidates": _sort_candidates(
                    [
                        _candidate(
                            symbol="VEVE",
                            name="Vanguard FTSE Developed World UCITS ETF",
                            domicile="IE",
                            accumulation="distributing",
                            expense_ratio=0.0012,
                            liquidity_proxy="Broad cross-listing liquidity on major UCITS exchanges",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Developed-market sleeve that may be paired with emerging and China satellites in split structures.",
                            citations=[sources["vanguard_veve"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.84,
                            extra={"isin": "IE00BKX55T58"},
                        ),
                        _candidate(
                            symbol="IWDA",
                            name="iShares Core MSCI World UCITS ETF (Acc)",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0020,
                            liquidity_proxy="High turnover UCITS developed-market listing",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Can function as developed-market proxy when optional regional split is monitored alongside EM and China satellites.",
                            citations=[sources["ishares_iwda"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.87,
                            extra={"isin": "IE00B4L5Y983"},
                        ),
                    ]
                ),
            }
        )
    
        sleeves.append(
            {
                "sleeve_key": "emerging_markets",
                "name": "Emerging Markets",
                "policy_weight_range": {"min": 5, "target": 7, "max": 10},
                "purpose": "Satellite growth sleeve for broad EM exposure outside developed-market core holdings.",
                "constraints": [
                    "Exposure should remain diversified across EM regions.",
                    "China concentration should be monitored and coordinated with explicit China satellite sleeve.",
                ],
                "candidates": _sort_candidates(
                    [
                        _candidate(
                            symbol="EIMI",
                            name="iShares Core MSCI EM IMI UCITS ETF",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0018,
                            liquidity_proxy="Core UCITS EM vehicle with broad exchange liquidity",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Broad IMI coverage including large, mid, and small-cap EM names aligns with long-horizon sleeve intent.",
                            citations=[sources["ishares_eimi"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.84,
                            extra={"isin": "IE00BKM4GZ66"},
                        ),
                        _candidate(
                            symbol="VFEA",
                            name="Vanguard FTSE Emerging Markets UCITS ETF (Acc)",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0017,
                            liquidity_proxy="UCITS emerging-market listing with broad issuer support",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Alternative broad EM exposure with accumulating share class and similar sleeve role.",
                            citations=[sources["vanguard_vfea"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.81,
                            extra={"isin": "IE00BK5BR733"},
                        ),
                    ]
                ),
            }
        )
    
        sleeves.append(
            {
                "sleeve_key": "china_satellite",
                "name": "China Satellite",
                "policy_weight_range": {"min": 0, "target": 3, "max": 5},
                "purpose": "Limited tactical satellite for explicit China allocation transparency.",
                "constraints": [
                    "China sleeve is capped and treated as satellite rather than core.",
                    "Combined China exposure across all sleeves should remain within policy cap.",
                ],
                "candidates": _sort_candidates(
                    [
                        _candidate(
                            symbol="HMCH",
                            name="HSBC MSCI China UCITS ETF",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0028,
                            liquidity_proxy="UCITS listing liquidity with market-maker support",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.10,
                            rationale="Direct China equity sleeve candidate for capped satellite allocation monitoring.",
                            citations=[sources["hsbc_hmch"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.76,
                            extra={"isin": "IE00B44T3H88"},
                        ),
                        _candidate(
                            symbol="XCHA",
                            name="Xtrackers MSCI China UCITS ETF 1C",
                            domicile="LU",
                            accumulation="accumulating",
                            expense_ratio=0.0065,
                            liquidity_proxy="UCITS exchange liquidity across major European venues",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.10,
                            rationale="Alternate China UCITS sleeve for implementation comparison under explicit cap discipline.",
                            citations=[sources["xtrackers_china"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.72,
                            extra={"isin": "LU0514695690"},
                        ),
                    ]
                ),
            }
        )
    
        sleeves.append(
            {
                "sleeve_key": "ig_bonds",
                "name": "Investment Grade Bonds",
                "policy_weight_range": {"min": 20, "target": 25, "max": 30},
                "purpose": "Defensive ballast and duration sleeve for macro drawdown dampening.",
                "constraints": [
                    "Preference for broad IG coverage with transparent duration profile.",
                    "SGD implementation options may be used when base-currency stability is prioritized.",
                ],
                "candidates": _sort_candidates(
                    [
                        _candidate(
                            symbol="AGGU",
                            name="iShares Core Global Aggregate Bond UCITS ETF",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0010,
                            liquidity_proxy="Core UCITS bond vehicle with deep aggregate-bond secondary liquidity",
                            replication_method="sampling",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Broad IG bond coverage across sovereign and corporate issuers in a diversified global wrapper.",
                            citations=[sources["ishares_aggu"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.86,
                            extra={"isin": "IE00BZ043R46"},
                        ),
                        _candidate(
                            symbol="VAGU",
                            name="Vanguard Global Aggregate Bond UCITS ETF (USD Hedged Acc)",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0008,
                            liquidity_proxy="Institutional UCITS bond liquidity profile",
                            replication_method="sampling",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Accumulating global aggregate option with currency-hedged profile for implementation comparison.",
                            citations=[sources["vanguard_vagu"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.82,
                            extra={"isin": "IE00BG47KJ78"},
                        ),
                        _candidate(
                            symbol="A35",
                            name="ABF Singapore Bond Index Fund",
                            domicile="SG",
                            accumulation="distributing",
                            expense_ratio=0.0024,
                            liquidity_proxy="SGX local listing for SGD-oriented bond implementation",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.00,
                            rationale="Local SGD bond sleeve reference for investors emphasizing domestic currency liability matching.",
                            citations=[sources["nikko_a35"], sources["iras_overseas_income"]],
                            liquidity_score=0.8,
                            extra={"isin": "SG1S08926457"},
                        ),
                    ]
                ),
            }
        )
    
        sleeves.append(
            {
                "sleeve_key": "cash_bills",
                "name": "Cash and Bills",
                "policy_weight_range": {"min": 5, "target": 10, "max": 15},
                "purpose": "Liquidity and optional deployment reserve with low duration risk.",
                "constraints": [
                    "Focus remains on high-liquidity, short-maturity instruments.",
                    "This sleeve functions as buffer capital, not return-maximization core.",
                ],
                "candidates": _sort_candidates(
                    [
                        _candidate(
                            symbol="IB01",
                            name="iShares $ Treasury Bond 0-1yr UCITS ETF",
                            instrument_type="etf_ucits",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0007,
                            liquidity_proxy="Short-duration treasury UCITS vehicle with steady turnover",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Short-maturity treasury profile that can support liquidity-focused reserve sleeve design.",
                            citations=[sources["ishares_ib01"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.8,
                            extra={"isin": "IE00BGSF1X88"},
                        ),
                        _candidate(
                            symbol="BIL",
                            name="SPDR Bloomberg 1-3 Month T-Bill ETF",
                            instrument_type="etf_us",
                            domicile="US",
                            accumulation="accumulating",
                            expense_ratio=0.00135,
                            liquidity_proxy="High on-screen liquidity in US session",
                            replication_method="physical",
                            us_situs_risk_flag=True,
                            expected_withholding_drag_estimate=0.30,
                            rationale="US-listed bills benchmark for cross-market implementation comparison under SG tax lens.",
                            citations=[sources["spdr_sg_mmf"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.92,
                            extra={"cusip": "78468R703"},
                        ),
                        _candidate(
                            symbol="UCITS_MMF_PLACEHOLDER",
                            name="UCITS money market ETF candidate (policy placeholder)",
                            instrument_type="etf_ucits",
                            domicile="IE",
                            accumulation="unknown",
                            expense_ratio=None,
                            liquidity_proxy="unknown",
                            replication_method="unknown",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=None,
                            rationale="Policy placeholder for UCITS money-market ETF slot when implementation review has not selected a concrete share class.",
                            citations=[],
                            liquidity_score=None,
                            extra={"policy_placeholder": True},
                        ),
                        _candidate(
                            symbol="SG_TBILL_POLICY",
                            name="Singapore Treasury Bills (MAS auctions)",
                            instrument_type="t_bill_sg",
                            domicile="SG",
                            accumulation="n/a",
                            expense_ratio=None,
                            liquidity_proxy="Primary auctions and local money-market channels",
                            replication_method="sovereign bill issuance",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=None,
                            rationale="Policy fallback candidate for SGD base-currency reserve sleeve implementation review.",
                            citations=[sources["mas_tbill"], sources["iras_overseas_income"]],
                            liquidity_score=None,
                            extra={
                                "share_class_id": "MAS_TBILL",
                            },
                        ),
                        _candidate(
                            symbol="SGD_MMF_POLICY",
                            name="SGD Money Market Fund (example class)",
                            instrument_type="money_market_fund_sg",
                            domicile="SG",
                            accumulation="distribution",
                            expense_ratio=None,
                            liquidity_proxy="Fund dealing cycle and local settlement",
                            replication_method="active cash management",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=None,
                            rationale="Policy fallback candidate where direct T-bill implementation is operationally constrained.",
                            citations=[sources["fullerton_sgd_cash"], sources["iras_overseas_income"]],
                            liquidity_score=None,
                        ),
                        _candidate(
                            symbol="SGD_CASH_RESERVE",
                            name="SGD cash reserve",
                            instrument_type="cash_account_sg",
                            domicile="SG",
                            accumulation="n/a",
                            expense_ratio=None,
                            liquidity_proxy="Bank cash account",
                            replication_method="cash account",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=None,
                            rationale="Policy placeholder for liquidity reserve when instrument implementation is pending.",
                            citations=[sources["iras_overseas_income"]],
                            liquidity_score=None,
                            extra={
                                "policy_placeholder": True,
                            },
                        ),
                    ]
                ),
            }
        )
    
        sleeves.append(
            {
                "sleeve_key": "real_assets",
                "name": "Real Assets",
                "policy_weight_range": {"min": 5, "target": 7, "max": 10},
                "purpose": "Inflation-sensitive diversifier sleeve through gold, commodities, and property proxies.",
                "constraints": [
                    "This sleeve is optional diversifier and should not displace core equity or bond policy anchors.",
                    "Position sizing should account for commodity structure and tracking differences.",
                ],
                "candidates": _sort_candidates(
                    [
                        _candidate(
                            symbol="SGLN",
                            name="iShares Physical Gold ETC",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0012,
                            liquidity_proxy="Large ETC with active market-maker depth",
                            replication_method="physically backed",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.00,
                            rationale="Physical gold sleeve candidate that can diversify inflation or policy-shock regimes.",
                            citations=[sources["ishares_sgln"], sources["iras_overseas_income"]],
                            liquidity_score=0.82,
                            extra={"isin": "IE00B4ND3602"},
                        ),
                        _candidate(
                            symbol="CMOD",
                            name="Invesco Bloomberg Commodity UCITS ETF",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0019,
                            liquidity_proxy="UCITS commodity vehicle with broad futures-based index exposure",
                            replication_method="swap-based",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.00,
                            rationale="Broad commodity beta reference for conservative alternatives and real-assets comparisons.",
                            citations=[sources["invesco_cmod"], sources["iras_overseas_income"]],
                            liquidity_score=0.71,
                            extra={"isin": "IE00BD6FTQ80"},
                        ),
                        _candidate(
                            symbol="IWDP",
                            name="iShares Developed Markets Property Yield UCITS ETF",
                            domicile="IE",
                            accumulation="distributing",
                            expense_ratio=0.0059,
                            liquidity_proxy="UCITS REIT listing liquidity with developed-market property basket",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Global REIT implementation reference for income-oriented real-asset sleeve comparisons.",
                            citations=[sources["ishares_iwdp"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.7,
                            extra={"isin": "IE00B1FZS350"},
                        ),
                    ]
                ),
            }
        )
    
        sleeves.append(
            {
                "sleeve_key": "alternatives",
                "name": "Alternatives",
                "policy_weight_range": {"min": 0, "target": 3, "max": 7},
                "purpose": "Conservative optional diversifier sleeve spanning gold, broad commodities, and global REIT pathways.",
                "constraints": [
                    "Sleeve remains diagnostic and optional under the base policy.",
                    "Instruments should preserve transparent max-loss profile and avoid margin-dependent structures.",
                ],
                "candidates": _sort_candidates(
                    [
                        _candidate(
                            symbol="SGLN",
                            name="iShares Physical Gold ETC",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0012,
                            liquidity_proxy="Large ETC with active market-maker depth",
                            replication_method="physically backed",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.00,
                            rationale="Gold candidate can act as defensive diversifier within conservative alternatives sleeve framing.",
                            citations=[sources["ishares_sgln"], sources["iras_overseas_income"]],
                            liquidity_score=0.82,
                        ),
                        _candidate(
                            symbol="CMOD",
                            name="Invesco Bloomberg Commodity UCITS ETF",
                            domicile="IE",
                            accumulation="accumulating",
                            expense_ratio=0.0019,
                            liquidity_proxy="UCITS commodity vehicle with broad futures-based index exposure",
                            replication_method="swap-based",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.00,
                            rationale="Broad commodity implementation provides an additional diversifier in inflation-sensitive windows.",
                            citations=[sources["invesco_cmod"], sources["iras_overseas_income"]],
                            liquidity_score=0.71,
                        ),
                        _candidate(
                            symbol="IWDP",
                            name="iShares Developed Markets Property Yield UCITS ETF",
                            domicile="IE",
                            accumulation="distribution",
                            expense_ratio=0.0059,
                            liquidity_proxy="UCITS REIT listing liquidity with developed-market property basket",
                            replication_method="physical",
                            us_situs_risk_flag=False,
                            expected_withholding_drag_estimate=0.15,
                            rationale="Global REIT implementation can be reviewed as optional real-income diversifier exposure.",
                            citations=[sources["ishares_iwdp"], sources["irs_nra"], sources["iras_overseas_income"]],
                            liquidity_score=0.7,
                        ),
                    ]
                ),
            }
        )
    
        convex_candidates = _sort_candidates(
            [
                _candidate(
                    symbol="DBMF",
                    name="iMGP DBi Managed Futures Strategy ETF",
                    instrument_type="etf_us",
                    domicile="US",
                    accumulation="distributing",
                    expense_ratio=0.0085,
                    liquidity_proxy="US ETF with consistent daily creation-redemption activity",
                    replication_method="active futures strategy",
                    us_situs_risk_flag=True,
                    expected_withholding_drag_estimate=0.00,
                    rationale="Managed-futures bucket reference for convex allocation sizing diagnostics.",
                    citations=[sources["dbmf"], sources["irs_nra"], sources["iras_overseas_income"]],
                    liquidity_score=0.8,
                    extra={
                        "bucket": "managed_futures",
                        "bucket_target_weight": 2.0,
                        "margin_required": False,
                        "max_loss_known": True,
                        "leverage_used": False,
                        "short_options": False,
                        "cusip": "46138G862",
                    },
                ),
                _candidate(
                    symbol="KMLM",
                    name="KFA Mount Lucas Managed Futures Index Strategy ETF",
                    instrument_type="etf_us",
                    domicile="US",
                    accumulation="distributing",
                    expense_ratio=0.0090,
                    liquidity_proxy="US ETF with moderate on-screen liquidity",
                    replication_method="index futures strategy",
                    us_situs_risk_flag=True,
                    expected_withholding_drag_estimate=0.00,
                    rationale="Managed-futures alternate candidate for convex bucket implementation review.",
                    citations=[sources["kmlm"], sources["irs_nra"], sources["iras_overseas_income"]],
                    liquidity_score=0.72,
                    extra={
                        "bucket": "managed_futures",
                        "bucket_target_weight": 2.0,
                        "margin_required": False,
                        "cusip": "50047X704",
                        "max_loss_known": True,
                        "leverage_used": False,
                        "short_options": False,
                    },
                ),
                _candidate(
                    symbol="TAIL",
                    name="Cambria Tail Risk ETF",
                    instrument_type="etf_us",
                    domicile="US",
                    accumulation="distributing",
                    expense_ratio=0.0059,
                    liquidity_proxy="US ETF with active options overlay and daily fund liquidity",
                    replication_method="options + fixed income mix",
                    us_situs_risk_flag=True,
                    expected_withholding_drag_estimate=0.00,
                    rationale="Tail-hedge bucket candidate aligned with known-max-loss ETF wrapper structure.",
                    citations=[sources["tail"], sources["irs_nra"], sources["iras_overseas_income"]],
                    liquidity_score=0.82,
                    extra={
                        "bucket": "tail_hedge",
                        "bucket_target_weight": 0.7,
                        "margin_required": False,
                        "cusip": "132061862",
                        "max_loss_known": True,
                        "leverage_used": False,
                        "short_options": False,
                    },
                ),
                _candidate(
                    symbol="SPX_LONG_PUT",
                    name="Long put overlay strategy (policy placeholder)",
                    instrument_type="long_put_overlay_strategy",
                    domicile="SG",
                    accumulation="n/a",
                    expense_ratio=None,
                    liquidity_proxy="Listed index option liquidity dependent on tenor and strike",
                    replication_method="long listed option",
                    us_situs_risk_flag=False,
                    expected_withholding_drag_estimate=None,
                    rationale="Illustrative policy overlay, not a product. Max loss is known at premium outlay with no short-options exposure.",
                    citations=[sources["cboe_spx"], sources["irs_nra"], sources["iras_overseas_income"]],
                    liquidity_score=None,
                    extra={
                        "bucket": "long_put",
                        "bucket_target_weight": 0.3,
                        "fallback_bucket": "tail_hedge",
                        "margin_required": False,
                        "max_loss_known": True,
                        "leverage_used": False,
                        "short_options": False,
                        "fallback_routing": "If options permissions unavailable, allocate this bucket to tail-hedge ETF bucket.",
                        "policy_placeholder": True,
                    },
                ),
                _candidate(
                    symbol="CAOS",
                    name="Alpha Architect Tail Risk ETF",
                    instrument_type="etf_us",
                    domicile="US",
                    accumulation="distributing",
                    expense_ratio=0.0063,
                    liquidity_proxy="US ETF with moderate turnover and listed ETF access",
                    replication_method="rules-based tail-risk strategy",
                    us_situs_risk_flag=True,
                    expected_withholding_drag_estimate=0.00,
                    rationale="Tail-hedge fallback candidate where long-put overlays are restricted.",
                    citations=[sources["caos"], sources["irs_nra"], sources["iras_overseas_income"]],
                    liquidity_score=0.68,
                    extra={
                        "bucket": "tail_hedge",
                        "bucket_target_weight": 0.7,
                        "fallback_for": "long_put",
                        "cusip": "02072C200",
                        "margin_required": False,
                        "max_loss_known": True,
                        "leverage_used": False,
                        "short_options": False,
                    },
                ),
            ]
        )
    
        sleeves.append(
            {
                "sleeve_key": "convex",
                "name": "Convex",
                "policy_weight_range": {"min": 3, "target": 3, "max": 3},
                "purpose": "Dedicated downside-convexity sleeve with predefined composition constraints.",
                "constraints": [
                    "Managed futures bucket about 2.0%.",
                    "Tail hedge bucket about 0.7%.",
                    "Long put bucket about 0.3% with fallback to tail hedge if long puts are not permitted.",
                    "No margin required, max loss known, no leverage.",
                ],
                "candidates": convex_candidates,
            }
        )


    _hydrate_blueprint_candidate_metadata(sleeves, settings=settings, now=now, conn=conn)
    maybe_refresh_performance_data(
        conn=conn,
        candidates=[candidate for sleeve in sleeves for candidate in list(sleeve.get("candidates") or [])],
        settings=settings,
        now=now,
    )

    verified = 0
    partial = 0
    unverified = 0
    dedup_citations: dict[str, dict[str, Any]] = {}
    factsheet_dates: list[str] = []
    for sleeve in sleeves:
        policy_range = dict(sleeve.get("policy_weight_range") or {})
        sleeve_target = float(policy_range.get("target") or 0.0)
        sleeve_min = float(policy_range.get("min") or 0.0)
        sleeve_max = float(policy_range.get("max") or 0.0)
        rebalance_policy = build_rebalance_policy(
            sleeve_key=str(sleeve.get("sleeve_key") or ""),
            target_weight=sleeve_target / 100.0,
            min_band=sleeve_min / 100.0,
            max_band=sleeve_max / 100.0,
        )
        sleeve["rebalance_policy"] = rebalance_policy.model_dump(mode="json")
        sleeve["rebalance_diagnostics"] = evaluate_rebalance_diagnostics(policy=rebalance_policy, actual_weight=None)
        raw_candidates = list(sleeve.get("candidates") or [])
        reviewed = [
            _verify_candidate(item, now=now, freshness_days_threshold=freshness_days_threshold)
            for item in raw_candidates
        ]
        for candidate in reviewed:
            candidate["investment_lens"] = _build_investment_lens(
                sleeve_key=str(sleeve.get("sleeve_key") or ""),
                candidate_symbol=str(candidate.get("symbol") or ""),
                candidate_name=str(candidate.get("name") or ""),
                candidate_data=candidate,
                regime_context=regime_context,
                profile_type=str(settings.blueprint_profile_type),
                concentration_warning_buffer=float(settings.blueprint_concentration_warning_buffer),
                target_weight_pct=sleeve_target,
            )
        sleeve["candidates"] = _sort_candidates(reviewed)
        for candidate in sleeve["candidates"]:
            status = str(candidate.get("verification_status"))
            if status == "verified":
                verified += 1
            elif status == "partially_verified":
                partial += 1
            else:
                unverified += 1
            factsheet_asof = str(candidate.get("factsheet_asof") or "").strip()
            if factsheet_asof:
                factsheet_dates.append(factsheet_asof)
            for citation in list(candidate.get("citations") or []):
                key = (
                    f"{citation.get('url')}|{citation.get('source_id')}|"
                    f"{citation.get('retrieved_at')}|{citation.get('purpose')}"
                )
                dedup_citations[key] = citation
        sleeve["last_verified"] = max(
            [
                str(candidate.get("factsheet_asof") or "").strip()
                for candidate in sleeve["candidates"]
                if str(candidate.get("factsheet_asof") or "").strip()
            ] or [""]
        ) or None

    total_candidates = verified + partial + unverified
    if total_candidates == 0:
        trust_level = "low"
    else:
        verified_ratio = verified / float(total_candidates)
        if verified_ratio >= 0.8:
            trust_level = "high"
        elif verified_ratio >= 0.5:
            trust_level = "medium"
        else:
            trust_level = "low"

    rebalance_status_counts = {"no_action": 0, "calendar_review_due": 0, "band_breach": 0, "interdependency_warning": 0, "data_incomplete": 0}
    for sleeve in sleeves:
        status = str(dict(sleeve.get("rebalance_diagnostics") or {}).get("status") or "data_incomplete")
        rebalance_status_counts[status if status in rebalance_status_counts else "data_incomplete"] += 1

    all_candidates = [candidate for sleeve in sleeves for candidate in list(sleeve.get("candidates") or [])]
    candidate_decisions = list_candidate_decisions(conn)
    candidate_decision_events = list_candidate_decision_events(conn)
    recommendation_history_map = _recommendation_history_by_candidate(conn)
    previous_payload = _load_previous_blueprint_payload(conn)
    refresh_monitor = latest_blueprint_refresh_status(conn, settings=settings)
    evaluation_mode = _resolve_evaluation_mode(refresh_monitor=refresh_monitor)
    for sleeve in sleeves:
        for candidate in list(sleeve.get("candidates") or []):
            decision_key = (str(sleeve.get("sleeve_key")), str(candidate.get("symbol") or "").upper())
            decision = candidate_decisions.get(decision_key)
            decision_events = candidate_decision_events.get(decision_key, [])
            candidate["decision_state"] = {
                "status": str(decision.get("status") or "draft") if decision else "draft",
                "note": decision.get("note") if decision else None,
                "override_reason": decision.get("override_reason") if decision else None,
                "actor_id": decision.get("actor_id") if decision else "local_actor",
                "updated_at": decision.get("updated_at") if decision else None,
            }
            if isinstance(candidate.get("investment_lens"), dict):
                advanced_pack = dict(candidate["investment_lens"].get("advanced_pack") or {})
                advanced_pack["decision_state"] = candidate["decision_state"]
                advanced_pack["decision_history"] = decision_events
                candidate["investment_lens"]["advanced_pack"] = advanced_pack

    citation_health = summarize_citation_health(conn, list(dedup_citations.values()), settings=settings, force_refresh=False)
    data_quality = summarize_blueprint_data_quality(
        candidates=all_candidates,
        citations=list(dedup_citations.values()),
        regime_context=regime_context,
        settings=settings,
        now=now,
    )
    data_quality["broken_citation_count"] = int(dict(citation_health.get("counts") or {}).get("broken") or 0)
    for sleeve in sleeves:
        for candidate in list(sleeve.get("candidates") or []):
            if isinstance(candidate.get("investment_lens"), dict):
                advanced_pack = dict(candidate["investment_lens"].get("advanced_pack") or {})
                advanced_pack["citation_health"] = {
                    "overall_status": citation_health.get("overall_status"),
                    "counts": citation_health.get("counts"),
                    "retention_counts": citation_health.get("retention_counts"),
                    "hashed_documents_count": citation_health.get("hashed_documents_count"),
                }
                advanced_pack["data_quality"] = {
                    "freshness": data_quality.get("freshness"),
                    "confidence": data_quality.get("confidence"),
                    "quarantined_metrics_count": data_quality.get("quarantined_metrics_count"),
                    "fallback_metrics_count": data_quality.get("fallback_metrics_count"),
                    "exclusions": data_quality.get("exclusions"),
                    "banner": data_quality.get("banner"),
                }
                candidate["investment_lens"]["advanced_pack"] = advanced_pack

    quality_scores: list[dict[str, Any]] = []
    sleeve_recommendations: list[dict[str, Any]] = []
    score_models = list_score_models()
    source_state_counts: dict[str, int] = {}
    display_source_state_counts: dict[str, int] = {}
    object_type_counts: dict[str, int] = {}
    score_mode_counts: dict[str, int] = {}
    action_readiness_counts: dict[str, int] = {}
    evidence_bucket_counts: dict[str, dict[str, int]] = {}
    truth_quality_state_counts: dict[str, int] = {}
    static_candidate_count = 0
    stale_live_candidate_count = 0
    broken_source_candidate_count = 0
    for sleeve in sleeves:
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        candidates = list(sleeve.get("candidates") or [])
        for candidate in candidates:
            candidate["sleeve_key"] = sleeve_key
            candidate_history = recommendation_history_map.get(f"{sleeve_key}::{str(candidate.get('symbol') or '').upper()}", [])
            benchmark_assignment = resolve_benchmark_assignment(
                conn,
                candidate=candidate,
                sleeve_key=sleeve_key,
            )
            performance_metrics = get_latest_performance_metrics(
                str(candidate.get("symbol") or ""),
                conn,
                candidate={**candidate, "benchmark_assignment": benchmark_assignment},
            )
            benchmark_assignment = validate_benchmark_assignment(benchmark_assignment, performance_metrics)
            benchmark_assignment = enrich_benchmark_assignment(benchmark_assignment, sleeve_key=sleeve_key)
            upsert_candidate_benchmark_assignment(
                conn,
                sleeve_key=sleeve_key,
                candidate_symbol=str(candidate.get("symbol") or ""),
                assignment=benchmark_assignment,
            )
            candidate["benchmark_assignment"] = benchmark_assignment
            candidate["aum_history_summary"] = get_etf_factsheet_history_summary(str(candidate.get("symbol") or ""), conn)
            candidate["market_history_summary"] = get_preferred_market_history_summary(
                str(candidate.get("symbol") or ""),
                conn,
            )
            candidate["latest_fetch_status"] = get_latest_etf_fetch_status(str(candidate.get("symbol") or ""), conn)
            if performance_metrics is not None:
                performance_metrics = {
                    **performance_metrics,
                    "benchmark_validation_status": benchmark_assignment.get("validation_status"),
                    "benchmark_source": benchmark_assignment.get("benchmark_source"),
                }
            candidate["performance_metrics"] = performance_metrics
            truth_state = _candidate_truth_state(candidate, settings=settings, now=now)
            candidate.update(truth_state)
            candidate["static_candidate_data"] = bool(truth_state.get("static_candidate_data"))
            field_truth = _upsert_candidate_truth_observations(
                conn=conn,
                candidate=candidate,
                sleeve_key=sleeve_key,
                benchmark_assignment=benchmark_assignment,
                performance_metrics=performance_metrics,
                truth_state=truth_state,
                now=now,
            )
            candidate["field_truth"] = field_truth
            resolved_overrides = {
                "isin": field_truth.get("isin", {}).get("resolved_value"),
                "trading_currency": field_truth.get("primary_trading_currency", {}).get("resolved_value"),
                "aum_usd": field_truth.get("aum", {}).get("resolved_value"),
                "us_weight_pct": field_truth.get("us_weight", {}).get("resolved_value"),
                "em_weight_pct": field_truth.get("em_weight", {}).get("resolved_value"),
                "top10_concentration_pct": field_truth.get("top_10_concentration", {}).get("resolved_value"),
                "holdings_count": field_truth.get("holdings_count", {}).get("resolved_value"),
                "tech_weight_pct": field_truth.get("sector_concentration_proxy", {}).get("resolved_value"),
                "volume_30d_avg": field_truth.get("volume_30d_avg", {}).get("resolved_value"),
                "tracking_difference_1y": field_truth.get("tracking_difference_1y", {}).get("resolved_value"),
                "tracking_difference_3y": field_truth.get("tracking_difference_3y", {}).get("resolved_value"),
            }
            for key, value in resolved_overrides.items():
                if value is not None and value not in {"", "unknown"}:
                    candidate[key] = value
            eligibility = evaluate_candidate_eligibility(
                candidate=candidate,
                sleeve_key=sleeve_key,
                settings=settings,
                now=now,
                candidate_history=candidate_history,
            )
            candidate["eligibility"] = eligibility
            candidate["data_completeness"] = _build_data_completeness(candidate, sleeve_key=sleeve_key, conn=conn, now=now)
            candidate["field_truth_surface"] = _build_field_truth_surface(candidate)
            candidate["sleeve_expression"] = evaluate_sleeve_expression(
                candidate=candidate,
                sleeve_key=sleeve_key,
                benchmark_assignment=benchmark_assignment,
                pressures=list(eligibility.get("pressures") or []),
                readiness_level=str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "research_visible"),
            )
            quality = build_investment_quality_score(
                candidate=candidate,
                sleeve_key=sleeve_key,
                sleeve_candidates=candidates,
                eligibility=eligibility,
                performance_metrics=performance_metrics,
                settings=settings,
            )
            candidate["investment_quality"] = quality
            candidate["score_honesty"] = _build_score_honesty_view(candidate, sleeve_candidates=candidates)
            candidate["tax_truth"] = _candidate_tax_truth_profile(candidate)
            candidate["evidence_buckets"] = _candidate_evidence_buckets(candidate)
            candidate["evidence_depth_class"] = _candidate_evidence_depth_class(candidate)
            candidate["bucket_support"] = _build_bucket_support(candidate)
            upstream_truth_contract = build_candidate_upstream_truth_contract(candidate)
            candidate["upstream_truth_contract"] = upstream_truth_contract
            candidate["source_directness"] = upstream_truth_contract.get("directness_class")
            candidate["coverage_class"] = upstream_truth_contract.get("coverage_class")
            candidate["authority_class"] = upstream_truth_contract.get("authority_class")
            candidate["fallback_state"] = upstream_truth_contract.get("fallback_state")
            candidate["claim_limit_class"] = upstream_truth_contract.get("claim_limit_class")
            candidate["evidence_density_class"] = upstream_truth_contract.get("evidence_density_class")
            # Attach policy authority record
            _quality = dict(candidate.get("investment_quality") or {})
            _policy_record = build_policy_authority_record(
                directness_class=str(upstream_truth_contract.get("directness_class") or "unknown"),
                authority_class=str(upstream_truth_contract.get("authority_class") or "support_grade"),
                fallback_state=str(upstream_truth_contract.get("fallback_state") or "none"),
                claim_limit_class=str(upstream_truth_contract.get("claim_limit_class") or "review_only"),
                coverage_class=str(upstream_truth_contract.get("coverage_class") or "missing"),
                evidence_density_class=str(upstream_truth_contract.get("evidence_density_class") or "thin"),
                benchmark_support_class=str(upstream_truth_contract.get("benchmark_support_class") or "unknown"),
                sleeve_key=sleeve_key,
                eligibility_state=str(_quality.get("eligibility_state") or "data_incomplete"),
                readiness_level=str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "research_visible"),
                data_quality_state=str(dict(candidate.get("decision_record") or {}).get("data_quality_state") or ""),
            )
            candidate["policy_authority_grade"] = _policy_record.get("policy_authority_grade")
            candidate["policy_action_class"] = _policy_record.get("policy_action_class")
            candidate["policy_allowed_actions"] = _policy_record.get("policy_allowed_actions")
            candidate["policy_blocked_actions"] = _policy_record.get("policy_blocked_actions")
            candidate["policy_restriction_codes"] = _policy_record.get("policy_restriction_codes")
            candidate["policy_benchmark_authority"] = _policy_record.get("policy_benchmark_authority")
            candidate["policy_replacement_authority"] = _policy_record.get("policy_replacement_authority")
            candidate["policy_truth_limit_summary"] = _policy_record.get("policy_truth_limit_summary")
            candidate["policy_escalation_allowed"] = bool(_policy_record.get("policy_escalation_allowed", True))
            candidate["policy_authority"] = {
                "policy_authority_grade": str(_policy_record.get("policy_authority_grade") or ""),
                "policy_action_class": str(_policy_record.get("policy_action_class") or ""),
                "policy_allowed_actions": list(_policy_record.get("policy_allowed_actions") or []),
                "policy_blocked_actions": list(_policy_record.get("policy_blocked_actions") or []),
                "policy_restriction_codes": list(_policy_record.get("policy_restriction_codes") or []),
                "policy_benchmark_authority": str(_policy_record.get("policy_benchmark_authority") or ""),
                "policy_replacement_authority": str(_policy_record.get("policy_replacement_authority") or ""),
                "policy_truth_limit_summary": str(_policy_record.get("policy_truth_limit_summary") or ""),
                "policy_escalation_allowed": bool(_policy_record.get("policy_escalation_allowed", True)),
            }
            display_source_state = str(candidate.get("display_source_state") or candidate.get("source_state") or "unknown")
            source_directness = str(candidate.get("source_directness") or "unknown")
            candidate["display_source_state"] = precise_source_state(display_source_state, source_directness)
            for bucket_name, bucket in dict(candidate.get("evidence_buckets") or {}).items():
                bucket_counts = evidence_bucket_counts.setdefault(str(bucket_name), {})
                bucket_state = str(dict(bucket).get("state") or "missing")
                bucket_counts[bucket_state] = bucket_counts.get(bucket_state, 0) + 1
            candidate["truth_quality_summary"] = _candidate_truth_quality_summary(candidate)
            truth_quality_state = str(dict(candidate.get("truth_quality_summary") or {}).get("overall_state") or "truth_gap")
            truth_quality_state_counts[truth_quality_state] = truth_quality_state_counts.get(truth_quality_state, 0) + 1
            candidate["decision_record"] = build_blueprint_decision_record(
                candidate=candidate,
                sleeve_key=sleeve_key,
                evaluation_mode=evaluation_mode,
            )
            candidate["benchmark_dependency_diagnostics"] = _build_benchmark_dependency_diagnostics(candidate)
            candidate["usability_memo"] = _candidate_operational_usability(candidate, sleeve_key=sleeve_key)
            candidate["action_readiness"] = str(dict(candidate.get("usability_memo") or {}).get("state") or "review_only")
            candidate["investor_consequence_summary"] = build_investor_consequence_summary(
                sleeve_expression=dict(candidate.get("sleeve_expression") or {}),
                benchmark_assignment=benchmark_assignment,
                pressures=list(eligibility.get("pressures") or []),
                readiness_level=str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "research_visible"),
            )
            candidate["investment_quality"] = quality
            source_state = str(truth_state.get("source_state") or "unknown")
            object_type = str(candidate.get("object_type") or LIVE_OBJECT_TYPE)
            source_state_count_key = normalize_source_state_base(source_state)
            display_source_state_count_key = str(truth_state.get("display_source_state") or source_state or "unknown")
            source_state_counts[source_state_count_key] = source_state_counts.get(source_state_count_key, 0) + 1
            display_source_state_counts[display_source_state_count_key] = display_source_state_counts.get(display_source_state_count_key, 0) + 1
            object_type_counts[object_type] = object_type_counts.get(object_type, 0) + 1
            score_mode_key = str(truth_state.get("score_mode") or "unknown")
            score_mode_counts[score_mode_key] = score_mode_counts.get(score_mode_key, 0) + 1
            readiness_key = str(candidate.get("action_readiness") or "review_only")
            action_readiness_counts[readiness_key] = action_readiness_counts.get(readiness_key, 0) + 1
            if bool(truth_state.get("static_candidate_data")):
                static_candidate_count += 1
            if source_state == STALE_LIVE_STATE:
                stale_live_candidate_count += 1
            if source_state == BROKEN_SOURCE_STATE:
                broken_source_candidate_count += 1
            if isinstance(candidate.get("investment_lens"), dict):
                candidate["investment_lens"]["trust_state"] = {
                    "source_state": truth_state.get("source_state"),
                    "display_source_state": truth_state.get("display_source_state"),
                    "freshness_state": truth_state.get("freshness_state"),
                    "source_directness": candidate.get("source_directness"),
                    "authority_class": candidate.get("authority_class"),
                    "fallback_state": candidate.get("fallback_state"),
                    "claim_limit_class": candidate.get("claim_limit_class"),
                    "score_mode": truth_state.get("score_mode"),
                    "score_mode_label": truth_state.get("score_mode_label"),
                    "performance_support_state": truth_state.get("performance_support_state"),
                    "action_readiness": candidate.get("action_readiness"),
                    "source_state_note": truth_state.get("source_state_note"),
                    "manual_provenance_note": candidate.get("manual_provenance_note"),
                }
                candidate["investment_lens"]["quality_score"] = quality
                candidate["investment_lens"]["eligibility"] = eligibility
                candidate["investment_lens"]["data_completeness"] = candidate.get("data_completeness")
                candidate["investment_lens"]["decision_record"] = candidate.get("decision_record")
                candidate["investment_lens"]["usability_memo"] = candidate.get("usability_memo")
                advanced_pack = dict(candidate["investment_lens"].get("advanced_pack") or {})
                advanced_pack["quality_score"] = quality
                advanced_pack["eligibility"] = eligibility
                advanced_pack["trust_state"] = candidate["investment_lens"]["trust_state"]
                advanced_pack["latest_fetch_status"] = candidate.get("latest_fetch_status")
                advanced_pack["data_completeness"] = candidate.get("data_completeness")
                advanced_pack["field_truth_surface"] = candidate.get("field_truth_surface")
                advanced_pack["score_honesty"] = candidate.get("score_honesty")
                advanced_pack["tax_truth"] = candidate.get("tax_truth")
                advanced_pack["evidence_depth_class"] = candidate.get("evidence_depth_class")
                advanced_pack["bucket_support"] = candidate.get("bucket_support")
                advanced_pack["upstream_truth_contract"] = candidate.get("upstream_truth_contract")
                advanced_pack["source_directness"] = candidate.get("source_directness")
                advanced_pack["coverage_class"] = candidate.get("coverage_class")
                advanced_pack["authority_class"] = candidate.get("authority_class")
                advanced_pack["fallback_state"] = candidate.get("fallback_state")
                advanced_pack["claim_limit_class"] = candidate.get("claim_limit_class")
                advanced_pack["evidence_density_class"] = candidate.get("evidence_density_class")
                advanced_pack["truth_quality_summary"] = candidate.get("truth_quality_summary")
                advanced_pack["benchmark_dependency_diagnostics"] = candidate.get("benchmark_dependency_diagnostics")
                advanced_pack["decision_record"] = candidate.get("decision_record")
                advanced_pack["usability_memo"] = candidate.get("usability_memo")
                candidate["investment_lens"]["advanced_pack"] = advanced_pack
        ranked_candidates, sleeve_recommendation = rank_sleeve_candidates(
            sleeve_key=sleeve_key,
            candidates=candidates,
        )
        for candidate in ranked_candidates:
            candidate["score_honesty"] = _build_score_honesty_view(candidate, sleeve_candidates=ranked_candidates)
            candidate["decision_readiness"] = _decision_readiness_summary(candidate, sleeve_candidates=ranked_candidates)
            current_snapshot = build_confidence_snapshot(
                candidate=candidate,
                recommendation_context=dict(candidate.get("recommendation_context") or {}),
            )
            confidence_history = build_confidence_history_summary(
                current_snapshot=current_snapshot,
                candidate_history=recommendation_history_map.get(f"{sleeve_key}::{str(candidate.get('symbol') or '').upper()}", []),
            )
            review_escalation = build_review_escalation(
                pressures=list(dict(candidate.get("eligibility") or {}).get("pressures") or []),
                benchmark_assignment=dict(candidate.get("benchmark_assignment") or {}),
                readiness_level=str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "research_visible"),
                confidence_history=confidence_history,
                recommendation_context=dict(candidate.get("recommendation_context") or {}),
            )
            current_snapshot = build_confidence_snapshot(
                candidate=candidate,
                recommendation_context=dict(candidate.get("recommendation_context") or {}),
                review_escalation=review_escalation,
            )
            confidence_history = build_confidence_history_summary(
                current_snapshot=current_snapshot,
                candidate_history=recommendation_history_map.get(f"{sleeve_key}::{str(candidate.get('symbol') or '').upper()}", []),
            )
            candidate["review_escalation"] = review_escalation
            candidate["confidence_history"] = confidence_history
            candidate["decision_readiness"]["review_escalation_level"] = review_escalation.get("level")
            candidate["decision_readiness"]["review_escalation_summary"] = review_escalation.get("summary")
            candidate["decision_readiness"]["recommendation_stability"] = dict(candidate.get("recommendation_context") or {}).get("stability")
            candidate["decision_readiness"]["investor_consequence_summary"] = dict(candidate.get("investor_consequence_summary") or {}).get("summary")
            candidate["decision_readiness"]["usability_state"] = dict(candidate.get("usability_memo") or {}).get("state")
            candidate["decision_readiness"]["usability_summary"] = dict(candidate.get("usability_memo") or {}).get("summary")
            candidate["upgrade_path"] = _build_upgrade_path(candidate)
            candidate["pressure_fix_mapping"] = _build_pressure_fix_mapping(candidate)
            candidate["decision_readiness"]["upgrade_path"] = candidate.get("upgrade_path")
            if isinstance(candidate.get("investment_lens"), dict):
                candidate["investment_lens"]["decision_readiness"] = candidate.get("decision_readiness")
                advanced_pack = dict(candidate["investment_lens"].get("advanced_pack") or {})
                advanced_pack["decision_readiness"] = candidate.get("decision_readiness")
                advanced_pack["review_escalation"] = review_escalation
                advanced_pack["confidence_history"] = confidence_history
                advanced_pack["recommendation_context"] = candidate.get("recommendation_context")
                advanced_pack["usability_memo"] = candidate.get("usability_memo")
                advanced_pack["upgrade_path"] = candidate.get("upgrade_path")
                advanced_pack["pressure_fix_mapping"] = candidate.get("pressure_fix_mapping")
                candidate["investment_lens"]["advanced_pack"] = advanced_pack
            state = str(dict(candidate.get("investment_quality") or {}).get("recommendation_state") or "research_only")
            reason = (
                "Highest valid score in sleeve after policy and data-quality gates."
                if state == "recommended_primary"
                else "Acceptable substitute that clears the same policy and data-quality gates."
                if state == "recommended_backup"
                else "Candidate remains visible for review but is not an active recommendation."
                if state in {"watchlist_only", "research_only"}
                else "Candidate does not clear the current decision stack."
            )
            candidate["decision_record"] = finalize_decision_record(
                decision_record=dict(candidate.get("decision_record") or {}),
                recommendation_state=state,
                reason=reason,
            )
            candidate["decision_record"]["user_facing_state"] = str(dict(candidate.get("investment_quality") or {}).get("user_facing_state") or "")
            candidate["benchmark_dependency_diagnostics"] = _build_benchmark_dependency_diagnostics(candidate)
            candidate["decision_readiness"]["current_status"] = str(dict(candidate.get("investment_quality") or {}).get("user_facing_state") or state)
            candidate.update(_candidate_universe_support(candidate))
            previous_candidate = _find_payload_candidate(
                previous_payload,
                sleeve_key=sleeve_key,
                symbol=str(candidate.get("symbol") or ""),
            )
            previous_symbols = _sleeve_candidate_symbols(previous_payload, sleeve_key=sleeve_key)
            current_symbols = {str(item.get("symbol") or "").upper() for item in ranked_candidates if str(item.get("symbol") or "").strip()}
            candidate_universe_changed = current_symbols != previous_symbols
            candidate_universe_reason = (
                str(candidate.get("candidate_universe_reason") or "candidate universe changed around the sleeve")
                if candidate_universe_changed
                else str(candidate.get("candidate_universe_reason") or "")
            )
            current_diff_record = {
                "recommendation_state": state,
                "benchmark_fit_type": dict(candidate.get("benchmark_assignment") or {}).get("benchmark_fit_type"),
                "readiness_level": dict(candidate.get("data_completeness") or {}).get("readiness_level"),
                "composite_score": dict(candidate.get("investment_quality") or {}).get("composite_score"),
                "recommendation_confidence": dict(candidate.get("investment_quality") or {}).get("recommendation_confidence"),
                "candidate_universe_changed": candidate_universe_changed,
                "candidate_universe_reason": candidate_universe_reason,
                "rejection_reasons": list(dict(candidate.get("decision_record") or {}).get("rejection_reasons") or []),
            }
            previous_diff_record = None
            if previous_candidate is not None:
                previous_diff_record = {
                    "recommendation_state": dict(previous_candidate.get("investment_quality") or {}).get("recommendation_state"),
                    "benchmark_fit_type": dict(previous_candidate.get("benchmark_assignment") or {}).get("benchmark_fit_type"),
                    "readiness_level": dict(previous_candidate.get("data_completeness") or {}).get("readiness_level"),
                    "composite_score": dict(previous_candidate.get("investment_quality") or {}).get("composite_score"),
                    "recommendation_confidence": dict(previous_candidate.get("investment_quality") or {}).get("recommendation_confidence"),
                    "candidate_universe_changed": candidate_universe_changed,
                    "candidate_universe_reason": candidate_universe_reason,
                    "rejection_reasons": list(dict(previous_candidate.get("decision_record") or {}).get("rejection_reasons") or []),
                }
            candidate["recommendation_diff"] = (
                build_recommendation_diff(previous_diff_record, current_diff_record) if previous_diff_record else None
            )
            if isinstance(candidate.get("investment_lens"), dict):
                advanced_pack = dict(candidate["investment_lens"].get("advanced_pack") or {})
                advanced_pack["recommendation_diff"] = candidate.get("recommendation_diff")
                candidate["investment_lens"]["advanced_pack"] = advanced_pack
        winner_candidate = next(
            (
                item
                for item in ranked_candidates
                if str(dict(item.get("investment_quality") or {}).get("recommendation_state") or "") == "recommended_primary"
            ),
            None,
        )
        for candidate in ranked_candidates:
            state = str(dict(candidate.get("investment_quality") or {}).get("recommendation_state") or "research_only")
            candidate["approval_memo"] = None
            candidate["rejection_memo"] = None
            if state == "recommended_primary":
                candidate["approval_memo"] = {
                    "candidate_id": str(candidate.get("symbol") or ""),
                    "sleeve": sleeve_key,
                    "evaluation_mode": dict(candidate.get("decision_record") or {}).get("evaluation_mode"),
                    "approval_reasons": list(dict(candidate.get("decision_record") or {}).get("approval_reasons") or []),
                    "caution_reasons": list(dict(candidate.get("decision_record") or {}).get("caution_reasons") or []),
                    "final_state": state,
                }
            elif state == "recommended_backup":
                candidate["approval_memo"] = {
                    "candidate_id": str(candidate.get("symbol") or ""),
                    "sleeve": sleeve_key,
                    "evaluation_mode": dict(candidate.get("decision_record") or {}).get("evaluation_mode"),
                    "approval_reasons": list(dict(candidate.get("decision_record") or {}).get("approval_reasons") or []),
                    "caution_reasons": list(dict(candidate.get("decision_record") or {}).get("caution_reasons") or []),
                    "final_state": state,
                }
            candidate["rejection_memo"] = build_rejection_memo(
                candidate,
                winner_candidate,
                {"sleeve_key": sleeve_key, "evaluation_mode": dict(candidate.get("decision_record") or {}).get("evaluation_mode")},
            )
        for candidate in ranked_candidates:
            canonical_pipeline = build_candidate_pipeline(
                candidate=candidate,
                sleeve_key=sleeve_key,
                sleeve_name=str(sleeve.get("name") or sleeve_key),
                sleeve_candidates=ranked_candidates,
                winner_candidate=winner_candidate,
                current_holdings=current_holdings,
            )
            candidate["blueprint_pipeline"] = canonical_pipeline
            candidate["candidate_record"] = canonical_pipeline.get("candidate_record")
            candidate["evidence_pack"] = canonical_pipeline.get("evidence_pack")
            candidate["source_integrity_result"] = canonical_pipeline.get("source_integrity_result")
            candidate["gate_result"] = canonical_pipeline.get("gate_result")
            candidate["review_intensity_decision"] = canonical_pipeline.get("review_intensity_decision")
            candidate["universal_review_result"] = canonical_pipeline.get("universal_review_result")
            candidate["deep_review_result"] = canonical_pipeline.get("deep_review_result")
            candidate["scoring_result"] = canonical_pipeline.get("scoring_result")
            candidate["current_holding_record"] = canonical_pipeline.get("current_holding_record")
            candidate["recommendation_result"] = canonical_pipeline.get("recommendation_result")
            candidate["decision_completeness_status"] = canonical_pipeline.get("decision_completeness_status")
            candidate["portfolio_completeness_status"] = canonical_pipeline.get("portfolio_completeness_status")
            candidate["investor_recommendation_status"] = canonical_pipeline.get("investor_recommendation_status")
            candidate["benchmark_support_status"] = canonical_pipeline.get("benchmark_support_status")
            candidate["gate_summary"] = canonical_pipeline.get("gate_summary")
            candidate["base_promotion_state"] = canonical_pipeline.get("base_promotion_state")
            candidate["lens_assessment"] = canonical_pipeline.get("lens_assessment")
            candidate["lens_fusion_result"] = canonical_pipeline.get("lens_fusion_result")
            candidate["decision_thesis"] = canonical_pipeline.get("decision_thesis")
            candidate["forecast_visual_model"] = canonical_pipeline.get("forecast_visual_model")
            candidate["forecast_defensibility_status"] = canonical_pipeline.get("forecast_defensibility_status")
            candidate["tax_assumption_status"] = canonical_pipeline.get("tax_assumption_status")
            candidate["cost_realism_summary"] = canonical_pipeline.get("cost_realism_summary")
            candidate["portfolio_consequence_summary"] = canonical_pipeline.get("portfolio_consequence_summary")
            candidate["decision_change_set"] = canonical_pipeline.get("decision_change_set")
            candidate["canonical_decision"] = canonical_pipeline.get("canonical_decision")
            candidate["supporting_metadata_summary"] = canonical_pipeline.get("supporting_metadata_summary")
            candidate["memo_result"] = canonical_pipeline.get("memo_result")
            candidate["audit_log_entries"] = canonical_pipeline.get("audit_log_entries")
            candidate["baseline_reference"] = canonical_pipeline.get("baseline_reference")
            upstream_truth_contract = dict(candidate.get("upstream_truth_contract") or {})
            if isinstance(candidate.get("decision_thesis"), dict):
                candidate["decision_thesis"] = _apply_claim_boundary_to_value(
                    dict(candidate.get("decision_thesis") or {}),
                    upstream_truth_contract,
                )
            if isinstance(candidate.get("memo_result"), dict):
                candidate["memo_result"] = _apply_claim_boundary_to_value(
                    dict(candidate.get("memo_result") or {}),
                    upstream_truth_contract,
                )
            if isinstance(candidate.get("investment_lens"), dict):
                advanced_pack = dict(candidate["investment_lens"].get("advanced_pack") or {})
                advanced_pack["candidate_record"] = candidate.get("candidate_record")
                advanced_pack["evidence_pack"] = candidate.get("evidence_pack")
                advanced_pack["source_integrity_result"] = candidate.get("source_integrity_result")
                advanced_pack["gate_result"] = candidate.get("gate_result")
                advanced_pack["review_intensity_decision"] = candidate.get("review_intensity_decision")
                advanced_pack["universal_review_result"] = candidate.get("universal_review_result")
                advanced_pack["deep_review_result"] = candidate.get("deep_review_result")
                advanced_pack["scoring_result"] = candidate.get("scoring_result")
                advanced_pack["decision_completeness_status"] = candidate.get("decision_completeness_status")
                advanced_pack["portfolio_completeness_status"] = candidate.get("portfolio_completeness_status")
                advanced_pack["investor_recommendation_status"] = candidate.get("investor_recommendation_status")
                advanced_pack["benchmark_support_status"] = candidate.get("benchmark_support_status")
                advanced_pack["gate_summary"] = candidate.get("gate_summary")
                advanced_pack["base_promotion_state"] = candidate.get("base_promotion_state")
                advanced_pack["lens_assessment"] = candidate.get("lens_assessment")
                advanced_pack["lens_fusion_result"] = candidate.get("lens_fusion_result")
                advanced_pack["recommendation_result"] = candidate.get("recommendation_result")
                advanced_pack["decision_thesis"] = candidate.get("decision_thesis")
                advanced_pack["evidence_depth_class"] = candidate.get("evidence_depth_class")
                advanced_pack["bucket_support"] = candidate.get("bucket_support")
                advanced_pack["upstream_truth_contract"] = candidate.get("upstream_truth_contract")
                advanced_pack["source_directness"] = candidate.get("source_directness")
                advanced_pack["coverage_class"] = candidate.get("coverage_class")
                advanced_pack["authority_class"] = candidate.get("authority_class")
                advanced_pack["fallback_state"] = candidate.get("fallback_state")
                advanced_pack["claim_limit_class"] = candidate.get("claim_limit_class")
                advanced_pack["evidence_density_class"] = candidate.get("evidence_density_class")
                advanced_pack["forecast_visual_model"] = candidate.get("forecast_visual_model")
                advanced_pack["forecast_defensibility_status"] = candidate.get("forecast_defensibility_status")
                advanced_pack["tax_assumption_status"] = candidate.get("tax_assumption_status")
                advanced_pack["tax_truth"] = candidate.get("tax_truth")
                advanced_pack["cost_realism_summary"] = candidate.get("cost_realism_summary")
                advanced_pack["portfolio_consequence_summary"] = candidate.get("portfolio_consequence_summary")
                advanced_pack["decision_change_set"] = candidate.get("decision_change_set")
                advanced_pack["canonical_decision"] = candidate.get("canonical_decision")
                advanced_pack["supporting_metadata_summary"] = candidate.get("supporting_metadata_summary")
                advanced_pack["memo_result"] = candidate.get("memo_result")
                advanced_pack["audit_log_entries"] = candidate.get("audit_log_entries")
                advanced_pack["baseline_reference"] = candidate.get("baseline_reference")
                candidate["investment_lens"]["advanced_pack"] = advanced_pack
        sleeve["candidates"] = ranked_candidates
        readiness_summary = _sleeve_readiness_summary(sleeve_key, ranked_candidates, sleeve_recommendation)
        no_pick_reason = persist_sleeve_no_pick_reason(
            conn,
            snapshot_id=f"runtime_{sleeve_key}_{now.isoformat()}",
            sleeve_key=sleeve_key,
            candidates=ranked_candidates,
        )
        readiness_summary["no_pick_reason_code"] = no_pick_reason.get("reason_code")
        readiness_summary["blocking_fields"] = list(no_pick_reason.get("blocking_fields") or [])
        sleeve_recommendation["readiness_summary"] = readiness_summary
        no_current_pick_reason = no_pick_reason.get("reason_text") or readiness_summary.get("no_pick_explanation")
        dominant_missing_categories = list(readiness_summary.get("dominant_missing_categories") or [])
        if dominant_missing_categories and "dominant missing categories" not in str(no_current_pick_reason).lower():
            category_suffix = f" Dominant missing categories: {', '.join(dominant_missing_categories)}."
            no_current_pick_reason = f"{str(no_current_pick_reason).rstrip('.')}." + category_suffix
        sleeve_recommendation["no_current_pick_reason"] = no_current_pick_reason
        sleeve_recommendation["nearest_passing_candidate"] = (
            no_pick_reason.get("nearest_passing_candidate") or readiness_summary.get("nearest_passing_candidate")
        )
        sleeve_recommendation["no_pick_reason_code"] = no_pick_reason.get("reason_code")
        sleeve_recommendation["blocking_fields"] = list(no_pick_reason.get("blocking_fields") or [])
        sleeve_recommendation["presentation"] = _build_sleeve_recommendation_presentation(
            sleeve,
            candidates=ranked_candidates,
            recommendation=sleeve_recommendation,
            readiness_summary=readiness_summary,
        )
        sleeve["recommendation"] = sleeve_recommendation
        quality_scores.extend(
            [dict(candidate.get("investment_quality") or {}) for candidate in ranked_candidates if isinstance(candidate.get("investment_quality"), dict)]
        )
        sleeve_recommendations.append(sleeve_recommendation)

        for candidate in list(sleeve.get("policy_placeholders") or []) + list(sleeve.get("strategy_placeholders") or []):
            truth_state = _candidate_truth_state(candidate, settings=settings, now=now)
            candidate.update(truth_state)
            candidate["static_candidate_data"] = bool(truth_state.get("static_candidate_data"))
            source_state = str(truth_state.get("source_state") or "unknown")
            object_type = str(candidate.get("object_type") or POLICY_PLACEHOLDER_TYPE)
            source_state_count_key = normalize_source_state_base(source_state)
            display_source_state_count_key = str(truth_state.get("display_source_state") or source_state or "unknown")
            source_state_counts[source_state_count_key] = source_state_counts.get(source_state_count_key, 0) + 1
            display_source_state_counts[display_source_state_count_key] = display_source_state_counts.get(display_source_state_count_key, 0) + 1
            object_type_counts[object_type] = object_type_counts.get(object_type, 0) + 1
            score_mode_key = str(truth_state.get("score_mode") or "unknown")
            score_mode_counts[score_mode_key] = score_mode_counts.get(score_mode_key, 0) + 1
            readiness_key = str(truth_state.get("action_readiness") or "review_only")
            action_readiness_counts[readiness_key] = action_readiness_counts.get(readiness_key, 0) + 1

    replacement_opportunities = build_replacement_opportunities(sleeves=sleeves)
    recommendation_summary = {
        "eligible_count": sum(
            1
            for candidate in quality_scores
            if str(candidate.get("eligibility_state") or "") in {"eligible", "eligible_with_caution"}
        ),
        "best_in_class_count": sum(1 for candidate in quality_scores if str(candidate.get("badge") or "") == "best_in_class"),
        "recommended_count": sum(
            1
            for candidate in quality_scores
            if str(candidate.get("recommendation_state") or "") in {"recommended_primary", "recommended_backup"}
        ),
        "recommended_primary_count": sum(
            1 for candidate in quality_scores if str(candidate.get("recommendation_state") or "") == "recommended_primary"
        ),
        "recommended_backup_count": sum(
            1 for candidate in quality_scores if str(candidate.get("recommendation_state") or "") == "recommended_backup"
        ),
        "watchlist_only_count": sum(1 for candidate in quality_scores if str(candidate.get("recommendation_state") or "") == "watchlist_only"),
        "research_only_count": sum(1 for candidate in quality_scores if str(candidate.get("recommendation_state") or "") == "research_only"),
        "rejected_policy_failure_count": sum(
            1 for candidate in quality_scores if str(candidate.get("recommendation_state") or "") == "rejected_policy_failure"
        ),
        "rejected_data_insufficient_count": sum(
            1 for candidate in quality_scores if str(candidate.get("recommendation_state") or "") in {"rejected_data_insufficient", "blocked_by_missing_required_evidence"}
        ),
        "blocked_unresolved_gate_count": sum(
            1 for candidate in quality_scores if str(candidate.get("recommendation_state") or "") == "blocked_by_unresolved_gate"
        ),
        "fully_clean_recommendable_count": sum(
            1 for candidate in quality_scores if str(candidate.get("user_facing_state") or "") == "fully_clean_recommendable"
        ),
        "best_available_with_limits_count": sum(
            1 for candidate in quality_scores if str(candidate.get("user_facing_state") or "") == "best_available_with_limits"
        ),
        "research_ready_but_not_recommendable_count": sum(
            1 for candidate in quality_scores if str(candidate.get("user_facing_state") or "") == "research_ready_but_not_recommendable"
        ),
        "actionable_recommendation_count": sum(
            1
            for sleeve in sleeves
            for candidate in list(sleeve.get("candidates") or [])
            if str(dict(candidate.get("recommendation_result") or {}).get("recommendation_tier") or "") == "actionable"
        ),
        "no_change_is_best_count": sum(
            1
            for sleeve in sleeves
            for candidate in list(sleeve.get("candidates") or [])
            if bool(dict(candidate.get("recommendation_result") or {}).get("no_change_is_best"))
        ),
        "current_holding_comparable_count": sum(
            1
            for sleeve in sleeves
            for candidate in list(sleeve.get("candidates") or [])
            if str(dict(candidate.get("current_holding_record") or {}).get("status") or "") in {"matched_to_current", "different_from_current"}
        ),
        "replacement_opportunity_count": len(replacement_opportunities),
        "score_versions": sorted({str(candidate.get("score_version") or "") for candidate in quality_scores if str(candidate.get("score_version") or "").strip()}),
    }

    score_models_active = sorted(score_mode_counts)
    truth_banner = None
    if not sleeves and not settings.blueprint_allow_legacy_inline_fallback:
        truth_banner = (
            "Blueprint candidate registry is empty and legacy inline fallback is disabled. "
            "Candidate ranking is blocked until registry-backed candidates are available."
        )
    elif static_candidate_count > 0:
        truth_banner = (
            f"Blueprint is operating on manual seeded candidate data for {static_candidate_count} "
            f"candidate{'s' if static_candidate_count != 1 else ''}. Review only until ingest refresh validates issuer and market fields."
        )
    elif stale_live_candidate_count > 0:
        truth_banner = (
            f"{stale_live_candidate_count} live candidate{'s are' if stale_live_candidate_count != 1 else ' is'} "
            "stale. Recommendations are partially withheld until refresh succeeds."
        )
    elif broken_source_candidate_count > 0:
        truth_banner = (
            f"{broken_source_candidate_count} source-linked candidate{'s have' if broken_source_candidate_count != 1 else ' has'} "
            "persistent refresh failures. Review only until source health recovers."
        )
    if refresh_monitor.get("alert_banner"):
        truth_banner = f"{truth_banner} {refresh_monitor.get('alert_banner')}".strip() if truth_banner else str(refresh_monitor.get("alert_banner"))

    candidate_readiness_counts: dict[str, int] = {}
    pipeline_candidate_status_counts: dict[str, int] = {}
    pipeline_recommendation_tier_counts: dict[str, int] = {}
    pipeline_review_intensity_counts: dict[str, int] = {}
    provider_coverage_counts = {
        "source_backed_current": 0,
        "source_backed_aging": 0,
        "source_backed_stale": 0,
        "fallback_current": 0,
        "fallback_stale": 0,
        "partial": 0,
        "missing_fetchable": 0,
        "missing_source_gap": 0,
    }
    for sleeve in sleeves:
        for candidate in list(sleeve.get("candidates") or []):
            readiness = str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "unknown")
            candidate_readiness_counts[readiness] = candidate_readiness_counts.get(readiness, 0) + 1
            recommendation_result = dict(candidate.get("recommendation_result") or {})
            candidate_status = str(recommendation_result.get("candidate_status") or "unknown")
            pipeline_candidate_status_counts[candidate_status] = pipeline_candidate_status_counts.get(candidate_status, 0) + 1
            recommendation_tier = str(recommendation_result.get("recommendation_tier") or "unknown")
            pipeline_recommendation_tier_counts[recommendation_tier] = (
                pipeline_recommendation_tier_counts.get(recommendation_tier, 0) + 1
            )
            review_intensity = str(dict(candidate.get("review_intensity_decision") or {}).get("review_intensity") or "unknown")
            pipeline_review_intensity_counts[review_intensity] = pipeline_review_intensity_counts.get(review_intensity, 0) + 1
            source_state = str(candidate.get("source_state") or "").lower()
            if source_state == SOURCE_VALIDATED_STATE:
                provider_coverage_counts["source_backed_current"] += 1
            elif source_state == AGING_STATE:
                provider_coverage_counts["source_backed_aging"] += 1
            elif source_state == STALE_LIVE_STATE:
                provider_coverage_counts["source_backed_stale"] += 1
            elif source_state == BROKEN_SOURCE_STATE:
                provider_coverage_counts["partial"] += 1
            else:
                provider_coverage_counts["missing_source_gap"] += 1

    payload = {
        "blueprint_meta": {
            "version": "2026-03-02",
            "payload_integrity_version": BLUEPRINT_PAYLOAD_INTEGRITY_VERSION,
            "required_detail_fields": list(BLUEPRINT_REQUIRED_DETAIL_FIELDS),
            "generated_at": now.isoformat(),
            "last_verified_at": max(factsheet_dates) if factsheet_dates else now.date().isoformat(),
            "base_currency": "SGD",
            "profile_type": str(settings.blueprint_profile_type),
            "domicile_preference": "IE UCITS",
            "accumulation_preference": True,
            "default_investor_profile": "SGD base, accumulating IE UCITS preference, limited China satellite exposure",
            "verification_summary": {
                "verified_count": verified,
                "partially_verified_count": partial,
                "unverified_count": unverified,
                "total_candidates": total_candidates,
                "data_trust_level": trust_level,
                "what_changed_since_last_blueprint": "No prior baseline",
                "factsheet_max_age_days": freshness_days_threshold,
            },
            "ips_linkage": {
                "profile_id": ips_profile.profile_id,
                "ips_version": ips_version_token(ips_profile),
                "updated_at": ips_profile.updated_at.isoformat() if getattr(ips_profile, "updated_at", None) else None,
                "governing_policy": "IPS",
            },
            "truth_summary": {
                "source_state_counts": source_state_counts,
                "display_source_state_counts": display_source_state_counts,
                "object_type_counts": object_type_counts,
                "score_mode_counts": score_mode_counts,
                "action_readiness_counts": action_readiness_counts,
                "static_candidate_count": static_candidate_count,
                "stale_live_candidate_count": stale_live_candidate_count,
                "broken_source_candidate_count": broken_source_candidate_count,
                "manual_seed_candidate_count": static_candidate_count,
                "policy_placeholder_count": object_type_counts.get(POLICY_PLACEHOLDER_TYPE, 0),
                "strategy_placeholder_count": object_type_counts.get(STRATEGY_PLACEHOLDER_TYPE, 0),
                "score_models_active": score_models_active,
                "banner": truth_banner,
            },
            "candidate_registry": {
                "active_candidate_count": len(export_candidate_registry(conn)),
                "backing_store": "blueprint_candidate_registry",
                "manual_override_policy": "Manual provenance note required for registry changes.",
                "legacy_inline_fallback_enabled": bool(settings.blueprint_allow_legacy_inline_fallback),
                "legacy_inline_fallback_used": bool(using_legacy_inline_fallback),
                "candidate_truth_summary": {
                    "readiness_counts": candidate_readiness_counts,
                    "provider_coverage_counts": provider_coverage_counts,
                    "review_ready_count": candidate_readiness_counts.get("review_ready", 0),
                    "shortlist_ready_count": candidate_readiness_counts.get("shortlist_ready", 0),
                    "recommendation_ready_count": candidate_readiness_counts.get("recommendation_ready", 0),
                    "blocked_count": provider_coverage_counts.get("missing_source_gap", 0) + provider_coverage_counts.get("partial", 0),
                },
            },
            "refresh_monitor": refresh_monitor,
            "rebalance_summary": rebalance_status_counts,
            "data_quality": data_quality,
            "citation_health": {
                "overall_status": citation_health.get("overall_status"),
                "counts": citation_health.get("counts"),
                "retention_counts": citation_health.get("retention_counts"),
                "hashed_documents_count": citation_health.get("hashed_documents_count"),
                "last_checked_at": max(
                    [str(item.get("last_checked_at") or "") for item in list(citation_health.get("entries") or []) if str(item.get("last_checked_at") or "").strip()]
                    or [""]
                )
                or None,
            },
            "regime_transition_context": get_regime_transition_context(),
            "recommendation_summary": recommendation_summary,
            "score_models": score_models,
            "benchmark_registry": build_benchmark_registry_summary(conn, sleeves=sleeves),
            "external_upstreams": build_cached_external_upstream_payload(conn, settings, surface_name="blueprint"),
            "source_truth_registry": truth_family_registry(),
            "truth_quality": {
                "evidence_bucket_counts": evidence_bucket_counts,
                "candidate_truth_quality_state_counts": truth_quality_state_counts,
                "recommendation_semantics_mode": "bucket_governed_semantics",
            },
            "portfolio_state": portfolio_state_context,
            "evaluation_mode": evaluation_mode,
            "architecture": {
                "target_layers": [
                    "candidate_sourcing_and_evidence_assembly",
                    "source_integrity_and_policy_checks",
                    "mandate_and_sleeve_framework_assessment",
                    "review_intensity_engine",
                    "scoring_and_portfolio_impact_assessment",
                    "recommendation_engine",
                    "memo_and_explanation_engine",
                ],
                "module_classification": classify_blueprint_modules(),
                "pipeline_summary": {
                    "candidate_status_counts": pipeline_candidate_status_counts,
                    "recommendation_tier_counts": pipeline_recommendation_tier_counts,
                    "review_intensity_counts": pipeline_review_intensity_counts,
                },
            },
            "portfolio_governance": _build_portfolio_governance_summary(sleeves=sleeves, previous_payload=previous_payload),
        },
        "sleeves": sleeves,
        "citations": list(dedup_citations.values()),
        "replacement_opportunities": replacement_opportunities,
        "label": "Candidates are for implementation review, not directives.",
    }
    payload["blueprint_meta"]["deliverable_candidates"] = build_deliverable_candidates(
        current_payload=payload,
        previous_payload=previous_payload,
    )
    payload["blueprint_meta"]["deliverable_candidates_diff"] = build_deliverable_candidates_diff(
        current_payload=payload,
        previous_payload=previous_payload,
    )
    payload["blueprint_meta"]["candidate_universe"] = build_candidate_universe(
        current_payload=payload,
        previous_payload=previous_payload,
    )
    payload["blueprint_meta"]["candidate_universe_diff"] = build_candidate_universe_diff(
        current_payload=payload,
        previous_payload=previous_payload,
    )
    try:
        persist_blueprint_runtime_cycle(conn, blueprint_payload=payload)
    except Exception:
        pass
    conn.close()
    return payload
