from __future__ import annotations

from collections import Counter
from typing import Any


TRUTH_GRADE = "truth_grade"
SUPPORT_GRADE = "support_grade"
OPS_ONLY = "ops_only"
STRUCTURE_FIRST = "structure_first"
PROXY_ONLY = "proxy_only"
RESEARCH_ONLY = "research_only"

_SOURCE_STATE_PREFIXES = (
    "source_validated_",
    "aging_",
)


def classify_directness(
    source_class: str | None,
    *,
    candidate_type: str | None = None,
    structure_first: bool = False,
) -> str:
    source = str(source_class or "").strip().lower()
    if structure_first or str(candidate_type or "").strip().lower() in {"managed_futures_fund", "tail_hedge_etf"}:
        return "structure_first"
    if source in {"direct_holdings", "issuer_holdings_primary"}:
        return "direct_holdings_backed"
    if source in {"factsheet_summary", "issuer_factsheet_secondary"}:
        return "issuer_structured_summary_backed"
    if source in {"html_summary", "issuer_html_summary"}:
        return "html_summary_backed"
    if source in {"fallback_or_proxy", "proxy", "proxy_only_last_resort"}:
        return "proxy_backed"
    if source in {"support_only", "verified_third_party_fallback"}:
        return "support_only"
    if source in {"ops_only"}:
        return "ops_only"
    return "unknown"


def classify_authority(
    directness_class: str,
    *,
    freshness_state: str | None = None,
    fallback_state: str | None = None,
    structure_first: bool = False,
    support_only: bool = False,
    ops_only: bool = False,
    research_only: bool = False,
) -> str:
    if ops_only:
        return OPS_ONLY
    if research_only:
        return RESEARCH_ONLY
    if structure_first or directness_class == "structure_first":
        return STRUCTURE_FIRST
    if support_only or directness_class == "support_only":
        return SUPPORT_GRADE
    if directness_class == "proxy_backed":
        return PROXY_ONLY
    if fallback_state not in {"", "none", "native", "not_applicable", None}:
        return SUPPORT_GRADE
    if freshness_state in {"stale", "quarantined"}:
        return SUPPORT_GRADE
    if directness_class in {
        "direct_holdings_backed",
        "issuer_structured_summary_backed",
        "html_summary_backed",
    }:
        return TRUTH_GRADE
    return SUPPORT_GRADE


def classify_claim_limit(
    directness_class: str,
    authority_class: str,
    *,
    freshness_state: str | None = None,
    benchmark_support_class: str | None = None,
    portfolio_mapping_class: str | None = None,
) -> str:
    freshness = str(freshness_state or "").strip().lower()
    benchmark_support = str(benchmark_support_class or "").strip().lower()
    portfolio_mapping = str(portfolio_mapping_class or "").strip().lower()
    if authority_class in {OPS_ONLY, RESEARCH_ONLY}:
        return "research_only"
    if authority_class == PROXY_ONLY:
        return "review_only"
    if directness_class in {"proxy_backed", "support_only"}:
        return "review_only"
    if freshness in {"stale", "quarantined"}:
        return "review_only"
    if benchmark_support in {"acceptable_proxy", "weak_proxy", "proxy_only", "insufficient", "proxy"}:
        return "benchmark_limited"
    if portfolio_mapping in {"proxy_only", "target_proxy", "macro_only"}:
        return "portfolio_mapping_limited"
    if directness_class == "html_summary_backed":
        return "summary_limited"
    if directness_class == "issuer_structured_summary_backed":
        return "summary_limited"
    if directness_class == "structure_first":
        return "structure_first_limited"
    if directness_class == "direct_holdings_backed":
        return "full_support"
    return "review_only"


def normalize_source_state_base(source_state: str | None) -> str:
    state = str(source_state or "").strip().lower()
    if not state:
        return "unknown"
    for prefix in _SOURCE_STATE_PREFIXES:
        if state.startswith(prefix):
            return prefix[:-1]
    if state.startswith("refresh_failed_using_last_validated"):
        return "source_validated"
    if state.startswith("refresh_failed_using_aging_validated"):
        return "aging"
    return state


def precise_source_state(source_state: str | None, directness_class: str | None) -> str:
    base_state = normalize_source_state_base(source_state)
    directness = str(directness_class or "").strip().lower()
    if base_state not in {"source_validated", "aging"}:
        return base_state
    if directness in {
        "issuer_structured_summary_backed",
        "html_summary_backed",
        "proxy_backed",
        "structure_first",
        "support_only",
    }:
        return f"{base_state}_{directness}"
    return base_state


def classify_coverage_class(
    *,
    field_truth: dict[str, Any] | None,
    evidence_buckets: dict[str, Any] | None,
) -> str:
    fields = dict(field_truth or {})
    buckets = dict(evidence_buckets or {})
    populated = 0
    direct = 0
    summary = 0
    proxy = 0
    for item in fields.values():
        if not isinstance(item, dict):
            continue
        if item.get("missingness_reason") != "populated":
            continue
        populated += 1
        source_class = str(item.get("source_class") or item.get("bucket_source_class") or "").strip().lower()
        if source_class in {"direct_holdings", "issuer_holdings_primary"}:
            direct += 1
        elif source_class in {"factsheet_summary", "html_summary", "issuer_factsheet_secondary"}:
            summary += 1
        elif source_class:
            proxy += 1
    if populated == 0 and not buckets:
        return "missing"
    if direct >= max(4, populated // 3):
        return "direct_majority"
    if summary > 0 and direct == 0:
        return "summary_majority"
    if proxy > 0 and populated == proxy:
        return "proxy_only"
    return "mixed"


def classify_evidence_density(field_truth: dict[str, Any] | None) -> str:
    fields = dict(field_truth or {})
    populated = sum(
        1
        for item in fields.values()
        if isinstance(item, dict) and str(item.get("missingness_reason") or "") == "populated"
    )
    if populated >= 18:
        return "dense"
    if populated >= 10:
        return "moderate"
    if populated >= 4:
        return "light"
    return "thin"


def infer_candidate_directness(candidate: dict[str, Any]) -> str:
    field_truth = dict(candidate.get("field_truth") or {})
    evidence_buckets = dict(candidate.get("evidence_buckets") or {})
    holdings_profile = dict(candidate.get("holdings_profile") or {})
    benchmark_assignment = dict(candidate.get("benchmark_assignment") or {})
    extra = dict(candidate.get("extra") or {})
    candidate_type = str(candidate.get("instrument_type") or candidate.get("object_type") or "")
    structure_first = bool(candidate.get("structure_first_by_design")) or candidate_type in {
        "managed_futures_fund",
        "tail_hedge_etf",
    } or str(extra.get("bucket") or "") in {"managed_futures", "tail_hedge"}
    if structure_first:
        return "structure_first"
    profile_directness = str(holdings_profile.get("directness_class") or "").strip()
    if profile_directness:
        return profile_directness
    holdings_bucket = dict(evidence_buckets.get("holdings_exposure") or {})
    bucket_directness = classify_directness(
        str(holdings_bucket.get("source_class") or ""),
        candidate_type=candidate_type,
        structure_first=structure_first,
    )
    if bucket_directness != "unknown":
        return bucket_directness
    for field_name in (
        "holdings_count",
        "top_10_concentration",
        "us_weight",
        "em_weight",
        "sector_concentration_proxy",
        "developed_market_exposure_summary",
        "emerging_market_exposure_summary",
        "top_country",
        "distribution_type",
        "share_class",
        "benchmark_key",
    ):
        field = dict(field_truth.get(field_name) or {})
        if str(field.get("missingness_reason") or "") != "populated":
            continue
        directness = classify_directness(
            str(field.get("source_class") or field.get("bucket_source_class") or ""),
            candidate_type=candidate_type,
            structure_first=structure_first,
        )
        if directness != "unknown":
            return directness
    try:
        from app.services.ingest_etf_data import get_etf_source_config
    except Exception:
        get_etf_source_config = None
    if get_etf_source_config is not None:
        source_config = dict(get_etf_source_config(str(candidate.get("symbol") or "")) or {})
        data_sources = dict(source_config.get("data_sources") or {})
        holdings_config = dict(data_sources.get("holdings") or {})
        factsheet_config = dict(data_sources.get("factsheet") or {})
        holdings_parser = str(holdings_config.get("parser_type") or holdings_config.get("parser_path") or "").lower()
        holdings_method = str(holdings_config.get("method") or "").lower()
        if holdings_method == "csv_download" or holdings_parser.endswith("_holdings_csv"):
            return "direct_holdings_backed"
        if holdings_parser in {
            "vanguard_holdings_html_summary",
            "ssga_holdings_html_summary",
            "xtrackers_holdings_html_summary",
            "hsbc_holdings_html_summary",
            "amova_holdings_html_summary",
            "issuer_holdings_pdf_summary",
            "hsbc_holdings_pdf_summary",
        }:
            return "issuer_structured_summary_backed"
        if holdings_parser in {"issuer_holdings_html_summary"}:
            return "html_summary_backed"
        factsheet_parser = str(factsheet_config.get("parser_type") or factsheet_config.get("parser_path") or "").lower()
        if factsheet_parser in {
            "vanguard_factsheet_pdf",
            "ssga_factsheet_pdf",
            "hsbc_factsheet_pdf",
            "xtrackers_factsheet_pdf",
            "issuer_factsheet_pdf",
            "factsheet_api_extract",
        }:
            return "issuer_structured_summary_backed"
        if factsheet_parser == "issuer_factsheet_html":
            return "html_summary_backed"
    benchmark_kind = str(benchmark_assignment.get("benchmark_kind") or "").strip().lower()
    if benchmark_kind == "proxy":
        return "proxy_backed"
    return "unknown"


def build_truth_field_contract(
    *,
    truth_family: str,
    field_name: str,
    value: Any,
    field_truth: dict[str, Any] | None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    field = dict(field_truth or {})
    ctx = dict(context or {})
    source_class = str(field.get("source_class") or field.get("bucket_source_class") or field.get("source_type") or "")
    structure_first = bool(ctx.get("structure_first"))
    directness_class = classify_directness(
        source_class,
        candidate_type=str(ctx.get("candidate_type") or ""),
        structure_first=structure_first,
    )
    fallback_state = str(field.get("fallback_state") or ctx.get("fallback_state") or "none")
    freshness_state = str(field.get("freshness_state") or ctx.get("freshness_state") or "unknown")
    benchmark_support_class = str(ctx.get("benchmark_support_class") or "")
    portfolio_mapping_class = str(ctx.get("portfolio_mapping_class") or "")
    support_only = bool(ctx.get("support_only")) or directness_class == "support_only"
    ops_only = bool(ctx.get("ops_only"))
    research_only = bool(ctx.get("research_only"))
    authority_class = classify_authority(
        directness_class,
        freshness_state=freshness_state,
        fallback_state=fallback_state,
        structure_first=structure_first,
        support_only=support_only,
        ops_only=ops_only,
        research_only=research_only,
    )
    claim_limit_class = classify_claim_limit(
        directness_class,
        authority_class,
        freshness_state=freshness_state,
        benchmark_support_class=benchmark_support_class,
        portfolio_mapping_class=portfolio_mapping_class,
    )
    raw_evidence_refs = []
    source_url = str(field.get("source_url") or "")
    if source_url:
        raw_evidence_refs.append(source_url)
    source_doc = str(field.get("source_doc_id") or "")
    if source_doc:
        raw_evidence_refs.append(source_doc)
    return {
        "truth_family": truth_family,
        "field_name": field_name,
        "value": value,
        "source_family": str(field.get("source_family") or truth_family),
        "source_provider": str(field.get("source_name") or field.get("provider_name") or ""),
        "source_url_or_doc_id": source_url or source_doc or None,
        "fetch_method": str(field.get("fetch_method") or field.get("parser_method") or ""),
        "parser_path": str(field.get("parser_path") or field.get("parser_method") or ""),
        "observed_at": field.get("observed_at") or field.get("as_of") or None,
        "as_of_date": field.get("as_of") or field.get("observed_at") or None,
        "freshness_state": freshness_state,
        "fallback_state": fallback_state,
        "directness_class": directness_class,
        "authority_class": authority_class,
        "completeness_class": str(field.get("completeness_state") or "unknown"),
        "benchmark_support_class": benchmark_support_class or None,
        "portfolio_mapping_class": portfolio_mapping_class or None,
        "claim_limit_class": claim_limit_class,
        "structural_limitations": list(ctx.get("structural_limitations") or []),
        "downgrade_reasons": list(
            item
            for item in [
                *list(field.get("quality_issues") or []),
                *list(field.get("failure_reasons") or []),
                *list(ctx.get("downgrade_reasons") or []),
            ]
            if item
        ),
        "raw_evidence_refs": raw_evidence_refs,
    }


def build_candidate_upstream_truth_contract(candidate: dict[str, Any]) -> dict[str, Any]:
    field_truth = dict(candidate.get("field_truth") or {})
    candidate_type = str(candidate.get("instrument_type") or candidate.get("object_type") or "")
    structure_first = bool(candidate.get("structure_first_by_design")) or candidate_type in {
        "managed_futures_fund",
        "tail_hedge_etf",
    }
    evidence_buckets = dict(candidate.get("evidence_buckets") or {})
    benchmark_assignment = dict(candidate.get("benchmark_assignment") or {})
    directness_class = infer_candidate_directness(candidate)
    fallback_state = "none"
    if str(candidate.get("latest_fetch_status", {}).get("status") or "") == "failed":
        fallback_state = "refresh_failed"
    elif any(
        str(item.get("missingness_reason") or "") in {"blocked_by_parser_gap", "blocked_by_source_gap"}
        for item in field_truth.values()
        if isinstance(item, dict)
    ):
        fallback_state = "partial_source_gap"
    freshness_state = str(candidate.get("freshness_state") or "unknown")
    benchmark_support_class = str(
        benchmark_assignment.get("benchmark_kind")
        or benchmark_assignment.get("benchmark_fit_type")
        or benchmark_assignment.get("benchmark_authority_level")
        or benchmark_assignment.get("benchmark_confidence")
        or "unknown"
    )
    authority_class = classify_authority(
        directness_class,
        freshness_state=freshness_state,
        fallback_state=fallback_state,
        structure_first=structure_first,
    )
    claim_limit_class = classify_claim_limit(
        directness_class,
        authority_class,
        freshness_state=freshness_state,
        benchmark_support_class=benchmark_support_class,
    )
    downgrade_reasons = []
    holdings_profile = dict(candidate.get("holdings_profile") or {})
    if holdings_profile:
        downgrade_reasons.extend(list(holdings_profile.get("quality_issues") or []))
        downgrade_reasons.extend(list(holdings_profile.get("downgrade_reasons") or []))
    if fallback_state != "none":
        downgrade_reasons.append(fallback_state)
    if benchmark_support_class in {"acceptable_proxy", "weak_proxy", "mismatched", "proxy_only"}:
        downgrade_reasons.append(f"benchmark_{benchmark_support_class}")
    canonical_fields: dict[str, dict[str, Any]] = {}
    family_counts: Counter[str] = Counter()
    for field_name, field in field_truth.items():
        if not isinstance(field, dict):
            continue
        family = str(field.get("bucket_name") or field.get("source_family") or "general")
        canonical_fields[field_name] = build_truth_field_contract(
            truth_family=family,
            field_name=field_name,
            value=field.get("resolved_value"),
            field_truth=field,
            context={
                "candidate_type": candidate_type,
                "structure_first": structure_first,
                "freshness_state": freshness_state,
                "fallback_state": fallback_state,
                "benchmark_support_class": benchmark_support_class,
                "structural_limitations": holdings_profile.get("structural_limitations") or [],
                "downgrade_reasons": downgrade_reasons,
            },
        )
        family_counts[family] += 1
    return {
        "symbol": str(candidate.get("symbol") or ""),
        "sleeve_key": str(candidate.get("sleeve_key") or ""),
        "directness_class": directness_class,
        "coverage_class": classify_coverage_class(field_truth=field_truth, evidence_buckets=evidence_buckets),
        "authority_class": authority_class,
        "fallback_state": fallback_state,
        "claim_limit_class": claim_limit_class,
        "evidence_density_class": classify_evidence_density(field_truth),
        "benchmark_support_class": benchmark_support_class,
        "field_count_by_truth_family": dict(family_counts),
        "fields": canonical_fields,
        "downgrade_reasons": sorted({item for item in downgrade_reasons if item}),
    }


def build_daily_brief_run_truth_contract(
    *,
    run_mode: str,
    grounding_mode: str,
    trust_banner: dict[str, Any] | None,
    cards: list[dict[str, Any]] | None,
    charts: list[dict[str, Any]] | None,
    target_proxy_used: bool,
    live_holdings_used: bool,
) -> dict[str, Any]:
    chart_items = list(charts or [])
    card_items = list(cards or [])
    if live_holdings_used:
        holdings_mapping_directness = "live_holdings_grounded"
        portfolio_relevance_basis = "current_holdings_direct"
    elif grounding_mode in {"target_proxy", "live_sleeve_grounded"} or target_proxy_used:
        holdings_mapping_directness = "proxy_mapped"
        portfolio_relevance_basis = "target_proxy"
    else:
        holdings_mapping_directness = "macro_only"
        portfolio_relevance_basis = "macro_only"
    source_density_class = "thin"
    if len(card_items) >= 6 or len(chart_items) >= 3:
        source_density_class = "dense"
    elif len(card_items) >= 3 or len(chart_items) >= 1:
        source_density_class = "moderate"
    chart_evidence_state = "no_chart_support"
    if chart_items:
        chart_evidence_state = "chart_backed"
    elif card_items:
        chart_evidence_state = "text_backed_only"
    action_authority_class = "review_only"
    if run_mode == "full_live_verified" and live_holdings_used and chart_evidence_state == "chart_backed":
        action_authority_class = "bounded_action_ready"
    elif run_mode == "live_partial":
        action_authority_class = "review_only"
    elif run_mode == "verified_bootstrap":
        action_authority_class = "bootstrap_review_only"
    elif run_mode == "text_fallback":
        action_authority_class = "text_fallback_non_actionable"
    downgrade_reasons: list[str] = []
    if not live_holdings_used:
        downgrade_reasons.append("live_holdings_unavailable")
    if target_proxy_used:
        downgrade_reasons.append("target_proxy_grounding")
    if chart_evidence_state != "chart_backed":
        downgrade_reasons.append(chart_evidence_state)
    trust_level = str(dict(trust_banner or {}).get("trust_level") or "").upper()
    if trust_level in {"LOW", "VERY_LOW"}:
        downgrade_reasons.append(f"trust_{trust_level.lower()}")
    return {
        "run_mode": run_mode,
        "holdings_mapping_directness": holdings_mapping_directness,
        "source_density_class": source_density_class,
        "chart_evidence_state": chart_evidence_state,
        "portfolio_relevance_basis": portfolio_relevance_basis,
        "action_authority_class": action_authority_class,
        "downgrade_reasons": downgrade_reasons,
    }


def apply_claim_boundary_from_truth(truth_contract: dict[str, Any], text: str) -> str:
    message = str(text or "").strip()
    claim_limit = str(truth_contract.get("claim_limit_class") or "")
    if not message:
        return message
    if "review-only context rather than a decisive investor claim" in message:
        return message
    if claim_limit == "full_support":
        return message
    if claim_limit == "summary_limited":
        return f"{message} This remains summary-backed rather than direct-holdings-backed."
    if claim_limit == "benchmark_limited":
        return f"{message} Benchmark support remains proxy-limited, so comparative claims should stay cautious."
    if claim_limit == "portfolio_mapping_limited":
        return f"{message} Portfolio mapping is still proxy-based, so treat this as review context rather than direct action guidance."
    if claim_limit in {"review_only", "research_only"}:
        return f"{message} Treat this as review-only context rather than a decisive investor claim."
    if claim_limit == "structure_first_limited":
        return f"{message} This product should be judged through structure-first evidence, not a fake holdings-completeness standard."
    return message


def apply_daily_brief_claim_boundary(run_mode_contract: dict[str, Any], text: str, *, field_role: str = "general") -> str:
    message = str(text or "").strip()
    if not message:
        return message
    authority = str(run_mode_contract.get("action_authority_class") or "")
    basis = str(run_mode_contract.get("portfolio_relevance_basis") or "")
    if authority == "bounded_action_ready":
        return message
    if authority == "bootstrap_review_only":
        if field_role == "portfolio":
            suffix = "This remains proxy-grounded portfolio review context rather than direct holdings-backed action guidance."
        else:
            suffix = "This remains bootstrap review context rather than full live action guidance."
        return message if suffix in message else f"{message} {suffix}"
    if authority == "text_fallback_non_actionable":
        suffix = "This is text-fallback context only and should not be treated as action guidance."
        return message if suffix in message else f"{message} {suffix}"
    if basis in {"target_proxy", "macro_only"}:
        suffix = "Portfolio relevance is still indirect, so treat this as review context rather than a direct sleeve action call."
        return message if suffix in message else f"{message} {suffix}"
    return message
