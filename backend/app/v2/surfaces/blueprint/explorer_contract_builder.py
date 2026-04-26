from __future__ import annotations

from datetime import UTC, datetime
from importlib import import_module
import json
import sqlite3
from typing import Any

from app.config import get_db_path
from app.services.blueprint_benchmark_registry import DEFAULT_BENCHMARK_ASSIGNMENTS, DEFAULT_SLEEVE_ASSIGNMENTS
from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, export_live_candidate_registry, seed_default_candidate_registry
from app.services.provider_cache import get_cached_provider_snapshot
from app.services.provider_registry import provider_support_status, routed_provider_candidates
from app.v2.core.domain_objects import CandidateAssessment, ConstraintSummary, MarketSeriesTruth, PolicyBoundary, utc_now_iso
from app.v2.contracts.chart_contracts import comparison_chart_panel
from app.v2.core.holdings_overlay import apply_overlay
from app.v2.core.ips_targets import get_ips_sleeve_profile, ordered_ips_sleeves
from app.v2.core.interpretation_engine import build_sleeve_assessment, interpret
from app.v2.core.mandate_rubric import apply_rubric, build_policy_boundaries
from app.v2.donors.instrument_truth import get_instrument_truth
from app.v2.donors.portfolio_truth import get_portfolio_truth
from app.v2.doctrine.doctrine_evaluator import evaluate
from app.v2.features.research_support import build_research_support_summary
from app.v2.core.change_ledger import record_change
from app.v2.blueprint_market import compact_forecast_support_from_market_path, market_path_artifact_requires_upgrade
from app.v2.blueprint_market.coverage import build_candidate_coverage_summary
from app.v2.blueprint_market.series_refresh_service import check_candidate_series_freshness
from app.v2.blueprint_market.series_store import latest_forecast_artifact, list_market_identities, persist_forecast_artifact, record_forecast_run
from app.v2.sources.freshness_registry import get_freshness
from app.v2.storage.surface_snapshot_store import latest_surface_snapshot, previous_surface_snapshot, record_surface_snapshot
from app.v2.surfaces.market_truth_support import load_surface_market_truth
from app.v2.surfaces.common import degraded_section, ready_section, runtime_provenance, surface_state
from app.v2.surfaces.blueprint.explanation_builders import build_candidate_explorer_explanations
from app.v2.surfaces.blueprint.report_contract_builder import _quick_brief
from app.v2.surfaces.changes.emitters import emit_explorer_snapshot_changes
from app.v2.surfaces.blueprint.semantic_packaging import (
    build_candidate_summary_pack,
    build_default_route_cache_state,
    build_market_path_summary_pack,
)
from app.v2.truth.candidate_quality import (
    _clamp_score,
    build_candidate_truth_context,
    build_score_rubric,
    enrich_score_decomposition_with_market_path_support,
    market_path_support_component,
)
from app.v2.truth.replay_inputs import compact_replay_inputs


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _slug(value: str) -> str:
    return str(value or "").strip().lower().replace(".", "_").replace("-", "_")


def _sleeve_id(sleeve_key: str) -> str:
    return f"sleeve_{sleeve_key}"


def _sleeve_label(sleeve_key: str) -> str:
    return str(sleeve_key or "").replace("_", " ").strip().title()


def _sleeve_purpose(sleeve_key: str) -> str:
    purposes = {
        "global_equity_core": "Broad global equity exposure",
        "developed_ex_us_optional": "Optional developed-market complement",
        "emerging_markets": "Emerging-market diversification",
        "china_satellite": "China-specific satellite exposure",
        "ig_bonds": "Investment-grade ballast",
        "cash_bills": "Cash and T-bill liquidity reserve",
        "real_assets": "Inflation-sensitive real asset hedge",
        "alternatives": "Alternative diversifier",
        "convex": "Convex downside hedge",
    }
    return purposes.get(sleeve_key, _sleeve_label(sleeve_key))


def _sleeve_role_statement(sleeve_key: str) -> str | None:
    return {
        "global_equity_core": "This sleeve is the portfolio's main equity engine and should only hold broad, durable core exposure.",
        "developed_ex_us_optional": "This sleeve adds non-US developed exposure when the portfolio wants a deliberate second equity source outside the US.",
        "emerging_markets": "This sleeve is for higher-growth but higher-volatility equity exposure that should complement, not replace, core equities.",
        "china_satellite": "This sleeve is a tactical China-specific line and should stay separate from broader EM exposure.",
        "ig_bonds": "This sleeve is the portfolio's main defensive bond allocation and should emphasize ballast and implementation reliability.",
        "cash_bills": "This sleeve is the liquidity reserve and should preserve optionality, funding flexibility, and low implementation risk.",
        "real_assets": "This sleeve is for inflation-sensitive diversification and should earn its place through differentiated protection value.",
        "alternatives": "This sleeve is for non-core diversifiers that add a different job than the main stock-bond book.",
        "convex": "This sleeve is for explicit downside protection and should only hold instruments with a clear hedging role.",
    }.get(sleeve_key)


def _cycle_sensitivity(sleeve_key: str) -> str | None:
    return {
        "global_equity_core": "Most sensitive to broad growth and risk appetite.",
        "developed_ex_us_optional": "Sensitive to global growth, dollar moves, and relative non-US leadership.",
        "emerging_markets": "Sensitive to global liquidity, the dollar, and China-linked growth.",
        "china_satellite": "Sensitive to China policy, property stress, and external risk sentiment.",
        "ig_bonds": "Sensitive to rates, inflation expectations, and credit conditions.",
        "cash_bills": "Sensitive to front-end rates and liquidity conditions, with low mark-to-market risk.",
        "real_assets": "Sensitive to inflation pressure, real yields, and commodity leadership.",
        "alternatives": "Sensitive to regime shifts and whether diversification benefits are actually showing up.",
        "convex": "Most useful when volatility jumps or equity downside risk is repriced quickly.",
    }.get(sleeve_key)


def _base_allocation_rationale(sleeve_key: str) -> str | None:
    return {
        "global_equity_core": "Keep a broad base allocation here because it carries the main long-run equity participation job.",
        "developed_ex_us_optional": "Use this sleeve when non-US developed exposure improves diversification or valuation balance.",
        "emerging_markets": "Keep this sleeve sized for diversification and growth optionality, not as a dominant risk bucket.",
        "china_satellite": "Treat this as a bounded satellite sleeve that should justify itself with a distinct China view.",
        "ig_bonds": "Keep this sleeve as the main defensive bond anchor unless inflation or rate risk still argues for restraint.",
        "cash_bills": "Hold this sleeve to preserve dry powder, liquidity, and near-term optionality.",
        "real_assets": "Use this sleeve when inflation protection or real-asset diversification still matters.",
        "alternatives": "Keep this sleeve only where the diversifier role is specific and defensible versus simpler exposures.",
        "convex": "Use this sleeve sparingly to buy explicit protection rather than general diversification.",
    }.get(sleeve_key)


def _coerce_freshness_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    return {"freshness_class": text}


def _market_truth(symbol: str, endpoint_family: str = "ohlcv_history") -> MarketSeriesTruth:
    return load_surface_market_truth(
        symbol=symbol,
        surface_name="blueprint",
        endpoint_family=endpoint_family,
        lookback=120,
        allow_live_fetch=False,
    )


def _snapshot_market_truth(symbol: str) -> MarketSeriesTruth:
    return _market_truth(symbol, endpoint_family="quote_latest")


def _meaningful_runtime_value(value: Any) -> bool:
    if isinstance(value, bool):
        return True
    return value is not None and value != ""


def _meaningful_compact_value(value: Any) -> bool:
    if isinstance(value, bool):
        return True
    if value is None:
        return False
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return value != ""


_TIMING_LABELS = {
    "timing_ready": "Timing ready",
    "timing_review": "Timing review",
    "timing_fragile": "Timing fragile",
    "timing_constrained": "Timing constrained",
    "timing_unavailable": "Timing unavailable",
}

_CURRENT_TIMING_SCHEMA_VERSION = "blueprint_market_path_timing_v2"
_ROUTE_HISTORY_MODEL_NAME = "route_history_timing_assessment"
_ROUTE_HISTORY_INPUT_VERSION_PREFIX = "route_history_timing_v2"
_MODEL_RUNTIME_FAILURES = {"forecast_model_missing_dependency", "forecast_runtime_failed"}
_MODEL_OUTPUT_INVALID_FAILURES = {"invalid_predicted_bar_geometry", "negative_predicted_liquidity"}
_CURRENT_TYPED_CONSTRAINT_REASONS = {
    "direct_series_broken",
    "proxy_series_stale",
    "proxy_series_degraded",
    "proxy_series_missing",
    "market_setup_unavailable",
    "forecast_provider_unavailable",
    "forecast_output_invalid_quarantined",
    "no_usable_route_history",
}


def _timing_label(timing_state: str | None) -> str:
    return _TIMING_LABELS.get(str(timing_state or "").strip(), "Timing not assessed")


def _artifact_schema_version(support: dict[str, Any] | None) -> str | None:
    if not isinstance(support, dict):
        return None
    metadata = dict(support.get("model_metadata") or {})
    manifest = dict(support.get("truth_manifest") or {})
    return (
        str(metadata.get("support_semantics_version") or "").strip()
        or str(support.get("support_semantics_version") or "").strip()
        or str(manifest.get("support_semantics_version") or "").strip()
        or None
    )


def _latest_forecast_run(conn: sqlite3.Connection, *, candidate_id: str) -> dict[str, Any] | None:
    try:
        row = conn.execute(
            """
            SELECT forecast_run_id, candidate_id, series_role, model_name, model_version,
                   input_series_version, run_status, usefulness_label, suppression_reason,
                   generated_at, details_json
            FROM candidate_market_forecast_runs
            WHERE candidate_id = ?
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (candidate_id,),
        ).fetchone()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    item = dict(row)
    try:
        item["details"] = json.loads(str(item.get("details_json") or "{}"))
    except Exception:
        item["details"] = {}
    return item


def _series_quality_for_role(conn: sqlite3.Connection, *, candidate_id: str, series_role: str) -> dict[str, Any] | None:
    try:
        freshness = check_candidate_series_freshness(conn, candidate_id=candidate_id, series_role=series_role)
    except Exception:
        return None
    summary = dict(dict(freshness or {}).get("series_quality_summary") or {})
    if not summary:
        return None
    summary["series_role"] = series_role
    try:
        identity = next(
            (
                dict(item)
                for item in list_market_identities(conn, candidate_id)
                if str(dict(item).get("series_role") or "").strip() == series_role
            ),
            {},
        )
    except Exception:
        identity = {}
    if identity:
        summary["provider_symbol"] = str(identity.get("provider_symbol") or identity.get("symbol") or "").strip().upper() or None
        summary["exchange_mic"] = str(identity.get("exchange_mic") or "").strip() or None
        summary["provider_asset_class"] = str(identity.get("provider_asset_class") or "").strip() or None
    return summary


def _series_status(summary: dict[str, Any] | None) -> str:
    return str(dict(summary or {}).get("quality_label") or "missing").strip().lower() or "missing"


def _series_depth(summary: dict[str, Any] | None) -> int:
    try:
        return int(dict(summary or {}).get("bars_present") or 0)
    except (TypeError, ValueError):
        return 0


def _series_age_days(summary: dict[str, Any] | None) -> int:
    try:
        return int(dict(summary or {}).get("stale_days") or 999)
    except (TypeError, ValueError):
        return 999


def _series_provider_symbol(summary: dict[str, Any] | None, fallback: str | None = None) -> str | None:
    raw = str(dict(summary or {}).get("provider_symbol") or fallback or "").strip().upper()
    return raw or None


def _series_is_current_usable(summary: dict[str, Any] | None) -> bool:
    if not isinstance(summary, dict):
        return False
    return _series_status(summary) in {"good", "watch"} and _series_depth(summary) >= 260 and _series_age_days(summary) <= 7


def _series_is_ready_clean(summary: dict[str, Any] | None) -> bool:
    if not isinstance(summary, dict):
        return False
    return _series_status(summary) == "good" and _series_depth(summary) >= 260 and _series_age_days(summary) <= 3


def _normalize_forecast_failure_reason(
    *,
    latest_run: dict[str, Any] | None,
    suppression_reason: str | None = None,
) -> str | None:
    run = dict(latest_run or {})
    details = dict(run.get("details") or {})
    text = " ".join(
        str(item or "")
        for item in (
            run.get("run_status"),
            run.get("suppression_reason"),
            suppression_reason,
            details.get("error"),
            details.get("last_model_error"),
        )
    ).lower()
    if not text.strip():
        return None
    if "bar geometry" in text:
        return "invalid_predicted_bar_geometry"
    if "negative" in text and "liquidity" in text:
        return "negative_predicted_liquidity"
    if "no module named 'torch'" in text or "kronos import failed" in text:
        return "forecast_model_missing_dependency"
    if "served_last_good" in text or "last_good" in text:
        return "last_good_artifact_served"
    if "failed" in text or "model_execution_failed" in text:
        return "forecast_runtime_failed"
    return str(suppression_reason or run.get("suppression_reason") or "").strip() or None


def _cached_artifact_is_older_than_latest_failure(cached: dict[str, Any] | None, latest_run: dict[str, Any] | None) -> bool:
    if not isinstance(cached, dict) or not isinstance(latest_run, dict):
        return False
    if str(latest_run.get("run_status") or "").strip().lower() not in {"failed", "served_last_good"}:
        return False
    artifact_created_at = str(cached.get("created_at") or "").strip()
    latest_generated_at = str(latest_run.get("generated_at") or "").strip()
    return bool(artifact_created_at and latest_generated_at and latest_generated_at >= artifact_created_at)


def _runtime_failure_for_timing(failure_reason: str | None) -> str | None:
    if failure_reason == "forecast_model_missing_dependency":
        return "forecast_provider_unavailable"
    if failure_reason == "forecast_runtime_failed":
        return "forecast_provider_unavailable"
    if failure_reason in _MODEL_OUTPUT_INVALID_FAILURES:
        return "forecast_output_invalid_quarantined"
    if failure_reason == "last_good_artifact_served":
        return "last_good_not_current_timing"
    return failure_reason


def _validation_status_for_timing(failure_reason: str | None) -> str:
    if failure_reason in _MODEL_OUTPUT_INVALID_FAILURES:
        return "forecast_output_invalid_quarantined"
    if failure_reason in _MODEL_RUNTIME_FAILURES:
        return "forecast_provider_unavailable_isolated"
    if failure_reason:
        return "forecast_runtime_constraint_typed"
    return "route_history_validated"


def _classify_route_history_timing(
    *,
    symbol: str,
    direct_summary: dict[str, Any] | None,
    proxy_summary: dict[str, Any] | None,
    latest_run: dict[str, Any] | None,
    raw_failure_reason: str | None,
) -> dict[str, Any]:
    runtime_reason = _runtime_failure_for_timing(raw_failure_reason)
    latest_status = str(dict(latest_run or {}).get("run_status") or "").strip().lower() or None
    direct_usable = _series_is_current_usable(direct_summary)
    proxy_usable = _series_is_current_usable(proxy_summary)
    direct_ready_clean = _series_is_ready_clean(direct_summary)
    reasons: list[str] = []
    timing_state = "timing_unavailable"
    driving_role = "direct"
    driving_symbol = _series_provider_symbol(direct_summary, symbol) or symbol
    timing_data_source = "route_history_assessment"
    if direct_usable:
        reasons.append("direct_series_current_and_usable")
        if runtime_reason in {"forecast_provider_unavailable", "forecast_output_invalid_quarantined"}:
            reasons.extend(["forecast_unavailable_route_history_usable", runtime_reason])
            timing_state = "timing_review"
        elif direct_ready_clean and latest_status == "ready":
            timing_state = "timing_ready"
        else:
            timing_state = "timing_review"
        if _series_status(direct_summary) == "watch":
            reasons.append("direct_series_watch")
    elif proxy_usable:
        driving_role = "approved_proxy"
        driving_symbol = _series_provider_symbol(proxy_summary, symbol) or symbol
        reasons.append("proxy_series_fresh_and_approved")
        if runtime_reason in {"forecast_provider_unavailable", "forecast_output_invalid_quarantined"}:
            reasons.extend(["forecast_unavailable_route_history_usable", runtime_reason])
        timing_state = "timing_review"
    else:
        direct_status = _series_status(direct_summary)
        proxy_status = _series_status(proxy_summary)
        if direct_status in {"broken", "missing", "thin"}:
            reasons.append("direct_series_broken")
        if proxy_status in {"good", "watch"} and _series_age_days(proxy_summary) > 7:
            reasons.append("proxy_series_stale")
        elif proxy_status == "degraded":
            reasons.append("proxy_series_degraded")
        elif proxy_status in {"broken", "missing", "thin"}:
            reasons.append("proxy_series_missing")
        if runtime_reason:
            reasons.append(runtime_reason)
        timing_state = "timing_constrained" if reasons else "timing_unavailable"
    if not reasons:
        reasons.append("market_setup_unavailable")
    return {
        "timing_state": timing_state,
        "timing_reasons": sorted(set(reasons)),
        "driving_symbol": driving_symbol,
        "driving_series_role": driving_role,
        "uses_proxy_series": driving_role == "approved_proxy",
        "proxy_symbol": _series_provider_symbol(proxy_summary),
        "timing_data_source": timing_data_source,
        "forecast_failure_reason": runtime_reason,
        "validation_status": _validation_status_for_timing(raw_failure_reason),
    }


def _derive_timing_state_from_support(support: dict[str, Any]) -> tuple[str, list[str]]:
    explicit = str(support.get("timing_state") or "").strip()
    explicit_reasons = [str(item).strip() for item in list(support.get("timing_reasons") or []) if str(item).strip()]
    if explicit:
        return explicit, explicit_reasons
    usefulness = str(support.get("usefulness_label") or "").strip().lower()
    setup_state = str(support.get("market_setup_state") or "").strip().lower()
    path_quality = str(support.get("path_quality_label") or "").strip().lower()
    fragility = str(support.get("candidate_fragility_label") or "").strip().lower()
    drift = str(support.get("threshold_drift_direction") or "").strip().lower()
    uses_proxy = bool(support.get("uses_proxy_series") or dict(support.get("series_quality_summary") or {}).get("uses_proxy_series"))
    reasons: list[str] = []
    if setup_state == "unavailable" or usefulness == "suppressed":
        return "timing_unavailable", ["market_setup_unavailable"]
    if setup_state in {"stale", "degraded"}:
        return "timing_constrained", [f"market_setup_{setup_state}"]
    reasons.append("proxy_series_fresh_and_approved" if uses_proxy else "direct_series_current_and_usable")
    if drift == "toward_weakening":
        reasons.append("threshold_drift_weakening")
    if fragility in {"fragile", "acute"}:
        reasons.append("path_fragility_current")
    if path_quality == "noisy":
        reasons.append("path_noisy_but_usable")
    if usefulness == "unstable" and {"threshold_drift_weakening", "path_fragility_current"}.intersection(reasons):
        return "timing_fragile", sorted(set(reasons))
    if usefulness == "strong" and setup_state == "direct_usable" and path_quality in {"clean", "balanced"} and fragility in {"resilient", "watchful"}:
        return "timing_ready", sorted(set(reasons))
    return "timing_review", sorted(set(reasons))


def _timing_assessment(
    *,
    support: dict[str, Any] | None,
    cached: dict[str, Any] | None,
    latest_run: dict[str, Any] | None,
    direct_summary: dict[str, Any] | None,
    proxy_summary: dict[str, Any] | None,
    symbol: str,
) -> dict[str, Any]:
    support_dict = dict(support or {})
    latest_status = str(dict(latest_run or {}).get("run_status") or "").strip().lower() or None
    raw_failure_reason = _normalize_forecast_failure_reason(
        latest_run=latest_run,
        suppression_reason=str(support_dict.get("suppression_reason") or "") or None,
    )
    failure_reason = _runtime_failure_for_timing(raw_failure_reason)
    artifact_id = str(dict(cached or {}).get("artifact_id") or "").strip() or None
    schema_version = _artifact_schema_version(support_dict)
    raw_schema_status = "missing_artifact" if not support_dict else "old_schema_artifact" if market_path_artifact_requires_upgrade(support_dict) else "current_schema"
    route_history = _classify_route_history_timing(
        symbol=symbol,
        direct_summary=direct_summary,
        proxy_summary=proxy_summary,
        latest_run=latest_run,
        raw_failure_reason=raw_failure_reason,
    )
    schema_status = "current_schema"
    artifact_valid = True
    reasons: list[str] = []
    if raw_schema_status != "current_schema":
        timing_state = str(route_history.get("timing_state") or "timing_constrained")
        reasons = list(route_history.get("timing_reasons") or [])
        if raw_schema_status == "old_schema_artifact":
            reasons.append("old_artifact_replaced")
        elif raw_schema_status == "missing_artifact":
            reasons.append("missing_artifact_replaced")
    else:
        timing_state, reasons = _derive_timing_state_from_support(support_dict)
        if _cached_artifact_is_older_than_latest_failure(cached, latest_run):
            if _series_is_current_usable(direct_summary) or _series_is_current_usable(proxy_summary):
                timing_state = "timing_review"
                reasons.extend(list(route_history.get("timing_reasons") or []))
                reasons.append("forecast_unavailable_route_history_usable")
            else:
                timing_state = "timing_constrained"
                if failure_reason:
                    reasons.append(failure_reason)
                reasons.append("latest_forecast_run_not_ready")
    if _series_is_current_usable(direct_summary):
        reasons.append("direct_series_current_and_usable")
    elif _series_is_current_usable(proxy_summary):
        reasons.append("proxy_series_fresh_and_approved")
    if timing_state == "timing_fragile" and not {"path_fragility_current", "threshold_drift_weakening"}.intersection(reasons):
        timing_state = "timing_constrained"
        reasons.append("fragile_label_without_current_path_evidence")
    if timing_state == "timing_constrained" and not _CURRENT_TYPED_CONSTRAINT_REASONS.intersection(reasons):
        if failure_reason:
            reasons.append(failure_reason)
        else:
            reasons.append("no_usable_route_history")
    return {
        "timing_state": timing_state,
        "timing_label": _timing_label(timing_state),
        "timing_reasons": sorted(set(str(item) for item in reasons if str(item).strip())),
        "timing_artifact_valid": artifact_valid,
        "timing_artifact_schema_status": schema_status,
        "timing_artifact_id": artifact_id or f"route_history_assessment:{symbol.lower()}",
        "timing_artifact_schema_version": schema_version or _CURRENT_TIMING_SCHEMA_VERSION,
        "timing_artifact_generated_at": str(dict(cached or {}).get("created_at") or support_dict.get("generated_at") or "").strip() or utc_now_iso(),
        "raw_artifact_schema_status": raw_schema_status,
        "latest_forecast_run_status": latest_status,
        "latest_forecast_failure_reason": failure_reason,
        "forecast_failure_reason": failure_reason,
        "validation_status": route_history.get("validation_status"),
        "direct_series_status": (direct_summary or {}).get("quality_label"),
        "direct_series_last_bar_age_days": (direct_summary or {}).get("stale_days"),
        "direct_series_depth": (direct_summary or {}).get("bars_present"),
        "proxy_series_status": (proxy_summary or {}).get("quality_label"),
        "proxy_series_last_bar_age_days": (proxy_summary or {}).get("stale_days"),
        "proxy_series_depth": (proxy_summary or {}).get("bars_present"),
        "driving_symbol": route_history.get("driving_symbol") or support_dict.get("driving_symbol") or symbol,
        "driving_series_role": route_history.get("driving_series_role") or support_dict.get("driving_series_role"),
        "uses_proxy_series": route_history.get("uses_proxy_series"),
        "proxy_symbol": route_history.get("proxy_symbol") or support_dict.get("proxy_symbol"),
        "timing_data_source": route_history.get("timing_data_source"),
        "timing_confidence": "high" if timing_state == "timing_ready" else "medium" if timing_state == "timing_review" else "low",
    }


def _apply_timing_assessment(support: dict[str, Any], assessment: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(support)
    for key, value in assessment.items():
        if _meaningful_compact_value(value):
            enriched[key] = value
    return enriched


def _constrained_market_path_support(
    *,
    candidate_id: str,
    label: str,
    symbol: str,
    source_support: dict[str, Any] | None,
    assessment: dict[str, Any],
) -> dict[str, Any]:
    source = dict(source_support or {})
    timing_state = str(assessment.get("timing_state") or "timing_constrained")
    reasons = list(assessment.get("timing_reasons") or [])
    suppression_reason = str(reasons[0] if reasons else "market_setup_constrained")
    unavailable = timing_state == "timing_unavailable"
    constrained = timing_state == "timing_constrained"
    review = timing_state == "timing_review"
    ready = timing_state == "timing_ready"
    uses_proxy = bool(assessment.get("uses_proxy_series"))
    market_setup_state = (
        "unavailable"
        if unavailable
        else "degraded"
        if constrained
        else "proxy_usable"
        if uses_proxy
        else "direct_usable"
    )
    usefulness_label = "usable" if ready else "usable_with_caution" if review else "suppressed"
    eligibility_state = "eligible" if ready or review else "unavailable" if unavailable else "constrained"
    implication = (
        "Market-path setup is unavailable because route truth is not reliable enough to use."
        if unavailable
        else "Market-path setup is usable from current route and history evidence; model output is not required for this timing classification."
        if ready or review
        else "Market-path setup is constrained; use current series truth, not stale or failed forecast artifacts."
    )
    return _apply_timing_assessment(
        {
            "candidate_id": candidate_id,
            "eligibility_state": eligibility_state,
            "usefulness_label": usefulness_label,
            "suppression_reason": None if ready or review else suppression_reason,
            "market_setup_state": market_setup_state,
            "freshness_state": "current" if ready or review else "unavailable" if unavailable else "degraded",
            "observed_series": [],
            "projected_series": [],
            "input_timestamps": [],
            "output_timestamps": [],
            "uncertainty_band": None,
            "threshold_map": [],
            "scenario_summary": [],
            "scenario_endpoint_summary": [],
            "scenario_takeaways": {
                "primary": "Timing state is classified from persisted route and history evidence.",
                "caveat": "No model path is used when forecast output is unavailable or invalid.",
            },
            "sampling_summary": {
                "sampling_mode": "not_required",
                "sample_path_count": 0,
                "summary_method": "route_history_assessment",
            },
            "liquidity_feature_mode": source.get("liquidity_feature_mode") or "not_required_for_route_history",
            "path_quality_label": "balanced" if ready or review else "unavailable",
            "path_quality_score": 0.72 if ready else 0.58 if review else 0.25,
            "candidate_fragility_label": "watchful" if ready or review else "fragile",
            "candidate_fragility_score": 0.28 if ready else 0.42 if review else 0.75,
            "threshold_drift_direction": "neutral",
            "candidate_implication": implication,
            "generated_at": utc_now_iso(),
            "provider_source": source.get("provider_source") or "twelve_data+kronos",
            "provider": source.get("provider") or "twelve_data",
            "forecast_horizon": int(source.get("forecast_horizon") or 21),
            "forecast_interval": str(source.get("forecast_interval") or "1day"),
            "quality_flags": sorted(set(str(item) for item in reasons if str(item).strip())),
            "series_quality_summary": source.get("series_quality_summary"),
            "model_family": source.get("model_family"),
            "checkpoint": source.get("checkpoint"),
            "service_name": source.get("service_name"),
            "driving_symbol": assessment.get("driving_symbol") or source.get("driving_symbol") or symbol,
            "driving_series_role": assessment.get("driving_series_role") or source.get("driving_series_role"),
            "uses_proxy_series": uses_proxy,
            "proxy_symbol": assessment.get("proxy_symbol") or source.get("proxy_symbol"),
            "market_path_case_family": "route_history" if ready or review else "suppressed",
            "market_path_objective": "bounded_context_only",
            "market_path_case_note": f"{label} has current route/history timing evidence.",
            "support_semantics_version": _CURRENT_TIMING_SCHEMA_VERSION,
            "truth_manifest": {
                "support_semantics_version": _CURRENT_TIMING_SCHEMA_VERSION,
                "timing_data_source": assessment.get("timing_data_source") or "route_history_assessment",
                "forecast_output_required": False,
            },
        },
        assessment,
    )


def _compact_market_path_support(support: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(support, dict):
        return None
    compact = {
        "usefulness_label": support.get("usefulness_label"),
        "market_setup_state": support.get("market_setup_state"),
        "freshness_state": support.get("freshness_state"),
        "suppression_reason": support.get("suppression_reason"),
        "driving_symbol": support.get("driving_symbol"),
        "proxy_symbol": support.get("proxy_symbol"),
        "liquidity_feature_mode": support.get("liquidity_feature_mode"),
        "path_quality_label": support.get("path_quality_label"),
        "candidate_fragility_label": support.get("candidate_fragility_label"),
        "threshold_drift_direction": support.get("threshold_drift_direction"),
        "provider_source": support.get("provider_source"),
        "generated_at": support.get("generated_at"),
        "candidate_implication": support.get("candidate_implication"),
        "series_quality_summary": support.get("series_quality_summary"),
        "timing_state": support.get("timing_state"),
        "timing_label": support.get("timing_label"),
        "timing_reasons": support.get("timing_reasons"),
        "timing_artifact_valid": support.get("timing_artifact_valid"),
        "timing_artifact_schema_status": support.get("timing_artifact_schema_status"),
        "timing_artifact_id": support.get("timing_artifact_id"),
        "timing_artifact_schema_version": support.get("timing_artifact_schema_version"),
        "timing_artifact_generated_at": support.get("timing_artifact_generated_at"),
        "raw_artifact_schema_status": support.get("raw_artifact_schema_status"),
        "timing_confidence": support.get("timing_confidence"),
        "timing_data_source": support.get("timing_data_source"),
        "validation_status": support.get("validation_status"),
        "forecast_failure_reason": support.get("forecast_failure_reason"),
        "latest_forecast_run_status": support.get("latest_forecast_run_status"),
        "latest_forecast_failure_reason": support.get("latest_forecast_failure_reason"),
        "direct_series_status": support.get("direct_series_status"),
        "direct_series_last_bar_age_days": support.get("direct_series_last_bar_age_days"),
        "direct_series_depth": support.get("direct_series_depth"),
        "proxy_series_status": support.get("proxy_series_status"),
        "proxy_series_last_bar_age_days": support.get("proxy_series_last_bar_age_days"),
        "proxy_series_depth": support.get("proxy_series_depth"),
    }
    return {
        key: value
        for key, value in compact.items()
        if _meaningful_compact_value(value)
    }


def _compact_candidate_row(candidate: dict[str, Any]) -> dict[str, Any]:
    compact = dict(candidate)
    compact["market_path_support"] = _compact_market_path_support(
        dict(candidate.get("market_path_support") or {}) if candidate.get("market_path_support") is not None else None
    )
    for field_name in (
        "source_authority_fields",
        "reconciliation_report",
        "coverage_workflow_summary",
        "detail_chart_panels",
        "candidate_market_provenance",
        "report_snapshot",
    ):
        compact.pop(field_name, None)
    return compact


def _compact_explorer_contract(contract: dict[str, Any]) -> dict[str, Any]:
    compact = dict(contract)
    compact["sleeves"] = [
        {
            **dict(sleeve),
            "candidates": [
                _compact_candidate_row(dict(candidate))
                for candidate in list(dict(sleeve).get("candidates") or [])
            ],
        }
        for sleeve in list(contract.get("sleeves") or [])
    ]
    return compact


_RICH_EXPLORER_ROW_FIELDS: tuple[str, ...] = (
    "sleeve_key",
    "gate_state",
    "coverage_workflow_summary",
    "source_completion_summary",
)


def _iter_explorer_candidate_rows(contract: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sleeve in list(contract.get("sleeves") or []):
        for candidate in list(dict(sleeve).get("candidates") or []):
            rows.append(dict(candidate))
    return rows


def _count_blocked_candidates(contract: dict[str, Any]) -> int:
    count = 0
    for row in _iter_explorer_candidate_rows(contract):
        status_label = str(row.get("status_label") or "").strip().lower()
        visible_state = str(dict(row.get("visible_decision_state") or {}).get("state") or "").strip().lower()
        if status_label == "blocked candidate" or visible_state == "blocked":
            count += 1
    return count


def _missing_rich_row_fields(row: dict[str, Any]) -> list[str]:
    missing = [
        field_name
        for field_name in _RICH_EXPLORER_ROW_FIELDS
        if row.get(field_name) is None or row.get(field_name) == ""
    ]
    if "market_path_support" not in row and "market_path" not in row:
        missing.append("market_path_support")
    return missing


def _source_completion_guard_metrics(row: dict[str, Any]) -> dict[str, int]:
    summary = dict(row.get("source_completion_summary") or {})
    return {
        "critical_fields_completed": int(summary.get("critical_fields_completed") or 0),
        "critical_fields_total": int(summary.get("critical_fields_total") or 0),
        "weak_fields": len(list(summary.get("weak_fields") or [])),
        "stale_fields": len(list(summary.get("stale_fields") or [])),
        "conflict_fields": len(list(summary.get("conflict_fields") or [])),
    }


def _blueprint_snapshot_guard_reason(
    *,
    previous_contract: dict[str, Any] | None,
    candidate_contract: dict[str, Any],
    override_reason: str | None = None,
) -> str | None:
    if override_reason:
        return None
    candidate_rows = _iter_explorer_candidate_rows(candidate_contract)
    if not candidate_rows:
        return None
    for row in candidate_rows:
        missing = _missing_rich_row_fields(row)
        if missing:
            return (
                f"rejected_blueprint_snapshot_missing_rich_fields:"
                f"{row.get('candidate_id') or row.get('symbol') or 'candidate'}:{','.join(missing)}"
            )
        completion_state = str(dict(row.get("source_completion_summary") or {}).get("state") or "").strip().lower()
        if completion_state != "complete":
            return f"rejected_blueprint_snapshot_source_incomplete:{row.get('candidate_id') or row.get('symbol') or 'candidate'}:{completion_state or 'missing'}"
        market_path_support = dict(row.get("market_path_support") or row.get("market_path") or {})
        timing_state = str(market_path_support.get("timing_state") or "").strip().lower()
        timing_schema_status = str(market_path_support.get("timing_artifact_schema_status") or "").strip().lower()
        timing_valid = bool(market_path_support.get("timing_artifact_valid"))
        timing_reasons = {str(item).strip() for item in list(market_path_support.get("timing_reasons") or []) if str(item).strip()}
        latest_failure_reason = str(market_path_support.get("latest_forecast_failure_reason") or "").strip()
        if not timing_state:
            return f"rejected_blueprint_snapshot_missing_timing_state:{row.get('candidate_id') or row.get('symbol') or 'candidate'}"
        if timing_schema_status != "current_schema" or not timing_valid:
            return f"rejected_blueprint_snapshot_noncurrent_timing_artifact:{row.get('candidate_id') or row.get('symbol') or 'candidate'}:{timing_schema_status or 'missing'}"
        if latest_failure_reason == "forecast_model_missing_dependency":
            return f"rejected_blueprint_snapshot_unisolated_forecast_dependency:{row.get('candidate_id') or row.get('symbol') or 'candidate'}"
        if timing_state == "timing_constrained" and not _CURRENT_TYPED_CONSTRAINT_REASONS.intersection(timing_reasons):
            return f"rejected_blueprint_snapshot_untyped_timing_constraint:{row.get('candidate_id') or row.get('symbol') or 'candidate'}"
        if timing_state == "timing_fragile":
            has_current_fragile_evidence = bool({"path_fragility_current", "threshold_drift_weakening"}.intersection(timing_reasons))
            if not timing_valid or timing_schema_status != "current_schema" or latest_failure_reason or not has_current_fragile_evidence:
                return f"rejected_blueprint_snapshot_noncanonical_timing_fragile:{row.get('candidate_id') or row.get('symbol') or 'candidate'}"
    previous = dict(previous_contract or {})
    if not previous:
        return None
    previous_rows = _iter_explorer_candidate_rows(previous)
    if previous_rows:
        for row in previous_rows:
            if _missing_rich_row_fields(row):
                return None
        previous_blocked = _count_blocked_candidates(previous)
        candidate_blocked = _count_blocked_candidates(candidate_contract)
        if candidate_blocked > previous_blocked:
            return (
                "rejected_blueprint_snapshot_harsher_than_previous:"
                f"blocked_count:{previous_blocked}->{candidate_blocked}"
            )
        previous_by_id = {
            str(row.get("candidate_id") or row.get("symbol") or ""): row
            for row in previous_rows
        }
        for row in candidate_rows:
            candidate_key = str(row.get("candidate_id") or row.get("symbol") or "")
            previous_row = previous_by_id.get(candidate_key)
            if not previous_row:
                continue
            previous_completion_state = str(dict(previous_row.get("source_completion_summary") or {}).get("state") or "").strip().lower()
            candidate_completion_state = str(dict(row.get("source_completion_summary") or {}).get("state") or "").strip().lower()
            if previous_completion_state == "complete" and candidate_completion_state != "complete":
                return f"rejected_blueprint_snapshot_source_completion_regressed:{candidate_key}"
            previous_metrics = _source_completion_guard_metrics(previous_row)
            candidate_metrics = _source_completion_guard_metrics(row)
            if candidate_metrics["critical_fields_completed"] < previous_metrics["critical_fields_completed"]:
                return (
                    "rejected_blueprint_snapshot_critical_ready_regressed:"
                    f"{candidate_key}:{previous_metrics['critical_fields_completed']}->{candidate_metrics['critical_fields_completed']}"
                )
            for metric_name in ("weak_fields", "stale_fields", "conflict_fields"):
                if candidate_metrics[metric_name] > previous_metrics[metric_name]:
                    return (
                        f"rejected_blueprint_snapshot_{metric_name}_regressed:"
                        f"{candidate_key}:{previous_metrics[metric_name]}->{candidate_metrics[metric_name]}"
                    )
            if previous_row.get("market_path_support") is not None and row.get("market_path_support") is None and row.get("market_path") is None:
                return f"rejected_blueprint_snapshot_null_market_path:{candidate_key}"
            if previous_row.get("coverage_workflow_summary") is not None and row.get("coverage_workflow_summary") is None:
                return f"rejected_blueprint_snapshot_null_coverage_workflow:{candidate_key}"
    return None


_SCORE_SUMMARY_COMPONENTS: tuple[tuple[str, str], ...] = (
    ("implementation", "Implementation"),
    ("source_integrity", "Source integrity"),
    ("benchmark_fidelity", "Benchmark fidelity"),
    ("sleeve_fit", "Sleeve fit"),
    ("long_horizon_quality", "Long-horizon quality"),
    ("market_path_support", "Market-path support"),
    ("instrument_quality", "Instrument quality"),
    ("portfolio_fit", "Portfolio fit"),
)


def _score_tone(score: int | float | None) -> str:
    if score is None:
        return "neutral"
    value = float(score)
    if value >= 75:
        return "good"
    if value >= 55:
        return "warn"
    return "bad"


def _freshness_badge(freshness_state: str | None) -> dict[str, str]:
    raw = str(freshness_state or "").strip()
    label = {
        "no_data": "No holdings loaded",
        "fresh_full_rebuild": "Fresh full rebuild",
        "fresh_partial_rebuild": "Fresh partial rebuild",
        "stored_valid_context": "Stored valid context",
        "degraded_monitoring_mode": "Degraded monitoring mode",
        "execution_failed_or_incomplete": "Execution failed or incomplete",
    }.get(raw, raw.replace("_", " ").title() or "Unknown")
    tone = {
        "fresh_full_rebuild": "good",
        "fresh_partial_rebuild": "good",
        "stored_valid_context": "neutral",
        "current": "neutral",
        "degraded_monitoring_mode": "warn",
        "no_data": "warn",
        "execution_failed_or_incomplete": "bad",
    }.get(raw, "neutral")
    return {
        "label": label,
        "tone": tone,
    }


def _deployment_score_label(score: int | float | None) -> str:
    if score is None:
        return "Unavailable"
    value = float(score)
    if value >= 90:
        return "Best in sleeve"
    if value >= 80:
        return "Strong recommendation"
    if value >= 70:
        return "Reviewable leader"
    if value >= 55:
        return "Needs more proof"
    return "Not recommendation-ready"


def _candidate_score_summary(
    score_decomposition: dict[str, Any] | None,
    source_integrity_summary: dict[str, Any] | None,
    market_path_support: dict[str, Any] | None,
) -> dict[str, Any] | None:
    decomposition = dict(score_decomposition or {})
    component_map = {
        str(component.get("component_id") or ""): dict(component)
        for component in list(decomposition.get("components") or [])
        if str(component.get("component_id") or "").strip()
    }
    components: list[dict[str, Any]] = []
    scores: list[int] = []
    for component_id, fallback_label in _SCORE_SUMMARY_COMPONENTS:
        component = component_map.get(component_id)
        if not component:
            continue
        try:
            score = int(round(float(component.get("score"))))
        except (TypeError, ValueError):
            continue
        components.append(
            {
                "component_id": component_id,
                "label": str(component.get("label") or fallback_label),
                "score": score,
                "tone": _score_tone(score),
                "summary": str(component.get("summary") or ""),
            }
        )
        scores.append(score)
    if not scores:
        return None
    headline_score = decomposition.get("recommendation_score", decomposition.get("total_score"))
    try:
        average_score = int(round(float(headline_score)))
    except (TypeError, ValueError):
        average_score = int(round(sum(scores) / len(scores)))
    source_state = str(dict(source_integrity_summary or {}).get("state") or "").strip().lower()
    support = dict(market_path_support or {})
    market_usefulness = str(support.get("usefulness_label") or "").strip().lower()
    market_eligible = str(support.get("eligibility_state") or "").strip().lower() == "eligible"
    if len(scores) < 4 or source_state in {"weak", "conflicted", "missing"}:
        reliability_state = "weak"
    elif (
        len(scores) == len(_SCORE_SUMMARY_COMPONENTS)
        and source_state == "strong"
        and market_eligible
        and market_usefulness not in {"suppressed", "unstable", ""}
    ):
        reliability_state = "strong"
    else:
        reliability_state = "mixed"
    reliability_note: str | None = None
    if len(scores) < len(_SCORE_SUMMARY_COMPONENTS):
        reliability_note = f"Based on {len(scores)} of {len(_SCORE_SUMMARY_COMPONENTS)} score pillars."
    elif source_state in {"weak", "conflicted", "missing"}:
        reliability_note = "Recommendation score is still constrained by weak source support."
    elif source_state == "mixed":
        reliability_note = "Recommendation score is still constrained by mixed source support."
    elif not market_eligible or market_usefulness in {"suppressed", "unstable", ""}:
        reliability_note = "Market-path support is not strong enough to fully anchor the recommendation score."
    return {
        "average_score": average_score,
        "component_count_used": len(scores),
        "tone": _score_tone(average_score),
        "reliability_state": reliability_state,
        "reliability_note": reliability_note,
        "components": components,
    }


def _sleeve_recommendation_score(candidate_rows: list[dict[str, Any]], lead_row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not candidate_rows or not lead_row:
        return None

    def _recommendation_score(row: dict[str, Any]) -> float:
        decomposition = dict(row.get("score_decomposition") or {})
        value = decomposition.get("recommendation_score", decomposition.get("total_score", 0.0))
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _truth_confidence_score(row: dict[str, Any]) -> float:
        decomposition = dict(row.get("score_decomposition") or {})
        value = decomposition.get("truth_confidence_score", 0.0)
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _deployability_score(row: dict[str, Any]) -> float:
        decomposition = dict(row.get("score_decomposition") or {})
        value = decomposition.get("deployability_score", decomposition.get("deployment_score", 0.0))
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _investment_merit_score(row: dict[str, Any]) -> float:
        decomposition = dict(row.get("score_decomposition") or {})
        value = decomposition.get("investment_merit_score", decomposition.get("optimality_score", 0.0))
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _readiness_posture(row: dict[str, Any]) -> str:
        return str(dict(row.get("score_decomposition") or {}).get("readiness_posture") or "blocked").strip().lower()

    def _canonical_deployability_badge(row: dict[str, Any]) -> str:
        visible_state = str(dict(row.get("visible_decision_state") or {}).get("state") or "").strip().lower()
        if visible_state == "eligible":
            return "deploy_now"
        if visible_state in {"review", "watch"}:
            return "review_before_deploy"
        if visible_state == "research_only":
            return "research_only"
        if visible_state == "blocked":
            return "blocked"
        return str(dict(row.get("score_decomposition") or {}).get("deployability_badge") or "blocked").strip().lower()

    recommendation_scores = sorted((_recommendation_score(row) for row in candidate_rows), reverse=True)
    leader_candidate_recommendation_score = _recommendation_score(lead_row)
    leader_truth_confidence_score = _truth_confidence_score(lead_row)
    leader_candidate_deployability_score = _deployability_score(lead_row)
    leader_candidate_investment_merit_score = _investment_merit_score(lead_row)
    leader_posture = _readiness_posture(lead_row)
    leader_badge = _canonical_deployability_badge(lead_row)
    candidate_count = max(1, len(candidate_rows))
    action_ready_count = sum(1 for row in candidate_rows if _canonical_deployability_badge(row) == "deploy_now")
    review_only_count = sum(1 for row in candidate_rows if _canonical_deployability_badge(row) == "review_before_deploy")
    blocked_count = sum(1 for row in candidate_rows if _canonical_deployability_badge(row) == "blocked")
    unresolved_count = sum(1 for row in candidate_rows if _canonical_deployability_badge(row) in {"blocked", "research_only"})
    blocked_rate = blocked_count / candidate_count
    review_only_rate = review_only_count / candidate_count

    if len(recommendation_scores) >= 2 and recommendation_scores[0] >= 85 and recommendation_scores[1] >= 85:
        depth_score = 100
    elif len(recommendation_scores) >= 2 and recommendation_scores[0] >= 85 and recommendation_scores[1] >= 75:
        depth_score = 85
    elif len(recommendation_scores) >= 2 and recommendation_scores[0] >= 80 and recommendation_scores[1] >= 70:
        depth_score = 70
    elif recommendation_scores and recommendation_scores[0] >= 75:
        depth_score = 55
    else:
        depth_score = 40

    blocker_burden_score = max(20.0, 100.0 - 45.0 * blocked_rate - 20.0 * review_only_rate)
    raw_score = (
        0.55 * leader_candidate_recommendation_score
        + 0.20 * depth_score
        + 0.15 * leader_truth_confidence_score
        + 0.10 * blocker_burden_score
    )
    if action_ready_count == 0 and review_only_count == 0:
        raw_score = min(raw_score, 59.0)
    if leader_badge == "research_only":
        raw_score = min(raw_score, 69.0)
    if leader_truth_confidence_score < 60:
        raw_score = min(raw_score, 74.0)
    if action_ready_count == 0:
        raw_score = min(raw_score, 84.0)
    if unresolved_count > candidate_count / 2:
        raw_score = min(raw_score, 64.0)
    if sum(1 for score in recommendation_scores if score >= 70) < 2:
        raw_score = min(raw_score, 74.0)

    average_score = _clamp_score(raw_score, minimum=18, maximum=96)
    tone = _score_tone(average_score)
    return {
        "average_score": average_score,
        "pillar_count_used": 4,
        "factor_count_used": 4,
        "score_basis": "recommendation_score",
        "leader_candidate_recommendation_score": int(round(leader_candidate_recommendation_score)),
        "leader_truth_confidence_score": int(round(leader_truth_confidence_score)),
        "leader_candidate_deployability_score": int(round(leader_candidate_deployability_score)),
        "leader_candidate_investment_merit_score": int(round(leader_candidate_investment_merit_score)),
        "leader_candidate_deployment_score": int(round(leader_candidate_deployability_score)),
        "depth_score": int(round(depth_score)),
        "blocker_burden_score": int(round(blocker_burden_score)),
        "tone": tone,
        "label": _deployment_score_label(average_score),
    }


def _cached_quote_runtime_provenance(conn: sqlite3.Connection, symbol: str) -> dict[str, Any] | None:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return None
    for provider_name in routed_provider_candidates("quote_latest", identifier=normalized):
        supported, _ = provider_support_status(provider_name, "quote_latest", normalized)
        if not supported:
            continue
        snapshot = get_cached_provider_snapshot(
            conn,
            provider_name=provider_name,
            endpoint_family="quote_latest",
            cache_key=normalized,
            surface_name="blueprint",
        )
        if snapshot is None:
            continue
        payload = dict(snapshot.get("payload") or {})
        price = payload.get("price")
        if price is None:
            price = payload.get("value")
        previous_close = payload.get("previous_close")
        change_pct_1d = payload.get("change_pct_1d")
        usable_truth = price is not None
        sufficiency_state = (
            "movement_capable"
            if change_pct_1d is not None or previous_close is not None
            else "price_present"
            if usable_truth
            else "insufficient"
        )
        data_mode = "cache" if usable_truth else "unavailable"
        authority_level = "live_authoritative" if usable_truth else "unavailable"
        freshness = _coerce_freshness_payload(payload.get("freshness_state") or snapshot.get("freshness_state")).get("freshness_class")
        return runtime_provenance(
            source_family=str(payload.get("endpoint_family") or "quote_latest"),
            provider_used=str(payload.get("provider_name") or provider_name),
            path_used=str(payload.get("retrieval_path") or "routed_cache"),
            provider_execution={
                "provider_name": str(payload.get("provider_name") or provider_name),
                "source_family": str(payload.get("endpoint_family") or "quote_latest"),
                "path_used": str(payload.get("retrieval_path") or "routed_cache"),
                "live_or_cache": "cache",
                "usable_truth": usable_truth,
                "sufficiency_state": sufficiency_state,
                "data_mode": data_mode,
                "authority_level": authority_level,
                "observed_at": payload.get("observed_at") or payload.get("as_of_utc"),
                "provenance_strength": "cache_continuity" if usable_truth else "insufficient",
                "insufficiency_reason": None if usable_truth else "missing_cached_price",
            },
            truth_envelope=dict(payload.get("truth_envelope") or {}) or {
                "source_authority": str(payload.get("provider_name") or provider_name),
                "acquisition_mode": "cache",
            },
            freshness=str(freshness or "").strip() or None,
            authority_kind=authority_level,
            provenance_strength="cache_continuity" if usable_truth else "insufficient",
            insufficiency_reason=None if usable_truth else "missing_cached_price",
            derived_or_proxy=False,
        )
    return None


def _market_runtime_provenance(
    market: MarketSeriesTruth | None,
    *,
    fallback_provenance: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if market is None or not getattr(market, "evidence", None):
        facts = {}
    else:
        facts = dict((market.evidence[0].facts or {}))
    provider_execution = dict(facts.get("provider_execution") or {})
    freshness_state = _coerce_freshness_payload(facts.get("freshness_state"))
    truth_envelope = dict(facts.get("truth_envelope") or {})
    source_family = str(
        facts.get("source_family")
        or provider_execution.get("source_family")
        or ""
    ).strip() or None
    provider_used = str(
        facts.get("source_provider")
        or provider_execution.get("provider_name")
        or ""
    ).strip() or None
    path_used = str(
        facts.get("retrieval_path")
        or provider_execution.get("path_used")
        or ""
    ).strip() or None
    freshness = str(freshness_state.get("freshness_class") or "").strip() or None
    authority_kind = str(
        facts.get("authority_level")
        or provider_execution.get("authority_level")
        or ""
    ).strip() or None
    provenance = runtime_provenance(
        source_family=source_family,
        provider_used=provider_used,
        path_used=path_used,
        provider_execution=provider_execution or None,
        truth_envelope=truth_envelope or None,
        freshness=freshness,
        authority_kind=authority_kind,
        provenance_strength=str(provider_execution.get("provenance_strength") or "").strip() or None,
        insufficiency_reason=str(provider_execution.get("insufficiency_reason") or "").strip() or None,
        derived_or_proxy=str(facts.get("movement_state") or "").strip().lower() == "proxy",
    )
    if fallback_provenance and (
        not provenance.get("provider_used")
        or str(provenance.get("provider_used") or "").strip().lower() == "market_price"
    ):
        merged = dict(fallback_provenance)
        for key, value in provenance.items():
            if _meaningful_runtime_value(value):
                merged[key] = value
        return merged
    return provenance


def _benchmark_proxy_symbol(symbol: str) -> str | None:
    assignment = DEFAULT_BENCHMARK_ASSIGNMENTS.get(str(symbol or "").upper(), {})
    proxy = str(assignment.get("benchmark_proxy_symbol") or "").strip().upper()
    return proxy or None


def _lightweight_forecast_support(*, label: str, symbol: str) -> dict[str, Any]:
    return {
        "provider": "deterministic_baseline",
        "model_name": "explorer_fast_path",
        "horizon": 21,
        "support_strength": "support_only",
        "confidence_summary": f"{label} keeps lightweight forecast support in Blueprint explorer; use the full report for richer scenario detail.",
        "degraded_state": "explorer_fast_path",
        "generated_at": utc_now_iso(),
    }


def _try_load_cached_forecast_support(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    label: str,
    symbol: str,
) -> dict[str, Any] | None:
    try:
        cached = latest_forecast_artifact(conn, candidate_id=str(candidate_id))
        latest_run = _latest_forecast_run(conn, candidate_id=str(candidate_id))
        direct_summary = _series_quality_for_role(conn, candidate_id=str(candidate_id), series_role="direct")
        proxy_summary = _series_quality_for_role(conn, candidate_id=str(candidate_id), series_role="approved_proxy")
        raw_support = dict(cached.get("market_path_support") or {}) if cached and isinstance(cached.get("market_path_support"), dict) else None
        assessment = _timing_assessment(
            support=raw_support,
            cached=dict(cached or {}),
            latest_run=latest_run,
            direct_summary=direct_summary,
            proxy_summary=proxy_summary,
            symbol=symbol,
        )
        if raw_support is None or str(assessment.get("raw_artifact_schema_status") or "") != "current_schema":
            market_path_support = _constrained_market_path_support(
                candidate_id=str(candidate_id),
                label=label,
                symbol=symbol,
                source_support=raw_support,
                assessment=assessment,
            )
        else:
            market_path_support = _apply_timing_assessment(raw_support, assessment)
        forecast_support = compact_forecast_support_from_market_path(market_path_support) or _lightweight_forecast_support(
            label=label,
            symbol=symbol,
        )
        return {
            "forecast_support": forecast_support,
            "market_path_support": market_path_support,
        }
    except Exception:
        return None


def generate_current_timing_assessment_artifacts(conn: sqlite3.Connection) -> dict[str, Any]:
    """Persist current-schema route/history timing artifacts without running live providers or forecasts."""
    ensure_candidate_registry_tables(conn)
    rows = export_live_candidate_registry(conn)
    if not rows:
        seed_default_candidate_registry(conn)
        rows = export_live_candidate_registry(conn)
    generated = 0
    states: dict[str, int] = {}
    seen: set[str] = set()
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        candidate_id = f"candidate_instrument_{symbol.lower()}"
        if candidate_id in seen:
            continue
        seen.add(candidate_id)
        support_bundle = _try_load_cached_forecast_support(
            conn,
            candidate_id=candidate_id,
            label=str(row.get("name") or symbol),
            symbol=symbol,
        )
        support = dict((support_bundle or {}).get("market_path_support") or {})
        if not support:
            continue
        timing_state = str(support.get("timing_state") or "timing_constrained")
        states[timing_state] = states.get(timing_state, 0) + 1
        input_series_version = f"{_ROUTE_HISTORY_INPUT_VERSION_PREFIX}:{candidate_id}"
        forecast_run_id = record_forecast_run(
            conn,
            candidate_id=candidate_id,
            series_role=str(support.get("driving_series_role") or "direct"),
            model_name=_ROUTE_HISTORY_MODEL_NAME,
            model_version=_CURRENT_TIMING_SCHEMA_VERSION,
            input_series_version=input_series_version,
            run_status="ready" if timing_state in {"timing_ready", "timing_review"} else "unavailable" if timing_state == "timing_unavailable" else "constrained",
            usefulness_label=str(support.get("usefulness_label") or "usable_with_caution"),
            suppression_reason=support.get("suppression_reason"),
            details={
                "timing_state": timing_state,
                "timing_reasons": list(support.get("timing_reasons") or []),
                "timing_data_source": support.get("timing_data_source"),
                "validation_status": support.get("validation_status"),
                "forecast_output_required": False,
            },
        )
        persist_forecast_artifact(
            conn,
            forecast_run_id=forecast_run_id,
            candidate_id=candidate_id,
            input_series_version=input_series_version,
            market_path_support=support,
        )
        generated += 1
    return {
        "generated_artifacts": generated,
        "candidate_count": len(seen),
        "timing_state_counts": states,
        "schema_version": _CURRENT_TIMING_SCHEMA_VERSION,
    }


def _candidate_assessment(
    *,
    truth,
    interpretation_card,
    signal_strength: float,
    sleeve_id: str,
    sleeve_purpose: str,
) -> CandidateAssessment:
    supports = [signal.summary for signal in interpretation_card.signals if signal.direction in {"up", "positive"}]
    risks = [signal.summary for signal in interpretation_card.signals if signal.direction in {"down", "negative", "mixed"}]
    return CandidateAssessment(
        candidate_id=f"candidate_{truth.instrument_id}",
        sleeve_id=sleeve_id,
        instrument=truth,
        interpretation=interpretation_card,
        mandate_fit="aligned" if sleeve_purpose.lower() in interpretation_card.why_it_matters_here.lower() else "watch",
        conviction=0.55 if signal_strength >= 0.5 else 0.42,
        score_breakdown={"signal_strength": signal_strength},
        key_supports=supports[:3],
        key_risks=risks[:3],
    )


def _dominant_boundary(boundaries: list[PolicyBoundary]) -> PolicyBoundary:
    return next((boundary for boundary in boundaries if boundary.action_boundary == "blocked"), boundaries[0])


def _review_posture(state: str, overclaim_risk: str) -> str:
    if state == "blocked" or overclaim_risk == "high":
        return "act_now"
    if state == "review" or overclaim_risk == "medium":
        return "review_soon"
    return "monitor"


def _lead_priority(
    candidate: dict[str, Any], sleeve_key: str, assessment: CandidateAssessment
) -> tuple[int, float, float, str]:
    default_symbol = DEFAULT_SLEEVE_ASSIGNMENTS.get(sleeve_key, "").upper()
    symbol = str(candidate.get("symbol") or "").upper()
    liquidity_score = float(candidate.get("liquidity_score") or 0.0)
    expense_ratio = float(candidate.get("expense_ratio") or 99.0)
    return (
        1 if symbol == default_symbol else 0,
        assessment.conviction,
        liquidity_score,
        -expense_ratio,
    )


def _instrument_quality(mandate_fit: str, conviction: float) -> str:
    if mandate_fit == "aligned" and conviction >= 0.5:
        return "High"
    if mandate_fit == "watch":
        return "Moderate"
    return "Low"


def _portfolio_fit_now(state: str) -> str:
    if state == "eligible":
        return "Highest"
    if state in {"review", "watch"}:
        return "Good"
    return "Weak today"


def _capital_priority_now(state: str, conviction: float) -> str:
    if state == "eligible" and conviction >= 0.55:
        return "First call on next dollar"
    if state in {"eligible", "review"}:
        return "Second choice"
    return "No new capital"


def _status_label(state: str) -> str:
    return {
        "eligible": "Eligible now",
        "blocked": "Blocked by sleeve",
        "review": "Under review",
        "watch": "Watching",
        "research_only": "Research only",
    }.get(state, state.replace("_", " ").title())


def _what_changes_view_from_restraints(restraints: list) -> str:
    for restraint in restraints:
        if restraint.what_changes_view:
            return restraint.what_changes_view[0]
    return ""


def _action_boundary_from(boundaries: list[PolicyBoundary], fallback: str) -> str:
    for boundary in boundaries:
        if boundary.required_action:
            return boundary.required_action
    return fallback


def _visible_state_from_investor_state(investor_state: str, *, is_lead: bool) -> str:
    normalized = str(investor_state or "").strip()
    if normalized == "blocked":
        return "blocked"
    if normalized == "research_only":
        return "research_only"
    if normalized == "actionable":
        return "eligible" if is_lead else "review"
    if normalized == "shortlisted":
        return "review" if is_lead else "watch"
    return "watch"


def _allowed_action_from_visible_state(visible_state: str, *, is_lead: bool) -> str:
    if visible_state == "eligible":
        return "approve" if is_lead else "review"
    if visible_state == "review":
        return "review"
    if visible_state in {"watch", "research_only"}:
        return "monitor"
    return "none"


def _candidate_status_label(*, investor_state: str, is_lead: bool) -> str:
    normalized = str(investor_state or "").strip()
    if normalized == "blocked":
        return "Blocked candidate"
    if normalized == "research_only":
        return "Research only"
    if is_lead and normalized == "actionable":
        return "Lead candidate"
    if is_lead:
        return "Lead under review"
    if normalized == "actionable":
        return "Shortlisted alternative"
    return "Alternative candidate"


def _portfolio_fit_from_investor_state(investor_state: str) -> str:
    normalized = str(investor_state or "").strip()
    if normalized == "actionable":
        return "Highest"
    if normalized == "shortlisted":
        return "Good"
    return "Weak today"


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _expense_ratio_label(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "—"
    return f"{number:.2%}"


def _aum_label(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "—"
    if number >= 1_000_000_000:
        return f"${number / 1_000_000_000:.1f}B"
    if number >= 1_000_000:
        return f"${number / 1_000_000:.0f}M"
    return f"${number:,.0f}"


def _freshness_days_label(truth) -> int | None:
    as_of = str(getattr(truth, "as_of", "") or "").strip()
    if not as_of:
        return None
    try:
        parsed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    except Exception:
        return None
    now = datetime.now(UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    return max(0, (now - parsed).days)


def _candidate_score(assessment: CandidateAssessment, constraint_summary: ConstraintSummary, truth) -> int:
    expense_ratio = _safe_float(truth.metrics.get("expense_ratio") or truth.metrics.get("ter"))
    liquidity_score = _safe_float(truth.metrics.get("liquidity_score"))
    evidence_count = sum(len(pack.citations) for pack in truth.evidence)
    base = int(round(assessment.conviction * 100))
    if expense_ratio is not None:
        base += 6 if expense_ratio <= 0.0035 else 2 if expense_ratio <= 0.0075 else -4
    if liquidity_score is not None:
        base += int(max(0, min(10, liquidity_score * 10)))
    base += min(8, evidence_count * 2)
    if constraint_summary.visible_decision_state.state == "blocked":
        base -= 15
    elif constraint_summary.visible_decision_state.state == "eligible":
        base += 6
    return max(18, min(96, base))


def _market_path_support_component(market_path_support: dict[str, Any] | None) -> tuple[int, str, str]:
    component = market_path_support_component(market_path_support)
    return int(component.get("score") or 0), str(component.get("summary") or ""), str(component.get("tone") or "neutral")


def _enrich_score_decomposition(
    score_decomposition: dict[str, Any] | None,
    market_path_support: dict[str, Any] | None,
) -> dict[str, Any] | None:
    return enrich_score_decomposition_with_market_path_support(score_decomposition, market_path_support)


def _support_pillars(
    assessment: CandidateAssessment,
    constraint_summary: ConstraintSummary,
    truth,
) -> list[dict[str, Any]]:
    expense_ratio = _safe_float(truth.metrics.get("expense_ratio") or truth.metrics.get("ter"))
    liquidity_score = _safe_float(truth.metrics.get("liquidity_score"))
    evidence_count = sum(len(pack.citations) for pack in truth.evidence)
    macro_score = max(28, min(92, int(round(assessment.conviction * 100))))
    policy_score = 32 if constraint_summary.visible_decision_state.state == "blocked" else 58 if constraint_summary.visible_decision_state.state == "review" else 76
    valuation_score = 74 if expense_ratio is not None and expense_ratio <= 0.0035 else 58 if expense_ratio is not None else 42
    implementation_score = 68 if liquidity_score is not None and liquidity_score >= 0.7 else 54 if liquidity_score is not None else 38
    evidence_score = min(90, 36 + evidence_count * 12)
    return [
        {"pillar_id": "macro", "label": "Macro", "score": macro_score, "note": assessment.interpretation.why_it_matters_economically, "tone": "good" if macro_score >= 70 else "warn"},
        {"pillar_id": "policy", "label": "Policy", "score": policy_score, "note": constraint_summary.visible_decision_state.rationale, "tone": "good" if policy_score >= 70 else "warn" if policy_score >= 50 else "bad"},
        {"pillar_id": "valuation", "label": "Valuation", "score": valuation_score, "note": f"Expense ratio { _expense_ratio_label(expense_ratio) }." if expense_ratio is not None else "Current cost data is partial.", "tone": "good" if valuation_score >= 70 else "warn"},
        {"pillar_id": "implementation", "label": "Implementation", "score": implementation_score, "note": f"Liquidity proxy {liquidity_score:.2f}." if liquidity_score is not None else "Liquidity proxy is missing.", "tone": "good" if implementation_score >= 65 else "warn"},
        {"pillar_id": "evidence", "label": "Evidence", "score": evidence_score, "note": f"{evidence_count} cited support item{'s' if evidence_count != 1 else ''} in current truth.", "tone": "good" if evidence_score >= 68 else "warn"},
    ]


def _funding_path(lead_row: dict[str, Any], lead_state: str) -> dict[str, Any]:
    funding_source = lead_row["funding_source"] or (
        "New capital"
        if lead_state in {"eligible", "review"}
        else "No funding path while blocked"
    )
    return {
        "funding_source": funding_source,
        "incumbent_label": lead_row["name"] if lead_state == "blocked" else None,
        "action_boundary": lead_row["action_boundary"],
        "degraded_state": None,
        "summary": "Funding and incumbent context are fully mapped.",
    }


def _portfolio_overlay_context() -> tuple[object | None, bool, dict[str, list[dict[str, Any]]], dict[str, Any] | None]:
    try:
        portfolio = get_portfolio_truth("default")
    except Exception:
        return None, False, {}, None

    holdings = list(getattr(portfolio, "holdings", []) or [])
    if not holdings:
        return portfolio, False, {}, None

    holdings_by_sleeve: dict[str, list[dict[str, Any]]] = {}
    for holding in holdings:
        sleeve = str(holding.get("sleeve") or "").strip().lower()
        if not sleeve:
            continue
        holdings_by_sleeve.setdefault(sleeve, []).append(dict(holding))

    overlay_summary = {
        "portfolio_id": getattr(portfolio, "portfolio_id", None),
        "summary": (
            f"{len(holdings)} holding{'s' if len(holdings) != 1 else ''} are active in the current portfolio overlay. "
            "Blueprint remains candidate-first, and incumbent/funding context is now additive where mapped."
        ),
        "holding_count": len(holdings),
    }
    return portfolio, True, holdings_by_sleeve, overlay_summary


def _overlay_details_for_sleeve(
    sleeve_key: str,
    *,
    overlay_present: bool,
    holdings_by_sleeve: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if not overlay_present:
        return {
            "funding_source": "Funding path becomes specific once holdings are loaded",
            "incumbent_label": None,
            "degraded_state": "overlay_absent",
            "summary": (
                "Sleeve logic is already usable without holdings. Funding source and replacement detail become specific once the live book is loaded."
            ),
        }

    portfolio_sleeve = _PORTFOLIO_SLEEVE_ALIASES.get(sleeve_key, sleeve_key)
    sleeve_holdings = list(holdings_by_sleeve.get(portfolio_sleeve, []))
    incumbent = sleeve_holdings[0] if sleeve_holdings else None
    incumbent_label = str(incumbent.get("symbol") or incumbent.get("name") or "").strip() if incumbent else None
    if incumbent_label:
        return {
            "funding_source": f"Replace or trim {incumbent_label}",
            "incumbent_label": incumbent_label,
            "degraded_state": None,
            "summary": f"The current book identifies {incumbent_label} as the incumbent comparison line for this sleeve.",
        }

    return {
        "funding_source": "New capital or cross-sleeve rebalance",
        "incumbent_label": None,
        "degraded_state": "incumbent_unavailable",
        "summary": (
            "Holdings are loaded, but no current position is mapped to this sleeve. Treat sleeve fit and implementation quality as the main comparison frame."
        ),
    }


def _candidate_overlay_weight_pct(
    symbol: str,
    *,
    sleeve_key: str,
    overlay_present: bool,
    holdings_by_sleeve: dict[str, list[dict[str, Any]]],
) -> float | None:
    if not overlay_present:
        return None
    profile = get_ips_sleeve_profile(sleeve_key)
    portfolio_sleeve = profile.portfolio_sleeve_id if profile is not None else sleeve_key
    sleeve_holdings = list(holdings_by_sleeve.get(portfolio_sleeve, []))
    matched = next(
        (
            holding
            for holding in sleeve_holdings
            if str(holding.get("symbol") or "").strip().upper() == str(symbol or "").strip().upper()
        ),
        None,
    )
    if not matched:
        return None
    try:
        weight = float(matched.get("weight") or matched.get("weight_pct") or 0.0)
    except (TypeError, ValueError):
        return None
    if weight <= 1.0:
        return round(weight * 100.0, 2)
    return round(weight, 2)


def _candidate_weight_state(current_weight_pct: float | None, *, overlay_present: bool) -> str:
    if not overlay_present:
        return "overlay_absent"
    if current_weight_pct is None:
        return "not_held"
    if current_weight_pct <= 0:
        return "not_held"
    return "held"


def _portfolio_weight_labels(sleeve_key: str, portfolio: object | None) -> tuple[str, str]:
    profile = get_ips_sleeve_profile(sleeve_key)
    target_label = profile.target_label if profile is not None else "Target pending"
    if portfolio is None:
        return "Current weight pending overlay", target_label
    exposures = dict(getattr(portfolio, "exposures", {}) or {})
    exposure_key = profile.portfolio_sleeve_id if profile is not None else sleeve_key
    current = exposures.get(exposure_key)
    try:
        if current is None:
            return "0.0%", target_label
        return f"{float(current) * 100.0:.1f}%", target_label
    except (TypeError, ValueError):
        return "Current weight pending overlay", target_label


def _surface_market_summary(notes: list[str], sleeves: list[dict[str, Any]]) -> str:
    if not sleeves:
        return "Blueprint is preserving the sleeve workflow while candidate truth is unavailable."
    lead = sleeves[0]
    lead_name = str(lead.get("lead_candidate_name") or "Lead candidate")
    implication = str(lead.get("implication_summary") or "").strip()
    if implication:
        return f"{lead_name} currently leads the highest-priority sleeve. {implication}"
    if notes:
        return notes[0]
    return "Blueprint is hydrated, but the current market summary remains bounded."


def _aggregate_failure_class_summary(candidate_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    class_counts: dict[str, dict[str, Any]] = {}
    for row in candidate_rows:
        summary = dict(row.get("failure_class_summary") or {})
        for item in list(summary.get("items") or []):
            class_id = str(item.get("class_id") or "").strip()
            if not class_id:
                continue
            bucket = class_counts.setdefault(
                class_id,
                {
                    "class_id": class_id,
                    "label": str(item.get("label") or class_id.replace("_", " ").title()),
                    "severity": str(item.get("severity") or "confidence_drag"),
                    "summary": str(item.get("summary") or "").strip(),
                    "count": 0,
                    "fields": set(),
                },
            )
            bucket["count"] += 1
            bucket["fields"].update(str(field or "").strip() for field in list(item.get("fields") or []) if str(field or "").strip())
    if not class_counts:
        return None
    ordered = sorted(
        class_counts.values(),
        key=lambda item: (
            0 if item["severity"] == "block" else 1 if item["severity"] == "review" else 2,
            -int(item["count"]),
            str(item["class_id"]),
        ),
    )
    primary = ordered[0]
    return {
        "primary_class": primary["class_id"],
        "primary_label": primary["label"],
        "summary": primary["summary"],
        "hard_classes": [item["class_id"] for item in ordered if item["severity"] == "block"],
        "review_classes": [item["class_id"] for item in ordered if item["severity"] == "review"],
        "confidence_drag_classes": [item["class_id"] for item in ordered if item["severity"] == "confidence_drag"],
        "items": [
            {
                "class_id": item["class_id"],
                "label": item["label"],
                "severity": item["severity"],
                "summary": item["summary"],
                "count": int(item["count"]),
                "fields": sorted(item["fields"]),
            }
            for item in ordered
        ],
    }


def _aggregate_sleeve_posture(candidate_rows: list[dict[str, Any]], *, lead_candidate_id: str | None) -> dict[str, Any]:
    ready_count = sum(1 for row in candidate_rows if str(dict(row.get("visible_decision_state") or {}).get("state") or "") == "eligible")
    reviewable_count = sum(
        1
        for row in candidate_rows
        if str(dict(row.get("recommendation_gate") or {}).get("gate_state") or "") == "review_only"
        or str(dict(row.get("visible_decision_state") or {}).get("state") or "") in {"review", "watch"}
    )
    blocked_count = sum(1 for row in candidate_rows if str(dict(row.get("visible_decision_state") or {}).get("state") or "") == "blocked")
    active_support_candidate_count = sum(
        1
        for row in candidate_rows
        if str(dict(row.get("market_path_support") or {}).get("usefulness_label") or "").strip().lower() != "suppressed"
        and dict(row.get("market_path_support") or {}).get("eligibility_state")
    )
    bounded_count = sum(
        1
        for row in candidate_rows
        if list(dict(row.get("failure_class_summary") or {}).get("confidence_drag_classes") or [])
        or str(dict(row.get("market_path_support") or {}).get("usefulness_label") or "").strip().lower() in {"strong", "usable", "usable_with_caution", "unstable"}
    )
    lead_row = next((row for row in candidate_rows if row.get("candidate_id") == lead_candidate_id), candidate_rows[0] if candidate_rows else None)
    leader_is_blocked_but_sleeve_still_reviewable = bool(
        lead_row
        and str(dict(lead_row.get("visible_decision_state") or {}).get("state") or "") == "blocked"
        and (ready_count > 0 or reviewable_count > 0)
    )
    if ready_count > 0:
        sleeve_actionability_state = "ready"
        sleeve_reviewability_state = "reviewable"
    elif reviewable_count > 0:
        sleeve_actionability_state = "reviewable"
        sleeve_reviewability_state = "reviewable"
    elif bounded_count > 0 or active_support_candidate_count > 0:
        sleeve_actionability_state = "bounded"
        sleeve_reviewability_state = "bounded"
    else:
        sleeve_actionability_state = "blocked"
        sleeve_reviewability_state = "blocked"
    failure_class_summary = _aggregate_failure_class_summary(candidate_rows)
    if sleeve_actionability_state == "ready":
        sleeve_block_reason_summary = "At least one candidate is clean enough to move beyond review."
    elif sleeve_actionability_state == "reviewable":
        sleeve_block_reason_summary = (
            "The sleeve still has reviewable alternatives even though the current leader is not clean enough for action."
            if leader_is_blocked_but_sleeve_still_reviewable
            else "The sleeve still has candidates worth active review, but none are fully action-ready yet."
        )
    elif sleeve_actionability_state == "bounded":
        sleeve_block_reason_summary = "Support remains bounded, but every candidate still needs more work before the sleeve becomes reviewable."
    else:
        sleeve_block_reason_summary = (
            str(dict(failure_class_summary or {}).get("summary") or "").strip()
            or str((lead_row or {}).get("what_blocks_action") or "").strip()
            or "Every candidate still carries a hard block."
        )
    return {
        "sleeve_actionability_state": sleeve_actionability_state,
        "sleeve_reviewability_state": sleeve_reviewability_state,
        "sleeve_block_reason_summary": sleeve_block_reason_summary,
        "blocked_count": blocked_count,
        "reviewable_count": reviewable_count,
        "bounded_count": bounded_count,
        "ready_count": ready_count,
        "active_support_candidate_count": active_support_candidate_count,
        "leader_is_blocked_but_sleeve_still_reviewable": leader_is_blocked_but_sleeve_still_reviewable,
        "failure_class_summary": failure_class_summary,
    }


def _sleeve_visible_state_from_posture(sleeve_actionability_state: str) -> str:
    normalized = str(sleeve_actionability_state or "").strip().lower()
    if normalized == "ready":
        return "eligible"
    if normalized == "reviewable":
        return "review"
    if normalized == "bounded":
        return "watch"
    return "blocked"


def build(report_candidate_id: str | None = None, snapshot_override_reason: str | None = None) -> dict[str, object]:
    report_module = import_module("app.v2.surfaces.blueprint.report_contract_builder")
    report_cached_forecast_support = getattr(report_module, "_try_load_cached_forecast_support", None)
    use_report_cached_forecast_support = callable(report_cached_forecast_support) and getattr(
        report_cached_forecast_support,
        "__module__",
        "",
    ) != "app.v2.surfaces.blueprint.report_contract_builder"
    with _connection() as conn:
        ensure_candidate_registry_tables(conn)
        candidates = export_live_candidate_registry(conn)
        if not candidates:
            seed_default_candidate_registry(conn)
            candidates = export_live_candidate_registry(conn)
        portfolio, overlay_present, holdings_by_sleeve, overlay_summary = _portfolio_overlay_context()
        grouped: dict[str, list[dict[str, Any]]] = {}
        for candidate in candidates:
            grouped.setdefault(str(candidate.get("sleeve_key") or ""), []).append(candidate)

        sleeves: list[dict[str, object]] = []
        sleeve_market_notes: list[str] = []

        for profile in ordered_ips_sleeves(grouped.keys()):
            sleeve_key = profile.sleeve_key
            sleeve_candidates = [row for row in grouped[sleeve_key] if str(row.get("symbol") or "").strip()]
            if not sleeve_candidates:
                continue

            purpose = _sleeve_purpose(sleeve_key)
            sleeve_id = _sleeve_id(sleeve_key)
            default_symbol = str(DEFAULT_SLEEVE_ASSIGNMENTS.get(sleeve_key) or "").strip().upper()
            lead_candidate = next(
                (row for row in sleeve_candidates if str(row.get("symbol") or "").strip().upper() == default_symbol),
                sleeve_candidates[0],
            )
            sleeve_overlay = _overlay_details_for_sleeve(
                sleeve_key,
                overlay_present=overlay_present,
                holdings_by_sleeve=holdings_by_sleeve,
            )

            candidate_rows: list[dict[str, Any]] = []
            for candidate in sleeve_candidates:
                symbol = str(candidate.get("symbol") or "").strip().upper()
                candidate_id = f"candidate_instrument_{symbol.lower()}"
                name = str(candidate.get("name") or symbol or "Candidate").strip()
                issuer = str(candidate.get("issuer") or "Issuer not emitted").strip()
                truth = get_instrument_truth(candidate_id, allow_live_fetch=False)
                truth_context = build_candidate_truth_context(conn, {**candidate, "symbol": symbol, "sleeve_key": sleeve_key})
                implementation_profile = dict(truth_context.get("implementation_profile") or {})
                recommendation_gate = dict(truth_context.get("recommendation_gate") or {})
                reconciliation = dict(truth_context.get("reconciliation") or {})
                reconciliation_report = list(truth_context.get("reconciliation_report") or [])
                source_authority_fields = list(truth_context.get("source_authority_map") or [])
                data_quality_summary = dict(truth_context.get("data_quality") or {})
                primary_document_manifest = list(truth_context.get("primary_document_manifest") or [])
                institutional_facts = dict(truth_context.get("institutional_facts") or {})
                identity_state = dict(truth_context.get("identity_state") or {})
                source_integrity_summary = dict(truth_context.get("source_integrity_summary") or {})
                source_completion_summary = dict(truth_context.get("source_completion_summary") or {})
                investor_decision_state = str(truth_context.get("investor_decision_state") or "research_only")
                visible_decision_state = dict(truth_context.get("visible_decision_state") or {})
                blocker_category = truth_context.get("blocker_category")
                failure_class_summary = dict(truth_context.get("failure_class_summary") or {})
                score_decomposition = dict(truth_context.get("score_decomposition") or {})
                if use_report_cached_forecast_support:
                    cached_forecast = report_cached_forecast_support(candidate_id, label=name)
                else:
                    cached_forecast = _try_load_cached_forecast_support(
                        conn,
                        candidate_id=candidate_id,
                        label=name,
                        symbol=symbol,
                    )
                gate_state = str(recommendation_gate.get("gate_state") or "review_only")
                execution_suitability = str(implementation_profile.get("execution_suitability") or "execution_mixed")
                is_lead_candidate = symbol == default_symbol
                fallback_score = int(score_decomposition.get("total_score") or (84 if gate_state == "admissible" else 62 if gate_state == "review_only" else 34))
                status_label = _candidate_status_label(investor_state=investor_decision_state, is_lead=is_lead_candidate)
                overlay_summary_text = str(sleeve_overlay.get("summary") or "").strip()
                blocked_reasons = list(recommendation_gate.get("blocked_reasons") or [])
                capital_priority_now = (
                    "First call on next dollar"
                    if is_lead_candidate and investor_decision_state in {"actionable", "shortlisted"}
                    else "Second choice"
                    if investor_decision_state in {"actionable", "shortlisted"}
                    else "No new capital"
                )
                benchmark_proxy_symbol = _benchmark_proxy_symbol(symbol)
                primary_truth = _snapshot_market_truth(symbol)
                comparison_truth = _snapshot_market_truth(benchmark_proxy_symbol) if benchmark_proxy_symbol else None
                market_provenance = _market_runtime_provenance(
                    primary_truth,
                    fallback_provenance=_cached_quote_runtime_provenance(conn, symbol),
                )
                research_support_summary = build_research_support_summary(
                    drift_surface_id="candidate_report",
                    drift_object_id=candidate_id,
                    drift_state={
                        "gate_state": gate_state,
                        "data_confidence": str(data_quality_summary.get("data_confidence") or recommendation_gate.get("data_confidence") or "mixed"),
                        "reconciliation_status": str(reconciliation.get("status") or "verified"),
                        "blocked_reason_count": len(blocked_reasons),
                        "critical_missing_count": len(list(recommendation_gate.get("critical_missing_fields") or [])),
                    },
                )
                current_weight_pct = _candidate_overlay_weight_pct(
                    symbol,
                    sleeve_key=sleeve_key,
                    overlay_present=overlay_present,
                    holdings_by_sleeve=holdings_by_sleeve,
                )
                weight_state = _candidate_weight_state(current_weight_pct, overlay_present=overlay_present)
                market_path_support = dict(cached_forecast.get("market_path_support") or {}) if cached_forecast else None
                forecast_support = (
                    dict(cached_forecast.get("forecast_support") or {})
                    if cached_forecast
                    else _lightweight_forecast_support(label=name, symbol=symbol)
                )
                score_decomposition = _enrich_score_decomposition(score_decomposition, market_path_support)
                score = int(dict(score_decomposition or {}).get("total_score") or fallback_score)
                instrument_quality_score = dict(score_decomposition or {}).get("instrument_quality_score")
                portfolio_fit_score = dict(score_decomposition or {}).get("portfolio_fit_score")
                instrument_quality_label = (
                    "High"
                    if isinstance(instrument_quality_score, (int, float)) and float(instrument_quality_score) >= 80
                    else "Moderate"
                    if isinstance(instrument_quality_score, (int, float)) and float(instrument_quality_score) >= 62
                    else "Low"
                    if isinstance(instrument_quality_score, (int, float))
                    else "High"
                    if execution_suitability == "execution_efficient"
                    else "Moderate"
                    if execution_suitability == "execution_mixed"
                    else "Low"
                )
                portfolio_fit_now = (
                    "Highest"
                    if isinstance(portfolio_fit_score, (int, float)) and float(portfolio_fit_score) >= 82
                    else "Good"
                    if isinstance(portfolio_fit_score, (int, float)) and float(portfolio_fit_score) >= 65
                    else "Weak today"
                    if isinstance(portfolio_fit_score, (int, float))
                    else _portfolio_fit_from_investor_state(investor_decision_state)
                )
                score_decomposition = {
                    **dict(score_decomposition or {}),
                    "total_score": int(score),
                } if score_decomposition else None
                score_summary = _candidate_score_summary(
                    score_decomposition=score_decomposition,
                    source_integrity_summary=source_integrity_summary,
                    market_path_support=market_path_support,
                )
                coverage_summary = build_candidate_coverage_summary(
                    conn,
                    candidate,
                    truth_context,
                    candidate_id=candidate_id,
                    market_path_support=market_path_support,
                )
                score_rubric = build_score_rubric(
                    sleeve_key=sleeve_key,
                    score_decomposition=score_decomposition,
                )
                semantic_summary = build_candidate_summary_pack(
                    candidate_name=name,
                    investor_decision_state=investor_decision_state,
                    recommendation_gate=recommendation_gate,
                    score_decomposition=score_decomposition,
                    source_integrity_summary=source_integrity_summary,
                    coverage_summary=coverage_summary,
                    market_path_support=market_path_support,
                )
                market_path_summary = build_market_path_summary_pack(market_path_support)
                explanation = build_candidate_explorer_explanations(
                    candidate_name=name,
                    sleeve_key=sleeve_key,
                    sleeve_purpose=purpose,
                    is_lead_candidate=is_lead_candidate,
                    investor_decision_state=investor_decision_state,
                    blocker_category=str(blocker_category) if blocker_category is not None else None,
                    recommendation_gate=recommendation_gate,
                    failure_class_summary=failure_class_summary,
                    implementation_profile=implementation_profile,
                    institutional_facts=institutional_facts,
                    source_integrity_summary=source_integrity_summary,
                    identity_state=identity_state,
                    reconciliation=reconciliation,
                    overlay_summary=overlay_summary_text,
                )
                quick_brief_snapshot = _quick_brief(
                    truth=truth,
                    sleeve_key=sleeve_key,
                    sleeve_purpose=purpose,
                    visible_decision_state=visible_decision_state,
                    blocker_category=str(blocker_category) if blocker_category is not None else None,
                    recommendation_gate=recommendation_gate,
                    source_integrity_summary=source_integrity_summary,
                    market_path_support=market_path_support,
                    implementation_profile=implementation_profile,
                    source_authority_fields=source_authority_fields,
                    benchmark_full_name=institutional_facts.get("benchmark_full_name"),
                    exposure_summary=institutional_facts.get("exposure_summary"),
                    ter_bps=institutional_facts.get("ter_bps"),
                    aum_usd=institutional_facts.get("aum_usd"),
                    action_boundary=explanation["action_boundary"],
                    what_blocks_action=explanation["what_blocks_action"],
                    what_changes_view=explanation["what_changes_view"],
                    upgrade_condition=None,
                    kill_condition=None,
                    overlay_context=sleeve_overlay,
                    primary_document_manifest=primary_document_manifest,
                    freshness_state=get_freshness("market_price").freshness_class.value,
                    market=primary_truth,
                    forecast_support=forecast_support,
                )
                candidate_rows.append(
                    {
                        "candidate_id": candidate_id,
                        "sleeve_key": sleeve_key,
                        "symbol": symbol,
                        "name": name,
                        "score": score,
                        "benchmark_full_name": institutional_facts.get("benchmark_full_name"),
                        "exposure_summary": institutional_facts.get("exposure_summary"),
                        "ter_bps": institutional_facts.get("ter_bps"),
                        "spread_proxy_bps": institutional_facts.get("spread_proxy_bps"),
                        "aum_usd": institutional_facts.get("aum_usd"),
                        "aum_state": institutional_facts.get("aum_state"),
                        "sg_tax_posture": institutional_facts.get("sg_tax_posture"),
                        "distribution_policy": institutional_facts.get("distribution_policy"),
                        "replication_risk_note": institutional_facts.get("replication_risk_note"),
                        "current_weight_pct": current_weight_pct,
                        "weight_state": weight_state,
                        "investor_decision_state": investor_decision_state,
                        "source_integrity_summary": source_integrity_summary or None,
                        "source_completion_summary": source_completion_summary or None,
                        "score_decomposition": score_decomposition,
                        "score_summary": score_summary,
                        "identity_state": identity_state or None,
                        "blocker_category": blocker_category,
                        "failure_class_summary": failure_class_summary or None,
                        "issuer": issuer,
                        "expense_ratio": _expense_ratio_label(candidate.get("expense_ratio") or candidate.get("ter")),
                        "aum": str(implementation_profile.get("aum") or _aum_label(candidate.get("aum_usd") or candidate.get("aum")) or "—"),
                        "freshness_days": None,
                        "instrument_quality": instrument_quality_label,
                        "portfolio_fit_now": portfolio_fit_now,
                        "capital_priority_now": capital_priority_now,
                        "gate_state": gate_state,
                        "status_label": status_label,
                        "why_now": explanation["why_now"],
                        "what_blocks_action": explanation["what_blocks_action"],
                        "what_changes_view": explanation["what_changes_view"],
                        "action_boundary": explanation["action_boundary"],
                        "funding_source": sleeve_overlay["funding_source"],
                        "winner_reason": explanation["winner_reason"],
                        "loser_reason": explanation["loser_reason"],
                        "compare_eligibility": "compare_ready",
                        "forecast_support": forecast_support,
                        "market_path_support": market_path_support,
                        "market_path_objective": market_path_summary["market_path_objective"],
                        "market_path_case_note": market_path_summary["market_path_case_note"],
                        "scenario_readiness_note": (
                            "Market path support is available in bounded form for expanded candidate detail."
                            if cached_forecast and dict(cached_forecast.get("market_path_support") or {}).get("eligibility_state") == "eligible"
                            else "Scenario detail is abbreviated here; open the full report for the fuller path split."
                        ),
                        "flip_risk_note": "The view turns if benchmark support weakens, implementation quality slips, or a blocker clears on a stronger rival.",
                        "report_href": f"/blueprint/candidates/{candidate_id}/report",
                        "candidate_row_summary": semantic_summary["candidate_row_summary"],
                        "candidate_supporting_factors": semantic_summary["candidate_supporting_factors"],
                        "candidate_penalizing_factors": semantic_summary["candidate_penalizing_factors"],
                        "report_summary_strip": semantic_summary["report_summary_strip"],
                        "source_confidence_label": semantic_summary["source_confidence_label"],
                        "coverage_status": coverage_summary["coverage_status"],
                        "coverage_workflow_summary": coverage_summary["coverage_workflow_summary"],
                        "score_rubric": score_rubric,
                        "detail_chart_panels": [
                            comparison_chart_panel(
                                panel_id=f"blueprint_detail_chart_candidate_instrument_{symbol.lower()}",
                                title=f"{symbol} relative context",
                                primary_truth=primary_truth,
                                comparison_truth=comparison_truth,
                                summary=(
                                    f"{name} is shown against its benchmark proxy so the recent market context stays grounded."
                                    if comparison_truth and comparison_truth.points
                                    else f"{name} snapshot context is available, but the benchmark comparison is still limited."
                                ),
                                what_to_notice="Use this chart to judge the recent market context, not as a stand-alone buy or sell call.",
                                primary_label=symbol,
                                comparison_label=benchmark_proxy_symbol,
                                source_family="quote_latest",
                            )
                        ],
                        "quick_brief_snapshot": quick_brief_snapshot,
                        "report_snapshot": None,
                        "implementation_profile": implementation_profile,
                        "recommendation_gate": recommendation_gate,
                        "reconciliation_status": reconciliation,
                        "source_authority_fields": source_authority_fields,
                        "reconciliation_report": reconciliation_report,
                        "data_quality_summary": data_quality_summary,
                        "candidate_market_provenance": market_provenance,
                        "research_support_summary": research_support_summary,
                        "visible_decision_state": {
                            "state": str(visible_decision_state.get("state") or "watch"),
                            "allowed_action": str(visible_decision_state.get("allowed_action") or "monitor"),
                            "rationale": (
                                str(visible_decision_state.get("rationale") or "")
                                or str(recommendation_gate.get("summary") or identity_state.get("summary") or "")
                            ),
                        },
                        "implication_summary": explanation["implication_summary"],
                    }
                )

            lead_row = next((row for row in candidate_rows if row["symbol"] == default_symbol), candidate_rows[0])
            if str(dict(lead_row.get("visible_decision_state") or {}).get("state") or "") == "blocked":
                reviewable_or_better = [
                    row
                    for row in candidate_rows
                    if str(dict(row.get("visible_decision_state") or {}).get("state") or "") != "blocked"
                ]
                if reviewable_or_better:
                    lead_row = sorted(
                        reviewable_or_better,
                        key=lambda row: (
                            0 if str(dict(row.get("visible_decision_state") or {}).get("state") or "") == "eligible" else 1,
                            -int(dict(row.get("score_decomposition") or {}).get("total_score") or 0),
                            str(row.get("symbol") or ""),
                        ),
                    )[0]
            sleeve_posture = _aggregate_sleeve_posture(candidate_rows, lead_candidate_id=str(lead_row.get("candidate_id") or ""))
            sleeve_visible_state = _sleeve_visible_state_from_posture(str(sleeve_posture.get("sleeve_actionability_state") or "blocked"))
            current_weight_label, _ = _portfolio_weight_labels(sleeve_key, portfolio if overlay_present else None)
            lead_source_integrity = dict(lead_row.get("source_integrity_summary") or {})
            lead_score_components = list(dict(lead_row.get("score_decomposition") or {}).get("components") or [])
            lead_score_summary = dict(lead_row.get("score_summary") or {})
            source_integrity_component = next(
                (component for component in lead_score_components if str(component.get("component_id") or "") == "source_integrity"),
                None,
            )
            support_pillars = [
                {
                    "pillar_id": str(component.get("component_id") or "pillar"),
                    "label": str(component.get("label") or "Score"),
                    "score": int(component.get("score") or 0),
                    "note": str(component.get("summary") or ""),
                    "tone": str(component.get("tone") or "neutral"),
                }
                for component in list(lead_score_summary.get("components") or [])
            ] or [
                {"pillar_id": "implementation", "label": "Implementation", "score": int(dict(lead_row.get("implementation_profile") or {}).get("execution_score") or 52), "note": str(dict(lead_row.get("implementation_profile") or {}).get("summary") or "Implementation quality still needs review."), "tone": _score_tone(int(dict(lead_row.get("implementation_profile") or {}).get("execution_score") or 52))},
                {"pillar_id": "source_integrity", "label": "Source integrity", "score": int((source_integrity_component or {}).get("score") or 52), "note": str(lead_source_integrity.get("summary") or "Source integrity remains bounded."), "tone": _score_tone(int((source_integrity_component or {}).get("score") or 52))},
                {"pillar_id": "benchmark_fidelity", "label": "Benchmark fidelity", "score": int(dict(lead_row.get("score_decomposition") or {}).get("benchmark_fidelity_score") or 52), "note": str(dict(lead_row.get("identity_state") or {}).get("summary") or "Benchmark fidelity still needs a cleaner read."), "tone": _score_tone(int(dict(lead_row.get("score_decomposition") or {}).get("benchmark_fidelity_score") or 52))},
                {"pillar_id": "sleeve_fit", "label": "Sleeve fit", "score": int(dict(lead_row.get("score_decomposition") or {}).get("sleeve_fit_score") or 52), "note": str(dict(lead_row.get("recommendation_gate") or {}).get("summary") or "Sleeve fit remains bounded."), "tone": _score_tone(int(dict(lead_row.get("score_decomposition") or {}).get("sleeve_fit_score") or 52))},
                {"pillar_id": "long_horizon_quality", "label": "Long-horizon quality", "score": int(dict(lead_row.get("score_decomposition") or {}).get("long_horizon_quality_score") or 52), "note": "Long-horizon quality still needs a cleaner read.", "tone": _score_tone(int(dict(lead_row.get("score_decomposition") or {}).get("long_horizon_quality_score") or 52))},
                {"pillar_id": "instrument_quality", "label": "Instrument quality", "score": int(dict(lead_row.get("score_decomposition") or {}).get("instrument_quality_score") or 52), "note": "Instrument quality still needs a cleaner read.", "tone": _score_tone(int(dict(lead_row.get("score_decomposition") or {}).get("instrument_quality_score") or 52))},
                {"pillar_id": "portfolio_fit", "label": "Portfolio fit", "score": int(dict(lead_row.get("score_decomposition") or {}).get("portfolio_fit_score") or 52), "note": "Portfolio fit still needs a cleaner sleeve-level read.", "tone": _score_tone(int(dict(lead_row.get("score_decomposition") or {}).get("portfolio_fit_score") or 52))},
                {"pillar_id": "market_path_support", "label": "Market-path support", "score": int(dict(lead_row.get("score_decomposition") or {}).get("market_path_support_score") or 52), "note": str(dict(lead_row.get("market_path_support") or {}).get("candidate_implication") or "Market-path support is still bounded."), "tone": _score_tone(int(dict(lead_row.get("score_decomposition") or {}).get("market_path_support_score") or 52))},
            ]
            recommendation_score = _sleeve_recommendation_score(candidate_rows, lead_row)
            sleeves.append(
                {
                    "sleeve_id": sleeve_id,
                    "sleeve_name": profile.sleeve_name,
                    "sleeve_purpose": purpose,
                    "sleeve_role_statement": _sleeve_role_statement(sleeve_key),
                    "cycle_sensitivity": _cycle_sensitivity(sleeve_key),
                    "base_allocation_rationale": _base_allocation_rationale(sleeve_key),
                    "target_pct": profile.target_pct if profile.target_pct is not None else profile.sort_midpoint_pct,
                    "target_display": profile.target_display,
                    "min_pct": profile.min_pct,
                    "max_pct": profile.max_pct,
                    "sort_midpoint_pct": profile.sort_midpoint_pct,
                    "is_nested": profile.is_nested,
                    "parent_sleeve_id": profile.parent_sleeve_id,
                    "parent_sleeve_name": profile.parent_sleeve_name,
                    "counts_as_top_level_total": profile.counts_as_top_level_total,
                    "target_label": profile.target_label,
                    "range_label": profile.range_label,
                    "pressure_level": (
                        "high"
                        if sleeve_visible_state == "blocked"
                        else "medium"
                        if sleeve_visible_state in {"review", "watch"}
                        else "low"
                    ),
                    "capital_memo": (
                        f"{purpose} stays active because the sleeve job is still valid. "
                        + (
                            "The current leader is still blocked, but reviewable alternatives keep the sleeve open."
                            if bool(sleeve_posture.get("leader_is_blocked_but_sleeve_still_reviewable"))
                            else f"{lead_row['name']} currently sets the decision line for this sleeve."
                        )
                    ),
                    "priority_rank": profile.rank,
                    "current_weight": current_weight_label,
                    "target_weight": profile.target_label,
                    "candidate_count": len(candidate_rows),
                    "reopen_condition": lead_row["what_changes_view"],
                    "lead_candidate_id": lead_row["candidate_id"],
                    "lead_candidate_name": lead_row["name"],
                    "visible_state": sleeve_visible_state,
                    "sleeve_actionability_state": sleeve_posture["sleeve_actionability_state"],
                    "sleeve_reviewability_state": sleeve_posture["sleeve_reviewability_state"],
                    "sleeve_block_reason_summary": sleeve_posture["sleeve_block_reason_summary"],
                    "blocked_count": sleeve_posture["blocked_count"],
                    "reviewable_count": sleeve_posture["reviewable_count"],
                    "bounded_count": sleeve_posture["bounded_count"],
                    "ready_count": sleeve_posture["ready_count"],
                    "active_support_candidate_count": sleeve_posture["active_support_candidate_count"],
                    "leader_is_blocked_but_sleeve_still_reviewable": sleeve_posture["leader_is_blocked_but_sleeve_still_reviewable"],
                    "failure_class_summary": sleeve_posture["failure_class_summary"],
                    "implication_summary": lead_row["implication_summary"],
                    "why_it_leads": lead_row["why_now"],
                    "main_limit": str(sleeve_posture["sleeve_block_reason_summary"] or lead_row["what_blocks_action"]),
                    "recommendation_score": recommendation_score,
                    "support_pillars": support_pillars,
                    "funding_path": {
                        **_funding_path(lead_row, sleeve_visible_state),
                        "funding_source": sleeve_overlay["funding_source"],
                        "incumbent_label": sleeve_overlay["incumbent_label"],
                        "degraded_state": sleeve_overlay["degraded_state"],
                        "summary": sleeve_overlay["summary"],
                    },
                    "forecast_watch": lead_row["forecast_support"],
                    "candidates": candidate_rows,
                }
            )
            sleeve_market_notes.append(f"{_sleeve_label(sleeve_key)}: {lead_row['implication_summary']}")

    sleeves.sort(
        key=lambda row: (
            int(row.get("priority_rank") or 999),
            str(row.get("sleeve_id") or ""),
        )
    )
    active_candidate_count = sum(int(row.get("candidate_count") or len(list(row.get("candidates") or []))) for row in sleeves)
    active_support_candidate_count = sum(int(row.get("active_support_candidate_count") or 0) for row in sleeves)

    posture = "review_soon" if sleeves else "monitor"

    freshness = get_freshness("market_price").freshness_class.value
    generated_at = utc_now_iso()
    base_contract = {
        "contract_version": "0.3.3",
        "surface_id": "blueprint_explorer",
        "generated_at": generated_at,
        "freshness_state": freshness,
        "header_badges": [_freshness_badge(freshness)],
        "route_cache_state": build_default_route_cache_state(generated_at=generated_at, max_age_seconds=300),
        "surface_state": surface_state(
            "ready" if sleeves else "degraded",
            reason_codes=[] if sleeves else ["no_candidate_registry_rows"],
            summary="Blueprint Explorer is hydrated from candidate truth and deterministic rubric logic." if sleeves else "Blueprint Explorer is preserving layout with no sleeve rows.",
        ),
        "section_states": {
            "sleeve_map": ready_section() if sleeves else degraded_section("no_sleeves", "No sleeve rows were emitted."),
            "candidate_rows": ready_section() if sleeves else degraded_section("no_candidates", "No candidate rows were emitted."),
            "support_pillars": ready_section() if sleeves else degraded_section("no_support_pillars", "Support pillars are unavailable."),
            "compare_strip": ready_section(),
            "changes_feed": ready_section(),
            "report_entry": ready_section(),
        },
        "sleeves": sleeves,
        "summary": {
            "active_candidate_count": active_candidate_count,
            "active_support_candidate_count": active_support_candidate_count,
            "sleeve_count": len(sleeves),
        },
        "market_state_summary": _surface_market_summary(sleeve_market_notes, sleeves),
        "review_posture": posture,
    }
    final_contract = apply_overlay(base_contract, holdings=overlay_summary)
    previous_snapshot = latest_surface_snapshot(
        surface_id="blueprint_explorer",
        object_id="blueprint_explorer",
    )
    prior_snapshot = previous_surface_snapshot(
        surface_id="blueprint_explorer",
        object_id="blueprint_explorer",
    )
    previous_contract = dict(previous_snapshot.get("contract") or {}) if previous_snapshot else None
    snapshot_guard_reason = _blueprint_snapshot_guard_reason(
        previous_contract=previous_contract,
        candidate_contract=final_contract,
        override_reason=snapshot_override_reason,
    )
    if snapshot_guard_reason and previous_snapshot:
        guarded_contract = dict(previous_contract or {})
        guarded_contract["surface_snapshot_id"] = str(previous_snapshot.get("snapshot_id") or guarded_contract.get("surface_snapshot_id") or "")
        guarded_contract["snapshot_guard_state"] = {
            "state": "rejected_new_snapshot",
            "reason": snapshot_guard_reason,
            "previous_snapshot_id": str(previous_snapshot.get("snapshot_id") or ""),
        }
        return guarded_contract
    emit_explorer_snapshot_changes(
        dict(prior_snapshot.get("contract") or {}) if prior_snapshot else previous_contract,
        final_contract,
    )
    snapshot_id = record_surface_snapshot(
        surface_id="blueprint_explorer",
        object_id="blueprint_explorer",
        snapshot_kind="blueprint_state",
        state_label=posture,
        data_confidence="high" if sleeves else "low",
        decision_confidence="medium",
        generated_at=str(final_contract.get("generated_at") or ""),
        contract=final_contract,
        input_summary={
            "sleeve_count": len(sleeves),
            "overlay_present": bool(overlay_summary),
            "lead_candidates": [str(sleeve.get("lead_candidate_name") or "") for sleeve in sleeves[:5]],
        },
        decision_inputs=compact_replay_inputs(
            extra={
                "snapshot_override_reason": snapshot_override_reason,
                "overlay_summary": dict(overlay_summary or {}),
                "sleeves": [
                    {
                        "sleeve_id": str(sleeve.get("sleeve_id") or ""),
                        "lead_candidate_name": str(sleeve.get("lead_candidate_name") or ""),
                        "funding_path": dict(sleeve.get("funding_path") or {}),
                        "lead_candidate_gate": dict((list(sleeve.get("candidates") or [{}])[0]).get("recommendation_gate") or {}),
                        "lead_candidate_quality": dict((list(sleeve.get("candidates") or [{}])[0]).get("data_quality_summary") or {}),
                    }
                    for sleeve in sleeves[:8]
                ],
            },
        ),
    )
    final_contract["surface_snapshot_id"] = snapshot_id
    return final_contract
