from __future__ import annotations

import json
import logging
import os
import re
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from app.config import Settings
from app.services.blueprint_candidate_registry import (
    active_candidate_universe_summary,
    export_live_candidate_registry,
    list_live_candidate_symbols,
)
from app.services.blueprint_candidate_truth import (
    compute_candidate_completeness,
    resolve_candidate_field_truth,
    upsert_field_observation,
)
from app.services.data_governance import (
    _parse_dt,
    build_governance_record,
    consistency_warning,
    error_family,
    summarize_surface_sufficiency,
)
from app.services.external_upstreams import _persist_public_context, build_external_upstream_payload, build_provider_status_registry
from app.services.portfolio_ingest import latest_snapshot_rows
from app.services.provider_adapters import ProviderAdapterError, fetch_provider_data
from app.services.provider_budget import (
    ensure_provider_budget_tables,
    get_provider_budget_state,
    list_provider_health,
    list_surface_budget_policies,
    list_surface_snapshot_versions,
    peek_surface_budget_state,
    peek_provider_budget_state,
    record_provider_health,
    record_provider_usage,
    upsert_surface_snapshot_version,
)
from app.services.provider_cache import (
    get_cached_provider_snapshot,
    list_provider_snapshots,
    put_provider_snapshot,
)
from app.services.provider_family_success import (
    compare_family_providers,
    ensure_provider_family_success_tables,
    list_provider_family_success,
    recompute_provider_family_success,
    record_provider_family_event,
)
from app.services.public_upstream_snapshots import public_upstream_health_summary
from app.services.provider_registry import (
    DATA_FAMILY_ROUTING,
    PROVIDER_CAPABILITY_MATRIX,
    canonical_blueprint_family_id,
    family_ownership_map,
    provider_family_config,
    provider_support_status,
    provider_supports_family,
    routed_provider_candidates,
    surface_families,
)
from app.services.provider_activation import build_provider_activation_report
from app.services.upstream_health_report import enforce_registry_parity
from app.services.symbol_resolution import (
    ensure_symbol_resolution_tables,
    record_resolution_failure,
    record_resolution_success,
    resolve_provider_identifiers,
    seed_symbol_resolution_registry,
)
from app.v2.core.market_strip_registry import daily_brief_targets
from app.v2.sources.execution_envelope import build_provider_execution, payload_execution_profile
from app.v2.sources.runtime_truth import record_runtime_truth, runtime_truth_rows
from app.v2.sources.source_policy import blueprint_targets_from_policy
from app.v2.sources.registry import get_freshness_registry

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _now() -> datetime:
    return datetime.now(UTC)


def _trace_enabled() -> bool:
    return os.getenv("IA_TRACE_MARKET_QUOTES", "").strip() == "1"


def _trace(event: str, **fields: Any) -> None:
    if not _trace_enabled():
        return
    logger.info("MARKET_QUOTE_TRACE %s", json.dumps({"event": event, **fields}, sort_keys=True, default=str))


def _family_ttl_seconds(provider_name: str, endpoint_family: str) -> int:
    config = provider_family_config(provider_name, endpoint_family)
    return max(300, int(config.get("cadence_seconds") or 1800))


def _canonical_family_name(family: str) -> str:
    return canonical_blueprint_family_id(family)


def _provider_runtime_configured(provider_name: str) -> bool:
    env_name = {
        "finnhub": "FINNHUB_API_KEY",
        "alpha_vantage": "ALPHA_VANTAGE_API_KEY",
        "polygon": "POLYGON_API_KEY",
        "eodhd": "EODHD_API_KEY",
        "tiingo": "TIINGO_API_KEY",
        "twelve_data": "TWELVE_DATA_API_KEY",
        "fmp": "FMP_API_KEY",
    }.get(str(provider_name or ""))
    if env_name is None:
        return True
    return bool(os.getenv(env_name, "").strip())


def _family_cost_unit(provider_name: str, endpoint_family: str) -> float:
    config = provider_family_config(provider_name, endpoint_family)
    budget_class = str(config.get("budget_class") or "medium")
    return {
        "low": 0.5,
        "medium": 1.0,
        "high": 2.0,
    }.get(budget_class, 1.0)


def _family_age_seconds(observed_at: Any) -> float | None:
    observed = _parse_dt(observed_at)
    if observed is None:
        return None
    return max(0.0, (_now() - observed).total_seconds())


def _should_skip_for_budget(provider_name: str, endpoint_family: str, surface_name: str, mode: str) -> tuple[bool, str | None]:
    if mode == "blocked":
        return True, "provider budget blocked"
    if mode == "normal":
        return False, None
    priority = str(provider_family_config(provider_name, endpoint_family).get("priority") or "secondary")
    critical_families = {
        "daily_brief": {"market_close", "benchmark_proxy", "fx", "fx_reference", "quote_latest", "usd_strength_fallback"},
        "dashboard": {"quote_latest", "fx", "benchmark_proxy"},
        "blueprint": {"quote_latest", "reference_meta", "fundamentals", "ohlcv_history", "etf_profile", "etf_holdings"},
    }.get(surface_name, set())
    if mode == "critical_only" and endpoint_family not in critical_families:
        return True, "provider budget in critical-only mode"
    if mode == "conserve" and priority not in {"primary", "secondary"}:
        return True, "provider budget in conserve mode"
    return False, None


def _record_retrieval_runtime(
    *,
    provider_name: str | None,
    endpoint_family: str,
    identifier: str,
    surface_name: str,
    path_used: str,
    live_or_cache: str,
    usable_truth: bool,
    freshness: str | None,
    insufficiency_reason: str | None,
    semantic_grade: str | None,
    status_reason: str | None,
    attempt_succeeded: bool | None,
    configured: bool = True,
) -> None:
    record_runtime_truth(
        source_id=f"routed_provider:{endpoint_family}",
        source_family=endpoint_family,
        field_name=endpoint_family,
        symbol_or_entity=str(identifier),
        provider_used=provider_name,
        path_used=path_used,
        live_or_cache=live_or_cache,
        usable_truth=usable_truth,
        freshness=freshness,
        insufficiency_reason=insufficiency_reason,
        semantic_grade=semantic_grade,
        investor_surface=surface_name,
        status_reason=status_reason,
        attempt_succeeded=attempt_succeeded,
        configured=configured,
    )
    if provider_name:
        record_runtime_truth(
            source_id=f"provider_runtime:{provider_name}:{endpoint_family}",
            source_family=endpoint_family,
            field_name=endpoint_family,
            symbol_or_entity=str(identifier),
            provider_used=provider_name,
            path_used=path_used,
            live_or_cache=live_or_cache,
            usable_truth=usable_truth,
            freshness=freshness,
            insufficiency_reason=insufficiency_reason,
            semantic_grade=semantic_grade,
            investor_surface=surface_name,
            status_reason=status_reason,
            attempt_succeeded=attempt_succeeded,
            configured=configured,
        )


def _status_rank(value: str) -> int:
    order = {
        "recommendation_ready": 5,
        "review_ready": 4,
        "enough_for_portfolio_relevance": 4,
        "enough_for_holdings_freshness": 4,
        "enough_for_interpretation": 3,
        "enough_for_benchmark_watch": 3,
        "enough_for_monitoring": 2,
        "not_enough_for_full_confidence": 1,
        "blocked_by_missing_critical_data": 0,
    }
    return order.get(str(value), 0)


def _downgrade_status_once(status: str) -> str:
    mapping = {
        "recommendation_ready": "review_ready",
        "review_ready": "blocked_by_missing_critical_data",
        "enough_for_portfolio_relevance": "enough_for_interpretation",
        "enough_for_holdings_freshness": "enough_for_benchmark_watch",
        "enough_for_interpretation": "enough_for_monitoring",
        "enough_for_benchmark_watch": "not_enough_for_full_confidence",
    }
    return mapping.get(str(status), str(status))


def _cache_key(surface_name: str, endpoint_family: str, identifier: str) -> str:
    return f"{surface_name}:{endpoint_family}:{identifier}".upper()


def _is_blank_value(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _scalar_value(value: Any) -> Any | None:
    if isinstance(value, (dict, list, tuple, set)):
        return None
    return None if _is_blank_value(value) else value


def _normalize_payload(
    provider_name: str,
    endpoint_family: str,
    identifier: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    value = payload.get("value")
    scalar_value = _scalar_value(value)
    current_value = scalar_value if scalar_value is not None else payload.get("price") or payload.get("close") or payload.get("c")
    previous_close = payload.get("previous_close") or payload.get("previousClose") or payload.get("pc")
    absolute_change = payload.get("absolute_change") or payload.get("change") or payload.get("d")
    change_pct_1d = payload.get("change_pct_1d") or payload.get("changePercent") or payload.get("percent_change") or payload.get("changesPercentage") or payload.get("dp")
    open_value = payload.get("open") or payload.get("o")
    if _is_blank_value(absolute_change) and not _is_blank_value(current_value) and previous_close not in {None, "", 0, 0.0}:
        try:
            absolute_change = float(current_value) - float(previous_close)
        except Exception:
            absolute_change = None
    if _is_blank_value(change_pct_1d):
        try:
            if not _is_blank_value(current_value) and previous_close not in {None, "", 0, 0.0}:
                change_pct_1d = ((float(current_value) - float(previous_close)) / abs(float(previous_close))) * 100.0
            elif not _is_blank_value(current_value) and open_value not in {None, "", 0, 0.0}:
                change_pct_1d = ((float(current_value) - float(open_value)) / abs(float(open_value))) * 100.0
        except Exception:
            change_pct_1d = None
    observed_at = str(payload.get("observed_at") or _now_iso())
    source_ref = str(payload.get("source_ref") or f"{provider_name}:{endpoint_family}")
    provider_label = str(PROVIDER_CAPABILITY_MATRIX.get(provider_name, {}).get("label") or provider_name)
    return {
        "provider_name": provider_name,
        "source_name": provider_label,
        "provider_family": endpoint_family,
        "endpoint_family": endpoint_family,
        "identifier": identifier,
        "value": value if not _is_blank_value(value) else current_value,
        "price": current_value,
        "current_value": current_value,
        "previous_close": previous_close,
        "absolute_change": absolute_change,
        "change_pct_1d": change_pct_1d,
        "open": open_value,
        "observed_at": observed_at,
        "source_ref": source_ref,
        "series": payload.get("series"),
        "raw_value": payload.get("value"),
        "summary": f"{identifier} via {provider_label}",
    }


def _attach_provider_execution(
    payload: dict[str, Any],
    *,
    provider_name: str | None,
    endpoint_family: str,
    identifier: str,
    cache_status: str | None,
    error_state: str | None,
    path_used: str,
) -> dict[str, Any]:
    enriched = dict(payload)
    execution_profile = payload_execution_profile(payload=enriched, source_family=endpoint_family)
    freshness = str(
        enriched.get("freshness_state")
        or ((enriched.get("governance") or {}).get("freshness_state") if isinstance(enriched.get("governance"), dict) else None)
        or ("stored_valid_context" if str(cache_status or "") in {"hit", "stale_reuse"} else "current")
    )
    execution = build_provider_execution(
        provider_name=provider_name,
        source_family=endpoint_family,
        identifier=identifier,
        provider_symbol=str(enriched.get("provider_symbol") or identifier or "").strip() or None,
        observed_at=str(enriched.get("observed_at") or "").strip() or None,
        fetched_at=str(enriched.get("fetched_at") or "").strip() or None,
        cache_status=cache_status,
        fallback_used=bool(enriched.get("fallback_used")),
        error_state=str(error_state or enriched.get("error_state") or "").strip() or None,
        freshness_class=freshness,
        path_used=path_used,
        live_or_cache="cache" if str(cache_status or "") in {"hit", "stale_reuse"} else "fallback" if "fallback" in path_used or "unavailable" in path_used else "live",
        usable_truth=bool(execution_profile.get("usable_truth")),
        semantic_grade=str(execution_profile.get("semantic_grade") or "").strip() or None,
        sufficiency_state=str(execution_profile.get("sufficiency_state") or "").strip() or None,
        data_mode=str(execution_profile.get("data_mode") or "").strip() or None,
        authority_level=str(execution_profile.get("authority_level") or "").strip() or None,
        provenance_strength=str(enriched.get("provenance_strength") or "").strip() or None,
        insufficiency_reason=str(error_state or enriched.get("error_state") or "").strip() or None,
    )
    enriched["provider_execution"] = execution
    enriched["usable_truth"] = bool(execution.get("usable_truth"))
    enriched["sufficiency_state"] = execution.get("sufficiency_state")
    enriched["data_mode"] = execution.get("data_mode")
    enriched["authority_level"] = execution.get("authority_level")
    enriched["provenance_strength"] = execution.get("provenance_strength")
    return enriched


def _fresher_item(left: dict[str, Any] | None, right: dict[str, Any]) -> dict[str, Any]:
    if left is None:
        return right
    left_dt = _parse_dt(left.get("observed_at")) or _parse_dt(left.get("fetched_at"))
    right_dt = _parse_dt(right.get("observed_at")) or _parse_dt(right.get("fetched_at"))
    if left_dt is None:
        return right
    if right_dt is None:
        return left
    return right if right_dt >= left_dt else left


def _apply_governance(
    payload: dict[str, Any],
    *,
    provider_name: str,
    endpoint_family: str,
    fetched_at: Any = None,
    cache_status: str | None = None,
    fallback_used: bool = False,
    error_state: str | None = None,
    provider_diversity: int = 1,
    specificity_score: float = 0.0,
) -> dict[str, Any]:
    enriched = dict(payload)
    config = provider_family_config(provider_name, endpoint_family)
    enriched["governance"] = build_governance_record(
        source_name=str(enriched.get("source_name") or PROVIDER_CAPABILITY_MATRIX.get(provider_name, {}).get("label") or provider_name),
        provider_family=str(enriched.get("provider_family") or endpoint_family),
        fetched_at=fetched_at or enriched.get("fetched_at"),
        observed_at=enriched.get("observed_at"),
        cadence_seconds=int(config.get("cadence_seconds") or 1800),
        fallback_used=bool(fallback_used),
        cache_status=cache_status or enriched.get("cache_status"),
        error_state=error_state or enriched.get("error_state"),
        source_tier=str(PROVIDER_CAPABILITY_MATRIX.get(provider_name, {}).get("confidence_tier") or "secondary"),
        provider_diversity=provider_diversity,
        specificity_score=specificity_score,
    )
    enriched["coverage_status"] = enriched["governance"]["coverage_status"]
    return enriched


def fetch_with_cache(
    conn,
    *,
    provider_name: str,
    endpoint_family: str,
    identifier: str,
    surface_name: str,
    triggered_by_job: str,
    force_refresh: bool = False,
) -> dict[str, Any]:
    _trace(
        "provider_refresh.fetch_with_cache.begin",
        surface_name=surface_name,
        endpoint_family=endpoint_family,
        identifier=identifier,
        provider_name=provider_name,
        triggered_by_job=triggered_by_job,
        force_refresh=force_refresh,
    )
    ensure_provider_budget_tables(conn)
    ensure_symbol_resolution_tables(conn)
    seed_symbol_resolution_registry(conn)
    ttl_seconds = _family_ttl_seconds(provider_name, endpoint_family)
    key = _cache_key(surface_name, endpoint_family, identifier)
    if not force_refresh:
        cached = get_cached_provider_snapshot(
            conn,
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            cache_key=key,
            surface_name=surface_name,
            max_age_seconds=ttl_seconds,
        )
        if cached is not None:
            cached_payload = dict(cached.get("payload") or {})
            cached_provider_symbol = str(
                cached_payload.get("provider_symbol")
                or cached_payload.get("resolved_symbol")
                or identifier
            ).strip().upper()
            canonical_identifier = str(identifier or "").strip().upper()
            if not (
                provider_name == "yahoo_finance"
                and endpoint_family == "market_close"
                and canonical_identifier.startswith("^")
                and cached_provider_symbol != canonical_identifier
            ):
                _trace(
                    "provider_refresh.cache.hit",
                    surface_name=surface_name,
                    endpoint_family=endpoint_family,
                    identifier=identifier,
                    provider_name=provider_name,
                    cache_key=key,
                    fetched_at=cached.get("fetched_at"),
                    freshness_state=cached.get("freshness_state"),
                    fallback_used=bool(cached.get("fallback_used")),
                )
                payload = cached_payload
                payload.update(
                    {
                        "cache_status": "hit",
                        "provider_name": provider_name,
                        "endpoint_family": endpoint_family,
                        "identifier": identifier,
                        "fetched_at": cached.get("fetched_at"),
                        "freshness_state": cached.get("freshness_state"),
                        "confidence_tier": cached.get("confidence_tier"),
                        "fallback_used": bool(cached.get("fallback_used")),
                    }
                )
                payload = _attach_provider_execution(
                    payload,
                    provider_name=provider_name,
                    endpoint_family=endpoint_family,
                    identifier=identifier,
                    cache_status="hit",
                    error_state=str(payload.get("error_state") or cached.get("error_state") or "").strip() or None,
                    path_used="routed_cache",
                )
                _record_retrieval_runtime(
                    provider_name=provider_name,
                    endpoint_family=endpoint_family,
                    identifier=identifier,
                    surface_name=surface_name,
                    path_used="routed_cache",
                    live_or_cache="cache",
                    usable_truth=bool(payload.get("usable_truth")),
                    freshness=str(payload.get("provider_execution", {}).get("freshness_class") or payload.get("freshness_state") or cached.get("freshness_state") or ""),
                    insufficiency_reason=str(payload.get("provider_execution", {}).get("insufficiency_reason") or payload.get("error_state") or "") or None,
                    semantic_grade=str(payload.get("provider_execution", {}).get("semantic_grade") or "cached_context"),
                    status_reason=str(payload.get("error_state") or "") or "cache_hit",
                    attempt_succeeded=True,
                )
                return _apply_governance(
                    payload,
                    provider_name=provider_name,
                    endpoint_family=endpoint_family,
                    fetched_at=cached.get("fetched_at"),
                    cache_status="hit",
                    fallback_used=bool(cached.get("fallback_used")),
                    error_state=cached.get("error_state"),
                )
            _trace(
                "provider_refresh.cache.skip_invalid_index_proxy",
                surface_name=surface_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                provider_name=provider_name,
                cache_key=key,
                cached_provider_symbol=cached_provider_symbol,
            )

    provider_budget = get_provider_budget_state(conn, provider_name)
    surface_budget = peek_surface_budget_state(conn, provider_name, surface_name)
    mode = str(surface_budget.get("mode") or provider_budget.get("mode") or "normal")
    skip, reason = _should_skip_for_budget(provider_name, endpoint_family, surface_name, mode)
    if skip:
        _trace(
            "provider_refresh.budget.skip",
            surface_name=surface_name,
            endpoint_family=endpoint_family,
            identifier=identifier,
            provider_name=provider_name,
            budget_mode=mode,
            fallback_reason=reason,
        )
        stale = get_cached_provider_snapshot(
            conn,
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            cache_key=key,
            surface_name=surface_name,
            max_age_seconds=None,
            allow_expired=True,
        )
        if stale is not None:
            _trace(
                "provider_refresh.cache.stale_reuse",
                surface_name=surface_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                provider_name=provider_name,
                fetched_at=stale.get("fetched_at"),
                fallback_reason=reason,
            )
            payload = dict(stale.get("payload") or {})
            payload.update(
                {
                    "cache_status": "stale_reuse",
                    "freshness_state": "stale",
                    "budget_mode": mode,
                    "budget_reason": reason,
                }
            )
            payload = _attach_provider_execution(
                payload,
                provider_name=provider_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                cache_status="stale_reuse",
                error_state=str(reason or payload.get("error_state") or "").strip() or None,
                path_used="routed_cache",
            )
            _record_retrieval_runtime(
                provider_name=provider_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                surface_name=surface_name,
                path_used="routed_cache",
                live_or_cache="cache",
                usable_truth=bool(payload.get("usable_truth")),
                freshness="stale",
                insufficiency_reason=str(payload.get("provider_execution", {}).get("insufficiency_reason") or reason or payload.get("error_state") or "") or None,
                semantic_grade=str(payload.get("provider_execution", {}).get("semantic_grade") or "stale_reuse"),
                status_reason=str(reason or payload.get("error_state") or "") or "stale_reuse",
                attempt_succeeded=True,
            )
            return _apply_governance(
                payload,
                provider_name=provider_name,
                endpoint_family=endpoint_family,
                fetched_at=stale.get("fetched_at"),
                cache_status="stale_reuse",
                fallback_used=True,
                error_state=reason,
            )
        raise ProviderAdapterError(provider_name, endpoint_family, reason or "budget skip", error_class="budget_block")

    resolution = resolve_provider_identifiers(
        conn,
        provider_name=provider_name,
        endpoint_family=endpoint_family,
        identifier=identifier,
    )
    last_exc: ProviderAdapterError | None = None
    tried_identifiers = [str(resolution.get("provider_symbol") or identifier), *list(resolution.get("fallback_aliases") or [])]
    _trace(
        "provider_refresh.symbol_resolution",
        surface_name=surface_name,
        endpoint_family=endpoint_family,
        identifier=identifier,
        provider_name=provider_name,
        symbol_mapping_succeeded=True,
        provider_symbol=resolution.get("provider_symbol"),
        fallback_aliases=resolution.get("fallback_aliases"),
        resolution_confidence=resolution.get("resolution_confidence"),
        resolution_reason=resolution.get("resolution_reason"),
    )
    used_identifier = str(identifier)
    try:
        raw = None
        for candidate_identifier in tried_identifiers:
            try:
                _trace(
                    "provider_refresh.live_request_attempt",
                    surface_name=surface_name,
                    endpoint_family=endpoint_family,
                    identifier=identifier,
                    provider_name=provider_name,
                    provider_symbol=candidate_identifier,
                    live_request_attempted=True,
                )
                raw = fetch_provider_data(provider_name, endpoint_family, candidate_identifier)
                used_identifier = candidate_identifier
                _trace(
                    "provider_refresh.live_request_succeeded",
                    surface_name=surface_name,
                    endpoint_family=endpoint_family,
                    identifier=identifier,
                    provider_name=provider_name,
                    provider_symbol=candidate_identifier,
                    result="succeeded" if raw.get("value") is not None else "payload_unusable",
                    fallback_reason=None if raw.get("value") is not None else "provider_responded_but_payload_unusable",
                )
                break
            except ProviderAdapterError as exc:
                last_exc = exc
                _trace(
                    "provider_refresh.live_request_failed",
                    surface_name=surface_name,
                    endpoint_family=endpoint_family,
                    identifier=identifier,
                    provider_name=provider_name,
                    provider_symbol=candidate_identifier,
                    result="failed",
                    error_class=exc.error_class,
                    fallback_reason=str(exc),
                )
                record_resolution_failure(
                    conn,
                    canonical_symbol=str(identifier),
                    provider_name=provider_name,
                    endpoint_family=endpoint_family,
                    provider_symbol=candidate_identifier,
                    error_class=exc.error_class,
                )
                if exc.error_class not in {"not_found", "empty_response"}:
                    raise
                continue
        if raw is None:
            _trace(
                "provider_refresh.live_request_exhausted",
                surface_name=surface_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                provider_name=provider_name,
                result="failed",
                error_class=(last_exc.error_class if last_exc else "empty_response"),
                fallback_reason=str(last_exc) if last_exc else "No provider payload returned",
            )
            raise last_exc or ProviderAdapterError(provider_name, endpoint_family, "No provider payload returned", error_class="empty_response")
        normalized = _normalize_payload(provider_name, endpoint_family, identifier, raw)
        normalized["provider_symbol"] = used_identifier
        normalized["resolution"] = resolution
        normalized["symbol_profile_summary"] = dict(resolution.get("symbol_profile_summary") or {})
        normalized["provider_identifier_strategy"] = resolution.get("provider_identifier_strategy")
        normalized["fallback_used"] = bool(used_identifier != str(resolution.get("provider_symbol") or identifier))
        normalized = _attach_provider_execution(
            normalized,
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            identifier=identifier,
            cache_status="miss",
            error_state=None,
            path_used="routed_live",
        )
        put_provider_snapshot(
            conn,
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            cache_key=key,
            payload=normalized,
            surface_name=surface_name,
            freshness_state="fresh",
            confidence_tier=str(PROVIDER_CAPABILITY_MATRIX.get(provider_name, {}).get("confidence_tier") or "secondary"),
            source_ref=str(normalized.get("source_ref") or ""),
            ttl_seconds=ttl_seconds,
            cache_status="miss",
            fallback_used=bool(normalized["fallback_used"]),
            error_state=None,
        )
        record_resolution_success(
            conn,
            canonical_symbol=str(identifier),
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            provider_symbol=used_identifier,
            fallback_aliases=list(resolution.get("fallback_aliases") or []),
            resolution_confidence=float(resolution.get("resolution_confidence") or 0.75),
            resolution_reason="verified_runtime",
        )
        record_provider_usage(
            conn,
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            estimated_cost_unit=_family_cost_unit(provider_name, endpoint_family),
            success=True,
            triggered_by_job=triggered_by_job,
            triggered_by_surface=surface_name,
            cache_hit=False,
        )
        record_provider_health(conn, provider_name=provider_name, success=True)
        normalized["cache_status"] = "miss"
        normalized["budget_mode"] = mode
        _record_retrieval_runtime(
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            identifier=identifier,
            surface_name=surface_name,
            path_used="routed_live",
            live_or_cache="live",
            usable_truth=bool(normalized.get("usable_truth")),
            freshness=str(normalized.get("freshness_state") or "fresh"),
            insufficiency_reason=None,
            semantic_grade=str(normalized.get("provider_execution", {}).get("semantic_grade") or "field_present"),
            status_reason="live_selected",
            attempt_succeeded=True,
        )
        _trace(
            "provider_refresh.fetch_with_cache.selected_live",
            surface_name=surface_name,
            endpoint_family=endpoint_family,
            identifier=identifier,
            provider_name=provider_name,
            provider_symbol=used_identifier,
            fallback_used=bool(normalized["fallback_used"]),
            observed_at=normalized.get("observed_at"),
        )
        return _apply_governance(
            normalized,
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            fetched_at=_now_iso(),
            cache_status="miss",
            fallback_used=bool(normalized["fallback_used"]),
            error_state=None,
        )
    except ProviderAdapterError as exc:
        _trace(
            "provider_refresh.fetch_with_cache.failed",
            surface_name=surface_name,
            endpoint_family=endpoint_family,
            identifier=identifier,
            provider_name=provider_name,
            result="failed",
            error_class=exc.error_class,
            fallback_reason=str(exc),
        )
        record_provider_usage(
            conn,
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            estimated_cost_unit=_family_cost_unit(provider_name, endpoint_family),
            success=False,
            triggered_by_job=triggered_by_job,
            triggered_by_surface=surface_name,
            cache_hit=False,
        )
        record_provider_health(conn, provider_name=provider_name, success=False, error=f"{exc.error_class}: {exc}")
        raise
    except Exception as exc:  # noqa: BLE001
        record_provider_usage(
            conn,
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            estimated_cost_unit=_family_cost_unit(provider_name, endpoint_family),
            success=False,
            triggered_by_job=triggered_by_job,
            triggered_by_surface=surface_name,
            cache_hit=False,
        )
        record_provider_health(conn, provider_name=provider_name, success=False, error=f"provider_error: {exc}")
        raise ProviderAdapterError(provider_name, endpoint_family, str(exc), error_class="provider_error") from exc


def fetch_routed_family(
    conn,
    *,
    surface_name: str,
    endpoint_family: str,
    identifier: str,
    triggered_by_job: str,
    force_refresh: bool = False,
) -> dict[str, Any]:
    last_error: str | None = None
    provider_candidates = routed_provider_candidates(endpoint_family, identifier=identifier)[:4]
    _trace(
        "provider_refresh.fetch_routed_family.begin",
        surface_name=surface_name,
        endpoint_family=endpoint_family,
        identifier=identifier,
        provider_candidates=provider_candidates,
        force_refresh=force_refresh,
    )
    for provider_name in provider_candidates:
        supports_family, support_reason = provider_support_status(provider_name, endpoint_family, identifier)
        env_name = {
            "finnhub": "FINNHUB_API_KEY",
            "alpha_vantage": "ALPHA_VANTAGE_API_KEY",
            "polygon": "POLYGON_API_KEY",
            "eodhd": "EODHD_API_KEY",
            "tiingo": "TIINGO_API_KEY",
            "twelve_data": "TWELVE_DATA_API_KEY",
            "fmp": "FMP_API_KEY",
        }.get(provider_name)
        key_present = bool(env_name and os.getenv(env_name, "").strip())
        provider_configured = _provider_runtime_configured(provider_name)
        _trace(
            "provider_refresh.provider.evaluate",
            surface_name=surface_name,
            endpoint_family=endpoint_family,
            identifier=identifier,
            provider_name=provider_name,
            enabled=provider_name in provider_candidates,
            supports_family=supports_family,
            support_reason=support_reason,
            expected_env=env_name,
            api_key_present=key_present,
        )
        if not supports_family:
            _record_retrieval_runtime(
                provider_name=provider_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                surface_name=surface_name,
                path_used="routed_skipped",
                live_or_cache="fallback",
                usable_truth=False,
                freshness="unavailable",
                insufficiency_reason=support_reason or "provider_does_not_support_family",
                semantic_grade="unsupported",
                status_reason=support_reason or "provider_does_not_support_family",
                attempt_succeeded=False,
                configured=provider_configured,
            )
            _trace(
                "provider_refresh.provider.skipped",
                surface_name=surface_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                provider_name=provider_name,
                result="skipped",
                fallback_reason=support_reason or "provider_does_not_support_family",
            )
            continue
        try:
            payload = fetch_with_cache(
                conn,
                provider_name=provider_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                surface_name=surface_name,
                triggered_by_job=triggered_by_job,
                force_refresh=force_refresh,
            )
            if payload.get("cache_status") in {"stale_reuse"}:
                payload["fallback_used"] = True
            payload["retrieval_path"] = "routed_cache" if str(payload.get("cache_status") or "") in {"hit", "stale_reuse"} else "routed_live"
            payload = _attach_provider_execution(
                payload,
                provider_name=provider_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                cache_status=str(payload.get("cache_status") or "").strip() or None,
                error_state=str(payload.get("error_state") or "").strip() or None,
                path_used=str(payload.get("retrieval_path") or "routed_live"),
            )
            _trace(
                "provider_refresh.provider.selected",
                surface_name=surface_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                provider_name=provider_name,
                cache_status=payload.get("cache_status"),
                fallback_used=payload.get("fallback_used"),
                error_state=payload.get("error_state"),
                freshness_state=payload.get("freshness_state"),
                observed_at=payload.get("observed_at"),
            )
            return payload
        except ProviderAdapterError as exc:
            last_error = f"{provider_name}:{exc.error_class}"
            _record_retrieval_runtime(
                provider_name=provider_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                surface_name=surface_name,
                path_used="routed_live",
                live_or_cache="live",
                usable_truth=False,
                freshness="unavailable",
                insufficiency_reason=str(exc),
                semantic_grade="provider_failed",
                status_reason=f"{provider_name}:{exc.error_class}",
                attempt_succeeded=False,
                configured=key_present,
            )
            _trace(
                "provider_refresh.provider.failed",
                surface_name=surface_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                provider_name=provider_name,
                result="failed",
                error_class=exc.error_class,
                fallback_reason=str(exc),
            )
            continue
    _trace(
        "provider_refresh.fetch_routed_family.unavailable",
        surface_name=surface_name,
        endpoint_family=endpoint_family,
        identifier=identifier,
        result="unavailable",
        fallback_reason=last_error or "no_provider_available",
    )
    _record_retrieval_runtime(
        provider_name=None,
        endpoint_family=endpoint_family,
        identifier=identifier,
        surface_name=surface_name,
        path_used="routed_unavailable",
        live_or_cache="fallback",
        usable_truth=False,
        freshness="unavailable",
        insufficiency_reason=last_error or "no_provider_available",
        semantic_grade="unavailable",
        status_reason=last_error or "no_provider_available",
        attempt_succeeded=False,
    )
    unavailable_payload = {
        "provider_name": None,
        "source_name": None,
        "provider_family": endpoint_family,
        "endpoint_family": endpoint_family,
        "identifier": identifier,
        "cache_status": "unavailable",
        "error_state": last_error or "no_provider_available",
        "freshness_state": "unavailable",
        "governance": build_governance_record(
            source_name=None,
            provider_family=endpoint_family,
            fetched_at=None,
            observed_at=None,
            cadence_seconds=1800,
            fallback_used=False,
            cache_status="unavailable",
            error_state=last_error or "no_provider_available",
            source_tier="secondary",
        ),
    }
    return _attach_provider_execution(
        unavailable_payload,
        provider_name=None,
        endpoint_family=endpoint_family,
        identifier=identifier,
        cache_status="unavailable",
        error_state=last_error or "no_provider_available",
        path_used="routed_unavailable",
    )


def _required_provider_successes(surface_name: str, endpoint_family: str) -> int:
    if surface_name == "daily_brief" and endpoint_family in {"quote_latest", "benchmark_proxy"}:
        return 2
    if surface_name == "dashboard" and endpoint_family in {"quote_latest", "benchmark_proxy"}:
        return 2
    if surface_name == "blueprint" and endpoint_family in {"reference_meta", "ohlcv_history"}:
        return 2
    return 1


_RETRY_WORTHY_RESCUE_CAUSES = {
    "unsupported_symbol",
    "no_data_for_symbol",
    "no_eligible_route",
    "empty_response",
    "provider_identifier_kind_unsupported",
    "provider_region_unsupported",
    "provider_symbol_family_unsupported",
}

_BUDGET_OR_PLAN_TERMINAL_CAUSES = {
    "provider_budget_block",
    "quota_exhausted",
    "plan_limit",
    "auth_failure",
}


def _normalized_family_error_cause(error_class: str | None, message: str | None = None) -> str:
    normalized = str(error_class or "").strip()
    lowered = str(message or "").strip().lower()
    if normalized == "budget_block":
        parsed = error_family(message)
        if parsed == "plan_limited":
            return "plan_limit"
        if parsed == "endpoint_blocked":
            return "auth_failure"
        if parsed == "rate_limited":
            return "quota_exhausted"
        return "provider_budget_block"
    mapping = {
        "rate_limited": "rate_limit",
        "no_data_for_symbol": "unsupported_symbol",
        "provider_identifier_kind_unsupported": "unsupported_symbol",
        "provider_region_unsupported": "no_eligible_route",
        "provider_symbol_family_unsupported": "no_eligible_route",
        "empty_response": "empty_payload",
        "invalid_payload": "invalid_payload",
        "timeout": "remote_timeout",
        "remote_timeout": "remote_timeout",
        "auth_failure": "auth_failure",
        "endpoint_blocked": "auth_failure",
        "manual_quarantine": "manual_quarantine",
        "all_routes_quarantined": "all_routes_quarantined",
        "no_eligible_route": "no_eligible_route",
    }
    if normalized in mapping:
        return mapping[normalized]
    if "rate limit" in lowered or "429" in lowered:
        return "rate_limit"
    if "timeout" in lowered:
        return "remote_timeout"
    if "empty" in lowered:
        return "empty_payload"
    return normalized or "unknown_failure"


def _provider_attempt_eligibility(
    row: dict[str, Any],
    *,
    provider_name: str,
    endpoint_family: str,
    identifier: str,
) -> dict[str, Any]:
    supported, reason = provider_support_status(provider_name, endpoint_family, identifier)
    if not supported:
        terminal = "unsupported_symbol" if str(reason or "") == "unsupported_symbol" else "no_eligible_route"
        role = "ineligible_symbol_support" if terminal == "unsupported_symbol" else "ineligible_route_shape"
        return {
            "provider_name": provider_name,
            "current_role": role,
            "should_attempt_now": False,
            "reason": str(reason or terminal),
            "terminal_cause": terminal,
        }

    state = str(row.get("current_terminal_state") or "").strip()
    cause = str(
        row.get("current_terminal_cause")
        or row.get("last_root_error_class")
        or row.get("last_effective_error_class")
        or ""
    ).strip()
    effective = str(row.get("last_effective_error_class") or "").strip()

    if state == "quarantined":
        return {
            "provider_name": provider_name,
            "current_role": "quarantined",
            "should_attempt_now": False,
            "reason": cause or "manual_quarantine",
            "terminal_cause": "all_routes_quarantined" if "quarant" in cause else (cause or "manual_quarantine"),
        }
    if cause in _BUDGET_OR_PLAN_TERMINAL_CAUSES or (effective == "budget_block" and not cause):
        role = "ineligible_plan_limit" if cause in {"plan_limit", "auth_failure"} else "ineligible_budget_block"
        return {
            "provider_name": provider_name,
            "current_role": role,
            "should_attempt_now": False,
            "reason": cause or effective or "provider_budget_block",
            "terminal_cause": cause or "provider_budget_block",
        }
    if cause == "unsupported_symbol":
        return {
            "provider_name": provider_name,
            "current_role": "ineligible_symbol_support",
            "should_attempt_now": False,
            "reason": cause,
            "terminal_cause": cause,
        }
    if cause == "no_eligible_route":
        return {
            "provider_name": provider_name,
            "current_role": "ineligible_route_shape",
            "should_attempt_now": False,
            "reason": cause,
            "terminal_cause": cause,
        }
    if state == "stale_context_only":
        return {
            "provider_name": provider_name,
            "current_role": "stale_context_only",
            "should_attempt_now": True,
            "reason": cause or "stale_only",
            "terminal_cause": "stale_only",
        }
    return {
        "provider_name": provider_name,
        "current_role": "eligible_current",
        "should_attempt_now": True,
        "reason": cause or "eligible",
        "terminal_cause": cause or None,
    }


def _ranked_provider_candidates(conn, *, surface_name: str, endpoint_family: str) -> list[str]:
    static_order = list(DATA_FAMILY_ROUTING.get(endpoint_family, []))
    success_rows = list_provider_family_success(conn, surface_name=surface_name, family_name=_canonical_family_name(endpoint_family))
    by_provider = {str(item.get("provider_name") or ""): item for item in success_rows}

    def _score(provider_name: str) -> tuple[float, int, float]:
        row = by_provider.get(provider_name) or {}
        tier = str(row.get("current_tier") or "")
        terminal_cause = str(row.get("current_terminal_cause") or row.get("last_root_error_class") or "")
        tier_weight = {
            "primary_active": 3,
            "secondary_active": 2,
            "backup_only": 1,
            "disabled_for_family": -5,
            "disabled_for_surface": -10,
        }.get(tier, 0)
        if terminal_cause in _BUDGET_OR_PLAN_TERMINAL_CAUSES:
            tier_weight -= 12
        elif terminal_cause == "stale_only":
            tier_weight -= 1
        reliability = float(row.get("reliability_score") or 0.0)
        freshness = -float(row.get("median_freshness_seconds") or 10**12)
        # Explicitly demote Alpha Vantage on weak benchmark/history families.
        if provider_name == "alpha_vantage" and endpoint_family in {"benchmark_proxy", "ohlcv_history"}:
            empty_count = int(row.get("empty_response_count") or 0)
            if empty_count >= 2 or tier == "disabled_for_family":
                tier_weight -= 6
            else:
                tier_weight += 2 if endpoint_family == "ohlcv_history" else 1
        if provider_name == "alpha_vantage" and endpoint_family == "quote_latest":
            tier_weight -= 2
        if provider_name == "fmp" and endpoint_family == "quote_latest":
            tier_weight -= 10
        if provider_name == "eodhd" and endpoint_family in {"quote_latest", "benchmark_proxy", "fx"}:
            tier_weight -= 12
        # Finnhub forex is plan-limited on this key.
        if provider_name == "finnhub" and endpoint_family == "fx":
            tier_weight -= 8
        if provider_name == "twelve_data" and endpoint_family == "fx":
            tier_weight += 4
        if provider_name == "twelve_data" and endpoint_family == "benchmark_proxy":
            tier_weight -= 2
        if provider_name == "twelve_data" and endpoint_family == "ohlcv_history":
            tier_weight -= 8
        if provider_name == "polygon" and endpoint_family == "ohlcv_history":
            tier_weight -= 4
        if provider_name == "tiingo" and endpoint_family == "benchmark_proxy":
            tier_weight -= 1
        if provider_name == "polygon" and endpoint_family in {"quote_latest", "benchmark_proxy"}:
            last_error_class = str(row.get("last_error_class") or "")
            rate_limited_count = int(row.get("rate_limited_count") or 0)
            empty_count = int(row.get("empty_response_count") or 0)
            if "rate_limited" in last_error_class or rate_limited_count >= 2:
                tier_weight -= 5
            if empty_count >= 2:
                tier_weight -= 3
        return (reliability, tier_weight, freshness)

    ranked = sorted(
        [provider for provider in static_order if provider_supports_family(provider, endpoint_family)],
        key=_score,
        reverse=True,
    )
    return ranked


def _collect_family_payloads(
    conn,
    *,
    surface_name: str,
    endpoint_family: str,
    identifiers: list[str],
    triggered_by_job: str,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    ensure_provider_family_success_tables(conn)
    canonical_family = _canonical_family_name(endpoint_family)
    if not identifiers:
        recompute_provider_family_success(conn, surface_name=surface_name, family_name=canonical_family)
        return []
    target_successes = _required_provider_successes(surface_name, endpoint_family)
    results: list[dict[str, Any]] = []
    provider_successes: set[str] = set()

    def _bounded_provider_candidates(identifier: str) -> tuple[list[str], list[dict[str, Any]]]:
        ranked = _ranked_provider_candidates(conn, surface_name=surface_name, endpoint_family=endpoint_family)
        routed = routed_provider_candidates(endpoint_family, identifier=identifier) if identifier else list(ranked)
        success_rows = list_provider_family_success(conn, surface_name=surface_name, family_name=canonical_family)
        by_provider = {str(item.get("provider_name") or ""): item for item in success_rows}
        ordered: list[str] = []
        diagnostics: list[dict[str, Any]] = []
        for provider_name in routed:
            normalized = str(provider_name or "").strip()
            if not normalized:
                continue
            verdict = _provider_attempt_eligibility(
                dict(by_provider.get(normalized) or {}),
                provider_name=normalized,
                endpoint_family=endpoint_family,
                identifier=str(identifier or ""),
            )
            diagnostics.append(verdict)
            if verdict["should_attempt_now"]:
                ordered.append(normalized)
            if len(ordered) >= 2:
                break
        return ordered[:2], diagnostics

    eligible_route_count = 0
    no_eligible_identifiers: list[str] = []
    ineligible_identifier_causes: list[str] = []
    for identifier in identifiers:
        identifier_succeeded = False
        fallback_stale_payload: dict[str, Any] | None = None
        identifier_candidates, identifier_diagnostics = _bounded_provider_candidates(str(identifier))
        if not identifier_candidates:
            no_eligible_identifiers.append(str(identifier))
            ineligible_identifier_causes.extend(
                [
                    str(item.get("terminal_cause") or "")
                    for item in identifier_diagnostics
                    if str(item.get("terminal_cause") or "").strip()
                ]
            )
            continue
        eligible_route_count += len(identifier_candidates)
        for index, provider_name in enumerate(identifier_candidates):
            if provider_name == "polygon" and endpoint_family == "ohlcv_history":
                if str(identifier or "").upper() not in {"SPY", "TLT", "EEM", "EWS", "USO", "BIL"}:
                    continue
            if identifier_succeeded and len(provider_successes) >= target_successes and provider_name not in provider_successes:
                continue
            try:
                payload = fetch_with_cache(
                    conn,
                    provider_name=provider_name,
                    endpoint_family=endpoint_family,
                    identifier=identifier,
                    surface_name=surface_name,
                    triggered_by_job=triggered_by_job,
                    force_refresh=force_refresh,
                )
                governance = dict(payload.get("governance") or {})
                operational_freshness = str(governance.get("operational_freshness_state") or governance.get("freshness_state") or payload.get("freshness_state") or "")
                governance.setdefault(
                    "terminal_state",
                    "current_success" if operational_freshness in {"current", "expected_lag", "aging"} else "stale_context_only",
                )
                governance.setdefault("terminal_cause", None if governance.get("terminal_state") == "current_success" else "stale_only")
                payload["governance"] = governance
                record_provider_family_event(
                    conn,
                    provider_name=provider_name,
                    surface_name=surface_name,
                    family_name=canonical_family,
                    identifier=identifier,
                    target_universe=identifiers,
                    success=True,
                    error_class=None,
                    cache_hit=str(payload.get("cache_status") or "") == "hit",
                    freshness_state=operational_freshness,
                    fallback_used=bool(payload.get("fallback_used")),
                    age_seconds=_family_age_seconds(payload.get("observed_at")),
                    root_error_class=None,
                    effective_error_class=None,
                    suppression_reason=None,
                    triggered_by_job=triggered_by_job,
                )
                terminal_state = str(governance.get("terminal_state") or "")
                if terminal_state == "current_success":
                    provider_successes.add(provider_name)
                    identifier_succeeded = True
                    results.append(payload)
                    if len(provider_successes) >= target_successes:
                        break
                    break
                if (
                    index == 0
                    and len(identifier_candidates) > 1
                    and str(governance.get("terminal_cause") or "stale_only") in _RETRY_WORTHY_RESCUE_CAUSES
                ):
                    fallback_stale_payload = payload
                    continue
                identifier_succeeded = True
                results.append(payload)
                break
            except ProviderAdapterError as exc:
                normalized_cause = _normalized_family_error_cause(str(exc.error_class or ""), str(exc))
                record_provider_family_event(
                    conn,
                    provider_name=provider_name,
                    surface_name=surface_name,
                    family_name=canonical_family,
                    identifier=identifier,
                    target_universe=identifiers,
                    success=False,
                    error_class=exc.error_class,
                    cache_hit=False,
                    freshness_state="unavailable",
                    fallback_used=False,
                    age_seconds=None,
                    root_error_class=normalized_cause,
                    effective_error_class=normalized_cause,
                    suppression_reason=str(exc) if str(exc.error_class or "") == "budget_block" else None,
                    triggered_by_job=triggered_by_job,
                )
                retry_allowed = (
                    index == 0
                    and len(identifier_candidates) > 1
                    and str(exc.error_class or "") in _RETRY_WORTHY_RESCUE_CAUSES
                )
                if retry_allowed:
                    continue
                break
        if not identifier_succeeded and fallback_stale_payload is not None:
            results.append(fallback_stale_payload)
            identifier_succeeded = True
    recompute_provider_family_success(conn, surface_name=surface_name, family_name=canonical_family)
    if not results:
        last_error = None
        comparison = compare_family_providers(conn, surface_name=surface_name, family_name=canonical_family)
        weakest = comparison.get("weakest_current_provider") or {}
        if weakest:
            last_error = str(weakest.get("last_error_class") or "")
        terminal_cause = next(
            (
                cause
                for cause in ineligible_identifier_causes
                if cause in {"plan_limit", "provider_budget_block", "quota_exhausted", "all_routes_quarantined", "unsupported_symbol", "no_eligible_route"}
            ),
            None,
        )
        if terminal_cause is None:
            terminal_cause = "no_eligible_route" if no_eligible_identifiers else (last_error or "all_routes_failed")
        governance = build_governance_record(
            source_name=None,
            provider_family=endpoint_family,
            fetched_at=None,
            observed_at=None,
            cadence_seconds=1800,
            fallback_used=False,
            cache_status="unavailable",
            error_state=terminal_cause,
            source_tier="secondary",
        )
        governance["terminal_state"] = "current_failure"
        governance["terminal_cause"] = terminal_cause
        governance["eligible_route_count"] = eligible_route_count
        governance["no_eligible_identifiers"] = no_eligible_identifiers[:8]
        results.append(
            {
                "provider_name": None,
                "source_name": None,
                "provider_family": endpoint_family,
                "endpoint_family": endpoint_family,
                "identifier": ",".join(identifiers[:3]),
                "cache_status": "unavailable",
                "error_state": terminal_cause,
                "freshness_state": "unavailable",
                "governance": governance,
            }
        )
    return results


def _bundle_public_context(conn, settings: Settings, *, surface_name: str, force_refresh: bool = False) -> dict[str, Any]:
    key = f"{surface_name}:EXTERNAL_CONTEXT:BUNDLE"
    if not force_refresh:
        cached = get_cached_provider_snapshot(
            conn,
            provider_name="public_context_bundle",
            endpoint_family="external_context",
            cache_key=key,
            surface_name=surface_name,
            max_age_seconds=6 * 60 * 60,
        )
        if cached is not None:
            payload = dict(cached.get("payload") or {})
            if not list((public_upstream_health_summary(conn) or {}).get("providers") or []):
                for context in list(payload.get("daily_brief_context") or []):
                    if isinstance(context, dict):
                        _persist_public_context(conn, context)
            return payload
    live = build_external_upstream_payload(settings, conn=conn, force_refresh=True)
    payload = {
        "daily_brief_context": list(live.get("daily_brief_context") or []),
        "blueprint_context": list(live.get("blueprint_context") or []),
        "dashboard_context": list(live.get("dashboard_context") or []),
        "generated_at": live.get("generated_at"),
    }
    put_provider_snapshot(
        conn,
        provider_name="public_context_bundle",
        endpoint_family="external_context",
        cache_key=key,
        payload=payload,
        surface_name=surface_name,
        freshness_state="fresh",
        confidence_tier="public",
        source_ref="public_context_bundle",
        ttl_seconds=6 * 60 * 60,
        cache_status="miss",
        fallback_used=False,
        error_state=None,
    )
    return payload


def _daily_brief_targets() -> dict[str, list[str]]:
    return daily_brief_targets()


def _dashboard_targets(conn, account_id: str | None = None) -> dict[str, list[str]]:
    rows = latest_snapshot_rows(conn, account_id=account_id)
    symbols: list[str] = []
    currencies: set[str] = set()
    for row in sorted(rows, key=lambda item: float(item.get("market_value") or 0.0), reverse=True):
        symbol = str(row.get("normalized_symbol") or "").strip().upper()
        currency = str(row.get("currency") or "").strip().upper()
        if symbol and not re.fullmatch(r"[A-Z0-9.\-]{1,12}", symbol):
            symbol = ""
        if symbol and symbol not in symbols:
            symbols.append(symbol)
        if currency and currency != "SGD":
            currencies.add(currency)
        if len(symbols) >= 5:
            break
    fx_pairs = [f"{currency}/SGD" for currency in sorted(currencies)[:4]]
    return {
        "quote_latest": symbols,
        "fx": fx_pairs,
        "benchmark_proxy": ["SPY", "TLT", "EEM"],
    }


def _surface_targets(conn, surface_name: str) -> dict[str, list[str]]:
    if surface_name == "daily_brief":
        return _daily_brief_targets()
    if surface_name == "dashboard":
        return _dashboard_targets(conn)
    if surface_name == "blueprint":
        return _blueprint_targets(conn)
    return {}


def _target_family_identifiers(surface_name: str, targets: dict[str, list[str]], endpoint_family: str) -> set[str]:
    active = {str(item).strip().upper() for item in list(targets.get(endpoint_family) or []) if str(item).strip()}
    if surface_name == "dashboard" and endpoint_family == "benchmark_proxy":
        active.update({"SPY", "TLT", "EEM"})
    return active


def _filter_snapshots_to_targets(
    snapshots: list[dict[str, Any]],
    *,
    surface_name: str,
    targets: dict[str, list[str]],
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for snapshot in snapshots:
        family = str(snapshot.get("endpoint_family") or "")
        payload = dict(snapshot.get("payload") or {})
        identifier = str(payload.get("identifier") or payload.get("metric") or "").strip().upper()
        active_identifiers = _target_family_identifiers(surface_name, targets, family)
        if family in {"quote_latest", "fx", "fx_reference", "benchmark_proxy"}:
            if active_identifiers:
                if identifier not in active_identifiers:
                    continue
            elif family != "benchmark_proxy":
                continue
        filtered.append(snapshot)
    return filtered


def _blueprint_targets(conn) -> dict[str, list[str]]:
    enforce_registry_parity(conn, fail_on_fatal=True)
    return blueprint_targets_from_policy(conn)


def _extract_provider_candidate_fields(endpoint_family: str, payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("value")

    def _safe_float(value: Any) -> float | None:
        try:
            if value in {None, ""}:
                return None
            return float(value)
        except Exception:
            return None

    def _nested(obj: dict[str, Any], *keys: str) -> Any:
        current: Any = obj
        for key in keys:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current

    def _latest_series_row(series: Any) -> dict[str, Any]:
        if isinstance(series, list):
            for item in series:
                if isinstance(item, dict):
                    return item
            return {}
        if isinstance(series, dict) and series:
            latest_key = sorted(series.keys())[-1]
            row = series.get(latest_key)
            if isinstance(row, dict):
                return {**row, "date": latest_key}
        return {}

    def _is_etf_specific_payload(profile: dict[str, Any]) -> bool:
        keys = {str(key).lower() for key in profile.keys()}
        return bool(
            {"etf_data", "holdings", "topholdings", "top_10_holdings", "countryweights", "country_weights", "sectorweights", "sector_weights"} & keys
            or {"fundfamily", "assettype", "fundtype", "category", "currency", "isfund"} & keys
        )

    def _extract_etf_reference_fields(profile: dict[str, Any]) -> dict[str, Any]:
        etf_data = _nested(profile, "ETF_Data")
        if not isinstance(etf_data, dict):
            etf_data = {}
        if not etf_data and not _is_etf_specific_payload(profile):
            return {}
        holdings_count = (
            etf_data.get("Holdings_Count")
            or etf_data.get("HoldingsCount")
            or etf_data.get("Number_Of_Holdings")
            or profile.get("holdingsCount")
        )
        aum = (
            etf_data.get("TotalAssets")
            or etf_data.get("Total_Assets")
            or etf_data.get("NetAssets")
            or etf_data.get("Assets")
            or etf_data.get("AUM")
            or profile.get("assetsUnderManagement")
            or profile.get("aum")
            or profile.get("mktCap")
            or profile.get("marketCap")
        )
        sector_weights = (
            etf_data.get("Sector_Weights")
            or etf_data.get("SectorWeights")
            or profile.get("sectorWeights")
            or profile.get("sectorWeightings")
        )
        country_weights = (
            etf_data.get("Country_Weights")
            or etf_data.get("CountryWeights")
            or profile.get("countryWeights")
            or profile.get("countryWeightings")
        )
        top_holdings = (
            etf_data.get("Top_10_Holdings")
            or etf_data.get("TopHoldings")
            or etf_data.get("Top_Holdings")
            or profile.get("topHoldings")
            or profile.get("holdings")
        )

        top_10_concentration = None
        if isinstance(top_holdings, list):
            weights = []
            for row in top_holdings[:10]:
                if isinstance(row, dict):
                    weight = _safe_float(
                        row.get("Weight")
                        or row.get("weight")
                        or row.get("HoldingPercent")
                        or row.get("holdingPercent")
                    )
                    if weight is not None:
                        weights.append(weight)
            if weights:
                top_10_concentration = round(sum(weights), 2)

        sector_concentration_proxy = None
        if isinstance(sector_weights, dict):
            largest = max((_safe_float(v) or 0.0) for v in sector_weights.values()) if sector_weights else 0.0
            sector_concentration_proxy = round(largest, 2) if largest > 0 else None
        elif isinstance(sector_weights, list):
            weights = []
            for row in sector_weights:
                if isinstance(row, dict):
                    weight = _safe_float(row.get("Weight") or row.get("weight") or row.get("Percentage") or row.get("percentage"))
                    if weight is not None:
                        weights.append(weight)
            if weights:
                sector_concentration_proxy = round(max(weights), 2)

        developed_summary = None
        emerging_summary = None
        em_weight = None
        if isinstance(country_weights, dict) and country_weights:
            developed = {
                "Australia", "Austria", "Belgium", "Canada", "Denmark", "Finland", "France", "Germany",
                "Hong Kong", "Ireland", "Israel", "Italy", "Japan", "Netherlands", "New Zealand", "Norway",
                "Portugal", "Singapore", "Spain", "Sweden", "Switzerland", "United Kingdom", "United States",
            }
            total_em = 0.0
            total_us = 0.0
            for country, raw_weight in country_weights.items():
                weight = _safe_float(raw_weight)
                if weight is None:
                    continue
                if str(country).strip() == "United States":
                    total_us += weight
                if str(country).strip() and str(country).strip() not in developed:
                    total_em += weight
            em_weight = round(total_em, 2)
            if em_weight <= 0:
                developed_summary = "Developed markets only"
                emerging_summary = "No emerging-market allocation detected"
            else:
                developed_summary = f"Developed plus emerging ({em_weight:.1f}% EM)"
                emerging_summary = f"Emerging market weight {em_weight:.1f}%"
            us_weight = round(total_us, 2) if total_us > 0 else None
        else:
            us_weight = None

        extracted = {
            "issuer": (
                profile.get("issuerName")
                or profile.get("fund_company")
                or profile.get("fundCompany")
                or profile.get("fundFamily")
                or profile.get("companyName")
                or profile.get("company")
                or profile.get("issuer")
            ),
            "primary_listing_exchange": profile.get("exchange") or profile.get("exchangeShortName"),
            "primary_trading_currency": profile.get("currency"),
        }
        expense_ratio = _safe_float(
            profile.get("expenseRatio")
            or profile.get("expense_ratio")
            or profile.get("netExpenseRatio")
            or etf_data.get("Expense_Ratio")
            or etf_data.get("ExpenseRatio")
            or etf_data.get("Net_Expense_Ratio")
        )
        if _safe_float(aum) is not None:
            extracted["aum"] = _safe_float(aum)
        if expense_ratio is not None:
            extracted["expense_ratio"] = expense_ratio
        if holdings_count not in {None, ""}:
            extracted["holdings_count"] = holdings_count
        if top_10_concentration is not None:
            extracted["top_10_concentration"] = top_10_concentration
        if sector_concentration_proxy is not None:
            extracted["sector_concentration_proxy"] = sector_concentration_proxy
        if us_weight is not None:
            extracted["us_weight"] = us_weight
        if em_weight is not None:
            extracted["em_weight"] = em_weight
        if developed_summary:
            extracted["developed_market_exposure_summary"] = developed_summary
        if emerging_summary:
            extracted["emerging_market_exposure_summary"] = emerging_summary
        return extracted

    if endpoint_family == "reference_meta" and isinstance(value, dict):
        return _extract_etf_reference_fields(value)
    if endpoint_family == "reference_meta" and isinstance(value, list) and value:
        row = value[0] if isinstance(value[0], dict) else {}
        if isinstance(row, dict):
            return _extract_provider_candidate_fields(endpoint_family, {"value": row})
        return {}
    if endpoint_family == "fundamentals":
        if isinstance(value, dict):
            explicit_aum = (
                value.get("aum")
                or value.get("totalAssets")
                or value.get("total_assets")
                or value.get("netAssets")
                or value.get("net_assets")
                or value.get("assetsUnderManagement")
                or value.get("AUM")
                or _nested(value, "ETF_Data", "TotalAssets")
                or _nested(value, "ETF_Data", "Total_Assets")
                or _nested(value, "ETF_Data", "NetAssets")
                or _nested(value, "ETF_Data", "AUM")
            )
            fields: dict[str, Any] = {}
            if _safe_float(explicit_aum) is not None:
                fields["aum"] = _safe_float(explicit_aum)
            explicit_expense_ratio = _safe_float(
                value.get("expenseRatio")
                or value.get("expense_ratio")
                or value.get("netExpenseRatio")
                or _nested(value, "ETF_Data", "Expense_Ratio")
                or _nested(value, "ETF_Data", "ExpenseRatio")
            )
            if explicit_expense_ratio is not None:
                fields["expense_ratio"] = explicit_expense_ratio
            if value.get("lastDiv") not in {None, ""}:
                fields["yield_proxy"] = value.get("lastDiv")
            fields.update(
                {
                    key: val
                    for key, val in _extract_etf_reference_fields(value).items()
                    if key in {
                        "holdings_count",
                        "top_10_concentration",
                        "sector_concentration_proxy",
                        "us_weight",
                        "em_weight",
                        "developed_market_exposure_summary",
                        "emerging_market_exposure_summary",
                        "primary_trading_currency",
                        "primary_listing_exchange",
                        "distribution_type",
                        "share_class",
                        "wrapper_or_vehicle_type",
                        "issuer",
                    }
                }
            )
            return fields
        return {}
    if endpoint_family == "benchmark_proxy":
        series = payload.get("series") or []
        latest = _latest_series_row(series)
        resolution = dict(payload.get("resolution") or {})
        resolution_reason = str(resolution.get("resolution_reason") or "").strip()
        route_validity_state = "alias_review_needed" if resolution_reason == "alias_resolution_review" else "direct_ready"
        volume = (
            payload.get("volume")
            or latest.get("volume")
            or latest.get("v")
            or latest.get("5. volume")
        )
        fields = {
            "market_data_as_of": payload.get("observed_at"),
            "liquidity_proxy": "provider_history_backed",
            "freshness_state": "current",
            "direct_history_depth": len(series),
            "route_validity_state": route_validity_state,
        }
        parsed_volume = _safe_float(volume)
        if parsed_volume is not None:
            fields["volume_30d_avg"] = parsed_volume
        return fields
    if endpoint_family == "quote_latest":
        return {
            "price": payload.get("price") or payload.get("value"),
            "change_pct_1d": payload.get("change_pct_1d"),
            "previous_close": payload.get("previous_close"),
            "market_data_as_of": payload.get("observed_at"),
            "freshness_state": "current",
            "quote_freshness_state": "fresh",
        }
    if endpoint_family == "ohlcv_history":
        series = payload.get("series") or []
        latest = _latest_series_row(series)
        resolution = dict(payload.get("resolution") or {})
        resolution_reason = str(resolution.get("resolution_reason") or "").strip()
        return {
            "market_data_as_of": payload.get("observed_at"),
            "volume_30d_avg": latest.get("volume") or latest.get("v") or latest.get("5. volume"),
            "liquidity_proxy": "provider_history_backed",
            "freshness_state": "current",
            "direct_history_depth": len(series),
            "route_validity_state": "alias_review_needed" if resolution_reason == "alias_resolution_review" else "direct_ready",
        }
    if endpoint_family == "etf_holdings":
        # EDGAR N-PORT normalized fields — mirrored from sec_edgar_ingestion.parse_nport_xml()
        etf_fields: dict[str, Any] = {}
        if _safe_float(payload.get("aum")) is not None:
            etf_fields["aum"] = _safe_float(payload.get("aum"))
        if _safe_float(payload.get("expense_ratio")) is not None:
            etf_fields["expense_ratio"] = _safe_float(payload.get("expense_ratio"))
        if payload.get("holdings_count") not in {None, ""}:
            etf_fields["holdings_count"] = payload.get("holdings_count")
        if _safe_float(payload.get("top_10_concentration")) is not None:
            etf_fields["top_10_concentration"] = _safe_float(payload.get("top_10_concentration"))
        if payload.get("factsheet_asof") not in {None, ""}:
            etf_fields["factsheet_asof"] = payload.get("factsheet_asof")
        return etf_fields
    if endpoint_family == "etf_profile":
        fields: dict[str, Any] = {}
        aum = _safe_float(payload.get("net_assets"))
        if aum is not None:
            fields["aum"] = aum
        expense_ratio = _safe_float(payload.get("expense_ratio"))
        if expense_ratio is not None:
            fields["expense_ratio"] = expense_ratio
        portfolio_turnover = _safe_float(payload.get("portfolio_turnover"))
        if portfolio_turnover is not None:
            fields["portfolio_turnover"] = portfolio_turnover
        holdings_count = payload.get("holdings_count")
        if holdings_count not in {None, ""}:
            fields["holdings_count"] = holdings_count
        top_10 = _safe_float(payload.get("top_10_concentration"))
        if top_10 is not None:
            fields["top_10_concentration"] = top_10
        sector_weightings = payload.get("sector_weightings")
        if isinstance(sector_weightings, dict) and sector_weightings:
            largest = max((_safe_float(v) or 0.0) for v in sector_weightings.values())
            if largest > 0:
                fields["sector_concentration_proxy"] = round(largest, 2)
        elif isinstance(sector_weightings, list) and sector_weightings:
            weights = [_safe_float(row.get("weight") or row.get("Weight")) for row in sector_weightings if isinstance(row, dict)]
            clean = [w for w in weights if w is not None]
            if clean:
                fields["sector_concentration_proxy"] = round(max(clean), 2)
        asset_allocation = payload.get("asset_allocation")
        if isinstance(asset_allocation, dict):
            domestic = _safe_float(asset_allocation.get("domestic_equity") or asset_allocation.get("domesticEquity"))
            if domestic is not None:
                fields["us_weight"] = domestic
        return fields
    return {}


_ISIN_PREFIX_TO_COUNTRY = {
    "IE": "IRELAND",
    "LU": "LUXEMBOURG",
    "SG": "SINGAPORE",
    "US": "UNITED STATES",
    "GB": "UNITED KINGDOM",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _exchange_region(exchange: Any) -> str:
    normalized = _text(exchange).upper()
    if not normalized:
        return ""
    if any(token in normalized for token in {"NYSE", "NASDAQ", "ARCA", "AMEX", "CBOE", "BATS", "OTC"}):
        return "US"
    if any(token in normalized for token in {"LSE", "XLON", "LONDON"}):
        return "UK"
    if "SGX" in normalized:
        return "SG"
    return ""


def _candidate_identity_context(candidate: dict[str, Any]) -> dict[str, Any]:
    extra = dict(candidate.get("extra") or {}) if isinstance(candidate.get("extra"), dict) else {}
    expected_isin = _text(extra.get("isin") or candidate.get("isin")).upper()
    expected_exchange = _text(extra.get("primary_listing_exchange") or candidate.get("primary_listing_exchange"))
    expected_domicile = _text(candidate.get("domicile") or extra.get("domicile")).upper()
    if not expected_domicile and len(expected_isin) >= 2:
        expected_domicile = _ISIN_PREFIX_TO_COUNTRY.get(expected_isin[:2], "")
    instrument_type = _text(candidate.get("instrument_type")).lower()
    us_like = (
        expected_isin.startswith("US")
        or _exchange_region(expected_exchange) == "US"
        or expected_domicile in {"US", "USA", "UNITED STATES"}
        or instrument_type in {"etf_us", "us_etf", "us_equity"}
    )
    return {
        "expected_isin": expected_isin,
        "expected_exchange": expected_exchange,
        "expected_exchange_region": _exchange_region(expected_exchange),
        "expected_domicile": expected_domicile,
        "us_like": us_like,
    }


def _exchange_matches_candidate(candidate: dict[str, Any], field_value: Any) -> bool:
    context = _candidate_identity_context(candidate)
    expected_exchange = _text(context.get("expected_exchange"))
    if not expected_exchange:
        return True
    actual_exchange = _text(field_value)
    if not actual_exchange:
        return False
    expected_upper = expected_exchange.upper()
    actual_upper = actual_exchange.upper()
    if expected_upper == actual_upper or expected_upper in actual_upper or actual_upper in expected_upper:
        return True
    expected_region = _text(context.get("expected_exchange_region"))
    actual_region = _exchange_region(actual_exchange)
    return bool(expected_region and actual_region and expected_region == actual_region)


def _provider_field_allowed(
    candidate: dict[str, Any],
    *,
    provider_name: str,
    endpoint_family: str,
    field_name: str,
    field_value: Any,
) -> bool:
    normalized_provider = _text(provider_name).lower()
    if field_name in {"fund_name", "isin", "domicile"}:
        return False
    if endpoint_family in {"reference_meta", "fundamentals"}:
        context = _candidate_identity_context(candidate)
        if field_name == "issuer":
            return normalized_provider in {"fmp", "financialmodelingprep"} and bool(context.get("us_like"))
        if field_name == "aum":
            return normalized_provider in {"fmp", "financialmodelingprep"}
        if field_name == "holdings_count":
            return normalized_provider in {"fmp", "financialmodelingprep"}
        if field_name == "primary_listing_exchange":
            if normalized_provider in {"finnhub", "fmp", "financialmodelingprep"} and not bool(context.get("us_like")):
                return False
            return _exchange_matches_candidate(candidate, field_value)
        if field_name == "primary_trading_currency":
            if not bool(context.get("us_like")):
                return False
            return True
        if field_name in {"wrapper_or_vehicle_type", "distribution_type", "share_class"}:
            return False
    if field_name in {"liquidity_proxy", "bid_ask_spread_proxy"}:
        return True
    return True


def _persist_blueprint_enrichment(conn, results: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = export_live_candidate_registry(conn)
    candidate_map: dict[str, list[dict[str, Any]]] = {}
    for item in candidates:
        candidate_map.setdefault(str(item.get("symbol") or "").upper(), []).append(item)
    enriched_symbols: set[str] = set()
    for payload in results:
        if str(payload.get("cache_status") or "") == "unavailable":
            continue
        family = str(payload.get("endpoint_family") or "")
        symbol = str(payload.get("identifier") or "").upper()
        if symbol not in candidate_map:
            continue
        fields = _extract_provider_candidate_fields(family, payload)
        if not fields:
            continue
        gov = dict(payload.get("governance") or {})
        coverage_status = str(gov.get("coverage_status") or "complete")
        for candidate in candidate_map.get(symbol, []):
            sleeve_key = str(candidate.get("sleeve_key") or "")
            provider_name = str(payload.get("source_name") or payload.get("provider_name") or "provider")
            for field_name, field_value in fields.items():
                if not _provider_field_allowed(
                    candidate,
                    provider_name=provider_name,
                    endpoint_family=family,
                    field_name=field_name,
                    field_value=field_value,
                ):
                    continue
                missingness = "populated" if field_value not in {None, ""} else (
                    "fetchable_from_current_sources" if coverage_status == "missing_fetchable" else "blocked_by_source_gap"
                )
                upsert_field_observation(
                    conn,
                    candidate_symbol=symbol,
                    sleeve_key=sleeve_key,
                    field_name=field_name,
                    value=field_value,
                    source_name=provider_name,
                    source_url=str(payload.get("source_ref") or ""),
                    observed_at=str(payload.get("observed_at") or ""),
                    provenance_level="verified_nonissuer",
                    confidence_label=str(gov.get("confidence_label") or "medium"),
                    parser_method=f"provider_refresh:{family}",
                    missingness_reason=missingness,
                )
            source_state = "source_validated" if coverage_status == "complete" else "aging"
            upsert_field_observation(
                conn,
                candidate_symbol=symbol,
                sleeve_key=sleeve_key,
                field_name="source_state",
                value=source_state,
                source_name=provider_name,
                observed_at=str(payload.get("observed_at") or ""),
                provenance_level="verified_nonissuer",
                confidence_label=str(gov.get("confidence_label") or "medium"),
                parser_method="provider_refresh:source_state",
            )
            resolve_candidate_field_truth(conn, candidate_symbol=symbol, sleeve_key=sleeve_key)
            compute_candidate_completeness(conn, candidate={**candidate, "sleeve_key": sleeve_key}, now=datetime.now(UTC))
            enriched_symbols.add(symbol)
    return {
        "enriched_candidate_count": len(enriched_symbols),
        "enriched_symbols": sorted(enriched_symbols),
        "active_universe": active_candidate_universe_summary(conn),
    }


def _refresh_targets(
    conn,
    *,
    settings: Settings,
    surface_name: str,
    targets: dict[str, list[str]],
    triggered_by_job: str,
    force_refresh: bool = False,
) -> dict[str, Any]:
    recovery_run_id = f"{surface_name}_{uuid4().hex[:12]}"
    results: list[dict[str, Any]] = []
    for family in surface_families(surface_name):
        family_results = _collect_family_payloads(
            conn,
            surface_name=surface_name,
            endpoint_family=family,
            identifiers=list(targets.get(family) or []),
            triggered_by_job=triggered_by_job,
            force_refresh=force_refresh,
        )
        results.extend(family_results)
    direct_provider_targets = {
        "daily_brief": [
            ("nasdaq_data_link", "research_dataset", "ETFG/FUND?ticker=SPY"),
        ],
        "blueprint": [],
        "dashboard": [],
    }.get(surface_name, [])
    for provider_name, endpoint_family, identifier in direct_provider_targets:
        try:
            payload = fetch_with_cache(
                conn,
                provider_name=provider_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                surface_name=surface_name,
                triggered_by_job=triggered_by_job,
                force_refresh=force_refresh,
            )
            record_provider_family_event(
                conn,
                provider_name=provider_name,
                surface_name=surface_name,
                family_name=_canonical_family_name(endpoint_family),
                identifier=identifier,
                target_universe=[identifier],
                success=True,
                error_class=None,
                cache_hit=str(payload.get("cache_status") or "") == "hit",
                freshness_state=str(((payload.get("governance") or {}).get("freshness_state") or payload.get("freshness_state") or "")),
                fallback_used=bool(payload.get("fallback_used")),
                age_seconds=_family_age_seconds(payload.get("observed_at")),
                triggered_by_job=triggered_by_job,
            )
        except ProviderAdapterError as exc:
            record_provider_family_event(
                conn,
                provider_name=provider_name,
                surface_name=surface_name,
                family_name=_canonical_family_name(endpoint_family),
                identifier=identifier,
                target_universe=[identifier],
                success=False,
                error_class=exc.error_class,
                cache_hit=False,
                freshness_state="unavailable",
                fallback_used=False,
                age_seconds=None,
                triggered_by_job=triggered_by_job,
            )
            payload = {
                "provider_name": provider_name,
                "source_name": str(PROVIDER_CAPABILITY_MATRIX.get(provider_name, {}).get("label") or provider_name),
                "provider_family": endpoint_family,
                "endpoint_family": endpoint_family,
                "identifier": identifier,
                "cache_status": "unavailable",
                "error_state": f"{provider_name}:{exc.error_class}",
                "freshness_state": "unavailable",
                "fallback_used": False,
                "governance": build_governance_record(
                    source_name=str(PROVIDER_CAPABILITY_MATRIX.get(provider_name, {}).get("label") or provider_name),
                    provider_family=endpoint_family,
                    fetched_at=None,
                    observed_at=None,
                    cadence_seconds=_family_ttl_seconds(provider_name, endpoint_family),
                    fallback_used=False,
                    cache_status="unavailable",
                    error_state=f"{provider_name}:{exc.error_class}",
                    source_tier=str(PROVIDER_CAPABILITY_MATRIX.get(provider_name, {}).get("confidence_tier") or "secondary"),
                ),
            }
        results.append(payload)
        recompute_provider_family_success(conn, surface_name=surface_name, family_name=_canonical_family_name(endpoint_family))
    public_bundle = _bundle_public_context(conn, settings, surface_name=surface_name, force_refresh=force_refresh)
    enrichment = _persist_blueprint_enrichment(conn, results) if surface_name == "blueprint" else None
    family_items: dict[str, list[dict[str, Any]]] = {}
    for item in results:
        family = str(item.get("endpoint_family") or "")
        if not family:
            continue
        family_items.setdefault(family, []).append(item)
    for family_name, items in family_items.items():
        version, latest_observed, latest_fetched, providers = _version_stamp(surface_name, family_name, items)
        upsert_surface_snapshot_version(
            conn,
            surface_name=surface_name,
            family_name=family_name,
            snapshot_version=version,
            latest_observed_at=latest_observed,
            latest_fetched_at=latest_fetched,
            provider_mix=providers,
        )
    return {
        "surface_name": surface_name,
        "recovery_run_id": recovery_run_id,
        "refreshed_at": _now_iso(),
        "items": results,
        "public_context": public_bundle,
        "sufficiency": summarize_surface_sufficiency(results, surface_name=surface_name),
        "family_ownership": family_ownership_map(),
        "enrichment": enrichment,
    }


def refresh_daily_brief_provider_snapshots(conn, settings: Settings, *, force_refresh: bool = False) -> dict[str, Any]:
    return _refresh_targets(
        conn,
        settings=settings,
        surface_name="daily_brief",
        targets=_daily_brief_targets(),
        triggered_by_job="daily_brief_refresh",
        force_refresh=force_refresh,
    )


def refresh_dashboard_provider_snapshots(conn, settings: Settings, *, account_id: str | None = None, force_refresh: bool = False) -> dict[str, Any]:
    return _refresh_targets(
        conn,
        settings=settings,
        surface_name="dashboard",
        targets=_dashboard_targets(conn, account_id=account_id),
        triggered_by_job="dashboard_refresh",
        force_refresh=force_refresh,
    )


def refresh_blueprint_provider_snapshots(conn, settings: Settings, *, force_refresh: bool = False) -> dict[str, Any]:
    return _refresh_targets(
        conn,
        settings=settings,
        surface_name="blueprint",
        targets=_blueprint_targets(conn),
        triggered_by_job="blueprint_refresh",
        force_refresh=force_refresh,
    )


def _build_surface_context_from_snapshots(surface_name: str, snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for snapshot in snapshots:
        provider_name = str(snapshot.get("provider_name") or "unknown")
        grouped.setdefault(provider_name, []).append(snapshot)
    contexts: list[dict[str, Any]] = []
    for provider_name, items in grouped.items():
        if provider_name == "public_context_bundle":
            bundle_payload = dict(items[0].get("payload") or {})
            contexts.extend(list(bundle_payload.get(f"{surface_name}_context") or []))
            continue
        provider_label = str(PROVIDER_CAPABILITY_MATRIX.get(provider_name, {}).get("label") or provider_name)
        rows = []
        for item in items[:6]:
            payload = dict(item.get("payload") or {})
            enriched = _apply_governance(
                payload,
                provider_name=provider_name,
                endpoint_family=str(item.get("endpoint_family") or ""),
                fetched_at=item.get("fetched_at"),
                cache_status=item.get("cache_status"),
                fallback_used=bool(item.get("fallback_used")),
                error_state=item.get("error_state"),
            )
            rows.append(
                {
                    "provider": provider_label,
                    "metric": str(payload.get("identifier") or ""),
                    "label": str(payload.get("identifier") or ""),
                    "value": payload.get("value"),
                    "observed_at": payload.get("observed_at"),
                    "summary": f"{payload.get('identifier')} via {provider_label} ({payload.get('endpoint_family')})",
                    "governance": enriched.get("governance"),
                    "citation": {
                        "source_id": str(payload.get("source_ref") or provider_name),
                        "url": None,
                        "publisher": provider_label,
                        "retrieved_at": item.get("fetched_at"),
                    },
                }
            )
        contexts.append(
            {
                "provider_key": provider_name,
                "headline": f"{provider_label} cached {surface_name.replace('_', ' ')} snapshots",
                "items": rows,
            }
        )
    return contexts


def _metric_family(metric: str) -> str | None:
    text = str(metric or "").upper()
    if any(token in text for token in ("USD/SGD", "EUR/USD", "DTWEX", "DEXSIUS", "EURUSD", "EURSGD")):
        return "fx"
    if any(token in text for token in ("ACWI", "AGG", "GLD", "SPY", "TLT", "LQD", "EEM", "EWS", "USO")):
        return "benchmark_proxy"
    if "DXY" in text:
        return "quote_latest"
    return None


def _preferred_consistency_metric(family: str, available_metrics: set[str]) -> str | None:
    preferences = {
        "fx": ("USD/SGD", "EUR/USD", "DEXSIUS", "DTWEXBGS", "EURUSD", "EURSGD"),
        "benchmark_proxy": ("ACWI", "AGG", "GLD", "SPY", "TLT", "LQD", "EEM", "EWS", "USO"),
        "quote_latest": ("DXY", "ACWI", "AGG", "GLD", "SPY", "TLT", "EEM"),
    }
    for candidate in preferences.get(family, ()):
        if candidate in available_metrics:
            return candidate
    return sorted(available_metrics)[0] if available_metrics else None


def _context_consistency(payload: dict[str, Any]) -> list[str]:
    by_family: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
    for surface_name in ("daily_brief", "dashboard", "blueprint"):
        for context in list(payload.get(f"{surface_name}_context") or []):
            for item in list(context.get("items") or []):
                family = str((item.get("governance") or {}).get("provider_family") or "")
                if family == "benchmark_proxy":
                    family = "benchmark_proxy"
                elif not family:
                    family = _metric_family(item.get("metric")) or ""
                if not family:
                    continue
                metric = str(item.get("metric") or "").upper()
                current = by_family.setdefault(family, {}).setdefault(surface_name, {}).get(metric)
                candidate = {
                    "observed_at": item.get("observed_at"),
                    "governance": item.get("governance"),
                    "fetched_at": ((item.get("governance") or {}).get("fetched_at")),
                }
                by_family[family][surface_name][metric] = _fresher_item(current, candidate)
    warnings: list[str] = []
    for family, surfaces in by_family.items():
        common_metrics = set.intersection(*(set(items.keys()) for items in surfaces.values())) if surfaces else set()
        selected_metric = _preferred_consistency_metric(family, common_metrics)
        if not selected_metric:
            continue
        comparable = {
            surface: items[selected_metric]
            for surface, items in surfaces.items()
            if selected_metric in items
        }
        warning = consistency_warning(family=f"{family}:{selected_metric}", surfaces=comparable)
        if warning:
            warnings.append(warning)
    return warnings[:8]


def _surface_issue_summary(
    *,
    surface_name: str,
    governance: dict[str, Any],
    source_diversity: dict[str, Any],
    quote_quality: dict[str, Any] | None = None,
    benchmark_watch_quality: dict[str, Any] | None = None,
) -> dict[str, Any]:
    issues: list[str] = []
    for item in list(source_diversity.get("critical_families") or []):
        family = str(item.get("family") or "family")
        state = str(item.get("state") or "")
        if state == "single_source":
            issues.append(f"{family}:single_source")
        elif state == "stale_context_only":
            issues.append(f"{family}:stale_context_only")
        elif state == "unavailable":
            issues.append(f"{family}:unavailable")
    if surface_name == "dashboard":
        qq = dict(quote_quality or {})
        tracked_quotes = int(qq.get("tracked_count") or 0)
        if tracked_quotes > 0 and str(qq.get("status") or "") not in {"healthy"}:
            issues.append(f"latest_quote:{str(qq.get('status') or 'weak')}")
        bwq = dict(benchmark_watch_quality or {})
        tracked_benchmarks = int(bwq.get("tracked_count") or 0)
        if tracked_benchmarks > 0 and str(bwq.get("status") or "") not in {"healthy"}:
            issues.append(f"benchmark_proxy_history:{str(bwq.get('status') or 'weak')}")
    if surface_name == "daily_brief":
        current_count = int(governance.get("current_count") or 0)
        stale_count = int(governance.get("stale_count") or 0)
        if stale_count > current_count and int(governance.get("gap_count") or 0) > 0:
            issues.append("market_context:stale_or_gapped")
    if surface_name == "blueprint":
        current_count = int(governance.get("current_count") or 0)
        gap_count = int(governance.get("gap_count") or 0)
        if gap_count > 0 and current_count == 0:
            issues.append("candidate_truth:coverage_gap")
    return {
        "issue_count": len(issues),
        "issues": issues,
    }


def _surface_source_diversity(payload: dict[str, Any], surface_name: str) -> dict[str, Any]:
    families = family_ownership_map()
    surface_family_map = {
        "daily_brief": {"market_close", "fx", "fx_reference", "usd_strength_fallback", "benchmark_proxy_history", "latest_quote"},
        "dashboard": {"fx", "benchmark_proxy_history", "latest_quote"},
        "blueprint": {"benchmark_proxy_history", "ohlcv_history", "etf_reference_metadata", "fundamentals", "latest_quote"},
    }
    by_family: dict[str, set[str]] = {}
    metrics_by_provider: dict[str, set[str]] = {}
    family_success_rows: dict[str, list[dict[str, Any]]] = {}
    for item in list(payload.get("family_success") or []):
        if str(item.get("surface_name") or "") != surface_name:
            continue
        family_name = _canonical_family_name(str(item.get("family_name") or ""))
        if family_name:
            family_success_rows.setdefault(family_name, []).append(dict(item))
    for context in list(payload.get(f"{surface_name}_context") or []):
        provider_key = str(context.get("provider_key") or "")
        for item in list(context.get("items") or []):
            metric = str(item.get("metric") or "").strip().upper()
            if provider_key and metric:
                metrics_by_provider.setdefault(provider_key, set()).add(metric)
            governance = dict(item.get("governance") or {})
            family = str((item.get("governance") or {}).get("provider_family") or "")
            if not family:
                family = _metric_family(item.get("metric")) or ""
            if not family:
                continue
            operational_freshness = str(governance.get("operational_freshness_state") or governance.get("freshness_state") or "")
            coverage_status = str(governance.get("coverage_status") or item.get("coverage_status") or "")
            if operational_freshness not in {"current", "aging", "expected_lag"}:
                continue
            if coverage_status in {"missing_fetchable", "missing_source_gap"}:
                continue
            normalized_family = _canonical_family_name(family)
            if normalized_family == "daily_market_close":
                normalized_family = "market_close"
            by_family.setdefault(normalized_family, set()).add(provider_key)
    if surface_name == "daily_brief":
        ecb_metrics = metrics_by_provider.get("ecb_data_api", set())
        if {"EURUSD_ECB", "EURSGD_ECB"}.issubset(ecb_metrics):
            by_family.setdefault("fx_reference", set()).add("ecb_data_api")
            by_family.setdefault("usd_strength_fallback", set()).add("ecb_data_api")
        if not by_family.get("fx"):
            fx_fallback_providers = set(by_family.get("fx_reference", set())) | set(by_family.get("usd_strength_fallback", set()))
            if fx_fallback_providers:
                by_family["fx"] = fx_fallback_providers
    critical: list[dict[str, Any]] = []
    for family_name, owner in families.items():
        if family_name not in surface_family_map.get(surface_name, set()):
            continue
        importance = str(owner.get("investor_importance") or "medium")
        if surface_name == "daily_brief" and family_name == "market_close":
            importance = "critical"
        if importance not in {"critical", "high"}:
            continue
        providers = sorted(by_family.get(family_name, set()))
        targets = dict(payload.get("active_targets") or {})
        canonical_target_family = {
            "benchmark_proxy_history": "benchmark_proxy",
            "latest_quote": "quote_latest",
            "etf_reference_metadata": "reference_meta",
        }.get(family_name, family_name)
        active_identifiers = _target_family_identifiers(surface_name, targets, canonical_target_family)
        if not active_identifiers:
            state = "not_applicable_no_active_targets"
        else:
            family_rows = list(family_success_rows.get(family_name, []))
            if len(providers) >= 2:
                state = "diverse"
            elif len(providers) == 1:
                state = "single_source"
            else:
                current_success_providers = sorted(
                    {
                        str(item.get("provider_name") or "").strip()
                        for item in family_rows
                        if int(item.get("current_snapshot_count") or 0) > 0 and str(item.get("provider_name") or "").strip()
                    }
                )
                stale_context_providers = sorted(
                    {
                        str(item.get("provider_name") or "").strip()
                        for item in family_rows
                        if (
                            str(item.get("current_terminal_state") or "") == "stale_context_only"
                            or int(item.get("stale_snapshot_count") or 0) > 0
                        )
                        and str(item.get("provider_name") or "").strip()
                    }
                )
                if len(current_success_providers) >= 2:
                    providers = current_success_providers
                    state = "diverse"
                elif len(current_success_providers) == 1:
                    providers = current_success_providers
                    state = "single_source"
                elif stale_context_providers:
                    providers = stale_context_providers
                    state = "stale_context_only"
                else:
                    state = "unavailable"
        critical.append(
            {
                "family": family_name,
                "importance": importance,
                "providers": providers,
                "state": state,
            }
        )
    return {
        "critical_families": critical,
        "single_source_count": sum(1 for item in critical if item["state"] == "single_source"),
        "stale_context_only_count": sum(1 for item in critical if item["state"] == "stale_context_only"),
        "unavailable_count": sum(1 for item in critical if item["state"] == "unavailable"),
    }


def _daily_brief_slot_validity(*, payload: dict[str, Any], settings: Settings) -> dict[str, Any]:
    from app.services.daily_brief_slots import current_slot_info, settings_slot_hours

    def _coerce_datetime(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    slot = current_slot_info(hours=settings_slot_hours(settings))
    slot_started_at = _coerce_datetime(slot.get("slot_started_at"))
    targets = dict(payload.get("active_targets") or {})
    eligible_providers_by_family: dict[str, set[str]] = {}
    metrics_by_provider: dict[str, set[str]] = {}
    slot_families = {
        "market_close",
        "benchmark_proxy_history",
        "fx",
        "fx_reference",
        "latest_quote",
        "usd_strength_fallback",
    }
    current_slot_families = {"market_close", "fx_reference", "usd_strength_fallback"}
    for context in list(payload.get("daily_brief_context") or []):
        provider_key = str(context.get("provider_key") or "")
        if not provider_key:
            continue
        for item in list(context.get("items") or []):
            metric = str(item.get("metric") or "").strip().upper()
            if metric:
                metrics_by_provider.setdefault(provider_key, set()).add(metric)
            governance = dict(item.get("governance") or {})
            family_name = _canonical_family_name(
                str(governance.get("provider_family") or _metric_family(item.get("metric")) or item.get("metric") or "")
            )
            if family_name == "daily_market_close":
                family_name = "market_close"
            if family_name not in slot_families:
                continue
            coverage_status = str(governance.get("coverage_status") or item.get("coverage_status") or "")
            if coverage_status in {"missing_fetchable", "missing_source_gap"}:
                continue
            operational_freshness = str(governance.get("operational_freshness_state") or governance.get("freshness_state") or "")
            fetched_at = _coerce_datetime(governance.get("fetched_at") or item.get("fetched_at"))
            eligible = operational_freshness in {"current", "aging", "expected_lag"}
            if family_name in current_slot_families:
                eligible = bool(slot_started_at and fetched_at and fetched_at >= slot_started_at)
                if not eligible and family_name in {"fx_reference", "usd_strength_fallback"} and operational_freshness == "expected_lag":
                    eligible = True
            if not eligible:
                continue
            eligible_providers_by_family.setdefault(family_name, set()).add(provider_key)
    ecb_metrics = metrics_by_provider.get("ecb_data_api", set())
    if {"EURUSD_ECB", "EURSGD_ECB"}.issubset(ecb_metrics):
        eligible_providers_by_family.setdefault("fx_reference", set()).add("ecb_data_api")
        eligible_providers_by_family.setdefault("usd_strength_fallback", set()).add("ecb_data_api")
    if not eligible_providers_by_family.get("fx"):
        fx_fallback_providers = set(eligible_providers_by_family.get("fx_reference", set())) | set(
            eligible_providers_by_family.get("usd_strength_fallback", set())
        )
        if fx_fallback_providers:
            eligible_providers_by_family["fx"] = fx_fallback_providers

    def _group_state(families: list[str]) -> dict[str, Any]:
        providers: set[str] = set()
        applicable = False
        for family_name in families:
            canonical_target_family = {
                "benchmark_proxy_history": "benchmark_proxy",
                "latest_quote": "quote_latest",
            }.get(family_name, family_name)
            if _target_family_identifiers("daily_brief", targets, canonical_target_family):
                applicable = True
            providers.update(eligible_providers_by_family.get(family_name, set()))
        if len(providers) >= 2:
            state = "diverse"
        elif len(providers) == 1:
            state = "single_source"
        elif not applicable:
            state = "not_applicable"
        else:
            state = "unavailable"
        return {
            "families": families,
            "providers": sorted(providers),
            "state": state,
        }

    required_groups = [
        {"group": "market_close", "importance": "required", **_group_state(["market_close"])},
        {"group": "fx_reference_context", "importance": "required", **_group_state(["fx_reference", "usd_strength_fallback"])},
    ]
    supporting_groups = [
        {"group": "benchmark_watch", "importance": "supporting", **_group_state(["benchmark_proxy_history"])},
        {"group": "latest_quote", "importance": "supporting", **_group_state(["latest_quote"])},
        {"group": "live_fx", "importance": "supporting", **_group_state(["fx"])},
    ]

    issues: list[str] = []
    for group in required_groups + supporting_groups:
        group_state = str(group.get("state") or "")
        if group_state in {"single_source", "unavailable"}:
            issues.append(f"{str(group.get('group') or 'group')}:{group_state}")

    has_required_gap = any(str(group.get("state") or "") == "unavailable" for group in required_groups)
    has_degradation = any(
        str(group.get("state") or "") in {"single_source", "unavailable"}
        for group in required_groups + supporting_groups
        if str(group.get("state") or "") != "not_applicable"
    )
    state = "slot_failed" if has_required_gap else "slot_valid_degraded" if has_degradation else "slot_valid"
    return {
        "slot_key": slot.get("slot_key"),
        "slot_label": slot.get("slot_label"),
        "slot_started_at": slot.get("slot_started_at"),
        "slot_ends_at": slot.get("slot_ends_at"),
        "state": state,
        "issues": issues,
        "required_groups": required_groups,
        "supporting_groups": supporting_groups,
        "valid_group_count": sum(
            1 for group in required_groups + supporting_groups if str(group.get("state") or "") in {"diverse", "single_source"}
        ),
        "missing_group_count": sum(1 for group in required_groups + supporting_groups if str(group.get("state") or "") == "unavailable"),
    }


def _dashboard_quote_quality(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for snapshot in snapshots:
        if str(snapshot.get("surface_name") or "") != "dashboard":
            continue
        payload = dict(snapshot.get("payload") or {})
        governance = dict(payload.get("governance") or {})
        if not governance:
            governance = _apply_governance(
                payload,
                provider_name=str(snapshot.get("provider_name") or ""),
                endpoint_family=str(snapshot.get("endpoint_family") or ""),
                fetched_at=snapshot.get("fetched_at"),
                cache_status=snapshot.get("cache_status"),
                fallback_used=bool(snapshot.get("fallback_used")),
                error_state=snapshot.get("error_state"),
            ).get("governance") or {}
        if str(snapshot.get("endpoint_family") or "") not in {"quote_latest", "fx"}:
            continue
        row = {
            "symbol": str(payload.get("identifier") or payload.get("metric") or ""),
            "provider": str(payload.get("provider_name") or snapshot.get("provider_name") or ""),
            "freshness_state": str(governance.get("freshness_state") or snapshot.get("freshness_state") or "unknown"),
            "confidence_label": str(governance.get("confidence_label") or snapshot.get("confidence_tier") or "low"),
            "fallback_used": bool(governance.get("fallback_used") or snapshot.get("fallback_used")),
            "coverage_status": str(governance.get("coverage_status") or "complete"),
            "fetched_at": str(snapshot.get("fetched_at") or ""),
        }
        key = (row["symbol"], row["provider"])
        current = rows_by_key.get(key)
        if current is None or str(current.get("fetched_at") or "") < row["fetched_at"]:
            rows_by_key[key] = row
    rows = sorted(rows_by_key.values(), key=lambda item: (item["symbol"], item["provider"]))
    current = sum(1 for row in rows if row["freshness_state"] == "current")
    aging = sum(1 for row in rows if row["freshness_state"] == "aging")
    stale = sum(1 for row in rows if row["freshness_state"] == "stale")
    fallback = sum(1 for row in rows if row["fallback_used"])
    low_conf = sum(1 for row in rows if row["confidence_label"] == "low")
    return {
        "tracked_count": len(rows),
        "current_count": current,
        "aging_count": aging,
        "stale_count": stale,
        "fallback_count": fallback,
        "low_confidence_count": low_conf,
        "status": (
            "high_confidence"
            if rows and stale == 0 and low_conf <= max(1, len(rows) // 4)
            else "mixed_confidence"
            if rows
            else "unavailable"
        ),
        "top_watch_items": rows[:8],
    }


def _dashboard_benchmark_watch_quality(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for snapshot in snapshots:
        if str(snapshot.get("surface_name") or "") != "dashboard" or str(snapshot.get("endpoint_family") or "") != "benchmark_proxy":
            continue
        payload = dict(snapshot.get("payload") or {})
        governance = dict(payload.get("governance") or {})
        if not governance:
            governance = _apply_governance(
                payload,
                provider_name=str(snapshot.get("provider_name") or ""),
                endpoint_family="benchmark_proxy",
                fetched_at=snapshot.get("fetched_at"),
                cache_status=snapshot.get("cache_status"),
                fallback_used=bool(snapshot.get("fallback_used")),
                error_state=snapshot.get("error_state"),
            ).get("governance") or {}
        row = {
            "symbol": str(payload.get("identifier") or payload.get("metric") or ""),
            "provider": str(payload.get("provider_name") or snapshot.get("provider_name") or ""),
            "freshness_state": str(governance.get("freshness_state") or snapshot.get("freshness_state") or "unknown"),
            "confidence_label": str(governance.get("confidence_label") or snapshot.get("confidence_tier") or "low"),
            "fallback_used": bool(governance.get("fallback_used") or snapshot.get("fallback_used")),
            "fetched_at": str(snapshot.get("fetched_at") or ""),
        }
        key = (row["symbol"], row["provider"])
        current = rows_by_key.get(key)
        if current is None or str(current.get("fetched_at") or "") < row["fetched_at"]:
            rows_by_key[key] = row
    rows = sorted(rows_by_key.values(), key=lambda item: (item["symbol"], item["provider"]))
    current = sum(1 for row in rows if row["freshness_state"] == "current")
    aging = sum(1 for row in rows if row["freshness_state"] == "aging")
    stale = sum(1 for row in rows if row["freshness_state"] == "stale")
    return {
        "tracked_count": len(rows),
        "current_count": current,
        "aging_count": aging,
        "stale_count": stale,
        "status": "healthy" if rows and stale == 0 else "aging" if rows and stale <= max(1, len(rows) // 2) else "stale" if rows else "unavailable",
        "top_watch_items": rows[:8],
    }


def _direct_runtime_sources() -> list[dict[str, Any]]:
    registry = get_freshness_registry()
    defaults: list[dict[str, Any]] = []
    for source_id in ("market_price", "benchmark_truth", "macro", "news"):
        freshness = registry.get_freshness(source_id)
        defaults.append(
            {
                "source_id": source_id,
                "source_family": source_id,
                "configured": True,
                "last_updated_utc": freshness.last_updated_utc,
                "staleness_seconds": freshness.staleness_seconds,
                "freshness_state": freshness.freshness_class.value,
                "current_status_reason": freshness.freshness_class.value,
            }
        )
    for provider in ("chronos", "timesfm", "moirai", "lagllama"):
        defaults.append(
            {
                "source_id": f"forecast_provider:{provider}",
                "source_family": "forecast_readiness",
                "configured": True,
                "current_status_reason": "unseen",
            }
        )
    return runtime_truth_rows(default_sources=defaults)


def _version_stamp(surface_name: str, family_name: str, items: list[dict[str, Any]]) -> tuple[str, str | None, str | None, list[str]]:
    providers = sorted({str(item.get("provider_name") or "") for item in items if str(item.get("provider_name") or "")})
    observed_candidates = [_parse_dt(item.get("observed_at")) for item in items]
    fetched_candidates = [_parse_dt(item.get("fetched_at")) for item in items]
    latest_observed = max((dt for dt in observed_candidates if dt is not None), default=None)
    latest_fetched = max((dt for dt in fetched_candidates if dt is not None), default=None)
    version = f"{surface_name}:{family_name}:{(latest_observed or latest_fetched or datetime.now(UTC)).strftime('%Y%m%d%H%M%S')}:{'-'.join(providers)[:80]}"
    return (
        version,
        latest_observed.isoformat() if latest_observed else None,
        latest_fetched.isoformat() if latest_fetched else None,
        providers,
    )


def build_cached_external_upstream_payload(conn, settings: Settings, *, surface_name: str | None = None) -> dict[str, Any]:
    ensure_provider_budget_tables(conn)
    activation_report = build_provider_activation_report(conn)
    registry = [dict(item) for item in build_provider_status_registry()]
    health_by_provider = {str(item.get("provider_name") or ""): item for item in list_provider_health(conn)}
    targets = _surface_targets(conn, surface_name) if surface_name else {}
    snapshots = list_provider_snapshots(conn, surface_name=surface_name, limit=300) if surface_name else list_provider_snapshots(conn, limit=300)
    if surface_name:
        snapshots = _filter_snapshots_to_targets(snapshots, surface_name=surface_name, targets=targets)
    snapshot_counts: dict[str, int] = {}
    for snapshot in snapshots:
        provider_name = str(snapshot.get("provider_name") or "")
        snapshot_counts[provider_name] = snapshot_counts.get(provider_name, 0) + 1
    providers: list[dict[str, Any]] = []
    governance_items: list[dict[str, Any]] = []
    public_health = public_upstream_health_summary(conn)
    for entry in registry:
        provider_key = str(entry.get("provider_key") or "")
        merged = dict(entry)
        health = health_by_provider.get(provider_key) or {}
        budget = peek_provider_budget_state(conn, provider_key) if provider_key in PROVIDER_CAPABILITY_MATRIX else {"mode": "normal"}
        merged["mode"] = budget.get("mode")
        merged["surface_budgets"] = [
            item for item in list_surface_budget_policies(conn)
            if str(item.get("provider_name") or "") == provider_key
        ]
        merged["last_successful_fetch_at"] = health.get("last_successful_fetch_at")
        merged["failure_streak"] = int(health.get("failure_streak") or 0)
        merged["last_error"] = health.get("last_error")
        merged["snapshot_count"] = int(snapshot_counts.get(provider_key) or 0)
        if provider_key not in PROVIDER_CAPABILITY_MATRIX:
            public_provider = next((item for item in list(public_health.get("providers") or []) if str(item.get("provider_key") or "") == provider_key), None) or {}
            if public_provider:
                merged["last_successful_fetch_at"] = public_provider.get("last_successful_fetch_at")
                merged["snapshot_count"] = int(public_provider.get("snapshot_count") or 0)
                merged["latest_observed_at"] = public_provider.get("latest_observed_at")
                merged["public_freshness_states"] = list(public_provider.get("freshness_states") or [])
                merged["public_snapshot_versions"] = list(public_provider.get("snapshot_versions") or [])
        if merged["snapshot_count"] > 0 and str(merged.get("status")) in {"configured_optional", "installed_optional", "public_available"}:
            merged["status"] = "active_cached"
        elif str(merged.get("status")) == "configured_optional":
            last_error = str(health.get("last_error") or "")
            if "auth_error:" in last_error or "403 Client Error" in last_error or "401 Client Error" in last_error:
                merged["status"] = "auth_failed_optional"
            elif "rate_limited:" in last_error or "429 Client Error" in last_error:
                merged["status"] = "rate_limited_optional"
            elif "not_found:" in last_error or "404 Client Error" in last_error:
                merged["status"] = "configured_optional_symbol_gap"
        elif str(health.get("current_mode") or "") in {"conserve", "critical_only", "blocked"}:
            merged["status"] = f"{str(merged.get('status') or 'unknown')}_{str(health.get('current_mode') or '').lower()}"
        merged["coverage_gap_classification"] = error_family(str(health.get("last_error") or ""))
        activation_entry = next(
            (
                item
                for item in list(activation_report.get("providers") or [])
                if str(item.get("provider_name") or "") == provider_key
            ),
            {},
        )
        merged["activation_state"] = activation_entry.get("activation_state")
        merged["active_families"] = list(activation_entry.get("active_families") or [])
        merged["inactive_families"] = list(activation_entry.get("inactive_families") or [])
        merged["truth_grade_active_families"] = list(activation_entry.get("truth_grade_active_families") or [])
        merged["support_grade_active_families"] = list(activation_entry.get("support_grade_active_families") or [])
        merged["canonical_truth_enabled"] = bool(activation_entry.get("canonical_truth_enabled"))
        providers.append(merged)
    for snapshot in snapshots:
        if str(snapshot.get("provider_name") or "") == "public_context_bundle":
            continue
        payload = dict(snapshot.get("payload") or {})
        governance_items.append(
            _apply_governance(
                payload,
                provider_name=str(snapshot.get("provider_name") or ""),
                endpoint_family=str(snapshot.get("endpoint_family") or ""),
                fetched_at=snapshot.get("fetched_at"),
                cache_status=snapshot.get("cache_status"),
                fallback_used=bool(snapshot.get("fallback_used")),
                error_state=snapshot.get("error_state"),
            )
        )
    daily_brief_context = _build_surface_context_from_snapshots("daily_brief", snapshots if surface_name == "daily_brief" else list_provider_snapshots(conn, surface_name="daily_brief", limit=120))
    dashboard_context = _build_surface_context_from_snapshots("dashboard", snapshots if surface_name == "dashboard" else list_provider_snapshots(conn, surface_name="dashboard", limit=120))
    blueprint_context = _build_surface_context_from_snapshots("blueprint", snapshots if surface_name == "blueprint" else list_provider_snapshots(conn, surface_name="blueprint", limit=120))
    runtime_rows = _direct_runtime_sources()
    payload = {
        "generated_at": _now_iso(),
        "providers": providers,
        "routed_provider_registry": [
            item for item in providers if bool(item.get("routed_provider"))
        ],
        "public_context_registry": [
            item for item in providers if str(item.get("source_scope") or "") == "public_context_only"
        ],
        "direct_sources": runtime_rows,
        "runtime_truth": runtime_rows,
        "active_targets": targets,
        "daily_brief_context": daily_brief_context,
        "dashboard_context": dashboard_context,
        "blueprint_context": blueprint_context,
        "health": list_provider_health(conn),
        "public_health": public_health,
        "governance_items": governance_items[:24],
        "surface_budget_policies": list_surface_budget_policies(conn),
        "snapshot_versions": list_surface_snapshot_versions(conn),
        "family_success": list_provider_family_success(conn, surface_name=surface_name) if surface_name else list_provider_family_success(conn),
        "activation_report": activation_report,
    }
    family_comparisons = [
        compare_family_providers(conn, surface_name=(surface_name or current_surface), family_name=_canonical_family_name(family_name))
        for current_surface in ([surface_name] if surface_name else ["daily_brief", "dashboard", "blueprint"])
        for family_name in surface_families(surface_name or current_surface)
    ]
    provider_issues = [str(item.get("label") or "") for item in providers if int(item.get("failure_streak") or 0) > 0 or str(item.get("mode") or "") == "blocked"]
    governance_summary = summarize_surface_sufficiency(governance_items, surface_name=surface_name or "daily_brief")
    source_diversity = _surface_source_diversity(payload, surface_name or "daily_brief")
    slot_validity = (
        _daily_brief_slot_validity(payload=payload, settings=settings)
        if (surface_name or "daily_brief") == "daily_brief"
        else None
    )
    quote_quality = _dashboard_quote_quality(
        _filter_snapshots_to_targets(
            list_provider_snapshots(conn, surface_name="dashboard", limit=120),
            surface_name="dashboard",
            targets=_surface_targets(conn, "dashboard"),
        )
    )
    benchmark_watch_quality = _dashboard_benchmark_watch_quality(
        _filter_snapshots_to_targets(
            list_provider_snapshots(conn, surface_name="dashboard", limit=120),
            surface_name="dashboard",
            targets=_surface_targets(conn, "dashboard"),
        )
    )
    surface_issue_summary = _surface_issue_summary(
        surface_name=surface_name or "daily_brief",
        governance=governance_summary,
        source_diversity=source_diversity,
        quote_quality=quote_quality,
        benchmark_watch_quality=benchmark_watch_quality,
    )
    payload["summary"] = {
        "provider_count": len(providers),
        "live_count": sum(
            1
            for item in providers
            if str(item.get("activation_state") or "") == "active"
            or (
                not bool(item.get("requires_api_key"))
                and int(item.get("snapshot_count") or 0) > 0
                and str(item.get("activation_state") or "") not in {"non_viable", "unconfigured"}
            )
        ),
        "canonical_truth_live_count": sum(1 for item in providers if bool(item.get("canonical_truth_enabled"))),
        "support_only_live_count": sum(
            1
            for item in providers
            if not bool(item.get("canonical_truth_enabled")) and bool(item.get("support_grade_active_families"))
        ),
        "public_live_count": sum(1 for item in providers if not bool(item.get("requires_api_key")) and str(item.get("snapshot_count") or 0) != "0"),
        "configured_optional_count": sum(1 for item in providers if bool(item.get("requires_api_key")) and bool(item.get("configured"))),
        "issues": surface_issue_summary["issues"],
        "surface_issue_count": surface_issue_summary["issue_count"],
        "provider_issue_count": len(provider_issues),
        "provider_issues": provider_issues,
        "governance": governance_summary,
        "family_ownership": family_ownership_map(),
        "source_family_coverage": dict(activation_report.get("source_family_coverage") or {}),
        "truth_grade_source_family_coverage": {
            key: value
            for key, value in dict(activation_report.get("source_family_coverage") or {}).items()
            if str(dict(value).get("authority") or "") == "truth_grade"
        },
        "support_grade_source_family_coverage": {
            key: value
            for key, value in dict(activation_report.get("source_family_coverage") or {}).items()
            if str(dict(value).get("authority") or "") == "support_grade"
        },
        "provider_activation_summary": dict(activation_report.get("summary") or {}),
        "source_diversity": source_diversity,
        "slot_validity": slot_validity,
        "quote_quality": quote_quality,
        "benchmark_watch_quality": benchmark_watch_quality,
        "family_comparisons": family_comparisons,
        "health_model": {
            "provider_health_scope": "global_provider_centric",
            "surface_sufficiency_scope": "surface_family_universe_specific",
            "note": "A provider can be healthy globally while a surface family remains weak because of cadence, symbol fit, or family success.",
        },
    }
    fx_coverage = dict(dict(activation_report.get("source_family_coverage") or {}).get("fx") or {})
    payload["summary"]["fx_runtime_viability"] = (
        "truth_grade_active"
        if str(fx_coverage.get("coverage_state") or "") == "healthy" and str(fx_coverage.get("authority") or "") == "truth_grade"
        else "support_only_latest_available"
        if str(fx_coverage.get("configured_provider_count") or "0") != "0"
        else "no_viable_runtime_provider"
    )
    payload["summary"]["fx_truth_state"] = (
        "truth_grade_same_day"
        if payload["summary"]["fx_runtime_viability"] == "truth_grade_active"
        else "contextual_fallback_only"
    )
    payload["summary"]["consistency_warnings"] = _context_consistency(payload)
    diversity = payload["summary"]["source_diversity"]
    if int(diversity.get("single_source_count") or 0) > 0 and _status_rank(payload["summary"]["governance"]["status"]) >= _status_rank("enough_for_interpretation"):
        payload["summary"]["governance"]["status"] = _downgrade_status_once(str(payload["summary"]["governance"]["status"]))
    if int(diversity.get("unavailable_count") or 0) > 0 and _status_rank(payload["summary"]["governance"]["status"]) >= _status_rank("enough_for_monitoring"):
        payload["summary"]["governance"]["status"] = _downgrade_status_once(str(payload["summary"]["governance"]["status"]))
    return payload


def build_provider_audit_report(conn, settings: Settings) -> dict[str, Any]:
    payload = build_cached_external_upstream_payload(conn, settings)
    report_rows: list[dict[str, Any]] = []
    by_surface_family: dict[tuple[str, str], dict[str, Any]] = {}
    for item in list(payload.get("family_success") or []):
        surface = str(item.get("surface_name") or "")
        family = str(item.get("family_name") or "")
        by_surface_family.setdefault((surface, family), {"surface": surface, "family": family, "rows": []})["rows"].append(item)
    for (surface, family), bucket in by_surface_family.items():
        rows = list(bucket["rows"])
        rows_sorted = sorted(
            rows,
            key=lambda item: (
                float(item.get("reliability_score") or 0.0),
                int(item.get("current_snapshot_count") or 0),
                -float(item.get("median_freshness_seconds") or 10**12),
            ),
            reverse=True,
        )
        diversity_item = next(
            (
                item
                for item in list(((payload.get("summary") or {}).get("source_diversity") or {}).get("critical_families") or [])
                if str(item.get("family") or "") == family
            ),
            {},
        )
        weakest = rows_sorted[-1] if rows_sorted else {}
        best = rows_sorted[0] if rows_sorted else {}
        failure_class = str(weakest.get("last_root_error_class") or weakest.get("last_error_class") or "")
        effective_failure_class = str(weakest.get("last_effective_error_class") or weakest.get("last_error_class") or "")
        if not failure_class and int(weakest.get("empty_response_count") or 0) > 0:
            failure_class = "empty_response"
        if str(diversity_item.get("state") or "") == "single_source":
            investor_impact = "critical_family_fragile"
        elif int(best.get("stale_snapshot_count") or 0) > int(best.get("current_snapshot_count") or 0):
            investor_impact = "freshness_weak"
        elif failure_class in {"plan_limited", "endpoint_blocked"}:
            investor_impact = "provider_constraint"
        else:
            investor_impact = "contained"
        fallback = (family_ownership_map().get(family) or {}).get("legacy_fallback") or []
        report_rows.append(
            {
                "issue": f"{surface}:{family}",
                "surface": surface,
                "family": family,
                "provider": str(weakest.get("provider_name") or ""),
                "failure_class": failure_class or "none",
                "effective_failure_class": effective_failure_class or "none",
                "current_freshness": "current" if int(best.get("current_snapshot_count") or 0) > 0 else "aging_or_stale",
                "source_diversity": str(diversity_item.get("state") or "unknown"),
                "investor_impact": investor_impact,
                "current_fallback": list(fallback),
                "suppression_reason": str(weakest.get("last_suppression_reason") or "") or None,
                "recommended_routing_action": compare_family_providers(conn, surface_name=surface, family_name=family).get("recommended_routing_change"),
            }
        )
    report_rows.extend(
        {
            "issue": f"public:{provider.get('provider_key')}",
            "surface": "shared",
            "family": ",".join(list(provider.get("families") or [])),
            "provider": str(provider.get("provider_key") or ""),
            "failure_class": str(provider.get("error_state") or "none"),
            "current_freshness": ",".join(list(provider.get("freshness_states") or [])),
            "source_diversity": "public_context",
            "investor_impact": "contextual",
            "current_fallback": [],
            "recommended_routing_action": "keep_current_routing",
        }
        for provider in list((payload.get("public_health") or {}).get("providers") or [])
    )
    return {
        "generated_at": _now_iso(),
        "issues": report_rows,
        "summary": {
            "issue_count": len(report_rows),
            "single_source_critical_families": int(((payload.get("summary") or {}).get("source_diversity") or {}).get("single_source_count") or 0),
            "public_upstream_provider_count": len(list((payload.get("public_health") or {}).get("providers") or [])),
        },
    }
