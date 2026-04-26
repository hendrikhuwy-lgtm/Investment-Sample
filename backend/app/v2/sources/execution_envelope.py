from __future__ import annotations

from typing import Any


def _as_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except Exception:
        return None


def market_execution_profile(
    *,
    current_value: Any,
    change_pct_1d: Any,
    source_family: str,
) -> dict[str, Any]:
    family = str(source_family or "").strip()
    current = _as_float(current_value)
    change = _as_float(change_pct_1d)

    if family in {"fx_reference", "macro_fx_proxy", "usd_strength_fallback"}:
        return {
            "usable_truth": current is not None,
            "semantic_grade": "derived_proxy" if current is not None else "unavailable",
            "sufficiency_state": "proxy_bounded" if current is not None else "insufficient",
            "data_mode": "derived" if current is not None else "unavailable",
            "authority_level": "derived" if current is not None else "unavailable",
        }

    authority_level = "proxy" if family == "benchmark_proxy" else "direct"
    if current is None:
        return {
            "usable_truth": False,
            "semantic_grade": "unavailable",
            "sufficiency_state": "insufficient",
            "data_mode": "unavailable",
            "authority_level": "unavailable",
        }
    if change is not None:
        return {
            "usable_truth": True,
            "semantic_grade": "movement_capable",
            "sufficiency_state": "movement_capable",
            "data_mode": "live",
            "authority_level": authority_level,
        }
    return {
        "usable_truth": True,
        "semantic_grade": "price_only",
        "sufficiency_state": "price_only",
        "data_mode": "live",
        "authority_level": authority_level,
    }


def payload_execution_profile(
    *,
    payload: dict[str, Any] | None,
    source_family: str,
) -> dict[str, Any]:
    raw = dict(payload or {})
    family = str(source_family or "").strip()
    if family in {
        "market_close",
        "quote_latest",
        "benchmark_proxy",
        "fx_reference",
        "macro_fx_proxy",
        "usd_strength_fallback",
    }:
        return market_execution_profile(
            current_value=raw.get("price") or raw.get("value") or raw.get("close"),
            change_pct_1d=raw.get("change_pct_1d"),
            source_family=family,
        )

    usable_truth = raw.get("value") is not None or bool(raw.get("series"))
    if not usable_truth:
        return {
            "usable_truth": False,
            "semantic_grade": "unavailable",
            "sufficiency_state": "insufficient",
            "data_mode": "unavailable",
            "authority_level": "unavailable",
        }
    if bool(raw.get("series")):
        return {
            "usable_truth": True,
            "semantic_grade": "history_capable",
            "sufficiency_state": "history_capable",
            "data_mode": "live",
            "authority_level": "direct",
        }
    return {
        "usable_truth": True,
        "semantic_grade": "field_present",
        "sufficiency_state": "field_present",
        "data_mode": "live",
        "authority_level": "direct",
    }


def build_provider_execution(
    *,
    provider_name: str | None,
    source_family: str,
    identifier: str,
    provider_symbol: str | None = None,
    observed_at: str | None = None,
    fetched_at: str | None = None,
    cache_status: str | None = None,
    fallback_used: bool = False,
    error_state: str | None = None,
    freshness_class: str | None = None,
    path_used: str | None = None,
    live_or_cache: str | None = None,
    usable_truth: bool,
    semantic_grade: str | None,
    sufficiency_state: str | None = None,
    data_mode: str | None = None,
    authority_level: str | None = None,
    provenance_strength: str | None = None,
    insufficiency_reason: str | None = None,
) -> dict[str, Any]:
    resolved_path = str(path_used or "").strip() or None
    resolved_live_or_cache = str(live_or_cache or "").strip() or None
    if resolved_live_or_cache is None:
        if str(cache_status or "") in {"hit", "stale_reuse"}:
            resolved_live_or_cache = "cache"
        elif resolved_path and "fallback" in resolved_path:
            resolved_live_or_cache = "fallback"
        else:
            resolved_live_or_cache = "live"

    resolved_data_mode = str(data_mode or "").strip() or None
    if resolved_data_mode is None:
        if not usable_truth:
            resolved_data_mode = "unavailable"
        elif str(authority_level or "") in {"derived", "proxy"} and str(source_family or "") in {
            "fx_reference",
            "macro_fx_proxy",
            "usd_strength_fallback",
        }:
            resolved_data_mode = "derived"
        elif resolved_live_or_cache == "cache":
            resolved_data_mode = "cache"
        else:
            resolved_data_mode = "live"

    resolved_authority = str(authority_level or "").strip() or None
    if resolved_authority is None:
        if not usable_truth:
            resolved_authority = "unavailable"
        elif resolved_data_mode == "derived":
            resolved_authority = "derived"
        elif str(source_family or "") == "benchmark_proxy":
            resolved_authority = "proxy"
        else:
            resolved_authority = "direct"

    resolved_sufficiency = str(sufficiency_state or "").strip() or None
    if resolved_sufficiency is None:
        resolved_sufficiency = "sufficient" if usable_truth else "insufficient"

    resolved_reason = str(insufficiency_reason or error_state or "").strip() or None
    resolved_provenance = str(provenance_strength or "").strip() or None
    if resolved_provenance is None:
        if not usable_truth:
            resolved_provenance = "degraded"
        elif resolved_data_mode == "derived" or resolved_authority in {"derived", "proxy"}:
            resolved_provenance = "derived_or_proxy"
        elif resolved_live_or_cache == "cache":
            resolved_provenance = "cache_continuity"
        else:
            resolved_provenance = "live_authoritative"

    return {
        "provider_name": str(provider_name or "").strip() or None,
        "source_family": str(source_family or "").strip() or None,
        "identifier": str(identifier or "").strip() or None,
        "provider_symbol": str(provider_symbol or identifier or "").strip() or None,
        "observed_at": str(observed_at or "").strip() or None,
        "fetched_at": str(fetched_at or "").strip() or None,
        "cache_status": str(cache_status or "").strip() or None,
        "fallback_used": bool(fallback_used),
        "error_state": str(error_state or "").strip() or None,
        "freshness_class": str(freshness_class or "").strip() or None,
        "path_used": resolved_path,
        "live_or_cache": resolved_live_or_cache,
        "usable_truth": bool(usable_truth),
        "semantic_grade": str(semantic_grade or "").strip() or None,
        "sufficiency_state": resolved_sufficiency,
        "data_mode": resolved_data_mode,
        "authority_level": resolved_authority,
        "provenance_strength": resolved_provenance,
        "insufficiency_reason": resolved_reason,
    }
