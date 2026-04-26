from __future__ import annotations

import html
import re
import sqlite3
from datetime import UTC, datetime
from typing import Any

import httpx
from app.config import get_db_path
from app.services.ingest_etf_data import get_etf_source_config
from app.services.provider_adapters import ProviderAdapterError, fetch_provider_data
from app.services.provider_cache import get_cached_provider_snapshot, put_provider_snapshot
from app.services.provider_registry import provider_support_status, routed_provider_candidates
from app.services.blueprint_benchmark_registry import DEFAULT_BENCHMARK_ASSIGNMENTS
from app.v2.donors.source_freshness import FreshnessClass, FreshnessState
from app.v2.sources.freshness_registry import register_source
from app.v2.sources.market_price_adapter import fetch as fetch_market_price
from app.v2.sources.runtime_truth import record_runtime_truth


source_tier: str = "1A"
_SOURCE_ID = "benchmark_truth"
_BENCHMARK_PROXY_PREFERENCES: dict[str, list[str]] = {
    "SHORT_TBILL": ["BIL", "SGOV", "BILS", "SHV"],
    "MSCI_WORLD": ["ACWI", "URTH"],
    "MSCI_EM_IMI": ["IEMG", "EEM"],
    "MSCI_CHINA": ["GXC", "ASHR", "MCHI"],
}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _cached_family_payload(
    endpoint_family: str,
    identifier: str,
    *,
    surface_name: str | None = None,
) -> dict[str, Any]:
    normalized = str(identifier or "").strip().upper()
    if not normalized:
        return {}
    surfaces = [item for item in [surface_name, "blueprint"] if item]
    with _connection() as conn:
        for provider_name in routed_provider_candidates(endpoint_family, identifier=normalized):
            supported, _ = provider_support_status(provider_name, endpoint_family, normalized)
            if not supported:
                continue
            for candidate_surface in surfaces:
                snapshot = get_cached_provider_snapshot(
                    conn,
                    provider_name=provider_name,
                    endpoint_family=endpoint_family,
                    cache_key=normalized,
                    surface_name=candidate_surface,
                )
                if snapshot is None:
                    continue
                payload = dict(snapshot.get("payload") or {})
                payload.setdefault("provider_name", provider_name)
                payload.setdefault("retrieval_path", "routed_cache")
                payload.setdefault("cache_status", str(snapshot.get("cache_status") or "hit"))
                payload.setdefault("freshness_state", snapshot.get("freshness_state"))
                return payload
    return {}


def _runtime_field_provenance(
    *,
    source_family: str,
    authority_kind: str,
    payload: dict[str, Any] | None,
    usable_truth: bool,
    sufficiency_state: str,
    data_mode: str,
    insufficiency_reason: str | None = None,
    observed_at: str | None = None,
) -> dict[str, Any]:
    raw = dict(payload or {})
    execution = dict(raw.get("provider_execution") or {})
    retrieval_path = str(raw.get("retrieval_path") or execution.get("path_used") or "").strip() or None
    cache_status = str(raw.get("cache_status") or execution.get("cache_status") or "").strip()
    live_or_cache = str(execution.get("live_or_cache") or "").strip() or None
    if live_or_cache is None:
        if cache_status in {"hit", "stale_reuse"}:
            live_or_cache = "cache"
        elif retrieval_path and "fallback" in retrieval_path:
            live_or_cache = "fallback"
        elif retrieval_path and "cache" in retrieval_path:
            live_or_cache = "cache"
        else:
            live_or_cache = "live" if usable_truth else "fallback"
    provenance_strength = str(execution.get("provenance_strength") or "").strip() or None
    if provenance_strength is None:
        if not usable_truth:
            provenance_strength = "degraded"
        elif data_mode == "cache" or live_or_cache == "cache":
            provenance_strength = "cache_continuity"
        elif authority_kind == "derived":
            provenance_strength = "derived_or_proxy"
        else:
            provenance_strength = "live_authoritative"
    return {
        "authority_kind": authority_kind,
        "source_family": source_family,
        "provider": raw.get("provider_name") or execution.get("provider_name"),
        "path": retrieval_path,
        "live_or_cache": live_or_cache,
        "usable_truth": usable_truth,
        "sufficiency_state": sufficiency_state,
        "data_mode": data_mode,
        "authority_level": execution.get("authority_level") or ("derived" if authority_kind == "derived" else "direct" if usable_truth else "unavailable"),
        "observed_at": observed_at or raw.get("observed_at") or raw.get("as_of_utc") or execution.get("observed_at"),
        "provenance_strength": provenance_strength,
        "insufficiency_reason": insufficiency_reason or raw.get("error_state") or raw.get("error"),
        "freshness": raw.get("freshness_state") or execution.get("freshness_class"),
    }


def _benchmark_rows() -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for ticker, assignment in DEFAULT_BENCHMARK_ASSIGNMENTS.items():
        benchmark_id = str(assignment.get("benchmark_key") or "").strip()
        if not benchmark_id:
            continue
        row = by_key.setdefault(
            benchmark_id,
            {
                "benchmark_id": benchmark_id,
                "name": str(assignment.get("benchmark_label") or benchmark_id.replace("_", " ").title()),
                "proxy_symbol": str(assignment.get("benchmark_proxy_symbol") or "").strip() or None,
                "benchmark_source_type": assignment.get("benchmark_source_type"),
                "benchmark_confidence": assignment.get("benchmark_confidence"),
                "mapped_tickers": [],
                "updated_at": _now_iso(),
                "source_tier": source_tier,
            },
        )
        row["mapped_tickers"].append(ticker)
    return [by_key[key] for key in sorted(by_key)]


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except Exception:
        return None


def _confidence_rank(value: Any) -> int:
    normalized = str(value or "").strip().lower()
    return {"high": 3, "medium": 2, "low": 1}.get(normalized, 0)


def _proxy_candidates_for_benchmark(benchmark_id: str, row: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    for symbol in _BENCHMARK_PROXY_PREFERENCES.get(str(benchmark_id or "").upper(), []):
        normalized = str(symbol or "").strip().upper()
        if normalized:
            candidates.append(normalized)
    mapped_rows: list[tuple[int, str]] = []
    for ticker in list(row.get("mapped_tickers") or []):
        assignment = dict(DEFAULT_BENCHMARK_ASSIGNMENTS.get(str(ticker or "").upper()) or {})
        proxy_symbol = str(assignment.get("benchmark_proxy_symbol") or "").strip().upper()
        if not proxy_symbol:
            continue
        mapped_rows.append((_confidence_rank(assignment.get("benchmark_confidence")), proxy_symbol))
    for _rank, proxy_symbol in sorted(mapped_rows, key=lambda item: (-item[0], item[1])):
        candidates.append(proxy_symbol)
    default_proxy = str(row.get("proxy_symbol") or "").strip().upper()
    if default_proxy:
        candidates.append(default_proxy)
    return list(dict.fromkeys([candidate for candidate in candidates if candidate]))


def _series_points(payload: dict[str, Any]) -> list[tuple[datetime, float]]:
    raw = payload.get("series")
    parsed: list[tuple[datetime, float]] = []
    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            observed_at = row.get("date") or row.get("datetime") or row.get("timestamp") or row.get("t")
            if observed_at in {None, ""}:
                continue
            try:
                dt = datetime.fromisoformat(str(observed_at).replace("Z", "+00:00")).astimezone(UTC)
            except Exception:
                continue
            value = _safe_float(
                row.get("close")
                or row.get("adjClose")
                or row.get("adj_close")
                or row.get("c")
                or row.get("value")
            )
            if value is None:
                continue
            parsed.append((dt, value))
    elif isinstance(raw, dict):
        for observed_at, row in raw.items():
            if not isinstance(row, dict):
                continue
            try:
                dt = datetime.fromisoformat(str(observed_at).replace("Z", "+00:00")).astimezone(UTC)
            except Exception:
                continue
            value = _safe_float(row.get("4. close") or row.get("close"))
            if value is None:
                continue
            parsed.append((dt, value))
    parsed.sort(key=lambda item: item[0])
    return parsed


def _fetch_history_payload(
    proxy_symbol: str,
    *,
    surface_name: str | None = None,
    allow_live_fetch: bool = True,
) -> dict[str, Any]:
    cached = _cached_family_payload("ohlcv_history", proxy_symbol, surface_name=surface_name)
    if cached and cached.get("series"):
        return cached
    if not allow_live_fetch:
        return {
            "provider_name": None,
            "endpoint_family": "ohlcv_history",
            "identifier": str(proxy_symbol),
            "cache_status": "unavailable",
            "error_state": str((cached or {}).get("error_state") or "history_cache_missing"),
            "freshness_state": "unavailable",
            "retrieval_path": "cached_only_unavailable",
        }
    from app.services.provider_refresh import fetch_routed_family

    last_error = str((cached or {}).get("error_state") or "history_cache_missing")
    with _connection() as conn:
        payload = fetch_routed_family(
            conn,
            surface_name=surface_name or "candidate_report",
            endpoint_family="ohlcv_history",
            identifier=str(proxy_symbol),
            triggered_by_job="benchmark_truth_adapter",
            force_refresh=False,
        )
    if payload.get("provider_name") or payload.get("series"):
        return payload
    last_error = str(payload.get("error_state") or last_error)
    normalized = str(proxy_symbol or "").strip().upper()
    for provider_name in routed_provider_candidates("ohlcv_history", identifier=normalized):
        supported, _ = provider_support_status(provider_name, "ohlcv_history", normalized)
        if not supported:
            continue
        try:
            direct_payload = dict(fetch_provider_data(provider_name, "ohlcv_history", normalized) or {})
        except ProviderAdapterError as exc:
            last_error = f"{provider_name}:{exc.error_class}"
            continue
        if direct_payload.get("series"):
            direct_payload.setdefault("provider_name", provider_name)
            direct_payload.setdefault("endpoint_family", "ohlcv_history")
            direct_payload.setdefault("retrieval_path", "direct_live")
            direct_payload.setdefault("cache_status", "miss")
            direct_payload.setdefault("freshness_state", "current")
            direct_payload.setdefault(
                "provider_execution",
                {
                    "provider_name": provider_name,
                    "source_family": "ohlcv_history",
                    "path_used": "direct_live",
                    "live_or_cache": "live",
                    "usable_truth": True,
                    "sufficiency_state": "history_capable",
                    "data_mode": "live",
                    "authority_level": "derived",
                    "observed_at": direct_payload.get("observed_at"),
                    "provenance_strength": "live_authoritative",
                },
            )
            with _connection() as conn:
                put_provider_snapshot(
                    conn,
                    provider_name=provider_name,
                    endpoint_family="ohlcv_history",
                    cache_key=normalized,
                    payload=direct_payload,
                    surface_name=surface_name,
                    freshness_state=str(direct_payload.get("freshness_state") or "current"),
                    confidence_tier="high",
                    source_ref=str(direct_payload.get("source_ref") or f"{provider_name}:ohlcv_history"),
                    ttl_seconds=6 * 60 * 60,
                    cache_status="miss",
                    fallback_used=False,
                    error_state=None,
                )
            return direct_payload
    return {
        "provider_name": None,
        "endpoint_family": "ohlcv_history",
        "identifier": str(proxy_symbol),
        "cache_status": "unavailable",
        "error_state": last_error,
        "freshness_state": "unavailable",
        "retrieval_path": "routed_unavailable",
    }


def _return_since(points: list[tuple[datetime, float]], *, anchor: datetime) -> float | None:
    if not points:
        return None
    latest_value = points[-1][1]
    anchor_point = next((value for ts, value in points if ts >= anchor), None)
    if anchor_point in {None, 0.0}:
        return None
    try:
        return ((latest_value - anchor_point) / abs(anchor_point)) * 100.0
    except Exception:
        return None


def _official_proxy_performance_fallback(proxy_symbol: str) -> dict[str, Any] | None:
    normalized = str(proxy_symbol or "").strip().upper()
    config = get_etf_source_config(normalized) or {}
    market_source = dict((config.get("data_sources") or {}).get("market_data") or {})
    url = str(market_source.get("url") or "").strip()
    provider_label = str(market_source.get("provider") or "").strip() or None
    if normalized not in {"BIL", "BILS"} or "ssga.com" not in url.lower():
        return None
    try:
        with httpx.Client(timeout=20.0, follow_redirects=True) as client:
            response = client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
    except Exception:
        return None
    payload = html.unescape(response.text)
    anchor = payload.find("fund-perf-ann-net-ytd")
    if anchor < 0:
        return None
    segment = payload[anchor : anchor + 1800]
    ytd_match = re.search(
        r'fund-perf-ann-net-ytd":\{"label":"YTD".*?"asOfDateSimple":"([^"]+)".*?"originalValue":"([+-]?[0-9]+(?:\.[0-9]+)?)"',
        segment,
        re.IGNORECASE | re.DOTALL,
    )
    annual_match = re.search(
        r'fund-perf-ann-net-total-qtr-before-tax":\{"label":"NAV","asOfDate":"as of ([^"]+)".*?"yr1":\{"label":"1 Year","value":"[^"]+".*?"originalValue":"([+-]?[0-9]+(?:\.[0-9]+)?)"',
        payload,
        re.IGNORECASE | re.DOTALL,
    )
    if not ytd_match or not annual_match:
        return None
    ytd_observed_at = None
    try:
        ytd_observed_at = datetime.strptime(ytd_match.group(1), "%b %d %Y").replace(tzinfo=UTC).isoformat()
    except Exception:
        ytd_observed_at = None
    one_year_observed_at = None
    try:
        one_year_observed_at = datetime.strptime(annual_match.group(1), "%b %d %Y").replace(tzinfo=UTC).isoformat()
    except Exception:
        one_year_observed_at = ytd_observed_at
    return {
        "provider_name": str(provider_label or "ssga").lower(),
        "retrieval_path": "official_summary_live",
        "cache_status": "miss",
        "freshness_state": "current",
        "observed_at": ytd_observed_at,
        "ytd_observed_at": ytd_observed_at,
        "one_year_observed_at": one_year_observed_at,
        "ytd_return_pct": _safe_float(ytd_match.group(2)),
        "one_year_return_pct": _safe_float(annual_match.group(2)),
        "provider_execution": {
            "provider_name": str(provider_label or "ssga").lower(),
            "source_family": "benchmark_proxy",
            "path_used": "official_summary_live",
            "live_or_cache": "live",
            "usable_truth": True,
            "sufficiency_state": "summary_return_available",
            "data_mode": "live",
            "authority_level": "proxy",
            "observed_at": ytd_observed_at,
            "provenance_strength": "live_authoritative",
        },
    }


def _history_fields(
    proxy_symbol: str | None,
    *,
    surface_name: str | None = None,
    allow_live_fetch: bool = True,
) -> dict[str, Any]:
    if not proxy_symbol:
        return {
            "ytd_return_pct": None,
            "one_year_return_pct": None,
            "history_provider_name": None,
            "history_error": "missing_proxy_symbol",
            "field_provenance": {
                "ytd_return_pct": {"authority_kind": "unavailable", "source_family": "ohlcv_history"},
                "one_year_return_pct": {"authority_kind": "unavailable", "source_family": "ohlcv_history"},
            },
        }
    payload = _fetch_history_payload(str(proxy_symbol), surface_name=surface_name, allow_live_fetch=allow_live_fetch)
    points = _series_points(payload)
    if not points:
        summary_fallback = _official_proxy_performance_fallback(str(proxy_symbol)) if allow_live_fetch else None
        if summary_fallback:
            record_runtime_truth(
                source_id=_SOURCE_ID,
                source_family="benchmark_proxy",
                field_name="benchmark_returns_summary",
                symbol_or_entity=str(proxy_symbol),
                provider_used=str(summary_fallback.get("provider_name") or "") or None,
                path_used=str(summary_fallback.get("retrieval_path") or "official_summary_live"),
                live_or_cache="live",
                usable_truth=True,
                freshness=str(summary_fallback.get("freshness_state") or "current"),
                insufficiency_reason=None,
                semantic_grade="summary_return_available",
                attempt_succeeded=True,
            )
            return {
                "ytd_return_pct": summary_fallback.get("ytd_return_pct"),
                "one_year_return_pct": summary_fallback.get("one_year_return_pct"),
                "history_provider_name": summary_fallback.get("provider_name"),
                "history_error": None,
                "field_provenance": {
                    "ytd_return_pct": _runtime_field_provenance(
                        source_family="benchmark_proxy",
                        authority_kind="derived",
                        payload=summary_fallback,
                        usable_truth=True,
                        sufficiency_state="summary_return_available",
                        data_mode="live",
                        observed_at=summary_fallback.get("ytd_observed_at") or summary_fallback.get("observed_at"),
                    ),
                    "one_year_return_pct": _runtime_field_provenance(
                        source_family="benchmark_proxy",
                        authority_kind="derived",
                        payload=summary_fallback,
                        usable_truth=True,
                        sufficiency_state="summary_return_available",
                        data_mode="live",
                        observed_at=summary_fallback.get("one_year_observed_at") or summary_fallback.get("observed_at"),
                    ),
                },
            }
        record_runtime_truth(
            source_id=_SOURCE_ID,
            source_family="ohlcv_history",
            field_name="benchmark_returns",
            symbol_or_entity=str(proxy_symbol),
            provider_used=str(payload.get("provider_name") or "") or None,
            path_used=str(payload.get("retrieval_path") or "routed_unavailable"),
            live_or_cache="cache" if str(payload.get("cache_status") or "") in {"hit", "stale_reuse"} else "live",
            usable_truth=False,
            freshness=str(payload.get("freshness_state") or "unavailable"),
            insufficiency_reason=str(payload.get("error_state") or "history_missing"),
            semantic_grade="insufficient_history",
            attempt_succeeded=bool(payload.get("provider_name")),
        )
        return {
            "ytd_return_pct": None,
            "one_year_return_pct": None,
            "history_provider_name": payload.get("provider_name"),
            "history_error": payload.get("error_state") or "history_missing",
            "field_provenance": {
                "ytd_return_pct": _runtime_field_provenance(
                    source_family="ohlcv_history",
                    authority_kind="unavailable",
                    payload=payload,
                    usable_truth=False,
                    sufficiency_state="insufficient",
                    data_mode="unavailable",
                    insufficiency_reason=payload.get("error_state") or "history_missing",
                ),
                "one_year_return_pct": _runtime_field_provenance(
                    source_family="ohlcv_history",
                    authority_kind="unavailable",
                    payload=payload,
                    usable_truth=False,
                    sufficiency_state="insufficient",
                    data_mode="unavailable",
                    insufficiency_reason=payload.get("error_state") or "history_missing",
                ),
            },
        }
    latest_dt = points[-1][0]
    ytd_anchor = datetime(latest_dt.year, 1, 1, tzinfo=UTC)
    one_year_anchor = latest_dt.replace(month=latest_dt.month, day=min(latest_dt.day, 28), year=latest_dt.year - 1)
    record_runtime_truth(
        source_id=_SOURCE_ID,
        source_family="ohlcv_history",
        field_name="benchmark_returns",
        symbol_or_entity=str(proxy_symbol),
        provider_used=str(payload.get("provider_name") or "") or None,
        path_used=str(payload.get("retrieval_path") or "routed_live"),
        live_or_cache="cache" if str(payload.get("cache_status") or "") in {"hit", "stale_reuse"} else "live",
        usable_truth=True,
        freshness=str(payload.get("freshness_state") or "current"),
        insufficiency_reason=None,
        semantic_grade="history_capable",
        attempt_succeeded=True,
    )
    return {
        "ytd_return_pct": _return_since(points, anchor=ytd_anchor),
        "one_year_return_pct": _return_since(points, anchor=one_year_anchor),
        "history_provider_name": payload.get("provider_name"),
        "history_error": payload.get("error_state"),
        "field_provenance": {
            "ytd_return_pct": _runtime_field_provenance(
                source_family="ohlcv_history",
                authority_kind="derived",
                payload=payload,
                usable_truth=True,
                sufficiency_state="history_capable",
                data_mode="cache" if str(payload.get("cache_status") or "") in {"hit", "stale_reuse"} else "live",
            ),
            "one_year_return_pct": _runtime_field_provenance(
                source_family="ohlcv_history",
                authority_kind="derived",
                payload=payload,
                usable_truth=True,
                sufficiency_state="history_capable",
                data_mode="cache" if str(payload.get("cache_status") or "") in {"hit", "stale_reuse"} else "live",
            ),
        },
    }


def _performance_fields(
    proxy_symbol: str | None,
    *,
    surface_name: str | None = None,
    allow_live_fetch: bool = True,
) -> dict[str, Any]:
    if not proxy_symbol:
        return {
            "current_value": None,
            "ytd_return_pct": None,
            "one_year_return_pct": None,
        }
    market_payload = _cached_family_payload("quote_latest", proxy_symbol, surface_name=surface_name)
    if allow_live_fetch and (not market_payload or market_payload.get("price") is None):
        live_payload = fetch_market_price(str(proxy_symbol), surface_name=surface_name)
        if isinstance(live_payload, dict):
            market_payload = dict(live_payload)
    if not market_payload:
        market_payload = {
            "provider_name": None,
            "price": None,
            "retrieval_path": "routed_unavailable",
            "freshness_state": "unavailable",
            "error": "quote_cache_missing",
        }
    history_fields = _history_fields(
        proxy_symbol,
        surface_name=surface_name or "candidate_report",
        allow_live_fetch=allow_live_fetch,
    )
    if (
        allow_live_fetch
        and
        proxy_symbol
        and history_fields.get("ytd_return_pct") is None
        and history_fields.get("one_year_return_pct") is None
        and history_fields.get("history_error")
    ):
        retry_history_fields = _history_fields(
            proxy_symbol,
            surface_name=surface_name or "candidate_report",
            allow_live_fetch=True,
        )
        if any(
            retry_history_fields.get(field_name) is not None
            for field_name in ("ytd_return_pct", "one_year_return_pct")
        ):
            history_fields = retry_history_fields
    record_runtime_truth(
        source_id=_SOURCE_ID,
        source_family="quote_latest",
        field_name="benchmark_current_value",
        symbol_or_entity=str(proxy_symbol),
        provider_used=str(market_payload.get("provider_name") or "") or None,
        path_used=str(market_payload.get("retrieval_path") or "fallback_degraded"),
        live_or_cache="cache" if str(market_payload.get("retrieval_path") or "").endswith("cache") else "live",
        usable_truth=market_payload.get("price") is not None,
        freshness=str(market_payload.get("freshness_state") or "unknown"),
        insufficiency_reason=str(market_payload.get("error") or "") or None,
        semantic_grade="price_present" if market_payload.get("price") is not None else "price_unavailable",
        investor_surface=surface_name or "candidate_report",
        attempt_succeeded=market_payload.get("provider_name") is not None,
    )
    return {
        "current_value": market_payload.get("price"),
        "ytd_return_pct": history_fields.get("ytd_return_pct"),
        "one_year_return_pct": history_fields.get("one_year_return_pct"),
        "proxy_price_as_of_utc": market_payload.get("as_of_utc"),
        "proxy_price_error": market_payload.get("error"),
        "history_provider_name": history_fields.get("history_provider_name"),
        "history_error": history_fields.get("history_error"),
        "field_provenance": {
            "current_value": _runtime_field_provenance(
                source_family="quote_latest",
                authority_kind="live_authoritative" if market_payload.get("price") is not None else "unavailable",
                payload=market_payload,
                usable_truth=market_payload.get("price") is not None,
                sufficiency_state="price_present" if market_payload.get("price") is not None else "insufficient",
                data_mode=str(
                    (market_payload.get("provider_execution") or {}).get("data_mode")
                    or ("cache" if str(market_payload.get("retrieval_path") or "").endswith("cache") else "live" if market_payload.get("price") is not None else "unavailable")
                ),
                insufficiency_reason=market_payload.get("error"),
                observed_at=market_payload.get("as_of_utc"),
            ),
            **dict(history_fields.get("field_provenance") or {}),
        },
    }


def fallback() -> dict[str, Any]:
    return {"error": "benchmark_unavailable"}


def fetch(
    benchmark_id: str,
    *,
    surface_name: str | None = None,
    allow_live_fetch: bool = True,
) -> dict[str, Any]:
    normalized = str(benchmark_id or "").strip().upper()
    for row in _benchmark_rows():
        if str(row.get("benchmark_id") or "").upper() != normalized:
            continue
        selected_proxy_symbol = str(row.get("proxy_symbol") or "").strip() or None
        selected_performance_fields: dict[str, Any] | None = None
        last_performance_fields: dict[str, Any] | None = None
        for proxy_symbol in _proxy_candidates_for_benchmark(normalized, row):
            candidate_performance_fields = _performance_fields(
                proxy_symbol,
                surface_name=surface_name,
                allow_live_fetch=allow_live_fetch,
            )
            last_performance_fields = candidate_performance_fields
            if any(
                candidate_performance_fields.get(field_name) is not None
                for field_name in ("current_value", "ytd_return_pct", "one_year_return_pct")
            ):
                selected_proxy_symbol = proxy_symbol
                selected_performance_fields = candidate_performance_fields
                break
        if selected_performance_fields is None:
            selected_performance_fields = last_performance_fields or _performance_fields(
                row.get("proxy_symbol"),
                surface_name=surface_name,
                allow_live_fetch=allow_live_fetch,
            )
        payload = {
            **row,
            "proxy_symbol": selected_proxy_symbol,
            **selected_performance_fields,
        }
        if surface_name:
            record_runtime_truth(
                source_id=_SOURCE_ID,
                source_family="benchmark_truth",
                field_name="benchmark_bundle",
                symbol_or_entity=normalized,
                provider_used=None,
                path_used="benchmark_truth_adapter",
                live_or_cache="mixed",
                usable_truth=payload.get("current_value") is not None,
                freshness="current" if payload.get("current_value") is not None else "unavailable",
                insufficiency_reason=str(payload.get("proxy_price_error") or payload.get("history_error") or "") or None,
                semantic_grade="quote_plus_history" if payload.get("current_value") is not None else "bounded_registry",
                investor_surface=surface_name,
                attempt_succeeded=True,
            )
        return payload
    return {"benchmark_id": normalized, **fallback()}


def fetch_all() -> list[dict[str, Any]]:
    return [fetch(str(row.get("benchmark_id") or "")) for row in _benchmark_rows()]


def warm_history_cache(
    *,
    surface_name: str | None = "candidate_report",
    benchmark_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    requested = {str(item or "").strip().upper() for item in list(benchmark_ids or []) if str(item or "").strip()}
    results: list[dict[str, Any]] = []
    for row in _benchmark_rows():
        benchmark_id = str(row.get("benchmark_id") or "").strip().upper()
        if requested and benchmark_id not in requested:
            continue
        selected_proxy = None
        usable_history = False
        history_error = None
        for proxy_symbol in _proxy_candidates_for_benchmark(benchmark_id, row):
            selected_proxy = proxy_symbol
            for _attempt in range(3):
                history_fields = _history_fields(proxy_symbol, surface_name=surface_name)
                history_error = history_fields.get("history_error")
                if any(
                    history_fields.get(field_name) is not None
                    for field_name in ("ytd_return_pct", "one_year_return_pct")
                ):
                    usable_history = True
                    break
            if usable_history:
                break
        results.append(
            {
                "benchmark_id": benchmark_id,
                "proxy_symbol": selected_proxy,
                "usable_history": usable_history,
                "history_error": history_error,
            }
        )
    return results


def freshness_state() -> FreshnessState:
    return FreshnessState(
        source_id=_SOURCE_ID,
        freshness_class=FreshnessClass.STORED_VALID_CONTEXT,
        last_updated_utc=_now_iso(),
        staleness_seconds=0,
    )


class BenchmarkTruthAdapter:
    source_id = _SOURCE_ID
    tier = source_tier

    def fetch(
        self,
        benchmark_id: str,
        *,
        surface_name: str | None = None,
        allow_live_fetch: bool = True,
    ) -> dict[str, Any]:
        return fetch(benchmark_id, surface_name=surface_name, allow_live_fetch=allow_live_fetch)

    def fetch_all(self) -> list[dict[str, Any]]:
        return fetch_all()

    def warm_history_cache(
        self,
        *,
        surface_name: str | None = "candidate_report",
        benchmark_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        return warm_history_cache(surface_name=surface_name, benchmark_ids=benchmark_ids)

    def freshness_state(self) -> FreshnessState:
        return freshness_state()


register_source(_SOURCE_ID, adapter=__import__(__name__, fromlist=["fetch"]))
