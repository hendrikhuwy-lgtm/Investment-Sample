from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.config import get_db_path
from app.services.provider_adapters import ProviderAdapterError, fetch_provider_data
from app.services.public_upstream_snapshots import ensure_public_upstream_snapshot_tables, put_public_upstream_snapshot
from app.services.provider_registry import DATA_FAMILY_ROUTING, provider_support_status, routed_provider_candidates
from app.v2.donors.source_freshness import FreshnessClass, FreshnessState
from app.v2.sources.execution_envelope import build_provider_execution, payload_execution_profile
from app.v2.sources.freshness import coerce_datetime
from app.v2.sources.freshness_registry import register_source
from app.v2.sources.runtime_truth import record_runtime_truth
from app.v2.core.market_strip_registry import market_strip_spec, market_symbol_family
from app.v2.truth.envelopes import build_market_truth_envelope, classify_market_quote_freshness


source_tier: str = "1A"
_SOURCE_ID = "market_price"
logger = logging.getLogger(__name__)
_ENV_BY_PROVIDER = {
    "finnhub": "FINNHUB_API_KEY",
    "alpha_vantage": "ALPHA_VANTAGE_API_KEY",
    "polygon": "POLYGON_API_KEY",
    "eodhd": "EODHD_API_KEY",
    "tiingo": "TIINGO_API_KEY",
    "twelve_data": "TWELVE_DATA_API_KEY",
    "fmp": "FMP_API_KEY",
}
_SYMBOL_EXCHANGE_HINTS = {
    "ACWI": ("NYSEARCA", "etf"),
    "AGG": ("NYSEARCA", "etf"),
    "GLD": ("NYSEARCA", "etf"),
    "SPY": ("NYSEARCA", "etf"),
    "TLT": ("NASDAQ", "etf"),
    "DXY": ("OTC", "fx_index"),
}


def _now() -> datetime:
    return datetime.now(UTC)


def _trace_enabled() -> bool:
    return os.getenv("IA_TRACE_MARKET_QUOTES", "").strip() == "1"


def _trace(event: str, **fields: Any) -> None:
    if not _trace_enabled():
        return
    logger.info("MARKET_QUOTE_TRACE %s", json.dumps({"event": event, **fields}, sort_keys=True, default=str))


def _normalize_symbol(text: str) -> str:
    return str(text or "").strip().upper()


def _read_only_connection() -> sqlite3.Connection | None:
    path = get_db_path()
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    conn.row_factory = sqlite3.Row
    return conn


def _write_connection() -> sqlite3.Connection | None:
    path = get_db_path()
    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error:
        return None
    conn.row_factory = sqlite3.Row
    return conn


def _configured_quote_providers() -> list[str]:
    configured: list[str] = []
    for provider_name in routed_provider_candidates("quote_latest"):
        env_name = _ENV_BY_PROVIDER.get(provider_name)
        if env_name is None or os.getenv(env_name, "").strip():
            configured.append(provider_name)
    return configured


def _as_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except Exception:
        return None


def _market_identity(
    ticker: str,
    payload: dict[str, Any] | None = None,
) -> tuple[str | None, str | None]:
    raw = dict(payload or {})
    exchange = str(raw.get("exchange") or raw.get("primary_exchange") or "").strip().upper() or None
    asset_class = str(raw.get("asset_class") or raw.get("instrument_type") or "").strip().lower() or None
    if exchange and asset_class:
        return exchange, asset_class
    hint = _SYMBOL_EXCHANGE_HINTS.get(_normalize_symbol(ticker))
    default_asset_class = hint[1] if hint else None
    if default_asset_class is None:
        symbol_family = market_symbol_family(ticker)
        if symbol_family == "dollar_index":
            default_asset_class = "fx_index"
    return exchange or (hint[0] if hint else None), asset_class or default_asset_class


def _derive_change_pct(payload: dict[str, Any]) -> float | None:
    direct = _as_float(
        payload.get("change_pct_1d")
        or payload.get("changePercent")
        or payload.get("dp")
        or payload.get("change_percent")
        or payload.get("percent_change")
    )
    if direct is not None:
        return direct
    current = _as_float(payload.get("price") or payload.get("value") or payload.get("close"))
    previous = _as_float(payload.get("previous_close") or payload.get("previousClose") or payload.get("pc"))
    absolute_change = _as_float(payload.get("absolute_change") or payload.get("change") or payload.get("d"))
    open_value = _as_float(payload.get("open") or payload.get("o"))
    try:
        if current is not None and previous not in {None, 0.0}:
            return ((current - previous) / abs(previous)) * 100.0
        if current is not None and absolute_change is not None and current != absolute_change:
            prior = current - absolute_change
            if prior not in {None, 0.0}:
                return (absolute_change / abs(prior)) * 100.0
        if current is not None and open_value not in {None, 0.0}:
            return ((current - open_value) / abs(open_value)) * 100.0
    except Exception:
        return None
    return None


def _cached_quote_snapshots() -> list[dict[str, Any]]:
    conn = _read_only_connection()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT provider_name, endpoint_family, cache_key, surface_name, payload_json,
                   fetched_at, freshness_state, confidence_tier, source_ref, cache_status,
                   fallback_used, error_state
            FROM provider_cache_snapshots
            WHERE endpoint_family = 'quote_latest'
            ORDER BY fetched_at DESC
            LIMIT 500
            """
        ).fetchall()
    except sqlite3.Error:
        rows = ()
    finally:
        conn.close()
    snapshots: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        payload_json = str(item.get("payload_json") or "{}")
        try:
            payload = json.loads(payload_json)
        except Exception:
            payload = {}
        item["payload"] = payload if isinstance(payload, dict) else {}
        snapshots.append(item)
    return snapshots


def _quote_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    payload = dict(snapshot.get("payload") or {})
    observed_at = payload.get("observed_at") or snapshot.get("fetched_at")
    ticker = _normalize_symbol(payload.get("identifier") or payload.get("ticker") or snapshot.get("cache_key") or "")
    exchange, asset_class = _market_identity(ticker, payload)
    current_price = _as_float(payload.get("price") or payload.get("value") or payload.get("close"))
    previous_close = _as_float(payload.get("previous_close") or payload.get("previousClose") or payload.get("pc"))
    absolute_change = _as_float(payload.get("absolute_change") or payload.get("change") or payload.get("d"))
    change_pct_1d = _derive_change_pct(payload)
    result = {
        "price": current_price,
        "previous_close": previous_close,
        "absolute_change": absolute_change,
        "change_pct_1d": change_pct_1d,
        "open": _as_float(payload.get("open") or payload.get("o")),
        "currency": str(payload.get("currency") or "").strip() or None,
        "as_of_utc": str(observed_at or "").strip() or None,
        "provider_name": snapshot.get("provider_name"),
        "retrieval_path": "direct_cache",
        "freshness_state": snapshot.get("freshness_state"),
        "error_state": snapshot.get("error_state"),
        "exchange": exchange,
        "asset_class": asset_class,
        "truth_envelope": build_market_truth_envelope(
            identifier=ticker,
            as_of_utc=str(observed_at or "").strip() or None,
            provider_name=str(snapshot.get("provider_name") or "").strip() or None,
            acquisition_mode="cached",
            degradation_reason=str(snapshot.get("error_state") or "").strip() or None,
            exchange=exchange,
            asset_class=asset_class,
            retrieved_at_utc=str(snapshot.get("fetched_at") or "").strip() or None,
        ),
        "source_tier": source_tier,
    }
    return _with_provider_execution(result, ticker=ticker)


def _with_provider_execution(payload: dict[str, Any], *, ticker: str, source_family: str = "quote_latest") -> dict[str, Any]:
    enriched = dict(payload)
    path_used = str(enriched.get("retrieval_path") or "direct_live").strip() or "direct_live"
    cache_status = "hit" if path_used == "direct_cache" else None
    freshness_class = str(enriched.get("freshness_state") or "").strip() or (
        "stored_valid_context"
        if path_used == "direct_cache"
        else "degraded_monitoring_mode"
        if "fallback" in path_used
        else "current"
    )
    execution_profile = payload_execution_profile(payload=enriched, source_family=source_family)
    execution = build_provider_execution(
        provider_name=str(enriched.get("provider_name") or "").strip() or None,
        source_family=source_family,
        identifier=_normalize_symbol(ticker),
        provider_symbol=_normalize_symbol(ticker),
        observed_at=str(enriched.get("as_of_utc") or "").strip() or None,
        fetched_at=str((dict(enriched.get("truth_envelope") or {})).get("retrieved_at_utc") or "").strip() or None,
        cache_status=cache_status,
        fallback_used="fallback" in path_used,
        error_state=str(enriched.get("error") or enriched.get("error_state") or "").strip() or None,
        freshness_class=freshness_class,
        path_used=path_used,
        live_or_cache="cache" if path_used == "direct_cache" else "fallback" if "fallback" in path_used else "live",
        usable_truth=bool(execution_profile.get("usable_truth")),
        semantic_grade=str(execution_profile.get("semantic_grade") or "").strip() or None,
        sufficiency_state=str(execution_profile.get("sufficiency_state") or "").strip() or None,
        data_mode=str(execution_profile.get("data_mode") or "").strip() or None,
        authority_level=str(execution_profile.get("authority_level") or "").strip() or None,
        insufficiency_reason=str(enriched.get("error") or enriched.get("error_state") or "").strip() or None,
    )
    enriched["provider_execution"] = execution
    enriched["usable_truth"] = bool(execution.get("usable_truth"))
    enriched["sufficiency_state"] = execution.get("sufficiency_state")
    enriched["data_mode"] = execution.get("data_mode")
    enriched["authority_level"] = execution.get("authority_level")
    return enriched


def _proxy_fallback_fetch(ticker: str) -> dict[str, Any] | None:
    normalized = _normalize_symbol(ticker)
    spec = market_strip_spec(normalized)
    fallback_family = str(spec.get("fallback_family") or "").strip()
    if fallback_family != "usd_strength_fallback":
        return None
    fallback_providers = routed_provider_candidates(fallback_family, identifier=normalized)
    for provider_name in fallback_providers:
        supported, _ = provider_support_status(provider_name, fallback_family, normalized)
        if not supported:
            continue
        try:
            payload = fetch_provider_data(provider_name, fallback_family, normalized)
        except ProviderAdapterError:
            continue
        exchange, asset_class = _market_identity(normalized, {"asset_class": "fx_index", "exchange": "OTC"})
        result = {
            "price": _as_float(payload.get("price") or payload.get("value") or payload.get("close")),
            "previous_close": _as_float(payload.get("previous_close")),
            "absolute_change": _as_float(payload.get("absolute_change")),
            "change_pct_1d": _derive_change_pct(payload),
            "open": _as_float(payload.get("open")),
            "currency": "index",
            "as_of_utc": str(payload.get("observed_at") or "").strip() or None,
            "provider_name": provider_name,
            "retrieval_path": "fallback_derived",
            "exchange": exchange,
            "asset_class": asset_class,
            "truth_envelope": build_market_truth_envelope(
                identifier=normalized,
                as_of_utc=str(payload.get("observed_at") or "").strip() or None,
                provider_name=provider_name,
                acquisition_mode="fallback",
                degradation_reason="usd_strength_proxy",
                exchange=exchange,
                asset_class=asset_class,
                retrieved_at_utc=_now().isoformat(),
            ),
            "source_tier": source_tier,
        }
        return _with_provider_execution(result, ticker=normalized, source_family=fallback_family)
    return None


def _persist_public_support_snapshot(
    *,
    provider_name: str,
    family_name: str,
    ticker: str,
    payload: dict[str, Any],
    surface_name: str | None,
) -> None:
    if provider_name != "frankfurter":
        return
    conn = _write_connection()
    if conn is None:
        return
    observed_at = str(payload.get("as_of_utc") or "").strip() or None
    price = _as_float(payload.get("price"))
    change_pct = _as_float(payload.get("change_pct_1d"))
    components = list(payload.get("proxy_components") or [])
    try:
        ensure_public_upstream_snapshot_tables(conn)
        put_public_upstream_snapshot(
            conn,
            provider_key=provider_name,
            family_name=family_name,
            surface_usage=[surface_name or "daily_brief"],
            payload={
                "provider_key": provider_name,
                "status": "ok",
                "observed_at": observed_at,
                "headline": "Frankfurter USD-strength proxy is supplying bounded DXY continuity.",
                "items": [
                    {
                        "metric": "USD_STRENGTH_PROXY",
                        "label": str(ticker or "").strip().upper(),
                        "value": price,
                        "observed_at": observed_at,
                        "summary": (
                            f"Bounded USD-strength proxy for {str(ticker or '').strip().upper()} "
                            f"is {price:.2f} with {change_pct:+.2f}% one-day change."
                            if price is not None and change_pct is not None
                            else f"Bounded USD-strength proxy for {str(ticker or '').strip().upper()} is available."
                        ),
                        "components": components,
                        "citation": {
                            "source_id": f"frankfurter:{family_name}:{str(ticker or '').strip().upper()}",
                            "url": "https://api.frankfurter.app/latest",
                            "publisher": "Frankfurter",
                            "retrieved_at": _now().isoformat(),
                        },
                    }
                ],
            },
            source_url="https://api.frankfurter.app/latest",
            observed_at=observed_at,
            freshness_state="fresh_full_rebuild",
            error_state=None,
        )
    except sqlite3.Error:
        return
    finally:
        conn.close()


def _freshest_snapshot_for_ticker(ticker: str) -> dict[str, Any] | None:
    normalized = _normalize_symbol(ticker)
    best: dict[str, Any] | None = None
    best_dt: datetime | None = None
    for snapshot in _cached_quote_snapshots():
        payload = dict(snapshot.get("payload") or {})
        identifier = _normalize_symbol(payload.get("identifier"))
        if identifier != normalized:
            continue
        observed_dt = coerce_datetime(payload.get("observed_at") or snapshot.get("fetched_at"))
        if best is None or (observed_dt is not None and (best_dt is None or observed_dt > best_dt)):
            best = snapshot
            best_dt = observed_dt
    return best


def _live_fetch(ticker: str) -> dict[str, Any] | None:
    normalized = _normalize_symbol(ticker)
    routed_providers = routed_provider_candidates("quote_latest", identifier=normalized)
    _trace(
        "market_price.live_fetch.begin",
        ticker=normalized,
        routed_providers=routed_providers,
        configured_providers=_configured_quote_providers(),
    )
    best_partial: dict[str, Any] | None = None
    for provider_name in routed_providers:
        env_name = _ENV_BY_PROVIDER.get(provider_name)
        key_present = bool(env_name and os.getenv(env_name, "").strip())
        provider_configured = env_name is None or key_present
        supported, support_reason = provider_support_status(provider_name, "quote_latest", normalized)
        _trace(
            "market_price.provider.evaluate",
            ticker=normalized,
            provider_name=provider_name,
            enabled=provider_name in routed_providers,
            expected_env=env_name,
            api_key_present=None if env_name is None else key_present,
            requires_api_key=env_name is not None,
            symbol_mapping_succeeded=supported,
            support_reason=support_reason,
            provider_symbol=normalized,
        )
        if not supported:
            _trace(
                "market_price.provider.skipped",
                ticker=normalized,
                provider_name=provider_name,
                reason=support_reason or "provider_symbol_family_unsupported",
                live_request_attempted=False,
            )
            continue
        if not provider_configured:
            _trace(
                "market_price.provider.skipped",
                ticker=normalized,
                provider_name=provider_name,
                reason="provider_configured_but_key_missing",
                live_request_attempted=False,
            )
            continue
        try:
            _trace(
                "market_price.provider.request_attempt",
                ticker=normalized,
                provider_name=provider_name,
                provider_symbol=normalized,
                live_request_attempted=True,
            )
            payload = fetch_provider_data(provider_name, "quote_latest", normalized)
        except ProviderAdapterError as exc:
            _trace(
                "market_price.provider.request_failed",
                ticker=normalized,
                provider_name=provider_name,
                provider_symbol=normalized,
                live_request_attempted=True,
                result="failed",
                error_class=exc.error_class,
                fallback_reason=str(exc),
            )
            continue
        exchange, asset_class = _market_identity(normalized, payload)
        change_pct_1d = _derive_change_pct(payload)
        result = {
            "price": _as_float(payload.get("price") or payload.get("value") or payload.get("close")),
            "previous_close": _as_float(payload.get("previous_close") or payload.get("previousClose") or payload.get("pc")),
            "absolute_change": _as_float(payload.get("absolute_change") or payload.get("change") or payload.get("d")),
            "change_pct_1d": change_pct_1d,
            "open": _as_float(payload.get("open") or payload.get("o")),
            "currency": str(payload.get("currency") or "").strip() or None,
            "as_of_utc": str(payload.get("observed_at") or "").strip() or None,
            "provider_name": provider_name,
            "retrieval_path": "direct_live",
            "exchange": exchange,
            "asset_class": asset_class,
            "truth_envelope": build_market_truth_envelope(
                identifier=normalized,
                as_of_utc=str(payload.get("observed_at") or "").strip() or None,
                provider_name=provider_name,
                acquisition_mode="live",
                exchange=exchange,
                asset_class=asset_class,
                retrieved_at_utc=_now().isoformat(),
            ),
            "source_tier": source_tier,
        }
        result = _with_provider_execution(result, ticker=normalized)
        _trace(
            "market_price.provider.request_succeeded",
            ticker=normalized,
            provider_name=provider_name,
            provider_symbol=normalized,
            live_request_attempted=True,
            result="succeeded" if result.get("usable_truth") and result.get("sufficiency_state") == "movement_capable" else "payload_unusable",
            price_present=result.get("price") is not None,
            change_pct_1d_present=result.get("change_pct_1d") is not None,
            fallback_reason=None if result.get("usable_truth") and result.get("sufficiency_state") == "movement_capable" else "provider_responded_but_payload_unusable",
        )
        if result.get("usable_truth") and result.get("sufficiency_state") == "movement_capable":
            return result
        if result.get("price") is not None and best_partial is None:
            best_partial = result
    _trace(
        "market_price.live_fetch.exhausted",
        ticker=normalized,
        result="partial_live_provider_succeeded" if best_partial is not None else "no_live_provider_succeeded",
    )
    return best_partial


def fallback(ticker: str) -> dict[str, Any]:
    normalized = _normalize_symbol(ticker)
    exchange, asset_class = _market_identity(normalized)
    return _with_provider_execution({
        "ticker": normalized,
        "price": None,
        "change_pct_1d": None,
        "error": "price_unavailable",
        "retrieval_path": "fallback_degraded",
        "exchange": exchange,
        "asset_class": asset_class,
        "truth_envelope": build_market_truth_envelope(
            identifier=normalized,
            as_of_utc=None,
            provider_name="market_price",
            acquisition_mode="fallback",
            degradation_reason="price_unavailable",
            exchange=exchange,
            asset_class=asset_class,
            retrieved_at_utc=_now().isoformat(),
        ),
    }, ticker=normalized)


def fetch(ticker: str, *, surface_name: str | None = None) -> dict[str, Any]:
    normalized = _normalize_symbol(ticker)
    _trace("market_price.fetch.begin", ticker=normalized)
    live_payload = _live_fetch(normalized)
    if live_payload is not None and live_payload.get("price") is not None:
        result = {"ticker": normalized, **live_payload}
        execution = dict(live_payload.get("provider_execution") or {})
        record_runtime_truth(
            source_id=_SOURCE_ID,
            source_family="quote_latest",
            field_name="price",
            symbol_or_entity=normalized,
            provider_used=str(live_payload.get("provider_name") or "") or None,
            path_used=str(live_payload.get("retrieval_path") or "direct_live"),
            live_or_cache="live",
            usable_truth=bool(execution.get("usable_truth")),
            freshness=str(execution.get("freshness_class") or live_payload.get("freshness_state") or "current"),
            insufficiency_reason=str(execution.get("insufficiency_reason") or "") or None,
            semantic_grade=str(execution.get("semantic_grade") or "price_only"),
            provenance_strength=str(execution.get("provenance_strength") or "") or None,
            investor_surface=surface_name,
            attempt_succeeded=True,
        )
        _trace(
            "market_price.fetch.selected_live",
            ticker=normalized,
            provider_name=live_payload.get("provider_name"),
            retrieval_path=live_payload.get("retrieval_path"),
            fallback_reason=None if live_payload.get("change_pct_1d") is not None else "movement_semantics_missing",
        )
        return result
    if live_payload is not None:
        execution = dict(live_payload.get("provider_execution") or {})
        record_runtime_truth(
            source_id=_SOURCE_ID,
            source_family="quote_latest",
            field_name="price",
            symbol_or_entity=normalized,
            provider_used=str(live_payload.get("provider_name") or "") or None,
            path_used=str(live_payload.get("retrieval_path") or "direct_live"),
            live_or_cache="live",
            usable_truth=bool(execution.get("usable_truth")),
            freshness=str(execution.get("freshness_class") or live_payload.get("freshness_state") or "unknown"),
            insufficiency_reason=str(execution.get("insufficiency_reason") or "provider_responded_but_payload_unusable"),
            semantic_grade=str(execution.get("semantic_grade") or ("price_only" if live_payload.get("price") is not None else "unusable")),
            provenance_strength=str(execution.get("provenance_strength") or "") or None,
            investor_surface=surface_name,
            attempt_succeeded=True,
        )
        _trace(
            "market_price.fetch.live_unusable",
            ticker=normalized,
            provider_name=live_payload.get("provider_name"),
            fallback_reason="provider_responded_but_payload_unusable",
        )

    snapshot = _freshest_snapshot_for_ticker(normalized)
    cached_unusable: dict[str, Any] | None = None
    if snapshot is not None:
        quoted = _quote_from_snapshot(snapshot)
        if quoted.get("price") is not None:
            execution = dict(quoted.get("provider_execution") or {})
            record_runtime_truth(
                source_id=_SOURCE_ID,
                source_family="quote_latest",
                field_name="price",
                symbol_or_entity=normalized,
                provider_used=str(quoted.get("provider_name") or "") or None,
                path_used=str(quoted.get("retrieval_path") or "direct_cache"),
                live_or_cache="cache",
                usable_truth=bool(execution.get("usable_truth")),
                freshness=str(execution.get("freshness_class") or quoted.get("freshness_state") or "unknown"),
                insufficiency_reason=str(execution.get("insufficiency_reason") or "") or None,
                semantic_grade=str(execution.get("semantic_grade") or "price_only"),
                provenance_strength=str(execution.get("provenance_strength") or "") or None,
                investor_surface=surface_name,
                attempt_succeeded=True,
            )
            _trace(
                "market_price.fetch.selected_cache",
                ticker=normalized,
                provider_name=quoted.get("provider_name"),
                cache_status=snapshot.get("cache_status"),
                cached_observed_at=quoted.get("as_of_utc"),
                price_present=True,
                change_pct_1d_present=quoted.get("change_pct_1d") is not None,
                fallback_reason=None,
            )
            return {"ticker": normalized, **quoted}

        cached_unusable = quoted
        execution = dict(quoted.get("provider_execution") or {})
        record_runtime_truth(
            source_id=_SOURCE_ID,
            source_family="quote_latest",
            field_name="price",
            symbol_or_entity=normalized,
            provider_used=str(quoted.get("provider_name") or "") or None,
            path_used=str(quoted.get("retrieval_path") or "direct_cache"),
            live_or_cache="cache",
            usable_truth=False,
            freshness=str(execution.get("freshness_class") or quoted.get("freshness_state") or "unknown"),
            insufficiency_reason=str(execution.get("insufficiency_reason") or "cache_payload_unusable"),
            semantic_grade="unusable",
            provenance_strength=str(execution.get("provenance_strength") or "") or None,
            investor_surface=surface_name,
            attempt_succeeded=False,
        )
        _trace(
            "market_price.fetch.cache_unusable",
            ticker=normalized,
            provider_name=quoted.get("provider_name"),
            cache_status=snapshot.get("cache_status"),
            cached_observed_at=quoted.get("as_of_utc"),
            fallback_reason="cache_payload_unusable",
        )

    proxy_payload = _proxy_fallback_fetch(normalized)
    if proxy_payload is not None:
        _persist_public_support_snapshot(
            provider_name=str(proxy_payload.get("provider_name") or "").strip(),
            family_name="usd_strength_fallback",
            ticker=normalized,
            payload=proxy_payload,
            surface_name=surface_name,
        )
        execution = dict(proxy_payload.get("provider_execution") or {})
        record_runtime_truth(
            source_id=_SOURCE_ID,
            source_family="usd_strength_fallback",
            field_name="price",
            symbol_or_entity=normalized,
            provider_used=str(proxy_payload.get("provider_name") or "") or None,
            path_used=str(proxy_payload.get("retrieval_path") or "fallback_derived"),
            live_or_cache="fallback",
            usable_truth=bool(execution.get("usable_truth")),
            freshness=str(execution.get("freshness_class") or "current"),
            insufficiency_reason=str(execution.get("insufficiency_reason") or "") or None,
            semantic_grade=str(execution.get("semantic_grade") or "derived_proxy"),
            provenance_strength=str(execution.get("provenance_strength") or "") or None,
            investor_surface=surface_name,
            attempt_succeeded=True,
        )
        _trace(
            "market_price.fetch.proxy_fallback",
            ticker=normalized,
            provider_name=proxy_payload.get("provider_name"),
            fallback_reason="usd_strength_proxy",
        )
        return {"ticker": normalized, **proxy_payload}

    if cached_unusable is not None:
        return {"ticker": normalized, **cached_unusable}

    _trace(
        "market_price.fetch.fallback",
        ticker=normalized,
        fallback_reason="no_live_provider_and_no_cached_snapshot",
    )
    record_runtime_truth(
        source_id=_SOURCE_ID,
        source_family="quote_latest",
        field_name="price",
        symbol_or_entity=normalized,
        provider_used=None,
        path_used="fallback_degraded",
        live_or_cache="fallback",
        usable_truth=False,
        freshness="unavailable",
        insufficiency_reason="no_live_provider_and_no_cached_snapshot",
        semantic_grade="unavailable",
        investor_surface=surface_name,
        attempt_succeeded=False,
    )
    return fallback(normalized)


def fetch_batch(tickers: list[str], *, surface_name: str | None = None) -> dict[str, dict]:
    return {_normalize_symbol(ticker): fetch(ticker, surface_name=surface_name) for ticker in tickers}


def freshness_state() -> FreshnessState:
    latest_dt: datetime | None = None
    latest_exchange: str | None = None
    latest_asset_class: str | None = None
    latest_identifier: str | None = None
    for snapshot in _cached_quote_snapshots():
        payload = dict(snapshot.get("payload") or {})
        observed_dt = coerce_datetime(payload.get("observed_at") or snapshot.get("fetched_at"))
        if observed_dt is None:
            continue
        if latest_dt is None or observed_dt > latest_dt:
            latest_dt = observed_dt
            latest_identifier = _normalize_symbol(payload.get("identifier") or payload.get("ticker") or snapshot.get("cache_key") or "")
            latest_exchange, latest_asset_class = _market_identity(latest_identifier, payload)

    if latest_dt is None and _configured_quote_providers():
        return FreshnessState(
            source_id=_SOURCE_ID,
            freshness_class=FreshnessClass.FRESH_PARTIAL_REBUILD,
            last_updated_utc=None,
            staleness_seconds=None,
        )
    if latest_dt is None:
        return FreshnessState(
            source_id=_SOURCE_ID,
            freshness_class=FreshnessClass.EXECUTION_FAILED_OR_INCOMPLETE,
            last_updated_utc=None,
            staleness_seconds=None,
        )

    age_seconds = max(0, int((_now() - latest_dt).total_seconds()))
    freshness_class = classify_market_quote_freshness(
        as_of=latest_dt,
        exchange=latest_exchange,
        asset_class=latest_asset_class,
        identifier=latest_identifier,
    )
    return FreshnessState(
        source_id=_SOURCE_ID,
        freshness_class=freshness_class,
        last_updated_utc=latest_dt.astimezone(UTC).isoformat(),
        staleness_seconds=age_seconds,
    )


class MarketPriceAdapter:
    source_id = _SOURCE_ID
    tier = source_tier

    def fetch(self, ticker: str, *, surface_name: str | None = None) -> dict[str, Any]:
        return fetch(ticker, surface_name=surface_name)

    def fetch_batch(self, tickers: list[str], *, surface_name: str | None = None) -> dict[str, dict]:
        return fetch_batch(tickers, surface_name=surface_name)

    def freshness_state(self) -> FreshnessState:
        return freshness_state()


register_source(_SOURCE_ID, adapter=__import__(__name__, fromlist=["fetch"]))
