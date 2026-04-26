from __future__ import annotations

from datetime import UTC, datetime
import sqlite3
from typing import Any

import pandas as pd

from app.v2.blueprint_market.exchange_calendar_service import session_dates_between
from app.services.blueprint_candidate_registry import (
    ensure_candidate_registry_tables,
    export_live_candidate_registry,
    seed_default_candidate_registry,
)
from app.services.provider_adapters import ProviderAdapterError, fetch_provider_data
from app.services.provider_family_success import record_provider_family_event, recompute_provider_family_success
from app.services.provider_registry import canonical_blueprint_family_id
from app.services.symbol_resolution import (
    record_resolution_failure,
    record_resolution_success,
    resolve_provider_identifiers,
)
from app.v2.blueprint_market.candidate_price_series_normalizer import normalize_twelvedata_series
from app.v2.blueprint_market.market_identity import candidate_id_for_symbol, ensure_candidate_market_identities
from app.v2.blueprint_market.series_store import (
    ensure_blueprint_market_tables,
    finish_series_run,
    list_market_identities,
    load_reusable_price_series,
    load_price_series,
    record_series_run_start,
    upsert_price_series_rows,
)
from app.v2.blueprint_market.twelvedata_price_client import TwelveDataPriceClient


_MAX_STALE_DAYS = 5
_MIN_HISTORY_BARS = 260


def _normalize_series_failure_class(error_class: str | None, message: str | None = None) -> str:
    normalized = str(error_class or "").strip()
    lowered = str(message or "").strip().lower()
    if normalized == "budget_block":
        if "402" in lowered or "payment required" in lowered or "plan" in lowered:
            return "provider_plan_limit"
        if "429" in lowered or "rate" in lowered:
            return "provider_rate_limit"
        return "provider_budget_block"
    mapping = {
        "rate_limited": "provider_rate_limit",
        "no_data_for_symbol": "unsupported_symbol",
        "provider_identifier_kind_unsupported": "unsupported_symbol",
        "provider_region_unsupported": "no_eligible_route",
        "provider_symbol_family_unsupported": "no_eligible_route",
        "empty_response": "empty_payload",
        "invalid_payload": "invalid_payload",
        "timeout": "remote_timeout",
        "remote_timeout": "remote_timeout",
        "manual_quarantine": "manual_quarantine",
    }
    if normalized in mapping:
        return mapping[normalized]
    return normalized or "no_usable_history"


def _record_blueprint_family_attempt(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    provider_name: str,
    success: bool,
    freshness_state: str,
    error_class: str | None,
    fallback_used: bool,
    cache_hit: bool = False,
    age_seconds: float | None = None,
) -> None:
    record_provider_family_event(
        conn,
        provider_name=provider_name,
        surface_name="blueprint",
        family_name=canonical_blueprint_family_id("ohlcv_history"),
        identifier=str(symbol or "").strip().upper(),
        target_universe=[str(symbol or "").strip().upper()],
        success=success,
        error_class=error_class,
        cache_hit=cache_hit,
        freshness_state=freshness_state,
        fallback_used=fallback_used,
        age_seconds=age_seconds,
        root_error_class=error_class,
        effective_error_class=error_class,
        triggered_by_job="candidate_series_refresh",
    )


def _attempt_details(
    *,
    identity: dict[str, Any],
    provider_name: str,
    provider_symbol: str,
    terminal_status: str,
    terminal_cause: str | None,
    freshness_state: str,
    rescue_used: bool,
    rescue_reason: str | None = None,
    bars_written: int = 0,
) -> dict[str, Any]:
    return {
        "attempted_at": _now_utc().isoformat(),
        "symbol": str(identity.get("symbol") or "").strip().upper(),
        "provider": provider_name,
        "provider_symbol": provider_symbol,
        "surface_family_id": canonical_blueprint_family_id("ohlcv_history"),
        "canonical_family_id": canonical_blueprint_family_id("ohlcv_history"),
        "route_stability_state": "stable",
        "current_classification": freshness_state,
        "terminal_status": terminal_status,
        "terminal_cause": terminal_cause,
        "direct_bars_written": int(bars_written or 0),
        "rescue_used": bool(rescue_used),
        "rescue_reason": rescue_reason,
    }


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _safe_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    return parsed


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_history_timestamp(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if "T" not in text:
        text = f"{text}T00:00:00+00:00"
    parsed = _safe_datetime(text)
    return parsed.isoformat() if parsed is not None else None


def _series_quality_summary(
    *,
    rows: list[dict[str, Any]],
    series_role: str,
    adjustment_mode: str,
    exchange_mic: str | None = None,
    provider_symbol: str | None = None,
    asset_class: str | None = None,
) -> dict[str, Any]:
    if not rows:
        return {
            "bars_expected": 0,
            "bars_present": 0,
            "missing_bar_ratio": 1.0,
            "stale_days": 999,
            "has_corporate_action_uncertainty": adjustment_mode != "adjusted",
            "uses_proxy_series": series_role != "direct",
            "quality_label": "broken",
        }
    timestamps = sorted(
        item["timestamp_utc"][:10]
        for item in rows
        if str(item.get("timestamp_utc") or "").strip()
    )
    bars_present = len(timestamps)
    expected_sessions = session_dates_between(
        start_timestamp=timestamps[0],
        end_timestamp=timestamps[-1],
        identity={
            "exchange_mic": exchange_mic,
            "provider_symbol": provider_symbol,
            "symbol": provider_symbol,
            "provider_asset_class": asset_class,
        },
    )
    if expected_sessions is None:
        expected_sessions = [item.strftime("%Y-%m-%d") for item in pd.bdate_range(start=timestamps[0], end=timestamps[-1])]
    bars_expected = max(len(expected_sessions), bars_present)
    missing_bar_ratio = max(0.0, float(bars_expected - bars_present) / float(bars_expected or 1))
    latest_dt = _safe_datetime(rows[-1].get("timestamp_utc"))
    stale_days = 999
    if latest_dt is not None:
        stale_days = max(0, int((_now_utc().date() - latest_dt.date()).days))
    if bars_present < _MIN_HISTORY_BARS:
        quality_label = "thin"
    elif stale_days > _MAX_STALE_DAYS or missing_bar_ratio > 0.08:
        quality_label = "degraded"
    elif missing_bar_ratio > 0.02:
        quality_label = "watch"
    else:
        quality_label = "good"
    return {
        "bars_expected": bars_expected,
        "bars_present": bars_present,
        "missing_bar_ratio": round(missing_bar_ratio, 4),
        "stale_days": stale_days,
        "has_corporate_action_uncertainty": adjustment_mode != "adjusted",
        "uses_proxy_series": series_role != "direct",
        "quality_label": quality_label,
    }


def _needs_secondary_history_recovery(summary: dict[str, Any]) -> bool:
    if not summary:
        return True
    if int(summary.get("bars_present") or 0) < _MIN_HISTORY_BARS:
        return True
    return str(summary.get("quality_label") or "") in {"broken", "degraded"}


def _normalize_secondary_history_rows(
    *,
    payload: dict[str, Any],
    provider_name: str,
    provider_symbol: str,
    identity: dict[str, Any],
    ingest_run_id: str,
    series_quality_summary: dict[str, Any],
    gap_flags: list[str] | None = None,
) -> list[dict[str, Any]]:
    rows = list(payload.get("series") or [])
    normalized: list[dict[str, Any]] = []
    extra_flags = ["secondary_provider_recovery", f"secondary_provider:{provider_name}", *(gap_flags or [])]
    for item in rows:
        timestamp = _normalize_history_timestamp(
            item.get("timestamp_utc")
            or item.get("date")
            or item.get("timestamp")
            or item.get("time")
        )
        open_value = _safe_float(item.get("open") or item.get("o"))
        high_value = _safe_float(item.get("high") or item.get("h"))
        low_value = _safe_float(item.get("low") or item.get("l"))
        close_value = _safe_float(item.get("close") or item.get("c") or item.get("value"))
        if timestamp is None or None in {open_value, high_value, low_value, close_value}:
            continue
        normalized.append(
            {
                "candidate_id": str(identity["candidate_id"]),
                "instrument_id": str(identity["instrument_id"]),
                "series_role": str(identity["series_role"]),
                "timestamp_utc": timestamp,
                "interval": str(identity.get("primary_interval") or "1day"),
                "open": float(open_value),
                "high": float(high_value),
                "low": float(low_value),
                "close": float(close_value),
                "volume": _safe_float(item.get("volume") or item.get("v")),
                "amount": _safe_float(item.get("amount") or item.get("vw")),
                "provider": provider_name,
                "provider_symbol": provider_symbol,
                "adjusted_flag": str(identity.get("adjustment_mode") or "adjusted") == "adjusted",
                "freshness_ts": timestamp,
                "quality_flags": sorted(set(extra_flags)),
                "series_quality_summary": series_quality_summary,
                "ingest_run_id": ingest_run_id,
            }
        )
    normalized.sort(key=lambda row: str(row.get("timestamp_utc") or ""))
    return normalized


def _yahoo_secondary_candidates(identity: dict[str, Any], canonical_symbol: str) -> list[str]:
    base_candidates = [
        str(identity.get("provider_symbol") or "").strip().upper(),
        canonical_symbol,
    ]
    exchange_hint = str(identity.get("exchange_mic") or "").strip().upper()
    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in base_candidates:
        if not candidate:
            continue
        variants = [candidate]
        if "." not in candidate:
            if any(token in exchange_hint for token in ("SES", "XSES", "SGX")):
                variants.append(f"{candidate}.SI")
            if any(token in exchange_hint for token in ("XLON", "LSE")):
                variants.append(f"{candidate}.L")
        for variant in variants:
            if not variant or variant in seen:
                continue
            seen.add(variant)
            ordered.append(variant)
    return ordered


def _fetch_yahoo_chart_history(provider_symbol: str) -> dict[str, Any]:
    import requests

    response = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{provider_symbol}",
        params={"interval": "1d", "range": "10y", "includeAdjustedClose": "true"},
        timeout=20,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    response.raise_for_status()
    payload = response.json()
    result = ((payload.get("chart") or {}).get("result") or [None])[0] or {}
    timestamps = list(result.get("timestamp") or [])
    quote = dict((((result.get("indicators") or {}).get("quote") or [{}])[0]) or {})
    series: list[dict[str, Any]] = []
    opens = list(quote.get("open") or [])
    highs = list(quote.get("high") or [])
    lows = list(quote.get("low") or [])
    closes = list(quote.get("close") or [])
    volumes = list(quote.get("volume") or [])
    for idx, ts in enumerate(timestamps):
        try:
            open_value = _safe_float(opens[idx] if idx < len(opens) else None)
            high_value = _safe_float(highs[idx] if idx < len(highs) else None)
            low_value = _safe_float(lows[idx] if idx < len(lows) else None)
            close_value = _safe_float(closes[idx] if idx < len(closes) else None)
        except Exception:
            continue
        if None in {open_value, high_value, low_value, close_value}:
            continue
        timestamp = datetime.fromtimestamp(int(ts), UTC).isoformat()
        series.append(
            {
                "timestamp_utc": timestamp,
                "open": open_value,
                "high": high_value,
                "low": low_value,
                "close": close_value,
                "volume": _safe_float(volumes[idx] if idx < len(volumes) else None),
            }
        )
    return {"series": series}


def _try_bounded_secondary_history_recovery(
    conn: sqlite3.Connection,
    *,
    identity: dict[str, Any],
    ingest_run_id: str,
) -> dict[str, Any] | None:
    canonical_symbol = str(identity.get("symbol") or "").strip().upper()
    if not canonical_symbol:
        return None
    for provider_name in ("polygon", "fmp"):
        resolution = resolve_provider_identifiers(
            conn,
            provider_name=provider_name,
            endpoint_family="ohlcv_history",
            identifier=canonical_symbol,
            asset_type=str(identity.get("provider_asset_class") or "").strip() or None,
            region=str(identity.get("quote_currency") or "").strip() or None,
        )
        candidates = [
            str(resolution.get("provider_symbol") or canonical_symbol).strip().upper(),
            *[str(item).strip().upper() for item in list(resolution.get("fallback_aliases") or []) if str(item).strip()],
        ]
        seen: set[str] = set()
        ordered_candidates: list[str] = []
        for candidate in candidates:
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            ordered_candidates.append(candidate)
        for provider_symbol in ordered_candidates:
            try:
                payload = fetch_provider_data(provider_name, "ohlcv_history", provider_symbol)
            except ProviderAdapterError as exc:
                record_resolution_failure(
                    conn,
                    canonical_symbol=canonical_symbol,
                    provider_name=provider_name,
                    endpoint_family="ohlcv_history",
                    provider_symbol=provider_symbol,
                    error_class=str(exc.error_class or "provider_error"),
                )
                continue
            preliminary = _normalize_secondary_history_rows(
                payload=payload,
                provider_name=provider_name,
                provider_symbol=provider_symbol,
                identity=identity,
                ingest_run_id=ingest_run_id,
                series_quality_summary={},
                gap_flags=[],
            )
            summary = _series_quality_summary(
                rows=preliminary,
                series_role=str(identity.get("series_role") or "direct"),
                adjustment_mode=str(identity.get("adjustment_mode") or "adjusted"),
                exchange_mic=str(identity.get("exchange_mic") or "").strip() or None,
                provider_symbol=str(identity.get("provider_symbol") or identity.get("symbol") or "").strip() or None,
                asset_class=str(identity.get("provider_asset_class") or "").strip() or None,
            )
            if _needs_secondary_history_recovery(summary):
                record_resolution_failure(
                    conn,
                    canonical_symbol=canonical_symbol,
                    provider_name=provider_name,
                    endpoint_family="ohlcv_history",
                    provider_symbol=provider_symbol,
                    error_class="insufficient_history",
                )
                continue
            gap_flags = detect_candidate_series_gaps(
                preliminary,
                exchange_mic=str(identity.get("exchange_mic") or "").strip() or None,
                provider_symbol=str(identity.get("provider_symbol") or identity.get("symbol") or "").strip() or None,
                asset_class=str(identity.get("provider_asset_class") or "").strip() or None,
            )
            normalized = _normalize_secondary_history_rows(
                payload=payload,
                provider_name=provider_name,
                provider_symbol=provider_symbol,
                identity=identity,
                ingest_run_id=ingest_run_id,
                series_quality_summary=summary,
                gap_flags=gap_flags,
            )
            if not normalized:
                continue
            upsert_price_series_rows(conn, normalized)
            finish_series_run(
                conn,
                ingest_run_id=ingest_run_id,
                status="succeeded",
                bars_written=len(normalized),
                failure_class=None,
                details={
                    "series_quality_summary": summary,
                    "gap_flags": gap_flags,
                    "secondary_provider_recovery": True,
                    "history_provider_name": provider_name,
                    "history_provider_symbol": provider_symbol,
                    "runtime_provider_name": "twelve_data",
                    **_attempt_details(
                        identity=identity,
                        provider_name=provider_name,
                        provider_symbol=provider_symbol,
                        terminal_status="current_success",
                        terminal_cause=None,
                        freshness_state="current",
                        rescue_used=True,
                        rescue_reason="primary_history_weak_or_failed",
                        bars_written=len(normalized),
                    ),
                },
            )
            _record_blueprint_family_attempt(
                conn,
                symbol=canonical_symbol,
                provider_name=provider_name,
                success=True,
                freshness_state="current",
                error_class=None,
                fallback_used=True,
                age_seconds=0.0,
            )
            record_resolution_success(
                conn,
                canonical_symbol=canonical_symbol,
                provider_name=provider_name,
                endpoint_family="ohlcv_history",
                provider_symbol=provider_symbol,
                fallback_aliases=[item for item in ordered_candidates if item != provider_symbol],
                resolution_confidence=max(0.78, float(resolution.get("resolution_confidence") or 0.78)),
                resolution_reason="secondary_history_verified",
            )
            return {
                "series_role": identity["series_role"],
                "provider_symbol": provider_symbol,
                "status": "recovered_secondary",
                "bars_written": len(normalized),
                "series_quality_summary": summary,
                "gap_flags": gap_flags,
                "ingest_run_id": ingest_run_id,
                "history_provider_name": provider_name,
                "runtime_provider_name": "twelve_data",
            }
    yahoo_candidates = _yahoo_secondary_candidates(identity, canonical_symbol)
    for provider_symbol in yahoo_candidates:
        try:
            payload = _fetch_yahoo_chart_history(provider_symbol)
        except Exception:
            record_resolution_failure(
                conn,
                canonical_symbol=canonical_symbol,
                provider_name="yahoo_chart",
                endpoint_family="ohlcv_history",
                provider_symbol=provider_symbol,
                error_class="remote_error",
            )
            continue
        preliminary = _normalize_secondary_history_rows(
            payload=payload,
            provider_name="yahoo_chart",
            provider_symbol=provider_symbol,
            identity=identity,
            ingest_run_id=ingest_run_id,
            series_quality_summary={},
            gap_flags=[],
        )
        summary = _series_quality_summary(
            rows=preliminary,
            series_role=str(identity.get("series_role") or "direct"),
            adjustment_mode=str(identity.get("adjustment_mode") or "adjusted"),
            exchange_mic=str(identity.get("exchange_mic") or "").strip() or None,
            provider_symbol=str(identity.get("provider_symbol") or identity.get("symbol") or "").strip() or None,
            asset_class=str(identity.get("provider_asset_class") or "").strip() or None,
        )
        if _needs_secondary_history_recovery(summary):
            record_resolution_failure(
                conn,
                canonical_symbol=canonical_symbol,
                provider_name="yahoo_chart",
                endpoint_family="ohlcv_history",
                provider_symbol=provider_symbol,
                error_class="empty_payload" if not preliminary else "insufficient_history",
            )
            continue
        gap_flags = detect_candidate_series_gaps(
            preliminary,
            exchange_mic=str(identity.get("exchange_mic") or "").strip() or None,
            provider_symbol=str(identity.get("provider_symbol") or identity.get("symbol") or "").strip() or None,
            asset_class=str(identity.get("provider_asset_class") or "").strip() or None,
        )
        normalized = _normalize_secondary_history_rows(
            payload=payload,
            provider_name="yahoo_chart",
            provider_symbol=provider_symbol,
            identity=identity,
            ingest_run_id=ingest_run_id,
            series_quality_summary=summary,
            gap_flags=gap_flags,
        )
        if not normalized:
            continue
        upsert_price_series_rows(conn, normalized)
        finish_series_run(
            conn,
            ingest_run_id=ingest_run_id,
            status="succeeded",
            bars_written=len(normalized),
            failure_class=None,
            details={
                "series_quality_summary": summary,
                "gap_flags": gap_flags,
                "secondary_provider_recovery": True,
                "history_provider_name": "yahoo_chart",
                "history_provider_symbol": provider_symbol,
                "runtime_provider_name": "twelve_data",
                **_attempt_details(
                    identity=identity,
                    provider_name="yahoo_chart",
                    provider_symbol=provider_symbol,
                    terminal_status="current_success",
                    terminal_cause=None,
                    freshness_state="current",
                    rescue_used=True,
                    rescue_reason="sgx_yahoo_chart_recovery",
                    bars_written=len(normalized),
                ),
            },
        )
        _record_blueprint_family_attempt(
            conn,
            symbol=canonical_symbol,
            provider_name="yahoo_chart",
            success=True,
            freshness_state="current",
            error_class=None,
            fallback_used=True,
            age_seconds=0.0,
        )
        record_resolution_success(
            conn,
            canonical_symbol=canonical_symbol,
            provider_name="yahoo_chart",
            endpoint_family="ohlcv_history",
            provider_symbol=provider_symbol,
            fallback_aliases=[item for item in yahoo_candidates if item != provider_symbol],
            resolution_confidence=0.82,
            resolution_reason="secondary_history_verified",
        )
        return {
            "series_role": identity["series_role"],
            "provider_symbol": provider_symbol,
            "status": "recovered_secondary",
            "bars_written": len(normalized),
            "series_quality_summary": summary,
            "gap_flags": gap_flags,
            "ingest_run_id": ingest_run_id,
            "history_provider_name": "yahoo_chart",
            "runtime_provider_name": "twelve_data",
        }
    return None


def detect_candidate_series_gaps(
    rows: list[dict[str, Any]],
    *,
    exchange_mic: str | None = None,
    provider_symbol: str | None = None,
    asset_class: str | None = None,
) -> list[str]:
    if not rows:
        return ["series_empty"]
    timestamps = sorted(
        item["timestamp_utc"][:10]
        for item in rows
        if str(item.get("timestamp_utc") or "").strip()
    )
    expected = session_dates_between(
        start_timestamp=timestamps[0],
        end_timestamp=timestamps[-1],
        identity={
            "exchange_mic": exchange_mic,
            "provider_symbol": provider_symbol,
            "symbol": provider_symbol,
            "provider_asset_class": asset_class,
        },
    )
    if expected is None:
        expected = [item.strftime("%Y-%m-%d") for item in pd.bdate_range(start=timestamps[0], end=timestamps[-1])]
    actual = set(timestamps)
    missing = [value for value in expected if value not in actual]
    if not missing:
        return []
    preview = missing[:5]
    return [f"gap:{value}" for value in preview]


def check_candidate_series_freshness(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    series_role: str = "direct",
) -> dict[str, Any]:
    rows = load_price_series(conn, candidate_id=candidate_id, series_role=series_role, ascending=True)
    adjustment_mode = "adjusted"
    exchange_mic = None
    provider_symbol = None
    asset_class = None
    identities = list_market_identities(conn, candidate_id)
    for item in identities:
        if str(item.get("series_role") or "") == series_role:
            adjustment_mode = str(item.get("adjustment_mode") or "adjusted")
            exchange_mic = str(item.get("exchange_mic") or "").strip() or None
            provider_symbol = str(item.get("provider_symbol") or item.get("symbol") or "").strip() or None
            asset_class = str(item.get("provider_asset_class") or "").strip() or None
            break
    summary = _series_quality_summary(
        rows=rows,
        series_role=series_role,
        adjustment_mode=adjustment_mode,
        exchange_mic=exchange_mic,
        provider_symbol=provider_symbol,
        asset_class=asset_class,
    )
    gap_flags = detect_candidate_series_gaps(
        rows,
        exchange_mic=exchange_mic,
        provider_symbol=provider_symbol,
        asset_class=asset_class,
    )
    return {
        "candidate_id": candidate_id,
        "series_role": series_role,
        "series_quality_summary": summary,
        "gap_flags": gap_flags,
    }


def _candidate_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_candidate_registry_tables(conn)
    rows = export_live_candidate_registry(conn)
    if not rows:
        seed_default_candidate_registry(conn)
        rows = export_live_candidate_registry(conn)
    return rows


def list_blueprint_market_candidates(
    conn: sqlite3.Connection,
    *,
    candidate_id: str | None = None,
    sleeve_key: str | None = None,
) -> list[dict[str, Any]]:
    rows = _candidate_rows(conn)
    selected = rows
    if candidate_id:
        normalized = str(candidate_id).strip().upper().replace("CANDIDATE_INSTRUMENT_", "")
        selected = [row for row in selected if str(row.get("symbol") or "").strip().upper() == normalized]
    if sleeve_key:
        selected = [row for row in selected if str(row.get("sleeve_key") or "").strip() == str(sleeve_key).strip()]
    return selected


def backfill_candidate_series(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    force_refresh: bool = False,
) -> dict[str, Any]:
    ensure_blueprint_market_tables(conn)
    identities = ensure_candidate_market_identities(conn, candidate_id)
    if not identities:
        return {
            "candidate_id": candidate_id,
            "status": "failed",
            "failure_class": "no_eligible_route",
            "series_roles": [],
        }
    client = TwelveDataPriceClient()
    results: list[dict[str, Any]] = []
    overall_failure: str | None = None
    for identity in identities:
        existing = load_price_series(
            conn,
            candidate_id=str(identity["candidate_id"]),
            series_role=str(identity["series_role"]),
            interval=str(identity.get("primary_interval") or "1day"),
            ascending=True,
        )
        if existing and not force_refresh:
            freshness = _series_quality_summary(
                rows=existing,
                series_role=str(identity["series_role"]),
                adjustment_mode=str(identity.get("adjustment_mode") or "adjusted"),
                exchange_mic=str(identity.get("exchange_mic") or "").strip() or None,
                provider_symbol=str(identity.get("provider_symbol") or identity.get("symbol") or "").strip() or None,
                asset_class=str(identity.get("provider_asset_class") or "").strip() or None,
            )
            if int(freshness.get("stale_days") or 999) <= _MAX_STALE_DAYS:
                results.append(
                    {
                        "series_role": identity["series_role"],
                        "provider_symbol": identity["provider_symbol"],
                        "status": "skipped_fresh",
                        "bars_written": 0,
                        "series_quality_summary": freshness,
                    }
                )
                continue
        ingest_run_id = record_series_run_start(
            conn,
            candidate_id=str(identity["candidate_id"]),
            series_role=str(identity["series_role"]),
            provider="twelve_data",
            run_type="backfill" if force_refresh or not existing else "incremental_refresh",
        )
        try:
            if str(identity.get("series_role") or "") == "approved_proxy":
                reusable = load_reusable_price_series(
                    conn,
                    provider_symbol=str(identity["provider_symbol"]),
                    series_role=str(identity["series_role"]),
                    interval=str(identity.get("primary_interval") or "1day"),
                    exclude_candidate_id=str(identity["candidate_id"]),
                )
                reusable_rows = list((reusable or {}).get("rows") or [])
                if reusable_rows:
                    reusable_summary = _series_quality_summary(
                        rows=reusable_rows,
                        series_role=str(identity["series_role"]),
                        adjustment_mode=str(identity.get("adjustment_mode") or "provider_native"),
                        exchange_mic=str(identity.get("exchange_mic") or "").strip() or None,
                        provider_symbol=str(identity.get("provider_symbol") or identity.get("symbol") or "").strip() or None,
                        asset_class=str(identity.get("provider_asset_class") or "").strip() or None,
                    )
                    if (
                        int(reusable_summary.get("bars_present") or 0) >= _MIN_HISTORY_BARS
                        and str(reusable_summary.get("quality_label") or "") != "broken"
                    ):
                        gap_flags = detect_candidate_series_gaps(
                            reusable_rows,
                            exchange_mic=str(identity.get("exchange_mic") or "").strip() or None,
                            provider_symbol=str(identity.get("provider_symbol") or identity.get("symbol") or "").strip() or None,
                            asset_class=str(identity.get("provider_asset_class") or "").strip() or None,
                        )
                        cloned_rows: list[dict[str, Any]] = []
                        for source_row in reusable_rows:
                            cloned_rows.append(
                                {
                                    "candidate_id": str(identity["candidate_id"]),
                                    "instrument_id": str(identity["instrument_id"]),
                                    "series_role": str(identity["series_role"]),
                                    "timestamp_utc": source_row["timestamp_utc"],
                                    "interval": str(source_row.get("interval") or identity.get("primary_interval") or "1day"),
                                    "open": float(source_row["open"]),
                                    "high": float(source_row["high"]),
                                    "low": float(source_row["low"]),
                                    "close": float(source_row["close"]),
                                    "volume": source_row.get("volume"),
                                    "amount": source_row.get("amount"),
                                    "provider": str(source_row.get("provider") or "twelve_data"),
                                    "provider_symbol": str(identity["provider_symbol"]),
                                    "adjusted_flag": bool(source_row.get("adjusted_flag")),
                                    "freshness_ts": str(source_row.get("freshness_ts") or source_row["timestamp_utc"]),
                                    "quality_flags": sorted(set(list(source_row.get("quality_flags") or []) + gap_flags)),
                                    "series_quality_summary": reusable_summary,
                                    "ingest_run_id": ingest_run_id,
                                }
                            )
                        upsert_price_series_rows(conn, cloned_rows)
                        finish_series_run(
                            conn,
                            ingest_run_id=ingest_run_id,
                            status="succeeded",
                            bars_written=len(cloned_rows),
                            failure_class=None,
                            details={
                                "series_quality_summary": reusable_summary,
                                "gap_flags": gap_flags,
                                "reused_from_store": True,
                                "reuse_source_candidate_id": reusable.get("source_candidate_id"),
                                "reuse_provider_symbol": identity.get("provider_symbol"),
                                **_attempt_details(
                                    identity=identity,
                                    provider_name=str(cloned_rows[-1].get("provider") or "twelve_data"),
                                    provider_symbol=str(identity.get("provider_symbol") or ""),
                                    terminal_status="stale_only",
                                    terminal_cause="stale_only",
                                    freshness_state="stale",
                                    rescue_used=True,
                                    rescue_reason="reused_stored_proxy",
                                    bars_written=len(cloned_rows),
                                ),
                            },
                        )
                        _record_blueprint_family_attempt(
                            conn,
                            symbol=str(identity.get("symbol") or ""),
                            provider_name=str(cloned_rows[-1].get("provider") or "twelve_data"),
                            success=True,
                            freshness_state="stale",
                            error_class=None,
                            fallback_used=True,
                            cache_hit=True,
                            age_seconds=float(reusable_summary.get("stale_days") or 999) * 86400.0,
                        )
                        results.append(
                            {
                                "series_role": identity["series_role"],
                                "provider_symbol": identity["provider_symbol"],
                                "status": "reused_stored_proxy",
                                "bars_written": len(cloned_rows),
                                "series_quality_summary": reusable_summary,
                                "gap_flags": gap_flags,
                                "ingest_run_id": ingest_run_id,
                                "reused_from_store": True,
                                "reuse_source_candidate_id": reusable.get("source_candidate_id"),
                            }
                        )
                        continue
            payload = client.fetch_daily_ohlcv(str(identity["provider_symbol"]))
            preliminary = normalize_twelvedata_series(
                payload=payload,
                identity=identity,
                ingest_run_id=ingest_run_id,
                series_quality_summary={},
            )
            summary = _series_quality_summary(
                rows=preliminary,
                series_role=str(identity["series_role"]),
                adjustment_mode=str(identity.get("adjustment_mode") or "adjusted"),
                exchange_mic=str(identity.get("exchange_mic") or "").strip() or None,
                provider_symbol=str(identity.get("provider_symbol") or identity.get("symbol") or "").strip() or None,
                asset_class=str(identity.get("provider_asset_class") or "").strip() or None,
            )
            if _needs_secondary_history_recovery(summary):
                recovered = _try_bounded_secondary_history_recovery(
                    conn,
                    identity=identity,
                    ingest_run_id=ingest_run_id,
                )
                if recovered is not None:
                    results.append(recovered)
                    continue
                failure_class = "empty_payload" if not list(payload.get("series") or []) else "no_usable_history"
                overall_failure = failure_class
                finish_series_run(
                    conn,
                    ingest_run_id=ingest_run_id,
                    status="failed",
                    bars_written=0,
                    failure_class=failure_class,
                    details={
                        "series_quality_summary": summary,
                        **_attempt_details(
                            identity=identity,
                            provider_name="twelve_data",
                            provider_symbol=str(identity.get("provider_symbol") or ""),
                            terminal_status="current_failure",
                            terminal_cause=failure_class,
                            freshness_state="unavailable",
                            rescue_used=False,
                            bars_written=0,
                        ),
                    },
                )
                _record_blueprint_family_attempt(
                    conn,
                    symbol=str(identity.get("symbol") or ""),
                    provider_name="twelve_data",
                    success=False,
                    freshness_state="unavailable",
                    error_class=failure_class,
                    fallback_used=False,
                )
                results.append(
                    {
                        "series_role": identity["series_role"],
                        "provider_symbol": identity["provider_symbol"],
                        "status": "failed",
                        "failure_class": failure_class,
                    }
                )
                continue
            gap_flags = detect_candidate_series_gaps(
                preliminary,
                exchange_mic=str(identity.get("exchange_mic") or "").strip() or None,
                provider_symbol=str(identity.get("provider_symbol") or identity.get("symbol") or "").strip() or None,
                asset_class=str(identity.get("provider_asset_class") or "").strip() or None,
            )
            normalized = normalize_twelvedata_series(
                payload=payload,
                identity=identity,
                ingest_run_id=ingest_run_id,
                series_quality_summary=summary,
            )
            for row in normalized:
                row["quality_flags"] = sorted(set(list(row.get("quality_flags") or []) + gap_flags))
            upsert_price_series_rows(conn, normalized)
            finish_series_run(
                conn,
                ingest_run_id=ingest_run_id,
                status="succeeded",
                bars_written=len(normalized),
                failure_class=None,
                details={
                    "series_quality_summary": summary,
                    "gap_flags": gap_flags,
                    **_attempt_details(
                        identity=identity,
                        provider_name="twelve_data",
                        provider_symbol=str(identity.get("provider_symbol") or ""),
                        terminal_status="current_success",
                        terminal_cause=None,
                        freshness_state="current",
                        rescue_used=False,
                        bars_written=len(normalized),
                    ),
                },
            )
            _record_blueprint_family_attempt(
                conn,
                symbol=str(identity.get("symbol") or ""),
                provider_name="twelve_data",
                success=True,
                freshness_state="current",
                error_class=None,
                fallback_used=False,
                age_seconds=0.0,
            )
            results.append(
                {
                    "series_role": identity["series_role"],
                    "provider_symbol": identity["provider_symbol"],
                    "status": "succeeded",
                    "bars_written": len(normalized),
                    "series_quality_summary": summary,
                    "gap_flags": gap_flags,
                    "ingest_run_id": ingest_run_id,
                }
            )
        except ProviderAdapterError as exc:
            recovered = _try_bounded_secondary_history_recovery(
                conn,
                identity=identity,
                ingest_run_id=ingest_run_id,
            )
            if recovered is not None:
                results.append(recovered)
                continue
            failure_class = _normalize_series_failure_class(str(exc.error_class or ""), str(exc))
            overall_failure = failure_class
            finish_series_run(
                conn,
                ingest_run_id=ingest_run_id,
                status="failed",
                bars_written=0,
                failure_class=failure_class,
                details={
                    "error": str(exc),
                    "error_class": exc.error_class,
                    **_attempt_details(
                        identity=identity,
                        provider_name="twelve_data",
                        provider_symbol=str(identity.get("provider_symbol") or ""),
                        terminal_status="current_failure",
                        terminal_cause=failure_class,
                        freshness_state="unavailable",
                        rescue_used=False,
                        bars_written=0,
                    ),
                },
            )
            _record_blueprint_family_attempt(
                conn,
                symbol=str(identity.get("symbol") or ""),
                provider_name="twelve_data",
                success=False,
                freshness_state="unavailable",
                error_class=failure_class,
                fallback_used=False,
            )
            results.append(
                {
                    "series_role": identity["series_role"],
                    "provider_symbol": identity["provider_symbol"],
                    "status": "failed",
                    "failure_class": failure_class,
                    "error": str(exc),
                }
            )
    recompute_provider_family_success(
        conn,
        surface_name="blueprint",
        family_name=canonical_blueprint_family_id("ohlcv_history"),
    )
    return {
        "candidate_id": candidate_id,
        "status": (
            "failed"
            if overall_failure and not any(str(item.get("status") or "") in {"succeeded", "reused_stored_proxy"} for item in results)
            else "succeeded"
        ),
        "failure_class": overall_failure,
        "series_roles": results,
    }


def refresh_candidate_series(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    stale_only: bool = True,
) -> dict[str, Any]:
    return backfill_candidate_series(conn, candidate_id=candidate_id, force_refresh=not stale_only)


def operator_market_series_refresh(
    conn: sqlite3.Connection,
    *,
    candidate_id: str | None = None,
    sleeve_key: str | None = None,
    stale_only: bool = False,
    verify_symbol_mapping_only: bool = False,
) -> dict[str, Any]:
    selected = list_blueprint_market_candidates(conn, candidate_id=candidate_id, sleeve_key=sleeve_key)
    outcomes: list[dict[str, Any]] = []
    for row in selected:
        cid = candidate_id_for_symbol(str(row.get("symbol") or "").strip())
        if verify_symbol_mapping_only:
            identities = ensure_candidate_market_identities(conn, cid)
            outcomes.append(
                {
                    "candidate_id": cid,
                    "symbol": row.get("symbol"),
                    "status": "verified",
                    "identities": [
                        {
                            "series_role": item.get("series_role"),
                            "provider_symbol": item.get("provider_symbol"),
                            "forecast_driving_series": bool(item.get("forecast_driving_series")),
                            "resolution_method": item.get("resolution_method"),
                        }
                        for item in identities
                    ],
                }
            )
            continue
        outcomes.append(refresh_candidate_series(conn, candidate_id=cid, stale_only=stale_only))
    return {
        "count": len(outcomes),
        "stale_only": stale_only,
        "verify_symbol_mapping_only": verify_symbol_mapping_only,
        "items": outcomes,
    }


def run_market_series_refresh_lane(
    conn: sqlite3.Connection,
    *,
    candidate_id: str | None = None,
    sleeve_key: str | None = None,
    stale_only: bool = True,
) -> dict[str, Any]:
    selected = list_blueprint_market_candidates(conn, candidate_id=candidate_id, sleeve_key=sleeve_key)
    outcomes: list[dict[str, Any]] = []
    refreshed = 0
    skipped = 0
    failed = 0
    for row in selected:
        cid = candidate_id_for_symbol(str(row.get("symbol") or "").strip())
        result = refresh_candidate_series(conn, candidate_id=cid, stale_only=stale_only)
        outcomes.append(result)
        status = str(result.get("status") or "")
        if status == "succeeded":
            series_roles = list(result.get("series_roles") or [])
            if any(str(item.get("status") or "") in {"succeeded", "reused_stored_proxy"} for item in series_roles):
                refreshed += 1
            elif series_roles and all(str(item.get("status") or "") == "skipped_fresh" for item in series_roles):
                skipped += 1
            else:
                refreshed += 1
        else:
            failed += 1
    return {
        "scope": {
            "candidate_id": candidate_id,
            "sleeve_key": sleeve_key,
            "eligible_count": len(selected),
            "stale_only": stale_only,
        },
        "status": "ok" if failed == 0 else "partial",
        "refreshed_count": refreshed,
        "skipped_count": skipped,
        "failure_count": failed,
        "items": outcomes,
    }


def run_market_identity_gap_audit(
    conn: sqlite3.Connection,
    *,
    candidate_id: str | None = None,
    sleeve_key: str | None = None,
) -> dict[str, Any]:
    selected = list_blueprint_market_candidates(conn, candidate_id=candidate_id, sleeve_key=sleeve_key)
    audits: list[dict[str, Any]] = []
    stale_count = 0
    degraded_count = 0
    broken_count = 0
    for row in selected:
        cid = candidate_id_for_symbol(str(row.get("symbol") or "").strip())
        identities = ensure_candidate_market_identities(conn, cid)
        identity_rows: list[dict[str, Any]] = []
        for identity in identities:
            freshness = check_candidate_series_freshness(
                conn,
                candidate_id=str(identity["candidate_id"]),
                series_role=str(identity["series_role"]),
            )
            summary = dict(freshness.get("series_quality_summary") or {})
            gap_flags = list(freshness.get("gap_flags") or [])
            quality_label = str(summary.get("quality_label") or "")
            if int(summary.get("stale_days") or 0) > _MAX_STALE_DAYS:
                stale_count += 1
            if quality_label == "degraded":
                degraded_count += 1
            if quality_label == "broken":
                broken_count += 1
            identity_rows.append(
                {
                    "series_role": identity.get("series_role"),
                    "provider_symbol": identity.get("provider_symbol"),
                    "forecast_driving_series": bool(identity.get("forecast_driving_series")),
                    "resolution_method": identity.get("resolution_method"),
                    "resolution_confidence": identity.get("resolution_confidence"),
                    "series_quality_summary": summary,
                    "gap_flags": gap_flags,
                }
            )
        audits.append(
            {
                "candidate_id": cid,
                "symbol": row.get("symbol"),
                "identity_count": len(identities),
                "identities": identity_rows,
            }
        )
    return {
        "scope": {"candidate_id": candidate_id, "sleeve_key": sleeve_key, "eligible_count": len(selected)},
        "status": "ok" if broken_count == 0 else "partial",
        "stale_count": stale_count,
        "degraded_count": degraded_count,
        "broken_count": broken_count,
        "items": audits,
    }
