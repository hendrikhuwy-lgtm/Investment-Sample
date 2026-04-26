from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.services.blueprint_benchmark_registry import resolve_benchmark_assignment
from app.services.blueprint_candidate_registry import (
    ensure_candidate_registry_tables,
    export_live_candidate_registry,
    seed_default_candidate_registry,
)
from app.services.symbol_resolution import ensure_symbol_resolution_tables, resolve_provider_identifiers
from app.v2.blueprint_market.series_store import ensure_blueprint_market_tables, list_market_identities, upsert_market_identity


_DEFAULT_INTERVAL = "1day"
_DEFAULT_LOOKBACK_DAYS = 430


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _slug(value: str) -> str:
    return str(value or "").strip().lower().replace(".", "_").replace("-", "_")


def instrument_id_for_symbol(symbol: str) -> str:
    return f"instrument_{_slug(symbol) or 'unknown'}"


def candidate_id_for_symbol(symbol: str) -> str:
    return f"candidate_{instrument_id_for_symbol(symbol)}"


def _normalize_candidate_symbol(candidate_id_or_symbol: str) -> str:
    raw = str(candidate_id_or_symbol or "").strip()
    if raw.startswith("candidate_instrument_"):
        return raw.removeprefix("candidate_instrument_").upper()
    if raw.startswith("instrument_"):
        return raw.removeprefix("instrument_").upper()
    if raw.startswith("candidate_"):
        return raw.removeprefix("candidate_").removeprefix("instrument_").upper()
    return raw.upper()


def _candidate_row(conn: sqlite3.Connection, candidate_id_or_symbol: str) -> dict[str, Any] | None:
    ensure_candidate_registry_tables(conn)
    rows = export_live_candidate_registry(conn)
    if not rows:
        seed_default_candidate_registry(conn)
        rows = export_live_candidate_registry(conn)
    symbol = _normalize_candidate_symbol(candidate_id_or_symbol)
    return next((row for row in rows if str(row.get("symbol") or "").strip().upper() == symbol), None)


def _canonical_instrument_row(conn: sqlite3.Connection, symbol: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT *
        FROM blueprint_canonical_instruments
        WHERE symbol = ?
        LIMIT 1
        """,
        (str(symbol).strip().upper(),),
    ).fetchone()
    return dict(row) if row is not None else {}


def _candidate_extra(candidate_row: dict[str, Any], canonical_row: dict[str, Any]) -> dict[str, Any]:
    extra: dict[str, Any] = {}
    for raw in (canonical_row.get("extra_json"), candidate_row.get("extra_json")):
        try:
            payload = json.loads(str(raw or "{}"))
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            extra.update(payload)
    return extra


def _quote_currency(candidate_row: dict[str, Any], canonical_row: dict[str, Any], extra: dict[str, Any]) -> str:
    for value in (
        extra.get("primary_trading_currency"),
        candidate_row.get("primary_trading_currency"),
        canonical_row.get("base_currency"),
        canonical_row.get("quote_currency"),
    ):
        text = str(value or "").strip().upper()
        if text:
            return text
    return "USD"


def _exchange_mic(candidate_row: dict[str, Any], canonical_row: dict[str, Any], extra: dict[str, Any]) -> str | None:
    for value in (
        extra.get("primary_listing_exchange"),
        candidate_row.get("primary_listing_exchange"),
        canonical_row.get("primary_listing_exchange"),
    ):
        text = str(value or "").strip().upper()
        if text:
            return text
    return None


def _provider_asset_class(candidate_row: dict[str, Any], canonical_row: dict[str, Any], *, proxy: bool) -> str:
    if proxy:
        return "benchmark_proxy"
    for value in (
        candidate_row.get("asset_class"),
        canonical_row.get("asset_class"),
        candidate_row.get("instrument_type"),
        canonical_row.get("instrument_type"),
    ):
        text = str(value or "").strip().lower()
        if text:
            return text
    return "etf"


def _adjustment_mode(asset_class: str) -> str:
    normalized = str(asset_class or "").strip().lower()
    if any(token in normalized for token in ("etf", "equity", "stock")):
        return "adjusted"
    if any(token in normalized for token in ("fx", "forex", "crypto")):
        return "unadjusted"
    return "provider_native"


def _timezone_for_identity(asset_class: str) -> str:
    normalized = str(asset_class or "").strip().lower()
    if "fx" in normalized or "crypto" in normalized:
        return "UTC"
    return "America/New_York"


def ensure_candidate_market_identities(conn: sqlite3.Connection, candidate_id_or_symbol: str) -> list[dict[str, Any]]:
    ensure_blueprint_market_tables(conn)
    ensure_symbol_resolution_tables(conn)
    row = _candidate_row(conn, candidate_id_or_symbol)
    if row is None:
        return []
    symbol = str(row.get("symbol") or "").strip().upper()
    candidate_id = candidate_id_for_symbol(symbol)
    instrument_id = instrument_id_for_symbol(symbol)
    canonical_row = _canonical_instrument_row(conn, symbol)
    extra = _candidate_extra(row, canonical_row)
    assignment = resolve_benchmark_assignment(conn, candidate=row, sleeve_key=str(row.get("sleeve_key") or "").strip())
    verified_at = _now_iso()
    direct_asset_class = _provider_asset_class(row, canonical_row, proxy=False)
    direct_resolution = resolve_provider_identifiers(
        conn,
        provider_name="twelve_data",
        endpoint_family="ohlcv_history",
        identifier=symbol,
        asset_type=direct_asset_class,
        region=str(row.get("domicile") or "").strip() or None,
    )
    direct = {
        "candidate_id": candidate_id,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "provider_symbol": str(direct_resolution.get("provider_symbol") or symbol).strip().upper() or symbol,
        "provider_asset_class": direct_asset_class,
        "exchange_mic": _exchange_mic(row, canonical_row, extra),
        "quote_currency": _quote_currency(row, canonical_row, extra),
        "series_role": "direct",
        "adjustment_mode": _adjustment_mode(direct_asset_class),
        "timezone": _timezone_for_identity(direct_asset_class),
        "primary_interval": _DEFAULT_INTERVAL,
        "preferred_lookback_days": _DEFAULT_LOOKBACK_DAYS,
        "forecast_eligibility": "eligible",
        "proxy_relationship": None,
        "resolution_method": str(direct_resolution.get("resolution_reason") or "registry_symbol_direct"),
        "resolution_confidence": float(direct_resolution.get("resolution_confidence") or 1.0),
        "resolved_from": str(direct_resolution.get("verification_source") or "symbol_resolution_registry"),
        "last_verified_at": verified_at,
        "forecast_driving_series": True,
    }
    upsert_market_identity(conn, direct)
    proxy_symbol = str(assignment.get("benchmark_proxy_symbol") or "").strip().upper()
    if proxy_symbol:
        proxy_asset_class = _provider_asset_class(row, canonical_row, proxy=True)
        proxy_resolution = resolve_provider_identifiers(
            conn,
            provider_name="twelve_data",
            endpoint_family="benchmark_proxy",
            identifier=proxy_symbol,
            asset_type=proxy_asset_class,
            region=str(row.get("domicile") or "").strip() or None,
        )
        proxy = {
            "candidate_id": candidate_id,
            "instrument_id": instrument_id,
            "symbol": symbol,
            "provider_symbol": str(proxy_resolution.get("provider_symbol") or proxy_symbol).strip().upper() or proxy_symbol,
            "provider_asset_class": proxy_asset_class,
            "exchange_mic": _exchange_mic(row, canonical_row, extra),
            "quote_currency": _quote_currency(row, canonical_row, extra),
            "series_role": "approved_proxy",
            "adjustment_mode": _adjustment_mode(proxy_asset_class),
            "timezone": _timezone_for_identity(proxy_asset_class),
            "primary_interval": _DEFAULT_INTERVAL,
            "preferred_lookback_days": _DEFAULT_LOOKBACK_DAYS,
            "forecast_eligibility": "eligible",
            "proxy_relationship": f"benchmark_proxy:{proxy_symbol}",
            "resolution_method": str(proxy_resolution.get("resolution_reason") or "benchmark_assignment_proxy"),
            "resolution_confidence": float(proxy_resolution.get("resolution_confidence") or 0.85),
            "resolved_from": str(proxy_resolution.get("verification_source") or "symbol_resolution_registry"),
            "last_verified_at": verified_at,
            "forecast_driving_series": False,
        }
        upsert_market_identity(conn, proxy)
    return list_market_identities(conn, candidate_id)


def resolve_market_identity(
    conn: sqlite3.Connection,
    candidate_id_or_symbol: str,
    *,
    allow_proxy: bool = True,
) -> dict[str, Any] | None:
    identities = ensure_candidate_market_identities(conn, candidate_id_or_symbol)
    if not identities:
        return None
    direct = next(
        (
            item
            for item in identities
            if str(item.get("series_role") or "") == "direct"
            and str(item.get("forecast_eligibility") or "") == "eligible"
        ),
        None,
    )
    if direct is not None:
        return direct
    if allow_proxy:
        return next(
            (
                item
                for item in identities
                if str(item.get("series_role") or "") == "approved_proxy"
                and str(item.get("forecast_eligibility") or "") == "eligible"
            ),
            None,
        )
    return None


def set_forecast_driving_series(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    series_role: str,
    interval: str = _DEFAULT_INTERVAL,
) -> None:
    ensure_blueprint_market_tables(conn)
    conn.execute(
        """
        UPDATE candidate_market_identities
        SET forecast_driving_series = CASE WHEN series_role = ? THEN 1 ELSE 0 END,
            updated_at = ?
        WHERE candidate_id = ? AND primary_interval = ?
        """,
        (
            str(series_role),
            _now_iso(),
            str(candidate_id),
            str(interval),
        ),
    )
    conn.commit()
