from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.v2.truth.exchange_calendar_registry import get_exchange_calendar_policy
from app.v2.truth.market_calendar import (
    coerce_datetime,
    is_exchange_trading_day,
    next_trading_day,
    normalize_asset_class,
    normalize_exchange,
)


def _identity_symbol(identity: dict[str, Any] | None) -> str | None:
    if not identity:
        return None
    return (
        str(identity.get("provider_symbol") or "").strip().upper()
        or str(identity.get("symbol") or "").strip().upper()
        or None
    )


def resolve_identity_calendar(identity: dict[str, Any] | None) -> dict[str, Any]:
    symbol = _identity_symbol(identity)
    exchange = normalize_exchange(str((identity or {}).get("exchange_mic") or "").strip() or None, symbol)
    asset_class = normalize_asset_class(str((identity or {}).get("provider_asset_class") or "").strip() or None, symbol)
    policy = get_exchange_calendar_policy(exchange)
    return {
        "symbol": symbol,
        "exchange": exchange,
        "asset_class": asset_class,
        "policy": policy,
        "calendar_resolved": policy is not None,
    }


def session_dates_between(
    *,
    start_timestamp: Any,
    end_timestamp: Any,
    identity: dict[str, Any] | None,
) -> list[str] | None:
    calendar = resolve_identity_calendar(identity)
    policy = calendar["policy"]
    start_dt = coerce_datetime(start_timestamp)
    end_dt = coerce_datetime(end_timestamp)
    if policy is None or start_dt is None or end_dt is None:
        return None
    start_date = min(start_dt.date(), end_dt.date())
    end_date = max(start_dt.date(), end_dt.date())
    current = start_date
    values: list[str] = []
    while current <= end_date:
        if is_exchange_trading_day(current, policy):
            values.append(current.isoformat())
        current = next_trading_day(current, policy) if current < end_date else current
        if values and values[-1] == current.isoformat():
            break
        if current > end_date:
            break
    return values


def future_session_timestamps(
    *,
    last_timestamp: Any,
    horizon: int,
    identity: dict[str, Any] | None,
) -> list[str] | None:
    calendar = resolve_identity_calendar(identity)
    policy = calendar["policy"]
    anchor = coerce_datetime(last_timestamp)
    if policy is None or anchor is None or horizon <= 0:
        return None
    current = anchor.date()
    values: list[str] = []
    for _ in range(horizon):
        current = next_trading_day(current, policy)
        values.append(datetime.combine(current, datetime.min.time(), tzinfo=UTC).isoformat())
    return values

