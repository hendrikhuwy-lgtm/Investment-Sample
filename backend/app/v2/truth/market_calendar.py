from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from app.v2.truth.exchange_calendar_registry import (
    ExchangeCalendarPolicy,
    get_exchange_calendar_policy,
    normalize_exchange_code,
)


_KNOWN_SYMBOL_HINTS: dict[str, tuple[str, str]] = {
    "ACWI": ("NYSEARCA", "etf"),
    "AGG": ("NYSEARCA", "etf"),
    "GLD": ("NYSEARCA", "etf"),
    "SPY": ("NYSEARCA", "etf"),
    "VEU": ("NYSEARCA", "etf"),
    "VT": ("NYSEARCA", "etf"),
    "TLT": ("NASDAQ", "etf"),
    "BND": ("NASDAQ", "etf"),
    "UUP": ("NYSEARCA", "etf"),
    "DXY": ("OTC", "fx_index"),
}


def now_utc() -> datetime:
    return datetime.now(UTC)


def coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        if len(normalized) == 10:
            try:
                parsed = datetime.fromisoformat(f"{normalized}T00:00:00+00:00")
            except ValueError:
                return None
        else:
            return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def normalize_exchange(exchange: str | None, identifier: str | None = None) -> str | None:
    raw = normalize_exchange_code(exchange)
    if raw:
        return raw
    symbol = str(identifier or "").strip().upper()
    hint = _KNOWN_SYMBOL_HINTS.get(symbol)
    return normalize_exchange_code(hint[0] if hint else None)


def normalize_asset_class(asset_class: str | None, identifier: str | None = None) -> str | None:
    raw = str(asset_class or "").strip().lower()
    if raw:
        return raw
    symbol = str(identifier or "").strip().upper()
    hint = _KNOWN_SYMBOL_HINTS.get(symbol)
    return hint[1] if hint else None


def is_exchange_trading_day(day_value: date, policy: ExchangeCalendarPolicy) -> bool:
    return day_value.weekday() < 5 and day_value not in policy.holiday_fn(day_value.year)


def previous_trading_day(day_value: date, policy: ExchangeCalendarPolicy) -> date:
    current = day_value - timedelta(days=1)
    while not is_exchange_trading_day(current, policy):
        current -= timedelta(days=1)
    return current


def next_trading_day(day_value: date, policy: ExchangeCalendarPolicy) -> date:
    current = day_value + timedelta(days=1)
    while not is_exchange_trading_day(current, policy):
        current += timedelta(days=1)
    return current


def build_market_session_context(
    *,
    as_of: Any,
    exchange: str | None,
    asset_class: str | None,
    identifier: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    normalized_exchange = normalize_exchange(exchange, identifier)
    normalized_asset_class = normalize_asset_class(asset_class, identifier)
    current_time = now or now_utc()
    anchor = coerce_datetime(as_of)
    fallback_date = (anchor or current_time).date().isoformat()
    policy = get_exchange_calendar_policy(normalized_exchange)

    if policy is None and normalized_asset_class not in {"equity", "etf"}:
        return {
            "exchange": normalized_exchange,
            "asset_class": normalized_asset_class,
            "session_state": "unsupported_exchange_generic",
            "session_label": "Unsupported exchange calendar",
            "session_date": fallback_date,
            "quote_session": None,
            "calendar_scope": "unsupported_exchange_generic",
            "calendar_precision": "none",
            "is_early_close": False,
            "extended_hours_state": None,
            "regular_open_utc": None,
            "regular_close_utc": None,
            "effective_close_utc": None,
            "next_regular_open_utc": None,
        }

    if policy is None:
        return {
            "exchange": normalized_exchange,
            "asset_class": normalized_asset_class,
            "session_state": "unsupported_exchange_generic",
            "session_label": "Unsupported exchange calendar",
            "session_date": fallback_date,
            "quote_session": None,
            "calendar_scope": "unsupported_exchange_generic",
            "calendar_precision": "none",
            "is_early_close": False,
            "extended_hours_state": None,
            "regular_open_utc": None,
            "regular_close_utc": None,
            "effective_close_utc": None,
            "next_regular_open_utc": None,
        }

    local_zone = policy.timezone
    current_local = current_time.astimezone(local_zone)
    anchor_local = anchor.astimezone(local_zone) if anchor is not None else None
    session_date = current_local.date()
    regular_open = datetime.combine(session_date, policy.regular_open, tzinfo=local_zone)
    regular_close = datetime.combine(session_date, policy.regular_close, tzinfo=local_zone)
    pre_market_open = (
        datetime.combine(session_date, policy.pre_market_start, tzinfo=local_zone)
        if policy.pre_market_start is not None
        else None
    )
    after_hours_end = (
        datetime.combine(session_date, policy.after_hours_end, tzinfo=local_zone)
        if policy.after_hours_end is not None
        else None
    )
    early_close = bool(policy.early_close_fn(session_date)) if policy.early_close_fn else False
    effective_close = regular_close
    if early_close:
        effective_close = regular_close - timedelta(hours=3)

    if not is_exchange_trading_day(session_date, policy):
        session_state = "holiday_or_weekend"
        session_label = "Market holiday or weekend"
        extended_hours_state = None
    elif pre_market_open is not None and current_local < regular_open and current_local >= pre_market_open:
        session_state = "pre_market"
        session_label = "Pre-market"
        extended_hours_state = "pre_market"
    elif current_local < regular_open:
        session_state = "closed_regular_session"
        session_label = "Closed between sessions"
        extended_hours_state = None
    elif current_local <= effective_close:
        session_state = "regular_hours"
        session_label = "Regular session (early close)" if early_close else "Regular session"
        extended_hours_state = None
    elif after_hours_end is not None and current_local <= after_hours_end:
        session_state = "after_hours"
        session_label = "After-hours"
        extended_hours_state = "after_hours"
    else:
        session_state = "closed_regular_session"
        session_label = "Closed between sessions"
        extended_hours_state = None

    quote_session = None
    if anchor_local is not None:
        if anchor_local.date() == session_date:
            quote_session = "same_session"
        elif anchor_local.date() == previous_trading_day(session_date, policy):
            quote_session = "previous_session"
        else:
            quote_session = "older_session"

    next_open_date = (
        next_trading_day(session_date, policy)
        if not is_exchange_trading_day(session_date, policy) or current_local > effective_close
        else session_date
    )
    next_regular_open = datetime.combine(next_open_date, policy.regular_open, tzinfo=local_zone)

    return {
        "exchange": normalized_exchange,
        "asset_class": normalized_asset_class or "market",
        "session_state": session_state,
        "session_label": session_label,
        "session_date": session_date.isoformat(),
        "quote_session": quote_session,
        "calendar_scope": policy.calendar_scope,
        "calendar_precision": policy.calendar_precision,
        "is_early_close": early_close,
        "extended_hours_state": extended_hours_state,
        "regular_open_utc": regular_open.astimezone(UTC).isoformat(),
        "regular_close_utc": regular_close.astimezone(UTC).isoformat(),
        "effective_close_utc": effective_close.astimezone(UTC).isoformat(),
        "next_regular_open_utc": next_regular_open.astimezone(UTC).isoformat(),
        "market_open_utc": regular_open.astimezone(UTC).isoformat(),
        "market_close_utc": effective_close.astimezone(UTC).isoformat(),
    }
