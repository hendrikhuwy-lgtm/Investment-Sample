from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time, timedelta
from typing import Callable
from zoneinfo import ZoneInfo


HolidayFn = Callable[[int], set[date]]
EarlyCloseFn = Callable[[date], bool]


@dataclass(frozen=True)
class ExchangeCalendarPolicy:
    exchange: str
    calendar_scope: str
    timezone_name: str
    regular_open: time
    regular_close: time
    holiday_fn: HolidayFn
    calendar_precision: str = "full"
    supports_extended_hours: bool = False
    pre_market_start: time | None = None
    after_hours_end: time | None = None
    early_close_fn: EarlyCloseFn | None = None

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    observed = date(year, month, day)
    if observed.weekday() == 5:
        return observed - timedelta(days=1)
    if observed.weekday() == 6:
        return observed + timedelta(days=1)
    return observed


def _nth_weekday(year: int, month: int, weekday: int, ordinal: int) -> date:
    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    return current + timedelta(days=7 * (ordinal - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        current = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        current = date(year, month + 1, 1) - timedelta(days=1)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def nyse_holidays(year: int) -> set[date]:
    easter = _easter_sunday(year)
    return {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        easter - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }


def lse_holidays(year: int) -> set[date]:
    easter = _easter_sunday(year)
    christmas = _observed_fixed_holiday(year, 12, 25)
    boxing = _observed_fixed_holiday(year, 12, 26)
    return {
        _observed_fixed_holiday(year, 1, 1),
        easter - timedelta(days=2),
        easter + timedelta(days=1),
        _nth_weekday(year, 5, 0, 1),
        _last_weekday(year, 5, 0),
        _last_weekday(year, 8, 0),
        christmas,
        boxing if boxing != christmas else boxing + timedelta(days=1),
    }


def euronext_holidays(year: int) -> set[date]:
    easter = _easter_sunday(year)
    return {
        _observed_fixed_holiday(year, 1, 1),
        easter - timedelta(days=2),
        easter + timedelta(days=1),
        date(year, 5, 1),
        _observed_fixed_holiday(year, 12, 25),
        _observed_fixed_holiday(year, 12, 26),
    }


def hkex_holidays(year: int) -> set[date]:
    easter = _easter_sunday(year)
    return {
        _observed_fixed_holiday(year, 1, 1),
        easter - timedelta(days=2),
        easter + timedelta(days=1),
        date(year, 5, 1),
        date(year, 7, 1),
        date(year, 10, 1),
        _observed_fixed_holiday(year, 12, 25),
        _observed_fixed_holiday(year, 12, 26),
    }


def sgx_holidays(year: int) -> set[date]:
    easter = _easter_sunday(year)
    return {
        _observed_fixed_holiday(year, 1, 1),
        easter - timedelta(days=2),
        date(year, 5, 1),
        date(year, 8, 9),
        _observed_fixed_holiday(year, 12, 25),
    }


def is_nyse_early_close(day_value: date) -> bool:
    year = day_value.year
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    christmas_eve = date(year, 12, 24)
    july_3 = date(year, 7, 3)
    early_closes = {
        thanksgiving + timedelta(days=1),
    }
    if christmas_eve.weekday() < 5 and christmas_eve not in nyse_holidays(year):
        early_closes.add(christmas_eve)
    if july_3.weekday() < 5 and july_3 not in nyse_holidays(year) and _observed_fixed_holiday(year, 7, 4) != july_3:
        early_closes.add(july_3)
    return day_value in early_closes


_CALENDAR_BY_EXCHANGE = {
    "XNYS": ExchangeCalendarPolicy(
        exchange="XNYS",
        calendar_scope="us_equities",
        timezone_name="America/New_York",
        regular_open=time(9, 30),
        regular_close=time(16, 0),
        holiday_fn=nyse_holidays,
        calendar_precision="full",
        supports_extended_hours=True,
        pre_market_start=time(4, 0),
        after_hours_end=time(20, 0),
        early_close_fn=is_nyse_early_close,
    ),
    "XNAS": ExchangeCalendarPolicy(
        exchange="XNAS",
        calendar_scope="us_equities",
        timezone_name="America/New_York",
        regular_open=time(9, 30),
        regular_close=time(16, 0),
        holiday_fn=nyse_holidays,
        calendar_precision="full",
        supports_extended_hours=True,
        pre_market_start=time(4, 0),
        after_hours_end=time(20, 0),
        early_close_fn=is_nyse_early_close,
    ),
    "NYSEARCA": ExchangeCalendarPolicy(
        exchange="NYSEARCA",
        calendar_scope="us_equities",
        timezone_name="America/New_York",
        regular_open=time(9, 30),
        regular_close=time(16, 0),
        holiday_fn=nyse_holidays,
        calendar_precision="full",
        supports_extended_hours=True,
        pre_market_start=time(4, 0),
        after_hours_end=time(20, 0),
        early_close_fn=is_nyse_early_close,
    ),
    "XLON": ExchangeCalendarPolicy(
        exchange="XLON",
        calendar_scope="uk_equities",
        timezone_name="Europe/London",
        regular_open=time(8, 0),
        regular_close=time(16, 30),
        holiday_fn=lse_holidays,
        calendar_precision="full",
    ),
    "XHKG": ExchangeCalendarPolicy(
        exchange="XHKG",
        calendar_scope="hong_kong_equities",
        timezone_name="Asia/Hong_Kong",
        regular_open=time(9, 30),
        regular_close=time(16, 0),
        holiday_fn=hkex_holidays,
        calendar_precision="partial",
    ),
    "XSES": ExchangeCalendarPolicy(
        exchange="XSES",
        calendar_scope="singapore_equities",
        timezone_name="Asia/Singapore",
        regular_open=time(9, 0),
        regular_close=time(17, 0),
        holiday_fn=sgx_holidays,
        calendar_precision="partial",
    ),
    "XPAR": ExchangeCalendarPolicy(
        exchange="XPAR",
        calendar_scope="euronext_equities",
        timezone_name="Europe/Paris",
        regular_open=time(9, 0),
        regular_close=time(17, 30),
        holiday_fn=euronext_holidays,
        calendar_precision="full",
    ),
    "XAMS": ExchangeCalendarPolicy(
        exchange="XAMS",
        calendar_scope="euronext_equities",
        timezone_name="Europe/Amsterdam",
        regular_open=time(9, 0),
        regular_close=time(17, 30),
        holiday_fn=euronext_holidays,
        calendar_precision="full",
    ),
    "XBRU": ExchangeCalendarPolicy(
        exchange="XBRU",
        calendar_scope="euronext_equities",
        timezone_name="Europe/Brussels",
        regular_open=time(9, 0),
        regular_close=time(17, 30),
        holiday_fn=euronext_holidays,
        calendar_precision="full",
    ),
}

_EXCHANGE_ALIASES = {
    "NYSE": "XNYS",
    "XNYS": "XNYS",
    "NASDAQ": "XNAS",
    "XNAS": "XNAS",
    "NYSEARCA": "NYSEARCA",
    "ARCA": "NYSEARCA",
    "LSE": "XLON",
    "XLON": "XLON",
    "HKEX": "XHKG",
    "SEHK": "XHKG",
    "XHKG": "XHKG",
    "SGX": "XSES",
    "XSES": "XSES",
    "EURONEXT": "XPAR",
    "XPAR": "XPAR",
    "XAMS": "XAMS",
    "XBRU": "XBRU",
}


def normalize_exchange_code(value: str | None) -> str | None:
    raw = str(value or "").strip().upper()
    if not raw:
        return None
    return _EXCHANGE_ALIASES.get(raw, raw)


def get_exchange_calendar_policy(exchange: str | None) -> ExchangeCalendarPolicy | None:
    normalized = normalize_exchange_code(exchange)
    if not normalized:
        return None
    return _CALENDAR_BY_EXCHANGE.get(normalized)
