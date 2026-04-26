from __future__ import annotations

from typing import Any


_MONTHLY_SERIES = {"FEDFUNDS", "CPIAUCSL"}
_RATE_SERIES = {"DGS10", "FEDFUNDS"}
_MARKET_LINKED_SERIES = {"SP500"}


def normalize_series_id(series_id: str | None) -> str:
    return str(series_id or "").strip().upper()


def macro_reference_period(series_id: str, observation_date: str | None) -> str | None:
    if not observation_date:
        return None
    normalized = normalize_series_id(series_id)
    if normalized in _MONTHLY_SERIES:
        return observation_date[:7]
    return observation_date[:10]


def macro_period_clock_class(series_id: str) -> str:
    normalized = normalize_series_id(series_id)
    if normalized in _MONTHLY_SERIES:
        return "monthly_release"
    if normalized in _RATE_SERIES:
        return "rate_series"
    if normalized in _MARKET_LINKED_SERIES:
        return "daily_market_series"
    return "series_clock_unknown"


def macro_revision_state(
    *,
    observation_date: str | None,
    realtime_start: str | None,
    realtime_end: str | None,
    acquisition_mode: str,
) -> str:
    if acquisition_mode == "fallback":
        return "unavailable"
    if acquisition_mode == "cached":
        return "cache_fallback"
    if realtime_start and observation_date and realtime_start[:10] == observation_date[:10]:
        return "first_release"
    if realtime_start or (realtime_end and realtime_end[:10] != "9999-12-31"):
        return "revised_vintage"
    return "latest_no_realtime"


def macro_release_semantics_state(
    *,
    acquisition_mode: str,
    realtime_start: str | None,
) -> str:
    if acquisition_mode == "cached":
        return "cache_fallback_inferred"
    if acquisition_mode == "fallback":
        return "unavailable"
    return "fred_realtime_vintage" if realtime_start else "fred_latest"


def macro_vintage_class(
    *,
    acquisition_mode: str,
    realtime_start: str | None,
) -> str:
    if acquisition_mode == "fallback":
        return "unavailable"
    if acquisition_mode == "cached":
        return "cache_fallback"
    return "realtime_release" if realtime_start else "latest_snapshot"


def macro_period_summary(envelope: dict[str, Any] | None) -> str | None:
    data = dict(envelope or {})
    if not data:
        return None
    reference_period = str(data.get("reference_period") or "").strip()
    release_date = str(data.get("release_date") or "").strip()
    period_clock_class = str(data.get("period_clock_class") or "").strip()
    revision_state = str(data.get("revision_state") or "").strip()
    parts: list[str] = []
    if reference_period:
        parts.append(f"Period {reference_period}")
    if release_date:
        parts.append(f"Release {release_date}")
    if period_clock_class:
        parts.append(period_clock_class.replace("_", " "))
    if revision_state:
        parts.append(revision_state.replace("_", " "))
    return " · ".join(parts) or None
