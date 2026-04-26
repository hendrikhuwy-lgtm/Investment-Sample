from __future__ import annotations

from typing import Any

from app.v2.donors.source_freshness import FreshnessClass
from app.v2.truth.market_calendar import (
    build_market_session_context as build_market_session_context_from_calendar,
    coerce_datetime,
    now_utc,
)
from app.v2.truth.macro_release_policy import macro_period_clock_class, macro_period_summary, macro_reference_period


def build_market_session_context(
    *,
    as_of: Any,
    exchange: str | None,
    asset_class: str | None,
    identifier: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    return build_market_session_context_from_calendar(
        as_of=as_of,
        exchange=exchange,
        asset_class=asset_class,
        identifier=identifier,
        now=now,
    )


def classify_market_quote_freshness(
    *,
    as_of: Any,
    exchange: str | None,
    asset_class: str | None,
    identifier: str | None = None,
    now: datetime | None = None,
) -> FreshnessClass:
    current_time = now or now_utc()
    anchor = coerce_datetime(as_of)
    if anchor is None:
        return FreshnessClass.EXECUTION_FAILED_OR_INCOMPLETE

    age_seconds = max(0, int((current_time - anchor).total_seconds()))
    session = build_market_session_context(
        as_of=anchor,
        exchange=exchange,
        asset_class=asset_class,
        identifier=identifier,
        now=current_time,
    )
    if session is None or str(session.get("calendar_scope") or "") == "unsupported_exchange_generic":
        if age_seconds <= 24 * 60 * 60:
            return FreshnessClass.FRESH_FULL_REBUILD
        if age_seconds <= 3 * 24 * 60 * 60:
            return FreshnessClass.FRESH_PARTIAL_REBUILD
        if age_seconds <= 7 * 24 * 60 * 60:
            return FreshnessClass.STORED_VALID_CONTEXT
        return FreshnessClass.DEGRADED_MONITORING_MODE

    session_state = str(session.get("session_state") or "")
    quote_session = str(session.get("quote_session") or "")
    calendar_precision = str(session.get("calendar_precision") or "full")
    regular_session_fresh = 15 * 60 if calendar_precision == "full" else 10 * 60
    regular_session_partial = 60 * 60 if calendar_precision == "full" else 45 * 60
    closed_session_carry = 72 * 60 * 60 if calendar_precision == "full" else 48 * 60 * 60
    if session_state == "regular_hours":
        if age_seconds <= regular_session_fresh:
            return FreshnessClass.FRESH_FULL_REBUILD
        if age_seconds <= regular_session_partial:
            return FreshnessClass.FRESH_PARTIAL_REBUILD
        if age_seconds <= 4 * 60 * 60:
            return FreshnessClass.STORED_VALID_CONTEXT
        return FreshnessClass.DEGRADED_MONITORING_MODE
    if session_state == "pre_market":
        if quote_session in {"same_session", "previous_session"} and age_seconds <= 24 * 60 * 60:
            return FreshnessClass.FRESH_FULL_REBUILD
        if age_seconds <= 48 * 60 * 60:
            return FreshnessClass.FRESH_PARTIAL_REBUILD
        return FreshnessClass.STORED_VALID_CONTEXT
    if session_state == "after_hours":
        if quote_session == "same_session" and age_seconds <= 4 * 60 * 60:
            return FreshnessClass.FRESH_FULL_REBUILD
        if quote_session in {"same_session", "previous_session"} and age_seconds <= 24 * 60 * 60:
            return FreshnessClass.FRESH_PARTIAL_REBUILD
        if age_seconds <= 72 * 60 * 60:
            return FreshnessClass.STORED_VALID_CONTEXT
        return FreshnessClass.DEGRADED_MONITORING_MODE
    if session_state in {"closed_regular_session", "holiday_or_weekend"}:
        if quote_session in {"same_session", "previous_session"} and age_seconds <= closed_session_carry:
            return FreshnessClass.FRESH_FULL_REBUILD
        if age_seconds <= 120 * 60 * 60:
            return FreshnessClass.FRESH_PARTIAL_REBUILD
        if age_seconds <= 10 * 24 * 60 * 60:
            return FreshnessClass.STORED_VALID_CONTEXT
        return FreshnessClass.DEGRADED_MONITORING_MODE
    return FreshnessClass.DEGRADED_MONITORING_MODE
def build_truth_envelope(
    *,
    as_of_utc: str | None,
    observation_date: str | None = None,
    reference_period: str | None,
    source_authority: str | None,
    acquisition_mode: str,
    degradation_reason: str | None = None,
    recommendation_critical: bool = False,
    retrieved_at_utc: str | None = None,
    release_date: str | None = None,
    availability_date: str | None = None,
    realtime_start: str | None = None,
    realtime_end: str | None = None,
    vintage_class: str | None = None,
    revision_state: str | None = None,
    release_semantics_state: str | None = None,
    period_clock_class: str | None = None,
    market_session_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "as_of_utc": as_of_utc,
        "observation_date": observation_date or as_of_utc,
        "reference_period": reference_period,
        "reference_period_label": reference_period.replace("-", " ") if isinstance(reference_period, str) else None,
        "source_authority": source_authority,
        "acquisition_mode": acquisition_mode,
        "degradation_reason": degradation_reason,
        "recommendation_critical": recommendation_critical,
        "retrieved_at_utc": retrieved_at_utc,
        "release_date": release_date,
        "availability_date": availability_date,
        "realtime_start": realtime_start,
        "realtime_end": realtime_end,
        "vintage_class": vintage_class,
        "revision_state": revision_state,
        "release_semantics_state": release_semantics_state,
        "period_clock_class": period_clock_class,
        "market_session_context": market_session_context,
    }


def build_market_truth_envelope(
    *,
    identifier: str,
    as_of_utc: str | None,
    provider_name: str | None,
    acquisition_mode: str,
    degradation_reason: str | None = None,
    exchange: str | None = None,
    asset_class: str | None = None,
    retrieved_at_utc: str | None = None,
) -> dict[str, Any]:
    session_context = build_market_session_context(
        as_of=as_of_utc,
        exchange=exchange,
        asset_class=asset_class,
        identifier=identifier,
    )
    return build_truth_envelope(
        as_of_utc=as_of_utc,
        observation_date=as_of_utc,
        reference_period="intraday_market",
        source_authority=provider_name or "market_price",
        acquisition_mode=acquisition_mode,
        degradation_reason=degradation_reason,
        recommendation_critical=True,
        retrieved_at_utc=retrieved_at_utc or as_of_utc,
        market_session_context=session_context,
    )


def build_macro_truth_envelope(
    *,
    series_id: str,
    observation_date: str | None,
    source_authority: str | None,
    acquisition_mode: str,
    retrieved_at_utc: str | None,
    release_date: str | None = None,
    availability_date: str | None = None,
    realtime_start: str | None = None,
    realtime_end: str | None = None,
    vintage_class: str | None = None,
    revision_state: str | None = None,
    release_semantics_state: str | None = None,
    degradation_reason: str | None = None,
) -> dict[str, Any]:
    return build_truth_envelope(
        as_of_utc=observation_date,
        observation_date=observation_date,
        reference_period=macro_reference_period(series_id, observation_date),
        source_authority=source_authority,
        acquisition_mode=acquisition_mode,
        degradation_reason=degradation_reason,
        recommendation_critical=True,
        retrieved_at_utc=retrieved_at_utc,
        release_date=release_date,
        availability_date=availability_date,
        realtime_start=realtime_start,
        realtime_end=realtime_end,
        vintage_class=vintage_class,
        revision_state=revision_state,
        release_semantics_state=release_semantics_state,
        period_clock_class=macro_period_clock_class(series_id),
        market_session_context=None,
    )


def describe_truth_envelope(envelope: dict[str, Any] | None) -> str | None:
    if not envelope:
        return None
    parts: list[str] = []
    macro_period = macro_period_summary(envelope)
    if macro_period:
        parts.append(macro_period)
    else:
        reference_period = str(envelope.get("reference_period") or "").strip()
        if reference_period:
            parts.append(f"Period {reference_period}")
    session = dict(envelope.get("market_session_context") or {})
    session_label = str(session.get("session_label") or "").strip()
    if session_label:
        parts.append(session_label)
    calendar_precision = str(session.get("calendar_precision") or "").strip()
    if calendar_precision and calendar_precision not in {"", "full"}:
        parts.append(f"Calendar {calendar_precision}")
    release_date = str(envelope.get("release_date") or "").strip()
    if release_date:
        parts.append(f"Release {release_date}")
    availability_date = str(envelope.get("availability_date") or "").strip()
    if availability_date:
        parts.append(f"Available {availability_date}")
    vintage_class = str(envelope.get("vintage_class") or "").strip()
    if vintage_class:
        parts.append(vintage_class.replace("_", " "))
    revision_state = str(envelope.get("revision_state") or "").strip()
    if revision_state:
        parts.append(revision_state.replace("_", " "))
    release_semantics_state = str(envelope.get("release_semantics_state") or "").strip()
    if release_semantics_state:
        parts.append(release_semantics_state.replace("_", " "))
    acquisition_mode = str(envelope.get("acquisition_mode") or "").strip()
    if acquisition_mode:
        parts.append(acquisition_mode.replace("_", " "))
    degradation_reason = str(envelope.get("degradation_reason") or "").strip()
    if degradation_reason:
        parts.append(f"Degraded: {degradation_reason.replace('_', ' ')}")
    return " · ".join(parts) or None
