from __future__ import annotations

import csv
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from app.config import get_repo_root
from app.v2.donors.source_freshness import FreshnessClass, FreshnessState
from app.v2.sources.freshness import coerce_datetime
from app.v2.sources.execution_envelope import build_provider_execution
from app.v2.sources.freshness_registry import register_source
from app.v2.sources.runtime_truth import record_runtime_truth
from app.v2.truth.envelopes import build_macro_truth_envelope
from app.v2.truth.macro_release_policy import (
    macro_period_clock_class,
    macro_reference_period,
    macro_release_semantics_state,
    macro_revision_state,
    macro_vintage_class,
)


source_tier: str = "1B"
_SOURCE_ID = "macro"
_USER_AGENT = "investment-agent-v2/0.1"
_CACHE_DIRS = (
    get_repo_root() / "backend" / "outbox" / "live_cache",
    get_repo_root() / "outbox" / "live_cache",
)
_SERIES_METADATA: dict[str, dict[str, str]] = {
    "DGS10": {"name": "10-Year Treasury Constant Maturity Rate", "unit": "percent"},
    "FEDFUNDS": {"name": "Effective Federal Funds Rate", "unit": "percent"},
    "CPIAUCSL": {"name": "Consumer Price Index for All Urban Consumers", "unit": "index"},
    "SP500": {"name": "S&P 500 Index", "unit": "index"},
}
# Series reported on a monthly cadence (FRED); used to widen freshness windows
_MONTHLY_SERIES: frozenset[str] = frozenset({"CPIAUCSL", "FEDFUNDS"})
def _now() -> datetime:
    return datetime.now(UTC)


def _normalize_series_id(series_id: str) -> str:
    return str(series_id or "").strip().upper()


def _as_float(value: Any) -> float | None:
    try:
        if value in {None, "", "."}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _series_name(series_id: str) -> str:
    meta = _SERIES_METADATA.get(_normalize_series_id(series_id), {})
    return str(meta.get("name") or _normalize_series_id(series_id) or "Unknown macro series")


def _series_unit(series_id: str) -> str:
    meta = _SERIES_METADATA.get(_normalize_series_id(series_id), {})
    return str(meta.get("unit") or "value")


def _cache_path(series_id: str) -> Path | None:
    filename = f"{_normalize_series_id(series_id)}.csv"
    for directory in _CACHE_DIRS:
        path = directory / filename
        if path.exists():
            return path
    return None


def _series_from_csv(series_id: str) -> dict[str, Any] | None:
    path = _cache_path(series_id)
    if path is None:
        return None

    normalized = _normalize_series_id(series_id)
    rows: list[tuple[str, float]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not isinstance(row, dict):
                continue
            value = _as_float(row.get(normalized))
            observation_date = str(row.get("observation_date") or "").strip()
            if value is None or not observation_date:
                continue
            rows.append((observation_date, value))

    if not rows:
        return None

    latest_date, latest_value = rows[-1]
    previous_value = rows[-2][1] if len(rows) >= 2 else None
    return {
        "series_id": normalized,
        "name": _series_name(normalized),
        "value": latest_value,
        "previous_value": previous_value,
        "date": latest_date,
        "unit": _series_unit(normalized),
        "retrieved_utc": datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat(),
        "source_tier": source_tier,
        "source_ref": f"fred_cache:{path.name}",
        "reference_period": macro_reference_period(normalized, latest_date),
        "release_date": None,
        "availability_date": None,
        "realtime_start": None,
        "realtime_end": None,
        "vintage_class": macro_vintage_class(acquisition_mode="cached", realtime_start=None),
        "revision_state": "cache_fallback",
        "release_semantics_state": "cache_fallback_inferred",
        "period_clock_class": macro_period_clock_class(normalized),
    }


def _series_from_fred(series_id: str) -> dict[str, Any] | None:
    api_key = os.getenv("FRED_API_KEY", "").strip()
    if not api_key:
        return None

    normalized = _normalize_series_id(series_id)
    try:
        response = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": normalized,
                "api_key": api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 12,
            },
            headers={"User-Agent": _USER_AGENT},
            timeout=6,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None

    observations: list[dict[str, Any]] = []
    for row in payload.get("observations") or []:
        if not isinstance(row, dict):
            continue
        value = _as_float(row.get("value"))
        observed_at = str(row.get("date") or "").strip()
        if value is None or not observed_at:
            continue
        observations.append(
            {
                "date": observed_at,
                "value": value,
                "realtime_start": str(row.get("realtime_start") or "").strip() or None,
                "realtime_end": str(row.get("realtime_end") or "").strip() or None,
            }
        )
        if len(observations) >= 2:
            break

    if not observations:
        return None

    latest = observations[0]
    previous = observations[1] if len(observations) >= 2 else None
    return {
        "series_id": normalized,
        "name": _series_name(normalized),
        "value": latest["value"],
        "previous_value": previous["value"] if previous is not None else None,
        "date": latest["date"],
        "unit": _series_unit(normalized),
        "retrieved_utc": _now().isoformat(),
        "source_tier": source_tier,
        "source_ref": f"fred_api:{normalized}",
        "reference_period": macro_reference_period(normalized, latest["date"]),
        "release_date": latest.get("realtime_start"),
        "availability_date": latest.get("realtime_start"),
        "realtime_start": latest.get("realtime_start"),
        "realtime_end": latest.get("realtime_end"),
        "vintage_class": macro_vintage_class(
            acquisition_mode="live",
            realtime_start=latest.get("realtime_start"),
        ),
        "revision_state": macro_revision_state(
            observation_date=latest.get("date"),
            realtime_start=latest.get("realtime_start"),
            realtime_end=latest.get("realtime_end"),
            acquisition_mode="live",
        ),
        "release_semantics_state": macro_release_semantics_state(
            acquisition_mode="live",
            realtime_start=latest.get("realtime_start"),
        ),
        "period_clock_class": macro_period_clock_class(normalized),
    }


def fallback(series_id: str) -> dict[str, Any]:
    normalized = _normalize_series_id(series_id)
    return {
        "series_id": normalized,
        "name": _series_name(normalized),
        "value": None,
        "date": None,
        "unit": _series_unit(normalized),
        "error": "macro_unavailable",
        "truth_envelope": build_macro_truth_envelope(
            series_id=normalized,
            observation_date=None,
            source_authority="macro",
            acquisition_mode="fallback",
            retrieved_at_utc=_now().isoformat(),
            release_date=None,
            availability_date=None,
            realtime_start=None,
            realtime_end=None,
            vintage_class="unavailable",
            revision_state="unavailable",
            release_semantics_state="unavailable",
            degradation_reason="macro_unavailable",
        ),
        "period_clock_class": macro_period_clock_class(normalized),
        "provider_execution": build_provider_execution(
            provider_name=None,
            source_family="macro",
            identifier=normalized,
            path_used="fallback",
            live_or_cache="fallback",
            usable_truth=False,
            semantic_grade="unavailable",
            freshness_class="unavailable",
            insufficiency_reason="macro_unavailable",
            data_mode="unavailable",
            authority_level="unavailable",
        ),
    }


def fetch(series_id: str, *, surface_name: str | None = None) -> dict[str, Any]:
    normalized = _normalize_series_id(series_id)
    if not normalized:
        return fallback(series_id)
    payload = _series_from_fred(normalized) or _series_from_csv(normalized)
    if payload is None:
        record_runtime_truth(
            source_id=_SOURCE_ID,
            source_family="macro",
            field_name="series_value",
            symbol_or_entity=normalized,
            provider_used=None,
            path_used="fallback",
            live_or_cache="fallback",
            usable_truth=False,
            freshness="unavailable",
            insufficiency_reason="macro_unavailable",
            semantic_grade="unavailable",
            investor_surface=surface_name,
            attempt_succeeded=False,
        )
        return fallback(normalized)
    acquisition_mode = "live" if str(payload.get("source_ref") or "").startswith("fred_api:") else "cached"
    payload["truth_envelope"] = build_macro_truth_envelope(
        series_id=normalized,
        observation_date=str(payload.get("date") or "").strip() or None,
        source_authority=str(payload.get("source_ref") or "").strip() or "macro",
        acquisition_mode=acquisition_mode,
        retrieved_at_utc=str(payload.get("retrieved_utc") or "").strip() or None,
        release_date=str(payload.get("release_date") or "").strip() or None,
        availability_date=str(payload.get("availability_date") or "").strip() or None,
        realtime_start=str(payload.get("realtime_start") or "").strip() or None,
        realtime_end=str(payload.get("realtime_end") or "").strip() or None,
        vintage_class=str(payload.get("vintage_class") or "").strip() or None,
        revision_state=str(payload.get("revision_state") or "").strip() or None,
        release_semantics_state=str(payload.get("release_semantics_state") or "").strip() or None,
        degradation_reason=None,
    )
    payload["provider_execution"] = build_provider_execution(
        provider_name="fred" if acquisition_mode == "live" else "fred_cache",
        source_family="macro",
        identifier=normalized,
        observed_at=str(payload.get("date") or "").strip() or None,
        fetched_at=str(payload.get("retrieved_utc") or "").strip() or None,
        cache_status="hit" if acquisition_mode == "cached" else None,
        path_used="fred_live" if acquisition_mode == "live" else "csv_cache",
        live_or_cache=acquisition_mode,
        usable_truth=payload.get("value") is not None,
        semantic_grade="release_aware" if payload.get("value") is not None else "unavailable",
        freshness_class="current" if acquisition_mode == "live" else "cached",
        sufficiency_state="value_and_reference_period" if payload.get("value") is not None else "insufficient",
        data_mode="live" if acquisition_mode == "live" else "cache",
        authority_level="direct" if payload.get("value") is not None else "unavailable",
    )
    record_runtime_truth(
        source_id=_SOURCE_ID,
        source_family="macro",
        field_name="series_value",
        symbol_or_entity=normalized,
        provider_used="fred" if acquisition_mode == "live" else "fred_cache",
        path_used="fred_live" if acquisition_mode == "live" else "csv_cache",
        live_or_cache=acquisition_mode,
        usable_truth=payload.get("value") is not None,
        freshness="current" if acquisition_mode == "live" else "cached",
        insufficiency_reason=None,
        semantic_grade="release_aware",
        investor_surface=surface_name,
        attempt_succeeded=True,
    )
    return payload


def fetch_all(*, surface_name: str | None = None) -> list[dict[str, Any]]:
    rows = [fetch(series_id, surface_name=surface_name) for series_id in _SERIES_METADATA]
    return [row for row in rows if row.get("value") is not None]


def _freshness_class(age_seconds: int | None, *, monthly: bool) -> FreshnessClass:
    if age_seconds is None:
        return FreshnessClass.EXECUTION_FAILED_OR_INCOMPLETE

    if monthly:
        if age_seconds <= 45 * 24 * 60 * 60:
            return FreshnessClass.FRESH_FULL_REBUILD
        if age_seconds <= 75 * 24 * 60 * 60:
            return FreshnessClass.FRESH_PARTIAL_REBUILD
        if age_seconds <= 120 * 24 * 60 * 60:
            return FreshnessClass.STORED_VALID_CONTEXT
        return FreshnessClass.DEGRADED_MONITORING_MODE

    if age_seconds <= 24 * 60 * 60:
        return FreshnessClass.FRESH_FULL_REBUILD
    if age_seconds <= 7 * 24 * 60 * 60:
        return FreshnessClass.FRESH_PARTIAL_REBUILD
    if age_seconds <= 30 * 24 * 60 * 60:
        return FreshnessClass.STORED_VALID_CONTEXT
    return FreshnessClass.DEGRADED_MONITORING_MODE


def freshness_state() -> FreshnessState:
    rows = fetch_all()
    best_anchor: datetime | None = None
    best_class = FreshnessClass.EXECUTION_FAILED_OR_INCOMPLETE

    for row in rows:
        anchor = coerce_datetime(row.get("retrieved_utc") or row.get("date"))
        if anchor is None:
            continue
        age_seconds = max(0, int((_now() - anchor).total_seconds()))
        monthly = _normalize_series_id(row.get("series_id") or "") in _MONTHLY_SERIES
        freshness_class = _freshness_class(age_seconds, monthly=monthly)
        if best_anchor is None or anchor > best_anchor:
            best_anchor = anchor
            best_class = freshness_class

    if best_anchor is None:
        return FreshnessState(
            source_id=_SOURCE_ID,
            freshness_class=FreshnessClass.EXECUTION_FAILED_OR_INCOMPLETE,
            last_updated_utc=None,
            staleness_seconds=None,
        )

    return FreshnessState(
        source_id=_SOURCE_ID,
        freshness_class=best_class,
        last_updated_utc=best_anchor.astimezone(UTC).isoformat(),
        staleness_seconds=max(0, int((_now() - best_anchor).total_seconds())),
    )


class MacroAdapter:
    source_id = _SOURCE_ID
    tier = source_tier

    def fetch(self, series_id: str, *, surface_name: str | None = None) -> dict[str, Any]:
        return fetch(series_id, surface_name=surface_name)

    def fetch_all(self, *, surface_name: str | None = None) -> list[dict[str, Any]]:
        return fetch_all(surface_name=surface_name)

    def fallback(self, series_id: str) -> dict[str, Any]:
        return fallback(series_id)

    def freshness_state(self) -> FreshnessState:
        return freshness_state()


register_source(_SOURCE_ID, adapter=__import__(__name__, fromlist=["fetch"]))
