from __future__ import annotations

import csv
from datetime import UTC, date, datetime
from io import StringIO
from pathlib import Path
from typing import Any

import requests

from app.config import get_repo_root
from app.v2.core.domain_objects import EvidenceCitation, EvidencePack, MarketDataPoint, MarketSeriesTruth
from app.v2.sources.execution_envelope import build_provider_execution
from app.v2.truth.envelopes import build_macro_truth_envelope


_USER_AGENT = "investment-agent-v2/0.1"
_CACHE_DIRS = (
    get_repo_root() / "backend" / "outbox" / "live_cache",
    get_repo_root() / "outbox" / "live_cache",
)
_FRED_BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"


def _now() -> datetime:
    return datetime.now(UTC)


def _as_float(value: Any) -> float | None:
    try:
        if value in {None, "", "."}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_date(value: str | None) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _iso_day(value: date | None) -> str | None:
    if value is None:
        return None
    return f"{value.isoformat()}T00:00:00+00:00"


def _cache_path(series_id: str) -> Path | None:
    filename = f"{str(series_id or '').strip().upper()}.csv"
    for directory in _CACHE_DIRS:
        path = directory / filename
        if path.exists():
            return path
    return None


def _rows_from_csv_text(series_id: str, csv_text: str) -> list[tuple[date, float]]:
    normalized = str(series_id or "").strip().upper()
    rows: list[tuple[date, float]] = []
    reader = csv.DictReader(StringIO(csv_text))
    for row in reader:
        if not isinstance(row, dict):
            continue
        observed_at = _parse_date(row.get("DATE") or row.get("observation_date"))
        value = _as_float(row.get(normalized))
        if observed_at is None or value is None:
            continue
        rows.append((observed_at, value))
    rows.sort(key=lambda item: item[0])
    return rows


def _load_series_rows(series_id: str) -> tuple[list[tuple[date, float]], str, str]:
    normalized = str(series_id or "").strip().upper()
    try:
        response = requests.get(
            _FRED_BASE_URL,
            params={"id": normalized},
            headers={"User-Agent": _USER_AGENT},
            timeout=6,
        )
        response.raise_for_status()
        rows = _rows_from_csv_text(normalized, response.text)
        if rows:
            return rows, "fred_public", "live"
    except Exception:
        pass

    cache_path = _cache_path(normalized)
    if cache_path is None:
        raise RuntimeError(f"public_fred_series_unavailable:{normalized}")
    rows = _rows_from_csv_text(normalized, cache_path.read_text(encoding="utf-8", errors="replace"))
    if not rows:
        raise RuntimeError(f"public_fred_series_empty:{normalized}")
    return rows, "fred_cache", "cached"


def _transform_rows(rows: list[tuple[date, float]], *, transform: str) -> list[tuple[date, float]]:
    if transform == "identity":
        return list(rows)
    if transform == "cpi_yoy":
        by_month = {(observed_at.year, observed_at.month): value for observed_at, value in rows}
        transformed: list[tuple[date, float]] = []
        for observed_at, value in rows:
            previous_value = by_month.get((observed_at.year - 1, observed_at.month))
            if previous_value == 0:
                continue
            if previous_value is None:
                continue
            transformed.append((observed_at, ((value / previous_value) - 1.0) * 100.0))
        return transformed
    raise ValueError(f"unsupported_market_state_transform:{transform}")


def _freshness_class(observed_at: date | None, *, cadence: str) -> str:
    if observed_at is None:
        return "execution_failed_or_incomplete"
    age_days = max(0, (_now().date() - observed_at).days)
    normalized = str(cadence or "daily").strip().lower()
    if normalized == "monthly":
        if age_days <= 7:
            return "fresh_full_rebuild"
        if age_days <= 31:
            return "fresh_partial_rebuild"
        if age_days <= 75:
            return "stored_valid_context"
        return "degraded_monitoring_mode"
    if normalized == "weekly":
        if age_days <= 2:
            return "fresh_full_rebuild"
        if age_days <= 10:
            return "fresh_partial_rebuild"
        if age_days <= 24:
            return "stored_valid_context"
        return "degraded_monitoring_mode"
    if age_days <= 1:
        return "fresh_full_rebuild"
    if age_days <= 3:
        return "fresh_partial_rebuild"
    if age_days <= 7:
        return "stored_valid_context"
    return "degraded_monitoring_mode"


def load_public_fred_market_state_truth(
    *,
    symbol: str,
    label: str,
    series_id: str,
    transform: str = "identity",
    units: str = "percent",
    cadence: str = "daily",
) -> MarketSeriesTruth:
    rows, provider_name, acquisition_mode = _load_series_rows(series_id)
    transformed_rows = _transform_rows(rows, transform=transform)
    if len(transformed_rows) < 2:
        raise RuntimeError(f"insufficient_market_state_history:{series_id}")

    previous_observed_at, previous_value = transformed_rows[-2]
    current_observed_at, current_value = transformed_rows[-1]
    observed_at_iso = _iso_day(current_observed_at)
    retrieved_at_iso = _now().isoformat()
    freshness = "fresh_full_rebuild" if acquisition_mode == "live" else _freshness_class(current_observed_at, cadence=cadence)
    change_pct = None
    if previous_value not in {None, 0.0}:
        change_pct = ((current_value - previous_value) / abs(previous_value)) * 100.0

    truth_envelope = build_macro_truth_envelope(
        series_id=str(series_id or "").strip().upper(),
        observation_date=current_observed_at.isoformat(),
        source_authority=provider_name,
        acquisition_mode="live" if acquisition_mode == "live" else "cached",
        retrieved_at_utc=retrieved_at_iso,
        release_semantics_state="fred_latest" if acquisition_mode == "live" else "cache_fallback_inferred",
        vintage_class="current_release" if acquisition_mode == "live" else "cached_snapshot",
        revision_state="latest_observation" if acquisition_mode == "live" else "cache_fallback",
    )
    freshness_payload = {
        "source_id": f"market_state:{symbol.lower()}",
        "freshness_class": freshness,
        "last_updated_utc": retrieved_at_iso,
        "staleness_seconds": max(0, int((_now().date() - current_observed_at).days * 24 * 60 * 60)),
    }
    provider_execution = build_provider_execution(
        provider_name=provider_name,
        source_family="macro_market_state",
        identifier=str(symbol or "").strip().upper(),
        provider_symbol=str(series_id or "").strip().upper(),
        observed_at=observed_at_iso,
        fetched_at=retrieved_at_iso,
        cache_status="hit" if acquisition_mode == "cached" else None,
        path_used="fred_public_live" if acquisition_mode == "live" else "fred_csv_cache",
        live_or_cache="live" if acquisition_mode == "live" else "cache",
        usable_truth=True,
        semantic_grade="movement_capable",
        sufficiency_state="movement_capable",
        data_mode="live" if acquisition_mode == "live" else "cache",
        authority_level="direct",
        freshness_class=freshness,
        provenance_strength="official_macro_reference",
    )

    evidence = EvidencePack(
        evidence_id=f"evidence_market_state_{str(symbol or '').strip().lower()}",
        thesis=f"{label} market-state reference",
        summary="Normalized public FRED series for the Daily Brief market-state strip.",
        freshness=freshness,
        citations=[
            EvidenceCitation(
                source_id=f"fred_{str(series_id or '').strip().lower()}",
                label="FRED public series",
                url=f"{_FRED_BASE_URL}?id={str(series_id or '').strip().upper()}",
            )
        ],
        facts={
            "ticker": str(symbol or "").strip().upper(),
            "current_value": current_value,
            "change_pct_1d": change_pct,
            "one_day_change_pct": change_pct,
            "currency": None,
            "freshness_state": freshness_payload,
            "source_id": f"market_state:{str(series_id or '').strip().lower()}",
            "movement_state": "known" if change_pct is not None else "input_constrained",
            "retrieval_path": "direct_live" if acquisition_mode == "live" else "routed_cache",
            "source_family": "macro_market_state",
            "source_provider": provider_name,
            "provider_execution": provider_execution,
            "usable_truth": True,
            "sufficiency_state": "movement_capable",
            "data_mode": "live" if acquisition_mode == "live" else "cache",
            "authority_level": "direct",
            "semantic_grade": "official_macro_reference",
            "truth_envelope": truth_envelope,
        },
        observed_at=observed_at_iso or retrieved_at_iso,
    )
    return MarketSeriesTruth(
        series_id=f"market_state:{str(symbol or '').strip().lower()}",
        label=str(symbol or "").strip().upper(),
        frequency=str(cadence or "daily"),
        units=units,
        points=[
            MarketDataPoint(at=_iso_day(previous_observed_at) or observed_at_iso or retrieved_at_iso, value=previous_value),
            MarketDataPoint(at=observed_at_iso or retrieved_at_iso, value=current_value),
        ],
        evidence=[evidence],
        as_of=observed_at_iso or retrieved_at_iso,
    )
