from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

import requests

from app.config import get_repo_root

_USER_AGENT = "investment-agent-v2/0.1"
_FRED_BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
_COINBASE_CANDLES_URL = "https://api.exchange.coinbase.com/products/{product_id}/candles"
_CACHE_DIRS = (
    get_repo_root() / "backend" / "outbox" / "live_cache",
    get_repo_root() / "outbox" / "live_cache",
)


@dataclass(frozen=True, slots=True)
class MarketAuthorityProbeResult:
    primary_provider: str | None
    cross_check_provider: str | None
    cross_check_status: str
    authority_gap_reason: str | None
    cross_check_value: float | None = None
    cross_check_as_of: str | None = None
    cross_check_authority_tier: str | None = None


@dataclass(frozen=True, slots=True)
class _ProbeSpec:
    kind: str
    cross_check_provider: str | None = None
    cross_check_authority_tier: str | None = None
    authority_gap_reason: str | None = None
    series_id: str | None = None
    product_id: str | None = None
    tolerance_abs: float | None = None
    tolerance_pct: float | None = None


_PROBE_SPECS: dict[str, _ProbeSpec] = {
    "^VIX": _ProbeSpec(
        kind="fred_series",
        cross_check_provider="fred_public",
        cross_check_authority_tier="public_benchmark_mirror",
        series_id="VIXCLS",
        tolerance_abs=0.05,
    ),
    "BTC-USD": _ProbeSpec(
        kind="coinbase_candles",
        cross_check_provider="coinbase_exchange",
        cross_check_authority_tier="venue_reference_close",
        product_id="BTC-USD",
        tolerance_pct=0.75,
    ),
    "CL=F": _ProbeSpec(
        kind="unavailable",
        authority_gap_reason="licensed_settlement_cross_check_unavailable",
    ),
    "BZ=F": _ProbeSpec(
        kind="unavailable",
        authority_gap_reason="licensed_settlement_cross_check_unavailable",
    ),
}


def _as_float(value: Any) -> float | None:
    try:
        if value in {None, "", "."}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_date(text: str | None) -> date | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _cache_path(series_id: str) -> Path | None:
    filename = f"{str(series_id or '').strip().upper()}.csv"
    for directory in _CACHE_DIRS:
        path = directory / filename
        if path.exists():
            return path
    return None


def _fred_rows(series_id: str) -> list[tuple[date, float]]:
    normalized = str(series_id or "").strip().upper()

    def _parse_csv(text: str) -> list[tuple[date, float]]:
        rows: list[tuple[date, float]] = []
        reader = csv.DictReader(StringIO(text))
        for row in reader:
            observed_at = _parse_date(row.get("DATE") or row.get("observation_date"))
            value = _as_float(row.get(normalized))
            if observed_at is None or value is None:
                continue
            rows.append((observed_at, value))
        rows.sort(key=lambda item: item[0])
        return rows

    try:
        response = requests.get(
            _FRED_BASE_URL,
            params={"id": normalized},
            headers={"User-Agent": _USER_AGENT},
            timeout=6,
        )
        response.raise_for_status()
        rows = _parse_csv(response.text)
        if rows:
            return rows
    except Exception:
        pass

    cache_path = _cache_path(normalized)
    if cache_path is None:
        return []
    return _parse_csv(cache_path.read_text(encoding="utf-8", errors="replace"))


def _fred_latest_on_or_before(series_id: str, *, as_of: date) -> tuple[float | None, date | None]:
    rows = _fred_rows(series_id)
    eligible = [(observed_at, value) for observed_at, value in rows if observed_at <= as_of]
    if not eligible:
        return None, None
    observed_at, value = eligible[-1]
    return value, observed_at


def _coinbase_latest_on_or_before(product_id: str, *, as_of: date) -> tuple[float | None, date | None]:
    start = datetime(as_of.year, as_of.month, as_of.day, tzinfo=UTC) - timedelta(days=7)
    end = datetime(as_of.year, as_of.month, as_of.day, tzinfo=UTC) + timedelta(days=1)
    try:
        response = requests.get(
            _COINBASE_CANDLES_URL.format(product_id=product_id),
            params={
                "granularity": 86400,
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
            },
            headers={"User-Agent": _USER_AGENT},
            timeout=6,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None, None
    if not isinstance(payload, list):
        return None, None
    rows: list[tuple[date, float]] = []
    for row in payload:
        if not isinstance(row, list) or len(row) < 5:
            continue
        try:
            observed_at = datetime.fromtimestamp(float(row[0]), tz=UTC).date()
        except Exception:
            continue
        close_value = _as_float(row[4])
        if close_value is None or observed_at > as_of:
            continue
        rows.append((observed_at, close_value))
    rows.sort(key=lambda item: item[0])
    if not rows:
        return None, None
    observed_at, value = rows[-1]
    return value, observed_at


def _matches_tolerance(
    primary_value: float,
    cross_check_value: float,
    *,
    tolerance_abs: float | None,
    tolerance_pct: float | None,
) -> bool:
    diff = abs(primary_value - cross_check_value)
    if tolerance_abs is not None and diff <= float(tolerance_abs):
        return True
    if tolerance_pct is not None and cross_check_value not in {0.0, -0.0}:
        pct_diff = (diff / abs(cross_check_value)) * 100.0
        if pct_diff <= float(tolerance_pct):
            return True
    return False


def evaluate_market_authority_probe(
    symbol: str,
    *,
    primary_provider: str | None,
    primary_value: float | None,
    as_of: date | None,
) -> MarketAuthorityProbeResult:
    normalized = str(symbol or "").strip().upper()
    spec = _PROBE_SPECS.get(normalized)
    if spec is None:
        return MarketAuthorityProbeResult(
            primary_provider=primary_provider,
            cross_check_provider=None,
            cross_check_status="validated_by_primary_only",
            authority_gap_reason=None,
        )
    if spec.kind == "unavailable":
        return MarketAuthorityProbeResult(
            primary_provider=primary_provider,
            cross_check_provider=spec.cross_check_provider,
            cross_check_status="validated_by_primary_only",
            authority_gap_reason=spec.authority_gap_reason,
            cross_check_authority_tier=spec.cross_check_authority_tier,
        )
    if primary_value is None or as_of is None:
        return MarketAuthorityProbeResult(
            primary_provider=primary_provider,
            cross_check_provider=spec.cross_check_provider,
            cross_check_status="validated_by_primary_only",
            authority_gap_reason="primary_value_or_as_of_missing",
            cross_check_authority_tier=spec.cross_check_authority_tier,
        )

    cross_check_value: float | None = None
    cross_check_date: date | None = None
    if spec.kind == "fred_series" and spec.series_id:
        cross_check_value, cross_check_date = _fred_latest_on_or_before(spec.series_id, as_of=as_of)
    elif spec.kind == "coinbase_candles" and spec.product_id:
        cross_check_value, cross_check_date = _coinbase_latest_on_or_before(spec.product_id, as_of=as_of)

    if cross_check_value is None or cross_check_date is None:
        return MarketAuthorityProbeResult(
            primary_provider=primary_provider,
            cross_check_provider=spec.cross_check_provider,
            cross_check_status="validated_by_primary_only",
            authority_gap_reason="cross_check_source_unavailable",
            cross_check_authority_tier=spec.cross_check_authority_tier,
        )
    if cross_check_date != as_of:
        return MarketAuthorityProbeResult(
            primary_provider=primary_provider,
            cross_check_provider=spec.cross_check_provider,
            cross_check_status="validated_by_primary_only",
            authority_gap_reason=f"cross_check_as_of_mismatch:{cross_check_date.isoformat()}",
            cross_check_value=cross_check_value,
            cross_check_as_of=cross_check_date.isoformat(),
            cross_check_authority_tier=spec.cross_check_authority_tier,
        )
    if _matches_tolerance(
        primary_value,
        cross_check_value,
        tolerance_abs=spec.tolerance_abs,
        tolerance_pct=spec.tolerance_pct,
    ):
        return MarketAuthorityProbeResult(
            primary_provider=primary_provider,
            cross_check_provider=spec.cross_check_provider,
            cross_check_status="cross_checked",
            authority_gap_reason=None,
            cross_check_value=cross_check_value,
            cross_check_as_of=cross_check_date.isoformat(),
            cross_check_authority_tier=spec.cross_check_authority_tier,
        )
    return MarketAuthorityProbeResult(
        primary_provider=primary_provider,
        cross_check_provider=spec.cross_check_provider,
        cross_check_status="authority_mismatch",
        authority_gap_reason="cross_check_value_mismatch",
        cross_check_value=cross_check_value,
        cross_check_as_of=cross_check_date.isoformat(),
        cross_check_authority_tier=spec.cross_check_authority_tier,
    )


def summarize_market_authority_probe(probe: MarketAuthorityProbeResult) -> str | None:
    provider = str(probe.cross_check_provider or "").replace("_", " ").strip() or None
    if probe.cross_check_status == "cross_checked" and provider:
        value_text = f"{probe.cross_check_value:.2f}" if probe.cross_check_value is not None else "matched"
        as_of_text = probe.cross_check_as_of or "same date"
        return f"Cross-check {provider} matched ({value_text}) on {as_of_text}."
    if probe.cross_check_status == "authority_mismatch" and provider:
        value_text = f"{probe.cross_check_value:.2f}" if probe.cross_check_value is not None else "unavailable"
        as_of_text = probe.cross_check_as_of or "same date"
        return f"Authority mismatch versus {provider} ({value_text} on {as_of_text})."
    if probe.authority_gap_reason:
        return f"Cross-check unavailable: {probe.authority_gap_reason.replace('_', ' ')}."
    if provider:
        return f"Validated by primary only; {provider} cross-check not available."
    return "Validated by primary only."
