from __future__ import annotations

from datetime import UTC, datetime
import math
from typing import Any


def _parse_timestamp(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt).replace(tzinfo=UTC)
            return parsed.isoformat()
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    return parsed.isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def normalize_twelvedata_series(
    *,
    payload: dict[str, Any],
    identity: dict[str, Any],
    ingest_run_id: str,
    series_quality_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    items = list(payload.get("series") or payload.get("values") or [])
    normalized: list[dict[str, Any]] = []
    for entry in reversed(items):
        if not isinstance(entry, dict):
            continue
        timestamp_utc = _parse_timestamp(entry.get("datetime"))
        open_value = _safe_float(entry.get("open"))
        high_value = _safe_float(entry.get("high"))
        low_value = _safe_float(entry.get("low"))
        close_value = _safe_float(entry.get("close"))
        volume = _safe_float(entry.get("volume"))
        amount = _safe_float(entry.get("amount"))
        quality_flags: list[str] = []
        if timestamp_utc is None:
            continue
        if None in {open_value, high_value, low_value, close_value}:
            quality_flags.append("missing_ohlc")
            continue
        if close_value is not None and close_value <= 0:
            quality_flags.append("non_positive_close")
        if high_value is not None and low_value is not None and high_value < low_value:
            quality_flags.append("high_below_low")
        if volume is None:
            quality_flags.append("volume_missing")
        normalized.append(
            {
                "candidate_id": identity["candidate_id"],
                "instrument_id": identity["instrument_id"],
                "series_role": identity["series_role"],
                "timestamp_utc": timestamp_utc,
                "interval": str(identity.get("primary_interval") or "1day"),
                "open": open_value,
                "high": high_value,
                "low": low_value,
                "close": close_value,
                "volume": volume,
                "amount": amount,
                "provider": "twelve_data",
                "provider_symbol": identity["provider_symbol"],
                "adjusted_flag": str(identity.get("adjustment_mode") or "") == "adjusted",
                "freshness_ts": payload.get("observed_at") or timestamp_utc,
                "quality_flags": sorted(set(quality_flags)),
                "series_quality_summary": dict(series_quality_summary),
                "ingest_run_id": ingest_run_id,
            }
        )
    return normalized
