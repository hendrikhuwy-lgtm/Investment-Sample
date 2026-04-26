from __future__ import annotations

from typing import Any

from app.v2.core.domain_objects import EvidenceCitation, EvidencePack, MarketDataPoint, MarketSeriesTruth, utc_now_iso
from app.v2.sources.registry import get_freshness_registry
from app.v2.truth.envelopes import build_market_truth_envelope


def _as_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(raw: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _as_float(raw.get(key))
        if value is not None:
            return value
    return None


def _current_value(raw: dict[str, Any]) -> float | None:
    return _first_float(raw, "price", "close", "c", "value")


def _one_day_change_pct(raw: dict[str, Any], current_value: float | None) -> float | None:
    direct_value = _first_float(
        raw,
        "change_pct_1d",
        "change_percent",
        "pct_change_1d",
        "daily_change_pct",
        "changePercent",
        "dp",
        "percent_change",
    )
    if direct_value is not None:
        return direct_value

    previous_close = _first_float(raw, "previous_close", "previousClose", "pc")
    if previous_close not in {None, 0} and current_value is not None:
        return ((current_value - previous_close) / abs(previous_close)) * 100.0

    absolute_change = _first_float(raw, "absolute_change", "change", "d")
    if current_value is not None and absolute_change is not None and current_value != absolute_change:
        prior_value = current_value - absolute_change
        if prior_value not in {None, 0}:
            return (absolute_change / abs(prior_value)) * 100.0

    open_value = _first_float(raw, "open", "o")
    close_value = _first_float(raw, "close", "c")
    reference_close = close_value if close_value is not None else current_value
    if open_value is None or reference_close is None or open_value == 0:
        return None
    return ((reference_close - open_value) / abs(open_value)) * 100.0


def _freshness_payload(source_id: str) -> dict[str, Any]:
    freshness = get_freshness_registry().get_freshness(source_id)
    return {
        "source_id": freshness.source_id,
        "freshness_class": freshness.freshness_class.value,
        "last_updated_utc": freshness.last_updated_utc,
        "staleness_seconds": freshness.staleness_seconds,
    }


def _points(raw: dict[str, Any], current_value: float | None, as_of: str) -> list[MarketDataPoint]:
    points: list[MarketDataPoint] = []
    previous_close = _first_float(raw, "previous_close", "previousClose", "pc")
    if previous_close is not None and current_value is not None:
        points.append(MarketDataPoint(at=as_of, value=previous_close))
        points.append(MarketDataPoint(at=as_of, value=current_value))
        return points

    open_value = _first_float(raw, "open", "o")
    close_value = _first_float(raw, "close", "c")

    if open_value is not None and close_value is not None:
        points.append(MarketDataPoint(at=as_of, value=open_value))
        points.append(MarketDataPoint(at=as_of, value=close_value))
        return points

    if current_value is not None:
        points.append(MarketDataPoint(at=as_of, value=current_value))
    return points


def translate(provider_data: Any) -> MarketSeriesTruth:
    """Translate a market price adapter payload into MarketSeriesTruth."""
    raw = dict(provider_data or {})
    ticker = str(raw.get("ticker") or "").strip()
    source_lookup_id = "market_price"
    source_id = "market_price_adapter"
    current_value = _current_value(raw)
    one_day_change_pct = _one_day_change_pct(raw, current_value)
    provider_execution = dict(raw.get("provider_execution") or {})
    freshness_state = _freshness_payload(source_lookup_id)
    as_of = (
        str(raw.get("as_of_utc") or freshness_state.get("last_updated_utc") or "").strip()
        or utc_now_iso()
    )
    truth_envelope = dict(raw.get("truth_envelope") or {}) or build_market_truth_envelope(
        identifier=ticker,
        as_of_utc=as_of,
        provider_name=str(raw.get("provider_name") or "").strip() or "market_price",
        acquisition_mode="live" if str(raw.get("provider_name") or "").strip() else "cached",
        degradation_reason=str(raw.get("error") or raw.get("error_state") or "").strip() or None,
        exchange=str(raw.get("exchange") or "").strip() or None,
        asset_class=str(raw.get("asset_class") or "").strip() or None,
        retrieved_at_utc=as_of,
    )

    evidence = EvidencePack(
        evidence_id=f"evidence_market_{ticker.lower() or 'unknown'}",
        thesis=f"{ticker or 'Unknown'} market price snapshot",
        summary="Translated market price payload for V2 market-series truth.",
        freshness=str(freshness_state["freshness_class"]),
        citations=[
            EvidenceCitation(
                source_id=source_id,
                label="Market price adapter",
                note=str(raw.get("provider_name") or "").strip() or None,
            )
        ],
        facts={
            "ticker": ticker or None,
            "current_value": current_value,
            "change_pct_1d": one_day_change_pct,
            "one_day_change_pct": one_day_change_pct,
            "one_week_change_pct": None,
            "regime_label": None,
            "currency": str(raw.get("currency") or "").strip() or None,
            "freshness_state": freshness_state,
            "source_id": source_id,
            "movement_state": (
                "proxy"
                if str(provider_execution.get("authority_level") or "") in {"derived", "proxy"}
                else "known"
                if one_day_change_pct is not None
                else "input_constrained"
                if current_value is not None
                else "unavailable"
            ),
            "retrieval_path": str(raw.get("retrieval_path") or "").strip() or None,
            "source_family": str(provider_execution.get("source_family") or "quote_latest"),
            "source_provider": str(provider_execution.get("provider_name") or raw.get("provider_name") or "").strip() or None,
            "provider_execution": provider_execution or None,
            "usable_truth": provider_execution.get("usable_truth"),
            "sufficiency_state": provider_execution.get("sufficiency_state"),
            "data_mode": provider_execution.get("data_mode"),
            "authority_level": provider_execution.get("authority_level"),
            "semantic_grade": provider_execution.get("semantic_grade"),
            "truth_envelope": truth_envelope,
        },
        observed_at=as_of,
    )

    return MarketSeriesTruth(
        series_id=f"market:{ticker or 'unknown'}",
        label=ticker or "Unknown market series",
        frequency="daily",
        units=str(raw.get("currency") or "").strip() or "price",
        points=_points(raw, current_value, as_of),
        evidence=[evidence],
        as_of=as_of,
    )
