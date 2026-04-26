from __future__ import annotations

from typing import Any

from app.v2.blueprint_market.exchange_calendar_service import future_session_timestamps


def _value_available(value: Any) -> bool:
    return value not in {None, ""}


def _liquidity_feature_mode(rows: list[dict[str, Any]]) -> tuple[str, bool, bool]:
    volume_available = all(_value_available(item.get("volume")) for item in rows)
    amount_available = all(_value_available(item.get("amount")) for item in rows)
    if volume_available and amount_available:
        return "full_liquidity", True, True
    return "price_only", volume_available, amount_available


def build_kronos_input(
    *,
    identity: dict[str, Any] | None,
    rows: list[dict[str, Any]],
    horizon: int,
    interval: str = "1day",
    max_context: int = 400,
    min_history_bars: int = 260,
) -> dict[str, Any]:
    if identity is None:
        return {"supported": False, "failure_class": "symbol_mapping_failed", "reason": "No candidate market identity was resolved."}
    if str(identity.get("forecast_eligibility") or "") != "eligible":
        return {"supported": False, "failure_class": "symbol_mapping_failed", "reason": "Resolved identity is not forecast eligible."}
    if str(identity.get("primary_interval") or interval) != interval:
        return {"supported": False, "failure_class": "insufficient_history", "reason": "Only daily interval is supported in V1."}
    if len(rows) < min_history_bars:
        return {"supported": False, "failure_class": "insufficient_history", "reason": "Not enough stored history for Kronos input."}
    trimmed = rows[-max_context:]
    last_timestamp = str(trimmed[-1].get("timestamp_utc") or "")
    if not last_timestamp:
        return {"supported": False, "failure_class": "quality_degraded", "reason": "Stored series has no usable terminal timestamp."}
    output_timestamps = future_session_timestamps(
        last_timestamp=last_timestamp,
        horizon=horizon,
        identity=identity,
    )
    if not output_timestamps:
        return {
            "supported": False,
            "failure_class": "invalid_calendar",
            "reason": "Exchange session routing could not be resolved for Kronos input.",
        }
    try:
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        return {"supported": False, "failure_class": "model_execution_failed", "reason": f"Pandas is unavailable: {exc}"}
    liquidity_feature_mode, volume_available, amount_available = _liquidity_feature_mode(trimmed)
    dataframe = pd.DataFrame(
        [
            {
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"]),
                "volume": (
                    float(item["volume"])
                    if liquidity_feature_mode == "full_liquidity"
                    else 0.0
                ),
                "amount": (
                    float(item["amount"])
                    if liquidity_feature_mode == "full_liquidity"
                    else 0.0
                ),
            }
            for item in trimmed
        ]
    )
    input_timestamps = [str(item["timestamp_utc"]) for item in trimmed]
    x_timestamp = pd.Series(pd.to_datetime(input_timestamps, utc=True))
    y_timestamp = pd.Series(pd.to_datetime(output_timestamps, utc=True))
    return {
        "supported": True,
        "failure_class": None,
        "reason": None,
        "df": dataframe,
        "x_timestamp": x_timestamp,
        "y_timestamp": y_timestamp,
        "rows": trimmed,
        "input_timestamps": input_timestamps,
        "output_timestamps": output_timestamps,
        "truncation_applied": len(rows) > max_context,
        "liquidity_feature_mode": liquidity_feature_mode,
        "volume_available": volume_available,
        "amount_available": amount_available,
    }
