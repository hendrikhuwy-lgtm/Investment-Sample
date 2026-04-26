from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.v2.core.domain_objects import utc_now_iso
from app.v2.forecasting.capabilities import ForecastRequest, ForecastResult


class ForecastNormalizationError(ValueError):
    def __init__(self, message: str, *, reason_code: str = "wrong_shape") -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _timestamp_key(item: dict[str, Any]) -> str:
    for key in ("timestamp", "datetime", "date", "ds", "time", "t"):
        value = item.get(key)
        if value not in {None, ""}:
            return str(value)
    return ""


def _series_points(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    rows = [row for row in value if isinstance(row, dict)]
    if not rows:
        return []

    def _sort_key(row: dict[str, Any]) -> float:
        key = _timestamp_key(row)
        if not key:
            return 0.0
        try:
            if key.isdigit():
                return float(key)
            return datetime.fromisoformat(key.replace("Z", "+00:00")).astimezone(UTC).timestamp()
        except Exception:
            return 0.0

    return sorted(rows, key=_sort_key)


def extract_series_values(series: Any) -> list[float]:
    values: list[float] = []
    if isinstance(series, list):
        if series and isinstance(series[0], dict):
            for row in _series_points(series):
                for key in ("close", "c", "value", "y"):
                    number = _safe_float(row.get(key))
                    if number is not None:
                        values.append(number)
                        break
        else:
            values = [number for number in (_safe_float(item) for item in series) if number is not None]
    return values


def extract_series_timestamps(series: Any) -> list[str]:
    return [_timestamp_key(row) for row in _series_points(series)]


def _normalize_quantiles(value: Any) -> dict[str, list[float]]:
    if isinstance(value, dict):
        normalized: dict[str, list[float]] = {}
        for key, row in value.items():
            numbers = extract_series_values(row if isinstance(row, list) else list(row.values()) if isinstance(row, dict) else [row])
            if numbers:
                normalized[str(key)] = numbers
            elif row not in (None, "", []):
                raise ForecastNormalizationError("Quantile rows must be numeric", reason_code="wrong_shape")
        return normalized
    if isinstance(value, list):
        normalized = {}
        for row in value:
            if not isinstance(row, dict):
                raise ForecastNormalizationError("Quantiles must be dict rows", reason_code="wrong_shape")
            quantile = str(row.get("quantile") or row.get("q") or row.get("name") or "").strip()
            numbers = extract_series_values(row.get("values") or row.get("path") or row.get("series") or [])
            if quantile and numbers:
                normalized[quantile] = numbers
            elif quantile:
                raise ForecastNormalizationError("Quantile paths must be numeric", reason_code="wrong_shape")
        return normalized
    return {}


def _direction(current_value: float | None, point_path: list[float]) -> str:
    if not point_path:
        return "mixed"
    reference = current_value if current_value is not None else point_path[0]
    final_value = point_path[-1]
    if final_value > reference:
        return "positive"
    if final_value < reference:
        return "negative"
    return "mixed"


def _confidence_band(point_path: list[float], quantiles: dict[str, list[float]]) -> str:
    if point_path and "0.9" in quantiles and "0.1" in quantiles:
        upper = quantiles["0.9"][-1]
        lower = quantiles["0.1"][-1]
        mid = abs(point_path[-1]) if point_path[-1] not in {0.0, -0.0} else 1.0
        spread = abs(upper - lower) / max(mid, 1.0)
        if spread <= 0.05:
            return "tight"
        if spread <= 0.15:
            return "moderate"
        return "wide"
    if len(point_path) >= 2:
        move = abs(point_path[-1] - point_path[0]) / max(abs(point_path[0]), 1.0)
        if move <= 0.03:
            return "bounded"
        if move <= 0.08:
            return "moderate"
        return "wide"
    return "bounded"


def normalize_forecast_payload(
    *,
    request: ForecastRequest,
    provider: str,
    model_name: str,
    payload: dict[str, Any],
) -> ForecastResult:
    if not isinstance(payload, dict):
        raise ForecastNormalizationError("Forecast payload must be a JSON object", reason_code="wrong_shape")
    raw_path = (
        payload.get("point_path")
        or payload.get("forecast")
        or payload.get("predictions")
        or payload.get("values")
        or payload.get("series")
        or payload.get("path")
    )
    if raw_path in (None, "", []):
        raise ForecastNormalizationError("Forecast path missing", reason_code="wrong_shape")
    point_path = extract_series_values(raw_path)
    if not point_path:
        raise ForecastNormalizationError("Forecast path must contain numeric values", reason_code="wrong_shape")
    if len(point_path) < int(request.horizon):
        raise ForecastNormalizationError("Forecast horizon shorter than requested horizon", reason_code="horizon_mismatch")
    quantiles = _normalize_quantiles(payload.get("quantiles") or payload.get("prediction_intervals") or payload.get("intervals") or {})
    generated_at = str(payload.get("generated_at") or payload.get("created_at") or utc_now_iso())
    current_value = _safe_float(payload.get("current_value") or request.covariates.get("current_value"))
    anomaly_score = _safe_float(payload.get("anomaly_score") or payload.get("anomaly") or payload.get("anomaly_probability"))
    notes = [str(item) for item in list(payload.get("notes") or []) if str(item).strip()]
    if payload.get("quantiles") is not None or payload.get("prediction_intervals") is not None or payload.get("intervals") is not None:
        for quantile, values in quantiles.items():
            if len(values) < int(request.horizon):
                raise ForecastNormalizationError(
                    f"Quantile {quantile} shorter than requested horizon",
                    reason_code="horizon_mismatch",
                )

    return ForecastResult(
        request_id=request.request_id,
        provider=provider,
        model_name=model_name,
        horizon=request.horizon,
        point_path=point_path[: request.horizon] if point_path else [],
        quantiles={key: values[: request.horizon] for key, values in quantiles.items()},
        direction=_direction(current_value, point_path),
        confidence_band=_confidence_band(point_path, quantiles),
        anomaly_score=anomaly_score,
        covariate_usage=sorted(str(key) for key in request.covariates.keys()),
        generated_at=generated_at,
        freshness_state="fresh_full_rebuild",
        degraded_state=str(payload.get("degraded_state") or "").strip() or None,
        notes=notes,
    )
