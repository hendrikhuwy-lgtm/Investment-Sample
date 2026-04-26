from __future__ import annotations

import os

from app.v2.forecasting.adapters.common import ForecastAdapterError, env_base_url, post_forecast_request
from app.v2.forecasting.capabilities import ForecastRequest


PROVIDER = "timesfm"


def configured() -> bool:
    return bool(base_url())


def base_url() -> str:
    return env_base_url("IA_TIMESFM_BASE_URL", "http://127.0.0.1:8003")


def forecast_path() -> str:
    return os.getenv("IA_TIMESFM_FORECAST_PATH", "/forecast").strip() or "/forecast"


def health_paths() -> tuple[str, ...]:
    return (os.getenv("IA_TIMESFM_HEALTH_PATH", "/health").strip() or "/health", "/")


def api_key() -> str | None:
    value = os.getenv("IA_TIMESFM_API_KEY", "").strip()
    return value or None


def timeout_seconds() -> int:
    return int(os.getenv("IA_TIMESFM_TIMEOUT_SECONDS", "20"))


def extra_payload() -> dict[str, object]:
    return {"task": "forecast"}


def model_name() -> str:
    return os.getenv("IA_TIMESFM_MODEL", "timesfm")


def forecast(request: ForecastRequest) -> dict[str, object]:
    if len(request.history) < 5:
        raise ForecastAdapterError(PROVIDER, "TimesFM needs longer history", reason_code="insufficient_series_history")
    return post_forecast_request(
        provider=PROVIDER,
        base_url=base_url(),
        path=forecast_path(),
        request=request,
        model_name=model_name(),
        api_key=api_key(),
        extra_payload=extra_payload(),
        timeout_seconds=timeout_seconds(),
    )
