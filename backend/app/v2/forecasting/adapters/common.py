from __future__ import annotations

import os
import time
from typing import Any

import requests

from app.v2.forecasting.capabilities import ForecastRequest


class ForecastAdapterError(RuntimeError):
    def __init__(
        self,
        provider: str,
        message: str,
        *,
        reason_code: str = "provider_unavailable",
        http_status: int | None = None,
        endpoint: str | None = None,
        latency_ms: int | None = None,
    ) -> None:
        super().__init__(message)
        self.provider = provider
        self.reason_code = reason_code
        self.http_status = http_status
        self.endpoint = endpoint
        self.latency_ms = latency_ms


def env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def env_base_url(name: str, default: str) -> str:
    if name in os.environ:
        return os.getenv(name, "").strip()
    return default


def build_headers(api_key: str | None = None) -> dict[str, str]:
    headers = {"User-Agent": "investment-agent-v2/forecasting"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def build_forecast_payload(
    *,
    request: ForecastRequest,
    model_name: str,
    extra_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "request_id": request.request_id,
        "object_type": request.object_type,
        "object_id": request.object_id,
        "series_family": request.series_family,
        "series_id": request.series_id,
        "horizon": request.horizon,
        "frequency": request.frequency,
        "history": request.history,
        "timestamps": request.timestamps,
        "covariates": request.covariates,
        "past_covariates": request.past_covariates,
        "future_covariates": request.future_covariates,
        "grouped_context_series": request.grouped_context_series,
        "regime_context": request.regime_context,
        "model_name": model_name,
    }
    if extra_payload:
        payload.update(extra_payload)
    return payload


def request_json(
    *,
    method: str,
    provider: str,
    base_url: str,
    path: str,
    api_key: str | None = None,
    json_payload: dict[str, Any] | None = None,
    timeout_seconds: int = 20,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not base_url:
        raise ForecastAdapterError(provider, f"{provider} base URL missing", reason_code="provider_unavailable", endpoint=path)
    url = base_url.rstrip("/") + path
    started = time.monotonic()
    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            json=json_payload,
            headers=build_headers(api_key),
            timeout=(5, timeout_seconds),
        )
        status_code = int(response.status_code or 0)
        response.raise_for_status()
    except requests.HTTPError as exc:
        status_code = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0)
        reason_code = "auth_missing" if status_code in {401, 403} else "provider_unavailable"
        raise ForecastAdapterError(
            provider,
            f"{provider} request failed",
            reason_code=reason_code,
            http_status=status_code,
            endpoint=path,
            latency_ms=int((time.monotonic() - started) * 1000),
        ) from exc
    except requests.RequestException as exc:
        raise ForecastAdapterError(
            provider,
            f"{provider} request failed",
            reason_code="provider_unavailable",
            endpoint=path,
            latency_ms=int((time.monotonic() - started) * 1000),
        ) from exc
    try:
        data = response.json()
    except ValueError as exc:
        raise ForecastAdapterError(
            provider,
            f"{provider} returned non-JSON payload",
            reason_code="bad_json",
            http_status=status_code,
            endpoint=path,
            latency_ms=int((time.monotonic() - started) * 1000),
        ) from exc
    if not isinstance(data, dict):
        raise ForecastAdapterError(
            provider,
            f"{provider} returned unsupported payload",
            reason_code="wrong_shape",
            http_status=status_code,
            endpoint=path,
            latency_ms=int((time.monotonic() - started) * 1000),
        )
    return data, {
        "http_status": status_code,
        "endpoint": path,
        "latency_ms": int((time.monotonic() - started) * 1000),
        "json_ok": True,
    }


def probe_json_endpoint(
    *,
    provider: str,
    base_url: str,
    paths: list[str] | tuple[str, ...],
    api_key: str | None = None,
    timeout_seconds: int = 5,
) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    for path in [str(item).strip() or "/" for item in paths]:
        try:
            _payload, meta = request_json(
                method="GET",
                provider=provider,
                base_url=base_url,
                path=path,
                api_key=api_key,
                timeout_seconds=timeout_seconds,
            )
            return {
                "provider": provider,
                "base_url": base_url,
                "endpoint": path,
                "success": True,
                "http_status": meta["http_status"],
                "latency_ms": meta["latency_ms"],
                "json_ok": True,
                "shape_ok": True,
                "horizon_ok": None,
                "error_code": None,
                "error_message": None,
            }
        except ForecastAdapterError as exc:
            failures.append(
                {
                    "provider": provider,
                    "base_url": base_url,
                    "endpoint": exc.endpoint or path,
                    "success": False,
                    "http_status": exc.http_status,
                    "latency_ms": exc.latency_ms,
                    "json_ok": False if exc.reason_code == "bad_json" else None,
                    "shape_ok": False if exc.reason_code == "wrong_shape" else None,
                    "horizon_ok": None,
                    "error_code": exc.reason_code,
                    "error_message": str(exc),
                }
            )
    return failures[-1] if failures else {
        "provider": provider,
        "base_url": base_url,
        "endpoint": "/health",
        "success": False,
        "http_status": None,
        "latency_ms": None,
        "json_ok": False,
        "shape_ok": False,
        "horizon_ok": None,
        "error_code": "provider_unavailable",
        "error_message": f"{provider} probe failed",
    }


def post_forecast_request(
    *,
    provider: str,
    base_url: str,
    path: str,
    request: ForecastRequest,
    model_name: str,
    api_key: str | None = None,
    extra_payload: dict[str, Any] | None = None,
    timeout_seconds: int = 20,
) -> dict[str, Any]:
    payload = build_forecast_payload(request=request, model_name=model_name, extra_payload=extra_payload)
    data, _meta = request_json(
        method="POST",
        provider=provider,
        base_url=base_url,
        path=path,
        api_key=api_key,
        json_payload=payload,
        timeout_seconds=timeout_seconds,
    )
    return data
