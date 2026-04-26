from __future__ import annotations

import logging
from typing import Any

from app.v2.core.domain_objects import utc_now_iso
from app.v2.forecasting.adapters.common import (
    ForecastAdapterError,
    build_forecast_payload,
    probe_json_endpoint,
    request_json,
)
from app.v2.forecasting.capabilities import ForecastRequest, ForecastResult
from app.v2.forecasting.normalizer import ForecastNormalizationError, normalize_forecast_payload
from app.v2.forecasting.registry import adapter_for, all_providers, provider_meta
from app.v2.forecasting.store import latest_provider_probe, list_provider_probes, record_provider_probe
from app.v2.sources.runtime_truth import record_runtime_truth


logger = logging.getLogger(__name__)


def build_probe_request(*, provider: str = "probe", horizon: int = 5) -> ForecastRequest:
    return ForecastRequest(
        request_id=f"forecast_probe_request_{provider}",
        object_type="market_series",
        object_id=f"probe_{provider}",
        series_family="benchmark_proxy",
        series_id="ACWI",
        horizon=max(3, int(horizon or 5)),
        frequency="daily",
        covariates={"symbol": "ACWI", "current_value": 104.8},
        regime_context="probe_validation",
        requested_at=utc_now_iso(),
        history=[100.0, 100.8, 101.4, 102.1, 102.9, 103.4, 104.1, 104.8],
        timestamps=[
            "2026-03-25T00:00:00+00:00",
            "2026-03-26T00:00:00+00:00",
            "2026-03-27T00:00:00+00:00",
            "2026-03-28T00:00:00+00:00",
            "2026-03-29T00:00:00+00:00",
            "2026-03-30T00:00:00+00:00",
            "2026-03-31T00:00:00+00:00",
            "2026-04-01T00:00:00+00:00",
        ],
    )


def _probe_health(provider: str, adapter: Any) -> dict[str, Any]:
    health = probe_json_endpoint(
        provider=provider,
        base_url=adapter.base_url(),
        paths=adapter.health_paths(),
        api_key=adapter.api_key(),
        timeout_seconds=min(adapter.timeout_seconds(), 5),
    )
    persisted = record_provider_probe(
        provider=provider,
        base_url=str(health.get("base_url") or adapter.base_url()),
        endpoint=str(health.get("endpoint") or "/health"),
        success=bool(health.get("success")),
        http_status=health.get("http_status"),
        latency_ms=health.get("latency_ms"),
        json_ok=health.get("json_ok"),
        shape_ok=health.get("shape_ok"),
        horizon_ok=health.get("horizon_ok"),
        error_code=health.get("error_code"),
        error_message=health.get("error_message"),
    )
    record_runtime_truth(
        source_id=f"forecast_provider:{provider}",
        source_family="forecast_readiness",
        field_name="health",
        symbol_or_entity=provider,
        provider_used=provider,
        path_used=str(health.get("endpoint") or "/health"),
        live_or_cache="live",
        usable_truth=bool(health.get("success")),
        freshness="current" if health.get("success") else "unavailable",
        insufficiency_reason=str(health.get("error_code") or "") or None,
        semantic_grade="health_ready" if health.get("success") else "health_failed",
        attempt_succeeded=bool(health.get("success")),
    )
    return persisted


def _normalize_runtime_result(
    *,
    provider: str,
    model_name: str,
    request: ForecastRequest,
    payload: dict[str, Any],
) -> ForecastResult:
    try:
        return normalize_forecast_payload(request=request, provider=provider, model_name=model_name, payload=payload)
    except ForecastNormalizationError:
        raise
    except Exception as exc:
        raise ForecastNormalizationError(str(exc), reason_code="wrong_shape") from exc


def _probe_forecast(provider: str, adapter: Any, request: ForecastRequest) -> tuple[dict[str, Any], ForecastResult | None]:
    model_name = adapter.model_name()
    try:
        payload, meta = request_json(
            method="POST",
            provider=provider,
            base_url=adapter.base_url(),
            path=adapter.forecast_path(),
            api_key=adapter.api_key(),
            json_payload=build_forecast_payload(
                request=request,
                model_name=model_name,
                extra_payload=adapter.extra_payload(),
            ),
            timeout_seconds=adapter.timeout_seconds(),
        )
        result = _normalize_runtime_result(provider=provider, model_name=model_name, request=request, payload=payload)
        persisted = record_provider_probe(
            provider=provider,
            base_url=adapter.base_url(),
            endpoint=adapter.forecast_path(),
            success=True,
            http_status=meta.get("http_status"),
            latency_ms=meta.get("latency_ms"),
            json_ok=True,
            shape_ok=True,
            horizon_ok=True,
            error_code=None,
            error_message=None,
        )
        record_runtime_truth(
            source_id=f"forecast_provider:{provider}",
            source_family="forecast_readiness",
            field_name="forecast",
            symbol_or_entity=provider,
            provider_used=provider,
            path_used=adapter.forecast_path(),
            live_or_cache="live",
            usable_truth=True,
            freshness="current",
            insufficiency_reason=None,
            semantic_grade="forecast_ready",
            attempt_succeeded=True,
        )
        return persisted, result
    except ForecastAdapterError as exc:
        persisted = record_provider_probe(
            provider=provider,
            base_url=adapter.base_url(),
            endpoint=exc.endpoint or adapter.forecast_path(),
            success=False,
            http_status=exc.http_status,
            latency_ms=exc.latency_ms,
            json_ok=False if exc.reason_code == "bad_json" else None,
            shape_ok=False if exc.reason_code == "wrong_shape" else None,
            horizon_ok=False if exc.reason_code == "horizon_mismatch" else None,
            error_code=exc.reason_code,
            error_message=str(exc),
        )
        record_runtime_truth(
            source_id=f"forecast_provider:{provider}",
            source_family="forecast_readiness",
            field_name="forecast",
            symbol_or_entity=provider,
            provider_used=provider,
            path_used=exc.endpoint or adapter.forecast_path(),
            live_or_cache="live",
            usable_truth=False,
            freshness="unavailable",
            insufficiency_reason=exc.reason_code,
            semantic_grade="forecast_failed",
            attempt_succeeded=False,
        )
        return persisted, None
    except ForecastNormalizationError as exc:
        horizon_reason = exc.reason_code == "horizon_mismatch"
        persisted = record_provider_probe(
            provider=provider,
            base_url=adapter.base_url(),
            endpoint=adapter.forecast_path(),
            success=False,
            http_status=200,
            latency_ms=None,
            json_ok=True,
            shape_ok=False if not horizon_reason else True,
            horizon_ok=False if horizon_reason else None,
            error_code=exc.reason_code,
            error_message=str(exc),
        )
        record_runtime_truth(
            source_id=f"forecast_provider:{provider}",
            source_family="forecast_readiness",
            field_name="forecast",
            symbol_or_entity=provider,
            provider_used=provider,
            path_used=adapter.forecast_path(),
            live_or_cache="live",
            usable_truth=False,
            freshness="unavailable",
            insufficiency_reason=exc.reason_code,
            semantic_grade="forecast_shape_failed",
            attempt_succeeded=False,
        )
        return persisted, None


def probe_provider(provider: str, *, request: ForecastRequest | None = None) -> dict[str, Any]:
    adapter = adapter_for(provider)
    meta = provider_meta(provider)
    if adapter is None:
        return {
            "provider": provider,
            "configured": False,
            "benchmark_only": bool(meta.get("benchmark_only")),
            "managed": bool(meta.get("managed")),
            "ready": False,
            "reason_code": "provider_unavailable",
            "health": None,
            "forecast": None,
            "normalized_result": None,
        }
    if not adapter.configured():
        probe = record_provider_probe(
            provider=provider,
            base_url=adapter.base_url(),
            endpoint=adapter.forecast_path(),
            success=False,
            http_status=None,
            latency_ms=None,
            json_ok=None,
            shape_ok=None,
            horizon_ok=None,
            error_code="provider_unavailable",
            error_message=f"{provider} base URL missing",
        )
        return {
            "provider": provider,
            "configured": False,
            "benchmark_only": bool(meta.get("benchmark_only")),
            "managed": bool(meta.get("managed")),
            "ready": False,
            "reason_code": probe.get("error_code"),
            "health": None,
            "forecast": probe,
            "normalized_result": None,
        }

    probe_request = request or build_probe_request(provider=provider)
    health = _probe_health(provider, adapter)
    if not bool(health.get("success")):
        logger.warning("Forecast provider health probe failed", extra={"provider": provider, "error_code": health.get("error_code")})
        return {
            "provider": provider,
            "configured": True,
            "benchmark_only": bool(meta.get("benchmark_only")),
            "managed": bool(meta.get("managed")),
            "ready": False,
            "reason_code": health.get("error_code"),
            "health": health,
            "forecast": None,
            "normalized_result": None,
        }

    forecast_probe, normalized = _probe_forecast(provider, adapter, probe_request)
    ready = bool(forecast_probe.get("success")) and normalized is not None
    if ready:
        logger.info("Forecast provider validated", extra={"provider": provider, "endpoint": adapter.forecast_path()})
    else:
        logger.warning(
            "Forecast provider forecast probe failed",
            extra={"provider": provider, "error_code": forecast_probe.get("error_code")},
        )
    return {
        "provider": provider,
        "configured": True,
        "benchmark_only": bool(meta.get("benchmark_only")),
        "managed": bool(meta.get("managed")),
        "ready": ready,
        "reason_code": None if ready else forecast_probe.get("error_code"),
        "health": health,
        "forecast": forecast_probe,
        "normalized_result": None if normalized is None else normalized.to_dict(),
    }


def probe_providers(*, provider: str | None = None) -> list[dict[str, Any]]:
    if provider is not None:
        return [probe_provider(provider)]
    return [probe_provider(name) for name in all_providers(include_benchmarks=True)]


def latest_readiness(provider: str) -> dict[str, Any]:
    adapter = adapter_for(provider)
    health = None
    if adapter is not None:
        for endpoint in adapter.health_paths():
            health = latest_provider_probe(provider, endpoint=endpoint)
            if health is not None:
                break
    health = health or latest_provider_probe(provider, endpoint="/")
    forecast = latest_provider_probe(provider, endpoint=adapter.forecast_path() if adapter is not None else "/forecast")
    meta = provider_meta(provider)
    forecast_endpoint = adapter.forecast_path() if adapter is not None else "/forecast"
    forecast_streak = 0
    for probe in list_provider_probes(provider=provider, limit=6):
        if str(probe.get("endpoint") or "") != forecast_endpoint:
            continue
        if bool(probe.get("success")):
            forecast_streak += 1
            continue
        break
    stability_required = 2 if provider == "timesfm" else 1
    stable_forecast = bool((forecast or {}).get("success")) and forecast_streak >= stability_required
    ready = bool((health or {}).get("success")) and stable_forecast
    reason_code = None
    if not ready:
        if bool((forecast or {}).get("success")) and forecast_streak < stability_required:
            reason_code = "forecast_unstable"
        else:
            reason_code = str((forecast or health or {}).get("error_code") or "provider_unavailable")
    return {
        "provider": provider,
        "configured": bool(adapter and adapter.configured()),
        "benchmark_only": bool(meta.get("benchmark_only")),
        "managed": bool(meta.get("managed")),
        "ready": ready,
        "reason_code": reason_code,
        "health": health,
        "forecast": forecast,
        "forecast_success_streak": forecast_streak,
        "stability_required": stability_required,
    }


def readiness_rows() -> list[dict[str, Any]]:
    return [latest_readiness(provider) for provider in all_providers(include_benchmarks=True)]
