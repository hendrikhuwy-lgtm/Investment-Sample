from __future__ import annotations

import logging
import os
import sqlite3
import uuid
from typing import Any

from app.config import get_db_path
from app.services.provider_refresh import fetch_routed_family
from app.v2.core.domain_objects import utc_now_iso
from app.v2.core.change_ledger import record_change
from app.v2.forecasting.adapters.common import ForecastAdapterError
from app.v2.forecasting.capabilities import ForecastBundle, ForecastRequest, ScenarioSupport, TriggerSupport
from app.v2.forecasting.degraded import (
    degraded_evaluation,
    degraded_forecast_result,
    degraded_scenario_support,
    degraded_support_summary,
    degraded_trigger_support,
)
from app.v2.forecasting.evaluation import evaluate_result, summarize_support
from app.v2.forecasting.normalizer import ForecastNormalizationError, extract_series_timestamps, extract_series_values, normalize_forecast_payload
from app.v2.forecasting.registry import benchmark_providers, configured_providers, provider_available, provider_sequence_for_request
from app.v2.forecasting.runtime_manager import forecast_runtime_status
from app.v2.forecasting.store import (
    latest_scenario_support,
    latest_trigger_states,
    list_evaluations,
    list_latest_runs,
    list_provider_probes,
    persist_forecast_bundle,
)
from app.v2.forecasting.validation import probe_provider, probe_providers, readiness_rows
from app.v2.sources.runtime_truth import record_runtime_truth


logger = logging.getLogger(__name__)


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_history_from_payload(payload: dict[str, Any]) -> tuple[list[float], list[str]]:
    series = payload.get("series") or []
    return extract_series_values(series), extract_series_timestamps(series)


def _fetch_symbol_history(*, symbol: str, surface_name: str) -> tuple[list[float], list[str], str | None]:
    with _connection() as conn:
        payload = fetch_routed_family(
            conn,
            surface_name="blueprint" if surface_name in {"candidate_report", "compare", "blueprint_explorer"} else surface_name,
            endpoint_family="ohlcv_history",
            identifier=symbol,
            triggered_by_job="forecast_support",
            force_refresh=False,
        )
    history, timestamps = _extract_history_from_payload(payload)
    provider_name = str(payload.get("provider_name") or "").strip() or None
    return history, timestamps, provider_name


def build_request(
    *,
    object_type: str,
    object_id: str,
    series_family: str,
    series_id: str,
    horizon: int,
    frequency: str,
    covariates: dict[str, Any] | None = None,
    past_covariates: dict[str, Any] | None = None,
    future_covariates: dict[str, Any] | None = None,
    grouped_context_series: dict[str, Any] | None = None,
    history_target_points: int | None = None,
    regime_context: str | None = None,
    history: list[float] | None = None,
    timestamps: list[str] | None = None,
    surface_name: str,
) -> ForecastRequest:
    request_covariates = dict(covariates or {})
    request_history = list(history or [])
    request_timestamps = list(timestamps or [])
    symbol = str(request_covariates.get("symbol") or "").strip().upper()
    history_target = max(5, int(history_target_points or 5))
    request_covariates.setdefault("history_target_points", history_target)
    if len(request_history) < history_target and symbol and series_family not in {"news"}:
        fetched_history, fetched_timestamps, provider_name = _fetch_symbol_history(symbol=symbol, surface_name=surface_name)
        if fetched_history:
            request_history = fetched_history
            request_timestamps = fetched_timestamps
            request_covariates.setdefault("history_provider", provider_name)
            request_covariates.setdefault("current_value", fetched_history[-1])
    return ForecastRequest(
        request_id=f"forecast_request_{uuid.uuid4().hex}",
        object_type=object_type,
        object_id=object_id,
        series_family=series_family,
        series_id=series_id,
        horizon=max(3, int(horizon or 5)),
        frequency=frequency or "daily",
        covariates=request_covariates,
        past_covariates=dict(past_covariates or {}),
        future_covariates=dict(future_covariates or {}),
        grouped_context_series=dict(grouped_context_series or {}),
        regime_context=regime_context,
        requested_at=utc_now_iso(),
        history=request_history,
        timestamps=request_timestamps,
    )


def _provider_candidates(request: ForecastRequest, *, surface_name: str) -> list[str]:
    sequence = [provider for provider in provider_sequence_for_request(request, surface_name=surface_name) if provider_available(provider)]
    readiness_by_provider = {str(row.get("provider") or ""): row for row in readiness_rows()}
    ready = [provider for provider in sequence if bool((readiness_by_provider.get(provider) or {}).get("ready"))]
    return ready or sequence


def _build_scenario_support(
    *,
    request: ForecastRequest,
    result,
    support,
    summary: str,
    implication: str,
    object_label: str,
) -> tuple[ScenarioSupport, list[TriggerSupport]]:
    current_value = _safe_float(request.covariates.get("current_value"))
    path = list(result.point_path or [])
    horizon_value = path[-1] if path else current_value
    first_step = path[0] if path else current_value
    direction = str(result.direction or "mixed")
    movement = 0.0
    if current_value not in {None, 0.0} and horizon_value is not None:
        movement = ((float(horizon_value) - float(current_value)) / abs(float(current_value))) * 100.0
    threshold_state = "breached" if result.anomaly_score is not None and result.anomaly_score >= 0.8 else "watch"
    thresholds = [
        TriggerSupport(
            object_id=request.object_id,
            trigger_type="near_term",
            threshold=f"{float(first_step):.2f}" if first_step is not None else "n/a",
            source_family=request.series_family,
            provider=support.provider,
            current_distance_to_trigger=(
                f"{abs(float(current_value or 0.0) - float(first_step or 0.0)):.2f}" if first_step is not None else "unknown"
            ),
            next_action_if_hit=f"Escalate review if {object_label.lower()} reaches the near-term forecast threshold.",
            next_action_if_broken=f"Reset the brief support if {object_label.lower()} breaks below the near-term band.",
            threshold_state=threshold_state,
            support_strength=support.support_strength,
            confidence_summary=support.confidence_summary,
            degraded_state=support.degraded_state,
            generated_at=support.generated_at,
        ),
        TriggerSupport(
            object_id=request.object_id,
            trigger_type="thesis",
            threshold=f"{float(horizon_value):.2f}" if horizon_value is not None else "n/a",
            source_family=request.series_family,
            provider=support.provider,
            current_distance_to_trigger=(
                f"{abs(float(current_value or 0.0) - float(horizon_value or 0.0)):.2f}" if horizon_value is not None else "unknown"
            ),
            next_action_if_hit=f"Recheck the thesis if {object_label.lower()} reaches the horizon threshold.",
            next_action_if_broken=f"Treat the current directional support as broken if {object_label.lower()} invalidates the horizon path.",
            threshold_state="watch" if support.degraded_state is None else "degraded",
            support_strength=support.support_strength,
            confidence_summary=support.confidence_summary,
            degraded_state=support.degraded_state,
            generated_at=support.generated_at,
        ),
    ]
    bull = {
        "label": "Bull case",
        "summary": f"{object_label} strengthens. {summary}",
        "expected_path": f"{movement + 2.5:.1f}% path bias" if movement else "Supportive path bias",
        "support_type": support.support_strength,
    }
    base = {
        "label": "Base case",
        "summary": implication or summary,
        "expected_path": f"{movement:.1f}% central path" if movement else "Bounded current path",
        "support_type": support.support_strength,
    }
    bear = {
        "label": "Bear case",
        "summary": f"{object_label} weakens against the current read.",
        "expected_path": f"{movement - 3.0:.1f}% downside path" if movement else "Downside break risk",
        "support_type": support.support_strength,
    }
    if direction == "negative":
        bull["expected_path"], bear["expected_path"] = bear["expected_path"], bull["expected_path"]
    scenario = ScenarioSupport(
        object_id=request.object_id,
        provider=support.provider,
        scenario_type="forecast_supported",
        bull_case=bull,
        base_case=base,
        bear_case=bear,
        support_strength=support.support_strength,
        what_confirms=(
            f"{object_label} tracking toward the near-term threshold would confirm the current support path."
            if not support.degraded_state
            else "A live provider and deeper history would confirm this path."
        ),
        what_breaks=(
            f"{object_label} breaking the thesis threshold would weaken the current support path."
            if not support.degraded_state
            else "Current support remains degraded until a live forecast backend is available."
        ),
        monitoring_thresholds=thresholds,
        degraded_state=support.degraded_state,
        generated_at=support.generated_at,
    )
    return scenario, thresholds


def _run_provider(provider: str, request: ForecastRequest):
    result = probe_provider(provider, request=request)
    if not bool(result.get("ready")):
        reason_code = str(result.get("reason_code") or "provider_unavailable")
        logger.warning("Forecast provider failed closed", extra={"provider": provider, "reason_code": reason_code})
        raise ForecastAdapterError(provider, f"{provider} validation failed", reason_code=reason_code)
    normalized = result.get("normalized_result")
    if not isinstance(normalized, dict):
        raise ForecastAdapterError(provider, f"{provider} returned no normalized result", reason_code="wrong_shape")
    model_name = str(normalized.get("model_name") or "unknown")
    return normalized, model_name


def _benchmark_if_enabled(request: ForecastRequest) -> None:
    if os.getenv("IA_FORECAST_BENCHMARK_MODE", "").strip().lower() not in {"1", "true", "yes", "on"}:
        return
    for provider in benchmark_providers():
        try:
            payload, model_name = _run_provider(provider, request)
            result = normalize_forecast_payload(request=request, provider=provider, model_name=model_name, payload=payload)
            evaluation = evaluate_result(request, result)
            _ = persist_forecast_bundle(
                bundle=ForecastBundle(
                    request=request,
                    result=result,
                    support=summarize_support(result, request),
                    scenario_support=ScenarioSupport(
                        object_id=request.object_id,
                        provider=provider,
                        scenario_type="benchmark",
                        bull_case={"label": "Bull case", "summary": "Benchmark mode only", "expected_path": "", "support_type": "benchmark"},
                        base_case={"label": "Base case", "summary": "Benchmark mode only", "expected_path": "", "support_type": "benchmark"},
                        bear_case={"label": "Bear case", "summary": "Benchmark mode only", "expected_path": "", "support_type": "benchmark"},
                        support_strength="benchmark",
                        what_confirms="Benchmark mode only.",
                        what_breaks="Benchmark mode only.",
                        monitoring_thresholds=[],
                        degraded_state=None,
                        generated_at=result.generated_at,
                    ),
                    trigger_support=[],
                    evaluation=evaluation,
                ),
                surface_name="benchmark",
                candidate_id=None,
                object_label=request.object_id,
                support_class="weak_support",
            )
        except Exception:
            continue


_STRENGTH_ORDER = {"weak": 0, "moderate": 1, "strong": 2}


def _emit_material_change_events(
    *,
    previous_scenario: dict[str, Any] | None,
    previous_triggers: list[dict[str, Any]],
    bundle: ForecastBundle,
    surface_name: str,
    candidate_id: str | None,
    object_label: str,
) -> None:
    current_strength = bundle.support.support_strength
    current_degraded = bundle.support.degraded_state
    previous_strength = str((previous_scenario or {}).get("support_strength") or "")
    previous_degraded = str((previous_scenario or {}).get("degraded_reason") or "")

    if previous_scenario is not None and previous_strength:
        if _STRENGTH_ORDER.get(current_strength, 0) > _STRENGTH_ORDER.get(previous_strength, 0) or (previous_degraded and not current_degraded):
            record_change(
                event_type="forecast_support_strengthened",
                surface_id=surface_name,
                summary=f"Forecast support strengthened for {object_label}.",
                candidate_id=candidate_id,
                change_trigger="forecast_support_strengthened",
                previous_state=previous_strength or previous_degraded or None,
                current_state=current_strength,
                implication_summary=bundle.scenario_support.what_confirms,
                next_action="Keep the current recommendation logic unchanged, but treat scenario support as stronger.",
                report_tab="scenarios",
                impact_level="medium",
                requires_review=False,
                deep_link_target={"target_type": "candidate_report" if candidate_id else surface_name, "target_id": candidate_id or bundle.request.object_id, "tab": "scenarios"},
            )
        elif _STRENGTH_ORDER.get(current_strength, 0) < _STRENGTH_ORDER.get(previous_strength, 0) or (current_degraded and not previous_degraded):
            record_change(
                event_type="forecast_support_weakened",
                surface_id=surface_name,
                summary=f"Forecast support weakened for {object_label}.",
                candidate_id=candidate_id,
                change_trigger="forecast_support_weakened",
                previous_state=previous_strength or previous_degraded or None,
                current_state=current_strength if not current_degraded else str(current_degraded),
                implication_summary=bundle.scenario_support.what_breaks,
                next_action="Recheck the scenario layer and thresholds before escalating posture.",
                report_tab="scenarios",
                impact_level="medium",
                requires_review=True,
                deep_link_target={"target_type": "candidate_report" if candidate_id else surface_name, "target_id": candidate_id or bundle.request.object_id, "tab": "scenarios"},
            )

    previous_by_type = {str(item.get("trigger_type") or ""): item for item in previous_triggers}
    for trigger in bundle.trigger_support:
        previous = previous_by_type.get(trigger.trigger_type)
        previous_state = str((previous or {}).get("threshold_state") or "")
        if previous_state and previous_state != trigger.threshold_state and trigger.threshold_state in {"breached", "watch"}:
            record_change(
                event_type="forecast_trigger_threshold_crossed",
                surface_id=surface_name,
                summary=f"{object_label} changed trigger state on {trigger.trigger_type.replace('_', ' ')} support.",
                candidate_id=candidate_id,
                change_trigger="forecast_trigger_threshold_crossed",
                previous_state=previous_state,
                current_state=trigger.threshold_state,
                implication_summary=trigger.next_action_if_hit if trigger.threshold_state == "breached" else trigger.next_action_if_broken,
                next_action=trigger.next_action_if_hit if trigger.threshold_state == "breached" else trigger.next_action_if_broken,
                report_tab="scenarios",
                impact_level="medium" if trigger.threshold_state == "breached" else "low",
                requires_review=trigger.threshold_state == "breached",
                deep_link_target={"target_type": "candidate_report" if candidate_id else surface_name, "target_id": candidate_id or bundle.request.object_id, "tab": "scenarios"},
            )

    previous_anomaly = _safe_float((previous_scenario or {}).get("anomaly_score"))
    current_anomaly = bundle.result.anomaly_score
    if current_anomaly is not None:
        if (previous_anomaly or 0.0) < 0.8 <= current_anomaly:
            record_change(
                event_type="forecast_anomaly_opened",
                surface_id=surface_name,
                summary=f"Forecast anomaly flag opened for {object_label}.",
                candidate_id=candidate_id,
                change_trigger="forecast_anomaly_opened",
                previous_state=str(previous_anomaly) if previous_anomaly is not None else None,
                current_state=f"{current_anomaly:.2f}",
                implication_summary="Scenario support now includes an anomaly warning.",
                next_action="Keep investor-facing action boundaries unchanged, but inspect the scenario support.",
                report_tab="scenarios",
                impact_level="medium",
                requires_review=False,
                deep_link_target={"target_type": "candidate_report" if candidate_id else surface_name, "target_id": candidate_id or bundle.request.object_id, "tab": "scenarios"},
            )
        elif previous_anomaly is not None and previous_anomaly >= 0.8 and current_anomaly < 0.8:
            record_change(
                event_type="forecast_anomaly_resolved",
                surface_id=surface_name,
                summary=f"Forecast anomaly flag resolved for {object_label}.",
                candidate_id=candidate_id,
                change_trigger="forecast_anomaly_resolved",
                previous_state=f"{previous_anomaly:.2f}",
                current_state=f"{current_anomaly:.2f}",
                implication_summary="Scenario support no longer carries an active anomaly warning.",
                next_action="Treat this as support normalization only, not recommendation authority.",
                report_tab="scenarios",
                impact_level="low",
                requires_review=False,
                deep_link_target={"target_type": "candidate_report" if candidate_id else surface_name, "target_id": candidate_id or bundle.request.object_id, "tab": "scenarios"},
            )


def build_forecast_bundle(
    *,
    request: ForecastRequest,
    surface_name: str,
    candidate_id: str | None,
    object_label: str,
    summary: str,
    implication: str,
    support_class: str = "proxy_support",
) -> ForecastBundle:
    previous_scenario = latest_scenario_support(request.object_id)
    previous_triggers = latest_trigger_states(request.object_id)
    if request.frequency not in {"daily", "weekly", "monthly", "snapshot"}:
        result = degraded_forecast_result(request, reason_code="unsupported_frequency")
        support = degraded_support_summary(result)
        thresholds = [
            degraded_trigger_support(
                object_id=request.object_id,
                provider=support.provider,
                source_family=request.series_family,
                reason_code="unsupported_frequency",
                label=object_label,
            )
        ]
        scenario = degraded_scenario_support(
            object_id=request.object_id,
            provider=support.provider,
            reason_code="unsupported_frequency",
            summary=summary,
            implication=implication,
            thresholds=thresholds,
        )
        bundle = ForecastBundle(
            request=request,
            result=result,
            support=support,
            scenario_support=scenario,
            trigger_support=thresholds,
            evaluation=degraded_evaluation(result),
        )
        persisted = persist_forecast_bundle(bundle=bundle, surface_name=surface_name, candidate_id=candidate_id, object_label=object_label, support_class=support_class)
        _emit_material_change_events(
            previous_scenario=previous_scenario,
            previous_triggers=previous_triggers,
            bundle=persisted,
            surface_name=surface_name,
            candidate_id=candidate_id,
            object_label=object_label,
        )
        return persisted

    provider_candidates = _provider_candidates(request, surface_name=surface_name)
    result = None
    last_reason_code: str | None = None
    for provider in provider_candidates:
        try:
            payload, model_name = _run_provider(provider, request)
            normalized = normalize_forecast_payload(request=request, provider=provider, model_name=model_name, payload=payload)
            result = normalized
            record_runtime_truth(
                source_id=f"forecast_provider:{provider}",
                source_family="forecast_readiness",
                field_name="forecast_bundle",
                symbol_or_entity=str(request.series_id),
                provider_used=provider,
                path_used="forecast_bundle",
                live_or_cache="live",
                usable_truth=True,
                freshness="current",
                insufficiency_reason=None,
                semantic_grade="investor_facing_forecast",
                investor_surface=surface_name,
                attempt_succeeded=True,
            )
            logger.info("Forecast provider succeeded", extra={"provider": provider, "surface_name": surface_name})
            break
        except ForecastAdapterError as exc:
            last_reason_code = exc.reason_code
            record_runtime_truth(
                source_id=f"forecast_provider:{provider}",
                source_family="forecast_readiness",
                field_name="forecast_bundle",
                symbol_or_entity=str(request.series_id),
                provider_used=provider,
                path_used="forecast_bundle",
                live_or_cache="live",
                usable_truth=False,
                freshness="unavailable",
                insufficiency_reason=exc.reason_code,
                semantic_grade="forecast_failed",
                investor_surface=surface_name,
                attempt_succeeded=False,
            )
            logger.warning("Forecast provider failed", extra={"provider": provider, "reason_code": exc.reason_code, "surface_name": surface_name})
            continue
        except ForecastNormalizationError as exc:
            last_reason_code = exc.reason_code
            logger.warning("Forecast provider normalization failed", extra={"provider": provider, "reason_code": exc.reason_code, "surface_name": surface_name})
            continue
        except Exception:
            last_reason_code = "evaluation_failed"
            logger.exception("Forecast provider evaluation failed", extra={"provider": provider, "surface_name": surface_name})
            continue

    if result is None:
        reason_code = "insufficient_series_history" if len(request.history) < 5 else (last_reason_code or "provider_unavailable")
        result = degraded_forecast_result(request, reason_code=reason_code)

    support = summarize_support(result, request) if result.degraded_state is None else degraded_support_summary(result)
    if result.degraded_state is None:
        scenario, thresholds = _build_scenario_support(
            request=request,
            result=result,
            support=support,
            summary=summary,
            implication=implication,
            object_label=object_label,
        )
        evaluation = evaluate_result(request, result)
    else:
        thresholds = [
            degraded_trigger_support(
                object_id=request.object_id,
                provider=support.provider,
                source_family=request.series_family,
                reason_code=str(result.degraded_state or "provider_unavailable"),
                label=object_label,
            )
        ]
        scenario = degraded_scenario_support(
            object_id=request.object_id,
            provider=support.provider,
            reason_code=str(result.degraded_state or "provider_unavailable"),
            summary=summary,
            implication=implication,
            thresholds=thresholds,
        )
        evaluation = degraded_evaluation(result)

    bundle = ForecastBundle(
        request=request,
        result=result,
        support=support,
        scenario_support=scenario,
        trigger_support=thresholds,
        evaluation=evaluation,
    )
    persisted = persist_forecast_bundle(
        bundle=bundle,
        surface_name=surface_name,
        candidate_id=candidate_id,
        object_label=object_label,
        support_class=support_class,
    )
    _emit_material_change_events(
        previous_scenario=previous_scenario,
        previous_triggers=previous_triggers,
        bundle=persisted,
        surface_name=surface_name,
        candidate_id=candidate_id,
        object_label=object_label,
    )
    _benchmark_if_enabled(request)
    return persisted


def forecast_admin_payload() -> dict[str, Any]:
    from app.v2.forecasting.registry import capability_matrix

    readiness = readiness_rows()
    readiness_by_provider = {str(row.get("provider")): row for row in readiness}
    capabilities = []
    for row in capability_matrix():
        provider = str(row.get("provider") or "")
        ready = bool((readiness_by_provider.get(provider) or {}).get("ready")) if provider != "deterministic_baseline" else True
        enriched = dict(row)
        enriched["ready"] = ready
        if not ready and provider != "deterministic_baseline":
                enriched["reason_code"] = str((readiness_by_provider.get(provider) or {}).get("reason_code") or enriched.get("reason_code") or "provider_unavailable")
        capabilities.append(enriched)
    ready_providers = [row["provider"] for row in readiness if bool(row.get("ready"))]
    configured = configured_providers(include_benchmarks=True)
    return {
        "generated_at": utc_now_iso(),
        "capabilities": capabilities,
        "declared_providers": configured,
        "configured_providers": configured,
        "ready_providers": ready_providers,
        "external_support_active": bool(ready_providers),
        "serving_mode": "external_plus_fallback" if ready_providers else "deterministic_only",
        "readiness": readiness,
        "runtime": forecast_runtime_status(),
        "latest_runs": list_latest_runs(limit=24),
        "latest_probes": list_provider_probes(limit=24),
        "recent_evaluations": list_evaluations(limit=24),
    }


def forecast_readiness_payload() -> dict[str, Any]:
    providers = readiness_rows()
    ready_providers = [row["provider"] for row in providers if bool(row.get("ready"))]
    return {
        "generated_at": utc_now_iso(),
        "providers": providers,
        "ready_count": len(ready_providers),
        "ready_providers": ready_providers,
        "external_support_active": bool(ready_providers),
        "serving_mode": "external_plus_fallback" if ready_providers else "deterministic_only",
        "runtime": forecast_runtime_status(),
    }


def forecast_probe_payload(*, provider: str | None = None) -> dict[str, Any]:
    rows = probe_providers(provider=provider)
    return {
        "generated_at": utc_now_iso(),
        "count": len(rows),
        "ready_count": sum(1 for row in rows if bool(row.get("ready"))),
        "providers": rows,
        "runtime": forecast_runtime_status(),
    }
