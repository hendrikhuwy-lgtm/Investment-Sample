from __future__ import annotations

from typing import Any

from app.v2.core.domain_objects import utc_now_iso
from app.v2.forecasting.capabilities import (
    ForecastEvaluation,
    ForecastRequest,
    ForecastResult,
    ForecastSupportSummary,
    ScenarioSupport,
    TriggerSupport,
)


def _request_context_keys(request: ForecastRequest) -> list[str]:
    keys = [str(key) for key in dict(request.covariates or {}).keys()]
    keys.extend(f"past:{key}" for key in dict(request.past_covariates or {}).keys())
    keys.extend(f"future:{key}" for key in dict(request.future_covariates or {}).keys())
    keys.extend(f"context:{key}" for key in dict(request.grouped_context_series or {}).keys())
    return sorted(keys)


DEGRADED_REASON_CODES = {
    "provider_unavailable",
    "auth_missing",
    "bad_json",
    "wrong_shape",
    "horizon_mismatch",
    "insufficient_series_history",
    "unsupported_frequency",
    "unsupported_covariates",
    "evaluation_failed",
    "support_not_trusted",
}


def degraded_forecast_result(
    request: ForecastRequest,
    *,
    reason_code: str,
    provider: str = "deterministic_baseline",
    model_name: str = "rule_based_support",
    notes: list[str] | None = None,
) -> ForecastResult:
    generated_at = utc_now_iso()
    return ForecastResult(
        request_id=request.request_id,
        provider=provider,
        model_name=model_name,
        horizon=request.horizon,
        point_path=list(request.history[-min(len(request.history), 3) :]) or [0.0] * max(1, request.horizon),
        quantiles={},
        direction="mixed",
        confidence_band="bounded",
        anomaly_score=None,
        covariate_usage=_request_context_keys(request),
        generated_at=generated_at,
        freshness_state="degraded_monitoring_mode",
        degraded_state=reason_code,
        notes=list(notes or [reason_code]),
    )


def degraded_support_summary(result: ForecastResult) -> ForecastSupportSummary:
    return ForecastSupportSummary(
        provider=result.provider,
        model_name=result.model_name,
        horizon=result.horizon,
        support_strength="weak",
        confidence_summary="Deterministic fallback only",
        degraded_state=result.degraded_state,
        generated_at=result.generated_at,
        uncertainty_width_label="bounded",
        scenario_support_strength="weak",
    )


def degraded_trigger_support(
    *,
    object_id: str,
    provider: str,
    source_family: str,
    reason_code: str,
    label: str,
) -> TriggerSupport:
    return TriggerSupport(
        object_id=object_id,
        trigger_type="monitor",
        threshold="No typed threshold available",
        source_family=source_family,
        provider=provider,
        current_distance_to_trigger="unknown",
        next_action_if_hit=f"Escalate review if {label.lower()} moves beyond the current deterministic range.",
        next_action_if_broken=f"Reset the view if {label.lower()} breaks the current bounded range.",
        threshold_state="degraded",
        support_strength="weak",
        confidence_summary="Deterministic fallback only",
        degraded_state=reason_code,
        generated_at=utc_now_iso(),
    )


def degraded_scenario_support(
    *,
    object_id: str,
    provider: str,
    reason_code: str,
    summary: str,
    implication: str,
    thresholds: list[TriggerSupport],
) -> ScenarioSupport:
    generated_at = utc_now_iso()
    bull = {
        "label": "Bull case",
        "summary": summary,
        "expected_path": "A better path is possible, but it still needs validated history and live forecast support before it can carry conviction.",
        "support_type": "weak",
    }
    base = {
        "label": "Base case",
        "summary": implication or summary,
        "expected_path": "The monitored path stays conditional because forecast support is running in degraded mode.",
        "support_type": "weak",
    }
    bear = {
        "label": "Bear case",
        "summary": summary,
        "expected_path": "A weaker path cannot be ranked confidently yet, so keep the downside case bounded and conditional.",
        "support_type": "weak",
    }
    return ScenarioSupport(
        object_id=object_id,
        provider=provider,
        scenario_type="degraded",
        bull_case=bull,
        base_case=base,
        bear_case=bear,
        support_strength="weak",
        what_confirms="A longer validated series history or a live forecast backend would confirm this support path.",
        what_breaks="Missing history, missing auth, or unsupported covariates keep this support path degraded.",
        monitoring_thresholds=thresholds,
        degraded_state=reason_code,
        generated_at=generated_at,
    )


def degraded_evaluation(result: ForecastResult) -> ForecastEvaluation:
    return ForecastEvaluation(
        provider=result.provider,
        model_name=result.model_name,
        series_family="degraded",
        horizon=result.horizon,
        metric_name="usefulness",
        metric_value=0.0,
        measured_at=result.generated_at,
        notes=[result.degraded_state or "degraded"],
    )


def summarize_degraded_support(support: ForecastSupportSummary) -> str:
    reason = str(support.degraded_state or "provider_unavailable").replace("_", " ")
    return f"{support.provider} fallback · {reason} · {support.horizon}d"


def degraded_evidence_item(
    *,
    candidate_id: str,
    object_type: str,
    object_id: str,
    object_label: str,
    support: ForecastSupportSummary,
    scenario_support: ScenarioSupport,
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "object_type": object_type,
        "object_id": object_id,
        "object_label": object_label,
        "provider": support.provider,
        "model_name": support.model_name,
        "support_strength": support.support_strength,
        "freshness_state": "degraded_monitoring_mode",
        "degraded_state": support.degraded_state,
        "evidence_label": f"Forecast support for {object_label}",
        "support_class": "weak_support",
        "summary": scenario_support.base_case.get("summary") or "Forecast support is degraded and bounded.",
        "created_at": support.generated_at,
    }
