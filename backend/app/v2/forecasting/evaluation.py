from __future__ import annotations

from app.v2.core.domain_objects import utc_now_iso
from app.v2.forecasting.capabilities import ForecastEvaluation, ForecastRequest, ForecastResult, ForecastSupportSummary


def _request_context_depth(request: ForecastRequest | None) -> float:
    if request is None:
        return 0.0
    scenario_depth = str((request.covariates or {}).get("scenario_depth") or "").strip().lower()
    grouped_count = sum(len(list(value or [])) for value in dict(request.grouped_context_series or {}).values())
    past_count = len(dict(request.past_covariates or {}))
    future_count = len(dict(request.future_covariates or {}))
    if scenario_depth == "significant":
        depth = (min(grouped_count, 10) * 0.08) + (min(past_count, 10) * 0.04) + (min(future_count, 10) * 0.03)
        return round(min(depth, 0.42), 4)
    depth = (min(grouped_count, 6) * 0.06) + (min(past_count, 6) * 0.03) + (min(future_count, 6) * 0.02)
    return round(min(depth, 0.28), 4)


def support_strength_for_result(result: ForecastResult, request: ForecastRequest | None = None) -> str:
    context_depth = _request_context_depth(request)
    if result.degraded_state:
        return "weak"
    if len(result.point_path) >= max(3, min(result.horizon, 5)) and result.confidence_band in {"tight", "moderate"}:
        return "strong"
    if len(result.point_path) >= max(3, min(result.horizon, 5)) and context_depth >= 0.14 and result.confidence_band in {"wide", "bounded"}:
        return "moderate"
    if len(result.point_path) >= 2:
        return "moderate"
    return "weak"


def confidence_summary_for_result(result: ForecastResult) -> str:
    if result.degraded_state:
        return "Deterministic fallback only"
    mapping = {
        "tight": "Tight interval support",
        "moderate": "Moderate interval support",
        "wide": "Wide interval support",
        "bounded": "Bounded directional support",
    }
    summary = mapping.get(result.confidence_band, "Bounded directional support")
    if result.anomaly_score is not None:
        summary = f"{summary} · anomaly {result.anomaly_score:.2f}"
    return summary


def evaluate_result(request: ForecastRequest, result: ForecastResult) -> ForecastEvaluation:
    context_depth = _request_context_depth(request)
    score = 0.0
    if result.point_path:
        score += 0.35
    if len(result.point_path) >= max(3, min(request.horizon, 5)):
        score += 0.2
    if result.quantiles:
        score += 0.2
    if result.anomaly_score is not None:
        score += 0.1
    score += context_depth
    if not result.degraded_state:
        score += 0.15
    if result.degraded_state:
        score = max(0.0, score - 0.25)
    return ForecastEvaluation(
        provider=result.provider,
        model_name=result.model_name,
        series_family=request.series_family,
        horizon=request.horizon,
        metric_name="usefulness",
        metric_value=round(min(1.0, max(0.0, score)), 4),
        measured_at=utc_now_iso(),
        notes=list(result.notes),
    )


def summarize_support(result: ForecastResult, request: ForecastRequest | None = None) -> ForecastSupportSummary:
    support_strength = support_strength_for_result(result, request)
    return ForecastSupportSummary(
        provider=result.provider,
        model_name=result.model_name,
        horizon=result.horizon,
        support_strength=support_strength,
        confidence_summary=confidence_summary_for_result(result),
        degraded_state=result.degraded_state,
        generated_at=result.generated_at,
        uncertainty_width_label=result.confidence_band,
        scenario_support_strength=support_strength,
    )
