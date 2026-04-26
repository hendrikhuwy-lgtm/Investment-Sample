from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class ForecastRequest:
    request_id: str
    object_type: str
    object_id: str
    series_family: str
    series_id: str
    horizon: int
    frequency: str
    covariates: dict[str, Any] = field(default_factory=dict)
    past_covariates: dict[str, Any] = field(default_factory=dict)
    future_covariates: dict[str, Any] = field(default_factory=dict)
    grouped_context_series: dict[str, Any] = field(default_factory=dict)
    regime_context: str | None = None
    requested_at: str = ""
    history: list[float] = field(default_factory=list)
    timestamps: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ForecastResult:
    request_id: str
    provider: str
    model_name: str
    horizon: int
    point_path: list[float]
    quantiles: dict[str, list[float]]
    direction: str
    confidence_band: str
    anomaly_score: float | None
    covariate_usage: list[str]
    generated_at: str
    freshness_state: str
    degraded_state: str | None = None
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ForecastSupportSummary:
    provider: str
    model_name: str
    horizon: int
    support_strength: str
    confidence_summary: str
    degraded_state: str | None
    generated_at: str
    persistence_score: float | None = None
    fade_risk: float | None = None
    trigger_distance: float | None = None
    trigger_pressure: float | None = None
    path_asymmetry: float | None = None
    uncertainty_width_score: float | None = None
    uncertainty_width_label: str | None = None
    regime_alignment_score: float | None = None
    cross_asset_confirmation_score: float | None = None
    scenario_support_strength: str | None = None
    escalation_flag: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TriggerSupport:
    object_id: str
    trigger_type: str
    threshold: str
    source_family: str
    provider: str
    current_distance_to_trigger: str
    next_action_if_hit: str
    next_action_if_broken: str
    threshold_state: str
    support_strength: str
    confidence_summary: str
    degraded_state: str | None = None
    generated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ScenarioSupport:
    object_id: str
    provider: str
    scenario_type: str
    bull_case: dict[str, Any]
    base_case: dict[str, Any]
    bear_case: dict[str, Any]
    support_strength: str
    what_confirms: str
    what_breaks: str
    monitoring_thresholds: list[TriggerSupport]
    degraded_state: str | None = None
    generated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["monitoring_thresholds"] = [threshold.to_dict() for threshold in self.monitoring_thresholds]
        return payload


@dataclass(frozen=True)
class ForecastEvaluation:
    provider: str
    model_name: str
    series_family: str
    horizon: int
    metric_name: str
    metric_value: float
    measured_at: str
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ForecastCapability:
    provider: str
    model_name: str
    tier: str
    managed: bool
    benchmark_only: bool
    configured: bool
    ready: bool = False
    reason_code: str | None = None
    endpoint: str | None = None
    base_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ForecastBundle:
    request: ForecastRequest
    result: ForecastResult
    support: ForecastSupportSummary
    scenario_support: ScenarioSupport
    trigger_support: list[TriggerSupport]
    evaluation: ForecastEvaluation
    forecast_run_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "request": self.request.to_dict(),
            "result": self.result.to_dict(),
            "support": self.support.to_dict(),
            "scenario_support": self.scenario_support.to_dict(),
            "trigger_support": [item.to_dict() for item in self.trigger_support],
            "evaluation": self.evaluation.to_dict(),
            "forecast_run_id": self.forecast_run_id,
        }
