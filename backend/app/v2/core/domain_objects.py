from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


ConfidenceLabel = Literal["high", "medium", "low"]
SignalDirection = Literal["positive", "neutral", "negative", "mixed", "up", "down", "unknown"]
MandateFit = Literal["aligned", "watch", "outside"]
PressureLevel = Literal["low", "medium", "high"]
PortfolioPressureLevel = Literal["calm", "watch", "elevated", "acute"]
BoundarySeverity = Literal["info", "warning", "blocking"]
BoundaryScope = Literal["candidate", "sleeve", "portfolio", "surface"]
FrameworkPosture = Literal["supportive", "neutral", "cautious", "constraining", "blocking", "explanatory_only"]
VisibleState = Literal["research_only", "watch", "review", "eligible", "blocked"]
VisibleAction = Literal["none", "monitor", "review", "compare", "approve"]
SignalMagnitude = Literal["significant", "moderate", "minor", "unknown"]
ImplicationHorizon = Literal["immediate", "near_term", "long_term"]
OverclaimRisk = Literal["low", "medium", "high"]


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class V2Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EvidenceCitation(V2Model):
    source_id: str
    label: str
    url: str | None = None
    note: str | None = None


class MarketDataPoint(V2Model):
    at: str
    value: float


class EvidencePack(V2Model):
    evidence_id: str
    thesis: str
    summary: str
    freshness: str = "current"
    confidence: ConfidenceLabel = "medium"
    citations: list[EvidenceCitation] = Field(default_factory=list)
    facts: dict[str, Any] = Field(default_factory=dict)
    observed_at: str = Field(default_factory=utc_now_iso)


class InstrumentTruth(V2Model):
    instrument_id: str
    symbol: str
    name: str
    asset_class: str
    vehicle_type: str | None = None
    benchmark_id: str | None = None
    domicile: str | None = None
    base_currency: str | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)
    evidence: list[EvidencePack] = Field(default_factory=list)
    as_of: str = Field(default_factory=utc_now_iso)


class MarketSeriesTruth(V2Model):
    series_id: str
    label: str
    frequency: str
    units: str
    points: list[MarketDataPoint] = Field(default_factory=list)
    evidence: list[EvidencePack] = Field(default_factory=list)
    as_of: str = Field(default_factory=utc_now_iso)


class BenchmarkTruth(V2Model):
    benchmark_id: str
    name: str
    methodology: str | None = None
    benchmark_authority_level: str = "bounded"
    mapped_instruments: list[str] = Field(default_factory=list)
    evidence: list[EvidencePack] = Field(default_factory=list)
    as_of: str = Field(default_factory=utc_now_iso)


class MacroTruth(V2Model):
    macro_id: str
    regime: str
    summary: str
    indicators: dict[str, Any] = Field(default_factory=dict)
    evidence: list[EvidencePack] = Field(default_factory=list)
    as_of: str = Field(default_factory=utc_now_iso)


class PortfolioTruth(V2Model):
    portfolio_id: str
    name: str
    base_currency: str
    benchmark_id: str | None = None
    holdings: list[dict[str, Any]] = Field(default_factory=list)
    exposures: dict[str, float] = Field(default_factory=dict)
    cash_weight: float = 0.0
    evidence: list[EvidencePack] = Field(default_factory=list)
    as_of: str = Field(default_factory=utc_now_iso)


class SignalPacket(V2Model):
    signal_id: str
    source_truth_id: str
    signal_kind: str
    direction: SignalDirection = "neutral"
    magnitude: SignalMagnitude = "minor"
    strength: float = 0.0
    horizon: str = "strategic"
    summary: str
    evidence_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class InterpretationCard(V2Model):
    card_id: str
    entity_id: str
    title: str
    thesis: str
    confidence: ConfidenceLabel = "medium"
    conviction: ConfidenceLabel = "medium"
    implication_horizon: ImplicationHorizon = "near_term"
    why_it_matters_economically: str = ""
    why_it_matters_here: str = ""
    signals: list[SignalPacket] = Field(default_factory=list)
    doctrine_tags: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class CandidateAssessment(V2Model):
    candidate_id: str
    sleeve_id: str
    instrument: InstrumentTruth
    interpretation: InterpretationCard
    mandate_fit: MandateFit = "watch"
    conviction: float = 0.0
    score_breakdown: dict[str, float] = Field(default_factory=dict)
    key_supports: list[str] = Field(default_factory=list)
    key_risks: list[str] = Field(default_factory=list)
    holdings_context: dict[str, Any] = Field(default_factory=dict)


class SleeveAssessment(V2Model):
    sleeve_id: str
    label: str
    purpose: str
    candidates: list[CandidateAssessment] = Field(default_factory=list)
    preferred_candidate_id: str | None = None
    pressure_level: PressureLevel = "low"
    summary: str = ""


class PortfolioPressure(V2Model):
    portfolio_id: str
    pressure_id: str
    level: PortfolioPressureLevel = "calm"
    drivers: list[str] = Field(default_factory=list)
    affected_sleeves: list[str] = Field(default_factory=list)
    summary: str = ""


class CompareAssessment(V2Model):
    compare_id: str
    left_candidate_id: str
    right_candidate_id: str
    winner_candidate_id: str | None = None
    confidence: ConfidenceLabel = "medium"
    rationale: list[str] = Field(default_factory=list)
    key_deltas: dict[str, Any] = Field(default_factory=dict)
    decision_state_hint: str = "review"


class PolicyBoundary(V2Model):
    boundary_id: str
    code: str
    action_boundary: str | None = None
    scope: BoundaryScope = "candidate"
    severity: BoundarySeverity = "info"
    passes: bool = True
    summary: str
    required_action: str | None = None
    evidence_ids: list[str] = Field(default_factory=list)


class FrameworkRestraint(V2Model):
    restraint_id: str
    framework_id: str
    label: str
    principle_ref: str | None = None
    constraint_type: str = "boundary_note"
    description: str = ""
    posture: FrameworkPosture = "neutral"
    promotion_cap: str = "none"
    review_intensity_modifier: str = "none"
    confidence_modifier: str = "none"
    action_tone_constraint: str = "none"
    rationale: str
    supports: list[str] = Field(default_factory=list)
    cautions: list[str] = Field(default_factory=list)
    claim_constraints: list[str] = Field(default_factory=list)
    what_changes_view: list[str] = Field(default_factory=list)


class VisibleDecisionState(V2Model):
    decision_id: str
    state: VisibleState = "research_only"
    allowed_action: VisibleAction = "none"
    manual_approval_required: bool = True
    rationale: str


class ConstraintSummary(V2Model):
    summary_id: str
    boundaries: list[PolicyBoundary] = Field(default_factory=list)
    restraints: list[FrameworkRestraint] = Field(default_factory=list)
    blocked_actions: list[str] = Field(default_factory=list)
    reviewer_notes: list[str] = Field(default_factory=list)
    overclaim_risk: OverclaimRisk = "low"
    doctrine_annotations: list[str] = Field(default_factory=list)
    visible_decision_state: VisibleDecisionState


class ChangeEvent(V2Model):
    event_id: str
    surface_id: str
    entity_id: str
    event_type: str
    summary: str
    decision_diff: DecisionDiff | None = None
    trust_diff: TrustDiff | None = None
    recorded_at: str = Field(default_factory=utc_now_iso)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ChangeEventRow(V2Model):
    event_id: str
    event_type: str
    summary: str
    changed_at_utc: str
    candidate_id: str | None = None
    sleeve_id: str | None = None
    sleeve_name: str | None = None
    change_trigger: str | None = None
    reason_summary: str | None = None
    previous_state: str | None = None
    current_state: str | None = None
    implication_summary: str | None = None
    portfolio_consequence: str | None = None
    next_action: str | None = None
    what_would_reverse: str | None = None
    requires_review: bool = False
    report_tab: str | None = None
    impact_level: str | None = None
    ui_category: str | None = None
    direction: str | None = None
    is_blocker_change: bool = False
    deep_link_target: dict[str, Any] | None = None


class ChangesSummary(V2Model):
    total_changes: int = 0
    upgrades: int = 0
    downgrades: int = 0
    blocker_changes: int = 0
    requires_review: int = 0


class ChangesAvailableSleeve(V2Model):
    sleeve_id: str | None = None
    sleeve_name: str
    count: int = 0


class ChangesFiltersApplied(V2Model):
    category: str | None = None
    sleeve_id: str | None = None
    candidate_id: str | None = None
    needs_review: bool | None = None
    limit: int | None = None
    cursor: str | None = None


class ChangesPagination(V2Model):
    limit: int | None = None
    returned: int = 0
    total_matching: int = 0
    has_more: bool = False
    next_cursor: str | None = None


class ChangesFeedFreshness(V2Model):
    feed_freshness_state: str = "empty"
    latest_event_at: str | None = None
    latest_event_age_days: float | None = None


class DecisionDiff(V2Model):
    entity_id: str
    before_state: str | None = None
    after_state: str | None = None
    changed_fields: list[str] = Field(default_factory=list)
    summary: str


class TrustDiff(V2Model):
    entity_id: str
    before_score: float | None = None
    after_score: float | None = None
    delta: float | None = None
    reasons: list[str] = Field(default_factory=list)


class SurfaceChangeSummary(V2Model):
    surface_id: str
    event_count: int = 0
    changed_entity_ids: list[str] = Field(default_factory=list)
    decisions: list[DecisionDiff] = Field(default_factory=list)
    trust_diffs: list[TrustDiff] = Field(default_factory=list)
    events: list[ChangeEvent] = Field(default_factory=list)
    emitted_at: str = Field(default_factory=utc_now_iso)
