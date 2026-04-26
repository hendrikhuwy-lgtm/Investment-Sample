from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl


class SourceRecord(BaseModel):
    source_id: str
    url: HttpUrl
    publisher: str
    published_at: datetime | None = None
    retrieved_at: datetime
    topic: str
    credibility_tier: Literal["primary", "secondary", "tertiary"] = "secondary"
    raw_hash: str
    source_type: Literal["web", "mcp"] = "web"


class Citation(BaseModel):
    url: HttpUrl
    source_id: str
    retrieved_at: datetime
    importance: str = Field(min_length=5)
    observed_at: str | None = None
    lag_days: int | None = None
    lag_class: Literal["fresh", "lagged", "stale"] | None = None
    lag_cause: Literal["expected_publication_lag", "unexpected_ingestion_lag", "unknown"] | None = None


class InsightRecord(BaseModel):
    insight_id: str
    theme: Literal["market", "macro", "valuation", "guru_view", "risk_event", "tax", "portfolio"]
    summary: str = Field(min_length=20)
    stance: Literal["bullish", "neutral", "defensive"] = "neutral"
    confidence: float = Field(ge=0, le=1)
    time_horizon: Literal["short", "medium", "long"] = "medium"
    citations: list[Citation]


class PortfolioSignal(BaseModel):
    signal_id: str
    metric: str
    value: float
    threshold: float
    state: Literal["normal", "watch", "alert"]
    portfolio_impact: dict[str, str]


class TaxResidencyProfile(BaseModel):
    profile_id: str
    tax_residency: Literal["SG"]
    base_currency: Literal["SGD"]
    dta_flags: dict[str, bool]
    estate_risk_flags: dict[str, bool]


class InstrumentTaxProfile(BaseModel):
    instrument_id: str
    domicile: str
    us_dividend_exposure: bool
    expected_withholding_rate: float = Field(ge=0, le=1)
    us_situs_risk_flag: bool
    expense_ratio: float = Field(ge=0, le=0.1)
    liquidity_score: float = Field(ge=0, le=1)


class ConvexSleevePosition(BaseModel):
    symbol: str
    allocation_weight: float = Field(ge=0, le=0.03)
    retail_accessible: bool
    margin_required: bool
    max_loss_known: bool
    instrument_type: Literal["managed_futures_etf", "tail_hedge_fund", "long_put_option"]


class AlertEvent(BaseModel):
    alert_id: str
    severity: Literal["info", "warning", "critical"]
    trigger_reason: str
    citations: list[Citation]
    sent_channel: Literal["email"] = "email"
    ack_state: Literal["pending", "acknowledged"] = "pending"
    created_at: datetime


class InstrumentCandidate(BaseModel):
    symbol: str
    name: str
    asset_class: str
    instrument_type: Literal[
        "etf_ucits",
        "etf_us",
        "t_bill_sg",
        "money_market_fund_sg",
        "cash_account_sg",
        "long_put_overlay_strategy",
    ] = "etf_ucits"
    domicile: str
    expense_ratio: float | None = Field(default=None, ge=0, le=0.1)
    average_volume: float | None = None
    dividend_yield: float | None = None
    withholding_rate: float | None = Field(default=None, ge=0, le=1)
    us_situs_risk_flag: bool
    liquidity_score: float | None = Field(default=None, ge=0, le=1)
    tax_score: float | None = None
    retrieved_at: datetime
    citations: list[Citation]
    primary_sources: list[Citation] = Field(default_factory=list)
    verification_status: Literal["verified", "partially_verified", "unverified"] = "partially_verified"
    verification_missing: list[str] = Field(default_factory=list)
    proof_isin: bool = False
    proof_domicile: bool = False
    proof_accumulating: bool = False
    proof_ter: bool = False
    proof_factsheet_fresh: bool = False
    factsheet_asof: str | None = None
    last_verified_at: datetime | None = None
    yield_proxy: float | None = None
    duration_years: float | None = None
    allocation_weight: float | None = None
    retail_accessible: bool | None = None
    margin_required: bool | None = None
    max_loss_known: bool | None = None
    short_options: bool | None = None
    option_position: Literal["long_put"] | None = None
    strike: float | None = None
    expiry: str | None = None
    premium_paid_pct_nav: float | None = None
    annualized_carry_estimate: float | None = None
    notes: str | None = None
    illustrative_label: str = "Illustrative candidate, not a recommendation"


# Additional MCP snapshot models. Public locked models above are unchanged.
class MCPServerEndpoint(BaseModel):
    endpoint_type: Literal["http", "sse", "websocket", "unknown", "unknown_remote_type", "templated_unresolved"]
    url: str


class MCPServerCapability(BaseModel):
    handshake_ok: bool
    tools_count: int
    resources_count: int
    auth_used: bool
    errors: list[str]


class MCPItem(BaseModel):
    server_id: str
    item_type: Literal["resource", "search", "tool", "metadata"]
    uri: str | None = None
    title: str | None = None
    content: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class MCPServerSnapshot(BaseModel):
    server_id: str
    server_name: str
    publisher: str
    endpoint_url: str
    endpoint_type: Literal["http", "sse", "websocket", "unknown", "unknown_remote_type", "templated_unresolved"]
    retrieved_at: datetime
    cached: bool = False
    capability: MCPServerCapability
    items: list[MCPItem] = Field(default_factory=list)
    raw_hash: str
    error: str | None = None


class PortfolioHolding(BaseModel):
    holding_id: str
    symbol: str
    name: str
    quantity: float
    cost_basis: float
    currency: str
    sleeve: Literal["global_equity", "ig_bond", "real_asset", "alt", "convex", "cash"]
    account_type: Literal["taxable", "broker", "other"]
    created_at: datetime
    updated_at: datetime


class PortfolioSnapshot(BaseModel):
    snapshot_id: str
    created_at: datetime
    total_value: float
    sleeve_weights: dict[str, float]
    concentration_metrics: dict[str, Any]
    convex_coverage_ratio: float
    tax_drag_estimate: float
    notes: str | None = None


class StressScenarioResult(BaseModel):
    scenario_id: str
    name: str
    estimated_impact_pct: float
    diagnostic: str


class PersonalPortfolioDiagnostic(BaseModel):
    created_at: datetime
    total_value: float
    policy_weights: dict[str, float]
    actual_weights: dict[str, float]
    allocation_drift: dict[str, float]
    concentration_metrics: dict[str, Any]
    tax_drag_estimate: float
    convex_coverage: dict[str, Any]
    regime_alignment_score: float
    regime_alignment_diagnostic: dict[str, Any]
    stress_scenarios: list[StressScenarioResult]


class DailyLog(BaseModel):
    log_id: str
    created_at: datetime
    macro_state_summary: str
    short_term_alert_state: str
    portfolio_snapshot_id: str | None = None
    regime_classification: str
    top_risk_flags: list[str]
    top_opportunity_flags: list[str]
    personal_alignment_score: float


class JournalEntry(BaseModel):
    entry_id: str
    created_at: datetime
    thesis: str
    concerns: str | None = None
    mistakes_avoided: str | None = None
    lessons: str | None = None


class PolicyAllocation(BaseModel):
    sleeve: Literal["global_equity", "ig_bond", "real_asset", "alt", "convex", "cash"]
    target_weight: float = Field(ge=0, le=1)
    min_band: float = Field(ge=0, le=1)
    max_band: float = Field(ge=0, le=1)
    calendar_rebalance_frequency: Literal["monthly", "quarterly", "semiannual"] = "monthly"
    drift_threshold_absolute: float = Field(ge=0, le=1, default=0.05)
    drift_threshold_relative: float = Field(ge=0, le=1, default=0.20)
    rebalance_priority: Literal["core", "defensive", "satellite"] = "core"
    interdependency_rules: list[str] = Field(default_factory=list)


class ConcentrationPolicy(BaseModel):
    profile_type: Literal["hnwi_sg"] = "hnwi_sg"
    issuer_max_weight: float | None = Field(default=None, ge=0, le=1)
    single_fund_max_weight: float | None = Field(default=0.55, ge=0, le=1)
    single_country_max_weight: float | None = Field(default=0.70, ge=0, le=1)
    single_region_max_weight: float | None = Field(default=None, ge=0, le=1)
    single_sector_max_weight: float | None = Field(default=0.35, ge=0, le=1)
    top10_concentration_warning: float | None = Field(default=0.28, ge=0, le=1)
    em_weight_band_min: float | None = Field(default=0.05, ge=0, le=1)
    em_weight_band_max: float | None = Field(default=0.20, ge=0, le=1)


class RiskControlResult(BaseModel):
    status: Literal["pass", "warn", "fail", "unknown"]
    metric_name: str
    current_value: float | str | None = None
    policy_limit: float | str | None = None
    rationale: str
    blockers: list[str] = Field(default_factory=list)
    provenance: list[str] = Field(default_factory=list)


class LiquidityProfile(BaseModel):
    liquidity_status: Literal["strong", "adequate", "weak", "limited_evidence", "unknown"]
    spread_status: Literal["tight", "acceptable", "wide", "unknown"]
    capacity_comment: str
    execution_comment: str
    days_to_liquidate_estimate: float | None = Field(default=None, ge=0)
    warnings: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    provenance: list[str] = Field(default_factory=list)
    explanation: str | None = None
    evidence_basis: list[str] = Field(default_factory=list)
    missing_inputs: list[str] = Field(default_factory=list)
    quote_freshness_state: Literal["fresh", "aging", "stale", "unknown"] | None = None
    history_depth_state: Literal["strong", "usable", "thin", "missing"] | None = None
    spread_support_state: Literal["usable", "degraded", "insufficient"] | None = None
    volume_support_state: Literal["usable", "degraded", "insufficient"] | None = None
    route_validity_state: Literal["direct_ready", "proxy_ready", "alias_review_needed", "invalid", "unknown"] | None = None
    execution_confidence: Literal["strong", "usable", "degraded", "insufficient"] | None = None


class RebalanceDiagnostics(BaseModel):
    status: Literal["no_action", "calendar_review_due", "band_breach", "interdependency_warning", "data_incomplete"]
    summary: str
    blockers: list[str] = Field(default_factory=list)
    triggered_rules: list[str] = Field(default_factory=list)
    provenance: list[str] = Field(default_factory=list)


class InvestmentQualityScore(BaseModel):
    symbol: str
    sleeve_key: str
    eligibility_state: Literal["eligible", "eligible_with_caution", "ineligible", "data_incomplete"]
    eligibility_blockers: list[str] = Field(default_factory=list)
    data_confidence: Literal["low", "medium", "high"]
    performance_score: float | None = None
    risk_adjusted_score: float | None = None
    cost_score: float | None = None
    liquidity_score: float | None = None
    structure_score: float | None = None
    tax_score: float | None = None
    governance_confidence_score: float | None = None
    composite_score: float | None = None
    rank_in_sleeve: int | None = None
    percentile_in_sleeve: float | None = None
    badge: Literal["best_in_class", "recommended", "acceptable", "caution", "not_ranked"] = "not_ranked"
    recommendation_state: Literal[
        "recommended_primary",
        "recommended_backup",
        "watchlist_only",
        "rejected_policy_failure",
        "rejected_data_insufficient",
        "rejected_inferior_to_selected",
        "research_only",
        "removed_from_deliverable_set",
    ] = "research_only"
    investment_thesis: str | None = None
    role_in_portfolio: str | None = None
    key_advantages: list[str] = Field(default_factory=list)
    key_risks: list[str] = Field(default_factory=list)
    comparison_vs_peers: dict[str, Any] = Field(default_factory=dict)
    score_provenance: dict[str, Any] = Field(default_factory=dict)
    score_version: str
    as_of_date: str


class SleeveRecommendation(BaseModel):
    sleeve_key: str
    our_pick_symbol: str | None = None
    our_pick: dict[str, Any] | None = None
    top_candidates: list[dict[str, Any]] = Field(default_factory=list)
    acceptable_candidates: list[dict[str, Any]] = Field(default_factory=list)
    caution_candidates: list[dict[str, Any]] = Field(default_factory=list)
    why_this_pick_wins: str
    what_would_change_the_pick: str
    missing_data: list[str] = Field(default_factory=list)
    common_blockers: list[str] = Field(default_factory=list)
    score_version: str
    as_of_date: str


class Constraints(BaseModel):
    base_currency: Literal["SGD"] = "SGD"
    max_single_name_pct: float = Field(ge=0, le=1, default=0.15)
    max_top5_pct: float = Field(ge=0, le=1, default=0.55)
    convex_required: bool = True
    convex_target_total: float = Field(ge=0, le=0.1, default=0.03)
    blueprint_global_equity_core_target: float = Field(ge=0, le=1, default=0.45)
    blueprint_global_equity_core_min: float = Field(ge=0, le=1, default=0.40)
    blueprint_global_equity_core_max: float = Field(ge=0, le=1, default=0.55)
    blueprint_developed_ex_us_optional_target: float = Field(ge=0, le=1, default=0.00)
    blueprint_developed_ex_us_optional_min: float = Field(ge=0, le=1, default=0.00)
    blueprint_developed_ex_us_optional_max: float = Field(ge=0, le=1, default=0.10)
    blueprint_emerging_markets_target: float = Field(ge=0, le=1, default=0.07)
    blueprint_emerging_markets_min: float = Field(ge=0, le=1, default=0.05)
    blueprint_emerging_markets_max: float = Field(ge=0, le=1, default=0.10)
    blueprint_china_satellite_target: float = Field(ge=0, le=1, default=0.03)
    blueprint_china_satellite_min: float = Field(ge=0, le=1, default=0.00)
    blueprint_china_satellite_max: float = Field(ge=0, le=1, default=0.05)
    blueprint_ig_bonds_target: float = Field(ge=0, le=1, default=0.25)
    blueprint_ig_bonds_min: float = Field(ge=0, le=1, default=0.20)
    blueprint_ig_bonds_max: float = Field(ge=0, le=1, default=0.30)
    blueprint_cash_bills_target: float = Field(ge=0, le=1, default=0.10)
    blueprint_cash_bills_min: float = Field(ge=0, le=1, default=0.05)
    blueprint_cash_bills_max: float = Field(ge=0, le=1, default=0.15)
    blueprint_real_assets_target: float = Field(ge=0, le=1, default=0.07)
    blueprint_real_assets_min: float = Field(ge=0, le=1, default=0.05)
    blueprint_real_assets_max: float = Field(ge=0, le=1, default=0.10)
    blueprint_alternatives_target: float = Field(ge=0, le=1, default=0.03)
    blueprint_alternatives_min: float = Field(ge=0, le=1, default=0.00)
    blueprint_alternatives_max: float = Field(ge=0, le=1, default=0.07)
    blueprint_cash_floor: float = Field(ge=0, le=1, default=0.05)
    no_margin: bool = True
    no_short_options: bool = True
    undefined_loss_forbidden: bool = True


class InvestorProfile(BaseModel):
    profile_id: str = "primary"
    owner_label: str = "Personal Investor"
    risk_tier: Literal["moderate_growth"] = "moderate_growth"
    target_return_min: float = Field(default=0.06, ge=0, le=1)
    target_return_max: float = Field(default=0.10, ge=0, le=1)
    horizon_years: int = Field(default=10, ge=1, le=50)
    rebalance_frequency: Literal["monthly", "quarterly"] = "monthly"
    allocations: list[PolicyAllocation]
    constraints: Constraints
    updated_at: datetime


class AttributionSleeveEffect(BaseModel):
    sleeve: str
    allocation_effect: float
    selection_effect: float
    interaction_effect: float
    total_effect: float


class AttributionSummary(BaseModel):
    period: str
    portfolio_return: float
    benchmark_return: float
    active_return: float
    sleeve_effects: list[AttributionSleeveEffect]
    notes: str | None = None
    notes: str
