import type {
  CandidateAssessmentId,
  CandidateId,
  ChangeEventId,
  CompareAssessmentId,
  InstrumentId,
  PortfolioId,
  SleeveId,
  SleeveAssessmentId,
  VisibleDecisionStateId,
} from './v2_ids';

export type V2Confidence = 'high' | 'medium' | 'low';
export type V2PressureLevel = 'low' | 'medium' | 'high';
export type V2VisibleState = 'research_only' | 'watch' | 'review' | 'eligible' | 'blocked';
export type V2VisibleAction = 'none' | 'monitor' | 'review' | 'compare' | 'approve';
export type V2ReviewPosture = 'monitor' | 'review_soon' | 'act_now';
export type V2EvidenceDepth = 'substantial' | 'moderate' | 'limited';
export type V2InstrumentQuality = 'High' | 'Moderate' | 'Low';
export type V2PortfolioFitNow = 'Highest' | 'Good' | 'Weak today';
export type V2CapitalPriorityNow =
  | 'First call on next dollar'
  | 'Second choice'
  | 'No new capital';
export type V2WorkUrgency = 'act' | 'review' | 'monitor';
export type V2MappingDirectness = 'direct' | 'sleeve-proxy' | 'macro-only';
export type V2ImpactLevel = 'high' | 'medium' | 'low';
export type FreshnessClass =
  | 'fresh_full_rebuild'
  | 'fresh_partial_rebuild'
  | 'stored_valid_context'
  | 'degraded_monitoring_mode'
  | 'execution_failed_or_incomplete';

export interface SurfaceBadgeContract {
  label: string;
  tone?: 'good' | 'warn' | 'bad' | 'neutral' | 'info' | string;
}

export interface MarketSessionContext {
  exchange: string | null;
  asset_class: string | null;
  session_state: string;
  session_label: string;
  session_date: string;
  quote_session: string | null;
  market_open_utc: string;
  market_close_utc: string;
  calendar_scope?: string | null;
  calendar_precision?: string | null;
  is_early_close?: boolean | null;
  extended_hours_state?: string | null;
  regular_open_utc?: string | null;
  regular_close_utc?: string | null;
  effective_close_utc?: string | null;
  next_regular_open_utc?: string | null;
}

export interface TruthEnvelope {
  as_of_utc: string | null;
  observation_date?: string | null;
  reference_period: string | null;
  reference_period_label?: string | null;
  source_authority: string | null;
  acquisition_mode: string;
  degradation_reason: string | null;
  recommendation_critical: boolean;
  retrieved_at_utc: string | null;
  release_date?: string | null;
  availability_date?: string | null;
  realtime_start?: string | null;
  realtime_end?: string | null;
  vintage_class?: string | null;
  revision_state?: string | null;
  release_semantics_state?: string | null;
  period_clock_class?: string | null;
  market_session_context?: MarketSessionContext | null;
}

export interface RuntimeSourceProvenance {
  source_family: string | null;
  provider_used: string | null;
  path_used: string | null;
  live_or_cache: string | null;
  freshness: string | null;
  provenance_strength: string | null;
  source_authority_tier?: string | null;
  derived_or_proxy: boolean;
  usable_truth?: boolean | null;
  sufficiency_state?: string | null;
  data_mode?: string | null;
  authority_level?: string | null;
  observed_at?: string | null;
  insufficiency_reason?: string | null;
  truth_envelope?: TruthEnvelope | null;
}

export interface PrimaryDocumentRef {
  doc_type: string;
  doc_url: string | null;
  status: string;
  retrieved_at: string | null;
  authority_class?: string | null;
  cache_file?: string | null;
  document_fingerprint?: string | null;
}

export interface CandidateImplementationProfile {
  issuer?: string | null;
  mandate_or_index?: string | null;
  replication_method?: string | null;
  primary_listing_exchange?: string | null;
  primary_trading_currency?: string | null;
  spread_proxy?: string | null;
  premium_discount_behavior?: string | null;
  aum?: string | null;
  domicile?: string | null;
  distribution_policy?: string | null;
  launch_date?: string | null;
  issuer_name?: string | null;
  tracking_difference?: string | null;
  execution_suitability?: string | null;
  execution_score?: number | null;
  missing_fields?: string[];
  summary?: string | null;
  primary_document_manifest?: PrimaryDocumentRef[];
}

export interface RecommendationGate {
  gate_state: 'admissible' | 'review_only' | 'blocked';
  readiness_level: string;
  critical_missing_fields: string[];
  blocked_reasons: string[];
  data_confidence: 'high' | 'mixed' | 'low' | string;
  execution_suitability: string;
  summary: string;
}

export interface ReconciliationStatus {
  status: 'verified' | 'soft_drift' | 'hard_conflict' | string;
  hard_conflicts: string[];
  soft_drifts: string[];
  summary: string;
}

export interface SourceAuthorityField {
  field_name: string;
  label: string;
  resolved_value: unknown;
  source_name: string | null;
  source_url: string | null;
  source_type: string | null;
  authority_class: string;
  observed_at: string | null;
  freshness_state: string;
  recommendation_critical: boolean;
  preferred_document_types?: string[];
  document_support_refs?: PrimaryDocumentRef[];
  document_support_state?: string | null;
}

export interface ReconciliationFieldStatus {
  field_name: string;
  label: string;
  status: string;
  summary: string;
  source_names: string[];
  severity?: string | null;
  blocking_effect?: string | null;
  fixability_kind?: string | null;
  fixability_label?: string | null;
  resolved_value?: unknown;
  observed_values?: unknown[];
  value_count: number;
  recommendation_critical: boolean;
}

export interface DataQualitySummary {
  data_confidence: string;
  critical_fields_ready: number;
  critical_fields_total: number;
  issuer_backed_fields: number;
  stale_critical_fields: number;
  document_gap_count?: number;
  summary: string;
}

export interface CandidateIdentityState {
  state: 'verified' | 'thin' | 'review' | 'conflict' | 'missing' | string;
  blocking: boolean;
  summary: string;
  resolved_name?: string | null;
  resolved_isin?: string | null;
  name_observation_count?: number | null;
  isin_observation_count?: number | null;
  rejected_record_count?: number | null;
}

export interface CandidateTaxPosture {
  withholding_tax_posture?: string | null;
  estate_risk_posture?: string | null;
  summary: string;
}

export interface CandidateSourceIntegritySummary {
  state: 'strong' | 'mixed' | 'weak' | 'conflicted' | 'missing' | string;
  integrity_label?: 'clean' | 'thin' | 'mixed' | 'weak' | 'conflicted' | 'missing' | string;
  summary: string;
  critical_fields_ready: number;
  critical_fields_total: number;
  issuer_backed_fields: number;
  stale_or_missing_fields: number;
  weak_authority_fields: number;
  conflict_items: number;
  document_gap_count: number;
  authority_mix?: {
    verified_current_truth: number;
    issuer_primary: number;
    issuer_secondary: number;
    provider_or_market_summary: number;
    registry_seed: number;
    missing: number;
  };
  issue_counts?: {
    hard_conflicts: number;
    soft_drifts: number;
    stale_fields: number;
    missing_critical_fields: number;
    weak_authority_fields: number;
    execution_review_required?: number;
    document_gaps: number;
    review_items: number;
  };
  hard_conflict_fields?: string[];
  missing_critical_fields?: string[];
  weakest_fields?: string[];
  identity_state?: string | null;
}

export interface CandidateSourceCompletionFieldState {
  field_name: string;
  completion_state: 'complete' | 'equivalent_complete' | 'incomplete' | string;
  completion_basis: string;
}

export interface CandidateSourceCompletionSummary {
  state: 'complete' | 'incomplete' | 'unresolved' | 'review' | string;
  summary: string;
  critical_fields_completed: number;
  critical_fields_total: number;
  equivalent_ready_count: number;
  incomplete_fields: string[];
  missing_fields?: string[];
  weak_fields?: string[];
  stale_fields?: string[];
  conflict_fields?: string[];
  authority_clean?: boolean;
  freshness_clean?: boolean;
  conflict_clean?: boolean;
  completeness_clean?: boolean;
  equivalent_ready_fields?: string[];
  not_applicable_verified_fields?: string[];
  resolved_by_curated_registry?: string[];
  completion_reasons?: string[];
  field_states?: CandidateSourceCompletionFieldState[];
}

export interface BlueprintFailureClassItem {
  class_id: string;
  label: string;
  severity: 'block' | 'review' | 'confidence_drag' | 'info' | string;
  summary: string;
  fields?: string[];
  count?: number | null;
}

export interface BlueprintFailureClassSummary {
  primary_class?: string | null;
  primary_label?: string | null;
  summary?: string | null;
  hard_classes?: string[];
  review_classes?: string[];
  confidence_drag_classes?: string[];
  items?: BlueprintFailureClassItem[];
}

export interface CandidateScoreComponent {
  component_id: string;
  key?: string | null;
  label: string;
  score: number;
  band?: 'strong' | 'good' | 'review' | 'weak' | 'blocked' | string;
  confidence?: number | null;
  tone: 'good' | 'warn' | 'bad' | 'neutral' | 'info' | string;
  summary: string;
  reasons?: string[];
  caps_applied?: string[];
  field_drivers?: string[];
  missing_fields?: string[];
  weak_fields?: string[];
  stale_fields?: string[];
  conflict_fields?: string[];
}

export interface CandidateScoreDecomposition {
  total_score: number;
  recommendation_score?: number;
  recommendation_merit_score?: number;
  investment_merit_score?: number;
  deployability_score?: number;
  truth_confidence_score?: number;
  source_completion_score?: number;
  freshness_cleanliness_score?: number;
  conflict_cleanliness_score?: number;
  truth_confidence_band?: string | null;
  truth_confidence_summary?: string | null;
  score_model_version?: string | null;
  admissibility_score?: number;
  admissibility_identity_score?: number;
  implementation_score?: number;
  source_integrity_score?: number;
  evidence_score?: number;
  sleeve_fit_score?: number;
  identity_score?: number;
  benchmark_fidelity_score?: number;
  market_path_support_score?: number;
  long_horizon_quality_score?: number;
  instrument_quality_score?: number;
  portfolio_fit_score?: number;
  optimality_score?: number;
  readiness_score?: number;
  deployment_score?: number;
  confidence_penalty?: number;
  readiness_posture?: 'action_ready' | 'reviewable' | 'blocked' | string;
  readiness_summary?: string | null;
  deployability_badge?: 'deploy_now' | 'review_before_deploy' | 'research_only' | 'blocked' | string;
  summary?: string | null;
  components: CandidateScoreComponent[];
}

export interface CandidateScoreSummaryComponent {
  component_id: string;
  label: string;
  score: number;
  band?: 'strong' | 'good' | 'review' | 'weak' | 'blocked' | string;
  confidence?: number | null;
  tone: 'good' | 'warn' | 'bad' | 'neutral' | 'info' | string;
  summary?: string | null;
}

export interface CandidateScoreSummary {
  average_score: number;
  component_count_used: number;
  tone: 'good' | 'warn' | 'bad' | 'neutral' | 'info' | string;
  reliability_state: 'strong' | 'mixed' | 'weak' | string;
  reliability_note?: string | null;
  components: CandidateScoreSummaryComponent[];
}

export interface ScoreRubricFamily {
  family_id: string;
  label: string;
  score?: number | null;
  weighting_bucket?: 'high' | 'medium' | 'low' | string;
  bounded_role?: string | null;
  summary: string;
}

export interface ScoreRubric {
  weighting_profile: string;
  summary: string;
  confidence_penalty?: number | null;
  dimension_priority_order: string[];
  families: ScoreRubricFamily[];
}

export interface CoverageChecklistItem {
  item_id: string;
  label: string;
  state: string;
  detail?: string | null;
}

export interface SymbolAliasRegistry {
  direct_symbol: string;
  exchange_qualified_symbol?: string | null;
  provider_symbol: string;
  provider_alias?: string | null;
  manual_override?: string | null;
  verification_source?: string | null;
  resolution_confidence?: number | null;
  resolution_reason?: string | null;
  fallback_aliases?: string[];
}

export interface CoverageWorkflowSummary {
  status: string;
  summary: string;
  checklist: CoverageChecklistItem[];
  current_runtime_provider: string;
  current_history_provider?: string | null;
  direct_history_depth?: number | null;
  proxy_history_depth?: number | null;
  benchmark_lineage_weak?: boolean;
  alias_review_needed?: boolean;
  symbol_alias_registry?: SymbolAliasRegistry | null;
  direct_quality?: Record<string, unknown> | null;
  proxy_quality?: Record<string, unknown> | null;
}

export interface ReportSummaryStrip {
  headline: string;
  stance: string;
  score_label?: string | null;
  source_confidence_label?: string | null;
  coverage_status?: string | null;
  market_path_note?: string | null;
}

export interface RouteCacheState {
  state: string;
  stale: boolean;
  revalidating: boolean;
  cached_at?: string | null;
  max_age_seconds?: number | null;
  summary: string;
}

export interface ReportLoadingHint {
  strategy: string;
  summary: string;
  route_cache_state?: string | null;
  staged_sections?: string[];
}

export interface CompareCandidateSnapshot {
  candidate_id: CandidateAssessmentId | string;
  symbol: string;
  name: string;
  investor_decision_state?: string | null;
  blocker_category?: string | null;
  benchmark_full_name?: string | null;
  exposure_summary?: string | null;
  ter_bps?: number | null;
  spread_proxy_bps?: number | null;
  aum_usd?: number | null;
  aum_state?: string | null;
  distribution_policy?: string | null;
  replication_method?: string | null;
  domicile?: string | null;
  primary_trading_currency?: string | null;
  primary_listing_exchange?: string | null;
  current_weight_pct?: number | null;
  weight_state?: string | null;
  source_integrity_state?: string | null;
  total_score?: number | null;
  recommendation_score?: number | null;
  investment_merit_score?: number | null;
  deployability_score?: number | null;
  truth_confidence_score?: number | null;
  decision_summary?: string | null;
  compare_card?: CompareCandidateCard | null;
}

export interface ComparePresentationStat {
  label: string;
  value: string;
  tone?: string | null;
}

export interface CompareCandidateCard {
  identity: {
    exposure_summary?: string | null;
    compact_tags?: string[];
  };
  verdict: {
    primary_state: string;
    reason_line: string;
  };
  sleeve_fit: {
    role_fit: string;
    benchmark_fit: string;
    scope_fit: string;
    thesis: string;
  };
  implementation: {
    stats: ComparePresentationStat[];
  };
  risk_evidence: {
    evidence_status: string;
    timing_status: string;
    impact_line: string;
  };
}

export interface CompareDimensionValue {
  candidate_id: CandidateAssessmentId | string;
  value: string;
  tone?: string | null;
  state?: string | null;
}

export interface DailyBriefDataTimeframe {
  label: string;
  summary: string;
  truth_envelope: TruthEnvelope | null;
  runtime_provenance?: RuntimeSourceProvenance | null;
}

export type ContractStateKind = 'ready' | 'degraded' | 'empty' | 'blocked';

export interface SurfaceState {
  state: ContractStateKind;
  reason_codes: string[];
  summary: string;
}

export interface SectionState {
  state: ContractStateKind;
  reason_code: string;
  reason_text: string;
}

export type SectionStateMap = Record<string, SectionState>;

export interface V2ContractBase {
  contract_version: string;
  surface_id: string;
  generated_at: string;
  freshness_state: string;
  holdings_overlay_present: boolean;
  route_cache_state?: RouteCacheState | null;
  surface_state?: SurfaceState;
  section_states?: SectionStateMap;
  surface_snapshot_id?: string | null;
  source_contract_version?: string | null;
}

export type VisibleDecisionStateBlock = {
  state: V2VisibleState;
  allowed_action: V2VisibleAction;
  rationale: string;
};

export type V2VisibleDecisionState = {
  decision_id: VisibleDecisionStateId | string;
  state: V2VisibleState;
  allowed_action: V2VisibleAction;
  manual_approval_required: boolean;
  rationale: string;
};

/** Enriched candidate row emitted per sleeve in the Blueprint Explorer. */
export type CandidateRowExpanded = {
  candidate_id: CandidateAssessmentId | string;
  sleeve_key?: string | null;
  symbol: string;
  name: string;
  score?: number;
  benchmark_full_name?: string | null;
  exposure_summary?: string | null;
  ter_bps?: number | null;
  spread_proxy_bps?: number | null;
  aum_usd?: number | null;
  aum_state?: 'resolved' | 'stale' | 'missing' | string;
  sg_tax_posture?: CandidateTaxPosture | null;
  distribution_policy?: string | null;
  replication_risk_note?: string | null;
  current_weight_pct?: number | null;
  weight_state?: 'overlay_active' | 'overlay_absent' | string;
  investor_decision_state?: 'actionable' | 'shortlisted' | 'blocked' | 'research_only' | string;
  source_integrity_summary?: CandidateSourceIntegritySummary | null;
  source_completion_summary?: CandidateSourceCompletionSummary | null;
  failure_class_summary?: BlueprintFailureClassSummary | null;
  score_decomposition?: CandidateScoreDecomposition | null;
  score_summary?: CandidateScoreSummary | null;
  identity_state?: CandidateIdentityState | null;
  blocker_category?: string | null;
  issuer?: string | null;
  expense_ratio?: string | null;
  aum?: string | null;
  freshness_days?: number | null;
  instrument_quality: V2InstrumentQuality;
  portfolio_fit_now: V2PortfolioFitNow;
  capital_priority_now: V2CapitalPriorityNow;
  gate_state?: string | null;
  status_label: string;
  why_now: string;
  what_blocks_action: string;
  what_changes_view: string;
  action_boundary: string | null;
  funding_source: string | null;
  winner_reason?: string | null;
  loser_reason?: string | null;
  compare_eligibility?: string | null;
  forecast_support?: ForecastSupport | null;
  market_path_support?: BlueprintMarketPathSupport | null;
  report_href?: string | null;
  scenario_readiness_note?: string | null;
  flip_risk_note?: string | null;
  detail_chart_panels?: ChartPanelContract[];
  quick_brief_snapshot?: CandidateQuickBrief | null;
  report_snapshot?: CandidateReportContract | null;
  implementation_profile?: CandidateImplementationProfile | null;
  recommendation_gate?: RecommendationGate | null;
  reconciliation_status?: ReconciliationStatus | null;
  source_authority_fields?: SourceAuthorityField[];
  reconciliation_report?: ReconciliationFieldStatus[];
  data_quality_summary?: DataQualitySummary | null;
  candidate_market_provenance?: RuntimeSourceProvenance | null;
  research_support_summary?: string | null;
  candidate_row_summary?: string | null;
  candidate_supporting_factors?: string[];
  candidate_penalizing_factors?: string[];
  report_summary_strip?: ReportSummaryStrip | null;
  source_confidence_label?: string | null;
  coverage_status?: string | null;
  coverage_workflow_summary?: CoverageWorkflowSummary | null;
  score_rubric?: ScoreRubric | null;
  market_path_objective?: string | null;
  market_path_case_note?: string | null;
  visible_decision_state: VisibleDecisionStateBlock;
  implication_summary: string;
};

export interface ResearchRetrievalGuide {
  guide_id: string;
  label: string;
  query: string;
  reason: string;
  priority: 'high' | 'medium' | 'low' | string;
  preferred_sources?: string[];
  target_surface?: string | null;
}

export interface ResearchNewsCluster {
  cluster_id: string;
  label: string;
  summary: string;
  tone: 'improving' | 'worsening' | 'mixed' | 'neutral' | string;
  headline_count: number;
  headlines: string[];
}

export interface ResearchMarketContext {
  title: string;
  summary: string;
  instrument_line?: string | null;
  benchmark_line?: string | null;
  freshness_note?: string | null;
}

export interface ThesisDriftSupport {
  state: 'thesis_strengthened' | 'thesis_weakened' | 'thesis_falsified' | 'thesis_unchanged' | string;
  summary: string;
  evidence_delta: string;
  consequence_delta: string;
  confidence_delta: string;
  watchlist_priority_delta: string;
  prior_generated_at?: string | null;
}

export interface ResearchDraftingSupport {
  suggested_title: string;
  summary: string;
  key_questions: string[];
  next_steps: string[];
}

export interface ResearchLogicMap {
  title: string;
  steps: Array<{
    label: string;
    detail: string;
  }>;
}

export interface ResearchSentimentAnnotation {
  label: string;
  tone: 'good' | 'warn' | 'bad' | 'neutral' | string;
  summary: string;
}

export interface ResearchSupportPack {
  retrieval_guides?: ResearchRetrievalGuide[];
  news_clusters?: ResearchNewsCluster[];
  market_context?: ResearchMarketContext | null;
  thesis_drift?: ThesisDriftSupport | null;
  drafting_support?: ResearchDraftingSupport | null;
  logic_map?: ResearchLogicMap | null;
  sentiment_annotation?: ResearchSentimentAnnotation | null;
}

export type CompareDecisionWinner =
  | 'candidate_a'
  | 'candidate_b'
  | 'tie'
  | 'depends'
  | 'not_applicable'
  | 'no_clear_winner'
  | string;

export interface CompareSubstitutionAssessment {
  status: string;
  are_true_substitutes: boolean;
  summary: string;
  reason: string;
  confidence: 'high' | 'medium' | 'low' | string;
}

export interface CompareWinnerSummary {
  best_overall: CompareDecisionWinner;
  investment_winner: CompareDecisionWinner;
  deployment_winner: CompareDecisionWinner;
  evidence_winner: CompareDecisionWinner;
  timing_winner: CompareDecisionWinner;
  sleeve_winner: CompareDecisionWinner;
  portfolio_winner: CompareDecisionWinner;
  summary: string;
  where_loser_wins: string | null;
}

export interface CompareDecisionRule {
  primary_rule: string;
  choose_candidate_a_if: string;
  choose_candidate_b_if: string;
  do_not_treat_as_substitutes_if: string;
  next_action: string;
}

export interface CompareDecisionDeltaRow {
  row_id: string;
  label: string;
  candidate_a_value: string;
  candidate_b_value: string;
  winner: CompareDecisionWinner;
  implication: string;
}

export interface ComparePortfolioConsequenceSide {
  candidate_id: CandidateAssessmentId | string;
  symbol: string;
  portfolio_effect: string;
  concentration_effect: string;
  region_exposure_effect: string;
  currency_or_trading_line_effect: string;
  overlap_effect: string;
  sleeve_mandate_effect: string;
  diversification_effect: string;
  funding_path_effect: string;
  target_allocation_drift_effect: string;
  confidence: 'high' | 'medium' | 'low' | string;
}

export interface CompareScenarioWinner {
  scenario: string;
  candidate_a_effect: string;
  candidate_b_effect: string;
  winner: CompareDecisionWinner;
  why: string;
}

export interface CompareFlipCondition {
  condition: string;
  current_state: string;
  flips_toward: CompareDecisionWinner;
  threshold_or_trigger: string;
}

export interface CompareEvidenceDiff {
  stronger_evidence: CompareDecisionWinner;
  unresolved_fields: string[];
  candidate_a_weak_fields: string[];
  candidate_b_weak_fields: string[];
  evidence_needed_to_decide: string[];
}

export interface CompareDecisionReadModel {
  compare_id: string;
  sleeve_id?: SleeveAssessmentId | string | null;
  candidate_a_id: CandidateAssessmentId | string;
  candidate_b_id: CandidateAssessmentId | string;
  substitution_assessment: CompareSubstitutionAssessment;
  winner_summary: CompareWinnerSummary;
  decision_rule: CompareDecisionRule;
  delta_table: CompareDecisionDeltaRow[];
  portfolio_consequence: {
    candidate_a: ComparePortfolioConsequenceSide | null;
    candidate_b: ComparePortfolioConsequenceSide | null;
  };
  scenario_winners: CompareScenarioWinner[];
  flip_conditions: CompareFlipCondition[];
  evidence_diff: CompareEvidenceDiff;
}

export interface CompareContract extends V2ContractBase {
  surface_id: 'compare';
  compare_ids?: Array<CandidateAssessmentId | string>;
  sleeve_id?: SleeveAssessmentId | string | null;
  sleeve_name?: string | null;
  candidates?: CompareCandidateSnapshot[];
  leader_candidate_id?: CandidateAssessmentId | string | null;
  compare_readiness_state?: string | null;
  compare_readiness_note?: string | null;
  substitution_verdict?: string | null;
  substitution_rationale?: string | null;
  substitution_answer?: string | null;
  winner_for_sleeve_job?: string | null;
  loser_weakness_summary?: string | null;
  change_the_read_summary?: string | null;
  compare_investor_summary?: string | null;
  compare_dimensions?: CompareDimension[];
  dimension_groups?: Array<{
    group_id: string;
    label: string;
    dimension_ids: string[];
  }>;
  dimension_priority_order?: string[];
  discriminating_dimension_ids?: string[];
  insufficient_dimensions?: string[];
  candidate_a_id: CandidateId | string;
  candidate_b_id: CandidateId | string;
  candidate_a_name: string;
  candidate_b_name: string;
  who_leads: CandidateId | string;
  /** Display name for the winning candidate — backend-authoritative, do not resolve from ID. */
  winner_name: string;
  why_leads: string;
  where_loser_wins: string | null;
  what_would_change_comparison: string | null;
  forecast_support?: ForecastSupport | null;
  flip_risk_note?: string | null;
  path_asymmetry?: number | null;
  downside_asymmetry?: number | null;
  stability_advantage?: CandidateId | string | 'tie' | null;
  market_path_compare_note?: string | null;
  compare_decision?: CompareDecisionReadModel | null;
  compare_summary?: {
    cleaner_for_sleeve_job?: string | null;
    main_separation?: string | null;
    change_trigger?: string | null;
  } | null;
  dimensions: CompareDimension[];
}

export interface CompareDimension {
  dimension_id?: string;
  dimension: string;
  label?: string;
  group?: string;
  discriminating?: boolean;
  importance?: string | null;
  rationale?: string | null;
  values?: CompareDimensionValue[];
  a_value: string;
  b_value: string;
  winner: CandidateId | string | 'tie';
}

export interface ChangesContract extends V2ContractBase {
  surface_id: string;
  change_events: ChangeEventRow[];
  net_impact: 'material' | 'minor' | 'none';
  since_utc: string | null;
  window?: 'today' | '3d' | '7d' | null;
  timezone?: string | null;
  effective_since_utc?: string | null;
  daily_source_scan?: BlueprintDailySourceScan | null;
  summary?: ChangesSummary | null;
  audit_groups?: ChangesAuditGroup[];
  available_sleeves?: ChangesAvailableSleeve[];
  available_categories?: string[];
  feed_freshness_state?: 'current' | 'stale' | 'empty' | 'degraded_runtime' | string | null;
  latest_event_at?: string | null;
  latest_event_age_days?: number | null;
  filters_applied?: ChangesFiltersApplied | null;
  pagination?: ChangesPagination | null;
}

export interface BlueprintDailySourceScan {
  trading_day: string;
  timezone: string;
  started_at: string | null;
  finished_at: string | null;
  status: 'success' | 'partial' | 'failed' | 'not_run' | string;
  source_freshness_state: 'fresh' | 'stale' | 'degraded_runtime' | 'empty' | string;
  emitted_event_count: number;
  material_candidate_count: number;
  no_material_change: boolean;
  latest_scan_at: string | null;
  failure_reasons: string[];
}

export interface ChangeEventRow {
  event_id: ChangeEventId | string;
  event_type: string;
  summary: string;
  changed_at_utc: string;
  candidate_id: string | null;
  sleeve_id?: string | null;
  sleeve_name?: string | null;
  change_trigger?: string | null;
  reason_summary?: string | null;
  occurred_at?: string | null;
  effective_at?: string | null;
  surface?: string | null;
  category?: string | null;
  severity?: V2ImpactLevel | string | null;
  actionability?: 'act_now' | 'review' | 'monitor' | 'no_action' | string | null;
  scope?: 'candidate' | 'sleeve' | 'portfolio' | 'system' | string | null;
  confidence?: 'high' | 'medium' | 'low' | string | null;
  freshness_state?: string | null;
  symbol?: string | null;
  title?: string | null;
  state_transition?: {
    from?: string | null;
    to?: string | null;
  } | null;
  score_delta?: {
    from?: number | null;
    to?: number | null;
    driver?: string | null;
  } | null;
  market_context?: Record<string, unknown> | null;
  portfolio_context?: Record<string, unknown> | null;
  why_it_matters?: string | null;
  next_step?: string | null;
  evidence_refs?: string[];
  source_freshness?: {
    state?: string | null;
    latest_event_age_days?: number | null;
  } | null;
  trading_day?: string | null;
  driver?: {
    family?: string | null;
    name?: string | null;
    previous_value?: number | string | null;
    current_value?: number | string | null;
    change?: number | string | null;
    threshold?: number | string | null;
  } | null;
  affected?: {
    candidate_id?: string | null;
    symbol?: string | null;
    sleeve_id?: string | null;
  } | null;
  implication?: {
    summary?: string | null;
    why_it_matters?: string | null;
    next_step?: string | null;
    reversal_condition?: string | null;
  } | null;
  previous_state: string | null;
  current_state: string | null;
  implication_summary: string | null;
  portfolio_consequence: string | null;
  next_action: string | null;
  what_would_reverse: string | null;
  requires_review: boolean;
  report_tab: string | null;
  impact_level: V2ImpactLevel | null;
  ui_category?: string | null;
  direction?: 'upgrade' | 'downgrade' | 'neutral' | string | null;
  is_blocker_change?: boolean;
  is_current?: boolean | null;
  event_age_hours?: number | null;
  source_scan_status?: string | null;
  closure_status?: string | null;
  materiality_status?: string | null;
  materiality_class?: 'investor_material' | 'review_material' | 'audit_only' | 'system_only' | 'suppressed' | string | null;
  materiality_reason?: string | null;
  render_mode?: 'full_investor_explanation' | 'compact_material' | 'grouped_audit' | 'hidden_audit' | 'suppressed' | 'full_investor' | 'compact_audit' | string | null;
  driver_packet?: ChangesDriverPacket | null;
  primary_trigger?: ChangesPrimaryTrigger | null;
  candidate_impact?: ChangesCandidateImpact | null;
  audit_detail?: ChangesAuditDetail | null;
  missing_driver_reason?: string | null;
  deep_link_target?: {
    target_type: string;
    target_id: string;
    tab?: string | null;
    section?: string | null;
    anchor?: string | null;
  } | null;
  change_detail?: ChangeDetail | null;
}

export interface ChangeDetail {
  event_id: ChangeEventId | string;
  summary: string;
  state_transition: {
    from?: string | null;
    to?: string | null;
  };
  driver_kind?: string | null;
  driver_label?: string | null;
  trigger?: string | null;
  source_evidence?: string | null;
  reason?: string | null;
  portfolio_consequence?: string | null;
  next_action?: string | null;
  reversal_condition?: string | null;
  reversal_conditions?: string | null;
  closure_status?: 'open_actionable' | 'open_review' | 'closed_no_action' | 'suppressed_not_material' | 'stale_historical' | 'unresolved_driver_missing' | string | null;
  materiality_status?: 'material' | 'material_source_backed' | 'material_portfolio_backed' | 'historical_source_backed' | 'suppressed_not_material' | 'unresolved_driver_missing' | 'raw_movement_only' | string | null;
  materiality_class?: 'investor_material' | 'review_material' | 'audit_only' | 'system_only' | 'suppressed' | string | null;
  materiality_reason?: string | null;
  render_mode?: 'full_investor_explanation' | 'compact_material' | 'grouped_audit' | 'hidden_audit' | 'suppressed' | 'full_investor' | 'compact_audit' | string | null;
  driver_packet?: ChangesDriverPacket | null;
  primary_trigger?: ChangesPrimaryTrigger | null;
  candidate_impact?: ChangesCandidateImpact | null;
  audit_detail?: ChangesAuditDetail | null;
  missing_driver_reason?: string | null;
  is_current?: boolean | null;
  event_age_hours?: number | null;
  source_scan_status?: string | null;
  score_delta: {
    from?: number | null;
    to?: number | null;
  };
  affected_candidate: {
    candidate_id?: string | null;
    symbol?: string | null;
    sleeve_id?: string | null;
  };
  evidence_refs: string[];
  source_freshness: {
    state: 'current' | 'stale' | 'last_good' | 'degraded_runtime' | 'unknown' | string;
    latest_event_at?: string | null;
  };
  links: {
    candidate_recommendation_href?: string | null;
    report_href?: string | null;
  };
}

export interface ChangesDriverPacket {
  driver_type?: 'source' | 'market' | 'portfolio' | 'timing' | 'blocker' | 'score' | 'policy' | 'system' | 'unknown' | string | null;
  driver_name?: string | null;
  driver_summary?: string | null;
  source_ref?: string | null;
  previous_value?: string | number | null;
  current_value?: string | number | null;
  threshold?: string | number | null;
  materiality?: 'high' | 'medium' | 'low' | 'audit' | string | null;
  confidence?: 'high' | 'medium' | 'low' | string | null;
  preserved?: boolean;
}

export interface ChangesPrimaryTrigger {
  trigger_type?: 'market' | 'portfolio' | 'source' | 'timing' | 'blocker' | 'score' | 'policy' | 'unknown' | string | null;
  driver_family?: string | null;
  driver_name?: string | null;
  display_label?: string | null;
  previous_value?: string | number | null;
  current_value?: string | number | null;
  change_value?: string | number | null;
  unit?: string | null;
  threshold?: string | number | null;
  observed_at?: string | null;
  source_ref?: string | null;
  freshness_state?: 'current' | 'stale' | 'last_good' | 'degraded_runtime' | 'unknown' | string | null;
  confidence?: 'high' | 'medium' | 'low' | string | null;
  materiality?: 'high' | 'medium' | 'low' | string | null;
  preserved?: boolean;
}

export interface ChangesCandidateImpact {
  affected_candidate_id?: string | null;
  symbol?: string | null;
  sleeve_id?: string | null;
  impact_direction?: 'strengthened' | 'weakened' | 'blocked' | 'cleared' | 'neutral' | string | null;
  affected_dimension?:
    | 'recommendation'
    | 'deployability'
    | 'portfolio_fit'
    | 'source_confidence'
    | 'timing'
    | 'market_path'
    | 'blocker'
    | 'policy'
    | string
    | null;
  before_state?: string | null;
  after_state?: string | null;
  score_before?: number | null;
  score_after?: number | null;
  why_it_matters?: string | null;
  next_action?: string | null;
  reversal_condition?: string | null;
}

export interface ChangesAuditDetail {
  audit_summary: string;
  missing_driver_reason?: string | null;
  original_event_type?: string | null;
  original_transition?: string | null;
  grouped_count?: number | null;
}

export interface ChangesSummary {
  total_changes: number;
  upgrades: number;
  downgrades: number;
  blocker_changes: number;
  requires_review: number;
  material_changes?: number;
  material_upgrades?: number;
  material_downgrades?: number;
  audit_only_count?: number;
  suppressed_count?: number;
  no_material_change?: boolean;
}

export interface ChangesAuditGroup {
  group_id: string;
  title: string;
  count: number;
  summary: string;
  missing_driver_reason?: string | null;
  render_mode?: 'grouped_audit' | string | null;
  materiality_class?: 'audit_only' | string | null;
  events_returned?: number;
  has_more_events?: boolean;
  events?: Array<{
    event_id?: string | null;
    ticker?: string | null;
    sleeve_id?: string | null;
    sleeve_name?: string | null;
    event_type?: string | null;
    category?: string | null;
    changed_at_utc?: string | null;
    event_age_hours?: number | null;
    closure_status?: string | null;
    materiality_status?: string | null;
    materiality_class?: string | null;
    transition?: {
      from?: string | null;
      to?: string | null;
    } | null;
    missing_driver_reason?: string | null;
  }>;
}

export interface ChangesAvailableSleeve {
  sleeve_id?: string | null;
  sleeve_name: string;
  count: number;
}

export interface ChangesFiltersApplied {
  category?: string | null;
  sleeve_id?: string | null;
  candidate_id?: string | null;
  needs_review?: boolean | null;
  limit?: number | null;
  cursor?: string | null;
  timezone?: string | null;
}

export interface ChangesPagination {
  limit?: number | null;
  returned: number;
  total_matching: number;
  has_more: boolean;
  next_cursor?: string | null;
}

/** Sleeve row in the Blueprint Explorer — includes full candidate roster. */
export type BlueprintSleeveRow = {
  sleeve_id: SleeveAssessmentId | string;
  sleeve_name?: string;
  sleeve_purpose: string;
  sleeve_role_statement?: string | null;
  cycle_sensitivity?: string | null;
  base_allocation_rationale?: string | null;
  target_pct?: number | null;
  target_display?: string | null;
  min_pct: number;
  max_pct: number;
  sort_midpoint_pct: number;
  is_nested: boolean;
  parent_sleeve_id?: string | null;
  parent_sleeve_name?: string | null;
  counts_as_top_level_total: boolean;
  target_label: string;
  range_label: string;
  pressure_level: V2PressureLevel;
  capital_memo: string;
  priority_rank?: number;
  current_weight?: string | null;
  target_weight?: string | null;
  candidate_count?: number;
  reopen_condition: string | null;
  lead_candidate_id: CandidateAssessmentId | string | null;
  lead_candidate_name: string | null;
  visible_state: V2VisibleState;
  sleeve_actionability_state?: 'ready' | 'reviewable' | 'bounded' | 'blocked' | string;
  sleeve_reviewability_state?: 'reviewable' | 'bounded' | 'blocked' | string;
  sleeve_block_reason_summary?: string | null;
  blocked_count?: number;
  reviewable_count?: number;
  bounded_count?: number;
  ready_count?: number;
  active_support_candidate_count?: number;
  leader_is_blocked_but_sleeve_still_reviewable?: boolean;
  failure_class_summary?: BlueprintFailureClassSummary | null;
  implication_summary: string;
  why_it_leads: string;
  main_limit: string;
  recommendation_score?: {
    average_score: number;
    pillar_count_used: number;
    factor_count_used?: number | null;
    score_basis: 'support_pillars_average' | 'deployment_score' | 'recommendation_score';
    leader_candidate_recommendation_score?: number | null;
    leader_truth_confidence_score?: number | null;
    leader_candidate_deployability_score?: number | null;
    leader_candidate_investment_merit_score?: number | null;
    leader_candidate_deployment_score?: number | null;
    depth_score?: number | null;
    sleeve_actionability_score?: number | null;
    blocker_burden_score?: number | null;
    tone: 'good' | 'warn' | 'bad' | 'neutral';
    label: string;
  } | null;
  support_pillars?: Array<{
    pillar_id: string;
    label: string;
    score: number;
    note: string;
    tone: 'good' | 'warn' | 'bad' | 'neutral' | 'info';
  }>;
  funding_path?: {
    funding_source: string | null;
    incumbent_label: string | null;
    action_boundary: string | null;
    degraded_state?: string | null;
    summary?: string | null;
  } | null;
  forecast_watch?: ForecastSupport | null;
  candidates: CandidateRowExpanded[];
};

export interface OverlayContext {
  state: string;
  summary: string;
}

export interface BlueprintExplorerSummary {
  active_candidate_count: number;
  active_support_candidate_count: number;
  sleeve_count: number;
}

export interface BlueprintExplorerContract extends V2ContractBase {
  surface_id: 'blueprint_explorer';
  header_badges: SurfaceBadgeContract[];
  sleeves: BlueprintSleeveRow[];
  summary?: BlueprintExplorerSummary | null;
  market_state_summary: string;
  review_posture: V2ReviewPosture;
}

export type BaselineComparison = {
  label: string;
  summary: string;
  verdict: string | null;
};

export interface DailyBriefMarketStateCard {
  card_id: string;
  label: string;
  value: string;
  note: string;
  tone: 'good' | 'warn' | 'bad' | 'neutral' | 'info';
  runtime_provenance?: RuntimeSourceProvenance | null;
  current_value?: number | null;
  change_pct_1d?: number | null;
  caption?: string | null;
  sub_caption?: string | null;
  as_of?: string | null;
  source_provider?: string | null;
  source_type?: string | null;
  source_authority_tier?: string | null;
  metric_definition?: string | null;
  metric_polarity?: string | null;
  is_exact?: boolean | null;
  validation_status?: string | null;
  validation_reason?: string | null;
  freshness_mode?: string | null;
  primary_provider?: string | null;
  cross_check_provider?: string | null;
  cross_check_status?: string | null;
  authority_gap_reason?: string | null;
  session_relevance_state?: string | null;
  observation_age_hours?: number | null;
  session_relevance_window_hours?: number | null;
}

export interface DailyBriefMonitoringCondition {
  condition_id: string;
  label: string;
  why_now: string;
  path_risk_note?: string | null;
  near_term_trigger: string;
  thesis_trigger: string;
  break_condition: string;
  portfolio_consequence: string;
  next_action: string;
  affected_sleeve?: string | null;
  affected_candidates?: string[];
  effect_type?: string | null;
  source_kind?: string | null;
  confidence_class?: string | null;
  sufficiency_state?: string | null;
  forecast_support?: ForecastSupport | null;
  trigger_support?: ForecastTriggerSupport | null;
}

export interface DailyBriefPortfolioImpactRow {
  object_id: string;
  object_label: string;
  object_type: string;
  mapping: string;
  status_label: string;
  consequence: string;
  next_step: string;
}

export interface DailyBriefReviewTrigger {
  trigger_id: string;
  lane: 'review_now' | 'monitor' | 'do_not_act_yet';
  label: string;
  reason: string;
}

export interface DailyBriefScenarioVariant {
  scenario_id: string;
  type: 'bull' | 'base' | 'bear';
  label: string;
  scenario_name?: string | null;
  path_statement?: string | null;
  timing_window?: string | null;
  scenario_likelihood_pct?: number | null;
  sleeve_consequence?: string | null;
  action_boundary?: string | null;
  upgrade_trigger?: string | null;
  downgrade_trigger?: string | null;
  support_strength?: string | null;
  regime_note?: string | null;
  confirmation_note?: string | null;
  portfolio_effect: string;
  lead_sentence?: string | null;
  action_consequence?: string | null;
  path_meaning?: string | null;
  trigger_state?: string | null;
  path_bias?: string | null;
  confirm_probability?: string | null;
  break_probability?: string | null;
  threshold_breach_risk?: string | null;
  uncertainty_width?: string | null;
  persistence_vs_reversion?: string | null;
  evidence_state?: string | null;
  macro: string | null;
  micro: string | null;
  short_term: string | null;
  long_term: string | null;
}

export interface DailyBriefScenarioBlock {
  signal_id: string;
  label: string;
  summary: string;
  scenarios: DailyBriefScenarioVariant[];
  forecast_support?: ForecastSupport | null;
  what_confirms?: string | null;
  what_breaks?: string | null;
  threshold_summary?: string | null;
  degraded_state?: string | null;
}

export interface DailyBriefContingentDriver {
  driver_id: string;
  label: string;
  trigger_title?: string | null;
  effect_type: string | null;
  why_it_matters_now: string;
  what_changes_if_confirmed?: string | null;
  trigger_condition?: string | null;
  what_to_watch_next?: string | null;
  current_status?: string | null;
  affected_sleeves?: string[];
  support_label?: string | null;
  supporting_lines?: string[];
  path_risk_note?: string | null;
  near_term_trigger: string;
  thesis_trigger: string;
  break_condition: string;
  affected_sleeve: string | null;
  portfolio_consequence: string;
  next_action: string;
  confidence_class?: string | null;
  sufficiency_state?: string | null;
  source_kind?: string | null;
  forecast_support?: ForecastSupport | null;
}

export interface DailyBriefSignalGroup {
  group_id: string;
  label: string;
  summary: string;
  representative?: SignalCardV2 | null;
  count: number;
  signals: SignalCardV2[];
}

export interface DailyBriefMetricBar {
  label: string;
  score: number;
  tone: 'good' | 'warn' | 'bad' | 'neutral' | 'info';
}

export interface ChartPointContract {
  timestamp: string;
  value: number;
}

export interface ChartSeriesContract {
  chart_id: string;
  series_type: 'line' | 'area' | 'histogram';
  label: string;
  points: ChartPointContract[];
  unit: string;
  source_family: string;
  source_label: string;
  freshness_state: string;
  trust_state: string;
}

export interface ChartBandContract {
  band_id: string;
  label: string;
  upper_points: ChartPointContract[];
  lower_points: ChartPointContract[];
  meaning: string;
  degraded_state: string | null;
}

export interface ChartMarkerContract {
  marker_id: string;
  timestamp: string;
  label: string;
  marker_type: string;
  linked_object_id: string | null;
  linked_surface: string | null;
  summary: string;
}

export interface ChartThresholdContract {
  threshold_id: string;
  label: string;
  value: number;
  threshold_type: string;
  action_if_crossed: string;
  what_it_means: string;
}

export interface ChartCalloutContract {
  callout_id: string;
  label: string;
  tone: string;
  detail: string;
}

export interface ChartLogicContract {
  current_value: number | null;
  previous_value?: number | null;
  trigger_level?: number | null;
  confirm_above?: boolean | null;
  break_below?: boolean | null;
  bands?: Array<{ label: string; min: number | null; max: number | null }>;
  current_band?: string | null;
  release_date?: string | null;
  as_of_date: string;
  /** decomposition family: each element is {label, value, target, low, high, unit} */
  allocation_bars?: Array<{
    label: string; value: number; target: number; low: number; high: number; unit: string;
  }>;
}

export interface ChartPanelContract {
  panel_id: string;
  title: string;
  chart_type: string;
  chart_mode?: string;
  primary_series: ChartSeriesContract | null;
  comparison_series?: ChartSeriesContract | null;
  bands?: ChartBandContract[];
  markers?: ChartMarkerContract[];
  thresholds?: ChartThresholdContract[];
  callouts?: ChartCalloutContract[];
  summary: string;
  what_to_notice: string;
  degraded_state: string | null;
  freshness_state: string;
  trust_state: string;
  chart_logic?: ChartLogicContract | null;
}

export interface CandidateReportMarketHistoryBlock {
  summary: string;
  sparkline_points: number[];
  benchmark_note: string | null;
  regime_windows: Array<{
    label: string;
    period: string;
    fund_return: string;
    benchmark_return: string;
    note: string;
  }>;
  field_provenance?: Record<string, RuntimeSourceProvenance | null>;
}

export interface CandidateReportScenarioBlock {
  type: 'bull' | 'base' | 'bear';
  label: string;
  trigger: string;
  expected_return: string;
  portfolio_effect: string;
  short_term: string | null;
  long_term: string | null;
  forecast_support?: ForecastSupport | null;
  what_confirms?: string | null;
  what_breaks?: string | null;
  degraded_state?: string | null;
}

export interface CandidateReportRiskBlock {
  category: string;
  title: string;
  detail: string;
}

export interface CandidateReportCompetitionBlock {
  label: string;
  summary: string;
  verdict: string | null;
}

export interface CandidateReportEvidenceSource {
  source_id: string;
  label: string;
  url: string | null;
  freshness_state: string;
  directness: string;
  truth_envelope?: TruthEnvelope | null;
}

export interface CandidateReportDecisionThreshold {
  label: string;
  value: string;
  forecast_support?: ForecastSupport | null;
  trigger_type?: string | null;
  threshold_state?: string | null;
}

export interface CandidateDecisionConditionItem {
  kind: 'upgrade' | 'downgrade' | 'kill';
  label: string;
  text: string;
  support_text?: string | null;
  confirmation_label?: string | null;
  confirmation_points?: string[] | null;
  confidence?: 'high' | 'medium' | 'low' | string | null;
  basis_labels?: string[];
}

export interface CandidateDecisionConditionPack {
  intro: string;
  upgrade?: CandidateDecisionConditionItem | null;
  downgrade?: CandidateDecisionConditionItem | null;
  kill?: CandidateDecisionConditionItem | null;
}

export interface CandidateQuickBriefFact {
  label: string;
  value: string;
}

export interface CandidateQuickBriefLine {
  label: string;
  value: string;
}

export interface CandidateQuickBriefIdentity {
  ticker: string;
  name: string;
  issuer?: string | null;
  exposure_label?: string | null;
}

export interface CandidateQuickBriefDecisionGuide {
  best_for: string;
  not_ideal_for: string;
  use_it_when: string;
  wait_if: string;
  compare_against: string;
}

export interface CandidateQuickBriefCheck {
  check_id: string;
  label: string;
  summary: string;
  metric?: string | null;
}

export interface CandidateQuickBriefPortfolioFit {
  role_in_portfolio: string;
  what_it_does_not_solve: string;
  current_need: string;
}

export interface CandidateQuickBriefEvidenceFooter {
  evidence_quality: string;
  data_completeness: string;
  document_support: string;
  monitoring_status: string;
}

export interface CandidateQuickBriefScenarioEntry {
  backdrop_summary: string;
  disclosure_label: string;
}

export interface CandidateQuickBriefKronosMarketSetup {
  scope_key?: string | null;
  scope_label?: string | null;
  market_setup_state?: string | null;
  route_label?: string | null;
  forecast_object_label?: string | null;
  horizon_label?: string | null;
  path_support_label?: string | null;
  confidence_label?: string | null;
  freshness_label?: string | null;
  downside_risk_label?: string | null;
  drift_label?: string | null;
  volatility_regime_label?: string | null;
  decision_impact_text?: string | null;
  quality_gate?: string | null;
  as_of?: string | null;
  scenario_available?: boolean | null;
  open_scenario_cta?: string | null;
}

export interface CandidateQuickBriefKronosDecisionBridge {
  selection_context?: string | null;
  regime_summary?: string | null;
  selection_consequence?: string | null;
  wrapper_boundary_text?: string | null;
  supports_exposure_choice?: boolean | null;
  supports_wrapper_choice?: boolean | null;
  decision_strength_label?: string | null;
}

export interface CandidateQuickBriefKronosCompareCheck {
  compare_context?: string | null;
  regime_check_text?: string | null;
  affects_peer_preference?: boolean | null;
  affects_exposure_preference?: boolean | null;
}

export interface CandidateQuickBriefKronosScenarioPack {
  observed_path?: string | null;
  base_path?: string | null;
  downside_path?: string | null;
  stress_path?: string | null;
  uncertainty_band?: string | null;
  drift_state?: string | null;
  fragility_state?: string | null;
  threshold_flags?: string[];
  quality_gate?: string | null;
  provenance?: string | null;
  refresh_status?: string | null;
  last_run_at?: string | null;
}

export interface CandidateQuickBriefKronosOptionalMetrics {
  upside_probability?: string | null;
  downside_breach_probability?: string | null;
  volatility_elevation_probability?: string | null;
  change_vs_prior_run?: string | null;
}

export interface CandidateQuickBriefPeerDocument {
  label: string;
  url?: string | null;
}

export interface CandidateQuickBriefPeerCompareRow {
  role: 'subject' | 'same_job_peer' | 'broader_control' | string;
  fund_name: string;
  ticker_or_line?: string | null;
  isin?: string | null;
  benchmark?: string | null;
  benchmark_family?: string | null;
  exposure_scope?: string | null;
  developed_only?: boolean | null;
  emerging_markets_included?: boolean | null;
  ter?: string | null;
  fund_assets?: string | null;
  share_class_assets?: string | null;
  holdings_count?: string | null;
  replication?: string | null;
  distribution?: string | null;
  domicile?: string | null;
  launch_date?: string | null;
  tracking_error_1y?: string | null;
  tracking_error_3y?: string | null;
  tracking_error_5y?: string | null;
  tracking_difference_1y?: string | null;
  tracking_difference_3y?: string | null;
  listing_exchange?: string | null;
  listing_currency?: string | null;
  primary_document_links?: CandidateQuickBriefPeerDocument[];
  why_this_peer_matters?: string | null;
  ter_delta?: string | null;
  fund_assets_delta?: string | null;
  holdings_delta?: string | null;
  tracking_error_1y_delta?: string | null;
  same_index?: boolean | null;
  same_job?: boolean | null;
  same_distribution?: boolean | null;
  same_domicile?: boolean | null;
}

export interface CandidateQuickBriefPeerComparePack {
  candidate_symbol: string;
  candidate_label: string;
  primary_question?: string | null;
  comparison_basis?: string | null;
  rows: CandidateQuickBriefPeerCompareRow[];
}

export interface CandidateQuickBriefFundProfile {
  objective?: string | null;
  benchmark?: string | null;
  benchmark_family?: string | null;
  domicile?: string | null;
  replication?: string | null;
  distribution?: string | null;
  fund_assets?: string | null;
  share_class_assets?: string | null;
  holdings_count?: string | null;
  launch_date?: string | null;
  issuer?: string | null;
  documents?: CandidateQuickBriefPeerDocument[];
}

export interface CandidateQuickBriefListingProfile {
  exchange?: string | null;
  trading_currency?: string | null;
  ticker?: string | null;
  market_price?: string | null;
  nav?: string | null;
  spread_proxy?: string | null;
  volume?: string | null;
  premium_discount?: string | null;
  as_of?: string | null;
}

export interface CandidateQuickBriefIndexScopeExplainer {
  label?: string | null;
  scope_type?: string | null;
  display_title?: string | null;
  summary?: string | null;
  covers?: string[] | null;
  does_not_cover?: string[] | null;
  sleeve_relevance?: string | null;
  specificity?: 'exact' | 'category' | 'strategy' | 'fallback' | string | null;
  source_basis?: 'candidate_registry' | 'benchmark_taxonomy' | 'sleeve_asset_class_fallback' | string | null;
  confidence?: 'high' | 'medium' | 'low' | string | null;
  index_name?: string | null;
  coverage_statement?: string | null;
  includes_statement?: string | null;
  excludes_statement?: string | null;
  market_cap_scope?: string | null;
  country_count?: string | null;
  constituent_count?: string | null;
  emerging_markets_included?: boolean | null;
}

export interface CandidateQuickBriefDecisionProofPack {
  why_candidate_exists?: string | null;
  why_in_scope?: string | null;
  why_not_complete_solution?: string | null;
  best_same_job_peers?: string | null;
  broader_control_peer?: string | null;
  fee_premium_question?: string | null;
  what_must_be_true_to_prefer_this?: string | null;
  what_would_change_verdict?: string | null;
}

export interface CandidateQuickBriefPerformanceTrackingPack {
  return_1y?: string | null;
  return_3y?: string | null;
  return_5y?: string | null;
  benchmark_return_1y?: string | null;
  benchmark_return_3y?: string | null;
  benchmark_return_5y?: string | null;
  tracking_error_1y?: string | null;
  tracking_error_3y?: string | null;
  tracking_error_5y?: string | null;
  tracking_difference_current_period?: string | null;
  tracking_difference_1y?: string | null;
  tracking_difference_3y?: string | null;
  tracking_difference_5y?: string | null;
  volatility?: string | null;
  max_drawdown?: string | null;
  as_of?: string | null;
}

export interface CandidateQuickBriefCompositionWeight {
  label: string;
  value: string;
}

export interface CandidateQuickBriefCompositionPack {
  number_of_stocks?: string | null;
  top_holdings?: CandidateQuickBriefCompositionWeight[];
  country_weights?: CandidateQuickBriefCompositionWeight[];
  sector_weights?: CandidateQuickBriefCompositionWeight[];
  top_10_weight?: string | null;
  us_weight?: string | null;
  non_us_weight?: string | null;
  em_weight?: string | null;
}

export interface CandidateQuickBriefDocumentCoverage {
  factsheet_present?: boolean | null;
  kid_present?: boolean | null;
  prospectus_present?: boolean | null;
  annual_report_present?: boolean | null;
  benchmark_methodology_present?: boolean | null;
  last_refreshed_at?: string | null;
  document_count?: number | null;
  missing_documents?: string[];
  document_confidence_grade?: string | null;
}

export interface CandidateQuickBrief {
  status_state: 'eligible' | 'watchlist' | 'research_only' | 'blocked' | string;
  status_label: string;
  fund_identity?: CandidateQuickBriefIdentity | null;
  portfolio_role?: string | null;
  role_label?: string | null;
  summary: string;
  decision_reasons?: string[];
  secondary_reasons: string[];
  key_facts: CandidateQuickBriefFact[];
  why_this_matters?: string | null;
  compare_first?: string | null;
  broader_alternative?: string | null;
  what_it_solves?: string | null;
  what_it_still_needs_to_prove?: string | null;
  decision_readiness?: string | null;
  should_i_use?: CandidateQuickBriefDecisionGuide | null;
  performance_checks?: CandidateQuickBriefCheck[];
  what_you_are_buying?: CandidateQuickBriefFact[];
  portfolio_fit?: CandidateQuickBriefPortfolioFit | null;
  how_to_decide?: string[];
  evidence_footer_detail?: CandidateQuickBriefEvidenceFooter | null;
  scenario_entry?: CandidateQuickBriefScenarioEntry | null;
  kronos_market_setup?: CandidateQuickBriefKronosMarketSetup | null;
  kronos_decision_bridge?: CandidateQuickBriefKronosDecisionBridge | null;
  kronos_compare_check?: CandidateQuickBriefKronosCompareCheck | null;
  kronos_scenario_pack?: CandidateQuickBriefKronosScenarioPack | null;
  kronos_optional_metrics?: CandidateQuickBriefKronosOptionalMetrics | null;
  peer_compare_pack?: CandidateQuickBriefPeerComparePack | null;
  fund_profile?: CandidateQuickBriefFundProfile | null;
  listing_profile?: CandidateQuickBriefListingProfile | null;
  index_scope_explainer?: CandidateQuickBriefIndexScopeExplainer | null;
  decision_proof_pack?: CandidateQuickBriefDecisionProofPack | null;
  performance_tracking_pack?: CandidateQuickBriefPerformanceTrackingPack | null;
  composition_pack?: CandidateQuickBriefCompositionPack | null;
  document_coverage?: CandidateQuickBriefDocumentCoverage | null;
  why_it_matters: CandidateQuickBriefLine[];
  performance_and_implementation: CandidateQuickBriefLine[];
  overlay_note?: string | null;
  backdrop_note?: string | null;
  evidence_footer: CandidateQuickBriefLine[];
}

export interface ForecastSupport {
  provider: string;
  model_name: string;
  horizon: number;
  support_strength: string;
  confidence_summary: string;
  degraded_state: string | null;
  generated_at: string;
  persistence_score?: number | null;
  fade_risk?: number | null;
  trigger_distance?: number | null;
  trigger_pressure?: number | null;
  path_asymmetry?: number | null;
  uncertainty_width_score?: number | null;
  uncertainty_width_label?: string | null;
  regime_alignment_score?: number | null;
  cross_asset_confirmation_score?: number | null;
  scenario_support_strength?: string | null;
  escalation_flag?: boolean | null;
}

export interface BlueprintMarketPathPoint {
  timestamp: string;
  value: number;
  label?: string | null;
}

export interface BlueprintMarketPathBand {
  label: string;
  lower_points: BlueprintMarketPathPoint[];
  upper_points: BlueprintMarketPathPoint[];
}

export interface BlueprintSeriesQualitySummary {
  bars_expected: number;
  bars_present: number;
  missing_bar_ratio: number;
  stale_days: number;
  has_corporate_action_uncertainty: boolean;
  uses_proxy_series: boolean;
  quality_label: string;
}

export interface BlueprintMarketPathThreshold {
  threshold_id: string;
  label: string;
  value: number;
  relation: string;
  delta_pct?: number | null;
  note?: string | null;
}

export interface BlueprintMarketPathScenario {
  scenario_type: 'base' | 'downside' | 'stress' | string;
  label: string;
  summary: string;
  path: BlueprintMarketPathPoint[];
  usefulness_label?: string | null;
}

export interface BlueprintMarketPathTakeaways {
  favorable_case_survives_mild_stress: boolean;
  favorable_case_is_narrow: boolean;
  downside_damage_is_contained: boolean;
  stress_breaks_candidate_support: boolean;
}

export interface BlueprintMarketPathSupport {
  candidate_id?: string | null;
  eligibility_state: string;
  usefulness_label: string;
  suppression_reason?: string | null;
  market_setup_state?: string | null;
  freshness_state?: string | null;
  observed_series: BlueprintMarketPathPoint[];
  projected_series: BlueprintMarketPathPoint[];
  input_timestamps?: string[];
  output_timestamps?: string[];
  uncertainty_band?: BlueprintMarketPathBand | null;
  volatility_outlook?: string | null;
  path_stability?: string | null;
  path_quality_label?: string | null;
  path_quality_score?: number | null;
  candidate_fragility_label?: string | null;
  candidate_fragility_score?: number | null;
  threshold_map?: BlueprintMarketPathThreshold[];
  strengthening_threshold?: BlueprintMarketPathThreshold | null;
  weakening_threshold?: BlueprintMarketPathThreshold | null;
  current_distance_to_strengthening?: number | null;
  current_distance_to_weakening?: number | null;
  threshold_drift_direction?: string | null;
  scenario_summary?: BlueprintMarketPathScenario[];
  scenario_takeaways?: BlueprintMarketPathTakeaways | null;
  candidate_implication: string;
  generated_at: string;
  timing_state?: 'timing_ready' | 'timing_review' | 'timing_fragile' | 'timing_constrained' | 'timing_unavailable' | string | null;
  timing_label?: string | null;
  timing_reasons?: string[];
  timing_artifact_valid?: boolean | null;
  timing_artifact_schema_status?: string | null;
  timing_artifact_id?: string | null;
  timing_artifact_schema_version?: string | null;
  timing_artifact_generated_at?: string | null;
  raw_artifact_schema_status?: string | null;
  timing_confidence?: 'high' | 'medium' | 'low' | string | null;
  timing_data_source?: string | null;
  validation_status?: string | null;
  forecast_failure_reason?: string | null;
  latest_forecast_run_status?: string | null;
  latest_forecast_failure_reason?: string | null;
  direct_series_status?: string | null;
  direct_series_last_bar_age_days?: number | null;
  direct_series_depth?: number | null;
  proxy_series_status?: string | null;
  proxy_series_last_bar_age_days?: number | null;
  proxy_series_depth?: number | null;
  provider_source?: string | null;
  provider?: string | null;
  forecast_horizon: number;
  forecast_interval: string;
  quality_flags?: string[];
  model_family?: string | null;
  checkpoint?: string | null;
  tokenizer?: string | null;
  wrapper_class?: string | null;
  service_name?: string | null;
  runtime_engine?: string | null;
  driving_symbol?: string | null;
  driving_series_role?: string | null;
  uses_proxy_series?: boolean | null;
  proxy_symbol?: string | null;
  proxy_reason?: string | null;
  route_state?: string | null;
  scope_key?: string | null;
  scope_label?: string | null;
  liquidity_feature_mode?: string | null;
  volume_available?: boolean | null;
  amount_available?: boolean | null;
  sampling_summary?: {
    sampling_mode?: string | null;
    temperature?: number | null;
    top_p?: number | null;
    seed_policy?: string | null;
    seed_count?: number | null;
    sample_path_count?: number | null;
    summary_method?: string | null;
    base_path_index?: number | null;
    downside_path_index?: number | null;
    stress_path_index?: number | null;
    seed_manifest?: number[];
  } | null;
  sample_path_manifest?: Array<{
    path_index: number;
    seed: number;
    endpoint_value?: number | null;
    endpoint_delta_pct?: number | null;
  }>;
  series_quality_summary?: BlueprintSeriesQualitySummary | null;
  market_path_case_family?: string | null;
  market_path_objective?: string | null;
  market_path_case_note?: string | null;
  support_provenance?: {
    runtime_canonical_provider: string;
    history_provider_name?: string | null;
    series_role?: string | null;
    uses_proxy_series: boolean;
    recovered_by_secondary_provider: boolean;
    last_good_artifact_served: boolean;
    provider_source?: string | null;
    route_state?: string | null;
    driving_symbol?: string | null;
    proxy_symbol?: string | null;
    direct_support_unavailable_reason?: string | null;
  } | null;
  truth_manifest?: {
    model_family?: string | null;
    checkpoint?: string | null;
    service_name?: string | null;
    provider?: string | null;
    driving_symbol?: string | null;
    driving_series_role?: string | null;
    forecast_horizon?: number | null;
    forecast_interval?: string | null;
    freshness_state?: string | null;
  } | null;
  threshold_context?: {
    summary: string;
    nearest_threshold_id?: string | null;
    drift_direction?: string | null;
    strengthening?: BlueprintMarketPathThreshold | null;
    weakening?: BlueprintMarketPathThreshold | null;
    stress?: BlueprintMarketPathThreshold | null;
  } | null;
  scenario_endpoint_summary?: Array<{
    scenario_type: string;
    label: string;
    endpoint_value?: number | null;
    endpoint_timestamp?: string | null;
    summary: string;
    sample_index?: number | null;
    seed?: number | null;
    endpoint_delta_pct?: number | null;
  }>;
  model_metadata?: Record<string, unknown> | null;
}

export interface ForecastTriggerSupport {
  object_id: string;
  trigger_type: string;
  threshold: string;
  source_family: string;
  provider: string;
  current_distance_to_trigger: string;
  next_action_if_hit: string;
  next_action_if_broken: string;
  threshold_state: string;
  support_strength: string;
  confidence_summary: string;
  degraded_state: string | null;
  generated_at: string;
}

export type DailyBriefChartKind =
  | 'threshold_line'
  | 'ohlc_candlestick'
  | 'confirmation_strip'
  | 'event_reaction_strip'
  | 'comparison_bar';

export type DailyBriefChartConfirmationState = 'none' | 'partial' | 'broad' | 'resisting';
export type DailyBriefChartDensityProfile = 'rich_line' | 'compact_line' | 'strip_only' | 'suppressed';
export type DailyBriefChartTheme = 'rates' | 'credit' | 'breadth' | 'fx' | 'commodity' | 'event' | 'neutral';
export type DailyBriefChartThresholdSemanticRole =
  | 'strengthen_line'
  | 'fade_line'
  | 'stall_line'
  | 'hold_line'
  | 'break_line'
  | 'review_line';
export type DailyBriefChartThresholdOverlapMode =
  | 'separate_lines'
  | 'merge_to_zone'
  | 'hide_secondary_line_from_plot_show_in_legend';
export type DailyBriefChartThresholdRenderMode =
  | 'line'
  | 'dashed_line'
  | 'merged_zone'
  | 'legend_only';
export type DailyBriefChartForecastVisibilityMode = 'emphasized' | 'contextual' | 'disabled';
export type DailyBriefChartObjectRole =
  | 'observed_path'
  | 'forecast_path'
  | 'review_context'
  | 'decision_reference';
export type DailyBriefChartObjectType =
  | 'observed_timeseries'
  | 'forecast_timeseries'
  | 'range_zone'
  | 'reference_line';
export type DailyBriefChartTooltipRole = 'overview' | 'path_compare' | 'decision_compare';

export interface DailyBriefChartPoint {
  timestamp: string;
  value: number;
  label?: string | null;
}

export interface DailyBriefChartSeries {
  series_id: string;
  label: string;
  unit: string;
  source_label?: string | null;
  plain_language_meaning?: string | null;
  visible_by_default?: boolean;
  points: DailyBriefChartPoint[];
}

export interface DailyBriefChartLine {
  value: number | null;
  label: string;
  note?: string | null;
}

export interface DailyBriefChartZone {
  min: number | null;
  max: number | null;
  label: string;
  note?: string | null;
}

export interface DailyBriefChartThresholds {
  review_line?: DailyBriefChartLine | null;
  confirm_line?: DailyBriefChartLine | null;
  break_line?: DailyBriefChartLine | null;
  current_status_line?: DailyBriefChartLine | null;
  review_zone?: DailyBriefChartZone | null;
  trigger_zone?: DailyBriefChartZone | null;
}

export interface DailyBriefChartCurrentPoint {
  value: number | null;
  timestamp: string | null;
  label: string;
}

export interface DailyBriefChartHorizon {
  selected: string;
  available: string[];
}

export interface DailyBriefChartForecastOverlay {
  point_path: DailyBriefChartPoint[];
  lower_band?: DailyBriefChartPoint[] | null;
  upper_band?: DailyBriefChartPoint[] | null;
  horizon_label: string;
  support_strength: string;
  forecast_start_timestamp?: string | null;
  forecast_label?: string | null;
  forecast_confidence_mode?: string | null;
  forecast_relative_direction?: string | null;
  forecast_vs_current_delta?: string | null;
  forecast_vs_key_line_delta?: string | null;
  visible_by_default?: boolean;
}

export interface DailyBriefChartSummaryMetric {
  label: string;
  value: string;
  tone?: 'neutral' | 'review' | 'confirm' | 'break' | 'support' | 'warn';
}

export interface DailyBriefChartLegendItem {
  legend_id: string;
  label: string;
  style: 'line' | 'zone';
  tone: 'review' | 'confirm' | 'break' | 'status';
}

export interface DailyBriefChartStripItem {
  item_id: string;
  label: string;
  status: 'confirming' | 'resisting' | 'neutral' | 'missing';
  direction: 'up' | 'down' | 'flat';
  value_label?: string | null;
  note?: string | null;
}

export interface DailyBriefChartStrip {
  title?: string | null;
  question?: string | null;
  items: DailyBriefChartStripItem[];
}

export interface DailyBriefChartReviewBand {
  band_id: string;
  label: string;
  min: number;
  max: number;
  plain_language_meaning: string;
  visible_by_default: boolean;
  object_role?: 'review_context';
  object_type?: 'range_zone';
  lower_bound?: number;
  upper_bound?: number;
  focus_y_domain_impact?: 'tighten' | 'anchor' | 'none' | null;
  narrow_band?: boolean | null;
}

export interface DailyBriefChartThresholdLineSpec {
  id: string;
  threshold_id: string;
  label: string;
  semantic_role: DailyBriefChartThresholdSemanticRole;
  value: number;
  numeric_value: number;
  plain_language_meaning: string;
  priority: number;
  visible_by_default: boolean;
  render_mode: DailyBriefChartThresholdRenderMode;
  visual_priority?: 'primary' | 'secondary' | 'tertiary' | null;
  visible_in_overview?: boolean | null;
  visible_in_focus?: boolean | null;
  hover_enabled?: boolean | null;
  legend_enabled?: boolean | null;
  current_relation_label?: string | null;
  forecast_relation_label?: string | null;
}

export interface DailyBriefChartDistanceToThreshold {
  threshold_id: string;
  label: string;
  relation_label: string;
  relation: 'above' | 'below' | 'inside' | 'near';
  delta_value?: number | null;
}

export interface DailyBriefChartYDomain {
  min: number;
  max: number;
}

export interface DailyBriefChartFocusGroup {
  group_id: string;
  group_label: string;
  member_line_ids: string[];
  suggested_y_domain?: DailyBriefChartYDomain | null;
  primary_line_id?: string | null;
  secondary_line_ids?: string[] | null;
  can_split_from_zone: boolean;
}

export interface DailyBriefChartGuideItem {
  id: string;
  label: string;
  text: string;
  muted?: boolean;
}

export interface DailyBriefChartObservedPath extends DailyBriefChartSeries {
  object_role: 'observed_path';
  object_type: 'observed_timeseries';
}

export interface DailyBriefChartForecastBand {
  lower_band?: DailyBriefChartPoint[] | null;
  upper_band?: DailyBriefChartPoint[] | null;
}

export interface DailyBriefChartForecastPath extends DailyBriefChartSeries {
  object_role: 'forecast_path';
  object_type: 'forecast_timeseries';
  forecast_start_timestamp?: string | null;
  forecast_end_timestamp?: string | null;
  forecast_relative_direction?: string | null;
  forecast_strength_label?: string | null;
  forecast_comparison_label?: string | null;
  forecast_visibility_mode?: DailyBriefChartForecastVisibilityMode | null;
  forecast_confidence_band?: DailyBriefChartForecastBand | null;
}

export interface DailyBriefChartDecisionReference extends DailyBriefChartThresholdLineSpec {
  object_role: 'decision_reference';
  object_type: 'reference_line';
}

export interface DailyBriefChartFocusMode {
  mode_id: string;
  mode_label: string;
  primary_object_roles: DailyBriefChartObjectRole[];
  secondary_object_roles?: DailyBriefChartObjectRole[] | null;
  hidden_object_roles?: DailyBriefChartObjectRole[] | null;
  visible_object_ids?: string[] | null;
  y_domain?: DailyBriefChartYDomain | null;
  tooltip_role?: DailyBriefChartTooltipRole | null;
  legend_state?: 'overview' | 'focus' | null;
}

export interface DailyBriefChartHoverReferenceValue {
  threshold_id: string;
  label: string;
  value: number;
}

export interface DailyBriefChartHoverRelation {
  threshold_id?: string | null;
  statement: string;
  priority: number;
}

export interface DailyBriefChartHoverPayload {
  timestamp: string;
  observed_value?: number | null;
  forecast_value?: number | null;
  review_band?: {
    label: string;
    min: number;
    max: number;
  } | null;
  reference_values?: DailyBriefChartHoverReferenceValue[] | null;
  relation_statements?: DailyBriefChartHoverRelation[] | null;
  implication?: string | null;
}

export interface DailyBriefChartInspectionPoint {
  timestamp: string;
  observed_value?: number | null;
  forecast_value?: number | null;
  current_relation_status?: string | null;
  distance_to_thresholds?: DailyBriefChartDistanceToThreshold[] | null;
}

export interface DailyBriefChartPayload {
  chart_kind: DailyBriefChartKind;
  chart_question: string;
  chart_density_profile?: DailyBriefChartDensityProfile | null;
  chart_theme?: DailyBriefChartTheme | null;
  primary_series?: DailyBriefChartSeries | null;
  observed_path?: DailyBriefChartObservedPath | null;
  forecast_path?: DailyBriefChartForecastPath | null;
  review_context?: DailyBriefChartReviewBand | null;
  decision_references?: DailyBriefChartDecisionReference[] | null;
  observed_series?: DailyBriefChartSeries | null;
  forecast_series?: DailyBriefChartSeries | null;
  comparison_series?: DailyBriefChartSeries[] | null;
  thresholds?: DailyBriefChartThresholds | null;
  review_band?: DailyBriefChartReviewBand | null;
  threshold_lines?: DailyBriefChartThresholdLineSpec[] | null;
  current_point?: DailyBriefChartCurrentPoint | null;
  chart_horizon?: DailyBriefChartHorizon | null;
  forecast_overlay?: DailyBriefChartForecastOverlay | null;
  source_validity_footer?: string | null;
  chart_suppressed_reason?: string | null;
  confirmation_state?: DailyBriefChartConfirmationState | null;
  compact_chart_summary?: DailyBriefChartSummaryMetric[] | null;
  threshold_legend?: DailyBriefChartLegendItem[] | null;
  path_state?: string | null;
  chart_guide_items?: DailyBriefChartGuideItem[] | null;
  chart_explainer_lines?: string[] | null;
  chart_takeaway?: string | null;
  primary_focus_series?: string | null;
  inspectable_series_order?: string[] | null;
  distance_to_thresholds?: DailyBriefChartDistanceToThreshold[] | null;
  inspection_points?: DailyBriefChartInspectionPoint[] | null;
  current_vs_thresholds?: DailyBriefChartDistanceToThreshold[] | null;
  forecast_vs_thresholds?: DailyBriefChartDistanceToThreshold[] | null;
  nearest_threshold?: DailyBriefChartDistanceToThreshold | null;
  relation_priority_order?: string[] | null;
  active_comparison_enabled?: boolean | null;
  threshold_overlap_mode?: DailyBriefChartThresholdOverlapMode | null;
  focus_split_available?: boolean | null;
  focusable_threshold_groups?: DailyBriefChartFocusGroup[] | null;
  focus_default_group?: string | null;
  focus_y_domain?: DailyBriefChartYDomain | null;
  inspectable_thresholds?: string[] | null;
  forecast_focus_ready?: boolean | null;
  focus_reason?: string | null;
  forecast_visibility_mode?: DailyBriefChartForecastVisibilityMode | null;
  forecast_strength_label?: string | null;
  forecast_comparison_label?: string | null;
  focus_modes?: DailyBriefChartFocusMode[] | null;
  hover_payload_by_timestamp?: DailyBriefChartHoverPayload[] | null;
  current_implication_label?: string | null;
  forecast_implication_label?: string | null;
  chart_annotations?: string[];
  confirmation_strip?: DailyBriefChartStrip | null;
  event_reaction_strip?: DailyBriefChartStrip | null;
}

export interface CandidateReportContract extends V2ContractBase {
  surface_id: 'candidate_report';
  candidate_id: CandidateAssessmentId | string;
  sleeve_id: SleeveAssessmentId | string;
  sleeve_key?: string | null;
  name: string;
  benchmark_full_name?: string | null;
  exposure_summary?: string | null;
  ter_bps?: number | null;
  spread_proxy_bps?: number | null;
  aum_usd?: number | null;
  aum_state?: 'resolved' | 'stale' | 'missing' | string;
  sg_tax_posture?: CandidateTaxPosture | null;
  distribution_policy?: string | null;
  replication_risk_note?: string | null;
  current_weight_pct?: number | null;
  weight_state?: 'overlay_active' | 'overlay_absent' | string;
  investor_decision_state?: 'actionable' | 'shortlisted' | 'blocked' | 'research_only' | string;
  source_integrity_summary?: CandidateSourceIntegritySummary | null;
  failure_class_summary?: BlueprintFailureClassSummary | null;
  score_decomposition?: CandidateScoreDecomposition | null;
  score_summary?: CandidateScoreSummary | null;
  identity_state?: CandidateIdentityState | null;
  blocker_category?: string | null;
  candidate_row_summary?: string | null;
  candidate_supporting_factors?: string[];
  candidate_penalizing_factors?: string[];
  report_summary_strip?: ReportSummaryStrip | null;
  source_confidence_label?: string | null;
  coverage_status?: string | null;
  coverage_workflow_summary?: CoverageWorkflowSummary | null;
  score_rubric?: ScoreRubric | null;
  investment_case: string;
  current_implication: string;
  action_boundary: string | null;
  what_changes_view: string;
  visible_decision_state: VisibleDecisionStateBlock;
  upgrade_condition: string | null;
  downgrade_condition: string | null;
  kill_condition: string | null;
  decision_condition_pack?: CandidateDecisionConditionPack | null;
  main_tradeoffs: string[];
  baseline_comparisons: BaselineComparison[];
  evidence_depth: V2EvidenceDepth;
  mandate_boundary: string | null;
  doctrine_annotations: string[];
  report_tabs: string[];
  quick_brief?: CandidateQuickBrief | null;
  holdings_overlay: Record<string, unknown> | null;
  market_history_block?: CandidateReportMarketHistoryBlock;
  market_history_charts?: ChartPanelContract[];
  scenario_blocks?: CandidateReportScenarioBlock[];
  scenario_charts?: ChartPanelContract[];
  risk_blocks?: CandidateReportRiskBlock[];
  competition_blocks?: CandidateReportCompetitionBlock[];
  competition_charts?: ChartPanelContract[];
  evidence_sources?: CandidateReportEvidenceSource[];
  decision_thresholds?: CandidateReportDecisionThreshold[];
  forecast_support?: ForecastSupport | null;
  market_path_support?: BlueprintMarketPathSupport | null;
  market_path_support_state?: string | null;
  forecast_runtime_state?: string | null;
  forecast_artifact_state?: string | null;
  scenario_support_state?: string | null;
  market_path_objective?: string | null;
  market_path_case_note?: string | null;
  report_cache_state?: string | null;
  report_generated_at?: string | null;
  report_source_snapshot_at?: string | null;
  report_loading_hint?: ReportLoadingHint | null;
  status?: 'stale_cached' | string;
  bound_source_snapshot_id?: string | null;
  bound_source_generated_at?: string | null;
  binding_state?: string | null;
  requested_source_snapshot_id?: string | null;
  requested_source_contract_version?: string | null;
  report_build_mode?: 'snapshot_only' | 'refresh' | string;
  overlay_context?: OverlayContext | null;
  data_confidence?: string | null;
  decision_confidence?: string | null;
  implementation_profile?: CandidateImplementationProfile | null;
  recommendation_gate?: RecommendationGate | null;
  reconciliation_status?: ReconciliationStatus | null;
  source_authority_fields?: SourceAuthorityField[];
  reconciliation_report?: ReconciliationFieldStatus[];
  data_quality_summary?: DataQualitySummary | null;
  primary_document_manifest?: PrimaryDocumentRef[];
  deep_report_support_state?: {
    overall_state?: string | null;
    documents_support_state?: string | null;
    performance_support_state?: string | null;
    composition_support_state?: string | null;
    listing_support_state?: string | null;
    implementation_support_state?: string | null;
    market_path_support_state?: string | null;
    evidence_sources_support_state?: string | null;
    unresolved_sections?: string[];
    stale_sections?: string[];
    proxy_sections?: string[];
    verified_not_applicable_sections?: string[];
    unavailable_sections?: string[];
    partial_sections?: string[];
    support_reasons?: string[];
    source_completion_is_report_support?: boolean;
    coverage_status?: string | null;
    forecast_runtime_state?: string | null;
    forecast_artifact_state?: string | null;
    scenario_support_state?: string | null;
    market_path_unavailable_reason?: string | null;
    section_states?: Record<string, { state?: string | null; reason?: string | null }>;
  } | null;
  research_support?: ResearchSupportPack | null;
}

export type SignalCardV2 = {
  card_id?: string;
  card_family?: string | null;
  prominence_class?: string | null;
  signal_label?: string | null;
  evidence_title?: string | null;
  interpretation_subtitle?: string | null;
  sleeve_tags?: string[];
  instrument_tags?: string[];
  freshness_state?: string | null;
  freshness_label?: string | null;
  decision_status?: string | null;
  action_posture?: string | null;
  support_label?: string | null;
  confidence_label?: string | null;
  market_confirmation_state?: string | null;
  signal_id: string;
  label: string;
  decision_title?: string | null;
  short_title?: string | null;
  short_subtitle?: string | null;
  signal_kind: string;
  direction: string;
  magnitude: string;
  summary: string;
  implication: string;
  confidence: V2Confidence;
  as_of: string;
  confirms: string;
  breaks: string;
  do_not_overread: string | null;
  affected_sleeves: string[];
  affected_holdings: string[];
  mapping_directness: V2MappingDirectness;
  trust_status: string;
  related_work_id: string | null;
  source_kind?: string | null;
  source_type?: string | null;
  effect_type?: string | null;
  primary_effect_bucket?: string | null;
  metric_definition?: string | null;
  reference_period?: string | null;
  release_date?: string | null;
  availability_date?: string | null;
  why_it_matters_macro?: string | null;
  why_it_matters_micro?: string | null;
  why_it_matters_short_term?: string | null;
  why_it_matters_long_term?: string | null;
  what_changed_today?: string | null;
  what_changed?: string | null;
  event_context_delta?: string | null;
  why_it_matters?: string | null;
  why_it_matters_economically?: string | null;
  portfolio_meaning?: string | null;
  portfolio_and_sleeve_meaning?: string | null;
  confirm_condition?: string | null;
  weaken_condition?: string | null;
  break_condition?: string | null;
  scenario_support?: string | null;
  evidence_class?: string | null;
  why_this_could_be_wrong?: string | null;
  why_now_not_before?: string | null;
  implementation_sensitivity?: string | null;
  implementation_set?: string[] | null;
  source_and_validity?: string | null;
  market_confirmation?: string | null;
  news_to_market_confirmation?: string | null;
  near_term_trigger?: string | null;
  thesis_trigger?: string | null;
  portfolio_consequence?: string | null;
  next_action?: string | null;
  affected_candidates?: string[];
  decision_relevance_score?: number | null;
  lead_relevance_score?: number | null;
  freshness_half_life_days?: number | null;
  freshness_age_days?: number | null;
  freshness_relevance_score?: number | null;
  novelty_class?: string | null;
  reactivation_reason?: string | null;
  threshold_state?: string | null;
  current_action_delta?: string | null;
  portfolio_read_delta?: string | null;
  lead_lane?: string | null;
  is_regime_context?: boolean | null;
  visibility_role?: string | null;
  coverage_reason?: string | null;
  aspect_bucket?: string | null;
  event_cluster_id?: string | null;
  event_family?: string | null;
  event_subtype?: string | null;
  event_region?: string | null;
  event_entities?: string[];
  market_channels?: string[];
  confirmation_assets?: string[];
  event_trigger_summary?: string | null;
  event_title?: string | null;
  event_fingerprint?: string | null;
  confidence_class?: string | null;
  sufficiency_state?: string | null;
  signal_support_class?: string | null;
  path_risk_note?: string | null;
  source_provenance_summary?: string | null;
  scenarios?: DailyBriefScenarioVariant[];
  chart_payload?: DailyBriefChartPayload | null;
  runtime_provenance?: RuntimeSourceProvenance | null;
  truth_envelope?: TruthEnvelope | null;
};

export type EvidenceSummary = {
  freshness_state: string;
  source_count: number;
  completeness_score: number;
};

export type PortfolioOverlaySummary = {
  portfolio_id: PortfolioId | string | null;
  summary: string;
  impacted_sleeves: string[];
};

export interface DailyBriefContract extends V2ContractBase {
  surface_id: 'daily_brief';
  what_changed: SignalCardV2[];
  why_it_matters_economically: string;
  why_it_matters_here: string;
  review_posture: string;
  what_confirms_or_breaks: string;
  surface_snapshot_id?: string | null;
  data_confidence?: string | null;
  decision_confidence?: string | null;
  evidence_and_trust: EvidenceSummary;
  portfolio_overlay: PortfolioOverlaySummary | null;
  portfolio_overlay_context?: OverlayContext | null;
  market_state_cards?: DailyBriefMarketStateCard[];
  macro_chart_panels?: ChartPanelContract[];
  cross_asset_chart_panels?: ChartPanelContract[];
  fx_chart_panels?: ChartPanelContract[];
  signal_chart_panels?: Array<{ signal_id: string; panel: ChartPanelContract }>;
  scenario_chart_panels?: Array<{ signal_id: string; panel: ChartPanelContract }>;
  signal_stack?: SignalCardV2[];
  signal_stack_groups?: DailyBriefSignalGroup[];
  regime_context_drivers?: SignalCardV2[];
  monitoring_conditions?: DailyBriefMonitoringCondition[];
  contingent_drivers?: DailyBriefContingentDriver[];
  portfolio_impact_rows?: DailyBriefPortfolioImpactRow[];
  review_triggers?: DailyBriefReviewTrigger[];
  scenario_blocks?: DailyBriefScenarioBlock[];
  evidence_bars?: DailyBriefMetricBar[];
  data_timeframes?: DailyBriefDataTimeframe[];
  diagnostics?: Array<{ label: string; value: string }>;
}

export interface SleeveTargetProfile {
  sleeve_id: SleeveId | string;
  sleeve_name: string;
  rank: number;
  target_pct?: number | null;
  target_display?: string | null;
  min_pct: number;
  max_pct: number;
  sort_midpoint_pct: number;
  is_nested: boolean;
  parent_sleeve_id?: SleeveId | string | null;
  parent_sleeve_name?: string | null;
  counts_as_top_level_total: boolean;
  target_label: string;
  range_label: string;
}

export interface SleeveDriftRow {
  sleeve_id: SleeveId;
  sleeve_name: string;
  rank: number;
  target_pct: number | null;
  target_display?: string | null;
  min_pct: number;
  max_pct: number;
  sort_midpoint_pct: number;
  is_nested: boolean;
  parent_sleeve_id?: SleeveId | string | null;
  parent_sleeve_name?: string | null;
  counts_as_top_level_total: boolean;
  target_label: string;
  range_label: string;
  current_pct: number | null;
  drift_pct: number | null;
  status: 'on_target' | 'needs_review' | 'off_target' | 'awaiting_holdings' | string;
  band_status?: 'in_band' | 'out_of_band' | 'awaiting_holdings' | string;
}

export interface WorkItem {
  work_id: string;
  title: string;
  urgency: V2WorkUrgency;
  affected_sleeves: string[];
  affected_holdings: string[];
  action_boundary: string | null;
  what_invalidates_view: string;
}

export interface HoldingRow {
  holding_id: string;
  symbol: string;
  name: string;
  sleeve_id: string;
  market_value?: number | null;
  weight_pct?: number | null;
  target_pct?: number | null;
  drift_pct?: number | null;
  review_status: 'monitor' | 'review';
  action_boundary: string | null;
  next_review_reason: string;
}

export interface PortfolioContract extends V2ContractBase {
  surface_id: 'portfolio';
  account_id: string;
  mandate_state: string;
  what_matters_now: string;
  action_posture: string;
  sleeve_drift_summary: SleeveDriftRow[];
  work_items: WorkItem[];
  holdings: HoldingRow[];
  blueprint_consequence: string | null;
  daily_brief_consequence: string | null;
  allocation_chart_panels?: ChartPanelContract[];
  portfolio_source_state?: SurfaceState;
  active_upload?: {
    run_id: string;
    uploaded_at: string;
    holdings_as_of_date: string;
    filename: string | null;
    source_name: string | null;
    status: string;
    normalized_position_count: number;
    total_market_value: number;
    stale_price_count: number;
    mapping_issue_count: number;
    warning_count: number;
  } | null;
  upload_history?: Array<{
    run_id: string;
    uploaded_at: string;
    holdings_as_of_date: string;
    filename: string | null;
    source_name: string | null;
    status: string;
    is_active: boolean;
    normalized_position_count: number;
    total_market_value: number;
    stale_price_count: number;
    mapping_issue_count: number;
    warning_count: number;
  }>;
  mapping_summary?: {
    quality_label: string;
    unresolved_count: number;
    stale_price_count: number;
    override_count: number;
  };
  unresolved_mapping_rows?: Array<{
    issue_id?: string | null;
    symbol: string | null;
    name: string | null;
    account_id: string | null;
    sleeve: string | null;
    mapping_status: string | null;
    issue_type: string | null;
    severity: string | null;
    detail: string | null;
    security_key?: string | null;
  }>;
  account_summary?: Array<{
    account_id: string;
    position_count: number;
    market_value: number;
    currencies: string[];
    wrapper_label: string;
  }>;
  base_currency?: string | null;
  mapping_overrides?: Array<{
    symbol: string;
    sleeve: string;
  }>;
  forecast_watchlist?: Array<{
    label: string;
    summary: string;
    forecast_support: ForecastSupport;
  }>;
}

export interface NotebookForecastReference {
  note_forecast_ref_id?: string;
  forecast_run_id: string;
  reference_label: string;
  threshold_summary: string | null;
  created_at: string;
}

export interface NotebookContract extends V2ContractBase {
  candidate_id: CandidateId | string;
  name: string;
  investment_case: string;
  evidence_sections: EvidenceSection[];
  evidence_depth: string;
  last_updated_utc: string | null;
  active_draft?: {
    entry_id: string;
    candidate_id: string;
    linked_object_type: string;
    linked_object_id: string;
    linked_object_label: string;
    status: 'draft' | 'finalized' | 'archived';
    date_label: string;
    title: string;
    thesis: string;
    assumptions: string;
    invalidation: string;
    watch_items: string;
    reflections: string;
    next_review_date: string | null;
    created_at: string;
    updated_at: string;
    finalized_at: string | null;
    archived_at: string | null;
    forecast_refs?: NotebookForecastReference[];
  } | null;
  finalized_notes?: Array<{
    entry_id: string;
    candidate_id: string;
    linked_object_type: string;
    linked_object_id: string;
    linked_object_label: string;
    status: 'draft' | 'finalized' | 'archived';
    date_label: string;
    title: string;
    thesis: string;
    assumptions: string;
    invalidation: string;
    watch_items: string;
    reflections: string;
    next_review_date: string | null;
    created_at: string;
    updated_at: string;
    finalized_at: string | null;
    archived_at: string | null;
    forecast_refs?: NotebookForecastReference[];
  }>;
  archived_notes?: Array<{
    entry_id: string;
    candidate_id: string;
    linked_object_type: string;
    linked_object_id: string;
    linked_object_label: string;
    status: 'draft' | 'finalized' | 'archived';
    date_label: string;
    title: string;
    thesis: string;
    assumptions: string;
    invalidation: string;
    watch_items: string;
    reflections: string;
    next_review_date: string | null;
    created_at: string;
    updated_at: string;
    finalized_at: string | null;
    archived_at: string | null;
    forecast_refs?: NotebookForecastReference[];
  }>;
  note_history?: Array<{
    revision_id: string;
    entry_id: string;
    action: string;
    created_at: string;
    candidate_id: string;
    status: string;
    title: string;
  }>;
  forecast_refs?: NotebookForecastReference[];
  memory_foundation_note?: string | null;
  research_support?: ResearchSupportPack | null;
}

export interface EvidenceSection {
  section_id: string;
  title: string;
  body: string;
  source_refs: string[];
  freshness_state: FreshnessClass;
}

export interface EvidenceWorkspaceContract extends V2ContractBase {
  candidate_id: CandidateId | string;
  name: string;
  evidence_pack: EvidencePackSummary;
  source_citations: SourceCitation[];
  completeness_score: number | null;
  source_authority_fields?: SourceAuthorityField[];
  reconciliation_report?: ReconciliationFieldStatus[];
  data_quality_summary?: DataQualitySummary | null;
  primary_document_manifest?: PrimaryDocumentRef[];
  summary?: {
    direct_count: number;
    proxy_count: number;
    stale_count: number;
    gap_count: number;
  };
  object_groups?: Array<{
    title: string;
    items: Array<{
      object_type: string;
      object_id: string;
      object_label: string;
      direct_count: number;
      proxy_count: number;
      stale_count: number;
      gap_flag: boolean;
      claims: Array<{
        claim_id: string;
        claim_text: string;
        claim_meta: string;
        directness: string;
        freshness_state: string;
      }>;
    }>;
  }>;
  documents?: Array<{
    document_id: string;
    title: string;
    document_type: string;
    linked_object_label: string;
    linked_object_type: string;
    retrieved_utc: string | null;
    freshness_state: string;
    stale: boolean;
    url: string | null;
  }>;
  benchmark_mappings?: Array<{
    mapping_id: string;
    sleeve_label: string;
    instrument_label: string;
    benchmark_label: string;
    baseline_label: string;
    directness: string;
  }>;
  tax_assumptions?: Array<{
    assumption_id: string;
    label: string;
    value: string;
  }>;
  gaps?: Array<{
    gap_id: string;
    object_label: string;
    issue_text: string;
  }>;
  forecast_support_items?: Array<{
    forecast_run_id: string | null;
    object_type: string;
    object_id: string;
    object_label: string;
    provider: string;
    model_name: string;
    support_strength: string;
    freshness_state: string;
    degraded_state: string | null;
    support_class: string;
    evidence_label: string;
    summary: string;
    created_at: string;
  }>;
  research_support?: ResearchSupportPack | null;
}

export interface EvidencePackSummary {
  source_count: number;
  freshness_state: FreshnessClass;
  completeness_score: number | null;
}

export interface SourceCitation {
  source_id: string;
  title: string;
  url: string | null;
  retrieved_utc: string | null;
  reliability: 'high' | 'medium' | 'low';
}

export type V2SurfaceContract =
  | CompareContract
  | ChangesContract
  | BlueprintExplorerContract
  | CandidateReportContract
  | DailyBriefContract
  | PortfolioContract
  | NotebookContract
  | EvidenceWorkspaceContract;
