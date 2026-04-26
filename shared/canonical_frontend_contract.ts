export const CANONICAL_ENDPOINTS = {
  auth: {
    login: '/api/auth/login',
    logout: '/api/auth/logout',
    session: '/api/auth/session',
  },
  platform: {
    version: '/api/platform/version',
    blueprint: '/api/platform/portfolio-blueprint',
  },
  dailyBrief: {
    latest: '/api/daily-brief/reader/latest',
  },
} as const;

// Official framework surfaces consume these backend-owned enums directly.
// Do not widen them with fallback strings on the official path.
export type CanonicalPromotionState = 'research_only' | 'acceptable' | 'near_decision_ready' | 'buyable';
export type CanonicalDailyBriefActionState = 'ignore' | 'monitor' | 'review';
export type CanonicalLensStatus = 'supportive' | 'neutral' | 'cautious' | 'constraining' | 'blocking' | 'explanatory_only';
export type CanonicalLensPromotionCap = 'none' | 'acceptable' | 'near_decision_ready';
export type CanonicalLensReviewModifier = 'none' | 'raise_to_universal' | 'raise_to_deep';
export type CanonicalLensConfidenceModifier = 'none' | 'soften' | 'materially_soften';
export type CanonicalLensActionTone = 'none' | 'restrained' | 'monitoring_only';
export type CanonicalLensPosture =
  | 'supportive_with_restraint'
  | 'mixed_but_constructive'
  | 'caution_dominant'
  | 'promotion_constrained'
  | 'blocked_by_fragility'
  | 'explanatory_only';

export type CanonicalPortfolioBlueprintCandidate = {
  symbol: string;
  name: string;
  domicile?: string | null;
  accumulation?: string | null;
  expense_ratio?: number | null;
  replication_method?: string | null;
  rationale?: string | null;
  citations?: Array<{ source_id: string; url: string; importance?: string | null }>;
  extra?: { isin?: string | null } | null;
  liquidity_score?: number | null;
  gates?: Record<string, { passed: boolean; decisive?: boolean; reason?: string | null }>;
  scores?: Record<string, unknown>;
  is_current?: boolean;
  is_recommended?: boolean;
  investment_quality?: {
    role_in_portfolio?: string | null;
    investment_thesis?: string | null;
    key_advantages?: string[];
    key_risks?: string[];
    main_limitations?: string[];
    recommendation_weakeners?: string[];
  } | null;
  decision_readiness?: {
    investor_consequence_summary?: string | null;
    primary_blocker?: string | null;
    what_must_change?: string[];
  } | null;
  recommendation_context?: {
    why_now?: string | null;
    lead_type?: string | null;
  } | null;
  decision_thesis?: {
    thesis_summary?: string | null;
    key_allocation_reason?: string | null;
    key_reservation_reason?: string | null;
  } | null;
  // Official renderer surfaces must read decision meaning from this object directly.
  canonical_decision: CanonicalCandidateDecision;
  [key: string]: unknown;
};

export type CanonicalPortfolioBlueprintSleeve = {
  sleeve_key: string;
  name: string;
  policy_weight_range?: { min: number; target: number; max: number } | null;
  purpose?: string | null;
  constraints?: string[];
  current_weight?: number | null;
  recommendation?: Record<string, unknown>;
  candidates?: CanonicalPortfolioBlueprintCandidate[];
  [key: string]: unknown;
};

export type CanonicalPortfolioBlueprintPayload = {
  blueprint_id?: string | null;
  version?: string | null;
  generated_at?: string | null;
  sleeves?: CanonicalPortfolioBlueprintSleeve[];
  trust_level?: Record<string, unknown>;
  data_freshness?: Record<string, unknown>;
  portfolio_summary?: Record<string, unknown>;
  evaluation_metadata?: Record<string, unknown>;
  [key: string]: unknown;
};

export type CanonicalDecisionBlocker = {
  code?: string | null;
  reason?: string | null;
};

export type CanonicalBucketAppendix = {
  bucket_name?: string | null;
  bucket_state?: string | null;
  source_url?: string | null;
  source_name?: string | null;
  source_kind?: string | null;
  observed_at?: string | null;
  retrieved_at?: string | null;
  extraction_methods?: string[];
  supported_fields?: string[];
  missing_fields?: string[];
  failure_reasons?: string[];
  claim_limits?: string[];
  supports?: string | null;
  does_not_support?: string | null;
};

export type CanonicalLensJudgment = {
  lens_id?: string | null;
  lens_status?: CanonicalLensStatus | null;
  confidence?: 'high' | 'medium' | 'low' | null;
  promotion_cap?: CanonicalLensPromotionCap | null;
  review_intensity_modifier?: CanonicalLensReviewModifier | null;
  confidence_modifier?: CanonicalLensConfidenceModifier | null;
  action_tone_constraint?: CanonicalLensActionTone | null;
  supports?: string[];
  cautions?: string[];
  blocker_flags?: string[];
  portfolio_relevance?: string | null;
  claim_constraints?: string[];
  what_changes_view?: string[];
  investor_summary?: string | null;
};

export type CanonicalLensFusionResult = {
  overall_lens_posture?: CanonicalLensPosture | null;
  promotion_cap?: CanonicalLensPromotionCap | null;
  confidence_modifier?: CanonicalLensConfidenceModifier | null;
  review_intensity_modifier?: CanonicalLensReviewModifier | null;
  action_tone_constraint?: CanonicalLensActionTone | null;
  explanatory_only?: boolean | null;
  dominant_supports?: string[];
  dominant_cautions?: string[];
  applied_by_lens?: Record<string, Record<string, unknown>>;
};

export type CanonicalCandidateDecision = {
  candidate_symbol?: string | null;
  candidate_name?: string | null;
  sleeve_key?: string | null;
  sleeve_role?: string | null;
  readiness_state?: string | null;
  promotion_state?: CanonicalPromotionState | null;
  eligibility_and_blockers?: {
    gate_overall_status?: string | null;
    decision_completeness_grade?: string | null;
    portfolio_completeness_grade?: string | null;
    benchmark_support_class?: string | null;
    tax_assumption_grade?: string | null;
    tax_confidence?: string | null;
    holdings_bucket_state?: string | null;
    liquidity_bucket_state?: string | null;
    failed_blockers?: CanonicalDecisionBlocker[];
    failed_blocker_codes?: string[];
    unresolved_limits?: string[];
    buyable_blocked?: boolean | null;
  } | null;
  evidence_summary?: {
    evidence_depth_class?: string | null;
    confidence?: string | null;
    support_depth?: string | null;
    strongest_support_points?: string[];
    strongest_limiting_points?: string[];
    bucket_support_appendix?: CanonicalBucketAppendix[];
  } | null;
  benchmark_authority?: {
    support_class?: string | null;
    authority_label?: string | null;
    comparative_claim_boundary?: string | null;
    performance_claims_allowed?: boolean | null;
  } | null;
  tax_authority?: {
    assumption_grade?: string | null;
    tax_confidence?: string | null;
    advisory_boundary?: string | null;
    decisive_tax_use_allowed?: boolean | null;
  } | null;
  score_validity?: {
    valid?: boolean | null;
    score?: number | null;
  } | null;
  portfolio_context?: {
    portfolio_completeness_grade?: string | null;
    current_holding_record?: Record<string, unknown> | null;
    portfolio_consequence_summary?: Record<string, unknown> | null;
  } | null;
  incumbent_comparison_result?: {
    current_symbol?: string | null;
    practical_edge?: Record<string, unknown> | null;
    switching_friction?: Record<string, unknown> | null;
    no_change_is_best?: boolean | null;
  } | null;
  recommendation_state?: {
    candidate_status?: string | null;
    decision_type?: string | null;
    recommendation_tier?: string | null;
    current_holding_should_be_kept?: boolean | null;
  } | null;
  action_boundary?: {
    state?: string | null;
    decision_type?: string | null;
    manual_approval_required?: boolean | null;
    do_now?: string | null;
    do_not_do_now?: string | null;
    why_boundary_exists?: string | null;
    manual_approval_note?: string | null;
  } | null;
  lens_assessment?: {
    per_lens?: Record<string, CanonicalLensJudgment>;
  } | null;
  lens_fusion_result?: CanonicalLensFusionResult | null;
  framework_judgment?: {
    summary?: string | null;
    overall_lens_posture?: string | null;
    dominant_supports?: string[];
    dominant_cautions?: string[];
    action_tone_constraint?: string | null;
    action_boundary_state?: string | null;
  } | null;
  report_sections?: {
    problem_or_opportunity?: string | null;
    what_this_is?: string | null;
    why_attractive?: string | null;
    potential_benefit?: string | null;
    evidence_support?: string[];
    evidence_limits?: string[];
    benchmark_authority?: string | null;
    tax_authority?: string | null;
    current_holding_comparison?: string | null;
    switch_cost_or_friction?: string | null;
    main_tradeoff?: string | null;
    current_view?: string | null;
    what_to_do_now?: string | null;
    what_not_to_do_now?: string | null;
    what_would_change_the_view?: string | null;
    framework_judgment?: string | null;
    lens_supports?: string[];
    lens_cautions?: string[];
  } | null;
  what_changes_the_view?: {
    summary?: string | null;
    conditions?: string[];
  } | null;
  plain_english_summary?: string | null;
};

export type CanonicalDailyBriefReaderCard = {
  signal_id?: string | null;
  headline?: string | null;
  category?: string | null;
  text?: string | null;
  what_changed?: string | null;
  why_it_matters?: string | null;
  action_tag?: string | null;
  severity_score?: number | null;
  confidence?: { label?: string | null; score?: number | null } | null;
  portfolio_implication?: {
    primary_affected_sleeves?: string[];
    secondary_affected_sleeves?: string[];
  } | null;
  evidence?: {
    sources?: Array<{ label?: string | null; url?: string | null }>;
    freshness?: { lag_class?: string | null } | null;
  } | null;
  action_state?: CanonicalDailyBriefActionState | null;
  signal_trust_status?: {
    freshness_strength?: string | null;
    source_support_quality?: string | null;
    holdings_grounding_quality?: string | null;
    benchmark_support_quality?: string | null;
    persistence_strength?: string | null;
    overall_trust_level?: string | null;
    practical_trust_explanation?: string | null;
  } | null;
  interpretive_strength_status?: {
    directness_of_support?: string | null;
    proxy_dependence?: string | null;
    narrative_confidence?: string | null;
    action_boundary?: string | null;
    overread_warning?: string | null;
    interpretation_strength_grade?: string | null;
  } | null;
  portfolio_mapping_directness_status?: {
    direct_holdings_grounded?: boolean | null;
    sleeve_proxy_grounded?: boolean | null;
    benchmark_proxy_grounded?: boolean | null;
    mapping_strength?: string | null;
    practical_mapping_warning?: string | null;
  } | null;
  refresh_strength_status?: {
    refresh_mode?: string | null;
    refresh_recency?: string | null;
    lag_present?: boolean | null;
    fallback_used?: boolean | null;
    freshness_policy_result?: string | null;
    practical_effect_on_read?: string | null;
  } | null;
  lens_context?: {
    overall_posture?: string | null;
    review_intensity_modifier?: string | null;
    marks_cycle_risk?: { summary?: string | null } | null;
    dalio_regime_transmission?: { summary?: string | null } | null;
    fragility_red_team?: { summary?: string | null } | null;
    implementation_reality?: { summary?: string | null } | null;
    portfolio_frame?: string | null;
  } | null;
  review_intensity_context?: {
    review_intensity_modifier?: string | null;
    summary?: string | null;
  } | null;
  action_relevance?: string | null;
  do_not_overread?: string | null;
  what_to_do_next?: string | null;
  why_it_matters_here?: string | null;
  watch_condition?: string | null;
  affected_sleeves?: string[];
  [key: string]: unknown;
};

export type CanonicalDailyBriefReaderPayload = {
  brief_run_id?: string | null;
  generated_at?: string | null;
  reader_cards?: CanonicalDailyBriefReaderCard[];
  cards?: CanonicalDailyBriefReaderCard[];
  executive_monitoring?: Array<{ text?: string | null }>;
  alerts_timeline?: Array<{ text?: string | null }>;
  implication_summary?: string[];
  what_changed?: string[];
  relevance?: {
    primary_sleeves?: string[];
    secondary_sleeves?: string[];
    summary?: string | null;
  } | null;
  freshness?: { summary?: string | null } | null;
  trust_banner?: { label?: string | null } | null;
  [key: string]: unknown;
};
