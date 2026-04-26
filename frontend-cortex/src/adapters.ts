import type {
  BlueprintExplorerContract,
  BlueprintMarketPathSupport,
  CandidateReportContract,
  ChangeDetail,
  ChangesContract,
  DailyBriefChartPayload,
  CompareContract,
  DailyBriefContract,
  EvidenceWorkspaceContract,
  ForecastSupport,
  ForecastTriggerSupport,
  NotebookContract,
  NotebookForecastReference,
  PortfolioContract,
  ResearchSupportPack,
  ReconciliationFieldStatus,
  RuntimeSourceProvenance,
  SignalCardV2,
} from "../../shared/v2_surface_contracts";
import { adaptChartPanel, adaptChartPanels } from "./charts/chartAdapters";
import type { ChartPanelDisplay } from "./charts/chartTypes";
import { describeMarketPathSupport, type MarketPathPresentation } from "./blueprint/marketPathPresentation";
import {
  cleanBlueprintCopy,
  deriveSleevePosture,
  presentBlueprintFailureClass,
  presentBlueprintCandidateStatus,
  presentBlueprintDecisionState,
  presentEvidenceDepth,
  presentRecommendationGateState,
  presentSleeveActionabilityState,
  presentSourceCoverageLabel,
} from "./blueprint/surfacePresentation";

export type Tone = "good" | "warn" | "bad" | "neutral" | "info";
export type PrimaryView = "portfolio" | "brief" | "candidates" | "notebook" | "evidence";
export type ReportTab =
  | "investment_case"
  | "market_history"
  | "scenarios"
  | "risks"
  | "competition"
  | "evidence";

export type Badge = {
  label: string;
  tone?: Tone;
  dot?: boolean;
};

export type InspectorLine = {
  label: string;
  value: string;
  tone?: Tone;
};

export type SummaryChip = {
  label: string;
  value: string;
  meta?: string;
  tone?: Tone;
};

export type CandidateDecisionConditionDisplayItem = {
  kind: "upgrade" | "downgrade" | "kill";
  label: string;
  text: string;
  supportText: string | null;
  confirmationLabel: string | null;
  confirmationPoints: string[];
  confidence: string | null;
  basisLabels: string[];
};

export type SurfaceMeta = {
  kicker: string;
  title: string;
  copy: string;
  badges: Badge[];
};

export type PortfolioDisplay = {
  meta: SurfaceMeta;
  degradedMessage: string | null;
  summaryChips: SummaryChip[];
  chartPanels: ChartPanelDisplay[];
  hero: {
    summary: string;
    postureLabel: string;
    postureTone: Tone;
    mandateLabel: string;
  };
  allocationRows: Array<{
    sleeveId: string;
    name: string;
    rank: number;
    targetLabel: string;
    rangeLabel: string;
    bandStatus: string;
    bandTone: Tone;
    isNested: boolean;
    parentSleeveId: string | null;
    parentSleeveName: string | null;
    countsAsTopLevelTotal: boolean;
    current: string;
    target: string;
    range: string;
    currentPct: number | null;
    targetPct: number;
    minPct: number;
    maxPct: number;
    drift: string;
    driftTone: Tone;
    statusLabel: string;
    statusTone: Tone;
    note: string;
    capitalEligible: boolean;
    fundingSource: string | null;
    supportBars: Array<{
      key: string;
      score: number;
    }>;
  }>;
  holdings: Array<{
    symbol: string;
    name: string;
    sleeve: string;
    weight: string;
    weightPct: number | null;      // null = data not available
    targetWeight: string;
    targetPct: number | null;
    weightDrift: string;
    weightTone: Tone;
    statusLabel: string;
    statusTone: Tone;
    blueprintLabel: string;
    blueprintTone: Tone;
    briefLabel: string;
    briefTone: Tone;
    actionLabel: string;
  }>;
  healthTiles: Array<{
    label: string;
    value: string;
    note: string;
    tone: Tone;
  }>;
  blueprintRows: Array<{
    sleeve: string;
    candidate: string;
    status: string;
    statusTone: Tone;
    note: string;
  }>;
  briefRows: Array<{
    title: string;
    posture: string;
    postureTone: Tone;
    affected: string[];
    note: string | null;
    caveat: string | null;
  }>;
  uploadStatus: {
    source: string;
    freshness: string;
    mappingQuality: string;
    unresolvedMappings: string;
    message: string;
  };
  inspector: InspectorLine[];
};

export type DailyBriefSignalDisplay = {
  id: string;
  cardFamily: string | null;
  prominenceClass: string | null;
  signalLabel: string;
  evidenceTitle: string;
  interpretationSubtitle: string;
  sleeveTags: string[];
  instrumentTags: string[];
  evidenceClassCode: string | null;
  freshnessState: string | null;
  freshnessLabel: string | null;
  decisionStatus: string | null;
  actionPosture: string | null;
  supportLabel: string | null;
  confidenceLabel: string | null;
  marketConfirmationState: string | null;
  title: string;
  shortTitle: string;
  shortSubtitle: string;
  posture: string;
  postureTone: Tone;
  category: string;
  summary: string;
  implication: string;
  sourceKind: string | null;
  effectType: string | null;
  bucket: string | null;
  whyEconomicMacro: string | null;
  whyEconomicMicro: string | null;
  whyHereShortTerm: string | null;
  whyHereLongTerm: string | null;
  whatChangedToday: string | null;
  whatChanged: string | null;
  eventContextDelta: string | null;
  whyItMatters: string | null;
  whyItMattersEconomically: string | null;
  portfolioMeaning: string | null;
  portfolioAndSleeveMeaning: string | null;
  confirmCondition: string | null;
  weakenCondition: string | null;
  breakCondition: string | null;
  scenarioSupport: string | null;
  evidenceClass: string | null;
  whyThisCouldBeWrong: string | null;
  whyNowNotBefore: string | null;
  implementationSensitivity: string | null;
  implementationSet: string[];
  sourceAndValidity: string | null;
  marketConfirmation: string | null;
  newsToMarketConfirmation: string | null;
  doNotOverread: string | null;
  confirms: string;
  breaks: string;
  nearTermTrigger: string | null;
  thesisTrigger: string | null;
  portfolioConsequence: string | null;
  nextAction: string | null;
  pathRiskNote: string | null;
  affected: string[];
  affectedCandidates: string[];
  mappingDirectness: string;
  trust: string;
  asOf: string;
  relevanceScore: number | null;
  confidenceClass: string | null;
  sufficiencyState: string | null;
  supportClass: string | null;
  sourceProvenanceSummary: string | null;
  visibilityRole: string | null;
  coverageReason: string | null;
  aspectBucket: string | null;
  eventClusterId: string | null;
  eventTitle: string | null;
  eventSubtype: string | null;
  eventRegion: string | null;
  eventEntities: string[];
  marketChannels: string[];
  confirmationAssets: string[];
  eventTriggerSummary: string | null;
  scenarios: Array<{
    label: string;
    type: string;
    scenarioName: string | null;
    pathStatement: string | null;
    timingWindow: string | null;
    scenarioLikelihoodPct: number | null;
    sleeveConsequence: string | null;
    actionBoundary: string | null;
    upgradeTrigger: string | null;
    downgradeTrigger: string | null;
    supportStrength: string | null;
    regimeNote: string | null;
    confirmationNote: string | null;
    leadSentence: string | null;
    effect: string;
    actionConsequence: string | null;
    pathMeaning: string | null;
    triggerState: string | null;
    pathBias: string | null;
    confirmProbability: string | null;
    breakProbability: string | null;
    thresholdBreachRisk: string | null;
    uncertaintyWidth: string | null;
    persistenceVsReversion: string | null;
    evidenceState: string | null;
    macro: string | null;
    micro: string | null;
    shortTerm: string | null;
    longTerm: string | null;
  }>;
  chartPayload: DailyBriefChartPayload | null;
  chart: ChartPanelDisplay | null;
};

export type DailyBriefSignalGroupDisplay = {
  id: string;
  label: string;
  summary: string;
  representative: DailyBriefSignalDisplay | null;
  count: number;
  signals: DailyBriefSignalDisplay[];
};

export type DailyBriefDisplay = {
  meta: SurfaceMeta;
  degradedMessage: string | null;
  statusBar: SummaryChip[];
  briefHeader: {
    economicRead: string;
    portfolioRead: string;
    changeCondition: string;
  };
  macroCharts: ChartPanelDisplay[];
  crossAssetCharts: ChartPanelDisplay[];
  fxCharts: ChartPanelDisplay[];
  marketState: Array<{
    label: string;
    value: string;
    note: string;
    tone: Tone;
    currentValue: number | null;
    changePct1d: number | null;
    caption: string | null;
    subCaption: string | null;
    freshness: string | null;
    freshnessLabel: string;
    freshnessTone: Tone;
    liveOrCache: string | null;
    isNonFresh: boolean;
    asOf: string | null;
    sourceProvider: string | null;
    sourceType: string | null;
    sourceAuthorityTier: string | null;
    metricDefinition: string | null;
    metricPolarity: string | null;
    isExact: boolean;
    validationStatus: string | null;
    validationReason: string | null;
    freshnessMode: string | null;
    primaryProvider: string | null;
    crossCheckProvider: string | null;
    crossCheckStatus: string | null;
    authorityGapReason: string | null;
  }>;
  signals: DailyBriefSignalDisplay[];
  signalGroups: DailyBriefSignalGroupDisplay[];
  regimeContextSignals: DailyBriefSignalDisplay[];
  monitoring: Array<{
    label: string;
    whyNow: string;
    nearTermTrigger: string;
    thesisTrigger: string;
    breakCondition: string;
    portfolioConsequence: string;
    nextAction: string;
  }>;
  contingentDrivers: Array<{
    label: string;
    triggerTitle: string;
    effectType: string | null;
    whyNow: string;
    whatChangesIfConfirmed: string;
    whatToWatchNext: string | null;
    currentStatus: string | null;
    affectedSleeves: string[];
    supportingLines: string[];
  }>;
  macroCards: Array<{
    label: string;
    value: string;
    note: string;
  }>;
  crossAssetCards: Array<{
    label: string;
    value: string;
    note: string;
  }>;
  fxCards: Array<{
    label: string;
    value: string;
    note: string;
  }>;
  impactRows: Array<{
    objectLabel: string;
    objectType: string;
    mapping: string;
    statusLabel: string;
    statusTone: Tone;
    consequence: string;
    nextStep: string;
  }>;
  reviewLanes: {
    reviewNow: Array<{ label: string; reason: string }>;
    monitor: Array<{ label: string; reason: string }>;
    doNotActYet: Array<{ label: string; reason: string }>;
  };
  evidenceBars: Array<{
    label: string;
    score: number;
    tone: Tone;
  }>;
  evidenceRows: Array<{
    label: string;
    value: string;
  }>;
  diagnostics: Array<{
    label: string;
    value: string;
  }>;
  scenarioMessage: string;
  scenarios: Array<{
    label: string;
    summary: string;
    chart: ChartPanelDisplay | null;
    variants: Array<{
      label: string;
      type: string;
      scenarioName: string | null;
      pathStatement: string | null;
      timingWindow: string | null;
      scenarioLikelihoodPct: number | null;
      sleeveConsequence: string | null;
      actionBoundary: string | null;
      upgradeTrigger: string | null;
      downgradeTrigger: string | null;
      supportStrength: string | null;
      regimeNote: string | null;
      confirmationNote: string | null;
      leadSentence: string | null;
      effect: string;
      actionConsequence: string | null;
      pathMeaning: string | null;
      triggerState: string | null;
      pathBias: string | null;
      confirmProbability: string | null;
      breakProbability: string | null;
      thresholdBreachRisk: string | null;
      uncertaintyWidth: string | null;
      persistenceVsReversion: string | null;
      evidenceState: string | null;
      macro: string | null;
      micro: string | null;
      shortTerm: string | null;
      longTerm: string | null;
    }>;
  }>;
  inspector: InspectorLine[];
};

export type CandidateCardDisplay = {
  id: string;
  symbol: string;
  name: string;
  score: number;
  decisionStateRaw: string | null;
  investorStateRaw: string | null;
  gateStateRaw: string | null;
  decisionState: string;
  decisionTone: Tone;
  decisionSummary: string | null;
  failureSummary: {
    primaryLabel: string | null;
    summary: string | null;
    hardClasses: string[];
    reviewClasses: string[];
    confidenceDragClasses: string[];
  } | null;
  blockerCategory: string | null;
  benchmarkFullName: string | null;
  exposureSummary: string | null;
  terBps: string | null;
  spreadProxyBps: string | null;
  aumUsd: string | null;
  aumState: string | null;
  taxPostureSummary: string | null;
  distributionPolicy: string | null;
  replicationRiskNote: string | null;
  currentWeight: string | null;
  weightState: string | null;
  sourceIntegritySummary: {
    state: string;
    stateTone: Tone;
    integrityLabel: string | null;
    summary: string;
    criticalReady: number;
    criticalTotal: number;
    authorityMix: Array<{ label: string; count: number }>;
    issueCounts: Array<{ label: string; count: number }>;
    hardConflictFields: string[];
    missingCriticalFields: string[];
    weakestFields: string[];
  } | null;
  sourceCompletionSummary: {
    state: string;
    summary: string;
    criticalCompleted: number;
    criticalTotal: number;
    equivalentReadyCount: number;
    incompleteFields: string[];
    weakFields: string[];
    staleFields: string[];
    conflictFields: string[];
    authorityClean: boolean;
    freshnessClean: boolean;
    conflictClean: boolean;
    completenessClean: boolean;
    completionReasons: string[];
  } | null;
  identitySummary: string | null;
  scoreBreakdown: {
    total: number;
    recommendation: number | null;
    recommendationMerit: number | null;
    investmentMerit: number | null;
    deployability: number | null;
    truthConfidence: number | null;
    truthConfidenceBand: string | null;
    truthConfidenceSummary: string | null;
    deployment: number | null;
    admissibility: number | null;
    admissibilityIdentity: number | null;
    implementation: number | null;
    sourceIntegrity: number | null;
    evidence: number | null;
    sleeveFit: number | null;
    identity: number | null;
    benchmarkFidelity: number | null;
    marketPathSupport: number | null;
    longHorizonQuality: number | null;
    instrumentQuality: number | null;
    portfolioFit: number | null;
    optimality: number | null;
    readiness: number | null;
    confidencePenalty: number | null;
    readinessPosture: string | null;
    readinessSummary: string | null;
    deployabilityBadge: string | null;
    summary: string | null;
  } | null;
  scoreSummary: {
    averageScore: number;
    componentCountUsed: number;
    tone: Tone;
    reliabilityState: "strong" | "mixed" | "weak";
    reliabilityNote: string | null;
    components: Array<{
      id: string;
      label: string;
      score: number;
      tone: Tone;
      summary: string;
    }>;
  } | null;
  scoreComponents: Array<{
    id?: string;
    label: string;
    score: number;
    band?: string | null;
    confidence?: number | null;
    tone: Tone;
    summary: string;
    reasons?: string[];
    capsApplied?: string[];
    fieldDrivers?: string[];
  }>;
  issuer: string | null;
  expenseRatio: string;
  aum: string;
  freshness: string | null;
  instrumentQuality: string;
  portfolioFit: string;
  capitalPriority: string;
  statusLabel: string;
  statusTone: Tone;
  whyNow: string;
  whatBlocksAction: string;
  whatChangesView: string;
  actionBoundary: string | null;
  fundingSource: string | null;
  implicationSummary: string;
  recommendationGate: {
    state: string;
    stateTone: Tone;
    summary: string;
    criticalMissing: string[];
    blockedReasons: string[];
  } | null;
  marketSupportBasis: string | null;
  coverageStatusRaw: string | null;
  coverageStatus: string | null;
  coverageSummary: string | null;
  fieldIssues: Array<{
    label: string;
    status: string;
    fixability: string;
    summary: string;
  }>;
  dataQualitySummary: {
    confidence: string;
    confidenceTone: Tone;
    criticalReady: number;
    criticalTotal: number;
    summary: string;
  } | null;
  scenarioReadinessNote: string | null;
  implementationSummary: string | null;
  detailCharts: ChartPanelDisplay[];
  marketPathSupport: BlueprintMarketPathSupport | null;
  marketPath: MarketPathPresentation | null;
  quickBrief: CandidateReportDisplay["quickBrief"];
};

export type SupportPillarDisplay = {
  label: string;
  score: number;
  note: string;
  tone: Tone;
};

export type BlueprintSleeveDisplay = {
  id: string;
  name: string;
  purpose: string;
  rank: number;
  targetLabel: string;
  rangeLabel: string;
  isNested: boolean;
  parentSleeveId: string | null;
  parentSleeveName: string | null;
  countsAsTopLevelTotal: boolean;
  sleeveRoleStatement: string | null;
  cycleSensitivity: string | null;
  baseAllocationRationale: string | null;
  priorityRank: number;
  currentWeight: string | null;
  targetWeight: string | null;
  candidateCount: number;
  statusLabel: string;
  statusTone: Tone;
  postureSummary: string;
  postureDetail: string;
  blockLabel: string;
  reopenLabel: string;
  actionableCandidateCount: number;
  reviewableCandidateCount: number;
  activeSupportCandidateCount: number;
  blockedCandidateCount: number;
  leaderBlockedButReviewable: boolean;
  capitalMemo: string;
  implicationSummary: string;
  whyItLeads: string;
  mainLimit: string;
  recommendationScore: {
    averageScore: number;
    pillarCountUsed: number;
    factorCountUsed: number;
    scoreBasis: "support_pillars_average" | "deployment_score" | "recommendation_score";
    leaderCandidateRecommendationScore: number | null;
    leaderTruthConfidenceScore: number | null;
    leaderCandidateDeployabilityScore: number | null;
    leaderCandidateInvestmentMeritScore: number | null;
    leaderCandidateDeploymentScore: number | null;
    depthScore: number | null;
    sleeveActionabilityScore: number | null;
    blockerBurdenScore: number | null;
    tone: Tone;
    label: string;
  } | null;
  reopenCondition: string | null;
  fundingPath: {
    fundingSource: string | null;
    incumbentLabel: string | null;
    actionBoundary: string | null;
    summary: string | null;
  } | null;
  forecastWatch: string | null;
  leadCandidateName: string | null;
  sleeveStateRaw: string | null;
  candidates: CandidateCardDisplay[];
  supportPillars: SupportPillarDisplay[];
};

export type CompareDisplay = {
  readinessState: string | null;
  readinessTone: Tone;
  readinessNote: string | null;
  substitutionVerdict: string | null;
  substitutionTone: Tone;
  substitutionRationale: string | null;
  winnerName: string;
  whyLeads: string;
  whatWouldChange: string;
  compareSummary: {
    cleanerForSleeveJob: string | null;
    mainSeparation: string | null;
    changeTrigger: string | null;
  };
  candidates: Array<{
    id: string;
    symbol: string;
    name: string;
    decisionState: string | null;
    decisionTone: Tone;
    blockerCategory: string | null;
    benchmark: string | null;
    totalScore: string;
    recommendationScore: number | null;
    investmentMeritScore: number | null;
    deployabilityScore: number | null;
    truthConfidenceScore: number | null;
    aumUsd: number | null;
    domicile: string | null;
    tradingCurrency: string | null;
    listingExchange: string | null;
    distributionPolicy: string | null;
    replicationMethod: string | null;
    currentWeight: string | null;
    weightState: string | null;
    decisionSummary: string | null;
    exposureSummary: string | null;
    compactTags: string[];
    verdictLabel: string | null;
    verdictTone: Tone;
    verdictReason: string | null;
    sleeveFit: {
      roleFit: string | null;
      benchmarkFit: string | null;
      scopeFit: string | null;
      thesis: string | null;
    };
    implementationStats: Array<{
      label: string;
      value: string;
      tone?: Tone;
    }>;
    riskEvidence: {
      evidenceStatus: string | null;
      timingStatus: string | null;
      impactLine: string | null;
    };
  }>;
  dimensions: Array<{
    id: string;
    label: string;
    group: string | null;
    rationale: string | null;
    importance: string | null;
    values: Array<{
      candidateId: string;
      candidateName: string;
      value: string;
    }>;
    winnerLabel: string | null;
    winnerTone: Tone;
    }>;
  insufficientDimensions: string[];
  decision: {
    substitutionStatus: string | null;
    substitutionSummary: string | null;
    substitutionReason: string | null;
    substitutionConfidence: string | null;
    bestOverall: string | null;
    investmentWinner: string | null;
    deploymentWinner: string | null;
    evidenceWinner: string | null;
    timingWinner: string | null;
    winnerSummary: string | null;
    whereLoserWins: string | null;
    decisionRule: {
      primaryRule: string | null;
      chooseCandidateAIf: string | null;
      chooseCandidateBIf: string | null;
      doNotTreatAsSubstitutesIf: string | null;
      nextAction: string | null;
    };
    deltaRows: Array<{
      id: string;
      label: string;
      candidateAValue: string;
      candidateBValue: string;
      winner: string | null;
      implication: string | null;
    }>;
    portfolioConsequence: {
      candidateA: {
        candidateId: string;
        symbol: string;
        portfolioEffect: string;
        concentrationEffect: string;
        regionExposureEffect: string;
        currencyOrTradingLineEffect: string;
        overlapEffect: string;
        sleeveMandateEffect: string;
        diversificationEffect: string;
        fundingPathEffect: string;
        targetAllocationDriftEffect: string;
        confidence: string;
      } | null;
      candidateB: {
        candidateId: string;
        symbol: string;
        portfolioEffect: string;
        concentrationEffect: string;
        regionExposureEffect: string;
        currencyOrTradingLineEffect: string;
        overlapEffect: string;
        sleeveMandateEffect: string;
        diversificationEffect: string;
        fundingPathEffect: string;
        targetAllocationDriftEffect: string;
        confidence: string;
      } | null;
    };
    scenarioWinners: Array<{
      scenario: string;
      candidateAEffect: string;
      candidateBEffect: string;
      winner: string | null;
      why: string | null;
    }>;
    flipConditions: Array<{
      condition: string;
      currentState: string;
      flipsToward: string | null;
      thresholdOrTrigger: string;
    }>;
    evidenceDiff: {
      strongerEvidence: string | null;
      unresolvedFields: string[];
      candidateAWeakFields: string[];
      candidateBWeakFields: string[];
      evidenceNeededToDecide: string[];
    };
  } | null;
};

function adaptStringList(values: unknown): string[] {
  if (!Array.isArray(values)) {
    return [];
  }
  return values
    .map((value) => {
      if (value === null || value === undefined) {
        return null;
      }
      const stringValue = typeof value === "string" ? value : String(value);
      return cleanBlueprintCopy(stringValue) ?? stringValue;
    })
    .filter((value): value is string => Boolean(value));
}

function adaptIndexScopeExplainer(scope: any) {
  return scope
    ? {
        label: cleanBlueprintCopy(scope.label) ?? scope.label ?? null,
        scopeType: cleanBlueprintCopy(scope.scope_type) ?? scope.scope_type ?? null,
        displayTitle: cleanBlueprintCopy(scope.display_title) ?? scope.display_title ?? null,
        summary: cleanBlueprintCopy(scope.summary) ?? scope.summary ?? null,
        covers: adaptStringList(scope.covers),
        doesNotCover: adaptStringList(scope.does_not_cover),
        sleeveRelevance: cleanBlueprintCopy(scope.sleeve_relevance) ?? scope.sleeve_relevance ?? null,
        specificity: cleanBlueprintCopy(scope.specificity) ?? scope.specificity ?? null,
        sourceBasis: cleanBlueprintCopy(scope.source_basis) ?? scope.source_basis ?? null,
        confidence: cleanBlueprintCopy(scope.confidence) ?? scope.confidence ?? null,
        indexName: cleanBlueprintCopy(scope.index_name) ?? scope.index_name ?? null,
        coverageStatement: cleanBlueprintCopy(scope.coverage_statement) ?? scope.coverage_statement ?? null,
        includesStatement: cleanBlueprintCopy(scope.includes_statement) ?? scope.includes_statement ?? null,
        excludesStatement: cleanBlueprintCopy(scope.excludes_statement) ?? scope.excludes_statement ?? null,
        marketCapScope: cleanBlueprintCopy(scope.market_cap_scope) ?? scope.market_cap_scope ?? null,
        countryCount: cleanBlueprintCopy(scope.country_count) ?? scope.country_count ?? null,
        constituentCount: cleanBlueprintCopy(scope.constituent_count) ?? scope.constituent_count ?? null,
        emergingMarketsIncluded: scope.emerging_markets_included ?? null,
      }
    : null;
}

function adaptQuickBrief(
  quickBrief: any,
): any {
  return quickBrief
    ? {
        statusState: quickBrief.status_state,
        statusLabel: cleanBlueprintCopy(quickBrief.status_label) ?? quickBrief.status_label,
        fundIdentity: quickBrief.fund_identity
          ? {
              ticker: cleanBlueprintCopy(quickBrief.fund_identity.ticker) ?? quickBrief.fund_identity.ticker,
              name: cleanBlueprintCopy(quickBrief.fund_identity.name) ?? quickBrief.fund_identity.name,
              issuer: cleanBlueprintCopy(quickBrief.fund_identity.issuer) ?? quickBrief.fund_identity.issuer ?? null,
              exposureLabel:
                cleanBlueprintCopy(quickBrief.fund_identity.exposure_label)
                ?? quickBrief.fund_identity.exposure_label
                ?? null,
            }
          : null,
        portfolioRole: cleanBlueprintCopy(quickBrief.portfolio_role) ?? quickBrief.portfolio_role ?? null,
        roleLabel: cleanBlueprintCopy(quickBrief.role_label) ?? quickBrief.role_label ?? null,
        summary: cleanBlueprintCopy(quickBrief.summary) ?? quickBrief.summary,
        decisionReasons: (quickBrief.decision_reasons ?? []).map((value: any) => cleanBlueprintCopy(value) ?? value),
        secondaryReasons: (quickBrief.secondary_reasons ?? []).map((value: any) => cleanBlueprintCopy(value) ?? value),
        keyFacts: (quickBrief.key_facts ?? []).map((row: any) => ({
          label: cleanBlueprintCopy(row.label) ?? row.label,
          value: cleanBlueprintCopy(row.value) ?? row.value,
        })),
        whyThisMattersLine:
          cleanBlueprintCopy(quickBrief.why_this_matters)
          ?? quickBrief.why_this_matters
          ?? null,
        compareFirstLine:
          cleanBlueprintCopy(quickBrief.compare_first)
          ?? quickBrief.compare_first
          ?? null,
        broaderAlternativeLine:
          cleanBlueprintCopy(quickBrief.broader_alternative)
          ?? quickBrief.broader_alternative
          ?? null,
        whatItSolvesLine:
          cleanBlueprintCopy(quickBrief.what_it_solves)
          ?? quickBrief.what_it_solves
          ?? null,
        whatItStillNeedsToProveLine:
          cleanBlueprintCopy(quickBrief.what_it_still_needs_to_prove)
          ?? quickBrief.what_it_still_needs_to_prove
          ?? null,
        decisionReadinessLine:
          cleanBlueprintCopy(quickBrief.decision_readiness)
          ?? quickBrief.decision_readiness
          ?? null,
        shouldIUse: quickBrief.should_i_use
          ? {
              bestFor: cleanBlueprintCopy(quickBrief.should_i_use.best_for) ?? quickBrief.should_i_use.best_for,
              notIdealFor:
                cleanBlueprintCopy(quickBrief.should_i_use.not_ideal_for) ?? quickBrief.should_i_use.not_ideal_for,
              useItWhen: cleanBlueprintCopy(quickBrief.should_i_use.use_it_when) ?? quickBrief.should_i_use.use_it_when,
              waitIf: cleanBlueprintCopy(quickBrief.should_i_use.wait_if) ?? quickBrief.should_i_use.wait_if,
              compareAgainst:
                cleanBlueprintCopy(quickBrief.should_i_use.compare_against) ?? quickBrief.should_i_use.compare_against,
            }
          : null,
        performanceChecks: (quickBrief.performance_checks ?? []).map((row: any) => ({
          checkId: row.check_id,
          label: cleanBlueprintCopy(row.label) ?? row.label,
          summary: cleanBlueprintCopy(row.summary) ?? row.summary,
          metric: cleanBlueprintCopy(row.metric) ?? row.metric ?? null,
        })),
        whatYouAreBuying: (quickBrief.what_you_are_buying ?? []).map((row: any) => ({
          label: cleanBlueprintCopy(row.label) ?? row.label,
          value: cleanBlueprintCopy(row.value) ?? row.value,
        })),
        portfolioFit: quickBrief.portfolio_fit
          ? {
              roleInPortfolio:
                cleanBlueprintCopy(quickBrief.portfolio_fit.role_in_portfolio)
                ?? quickBrief.portfolio_fit.role_in_portfolio,
              whatItDoesNotSolve:
                cleanBlueprintCopy(quickBrief.portfolio_fit.what_it_does_not_solve)
                ?? quickBrief.portfolio_fit.what_it_does_not_solve,
              currentNeed:
                cleanBlueprintCopy(quickBrief.portfolio_fit.current_need)
                ?? quickBrief.portfolio_fit.current_need,
            }
          : null,
        howToDecide: (quickBrief.how_to_decide ?? []).map((value: any) => cleanBlueprintCopy(value) ?? value),
        evidenceFooterDetail: quickBrief.evidence_footer_detail
          ? {
              evidenceQuality:
                cleanBlueprintCopy(quickBrief.evidence_footer_detail.evidence_quality)
                ?? quickBrief.evidence_footer_detail.evidence_quality,
              dataCompleteness:
                cleanBlueprintCopy(quickBrief.evidence_footer_detail.data_completeness)
                ?? quickBrief.evidence_footer_detail.data_completeness,
              documentSupport:
                cleanBlueprintCopy(quickBrief.evidence_footer_detail.document_support)
                ?? quickBrief.evidence_footer_detail.document_support,
              monitoringStatus:
                cleanBlueprintCopy(quickBrief.evidence_footer_detail.monitoring_status)
                ?? quickBrief.evidence_footer_detail.monitoring_status,
            }
          : null,
        scenarioEntry: quickBrief.scenario_entry
          ? {
              backdropSummary:
                cleanBlueprintCopy(quickBrief.scenario_entry.backdrop_summary)
                ?? quickBrief.scenario_entry.backdrop_summary,
              disclosureLabel:
                cleanBlueprintCopy(quickBrief.scenario_entry.disclosure_label)
                ?? quickBrief.scenario_entry.disclosure_label,
            }
          : null,
        kronosMarketSetup: quickBrief.kronos_market_setup
          ? {
              scopeKey: cleanBlueprintCopy(quickBrief.kronos_market_setup.scope_key) ?? quickBrief.kronos_market_setup.scope_key ?? null,
              scopeLabel: cleanBlueprintCopy(quickBrief.kronos_market_setup.scope_label) ?? quickBrief.kronos_market_setup.scope_label ?? null,
              marketSetupState:
                cleanBlueprintCopy(quickBrief.kronos_market_setup.market_setup_state)
                ?? quickBrief.kronos_market_setup.market_setup_state
                ?? null,
              routeLabel:
                cleanBlueprintCopy(quickBrief.kronos_market_setup.route_label)
                ?? quickBrief.kronos_market_setup.route_label
                ?? null,
              forecastObjectLabel:
                cleanBlueprintCopy(quickBrief.kronos_market_setup.forecast_object_label)
                ?? quickBrief.kronos_market_setup.forecast_object_label
                ?? null,
              horizonLabel:
                cleanBlueprintCopy(quickBrief.kronos_market_setup.horizon_label)
                ?? quickBrief.kronos_market_setup.horizon_label
                ?? null,
              pathSupportLabel:
                cleanBlueprintCopy(quickBrief.kronos_market_setup.path_support_label)
                ?? quickBrief.kronos_market_setup.path_support_label
                ?? null,
              confidenceLabel:
                cleanBlueprintCopy(quickBrief.kronos_market_setup.confidence_label)
                ?? quickBrief.kronos_market_setup.confidence_label
                ?? null,
              freshnessLabel:
                cleanBlueprintCopy(quickBrief.kronos_market_setup.freshness_label)
                ?? quickBrief.kronos_market_setup.freshness_label
                ?? null,
              decisionImpactText:
                cleanBlueprintCopy(quickBrief.kronos_market_setup.decision_impact_text)
                ?? quickBrief.kronos_market_setup.decision_impact_text
                ?? null,
            }
          : null,
        kronosDecisionBridge: quickBrief.kronos_decision_bridge
          ? {
              statusLabel:
                cleanBlueprintCopy(quickBrief.kronos_decision_bridge.status_label)
                ?? quickBrief.kronos_decision_bridge.status_label
                ?? null,
              statusTone:
                confidenceTone(quickBrief.kronos_decision_bridge.status_tone ?? null),
              supportStatement:
                cleanBlueprintCopy(quickBrief.kronos_decision_bridge.support_statement)
                ?? quickBrief.kronos_decision_bridge.support_statement
                ?? null,
              gateEffect:
                cleanBlueprintCopy(quickBrief.kronos_decision_bridge.gate_effect)
                ?? quickBrief.kronos_decision_bridge.gate_effect
                ?? null,
            }
          : null,
        kronosCompareCheck: quickBrief.kronos_compare_check
          ? {
              regimeCheckText:
                cleanBlueprintCopy(quickBrief.kronos_compare_check.regime_check_text)
                ?? quickBrief.kronos_compare_check.regime_check_text
                ?? null,
              affectsExposurePreference: Boolean(quickBrief.kronos_compare_check.affects_exposure_preference),
              affectsPeerPreference: Boolean(quickBrief.kronos_compare_check.affects_peer_preference),
            }
          : null,
        fundProfile: quickBrief.fund_profile
          ? {
              objective: cleanBlueprintCopy(quickBrief.fund_profile.objective) ?? quickBrief.fund_profile.objective ?? null,
              benchmark: cleanBlueprintCopy(quickBrief.fund_profile.benchmark) ?? quickBrief.fund_profile.benchmark ?? null,
              benchmarkFamily:
                cleanBlueprintCopy(quickBrief.fund_profile.benchmark_family) ?? quickBrief.fund_profile.benchmark_family ?? null,
              issuer: cleanBlueprintCopy(quickBrief.fund_profile.issuer) ?? quickBrief.fund_profile.issuer ?? null,
              domicile: cleanBlueprintCopy(quickBrief.fund_profile.domicile) ?? quickBrief.fund_profile.domicile ?? null,
              replication: cleanBlueprintCopy(quickBrief.fund_profile.replication) ?? quickBrief.fund_profile.replication ?? null,
              distribution: cleanBlueprintCopy(quickBrief.fund_profile.distribution) ?? quickBrief.fund_profile.distribution ?? null,
              fundAssets: cleanBlueprintCopy(quickBrief.fund_profile.fund_assets) ?? quickBrief.fund_profile.fund_assets ?? null,
              shareClassAssets:
                cleanBlueprintCopy(quickBrief.fund_profile.share_class_assets) ?? quickBrief.fund_profile.share_class_assets ?? null,
              holdingsCount:
                cleanBlueprintCopy(quickBrief.fund_profile.holdings_count) ?? quickBrief.fund_profile.holdings_count ?? null,
              launchDate: cleanBlueprintCopy(quickBrief.fund_profile.launch_date) ?? quickBrief.fund_profile.launch_date ?? null,
              documents: (quickBrief.fund_profile.documents ?? []).map((row: any) => ({
                label: cleanBlueprintCopy(row.label) ?? row.label,
                url: row.url ?? null,
              })),
            }
          : null,
        listingProfile: quickBrief.listing_profile
          ? {
              ticker: cleanBlueprintCopy(quickBrief.listing_profile.ticker) ?? quickBrief.listing_profile.ticker ?? null,
              exchange: cleanBlueprintCopy(quickBrief.listing_profile.exchange) ?? quickBrief.listing_profile.exchange ?? null,
              tradingCurrency:
                cleanBlueprintCopy(quickBrief.listing_profile.trading_currency) ?? quickBrief.listing_profile.trading_currency ?? null,
              spreadProxy:
                cleanBlueprintCopy(quickBrief.listing_profile.spread_proxy) ?? quickBrief.listing_profile.spread_proxy ?? null,
              asOf: quickBrief.listing_profile.as_of ?? null,
            }
          : null,
        indexScopeExplainer: adaptIndexScopeExplainer(quickBrief.index_scope_explainer),
        decisionProofPack: quickBrief.decision_proof_pack
          ? {
              whyInScope:
                cleanBlueprintCopy(quickBrief.decision_proof_pack.why_in_scope) ?? quickBrief.decision_proof_pack.why_in_scope ?? null,
              compareAgainstPeers:
                cleanBlueprintCopy(quickBrief.decision_proof_pack.compare_against_peers) ?? quickBrief.decision_proof_pack.compare_against_peers ?? null,
              broaderAlternative:
                cleanBlueprintCopy(quickBrief.decision_proof_pack.broader_alternative) ?? quickBrief.decision_proof_pack.broader_alternative ?? null,
              whatMustBeTrueToPreferThis:
                cleanBlueprintCopy(quickBrief.decision_proof_pack.what_must_be_true_to_prefer_this) ?? quickBrief.decision_proof_pack.what_must_be_true_to_prefer_this ?? null,
              whyNotCompleteSolution:
                cleanBlueprintCopy(quickBrief.decision_proof_pack.why_not_complete_solution) ?? quickBrief.decision_proof_pack.why_not_complete_solution ?? null,
              feePremiumQuestion:
                cleanBlueprintCopy(quickBrief.decision_proof_pack.fee_premium_question) ?? quickBrief.decision_proof_pack.fee_premium_question ?? null,
            }
          : null,
        performanceTrackingPack: quickBrief.performance_tracking_pack
          ? {
              trackingDifferenceCurrentPeriod:
                cleanBlueprintCopy(quickBrief.performance_tracking_pack.tracking_difference_current_period)
                ?? quickBrief.performance_tracking_pack.tracking_difference_current_period
                ?? null,
              trackingDifference1Y:
                cleanBlueprintCopy(quickBrief.performance_tracking_pack.tracking_difference_1y)
                ?? quickBrief.performance_tracking_pack.tracking_difference_1y
                ?? null,
              trackingError1Y:
                cleanBlueprintCopy(quickBrief.performance_tracking_pack.tracking_error_1y)
                ?? quickBrief.performance_tracking_pack.tracking_error_1y
                ?? null,
              notes: (quickBrief.performance_tracking_pack.notes ?? []).map((value: any) => cleanBlueprintCopy(value) ?? value),
            }
          : null,
        compositionPack: quickBrief.composition_pack
          ? {
              topCountry: cleanBlueprintCopy(quickBrief.composition_pack.top_country) ?? quickBrief.composition_pack.top_country ?? null,
              topSector: cleanBlueprintCopy(quickBrief.composition_pack.top_sector) ?? quickBrief.composition_pack.top_sector ?? null,
              topHolding: cleanBlueprintCopy(quickBrief.composition_pack.top_holding) ?? quickBrief.composition_pack.top_holding ?? null,
              concentrationNote:
                cleanBlueprintCopy(quickBrief.composition_pack.concentration_note) ?? quickBrief.composition_pack.concentration_note ?? null,
            }
          : null,
        documentCoverage: quickBrief.document_coverage
          ? {
              statusState: cleanBlueprintCopy(quickBrief.document_coverage.status_state) ?? quickBrief.document_coverage.status_state ?? null,
              statusLabel: cleanBlueprintCopy(quickBrief.document_coverage.status_label) ?? quickBrief.document_coverage.status_label ?? null,
              documentCount:
                typeof quickBrief.document_coverage.document_count === "number" ? quickBrief.document_coverage.document_count : null,
              presentDocumentLabels: (quickBrief.document_coverage.present_document_labels ?? []).map((value: any) => cleanBlueprintCopy(value) ?? value),
              missingDocumentLabels: (quickBrief.document_coverage.missing_document_labels ?? []).map((value: any) => cleanBlueprintCopy(value) ?? value),
              lastRefreshedAt: quickBrief.document_coverage.last_refreshed_at ?? null,
            }
          : null,
        peerComparePack: quickBrief.peer_compare_pack
          ? {
              primaryQuestion:
                cleanBlueprintCopy(quickBrief.peer_compare_pack.primary_question) ?? quickBrief.peer_compare_pack.primary_question ?? null,
              rows: (quickBrief.peer_compare_pack.rows ?? []).map((row: any) => ({
                fundName: cleanBlueprintCopy(row.fund_name) ?? row.fund_name,
                role: cleanBlueprintCopy(row.role) ?? row.role,
                benchmark: cleanBlueprintCopy(row.benchmark) ?? row.benchmark ?? null,
                benchmarkFamily: cleanBlueprintCopy(row.benchmark_family) ?? row.benchmark_family ?? null,
                ter: cleanBlueprintCopy(row.ter) ?? row.ter ?? null,
                fundAssets: cleanBlueprintCopy(row.fund_assets) ?? row.fund_assets ?? null,
                shareClassAssets: cleanBlueprintCopy(row.share_class_assets) ?? row.share_class_assets ?? null,
                distribution: cleanBlueprintCopy(row.distribution) ?? row.distribution ?? null,
                domicile: cleanBlueprintCopy(row.domicile) ?? row.domicile ?? null,
                whyThisPeerMatters:
                  cleanBlueprintCopy(row.why_this_peer_matters) ?? row.why_this_peer_matters ?? null,
              })),
            }
          : null,
        overlayNote: cleanBlueprintCopy(quickBrief.overlay_note) ?? quickBrief.overlay_note ?? null,
      }
    : null;
}

type RawCompareDimension = {
  dimension?: string | null;
  dimension_id?: string | null;
  label?: string | null;
  group?: string | null;
  importance?: string | null;
  discriminating?: boolean | null;
  winner?: string | null;
  values?: Array<{ candidate_id: string; value: string }>;
};

export type ChangeDisplay = {
  id: string;
  eventType: string;
  category: string;
  direction: string | null;
  ticker: string;
  sleeve: string;
  sleeveId: string | null;
  typeLabel: string;
  impactLevel: "high" | "medium" | "low";
  impactLabel: string;
  impactTone: Tone;
  needsReview: boolean;
  timestamp: string;
  changedAtUtc: string;
  previousState: string;
  currentState: string;
  implication: string;
  title: string;
  actionability: string | null;
  scope: string | null;
  confidence: string | null;
  driverSummary: string | null;
  whyItMatters: string | null;
  consequence: string | null;
  nextAction: string | null;
  whatWouldReverse: string | null;
  reportTab: string | null;
  candidateId: string | null;
  renderMode: string | null;
  materialityClass: string | null;
  auditDetail: ChangeDetail["audit_detail"] | null;
  changeDetail: ChangeDetail | null;
};

function compareDimensionBucketKey(dimension: RawCompareDimension): string {
  const key = [
    dimension.dimension_id,
    dimension.dimension,
    dimension.label,
    dimension.group,
  ]
    .map((value) => String(value ?? "").trim().toLowerCase())
    .filter(Boolean)
    .join(" ");
  if (/sleeve|job|fit|role|substitute|mandate|capital priority/.test(key)) return "sleeve_job";
  if (/benchmark|exposure|index|tracking|baseline|region|sector/.test(key)) return "benchmark_exposure";
  if (/implementation|cost|ter|spread|aum|tax|domicile|currency|replication|distribution|liquidity/.test(key)) return "implementation_cost";
  if (/source|authority|evidence|identity|coverage|stale|truth|conflict|confidence/.test(key)) return "source_integrity";
  if (/market|path|forecast|scenario|fragility|drift|threshold|proxy|kronos/.test(key)) return "market_path_context";
  return "secondary";
}

function compareDimensionBucketLabel(dimension: RawCompareDimension): string | null {
  const bucket = compareDimensionBucketKey(dimension);
  if (bucket === "sleeve_job") return "Sleeve job";
  if (bucket === "benchmark_exposure") return "Benchmark and exposure";
  if (bucket === "implementation_cost") return "Implementation and cost";
  if (bucket === "source_integrity") return "Source integrity";
  if (bucket === "market_path_context") return "Market-path context";
  return dimension.group ? humanizeState(dimension.group) : null;
}

function compareDimensionBucketOrder(dimension: RawCompareDimension): number {
  const bucket = compareDimensionBucketKey(dimension);
  if (bucket === "sleeve_job") return 0;
  if (bucket === "benchmark_exposure") return 1;
  if (bucket === "implementation_cost") return 2;
  if (bucket === "source_integrity") return 3;
  if (bucket === "market_path_context") return 4;
  return 5;
}

function compareImportanceOrder(value: string | null | undefined): number {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw === "primary" || raw === "high") return 0;
  if (raw === "secondary" || raw === "medium") return 1;
  if (raw === "supporting" || raw === "low") return 2;
  return 3;
}

function hasMeaningfulCompareSpread(dimension: RawCompareDimension): boolean {
  const normalized = (dimension.values ?? [])
    .map((item) => String(item.value ?? "").trim().toLowerCase())
    .filter(Boolean);
  return normalized.length <= 1 || new Set(normalized).size > 1;
}

export type CandidateReportDisplay = {
  meta: SurfaceMeta;
  summaryChips: SummaryChip[];
  tabs: Array<{ id: ReportTab; label: string }>;
  rationale: string;
  investmentCase: string;
  currentImplication: string;
  actionBoundary: string | null;
  whatChangesView: string | null;
  failureSummary: {
    primaryLabel: string | null;
    summary: string | null;
    hardClasses: string[];
    reviewClasses: string[];
    confidenceDragClasses: string[];
  } | null;
  scoreBreakdown: {
    total: number;
    recommendation: number | null;
    recommendationMerit: number | null;
    investmentMerit: number | null;
    deployability: number | null;
    truthConfidence: number | null;
    truthConfidenceBand: string | null;
    truthConfidenceSummary: string | null;
    deployment: number | null;
    admissibility: number | null;
    admissibilityIdentity: number | null;
    implementation: number | null;
    sourceIntegrity: number | null;
    evidence: number | null;
    sleeveFit: number | null;
    identity: number | null;
    benchmarkFidelity: number | null;
    marketPathSupport: number | null;
    longHorizonQuality: number | null;
    instrumentQuality: number | null;
    portfolioFit: number | null;
    optimality: number | null;
    readiness: number | null;
    confidencePenalty: number | null;
    readinessPosture: string | null;
    readinessSummary: string | null;
    deployabilityBadge: string | null;
    summary: string | null;
  } | null;
  scoreSummary: {
    averageScore: number;
    componentCountUsed: number;
    tone: Tone;
    reliabilityState: "strong" | "mixed" | "weak";
    reliabilityNote: string | null;
    components: Array<{
      id: string;
      label: string;
      score: number;
      tone: Tone;
      summary: string;
    }>;
  } | null;
  scoreComponents: Array<{
    id?: string;
    label: string;
    score: number;
    band?: string | null;
    confidence?: number | null;
    tone: Tone;
    summary: string;
    reasons?: string[];
    capsApplied?: string[];
    fieldDrivers?: string[];
  }>;
  upgradeCondition: string | null;
  downgradeCondition: string | null;
  killCondition: string | null;
  decisionConditions: {
    intro: string;
    items: CandidateDecisionConditionDisplayItem[];
  } | null;
  tradeoffs: string[];
  baselineComparisons: Array<{
    label: string;
    summary: string;
    verdict: string | null;
  }>;
  doctrineAnnotations: string[];
  evidenceDepth: string;
  mandateBoundary: string | null;
  overlayMessage: string | null;
  quickBrief: {
    statusState: string;
    statusLabel: string;
    fundIdentity: {
      ticker: string;
      name: string;
      issuer: string | null;
      exposureLabel: string | null;
    } | null;
    portfolioRole: string | null;
    roleLabel: string | null;
    summary: string;
    decisionReasons: string[];
    secondaryReasons: string[];
    keyFacts: Array<{ label: string; value: string }>;
    whyThisMattersLine: string | null;
    compareFirstLine: string | null;
    broaderAlternativeLine: string | null;
    whatItSolvesLine: string | null;
    whatItStillNeedsToProveLine: string | null;
    decisionReadinessLine: string | null;
    shouldIUse: {
      bestFor: string;
      notIdealFor: string;
      useItWhen: string;
      waitIf: string;
      compareAgainst: string;
    } | null;
    performanceChecks: Array<{
      checkId: string;
      label: string;
      summary: string;
      metric: string | null;
    }>;
    whatYouAreBuying: Array<{ label: string; value: string }>;
    portfolioFit: {
      roleInPortfolio: string;
      whatItDoesNotSolve: string;
      currentNeed: string;
    } | null;
    howToDecide: string[];
    evidenceFooterDetail: {
      evidenceQuality: string;
      dataCompleteness: string;
      documentSupport: string;
      monitoringStatus: string;
    } | null;
    scenarioEntry: {
      backdropSummary: string;
      disclosureLabel: string;
    } | null;
    kronosMarketSetup: {
      scopeKey: string | null;
      scopeLabel: string | null;
      marketSetupState: string | null;
      routeLabel: string | null;
      forecastObjectLabel: string | null;
      horizonLabel: string | null;
      pathSupportLabel: string | null;
      confidenceLabel: string | null;
      freshnessLabel: string | null;
      downsideRiskLabel: string | null;
      driftLabel: string | null;
      volatilityRegimeLabel: string | null;
      decisionImpactText: string | null;
      qualityGate: string | null;
      asOf: string | null;
      scenarioAvailable: boolean | null;
      openScenarioCta: string | null;
    } | null;
    kronosDecisionBridge: {
      selectionContext: string | null;
      regimeSummary: string | null;
      selectionConsequence: string | null;
      wrapperBoundaryText: string | null;
      supportsExposureChoice: boolean | null;
      supportsWrapperChoice: boolean | null;
      decisionStrengthLabel: string | null;
    } | null;
    kronosCompareCheck: {
      compareContext: string | null;
      regimeCheckText: string | null;
      affectsPeerPreference: boolean | null;
      affectsExposurePreference: boolean | null;
    } | null;
    kronosScenarioPack: {
      observedPath: string | null;
      basePath: string | null;
      downsidePath: string | null;
      stressPath: string | null;
      uncertaintyBand: string | null;
      driftState: string | null;
      fragilityState: string | null;
      thresholdFlags: string[];
      qualityGate: string | null;
      provenance: string | null;
      refreshStatus: string | null;
      lastRunAt: string | null;
    } | null;
    kronosOptionalMetrics: {
      upsideProbability: string | null;
      downsideBreachProbability: string | null;
      volatilityElevationProbability: string | null;
      changeVsPriorRun: string | null;
    } | null;
    peerComparePack: {
      candidateSymbol: string;
      candidateLabel: string;
      primaryQuestion: string | null;
      comparisonBasis: string | null;
      rows: Array<{
        role: string;
        fundName: string;
        tickerOrLine: string | null;
        isin: string | null;
        benchmark: string | null;
        benchmarkFamily: string | null;
        exposureScope: string | null;
        developedOnly: boolean | null;
        emergingMarketsIncluded: boolean | null;
        ter: string | null;
        fundAssets: string | null;
        shareClassAssets: string | null;
        holdingsCount: string | null;
        replication: string | null;
        distribution: string | null;
        domicile: string | null;
        launchDate: string | null;
        trackingError1Y: string | null;
        trackingError3Y: string | null;
        trackingError5Y: string | null;
        trackingDifference1Y: string | null;
        trackingDifference3Y: string | null;
        listingExchange: string | null;
        listingCurrency: string | null;
        whyThisPeerMatters: string | null;
        terDelta: string | null;
        holdingsDelta: string | null;
        sameIndex: boolean | null;
        sameJob: boolean | null;
        sameDistribution: boolean | null;
        sameDomicile: boolean | null;
      }>;
    } | null;
    fundProfile: {
      objective: string | null;
      benchmark: string | null;
      benchmarkFamily: string | null;
      domicile: string | null;
      replication: string | null;
      distribution: string | null;
      fundAssets: string | null;
      shareClassAssets: string | null;
      holdingsCount: string | null;
      launchDate: string | null;
      issuer: string | null;
      documents: Array<{ label: string; url: string | null }>;
    } | null;
    listingProfile: {
      exchange: string | null;
      tradingCurrency: string | null;
      ticker: string | null;
      marketPrice: string | null;
      nav: string | null;
      spreadProxy: string | null;
      volume: string | null;
      premiumDiscount: string | null;
      asOf: string | null;
    } | null;
    indexScopeExplainer: {
      label: string | null;
      scopeType: string | null;
      displayTitle: string | null;
      summary: string | null;
      covers: string[];
      doesNotCover: string[];
      sleeveRelevance: string | null;
      specificity: string | null;
      sourceBasis: string | null;
      confidence: string | null;
      indexName: string | null;
      coverageStatement: string | null;
      includesStatement: string | null;
      excludesStatement: string | null;
      marketCapScope: string | null;
      countryCount: string | null;
      constituentCount: string | null;
      emergingMarketsIncluded: boolean | null;
    } | null;
    decisionProofPack: {
      whyCandidateExists: string | null;
      whyInScope: string | null;
      whyNotCompleteSolution: string | null;
      bestSameJobPeers: string | null;
      broaderControlPeer: string | null;
      feePremiumQuestion: string | null;
      whatMustBeTrueToPreferThis: string | null;
      whatWouldChangeVerdict: string | null;
    } | null;
    performanceTrackingPack: {
      return1Y: string | null;
      return3Y: string | null;
      return5Y: string | null;
      benchmarkReturn1Y: string | null;
      benchmarkReturn3Y: string | null;
      benchmarkReturn5Y: string | null;
      trackingError1Y: string | null;
      trackingError3Y: string | null;
      trackingError5Y: string | null;
      trackingDifferenceCurrentPeriod: string | null;
      trackingDifference1Y: string | null;
      trackingDifference3Y: string | null;
      trackingDifference5Y: string | null;
      volatility: string | null;
      maxDrawdown: string | null;
      asOf: string | null;
    } | null;
    compositionPack: {
      numberOfStocks: string | null;
      topHoldings: Array<{ label: string; value: string }>;
      countryWeights: Array<{ label: string; value: string }>;
      sectorWeights: Array<{ label: string; value: string }>;
      top10Weight: string | null;
      usWeight: string | null;
      nonUsWeight: string | null;
      emWeight: string | null;
    } | null;
    documentCoverage: {
      factsheetPresent: boolean | null;
      kidPresent: boolean | null;
      prospectusPresent: boolean | null;
      annualReportPresent: boolean | null;
      benchmarkMethodologyPresent: boolean | null;
      lastRefreshedAt: string | null;
      documentCount: number | null;
      missingDocuments: string[];
      documentConfidenceGrade: string | null;
    } | null;
    whyItMatters: Array<{ label: string; value: string }>;
    performanceAndImplementation: Array<{ label: string; value: string }>;
    overlayNote: string | null;
    backdropNote: string | null;
    evidenceFooter: Array<{ label: string; value: string }>;
  } | null;
  marketHistorySummary: string | null;
  marketHistoryCharts: ChartPanelDisplay[];
  marketHistoryWindows: Array<{
    label: string;
    period: string;
    fundReturn: string;
    benchmarkReturn: string;
    note: string;
  }>;
  scenarioBlocks: Array<{
    label: string;
    trigger: string;
    expectedReturn: string;
    portfolioEffect: string;
    shortTerm: string | null;
    longTerm: string | null;
  }>;
  scenarioCharts: ChartPanelDisplay[];
  riskBlocks: Array<{
    category: string;
    title: string;
    detail: string;
  }>;
  competitionBlocks: Array<{
    label: string;
    summary: string;
    verdict: string | null;
  }>;
  competitionCharts: ChartPanelDisplay[];
  evidenceSources: Array<{
    label: string;
    freshness: string;
    directness: string;
    url: string | null;
  }>;
  implementationProfile: Array<{ label: string; value: string; caution?: string | null }> | null;
  fieldIssues: Array<{
    label: string;
    status: string;
    fixability: string;
    summary: string;
  }>;
  sourceAuthorityFields: Array<{
    fieldName: string;
    label: string;
    sourceLabel: string;
    authorityClass: string;
    freshness: string;
    isCritical: boolean;
    isRecommendationCritical: boolean;
  }> | null;
  primaryDocuments: Array<{
    docType: string;
    status: string;
    retrievedAt: string | null;
  }> | null;
  decisionThresholds: Array<{
    label: string;
    value: string;
  }>;
  researchSupport: {
    thesisDrift: string | null;
    marketContext: string | null;
    draftingSummary: string | null;
    keyQuestions: string[];
    nextSteps: string[];
    retrievalGuides: Array<{
      label: string;
      query: string;
      reason: string;
      priority: string;
    }>;
    newsClusters: Array<{
      label: string;
      summary: string;
      tone: Tone;
      headlines: string[];
    }>;
    logicSteps: Array<{
      label: string;
      detail: string;
    }>;
    sentimentSummary: string | null;
    sentimentTone: Tone;
  } | null;
  marketPathSupport: BlueprintMarketPathSupport | null;
  marketPath: MarketPathPresentation | null;
};

export type BlueprintDisplay = {
  meta: SurfaceMeta;
  degradedMessage: string | null;
  summary: NonNullable<BlueprintExplorerContract["summary"]> | null;
  summaryChips: SummaryChip[];
  sleeves: BlueprintSleeveDisplay[];
  compare: CompareDisplay | null;
  compareMessage: string;
  changes: ChangeDisplay[];
  changesEmptyMessage: string | null;
  changesSummary: NonNullable<ChangesContract["summary"]> | null;
  changesAuditGroups: NonNullable<ChangesContract["audit_groups"]>;
  changesAvailableSleeves: NonNullable<ChangesContract["available_sleeves"]>;
  changesAvailableCategories: string[];
  changesPagination: NonNullable<ChangesContract["pagination"]> | null;
  changesDailySourceScan: NonNullable<ChangesContract["daily_source_scan"]> | null;
  changesFreshness: {
    state: string | null;
    latestEventAt: string | null;
    latestEventAgeDays: number | null;
  };
  report: CandidateReportDisplay | null;
  inspector: InspectorLine[];
};

export type NotebookDisplay = {
  meta: SurfaceMeta;
  degradedMessage: string | null;
  memoryFoundationNote: string | null;
  activeNote: {
    date: string;
    title: string;
    linked: string;
    nextReview: string | null;
    thesis: string;
    assumptions: string | null;
    invalidation: string | null;
    watchItems: string | null;
    reflections: string | null;
  };
  finalizedNotes: Array<{
    date: string;
    title: string;
    body: string;
  }>;
  archiveNotes: Array<{
    date: string;
    title: string;
    body: string;
  }>;
  researchSupport: CandidateReportDisplay["researchSupport"];
  inspector: InspectorLine[];
};

export type EvidenceDisplay = {
  meta: SurfaceMeta;
  degradedMessage: string | null;
  summaryTiles: SummaryChip[];
  objectGroups: Array<{
    title: string;
    items: Array<{
      name: string;
      direct: string;
      proxy: string;
      stale: string;
      gap: boolean;
      claims: Array<{ text: string; meta: string }>;
    }>;
  }>;
  documents: Array<{
    title: string;
    type: string;
    linked: string;
    age: string;
    stale: boolean;
  }>;
  mappings: Array<{
    sleeve: string;
    instrument: string;
    benchmark: string;
    baseline: string;
    directness: string;
  }>;
  taxAssumptions: Array<{
    label: string;
    value: string;
  }>;
  gaps: Array<{
    object: string;
    issue: string;
  }>;
  researchSupport: CandidateReportDisplay["researchSupport"];
  inspector: InspectorLine[];
};

const PRIMARY_BADGES: Record<PrimaryView, Badge[]> = {
  portfolio: [],  // built dynamically in adaptPortfolio()
  brief: [],  // built dynamically in adaptDailyBrief()
  candidates: [],
  notebook: [],
  evidence: [],
};

const SURFACE_META: Record<PrimaryView, { kicker: string; title: string; copy: string }> = {
  portfolio: {
    kicker: "Book state",
    title: "Portfolio",
    copy: "What you own, how it compares with the plan, and what matters now.",
  },
  brief: {
    kicker: "What changed",
    title: "Daily Brief",
    copy: "Daily changes grouped by urgency and portfolio consequence rather than by strategist framing.",
  },
  candidates: {
    kicker: "Capital deployment",
    title: "Blueprint",
    copy: "Start with sleeve capital priorities, then compare candidates within the sleeve that actually competes for the next dollar.",
  },
  notebook: {
    kicker: "Reasoning",
    title: "Research Notebook",
    copy: "Record what we think, why we think it, what we assumed, and what would change our mind.",
  },
  evidence: {
    kicker: "Supporting material",
    title: "Evidence Workspace",
    copy: "Hold deeper support material without cluttering main decision surfaces.",
  },
};

const EMPTY_SLEEVE_ROWS = [
  "Global Equity",
  "Developed ex-US",
  "Emerging Markets",
  "China Satellite",
  "IG Bonds",
  "Cash and Bills",
  "Real Assets",
  "Alternatives",
  "Convex Protection",
];

function surfaceMeta(view: PrimaryView, extraBadges: Badge[] = []): SurfaceMeta {
  return {
    ...SURFACE_META[view],
    badges: [...PRIMARY_BADGES[view], ...extraBadges],
  };
}

function text(value: string | null | undefined, fallback = "Unavailable"): string {
  const normalized = String(value ?? "").trim();
  return normalized || fallback;
}

function titleFromCode(value: string | null | undefined): string {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return "Unassigned";
  }
  return raw
    .replace(/^candidate_instrument_/, "")
    .replace(/^sleeve_/, "")
    .replace(/^surface_/, "")
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function tickerFromCandidate(candidateId: string | null | undefined): string {
  const raw = String(candidateId ?? "").trim();
  if (!raw) {
    return "Surface";
  }
  if (raw.startsWith("candidate_instrument_")) {
    return raw.replace("candidate_instrument_", "").toUpperCase();
  }
  return raw.toUpperCase();
}

function formatDate(value: string | null | undefined): string {
  if (!value) {
    return "No date";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(parsed);
}

function formatDateTime(value: string | null | undefined): string {
  if (!value) {
    return "No timestamp";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

function formatCurrency(value: number | null | undefined, currency = "USD"): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "—";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    maximumFractionDigits: value >= 1000 ? 0 : 2,
  }).format(value);
}

function percentFromMaybeFraction(value: number | null | undefined): number {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return 0;
  }
  return Math.abs(value) <= 1 ? value * 100 : value;
}

function formatPercent(value: number | null | undefined, signed = false): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "—";
  }
  const pct = percentFromMaybeFraction(value);
  const prefix = signed && pct > 0 ? "+" : "";
  return `${prefix}${pct.toFixed(1)}%`;
}

function formatBps(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "—";
  }
  return `${value.toFixed(1)} bps`;
}

function humanizeState(value: string | null | undefined): string {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return "Unknown";
  }
  const map: Record<string, string> = {
    no_data: "No data",
    wait_for_holdings: "Awaiting holdings",
    monitor_only: "Monitor only",
    review_required: "Review required",
    act_now: "Act now",
    eligible: "Eligible now",
    review: "Under review",
    blocked: "Blocked",
    watch: "Monitor",
    research_only: "Research only",
    direct: "Direct",
    "sleeve-proxy": "Sleeve proxy",
    "macro-only": "Macro only",
    moderate: "Moderate",
    substantial: "Substantial",
    limited: "Limited",
    provider_unavailable: "Provider unavailable",
    auth_missing: "Auth missing",
    insufficient_series_history: "Insufficient series history",
    unsupported_frequency: "Unsupported frequency",
    unsupported_covariates: "Unsupported covariates",
    evaluation_failed: "Evaluation failed",
    support_not_trusted: "Support not trusted",
    breached: "Breached",
    degraded: "Degraded",
    admissible: "Admissible",
    review_only: "Review only",
    verified: "Verified",
    soft_drift: "Soft drift",
    hard_conflict: "Hard conflict",
    execution_efficient: "Execution efficient",
    execution_mixed: "Execution mixed",
    execution_weak: "Execution weak",
    overlay_absent: "Overlay absent",
    in_band: "In band",
    out_of_band: "Out of band",
    awaiting_holdings: "Awaiting holdings",
    incumbent_unavailable: "Incumbent unavailable",
    funding_path_unresolved: "Funding path unresolved",
    portfolio_consequence_not_specific: "Portfolio consequence not specific",
    live: "Live",
    cached: "Cached",
    fallback: "Fallback",
    donor: "Donor",
  };
  return map[raw] ?? titleFromCode(raw);
}

function freshnessTone(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim();
  if (raw === "fresh_full_rebuild" || raw === "fresh_partial_rebuild") {
    return "good";
  }
  if (raw === "stored_valid_context" || raw === "current") {
    return "neutral";
  }
  if (raw === "degraded_monitoring_mode" || raw === "no_data") {
    return "warn";
  }
  if (raw === "execution_failed_or_incomplete") {
    return "bad";
  }
  return "neutral";
}

function freshnessLabel(value: string | null | undefined): string {
  const raw = String(value ?? "").trim();
  const map: Record<string, string> = {
    no_data: "No holdings loaded",
    fresh_full_rebuild: "Fresh full rebuild",
    fresh_partial_rebuild: "Fresh partial rebuild",
    stored_valid_context: "Stored valid context",
    degraded_monitoring_mode: "Degraded monitoring mode",
    execution_failed_or_incomplete: "Execution failed or incomplete",
  };
  return map[raw] ?? humanizeState(raw);
}

function badgeTone(value: string | null | undefined): Tone | undefined {
  const raw = String(value ?? "").trim();
  if (raw === "good" || raw === "warn" || raw === "bad" || raw === "neutral" || raw === "info") {
    return raw;
  }
  return undefined;
}

function marketStateFreshnessTone(
  freshnessMode: string | null | undefined,
  freshness: string | null | undefined,
  liveOrCache: string | null | undefined,
  validationStatus?: string | null | undefined,
): Tone {
  const freshnessModeRaw = String(freshnessMode ?? "").trim();
  const freshnessRaw = String(freshness ?? "").trim();
  const liveRaw = String(liveOrCache ?? "").trim();
  const validationRaw = String(validationStatus ?? "").trim();
  if (validationRaw && validationRaw !== "valid") {
    return "bad";
  }
  if (freshnessModeRaw === "fresh_current_slot") {
    return "good";
  }
  if (freshnessModeRaw === "fresh_previous_slot") {
    return "warn";
  }
  if (freshnessModeRaw === "stale" || freshnessModeRaw === "rejected") {
    return "bad";
  }
  if (liveRaw === "fallback" || ["degraded_monitoring_mode", "execution_failed_or_incomplete", "stale", "unavailable"].includes(freshnessRaw)) {
    return "bad";
  }
  if ((liveRaw === "cache" && ["fresh_full_rebuild", "current", "fresh"].includes(freshnessRaw)) || (liveRaw === "live" && ["fresh_full_rebuild", "current", "fresh"].includes(freshnessRaw))) {
    return "good";
  }
  if (liveRaw === "cache" || ["fresh_partial_rebuild", "stored_valid_context", "aging"].includes(freshnessRaw)) {
    return "warn";
  }
  return "neutral";
}

function marketStateFreshnessLabel(
  freshnessMode: string | null | undefined,
  freshness: string | null | undefined,
  liveOrCache: string | null | undefined,
  validationStatus?: string | null | undefined,
): string {
  const freshnessModeRaw = String(freshnessMode ?? "").trim();
  const validationRaw = String(validationStatus ?? "").trim();
  if (validationRaw && validationRaw !== "valid") {
    return humanizeState(validationRaw);
  }
  if (freshnessModeRaw) {
    return humanizeState(freshnessModeRaw);
  }
  const freshnessText = freshnessLabel(freshness);
  const modeRaw = String(liveOrCache ?? "").trim();
  const modeText = modeRaw ? humanizeState(modeRaw) : "";
  if (!freshnessText) return modeText || "Unknown";
  if (!modeText || modeText.toLowerCase() === "unknown") return freshnessText;
  return `${freshnessText} · ${modeText}`;
}

function marketStateIsNonFresh(
  freshnessMode: string | null | undefined,
  freshness: string | null | undefined,
  liveOrCache: string | null | undefined,
  validationStatus?: string | null | undefined,
): boolean {
  const freshnessModeRaw = String(freshnessMode ?? "").trim();
  const validationRaw = String(validationStatus ?? "").trim();
  if (validationRaw && validationRaw !== "valid") {
    return true;
  }
  if (freshnessModeRaw) {
    return freshnessModeRaw !== "fresh_current_slot";
  }
  const freshnessRaw = String(freshness ?? "").trim();
  const liveRaw = String(liveOrCache ?? "").trim();
  if (liveRaw === "fallback") {
    return true;
  }
  return !["fresh_full_rebuild", "current", "fresh"].includes(freshnessRaw);
}

function stateTone(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim();
  if (raw === "eligible" || raw === "on_target" || raw === "on_mandate" || raw === "in_band") {
    return "good";
  }
  if (raw === "review" || raw === "needs_review" || raw === "off_target" || raw === "wait_for_holdings" || raw === "awaiting_holdings") {
    return "warn";
  }
  if (raw === "blocked" || raw === "outside" || raw === "out_of_band") {
    return "bad";
  }
  if (raw === "watch" || raw === "monitor") {
    return "info";
  }
  return "neutral";
}

function bandTone(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim();
  if (raw === "in_band") return "good";
  if (raw === "out_of_band") return "warn";
  if (raw === "awaiting_holdings") return "warn";
  return "neutral";
}

function recommendationGateTone(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim();
  if (raw === "admissible") {
    return "good";
  }
  if (raw === "review_only") {
    return "warn";
  }
  if (raw === "blocked") {
    return "bad";
  }
  return "neutral";
}

function adaptFailureSummary(summary: {
  primary_class?: string | null;
  primary_label?: string | null;
  summary?: string | null;
  hard_classes?: string[] | null;
  review_classes?: string[] | null;
  confidence_drag_classes?: string[] | null;
} | null | undefined) {
  if (!summary) {
    return null;
  }
  return {
    primaryLabel: summary.primary_label ? presentBlueprintFailureClass(summary.primary_label) : presentBlueprintFailureClass(summary.primary_class),
    summary: cleanBlueprintCopy(summary.summary) ?? summary.summary ?? null,
    hardClasses: (summary.hard_classes ?? []).map((item) => presentBlueprintFailureClass(item)),
    reviewClasses: (summary.review_classes ?? []).map((item) => presentBlueprintFailureClass(item)),
    confidenceDragClasses: (summary.confidence_drag_classes ?? []).map((item) => presentBlueprintFailureClass(item)),
  };
}

function isSourceFailureSummary(
  summary: {
    primaryLabel?: string | null;
    hardClasses?: string[] | null;
    reviewClasses?: string[] | null;
    confidenceDragClasses?: string[] | null;
  } | null | undefined,
): boolean {
  const tokens = [
    summary?.primaryLabel,
    ...(summary?.hardClasses ?? []),
    ...(summary?.reviewClasses ?? []),
    ...(summary?.confidenceDragClasses ?? []),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return /weak-authority|stale truth|missing key facts|conflicting key facts|benchmark-lineage|cross-source recommendation conflict|source integrity/.test(tokens);
}

function confidenceTone(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim();
  if (raw === "high") {
    return "good";
  }
  if (raw === "mixed" || raw === "medium") {
    return "warn";
  }
  if (raw === "low") {
    return "bad";
  }
  return "neutral";
}

function impactTone(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim();
  if (raw === "high") {
    return "bad";
  }
  if (raw === "medium") {
    return "warn";
  }
  if (raw === "low") {
    return "info";
  }
  return "neutral";
}

function actionPostureTone(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim();
  if (raw.includes("act")) {
    return "bad";
  }
  if (raw.includes("review") || raw.includes("wait")) {
    return "warn";
  }
  if (raw.includes("monitor")) {
    return "info";
  }
  return "neutral";
}

function normalizeImpactStatusLabel(value: string | null | undefined): string {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return "Monitor";
  if (raw.includes("review") || raw.includes("triggered")) return "Review";
  if (raw.includes("background") || raw.includes("backdrop") || raw.includes("do not act")) return "Background";
  return "Monitor";
}

function impactStatusTone(value: string | null | undefined): Tone {
  const normalized = normalizeImpactStatusLabel(value);
  if (normalized === "Review") return "warn";
  if (normalized === "Monitor") return "info";
  return "neutral";
}

function compactImpactLine(value: string | null | undefined, fallback: string): string {
  const raw = String(value ?? "").trim();
  const base = raw || fallback;
  const withoutImplementation = base
    .replace(/\s+The main ETF choices are[\s\S]*$/i, "")
    .replace(/\s+Main ETF choices are[\s\S]*$/i, "")
    .trim();
  const firstSentence = withoutImplementation.match(/.*?[.!?](?:\s|$)/)?.[0]?.trim() ?? withoutImplementation;
  return firstSentence || fallback;
}

function impactNextStep(value: string | null | undefined, statusLabel: string): string {
  const raw = String(value ?? "").trim();
  if (raw) {
    const lowered = raw.toLowerCase();
    if (!["review", "review now", "monitor", "background", "do not act yet", "backdrop"].includes(lowered)) {
      return raw;
    }
  }
  if (statusLabel === "Review") {
    return "Review this now and decide whether the current stance should change.";
  }
  if (statusLabel === "Background") {
    return "Keep this in the background unless the signal reactivates.";
  }
  return "Keep this on monitor and wait for confirmation before changing the current stance.";
}

function humanizeChangeType(value: string | null | undefined): string {
  const raw = String(value ?? "").trim();
  const map: Record<string, string> = {
    rebuild: "Contract rebuild",
    blocker: "Blocker change",
    evidence: "Evidence change",
    recommendation_state: "Recommendation state",
    freshness_risk: "Freshness risk",
    portfolio_fit: "Portfolio fit",
    forecast_support_strengthened: "Forecast support strengthened",
    forecast_support_weakened: "Forecast support weakened",
    forecast_trigger_threshold_crossed: "Forecast trigger crossed",
    forecast_anomaly_opened: "Forecast anomaly opened",
    forecast_anomaly_resolved: "Forecast anomaly resolved",
    notebook_forecast_ref_added: "Notebook forecast linked",
  };
  return map[raw] ?? titleFromCode(raw);
}

function scoreFromFreshness(value: string | null | undefined): number {
  const raw = String(value ?? "").trim();
  if (raw === "fresh_full_rebuild") {
    return 92;
  }
  if (raw === "fresh_partial_rebuild") {
    return 76;
  }
  if (raw === "stored_valid_context") {
    return 62;
  }
  if (raw === "degraded_monitoring_mode") {
    return 44;
  }
  if (raw === "execution_failed_or_incomplete") {
    return 28;
  }
  return 55;
}

function signalPosture(signal: SignalCardV2, reviewPosture: string): { label: string; tone: Tone; lane: "reviewNow" | "monitor" | "doNotActYet" } {
  const decisionStatus = String(signal.decision_status ?? "").trim().toLowerCase();
  if (decisionStatus === "review_now") {
    return { label: "Review Now", tone: "bad", lane: "reviewNow" };
  }
  if (decisionStatus === "triggered") {
    return { label: "Review Now", tone: "bad", lane: "reviewNow" };
  }
  if (decisionStatus === "near_trigger" || decisionStatus === "watch_trigger") {
    return { label: "Near Trigger", tone: "warn", lane: "monitor" };
  }
  if (decisionStatus === "backdrop") {
    return { label: "Backdrop", tone: "neutral", lane: "doNotActYet" };
  }
  if (decisionStatus === "do_not_act_yet") {
    return { label: "Do Not Act Yet", tone: "neutral", lane: "doNotActYet" };
  }
  if (decisionStatus === "monitor") {
    return { label: "Monitor", tone: "info", lane: "monitor" };
  }
  const declared = String(signal.next_action ?? "").trim().toLowerCase();
  if (declared.includes("review")) {
    return { label: "Review Now", tone: "bad", lane: "reviewNow" };
  }
  if (declared.includes("monitor")) {
    return { label: "Monitor", tone: "info", lane: "monitor" };
  }
  if (declared.includes("do not act")) {
    return { label: "Do Not Act Yet", tone: "neutral", lane: "doNotActYet" };
  }
  const posture = reviewPosture.toLowerCase();
  if (signal.mapping_directness === "direct" && posture.includes("review")) {
    return { label: "Review Now", tone: "bad", lane: "reviewNow" };
  }
  if (signal.mapping_directness === "macro-only") {
    return { label: "Monitor", tone: "info", lane: "monitor" };
  }
  if (signal.affected_holdings.length > 0 || signal.affected_sleeves.length > 0) {
    return { label: "Monitor", tone: "info", lane: "monitor" };
  }
  return { label: "Do not act yet", tone: "neutral", lane: "doNotActYet" };
}

function compactParts(parts: Array<string | null | undefined>): string {
  return parts
    .map((part) => String(part ?? "").trim())
    .filter(Boolean)
    .join(" · ");
}

function humanizeRuntimeToken(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return null;
  }
  return raw
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .trim();
}

function runtimeSupportLabel(provenance: RuntimeSourceProvenance | null | undefined): string | null {
  if (!provenance) {
    return null;
  }
  if (provenance.usable_truth === false) {
    return "Insufficient truth";
  }
  if (provenance.derived_or_proxy || provenance.sufficiency_state === "proxy_bounded") {
    return String(provenance.data_mode || "").toLowerCase() === "derived" ? "Bounded proxy" : "Proxy support";
  }
  if (String(provenance.live_or_cache || "").toLowerCase() === "cache") {
    return "Cache continuity";
  }
  if (String(provenance.live_or_cache || "").toLowerCase() === "live") {
    return "Direct live";
  }
  if (String(provenance.live_or_cache || "").toLowerCase() === "fallback") {
    return "Fallback support";
  }
  return humanizeRuntimeToken(provenance.provenance_strength) || "Bounded support";
}

function runtimeDetail(provenance: RuntimeSourceProvenance | null | undefined): string | null {
  if (!provenance) {
    return null;
  }
  return compactParts([
    runtimeSupportLabel(provenance),
    humanizeRuntimeToken(provenance.provider_used),
    humanizeRuntimeToken(provenance.source_family),
    humanizeRuntimeToken(provenance.sufficiency_state),
  ]);
}

function issueStatusLabel(status: string | null | undefined): string {
  return humanizeState(status || "review");
}

function issueFixabilityLabel(issue: ReconciliationFieldStatus): string {
  return text(issue.fixability_label, humanizeState(issue.fixability_kind || "review"));
}

function mapFieldIssues(
  reconciliationReport: ReconciliationFieldStatus[] | null | undefined,
): Array<{ label: string; status: string; fixability: string; summary: string }> {
  return (reconciliationReport ?? [])
    .filter((item) => item.status && item.status !== "verified")
    .slice(0, 6)
    .map((item) => ({
      label: text(item.label, humanizeState(item.field_name)),
      status: issueStatusLabel(item.status),
      fixability: issueFixabilityLabel(item),
      summary: text(item.summary, "Field review is still required."),
    }));
}

function implementationFieldValue(
  value: string | null | undefined,
  missingFields: string[] | null | undefined,
  fieldName: string,
): { value: string; caution?: string | null } {
  const normalized = String(value ?? "").trim();
  if (normalized) {
    return { value: normalized };
  }
  if ((missingFields ?? []).includes(fieldName)) {
    return { value: "Not yet resolved", caution: "Still missing from current implementation truth." };
  }
  return { value: "Unavailable" };
}

function humanizeCriticalMissing(
  fields: string[] | null | undefined,
  reconciliationReport: ReconciliationFieldStatus[] | null | undefined,
): string[] {
  const labelsByField = new Map(
    (reconciliationReport ?? []).map((item) => [
      item.field_name,
      compactParts([text(item.label, humanizeState(item.field_name)), issueFixabilityLabel(item)]),
    ]),
  );
  return (fields ?? []).map((field) => labelsByField.get(field) || humanizeState(field));
}

function forecastMeta(support: ForecastSupport | null | undefined): string | null {
  if (!support) {
    return null;
  }
  return compactParts([
    "Model-backed forecast support",
    support.horizon ? `${support.horizon}d horizon` : null,
    support.degraded_state ? `Degraded: ${humanizeState(support.degraded_state)}` : support.confidence_summary,
  ]);
}

function triggerMeta(trigger: ForecastTriggerSupport | null | undefined): string | null {
  if (!trigger) {
    return null;
  }
  return compactParts([
    `${titleFromCode(trigger.trigger_type)} threshold ${trigger.threshold}`,
    `State ${humanizeState(trigger.threshold_state)}`,
    trigger.provider,
    trigger.degraded_state ? `Degraded: ${humanizeState(trigger.degraded_state)}` : trigger.confidence_summary,
  ]);
}

function appendDetail(base: string | null | undefined, detail: string | null | undefined): string | null {
  const left = String(base ?? "").trim();
  const right = String(detail ?? "").trim();
  if (!left) {
    return right || null;
  }
  if (!right) {
    return left;
  }
  return `${left} · ${right}`;
}

function truthEnvelopeMeta(
  envelope:
    | {
        reference_period?: string | null;
        release_date?: string | null;
        availability_date?: string | null;
        vintage_class?: string | null;
        revision_state?: string | null;
        release_semantics_state?: string | null;
        period_clock_class?: string | null;
        acquisition_mode?: string | null;
        degradation_reason?: string | null;
        market_session_context?: {
          session_label?: string | null;
          calendar_precision?: string | null;
          is_early_close?: boolean | null;
          extended_hours_state?: string | null;
        } | null;
      }
    | null
    | undefined
): string | null {
  if (!envelope) {
    return null;
  }
  return compactParts([
    envelope.reference_period ? `Period ${envelope.reference_period}` : null,
    envelope.market_session_context?.session_label ?? null,
    envelope.market_session_context?.calendar_precision && envelope.market_session_context.calendar_precision !== "full"
      ? `Calendar ${humanizeState(envelope.market_session_context.calendar_precision)}`
      : null,
    envelope.market_session_context?.is_early_close ? "Early close" : null,
    envelope.market_session_context?.extended_hours_state
      ? humanizeState(envelope.market_session_context.extended_hours_state)
      : null,
    envelope.release_date ? `Release ${envelope.release_date}` : null,
    envelope.availability_date ? `Available ${envelope.availability_date}` : null,
    envelope.vintage_class ? humanizeState(envelope.vintage_class) : null,
    envelope.revision_state ? humanizeState(envelope.revision_state) : null,
    envelope.release_semantics_state ? humanizeState(envelope.release_semantics_state) : null,
    envelope.period_clock_class ? humanizeState(envelope.period_clock_class) : null,
    envelope.acquisition_mode ? `Mode ${humanizeState(envelope.acquisition_mode)}` : null,
    envelope.degradation_reason ? `Degraded: ${humanizeState(envelope.degradation_reason)}` : null,
  ]);
}

function formatForecastRefs(refs: NotebookForecastReference[] | null | undefined): string | null {
  const rows = (refs ?? [])
    .slice(0, 3)
    .map((ref) =>
      compactParts([
        ref.reference_label,
        ref.threshold_summary ?? null,
        ref.created_at ? formatDateTime(ref.created_at) : null,
      ])
    )
    .filter(Boolean);
  if (!rows.length) {
    return null;
  }
  return rows.join(" | ");
}

function researchTone(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw === "good" || raw === "improving") {
    return "good";
  }
  if (raw === "bad") {
    return "bad";
  }
  if (raw === "warn" || raw === "worsening" || raw === "mixed") {
    return "warn";
  }
  if (raw === "info") {
    return "info";
  }
  return "neutral";
}

function adaptResearchSupport(
  pack: ResearchSupportPack | null | undefined,
): CandidateReportDisplay["researchSupport"] {
  if (!pack) {
    return null;
  }
  return {
    thesisDrift: pack.thesis_drift
      ? appendDetail(
          pack.thesis_drift.summary,
          pack.thesis_drift.prior_generated_at
            ? `Prior saved view ${formatDateTime(pack.thesis_drift.prior_generated_at)}`
            : null,
        )
      : null,
    marketContext: pack.market_context
      ? compactParts([
          pack.market_context.summary,
          pack.market_context.instrument_line ?? null,
          pack.market_context.benchmark_line ?? null,
          pack.market_context.freshness_note ?? null,
        ])
      : null,
    draftingSummary: pack.drafting_support?.summary ?? null,
    keyQuestions: pack.drafting_support?.key_questions ?? [],
    nextSteps: pack.drafting_support?.next_steps ?? [],
    retrievalGuides: (pack.retrieval_guides ?? []).map((guide) => ({
      label: guide.label,
      query: guide.query,
      reason: guide.reason,
      priority: humanizeState(guide.priority),
    })),
    newsClusters: (pack.news_clusters ?? []).map((cluster) => ({
      label: cluster.label,
      summary: cluster.summary,
      tone: researchTone(cluster.tone),
      headlines: cluster.headlines ?? [],
    })),
    logicSteps: (pack.logic_map?.steps ?? []).map((step) => ({
      label: step.label,
      detail: step.detail,
    })),
    sentimentSummary: pack.sentiment_annotation?.summary ?? null,
    sentimentTone: researchTone(pack.sentiment_annotation?.tone),
  };
}

function pickSupportPillars(message: string): SupportPillarDisplay[] {
  return [
    { label: "Macro", score: 0, note: message, tone: "warn" },
    { label: "Policy", score: 0, note: message, tone: "warn" },
    { label: "Valuation", score: 0, note: message, tone: "warn" },
    { label: "Implementation", score: 0, note: message, tone: "warn" },
    { label: "Evidence", score: 0, note: message, tone: "warn" },
  ];
}

function legacyScoreFromLabels(instrumentQuality: string, portfolioFit: string, state: string): number {
  const qualityScores: Record<string, number> = {
    High: 86,
    Moderate: 68,
    Low: 42,
  };
  const fitScores: Record<string, number> = {
    Highest: 88,
    Good: 72,
    "Weak today": 46,
  };
  const stateBonus: Record<string, number> = {
    eligible: 8,
    review: 2,
    watch: -4,
    research_only: -6,
    blocked: -14,
  };
  const quality = qualityScores[instrumentQuality] ?? 58;
  const fit = fitScores[portfolioFit] ?? 58;
  const bonus = stateBonus[state] ?? 0;
  return Math.max(18, Math.min(96, Math.round((quality + fit) / 2 + bonus)));
}

export function adaptPortfolio(
  contract: PortfolioContract,
  blueprint: BlueprintExplorerContract | null,
  brief: DailyBriefContract | null
): PortfolioDisplay {
  const forecastWatch = contract.forecast_watchlist ?? [];
  const chartPanels = adaptChartPanels(contract.allocation_chart_panels);
  const degradedMessage =
    contract.freshness_state === "no_data"
      ? "No holdings are loaded yet. Keep the full Portfolio surface visible and render typed empty states instead of collapsing sections."
      : contract.freshness_state === "execution_failed_or_incomplete"
        ? "Portfolio data is incomplete. Renderer keeps the full structure and marks incomplete sections explicitly."
        : null;

  const SUPPORT_KEYS = ["macro", "policy", "valuation", "implementation", "evidence"] as const;
  const totalValue =
    typeof contract.active_upload?.total_market_value === "number"
      ? contract.active_upload.total_market_value
      : contract.holdings.reduce((sum, holding) => sum + (typeof holding.market_value === "number" ? holding.market_value : 0), 0);
  const driftRows: PortfolioDisplay["allocationRows"] = contract.sleeve_drift_summary
    .map((row) => {
      const blueprintSleeve = blueprint?.sleeves?.find((s) => s.sleeve_id === row.sleeve_id);
      const pillars = blueprintSleeve?.support_pillars ?? [];
      const supportBars = SUPPORT_KEYS.map((k) => {
        const match = pillars.find((p) => p.label.toLowerCase() === k);
        return { key: k, score: match?.score ?? 0 };
      });
      const bandState = row.band_status ?? row.status;
      const currentPct = typeof row.current_pct === "number" ? percentFromMaybeFraction(row.current_pct) : null;
      const targetAnchorPct =
        typeof row.target_pct === "number"
          ? percentFromMaybeFraction(row.target_pct)
          : percentFromMaybeFraction(row.sort_midpoint_pct);
      return {
        sleeveId: row.sleeve_id,
        name: row.sleeve_name ?? titleFromCode(row.sleeve_id),
        rank: row.rank,
        targetLabel: row.target_label,
        rangeLabel: row.range_label,
        bandStatus: humanizeState(bandState),
        bandTone: bandTone(bandState),
        isNested: !!row.is_nested,
        parentSleeveId: row.parent_sleeve_id ?? null,
        parentSleeveName: row.parent_sleeve_name ?? null,
        countsAsTopLevelTotal: row.counts_as_top_level_total,
        current: formatPercent(row.current_pct),
        target: row.target_label,
        range: row.range_label,
        currentPct,
        targetPct: targetAnchorPct,
        minPct: percentFromMaybeFraction(row.min_pct),
        maxPct: percentFromMaybeFraction(row.max_pct),
        drift: formatPercent(row.drift_pct, true),
        driftTone:
          typeof row.drift_pct === "number"
            ? percentFromMaybeFraction(row.drift_pct) > 0
              ? "warn"
              : percentFromMaybeFraction(row.drift_pct) < 0
                ? "info"
                : "good"
            : ("neutral" as Tone),
        statusLabel: humanizeState(bandState),
        statusTone: bandTone(bandState),
        note: row.is_nested
          ? `Equity carveout inside ${row.parent_sleeve_name ?? "Global Equity Core"}.`
          : contract.blueprint_consequence ?? contract.daily_brief_consequence ?? "No cross-surface consequence has been emitted yet.",
        capitalEligible: blueprintSleeve?.visible_state === "eligible",
        fundingSource: blueprintSleeve?.funding_path?.funding_source ?? null,
        supportBars,
      };
    })
    .sort((a, b) => a.rank - b.rank);

  const aboveTargetCount = contract.sleeve_drift_summary.filter((row) => typeof row.drift_pct === "number" && percentFromMaybeFraction(row.drift_pct) > 0.05).length;
  const underTargetCount = contract.sleeve_drift_summary.filter((row) => typeof row.drift_pct === "number" && percentFromMaybeFraction(row.drift_pct) < -0.05).length;
  const mappingSummary = contract.mapping_summary;
  const dominantDriftRow = contract.sleeve_drift_summary.length
    ? [...contract.sleeve_drift_summary].sort(
        (a, b) => Math.abs(percentFromMaybeFraction(b.drift_pct)) - Math.abs(percentFromMaybeFraction(a.drift_pct)),
      )[0]
    : null;
  const topLevelCount = driftRows.filter((row) => row.countsAsTopLevelTotal).length;
  const nestedCount = driftRows.filter((row) => row.isNested).length;

  const summaryChips: SummaryChip[] = [
    {
      label: "Total value",
      value: totalValue > 0 ? formatCurrency(totalValue, contract.base_currency ?? "USD") : "Awaiting holdings",
      meta: contract.active_upload?.uploaded_at ? formatDateTime(contract.active_upload.uploaded_at) : "No active holdings upload",
      tone: totalValue > 0 ? "good" : "warn",
    },
    {
      label: "Sleeves",
      value: driftRows.length
        ? `${topLevelCount} top level`
        : "Awaiting holdings",
      meta: driftRows.length
        ? `${nestedCount} nested · ${underTargetCount} under · ${aboveTargetCount} above`
        : "No sleeve drift rows yet",
      tone: driftRows.length ? "info" : "warn",
    },
    {
      label: "Biggest drift",
      value: dominantDriftRow ? formatPercent(dominantDriftRow.drift_pct, true) : "—",
      meta: dominantDriftRow ? (dominantDriftRow.sleeve_name ?? titleFromCode(dominantDriftRow.sleeve_id)) : "No live drift row",
      tone: dominantDriftRow ? stateTone(dominantDriftRow.status) : "neutral",
    },
    {
      label: "Action posture",
      value: humanizeState(contract.action_posture),
      meta: humanizeState(contract.mandate_state),
      tone: actionPostureTone(contract.action_posture),
    },
    {
      label: "Data trust",
      value: freshnessLabel(contract.freshness_state),
      meta: compactParts([
        contract.active_upload?.source_name ? humanizeState(contract.active_upload.source_name) : null,
        mappingSummary?.quality_label ?? null,
      ]) ?? formatDateTime(contract.generated_at),
      tone: freshnessTone(contract.freshness_state),
    },
  ];
  if (forecastWatch.length) {
    summaryChips.push({
      label: "Forecast watch",
      value: `${forecastWatch.length} active`,
      meta: appendDetail(forecastWatch[0]?.summary, forecastMeta(forecastWatch[0]?.forecast_support)) ?? undefined,
      tone: forecastWatch.some((item) => item.forecast_support.degraded_state) ? "warn" : "info",
    });
  }

  const holdings: PortfolioDisplay["holdings"] = contract.holdings.map((holding) => {
    const relatedSleeve = blueprint?.sleeves.find((sleeve) =>
      sleeve.sleeve_id.toLowerCase().includes(holding.sleeve_id.replace(/^sleeve_/, "").split("_")[0] ?? "")
    );
    const relatedSignals = brief?.what_changed.filter(
      (signal) =>
        signal.affected_holdings.includes(holding.symbol) ||
        signal.affected_sleeves.includes(holding.sleeve_id)
    ) ?? [];
    return {
      symbol: holding.symbol,
      name: holding.name,
      sleeve: titleFromCode(holding.sleeve_id),
      weight: formatPercent(holding.weight_pct ?? null),
      weightPct: typeof holding.weight_pct === "number" ? holding.weight_pct : null,
      targetWeight: formatPercent(holding.target_pct ?? null),
      targetPct: typeof holding.target_pct === "number" ? holding.target_pct : null,
      weightDrift: formatPercent(holding.drift_pct ?? null, true),
      weightTone:
        typeof holding.drift_pct === "number"
          ? percentFromMaybeFraction(holding.drift_pct) > 0
            ? "warn"
            : percentFromMaybeFraction(holding.drift_pct) < 0
              ? "info"
              : "good"
          : ("neutral" as Tone),
      statusLabel: holding.review_status === "review" ? "Review" : "Hold",
      statusTone: holding.review_status === "review" ? "warn" : "good",
      blueprintLabel: relatedSleeve ? "Blueprint-linked" : "No blueprint link",
      blueprintTone: relatedSleeve ? "good" : "neutral",
      briefLabel: relatedSignals.length ? `${relatedSignals.length} linked brief item${relatedSignals.length > 1 ? "s" : ""}` : "No current signal",
      briefTone: relatedSignals.length ? "warn" : "neutral",
      actionLabel: holding.review_status === "review" ? "Review" : "Hold",
    };
  });

  const blueprintRows = (blueprint?.sleeves ?? []).slice(0, 6).map((sleeve) => ({
    sleeve: sleeve.sleeve_name ?? titleFromCode(sleeve.sleeve_id),
    candidate: sleeve.lead_candidate_name ?? "No preferred candidate yet",
    status: humanizeState(sleeve.visible_state),
    statusTone: stateTone(sleeve.visible_state),
    note: sleeve.implication_summary,
  }));

  const briefRows = (brief?.what_changed ?? []).map((signal) => {
    const posture = signalPosture(signal, brief?.review_posture ?? "monitor");
    return {
      title: signal.label,
      posture: posture.label,
      postureTone: posture.tone,
      affected: [...signal.affected_sleeves.map(titleFromCode), ...signal.affected_holdings],
      note: signal.summary ?? signal.implication ?? null,
      caveat: signal.do_not_overread,
    };
  });

  const upload = contract.active_upload;
  const accountSummary = contract.account_summary ?? [];
  const sourceState = contract.portfolio_source_state?.state ?? contract.surface_state?.state ?? (contract.holdings.length ? "ready" : "empty");
  const sourceReason = contract.portfolio_source_state?.summary ?? contract.surface_state?.summary ?? "Portfolio source state unavailable.";
  const unresolvedCount = contract.unresolved_mapping_rows?.length ?? mappingSummary?.unresolved_count ?? 0;
  const uploadSourceLabel = upload
    ? `${text(upload.source_name, "Upload")} · ${text(upload.filename, upload.run_id)}`
    : "No uploaded holdings yet";
  const uploadFreshnessLabel = upload
    ? (appendDetail(
        formatDateTime(upload.uploaded_at),
        upload.holdings_as_of_date ? `As of ${formatDate(upload.holdings_as_of_date)}` : null
      ) ?? "")
    : freshnessLabel(contract.freshness_state);
  const mappingQualityLabel = mappingSummary?.quality_label
    ?? (contract.holdings.length ? "Mapping inferred from loaded holdings" : "Mapping not available");
  const unresolvedMappingLabel = unresolvedCount
    ? `${unresolvedCount} unresolved`
    : upload
      ? "0 unresolved"
      : "Unknown until upload";
  const uploadMessage = compactParts([
    sourceReason,
    upload ? `${upload.normalized_position_count} positions in ${contract.base_currency ?? "base"} base context.` : null,
    accountSummary.length ? `${accountSummary.length} account${accountSummary.length > 1 ? "s" : ""} in active upload.` : null,
    mappingSummary?.override_count ? `${mappingSummary.override_count} mapping override${mappingSummary.override_count > 1 ? "s" : ""} active.` : null,
  ]);

  return {
    meta: surfaceMeta("portfolio", [
      // Freshness — always show so the user knows how fresh the data is
      { label: freshnessLabel(contract.freshness_state), tone: freshnessTone(contract.freshness_state) },
      // Mandate state — only show when the mandate is in a known/active state
      ...(contract.mandate_state && contract.mandate_state !== "no_data"
        ? [{ label: humanizeState(contract.mandate_state), tone: stateTone(contract.mandate_state) }]
        : []),
      // Sleeve drift — only show when drift data is actually present
      ...(contract.sleeve_drift_summary.length > 0
        ? [{ label: `${contract.sleeve_drift_summary.length} sleeve${contract.sleeve_drift_summary.length > 1 ? "s" : ""} tracked`, tone: "neutral" as Tone }]
        : []),
      // Blueprint linkage — count matches the displayed sleeves in the Blueprint Developments section
      ...(blueprint !== null
        ? [{ label: `${blueprintRows.length} blueprint development${blueprintRows.length !== 1 ? "s" : ""}`, tone: "good" as Tone }]
        : []),
      // Brief connection — only show when today's brief has signals
      ...((brief?.what_changed?.length ?? 0) > 0
        ? [{ label: `${brief!.what_changed.length} brief signal${brief!.what_changed.length > 1 ? "s" : ""}`, tone: "info" as Tone }]
        : []),
      // Work queue — only show when there are open items
      ...(contract.work_items.length > 0
        ? [{ label: `${contract.work_items.length} open item${contract.work_items.length > 1 ? "s" : ""}`, tone: "warn" as Tone }]
        : []),
    ]),
    degradedMessage,
    summaryChips,
    chartPanels,
    hero: {
      summary: contract.what_matters_now,
      postureLabel: humanizeState(contract.action_posture),
      postureTone: actionPostureTone(contract.action_posture),
      mandateLabel: humanizeState(contract.mandate_state),
    },
    allocationRows: driftRows,
    holdings,
    healthTiles: [
      {
        label: "Holdings readiness",
        value: contract.holdings.length ? "Live holdings loaded" : "Awaiting holdings upload",
        note: contract.holdings.length
          ? "Portfolio sections are rendering from real holdings."
          : "Keep the shell visible and prompt for holdings instead of deleting decision sections.",
        tone: contract.holdings.length ? "good" : "warn",
      },
      {
        label: "Mandate visibility",
        value: humanizeState(contract.mandate_state),
        note: "Current V2 contract does not emit a full health rubric yet.",
        tone: stateTone(contract.mandate_state),
      },
      {
        label: "Blueprint linkage",
        value: blueprintRows.length ? "Blueprint-linked" : "Waiting for blueprint context",
        note: blueprintRows.length
          ? "Portfolio relevance is being pulled from the Blueprint Explorer."
          : "Cross-surface relevance remains visible as an explicit degraded shell.",
        tone: blueprintRows.length ? "good" : "warn",
      },
      {
        label: "Daily brief linkage",
        value: briefRows.length ? "Brief-linked" : "No current portfolio-linked signal",
        note: briefRows.length
          ? "Daily Brief connection is derived from affected sleeves and holdings."
          : "No signal currently maps directly into the book.",
        tone: briefRows.length ? "info" : "neutral",
      },
      {
        label: "Upload and sync",
        value: humanizeState(sourceState),
        note: uploadMessage,
        tone:
          sourceState === "ready"
            ? "good"
            : sourceState === "degraded" || sourceState === "empty"
              ? "warn"
              : "bad",
      },
      {
        label: "Review queue",
        value: contract.work_items.length ? `${contract.work_items.length} active` : "No current work items",
        note: "Queue stays visible even when no work items are present.",
        tone: contract.work_items.length ? "warn" : "neutral",
      },
      ...(forecastWatch.length
        ? [
            {
              label: "Cross-sleeve pressure watch",
              value: forecastWatch[0]?.label ?? "Forecast watch active",
              note: appendDetail(forecastWatch[0]?.summary, forecastMeta(forecastWatch[0]?.forecast_support)) ?? "",
              tone: forecastWatch[0]?.forecast_support?.degraded_state ? ("warn" as Tone) : ("info" as Tone),
            },
          ]
        : []),
    ],
    blueprintRows,
    briefRows,
    uploadStatus: {
      source: uploadSourceLabel,
      freshness: uploadFreshnessLabel,
      mappingQuality: mappingQualityLabel,
      unresolvedMappings: unresolvedMappingLabel,
      message: uploadMessage,
    },
    inspector: [
      {
        label: "Portfolio freshness",
        value: freshnessLabel(contract.freshness_state),
        tone: freshnessTone(contract.freshness_state),
      },
      ...(contract.blueprint_consequence ? [{ label: "Blueprint consequence", value: contract.blueprint_consequence }] : []),
      ...(contract.daily_brief_consequence ? [{ label: "Daily Brief consequence", value: contract.daily_brief_consequence }] : []),
      {
        label: "Base currency",
        value: contract.base_currency ?? "Unknown",
      },
      ...(accountSummary.length
        ? [
            {
              label: "Account scope",
              value: `${accountSummary.length} account${accountSummary.length > 1 ? "s" : ""}`,
            },
          ]
        : []),
      ...(forecastWatch.length
        ? [
            {
              label: "Forecast watch",
              value: appendDetail(forecastWatch[0]?.label, forecastMeta(forecastWatch[0]?.forecast_support)) ?? "",
              tone: forecastWatch[0]?.forecast_support?.degraded_state ? ("warn" as Tone) : ("info" as Tone),
            },
          ]
        : []),
    ],
  };
}

export function adaptDailyBrief(contract: DailyBriefContract): DailyBriefDisplay {
  const signalSource = contract.what_changed?.length ? contract.what_changed : (contract.signal_stack ?? []);
  const signalChartMap = new Map((contract.signal_chart_panels ?? []).map((item) => [item.signal_id, adaptChartPanel(item.panel)]));
  const scenarioChartMap = new Map((contract.scenario_chart_panels ?? []).map((item) => [item.signal_id, adaptChartPanel(item.panel)]));
  const leadSignal = signalSource[0] ?? null;
  const adaptBriefSignal = (signal: SignalCardV2): DailyBriefSignalDisplay => {
    const posture = signalPosture(signal, contract.review_posture);
    const affected = [...signal.affected_sleeves.map(titleFromCode), ...signal.affected_holdings];
    const envelopeMeta = truthEnvelopeMeta(signal.truth_envelope);
    const runtimeMeta = runtimeDetail(signal.runtime_provenance);
    const sleeveTags = (signal.sleeve_tags ?? signal.affected_sleeves).map(titleFromCode);
    const instrumentTags = signal.instrument_tags ?? signal.affected_candidates ?? [];
    return {
      id: signal.signal_id,
      cardFamily: signal.card_family ?? null,
      prominenceClass: signal.prominence_class ?? signal.visibility_role ?? null,
      signalLabel: signal.signal_label ?? signal.label,
      evidenceTitle: signal.evidence_title ?? signal.short_title ?? signal.decision_title ?? signal.label,
      interpretationSubtitle: signal.interpretation_subtitle ?? signal.short_subtitle ?? signal.summary,
      sleeveTags,
      instrumentTags,
      evidenceClassCode: signal.evidence_class ?? null,
      freshnessState: signal.freshness_state ?? null,
      freshnessLabel: signal.freshness_label ?? null,
      decisionStatus: signal.decision_status ?? null,
      actionPosture: signal.action_posture ?? null,
      supportLabel: signal.support_label ?? null,
      confidenceLabel: signal.confidence_label ?? signal.confidence_class ?? null,
      marketConfirmationState: signal.market_confirmation_state ?? null,
      title: signal.decision_title ?? signal.label,
      shortTitle: signal.evidence_title ?? signal.short_title ?? signal.decision_title ?? signal.label,
      shortSubtitle: signal.interpretation_subtitle ?? signal.short_subtitle ?? signal.summary,
      posture: posture.label,
      postureTone: posture.tone,
      category: humanizeState(signal.effect_type ?? signal.signal_kind),
      summary: signal.summary,
      implication: signal.implication,
      sourceKind: signal.source_kind ?? null,
      effectType: signal.effect_type ?? null,
      bucket: signal.primary_effect_bucket ?? null,
      whyEconomicMacro: signal.why_it_matters_macro ?? null,
      whyEconomicMicro: signal.why_it_matters_micro ?? null,
      whyHereShortTerm: signal.why_it_matters_short_term ?? null,
      whyHereLongTerm: signal.why_it_matters_long_term ?? null,
      whatChangedToday: signal.what_changed_today ?? null,
      whatChanged: signal.what_changed ?? null,
      eventContextDelta: signal.event_context_delta ?? null,
      whyItMatters: signal.why_it_matters ?? null,
      whyItMattersEconomically: signal.why_it_matters_economically ?? signal.why_it_matters ?? null,
      portfolioMeaning: signal.portfolio_meaning ?? null,
      portfolioAndSleeveMeaning: signal.portfolio_and_sleeve_meaning ?? signal.portfolio_meaning ?? null,
      confirmCondition: signal.confirm_condition ?? null,
      weakenCondition: signal.weaken_condition ?? null,
      breakCondition: signal.break_condition ?? null,
      scenarioSupport: signal.scenario_support ?? null,
      evidenceClass: signal.evidence_class ?? null,
      whyThisCouldBeWrong: signal.why_this_could_be_wrong ?? null,
      whyNowNotBefore: signal.why_now_not_before ?? null,
      implementationSensitivity: signal.implementation_sensitivity ?? null,
      implementationSet: signal.implementation_set ?? signal.affected_candidates ?? [],
      sourceAndValidity: signal.source_and_validity ?? null,
      marketConfirmation: signal.market_confirmation ?? null,
      newsToMarketConfirmation: signal.news_to_market_confirmation ?? null,
      doNotOverread: signal.do_not_overread,
      confirms: signal.confirms,
      breaks: signal.breaks,
      nearTermTrigger: signal.near_term_trigger ?? null,
      thesisTrigger: signal.thesis_trigger ?? null,
      portfolioConsequence: signal.portfolio_consequence ?? null,
      nextAction: signal.next_action ?? posture.label,
      pathRiskNote: signal.path_risk_note ?? null,
      affected,
      affectedCandidates: signal.affected_candidates ?? [],
      mappingDirectness: humanizeState(signal.mapping_directness),
      trust: appendDetail(
        signal.source_provenance_summary ?? runtimeSupportLabel(signal.runtime_provenance) ?? text(signal.trust_status, "Trust status unavailable"),
        compactParts([runtimeMeta, envelopeMeta, signal.signal_support_class ? humanizeState(signal.signal_support_class) : null])
      ) ?? "",
      asOf: formatDateTime(signal.as_of),
      relevanceScore: signal.decision_relevance_score ?? null,
      confidenceClass: signal.confidence_class ?? null,
      sufficiencyState: signal.sufficiency_state ?? null,
      supportClass: signal.signal_support_class ?? null,
      sourceProvenanceSummary: signal.source_provenance_summary ?? null,
      visibilityRole: signal.visibility_role ?? null,
      coverageReason: signal.coverage_reason ?? null,
      aspectBucket: signal.aspect_bucket ?? null,
      eventClusterId: signal.event_cluster_id ?? null,
      eventTitle: signal.event_title ?? null,
      eventSubtype: signal.event_subtype ?? null,
      eventRegion: signal.event_region ?? null,
      eventEntities: signal.event_entities ?? [],
      marketChannels: signal.market_channels ?? [],
      confirmationAssets: signal.confirmation_assets ?? [],
      eventTriggerSummary: signal.event_trigger_summary ?? null,
      scenarios: (signal.scenarios ?? []).map((scenario) => ({
        label: scenario.label,
        type: scenario.type,
        scenarioName: scenario.scenario_name ?? null,
        pathStatement: scenario.path_statement ?? null,
        timingWindow: scenario.timing_window ?? null,
        scenarioLikelihoodPct: scenario.scenario_likelihood_pct ?? null,
        sleeveConsequence: scenario.sleeve_consequence ?? null,
        actionBoundary: scenario.action_boundary ?? null,
        upgradeTrigger: scenario.upgrade_trigger ?? null,
        downgradeTrigger: scenario.downgrade_trigger ?? null,
        supportStrength: scenario.support_strength ?? null,
        regimeNote: scenario.regime_note ?? null,
        confirmationNote: scenario.confirmation_note ?? null,
        leadSentence: scenario.lead_sentence ?? null,
        effect: scenario.portfolio_effect,
        actionConsequence: scenario.action_consequence ?? null,
        pathMeaning: scenario.path_meaning ?? null,
        triggerState: scenario.trigger_state ?? null,
        pathBias: scenario.path_bias ?? null,
        confirmProbability: scenario.confirm_probability ?? null,
        breakProbability: scenario.break_probability ?? null,
        thresholdBreachRisk: scenario.threshold_breach_risk ?? null,
        uncertaintyWidth: scenario.uncertainty_width ?? null,
        persistenceVsReversion: scenario.persistence_vs_reversion ?? null,
        evidenceState: scenario.evidence_state ?? null,
        macro: scenario.macro,
        micro: scenario.micro,
        shortTerm: scenario.short_term,
        longTerm: scenario.long_term,
      })),
      chartPayload: signal.chart_payload ?? null,
      chart: signalChartMap.get(signal.signal_id) ?? null,
    };
  };
  const signals = signalSource.map(adaptBriefSignal);
  const signalGroups = (contract.signal_stack_groups ?? [])
    .map((group) => ({
      id: group.group_id,
      label: group.label,
      summary: group.summary,
      representative: group.representative ? adaptBriefSignal(group.representative) : null,
      count: group.count ?? (group.signals?.length ?? 0),
      signals: (group.signals ?? []).map(adaptBriefSignal),
    }))
    .filter((group) => group.signals.length > 0);
  const regimeContextSignals = (contract.regime_context_drivers ?? []).map(adaptBriefSignal);

  const reviewLanes: DailyBriefDisplay["reviewLanes"] = {
    reviewNow: [],
    monitor: [],
    doNotActYet: [],
  };
  if (contract.review_triggers?.length) {
    for (const trigger of contract.review_triggers) {
      if (trigger.lane === "review_now") {
        reviewLanes.reviewNow.push({ label: trigger.label, reason: trigger.reason });
      } else if (trigger.lane === "monitor") {
        reviewLanes.monitor.push({ label: trigger.label, reason: trigger.reason });
      } else {
        reviewLanes.doNotActYet.push({ label: trigger.label, reason: trigger.reason });
      }
    }
  } else {
    for (const signal of signalSource) {
      const posture = signalPosture(signal, contract.review_posture);
      reviewLanes[posture.lane].push({
        label: signal.label,
        reason: signal.summary,
      });
    }
  }

  const directnessSet = new Set(signalSource.map((signal) => humanizeState(signal.mapping_directness)));
  const impactRows = contract.portfolio_impact_rows?.length
    ? contract.portfolio_impact_rows.map((row) => ({
        objectLabel: titleFromCode(row.object_label),
        objectType: row.object_type,
        mapping: humanizeState(row.mapping),
        statusLabel: normalizeImpactStatusLabel(row.status_label),
        statusTone: impactStatusTone(row.status_label),
        consequence: compactImpactLine(row.consequence, "This still changes the portfolio read."),
        nextStep: impactNextStep(row.next_step, normalizeImpactStatusLabel(row.status_label)),
      }))
    : signalSource.flatMap((signal) => {
        const posture = signalPosture(signal, contract.review_posture);
        const statusLabel = posture.lane === "reviewNow" ? "Review" : posture.lane === "doNotActYet" ? "Background" : "Monitor";
        const sleeveRows = signal.affected_sleeves.map((sleeve) => ({
          objectLabel: titleFromCode(sleeve),
          objectType: "Sleeve",
          mapping: humanizeState(signal.mapping_directness),
          statusLabel,
          statusTone: impactStatusTone(statusLabel),
          consequence: compactImpactLine(signal.interpretation_subtitle ?? signal.summary, "This still changes the portfolio read."),
          nextStep: impactNextStep(null, statusLabel),
        }));
        const holdingRows = signal.affected_holdings.map((holding) => ({
          objectLabel: holding,
          objectType: "Holding",
          mapping: "Direct",
          statusLabel,
          statusTone: impactStatusTone(statusLabel),
          consequence: compactImpactLine(signal.interpretation_subtitle ?? signal.summary, "This still changes the portfolio read."),
          nextStep: impactNextStep(null, statusLabel),
        }));
        return [...sleeveRows, ...holdingRows];
      });

  const scenarios = (contract.scenario_blocks ?? []).map((block) => ({
    label: block.label,
    summary: appendDetail(
      compactParts([
        block.summary,
        block.what_confirms ? `Confirms: ${block.what_confirms}` : null,
        block.what_breaks ? `Breaks: ${block.what_breaks}` : null,
        block.threshold_summary ? `Thresholds: ${block.threshold_summary}` : null,
      ]),
      forecastMeta(block.forecast_support)
    ) ?? "",
    variants: block.scenarios.map((scenario) => ({
      label: scenario.label,
      type: scenario.type,
      scenarioName: scenario.scenario_name ?? null,
      pathStatement: scenario.path_statement ?? null,
      timingWindow: scenario.timing_window ?? null,
      scenarioLikelihoodPct: scenario.scenario_likelihood_pct ?? null,
      sleeveConsequence: scenario.sleeve_consequence ?? null,
      actionBoundary: scenario.action_boundary ?? null,
      upgradeTrigger: scenario.upgrade_trigger ?? null,
      downgradeTrigger: scenario.downgrade_trigger ?? null,
      supportStrength: scenario.support_strength ?? null,
      regimeNote: scenario.regime_note ?? null,
      confirmationNote: scenario.confirmation_note ?? null,
      leadSentence: scenario.lead_sentence ?? null,
      effect: scenario.portfolio_effect,
      actionConsequence: scenario.action_consequence ?? null,
      pathMeaning: scenario.path_meaning ?? null,
      triggerState: scenario.trigger_state ?? null,
      pathBias: scenario.path_bias ?? null,
      confirmProbability: scenario.confirm_probability ?? null,
      breakProbability: scenario.break_probability ?? null,
      thresholdBreachRisk: scenario.threshold_breach_risk ?? null,
      uncertaintyWidth: scenario.uncertainty_width ?? null,
      persistenceVsReversion: scenario.persistence_vs_reversion ?? null,
      evidenceState: scenario.evidence_state ?? null,
      macro: scenario.macro,
      micro: scenario.micro,
      shortTerm: scenario.short_term,
      longTerm: scenario.long_term,
    })),
    chart: scenarioChartMap.get(block.signal_id) ?? null,
  }));
  const leadScenarioSupport = contract.scenario_blocks?.find((block) => block.forecast_support)?.forecast_support ?? null;
  const contingentDrivers = contract.contingent_drivers?.length
    ? contract.contingent_drivers.map((item) => ({
        label: item.label,
        triggerTitle: item.trigger_title ?? item.label,
        effectType: item.effect_type ?? null,
        whyNow: item.why_it_matters_now,
        whatChangesIfConfirmed:
          item.what_changes_if_confirmed ?? item.portfolio_consequence ?? item.trigger_condition ?? item.near_term_trigger,
        whatToWatchNext: item.what_to_watch_next ?? null,
        currentStatus: item.current_status ?? null,
        affectedSleeves: (item.affected_sleeves ?? (item.affected_sleeve ? [item.affected_sleeve] : [])).map(titleFromCode),
        supportingLines: item.supporting_lines ?? [],
      }))
    : [];

  const refreshTone = freshnessTone(contract.freshness_state);
  const isRefreshed = refreshTone === "good";
  const refreshInProgress = (contract.surface_state?.reason_codes ?? []).includes("refresh_in_progress");
  const monitorCount = contingentDrivers.length || reviewLanes.monitor.length;
  const reviewNowCount = reviewLanes.reviewNow.length;

  return {
    meta: surfaceMeta("brief", [
      {
        label: isRefreshed ? "Refreshed" : "Not Refreshed",
        tone: isRefreshed ? "good" : "bad",
        dot: true,
      },
      {
        label: `${monitorCount} Monitor${monitorCount !== 1 ? "s" : ""}`,
        tone: "info",
      },
      {
        label: `${reviewNowCount} Review / Act Now`,
        tone: reviewNowCount > 0 ? "bad" : "neutral",
      },
    ]),
    degradedMessage:
      refreshInProgress
        ? contract.surface_state?.summary ?? "Daily Brief refresh is running. Cached content will remain visible when available."
        : contract.freshness_state === "degraded_monitoring_mode"
        ? "Daily Brief is running in degraded monitoring mode. Keep all reference sections visible and mark missing market, scenario, and portfolio context as typed degraded content."
        : null,
    statusBar: [
      {
        label: "Posture",
        value: humanizeState(contract.review_posture),
        meta: leadSignal?.summary ?? "Current brief posture",
        tone: actionPostureTone(contract.review_posture),
      },
      {
        label: "Signals",
        value: String(signalSource.length),
        meta: leadSignal?.label ? `Lead signal: ${leadSignal.label}` : "No prioritized signals",
        tone: signalSource.length ? "good" : "warn",
      },
      {
        label: "Freshness",
        value: freshnessLabel(contract.evidence_and_trust.freshness_state),
        meta: "Evidence freshness state",
        tone: freshnessTone(contract.evidence_and_trust.freshness_state),
      },
      {
        label: "Trust",
        value: [...directnessSet].join(" · "),
        meta: `${contract.evidence_and_trust.source_count} source famil${contract.evidence_and_trust.source_count === 1 ? "y" : "ies"}`,
        tone: contract.portfolio_overlay ? "good" : "info",
      },
      {
        label: "Generated",
        value: formatDateTime(contract.generated_at),
        meta: contract.portfolio_overlay ? "Portfolio overlay attached" : "No portfolio overlay",
      },
      ...(contract.data_confidence
        ? [
            {
              label: "Data confidence",
              value: humanizeState(contract.data_confidence),
              meta: contract.data_timeframes?.[0]?.summary ?? "Reference clocks carried into the brief contract.",
              tone:
                contract.data_confidence === "high"
                  ? ("good" as Tone)
                  : contract.data_confidence === "mixed"
                    ? ("warn" as Tone)
                    : ("bad" as Tone),
            },
          ]
        : []),
    ],
    briefHeader: {
      economicRead: contract.why_it_matters_economically,
      portfolioRead: contract.why_it_matters_here,
      changeCondition: contract.what_confirms_or_breaks,
    },
    macroCharts: adaptChartPanels(contract.macro_chart_panels),
    crossAssetCharts: adaptChartPanels(contract.cross_asset_chart_panels),
    fxCharts: adaptChartPanels(contract.fx_chart_panels),
    marketState: contract.market_state_cards?.length
      ? contract.market_state_cards
        .filter((card) => !card.validation_status || card.validation_status === "valid")
        .map((card) => {
          const freshness = card.runtime_provenance?.freshness ?? null;
          const liveOrCache = card.runtime_provenance?.live_or_cache ?? null;
          const validationStatus = card.validation_status ?? null;
          const freshnessMode = card.freshness_mode ?? null;
          return {
            label: card.label,
            value: card.value,
            note: card.note ?? "",
            tone: card.tone,
            currentValue: card.current_value ?? null,
            changePct1d: card.change_pct_1d ?? null,
            caption: card.caption ?? null,
            subCaption: card.sub_caption ?? null,
            freshness,
            freshnessLabel: marketStateFreshnessLabel(freshnessMode, freshness, liveOrCache, validationStatus),
            freshnessTone: marketStateFreshnessTone(freshnessMode, freshness, liveOrCache, validationStatus),
            liveOrCache,
            isNonFresh: marketStateIsNonFresh(freshnessMode, freshness, liveOrCache, validationStatus),
            asOf: card.as_of ?? null,
            sourceProvider: card.source_provider ?? card.runtime_provenance?.provider_used ?? null,
            sourceType: card.source_type ?? null,
            sourceAuthorityTier: card.source_authority_tier ?? card.runtime_provenance?.source_authority_tier ?? card.runtime_provenance?.provenance_strength ?? null,
            metricDefinition: card.metric_definition ?? null,
            metricPolarity: card.metric_polarity ?? null,
            isExact: card.is_exact !== false,
            validationStatus,
            validationReason: card.validation_reason ?? null,
            freshnessMode,
            primaryProvider: card.primary_provider ?? card.source_provider ?? card.runtime_provenance?.provider_used ?? null,
            crossCheckProvider: card.cross_check_provider ?? null,
            crossCheckStatus: card.cross_check_status ?? null,
            authorityGapReason: card.authority_gap_reason ?? null,
          };
        })
      : signalSource.slice(0, 5).map((signal) => ({
          label: signal.label,
          value: `${humanizeState(signal.direction)} · ${humanizeState(signal.magnitude)}`,
          note: signal.summary,
          tone: signal.mapping_directness === "macro-only" ? "info" : "neutral",
          currentValue: null,
          changePct1d: null,
          caption: null,
          subCaption: null,
          freshness: null,
          freshnessLabel: "Unknown",
          freshnessTone: "neutral" as Tone,
          liveOrCache: null,
          isNonFresh: true,
          asOf: null,
          sourceProvider: null,
          sourceType: null,
          sourceAuthorityTier: null,
          metricDefinition: null,
          metricPolarity: null,
          isExact: false,
          validationStatus: null,
          validationReason: null,
          freshnessMode: null,
          primaryProvider: null,
          crossCheckProvider: null,
          crossCheckStatus: null,
          authorityGapReason: null,
        })),
    signals,
    signalGroups,
    regimeContextSignals,
    monitoring: contract.monitoring_conditions?.length
      ? contract.monitoring_conditions.map((item) => ({
          label: item.label,
          whyNow: appendDetail(item.why_now, forecastMeta(item.forecast_support)) ?? "",
          nearTermTrigger: appendDetail(item.near_term_trigger, triggerMeta(item.trigger_support)) ?? "",
          thesisTrigger: appendDetail(item.thesis_trigger, forecastMeta(item.forecast_support)) ?? "",
          breakCondition: item.break_condition,
          portfolioConsequence: item.portfolio_consequence,
          nextAction: item.next_action,
        }))
      : signalSource.slice(0, 6).map((signal) => ({
          label: signal.label,
          whyNow: signal.summary,
          nearTermTrigger: signal.confirms,
          thesisTrigger: signal.implication,
          breakCondition: signal.breaks,
          portfolioConsequence: signal.affected_sleeves.length
            ? `Touches ${signal.affected_sleeves.map(titleFromCode).join(", ")}.`
            : signal.affected_holdings.length
              ? `Touches ${signal.affected_holdings.join(", ")}.`
              : "No direct portfolio object is mapped yet.",
          nextAction: signalPosture(signal, contract.review_posture).label,
        })),
    contingentDrivers,
    macroCards: signalSource.slice(0, 4).map((signal) => ({
      label: signal.label,
      value: humanizeState(signal.signal_kind),
      note: signal.implication,
    })),
    crossAssetCards: signalSource.slice(1, 5).map((signal) => ({
      label: signal.label,
      value: humanizeState(signal.direction),
      note: signal.summary,
    })),
    fxCards: signalSource.slice(0, 3).map((signal) => ({
      label: signal.label,
      value: humanizeState(signal.mapping_directness),
      note: signal.do_not_overread ?? signal.summary,
    })),
    impactRows,
    reviewLanes,
    evidenceBars: contract.evidence_bars?.length
      ? contract.evidence_bars.map((bar) => ({
          label: bar.label,
          score: bar.score,
          tone: bar.tone,
        }))
      : [
          { label: "Freshness", score: scoreFromFreshness(contract.evidence_and_trust.freshness_state), tone: freshnessTone(contract.evidence_and_trust.freshness_state) },
          { label: "Support depth", score: Math.max(18, Math.min(100, Math.round(contract.evidence_and_trust.completeness_score * 100))), tone: contract.evidence_and_trust.completeness_score >= 0.7 ? "good" : "warn" },
          { label: "Source quality", score: contract.evidence_and_trust.source_count > 1 ? 68 : 42, tone: contract.evidence_and_trust.source_count > 1 ? "good" : "warn" },
          { label: "Directness", score: directnessSet.has("Direct") ? 72 : 44, tone: directnessSet.has("Direct") ? "good" : "warn" },
          { label: "Run strength", score: contract.freshness_state === "degraded_monitoring_mode" ? 36 : 78, tone: contract.freshness_state === "degraded_monitoring_mode" ? "warn" : "good" },
        ],
    evidenceRows: [
      { label: "Bottom line", value: leadSignal?.summary ?? contract.why_it_matters_economically },
      { label: "Portfolio read", value: contract.why_it_matters_here },
      { label: "What changes the brief", value: contract.what_confirms_or_breaks },
      {
        label: "Reference clocks",
        value:
          contract.data_timeframes?.slice(0, 3).map((item) => appendDetail(item.label, item.summary) ?? "").join(" | ")
          ?? "No explicit reference clocks emitted.",
      },
      {
        label: "Holdings context",
        value:
          contract.portfolio_overlay_context?.summary
          ?? contract.portfolio_overlay?.summary
          ?? "No holdings overlay is active yet. The brief remains market-first and portfolio consequence stays sleeve-level until overlay is loaded.",
      },
    ],
    diagnostics: contract.diagnostics?.length
      ? (() => {
          const selected = contract.diagnostics.slice(0, 4);
          const referenceClocks = contract.diagnostics.find((item) => item.label === "Reference clocks");
          return [
            ...selected,
            ...(referenceClocks && !selected.some((item) => item.label === "Reference clocks") ? [referenceClocks] : []),
            {
              label: "Forecast-supported blocks",
              value: String((contract.scenario_blocks ?? []).filter((block) => block.forecast_support).length),
            },
          ];
        })()
      : [
          { label: "Signals processed", value: String(signalSource.length) },
          { label: "Source count", value: String(contract.evidence_and_trust.source_count) },
          { label: "Completeness score", value: `${Math.round(contract.evidence_and_trust.completeness_score * 100)}%` },
          { label: "Review posture", value: contract.review_posture },
          {
            label: "Reference clocks",
            value:
              contract.data_timeframes?.slice(0, 3).map((item) => appendDetail(item.label, item.summary) ?? "").join(" | ")
              ?? "No explicit reference clocks emitted.",
          },
          { label: "Forecast-supported blocks", value: String((contract.scenario_blocks ?? []).filter((block) => block.forecast_support).length) },
        ],
    scenarioMessage: scenarios.length
      ? (appendDetail(
          leadScenarioSupport?.degraded_state
            ? "Scenario blocks are available with bounded forecast support."
            : "Scenario blocks are backed by active forecast support in the live brief.",
          forecastMeta(leadScenarioSupport)
        ) ?? "")
      : "Scenario blocks are not available in the current brief.",
    scenarios,
    inspector: [
      {
        label: "Review posture",
        value: contract.review_posture,
        tone: actionPostureTone(contract.review_posture),
      },
      ...(contract.data_confidence
        ? [
            {
              label: "Data confidence",
              value: humanizeState(contract.data_confidence),
              tone:
                contract.data_confidence === "high"
                  ? ("good" as Tone)
                  : contract.data_confidence === "mixed"
                    ? ("warn" as Tone)
                    : ("bad" as Tone),
            },
          ]
        : []),
      {
        label: "Evidence freshness",
        value: freshnessLabel(contract.evidence_and_trust.freshness_state),
        tone: freshnessTone(contract.evidence_and_trust.freshness_state),
      },
      {
        label: "Portfolio overlay",
        value:
          contract.portfolio_overlay_context?.summary
          ?? contract.portfolio_overlay?.summary
          ?? "No holdings overlay attached to this brief. Primary market and sleeve logic remains intact.",
      },
    ],
  };
}

function adaptScoreSummary(
  scoreSummary: {
    average_score: number;
    component_count_used: number;
    tone: string;
    reliability_state: string;
    reliability_note?: string | null;
    components: Array<{
      component_id: string;
      label: string;
      score: number;
      tone: string;
      summary?: string | null;
    }>;
  } | null | undefined,
): CandidateReportDisplay["scoreSummary"] {
  if (!scoreSummary) return null;
  return {
    averageScore: Number(scoreSummary.average_score ?? 0),
    componentCountUsed: Number(scoreSummary.component_count_used ?? 0),
    tone: (scoreSummary.tone ?? "neutral") as Tone,
    reliabilityState: (scoreSummary.reliability_state ?? "mixed") as "strong" | "mixed" | "weak",
    reliabilityNote: cleanBlueprintCopy(scoreSummary.reliability_note) ?? scoreSummary.reliability_note ?? null,
    components: (scoreSummary.components ?? []).map((component) => ({
      id: component.component_id,
      label: component.label,
      score: Number(component.score ?? 0),
      tone: (component.tone ?? "neutral") as Tone,
      summary: cleanBlueprintCopy(component.summary) ?? component.summary ?? "",
    })),
  };
}

export function adaptCandidateReport(contract: CandidateReportContract): CandidateReportDisplay {
  const marketPathSupport = contract.market_path_support ?? null;
  const marketPath = describeMarketPathSupport(marketPathSupport);
  const reportForecastMeta = marketPathSupport ? null : forecastMeta(contract.forecast_support);
  const recommendationGate = contract.recommendation_gate;
  const failureSummary = adaptFailureSummary(contract.failure_class_summary);
  const implementationProfile = contract.implementation_profile;
  const reconciliationStatus = contract.reconciliation_status;
  const dataQualitySummary = contract.data_quality_summary;
  const primaryDocumentCount = contract.primary_document_manifest?.length ?? 0;
  const fieldIssues = mapFieldIssues(contract.reconciliation_report);
  const implementationProfileRows: CandidateReportDisplay["implementationProfile"] = implementationProfile
    ? [
        { label: "Issuer", fieldName: "issuer", rawValue: implementationProfile.issuer },
        { label: "Issuer name", fieldName: "issuer_name", rawValue: implementationProfile.issuer_name },
        { label: "Mandate or index", fieldName: "mandate_or_index", rawValue: implementationProfile.mandate_or_index },
        { label: "Replication method", fieldName: "replication_method", rawValue: implementationProfile.replication_method },
        { label: "Primary listing exchange", fieldName: "primary_listing_exchange", rawValue: implementationProfile.primary_listing_exchange },
        { label: "Primary trading currency", fieldName: "primary_trading_currency", rawValue: implementationProfile.primary_trading_currency },
        { label: "Spread proxy", fieldName: "liquidity_proxy", rawValue: implementationProfile.spread_proxy },
        { label: "Premium / discount behavior", fieldName: "premium_discount_behavior", rawValue: implementationProfile.premium_discount_behavior },
        { label: "AUM", fieldName: "aum", rawValue: implementationProfile.aum },
        { label: "Domicile", fieldName: "domicile", rawValue: implementationProfile.domicile },
        { label: "Distribution policy", fieldName: "distribution_type", rawValue: implementationProfile.distribution_policy },
        { label: "Launch date", fieldName: "launch_date", rawValue: implementationProfile.launch_date ? formatDate(implementationProfile.launch_date) : null },
        { label: "Tracking difference", fieldName: "tracking_difference_1y", rawValue: implementationProfile.tracking_difference },
        {
          label: "Execution suitability",
          fieldName: "execution_suitability",
          rawValue: implementationProfile.execution_suitability ? humanizeState(implementationProfile.execution_suitability) : null,
        },
        {
          label: "Execution score",
          fieldName: "execution_score",
          rawValue:
            typeof implementationProfile.execution_score === "number"
              ? String(implementationProfile.execution_score)
              : null,
        },
        { label: "Summary", fieldName: "summary", rawValue: implementationProfile.summary },
        {
          label: "Missing fields",
          fieldName: "missing_fields",
          rawValue: implementationProfile.missing_fields?.length ? implementationProfile.missing_fields.join(", ") : null,
        },
      ]
        .map((row) => {
          const rendered = implementationFieldValue(row.rawValue, implementationProfile.missing_fields, row.fieldName);
          return { label: row.label, value: rendered.value, caution: rendered.caution ?? null };
        })
    : null;
  const sourceAuthorityFields: CandidateReportDisplay["sourceAuthorityFields"] = contract.source_authority_fields?.length
    ? contract.source_authority_fields.map((field) => ({
        fieldName: field.field_name,
        label: field.label,
        sourceLabel: appendDetail(text(field.source_name, "Unknown source"), field.source_type ? humanizeState(field.source_type) : null) ?? "",
        authorityClass: humanizeState(field.authority_class),
        freshness: humanizeState(field.freshness_state),
        isCritical: field.recommendation_critical,
        isRecommendationCritical: field.recommendation_critical,
      }))
    : null;
  const primaryDocuments: CandidateReportDisplay["primaryDocuments"] = contract.primary_document_manifest?.length
    ? contract.primary_document_manifest.map((document) => ({
        docType: humanizeState(document.doc_type),
        status: humanizeState(document.status),
        retrievedAt: document.retrieved_at ? formatDateTime(document.retrieved_at) : null,
      }))
    : null;
  const display: CandidateReportDisplay = {
    meta: {
      kicker: "ETF report",
      title: contract.name || (contract.candidate_id ? tickerFromCandidate(contract.candidate_id) : "Candidate"),
      copy: cleanBlueprintCopy(contract.current_implication) ?? contract.current_implication,
      badges: [
        { label: freshnessLabel(contract.freshness_state), tone: freshnessTone(contract.freshness_state) },
        { label: presentBlueprintDecisionState(contract.visible_decision_state.state), tone: stateTone(contract.visible_decision_state.state) },
        ...(marketPath
          ? [{ label: marketPath.stateLabel, tone: marketPath.stateTone as Tone }]
          : [{ label: presentEvidenceDepth(contract.evidence_depth), tone: (contract.evidence_depth === "substantial" ? "good" : contract.evidence_depth === "moderate" ? "info" : "warn") as Tone }]),
        ...(marketPath?.provenanceLabel ? [{ label: marketPath.provenanceLabel, tone: marketPath.provenanceTone as Tone }] : []),
        ...(reportForecastMeta ? [{ label: reportForecastMeta, tone: contract.forecast_support?.degraded_state ? "warn" as Tone : "info" as Tone }] : []),
      ],
    },
    summaryChips: [
      {
        label: "Decision state",
        value: presentBlueprintDecisionState(contract.visible_decision_state.state),
        meta: failureSummary?.summary ?? cleanBlueprintCopy(contract.visible_decision_state.rationale) ?? contract.visible_decision_state.rationale,
        tone: stateTone(contract.visible_decision_state.state),
      },
      {
        label: marketPath ? "Market path" : "Source coverage",
        value: marketPath?.stateLabel ?? presentEvidenceDepth(contract.evidence_depth),
        meta: marketPath?.summaryLine ?? freshnessLabel(contract.freshness_state),
        tone: marketPath?.stateTone ?? freshnessTone(contract.freshness_state),
      },
      {
        label: "Action boundary",
        value: cleanBlueprintCopy(contract.action_boundary) ?? text(contract.action_boundary, "No investor-facing action boundary was surfaced."),
        meta: "Boundary stays investor-visible",
      },
        {
          label: "Mandate boundary",
          value: cleanBlueprintCopy(contract.mandate_boundary) ?? text(contract.mandate_boundary, "No mandate boundary was surfaced."),
          meta: "Rubric only when decisive",
        },
        ...(recommendationGate
          ? [
              {
                label: "Admissibility",
                value: presentRecommendationGateState(recommendationGate.gate_state),
                meta: cleanBlueprintCopy(recommendationGate.summary) ?? recommendationGate.summary,
                tone:
                  recommendationGate.gate_state === "admissible"
                    ? ("good" as Tone)
                    : recommendationGate.gate_state === "review_only"
                      ? ("warn" as Tone)
                      : ("bad" as Tone),
              },
            ]
          : []),
        ...(failureSummary?.primaryLabel
          ? [
              {
                label: "Restriction basis",
                value: failureSummary.primaryLabel,
                meta: failureSummary.summary ?? undefined,
                tone: failureSummary.hardClasses.length ? ("bad" as Tone) : failureSummary.reviewClasses.length ? ("warn" as Tone) : ("info" as Tone),
              },
            ]
          : []),
        ...(dataQualitySummary
          ? [
              {
                label: "Data confidence",
                value: humanizeState(dataQualitySummary.data_confidence),
                meta: cleanBlueprintCopy(dataQualitySummary.summary) ?? dataQualitySummary.summary,
                tone:
                  dataQualitySummary.data_confidence === "high"
                    ? ("good" as Tone)
                    : dataQualitySummary.data_confidence === "mixed"
                      ? ("warn" as Tone)
                      : ("bad" as Tone),
              },
            ]
          : []),
        ...(implementationProfile
          ? [
              {
                label: "Execution",
                value: humanizeState(implementationProfile.execution_suitability),
                meta: appendDetail(cleanBlueprintCopy(implementationProfile.summary) ?? implementationProfile.summary ?? null, implementationProfile.spread_proxy ?? null) ?? undefined,
                tone:
                  implementationProfile.execution_suitability === "execution_efficient"
                    ? ("good" as Tone)
                    : implementationProfile.execution_suitability === "execution_mixed"
                      ? ("warn" as Tone)
                      : ("bad" as Tone),
              },
            ]
          : []),
        ...(primaryDocumentCount
          ? [
              {
                label: "Primary docs",
                value: `${primaryDocumentCount}`,
                meta: "Issuer document support attached to recommendation-critical fields",
                tone: "info" as Tone,
              },
            ]
          : []),
        ...(reconciliationStatus && reconciliationStatus.status !== "verified"
          ? [
              {
                label: "Reconciliation",
                value: humanizeState(reconciliationStatus.status),
                meta: cleanBlueprintCopy(reconciliationStatus.summary) ?? reconciliationStatus.summary,
                tone: reconciliationStatus.status === "hard_conflict" ? ("bad" as Tone) : ("warn" as Tone),
              },
            ]
          : []),
        ...(marketPath
          ? [
              {
                label: "Market-path provenance",
                value: marketPath.provenanceLabel ?? "Bounded support",
                meta: marketPath.providerLabel ?? marketPath.qualityNote ?? undefined,
                tone: marketPath.provenanceTone,
              },
            ]
          : reportForecastMeta
            ? [
                {
                  label: "Forecast support",
                  value: reportForecastMeta,
                  meta: contract.forecast_support?.support_strength ?? "Support only",
                  tone: contract.forecast_support?.degraded_state ? ("warn" as Tone) : ("info" as Tone),
                },
              ]
            : []),
    ],
    tabs: [
      { id: "investment_case", label: "Investment Case" },
      { id: "market_history", label: "Market & History" },
      { id: "scenarios", label: "Scenarios & Outlook" },
      { id: "risks", label: "Risks & Implementation" },
      { id: "competition", label: "Competition" },
      { id: "evidence", label: "Evidence & Sources" },
    ],
    rationale: failureSummary?.summary ?? cleanBlueprintCopy(contract.visible_decision_state.rationale) ?? contract.visible_decision_state.rationale,
    investmentCase: cleanBlueprintCopy(contract.investment_case) ?? contract.investment_case,
    currentImplication: cleanBlueprintCopy(contract.current_implication) ?? contract.current_implication,
    actionBoundary: cleanBlueprintCopy(contract.action_boundary) ?? contract.action_boundary ?? null,
    whatChangesView: appendDetail(
      cleanBlueprintCopy(contract.what_changes_view) ?? contract.what_changes_view ?? null,
      reconciliationStatus && reconciliationStatus.status !== "verified" ? (cleanBlueprintCopy(reconciliationStatus.summary) ?? reconciliationStatus.summary) : null
    ),
    failureSummary,
    scoreBreakdown: contract.score_decomposition
      ? {
          total: contract.score_decomposition.total_score,
          recommendation: contract.score_decomposition.recommendation_score ?? contract.score_decomposition.total_score,
          recommendationMerit: contract.score_decomposition.recommendation_merit_score ?? contract.score_decomposition.investment_merit_score ?? contract.score_decomposition.optimality_score ?? null,
          investmentMerit: contract.score_decomposition.investment_merit_score ?? contract.score_decomposition.optimality_score ?? null,
          deployability: contract.score_decomposition.deployability_score ?? contract.score_decomposition.deployment_score ?? contract.score_decomposition.readiness_score ?? null,
          truthConfidence: contract.score_decomposition.truth_confidence_score ?? null,
          truthConfidenceBand: contract.score_decomposition.truth_confidence_band ?? null,
          truthConfidenceSummary: cleanBlueprintCopy(contract.score_decomposition.truth_confidence_summary) ?? contract.score_decomposition.truth_confidence_summary ?? null,
          deployment: contract.score_decomposition.deployment_score ?? contract.score_decomposition.total_score,
          admissibility: contract.score_decomposition.admissibility_score ?? null,
          admissibilityIdentity: contract.score_decomposition.admissibility_identity_score ?? null,
          implementation: contract.score_decomposition.implementation_score ?? null,
          sourceIntegrity: contract.score_decomposition.source_integrity_score ?? null,
          evidence: contract.score_decomposition.evidence_score ?? null,
          sleeveFit: contract.score_decomposition.sleeve_fit_score ?? null,
          identity: contract.score_decomposition.identity_score ?? null,
          benchmarkFidelity: contract.score_decomposition.benchmark_fidelity_score ?? null,
          marketPathSupport: contract.score_decomposition.market_path_support_score ?? null,
          longHorizonQuality: contract.score_decomposition.long_horizon_quality_score ?? null,
          instrumentQuality: contract.score_decomposition.instrument_quality_score ?? null,
          portfolioFit: contract.score_decomposition.portfolio_fit_score ?? null,
          optimality: contract.score_decomposition.optimality_score ?? null,
          readiness: contract.score_decomposition.readiness_score ?? null,
          confidencePenalty: contract.score_decomposition.confidence_penalty ?? null,
          readinessPosture: contract.score_decomposition.readiness_posture ?? null,
          readinessSummary: cleanBlueprintCopy(contract.score_decomposition.readiness_summary) ?? contract.score_decomposition.readiness_summary ?? null,
          deployabilityBadge: contract.score_decomposition.deployability_badge ?? null,
          summary: cleanBlueprintCopy(contract.score_decomposition.summary) ?? contract.score_decomposition.summary ?? null,
        }
      : null,
    scoreSummary: adaptScoreSummary(contract.score_summary),
    scoreComponents: (contract.score_decomposition?.components ?? []).map((component) => ({
      id: component.component_id ?? component.key ?? undefined,
      label: component.label,
      score: component.score,
      band: component.band ?? null,
      confidence: typeof component.confidence === "number" ? component.confidence : null,
      tone: (component.tone as Tone) ?? "neutral",
      summary: cleanBlueprintCopy(component.summary) ?? component.summary,
      reasons: (component.reasons ?? []).map((reason) => cleanBlueprintCopy(reason) ?? reason),
      capsApplied: (component.caps_applied ?? []).map((item) => cleanBlueprintCopy(item) ?? item),
      fieldDrivers: (component.field_drivers ?? []).map((item) => humanizeState(item)),
    })),
    upgradeCondition: cleanBlueprintCopy(contract.upgrade_condition) ?? contract.upgrade_condition ?? null,
    downgradeCondition: cleanBlueprintCopy(contract.downgrade_condition) ?? contract.downgrade_condition ?? null,
    killCondition: cleanBlueprintCopy(contract.kill_condition) ?? contract.kill_condition ?? null,
    decisionConditions: contract.decision_condition_pack
      ? {
          intro:
            cleanBlueprintCopy(contract.decision_condition_pack.intro)
            ?? contract.decision_condition_pack.intro
            ?? "These are the conditions that would materially change the current verdict.",
          items: (["upgrade", "downgrade", "kill"] as const)
            .map((kind) => {
              const item = contract.decision_condition_pack?.[kind];
              if (!item?.text) return null;
              return {
                kind: kind as "upgrade" | "downgrade" | "kill",
                label: cleanBlueprintCopy(item.label) ?? item.label ?? `${kind[0].toUpperCase()}${kind.slice(1)} if`,
                text: cleanBlueprintCopy(item.text) ?? item.text,
                supportText: cleanBlueprintCopy(item.support_text) ?? item.support_text ?? null,
                confirmationLabel: cleanBlueprintCopy(item.confirmation_label) ?? item.confirmation_label ?? null,
                confirmationPoints: (item.confirmation_points ?? [])
                  .map((value) => cleanBlueprintCopy(value) ?? value)
                  .filter(Boolean),
                confidence: cleanBlueprintCopy(item.confidence) ?? item.confidence ?? null,
                basisLabels: (item.basis_labels ?? []).map((value) => cleanBlueprintCopy(value) ?? value).filter(Boolean),
              };
            })
            .filter(Boolean) as CandidateDecisionConditionDisplayItem[],
        }
      : {
          intro: "These are the conditions that would materially change the current verdict.",
          items: [
            contract.upgrade_condition
              ? {
                  kind: "upgrade" as const,
                  label: "Upgrade if",
                  text: cleanBlueprintCopy(contract.upgrade_condition) ?? contract.upgrade_condition,
                  supportText: null,
                  confirmationLabel: null,
                  confirmationPoints: [] as string[],
                  confidence: null,
                  basisLabels: [] as string[],
                }
              : null,
            contract.downgrade_condition
              ? {
                  kind: "downgrade" as const,
                  label: "Downgrade if",
                  text: cleanBlueprintCopy(contract.downgrade_condition) ?? contract.downgrade_condition,
                  supportText: null,
                  confirmationLabel: null,
                  confirmationPoints: [] as string[],
                  confidence: null,
                  basisLabels: [] as string[],
                }
              : null,
            contract.kill_condition
              ? {
                  kind: "kill" as const,
                  label: "Kill if",
                  text: cleanBlueprintCopy(contract.kill_condition) ?? contract.kill_condition,
                  supportText: null,
                  confirmationLabel: null,
                  confirmationPoints: [] as string[],
                  confidence: null,
                  basisLabels: [] as string[],
                }
              : null,
          ].filter(Boolean) as CandidateDecisionConditionDisplayItem[],
        },
    tradeoffs: [
      ...contract.main_tradeoffs.map((value) => cleanBlueprintCopy(value) ?? value),
      ...(failureSummary?.summary ? [failureSummary.summary] : []),
      ...((recommendationGate?.blocked_reasons ?? []).slice(0, 2)).map((value) => cleanBlueprintCopy(value) ?? value),
    ].filter((value, index, rows) => Boolean(value) && rows.indexOf(value) === index),
    baselineComparisons: contract.baseline_comparisons,
    doctrineAnnotations: contract.doctrine_annotations,
    evidenceDepth: presentEvidenceDepth(contract.evidence_depth),
    mandateBoundary: cleanBlueprintCopy(contract.mandate_boundary) ?? contract.mandate_boundary ?? null,
    overlayMessage:
      contract.overlay_context?.summary
      ?? (contract.holdings_overlay_present ? "Holdings overlay data is attached." : null),
    quickBrief: contract.quick_brief
      ? {
          statusState: contract.quick_brief.status_state,
          statusLabel: cleanBlueprintCopy(contract.quick_brief.status_label) ?? contract.quick_brief.status_label,
          fundIdentity: contract.quick_brief.fund_identity
            ? {
                ticker: cleanBlueprintCopy(contract.quick_brief.fund_identity.ticker) ?? contract.quick_brief.fund_identity.ticker,
                name: cleanBlueprintCopy(contract.quick_brief.fund_identity.name) ?? contract.quick_brief.fund_identity.name,
                issuer: cleanBlueprintCopy(contract.quick_brief.fund_identity.issuer) ?? contract.quick_brief.fund_identity.issuer ?? null,
                exposureLabel:
                  cleanBlueprintCopy(contract.quick_brief.fund_identity.exposure_label)
                  ?? contract.quick_brief.fund_identity.exposure_label
                  ?? null,
              }
            : null,
          portfolioRole: cleanBlueprintCopy(contract.quick_brief.portfolio_role) ?? contract.quick_brief.portfolio_role ?? null,
          roleLabel: cleanBlueprintCopy(contract.quick_brief.role_label) ?? contract.quick_brief.role_label ?? null,
          summary: cleanBlueprintCopy(contract.quick_brief.summary) ?? contract.quick_brief.summary,
          decisionReasons: (contract.quick_brief.decision_reasons ?? []).map((value) => cleanBlueprintCopy(value) ?? value),
          secondaryReasons: (contract.quick_brief.secondary_reasons ?? []).map((value) => cleanBlueprintCopy(value) ?? value),
          keyFacts: (contract.quick_brief.key_facts ?? []).map((row) => ({
            label: cleanBlueprintCopy(row.label) ?? row.label,
            value: cleanBlueprintCopy(row.value) ?? row.value,
          })),
          whyThisMattersLine:
            cleanBlueprintCopy(contract.quick_brief.why_this_matters)
            ?? contract.quick_brief.why_this_matters
            ?? null,
          compareFirstLine:
            cleanBlueprintCopy(contract.quick_brief.compare_first)
            ?? contract.quick_brief.compare_first
            ?? null,
          broaderAlternativeLine:
            cleanBlueprintCopy(contract.quick_brief.broader_alternative)
            ?? contract.quick_brief.broader_alternative
            ?? null,
          whatItSolvesLine:
            cleanBlueprintCopy(contract.quick_brief.what_it_solves)
            ?? contract.quick_brief.what_it_solves
            ?? null,
          whatItStillNeedsToProveLine:
            cleanBlueprintCopy(contract.quick_brief.what_it_still_needs_to_prove)
            ?? contract.quick_brief.what_it_still_needs_to_prove
            ?? null,
          decisionReadinessLine:
            cleanBlueprintCopy(contract.quick_brief.decision_readiness)
            ?? contract.quick_brief.decision_readiness
            ?? null,
          shouldIUse: contract.quick_brief.should_i_use
            ? {
                bestFor: cleanBlueprintCopy(contract.quick_brief.should_i_use.best_for) ?? contract.quick_brief.should_i_use.best_for,
                notIdealFor:
                  cleanBlueprintCopy(contract.quick_brief.should_i_use.not_ideal_for) ?? contract.quick_brief.should_i_use.not_ideal_for,
                useItWhen: cleanBlueprintCopy(contract.quick_brief.should_i_use.use_it_when) ?? contract.quick_brief.should_i_use.use_it_when,
                waitIf: cleanBlueprintCopy(contract.quick_brief.should_i_use.wait_if) ?? contract.quick_brief.should_i_use.wait_if,
                compareAgainst:
                  cleanBlueprintCopy(contract.quick_brief.should_i_use.compare_against) ?? contract.quick_brief.should_i_use.compare_against,
              }
            : null,
          performanceChecks: (contract.quick_brief.performance_checks ?? []).map((row) => ({
            checkId: row.check_id,
            label: cleanBlueprintCopy(row.label) ?? row.label,
            summary: cleanBlueprintCopy(row.summary) ?? row.summary,
            metric: cleanBlueprintCopy(row.metric) ?? row.metric ?? null,
          })),
          whatYouAreBuying: (contract.quick_brief.what_you_are_buying ?? []).map((row) => ({
            label: cleanBlueprintCopy(row.label) ?? row.label,
            value: cleanBlueprintCopy(row.value) ?? row.value,
          })),
          portfolioFit: contract.quick_brief.portfolio_fit
            ? {
                roleInPortfolio:
                  cleanBlueprintCopy(contract.quick_brief.portfolio_fit.role_in_portfolio)
                  ?? contract.quick_brief.portfolio_fit.role_in_portfolio,
                whatItDoesNotSolve:
                  cleanBlueprintCopy(contract.quick_brief.portfolio_fit.what_it_does_not_solve)
                  ?? contract.quick_brief.portfolio_fit.what_it_does_not_solve,
                currentNeed:
                  cleanBlueprintCopy(contract.quick_brief.portfolio_fit.current_need)
                  ?? contract.quick_brief.portfolio_fit.current_need,
              }
            : null,
          howToDecide: (contract.quick_brief.how_to_decide ?? []).map((value) => cleanBlueprintCopy(value) ?? value),
          evidenceFooterDetail: contract.quick_brief.evidence_footer_detail
            ? {
                evidenceQuality:
                  cleanBlueprintCopy(contract.quick_brief.evidence_footer_detail.evidence_quality)
                  ?? contract.quick_brief.evidence_footer_detail.evidence_quality,
                dataCompleteness:
                  cleanBlueprintCopy(contract.quick_brief.evidence_footer_detail.data_completeness)
                  ?? contract.quick_brief.evidence_footer_detail.data_completeness,
                documentSupport:
                  cleanBlueprintCopy(contract.quick_brief.evidence_footer_detail.document_support)
                  ?? contract.quick_brief.evidence_footer_detail.document_support,
                monitoringStatus:
                  cleanBlueprintCopy(contract.quick_brief.evidence_footer_detail.monitoring_status)
                  ?? contract.quick_brief.evidence_footer_detail.monitoring_status,
              }
            : null,
          scenarioEntry: contract.quick_brief.scenario_entry
            ? {
                backdropSummary:
                  cleanBlueprintCopy(contract.quick_brief.scenario_entry.backdrop_summary)
                  ?? contract.quick_brief.scenario_entry.backdrop_summary,
                disclosureLabel:
                  cleanBlueprintCopy(contract.quick_brief.scenario_entry.disclosure_label)
                  ?? contract.quick_brief.scenario_entry.disclosure_label,
              }
            : null,
          kronosMarketSetup: contract.quick_brief.kronos_market_setup
            ? {
                scopeKey: cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.scope_key) ?? contract.quick_brief.kronos_market_setup.scope_key ?? null,
                scopeLabel: cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.scope_label) ?? contract.quick_brief.kronos_market_setup.scope_label ?? null,
                marketSetupState:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.market_setup_state)
                  ?? contract.quick_brief.kronos_market_setup.market_setup_state
                  ?? null,
                routeLabel:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.route_label)
                  ?? contract.quick_brief.kronos_market_setup.route_label
                  ?? null,
                forecastObjectLabel:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.forecast_object_label)
                  ?? contract.quick_brief.kronos_market_setup.forecast_object_label
                  ?? null,
                horizonLabel:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.horizon_label)
                  ?? contract.quick_brief.kronos_market_setup.horizon_label
                  ?? null,
                pathSupportLabel:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.path_support_label)
                  ?? contract.quick_brief.kronos_market_setup.path_support_label
                  ?? null,
                confidenceLabel:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.confidence_label)
                  ?? contract.quick_brief.kronos_market_setup.confidence_label
                  ?? null,
                freshnessLabel:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.freshness_label)
                  ?? contract.quick_brief.kronos_market_setup.freshness_label
                  ?? null,
                downsideRiskLabel:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.downside_risk_label)
                  ?? contract.quick_brief.kronos_market_setup.downside_risk_label
                  ?? null,
                driftLabel:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.drift_label)
                  ?? contract.quick_brief.kronos_market_setup.drift_label
                  ?? null,
                volatilityRegimeLabel:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.volatility_regime_label)
                  ?? contract.quick_brief.kronos_market_setup.volatility_regime_label
                  ?? null,
                decisionImpactText:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.decision_impact_text)
                  ?? contract.quick_brief.kronos_market_setup.decision_impact_text
                  ?? null,
                qualityGate:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.quality_gate)
                  ?? contract.quick_brief.kronos_market_setup.quality_gate
                  ?? null,
                asOf: cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.as_of) ?? contract.quick_brief.kronos_market_setup.as_of ?? null,
                scenarioAvailable: contract.quick_brief.kronos_market_setup.scenario_available ?? null,
                openScenarioCta:
                  cleanBlueprintCopy(contract.quick_brief.kronos_market_setup.open_scenario_cta)
                  ?? contract.quick_brief.kronos_market_setup.open_scenario_cta
                  ?? null,
              }
            : null,
          kronosDecisionBridge: contract.quick_brief.kronos_decision_bridge
            ? {
                selectionContext:
                  cleanBlueprintCopy(contract.quick_brief.kronos_decision_bridge.selection_context)
                  ?? contract.quick_brief.kronos_decision_bridge.selection_context
                  ?? null,
                regimeSummary:
                  cleanBlueprintCopy(contract.quick_brief.kronos_decision_bridge.regime_summary)
                  ?? contract.quick_brief.kronos_decision_bridge.regime_summary
                  ?? null,
                selectionConsequence:
                  cleanBlueprintCopy(contract.quick_brief.kronos_decision_bridge.selection_consequence)
                  ?? contract.quick_brief.kronos_decision_bridge.selection_consequence
                  ?? null,
                wrapperBoundaryText:
                  cleanBlueprintCopy(contract.quick_brief.kronos_decision_bridge.wrapper_boundary_text)
                  ?? contract.quick_brief.kronos_decision_bridge.wrapper_boundary_text
                  ?? null,
                supportsExposureChoice: contract.quick_brief.kronos_decision_bridge.supports_exposure_choice ?? null,
                supportsWrapperChoice: contract.quick_brief.kronos_decision_bridge.supports_wrapper_choice ?? null,
                decisionStrengthLabel:
                  cleanBlueprintCopy(contract.quick_brief.kronos_decision_bridge.decision_strength_label)
                  ?? contract.quick_brief.kronos_decision_bridge.decision_strength_label
                  ?? null,
              }
            : null,
          kronosCompareCheck: contract.quick_brief.kronos_compare_check
            ? {
                compareContext:
                  cleanBlueprintCopy(contract.quick_brief.kronos_compare_check.compare_context)
                  ?? contract.quick_brief.kronos_compare_check.compare_context
                  ?? null,
                regimeCheckText:
                  cleanBlueprintCopy(contract.quick_brief.kronos_compare_check.regime_check_text)
                  ?? contract.quick_brief.kronos_compare_check.regime_check_text
                  ?? null,
                affectsPeerPreference: contract.quick_brief.kronos_compare_check.affects_peer_preference ?? null,
                affectsExposurePreference: contract.quick_brief.kronos_compare_check.affects_exposure_preference ?? null,
              }
            : null,
          kronosScenarioPack: contract.quick_brief.kronos_scenario_pack
            ? {
                observedPath:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.observed_path)
                  ?? contract.quick_brief.kronos_scenario_pack.observed_path
                  ?? null,
                basePath:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.base_path)
                  ?? contract.quick_brief.kronos_scenario_pack.base_path
                  ?? null,
                downsidePath:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.downside_path)
                  ?? contract.quick_brief.kronos_scenario_pack.downside_path
                  ?? null,
                stressPath:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.stress_path)
                  ?? contract.quick_brief.kronos_scenario_pack.stress_path
                  ?? null,
                uncertaintyBand:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.uncertainty_band)
                  ?? contract.quick_brief.kronos_scenario_pack.uncertainty_band
                  ?? null,
                driftState:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.drift_state)
                  ?? contract.quick_brief.kronos_scenario_pack.drift_state
                  ?? null,
                fragilityState:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.fragility_state)
                  ?? contract.quick_brief.kronos_scenario_pack.fragility_state
                  ?? null,
                thresholdFlags: (contract.quick_brief.kronos_scenario_pack.threshold_flags ?? []).map((value) => cleanBlueprintCopy(value) ?? value),
                qualityGate:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.quality_gate)
                  ?? contract.quick_brief.kronos_scenario_pack.quality_gate
                  ?? null,
                provenance:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.provenance)
                  ?? contract.quick_brief.kronos_scenario_pack.provenance
                  ?? null,
                refreshStatus:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.refresh_status)
                  ?? contract.quick_brief.kronos_scenario_pack.refresh_status
                  ?? null,
                lastRunAt:
                  cleanBlueprintCopy(contract.quick_brief.kronos_scenario_pack.last_run_at)
                  ?? contract.quick_brief.kronos_scenario_pack.last_run_at
                  ?? null,
              }
            : null,
          kronosOptionalMetrics: contract.quick_brief.kronos_optional_metrics
            ? {
                upsideProbability:
                  cleanBlueprintCopy(contract.quick_brief.kronos_optional_metrics.upside_probability)
                  ?? contract.quick_brief.kronos_optional_metrics.upside_probability
                  ?? null,
                downsideBreachProbability:
                  cleanBlueprintCopy(contract.quick_brief.kronos_optional_metrics.downside_breach_probability)
                  ?? contract.quick_brief.kronos_optional_metrics.downside_breach_probability
                  ?? null,
                volatilityElevationProbability:
                  cleanBlueprintCopy(contract.quick_brief.kronos_optional_metrics.volatility_elevation_probability)
                  ?? contract.quick_brief.kronos_optional_metrics.volatility_elevation_probability
                  ?? null,
                changeVsPriorRun:
                  cleanBlueprintCopy(contract.quick_brief.kronos_optional_metrics.change_vs_prior_run)
                  ?? contract.quick_brief.kronos_optional_metrics.change_vs_prior_run
                  ?? null,
              }
            : null,
          peerComparePack: contract.quick_brief.peer_compare_pack
            ? {
                candidateSymbol:
                  cleanBlueprintCopy(contract.quick_brief.peer_compare_pack.candidate_symbol)
                  ?? contract.quick_brief.peer_compare_pack.candidate_symbol,
                candidateLabel:
                  cleanBlueprintCopy(contract.quick_brief.peer_compare_pack.candidate_label)
                  ?? contract.quick_brief.peer_compare_pack.candidate_label,
                primaryQuestion:
                  cleanBlueprintCopy(contract.quick_brief.peer_compare_pack.primary_question)
                  ?? contract.quick_brief.peer_compare_pack.primary_question
                  ?? null,
                comparisonBasis:
                  cleanBlueprintCopy(contract.quick_brief.peer_compare_pack.comparison_basis)
                  ?? contract.quick_brief.peer_compare_pack.comparison_basis
                  ?? null,
                rows: (contract.quick_brief.peer_compare_pack.rows ?? []).map((row) => ({
                  role: row.role,
                  fundName: cleanBlueprintCopy(row.fund_name) ?? row.fund_name,
                  tickerOrLine: cleanBlueprintCopy(row.ticker_or_line) ?? row.ticker_or_line ?? null,
                  isin: cleanBlueprintCopy(row.isin) ?? row.isin ?? null,
                  benchmark: cleanBlueprintCopy(row.benchmark) ?? row.benchmark ?? null,
                  benchmarkFamily: cleanBlueprintCopy(row.benchmark_family) ?? row.benchmark_family ?? null,
                  exposureScope: cleanBlueprintCopy(row.exposure_scope) ?? row.exposure_scope ?? null,
                  developedOnly: row.developed_only ?? null,
                  emergingMarketsIncluded: row.emerging_markets_included ?? null,
                  ter: cleanBlueprintCopy(row.ter) ?? row.ter ?? null,
                  fundAssets: cleanBlueprintCopy(row.fund_assets) ?? row.fund_assets ?? null,
                  shareClassAssets: cleanBlueprintCopy(row.share_class_assets) ?? row.share_class_assets ?? null,
                  holdingsCount: cleanBlueprintCopy(row.holdings_count) ?? row.holdings_count ?? null,
                  replication: cleanBlueprintCopy(row.replication) ?? row.replication ?? null,
                  distribution: cleanBlueprintCopy(row.distribution) ?? row.distribution ?? null,
                  domicile: cleanBlueprintCopy(row.domicile) ?? row.domicile ?? null,
                  launchDate: cleanBlueprintCopy(row.launch_date) ?? row.launch_date ?? null,
                  trackingError1Y: cleanBlueprintCopy(row.tracking_error_1y) ?? row.tracking_error_1y ?? null,
                  trackingError3Y: cleanBlueprintCopy(row.tracking_error_3y) ?? row.tracking_error_3y ?? null,
                  trackingError5Y: cleanBlueprintCopy(row.tracking_error_5y) ?? row.tracking_error_5y ?? null,
                  trackingDifference1Y:
                    cleanBlueprintCopy(row.tracking_difference_1y) ?? row.tracking_difference_1y ?? null,
                  trackingDifference3Y:
                    cleanBlueprintCopy(row.tracking_difference_3y) ?? row.tracking_difference_3y ?? null,
                  listingExchange: cleanBlueprintCopy(row.listing_exchange) ?? row.listing_exchange ?? null,
                  listingCurrency: cleanBlueprintCopy(row.listing_currency) ?? row.listing_currency ?? null,
                  whyThisPeerMatters: cleanBlueprintCopy(row.why_this_peer_matters) ?? row.why_this_peer_matters ?? null,
                  terDelta: cleanBlueprintCopy(row.ter_delta) ?? row.ter_delta ?? null,
                  holdingsDelta: cleanBlueprintCopy(row.holdings_delta) ?? row.holdings_delta ?? null,
                  sameIndex: row.same_index ?? null,
                  sameJob: row.same_job ?? null,
                  sameDistribution: row.same_distribution ?? null,
                  sameDomicile: row.same_domicile ?? null,
                })),
              }
            : null,
          fundProfile: contract.quick_brief.fund_profile
            ? {
                objective: cleanBlueprintCopy(contract.quick_brief.fund_profile.objective) ?? contract.quick_brief.fund_profile.objective ?? null,
                benchmark: cleanBlueprintCopy(contract.quick_brief.fund_profile.benchmark) ?? contract.quick_brief.fund_profile.benchmark ?? null,
                benchmarkFamily:
                  cleanBlueprintCopy(contract.quick_brief.fund_profile.benchmark_family)
                  ?? contract.quick_brief.fund_profile.benchmark_family
                  ?? null,
                domicile: cleanBlueprintCopy(contract.quick_brief.fund_profile.domicile) ?? contract.quick_brief.fund_profile.domicile ?? null,
                replication:
                  cleanBlueprintCopy(contract.quick_brief.fund_profile.replication) ?? contract.quick_brief.fund_profile.replication ?? null,
                distribution:
                  cleanBlueprintCopy(contract.quick_brief.fund_profile.distribution) ?? contract.quick_brief.fund_profile.distribution ?? null,
                fundAssets: cleanBlueprintCopy(contract.quick_brief.fund_profile.fund_assets) ?? contract.quick_brief.fund_profile.fund_assets ?? null,
                shareClassAssets:
                  cleanBlueprintCopy(contract.quick_brief.fund_profile.share_class_assets)
                  ?? contract.quick_brief.fund_profile.share_class_assets
                  ?? null,
                holdingsCount:
                  cleanBlueprintCopy(contract.quick_brief.fund_profile.holdings_count)
                  ?? contract.quick_brief.fund_profile.holdings_count
                  ?? null,
                launchDate: cleanBlueprintCopy(contract.quick_brief.fund_profile.launch_date) ?? contract.quick_brief.fund_profile.launch_date ?? null,
                issuer: cleanBlueprintCopy(contract.quick_brief.fund_profile.issuer) ?? contract.quick_brief.fund_profile.issuer ?? null,
                documents: (contract.quick_brief.fund_profile.documents ?? []).map((row) => ({
                  label: cleanBlueprintCopy(row.label) ?? row.label,
                  url: cleanBlueprintCopy(row.url) ?? row.url ?? null,
                })),
              }
            : null,
          listingProfile: contract.quick_brief.listing_profile
            ? {
                exchange: cleanBlueprintCopy(contract.quick_brief.listing_profile.exchange) ?? contract.quick_brief.listing_profile.exchange ?? null,
                tradingCurrency:
                  cleanBlueprintCopy(contract.quick_brief.listing_profile.trading_currency)
                  ?? contract.quick_brief.listing_profile.trading_currency
                  ?? null,
                ticker: cleanBlueprintCopy(contract.quick_brief.listing_profile.ticker) ?? contract.quick_brief.listing_profile.ticker ?? null,
                marketPrice:
                  cleanBlueprintCopy(contract.quick_brief.listing_profile.market_price)
                  ?? contract.quick_brief.listing_profile.market_price
                  ?? null,
                nav: cleanBlueprintCopy(contract.quick_brief.listing_profile.nav) ?? contract.quick_brief.listing_profile.nav ?? null,
                spreadProxy:
                  cleanBlueprintCopy(contract.quick_brief.listing_profile.spread_proxy)
                  ?? contract.quick_brief.listing_profile.spread_proxy
                  ?? null,
                volume: cleanBlueprintCopy(contract.quick_brief.listing_profile.volume) ?? contract.quick_brief.listing_profile.volume ?? null,
                premiumDiscount:
                  cleanBlueprintCopy(contract.quick_brief.listing_profile.premium_discount)
                  ?? contract.quick_brief.listing_profile.premium_discount
                  ?? null,
                asOf: cleanBlueprintCopy(contract.quick_brief.listing_profile.as_of) ?? contract.quick_brief.listing_profile.as_of ?? null,
              }
            : null,
          indexScopeExplainer: adaptIndexScopeExplainer(contract.quick_brief.index_scope_explainer),
          decisionProofPack: contract.quick_brief.decision_proof_pack
            ? {
                whyCandidateExists:
                  cleanBlueprintCopy(contract.quick_brief.decision_proof_pack.why_candidate_exists)
                  ?? contract.quick_brief.decision_proof_pack.why_candidate_exists
                  ?? null,
                whyInScope:
                  cleanBlueprintCopy(contract.quick_brief.decision_proof_pack.why_in_scope)
                  ?? contract.quick_brief.decision_proof_pack.why_in_scope
                  ?? null,
                whyNotCompleteSolution:
                  cleanBlueprintCopy(contract.quick_brief.decision_proof_pack.why_not_complete_solution)
                  ?? contract.quick_brief.decision_proof_pack.why_not_complete_solution
                  ?? null,
                bestSameJobPeers:
                  cleanBlueprintCopy(contract.quick_brief.decision_proof_pack.best_same_job_peers)
                  ?? contract.quick_brief.decision_proof_pack.best_same_job_peers
                  ?? null,
                broaderControlPeer:
                  cleanBlueprintCopy(contract.quick_brief.decision_proof_pack.broader_control_peer)
                  ?? contract.quick_brief.decision_proof_pack.broader_control_peer
                  ?? null,
                feePremiumQuestion:
                  cleanBlueprintCopy(contract.quick_brief.decision_proof_pack.fee_premium_question)
                  ?? contract.quick_brief.decision_proof_pack.fee_premium_question
                  ?? null,
                whatMustBeTrueToPreferThis:
                  cleanBlueprintCopy(contract.quick_brief.decision_proof_pack.what_must_be_true_to_prefer_this)
                  ?? contract.quick_brief.decision_proof_pack.what_must_be_true_to_prefer_this
                  ?? null,
                whatWouldChangeVerdict:
                  cleanBlueprintCopy(contract.quick_brief.decision_proof_pack.what_would_change_verdict)
                  ?? contract.quick_brief.decision_proof_pack.what_would_change_verdict
                  ?? null,
              }
            : null,
          performanceTrackingPack: contract.quick_brief.performance_tracking_pack
            ? {
                return1Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.return_1y)
                  ?? contract.quick_brief.performance_tracking_pack.return_1y
                  ?? null,
                return3Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.return_3y)
                  ?? contract.quick_brief.performance_tracking_pack.return_3y
                  ?? null,
                return5Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.return_5y)
                  ?? contract.quick_brief.performance_tracking_pack.return_5y
                  ?? null,
                benchmarkReturn1Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.benchmark_return_1y)
                  ?? contract.quick_brief.performance_tracking_pack.benchmark_return_1y
                  ?? null,
                benchmarkReturn3Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.benchmark_return_3y)
                  ?? contract.quick_brief.performance_tracking_pack.benchmark_return_3y
                  ?? null,
                benchmarkReturn5Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.benchmark_return_5y)
                  ?? contract.quick_brief.performance_tracking_pack.benchmark_return_5y
                  ?? null,
                trackingError1Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.tracking_error_1y)
                  ?? contract.quick_brief.performance_tracking_pack.tracking_error_1y
                  ?? null,
                trackingError3Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.tracking_error_3y)
                  ?? contract.quick_brief.performance_tracking_pack.tracking_error_3y
                  ?? null,
                trackingError5Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.tracking_error_5y)
                  ?? contract.quick_brief.performance_tracking_pack.tracking_error_5y
                  ?? null,
                trackingDifferenceCurrentPeriod:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.tracking_difference_current_period)
                  ?? contract.quick_brief.performance_tracking_pack.tracking_difference_current_period
                  ?? null,
                trackingDifference1Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.tracking_difference_1y)
                  ?? contract.quick_brief.performance_tracking_pack.tracking_difference_1y
                  ?? null,
                trackingDifference3Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.tracking_difference_3y)
                  ?? contract.quick_brief.performance_tracking_pack.tracking_difference_3y
                  ?? null,
                trackingDifference5Y:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.tracking_difference_5y)
                  ?? contract.quick_brief.performance_tracking_pack.tracking_difference_5y
                  ?? null,
                volatility:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.volatility)
                  ?? contract.quick_brief.performance_tracking_pack.volatility
                  ?? null,
                maxDrawdown:
                  cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.max_drawdown)
                  ?? contract.quick_brief.performance_tracking_pack.max_drawdown
                  ?? null,
                asOf: cleanBlueprintCopy(contract.quick_brief.performance_tracking_pack.as_of) ?? contract.quick_brief.performance_tracking_pack.as_of ?? null,
              }
            : null,
          compositionPack: contract.quick_brief.composition_pack
            ? {
                numberOfStocks:
                  cleanBlueprintCopy(contract.quick_brief.composition_pack.number_of_stocks)
                  ?? contract.quick_brief.composition_pack.number_of_stocks
                  ?? null,
                topHoldings: (contract.quick_brief.composition_pack.top_holdings ?? []).map((row) => ({
                  label: cleanBlueprintCopy(row.label) ?? row.label,
                  value: cleanBlueprintCopy(row.value) ?? row.value,
                })),
                countryWeights: (contract.quick_brief.composition_pack.country_weights ?? []).map((row) => ({
                  label: cleanBlueprintCopy(row.label) ?? row.label,
                  value: cleanBlueprintCopy(row.value) ?? row.value,
                })),
                sectorWeights: (contract.quick_brief.composition_pack.sector_weights ?? []).map((row) => ({
                  label: cleanBlueprintCopy(row.label) ?? row.label,
                  value: cleanBlueprintCopy(row.value) ?? row.value,
                })),
                top10Weight:
                  cleanBlueprintCopy(contract.quick_brief.composition_pack.top_10_weight)
                  ?? contract.quick_brief.composition_pack.top_10_weight
                  ?? null,
                usWeight:
                  cleanBlueprintCopy(contract.quick_brief.composition_pack.us_weight)
                  ?? contract.quick_brief.composition_pack.us_weight
                  ?? null,
                nonUsWeight:
                  cleanBlueprintCopy(contract.quick_brief.composition_pack.non_us_weight)
                  ?? contract.quick_brief.composition_pack.non_us_weight
                  ?? null,
                emWeight:
                  cleanBlueprintCopy(contract.quick_brief.composition_pack.em_weight)
                  ?? contract.quick_brief.composition_pack.em_weight
                  ?? null,
              }
            : null,
          documentCoverage: contract.quick_brief.document_coverage
            ? {
                factsheetPresent: contract.quick_brief.document_coverage.factsheet_present ?? null,
                kidPresent: contract.quick_brief.document_coverage.kid_present ?? null,
                prospectusPresent: contract.quick_brief.document_coverage.prospectus_present ?? null,
                annualReportPresent: contract.quick_brief.document_coverage.annual_report_present ?? null,
                benchmarkMethodologyPresent: contract.quick_brief.document_coverage.benchmark_methodology_present ?? null,
                lastRefreshedAt:
                  cleanBlueprintCopy(contract.quick_brief.document_coverage.last_refreshed_at)
                  ?? contract.quick_brief.document_coverage.last_refreshed_at
                  ?? null,
                documentCount: contract.quick_brief.document_coverage.document_count ?? null,
                missingDocuments: (contract.quick_brief.document_coverage.missing_documents ?? []).map((value) => cleanBlueprintCopy(value) ?? value),
                documentConfidenceGrade:
                  cleanBlueprintCopy(contract.quick_brief.document_coverage.document_confidence_grade)
                  ?? contract.quick_brief.document_coverage.document_confidence_grade
                  ?? null,
              }
            : null,
          whyItMatters: (contract.quick_brief.why_it_matters ?? []).map((row) => ({
            label: cleanBlueprintCopy(row.label) ?? row.label,
            value: cleanBlueprintCopy(row.value) ?? row.value,
          })),
          performanceAndImplementation: (contract.quick_brief.performance_and_implementation ?? []).map((row) => ({
            label: cleanBlueprintCopy(row.label) ?? row.label,
            value: cleanBlueprintCopy(row.value) ?? row.value,
          })),
          overlayNote: cleanBlueprintCopy(contract.quick_brief.overlay_note) ?? contract.quick_brief.overlay_note ?? null,
          backdropNote: cleanBlueprintCopy(contract.quick_brief.backdrop_note) ?? contract.quick_brief.backdrop_note ?? null,
          evidenceFooter: (contract.quick_brief.evidence_footer ?? []).map((row) => ({
            label: cleanBlueprintCopy(row.label) ?? row.label,
            value: cleanBlueprintCopy(row.value) ?? row.value,
          })),
        }
      : null,
    marketHistorySummary: appendDetail(
      cleanBlueprintCopy(contract.market_history_block?.summary) ?? contract.market_history_block?.summary ?? null,
      contract.market_history_block?.benchmark_note ?? (!marketPathSupport ? reportForecastMeta : null)
    ),
    marketHistoryCharts: adaptChartPanels(contract.market_history_charts),
    marketHistoryWindows: (contract.market_history_block?.regime_windows ?? []).map((window) => ({
      label: window.label,
      period: window.period,
      fundReturn: window.fund_return,
      benchmarkReturn: window.benchmark_return,
      note: window.note,
    })),
    scenarioBlocks: (contract.scenario_blocks ?? []).map((block) => ({
      label: block.label,
      trigger: appendDetail(
        cleanBlueprintCopy(block.trigger) ?? block.trigger,
        block.what_confirms ? `Confirms: ${cleanBlueprintCopy(block.what_confirms) ?? block.what_confirms}` : block.what_breaks ? `Breaks: ${cleanBlueprintCopy(block.what_breaks) ?? block.what_breaks}` : null
      ) ?? "",
      expectedReturn: appendDetail(cleanBlueprintCopy(block.expected_return) ?? block.expected_return, forecastMeta(block.forecast_support)) ?? "",
      portfolioEffect: appendDetail(cleanBlueprintCopy(block.portfolio_effect) ?? block.portfolio_effect, block.degraded_state ? `Degraded: ${humanizeState(block.degraded_state)}` : null) ?? "",
      shortTerm: block.short_term,
      longTerm: block.long_term,
    })),
    scenarioCharts: adaptChartPanels(contract.scenario_charts),
    riskBlocks: (contract.risk_blocks ?? []).map((block) => ({
      category: block.category,
      title: block.title,
      detail: cleanBlueprintCopy(block.detail) ?? block.detail,
    })).concat(
      dataQualitySummary
        ? [
            {
              category: "data_quality",
              title: "Data quality",
              detail: cleanBlueprintCopy(dataQualitySummary.summary) ?? dataQualitySummary.summary,
            },
          ]
        : []
    ),
    competitionBlocks: (contract.competition_blocks ?? []).map((block) => ({
      label: block.label,
      summary: cleanBlueprintCopy(block.summary) ?? block.summary,
      verdict: cleanBlueprintCopy(block.verdict) ?? block.verdict,
    })),
    competitionCharts: adaptChartPanels(contract.competition_charts),
    evidenceSources: (contract.evidence_sources ?? []).map((source) => ({
      label: source.label,
      freshness: appendDetail(freshnessLabel(source.freshness_state), truthEnvelopeMeta(source.truth_envelope)) ?? "",
      directness: humanizeState(source.directness),
      url: source.url,
    })),
    implementationProfile: implementationProfileRows,
    fieldIssues,
    sourceAuthorityFields,
    primaryDocuments,
    researchSupport: adaptResearchSupport(contract.research_support),
    marketPathSupport,
    marketPath,
    decisionThresholds: (contract.decision_thresholds ?? []).map((threshold) => ({
      label: threshold.label,
      value: appendDetail(
        cleanBlueprintCopy(threshold.value) ?? threshold.value,
        compactParts([
          threshold.trigger_type ? titleFromCode(threshold.trigger_type) : null,
          threshold.threshold_state ? `State ${humanizeState(threshold.threshold_state)}` : null,
          forecastMeta(threshold.forecast_support),
        ])
      ) ?? "",
    })),
  };
  return display;
}

export function adaptBlueprint(
  blueprint: BlueprintExplorerContract,
  compare: CompareContract | null,
  changes: ChangesContract | null,
  report: CandidateReportContract | null,
  selectedCandidateId: string | null,
  activeSleeveId: string | null = null,
): BlueprintDisplay {
  const sleeves: BlueprintSleeveDisplay[] = blueprint.sleeves.map((sleeve, index) => {
    const candidateDisplays = sleeve.candidates.map((candidate) => {
      const fieldIssues = mapFieldIssues(candidate.reconciliation_report);
      const marketPathSupport = candidate.market_path_support ?? null;
      const marketPath = describeMarketPathSupport(marketPathSupport);
      const sourceCompletionSummary = candidate.source_completion_summary ?? null;
      const sourceCompletionReady = String(sourceCompletionSummary?.state ?? "").trim().toLowerCase() === "complete";
      const rawFailureSummary = adaptFailureSummary(candidate.failure_class_summary);
      const failureSummary = sourceCompletionReady && isSourceFailureSummary(rawFailureSummary) ? null : rawFailureSummary;
      const scoreComponents = (candidate.score_decomposition?.components ?? []).map((component) => ({
        id: component.component_id ?? component.key ?? undefined,
        label: component.label,
        score: component.score,
        band: component.band ?? null,
        confidence: typeof component.confidence === "number" ? component.confidence : null,
        tone: (component.tone as Tone) ?? "neutral",
        summary: cleanBlueprintCopy(component.summary) ?? component.summary,
        reasons: (component.reasons ?? []).map((reason) => cleanBlueprintCopy(reason) ?? reason),
        capsApplied: (component.caps_applied ?? []).map((item) => cleanBlueprintCopy(item) ?? item),
        fieldDrivers: (component.field_drivers ?? []).map((item) => humanizeState(item)),
      }));
      const authorityMix = Object.entries(candidate.source_integrity_summary?.authority_mix ?? {})
        .filter(([, count]) => Number(count) > 0)
        .map(([label, count]) => ({ label: humanizeState(label), count: Number(count) }));
      const integrityIssueCounts = Object.entries(candidate.source_integrity_summary?.issue_counts ?? {})
        .filter(([label, count]) => label !== "review_items" && Number(count) > 0)
        .map(([label, count]) => ({ label: humanizeState(label), count: Number(count) }));
      const candidateDisplay: CandidateCardDisplay = {
        id: candidate.candidate_id,
        symbol: candidate.symbol,
        name: candidate.name,
        score: candidate.score
          ?? candidate.score_decomposition?.total_score
          ?? legacyScoreFromLabels(
            candidate.instrument_quality,
            candidate.portfolio_fit_now,
            candidate.visible_decision_state.state
          ),
        decisionStateRaw: candidate.visible_decision_state.state ?? null,
        investorStateRaw: candidate.investor_decision_state ?? null,
        gateStateRaw: candidate.gate_state ?? candidate.recommendation_gate?.gate_state ?? null,
        decisionState: presentBlueprintDecisionState(candidate.visible_decision_state.state),
        decisionTone: stateTone(candidate.visible_decision_state.state),
        decisionSummary:
          cleanBlueprintCopy(candidate.visible_decision_state.rationale)
          ?? failureSummary?.summary
          ?? cleanBlueprintCopy(candidate.source_integrity_summary?.summary)
          ?? cleanBlueprintCopy(candidate.identity_state?.summary)
          ?? cleanBlueprintCopy(candidate.recommendation_gate?.summary)
          ?? candidate.visible_decision_state.rationale
          ?? candidate.source_integrity_summary?.summary
          ?? candidate.identity_state?.summary
          ?? candidate.recommendation_gate?.summary
          ?? candidate.visible_decision_state.rationale,
        failureSummary,
        blockerCategory: candidate.blocker_category ? humanizeState(candidate.blocker_category) : null,
        benchmarkFullName: candidate.benchmark_full_name ?? null,
        exposureSummary: candidate.exposure_summary ?? null,
        terBps: typeof candidate.ter_bps === "number" ? formatBps(candidate.ter_bps) : null,
        spreadProxyBps: typeof candidate.spread_proxy_bps === "number" ? formatBps(candidate.spread_proxy_bps) : null,
        aumUsd: typeof candidate.aum_usd === "number" ? formatCurrency(candidate.aum_usd) : null,
        aumState: candidate.aum_state ? humanizeState(candidate.aum_state) : null,
        taxPostureSummary: candidate.sg_tax_posture?.summary ?? null,
        distributionPolicy: candidate.distribution_policy ?? null,
        replicationRiskNote: candidate.replication_risk_note ?? null,
        currentWeight: typeof candidate.current_weight_pct === "number" ? formatPercent(candidate.current_weight_pct) : null,
        weightState: candidate.weight_state ? humanizeState(candidate.weight_state) : null,
        sourceIntegritySummary: candidate.source_integrity_summary
          ? {
              state: sourceCompletionReady
                ? "Source complete"
                : presentSourceCoverageLabel(candidate.source_integrity_summary.state),
              stateTone: sourceCompletionReady
                ? ("good" as Tone)
                : confidenceTone(candidate.source_integrity_summary.state === "strong" ? "high" : candidate.source_integrity_summary.state === "mixed" ? "mixed" : "low"),
              integrityLabel: sourceCompletionReady
                ? "Source integrity clean"
                : candidate.source_integrity_summary.integrity_label
                  ? presentSourceCoverageLabel(candidate.source_integrity_summary.integrity_label)
                  : null,
              summary: sourceCompletionReady
                ? cleanBlueprintCopy(sourceCompletionSummary?.summary) ?? sourceCompletionSummary?.summary ?? cleanBlueprintCopy(candidate.source_integrity_summary.summary) ?? candidate.source_integrity_summary.summary
                : cleanBlueprintCopy(candidate.source_integrity_summary.summary) ?? candidate.source_integrity_summary.summary,
              criticalReady: sourceCompletionReady
                ? Number(sourceCompletionSummary?.critical_fields_completed ?? candidate.source_integrity_summary.critical_fields_ready)
                : candidate.source_integrity_summary.critical_fields_ready,
              criticalTotal: sourceCompletionReady
                ? Number(sourceCompletionSummary?.critical_fields_total ?? candidate.source_integrity_summary.critical_fields_total)
                : candidate.source_integrity_summary.critical_fields_total,
              authorityMix,
              issueCounts: sourceCompletionReady ? [] : integrityIssueCounts,
              hardConflictFields: sourceCompletionReady ? [] : (candidate.source_integrity_summary.hard_conflict_fields ?? []).map((field) => humanizeState(field)),
              missingCriticalFields: sourceCompletionReady
                ? (sourceCompletionSummary?.incomplete_fields ?? []).map((field) => humanizeState(field))
                : (candidate.source_integrity_summary.missing_critical_fields ?? []).map((field) => humanizeState(field)),
              weakestFields: sourceCompletionReady ? [] : (candidate.source_integrity_summary.weakest_fields ?? []).map((field) => humanizeState(field)),
            }
          : null,
        sourceCompletionSummary: sourceCompletionSummary
          ? {
              state: humanizeState(sourceCompletionSummary.state),
              summary: cleanBlueprintCopy(sourceCompletionSummary.summary) ?? sourceCompletionSummary.summary,
              criticalCompleted: Number(sourceCompletionSummary.critical_fields_completed ?? 0),
              criticalTotal: Number(sourceCompletionSummary.critical_fields_total ?? 0),
              equivalentReadyCount: Number(sourceCompletionSummary.equivalent_ready_count ?? 0),
              incompleteFields: (sourceCompletionSummary.incomplete_fields ?? []).map((field) => humanizeState(field)),
              weakFields: (sourceCompletionSummary.weak_fields ?? []).map((field) => humanizeState(field)),
              staleFields: (sourceCompletionSummary.stale_fields ?? []).map((field) => humanizeState(field)),
              conflictFields: (sourceCompletionSummary.conflict_fields ?? []).map((field) => humanizeState(field)),
              authorityClean: Boolean(sourceCompletionSummary.authority_clean),
              freshnessClean: Boolean(sourceCompletionSummary.freshness_clean),
              conflictClean: Boolean(sourceCompletionSummary.conflict_clean),
              completenessClean: Boolean(sourceCompletionSummary.completeness_clean),
              completionReasons: (sourceCompletionSummary.completion_reasons ?? []).map((reason) => cleanBlueprintCopy(reason) ?? reason),
            }
          : null,
        identitySummary: cleanBlueprintCopy(candidate.identity_state?.summary) ?? candidate.identity_state?.summary ?? null,
        scoreBreakdown: candidate.score_decomposition
          ? {
              total: candidate.score_decomposition.total_score,
              recommendation: candidate.score_decomposition.recommendation_score ?? candidate.score_decomposition.total_score,
              recommendationMerit: candidate.score_decomposition.recommendation_merit_score ?? candidate.score_decomposition.investment_merit_score ?? candidate.score_decomposition.optimality_score ?? null,
              investmentMerit: candidate.score_decomposition.investment_merit_score ?? candidate.score_decomposition.optimality_score ?? null,
              deployability: candidate.score_decomposition.deployability_score ?? candidate.score_decomposition.deployment_score ?? candidate.score_decomposition.readiness_score ?? null,
              truthConfidence: candidate.score_decomposition.truth_confidence_score ?? null,
              truthConfidenceBand: candidate.score_decomposition.truth_confidence_band ?? null,
              truthConfidenceSummary: cleanBlueprintCopy(candidate.score_decomposition.truth_confidence_summary) ?? candidate.score_decomposition.truth_confidence_summary ?? null,
              deployment: candidate.score_decomposition.deployment_score ?? candidate.score_decomposition.total_score,
              admissibility: candidate.score_decomposition.admissibility_score ?? null,
              admissibilityIdentity: candidate.score_decomposition.admissibility_identity_score ?? null,
              implementation: candidate.score_decomposition.implementation_score ?? null,
              sourceIntegrity: candidate.score_decomposition.source_integrity_score ?? null,
              evidence: candidate.score_decomposition.evidence_score ?? null,
              sleeveFit: candidate.score_decomposition.sleeve_fit_score ?? null,
              identity: candidate.score_decomposition.identity_score ?? null,
              benchmarkFidelity: candidate.score_decomposition.benchmark_fidelity_score ?? null,
              marketPathSupport: candidate.score_decomposition.market_path_support_score ?? null,
              longHorizonQuality: candidate.score_decomposition.long_horizon_quality_score ?? null,
              instrumentQuality: candidate.score_decomposition.instrument_quality_score ?? null,
              portfolioFit: candidate.score_decomposition.portfolio_fit_score ?? null,
              optimality: candidate.score_decomposition.optimality_score ?? null,
              readiness: candidate.score_decomposition.readiness_score ?? null,
              confidencePenalty: candidate.score_decomposition.confidence_penalty ?? null,
              readinessPosture: candidate.score_decomposition.readiness_posture ?? null,
              readinessSummary: cleanBlueprintCopy(candidate.score_decomposition.readiness_summary) ?? candidate.score_decomposition.readiness_summary ?? null,
              deployabilityBadge: candidate.score_decomposition.deployability_badge ?? null,
              summary: cleanBlueprintCopy(candidate.score_decomposition.summary) ?? candidate.score_decomposition.summary ?? null,
            }
          : null,
        scoreSummary: adaptScoreSummary(candidate.score_summary),
        scoreComponents,
        issuer: candidate.issuer ?? null,
        expenseRatio: text(candidate.expense_ratio, "—"),
        aum: text(candidate.aum, "—"),
        freshness:
          typeof candidate.freshness_days === "number"
            ? `${candidate.freshness_days}d`
            : null,
        instrumentQuality: candidate.instrument_quality,
        portfolioFit: candidate.portfolio_fit_now,
        capitalPriority: candidate.capital_priority_now,
        statusLabel: presentBlueprintCandidateStatus(candidate.status_label),
        statusTone: stateTone(candidate.visible_decision_state.state),
        whyNow: appendDetail(cleanBlueprintCopy(candidate.why_now) ?? candidate.why_now, cleanBlueprintCopy(candidate.winner_reason) ?? candidate.winner_reason ?? null) ?? "",
        whatBlocksAction: failureSummary?.summary ?? cleanBlueprintCopy(candidate.what_blocks_action) ?? candidate.what_blocks_action,
        whatChangesView: appendDetail(cleanBlueprintCopy(candidate.what_changes_view) ?? candidate.what_changes_view, cleanBlueprintCopy(candidate.flip_risk_note) ?? candidate.flip_risk_note ?? null) ?? "",
        actionBoundary: cleanBlueprintCopy(candidate.action_boundary) ?? candidate.action_boundary ?? null,
        fundingSource: candidate.funding_source ?? null,
        implicationSummary: cleanBlueprintCopy(candidate.implication_summary) ?? candidate.implication_summary,
        recommendationGate: candidate.recommendation_gate
          ? {
              state: presentRecommendationGateState(candidate.recommendation_gate.gate_state),
              stateTone: recommendationGateTone(candidate.recommendation_gate.gate_state),
              summary: cleanBlueprintCopy(candidate.recommendation_gate.summary) ?? candidate.recommendation_gate.summary,
              criticalMissing: humanizeCriticalMissing(
                candidate.recommendation_gate.critical_missing_fields,
                candidate.reconciliation_report,
              ),
              blockedReasons: candidate.recommendation_gate.blocked_reasons.map((reason) => cleanBlueprintCopy(reason) ?? reason),
            }
          : null,
        marketSupportBasis:
          marketPath?.providerLabel
          ?? runtimeDetail(candidate.candidate_market_provenance)
          ?? candidate.coverage_workflow_summary?.current_history_provider
          ?? null,
        coverageStatusRaw: candidate.coverage_workflow_summary?.status ?? candidate.coverage_status ?? null,
        coverageStatus: (candidate.coverage_workflow_summary?.status ?? candidate.coverage_status)
          ? humanizeState(candidate.coverage_workflow_summary?.status ?? candidate.coverage_status)
          : null,
        coverageSummary:
          cleanBlueprintCopy(candidate.coverage_workflow_summary?.summary)
          ?? candidate.coverage_workflow_summary?.summary
          ?? null,
        fieldIssues,
        dataQualitySummary: candidate.data_quality_summary
          ? {
              confidence: humanizeState(candidate.data_quality_summary.data_confidence),
              confidenceTone: confidenceTone(candidate.data_quality_summary.data_confidence),
              criticalReady: candidate.data_quality_summary.critical_fields_ready,
              criticalTotal: candidate.data_quality_summary.critical_fields_total,
              summary: cleanBlueprintCopy(candidate.data_quality_summary.summary) ?? candidate.data_quality_summary.summary,
            }
          : null,
        scenarioReadinessNote: cleanBlueprintCopy(candidate.scenario_readiness_note) ?? candidate.scenario_readiness_note ?? null,
        implementationSummary: cleanBlueprintCopy(candidate.implementation_profile?.summary) ?? candidate.implementation_profile?.summary ?? null,
        detailCharts: adaptChartPanels(candidate.detail_chart_panels),
        marketPathSupport,
        marketPath,
        quickBrief: adaptQuickBrief(candidate.quick_brief_snapshot),
      };
      return candidateDisplay;
    });
    const sleevePosture = sleeve.sleeve_actionability_state
      ? {
          label: presentSleeveActionabilityState(sleeve.sleeve_actionability_state),
          tone:
            sleeve.sleeve_actionability_state === "ready"
              ? ("good" as Tone)
              : sleeve.sleeve_actionability_state === "reviewable"
                ? ("info" as Tone)
                : sleeve.sleeve_actionability_state === "bounded"
                  ? ("warn" as Tone)
                  : ("bad" as Tone),
          summary: cleanBlueprintCopy(sleeve.sleeve_block_reason_summary) ?? sleeve.sleeve_block_reason_summary ?? "Sleeve posture remains bounded.",
          detail: sleeve.leader_is_blocked_but_sleeve_still_reviewable
            ? "The current leader is blocked, but reviewable alternatives keep the sleeve active."
            : cleanBlueprintCopy(sleeve.main_limit) ?? sleeve.main_limit ?? "Sleeve posture remains bounded.",
          blockLabel: sleeve.sleeve_actionability_state === "blocked" ? "What blocks action" : "What still needs cleanup",
          reopenLabel: sleeve.sleeve_actionability_state === "blocked" ? "What must clear first" : "What would strengthen the sleeve",
          actionableCandidateCount: Number(sleeve.ready_count ?? 0),
          reviewableCandidateCount: Number(sleeve.reviewable_count ?? 0),
          activeSupportCandidateCount: Number(sleeve.active_support_candidate_count ?? 0),
          blockedCandidateCount: Number(sleeve.blocked_count ?? 0),
          leaderBlockedButReviewable: Boolean(sleeve.leader_is_blocked_but_sleeve_still_reviewable),
        }
      : deriveSleevePosture(candidateDisplays);
    return {
      id: sleeve.sleeve_id,
      name: sleeve.sleeve_name ?? titleFromCode(sleeve.sleeve_id),
      purpose: cleanBlueprintCopy(sleeve.sleeve_purpose) ?? sleeve.sleeve_purpose,
      rank: sleeve.priority_rank ?? index + 1,
      targetLabel: sleeve.target_label,
      rangeLabel: sleeve.range_label,
      isNested: sleeve.is_nested,
      parentSleeveId: sleeve.parent_sleeve_id ?? null,
      parentSleeveName: sleeve.parent_sleeve_name ?? null,
      countsAsTopLevelTotal: sleeve.counts_as_top_level_total,
      sleeveRoleStatement: cleanBlueprintCopy(sleeve.sleeve_role_statement) ?? sleeve.sleeve_role_statement ?? null,
      cycleSensitivity: cleanBlueprintCopy(sleeve.cycle_sensitivity) ?? sleeve.cycle_sensitivity ?? null,
      baseAllocationRationale: cleanBlueprintCopy(sleeve.base_allocation_rationale) ?? sleeve.base_allocation_rationale ?? null,
      priorityRank: sleeve.priority_rank ?? index + 1,
      currentWeight: sleeve.current_weight ?? null,
      targetWeight: sleeve.target_label ?? sleeve.target_weight ?? null,
      candidateCount: sleeve.candidate_count ?? sleeve.candidates.length,
      statusLabel: sleevePosture.label,
      statusTone: sleevePosture.tone as Tone,
      postureSummary: sleevePosture.summary,
      postureDetail: sleevePosture.detail,
      blockLabel: sleevePosture.blockLabel,
      reopenLabel: sleevePosture.reopenLabel,
      actionableCandidateCount: sleevePosture.actionableCandidateCount,
      reviewableCandidateCount: sleevePosture.reviewableCandidateCount,
      activeSupportCandidateCount: sleevePosture.activeSupportCandidateCount,
      blockedCandidateCount: sleevePosture.blockedCandidateCount,
      leaderBlockedButReviewable: sleevePosture.leaderBlockedButReviewable,
      capitalMemo: cleanBlueprintCopy(sleeve.capital_memo) ?? sleeve.capital_memo,
      implicationSummary: cleanBlueprintCopy(sleeve.implication_summary) ?? sleeve.implication_summary,
      whyItLeads: cleanBlueprintCopy(sleeve.why_it_leads) ?? sleeve.why_it_leads,
      mainLimit: cleanBlueprintCopy(sleeve.main_limit) ?? sleeve.main_limit,
      recommendationScore: sleeve.recommendation_score
        ? {
            averageScore: Number(sleeve.recommendation_score.average_score ?? 0),
            pillarCountUsed: Number(sleeve.recommendation_score.pillar_count_used ?? 0),
            factorCountUsed: Number(sleeve.recommendation_score.factor_count_used ?? sleeve.recommendation_score.pillar_count_used ?? 0),
            scoreBasis: (sleeve.recommendation_score.score_basis ?? "support_pillars_average") as "support_pillars_average" | "deployment_score" | "recommendation_score",
            leaderCandidateRecommendationScore:
              typeof sleeve.recommendation_score.leader_candidate_recommendation_score === "number"
                ? sleeve.recommendation_score.leader_candidate_recommendation_score
                : null,
            leaderTruthConfidenceScore:
              typeof sleeve.recommendation_score.leader_truth_confidence_score === "number"
                ? sleeve.recommendation_score.leader_truth_confidence_score
                : null,
            leaderCandidateDeployabilityScore:
              typeof sleeve.recommendation_score.leader_candidate_deployability_score === "number"
                ? sleeve.recommendation_score.leader_candidate_deployability_score
                : null,
            leaderCandidateInvestmentMeritScore:
              typeof sleeve.recommendation_score.leader_candidate_investment_merit_score === "number"
                ? sleeve.recommendation_score.leader_candidate_investment_merit_score
                : null,
            leaderCandidateDeploymentScore:
              typeof sleeve.recommendation_score.leader_candidate_deployment_score === "number"
                ? sleeve.recommendation_score.leader_candidate_deployment_score
                : null,
            depthScore:
              typeof sleeve.recommendation_score.depth_score === "number"
                ? sleeve.recommendation_score.depth_score
                : null,
            sleeveActionabilityScore:
              typeof sleeve.recommendation_score.sleeve_actionability_score === "number"
                ? sleeve.recommendation_score.sleeve_actionability_score
                : null,
            blockerBurdenScore:
              typeof sleeve.recommendation_score.blocker_burden_score === "number"
                ? sleeve.recommendation_score.blocker_burden_score
                : null,
            tone: (sleeve.recommendation_score.tone ?? "neutral") as Tone,
            label: sleeve.recommendation_score.label ?? "Unavailable",
          }
        : null,
      reopenCondition: cleanBlueprintCopy(sleeve.reopen_condition) ?? sleeve.reopen_condition,
      fundingPath: sleeve.funding_path
        ? {
            fundingSource: sleeve.funding_path.funding_source,
            incumbentLabel: sleeve.funding_path.incumbent_label,
            actionBoundary: cleanBlueprintCopy(sleeve.funding_path.action_boundary) ?? sleeve.funding_path.action_boundary,
            summary: cleanBlueprintCopy(sleeve.funding_path.summary) ?? sleeve.funding_path.summary ?? null,
          }
        : null,
      forecastWatch: sleeve.forecast_watch
        ? appendDetail(sleeve.forecast_watch.support_strength, forecastMeta(sleeve.forecast_watch))
        : null,
      leadCandidateName: sleeve.lead_candidate_name,
      sleeveStateRaw: sleeve.sleeve_actionability_state ?? null,
      candidates: candidateDisplays,
      supportPillars: sleeve.support_pillars?.length
      ? sleeve.support_pillars.map((pillar) => ({
          label: pillar.label,
          score: pillar.score,
          note: pillar.note,
          tone: pillar.tone,
        }))
      : [],
    };
  });

  const allCandidates = sleeves.flatMap((sleeve) => sleeve.candidates);
  const selectedCandidate = allCandidates.find((candidate) => candidate.id === selectedCandidateId) ?? allCandidates[0] ?? null;
  const preferredSleeve = activeSleeveId ? sleeves.find((sleeve) => sleeve.id === activeSleeveId) ?? null : null;
  const selectedCandidateSleeves = selectedCandidate
    ? sleeves.filter((sleeve) => sleeve.candidates.some((candidate) => candidate.id === selectedCandidate.id))
    : [];
  const selectedSleeve =
    (preferredSleeve && selectedCandidate && preferredSleeve.candidates.some((candidate) => candidate.id === selectedCandidate.id)
      ? preferredSleeve
      : null)
    ?? selectedCandidateSleeves[0]
    ?? preferredSleeve
    ?? sleeves[0]
    ?? null;
  const candidateSleeveMap = new Map<string, string>();
  const candidateNameMap = new Map<string, string>();
  for (const sleeve of sleeves) {
    for (const candidate of sleeve.candidates) {
      candidateSleeveMap.set(candidate.id, sleeve.name);
      candidateNameMap.set(candidate.id, candidate.name);
    }
  }

  const compareCandidateNames = new Map(
    (compare?.candidates ?? []).map((candidate) => [candidate.candidate_id, candidate.name]),
  );
  const compareDimensions = (compare?.compare_dimensions ?? compare?.dimensions ?? [])
    .filter((dimension) => (dimension.discriminating ?? true) && hasMeaningfulCompareSpread(dimension))
    .sort((left, right) => {
      const bucketDelta = compareDimensionBucketOrder(left) - compareDimensionBucketOrder(right);
      if (bucketDelta !== 0) return bucketDelta;
      const importanceDelta = compareImportanceOrder(left.importance) - compareImportanceOrder(right.importance);
      if (importanceDelta !== 0) return importanceDelta;
      return String(left.label ?? left.dimension ?? "").localeCompare(String(right.label ?? right.dimension ?? ""));
    });
  const compareDecision = compare?.compare_decision ?? null;
  const compareRoleLabel = (role: unknown): string | null => {
    const raw = String(role ?? "").trim();
    if (!raw) return null;
    if (raw === "candidate_a") {
      return compare?.candidate_a_name ?? compare?.candidates?.[0]?.symbol ?? "Candidate A";
    }
    if (raw === "candidate_b") {
      return compare?.candidate_b_name ?? compare?.candidates?.[1]?.symbol ?? "Candidate B";
    }
    if (raw === "tie") return "Tie";
    if (raw === "depends") return "Depends";
    if (raw === "not_applicable") return "Not applicable";
    if (raw === "no_clear_winner") return "No clear winner";
    return humanizeState(raw);
  };
  const numericOrNull = (value: unknown): number | null => (
    typeof value === "number" && Number.isFinite(value) ? value : null
  );
  const adaptPortfolioConsequence = (value: any) => value
    ? {
        candidateId: String(value.candidate_id ?? ""),
        symbol: String(value.symbol ?? ""),
        portfolioEffect: text(value.portfolio_effect, "Portfolio consequence unavailable."),
        concentrationEffect: text(value.concentration_effect, "Concentration effect unavailable."),
        regionExposureEffect: text(value.region_exposure_effect, "Region exposure effect unavailable."),
        currencyOrTradingLineEffect: text(value.currency_or_trading_line_effect, "Trading line effect unavailable."),
        overlapEffect: text(value.overlap_effect, "Overlap effect unavailable."),
        sleeveMandateEffect: text(value.sleeve_mandate_effect, "Sleeve mandate effect unavailable."),
        diversificationEffect: text(value.diversification_effect, "Diversification effect unavailable."),
        fundingPathEffect: text(value.funding_path_effect, "Funding path effect unavailable."),
        targetAllocationDriftEffect: text(value.target_allocation_drift_effect, "Target drift effect unavailable."),
        confidence: humanizeState(value.confidence ?? "low"),
      }
    : null;
  const compareDisplay: CompareDisplay | null = compare
    ? {
        readinessState: compare.compare_readiness_state ?? null,
        readinessTone:
          compare.compare_readiness_state === "ready"
            ? "good"
            : compare.compare_readiness_state === "cross_sleeve"
              ? "warn"
              : "bad",
        readinessNote: compare.compare_readiness_note ?? null,
        substitutionVerdict: compare.substitution_verdict ? humanizeState(compare.substitution_verdict) : null,
        substitutionTone:
          compare.substitution_verdict === "direct_substitutes"
            ? "good"
            : compare.substitution_verdict === "partial_substitutes"
              ? "warn"
              : compare.substitution_verdict === "different_jobs"
                ? "info"
                : "bad",
        substitutionRationale: compare.substitution_rationale ?? null,
        winnerName: compare.winner_name,
        whyLeads: compare.why_leads,
        whatWouldChange: appendDetail(
          text(compare.what_would_change_comparison, "No explicit change trigger emitted."),
          compactParts([compare.flip_risk_note ?? null, forecastMeta(compare.forecast_support)])
        ) ?? "",
        compareSummary: {
          cleanerForSleeveJob:
            cleanBlueprintCopy(compare.compare_summary?.cleaner_for_sleeve_job)
            ?? cleanBlueprintCopy(compare.winner_for_sleeve_job)
            ?? compare.winner_for_sleeve_job
            ?? null,
          mainSeparation:
            cleanBlueprintCopy(compare.compare_summary?.main_separation)
            ?? cleanBlueprintCopy(compare.why_leads)
            ?? compare.why_leads
            ?? null,
          changeTrigger:
            cleanBlueprintCopy(compare.compare_summary?.change_trigger)
            ?? cleanBlueprintCopy(compare.what_would_change_comparison)
            ?? compare.what_would_change_comparison
            ?? null,
        },
        candidates: (compare.candidates ?? []).map((candidate) => ({
          id: candidate.candidate_id,
          symbol: candidate.symbol,
          name: candidate.name,
          decisionState: candidate.investor_decision_state ? humanizeState(candidate.investor_decision_state) : null,
          decisionTone: stateTone(candidate.investor_decision_state),
          blockerCategory: candidate.blocker_category ? humanizeState(candidate.blocker_category) : null,
          benchmark: candidate.benchmark_full_name ?? null,
          totalScore:
            typeof candidate.total_score === "number"
              ? String(candidate.total_score)
              : "Unavailable",
          recommendationScore: numericOrNull(candidate.recommendation_score ?? candidate.total_score),
          investmentMeritScore: numericOrNull(candidate.investment_merit_score),
          deployabilityScore: numericOrNull(candidate.deployability_score),
          truthConfidenceScore: numericOrNull(candidate.truth_confidence_score),
          aumUsd: numericOrNull(candidate.aum_usd),
          domicile:
            cleanBlueprintCopy(candidate.domicile)
            ?? candidate.domicile
            ?? null,
          tradingCurrency:
            cleanBlueprintCopy(candidate.primary_trading_currency)
            ?? candidate.primary_trading_currency
            ?? null,
          listingExchange:
            cleanBlueprintCopy(candidate.primary_listing_exchange)
            ?? candidate.primary_listing_exchange
            ?? null,
          distributionPolicy:
            cleanBlueprintCopy(candidate.distribution_policy)
            ?? candidate.distribution_policy
            ?? null,
          replicationMethod:
            cleanBlueprintCopy(candidate.replication_method)
            ?? candidate.replication_method
            ?? null,
          currentWeight:
            typeof candidate.current_weight_pct === "number"
              ? formatPercent(candidate.current_weight_pct)
              : null,
          weightState: candidate.weight_state ? humanizeState(candidate.weight_state) : null,
          decisionSummary: candidate.decision_summary ?? null,
          exposureSummary:
            cleanBlueprintCopy(candidate.compare_card?.identity?.exposure_summary)
            ?? cleanBlueprintCopy(candidate.exposure_summary)
            ?? candidate.exposure_summary
            ?? null,
          compactTags: (candidate.compare_card?.identity?.compact_tags ?? [])
            .map((value) => cleanBlueprintCopy(value) ?? value)
            .filter((value): value is string => Boolean(value && value.trim())),
          verdictLabel:
            cleanBlueprintCopy(candidate.compare_card?.verdict?.primary_state)
            ?? (candidate.investor_decision_state ? humanizeState(candidate.investor_decision_state) : null),
          verdictTone: stateTone(
            String(candidate.compare_card?.verdict?.primary_state ?? candidate.investor_decision_state ?? "")
              .trim()
              .toLowerCase()
              .replace(/\s+/g, "_")
          ),
          verdictReason:
            cleanBlueprintCopy(candidate.compare_card?.verdict?.reason_line)
            ?? cleanBlueprintCopy(candidate.decision_summary)
            ?? candidate.decision_summary
            ?? null,
          sleeveFit: {
            roleFit:
              cleanBlueprintCopy(candidate.compare_card?.sleeve_fit?.role_fit)
              ?? candidate.compare_card?.sleeve_fit?.role_fit
              ?? null,
            benchmarkFit:
              cleanBlueprintCopy(candidate.compare_card?.sleeve_fit?.benchmark_fit)
              ?? candidate.compare_card?.sleeve_fit?.benchmark_fit
              ?? null,
            scopeFit:
              cleanBlueprintCopy(candidate.compare_card?.sleeve_fit?.scope_fit)
              ?? candidate.compare_card?.sleeve_fit?.scope_fit
              ?? null,
            thesis:
              cleanBlueprintCopy(candidate.compare_card?.sleeve_fit?.thesis)
              ?? candidate.compare_card?.sleeve_fit?.thesis
              ?? null,
          },
          implementationStats: (candidate.compare_card?.implementation?.stats ?? []).map((stat) => ({
            label: cleanBlueprintCopy(stat.label) ?? stat.label,
            value: cleanBlueprintCopy(stat.value) ?? stat.value,
          })),
          riskEvidence: {
            evidenceStatus:
              cleanBlueprintCopy(candidate.compare_card?.risk_evidence?.evidence_status)
              ?? candidate.compare_card?.risk_evidence?.evidence_status
              ?? null,
            timingStatus:
              cleanBlueprintCopy(candidate.compare_card?.risk_evidence?.timing_status)
              ?? candidate.compare_card?.risk_evidence?.timing_status
              ?? null,
            impactLine:
              cleanBlueprintCopy(candidate.compare_card?.risk_evidence?.impact_line)
              ?? candidate.compare_card?.risk_evidence?.impact_line
              ?? null,
          },
        })),
        dimensions: compareDimensions.map((dimension) => ({
          id: dimension.dimension_id ?? dimension.dimension,
          label: humanizeState(dimension.label ?? dimension.dimension),
          group: compareDimensionBucketLabel(dimension),
          rationale: dimension.rationale ?? null,
          importance: dimension.importance ? humanizeState(dimension.importance) : null,
          values: (dimension.values ?? []).map((value) => ({
            candidateId: value.candidate_id,
            candidateName: compareCandidateNames.get(value.candidate_id) ?? value.candidate_id,
            value: value.value,
          })),
          winnerLabel:
            dimension.winner === "tie"
              ? null
              : compareCandidateNames.get(String(dimension.winner)) ?? null,
          winnerTone: dimension.winner === "tie" ? "neutral" : "good",
        })),
        insufficientDimensions: (compare.insufficient_dimensions ?? []).map((value) => humanizeState(value)),
        decision: compareDecision
          ? {
              substitutionStatus: compareDecision.substitution_assessment?.status
                ? humanizeState(compareDecision.substitution_assessment.status)
                : null,
              substitutionSummary:
                cleanBlueprintCopy(compareDecision.substitution_assessment?.summary)
                ?? compareDecision.substitution_assessment?.summary
                ?? null,
              substitutionReason:
                cleanBlueprintCopy(compareDecision.substitution_assessment?.reason)
                ?? compareDecision.substitution_assessment?.reason
                ?? null,
              substitutionConfidence: compareDecision.substitution_assessment?.confidence
                ? humanizeState(compareDecision.substitution_assessment.confidence)
                : null,
              bestOverall: compareRoleLabel(compareDecision.winner_summary?.best_overall),
              investmentWinner: compareRoleLabel(compareDecision.winner_summary?.investment_winner),
              deploymentWinner: compareRoleLabel(compareDecision.winner_summary?.deployment_winner),
              evidenceWinner: compareRoleLabel(compareDecision.winner_summary?.evidence_winner),
              timingWinner: compareRoleLabel(compareDecision.winner_summary?.timing_winner),
              winnerSummary:
                cleanBlueprintCopy(compareDecision.winner_summary?.summary)
                ?? compareDecision.winner_summary?.summary
                ?? null,
              whereLoserWins:
                cleanBlueprintCopy(compareDecision.winner_summary?.where_loser_wins)
                ?? compareDecision.winner_summary?.where_loser_wins
                ?? null,
              decisionRule: {
                primaryRule:
                  cleanBlueprintCopy(compareDecision.decision_rule?.primary_rule)
                  ?? compareDecision.decision_rule?.primary_rule
                  ?? null,
                chooseCandidateAIf:
                  cleanBlueprintCopy(compareDecision.decision_rule?.choose_candidate_a_if)
                  ?? compareDecision.decision_rule?.choose_candidate_a_if
                  ?? null,
                chooseCandidateBIf:
                  cleanBlueprintCopy(compareDecision.decision_rule?.choose_candidate_b_if)
                  ?? compareDecision.decision_rule?.choose_candidate_b_if
                  ?? null,
                doNotTreatAsSubstitutesIf:
                  cleanBlueprintCopy(compareDecision.decision_rule?.do_not_treat_as_substitutes_if)
                  ?? compareDecision.decision_rule?.do_not_treat_as_substitutes_if
                  ?? null,
                nextAction:
                  cleanBlueprintCopy(compareDecision.decision_rule?.next_action)
                  ?? compareDecision.decision_rule?.next_action
                  ?? null,
              },
              deltaRows: (compareDecision.delta_table ?? []).map((row) => ({
                id: String(row.row_id ?? row.label ?? ""),
                label: humanizeState(row.label ?? row.row_id),
                candidateAValue: text(row.candidate_a_value, "Unavailable"),
                candidateBValue: text(row.candidate_b_value, "Unavailable"),
                winner: compareRoleLabel(row.winner),
                implication:
                  cleanBlueprintCopy(row.implication)
                  ?? row.implication
                  ?? null,
              })),
              portfolioConsequence: {
                candidateA: adaptPortfolioConsequence(compareDecision.portfolio_consequence?.candidate_a),
                candidateB: adaptPortfolioConsequence(compareDecision.portfolio_consequence?.candidate_b),
              },
              scenarioWinners: (compareDecision.scenario_winners ?? []).map((row) => ({
                scenario: humanizeState(row.scenario),
                candidateAEffect:
                  cleanBlueprintCopy(row.candidate_a_effect)
                  ?? row.candidate_a_effect
                  ?? "Unavailable",
                candidateBEffect:
                  cleanBlueprintCopy(row.candidate_b_effect)
                  ?? row.candidate_b_effect
                  ?? "Unavailable",
                winner: compareRoleLabel(row.winner),
                why:
                  cleanBlueprintCopy(row.why)
                  ?? row.why
                  ?? null,
              })),
              flipConditions: (compareDecision.flip_conditions ?? []).map((row) => ({
                condition: humanizeState(row.condition),
                currentState:
                  cleanBlueprintCopy(row.current_state)
                  ?? row.current_state
                  ?? "Unavailable",
                flipsToward: compareRoleLabel(row.flips_toward),
                thresholdOrTrigger:
                  cleanBlueprintCopy(row.threshold_or_trigger)
                  ?? row.threshold_or_trigger
                  ?? "No trigger emitted.",
              })),
              evidenceDiff: {
                strongerEvidence: compareRoleLabel(compareDecision.evidence_diff?.stronger_evidence),
                unresolvedFields: adaptStringList(compareDecision.evidence_diff?.unresolved_fields),
                candidateAWeakFields: adaptStringList(compareDecision.evidence_diff?.candidate_a_weak_fields),
                candidateBWeakFields: adaptStringList(compareDecision.evidence_diff?.candidate_b_weak_fields),
                evidenceNeededToDecide: adaptStringList(compareDecision.evidence_diff?.evidence_needed_to_decide),
              },
            }
          : null,
      }
    : null;

  const changesDisplay = (changes?.change_events ?? []).map((change) => ({
    id: change.event_id,
    eventType: change.event_type,
    category: change.category ?? change.ui_category ?? "source_evidence",
    direction: change.direction ?? null,
    ticker: change.symbol ?? ((change.candidate_id && candidateNameMap.get(change.candidate_id)) || tickerFromCandidate(change.candidate_id)),
    sleeve:
      change.sleeve_name
      ?? (change.candidate_id ? candidateSleeveMap.get(change.candidate_id) ?? "Blueprint" : "Blueprint"),
    sleeveId: change.sleeve_id ?? null,
    typeLabel: humanizeChangeType(change.event_type),
    impactLevel:
      change.severity === "high" || change.severity === "medium" || change.severity === "low"
        ? change.severity
        : change.impact_level === "high" || change.impact_level === "medium" || change.impact_level === "low"
          ? change.impact_level
          : "low",
    impactLabel: change.severity ? humanizeState(change.severity) : change.impact_level ? humanizeState(change.impact_level) : "Low",
    impactTone: impactTone(change.severity ?? change.impact_level),
    needsReview: change.requires_review,
    timestamp: formatDateTime(change.changed_at_utc),
    changedAtUtc: change.changed_at_utc,
    previousState: text(change.previous_state, "No prior state"),
    currentState: text(change.current_state, "Rebuilt"),
    implication: text(change.implication_summary, change.summary),
    title: change.title ?? change.summary,
    actionability: change.actionability ?? null,
    scope: change.scope ?? null,
    confidence: change.confidence ?? null,
    driverSummary: change.driver?.name
      ? appendDetail(humanizeState(change.driver.name), change.driver.family ? humanizeState(change.driver.family) : null)
      : null,
    whyItMatters: change.implication?.why_it_matters ?? change.why_it_matters ?? change.implication_summary ?? null,
    consequence: change.portfolio_consequence ?? null,
    nextAction: change.implication?.next_step ?? change.next_step ?? change.next_action ?? null,
    whatWouldReverse: change.implication?.reversal_condition ?? change.what_would_reverse ?? null,
    reportTab: change.report_tab,
    candidateId: change.candidate_id,
    renderMode: change.render_mode ?? change.change_detail?.render_mode ?? null,
    materialityClass: change.materiality_class ?? change.change_detail?.materiality_class ?? null,
    auditDetail: change.audit_detail ?? change.change_detail?.audit_detail ?? null,
    changeDetail: change.change_detail ?? null,
  }));

  const headerBadges = (blueprint.header_badges ?? [])
    .map((badge) => ({
      label: badge.label,
      tone: badgeTone(badge.tone),
    }))
    .filter((badge) => badge.label);

  const display: BlueprintDisplay = {
    meta: surfaceMeta(
      "candidates",
      headerBadges.length
        ? headerBadges
        : [{ label: freshnessLabel(blueprint.freshness_state), tone: freshnessTone(blueprint.freshness_state) }],
    ),
    degradedMessage:
      blueprint.freshness_state === "execution_failed_or_incomplete"
        ? "Blueprint data is incomplete — running in degraded mode."
        : null,
    summary: blueprint.summary ?? null,
    summaryChips: [
      {
        label: "Market context",
        value: blueprint.market_state_summary.length > 56
          ? `${blueprint.market_state_summary.slice(0, 53).trimEnd()}...`
          : blueprint.market_state_summary,
        meta: "Top-line blueprint read",
        tone: actionPostureTone(blueprint.review_posture),
      },
      {
        label: "Review posture",
        value: humanizeState(blueprint.review_posture),
        meta: "Current blueprint review posture",
        tone: actionPostureTone(blueprint.review_posture),
      },
      {
        label: "Sleeves",
        value: String(blueprint.sleeves.length),
        meta: "Capital map scope",
      },
      {
        label: "Candidates",
        value: String(allCandidates.length),
        meta: "Visible candidate rows",
      },
      {
        label: "Freshness",
        value: freshnessLabel(blueprint.freshness_state),
        meta: formatDateTime(blueprint.generated_at),
        tone: freshnessTone(blueprint.freshness_state),
      },
    ],
    sleeves,
    compare: compareDisplay,
    compareMessage: compareDisplay
      ? compareDisplay.readinessNote ?? "Compare is ready in the centered modal."
      : "Select two candidates inside the active sleeve to open compare.",
    changes: changesDisplay,
    changesEmptyMessage: changesDisplay.length
      ? null
      : "No change events are available for the current surface. Preserve the Changes section shell and render an explicit empty state.",
    changesSummary: changes?.summary ?? null,
    changesAuditGroups: changes?.audit_groups ?? [],
    changesAvailableSleeves: changes?.available_sleeves ?? [],
    changesAvailableCategories: changes?.available_categories ?? [],
    changesPagination: changes?.pagination ?? null,
    changesDailySourceScan: changes?.daily_source_scan ?? null,
    changesFreshness: {
      state: changes?.feed_freshness_state ?? null,
      latestEventAt: changes?.latest_event_at ?? null,
      latestEventAgeDays: typeof changes?.latest_event_age_days === "number" ? changes.latest_event_age_days : null,
    },
    report: report ? adaptCandidateReport(report) : null,
    inspector: [
      {
        label: "Selected sleeve",
        value: selectedSleeve?.name ?? "No sleeve available",
      },
      {
        label: "Selected candidate",
        value: selectedCandidate ? `${selectedCandidate.symbol} · ${selectedCandidate.name}` : "No candidate selected",
        tone: selectedCandidate?.statusTone,
      },
      {
        label: "Action boundary",
        value: selectedCandidate?.actionBoundary ?? "Select a candidate to inspect its action boundary.",
      },
    ],
  };
  return display;
}

export function adaptNotebook(contract: NotebookContract): NotebookDisplay {
  const activeDraft = contract.active_draft;
  const finalizedNoteCount = contract.finalized_notes?.length ?? 0;
  const hasNotebookMemory = Boolean(
    activeDraft ||
      contract.finalized_notes?.length ||
      contract.archived_notes?.length ||
      contract.note_history?.length
  );
  const finalizedNotes = contract.finalized_notes?.length
    ? contract.finalized_notes.map((entry) => ({
        date: formatDate(entry.date_label),
        title: entry.title,
        body: compactParts([
          entry.thesis,
          entry.watch_items ? `Watch: ${entry.watch_items}` : null,
          formatForecastRefs(entry.forecast_refs),
        ]),
      }))
    : contract.evidence_sections.map((section) => ({
        date: formatDate(contract.last_updated_utc),
        title: section.title,
        body: section.body,
      }));
  const archiveNotes = (contract.archived_notes ?? []).map((entry) => ({
    date: formatDate(entry.date_label),
    title: entry.title,
    body: compactParts([entry.thesis, entry.reflections, formatForecastRefs(entry.forecast_refs)]),
  }));

  const display: NotebookDisplay = {
    meta: surfaceMeta("notebook", [
      { label: "Daily reflections", tone: "neutral" },
      { label: `${finalizedNoteCount} finalized note${finalizedNoteCount === 1 ? "" : "s"}`, tone: "info" },
    ]),
    degradedMessage: hasNotebookMemory
      ? null
      : "Notebook data is not yet persistent — showing reconstructed view.",
    memoryFoundationNote: contract.memory_foundation_note ?? null,
    activeNote: {
      date: formatDate(activeDraft?.date_label ?? contract.last_updated_utc),
      title: activeDraft?.title ?? contract.name,
      linked: activeDraft?.linked_object_label ?? tickerFromCandidate(contract.candidate_id),
      nextReview: activeDraft?.next_review_date ? formatDate(activeDraft.next_review_date) : null,
      thesis: activeDraft?.thesis ?? contract.investment_case,
      assumptions: activeDraft?.assumptions ?? null,
      invalidation: activeDraft?.invalidation ?? null,
      watchItems: activeDraft?.watch_items ?? (contract.evidence_sections.length
        ? contract.evidence_sections.map((section) => section.title).join(" · ")
        : null),
      reflections: appendDetail(
        activeDraft?.reflections ?? null,
        formatForecastRefs(activeDraft?.forecast_refs ?? contract.forecast_refs)
      ),
    },
    finalizedNotes,
    archiveNotes,
    researchSupport: adaptResearchSupport(contract.research_support),
    inspector: [
      {
        label: "Linked candidate",
        value: `${tickerFromCandidate(contract.candidate_id)} · ${contract.name}`,
      },
      {
        label: "Last updated",
        value: formatDateTime(contract.last_updated_utc),
      },
      {
        label: "Evidence depth",
        value: humanizeState(contract.evidence_depth),
        tone: "info",
      },
      ...(contract.note_history?.length
        ? [
            {
              label: "Revision history",
              value: `${contract.note_history.length} recorded revision${contract.note_history.length > 1 ? "s" : ""}`,
            },
          ]
        : []),
    ],
  };
  return display;
}

export function adaptEvidence(contract: EvidenceWorkspaceContract): EvidenceDisplay {
  const sourceCount = contract.evidence_pack.source_count;
  const forecastSupportCount = contract.forecast_support_items?.length ?? 0;
  const dataQualitySummary = contract.data_quality_summary;
  const documents = contract.documents?.length
    ? contract.documents.map((document) => ({
        title: document.title,
        type: humanizeState(document.document_type),
        linked: document.linked_object_label,
        age: document.retrieved_utc ? formatDateTime(document.retrieved_utc) : "No retrieval timestamp",
        stale: document.stale,
      }))
    : contract.source_citations.map((citation) => ({
        title: citation.title,
        type: humanizeState(citation.reliability),
        linked: contract.name,
        age: citation.retrieved_utc ? formatDateTime(citation.retrieved_utc) : "No retrieval timestamp",
        stale: !citation.retrieved_utc,
      }));

  const objectGroups = contract.object_groups?.length
    ? contract.object_groups.map((group) => ({
        title: group.title,
        items: group.items.map((item) => ({
          name: item.object_label,
          direct: String(item.direct_count),
          proxy: String(item.proxy_count),
          stale: String(item.stale_count),
          gap: item.gap_flag,
          claims: item.claims.length
            ? [
                ...item.claims.slice(0, 3).map((claim) => ({
                  text: claim.claim_text,
                  meta: `${humanizeState(claim.directness)} · ${freshnessLabel(claim.freshness_state)}`,
                })),
                ...(item.claims.length > 3
                  ? [
                      {
                        text: `${item.claims.length - 3} more support item${item.claims.length - 3 === 1 ? "" : "s"} remain in the workspace.`,
                        meta: "Additional support preserved in backend contract",
                      },
                    ]
                  : []),
              ]
            : [
                {
                  text: "No claims have been emitted for this object yet.",
                  meta: "Typed degraded state",
                },
              ],
        })),
      }))
    : [
        {
          title: "Candidate",
          items: [
            {
              name: contract.name || tickerFromCandidate(contract.candidate_id),
              direct: String(sourceCount),
              proxy: "0",
              stale: sourceCount ? "0" : "0",
              gap: sourceCount === 0,
              claims: contract.source_citations.length
                ? contract.source_citations.slice(0, 3).map((citation) => ({
                    text: citation.title,
                    meta: citation.url ?? citation.reliability,
                  }))
                : [
                    {
                      text: "No source citations have been emitted for this candidate yet.",
                      meta: "Typed degraded state",
                    },
                  ],
            },
          ],
        },
      ];

  const display: EvidenceDisplay = {
    meta: surfaceMeta("evidence", [
      { label: `${sourceCount} source citation${sourceCount === 1 ? "" : "s"}`, tone: sourceCount ? "good" : "warn" },
      ...(forecastSupportCount ? [{ label: `${forecastSupportCount} forecast support item${forecastSupportCount === 1 ? "" : "s"}`, tone: "info" as Tone }] : []),
    ]),
    degradedMessage:
      sourceCount === 0 && forecastSupportCount === 0
        ? "Evidence Workspace has no citations yet. Preserve the reference object, documents, mappings, tax, and gaps sections as explicit degraded shells."
        : null,
    summaryTiles: [
      { label: "Direct evidence items", value: String(contract.summary?.direct_count ?? sourceCount), meta: `${documents.length} document${documents.length === 1 ? "" : "s"} in support map`, tone: (contract.summary?.direct_count ?? sourceCount) ? "good" : "warn" },
      { label: "Proxy evidence items", value: String(contract.summary?.proxy_count ?? 0), meta: `${objectGroups.reduce((total, group) => total + group.items.length, 0)} linked object${objectGroups.reduce((total, group) => total + group.items.length, 0) === 1 ? "" : "s"}`, tone: "neutral" },
      { label: "Stale items", value: String(contract.summary?.stale_count ?? documents.filter((doc) => doc.stale).length), meta: "Items using stored or stale support", tone: documents.some((doc) => doc.stale) ? "warn" : "good" },
      { label: "Unresolved gaps", value: String(contract.summary?.gap_count ?? (sourceCount ? 0 : 1)), meta: "Open support-map gaps", tone: (contract.summary?.gap_count ?? (sourceCount ? 0 : 1)) ? "warn" : "neutral" },
      ...(dataQualitySummary
        ? [
            {
              label: "Critical fields ready",
              value: `${dataQualitySummary.critical_fields_ready}/${dataQualitySummary.critical_fields_total}`,
              meta: dataQualitySummary.summary,
              tone:
                dataQualitySummary.data_confidence === "high"
                  ? ("good" as Tone)
                  : dataQualitySummary.data_confidence === "mixed"
                    ? ("warn" as Tone)
                    : ("bad" as Tone),
            },
          ]
        : []),
    ],
    objectGroups,
    documents,
    mappings: (contract.benchmark_mappings ?? []).map((mapping) => ({
      sleeve: mapping.sleeve_label,
      instrument: mapping.instrument_label,
      benchmark: mapping.benchmark_label,
      baseline: mapping.baseline_label,
      directness: humanizeState(mapping.directness),
    })),
    taxAssumptions: (contract.tax_assumptions ?? []).map((assumption) => ({
      label: assumption.label,
      value: assumption.value,
    })),
    gaps: contract.gaps?.length
      ? contract.gaps.map((gap) => ({
          object: gap.object_label,
          issue: gap.issue_text,
        }))
      : sourceCount
        ? []
        : [
            {
              object: tickerFromCandidate(contract.candidate_id),
              issue: "No source citations have been attached to this evidence workspace yet.",
            },
          ],
    researchSupport: adaptResearchSupport(contract.research_support),
    inspector: [
      {
        label: "Evidence freshness",
        value: freshnessLabel(contract.evidence_pack.freshness_state),
        tone: freshnessTone(contract.evidence_pack.freshness_state),
      },
      {
        label: "Completeness score",
        value: `${Math.round((contract.completeness_score ?? 0) * 100)}%`,
      },
      ...(dataQualitySummary
        ? [
            {
              label: "Data confidence",
              value: humanizeState(dataQualitySummary.data_confidence),
            },
          ]
        : []),
      {
        label: "Candidate",
        value: `${tickerFromCandidate(contract.candidate_id)} · ${contract.name}`,
      },
    ],
  };
  return display;
}
