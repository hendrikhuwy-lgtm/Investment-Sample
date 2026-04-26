import React, { useEffect, useRef, useState } from "react";

import {
  adaptBlueprint,
  adaptCandidateReport,
  adaptDailyBrief,
  adaptEvidence,
  adaptNotebook,
  adaptPortfolio,
  type BlueprintDisplay,
  type BlueprintSleeveDisplay,
  type CandidateReportDisplay,
  type ChangeDisplay,
  type DailyBriefDisplay,
  type EvidenceDisplay,
  type InspectorLine,
  type NotebookDisplay,
  type PortfolioDisplay,
  type PrimaryView,
  type ReportTab,
  type Tone,
} from "./adapters";
import { MarketPathSupportPanel } from "./blueprint/MarketPathSupportPanel";
import { cleanBlueprintCopy, presentBlueprintBlocker } from "./blueprint/surfacePresentation";
import { ChartPanel } from "./charts/ChartPanel";
import { DailyBriefDecisionChart } from "./charts/DailyBriefDecisionChart";
import {
  activatePortfolioUpload,
  ApiRequestError,
  fetchBlueprintCoverageAudit,
  fetchBlueprintExplorer,
  fetchCandidateReport,
  fetchChanges,
  fetchCompare,
  fetchDailyBrief,
  fetchEvidenceWorkspace,
  fetchHealth,
  fetchNotebook,
  fetchPortfolio,
  requestDeferredForecastStart,
  type BlueprintCoverageAuditContract,
  type CandidateReportResponse,
  uploadPortfolioHoldings,
} from "./api";
import type {
  BlueprintExplorerContract,
  CandidateReportContract,
  ChangesContract,
  CompareContract,
  DailyBriefContract,
  EvidenceWorkspaceContract,
  NotebookContract,
  PortfolioContract,
} from "../../shared/v2_surface_contracts";

type Status<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type ReportLoadState =
  | "idle"
  | "loading"
  | "ready"
  | "pending"
  | "stale_cached"
  | "unavailable"
  | "error";

type ReportSourceBinding = {
  sleeveKey: string | null;
  sourceSnapshotId: string | null;
  sourceGeneratedAt: string | null;
  sourceContractVersion: string | null;
};

type ReportStatus = Status<CandidateReportContract> & {
  state: ReportLoadState;
  userMessage: string | null;
  developerMessage?: string | null;
  bindingKey: string;
  retryAfterMs?: number | null;
};

type ExplorerChangesWindow = "today" | "3d" | "7d";
type ExplorerChangesType =
  | "all"
  | "requires_review"
  | "upgrades"
  | "downgrades"
  | "blocker_changes"
  | "evidence"
  | "sleeve"
  | "freshness_risk"
  | "decision"
  | "market_impact"
  | "portfolio_drift"
  | "source_evidence"
  | "blocker"
  | "timing"
  | "audit_only"
  | "system";

type QueryState = {
  view: PrimaryView;
  candidateId: string | null;
  reportCandidateId: string | null;
  reportTab: ReportTab;
};

type BlueprintCandidateDisplay = BlueprintDisplay["sleeves"][number]["candidates"][number];
type CompareDisplayView = NonNullable<BlueprintDisplay["compare"]>;
type CompareDimensionView = CompareDisplayView["dimensions"][number];
type QuickBriefPeerRow = NonNullable<NonNullable<CandidateReportDisplay["quickBrief"]>["peerComparePack"]>["rows"][number];
type CompareBucketId =
  | "sleeve_job"
  | "benchmark_exposure"
  | "implementation_cost"
  | "source_integrity"
  | "market_path_context"
  | "secondary";

function compactParts(parts: Array<string | null | undefined>) {
  return parts.map((part) => String(part ?? "").trim()).filter(Boolean).join(" · ");
}

function dedupeStateBadges(badges: Array<{ label: string; tone?: Tone }>) {
  const seen = new Set<string>();
  return badges.filter((badge) => {
    const key = `${badge.label}|${badge.tone ?? "neutral"}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function candidateStateSignals(candidate: BlueprintCandidateDisplay) {
  const decisionRaw = String(candidate.decisionStateRaw ?? "").trim().toLowerCase();
  const gateRaw = String(candidate.gateStateRaw ?? "").trim().toLowerCase();
  const sourceTone = candidate.sourceIntegritySummary?.stateTone ?? candidate.dataQualitySummary?.confidenceTone ?? "neutral";
  const primaryFailureLabel = candidate.failureSummary?.primaryLabel ?? null;
  const primaryFailureTone: Tone | undefined = candidate.failureSummary?.hardClasses.length
    ? "bad"
    : candidate.failureSummary?.reviewClasses.length
      ? "warn"
      : candidate.failureSummary?.confidenceDragClasses.length
        ? sourceTone
        : undefined;
  const badges: Array<{ label: string; tone?: Tone }> = [];

  if (candidate.decisionTone === "bad" || gateRaw === "blocked" || decisionRaw.includes("blocked")) {
    badges.push({ label: "Blocked", tone: "bad" });
  } else if (
    gateRaw === "review_only"
    || decisionRaw.includes("review")
    || decisionRaw.includes("watch")
    || decisionRaw.includes("short")
  ) {
    badges.push({ label: "Reviewable", tone: "info" });
  }

  if (primaryFailureLabel) {
    badges.push({ label: primaryFailureLabel, tone: primaryFailureTone });
  } else if ((sourceTone === "warn" || sourceTone === "bad") && candidate.sourceIntegritySummary?.integrityLabel) {
    badges.push({ label: candidate.sourceIntegritySummary.integrityLabel, tone: sourceTone });
  }

  if (!badges.length && candidate.decisionTone === "good") {
    badges.push({ label: "Actionable", tone: "good" });
  }

  return dedupeStateBadges(badges);
}

function candidateSourceSummary(candidate: BlueprintCandidateDisplay) {
  const integrity = candidate.sourceIntegritySummary;
  const completion = candidate.sourceCompletionSummary;
  const confidence = candidate.dataQualitySummary;
  const completionReady = String(completion?.state ?? "").trim().toLowerCase() === "complete";
  const criticalReady = completion?.criticalTotal
    ? `${completion.criticalCompleted}/${completion.criticalTotal} critical ready`
    : integrity?.criticalTotal
    ? `${integrity.criticalReady}/${integrity.criticalTotal} critical ready`
    : confidence?.criticalTotal
      ? `${confidence.criticalReady}/${confidence.criticalTotal} critical ready`
      : null;
  if (completionReady) {
    return {
      chips: dedupeStateBadges([
        { label: "Source complete", tone: "good" as Tone },
        ...(criticalReady ? [{ label: criticalReady }] : []),
      ]),
      line: completion?.summary ?? integrity?.summary ?? "All recommendation-critical fields are source-complete.",
      meta: completion?.completionReasons?.[0] ?? "Source integrity clean",
    };
  }
  return {
    chips: dedupeStateBadges([
      ...(integrity ? [{ label: integrity.state, tone: integrity.stateTone }] : []),
      ...(confidence ? [{ label: confidence.confidence, tone: confidence.confidenceTone }] : []),
      ...(criticalReady ? [{ label: criticalReady }] : []),
    ]),
    line: integrity?.summary ?? confidence?.summary ?? "No explicit source-confidence summary was emitted.",
    meta: integrity?.integrityLabel ?? null,
  };
}

function timingReasonLabel(reason: string) {
  const raw = String(reason ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    direct_series_current_and_usable: "Direct series current",
    direct_series_watch: "Direct route watch",
    proxy_series_fresh_and_approved: "Proxy fresh",
    proxy_series_stale: "Proxy stale",
    proxy_series_degraded: "Proxy degraded",
    proxy_series_missing: "Proxy missing",
    direct_series_broken: "Direct route broken",
    old_schema_artifact: "Old forecast artifact",
    old_artifact_replaced: "Old artifact replaced",
    missing_artifact_replaced: "Missing artifact replaced",
    forecast_runtime_failed: "Forecast runtime failed",
    forecast_model_missing_dependency: "Kronos dependency missing",
    forecast_provider_unavailable: "Forecast provider unavailable",
    forecast_unavailable_route_history_usable: "Route history usable",
    forecast_output_invalid_quarantined: "Model output quarantined",
    invalid_predicted_bar_geometry: "Invalid predicted bars",
    negative_predicted_liquidity: "Invalid liquidity output",
    market_setup_unavailable: "Market setup unavailable",
    no_usable_route_history: "No usable route history",
    threshold_drift_weakening: "Drift weakening",
    path_fragility_current: "Current path fragile",
    path_noisy_but_usable: "Path noisy",
    missing_artifact: "No forecast artifact",
    latest_forecast_run_not_ready: "Latest run not ready",
  };
  return map[raw] ?? raw.replace(/[_-]+/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function candidateMarketPathSummary(candidate: BlueprintCandidateDisplay) {
  const marketPath = candidate.marketPath;
  const coverageStatusRaw = String(candidate.coverageStatusRaw ?? "").trim().toLowerCase();
  const coverageSummary =
    coverageStatusRaw === "direct_ready"
      ? "Stored direct history is present and usable for bounded market-path review."
      : coverageStatusRaw === "proxy_ready"
        ? "Stored proxy history is present and usable, but provenance should stay explicit."
        : coverageStatusRaw === "benchmark_lineage_weak"
          ? "Market history is present, but benchmark lineage still keeps the path bounded."
          : coverageStatusRaw === "missing_history"
            ? "No usable direct or proxy market history is currently available."
            : null;
  const shouldPreferCoverage =
    Boolean(marketPath)
    && ["unavailable", "stale"].includes(String(marketPath?.setupStateRaw ?? "").trim().toLowerCase())
    && ["direct_ready", "proxy_ready", "benchmark_lineage_weak"].includes(coverageStatusRaw);
  if (!marketPath) {
    return {
      chips: candidate.coverageStatus ? [{ label: candidate.coverageStatus, tone: coverageStatusRaw === "missing_history" ? "bad" : "info" as Tone }] : [] as Array<{ label: string; tone?: Tone }>,
      line: candidate.coverageSummary ?? coverageSummary ?? candidate.marketSupportBasis ?? "No typed market-path support was emitted.",
      meta: null as string | null,
    };
  }
  if (shouldPreferCoverage) {
    return {
      chips: dedupeStateBadges([
        ...(marketPath.timingLabel ? [{ label: marketPath.timingLabel, tone: marketPath.timingTone as Tone }] : []),
        ...marketPath.timingReasons.slice(0, 2).map((reason) => ({ label: timingReasonLabel(reason), tone: marketPath.timingTone as Tone })),
        ...(candidate.coverageStatus ? [{ label: candidate.coverageStatus, tone: coverageStatusRaw === "direct_ready" ? "good" as Tone : "warn" as Tone }] : []),
        ...(marketPath.provenanceLabel ? [{ label: marketPath.provenanceLabel, tone: marketPath.provenanceTone as Tone }] : []),
      ]),
      line: candidate.coverageSummary ?? coverageSummary ?? marketPath.summaryLine ?? marketPath.objectiveLabel ?? candidate.marketSupportBasis ?? "No typed market-path support was emitted.",
      meta: marketPath.qualityNote ?? null,
    };
  }
  return {
    chips: dedupeStateBadges([
      ...(marketPath.timingLabel ? [{ label: marketPath.timingLabel, tone: marketPath.timingTone as Tone }] : []),
      ...marketPath.timingReasons.slice(0, 2).map((reason) => ({ label: timingReasonLabel(reason), tone: marketPath.timingTone as Tone })),
      { label: marketPath.stateLabel, tone: marketPath.stateTone as Tone },
      ...(marketPath.provenanceLabel ? [{ label: marketPath.provenanceLabel, tone: marketPath.provenanceTone as Tone }] : []),
    ]),
    line:
      marketPath.objectiveLabel
      ?? marketPath.summaryLine
      ?? marketPath.suppressionLabel
      ?? marketPath.implication
      ?? candidate.marketSupportBasis
      ?? "No typed market-path support was emitted.",
    meta: marketPath.qualityNote ?? null,
  };
}

function compactSentence(value: string | null | undefined, fallback: string | null = null): string | null {
  const cleaned = cleanBlueprintCopy(value) ?? String(value ?? "").trim();
  if (!cleaned) return fallback;
  const sentence = cleaned.match(/.*?[.!?](?:\s|$)/)?.[0]?.trim();
  return sentence || cleaned || fallback;
}

function compressLaneCopy(value: string | null | undefined, fallback: string | null = null): string | null {
  const base = compactSentence(value, fallback);
  if (!base) return fallback;
  return base
    .replace(/Execution proxies still disagree enough to require review, but they do not invalidate the candidate on their own\.?/i, "Execution review required, not disqualifying.")
    .replace(/Current path is active but too fragile to strengthen the candidate\.?/i, "Timing does not strengthen the case.")
    .replace(/Support unavailable; do not use this path as evidence\.?/i, "Timing unavailable.")
    .replace(/No typed market-path support was emitted\.?/i, "Timing unavailable.")
    .replace(/Execution proxies still disagree enough to require review\.?/i, "Execution review required.")
    .replace(/The sleeve question is whether .*? is the cleanest way to /i, "")
    .replace(/\s+/g, " ")
    .trim();
}

function laneDecisionReason(candidate: BlueprintCandidateDisplay, stateSummary: { headline: string; detail: string | null; tone: Tone }) {
  const blocker = `${candidate.whatBlocksAction ?? ""} ${stateSummary.detail ?? ""}`.toLowerCase();
  if (/execution|proxy|spread|tracking|implementation/.test(blocker) && /review|required|disagree|cleanup|clean up|drag/.test(blocker)) {
    return "Execution review required.";
  }
  if (/source|document|coverage|confidence|evidence/.test(blocker) && /review|required|mixed|thin|cleanup|clean up|drag|limited/.test(blocker)) {
    return "Evidence cleanup required.";
  }
  if (/mandate|benchmark|sleeve/.test(blocker) && /review|required|uncertain|uncertainty|break/.test(blocker)) {
    return "Mandate review required.";
  }
  if (/blocked|hard blocker|remove/.test(blocker) || candidate.decisionTone === "bad") {
    return "Blocked by current sleeve rules.";
  }
  if (/timing|market|path/.test(blocker) && /fragile|weak|unavailable|stale/.test(blocker)) {
    return "Timing does not strengthen the case.";
  }
  return compressLaneCopy(stateSummary.headline, "Still under review.") ?? "Still under review.";
}

function laneDecisionSubline(candidate: BlueprintCandidateDisplay, stateSummary: { headline: string; detail: string | null; tone: Tone }) {
  const decision = String(candidate.decisionState ?? "").trim().toLowerCase();
  if (decision === "reviewable" || decision === "watchlist only" || decision === "watchlist") {
    return "Worth active review, not action-ready.";
  }
  return compressLaneCopy(candidate.decisionSummary, stateSummary.detail) ?? stateSummary.detail ?? null;
}

function laneContextConsequence(
  sourceTone: Tone | undefined,
  marketTone: Tone | undefined,
  marketLabel: string | null | undefined,
) {
  const marketUnavailable = /unavailable|not surfaced|route unavailable/i.test(String(marketLabel ?? ""));
  if (marketTone === "bad" || marketTone === "warn") return "Does not strengthen preference.";
  if (sourceTone === "bad" || sourceTone === "warn") return "Evidence still limits conviction.";
  if (marketUnavailable) return "No decision-changing warning.";
  return "Supports review, not decisive.";
}

function laneThesisLine(
  candidate: BlueprintCandidateDisplay,
  sleeveName: string,
  decisionReason: string,
) {
  const blocker = decisionReason.replace(/\.$/, "").toLowerCase();
  const quality = compressLaneCopy(candidate.implementationSummary, null);
  if (quality && /best|cleanest|low fee|large scale|strong scale/i.test(quality)) {
    return `${quality.replace(/\.$/, "")}, but ${blocker}.`;
  }
  return `In scope for ${sleeveName}, but ${blocker}.`;
}

function quickBriefGapNote(
  sectionLabel: string,
  {
  loading = false,
  error = false,
  sourceLine,
}: {
  loading?: boolean;
  error?: boolean;
  sourceLine?: string | null | undefined;
}): string {
  if (loading) {
    return `Loading ${sectionLabel.toLowerCase()} from the full candidate report.`;
  }
  if (error) {
    return `Source issue: ${sectionLabel.toLowerCase()} is still using limited fallback data because the live candidate report did not load cleanly.`;
  }
  const reason = compactSentence(sourceLine);
  if (reason) {
    return `Source issue: ${sectionLabel.toLowerCase()} is not surfaced cleanly enough for this candidate yet. ${reason}`;
  }
  return `Source issue: ${sectionLabel.toLowerCase()} is not surfaced cleanly enough for this candidate yet.`;
}

function parsePercentLike(value: string | null | undefined): number | null {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const percentMatch = raw.match(/(-?\d+(?:\.\d+)?)\s*%/);
  if (percentMatch) return Number.parseFloat(percentMatch[1]);
  const bpsMatch = raw.match(/(-?\d+(?:\.\d+)?)\s*bps?/i);
  if (bpsMatch) return Number.parseFloat(bpsMatch[1]) / 100;
  return null;
}

function percentBarWidth(value: string | null | undefined): number | null {
  const parsed = parsePercentLike(value);
  if (parsed === null) return null;
  return Math.max(8, Math.min(100, parsed));
}

function formatHumanDate(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
}

function shortDistributionLabel(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  if (/accum/i.test(raw)) return "Acc";
  if (/dist/i.test(raw)) return "Dist";
  return raw;
}

function peerCostTone(subjectCost: string | null | undefined, peerCost: string | null | undefined): Tone | undefined {
  const subject = parsePercentLike(subjectCost);
  const peer = parsePercentLike(peerCost);
  if (subject === null || peer === null) return undefined;
  if (peer < subject) return "good";
  if (peer > subject) return "bad";
  return undefined;
}

function peerRoleTag(row: QuickBriefPeerRow): string {
  if (row.role === "same_job_peer") {
    if (row.sameIndex && row.benchmarkFamily) return `Same ${row.benchmarkFamily} job`;
    if (row.sameJob) return "Same-job peer";
    return "Closest direct peer";
  }
  if (row.emergingMarketsIncluded) return "Broader all-world control";
  return "Broader developed-markets alternative";
}

function peerDeltaPills(
  row: QuickBriefPeerRow,
  subjectRow: QuickBriefPeerRow | null,
): Array<{ label: string; tone?: Tone }> {
  const pills: Array<{ label: string; tone?: Tone }> = [];
  if (row.ter) {
    pills.push({ label: `${row.ter} TER`, tone: peerCostTone(subjectRow?.ter, row.ter) });
  }
  if (row.role === "same_job_peer") {
    pills.push({
      label: row.exposureScope ?? "Same developed-markets scope",
      tone: row.exposureScope && row.exposureScope !== subjectRow?.exposureScope ? "info" : "info",
    });
  } else if (row.emergingMarketsIncluded !== null && row.emergingMarketsIncluded !== undefined) {
    pills.push({ label: row.emergingMarketsIncluded ? "EM included" : "EM excluded", tone: "info" });
  } else if (row.exposureScope) {
    pills.push({ label: row.exposureScope, tone: "info" });
  } else if (row.domicile) {
    pills.push({ label: row.domicile, tone: row.sameDomicile === false ? "warn" : undefined });
  }
  return pills.slice(0, 2);
}

function compareConclusionHeadline(subjectRow: QuickBriefPeerRow | null, directPeerRows: QuickBriefPeerRow[]): string {
  const subjectCost = parsePercentLike(subjectRow?.ter);
  const peerCosts = directPeerRows
    .map((row) => parsePercentLike(row.ter))
    .filter((value): value is number => value !== null);
  if (subjectCost !== null && peerCosts.length && Math.min(...peerCosts) < subjectCost) {
    return "Direct same-job peers are cheaper.";
  }
  if (directPeerRows.length) {
    return "Direct same-job peers are the first real pressure test.";
  }
  return "The closest same-job peers still define the first decision.";
}

function kronosToneFromPath(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw.includes("support") || raw.includes("contained")) return "good";
  if (raw.includes("fragile") || raw.includes("elevated") || raw.includes("bounded") || raw.includes("weakening") || raw.includes("watch")) return "warn";
  if (raw.includes("adverse")) return "bad";
  return "info";
}

function kronosToneFromQuality(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw === "high") return "good";
  if (raw === "medium") return "warn";
  if (raw === "low" || raw === "unavailable") return "bad";
  return "info";
}

function candidateStateSummary(candidate: BlueprintCandidateDisplay) {
  const decisionRaw = String(candidate.decisionStateRaw ?? "").trim().toLowerCase();
  const gateRaw = String(candidate.gateStateRaw ?? "").trim().toLowerCase();
  const usefulness = String(candidate.marketPath?.usefulness ?? "").trim().toLowerCase();
  const sourceTone = candidate.sourceIntegritySummary?.stateTone ?? candidate.dataQualitySummary?.confidenceTone ?? "neutral";
  if (gateRaw === "blocked" || decisionRaw.includes("blocked")) {
    return {
      headline: "Not ready to rely on yet",
      detail: compactSentence(candidate.whatBlocksAction ?? candidate.decisionSummary, "A hard blocker still keeps this candidate out of action."),
      tone: "bad" as Tone,
    };
  }
  if (gateRaw === "review_only" || decisionRaw.includes("review") || decisionRaw.includes("watch") || decisionRaw.includes("short")) {
    return {
      headline: "Worth active review, not action-ready",
      detail: compactSentence(candidate.decisionSummary ?? candidate.whatChangesView, "The candidate stays worth active review, but it is not clean enough to act on yet."),
      tone: "info" as Tone,
    };
  }
  if (usefulness === "unstable") {
    return {
      headline: "Active support, but still fragile",
      detail: compactSentence(candidate.marketPath?.objectiveNote ?? candidate.marketPath?.implication, "The market path is active, but too fragile to strengthen the case."),
      tone: "bad" as Tone,
    };
  }
  if (usefulness === "usable_with_caution") {
    return {
      headline: "Useful, but still bounded",
      detail: compactSentence(candidate.marketPath?.objectiveNote ?? candidate.marketPath?.summaryLine, "Support exists, but it should stay secondary and bounded."),
      tone: "warn" as Tone,
    };
  }
  if (sourceTone === "warn" || sourceTone === "bad") {
    return {
      headline: "Facts mostly present, confidence still limited",
      detail: compactSentence(candidate.failureSummary?.summary ?? candidate.sourceIntegritySummary?.summary, "The candidate remains usable, but confidence is still being dragged by thinner support."),
      tone: sourceTone,
    };
  }
  return {
    headline: "Clean enough to keep moving",
    detail: compactSentence(candidate.decisionSummary ?? candidate.implicationSummary, "The candidate is carrying enough support to stay decision-relevant."),
    tone: "good" as Tone,
  };
}

function scoreWeightingNote(sleeveName: string | null | undefined): string {
  const raw = String(sleeveName ?? "").trim().toLowerCase();
  const prefix = "Recommendation score now weights recommendation merit 80% and deployability 20%, while truth confidence controls how strongly the read can be trusted and promoted.";
  if (!raw) return `${prefix} The diagnostic bars stay visible so you can still see the sleeve-specific strengths and drags.`;
  if (raw.includes("global equity") || raw.includes("developed ex us") || raw.includes("emerging")) {
    return `${prefix} In equity sleeves, benchmark fidelity and source integrity still matter more than short-lived market-path strength.`;
  }
  if (raw.includes("ig bonds") || raw.includes("cash and bills")) {
    return `${prefix} In defensive sleeves, implementation and source integrity still dominate the diagnostic read because execution slippage matters more than aggressive market-path strength.`;
  }
  if (raw.includes("real assets") || raw.includes("alternatives")) {
    return `${prefix} In diversifier sleeves, sleeve fit and source integrity still anchor the diagnostics, with market-path support acting only as bounded reinforcement.`;
  }
  if (raw.includes("convex")) {
    return `${prefix} In convex sleeves, sleeve fit and implementation discipline still come first. Market-path support can confirm timing, but it does not override the hedge job.`;
  }
  return `${prefix} The diagnostic bars remain sleeve-aware rather than a flat cross-category average.`;
}

function compareGapRows(compareDisplay: CompareDisplayView | null | undefined) {
  const buckets = groupCompareDimensions(compareDisplay);
  const primary = buckets.filter((bucket) => bucket.id !== "secondary").flatMap((bucket) => bucket.dimensions);
  return primary.slice(0, 3).map((dimension) => ({
    label: dimension.label,
    summary: dimension.rationale || (dimension.winnerLabel ? `${dimension.winnerLabel} stays cleaner on this dimension.` : "This dimension is still differentiating the candidates."),
  }));
}

function renderReportSkeleton(tabLabel: string) {
  return (
    <div className="layout-stack" aria-live="polite">
      <div className="surface-warning">Loading {tabLabel}…</div>
      <div className="report-summary-strip-fixed">
        {[0, 1, 2, 3].map((index) => (
          <div className="report-card" key={`report-skeleton-${index}`}>
            <div className="panel-kicker">Loading</div>
            <div className="panel-title" style={{ fontSize: 20 }}>Building report view…</div>
            <div className="panel-copy">The current candidate report is still loading, but the existing drawer state will stay stable.</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function renderReportPendingState(status: ReportStatus | undefined, blueprintLoading: boolean) {
  const message =
    status?.userMessage
    ?? (blueprintLoading
      ? "Waiting for the Explorer snapshot before loading the bound report."
      : "Report is being prepared from the selected Explorer snapshot.");
  const title = status?.state === "unavailable" ? "Report unavailable" : "Preparing report";
  return (
    <div className="layout-stack" aria-live="polite">
      <div className="surface-warning">{message}</div>
      <div className="report-summary-strip-fixed">
        {[0, 1, 2].map((index) => (
          <div className="report-card" key={`report-pending-${index}`}>
            <div className="panel-kicker">{status?.state === "stale_cached" ? "Cached report" : "Deep report"}</div>
            <div className="panel-title" style={{ fontSize: 20 }}>{title}</div>
            <div className="panel-copy">The drawer will stay open and stable while the source-bound report is prepared.</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function compareBucketMeta(dimension: CompareDimensionView): { id: CompareBucketId; label: string; summary: string; order: number } {
  const key = [dimension.id, dimension.label, dimension.group]
    .map((value) => String(value ?? "").trim().toLowerCase())
    .filter(Boolean)
    .join(" ");
  if (/sleeve|job|fit|role|substitute|mandate/.test(key)) {
    return { id: "sleeve_job", label: "Sleeve job", summary: "Start here: which candidate is cleaner for the sleeve job?", order: 0 };
  }
  if (/benchmark|exposure|index|tracking|baseline|region|sector/.test(key)) {
    return { id: "benchmark_exposure", label: "Benchmark and exposure", summary: "Check whether the winner is cleaner on benchmark fidelity and role exposure.", order: 1 };
  }
  if (/implementation|cost|ter|spread|aum|tax|domicile|currency|replication|distribution|liquidity/.test(key)) {
    return { id: "implementation_cost", label: "Implementation and cost", summary: "Check friction only after sleeve fit is clear.", order: 2 };
  }
  if (/source|authority|evidence|identity|coverage|stale|truth|conflict|confidence/.test(key)) {
    return { id: "source_integrity", label: "Source integrity", summary: "Use this bucket to see where authority or identity still weakens the loser.", order: 3 };
  }
  if (/market|path|forecast|scenario|fragility|drift|threshold|proxy|kronos/.test(key)) {
    return { id: "market_path_context", label: "Market-path context", summary: "Keep this secondary. It can support the read, but it does not override sleeve or implementation truth.", order: 4 };
  }
  return {
    id: "secondary",
    label: dimension.group ?? "Secondary",
    summary: "Lower-priority dimensions that do not change the top compare read.",
    order: 5,
  };
}

function dimensionHasMeaningfulSpread(dimension: CompareDimensionView) {
  const normalized = dimension.values
    .map((item) => String(item.value ?? "").trim().toLowerCase())
    .filter(Boolean);
  return normalized.length <= 1 || new Set(normalized).size > 1;
}

function groupCompareDimensions(compareDisplay: CompareDisplayView | null | undefined) {
  if (!compareDisplay) return [];
  const grouped = new Map<CompareBucketId, { id: CompareBucketId; label: string; summary: string; order: number; dimensions: CompareDimensionView[] }>();
  compareDisplay.dimensions
    .filter((dimension) => dimensionHasMeaningfulSpread(dimension))
    .forEach((dimension) => {
      const meta = compareBucketMeta(dimension);
      const bucket = grouped.get(meta.id) ?? { ...meta, dimensions: [] };
      bucket.dimensions.push(dimension);
      grouped.set(meta.id, bucket);
    });
  return [...grouped.values()]
    .sort((left, right) => left.order - right.order)
    .filter((bucket) => bucket.dimensions.length);
}

const NAV: Array<{ id: PrimaryView; kicker: string; title: string }> = [
  { id: "portfolio", kicker: "Book state", title: "Portfolio" },
  { id: "brief", kicker: "What changed", title: "Daily Brief" },
  { id: "candidates", kicker: "Capital deployment", title: "Blueprint" },
  { id: "notebook", kicker: "Reasoning", title: "Research Notebook" },
  { id: "evidence", kicker: "Supporting material", title: "Evidence Workspace" },
];

const REPORT_TABS: Array<{ id: ReportTab; label: string }> = [
  { id: "investment_case", label: "Investment case" },
  { id: "market_history", label: "Market & history" },
  { id: "scenarios", label: "Scenarios" },
  { id: "risks", label: "Risks" },
  { id: "competition", label: "Competition" },
  { id: "evidence", label: "Evidence & sources" },
];

function createStatus<T>(): Status<T> {
  return { data: null, loading: false, error: null };
}

function createInitialStatus<T>(loading: boolean): Status<T> {
  return { data: null, loading, error: null };
}

const BLUEPRINT_BROWSER_CACHE_KEY = "cortex.blueprint.last.v1";

function readCachedBlueprint(): BlueprintExplorerContract | null {
  try {
    const raw = window.localStorage.getItem(BLUEPRINT_BROWSER_CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as BlueprintExplorerContract;
    return parsed?.surface_id === "blueprint_explorer" ? parsed : null;
  } catch {
    return null;
  }
}

function writeCachedBlueprint(contract: BlueprintExplorerContract) {
  try {
    window.localStorage.setItem(BLUEPRINT_BROWSER_CACHE_KEY, JSON.stringify(contract));
  } catch {
    // Browser storage is a performance hint only; failing to write should not affect the surface.
  }
}

type SimpleNotebookEntryStatus = "draft" | "finalized" | "archived";

type SimpleNotebookEntry = {
  id: string;
  status: SimpleNotebookEntryStatus;
  date: string;
  linked: string;
  title: string;
  thesis: string;
  assumptions: string;
  invalidation: string;
  watchItems: string;
  reflections: string;
  nextReview: string;
  dirty?: boolean;
};

type NotebookPromptType = "challenge" | "assumptions" | "change" | "brief";

type NotebookAssistResponse = {
  label: string;
  loading: boolean;
  text: string;
};

const NOTEBOOK_ASSIST_LABELS: Record<NotebookPromptType, string> = {
  challenge: "Challenge",
  assumptions: "Weak assumptions",
  change: "What would change this",
  brief: "Monitor list",
};

function formatNotebookDate(value?: string | null): string {
  const date = value ? new Date(value) : new Date();
  const safeDate = Number.isNaN(date.getTime()) ? new Date() : date;
  return safeDate.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
}

function compactNotebookText(parts: Array<string | null | undefined>): string {
  return parts
    .map((part) => String(part ?? "").trim())
    .filter(Boolean)
    .join(" · ");
}

function notebookForecastText(forecastRefs: Array<{ reference_label?: string; threshold_summary?: string | null; created_at?: string }> | undefined): string {
  return (forecastRefs ?? [])
    .slice(0, 3)
    .map((ref) => compactNotebookText([
      ref.reference_label,
      ref.threshold_summary,
      ref.created_at ? formatNotebookDate(ref.created_at) : null,
    ]))
    .filter(Boolean)
    .join(" | ");
}

function notebookContractEntryToSimple(
  entry: NonNullable<NotebookContract["active_draft"]>,
  fallbackId: string,
): SimpleNotebookEntry {
  return {
    id: entry.entry_id || fallbackId,
    status: entry.status,
    date: formatNotebookDate(entry.date_label || entry.updated_at || entry.created_at),
    linked: entry.linked_object_label || entry.linked_object_id || entry.candidate_id,
    title: entry.title ?? "",
    thesis: entry.thesis ?? "",
    assumptions: entry.assumptions ?? "",
    invalidation: entry.invalidation ?? "",
    watchItems: entry.watch_items ?? "",
    reflections: compactNotebookText([entry.reflections, notebookForecastText(entry.forecast_refs)]),
    nextReview: entry.next_review_date ?? "",
  };
}

function seedSimpleNotebookEntries(candidateId: string, contract: NotebookContract | null | undefined): SimpleNotebookEntry[] {
  if (!contract) return [];
  const entries: SimpleNotebookEntry[] = [];
  if (contract.active_draft) {
    entries.push(notebookContractEntryToSimple(contract.active_draft, `${candidateId}-active`));
  } else {
    entries.push({
      id: `${candidateId}-active`,
      status: "draft",
      date: formatNotebookDate(contract.last_updated_utc || contract.generated_at),
      linked: contract.name || candidateId,
      title: contract.name || "",
      thesis: contract.investment_case || "",
      assumptions: "",
      invalidation: "",
      watchItems: (contract.evidence_sections ?? []).slice(0, 4).map((section) => section.title).join(" · "),
      reflections: "Starter draft seeded from the current notebook contract.",
      nextReview: "",
    });
  }
  (contract.finalized_notes ?? []).forEach((entry, index) => {
    entries.push(notebookContractEntryToSimple(entry, `${candidateId}-finalized-${index}`));
  });
  (contract.archived_notes ?? []).forEach((entry, index) => {
    entries.push(notebookContractEntryToSimple(entry, `${candidateId}-archived-${index}`));
  });
  return entries;
}

function buildNotebookAssistResponse(entry: SimpleNotebookEntry, promptType: NotebookPromptType): string {
  if (promptType === "challenge") {
    return compactNotebookText([
      entry.thesis ? `Pressure-test the thesis against missing evidence: ${entry.thesis}` : "No thesis entered yet.",
      entry.invalidation ? `Compare it with the stated invalidation condition: ${entry.invalidation}` : "Add a concrete invalidation condition before finalizing.",
      entry.watchItems ? `Check whether the watch list is broad enough: ${entry.watchItems}` : "Add at least one external signal to monitor.",
    ]);
  }
  if (promptType === "assumptions") {
    return entry.assumptions
      ? `Weak assumptions to verify: ${entry.assumptions}`
      : "No assumptions have been recorded yet. Add the claims that must stay true for the thesis to hold.";
  }
  if (promptType === "change") {
    return entry.invalidation
      ? `A rational change point is already stated: ${entry.invalidation}`
      : "Define price, evidence, policy, liquidity, or portfolio-fit events that would force a change.";
  }
  return compactNotebookText([
    entry.watchItems ? `Monitor: ${entry.watchItems}` : "Add monitor items before the next review.",
    entry.nextReview ? `Next review: ${entry.nextReview}` : "Set a next review date if this note remains open.",
    entry.reflections ? `Current reflection: ${entry.reflections}` : null,
  ]);
}

function readQueryState(): QueryState {
  const params = new URLSearchParams(window.location.search);
  const maybeView = params.get("view");
  const view: PrimaryView =
    maybeView === "portfolio" ||
    maybeView === "brief" ||
    maybeView === "candidates" ||
    maybeView === "notebook" ||
    maybeView === "evidence"
      ? maybeView
      : "portfolio";
  const tab = params.get("report_tab");
  const reportTab: ReportTab =
    tab === "market_history" ||
    tab === "scenarios" ||
    tab === "risks" ||
    tab === "competition" ||
    tab === "evidence" ||
    tab === "investment_case"
      ? tab
      : "investment_case";
  return {
    view,
    candidateId: params.get("candidate"),
    reportCandidateId: params.get("report"),
    reportTab,
  };
}

function badgeClass(tone?: Tone) {
  if (tone === "good") return "badge badge-good";
  if (tone === "warn") return "badge badge-warn";
  if (tone === "bad") return "badge badge-bad";
  if (tone === "info") return "badge badge-info";
  return "badge";
}

function pillClass(tone?: Tone) {
  if (tone === "good") return "chip chip-green";
  if (tone === "warn") return "chip chip-amber";
  if (tone === "bad") return "chip chip-red";
  if (tone === "info") return "chip chip-blue";
  return "chip";
}

function humanizeCode(value: string | null | undefined) {
  const raw = String(value ?? "").trim();
  if (!raw) return "Unavailable";
  return raw
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

function reportErrorMessage(error: unknown) {
  if (error instanceof ApiRequestError) {
    if (error.developerMessage) {
      console.debug(error.developerMessage);
    }
    return error.userMessage;
  }
  return "Report is temporarily unavailable. Cached content will remain visible if available.";
}

function reportSourceBinding(
  blueprint: BlueprintExplorerContract | null,
  candidateId?: string | null,
  activeSleeveId?: string | null,
): ReportSourceBinding | null {
  if (!blueprint) return null;
  const activeSleeve = activeSleeveId
    ? blueprint.sleeves.find((sleeve) => sleeve.sleeve_id === activeSleeveId) ?? null
    : null;
  const boundSleeve =
    (activeSleeve && candidateId && activeSleeve.candidates.some((candidate) => candidate.candidate_id === candidateId)
      ? activeSleeve
      : null)
    ?? (candidateId
      ? blueprint.sleeves.find((sleeve) => sleeve.candidates.some((candidate) => candidate.candidate_id === candidateId)) ?? null
      : null);
  return {
    sleeveKey: boundSleeve?.sleeve_id ?? null,
    sourceSnapshotId: blueprint.surface_snapshot_id ?? null,
    sourceGeneratedAt: blueprint.generated_at ?? null,
    sourceContractVersion: blueprint.contract_version ?? null,
  };
}

function reportBindingKey(candidateId: string, binding: ReportSourceBinding | null) {
  return [
    candidateId,
    binding?.sleeveKey ?? "unbound_sleeve",
    binding?.sourceSnapshotId ?? "waiting",
    binding?.sourceContractVersion ?? "unknown",
  ].join("::");
}

function isCandidateReportContract(response: CandidateReportResponse): response is CandidateReportContract {
  const value = response as Partial<CandidateReportContract>;
  return value.surface_id === "candidate_report" && typeof value.investment_case === "string";
}

function responseReportState(response: CandidateReportResponse): ReportLoadState {
  const status = String((response as { status?: string }).status ?? "");
  const cacheState = String((response as { report_cache_state?: string | null }).report_cache_state ?? "");
  if (status === "report_pending") return "pending";
  if (status === "report_unavailable") return "unavailable";
  if (status === "stale_cached" || cacheState === "stale_cached") return "stale_cached";
  return "ready";
}

function reportMatchesBinding(contract: CandidateReportContract, binding: ReportSourceBinding | null) {
  if (!binding?.sourceSnapshotId) return true;
  const requestedSleeve = binding.sleeveKey?.replace(/^sleeve_/, "") ?? "";
  const contractSleeve = (contract.sleeve_key ?? contract.sleeve_id ?? "").replace(/^sleeve_/, "");
  return (
    contract.bound_source_snapshot_id === binding.sourceSnapshotId
    && (!requestedSleeve || contractSleeve === requestedSleeve)
    && (!binding.sourceContractVersion || contract.source_contract_version === binding.sourceContractVersion)
  );
}

function querySync(
  view: PrimaryView,
  candidateId: string | null,
  reportCandidateId: string | null,
  reportTab: ReportTab,
) {
  const params = new URLSearchParams(window.location.search);
  params.set("view", view);
  if (candidateId) params.set("candidate", candidateId);
  else params.delete("candidate");
  if (reportCandidateId) {
    params.set("report", reportCandidateId);
    params.set("report_tab", reportTab);
  } else {
    params.delete("report");
    params.delete("report_tab");
  }
  const next = `${window.location.pathname}?${params.toString()}`;
  window.history.replaceState(null, "", next);
}

function findReportSnapshot(
  blueprint: BlueprintExplorerContract | null,
  candidateId: string,
): CandidateReportContract | null {
  if (!blueprint) return null;
  for (const sleeve of blueprint.sleeves) {
    for (const candidate of sleeve.candidates) {
      if (candidate.candidate_id === candidateId && candidate.report_snapshot) {
        return candidate.report_snapshot;
      }
    }
  }
  return null;
}

function currentCompareIdsForSleeve(
  sleeve: { candidates: Array<{ candidate_id?: string | null; id?: string | null }> } | null,
  compareIds: Set<string>,
) {
  if (!sleeve) return [];
  return sleeve.candidates
    .map((candidate) => candidate.candidate_id ?? candidate.id ?? null)
    .filter((candidateId): candidateId is string => Boolean(candidateId))
    .filter((candidateId) => compareIds.has(candidateId))
    .slice(0, 2);
}

function compareRequestKey(sleeveId: string | null | undefined, candidateIds: string[], sourceSnapshotId?: string | null) {
  if (!sleeveId || candidateIds.length < 2) return null;
  return `${sourceSnapshotId ?? "unbound"}::${sleeveId}::${[...candidateIds].sort().join(",")}`;
}

function changesSinceUtc(window: ExplorerChangesWindow): string {
  const now = new Date();
  if (window === "today") {
    const startOfDay = new Date(now);
    startOfDay.setHours(0, 0, 0, 0);
    return startOfDay.toISOString();
  }
  const days = window === "3d" ? 3 : 7;
  const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
  return start.toISOString();
}

function changeWindowLabel(window: ExplorerChangesWindow): string {
  if (window === "today") return "changes today";
  if (window === "3d") return "changes in 3 days";
  if (window === "7d") return "changes in 7 days";
  return "changes in 7 days";
}

function changeCategoryLabel(category: string): string {
  const normalized = normalizeChangeCode(category);
  return ({
    all: "All",
    requires_review: "Requires review",
    upgrades: "Upgrades",
    downgrades: "Downgrades",
    blocker_changes: "Blocker changes",
    evidence: "Evidence",
    sleeve: "Sleeve",
    freshness_risk: "Freshness risk",
    decision: "Decision",
    market_impact: "Market impact",
    portfolio_drift: "Portfolio drift",
    source_evidence: "Source evidence",
    blocker: "Blocker",
    timing: "Timing",
    audit_only: "Audit only",
    system: "System",
  } as Record<string, string>)[normalized] ?? humanizeCode(normalized);
}

function normalizeChangeCode(value: string | null | undefined): string {
  return String(value ?? "").trim().toLowerCase();
}

function changeMatchesCategory(
  change: BlueprintDisplay["changes"][number],
  category: ExplorerChangesType,
): boolean {
  if (category === "all") return true;
  if (category === "audit_only") return isCompactAuditChange(change);
  const eventType = normalizeChangeCode(change.eventType);
  const eventCategory = normalizeChangeCode(change.category);
  const previousState = normalizeChangeCode(change.previousState);
  const currentState = normalizeChangeCode(change.currentState);
  const narrative = normalizeChangeCode(
    `${change.typeLabel} ${change.implication} ${change.currentState} ${change.previousState}`
  );

  if (category === "requires_review") {
    return change.needsReview;
  }

  if (category === "blocker_changes") {
    return /blocker|boundary/.test(eventType) || /blocker|blocked/.test(narrative);
  }

  if (category === "evidence") {
    return (
      /truth_change|evidence|document|mapping|claim|tax_assumption|source/.test(eventType)
      || /evidence|source|document|truth/.test(narrative)
    );
  }

  if (category === "sleeve") {
    return /portfolio_fit|sleeve|allocation|capital/.test(eventType) || /sleeve|allocation/.test(narrative);
  }

  if (category === "freshness_risk") {
    return /freshness/.test(eventType) || /fresh|stale|aging|degraded/.test(narrative);
  }

  if (category === "upgrades") {
    return (
      /strengthened|resolved|created|added/.test(eventType)
      || /eligible|sufficient|strengthened|improved|cleared|resolved|now/.test(currentState)
      || /upgrade|eligible now|evidence sufficient|support strengthened/.test(narrative)
    ) && !/blocked|weakened|aging/.test(narrative);
  }

  if (category === "downgrades") {
    return (
      /weakened|opened|blocker|boundary|freshness_risk/.test(eventType)
      || /blocked|hold|weakened|aging|stale|conflict|risk/.test(currentState)
      || /downgrade|blocked|aging|freshness|weakened|conflict/.test(narrative)
    );
  }

  return eventCategory === category || eventType === category;
}

function detailText(value: string | null | undefined) {
  return String(value ?? "").trim();
}

function changeClosureText(value: string | null | undefined) {
  const normalized = detailText(value).toLowerCase();
  if (normalized === "stale_historical") return "Historical context, not a current action.";
  if (normalized === "unresolved_driver_missing") return "Driver not preserved; treat as review context only.";
  if (normalized === "closed_no_action") return "No current portfolio action.";
  if (normalized === "open_actionable") return "Open action item.";
  if (normalized === "open_review") return "Open review item.";
  return "";
}

function changeAgeText(hours: number | null | undefined) {
  if (typeof hours !== "number" || Number.isNaN(hours)) return "";
  if (hours < 1) return "Current scan";
  if (hours < 24) return `${Math.round(hours)}h old`;
  return `${Math.round(hours / 24)}d old`;
}

function changeRenderMode(change: ChangeDisplay) {
  return detailText(change.renderMode ?? change.changeDetail?.render_mode).toLowerCase();
}

function isCompactAuditChange(change: ChangeDisplay) {
  const mode = changeRenderMode(change);
  const materiality = detailText(change.changeDetail?.materiality_status).toLowerCase();
  const materialityClass = detailText(change.materialityClass ?? change.changeDetail?.materiality_class).toLowerCase();
  return (
    mode === "compact_audit"
    || mode === "grouped_audit"
    || mode === "hidden_audit"
    || materialityClass === "audit_only"
    || materiality === "unresolved_driver_missing"
    || materiality === "raw_movement_only"
  );
}

function isFullInvestorChange(change: ChangeDisplay) {
  const mode = changeRenderMode(change);
  return mode === "full_investor_explanation" || mode === "full_investor";
}

function formatChangeTransition(change: ChangeDisplay) {
  const detail = change.changeDetail;
  const from = detailText(detail?.state_transition?.from) || detailText(change.previousState);
  const to = detailText(detail?.state_transition?.to) || detailText(change.currentState);
  if (!from && !to) return null;
  return `${from || "Prior state"} to ${to || "Current state"}.`;
}

function formatScoreMovement(change: ChangeDisplay) {
  const from = change.changeDetail?.score_delta?.from;
  const to = change.changeDetail?.score_delta?.to;
  if (typeof from === "number" || typeof to === "number") {
    return `${from ?? "n/a"} -> ${to ?? "n/a"}`;
  }
  return null;
}

function formatTriggerLine(change: ChangeDisplay) {
  const detail = change.changeDetail;
  return (
    detailText(detail?.primary_trigger?.display_label)
    || detailText(detail?.trigger)
    || detailText(detail?.driver_packet?.driver_summary)
    || null
  );
}

function formatImpactLine(change: ChangeDisplay) {
  const detail = change.changeDetail;
  return (
    detailText(detail?.candidate_impact?.why_it_matters)
    || detailText(detail?.portfolio_consequence)
    || detailText(detail?.summary)
    || change.whyItMatters
    || change.implication
  );
}

function ChangeEventDetailCard({
  change,
  onOpenRecommendation,
  onOpenReport,
}: {
  change: ChangeDisplay;
  onOpenRecommendation: (change: ChangeDisplay) => void;
  onOpenReport: (change: ChangeDisplay) => void;
}) {
  const detail = change.changeDetail;
  const transition = formatChangeTransition(change);
  const scoreMovement = formatScoreMovement(change);
  if (!detail) {
    return (
      <div className="change-detail-card" onClick={(event) => event.stopPropagation()}>
        <div className="change-detail-empty">Change detail is not available for this event yet.</div>
      </div>
    );
  }

  if (isCompactAuditChange(change)) {
    const auditDetail = change.auditDetail ?? detail.audit_detail ?? null;
    return (
      <div className="change-detail-card compact-audit-detail" onClick={(event) => event.stopPropagation()}>
        <div className="compact-audit-title">Audit context only</div>
        <div className="compact-audit-body">
          {auditDetail?.audit_summary || detail.summary || "Historical review movement. The prior ledger records a review state change, but the source driver was not preserved. Treat this as audit context, not an investment signal."}
        </div>
        <div className="compact-audit-meta">
          {transition ? <span>{transition}</span> : null}
          {detail.closure_status ? <span>{changeClosureText(detail.closure_status)}</span> : null}
          {auditDetail?.original_event_type ? <span>{humanizeCode(auditDetail.original_event_type)}</span> : null}
          {scoreMovement ? <span>Score {scoreMovement}</span> : null}
        </div>
      </div>
    );
  }

  const rows = [
    { label: "State transition", value: transition },
    { label: "Trigger", value: detail.trigger },
    { label: "Source evidence", value: detail.source_evidence },
    { label: "Reason", value: detail.reason },
    { label: "Portfolio consequence", value: detail.portfolio_consequence },
    { label: "Next action", value: detail.next_action },
    { label: "What would reverse", value: detail.reversal_condition ?? detail.reversal_conditions },
    { label: "Closure", value: changeClosureText(detail.closure_status) },
    { label: "Score", value: scoreMovement },
  ].filter((row): row is { label: string; value: string } => Boolean(detailText(row.value)));

  return (
    <div className="change-detail-card" onClick={(event) => event.stopPropagation()}>
      <div className="change-detail-grid">
        {rows.map((row) => (
          <div className="change-detail-row" key={`${change.id}-${row.label}`}>
            <span>{row.label}</span>
            <strong>{row.value}</strong>
          </div>
        ))}
      </div>
      <div className="change-detail-actions">
        <button
          className="action-btn"
          type="button"
          disabled={!change.candidateId && !detail.affected_candidate?.candidate_id}
          onClick={(event) => {
            event.stopPropagation();
            onOpenRecommendation(change);
          }}
        >
          {"Open ETF -> Recommendation"}
        </button>
        <button
          className="action-btn action-btn-text"
          type="button"
          disabled={!change.candidateId && !detail.affected_candidate?.candidate_id}
          onClick={(event) => {
            event.stopPropagation();
            onOpenReport(change);
          }}
        >
          {"Open Report: Investment Case"}
        </button>
      </div>
    </div>
  );
}

function App() {
  const initial = readQueryState();
  const [view, setView] = useState<PrimaryView>(initial.view);
  const [apiOk, setApiOk] = useState<boolean | null>(null);
  const [activeSleeveId, setActiveSleeveId] = useState<string | null>(null);
  const [selectedCandidateId, setSelectedCandidateId] = useState<string | null>(initial.candidateId);
  const [reportCandidateId, setReportCandidateId] = useState<string | null>(initial.reportCandidateId);
  const [reportTab, setReportTab] = useState<ReportTab>(initial.reportTab);
  const [expandedCandidateId, setExpandedCandidateId] = useState<string | null>(null);
  const [expandedSignalId, setExpandedSignalId] = useState<string | null>(null);
  const reportInFlight = useRef<Record<string, boolean>>({});
  const forecastDeferredStartRequested = useRef(false);
  const portfolioUploadInputRef = useRef<HTMLInputElement | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [portfolioUploadMessage, setPortfolioUploadMessage] = useState<string | null>(null);
  const [portfolioUploadError, setPortfolioUploadError] = useState<string | null>(null);
  const [portfolioUploading, setPortfolioUploading] = useState(false);
  const [compareIds, setCompareIds] = useState<Set<string>>(new Set());
  const [comparePanelOpen, setComparePanelOpen] = useState(false);
  const [compareKey, setCompareKey] = useState<string | null>(null);
  const [changesWindow, setChangesWindow] = useState<ExplorerChangesWindow>("today");
  const [changesTypeFilter, setChangesTypeFilter] = useState<ExplorerChangesType>("all");
  const [changesSleeveFilter, setChangesSleeveFilter] = useState<string>("all");
  const [showAllExplorerChanges, setShowAllExplorerChanges] = useState(false);
  const [expandedChangeId, setExpandedChangeId] = useState<string | null>(null);

  const [blueprint, setBlueprint] = useState<Status<BlueprintExplorerContract>>(() => createInitialStatus<BlueprintExplorerContract>(initial.view === "candidates"));
  const [portfolio, setPortfolio] = useState<Status<PortfolioContract>>(() => createInitialStatus<PortfolioContract>(initial.view === "portfolio"));
  const [brief, setBrief] = useState<Status<DailyBriefContract>>(() => createInitialStatus<DailyBriefContract>(initial.view === "brief"));
  const [changes, setChanges] = useState<Status<ChangesContract>>(() => createStatus<ChangesContract>());
  const [blueprintTodayChanges, setBlueprintTodayChanges] = useState<Status<ChangesContract>>(() => createStatus<ChangesContract>());
  const [compare, setCompare] = useState<Status<CompareContract>>(() => createStatus<CompareContract>());
  const [coverageAudit, setCoverageAudit] = useState<Status<BlueprintCoverageAuditContract>>(() => createStatus<BlueprintCoverageAuditContract>());
  const [reportCache, setReportCache] = useState<Record<string, ReportStatus>>({});
  const [notebookCache, setNotebookCache] = useState<Record<string, Status<NotebookContract>>>({});
  const [simpleNotebookEntriesByCandidate, setSimpleNotebookEntriesByCandidate] = useState<Record<string, SimpleNotebookEntry[]>>({});
  const [notebookAssistResponses, setNotebookAssistResponses] = useState<Record<string, NotebookAssistResponse>>({});
  const [evidenceCache, setEvidenceCache] = useState<Record<string, Status<EvidenceWorkspaceContract>>>({});

  useEffect(() => {
    let cancelled = false;
    let timer: number | null = null;
    let lastOk = false;

    const pollHealth = () => {
      fetchHealth()
        .then(() => {
          if (cancelled) return;
          lastOk = true;
          setApiOk(true);
        })
        .catch(() => {
          if (cancelled) return;
          lastOk = false;
          setApiOk(false);
        })
        .finally(() => {
          if (cancelled) return;
          timer = window.setTimeout(pollHealth, lastOk ? 30_000 : 3_000);
        });
    };

    pollHealth();
    return () => {
      cancelled = true;
      if (timer !== null) {
        window.clearTimeout(timer);
      }
    };
  }, []);

  useEffect(() => {
    querySync(view, selectedCandidateId, reportCandidateId, reportTab);
  }, [view, selectedCandidateId, reportCandidateId, reportTab]);

  async function loadBlueprint() {
    const cached = readCachedBlueprint();
    setBlueprint((state) => ({
      data: state.data ?? cached,
      loading: true,
      error: null,
    }));
    try {
      const data = await fetchBlueprintExplorer();
      writeCachedBlueprint(data);
      setBlueprint({ data, loading: false, error: null });
    } catch (error) {
      setBlueprint((state) => ({
        data: state.data ?? cached,
        loading: false,
        error: state.data || cached
          ? "Showing the last loaded Blueprint while the live surface reconnects."
          : errorMessage(error, "Blueprint fetch failed."),
      }));
    }
  }

  async function loadPortfolio() {
    setPortfolio((state) => ({ ...state, loading: true, error: null }));
    try {
      const data = await fetchPortfolio("default");
      setPortfolio({ data, loading: false, error: null });
    } catch (error) {
      setPortfolio({ data: null, loading: false, error: errorMessage(error, "Portfolio fetch failed.") });
    }
  }

  async function handlePortfolioUpload(file: File | null) {
    if (!file) return;
    setPortfolioUploadError(null);
    setPortfolioUploadMessage(null);
    setPortfolioUploading(true);
    try {
      const csvText = await file.text();
      const upload = await uploadPortfolioHoldings(csvText, file.name);
      await activatePortfolioUpload(upload.run_id);
      await loadPortfolio();
      setPortfolioUploadMessage(`${file.name} uploaded and activated.`);
    } catch (error) {
      setPortfolioUploadError(errorMessage(error, "Portfolio upload failed."));
    } finally {
      setPortfolioUploading(false);
      if (portfolioUploadInputRef.current) {
        portfolioUploadInputRef.current.value = "";
      }
    }
  }

  async function loadBrief(force = false) {
    setBrief((state) => ({ ...state, loading: true, error: null }));
    try {
      const data = await fetchDailyBrief({ force });
      setBrief({ data, loading: false, error: null });
    } catch (error) {
      setBrief({ data: null, loading: false, error: errorMessage(error, "Daily Brief fetch failed.") });
    }
  }

  async function loadChanges(window: ExplorerChangesWindow = changesWindow) {
    setChanges((state) => ({ ...state, loading: true, error: null }));
    try {
      const data = await fetchChanges("blueprint_explorer", {
        window,
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Singapore",
        category: changesTypeFilter,
        sleeveId: changesSleeveFilter,
        limit: showAllExplorerChanges ? 50 : 6,
      });
      setChanges({ data, loading: false, error: null });
    } catch (error) {
      setChanges({
        data: null,
        loading: false,
        error: errorMessage(error, "Changes fetch failed."),
      });
    }
  }

  async function loadBlueprintTodayChanges() {
    setBlueprintTodayChanges((state) => ({ ...state, loading: true, error: null }));
    try {
      const data = await fetchChanges("blueprint_explorer", {
        window: "today",
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Singapore",
        category: "all",
        sleeveId: "all",
        limit: 1,
      });
      setBlueprintTodayChanges({ data, loading: false, error: null });
    } catch (error) {
      setBlueprintTodayChanges({
        data: null,
        loading: false,
        error: errorMessage(error, "Changes fetch failed."),
      });
    }
  }

  async function loadCoverageAudit() {
    setCoverageAudit((state) => ({ ...state, loading: true, error: null }));
    try {
      const data = await fetchBlueprintCoverageAudit();
      setCoverageAudit({ data, loading: false, error: null });
    } catch (error) {
      setCoverageAudit({ data: null, loading: false, error: errorMessage(error, "Coverage audit fetch failed.") });
    }
  }

  useEffect(() => {
    const tasks: Array<Promise<unknown>> = [];
    if (initial.view === "candidates") tasks.push(loadBlueprint());
    if (initial.view === "portfolio") tasks.push(loadPortfolio());
    if (initial.view === "brief") tasks.push(loadBrief());
    if (initial.reportCandidateId) {
      tasks.push(ensureReport(initial.reportCandidateId));
    }
    void Promise.all(tasks);
  }, []);

  useEffect(() => {
    if (view === "candidates" && !blueprint.data && !blueprint.loading && !blueprint.error) {
      void loadBlueprint();
      return;
    }
    if (view === "portfolio" && !portfolio.data && !portfolio.loading && !portfolio.error) {
      void loadPortfolio();
      return;
    }
    if (view === "brief" && !brief.data && !brief.loading && !brief.error) {
      void loadBrief();
    }
  }, [view, blueprint.data, blueprint.loading, blueprint.error, portfolio.data, portfolio.loading, portfolio.error, brief.data, brief.loading, brief.error]);

  useEffect(() => {
    if (view !== "candidates") return;
    if (blueprint.data || blueprint.loading || !blueprint.error) return;
    const retryDelay = apiOk ? 500 : 4_000;
    const timer = window.setTimeout(() => {
      void loadBlueprint();
    }, retryDelay);
    return () => window.clearTimeout(timer);
  }, [view, apiOk, blueprint.data, blueprint.loading, blueprint.error]);

  useEffect(() => {
    if (view !== "candidates") return;
    if (!blueprint.data) return;
    void loadChanges(changesWindow);
  }, [view, blueprint.data?.surface_snapshot_id, changesWindow, changesTypeFilter, changesSleeveFilter, showAllExplorerChanges]);

  useEffect(() => {
    if (view !== "candidates") return;
    if (!blueprint.data) return;
    void loadBlueprintTodayChanges();
  }, [view, blueprint.data?.surface_snapshot_id]);

  useEffect(() => {
    const surfaceReady =
      Boolean(portfolio.data)
      || Boolean(brief.data)
      || Boolean(blueprint.data && !changes.loading && !blueprintTodayChanges.loading);
    if (!apiOk || !surfaceReady || forecastDeferredStartRequested.current) return;
    forecastDeferredStartRequested.current = true;
    void requestDeferredForecastStart().catch((error) => {
      forecastDeferredStartRequested.current = false;
      if (error instanceof Error) {
        console.debug(error.message);
      }
    });
  }, [
    apiOk,
    portfolio.data,
    brief.data,
    blueprint.data?.surface_snapshot_id,
    changes.loading,
    blueprintTodayChanges.loading,
  ]);

  useEffect(() => {
    setChangesTypeFilter("all");
    setChangesSleeveFilter("all");
    setShowAllExplorerChanges(false);
    setExpandedChangeId(null);
  }, [changesWindow]);

  useEffect(() => {
    if (!blueprint.data) return;
    const activeSleeveById =
      activeSleeveId
        ? blueprint.data.sleeves.find((sleeve) => sleeve.sleeve_id === activeSleeveId) ?? null
        : null;
    const reportCandidateSleeves =
      reportCandidateId
        ? blueprint.data.sleeves.filter((sleeve) =>
            sleeve.candidates.some((candidate) => candidate.candidate_id === reportCandidateId)
          )
        : [];
    const reportSleeve =
      (activeSleeveById && reportCandidateId && activeSleeveById.candidates.some((candidate) => candidate.candidate_id === reportCandidateId)
        ? activeSleeveById
        : null)
      ?? reportCandidateSleeves[0]
      ?? null;
    const selectedCandidateSleeves =
      selectedCandidateId
        ? blueprint.data.sleeves.filter((sleeve) =>
            sleeve.candidates.some((candidate) => candidate.candidate_id === selectedCandidateId)
          )
        : [];
    const selectedCandidateSleeve =
      (activeSleeveById && selectedCandidateId && activeSleeveById.candidates.some((candidate) => candidate.candidate_id === selectedCandidateId)
        ? activeSleeveById
        : null)
      ?? selectedCandidateSleeves[0]
      ?? null;
    const activeSleeve =
      activeSleeveById ??
      reportSleeve ??
      selectedCandidateSleeve ??
      blueprint.data.sleeves[0] ??
      null;
    if (!activeSleeve) return;
    if (reportSleeve && activeSleeveId !== reportSleeve.sleeve_id) {
      setActiveSleeveId(reportSleeve.sleeve_id);
      return;
    }
    if (!reportSleeve && !activeSleeveById && selectedCandidateSleeve && activeSleeveId !== selectedCandidateSleeve.sleeve_id) {
      setActiveSleeveId(selectedCandidateSleeve.sleeve_id);
      return;
    }
    if (!activeSleeveId || !activeSleeveById) {
      setActiveSleeveId(activeSleeve.sleeve_id);
      return;
    }
    if (reportCandidateId && selectedCandidateId !== reportCandidateId) {
      setSelectedCandidateId(reportCandidateId);
      return;
    }
    if (!selectedCandidateId || !activeSleeve.candidates.some((candidate) => candidate.candidate_id === selectedCandidateId)) {
      const nextCandidate = activeSleeve.candidates[0]?.candidate_id ?? null;
      setSelectedCandidateId(nextCandidate);
    }
  }, [blueprint.data, activeSleeveId, selectedCandidateId, reportCandidateId]);

  async function ensureReport(candidateId: string, force = false) {
    const binding = reportSourceBinding(blueprint.data, candidateId, activeSleeveId);
    const cacheKey = reportBindingKey(candidateId, binding);
    if (!binding?.sourceSnapshotId) {
      setReportCache((state) => ({
        ...state,
        [cacheKey]: {
          data: state[cacheKey]?.data ?? null,
          loading: Boolean(blueprint.loading),
          error: null,
          state: "idle",
          userMessage: "Waiting for the Explorer snapshot before loading the bound report.",
          bindingKey: cacheKey,
        },
      }));
      return;
    }
    if (reportInFlight.current[cacheKey]) return;
    const cached = reportCache[cacheKey];
    if (cached?.data && !force && reportMatchesBinding(cached.data, binding)) return;
    if (cached?.state === "pending" && !force) return;
    const snapshot = findReportSnapshot(blueprint.data, candidateId);
    if (snapshot && reportMatchesBinding(snapshot, binding) && !force) {
      setReportCache((state) => ({
        ...state,
        [cacheKey]: {
          data: snapshot,
          loading: false,
          error: null,
          state: "ready",
          userMessage: null,
          bindingKey: cacheKey,
        },
      }));
      return;
    }
    reportInFlight.current[cacheKey] = true;
    setReportCache((state) => ({
      ...state,
      [cacheKey]: {
        data: state[cacheKey]?.data ?? null,
        loading: true,
        error: null,
        state: "loading",
        userMessage: state[cacheKey]?.userMessage ?? null,
        bindingKey: cacheKey,
      },
    }));
    try {
      const data = await fetchCandidateReport(candidateId, {
        sleeveKey: binding.sleeveKey,
        sourceSnapshotId: binding.sourceSnapshotId,
        sourceGeneratedAt: binding.sourceGeneratedAt,
        sourceContractVersion: binding.sourceContractVersion,
        refresh: force,
      });
      const nextState = responseReportState(data);
      const userMessage = "message" in data ? data.message ?? null : null;
      const retryAfterMs = "retry_after_ms" in data ? data.retry_after_ms ?? null : null;
      setReportCache((state) => ({
        ...state,
        [cacheKey]: {
          data: isCandidateReportContract(data) ? data : state[cacheKey]?.data ?? null,
          loading: false,
          error: null,
          state: nextState,
          userMessage,
          bindingKey: cacheKey,
          retryAfterMs,
        },
      }));
    } catch (error) {
      const userMessage = reportErrorMessage(error);
      setReportCache((state) => ({
        ...state,
        [cacheKey]: (() => {
          const fallbackSnapshot = snapshot && reportMatchesBinding(snapshot, binding) ? snapshot : null;
          const fallbackData = state[cacheKey]?.data ?? fallbackSnapshot;
          return {
            data: fallbackData,
            loading: false,
            error: userMessage,
            state: fallbackData ? "stale_cached" : "error",
            userMessage,
            developerMessage: error instanceof ApiRequestError ? error.developerMessage : null,
            bindingKey: cacheKey,
          };
        })(),
      }));
    } finally {
      reportInFlight.current[cacheKey] = false;
    }
  }

  async function ensureNotebook(candidateId: string, force = false) {
    const cached = notebookCache[candidateId];
    if (cached?.data && !force) return;
    setNotebookCache((state) => ({ ...state, [candidateId]: { data: state[candidateId]?.data ?? null, loading: true, error: null } }));
    try {
      const data = await fetchNotebook(candidateId);
      setNotebookCache((state) => ({ ...state, [candidateId]: { data, loading: false, error: null } }));
    } catch (error) {
      setNotebookCache((state) => ({
        ...state,
        [candidateId]: { data: state[candidateId]?.data ?? null, loading: false, error: errorMessage(error, "Notebook fetch failed.") },
      }));
    }
  }

  async function ensureEvidence(candidateId: string, force = false) {
    const cached = evidenceCache[candidateId];
    if (cached?.data && !force) return;
    setEvidenceCache((state) => ({ ...state, [candidateId]: { data: state[candidateId]?.data ?? null, loading: true, error: null } }));
    try {
      const data = await fetchEvidenceWorkspace(candidateId);
      setEvidenceCache((state) => ({ ...state, [candidateId]: { data, loading: false, error: null } }));
    } catch (error) {
      setEvidenceCache((state) => ({
        ...state,
        [candidateId]: { data: state[candidateId]?.data ?? null, loading: false, error: errorMessage(error, "Evidence fetch failed.") },
      }));
    }
  }

  useEffect(() => {
    if (reportCandidateId) {
      void ensureReport(reportCandidateId);
    }
  }, [reportCandidateId, blueprint.data?.surface_snapshot_id, blueprint.data?.generated_at, blueprint.data?.contract_version]);

  useEffect(() => {
    if (view !== "candidates") return;
    if (!expandedCandidateId && !reportCandidateId) return;
    if (coverageAudit.data || coverageAudit.loading) return;
    void loadCoverageAudit();
  }, [view, expandedCandidateId, reportCandidateId, coverageAudit.data, coverageAudit.loading]);

  useEffect(() => {
    if (view === "notebook" && selectedCandidateId) {
      void ensureNotebook(selectedCandidateId);
    }
  }, [view, selectedCandidateId]);

  useEffect(() => {
    if (view === "evidence" && selectedCandidateId) {
      void ensureEvidence(selectedCandidateId);
    }
  }, [view, selectedCandidateId]);

  useEffect(() => {
    const activeSleeve =
      blueprint.data?.sleeves.find((sleeve) => sleeve.sleeve_id === activeSleeveId) ??
      blueprint.data?.sleeves[0] ??
      null;
    const activeCompareIds = currentCompareIdsForSleeve(activeSleeve, compareIds);
    const nextKey = compareRequestKey(activeSleeve?.sleeve_id, activeCompareIds, blueprint.data?.surface_snapshot_id);
    if (!nextKey) {
      setCompare({ data: null, loading: false, error: null });
      setCompareKey(null);
      setComparePanelOpen(false);
      return;
    }
    if (compareKey && nextKey !== compareKey) {
      setCompare({ data: null, loading: false, error: null });
      setCompareKey(null);
      if (comparePanelOpen) {
        setComparePanelOpen(false);
      }
    }
  }, [blueprint.data, activeSleeveId, compareIds, compareKey, comparePanelOpen]);

  async function refreshSurface() {
    setRefreshing(true);
    try {
      if (view === "candidates") {
        await loadBlueprint();
        void loadChanges(changesWindow);
      }
      const activeSleeve =
        blueprint.data?.sleeves.find((sleeve) => sleeve.sleeve_id === activeSleeveId) ??
        blueprint.data?.sleeves[0] ??
        null;
      const activeCompareIds = activeSleeve
        ? activeSleeve.candidates
            .map((candidate) => candidate.candidate_id)
            .filter((candidateId) => compareIds.has(candidateId))
            .slice(0, 2)
          : [];
      await Promise.all([
        ...(view === "portfolio" ? [loadPortfolio()] : []),
        ...(view === "brief" ? [loadBrief(true)] : []),
        ...(view === "candidates" && (expandedCandidateId || reportCandidateId) ? [loadCoverageAudit()] : []),
        ...(comparePanelOpen && activeSleeve && activeCompareIds.length >= 2
          ? [fetchCompare(activeCompareIds, activeSleeve.sleeve_id).then((data) => setCompare({ data, loading: false, error: null }))]
          : []),
        ...(reportCandidateId ? [ensureReport(reportCandidateId, true)] : []),
        ...(selectedCandidateId && view === "notebook" ? [ensureNotebook(selectedCandidateId, true)] : []),
        ...(selectedCandidateId && view === "evidence" ? [ensureEvidence(selectedCandidateId, true)] : []),
      ]);
    } finally {
      setRefreshing(false);
    }
  }

  function openReport(candidateId: string, tab: ReportTab = "investment_case") {
    setSelectedCandidateId(candidateId);
    setExpandedCandidateId(candidateId);
    setReportCandidateId(candidateId);
    setReportTab(tab);
    void ensureReport(candidateId);
  }

  function openChangeRecommendation(change: ChangeDisplay) {
    const candidateId = change.changeDetail?.affected_candidate?.candidate_id ?? change.candidateId;
    if (!candidateId) return;
    const sleeveId = change.changeDetail?.affected_candidate?.sleeve_id ?? change.sleeveId;
    if (sleeveId) {
      setActiveSleeveId(sleeveId);
    }
    setSelectedCandidateId(candidateId);
    setExpandedCandidateId(candidateId);
    window.setTimeout(() => {
      document
        .getElementById(`candidate-row-${candidateId}`)
        ?.scrollIntoView({ behavior: "smooth", block: "center" });
    }, 80);
  }

  function openChangeReport(change: ChangeDisplay) {
    const candidateId = change.changeDetail?.affected_candidate?.candidate_id ?? change.candidateId;
    if (!candidateId) return;
    openReport(candidateId, (change.reportTab as ReportTab) ?? "investment_case");
  }

  function openComparePanel(sleeve: BlueprintSleeveDisplay) {
    const activeCompareIds = currentCompareIdsForSleeve(sleeve, compareIds);
    const nextKey = compareRequestKey(sleeve.id, activeCompareIds, blueprint.data?.surface_snapshot_id);
    if (!nextKey || activeCompareIds.length < 2) return;
    setComparePanelOpen(true);
    if (compareKey === nextKey && (compare.data || compare.loading)) {
      return;
    }
    setCompareKey(nextKey);
    setCompare({ data: null, loading: true, error: null });
    void fetchCompare(activeCompareIds, sleeve.id)
      .then((data) => {
        setCompareKey(nextKey);
        setCompare({ data, loading: false, error: null });
      })
      .catch((error) => {
        setCompareKey(nextKey);
        setCompare({ data: null, loading: false, error: errorMessage(error, "Compare fetch failed.") });
      });
  }

  function toggleCompare(candidateId: string) {
    setCompareIds((prev) => {
      const next = new Set(prev);
      if (next.has(candidateId)) next.delete(candidateId);
      else if (next.size < 2) next.add(candidateId);
      return next;
    });
  }
  function clearCompare() {
    setCompareIds(new Set());
    setCompare({ data: null, loading: false, error: null });
    setCompareKey(null);
    setComparePanelOpen(false);
  }

  const currentReportBinding = reportSourceBinding(blueprint.data, reportCandidateId, activeSleeveId);
  const activeReportCacheKey = reportCandidateId ? reportBindingKey(reportCandidateId, currentReportBinding) : null;
  const reportStatus = activeReportCacheKey ? reportCache[activeReportCacheKey] : undefined;
  const reportSnapshot = reportCandidateId ? findReportSnapshot(blueprint.data, reportCandidateId) : null;
  const compatibleReportSnapshot = reportSnapshot && reportMatchesBinding(reportSnapshot, currentReportBinding) ? reportSnapshot : null;

  const portfolioDisplay = portfolio.data ? adaptPortfolio(portfolio.data, blueprint.data, brief.data) : null;
  const briefDisplay = brief.data ? adaptDailyBrief(brief.data) : null;
  const reportDisplay = reportStatus?.data
    ? adaptCandidateReport(reportStatus.data)
    : compatibleReportSnapshot
      ? adaptCandidateReport(compatibleReportSnapshot)
      : null;
  const blueprintDisplay = blueprint.data
    ? adaptBlueprint(
        blueprint.data,
        compare.data,
        changes.data,
        reportStatus?.data ?? compatibleReportSnapshot,
        selectedCandidateId,
        activeSleeveId,
      )
    : null;
  const selectedNotebookContract = selectedCandidateId ? notebookCache[selectedCandidateId]?.data ?? null : null;
  const notebookDisplay =
    selectedCandidateId && selectedNotebookContract
      ? adaptNotebook(selectedNotebookContract)
      : null;
  const evidenceDisplay =
    selectedCandidateId && evidenceCache[selectedCandidateId]?.data
      ? adaptEvidence(evidenceCache[selectedCandidateId].data as EvidenceWorkspaceContract)
      : null;

  const fallbackNavMeta = NAV.find((item) => item.id === view);
  const fallbackMeta = {
    kicker: fallbackNavMeta?.kicker ?? "Investor workflow",
    title: fallbackNavMeta?.title ?? "CORTEX",
    copy:
      view === "candidates" && blueprint.error
        ? "Reconnecting to the live Blueprint surface."
        : apiOk === false
          ? "Waiting for the V2 API to reconnect."
          : "Loading the selected investor surface.",
    badges: [
      {
        label: apiOk === false ? "API reconnecting" : "Loading",
        tone: apiOk === false ? "bad" as Tone : "neutral" as Tone,
      },
    ],
  };
  const activeSurfaceMeta =
    view === "portfolio"
      ? portfolioDisplay?.meta
      : view === "brief"
        ? briefDisplay?.meta
        : view === "candidates"
          ? blueprintDisplay?.meta
        : view === "notebook"
          ? notebookDisplay?.meta
          : evidenceDisplay?.meta;
  const currentMeta = activeSurfaceMeta ?? fallbackMeta;
  const blueprintActiveCandidateCount = blueprintDisplay
    ? blueprintDisplay.summary?.active_candidate_count
      ?? blueprintDisplay.sleeves.reduce((total, sleeve) => total + (sleeve.candidateCount ?? sleeve.candidates.length), 0)
    : null;
  const blueprintActiveChangesTodayCount =
    blueprintTodayChanges.data?.summary?.total_changes
    ?? (changesWindow === "today" ? changes.data?.summary?.total_changes ?? null : null);
  const topbarBadges = [
    ...(currentMeta?.badges ?? []),
    ...(view === "candidates"
      ? [
          {
            label: blueprintTodayChanges.loading && blueprintActiveChangesTodayCount === null
              ? "Loading active changes"
              : `${blueprintActiveChangesTodayCount ?? 0} active change${(blueprintActiveChangesTodayCount ?? 0) === 1 ? "" : "s"} today`,
            tone: (blueprintActiveChangesTodayCount ?? 0) > 0 ? "warn" as Tone : "good" as Tone,
          },
          {
            label: blueprintActiveCandidateCount === null
              ? "Loading active candidates"
              : `${blueprintActiveCandidateCount} active candidate${blueprintActiveCandidateCount === 1 ? "" : "s"}`,
            tone: "info" as Tone,
          },
        ]
      : []),
  ];

  const allBlueprintCandidates = blueprintDisplay?.sleeves.flatMap((sleeve) => sleeve.candidates) ?? [];
  const reportBlueprintCandidate = reportCandidateId
    ? allBlueprintCandidates.find((candidate) => candidate.id === reportCandidateId) ?? null
    : null;
  const reportBlueprintSleeve = reportCandidateId
    ? blueprintDisplay?.sleeves.find((sleeve) => sleeve.candidates.some((candidate) => candidate.id === reportCandidateId)) ?? null
    : null;

  useEffect(() => {
    if (!selectedCandidateId || !selectedNotebookContract) return;
    setSimpleNotebookEntriesByCandidate((state) => {
      if (Object.prototype.hasOwnProperty.call(state, selectedCandidateId)) return state;
      return {
        ...state,
        [selectedCandidateId]: seedSimpleNotebookEntries(selectedCandidateId, selectedNotebookContract),
      };
    });
  }, [selectedCandidateId, selectedNotebookContract]);

  useEffect(() => {
    if (!reportCandidateId || !activeReportCacheKey) return;
    if (reportStatus?.state !== "pending") return;
    const retryAfter = Math.max(750, reportStatus.retryAfterMs ?? 1500);
    const timer = window.setTimeout(() => {
      void ensureReport(reportCandidateId, true);
    }, retryAfter);
    return () => window.clearTimeout(timer);
  }, [reportCandidateId, activeReportCacheKey, reportStatus?.state, reportStatus?.retryAfterMs]);

  function updateNotebookEntry(entryId: string, updates: Partial<Omit<SimpleNotebookEntry, "id" | "status">>) {
    if (!selectedCandidateId) return;
    setSimpleNotebookEntriesByCandidate((state) => ({
      ...state,
      [selectedCandidateId]: (state[selectedCandidateId] ?? []).map((entry) =>
        entry.id === entryId ? { ...entry, ...updates, dirty: true } : entry,
      ),
    }));
  }

  function saveNotebookDraft(entryId: string) {
    if (!selectedCandidateId) return;
    setSimpleNotebookEntriesByCandidate((state) => ({
      ...state,
      [selectedCandidateId]: (state[selectedCandidateId] ?? []).map((entry) =>
        entry.id === entryId ? { ...entry, dirty: false } : entry,
      ),
    }));
  }

  function finalizeNotebookEntry(entryId: string) {
    if (!selectedCandidateId) return;
    setSimpleNotebookEntriesByCandidate((state) => ({
      ...state,
      [selectedCandidateId]: (state[selectedCandidateId] ?? []).map((entry) =>
        entry.id === entryId
          ? { ...entry, status: "finalized", date: formatNotebookDate(), dirty: false }
          : entry,
      ),
    }));
  }

  function archiveNotebookEntry(entryId: string) {
    if (!selectedCandidateId) return;
    setSimpleNotebookEntriesByCandidate((state) => ({
      ...state,
      [selectedCandidateId]: (state[selectedCandidateId] ?? []).map((entry) =>
        entry.id === entryId ? { ...entry, status: "archived", dirty: false } : entry,
      ),
    }));
  }

  function reopenNotebookEntry(entryId: string) {
    if (!selectedCandidateId) return;
    setSimpleNotebookEntriesByCandidate((state) => ({
      ...state,
      [selectedCandidateId]: (state[selectedCandidateId] ?? []).map((entry) => {
        if (entry.id === entryId) return { ...entry, status: "draft", dirty: false };
        if (entry.status === "draft") return { ...entry, status: "finalized", dirty: false };
        return entry;
      }),
    }));
  }

  function newNotebookEntry() {
    if (!selectedCandidateId) return;
    const id = `n${Date.now()}`;
    setSimpleNotebookEntriesByCandidate((state) => ({
      ...state,
      [selectedCandidateId]: [
        {
          id,
          status: "draft",
          date: formatNotebookDate(),
          linked: "",
          title: "",
          thesis: "",
          assumptions: "",
          invalidation: "",
          watchItems: "",
          reflections: "",
          nextReview: "",
          dirty: false,
        },
        ...(state[selectedCandidateId] ?? []).map((entry) =>
          entry.status === "draft" ? { ...entry, status: "finalized" as const, dirty: false } : entry,
        ),
      ],
    }));
  }

  function deleteNotebookEntry(entryId: string) {
    if (!selectedCandidateId) return;
    setSimpleNotebookEntriesByCandidate((state) => ({
      ...state,
      [selectedCandidateId]: (state[selectedCandidateId] ?? []).filter((entry) => entry.id !== entryId),
    }));
    setNotebookAssistResponses((state) => {
      const next = { ...state };
      Object.keys(next).forEach((key) => {
        if (key.startsWith(`${entryId}_`)) delete next[key];
      });
      return next;
    });
  }

  function askNotebookAssistant(entry: SimpleNotebookEntry, promptType: NotebookPromptType) {
    const key = `${entry.id}_${promptType}`;
    setNotebookAssistResponses((state) => ({
      ...state,
      [key]: {
        label: NOTEBOOK_ASSIST_LABELS[promptType],
        loading: false,
        text: buildNotebookAssistResponse(entry, promptType),
      },
    }));
  }

  function dismissNotebookAssistant(key: string) {
    setNotebookAssistResponses((state) => {
      const next = { ...state };
      delete next[key];
      return next;
    });
  }

  function summaryChipValue(chips: Array<{ label: string; value: string }>, label: string) {
    return chips.find((chip) => chip.label === label)?.value ?? "Unavailable";
  }

  function presentWeightRead(candidate: { currentWeight: string | null; weightState: string | null }) {
    if (candidate.currentWeight) {
      return `${candidate.currentWeight} live weight`;
    }
    if (candidate.weightState === "Overlay Absent") {
      return "No portfolio loaded yet";
    }
    return candidate.weightState ?? "No live position attached";
  }

  function presentOverlayMessage(value: string | null | undefined) {
    if (!value) return null;
    const lower = value.toLowerCase();
    if (lower.includes("overlay absent") || lower.includes("holdings overlay")) {
      return "No portfolio loaded yet.";
    }
    return value;
  }

  function scoreTone(score: number | null | undefined): Tone {
    if (typeof score !== "number") return "neutral";
    if (score >= 75) return "good";
    if (score >= 55) return "warn";
    return "bad";
  }

  function scoreBandLabel(score: number | null | undefined) {
    if (typeof score !== "number") return "Unavailable";
    if (score >= 82) return "Strong support";
    if (score >= 68) return "Usable support";
    if (score >= 55) return "Bounded";
    if (score >= 40) return "Penalty zone";
    return "Major drag";
  }

  function scoreMeaning(label: string) {
    const normalized = label.toLowerCase();
    if (normalized.includes("sleeve fit")) return "How well the ETF matches the exact sleeve job.";
    if (normalized.includes("implementation")) return "Cost, liquidity, wrapper, and trading friction.";
    if (normalized.includes("source integrity")) return "How clean and trustworthy the current source stack is.";
    if (normalized.includes("benchmark fidelity")) return "How explicit and reliable the benchmark lineage is.";
    if (normalized.includes("instrument quality")) return "The quality of the fund itself: cost, scale, structure, tracking, and sponsor durability.";
    if (normalized.includes("portfolio fit")) return "The marginal portfolio contribution, diversification value, and sleeve-role quality independent of the final decision label.";
    if (normalized.includes("identity")) return "How cleanly the instrument identity resolves.";
    if (normalized.includes("market-path")) return "Bounded support only from current market structure.";
    if (normalized.includes("long-horizon")) return "Marks/Buffett lens on staying power, friction, and role clarity.";
    if (normalized.includes("admissibility")) return "Whether the candidate is currently clean enough to rely on.";
    return "Investor-facing score component.";
  }

  function formatScoreRead(score: number | null | undefined) {
    if (typeof score !== "number") return "Unavailable";
    return `${score} / 100 · ${scoreBandLabel(score)}`;
  }

function quickScoreFill(score: number) {
  if (score >= 75) return "linear-gradient(90deg, rgba(104, 168, 130, 0.96), rgba(140, 200, 165, 0.88))";
  if (score >= 55) return "linear-gradient(90deg, rgba(196, 146, 60, 0.96), rgba(235, 205, 152, 0.88))";
  return "linear-gradient(90deg, rgba(191, 100, 100, 0.96), rgba(210, 130, 130, 0.88))";
}

function scoreRead(score: number | null | undefined) {
  if (typeof score !== "number") return "—";
  return `${Math.round(score)}`;
}

function scoreSummaryTitle(summary: {
  averageScore: number;
  componentCountUsed: number;
  reliabilityState: "strong" | "mixed" | "weak";
} | null | undefined) {
    if (!summary) return "Score family unavailable";
  return `Recommendation score ${summary.averageScore} out of 100, with ${summary.componentCountUsed} diagnostic pillars still exposed underneath. Reliability: ${summary.reliabilityState}.`;
}

function truthConfidenceBandLabel(value: string | null | undefined) {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    high_confidence: "High confidence",
    good_confidence: "Good confidence",
    review_confidence: "Review confidence",
    low_confidence: "Low confidence",
    unreliable: "Unreliable",
  };
  return map[raw] ?? humanizeCode(value);
}

function truthConfidenceTone(
  score: number | null | undefined,
  band: string | null | undefined,
): Tone {
  if (typeof score === "number") {
    return score >= 75 ? "good" : score >= 60 ? "warn" : "bad";
  }
  const raw = String(band ?? "").trim().toLowerCase();
  if (raw === "high_confidence" || raw === "good_confidence") return "good";
  if (raw === "review_confidence") return "warn";
  if (raw === "low_confidence" || raw === "unreliable") return "bad";
  return "neutral";
}

function deployabilityBadgeLabel(value: string | null | undefined) {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    deploy_now: "Deploy now",
    review_before_deploy: "Review before deploy",
    research_only: "Research only",
    blocked: "Blocked",
  };
  return map[raw] ?? humanizeCode(value);
}

function deployabilityBadgeTone(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw === "deploy_now") return "good";
  if (raw === "review_before_deploy") return "warn";
  if (raw === "research_only") return "neutral";
  if (raw === "blocked") return "bad";
  return "neutral";
}

function sleeveDeployabilityLabel(value: string | null | undefined) {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    ready: "Deployable",
    reviewable: "Reviewable",
    bounded: "Building evidence",
    blocked: "Not ready",
  };
  return map[raw] ?? humanizeCode(value);
}

function sleeveDeployabilityTone(value: string | null | undefined): Tone {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw === "ready") return "good";
  if (raw === "reviewable") return "warn";
  if (raw === "bounded") return "neutral";
  if (raw === "blocked") return "bad";
  return "neutral";
}

function readinessPostureLabel(value: string | null | undefined) {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    action_ready: "Action-ready",
    reviewable: "Reviewable",
    blocked: "Blocked",
  };
  return map[raw] ?? humanizeCode(value);
}

function fallbackScoreSummaryFromBreakdown(
  breakdown:
    | CandidateReportDisplay["scoreBreakdown"]
    | BlueprintDisplay["sleeves"][number]["candidates"][number]["scoreBreakdown"]
    | null
    | undefined,
) {
  if (!breakdown) return null;
  const rawComponents = [
    { id: "implementation", label: "Implementation", score: breakdown.implementation },
    { id: "source_integrity", label: "Source integrity", score: breakdown.sourceIntegrity ?? breakdown.evidence },
    { id: "benchmark_fidelity", label: "Benchmark fidelity", score: breakdown.benchmarkFidelity },
    { id: "sleeve_fit", label: "Sleeve fit", score: breakdown.sleeveFit },
    { id: "long_horizon_quality", label: "Long-horizon quality", score: breakdown.longHorizonQuality },
    { id: "market_path_support", label: "Market-path support", score: breakdown.marketPathSupport },
    { id: "instrument_quality", label: "Instrument quality", score: breakdown.instrumentQuality },
    { id: "portfolio_fit", label: "Portfolio fit", score: breakdown.portfolioFit },
  ].filter((component): component is { id: string; label: string; score: number } => typeof component.score === "number");
  if (!rawComponents.length) return null;
  const averageScore = Math.round(breakdown.recommendation ?? breakdown.total ?? rawComponents.reduce((sum, component) => sum + component.score, 0) / rawComponents.length);
  return {
    averageScore,
    componentCountUsed: rawComponents.length,
    tone: scoreTone(averageScore),
    reliabilityState: (rawComponents.length >= 8 ? "mixed" : "weak") as "mixed" | "weak",
    reliabilityNote:
      rawComponents.length >= 8
        ? "Score family is using the current eight backend-native investor-facing pillars."
        : `Score family is currently based on ${rawComponents.length} of 8 investor-facing pillars.`,
    components: rawComponents.map((component) => ({
      id: component.id,
      label: component.label,
      score: component.score,
      tone: scoreTone(component.score),
      summary: "",
    })),
  };
}

function reportPreview(candidateId: string) {
  const binding = reportSourceBinding(blueprint.data, candidateId, activeSleeveId);
  const snapshot = findReportSnapshot(blueprint.data, candidateId);
  const contract = reportCache[reportBindingKey(candidateId, binding)]?.data ?? (snapshot && reportMatchesBinding(snapshot, binding) ? snapshot : null);
  return contract ? adaptCandidateReport(contract) : null;
}

  function scoreRows(breakdown: CandidateReportDisplay["scoreBreakdown"] | BlueprintDisplay["sleeves"][number]["candidates"][number]["scoreBreakdown"]) {
    if (!breakdown) return [];
    return [
      { label: "Sleeve fit", value: breakdown.sleeveFit },
      { label: "Implementation", value: breakdown.implementation },
      { label: "Source integrity", value: breakdown.sourceIntegrity ?? breakdown.evidence },
      { label: "Benchmark fidelity", value: breakdown.benchmarkFidelity },
      { label: "Instrument quality", value: breakdown.instrumentQuality },
      { label: "Portfolio fit", value: breakdown.portfolioFit },
      { label: "Identity", value: breakdown.identity },
      { label: "Market-path support", value: breakdown.marketPathSupport },
      { label: "Long-horizon quality", value: breakdown.longHorizonQuality },
      { label: "Admissibility", value: breakdown.admissibility },
    ]
      .filter((row) => row.value !== null && row.value !== undefined)
      .map((row) => ({
        label: row.label,
        value: formatScoreRead(row.value),
        note: scoreMeaning(row.label),
      }));
  }

  function coverageVerdictLabel(value: string | null | undefined) {
    const raw = String(value ?? "").trim().toLowerCase();
    const map: Record<string, string> = {
      direct_ready: "Direct history ready",
      proxy_ready: "Proxy history ready",
      missing_history: "History still missing",
      alias_review: "Alias review still needed",
      weak_quality: "Series quality still weak",
      onboarding_in_progress: "Coverage onboarding still in progress",
    };
    return map[raw] ?? humanizeCode(value);
  }

  function supportVerdictLabel(value: string | null | undefined) {
    const raw = String(value ?? "").trim().toLowerCase();
    const map: Record<string, string> = {
      direct_backed: "Direct-series support",
      proxy_backed: "Proxy-backed support",
      suppressed: "No usable market-path support",
      cautionary: "Support stays cautionary",
      unstable: "Support remains review-only",
      under_review: "Support still under review",
    };
    return map[raw] ?? humanizeCode(value);
  }

  function coverageTone(value: string | null | undefined): Tone {
    const raw = String(value ?? "").trim().toLowerCase();
    if (raw.includes("direct_ready") || raw.includes("direct_backed")) return "good";
    if (raw.includes("proxy_ready") || raw.includes("proxy_backed")) return "info";
    if (raw.includes("alias_review") || raw.includes("cautionary")) return "warn";
    if (raw.includes("missing_history") || raw.includes("suppressed") || raw.includes("weak_quality")) return "bad";
    return "neutral";
  }

  function checklistTone(value: string | null | undefined): Tone {
    const raw = String(value ?? "").trim().toLowerCase();
    if (raw === "ready" || raw === "complete" || raw === "passed") return "good";
    if (raw === "review" || raw === "pending" || raw === "partial") return "warn";
    if (raw === "failed" || raw === "missing" || raw === "blocked") return "bad";
    return "neutral";
  }

  function quickBriefStatusTone(value: string | null | undefined): Tone {
    const raw = String(value ?? "").trim().toLowerCase();
    if (raw === "eligible") return "good";
    if (raw === "blocked") return "bad";
    if (raw === "research_only") return "neutral";
    return "warn";
  }

  function renderScoreFamily(
    breakdown: CandidateReportDisplay["scoreBreakdown"] | BlueprintDisplay["sleeves"][number]["candidates"][number]["scoreBreakdown"],
    components: Array<{
      label: string;
      score: number;
      tone: Tone;
      summary: string;
      band?: string | null;
      confidence?: number | null;
      reasons?: string[];
      capsApplied?: string[];
      fieldDrivers?: string[];
    }>,
    sleeveName?: string | null,
  ) {
    if (!breakdown && !components.length) {
      return <div className="panel-copy">Score interpretation is not available yet.</div>;
    }
    const recommendationScore = breakdown?.recommendation ?? breakdown?.total ?? null;
    const recommendationMeritScore = breakdown?.recommendationMerit ?? breakdown?.investmentMerit ?? breakdown?.optimality ?? null;
    const deployabilityScore = breakdown?.deployability ?? breakdown?.deployment ?? breakdown?.readiness ?? null;
    const truthConfidenceScore = breakdown?.truthConfidence ?? null;
    const truthConfidenceBand = breakdown?.truthConfidenceBand ?? null;
    const deployabilityBadge = breakdown?.deployabilityBadge ?? null;
    const strongestComponent = components.length ? [...components].sort((a, b) => b.score - a.score)[0] : null;
    const weakestComponent = components.length ? [...components].sort((a, b) => a.score - b.score)[0] : null;
    const supportDrivers = components.filter((component) => component.score >= 75).slice(0, 3);
    const penaltyDrivers = components.filter((component) => component.score < 60).slice(0, 3);
    return (
      <>
        {breakdown ? (
          <>
            <div className="blueprint-score-header">
              <span className={`chip ${pillClass(scoreTone(recommendationScore ?? breakdown.total)).replace("chip ", "")}`}>
                Recommendation {recommendationScore ?? breakdown.total}
              </span>
              {deployabilityBadge ? (
                <span className={`chip ${pillClass(deployabilityBadgeTone(deployabilityBadge)).replace("chip ", "")}`}>
                  {deployabilityBadgeLabel(deployabilityBadge)}
                </span>
              ) : null}
              {truthConfidenceBand ? (
                <span className={`chip ${pillClass(truthConfidenceTone(truthConfidenceScore, truthConfidenceBand)).replace("chip ", "")}`}>
                  {truthConfidenceBandLabel(truthConfidenceBand)}
                </span>
              ) : null}
              {strongestComponent ? (
                <span className="chip chip-green">Best support {strongestComponent.label}</span>
              ) : null}
              {weakestComponent ? (
                <span className="chip chip-red">Main drag {weakestComponent.label}</span>
              ) : null}
            </div>
            <div className="panel-copy" style={{ marginTop: 10 }}>
              Read the headline number as recommendation quality first. Deployability stays explicit underneath it, and truth confidence tells you how strongly the current read should be trusted.
            </div>
            <div className="etf-inline-meta" style={{ marginTop: 8 }}>
              {scoreWeightingNote(sleeveName)}
            </div>
            <div className="fact-list" style={{ marginTop: 10 }}>
              <div className="fact-line">
                <strong>Recommendation score means</strong>
                <span>80% recommendation merit plus 20% deployability, while truth confidence controls whether the read can be trusted strongly enough to stay promotable.</span>
              </div>
              {recommendationMeritScore !== null ? (
                <div className="fact-line">
                  <strong>Recommendation merit</strong>
                  <span>{formatScoreRead(recommendationMeritScore)} · Sleeve fit, instrument quality, long-horizon quality, benchmark fidelity, and portfolio fit.</span>
                </div>
              ) : null}
              {deployabilityScore !== null ? (
                <div className="fact-line">
                  <strong>Deployability</strong>
                  <span>{formatScoreRead(deployabilityScore)} · Current source integrity, implementation discipline, market-path support, and admissibility plus identity.</span>
                </div>
              ) : null}
              {truthConfidenceScore !== null ? (
                <div className="fact-line">
                  <strong>Truth confidence</strong>
                  <span>
                    {formatScoreRead(truthConfidenceScore)}
                    {truthConfidenceBand ? ` · ${truthConfidenceBandLabel(truthConfidenceBand)}` : ""}
                    {breakdown.truthConfidenceSummary ? ` · ${breakdown.truthConfidenceSummary}` : ""}
                  </span>
                </div>
              ) : null}
              {breakdown.admissibilityIdentity !== null ? (
                <div className="fact-line">
                  <strong>Admissibility + identity</strong>
                  <span>{formatScoreRead(breakdown.admissibilityIdentity)} · The cleaner of the current admissibility gate and identity resolution state.</span>
                </div>
              ) : null}
              <div className="fact-line">
                <strong>Diagnostic bars</strong>
                <span>The component bars stay visible as diagnostics. They explain why the recommendation remains strong, reviewable, or capped; they are not the headline formula by themselves.</span>
              </div>
              <div className="fact-line">
                <strong>Confidence penalty</strong>
                <span>{breakdown.confidencePenalty !== null ? `${breakdown.confidencePenalty} points are being held back by weaker authority, thinner support, or unresolved friction.` : "No explicit confidence penalty is currently surfaced."}</span>
              </div>
              {breakdown.readinessPosture || breakdown.readinessSummary ? (
                <div className="fact-line">
                  <strong>Deployability posture</strong>
                  <span>
                    {deployabilityBadge ? deployabilityBadgeLabel(deployabilityBadge) : breakdown.readinessPosture ? readinessPostureLabel(breakdown.readinessPosture) : "Unavailable"}
                    {breakdown.readinessSummary ? ` · ${breakdown.readinessSummary}` : ""}
                  </span>
                </div>
              ) : null}
            </div>
            <div className="fact-list">
              {scoreRows(breakdown).map((row) => (
                <div className="fact-line" key={row.label}>
                  <strong>{row.label}</strong>
                  <span>{row.value} · {row.note}</span>
                </div>
              ))}
            </div>
            {breakdown.summary ? (
              <div className="panel-copy" style={{ marginTop: 10 }}>{breakdown.summary}</div>
            ) : null}
            {supportDrivers.length ? (
              <div style={{ marginTop: 10 }}>
                <div className="panel-kicker">What currently supports the score</div>
                <div className="fact-list">
                  {supportDrivers.map((component) => (
                    <div className="fact-line" key={`support-${component.label}`}>
                      <strong>{component.label}</strong>
                      <span>
                      {[
                          component.summary,
                          component.band ? `Band ${humanizeCode(component.band)}` : null,
                          typeof component.confidence === "number" ? `Confidence ${Math.round(component.confidence)}` : null,
                          component.capsApplied?.length ? `${component.capsApplied.length} cap${component.capsApplied.length === 1 ? "" : "s"} active` : null,
                        ].filter(Boolean).join(" · ")}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
            {penaltyDrivers.length ? (
              <div style={{ marginTop: 10 }}>
                <div className="panel-kicker">What is still penalizing it</div>
                <div className="fact-list">
                  {penaltyDrivers.map((component) => (
                    <div className="fact-line" key={`penalty-${component.label}`}>
                      <strong>{component.label}</strong>
                      <span>
                        {[
                          component.summary,
                          component.band ? `Band ${humanizeCode(component.band)}` : null,
                          typeof component.confidence === "number" ? `Confidence ${Math.round(component.confidence)}` : null,
                          component.capsApplied?.length ? `${component.capsApplied.length} cap${component.capsApplied.length === 1 ? "" : "s"} active` : null,
                        ].filter(Boolean).join(" · ")}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
          </>
        ) : null}
        {components.length ? (
          <div className="support-grid" style={{ marginTop: 12 }}>
            {components.map((component) => (
              <div key={component.label}>
                <div className="support-row">
                  <span className="support-label">{component.label}</span>
                  <div className="support-track">
                    <div className={`support-fill tone-${component.tone}`} style={{ width: `${Math.max(6, Math.min(100, component.score))}%` }} />
                  </div>
                  <span className={`support-num tone-${component.tone}`}>{component.score}</span>
                </div>
                <div className="fact-chip-row" style={{ marginTop: 6 }}>
                  <span className={pillClass(component.tone)}>
                    {component.score >= 75 ? "Currently helping" : component.score < 60 ? "Currently penalizing" : "Still mixed"}
                  </span>
                  <span className="chip">{component.band ? humanizeCode(component.band) : scoreBandLabel(component.score)}</span>
                  {typeof component.confidence === "number" ? (
                    <span className="chip">Confidence {Math.round(component.confidence)}</span>
                  ) : null}
                  {component.capsApplied?.length ? (
                    <span className="chip chip-amber">
                      {component.capsApplied.length} cap{component.capsApplied.length === 1 ? "" : "s"}
                    </span>
                  ) : null}
                </div>
                <div className="etf-inline-meta" style={{ marginTop: 6 }}>
                  {[
                    scoreMeaning(component.label),
                    component.summary,
                    component.reasons?.[0],
                    component.fieldDrivers?.length ? `Drivers: ${component.fieldDrivers.slice(0, 3).join(", ")}` : null,
                  ].filter(Boolean).join(" · ")}
                </div>
              </div>
            ))}
          </div>
        ) : null}
      </>
    );
  }

  function renderResearchSupportBody(researchSupport: CandidateReportDisplay["researchSupport"]) {
    if (!researchSupport) {
      return <div className="changes-empty">No bounded research support is attached yet.</div>;
    }
    return (
      <>
        {researchSupport.thesisDrift ? (
          <div className="fact-line">
            <strong>Thesis drift</strong>
            <span>{researchSupport.thesisDrift}</span>
          </div>
        ) : null}
        {researchSupport.marketContext ? (
          <div className="fact-line">
            <strong>Market context</strong>
            <span>{researchSupport.marketContext}</span>
          </div>
        ) : null}
        {researchSupport.draftingSummary ? (
          <div className="fact-line">
            <strong>Drafting support</strong>
            <span>{researchSupport.draftingSummary}</span>
          </div>
        ) : null}
        {researchSupport.sentimentSummary ? (
          <div className="fact-line">
            <strong>Narrative tone</strong>
            <span>
              <span className={pillClass(researchSupport.sentimentTone)}>{humanizeCode(researchSupport.sentimentTone)}</span>
              {` ${researchSupport.sentimentSummary}`}
            </span>
          </div>
        ) : null}
        {researchSupport.keyQuestions.length ? (
          <div style={{ marginTop: 10 }}>
            <div className="panel-kicker">Key questions</div>
            {researchSupport.keyQuestions.map((question) => (
              <div className="fact-line" key={question}>
                <strong>Question</strong>
                <span>{question}</span>
              </div>
            ))}
          </div>
        ) : null}
        {researchSupport.nextSteps.length ? (
          <div style={{ marginTop: 10 }}>
            <div className="panel-kicker">Next steps</div>
            {researchSupport.nextSteps.map((step) => (
              <div className="fact-line" key={step}>
                <strong>Next</strong>
                <span>{step}</span>
              </div>
            ))}
          </div>
        ) : null}
        {researchSupport.retrievalGuides.length ? (
          <div style={{ marginTop: 10 }}>
            <div className="panel-kicker">Retrieval guides</div>
            {researchSupport.retrievalGuides.map((guide) => (
              <div className="fact-line" key={guide.label}>
                <strong>{guide.label}</strong>
                <span>{guide.query} · {guide.reason} · {guide.priority}</span>
              </div>
            ))}
          </div>
        ) : null}
        {researchSupport.newsClusters.length ? (
          <div style={{ marginTop: 10 }}>
            <div className="panel-kicker">News clusters</div>
            {researchSupport.newsClusters.map((cluster) => (
              <div className="fact-line" key={cluster.label}>
                <strong>{cluster.label}</strong>
                <span>
                  <span className={pillClass(cluster.tone)}>{humanizeCode(cluster.tone)}</span>
                  {` ${cluster.summary}`}
                  {cluster.headlines.length ? ` · ${cluster.headlines.join(" | ")}` : ""}
                </span>
              </div>
            ))}
          </div>
        ) : null}
        {researchSupport.logicSteps.length ? (
          <div style={{ marginTop: 10 }}>
            <div className="panel-kicker">Logic map</div>
            {researchSupport.logicSteps.map((step) => (
              <div className="fact-line" key={`${step.label}-${step.detail}`}>
                <strong>{step.label}</strong>
                <span>{step.detail}</span>
              </div>
            ))}
          </div>
        ) : null}
      </>
    );
  }

  function renderPortfolioSurface(display: PortfolioDisplay) {
    const topHoldings = [...display.holdings]
      .sort((a, b) => (b.weightPct ?? -1) - (a.weightPct ?? -1))
      .slice(0, 5);
    const topHoldingWeight = topHoldings.reduce((sum, holding) => sum + (holding.weightPct ?? 0), 0);
    const topLevelAllocationRows = display.allocationRows.filter((row) => row.countsAsTopLevelTotal);
    const nestedAllocationRows = display.allocationRows.filter((row) => row.isNested);
    const healthWidth = (tone?: Tone) => {
      if (tone === "good") return 88;
      if (tone === "info") return 74;
      if (tone === "warn") return 62;
      if (tone === "bad") return 38;
      return 50;
    };
    const allocationFillWidth = (row: PortfolioDisplay["allocationRows"][number]) => {
      if (row.currentPct == null) return 4;
      if (row.currentPct <= 0) return 4;
      return Math.min(Math.max(row.currentPct, 4), 100);
    };
    const allocationTargetOffset = (row: PortfolioDisplay["allocationRows"][number]) =>
      Math.min(Math.max(row.targetPct, 0), 100);
    const allocationBandLeft = (row: PortfolioDisplay["allocationRows"][number]) =>
      Math.min(Math.max(row.minPct, 0), 100);
    const allocationBandWidth = (row: PortfolioDisplay["allocationRows"][number]) =>
      Math.max(2, Math.min(100, row.maxPct) - Math.min(Math.max(row.minPct, 0), 100));

    return (
      <div className="layout-stack portfolio-surface">
        {display.degradedMessage ? <div className="surface-warning">{display.degradedMessage}</div> : null}
        {portfolio.error ? <div className="surface-error">{portfolio.error}</div> : null}
        {portfolioUploadError ? <div className="surface-error">{portfolioUploadError}</div> : null}
        {portfolioUploadMessage ? <div className="surface-warning" style={{ color: "var(--green)" }}>{portfolioUploadMessage}</div> : null}

        <div className="portfolio-summary-strip">
          {display.summaryChips.map((chip) => (
            <div className="pf-chip" key={chip.label}>
              <div className="pf-chip-label">{chip.label}</div>
              <div className={`pf-chip-value tone-${chip.tone ?? "neutral"}`}>{chip.value}</div>
              {chip.meta ? <div className="pf-chip-meta">{chip.meta}</div> : null}
            </div>
          ))}
        </div>

        <section className="panel portfolio-panel">
          <div className="pf-section-header">
            <div className="pf-section-title">Holdings by weight</div>
            <div className="pf-section-meta">
              {display.holdings.length
                ? `${display.holdings.length} position${display.holdings.length === 1 ? "" : "s"} · ${topHoldingWeight.toFixed(1)}% allocated`
                : "Awaiting holdings"}
            </div>
          </div>
          {display.holdings.length ? (
            <div className="pf-charts-grid">
              <div className="pf-chart-panel">
                <div className="pf-chart-title">Current book</div>
                {display.chartPanels.length ? (
                  <ChartPanel panel={display.chartPanels[0]} height={220} />
                ) : (
                  <div className="pf-empty-state">No allocation chart is available yet.</div>
                )}
              </div>
              <div className="pf-chart-panel">
                <div className="pf-chart-title">Top positions</div>
                {topHoldings.length ? (
                  <>
                    <div className="pf-donut-legend">
                      {topHoldings.map((holding, index) => (
                        <div className="pf-legend-row" key={`${holding.symbol}-${holding.sleeve}`}>
                          <span
                            className="pf-legend-dot"
                            style={{
                              background: ["#c49a47", "#7a9cb8", "#68a882", "#d07e48", "#7b7f92"][index % 5],
                            }}
                          />
                          <span>
                            {holding.symbol} {holding.weight}
                            {holding.targetWeight !== "—" ? ` vs ${holding.targetWeight}` : ""}
                          </span>
                        </div>
                      ))}
                    </div>
                    <div className="surface-note">Top positions show where concentration and funding flexibility sit right now.</div>
                  </>
                ) : (
                  <div className="pf-empty-state">No active holdings are available yet.</div>
                )}
              </div>
            </div>
          ) : (
            <div className="pf-empty-book-grid">
              <div className="pf-empty-book-panel">
                <div className="pf-chart-title">Current book</div>
                <div className="pf-empty-book-title">Awaiting holdings upload</div>
                <div className="pf-empty-book-copy">Upload a CSV to populate current weights, drift against target, and sleeve funding context.</div>
              </div>
              <div className="pf-empty-book-panel">
                <div className="pf-chart-title">Top positions</div>
                <div className="pf-empty-book-title">No active holdings yet</div>
                <div className="pf-empty-book-copy">Once holdings are loaded, concentration, top positions, and funding flexibility will render here.</div>
              </div>
            </div>
          )}
        </section>

        <section className="panel portfolio-panel">
          <div className="pf-section-header">
            <div className="pf-section-title">Allocation vs Target</div>
            <div className="pf-section-meta">{topLevelAllocationRows.length} top level · {nestedAllocationRows.length} nested</div>
          </div>
          <div className="portfolio-allocation-card-grid">
            {topLevelAllocationRows.map((row) => (
              <div className={`portfolio-allocation-card tone-${row.statusTone ?? "neutral"}`} key={row.sleeveId}>
                <div className="portfolio-allocation-card-top">
                  <div>
                    <div className="portfolio-allocation-card-name">{row.name}</div>
                    <div className="portfolio-allocation-card-rank">#{row.rank} strategic sleeve</div>
                  </div>
                  {row.drift !== "—" ? (
                    <div className={`portfolio-allocation-card-drift tone-${row.driftTone ?? "neutral"}`}>{row.drift}</div>
                  ) : null}
                </div>
                <div className={`portfolio-allocation-card-state tone-${row.bandTone ?? "neutral"}`}>
                  {row.bandStatus}
                </div>
                <div className="portfolio-allocation-card-metrics portfolio-allocation-card-metrics-3up">
                  <div>
                    <span>Current</span>
                    <strong>{row.current}</strong>
                  </div>
                  <div>
                    <span>Target</span>
                    <strong>{row.target}</strong>
                  </div>
                  <div>
                    <span>Range</span>
                    <strong>{row.range}</strong>
                  </div>
                </div>
                <div className="portfolio-allocation-track-shell">
                  <div className="portfolio-allocation-track">
                    <div
                      className="portfolio-allocation-band"
                      style={{ left: `${allocationBandLeft(row)}%`, width: `${allocationBandWidth(row)}%` }}
                    />
                    <div
                      className={`portfolio-allocation-fill tone-${row.driftTone ?? "neutral"}`}
                      style={{ width: `${allocationFillWidth(row)}%` }}
                    />
                    <div className="portfolio-allocation-target" style={{ left: `${allocationTargetOffset(row)}%` }} />
                  </div>
                </div>
                <div className="portfolio-allocation-card-support">
                  <span>{row.statusLabel}</span>
                  {row.capitalEligible ? <span>Eligible now</span> : null}
                  {row.fundingSource ? <span>Funding {row.fundingSource}</span> : null}
                </div>
              </div>
            ))}
          </div>

          {nestedAllocationRows.length ? (
            <div className="portfolio-allocation-nested-group">
              <div className="portfolio-allocation-nested-head">
                <strong>Inside Global Equity Core</strong>
                <span>Nested equity carveouts do not add to the top-level total.</span>
              </div>
              <div className="portfolio-allocation-lines portfolio-allocation-lines-nested">
                {nestedAllocationRows.map((row) => (
                  <div className="portfolio-allocation-row portfolio-allocation-row-nested" key={row.sleeveId}>
                    <div className="portfolio-allocation-main">
                      <div>
                        <div className="portfolio-allocation-sleeve">{row.name}</div>
                        <div className="portfolio-allocation-parent">{row.parentSleeveName ?? "Global Equity Core"} carveout</div>
                      </div>
                      <div className="portfolio-allocation-values">{row.current} current · {row.target} target · {row.range} range</div>
                    </div>
                    <div className="portfolio-allocation-track-shell">
                      <div className="portfolio-allocation-track">
                        <div
                          className="portfolio-allocation-band"
                          style={{ left: `${allocationBandLeft(row)}%`, width: `${allocationBandWidth(row)}%` }}
                        />
                        <div
                          className={`portfolio-allocation-fill tone-${row.driftTone ?? "neutral"}`}
                          style={{ width: `${allocationFillWidth(row)}%` }}
                        />
                        <div className="portfolio-allocation-target" style={{ left: `${allocationTargetOffset(row)}%` }} />
                      </div>
                    </div>
                    <div className="portfolio-allocation-outcome">
                      {row.drift !== "—" ? (
                        <div className={`portfolio-allocation-drift tone-${row.driftTone ?? "neutral"}`}>{row.drift}</div>
                      ) : null}
                      <div className={`portfolio-allocation-status tone-${row.bandTone ?? "neutral"}`}>{row.bandStatus}</div>
                      {row.fundingSource ? (
                        <div className="portfolio-allocation-subnote">Funding {row.fundingSource}</div>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}
        </section>

        <section className="panel portfolio-panel">
          <div className="pf-section-header">
            <div className="pf-section-title">Holdings Explorer</div>
            <div className="pf-section-meta">{display.holdings.length} position{display.holdings.length === 1 ? "" : "s"}</div>
          </div>
          <table className="holdings-explorer">
            <thead>
              <tr>
                <th>Holding</th>
                <th>Weight vs target</th>
                <th>Status</th>
                <th>Blueprint</th>
                <th>Daily Brief</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {display.holdings.length ? (
                display.holdings.map((holding) => (
                  <tr key={`${holding.symbol}-${holding.sleeve}`}>
                    <td>
                      <div className="panel-kicker">{holding.symbol}</div>
                      <div>{holding.name}</div>
                      <div className="surface-note">{holding.sleeve}</div>
                    </td>
                    <td>
                      <div className={`tone-${holding.weightTone}`}>{holding.weight}</div>
                      <div className="surface-note">
                        {holding.targetWeight !== "—" ? `${holding.weightDrift} vs ${holding.targetWeight}` : holding.weightDrift}
                      </div>
                    </td>
                    <td><span className={`holding-state-tag holding-state-${holding.statusTone ?? "neutral"}`}>{holding.statusLabel}</span></td>
                    <td><span className={`blueprint-tag blueprint-${holding.blueprintTone ?? "neutral"}`}>{holding.blueprintLabel}</span></td>
                    <td className={`tone-${holding.briefTone ?? "neutral"}`}>
                      {holding.briefTone !== "neutral" ? <span className="brief-impact-dot" /> : null}
                      {holding.briefLabel}
                    </td>
                    <td>{holding.actionLabel}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={6}>No holdings are available yet.</td>
                </tr>
              )}
            </tbody>
          </table>
        </section>

        <section className="panel portfolio-panel">
          <div className="pf-section-header">
            <div className="pf-section-title">Portfolio Health</div>
            <div className="pf-section-meta">{display.healthTiles.length} dimension{display.healthTiles.length === 1 ? "" : "s"}</div>
          </div>
          <div className="health-grid">
            {display.healthTiles.map((tile) => (
              <div className="health-cell" key={tile.label}>
                <div className="health-metric-name">{tile.label}</div>
                <div className="health-score-track">
                  <div className={`health-score-fill ${tile.tone ?? "neutral"}`} style={{ width: `${healthWidth(tile.tone)}%` }} />
                </div>
                <div className="health-label">{tile.value}</div>
                <div className="health-note">{tile.note}</div>
              </div>
            ))}
          </div>
        </section>

        <section className="panel portfolio-panel">
          <div className="pf-section-header">
            <div className="pf-section-title">Blueprint Relevance</div>
            <div className="pf-section-meta">{display.blueprintRows.length} sleeve{display.blueprintRows.length === 1 ? "" : "s"}</div>
          </div>
          {display.blueprintRows.length ? (
            <div className="blueprint-strip">
              {display.blueprintRows.map((row) => (
                <div className="blueprint-row" key={`${row.sleeve}-${row.candidate}`}>
                  <div className="blueprint-row-sleeve">{row.sleeve}</div>
                  <div className="blueprint-row-candidate">{row.candidate}</div>
                  <div className={`blueprint-row-status blueprint-tag blueprint-${row.statusTone ?? "neutral"}`}>{row.status}</div>
                  <div className="blueprint-row-note">{row.note}</div>
                </div>
              ))}
            </div>
          ) : (
            <div className="pf-empty-state">No Blueprint linkage available yet.</div>
          )}
        </section>

        <section className="panel portfolio-panel">
          <div className="pf-section-header">
            <div className="pf-section-title">Daily Brief Connection</div>
            <div className="pf-section-meta">{display.briefRows.length} signal{display.briefRows.length === 1 ? "" : "s"} with portfolio impact</div>
          </div>
          {display.briefRows.length ? (
            <div>
              {display.briefRows.map((row) => (
                <div className="brief-signal-row" key={`${row.title}-brief-link`}>
                  <div className="brief-signal-title">
                    <span className={`holding-state-tag holding-state-${row.postureTone ?? "neutral"}`}>{row.posture}</span>
                    <span>{row.title}</span>
                  </div>
                  <div className="brief-signal-affected">
                    {row.affected.length ? row.affected.map((item) => (
                      <span className="brief-affected-tag" key={`${row.title}-${item}`}>{item}</span>
                    )) : <span className="brief-affected-tag">No mapped holdings or sleeves</span>}
                  </div>
                  {row.note ? <div className="wmn-boundary">{row.note}</div> : null}
                  {row.caveat ? <div className="brief-signal-caveat">{row.caveat}</div> : null}
                </div>
              ))}
            </div>
          ) : (
            <div className="pf-empty-state">No Brief signals link to current holdings.</div>
          )}
        </section>

        <section className="panel portfolio-panel">
          <div className="pf-section-header">
            <div className="pf-section-title">Upload &amp; Sync</div>
            <div className="pf-section-meta">Data integrity surface</div>
          </div>
          <div className="upload-sync-card">
            <div className="upload-sync-header">
              <div>
                <div className="panel-kicker">Holdings data</div>
                <div className="panel-title" style={{ fontSize: 18, marginTop: 6 }}>{display.uploadStatus.source}</div>
              </div>
              <div className="trust-freshness-indicator">
                <span className={`trust-dot ${display.degradedMessage ? "aging" : "fresh"}`} />
                <span>{display.uploadStatus.freshness}</span>
              </div>
            </div>
            <div className="upload-sync-stats">
              <div className="upload-sync-stat">
                <strong>{display.uploadStatus.freshness}</strong>
                <span>Freshness</span>
              </div>
              <div className="upload-sync-stat">
                <strong>{display.uploadStatus.mappingQuality}</strong>
                <span>Mapping quality</span>
              </div>
              <div className="upload-sync-stat">
                <strong>{display.uploadStatus.unresolvedMappings}</strong>
                <span>Unresolved mappings</span>
              </div>
            </div>
            <div className="upload-sync-actions">
              <input
                ref={portfolioUploadInputRef}
                type="file"
                accept=".csv,text/csv"
                style={{ display: "none" }}
                onChange={(event) => {
                  const file = event.target.files?.[0] ?? null;
                  void handlePortfolioUpload(file);
                }}
              />
              <button
                className="upload-btn"
                type="button"
                onClick={() => portfolioUploadInputRef.current?.click()}
                disabled={portfolioUploading}
              >
                {portfolioUploading ? "Uploading..." : "Upload holdings"}
              </button>
            </div>
            <div className="surface-note">{display.uploadStatus.message}</div>
          </div>
        </section>
      </div>
    );
  }

  function renderBriefSurface(display: DailyBriefDisplay) {
    const referenceClocks = brief.data?.data_timeframes ?? [];
    const nonFreshMarket = display.marketState.filter((card) => card.isNonFresh);
    const secondarySignalGroups = display.signalGroups.filter((group) => group.signals.length > 0);
    const backdropSignals = display.regimeContextSignals;
    const contingentItems = display.contingentDrivers.map((item) => ({
      label: item.label,
      triggerTitle: item.triggerTitle,
      whyNow: item.whyNow,
      whatChangesIfConfirmed: item.whatChangesIfConfirmed,
      whatToWatchNext: item.whatToWatchNext,
      currentStatus: item.currentStatus,
      affectedSleeves: item.affectedSleeves,
      supportingLines: item.supportingLines,
    }));
    const renderDecisionSignalCard = (
      signal: DailyBriefDisplay["signals"][number],
      options?: { demoted?: boolean },
    ) => {
      const demoted = options?.demoted ?? false;
      const expanded = expandedSignalId === signal.id;
      const scenarioRows = signal.scenarios.map((scenario) => ({
        ...scenario,
        toneClass: scenario.type === "bear" ? "worsens" : scenario.type === "bull" ? "improves" : "base",
      }));
      const scopeItems = [...signal.sleeveTags, ...signal.instrumentTags];
      return (
        <div
          className={`brief-signal-card posture-${signal.postureTone === "bad" ? "critical" : signal.postureTone === "warn" ? "review" : signal.postureTone === "info" ? "monitor" : "ignore"}`}
          key={signal.id}
          onClick={() => setExpandedSignalId(expanded ? null : signal.id)}
          style={demoted ? { opacity: 0.96 } : undefined}
        >
          <div className="brief-signal-header">
            <span className={`brief-posture-tag posture-${signal.postureTone}`}>{signal.posture}</span>
            <span className="brief-category-tag">{humanizeCode(signal.cardFamily ?? signal.category)}</span>
            {signal.freshnessLabel ? <span className="brief-category-tag">{signal.freshnessLabel}</span> : null}
            <div className="brief-signal-title">{signal.evidenceTitle}</div>
            <div className="brief-signal-summary">{signal.interpretationSubtitle}</div>
            <div className="brief-signal-meta">
              {scopeItems.length ? (
                <>
                  {signal.sleeveTags.map((item) => <span className="brief-sleeve-chip" key={item}>{item}</span>)}
                  {signal.instrumentTags.map((item) => <span className="brief-sleeve-chip" key={item}>{item}</span>)}
                </>
              ) : <span className="brief-sleeve-chip">No mapped objects</span>}
              {signal.supportLabel ? (
                <span className="brief-sleeve-chip">{signal.supportLabel}</span>
              ) : signal.confidenceLabel ? (
                <span className="brief-sleeve-chip">{humanizeCode(signal.confidenceLabel)}</span>
              ) : null}
              {signal.eventTitle ? (
                <span className="brief-sleeve-chip">{signal.eventTitle}</span>
              ) : null}
              {signal.confirmationAssets.slice(0, 4).map((asset) => (
                <span className="brief-sleeve-chip" key={`${signal.id}-${asset}`}>watch {asset}</span>
              ))}
            </div>
          </div>
          {expanded ? (
            <div className="brief-signal-detail">
              {signal.whyItMattersEconomically ? (
                <div className="brief-detail-row">
                  <span>Economic implications</span>
                  <p>{signal.whyItMattersEconomically}</p>
                </div>
              ) : null}
              {signal.portfolioAndSleeveMeaning ? (
                <div className="brief-detail-row">
                  <span>Portfolio implications</span>
                  <p>{signal.portfolioAndSleeveMeaning}</p>
                </div>
              ) : null}
              <div className="brief-confirms-breaks">
                <div>
                  <div className="brief-cb-label">Confirms</div>
                  <p>{signal.confirmCondition ?? signal.confirms}</p>
                </div>
                <div>
                  <div className="brief-cb-label">Weakens</div>
                  <p>{signal.weakenCondition ?? signal.whyThisCouldBeWrong ?? "The read weakens if follow-through stalls."}</p>
                </div>
                <div>
                  <div className="brief-cb-label">Breaks</div>
                  <p>{signal.breakCondition ?? signal.breaks}</p>
                </div>
              </div>
              {scenarioRows.length || signal.scenarioSupport ? (
                <div className="brief-detail-row">
                  <span>Scenario support</span>
                  {signal.scenarioSupport ? <p>{signal.scenarioSupport}</p> : null}
                  {scenarioRows.length ? (
                    <div className="brief-scenario-rows">
                      {scenarioRows.map((scenario) => (
                        <div className={`brief-scenario-row ${scenario.toneClass}`} key={`${signal.id}-${scenario.type}`}>
                          <div className="brief-scenario-type">{scenario.scenarioName ?? scenario.type}</div>
                          <div className="brief-scenario-label">{scenario.pathStatement ?? scenario.leadSentence ?? scenario.label}</div>
                          {scenario.timingWindow ? (
                            <div className="brief-scenario-sub">
                              <span className="brief-sublabel short">Timing</span>
                              <span>
                                {scenario.timingWindow}
                                {scenario.scenarioLikelihoodPct != null
                                  ? ` (${scenario.scenarioName ?? humanizeCode(scenario.type)} ${scenario.scenarioLikelihoodPct}%)`
                                  : ""}
                              </span>
                            </div>
                          ) : null}
                          {scenario.sleeveConsequence ? (
                            <div className="brief-scenario-sub">
                              <span className="brief-sublabel macro">Sleeve</span>
                              <span>{scenario.sleeveConsequence}</span>
                            </div>
                          ) : null}
                          {scenario.actionBoundary ? (
                            <div className="brief-scenario-sub">
                              <span className="brief-sublabel long">Boundary</span>
                              <span>{scenario.actionBoundary}</span>
                            </div>
                          ) : null}
                          {scenario.supportStrength ? (
                            <div className="brief-scenario-sub">
                              <span className="brief-sublabel thesis">Support</span>
                              <span>{scenario.supportStrength}</span>
                            </div>
                          ) : null}
                          {scenario.regimeNote ? (
                            <div className="brief-scenario-sub">
                              <span className="brief-sublabel macro">Regime</span>
                              <span>{scenario.regimeNote}</span>
                            </div>
                          ) : null}
                          {scenario.confirmationNote ? (
                            <div className="brief-scenario-sub">
                              <span className="brief-sublabel near">Confirmation</span>
                              <span>{scenario.confirmationNote}</span>
                            </div>
                          ) : null}
                          {scenario.upgradeTrigger ? (
                            <div className="brief-scenario-sub">
                              <span className="brief-sublabel near">Upgrade</span>
                              <span>{scenario.upgradeTrigger}</span>
                            </div>
                          ) : null}
                          {scenario.downgradeTrigger ? (
                            <div className="brief-scenario-sub">
                              <span className="brief-sublabel break">Downgrade</span>
                              <span>{scenario.downgradeTrigger}</span>
                            </div>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : null}
              <div className="brief-detail-row">
                <span>Do not overread</span>
                <p>{signal.doNotOverread ?? "No explicit caveat emitted."}</p>
              </div>
              {signal.sourceAndValidity ? (
                <div className="brief-detail-row" style={{ opacity: 0.88 }}>
                  <span>Source and validity</span>
                  <p>{signal.sourceAndValidity}</p>
                </div>
              ) : null}
              {(signal.marketConfirmation ?? signal.newsToMarketConfirmation) ? (
                <div className="brief-detail-row">
                  <span>Market confirmation</span>
                  <p>{signal.marketConfirmation ?? signal.newsToMarketConfirmation}</p>
                </div>
              ) : null}
              {signal.whatChanged ? (
                <div className="brief-detail-row">
                  <span>What changed</span>
                  <p>{signal.whatChanged}</p>
                </div>
              ) : null}
              {signal.chartPayload ? <DailyBriefDecisionChart payload={signal.chartPayload} height={230} /> : null}
            </div>
          ) : null}
        </div>
      );
    };

    return (
      <div className="layout-stack">
        {display.degradedMessage ? <div className="surface-warning">{display.degradedMessage}</div> : null}
        {brief.error ? <div className="surface-error">{brief.error}</div> : null}

        <div className="brief-status-bar">
          {display.statusBar.map((chip) => (
            <div className="brief-status-item" key={chip.label}>
              <span className="brief-status-label">{chip.label}</span>
              <span className={`brief-status-value tone-${chip.tone ?? "neutral"}`}>{chip.value}</span>
            </div>
          ))}
        </div>

        <div className="brief-section-row">
          <div className="brief-section-header" style={{ borderLeftColor: "var(--blue)", marginBottom: 0 }}>
            Market State
          </div>
          <div className="market-state-status-bar">
            {nonFreshMarket.length ? nonFreshMarket.map((card) => (
              <span
                className={`market-state-status-item tone-${card.freshnessTone}`}
                key={card.label}
                title={`${card.label} · ${card.freshnessLabel}`}
              >
                <span className={`market-state-status-dot tone-${card.freshnessTone}`} />
                <span>{card.label}</span>
              </span>
            )) : (
              <span className="market-state-status-ok">All cards validated for current slot</span>
            )}
          </div>
        </div>
        <div className="brief-market-strip">
          {display.marketState.map((card) => {
            const parts = card.value.split(" · ");
            const dir = parts[0] ?? "unknown";
            const mag = parts[1] ?? "unknown";
            const deltaStr = parts[2];
            const toneClass = card.tone ?? "neutral";
            const displayValue = card.currentValue != null
              ? card.currentValue >= 10000
                ? card.currentValue.toLocaleString(undefined, { maximumFractionDigits: 0 })
                : card.currentValue.toFixed(2)
              : deltaStr ?? "—";
            const dirIcon = dir === "up" ? "↑" : dir === "down" ? "↓" : "→";
            return (
              <div
                className={`brief-market-item sev-${mag} tone-${toneClass} dir-${dir}`}
                key={card.label}
                title={[
                  card.metricDefinition,
                  card.sourceProvider,
                  card.sourceAuthorityTier,
                  card.crossCheckProvider ? `Cross-check ${card.crossCheckProvider}` : null,
                  card.crossCheckStatus,
                  card.authorityGapReason,
                  card.asOf,
                  card.validationReason,
                ].filter(Boolean).join(" · ") || undefined}
              >
                <div className="bmc-header">
                  <span className="bmc-label-row">
                    <span className="bmc-label">{card.label}</span>
                    <span className={`bmc-fresh-dot tone-${card.freshnessTone}`} title={card.freshnessLabel} />
                  </span>
                  {mag !== "unknown" && dir !== "neutral" ? (
                    <span className={`bmc-sev-chip sev-${mag}`}>{mag}</span>
                  ) : null}
                </div>
                <div className="bmc-value">{displayValue}</div>
                <div className={`bmc-delta tone-${toneClass}`}>{dirIcon}{deltaStr ? ` ${deltaStr}` : ""}</div>
                {(card.caption || card.subCaption) ? (
                  <div className="bmc-captions">
                    {mag === "minor" ? (
                      <div className="bmc-caption">{[card.caption, card.subCaption].filter(Boolean).join(" · ")}</div>
                    ) : (
                      <>
                        {card.caption ? <div className="bmc-caption">{card.caption}</div> : null}
                        {card.subCaption ? <div className="bmc-sub-caption">{card.subCaption}</div> : null}
                      </>
                    )}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>

        <div className="brief-section-header">What Matters Now</div>
        <div className="brief-two-col">
          <div>
            {display.signals.map((signal) => renderDecisionSignalCard(signal))}
            {(secondarySignalGroups.length || backdropSignals.length) ? (
              <div style={{ marginTop: 18, display: "grid", gap: 12 }}>
                {secondarySignalGroups.map((group) => (
                  <details key={group.id} className="brief-monitor-block">
                    <summary style={{ cursor: "pointer", listStyle: "none" }}>
                      <div className="brief-monitor-signal-title" style={{ marginBottom: 6 }}>{group.label}</div>
                      <div className="brief-signal-summary">{group.summary || group.representative?.shortSubtitle}</div>
                      <div className="brief-signal-meta" style={{ marginTop: 8 }}>
                        <span className="brief-sleeve-chip">{group.count} daily signal{group.count > 1 ? "s" : ""}</span>
                        {group.representative?.visibilityRole ? (
                          <span className="brief-sleeve-chip">{humanizeCode(group.representative.visibilityRole)}</span>
                        ) : null}
                        {group.representative?.eventTitle ? (
                          <span className="brief-sleeve-chip">{group.representative.eventTitle}</span>
                        ) : null}
                        {group.representative?.confirmationAssets.slice(0, 4).map((asset) => (
                          <span className="brief-sleeve-chip" key={`${group.id}-${asset}`}>watch {asset}</span>
                        ))}
                      </div>
                    </summary>
                    <div style={{ marginTop: 12, display: "grid", gap: 12 }}>
                      {group.signals.map((signal) => renderDecisionSignalCard(signal, { demoted: true }))}
                    </div>
                  </details>
                ))}
                {backdropSignals.length ? (
                  <details className="brief-monitor-block">
                    <summary style={{ cursor: "pointer", listStyle: "none" }}>
                      <div className="brief-monitor-signal-title" style={{ marginBottom: 6 }}>Regime Context</div>
                      <div className="brief-signal-summary">
                        Longer-running themes that still matter, but are not fresh enough to lead today’s brief unless they reactivate.
                      </div>
                      <div className="brief-signal-meta" style={{ marginTop: 8 }}>
                        <span className="brief-sleeve-chip">{backdropSignals.length} backdrop item{backdropSignals.length > 1 ? "s" : ""}</span>
                      </div>
                    </summary>
                    <div style={{ marginTop: 12, display: "grid", gap: 12 }}>
                      {backdropSignals.map((signal) => renderDecisionSignalCard(signal, { demoted: true }))}
                    </div>
                  </details>
                ) : null}
              </div>
            ) : null}
          </div>
          <div className="brief-two-col-aside">
            <div className="brief-section-header" style={{ marginTop: 0, borderLeftColor: "var(--green)" }}>
              What could change the brief
            </div>
            {contingentItems.length ? contingentItems.map((item) => (
              <div className="brief-monitor-block" key={item.triggerTitle}>
                <div className="brief-monitor-signal-title">{item.triggerTitle}</div>
                {item.currentStatus ? (
                  <div className="brief-monitor-field">
                    <div className="brief-monitor-field-label">Current status</div>
                    <p><span className="brief-sleeve-chip">{humanizeCode(item.currentStatus)}</span></p>
                  </div>
                ) : null}
                {item.supportingLines.length ? (
                  <div style={{ display: "grid", gap: 6, marginBottom: 10 }}>
                    {item.supportingLines.map((line) => (
                      <div className="brief-signal-summary" key={line}>{line}</div>
                    ))}
                  </div>
                ) : null}
                <div className="brief-monitor-field">
                  <div className="brief-monitor-field-label">Why it matters now</div>
                  <p>{item.whyNow}</p>
                </div>
                <div className="brief-monitor-field">
                  <div className="brief-monitor-field-label near">What changes if confirmed</div>
                  <p>{item.whatChangesIfConfirmed}</p>
                </div>
                {item.whatToWatchNext ? (
                  <div className="brief-monitor-field">
                    <div className="brief-monitor-field-label thesis">What to watch next</div>
                    <p>{item.whatToWatchNext}</p>
                  </div>
                ) : null}
                {item.affectedSleeves.length ? (
                  <div className="brief-monitor-action">
                    <span className="brief-monitor-action-label">Affected sleeves</span>
                    <span className="brief-monitor-action-value">{item.affectedSleeves.join(", ")}</span>
                  </div>
                ) : null}
              </div>
            )) : (
              <div className="surface-note">No contingent triggers are close enough to change the brief right now.</div>
            )}
          </div>
        </div>

        <div className="brief-section-header" style={{ borderLeftColor: "var(--orange)" }}>
          Portfolio Impact
        </div>
        <div className="brief-impact-block">
          <table className="brief-portfolio-matrix">
            <thead>
              <tr>
                <th>Object</th>
                <th>Type</th>
                <th>Mapping</th>
                <th>Status</th>
                <th>Consequence</th>
                <th>Next step</th>
              </tr>
            </thead>
            <tbody>
              {display.impactRows.length ? (
                display.impactRows.map((row) => (
                  <tr key={`${row.objectType}:${row.objectLabel}`}>
                    <td>{row.objectLabel}</td>
                    <td>{row.objectType}</td>
                    <td>{row.mapping}</td>
                    <td><span className={pillClass(row.statusTone)}>{row.statusLabel}</span></td>
                    <td>{row.consequence}</td>
                    <td>{row.nextStep}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={6}><div className="changes-empty">No portfolio objects were mapped.</div></td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <details className="brief-details-section">
          <summary className="brief-toggle-row">
            <span>Scenarios</span>
            <span className="brief-toggle-arrow">▶</span>
          </summary>
          <div style={{ paddingTop: 10 }}>
            <div className="surface-note">{display.scenarioMessage}</div>
            {display.scenarios.length ? (
              <div style={{ display: "grid", gap: 12, marginTop: 12 }}>
                {display.scenarios.map((block) => (
                  <div className="queue-card" key={block.label}>
                    <div className="panel-kicker">{block.label}</div>
                    <div className="panel-copy">{block.summary}</div>
                    {block.chart ? <ChartPanel panel={block.chart} height={220} /> : null}
                    <div style={{ display: "grid", gap: 8, marginTop: 10 }}>
                      {block.variants.map((variant) => (
                        <div className="fact-line" key={`${block.label}-${variant.type}`}>
                          <strong>{variant.scenarioName ?? variant.label}</strong>
                          <span>
                            {variant.pathStatement ?? variant.leadSentence ?? variant.effect}
                            {variant.timingWindow ? ` · ${variant.timingWindow}` : ""}
                            {variant.sleeveConsequence ? ` · ${variant.sleeveConsequence}` : ""}
                            {variant.actionBoundary ? ` · ${variant.actionBoundary}` : ""}
                            {variant.supportStrength ? ` · ${variant.supportStrength}` : ""}
                            {variant.regimeNote ? ` · ${variant.regimeNote}` : ""}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </details>

        <details className="brief-details-section">
          <summary className="brief-toggle-row">
            <span>Evidence &amp; Trust</span>
            <span className="brief-toggle-arrow">▶</span>
          </summary>
          <div style={{ paddingTop: 10 }}>
            {referenceClocks.length ? (
              <div style={{ marginBottom: 16 }}>
                <div className="surface-note" style={{ marginBottom: 8 }}>
                  Reference clocks · {referenceClocks.length} timeframe{referenceClocks.length === 1 ? "" : "s"}
                </div>
                <div className="brief-chart-cluster">
                  {referenceClocks.map((tf, i) => (
                    <div className="brief-chart-card" key={`${tf.label}-${i}`}>
                      <div className="brief-chart-label">{tf.label}</div>
                      <div className="brief-chart-note">
                        {tf.summary}{tf.truth_envelope?.reference_period ? ` · ${tf.truth_envelope.reference_period}` : ""}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
            <div className="brief-evidence-bars">
              {display.evidenceBars.map((bar) => (
                <div className="brief-evidence-bar-row" key={bar.label}>
                  <span className="brief-evidence-bar-label">{bar.label}</span>
                  <div className="brief-evidence-bar-track">
                    <div className={`brief-evidence-bar-fill ${bar.score >= 75 ? "high" : bar.score >= 55 ? "mid" : "low"}`} style={{ width: `${bar.score}%` }} />
                  </div>
                  <span className="brief-evidence-bar-pct">{bar.score}</span>
                </div>
              ))}
            </div>
            <div className="brief-evidence-block" style={{ marginTop: 12 }}>
              {display.evidenceRows.map((row) => (
                <div className="brief-evidence-row" key={row.label}>
                  <span>{row.label}</span>
                  <span>{row.value}</span>
                </div>
              ))}
            </div>
          </div>
        </details>
      </div>
    );
  }

  function renderCompareLauncher(sleeve: BlueprintSleeveDisplay, display: BlueprintDisplay) {
    const compared = sleeve.candidates.filter((candidate) => compareIds.has(candidate.id)).slice(0, 2);
    const selectedIds = compared.map((candidate) => candidate.id);
    const selectedKey = compareRequestKey(sleeve.id, selectedIds, blueprint.data?.surface_snapshot_id);
    const compareDisplay = display.compare;
    const ready = compared.length >= 2;
    const loaded = ready && !!compareDisplay && !!compareKey && compareKey === selectedKey;
    return (
      <div className="compare-launcher blueprint-compare-launcher">
        <div>
          <div className="panel-kicker">Compare</div>
          <div className="compare-launcher-title">
            {compared.length >= 2
              ? `${compared.length} candidates selected`
              : "Select two candidates to test substitution"}
          </div>
          <div className="panel-copy">
            {comparePanelOpen && compare.loading
              ? "Loading the side-by-side compare read."
              : compare.error
                ? compare.error
                : loaded
                  ? compareDisplay.readinessNote ?? "Compare is ready in the centered modal."
                  : ready
                    ? "Open compare to load the current side-by-side read."
                  : "Use compare only when there is a real same-sleeve alternative worth judging."}
          </div>
          <div className="etf-inline-meta" style={{ marginTop: 8 }}>
            Judge in this order: sleeve job, benchmark fidelity, implementation friction, source integrity, then bounded market-path context.
          </div>
        </div>
        <div className="compare-launcher-actions">
          {loaded && compareDisplay ? (
            <>
              <span className={pillClass(compareDisplay.substitutionTone)}>
                {compareDisplay.substitutionVerdict ?? "Compare ready"}
              </span>
              <span className="compare-launcher-meta">Leader {compareDisplay.winnerName}</span>
            </>
          ) : null}
          <button
            className="action-btn"
            type="button"
            disabled={!ready}
            onClick={() => {
              if (comparePanelOpen) {
                setComparePanelOpen(false);
                return;
              }
              openComparePanel(sleeve);
            }}
          >
            {comparePanelOpen ? "Close compare" : "Open compare"}
          </button>
          {compared.length ? (
            <button className="action-btn" type="button" onClick={clearCompare}>Clear</button>
          ) : null}
        </div>
      </div>
    );
  }

  function renderCompareDrawer(display: BlueprintDisplay, sleeve: BlueprintSleeveDisplay | null) {
    const compareDisplay = display.compare;
    if (!comparePanelOpen || !sleeve) return null;
    const comparedCandidates = (compareDisplay?.candidates ?? []).slice(0, 2);
    const compareDecision = compareDisplay?.decision ?? null;
    const firstFlip = compareDecision?.flipConditions[0] ?? null;
    return (
      <div className="compare-root" data-testid="blueprint-compare-modal">
        <div className="compare-backdrop" onClick={() => setComparePanelOpen(false)} />
        <aside className="compare-drawer visible" role="dialog" aria-modal="true" aria-label={`${sleeve.name} compare modal`}>
          <div className="report-top compare-top">
            <div>
              <div className="panel-kicker">Compare</div>
              <h2>{sleeve.name}</h2>
              <p>{compareDisplay?.readinessNote ?? "Loading the current side-by-side compare read."}</p>
            </div>
            <button className="report-close" type="button" onClick={() => setComparePanelOpen(false)}>
              Close
            </button>
          </div>

          <div className="compare-body">
            {!compareDisplay ? (
              <div className="report-note">Compare is still loading for the selected pair.</div>
            ) : (
              <>
                {compareDecision ? (
                  <div className="compare-decision-header">
                    <div>
                      <div className="panel-kicker">Decision compare</div>
                      <div className="compare-focus-title">
                        {compareDecision.bestOverall ? `Best now: ${compareDecision.bestOverall}` : compareDisplay.winnerName}
                      </div>
                      <div className="panel-copy">{compareDecision.winnerSummary ?? compareDisplay.whyLeads}</div>
                    </div>
                    <div className="compare-decision-meta">
                      {compareDecision.substitutionStatus ? <span className={pillClass(compareDisplay.substitutionTone)}>{compareDecision.substitutionStatus}</span> : null}
                      {compareDecision.substitutionConfidence ? <span className="chip">Confidence {compareDecision.substitutionConfidence}</span> : null}
                      {compareDisplay.readinessState ? <span className={pillClass(compareDisplay.readinessTone)}>{humanizeCode(compareDisplay.readinessState)}</span> : null}
                    </div>
                  </div>
                ) : null}

                <div className="compare-summary-strip">
                  <div className="compare-focus-card">
                    <div className="panel-kicker">Best for sleeve job</div>
                    <div className="compare-focus-title">{compareDisplay.compareSummary.cleanerForSleeveJob ?? compareDecision?.bestOverall ?? compareDisplay.winnerName}</div>
                    <div className="panel-copy">{compareDecision?.substitutionReason ?? compareDisplay.substitutionRationale ?? compareDisplay.readinessNote}</div>
                  </div>
                  <div className="compare-focus-card">
                    <div className="panel-kicker">Winner split</div>
                    <div className="compare-focus-title">{compareDecision?.deploymentWinner ? `Deploy ${compareDecision.deploymentWinner}` : compareDisplay.substitutionVerdict ?? "Under review"}</div>
                    <div className="panel-copy">
                      {compactParts([
                        compareDecision?.investmentWinner ? `Investment ${compareDecision.investmentWinner}` : null,
                        compareDecision?.evidenceWinner ? `Evidence ${compareDecision.evidenceWinner}` : null,
                        compareDecision?.timingWinner ? `Timing ${compareDecision.timingWinner}` : null,
                      ]) || compareDisplay.compareSummary.mainSeparation || compareDisplay.whyLeads}
                    </div>
                  </div>
                  <div className="compare-focus-card">
                    <div className="panel-kicker">What would change the read</div>
                    <div className="compare-focus-title">{firstFlip?.condition ?? (compareDisplay.readinessState ? humanizeCode(compareDisplay.readinessState) : "Ready")}</div>
                    <div className="panel-copy">{firstFlip?.thresholdOrTrigger ?? compareDisplay.compareSummary.changeTrigger ?? compareDisplay.whatWouldChange}</div>
                  </div>
                </div>

                {compareDecision?.decisionRule.primaryRule || compareDecision?.decisionRule.nextAction ? (
                  <section className="compare-decision-section">
                    <div className="compare-decision-section-head">
                      <div>
                        <div className="panel-kicker">Decision rule</div>
                        <h3>{compareDecision.decisionRule.primaryRule ?? "Use the stronger line only for the matching sleeve job."}</h3>
                      </div>
                      {compareDecision.decisionRule.nextAction ? <span className="chip">{compareDecision.decisionRule.nextAction}</span> : null}
                    </div>
                    <div className="compare-rule-grid">
                      <div><strong>Choose A if</strong><span>{compareDecision.decisionRule.chooseCandidateAIf ?? "No A-specific rule emitted."}</span></div>
                      <div><strong>Choose B if</strong><span>{compareDecision.decisionRule.chooseCandidateBIf ?? "No B-specific rule emitted."}</span></div>
                      <div><strong>Do not substitute if</strong><span>{compareDecision.decisionRule.doNotTreatAsSubstitutesIf ?? "No substitution guard emitted."}</span></div>
                    </div>
                  </section>
                ) : null}

                <div className="compare-structured-grid">
                  {comparedCandidates.map((candidate) => (
                    <section className="compare-candidate-card compare-candidate-card-structured" key={candidate.id}>
                      <div className="compare-card-header">
                        <div>
                          <div className="compare-candidate-symbol">{candidate.symbol}</div>
                          <div className="compare-card-name">{candidate.name}</div>
                          {candidate.exposureSummary ? <div className="compare-card-exposure">{candidate.exposureSummary}</div> : null}
                        </div>
                        <button className="action-btn active" type="button" disabled>
                          Comparing
                        </button>
                      </div>

                      {candidate.compactTags.length ? (
                        <div className="fact-chip-row" style={{ marginTop: 10 }}>
                          {candidate.compactTags.map((tag) => (
                            <span className="chip" key={`${candidate.id}-${tag}`}>{tag}</span>
                          ))}
                        </div>
                      ) : null}

                      <div className="compare-score-strip">
                        {[
                          ["Deploy", candidate.deployabilityScore],
                          ["Merit", candidate.investmentMeritScore],
                          ["Truth", candidate.truthConfidenceScore],
                          ["Total", candidate.recommendationScore],
                        ].map(([label, score]) => (
                          <span className={`compare-score-chip tone-${scoreTone(score as number | null)}`} key={`${candidate.id}-${label}`}>
                            {label} {typeof score === "number" ? Math.round(score) : "—"}
                          </span>
                        ))}
                      </div>

                      <div className="compare-section-list">
                        <section className="compare-card-section">
                          <div className="row-cell-label">Verdict</div>
                          <span className={pillClass(candidate.verdictTone)}>{candidate.verdictLabel ?? candidate.decisionState ?? "Unavailable"}</span>
                          <div className="etf-inline-meta" style={{ marginTop: 8 }}>
                            {candidate.verdictReason ?? candidate.decisionSummary ?? "No compare verdict explanation emitted."}
                          </div>
                        </section>

                        <section className="compare-card-section">
                          <div className="row-cell-label">Sleeve fit</div>
                          <div className="fact-list">
                            <div className="fact-line">
                              <strong>Role fit</strong>
                              <span>{candidate.sleeveFit.roleFit ?? "Unavailable"}</span>
                            </div>
                            <div className="fact-line">
                              <strong>Benchmark fit</strong>
                              <span>{candidate.sleeveFit.benchmarkFit ?? "Unavailable"}</span>
                            </div>
                            <div className="fact-line">
                              <strong>Scope fit</strong>
                              <span>{candidate.sleeveFit.scopeFit ?? "Unavailable"}</span>
                            </div>
                          </div>
                          {candidate.sleeveFit.thesis ? (
                            <div className="etf-inline-meta" style={{ marginTop: 8 }}>{candidate.sleeveFit.thesis}</div>
                          ) : null}
                        </section>

                        <section className="compare-card-section">
                          <div className="row-cell-label">Key implementation</div>
                          <div className="compare-stat-grid">
                            {[
                              ...candidate.implementationStats,
                              ...(candidate.domicile ? [{ label: "Domicile", value: candidate.domicile }] : []),
                              ...(candidate.tradingCurrency ? [{ label: "Currency", value: candidate.tradingCurrency }] : []),
                              ...(candidate.listingExchange ? [{ label: "Exchange", value: candidate.listingExchange }] : []),
                            ].slice(0, 6).map((stat) => (
                              <div className="compare-stat-cell" key={`${candidate.id}-${stat.label}`}>
                                <span>{stat.label}</span>
                                <strong>{stat.value}</strong>
                              </div>
                            ))}
                          </div>
                        </section>

                        <section className="compare-card-section">
                          <div className="row-cell-label">Risk and evidence</div>
                          <div className="fact-list">
                            <div className="fact-line">
                              <strong>Evidence</strong>
                              <span>{candidate.riskEvidence.evidenceStatus ?? "Unavailable"}</span>
                            </div>
                            <div className="fact-line">
                              <strong>Timing</strong>
                              <span>{candidate.riskEvidence.timingStatus ?? "Unavailable"}</span>
                            </div>
                          </div>
                          {candidate.riskEvidence.impactLine ? (
                            <div className="etf-inline-meta" style={{ marginTop: 8 }}>{candidate.riskEvidence.impactLine}</div>
                          ) : null}
                        </section>

                        <section className="compare-card-section">
                          <div className="row-cell-label">Actions</div>
                          <div className="compare-card-actions">
                            <button
                              className="action-btn action-btn-primary"
                              type="button"
                              onClick={() => {
                                setSelectedCandidateId(candidate.id);
                                setExpandedCandidateId(candidate.id);
                                setComparePanelOpen(false);
                                void ensureReport(candidate.id);
                              }}
                            >
                              Quick brief
                            </button>
                            <button className="action-btn active" type="button" disabled>
                              Compare
                            </button>
                            <button
                              className="action-btn"
                              type="button"
                              onClick={() => {
                                setComparePanelOpen(false);
                                openReport(candidate.id);
                              }}
                            >
                              Deep report
                            </button>
                          </div>
                        </section>
                      </div>
                    </section>
                  ))}
                </div>

                {compareDecision?.deltaRows.length ? (
                  <section className="compare-decision-section">
                    <div className="compare-decision-section-head">
                      <div>
                        <div className="panel-kicker">Decision delta table</div>
                        <h3>Where the comparison actually separates</h3>
                      </div>
                    </div>
                    <div className="compare-delta-table">
                      <div className="compare-delta-row compare-delta-head">
                        <span>Field</span>
                        <span>{comparedCandidates[0]?.symbol ?? "Candidate A"}</span>
                        <span>{comparedCandidates[1]?.symbol ?? "Candidate B"}</span>
                        <span>Winner / implication</span>
                      </div>
                      {compareDecision.deltaRows.map((row) => (
                        <div className="compare-delta-row" key={row.id}>
                          <span>{row.label}</span>
                          <span>{row.candidateAValue}</span>
                          <span>{row.candidateBValue}</span>
                          <span>
                            {row.winner ? <strong className="compare-delta-winner">{row.winner}</strong> : null}
                            {row.implication ? <em>{row.implication}</em> : null}
                          </span>
                        </div>
                      ))}
                    </div>
                  </section>
                ) : null}

                {(compareDecision?.portfolioConsequence.candidateA || compareDecision?.portfolioConsequence.candidateB) ? (
                  <section className="compare-decision-section">
                    <div className="compare-decision-section-head">
                      <div>
                        <div className="panel-kicker">Portfolio consequence</div>
                        <h3>What changes if this line receives the next dollar</h3>
                      </div>
                    </div>
                    <div className="compare-consequence-grid">
                      {[compareDecision.portfolioConsequence.candidateA, compareDecision.portfolioConsequence.candidateB].filter(Boolean).map((item) => (
                        <div className="compare-consequence-card" key={item!.candidateId}>
                          <div className="compare-consequence-title">
                            <strong>{item!.symbol}</strong>
                            <span className="chip">Confidence {item!.confidence}</span>
                          </div>
                          <p>{item!.portfolioEffect}</p>
                          <div className="fact-list">
                            <div className="fact-line"><strong>Concentration</strong><span>{item!.concentrationEffect}</span></div>
                            <div className="fact-line"><strong>Region</strong><span>{item!.regionExposureEffect}</span></div>
                            <div className="fact-line"><strong>Trading line</strong><span>{item!.currencyOrTradingLineEffect}</span></div>
                            <div className="fact-line"><strong>Diversification</strong><span>{item!.diversificationEffect}</span></div>
                            <div className="fact-line"><strong>Funding path</strong><span>{item!.fundingPathEffect}</span></div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </section>
                ) : null}

                {compareDecision?.scenarioWinners.length ? (
                  <section className="compare-decision-section">
                    <div className="compare-decision-section-head">
                      <div>
                        <div className="panel-kicker">Scenario winners</div>
                        <h3>Which candidate wins under different market jobs</h3>
                      </div>
                    </div>
                    <div className="compare-scenario-table">
                      {compareDecision.scenarioWinners.map((row) => (
                        <div className="compare-scenario-row" key={row.scenario}>
                          <div>
                            <strong>{row.scenario}</strong>
                            <span>{row.why ?? "Scenario rationale unavailable."}</span>
                          </div>
                          <div><b>{comparedCandidates[0]?.symbol ?? "A"}</b><span>{row.candidateAEffect}</span></div>
                          <div><b>{comparedCandidates[1]?.symbol ?? "B"}</b><span>{row.candidateBEffect}</span></div>
                          <div><b>Winner</b><span>{row.winner ?? "No clear winner"}</span></div>
                        </div>
                      ))}
                    </div>
                  </section>
                ) : null}

                {(compareDecision?.flipConditions.length || compareDecision?.evidenceDiff.evidenceNeededToDecide.length) ? (
                  <section className="compare-decision-section">
                    <div className="compare-decision-section-head">
                      <div>
                        <div className="panel-kicker">Flip conditions and evidence</div>
                        <h3>What would change the decision</h3>
                      </div>
                      {compareDecision.evidenceDiff.strongerEvidence ? <span className="chip">Evidence {compareDecision.evidenceDiff.strongerEvidence}</span> : null}
                    </div>
                    <div className="compare-evidence-grid">
                      <div>
                        <div className="row-cell-label">Flip conditions</div>
                        <div className="fact-list">
                          {compareDecision.flipConditions.map((row) => (
                            <div className="fact-line" key={row.condition}>
                              <strong>{row.condition}</strong>
                              <span>{compactParts([row.currentState, row.thresholdOrTrigger, row.flipsToward ? `Toward ${row.flipsToward}` : null])}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                      <div>
                        <div className="row-cell-label">Evidence still needed</div>
                        <div className="fact-list">
                          {compareDecision.evidenceDiff.evidenceNeededToDecide.map((item) => (
                            <div className="fact-line" key={item}>
                              <strong>Need</strong>
                              <span>{item}</span>
                            </div>
                          ))}
                        </div>
                        {compareDecision.evidenceDiff.unresolvedFields.length ? (
                          <div className="fact-chip-row" style={{ marginTop: 10 }}>
                            {compareDecision.evidenceDiff.unresolvedFields.map((field) => (
                              <span className="chip" key={field}>{field}</span>
                            ))}
                          </div>
                        ) : null}
                      </div>
                    </div>
                  </section>
                ) : null}
              </>
            )}
          </div>
        </aside>
      </div>
    );
  }

  function renderBlueprintSurface(display: BlueprintDisplay) {
    const activeSleeve = display.sleeves.find((sleeve) => sleeve.id === activeSleeveId) ?? display.sleeves[0] ?? null;
    const leaderCandidate = activeSleeve?.candidates[0] ?? null;
    const coverageData = coverageAudit.data;
    const overlayAbsent = !!activeSleeve && activeSleeve.candidates.every((candidate) => !candidate.currentWeight && candidate.weightState === "Overlay Absent");
    const totalCandidates = display.sleeves.reduce((sum, sleeve) => sum + sleeve.candidateCount, 0);
    const auditGroups = display.changesAuditGroups ?? [];
    const auditOnlyCount = display.changesSummary?.audit_only_count ?? auditGroups.reduce((sum, group) => sum + (group.count ?? 0), 0);
    const globalOverviewChips = [
      { label: "Total sleeves", value: String(display.sleeves.length) },
      { label: "Total candidates", value: String(totalCandidates) },
      { label: "Freshness", value: summaryChipValue(display.summaryChips, "Freshness") },
      { label: "Material changes", value: String(display.changesSummary?.material_changes ?? display.changesSummary?.total_changes ?? display.changes.length) },
    ];
    const sleeveOverviewChips = [
      { label: "Active sleeve", value: activeSleeve?.name ?? "Unavailable" },
      { label: "Sleeve posture", value: activeSleeve?.statusLabel ?? "Unavailable" },
      { label: "Reviewable", value: activeSleeve ? String(activeSleeve.actionableCandidateCount + activeSleeve.reviewableCandidateCount) : "0" },
      { label: "Active support", value: activeSleeve ? String(activeSleeve.activeSupportCandidateCount) : "0" },
      { label: "Leader", value: leaderCandidate?.symbol ?? "—" },
    ];
    const windowedChanges = display.changes;
    const availableChangeSleeves = (() => {
      const options: Array<{ sleeve_id: string | null; sleeve_name: string; sleeve_label: string }> = [];
      const seen = new Set<string>();
      const addOption = (sleeve_id: string | null, sleeve_name: string, sleeve_label?: string | null) => {
        const value = sleeve_id ?? sleeve_name;
        if (!value || seen.has(value)) return;
        seen.add(value);
        options.push({
          sleeve_id,
          sleeve_name,
          sleeve_label: sleeve_label || sleeve_name,
        });
      };
      display.sleeves.forEach((sleeve) => {
        addOption(sleeve.id, sleeve.name, sleeve.candidates[0]?.symbol ?? sleeve.name);
      });
      display.changesAvailableSleeves.forEach((sleeve) => {
        addOption(sleeve.sleeve_id ?? null, sleeve.sleeve_name, sleeve.sleeve_name);
      });
      return options;
    })();
    const effectiveSleeveFilter =
      changesSleeveFilter === "all" || availableChangeSleeves.some((sleeve) => (sleeve.sleeve_id ?? sleeve.sleeve_name) === changesSleeveFilter)
        ? changesSleeveFilter
        : "all";
    const changeMatchesSleeve = (change: ChangeDisplay) => {
      if (effectiveSleeveFilter === "all") return true;
      return change.sleeveId === effectiveSleeveFilter || change.sleeve === effectiveSleeveFilter;
    };
    const sleeveFilteredChanges = windowedChanges.filter(changeMatchesSleeve);
    const selectedSleeveSummaryCounts = {
      total: sleeveFilteredChanges.length,
      upgrades: sleeveFilteredChanges.filter((change) => changeMatchesCategory(change, "upgrades")).length,
      downgrades: sleeveFilteredChanges.filter((change) => changeMatchesCategory(change, "downgrades")).length,
      blockerChanges: sleeveFilteredChanges.filter((change) => changeMatchesCategory(change, "blocker_changes")).length,
      requiresReview: sleeveFilteredChanges.filter((change) => changeMatchesCategory(change, "requires_review")).length,
    };
    const changeSummaryCounts =
      effectiveSleeveFilter === "all"
        ? {
          total: display.changesSummary?.material_changes ?? display.changesSummary?.total_changes ?? windowedChanges.length,
          upgrades: display.changesSummary?.material_upgrades ?? display.changesSummary?.upgrades ?? 0,
          downgrades: display.changesSummary?.material_downgrades ?? display.changesSummary?.downgrades ?? 0,
          blockerChanges: display.changesSummary?.blocker_changes ?? 0,
          requiresReview: display.changesSummary?.requires_review ?? 0,
        }
        : selectedSleeveSummaryCounts;
    const filteredChanges = windowedChanges.filter(
      (change) => changeMatchesCategory(change, changesTypeFilter) && changeMatchesSleeve(change),
    );
    const visibleChanges = filteredChanges;
    const visibleAuditGroups = auditGroups
      .map((group) => {
        const groupEvents = (group.events ?? []).filter((event) => {
          if (effectiveSleeveFilter === "all") return true;
          return event.sleeve_id === effectiveSleeveFilter || event.sleeve_name === effectiveSleeveFilter;
        });
        return {
          ...group,
          events: groupEvents,
          displayCount: effectiveSleeveFilter === "all" ? group.count : groupEvents.length,
        };
      })
      .filter((group) => group.displayCount > 0);
    const hasMoreFilteredChanges =
      Boolean(display.changesPagination?.has_more)
      && filteredChanges.length === windowedChanges.length;
    const visibleChangeCategories: ExplorerChangesType[] = auditOnlyCount > 0
      ? ["all", "requires_review", "upgrades", "downgrades", "audit_only"]
      : ["all", "requires_review", "upgrades", "downgrades"];
    const changeSummaryCards: Array<{
      id: ExplorerChangesType;
      label: string;
      count: number;
      cardClass?: string;
    }> = [
      { id: "all", label: changeWindowLabel(changesWindow), count: changeSummaryCounts.total },
      { id: "upgrades", label: "upgrades", count: changeSummaryCounts.upgrades, cardClass: "upgrade" },
      { id: "downgrades", label: "downgrades", count: changeSummaryCounts.downgrades, cardClass: "downgrade" },
      { id: "blocker_changes", label: "blocker changes", count: changeSummaryCounts.blockerChanges, cardClass: "downgrade" },
      { id: "requires_review", label: "require review", count: changeSummaryCounts.requiresReview, cardClass: "review" },
      ...(auditOnlyCount > 0
        ? [{ id: "audit_only" as ExplorerChangesType, label: "audit only", count: auditOnlyCount, cardClass: "audit" }]
        : []),
    ];
    const dailySourceScan = display.changesDailySourceScan;
    const changesFreshnessText =
      display.changesFreshness.state === "degraded_runtime"
        ? "Changes runtime is degraded; feed is based on stored events and route-triggered refresh."
        : display.changesFreshness.state === "stale" && display.changesFreshness.latestEventAgeDays !== null
          ? `Latest change is ${display.changesFreshness.latestEventAgeDays} days old.`
          : null;
    const dailySourceScanText =
      dailySourceScan?.status === "success" && dailySourceScan.no_material_change
        ? "Fresh source scan complete. No material Blueprint changes today."
        : dailySourceScan?.status === "not_run"
          ? "No Blueprint daily source scan has run for the current trading day."
          : dailySourceScan?.status === "failed"
            ? "Blueprint daily source scan failed; Changes is showing stored events only."
            : null;
    const emptyChangesMessage =
      !windowedChanges.length && dailySourceScan?.status === "success" && dailySourceScan.no_material_change
        ? "Fresh source scan complete. No material Blueprint changes today."
        : display.changesEmptyMessage ?? "No changes match the selected filters.";
    const changesWorkspaceSection = (
      <section className="blueprint-review-lane blueprint-embedded-changes blueprint-changes-workspace">
        <div className="candidate-page-header blueprint-page-header blueprint-changes-header">
          <div className="page-kicker">Changes log</div>
          <h2>Changes</h2>
          <p>Track recommendation state changes, upgrades, downgrades, and blocker events.</p>
        </div>

        <div className="changes-summary-strip">
          {changeSummaryCards.map((card) => (
            <button
              key={card.id}
              type="button"
              className={`changes-stat-chip ${card.cardClass ?? ""} ${changesTypeFilter === card.id ? "active" : ""}`.trim()}
              onClick={() => {
                setChangesTypeFilter(card.id);
                setShowAllExplorerChanges(false);
                setExpandedChangeId(null);
              }}
            >
              <span className="changes-stat-num">{card.count}</span>
              {card.label}
            </button>
          ))}
        </div>

        <div className="changes-filter-bar">
          <div className="changes-filter-group">
            <span className="changes-filter-label">Time</span>
            {([
              ["today", "Today"],
              ["3d", "3 days"],
              ["7d", "7 days"],
            ] as Array<[ExplorerChangesWindow, string]>).map(([value, label]) => (
              <button
                key={value}
                className={`filter-chip ${changesWindow === value ? "active" : ""}`}
                type="button"
                onClick={() => {
                  setChangesWindow(value);
                  setExpandedChangeId(null);
                }}
              >
                {label}
              </button>
            ))}
          </div>

          <div className="changes-filter-group">
            <span className="changes-filter-label">Type</span>
            {visibleChangeCategories.map((value) => (
              <button
                key={value}
                className={`filter-chip ${changesTypeFilter === value ? "active" : ""}`}
                type="button"
                onClick={() => {
                  setChangesTypeFilter(value as ExplorerChangesType);
                  setShowAllExplorerChanges(false);
                  setExpandedChangeId(null);
                }}
              >
                {changeCategoryLabel(value)}
              </button>
            ))}
          </div>

          <div className="changes-filter-group">
            <span className="changes-filter-label">Sleeve</span>
            <button
              className={`filter-chip ${effectiveSleeveFilter === "all" ? "active" : ""}`}
              type="button"
              onClick={() => {
                setChangesSleeveFilter("all");
                setShowAllExplorerChanges(false);
                setExpandedChangeId(null);
              }}
            >
              All
            </button>
            {availableChangeSleeves.map((sleeve) => {
              const sleeveValue = sleeve.sleeve_id ?? sleeve.sleeve_name;
              return (
                <button
                  key={sleeveValue}
                  className={`filter-chip changes-sleeve-chip ${effectiveSleeveFilter === sleeveValue ? "active" : ""}`}
                  type="button"
                  title={sleeve.sleeve_name}
                  onClick={() => {
                    setChangesSleeveFilter(sleeveValue);
                    setShowAllExplorerChanges(false);
                    setExpandedChangeId(null);
                  }}
                >
                  {sleeve.sleeve_label}
                </button>
              );
            })}
          </div>
        </div>

        {changes.error ? <div className="surface-error">{changes.error}</div> : null}
        {changesFreshnessText ? <div className="surface-note">{changesFreshnessText}</div> : null}
        {dailySourceScanText ? <div className="surface-note">{dailySourceScanText}</div> : null}
        {changesTypeFilter !== "audit_only" && auditGroups.length ? (
          <div className="changes-audit-summary">
            <span>
              Historical audit context available: {auditOnlyCount} review movement{auditOnlyCount === 1 ? "" : "s"} without preserved drivers.
            </span>
            <button
              className="changes-audit-link"
              type="button"
              onClick={() => {
                setChangesTypeFilter("audit_only");
                setShowAllExplorerChanges(false);
                setExpandedChangeId(null);
              }}
            >
              Show audit only
            </button>
          </div>
        ) : null}

        {changes.loading && !changes.data ? (
          <div className="surface-placeholder">Loading the current Blueprint changes feed.</div>
        ) : changesTypeFilter === "audit_only" ? (
          visibleAuditGroups.length ? (
            <div className="changes-audit-groups">
              {visibleAuditGroups.map((group) => (
                <div className="changes-audit-group" key={group.group_id}>
                  <div className="changes-audit-group-header">
                    <div>
                      <span className="changes-audit-group-kicker">Audit context</span>
                      <strong>{group.title}</strong>
                    </div>
                    <span>{group.displayCount} movement{group.displayCount === 1 ? "" : "s"}</span>
                  </div>
                  <p>{group.summary}</p>
                  {group.events?.length ? (
                    <div className="changes-audit-event-list">
                      {group.events.map((event) => {
                        const from = detailText(event.transition?.from);
                        const to = detailText(event.transition?.to);
                        const transition = from || to ? `${from || "Prior state"} -> ${to || "Current state"}` : "Review movement";
                        return (
                          <div className="changes-audit-event" key={event.event_id ?? `${event.ticker}-${event.changed_at_utc}`}>
                            <strong>{event.ticker || "ETF"}</strong>
                            <span>{event.sleeve_name || "Blueprint"}</span>
                            <span>{transition}</span>
                            <span>{changeAgeText(event.event_age_hours) || "Historical"}</span>
                          </div>
                        );
                      })}
                    </div>
                  ) : null}
                  {group.has_more_events ? (
                    <div className="changes-audit-group-note">
                      Showing {group.events_returned ?? group.events?.length ?? 0} examples. Remaining audit movements stay grouped to keep the investor feed light.
                    </div>
                  ) : null}
                </div>
              ))}
            </div>
          ) : (
            <div className="changes-empty">No audit-only movements match the selected filters.</div>
          )
        ) : filteredChanges.length ? (
          <>
            <div className="changes-feed">
              {visibleChanges.map((change) => {
                const expanded = expandedChangeId === change.id;
                const visibleSummary = detailText(change.changeDetail?.summary) || change.whyItMatters || change.implication;
                const visiblePreviousState = detailText(change.changeDetail?.state_transition?.from) || change.previousState;
                const visibleCurrentState = detailText(change.changeDetail?.state_transition?.to) || change.currentState;
                const driverLabel = detailText(change.changeDetail?.driver_label);
                const isHistorical = change.changeDetail?.is_current === false;
                const isCompactAudit = isCompactAuditChange(change);
                const isFullInvestor = isFullInvestorChange(change);
                const triggerLine = formatTriggerLine(change);
                const impactLine = formatImpactLine(change);
                const ageText = changeAgeText(change.changeDetail?.event_age_hours);
                return (
                  <div
                    key={change.id}
                    className={`change-card impact-${change.impactLevel} direction-${normalizeChangeCode(change.direction)}${change.needsReview ? " needs-review" : ""}${expanded ? " expanded" : ""}${isHistorical ? " historical" : ""}${isCompactAudit ? " compact-audit" : ""}${isFullInvestor ? " full-investor" : ""}`}
                    role="button"
                    tabIndex={0}
                    aria-expanded={expanded}
                    onClick={() => {
                      setExpandedChangeId(expanded ? null : change.id);
                    }}
                    onKeyDown={(event) => {
                      if (event.key !== "Enter" && event.key !== " ") return;
                      event.preventDefault();
                      setExpandedChangeId(expanded ? null : change.id);
                    }}
                  >
                    <div className="change-card-header">
                      <span className="change-ticker">{change.ticker}</span>
                      <span className="change-sleeve-tag">{change.sleeve}</span>
                      <span className="change-type-tag">{changeCategoryLabel(change.category)}</span>
                      <span className={`change-impact impact-${change.impactLevel}`}>{change.impactLabel}</span>
                      {change.needsReview ? <span className="change-review-flag">Review</span> : null}
                      {isHistorical ? <span className="change-history-flag">Historical</span> : null}
                      {isCompactAudit ? <span className="change-audit-flag">Audit</span> : null}
                      <span className="change-time">{change.timestamp}</span>
                      {ageText ? <span className="change-age">{ageText}</span> : null}
                    </div>
                    {isFullInvestor && triggerLine ? (
                      <div className="change-trigger-line">
                        <span>Trigger</span>
                        <strong>{triggerLine}</strong>
                      </div>
                    ) : null}
                    <div className={isFullInvestor ? "change-state-line change-result-line" : "change-state-line"}>
                      {isFullInvestor ? <span className="change-result-label">Result</span> : null}
                      <span className="change-old">{visiblePreviousState}</span>
                      <span className="change-arrow">→</span>
                      <span className="change-new">{visibleCurrentState}</span>
                    </div>
                    {driverLabel && driverLabel !== "driver unavailable" && !isCompactAudit ? (
                      <div className="change-driver-line">Driver: {driverLabel}</div>
                    ) : null}
                    <div className="change-implication">{isFullInvestor ? impactLine : visibleSummary}</div>
                    {expanded || isCompactAudit ? (
                      <ChangeEventDetailCard
                        change={change}
                        onOpenRecommendation={openChangeRecommendation}
                        onOpenReport={openChangeReport}
                      />
                    ) : null}
                  </div>
                );
              })}
            </div>
            {hasMoreFilteredChanges ? (
              <div className="blueprint-embedded-changes-footer">
                <button
                  className="action-btn action-btn-text"
                  type="button"
                  onClick={() => {
                    setShowAllExplorerChanges((state) => !state);
                    setExpandedChangeId(null);
                  }}
                >
                  {showAllExplorerChanges ? "Show fewer" : `Show all ${display.changesPagination?.total_matching ?? filteredChanges.length} changes`}
                </button>
              </div>
            ) : null}
          </>
        ) : (
          <div className="changes-empty">
            {changes.loading
              ? "Refreshing the current changes window."
              : windowedChanges.length
                ? "No changes match the selected filters."
                : emptyChangesMessage}
          </div>
        )}
      </section>
    );

    return (
      <div className="blueprint-shell blueprint-horizontal-shell">
        {display.degradedMessage ? <div className="surface-warning">{display.degradedMessage}</div> : null}
        {blueprint.error ? <div className="surface-error">{blueprint.error}</div> : null}

        <div className="candidate-page-header blueprint-page-header">
          <div className="page-kicker">Blueprint</div>
          <h2 style={{ margin: "4px 0 6px", fontFamily: "var(--font-display)", fontSize: 28, lineHeight: 1.05 }}>Candidate workspace</h2>
        </div>

        <div className="blueprint-summary-stack">
          <div className="candidate-stats-strip blueprint-compact-strip blueprint-summary-strip">
            {globalOverviewChips.map((chip) => (
              <div className="stat-tile" key={chip.label}>
                <div className="stat-value">{chip.value}</div>
                <div className="stat-label">{chip.label}</div>
              </div>
            ))}
          </div>

          <div className="candidate-stats-strip blueprint-compact-strip blueprint-summary-strip blueprint-summary-strip-secondary">
            {sleeveOverviewChips.map((chip) => (
              <div className="stat-tile" key={chip.label}>
                <div className="stat-value">{chip.value}</div>
                <div className="stat-label">{chip.label}</div>
              </div>
            ))}
          </div>
        </div>

        <section className="blueprint-sleeve-selector">
          {display.sleeves.map((sleeve) => {
            const leader = sleeve.leadCandidateName ?? sleeve.candidates[0]?.symbol ?? "Leader pending";
            const leaderSymbol = sleeve.candidates[0]?.symbol ?? "—";
            const recommendationScore = sleeve.recommendationScore;
            const sleevePostureLabel = sleeveDeployabilityLabel(sleeve.sleeveStateRaw);
            const sleevePostureTone = sleeveDeployabilityTone(sleeve.sleeveStateRaw);
            return (
              <button
                className={`blueprint-sleeve-card ${sleeve.isNested ? "nested" : ""} ${activeSleeve?.id === sleeve.id ? "active" : ""}`}
                key={sleeve.id}
                type="button"
                onClick={() => {
                  setActiveSleeveId(sleeve.id);
                  setSelectedCandidateId(sleeve.candidates[0]?.id ?? null);
                  setExpandedCandidateId(null);
                  setComparePanelOpen(false);
                }}
              >
                <div className="blueprint-sleeve-card-top">
                  <div>
                    <div className="blueprint-sleeve-card-structure">
                      {sleeve.isNested ? `Nested in ${sleeve.parentSleeveName ?? "Global Equity Core"}` : "Top-level sleeve"} · #{sleeve.priorityRank}
                    </div>
                    <div className="blueprint-sleeve-card-title">{sleeve.name}</div>
                  </div>
                </div>
                <div className="blueprint-sleeve-card-data">
                  <div className="blueprint-sleeve-card-target-row">
                    <span>Target</span>
                    <strong>{sleeve.targetLabel}</strong>
                  </div>
                  <div className="blueprint-sleeve-card-target-row">
                    <span>Range</span>
                    <strong>{sleeve.rangeLabel}</strong>
                  </div>
                  <div title={leader}>
                    <span>Leader</span>
                    <strong>{leaderSymbol}</strong>
                  </div>
                  <div>
                    <span>Candidates</span>
                    <strong>{sleeve.candidateCount}</strong>
                  </div>
                </div>
                {recommendationScore ? (
                  <div
                    className="blueprint-sleeve-card-score"
                    aria-label={
                      recommendationScore.scoreBasis === "recommendation_score"
                        ? `Sleeve recommendation score: ${recommendationScore.averageScore} out of 100, based on leader recommendation quality, sleeve depth, leader truth confidence, and current row actionability.`
                        : recommendationScore.scoreBasis === "deployment_score"
                        ? `Sleeve deployment score: ${recommendationScore.averageScore} out of 100, based on leader deployability, sleeve depth, sleeve actionability, and blocker burden.`
                        : `Sleeve recommendation score: ${recommendationScore.averageScore} out of 100, based on ${recommendationScore.pillarCountUsed} sleeve score pillars`
                    }
                    title={
                      recommendationScore.scoreBasis === "recommendation_score"
                        ? `Sleeve recommendation score: ${recommendationScore.averageScore} out of 100. Leader recommendation ${recommendationScore.leaderCandidateRecommendationScore ?? "—"}, leader recommendation merit ${recommendationScore.leaderCandidateInvestmentMeritScore ?? "—"}, leader deployability ${recommendationScore.leaderCandidateDeployabilityScore ?? "—"}, leader truth confidence ${recommendationScore.leaderTruthConfidenceScore ?? "—"}, depth ${recommendationScore.depthScore ?? "—"}, row actionability ${recommendationScore.blockerBurdenScore ?? "—"}.`
                        : recommendationScore.scoreBasis === "deployment_score"
                        ? `Sleeve deployment score: ${recommendationScore.averageScore} out of 100. Leader deployability ${recommendationScore.leaderCandidateDeploymentScore ?? "—"}, depth ${recommendationScore.depthScore ?? "—"}, actionability ${recommendationScore.sleeveActionabilityScore ?? "—"}, blocker burden ${recommendationScore.blockerBurdenScore ?? "—"}.`
                        : `Sleeve recommendation score: ${recommendationScore.averageScore} out of 100, based on ${recommendationScore.pillarCountUsed} sleeve score pillars`
                    }
                  >
                    <div className="blueprint-sleeve-card-score-track">
                      <div
                        className={`blueprint-sleeve-card-score-fill tone-${recommendationScore.tone}`}
                        style={{ width: `${Math.max(0, Math.min(100, recommendationScore.averageScore))}%` }}
                      />
                    </div>
                    <div className={`blueprint-sleeve-card-score-value tone-${recommendationScore.tone}`}>
                      {scoreRead(recommendationScore.averageScore)}
                    </div>
                  </div>
                ) : null}
                <div className="blueprint-sleeve-card-signal-band">
                  <span className={pillClass(sleevePostureTone)}>{sleevePostureLabel}</span>
                </div>
              </button>
            );
          })}
        </section>

        {activeSleeve ? (
          <section className="blueprint-workspace-stack">
            <section className="blueprint-focus-strip">
              <div className="blueprint-focus-strip-main">
                <div className="panel-kicker">Active sleeve</div>
                <div className="blueprint-focus-strip-head">
                  <div>
                    <div className="panel-title" style={{ marginTop: 8 }}>{activeSleeve.name}</div>
                    <div className="panel-copy">{activeSleeve.sleeveRoleStatement ?? activeSleeve.purpose}</div>
                    <div className="fact-chip-row" style={{ marginTop: 12 }}>
                      <span className="chip">#{activeSleeve.priorityRank}</span>
                      <span className="chip">Target {activeSleeve.targetLabel}</span>
                      <span className="chip">Range {activeSleeve.rangeLabel}</span>
                      <span className="chip">{activeSleeve.isNested ? `Nested in ${activeSleeve.parentSleeveName ?? "Global Equity Core"}` : "Top-level sleeve"}</span>
                      <span className="chip">{activeSleeve.actionableCandidateCount + activeSleeve.reviewableCandidateCount} reviewable</span>
                      <span className="chip">{activeSleeve.activeSupportCandidateCount} with active support</span>
                      {activeSleeve.currentWeight ? <span className="chip">Current {activeSleeve.currentWeight}</span> : null}
                    </div>
                  </div>
                  <div className="blueprint-focus-top-meta">
                    <span className={pillClass(activeSleeve.statusTone)}>{activeSleeve.statusLabel}</span>
                    {leaderCandidate ? <span className="chip">Leader {leaderCandidate.symbol}</span> : null}
                    {activeSleeve.leaderBlockedButReviewable ? <span className="chip chip-amber">Leader blocked</span> : null}
                  </div>
                </div>

                <div className="blueprint-focus-callouts">
                  <div className="blueprint-focus-callout">
                    <span>Current posture</span>
                    <strong>{activeSleeve.statusLabel}</strong>
                    <p>{compactSentence(activeSleeve.postureSummary, activeSleeve.statusLabel)}</p>
                  </div>
                  <div className="blueprint-focus-callout">
                    <span>Why the leader matters</span>
                    <strong>{compactSentence(leaderCandidate?.implicationSummary ?? activeSleeve.whyItLeads, "Leader context is still being cleaned up.")}</strong>
                    <p>{compactSentence(leaderCandidate?.whyNow ?? activeSleeve.baseAllocationRationale ?? activeSleeve.capitalMemo, "The current leader still carries the clearest sleeve role.")}</p>
                  </div>
                  <div className="blueprint-focus-callout">
                    <span>{activeSleeve.blockLabel}</span>
                    <strong>{compactSentence(leaderCandidate?.whatBlocksAction ?? activeSleeve.mainLimit, "No investor-facing blocker has been surfaced yet.")}</strong>
                    <p>{compactSentence(leaderCandidate?.actionBoundary ?? activeSleeve.fundingPath?.summary, "Decision boundary still needs cleanup.")}</p>
                  </div>
                  <div className="blueprint-focus-callout">
                    <span>{activeSleeve.reopenLabel}</span>
                    <strong>{compactSentence(activeSleeve.reopenCondition ?? leaderCandidate?.whatChangesView, "No investor-facing reopen trigger has been surfaced yet.")}</strong>
                    <p>{compactSentence(activeSleeve.fundingPath?.fundingSource ?? activeSleeve.capitalMemo, "The sleeve needs cleaner capital or evidence support before the read can strengthen.")}</p>
                  </div>
                </div>

                {activeSleeve.supportPillars.slice(0, 6).length ? (
                  <div className="blueprint-focus-pillars">
                    {activeSleeve.supportPillars.slice(0, 6).map((pillar) => (
                      <div className="blueprint-focus-pillar" key={`${activeSleeve.id}-${pillar.label}`}>
                        <span>{pillar.label}</span>
                        <div className="support-wrap">
                          <strong className={`support-num tone-${pillar.tone}`}>{pillar.score}</strong>
                          <div className="support-track">
                            <div className={`support-fill tone-${pillar.tone}`} style={{ width: `${Math.max(6, Math.min(100, pillar.score))}%` }} />
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : null}

              </div>

              {overlayAbsent ? (
                <div className="surface-note blueprint-overlay-note">No portfolio loaded yet.</div>
              ) : null}
            </section>

            {renderCompareLauncher(activeSleeve, display)}
            {renderCompareDrawer(display, activeSleeve)}

            <section className="etf-board blueprint-candidate-workspace">
              <div className="blueprint-board-header">
                <div>
                  <div className="panel-kicker">Candidate lane</div>
                  <div className="panel-title" style={{ marginTop: 8 }}>{activeSleeve.name} candidates</div>
                  <div className="panel-copy">Use the row to judge the decision first. Open the quick brief only when the row is not enough.</div>
                </div>
                <div className="blueprint-board-meta">
                  <span>Target {activeSleeve.targetLabel}</span>
                  <span>Range {activeSleeve.rangeLabel}</span>
                  <span>{activeSleeve.isNested ? `Nested in ${activeSleeve.parentSleeveName ?? "Global Equity Core"}` : "Top-level sleeve"}</span>
                  <span>{leaderCandidate ? `Leader ${leaderCandidate.symbol}` : "Leader pending"}</span>
                </div>
              </div>

              <div className="candidate-workspace-head">
                <span>ETF identity</span>
                <span>Verdict</span>
                <span>Key metrics</span>
                <span>Context</span>
                <span>Actions</span>
              </div>

              <div className="candidate-row-set candidate-row-set-horizontal">
                {activeSleeve.candidates.map((candidate) => {
                  const expanded = expandedCandidateId === candidate.id;
                  const isCompared = compareIds.has(candidate.id);
                  const showPosition = candidate.currentWeight && candidate.weightState !== "Overlay Absent";
                  const compactFieldIssues = candidate.fieldIssues.slice(0, 4);
                  const candidateCoverage = (coverageData?.items ?? []).find((item) => item.candidate_id === candidate.id) ?? null;
                  const stateSignals = candidateStateSignals(candidate);
                  const stateSummary = candidateStateSummary(candidate);
                  const sourceSummary = candidateSourceSummary(candidate);
                  const marketPathSummary = candidateMarketPathSummary(candidate);
                  const preview = reportPreview(candidate.id);
                  const quickBrief = preview?.quickBrief ?? candidate.quickBrief ?? null;
                  const scoreSummary =
                    preview?.scoreSummary
                    ?? candidate.scoreSummary
                    ?? fallbackScoreSummaryFromBreakdown(preview?.scoreBreakdown ?? candidate.scoreBreakdown);
                  const candidateScoreBreakdown = preview?.scoreBreakdown ?? candidate.scoreBreakdown ?? null;
                  const inlineRecommendationScore =
                    candidateScoreBreakdown?.recommendation
                    ?? scoreSummary?.averageScore
                    ?? candidate.score;
                  const inlineTruthConfidenceScore = candidateScoreBreakdown?.truthConfidence ?? null;
                  const inlineTruthConfidenceBand = candidateScoreBreakdown?.truthConfidenceBand ?? null;
                  const inlineDeployabilityBadge = candidateScoreBreakdown?.deployabilityBadge ?? null;
                  const scoreFamilyComponents = scoreSummary?.components ?? [];
                  const quickBriefScoreFamilyComponents = scoreFamilyComponents.filter(
                    (component) => !["instrument_quality", "portfolio_fit"].includes(String(component.id ?? "").trim().toLowerCase())
                  );
                  const briefIdentity = quickBrief?.fundIdentity ?? {
                    ticker: candidate.symbol,
                    name: candidate.name,
                    issuer: candidate.issuer ?? null,
                    exposureLabel: candidate.benchmarkFullName ?? candidate.exposureSummary ?? null,
                  };
                  const briefStatusLabel = quickBrief?.statusLabel ?? candidate.decisionState;
                  const briefStatusTone = quickBrief ? quickBriefStatusTone(quickBrief.statusState) : candidate.decisionTone;
                  const kronosMarketSetup = quickBrief?.kronosMarketSetup ?? null;
                  const kronosDecisionBridge = quickBrief?.kronosDecisionBridge ?? null;
                  const kronosCompareCheck = quickBrief?.kronosCompareCheck ?? null;
                  const decisionReasons = (quickBrief?.decisionReasons?.length ? quickBrief.decisionReasons : quickBrief?.secondaryReasons ?? [])
                    .filter((reason) => !(kronosMarketSetup && /market|backdrop|path|regime|scenario|kronos/i.test(reason)))
                    .slice(0, 3);
                  const performanceChecks = quickBrief?.performanceChecks?.length
                    ? quickBrief.performanceChecks
                    : [
                        {
                          checkId: "cost",
                          label: "Cost",
                          summary: candidate.terBps ? `${candidate.terBps} TER currently surfaced.` : "Expense ratio is still loading.",
                          metric: candidate.terBps ?? candidate.expenseRatio ?? null,
                        },
                        {
                          checkId: "liquidity",
                          label: "Liquidity",
                          summary: candidate.spreadProxyBps ? `${candidate.spreadProxyBps} spread proxy is currently surfaced.` : "Spread still needs a cleaner read.",
                          metric: candidate.spreadProxyBps ?? null,
                        },
                        {
                          checkId: "portfolio_fit",
                          label: "Portfolio fit",
                          summary: compactSentence(candidate.whyNow, candidate.implicationSummary),
                          metric: null,
                        },
                        {
                          checkId: "decision_readiness",
                          label: "Decision readiness",
                          summary: `${candidate.decisionState} while ${(compactSentence(candidate.whatBlocksAction, candidate.decisionSummary) ?? "the current blocker set still needs review").toLowerCase()}`,
                          metric: candidate.decisionState,
                        },
                      ];
                  const evidenceFooterRows = quickBrief?.evidenceFooterDetail
                    ? [
                        { label: "Evidence quality", value: quickBrief.evidenceFooterDetail.evidenceQuality },
                        { label: "Data completeness", value: quickBrief.evidenceFooterDetail.dataCompleteness },
                        { label: "Document support", value: quickBrief.evidenceFooterDetail.documentSupport },
                        { label: "Monitoring status", value: quickBrief.evidenceFooterDetail.monitoringStatus },
                      ]
                    : [
                        ...(candidate.sourceIntegritySummary ? [{ label: "Evidence quality", value: candidate.sourceIntegritySummary.summary }] : []),
                        ...(candidate.sourceIntegritySummary ? [{ label: "Data completeness", value: `${candidate.sourceIntegritySummary.criticalReady}/${candidate.sourceIntegritySummary.criticalTotal} critical fields surfaced.` }] : []),
                      ];
                  const overlayNote = quickBrief?.overlayNote ?? "Portfolio overlay unavailable. Fund-level view shown.";
                  const documentRows = preview?.primaryDocuments ?? [];
                  const peerComparePack = quickBrief?.peerComparePack ?? null;
                  const fundProfile = quickBrief?.fundProfile ?? null;
                  const listingProfile = quickBrief?.listingProfile ?? null;
                  const indexScopeExplainer = quickBrief?.indexScopeExplainer ?? null;
                  const decisionProofPack = quickBrief?.decisionProofPack ?? null;
                  const performanceTrackingPack = quickBrief?.performanceTrackingPack ?? null;
                  const compositionPack = quickBrief?.compositionPack ?? null;
                  const documentCoverage = quickBrief?.documentCoverage ?? null;
                  const whyThisMattersLine = quickBrief?.whyThisMattersLine
                    ?? decisionProofPack?.whyInScope
                    ?? quickBrief?.whyItMatters?.find((row) => row.label.toLowerCase().includes("why this matters"))?.value
                    ?? compactSentence(candidate.implicationSummary, candidate.name);
                  const compareFirstLine = quickBrief?.compareFirstLine
                    ?? decisionProofPack?.bestSameJobPeers
                    ?? quickBrief?.whyItMatters?.find((row) => row.label.toLowerCase().includes("compare first"))?.value
                    ?? quickBrief?.shouldIUse?.compareAgainst
                    ?? "Compare first against the closest same-job substitutes before treating this line as the default pick.";
                  const broaderAlternativeLine = quickBrief?.broaderAlternativeLine
                    ?? decisionProofPack?.broaderControlPeer
                    ?? quickBrief?.whyItMatters?.find((row) => row.label.toLowerCase().includes("broader alternative"))?.value
                    ?? null;
                  const whatItSolvesLine = quickBrief?.whatItSolvesLine
                    ?? decisionProofPack?.whyCandidateExists
                    ?? quickBrief?.whyItMatters?.find((row) => row.label.toLowerCase().includes("what it solves"))?.value
                    ?? quickBrief?.portfolioRole
                    ?? activeSleeve.sleeveRoleStatement
                    ?? "Use as a sleeve-level building block rather than a portfolio-wide answer.";
                  const whatItStillNeedsToProveLine = quickBrief?.whatItStillNeedsToProveLine
                    ?? decisionProofPack?.whatMustBeTrueToPreferThis
                    ?? "It still needs cleaner proof that its current advantages are worth preferring over the closest same-job peers.";
                  const whatItDoesNotSolveLine = quickBrief?.portfolioFit?.whatItDoesNotSolve
                    ?? decisionProofPack?.whyNotCompleteSolution
                    ?? quickBrief?.whyItMatters?.find((row) => row.label.toLowerCase().includes("does not solve"))?.value
                    ?? "This ETF should be judged as one sleeve building block, not a full portfolio solution.";
                  const roleInPortfolioLine = quickBrief?.portfolioFit?.roleInPortfolio
                    ?? quickBrief?.portfolioRole
                    ?? activeSleeve.sleeveRoleStatement
                    ?? "Portfolio role is still loading.";
                  const currentNeedBaseLine = quickBrief?.portfolioFit?.currentNeed
                    ?? "Current need stays sleeve-level until a portfolio overlay is loaded.";
                  const decisionReadinessLine = quickBrief?.decisionReadinessLine
                    ?? quickBrief?.whyItMatters?.find((row) => row.label.toLowerCase().includes("decision readiness"))?.value
                    ?? performanceChecks.find((row) => row.checkId === "decision_readiness")?.summary
                    ?? `${briefStatusLabel} while ${(compactSentence(candidate.whatBlocksAction, candidate.decisionSummary) ?? "the current blocker set still needs review").toLowerCase()}`;
                  const proofDocuments = fundProfile?.documents?.length
                    ? fundProfile.documents
                    : documentRows.map((row) => ({
                        label: `${row.docType}${row.retrievedAt ? ` · ${row.retrievedAt}` : ""}`,
                        url: null,
                      }));
                  const peerComparisonRows = peerComparePack?.rows ?? [];
                  const subjectPeerRow = peerComparisonRows.find((row) => row.role === "subject") ?? null;
                  const directPeerRows = peerComparisonRows.filter((row) => row.role === "same_job_peer");
                  const broaderPeerRows = peerComparisonRows.filter((row) => row.role !== "subject" && row.role !== "same_job_peer");
                  const peerSummaryRows = [
                    directPeerRows[0] ?? null,
                    directPeerRows[1] ?? null,
                    broaderPeerRows[0] ?? null,
                  ].filter((row): row is QuickBriefPeerRow => Boolean(row));
                  const firstSameJobPeer = directPeerRows[0] ?? null;
                  const compareConclusionLead = compareConclusionHeadline(subjectPeerRow, directPeerRows);
                  const compareConclusionSupport = decisionProofPack?.feePremiumQuestion
                    ?? decisionProofPack?.whatMustBeTrueToPreferThis
                    ?? "Prefer this ETF only if its scale, trading comfort, document support, or execution quality are strong enough to justify paying more than the closest same-job peers.";
                  const showKronosCompareCheck = Boolean(
                    kronosCompareCheck?.regimeCheckText
                    && (kronosCompareCheck.affectsExposurePreference || kronosCompareCheck.affectsPeerPreference || kronosMarketSetup),
                  );
                  const currentNeedLine = [
                    currentNeedBaseLine,
                    /overlay unavailable/i.test(overlayNote) ? overlayNote : null,
                  ].filter(Boolean).join(" ");
                  const fundFactRows = [
                    ...(fundProfile?.objective ? [{ label: "Objective", value: fundProfile.objective }] : []),
                    ...(fundProfile?.benchmark ? [{ label: "Benchmark", value: fundProfile.benchmark }] : []),
                    ...(fundProfile?.benchmarkFamily ? [{ label: "Benchmark family", value: fundProfile.benchmarkFamily }] : []),
                    ...(fundProfile?.issuer ? [{ label: "Issuer", value: fundProfile.issuer }] : []),
                    ...(fundProfile?.domicile ? [{ label: "Fund domicile", value: fundProfile.domicile }] : []),
                    ...(fundProfile?.replication ? [{ label: "Replication", value: fundProfile.replication }] : []),
                    ...(fundProfile?.distribution ? [{ label: "Distribution", value: fundProfile.distribution }] : []),
                    ...(fundProfile?.fundAssets ? [{ label: "Fund assets", value: fundProfile.fundAssets }] : []),
                    ...(fundProfile?.shareClassAssets ? [{ label: "Share-class assets", value: fundProfile.shareClassAssets }] : []),
                    ...(fundProfile?.holdingsCount ? [{ label: "Holdings count", value: fundProfile.holdingsCount }] : []),
                    ...(fundProfile?.launchDate ? [{ label: "Launch date", value: fundProfile.launchDate }] : []),
                  ];
                  const listingFactRows = [
                    ...(listingProfile?.ticker ? [{ label: "Ticker", value: listingProfile.ticker }] : []),
                    ...(listingProfile?.exchange ? [{ label: "Exchange", value: listingProfile.exchange }] : []),
                    ...(listingProfile?.tradingCurrency ? [{ label: "Trading currency", value: listingProfile.tradingCurrency }] : []),
                    ...(listingProfile?.spreadProxy ? [{ label: "Spread proxy", value: listingProfile.spreadProxy }] : []),
                    ...(listingProfile?.asOf ? [{ label: "As of date", value: formatHumanDate(listingProfile.asOf) ?? listingProfile.asOf }] : []),
                  ];
                  const subjectCost = subjectPeerRow?.ter ?? performanceChecks.find((row) => row.checkId === "cost")?.metric ?? candidate.terBps ?? candidate.expenseRatio ?? null;
                  const peerCost = firstSameJobPeer?.ter ?? null;
                  const heroFactRows = [
                    ...(
                      indexScopeExplainer?.summary || indexScopeExplainer?.coverageStatement
                        ? [{
                            label: "Scope",
                            value: (indexScopeExplainer.summary ?? indexScopeExplainer.coverageStatement ?? "").replace(/\.$/, ""),
                          }]
                        : []
                    ),
                    ...((fundProfile?.benchmarkFamily ?? fundProfile?.benchmark ?? candidate.benchmarkFullName)
                      ? [{ label: "Benchmark", value: fundProfile?.benchmarkFamily ?? fundProfile?.benchmark ?? candidate.benchmarkFullName ?? "" }]
                      : []),
                    ...(subjectCost ? [{ label: "TER", value: subjectCost }] : []),
                    ...(fundProfile?.fundAssets ? [{ label: "Fund assets", value: fundProfile.fundAssets }] : []),
                    ...(fundProfile?.distribution ? [{ label: "Distribution", value: fundProfile.distribution }] : []),
                    ...(fundProfile?.domicile ? [{ label: "Domicile", value: fundProfile.domicile }] : []),
                  ].slice(0, 6);
                  const timingRibbonLine = kronosMarketSetup
                    ? [
                        kronosMarketSetup.routeLabel ?? kronosMarketSetup.pathSupportLabel ?? "Market setup unavailable",
                        kronosMarketSetup.horizonLabel ?? null,
                        kronosMarketSetup.pathSupportLabel ?? null,
                        kronosMarketSetup.confidenceLabel ? `Confidence ${kronosMarketSetup.confidenceLabel.toLowerCase()}` : null,
                        kronosMarketSetup.freshnessLabel ?? null,
                        kronosMarketSetup.decisionImpactText ?? "Does not override the current ETF verdict.",
                      ].filter(Boolean).join(". ")
                    : "Timing context is not active enough to change the current ETF verdict.";
                  const buyingWrapperRows = [
                    ...(fundProfile?.objective ? [{ label: "Objective", value: fundProfile.objective }] : []),
                    ...(fundProfile?.benchmarkFamily ? [{ label: "Benchmark family", value: fundProfile.benchmarkFamily }] : []),
                    ...(fundProfile?.issuer ? [{ label: "Issuer", value: fundProfile.issuer }] : []),
                    ...(compactParts([fundProfile?.replication, fundProfile?.distribution, fundProfile?.domicile])
                      ? [{
                          label: "Wrapper shape",
                          value: `${compactParts([fundProfile?.replication, fundProfile?.distribution, fundProfile?.domicile])}. Use this line only if the wrapper fits the account, payout need, and mandate better than nearby substitutes.`,
                        }]
                      : []),
                  ];
                  const performanceProofRows = [
                    {
                      label: "Cost",
                      value: subjectCost && peerCost
                        ? `This fund charges ${subjectCost}, while direct same-job peers sit around ${peerCost}. That gap matters only if this line gives clearly better scale, trading comfort, or confidence.`
                        : subjectCost
                          ? `This fund charges ${subjectCost}. That only matters if cheaper same-job peers do not offer comparable implementation quality.`
                          : (performanceChecks.find((row) => row.checkId === "cost")?.summary ?? "Cost needs a cleaner read before this line can earn a preference."),
                      trail: subjectCost && peerCost ? `${subjectCost} vs ${peerCost}` : subjectCost,
                    },
                    {
                      label: "Liquidity",
                      value: listingProfile?.spreadProxy
                        ? `Current spread proxy is ${listingProfile.spreadProxy}. That only helps if the actual trading line stays efficient enough to offset the cheaper peer's fee advantage.`
                        : (performanceChecks.find((row) => row.checkId === "liquidity")?.summary ?? "Liquidity still needs a cleaner live trading read."),
                      trail: listingProfile?.spreadProxy ?? performanceChecks.find((row) => row.checkId === "liquidity")?.metric ?? null,
                    },
                    {
                      label: "Tracking",
                      value:
                        (performanceTrackingPack?.trackingDifferenceCurrentPeriod || performanceTrackingPack?.trackingDifference1Y || performanceTrackingPack?.trackingError1Y)
                          ? `Current tracking looks acceptable, but that matters only if cheaper peers do not track just as well.`
                          : (performanceChecks.find((row) => row.checkId === "tracking")?.summary ?? "Tracking still needs a cleaner proof point."),
                      trail:
                        performanceTrackingPack?.trackingDifferenceCurrentPeriod
                        ?? performanceTrackingPack?.trackingDifference1Y
                        ?? performanceTrackingPack?.trackingError1Y
                        ?? null,
                    },
                    {
                      label: "Size and survivability",
                      value: fundProfile?.fundAssets
                        ? `Fund assets are ${fundProfile.fundAssets}, which supports scale and survivability but does not settle the fee-versus-implementation tradeoff on its own.`
                        : (performanceChecks.find((row) => row.checkId === "size_and_survivability")?.summary ?? "Scale needs a cleaner read."),
                      trail: fundProfile?.fundAssets ?? performanceChecks.find((row) => row.checkId === "size_and_survivability")?.metric ?? null,
                    },
                    {
                      label: "Structure",
                      value: compactParts([fundProfile?.replication, fundProfile?.distribution, fundProfile?.domicile])
                        ? `${compactParts([fundProfile?.replication, fundProfile?.distribution, fundProfile?.domicile])}. The wrapper matters only if it fits the account, mandate, and payout needs better than close substitutes.`
                        : (performanceChecks.find((row) => row.checkId === "structure")?.summary ?? "Structure needs a cleaner read."),
                      trail: compactParts([fundProfile?.domicile, shortDistributionLabel(fundProfile?.distribution)]) || null,
                    },
                  ].filter((row) => row.value);
                  const documentCoverageRows = [
                    ...(documentCoverage?.lastRefreshedAt ? [{ label: "Last refreshed", value: formatHumanDate(documentCoverage.lastRefreshedAt) ?? documentCoverage.lastRefreshedAt }]
                      : []),
                    ...(documentCoverage?.documentCount !== null && documentCoverage?.documentCount !== undefined
                      ? [{ label: "Document count", value: String(documentCoverage.documentCount) }]
                      : []),
                    ...(documentCoverage?.documentConfidenceGrade ? [{ label: "Document confidence", value: documentCoverage.documentConfidenceGrade }] : []),
                  ];
                  const compositionSummaryRows = [
                    ...(compositionPack?.numberOfStocks ? [{ label: "Number of stocks", value: compositionPack.numberOfStocks }] : []),
                    ...(compositionPack?.top10Weight ? [{ label: "Top 10 weight", value: compositionPack.top10Weight }] : []),
                    ...(compositionPack?.usWeight ? [{ label: "U.S. weight", value: compositionPack.usWeight }] : []),
                    ...(compositionPack?.nonUsWeight ? [{ label: "Non-U.S. weight", value: compositionPack.nonUsWeight }] : []),
                    ...(compositionPack?.emWeight ? [{ label: "EM weight", value: compositionPack.emWeight }] : []),
                  ];
                  const presentDocumentLabels = proofDocuments.map((row) => row.label.split(" · ")[0]).slice(0, 4);
                  const missingDocumentLabels = documentCoverage?.missingDocuments ?? [];
                  const presentDocumentSummary = presentDocumentLabels.length
                    ? `${presentDocumentLabels.join(", ")} ${presentDocumentLabels.length === 1 ? "is" : "are"} present.`
                    : "No primary documents were surfaced for the quick brief yet.";
                  const missingDocumentSummary = missingDocumentLabels.length
                    ? `${missingDocumentLabels.join(", ")} ${missingDocumentLabels.length === 1 ? "is" : "are"} still missing.`
                    : "No critical primary documents are currently marked missing.";
                  const evidenceQualityLine = evidenceFooterRows.find((row) => row.label === "Evidence quality")?.value
                    ?? "Evidence quality is not yet clearly surfaced.";
                  const monitoringStatusLine = evidenceFooterRows.find((row) => row.label === "Monitoring status")?.value
                    ?? "Monitoring status is not yet clearly surfaced.";
                  const dataCompletenessLine = evidenceFooterRows.find((row) => row.label === "Data completeness")?.value ?? null;
                  const identityChips = [
                    showPosition ? candidate.currentWeight : null,
                    candidate.instrumentQuality,
                    candidate.portfolioFit,
                  ].filter((value): value is string => Boolean(String(value ?? "").trim())).slice(0, 2);
                  const verdictSignals = stateSignals.filter((badge) => badge.label !== candidate.decisionState).slice(0, 2);
                  const sleeveFitLead = compactSentence(candidate.whyNow, candidate.implicationSummary)
                    ?? "Sleeve fit still needs a cleaner investor-facing read.";
                  const sleeveFitSupport = compactSentence(candidate.implicationSummary, activeSleeve.sleeveRoleStatement)
                    ?? activeSleeve.sleeveRoleStatement
                    ?? "Sleeve role is still being cleaned up.";
                  const sleeveFitConsequence = compactSentence(candidate.decisionSummary, null);
                  const implementationNote = compactSentence(
                    candidate.implementationSummary,
                    compactSentence(candidate.fieldIssues[0]?.summary, null),
                  );
                  const sleeveMetricKey = `${activeSleeve.id} ${activeSleeve.name}`.toLowerCase();
                  const extraMetricRow = shortDistributionLabel(candidate.distributionPolicy)
                    ? {
                        label: /bond|bill|cash/i.test(sleeveMetricKey) ? "Dist" : "Dist",
                        value: shortDistributionLabel(candidate.distributionPolicy),
                      }
                    : candidate.currentWeight
                      ? { label: "Weight", value: candidate.currentWeight }
                      : (candidate.aumState ? { label: "AUM state", value: candidate.aumState } : null);
                  const keyMetricRows = [
                    { label: "TER", value: candidate.terBps ?? candidate.expenseRatio ?? "Pending" },
                    { label: "AUM", value: candidate.aumUsd ?? candidate.aumState ?? candidate.aum ?? "Pending" },
                    { label: "Spread", value: candidate.spreadProxyBps ?? "Pending" },
                    ...(extraMetricRow ? [extraMetricRow] : []),
                  ].slice(0, 4);
                  const sourcePrimaryChip = sourceSummary.chips[0] ?? null;
                  const marketPrimaryChip = marketPathSummary.chips[0] ?? null;
                  const riskEvidenceChips = [
                    sourcePrimaryChip,
                    marketPrimaryChip && marketPrimaryChip.label !== sourcePrimaryChip?.label ? marketPrimaryChip : null,
                  ].filter((badge): badge is { label: string; tone?: Tone } => Boolean(badge));
                  const evidenceLead = compactSentence(
                    sourceSummary.line,
                    "Evidence quality is not clearly surfaced yet.",
                  ) ?? "Evidence quality is not clearly surfaced yet.";
                  const marketRiskLead = compactSentence(
                    marketPathSummary.line ?? candidate.marketSupportBasis,
                    "Market setup is not clearly surfaced.",
                  ) ?? "Market setup is not clearly surfaced.";
                  const riskEvidenceMeta = sourceSummary.meta ?? marketPathSummary.meta ?? null;
                  const snapshotBriefNote = !preview
                    ? "Using the current explorer snapshot. Open deep report for the full candidate report."
                    : null;
                  const indexScopeType = String(indexScopeExplainer?.scopeType ?? "").trim().toLowerCase();
                  const indexScopeTitle = indexScopeExplainer?.label ?? (indexScopeType === "equity_index" ? "Index scope" : "Exposure scope");
                  const hasIndexScopeContent = Boolean(
                    indexScopeExplainer?.summary
                    || indexScopeExplainer?.displayTitle
                    || indexScopeExplainer?.coverageStatement
                    || indexScopeExplainer?.includesStatement
                    || indexScopeExplainer?.excludesStatement
                    || indexScopeExplainer?.indexName
                    || indexScopeExplainer?.marketCapScope
                    || indexScopeExplainer?.constituentCount
                    || indexScopeExplainer?.covers?.length
                    || indexScopeExplainer?.doesNotCover?.length
                    || indexScopeExplainer?.emergingMarketsIncluded !== null && indexScopeExplainer?.emergingMarketsIncluded !== undefined,
                  );
                  const hasComposition = Boolean(
                    compositionSummaryRows.length
                    || compositionPack?.countryWeights?.length
                    || compositionPack?.sectorWeights?.length
                    || compositionPack?.topHoldings?.length,
                  );
                  const heroScoreRows = [
                    ...(typeof candidate.scoreBreakdown?.instrumentQuality === "number"
                      ? [{ label: "Instrument quality", score: candidate.scoreBreakdown.instrumentQuality }]
                      : []),
                    ...(typeof candidate.scoreBreakdown?.portfolioFit === "number"
                      ? [{ label: "Portfolio fit", score: candidate.scoreBreakdown.portfolioFit }]
                      : []),
                  ];
                  const decisionReason = laneDecisionReason(candidate, stateSummary);
                  const decisionSubline = laneDecisionSubline(candidate, stateSummary);
                  const thesisLine = laneThesisLine(candidate, activeSleeve.name, decisionReason);
                  const sourceStatusLine = (() => {
                    if (String(candidate.sourceCompletionSummary?.state ?? "").trim().toLowerCase() === "complete") {
                      return "Source complete";
                    }
                    const summary = candidate.sourceIntegritySummary;
                    const sourceText = `${sourcePrimaryChip?.label ?? ""} ${sourceSummary.line ?? ""} ${sourceSummary.meta ?? ""}`.toLowerCase();
                    if (summary && summary.criticalTotal > 0 && summary.criticalReady >= summary.criticalTotal && !/mixed|thin|weak|limited|gap|stale|conflict/.test(sourceText)) {
                      return "Source complete";
                    }
                    if (/mixed|thin|weak|limited|gap|stale|conflict|drag/.test(sourceText)) {
                      return "Source mixed";
                    }
                    return compressLaneCopy(sourcePrimaryChip?.label ?? sourceSummary.line ?? sourceSummary.meta, "Source review needed.") ?? "Source review needed.";
                  })();
                  const timingStatusLine =
                    candidate.marketPath?.timingLabel
                    ?? compressLaneCopy(marketPrimaryChip?.label ?? marketPathSummary.line ?? marketPathSummary.meta, "Timing not assessed.")
                    ?? "Timing not assessed";
                  const contextConsequence = laneContextConsequence(
                    sourcePrimaryChip?.tone,
                    marketPrimaryChip?.tone,
                    timingStatusLine,
                  );
                  return (
                    <div key={candidate.id} className="candidate-row-shell">
                      <article
                        id={`candidate-row-${candidate.id}`}
                        className={`candidate-grid-row${expanded ? " active candidate-grid-row-compact" : ""}`}
                        onClick={() => {
                          setSelectedCandidateId(candidate.id);
                          setExpandedCandidateId(expanded ? null : candidate.id);
                        }}
                      >
                        {expanded ? (
                          <>
                            <div className="candidate-row-compact-main">
                              <div className="row-candidate-title">{candidate.symbol}</div>
                              <div className="row-cell-value">{candidate.name}</div>
                            </div>
                            <div className="candidate-row-compact-verdict">
                              <span className={pillClass(briefStatusTone)}>{briefStatusLabel}</span>
                            </div>
                            <div className="candidate-row-compact-actions">
                              <button
                                className={`action-btn${isCompared ? " active" : ""}`}
                                type="button"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  toggleCompare(candidate.id);
                                }}
                              >
                                {isCompared ? "Comparing" : "Compare"}
                              </button>
                              <button className="action-btn" type="button" onClick={(event) => { event.stopPropagation(); openReport(candidate.id); }}>
                                Deep report
                              </button>
                            </div>
                          </>
                        ) : (
                          <>
                            <div className="candidate-row-summary">
                              <div className="candidate-row-top-grid">
                                <div className="candidate-row-identity">
                                  <div style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
                                    <span
                                      className={`compare-checkbox${isCompared ? " checked" : ""}`}
                                      role="checkbox"
                                      aria-checked={isCompared}
                                      title="Add to compare"
                                      onClick={(event) => {
                                        event.stopPropagation();
                                        toggleCompare(candidate.id);
                                      }}
                                    />
                                    <div className="candidate-row-identity-main">
                                      <div className="row-candidate-title">{candidate.symbol}</div>
                                      <div className="row-cell-value">{candidate.name}</div>
                                      {scoreSummary || candidateScoreBreakdown ? (
                                        <div
                                          className="candidate-inline-score"
                                          title={
                                            candidateScoreBreakdown
                                              ? `Recommendation score ${scoreRead(inlineRecommendationScore)} out of 100.${inlineTruthConfidenceBand ? ` ${truthConfidenceBandLabel(inlineTruthConfidenceBand)}.` : ""}`
                                              : scoreSummaryTitle(scoreSummary)
                                          }
                                          aria-label={
                                            candidateScoreBreakdown
                                              ? `Recommendation score ${scoreRead(inlineRecommendationScore)} out of 100.${inlineTruthConfidenceBand ? ` ${truthConfidenceBandLabel(inlineTruthConfidenceBand)}.` : ""}`
                                              : scoreSummaryTitle(scoreSummary)
                                          }
                                        >
                                          <div className="candidate-inline-score-track">
                                            <div
                                              className={`candidate-inline-score-fill tone-${scoreTone(inlineRecommendationScore)}`}
                                              style={{ width: `${Math.max(6, Math.min(100, inlineRecommendationScore))}%` }}
                                            />
                                          </div>
                                          <div className={`candidate-inline-score-value tone-${scoreTone(inlineRecommendationScore)}`}>
                                            {scoreRead(inlineRecommendationScore)}
                                          </div>
                                        </div>
                                      ) : null}
                                      {inlineTruthConfidenceScore !== null ? (
                                        <div className="candidate-inline-score-note">
                                          Confidence {scoreRead(inlineTruthConfidenceScore)}{inlineTruthConfidenceBand ? ` · ${truthConfidenceBandLabel(inlineTruthConfidenceBand).replace(/\s*confidence$/i, "")}` : ""}
                                        </div>
                                      ) : scoreSummary?.reliabilityState === "weak" && scoreSummary.reliabilityNote ? (
                                        <div className="candidate-inline-score-note">{scoreSummary.reliabilityNote}</div>
                                      ) : null}
                                      <div className="etf-inline-meta">{candidate.exposureSummary ?? candidate.benchmarkFullName ?? "Exposure context pending"}</div>
                                      {identityChips.length ? (
                                        <div className="fact-chip-row candidate-identity-chip-row">
                                          {identityChips.map((chip) => (
                                            <span className="chip" key={`${candidate.id}-identity-${chip}`}>{chip}</span>
                                          ))}
                                        </div>
                                      ) : null}
                                    </div>
                                  </div>
                                </div>

                                <div className="candidate-row-decision-column">
                                  <div className="candidate-row-state-stack">
                                    <span className={pillClass(candidate.decisionTone)}>{candidate.decisionState}</span>
                                    {verdictSignals.slice(0, 1).map((badge) => (
                                      <span
                                        className={`state-chip-secondary${badge.tone && badge.tone !== "neutral" ? ` state-chip-secondary-${badge.tone}` : ""}`}
                                        key={`${candidate.id}-${badge.label}`}
                                      >{badge.label}</span>
                                    ))}
                                  </div>
                                  <div className={`candidate-row-headline tone-${stateSummary.tone}`}>{decisionReason}</div>
                                  {decisionSubline ? (
                                    <div className="candidate-row-subline">{decisionSubline}</div>
                                  ) : null}
                                </div>

                                <div className="candidate-row-metrics-column">
                                  <div className="candidate-implementation-stack">
                                    {keyMetricRows.map((row) => (
                                      <div className="candidate-implementation-row" key={`${candidate.id}-${row.label}`}>
                                        <span>{row.label}</span>
                                        <strong>{row.value}</strong>
                                      </div>
                                    ))}
                                  </div>
                                  {implementationNote ? (
                                    <div className="candidate-row-subline candidate-row-metric-note">
                                      {compressLaneCopy(implementationNote, implementationNote)}
                                    </div>
                                  ) : null}
                                </div>

                                <div className="candidate-row-context-column">
                                  <div className="candidate-context-statuses">
                                    <div className="candidate-context-status">
                                      <strong>Source</strong>
                                      <span>{sourceStatusLine}</span>
                                    </div>
                                    <div className="candidate-context-status">
                                      <strong>Timing</strong>
                                      <span>{timingStatusLine}</span>
                                    </div>
                                  </div>
                                  <div className="candidate-row-subline">{contextConsequence}</div>
                                </div>

                                <div className="candidate-row-actions-column">
                                  <div className="candidate-action-stack candidate-action-stack-inline" onClick={(event) => event.stopPropagation()}>
                                    <button
                                      className="action-btn action-btn-primary"
                                      type="button"
                                      onClick={(event) => {
                                        event.stopPropagation();
                                        setExpandedCandidateId(expanded ? null : candidate.id);
                                        setSelectedCandidateId(candidate.id);
                                      }}
                                    >
                                      Brief
                                    </button>
                                    <button
                                      className={`action-btn${isCompared ? " active" : ""}`}
                                      type="button"
                                      onClick={(event) => {
                                        event.stopPropagation();
                                        toggleCompare(candidate.id);
                                      }}
                                    >
                                      Compare
                                    </button>
                                    <details className="candidate-more-menu" onClick={(event) => event.stopPropagation()}>
                                      <summary className="action-btn action-btn-text">More</summary>
                                      <div className="candidate-more-menu-popover">
                                        <button className="action-btn" type="button" onClick={(event) => { event.stopPropagation(); openReport(candidate.id); }}>
                                          Deep report
                                        </button>
                                        <button className="action-btn action-btn-text" type="button" onClick={(event) => { event.stopPropagation(); setSelectedCandidateId(candidate.id); setView("evidence"); }}>
                                          Evidence
                                        </button>
                                      </div>
                                    </details>
                                  </div>
                                </div>
                              </div>

                              <div className="candidate-row-thesis">
                                {thesisLine}
                              </div>
                            </div>
                          </>
                        )}
                      </article>

                      {expanded ? (
                        <div className="detail-shell blueprint-detail-shell">
                          <div className="blueprint-detail-flow">
                            <section className="exp-section quick-brief-hero">
                              <div className="panel-kicker">Quick Brief</div>
                              <div className="quick-brief-hero-callout">
                                <div className="quick-brief-verdict">{briefStatusLabel}.</div>
                                <div className="quick-brief-summary">
                                  {quickBrief?.summary ?? preview?.currentImplication ?? candidate.implicationSummary}
                                </div>
                                {decisionReasons.length ? (
                                  <div className="quick-brief-reasons">
                                    <div className="brief-line-list">
                                      {decisionReasons.map((reason, index) => (
                                        <div className="brief-line brief-line-interpretive" key={`${candidate.id}-reason-${index}`}>
                                          <div className="brief-line-label">{index === 0 ? "Main reason" : `Reason ${index + 1}`}</div>
                                          <div className="brief-line-value">{reason}</div>
                                        </div>
                                      ))}
                                    </div>
                                  </div>
                                ) : null}
                                {snapshotBriefNote ? (
                                  <div className="quick-source-issue quick-source-issue-subtle" style={{ marginTop: 10 }}>
                                    {snapshotBriefNote}
                                  </div>
                                ) : null}
                              </div>
                              <div className="quick-facts-strip quick-brief-hero-facts">
                                {(heroFactRows.length ? heroFactRows : keyMetricRows).map((row) => (
                                  <div className="quick-fact" key={`${candidate.id}-brief-fact-${row.label}`}>
                                    <div className="quick-fact-label">{row.label}</div>
                                    <div className="quick-fact-value">{row.value}</div>
                                  </div>
                                ))}
                              </div>
                              <div className="quick-timing-ribbon">
                                <div className="quick-timing-ribbon-label">Timing context</div>
                                <div className="quick-timing-ribbon-copy">{timingRibbonLine}</div>
                              </div>
                            </section>

                            <section className="exp-section exp-section-interpretive">
                              <div className="exp-section-title">What you are actually buying</div>
                              <div className="exp-section-lead">
                                {whyThisMattersLine ?? "Separate the index exposure from the fund wrapper before deciding whether the line fits the job."}
                              </div>
                              <div className="quick-two-column">
                                <div className="quick-proof-subblock">
                                  <div className="quick-proof-note">{indexScopeTitle}</div>
                                  {hasIndexScopeContent ? (
                                    <div className="brief-line-list">
                                      {indexScopeExplainer?.displayTitle || indexScopeExplainer?.summary ? (
                                        <div className="brief-line brief-line-reference">
                                          <div className="brief-line-label">Scope</div>
                                          <div className="brief-line-value">
                                            {indexScopeExplainer.displayTitle ?? indexScopeExplainer.summary}
                                          </div>
                                        </div>
                                      ) : null}
                                      {!indexScopeExplainer?.summary && indexScopeExplainer?.coverageStatement ? (
                                        <div className="brief-line brief-line-reference">
                                          <div className="brief-line-label">Coverage</div>
                                          <div className="brief-line-value">{indexScopeExplainer.coverageStatement}</div>
                                        </div>
                                      ) : null}
                                      {indexScopeExplainer?.covers?.length ? (
                                        <div className="brief-line brief-line-reference">
                                          <div className="brief-line-label">Covers</div>
                                          <div className="brief-line-value">{indexScopeExplainer.covers.join("; ")}</div>
                                        </div>
                                      ) : null}
                                      {!indexScopeExplainer?.covers?.length && indexScopeExplainer?.includesStatement ? (
                                        <div className="brief-line brief-line-reference">
                                          <div className="brief-line-label">Includes</div>
                                          <div className="brief-line-value">{indexScopeExplainer.includesStatement}</div>
                                        </div>
                                      ) : null}
                                      {indexScopeExplainer?.doesNotCover?.length ? (
                                        <div className="brief-line brief-line-reference">
                                          <div className="brief-line-label">Does not cover</div>
                                          <div className="brief-line-value">{indexScopeExplainer.doesNotCover.join("; ")}</div>
                                        </div>
                                      ) : null}
                                      {!indexScopeExplainer?.doesNotCover?.length && indexScopeExplainer?.excludesStatement ? (
                                        <div className="brief-line brief-line-reference">
                                          <div className="brief-line-label">Excludes</div>
                                          <div className="brief-line-value">{indexScopeExplainer.excludesStatement}</div>
                                        </div>
                                      ) : null}
                                      {indexScopeExplainer?.indexName ? (
                                        <div className="brief-line brief-line-reference">
                                          <div className="brief-line-label">Index</div>
                                          <div className="brief-line-value">{indexScopeExplainer.indexName}</div>
                                        </div>
                                      ) : null}
                                      {indexScopeExplainer?.marketCapScope ? (
                                        <div className="brief-line brief-line-reference">
                                          <div className="brief-line-label">Market cap</div>
                                          <div className="brief-line-value">{indexScopeExplainer.marketCapScope}</div>
                                        </div>
                                      ) : null}
                                      {indexScopeExplainer?.constituentCount ? (
                                        <div className="brief-line brief-line-reference">
                                          <div className="brief-line-label">Constituents</div>
                                          <div className="brief-line-value">{indexScopeExplainer.constituentCount}</div>
                                        </div>
                                      ) : null}
                                    </div>
                                  ) : (
                                    <div className="quick-source-issue quick-source-issue-subtle">
                                      {quickBriefGapNote("index scope", { sourceLine: sourceSummary.line })}
                                    </div>
                                  )}
                                </div>
                                <div className="quick-proof-subblock">
                                  <div className="quick-proof-note">Fund wrapper summary</div>
                                  {buyingWrapperRows.length ? (
                                    <div className="brief-line-list">
                                      {buyingWrapperRows.map((row) => (
                                        <div className="brief-line brief-line-reference" key={`${candidate.id}-buying-${row.label}`}>
                                          <div className="brief-line-label">{row.label}</div>
                                          <div className="brief-line-value">{row.value}</div>
                                        </div>
                                      ))}
                                    </div>
                                  ) : (
                                    <div className="quick-source-issue quick-source-issue-subtle">
                                      {quickBriefGapNote("fund wrapper summary", { sourceLine: sourceSummary.line })}
                                    </div>
                                  )}
                                </div>
                              </div>
                            </section>

                            <section className="exp-section exp-section-medium">
                              <div className="exp-section-title">Fund facts</div>
                              <div className="quick-two-column quick-two-column-facts">
                                <div className="quick-proof-subblock">
                                  {fundFactRows.length ? (
                                    <div className="brief-line-list">
                                      {fundFactRows.map((row) => (
                                        <div className="brief-line brief-line-reference" key={`${candidate.id}-fund-${row.label}`}>
                                          <div className="brief-line-label">{row.label}</div>
                                          <div className="brief-line-value">{row.value}</div>
                                        </div>
                                      ))}
                                    </div>
                                  ) : (
                                    <div className="quick-source-issue quick-source-issue-subtle">
                                      {quickBriefGapNote("fund facts", { sourceLine: sourceSummary.line })}
                                    </div>
                                  )}
                                </div>
                                <div className="quick-proof-subblock">
                                  <div className="quick-proof-note">Listing facts</div>
                                  {listingFactRows.length ? (
                                    <div className="brief-line-list">
                                      {listingFactRows.map((row) => (
                                        <div className="brief-line brief-line-reference" key={`${candidate.id}-listing-${row.label}`}>
                                          <div className="brief-line-label">{row.label}</div>
                                          <div className="brief-line-value">{row.value}</div>
                                        </div>
                                      ))}
                                    </div>
                                  ) : (
                                    <div className="quick-source-issue quick-source-issue-subtle">
                                      {quickBriefGapNote("listing facts", { sourceLine: marketPathSummary.line ?? sourceSummary.line })}
                                    </div>
                                  )}
                                </div>
                              </div>
                            </section>

                            <section className="exp-section exp-section-analytic">
                              <div className="exp-section-title">Cost and execution</div>
                              <div className="exp-section-lead">
                                {compareFirstLine}
                              </div>
                              <div className="brief-line-list quick-check-list">
                                {performanceProofRows.map((row) => (
                                  <div
                                    className={`brief-line brief-line-analytic${row.trail ? " brief-line-has-trail" : ""}`}
                                    key={`${candidate.id}-proof-${row.label}`}
                                  >
                                    <div className="brief-line-label">{row.label}</div>
                                    <div className="brief-line-value">{row.value}</div>
                                    {row.trail ? <div className="brief-line-trail">{row.trail}</div> : null}
                                  </div>
                                ))}
                              </div>
                            </section>

                            <section className="exp-section exp-section-interpretive">
                              <div className="exp-section-title">Portfolio fit</div>
                              <div className="exp-section-lead">{whatItSolvesLine}</div>
                              <div className="brief-line-list">
                                <div className="brief-line brief-line-portfolio">
                                  <div className="brief-line-label">Portfolio role</div>
                                  <div className="brief-line-value">{roleInPortfolioLine}</div>
                                </div>
                                <div className="brief-line brief-line-portfolio">
                                  <div className="brief-line-label">Current need</div>
                                  <div className="brief-line-value">{currentNeedLine}</div>
                                </div>
                                <div className="brief-line brief-line-portfolio">
                                  <div className="brief-line-label">What it still needs to prove</div>
                                  <div className="brief-line-value">{whatItStillNeedsToProveLine}</div>
                                </div>
                                <div className="brief-line brief-line-portfolio">
                                  <div className="brief-line-label">What it does not solve</div>
                                  <div className="brief-line-value">{whatItDoesNotSolveLine}</div>
                                </div>
                                <div className="brief-line brief-line-portfolio">
                                  <div className="brief-line-label">Decision readiness</div>
                                  <div className="brief-line-value">{decisionReadinessLine}</div>
                                </div>
                              </div>
                            </section>

                            {(peerSummaryRows.length || peerComparePack?.rows?.length) ? (
                              <>
                                <section className="exp-section compare-conclusion-section">
                                  <div className="compare-conclusion-lead">{compareConclusionLead}</div>
                                  <div className="compare-conclusion-copy">{compareConclusionSupport}</div>
                                  {broaderAlternativeLine || showKronosCompareCheck ? (
                                    <div className="compare-context-check">
                                      <div className="compare-context-check-label">What changes the read</div>
                                      <div className="compare-context-check-copy">
                                        {[broaderAlternativeLine, showKronosCompareCheck ? kronosCompareCheck?.regimeCheckText ?? null : null].filter(Boolean).join(" ")}
                                      </div>
                                    </div>
                                  ) : null}
                                </section>
                                {peerSummaryRows.length ? (
                                  <section className="exp-section exp-section-compare-cards">
                                    <div className="exp-section-title">Peer compare</div>
                                    <div className="peer-summary-grid">
                                      {peerSummaryRows.map((row) => (
                                        <div className="peer-summary-card" key={`${candidate.id}-peer-${row.fundName}-${row.role}`}>
                                          <div className="peer-summary-name">{row.fundName}</div>
                                          <div className="peer-summary-tag">{peerRoleTag(row)}</div>
                                          <div className="peer-summary-copy">
                                            {row.whyThisPeerMatters ?? "Use this peer to pressure-test the current preference."}
                                          </div>
                                          <div className="peer-summary-deltas">
                                            {peerDeltaPills(row, subjectPeerRow).map((pill) => (
                                              <span className={pillClass(pill.tone)} key={`${candidate.id}-peer-pill-${row.fundName}-${pill.label}`}>{pill.label}</span>
                                            ))}
                                          </div>
                                        </div>
                                      ))}
                                    </div>
                                  </section>
                                ) : null}
                                {peerComparePack?.rows?.length ? (
                                  <section className="exp-section exp-section-compare">
                                    <details className="quick-peer-details">
                                      <summary>Detailed peer comparison</summary>
                                      <div className="quick-peer-details-body">
                                        {peerComparePack.primaryQuestion ? (
                                          <div className="quick-proof-note">{peerComparePack.primaryQuestion}</div>
                                        ) : null}
                                        <div className="quick-peer-table-wrap">
                                          <table className="quick-peer-table">
                                            <thead>
                                              <tr>
                                                <th>Fund</th>
                                                <th>Role</th>
                                                <th>Benchmark</th>
                                                <th>TER</th>
                                                <th>Assets</th>
                                                <th>Distribution</th>
                                                <th>Domicile</th>
                                                <th>Why this peer matters</th>
                                              </tr>
                                            </thead>
                                            <tbody>
                                              {peerComparePack.rows.map((row) => (
                                                <tr key={`${candidate.id}-peer-row-${row.fundName}-${row.role}`}>
                                                  <td>{row.fundName}</td>
                                                  <td>{peerRoleTag(row)}</td>
                                                  <td>{row.benchmarkFamily ?? row.benchmark ?? "—"}</td>
                                                  <td>{row.ter ?? "—"}</td>
                                                  <td>{row.fundAssets ?? row.shareClassAssets ?? "—"}</td>
                                                  <td>{row.distribution ?? "—"}</td>
                                                  <td>{row.domicile ?? "—"}</td>
                                                  <td>{row.whyThisPeerMatters ?? "—"}</td>
                                                </tr>
                                              ))}
                                            </tbody>
                                          </table>
                                        </div>
                                      </div>
                                    </details>
                                  </section>
                                ) : null}
                              </>
                            ) : null}

                            <section className="exp-section exp-section-quiet">
                              <div className="exp-section-title">Evidence and documents</div>
                              <div className="exp-section-lead">{evidenceLead}</div>
                              <div className="quick-two-column">
                                <div className="quick-proof-subblock">
                                  <div className="quick-proof-note">Source integrity and timing</div>
                                  <div className="brief-line-list">
                                    <div className="brief-line">
                                      <div className="brief-line-label">Evidence quality</div>
                                      <div className="brief-line-value">{evidenceQualityLine}</div>
                                    </div>
                                    {dataCompletenessLine ? (
                                      <div className="brief-line">
                                        <div className="brief-line-label">Data completeness</div>
                                        <div className="brief-line-value">{dataCompletenessLine}</div>
                                      </div>
                                    ) : null}
                                    <div className="brief-line">
                                      <div className="brief-line-label">Timing context</div>
                                      <div className="brief-line-value">{marketRiskLead}</div>
                                    </div>
                                    <div className="brief-line">
                                      <div className="brief-line-label">Monitoring status</div>
                                      <div className="brief-line-value">{monitoringStatusLine}</div>
                                    </div>
                                  </div>
                                  <div className="fact-chip-row" style={{ marginTop: 12 }}>
                                    {riskEvidenceChips.map((chip) => (
                                      <span className={pillClass(chip.tone)} key={`${candidate.id}-risk-chip-${chip.label}`}>{chip.label}</span>
                                    ))}
                                  </div>
                                </div>
                                <div className="quick-proof-subblock">
                                  <div className="quick-proof-note">Document coverage</div>
                                  <div className="brief-line-list">
                                    <div className="brief-line">
                                      <div className="brief-line-label">Present docs</div>
                                      <div className="brief-line-value">{presentDocumentSummary}</div>
                                    </div>
                                    <div className="brief-line">
                                      <div className="brief-line-label">Missing docs</div>
                                      <div className="brief-line-value">{missingDocumentSummary}</div>
                                    </div>
                                    {documentCoverageRows.map((row) => (
                                      <div className="brief-line brief-line-reference" key={`${candidate.id}-doc-${row.label}`}>
                                        <div className="brief-line-label">{row.label}</div>
                                        <div className="brief-line-value">{row.value}</div>
                                      </div>
                                    ))}
                                    {proofDocuments.slice(0, 4).map((row) => (
                                      <div className="brief-line brief-line-reference" key={`${candidate.id}-proof-doc-${row.label}`}>
                                        <div className="brief-line-label">Primary doc</div>
                                        <div className="brief-line-value">{row.label}</div>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              </div>
                            </section>
                            <section className="exp-section deep-report-entry">
                              <button className="action-btn" type="button" onClick={(event) => { event.stopPropagation(); openReport(candidate.id); }}>
                                Open deep report
                              </button>
                            </section>
                          </div>
                        </div>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            </section>

          </section>
        ) : (
          <div className="changes-empty">No sleeves were emitted by the Blueprint route.</div>
        )}

        {changesWorkspaceSection}
      </div>
    );
  }

  function renderNotebookSurface(display: NotebookDisplay) {
    const seededEntries =
      selectedCandidateId && selectedNotebookContract
        ? seedSimpleNotebookEntries(selectedCandidateId, selectedNotebookContract)
        : [];
    const entries =
      selectedCandidateId && Object.prototype.hasOwnProperty.call(simpleNotebookEntriesByCandidate, selectedCandidateId)
        ? simpleNotebookEntriesByCandidate[selectedCandidateId]
        : seededEntries;
    const drafts = entries.filter((entry) => entry.status === "draft");
    const recent = entries.filter((entry) => entry.status === "finalized");
    const archived = entries.filter((entry) => entry.status === "archived");

    const renderAiPanel = (entryId: string) => {
      const panels = Object.entries(notebookAssistResponses).filter(([key]) => key.startsWith(`${entryId}_`));
      if (!panels.length) return null;
      return panels.map(([key, response]) => (
        <div className="nb-ai-response" key={key}>
          <div className="nb-ai-response-header">
            <span className="nb-ai-response-label">{response.label}</span>
            <button className="nb-ai-dismiss" type="button" onClick={() => dismissNotebookAssistant(key)}>
              dismiss
            </button>
          </div>
          {response.loading ? <div className="nb-ai-loading">Thinking...</div> : <div>{response.text}</div>}
        </div>
      ));
    };

    const renderAskAiStrip = (entry: SimpleNotebookEntry) => (
      <div className="nb-ask-ai-strip">
        <button className="filter-chip" type="button" onClick={() => askNotebookAssistant(entry, "challenge")}>Challenge this thesis</button>
        <button className="filter-chip" type="button" onClick={() => askNotebookAssistant(entry, "assumptions")}>Check weak assumptions</button>
        <button className="filter-chip" type="button" onClick={() => askNotebookAssistant(entry, "change")}>What would change this?</button>
        <button className="filter-chip" type="button" onClick={() => askNotebookAssistant(entry, "brief")}>Monitor list</button>
      </div>
    );

    const renderDraftEntry = (entry: SimpleNotebookEntry) => {
      const fields: Array<{
        field: "thesis" | "assumptions" | "invalidation" | "watchItems" | "reflections";
        label: string;
        placeholder: string;
      }> = [
        { field: "thesis", label: "Thesis", placeholder: "What is the position and why does it make sense?" },
        { field: "assumptions", label: "Assumptions in play", placeholder: "What must be true for this to hold?" },
        { field: "invalidation", label: "Invalidation conditions", placeholder: "What would force a change?" },
        { field: "watchItems", label: "What to watch", placeholder: "Signals and data to monitor." },
        { field: "reflections", label: "Reflections", placeholder: "Notes after review or events." },
      ];

      return (
        <div className="nb-entry draft" key={entry.id}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: "1rem" }}>
            <div className="nb-entry-date">{entry.date}</div>
            <span className="nb-entry-status draft">Draft</span>
          </div>
          <input
            className="nb-title-input"
            placeholder="Note title..."
            value={entry.title}
            onChange={(event) => updateNotebookEntry(entry.id, { title: event.currentTarget.value })}
          />
          <div className="nb-meta-row">
            <input
              className="nb-meta-input"
              placeholder="Ticker / asset..."
              value={entry.linked}
              onChange={(event) => updateNotebookEntry(entry.id, { linked: event.currentTarget.value })}
            />
            <input
              className="nb-meta-input"
              placeholder="Next review date..."
              value={entry.nextReview}
              onChange={(event) => updateNotebookEntry(entry.id, { nextReview: event.currentTarget.value })}
            />
          </div>
          {fields.map(({ field, label, placeholder }) => (
            <div className="nb-field" key={field}>
              <div className="nb-field-label">{label}</div>
              <textarea
                className="nb-field-input"
                placeholder={placeholder}
                value={entry[field]}
                onChange={(event) => updateNotebookEntry(entry.id, { [field]: event.currentTarget.value })}
              />
            </div>
          ))}
          <div className="nb-entry-actions">
            <div className="nb-entry-actions-row">
              <button className="filter-chip active" type="button" onClick={() => saveNotebookDraft(entry.id)}>Save draft</button>
              <span style={{ display: entry.dirty ? "inline" : "none", fontSize: 10, color: "var(--text-faint)", marginLeft: 8 }}>
                Unsaved changes
              </span>
              <button className="filter-chip finalize" type="button" onClick={() => finalizeNotebookEntry(entry.id)}>Finalize note</button>
              <button className="filter-chip danger" type="button" onClick={() => deleteNotebookEntry(entry.id)}>Delete</button>
            </div>
            {renderAskAiStrip(entry)}
          </div>
          {renderAiPanel(entry.id)}
        </div>
      );
    };

    const renderReadEntry = (entry: SimpleNotebookEntry) => {
      const fields = [
        ["Thesis", entry.thesis],
        ["Assumptions in play", entry.assumptions],
        ["Invalidation conditions", entry.invalidation],
        ["What to watch", entry.watchItems],
        ["Reflections", entry.reflections],
      ].filter(([, body]) => body);

      return (
        <div className={`nb-entry ${entry.status}`} key={entry.id}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: "1rem" }}>
            <div className="nb-entry-date">{entry.date}{entry.linked ? ` · ${entry.linked}` : ""}</div>
            <span className={`nb-entry-status ${entry.status}`}>{entry.status === "finalized" ? "Finalized" : "Archived"}</span>
          </div>
          <div className="nb-entry-title">{entry.title || "(Untitled)"}</div>
          {fields.map(([label, body]) => (
            <div className="nb-field" key={label}>
              <div className="nb-field-label">{label}</div>
              <div className="nb-field-body">{body}</div>
            </div>
          ))}
          {entry.nextReview ? (
            <div className="nb-field">
              <div className="nb-field-label">Next review</div>
              <div className="nb-field-body">{entry.nextReview}</div>
            </div>
          ) : null}
          <div className="nb-entry-actions">
            <div className="nb-entry-actions-row">
              {entry.status === "finalized" ? (
                <>
                  <button className="filter-chip" type="button" onClick={() => reopenNotebookEntry(entry.id)}>Edit</button>
                  <button className="filter-chip" type="button" onClick={() => archiveNotebookEntry(entry.id)}>Archive</button>
                  <button className="filter-chip danger" type="button" onClick={() => deleteNotebookEntry(entry.id)}>Delete</button>
                </>
              ) : (
                <>
                  <button className="filter-chip" type="button" onClick={() => reopenNotebookEntry(entry.id)}>Reopen</button>
                  <button className="filter-chip danger" type="button" onClick={() => deleteNotebookEntry(entry.id)}>Delete</button>
                </>
              )}
            </div>
            {renderAskAiStrip(entry)}
          </div>
          {renderAiPanel(entry.id)}
        </div>
      );
    };

    return (
      <div className="layout-stack">
        {display.degradedMessage ? <div className="surface-warning">{display.degradedMessage}</div> : null}
        {selectedCandidateId && notebookCache[selectedCandidateId]?.error ? (
          <div className="surface-error">{notebookCache[selectedCandidateId]?.error}</div>
        ) : null}
        {display.memoryFoundationNote ? <div className="surface-note">{display.memoryFoundationNote}</div> : null}

        <section className="panel">
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
            <div>
              <div className="panel-kicker">Today · {formatNotebookDate()}</div>
              <div className="panel-title">Active note</div>
            </div>
            <button className="filter-chip active" type="button" onClick={newNotebookEntry}>+ New note</button>
          </div>
          <div style={{ marginTop: "1rem" }}>
            {drafts.length ? (
              drafts.map((entry) => renderDraftEntry(entry))
            ) : (
              <div style={{ padding: 24, textAlign: "center", color: "var(--text-faint)", fontSize: 13, border: "1px dashed var(--line)", borderRadius: "var(--radius-xl)" }}>
                No active draft. Press <strong>+ New note</strong> to start writing.
              </div>
            )}
          </div>
        </section>

        <section className="panel">
          <div className="panel-kicker">Log</div>
          <div className="panel-title">Finalized notes</div>
          <div style={{ marginTop: "1rem" }}>
            {recent.length ? (
              recent.map((entry) => renderReadEntry(entry))
            ) : (
              <div className="changes-empty">No finalized notes yet.</div>
            )}
          </div>
        </section>

        {archived.length ? (
          <section className="panel">
            <div className="panel-kicker">Archive</div>
            <div className="panel-title">Older entries</div>
            <div style={{ marginTop: "1rem" }}>
              {archived.map((entry) => renderReadEntry(entry))}
            </div>
          </section>
        ) : null}
      </div>
    );
  }

  function renderEvidenceSurface(display: EvidenceDisplay) {
    return (
      <div className="layout-stack">
        {display.degradedMessage ? <div className="surface-warning">{display.degradedMessage}</div> : null}
        {selectedCandidateId && evidenceCache[selectedCandidateId]?.error ? (
          <div className="surface-error">{evidenceCache[selectedCandidateId]?.error}</div>
        ) : null}

        <section className="panel">
          <div className="panel-kicker">Evidence summary</div>
          <div className="panel-title">What supports the book</div>
          <div className="ev-summary-strip" style={{ marginTop: "1rem" }}>
            {display.summaryTiles.map((tile) => (
              <div className="ev-summary-tile" key={tile.label}>
                <div className={`ev-summary-count tone-${tile.tone ?? "neutral"}`}>{tile.value}</div>
                <div className="ev-summary-label">{tile.label}</div>
              </div>
            ))}
          </div>
        </section>

        <section className="panel">
          <div className="panel-kicker">Research support</div>
          <div className="panel-title">Retrieval and drift support</div>
          <div style={{ marginTop: "1rem" }}>
            {renderResearchSupportBody(display.researchSupport)}
          </div>
        </section>

        <section className="panel">
          <div className="panel-kicker">Evidence by object</div>
          <div className="panel-title">What supports each holding and candidate</div>
          {display.objectGroups.map((group) => (
            <div className="ev-object-section" key={group.title}>
              <div className="ev-object-heading">{group.title}</div>
              <div className="grid cols-2">
                {group.items.map((item) => (
                  <div className="queue-card" key={item.name}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                      <div className="panel-kicker">{item.name}</div>
                      {item.gap ? <span className="chip chip-red">Evidence gap</span> : null}
                    </div>
                    <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginTop: "0.5rem" }}>
                      <span className="chip">{item.direct} direct</span>
                      <span className="chip">{item.proxy} proxy</span>
                      <span className="chip">{item.stale} stale</span>
                    </div>
                    <div style={{ marginTop: 10 }}>
                      {item.claims.map((claim, index) => (
                        <div className="ev-claim-row" key={`${item.name}-${index}`}>
                          <div className="ev-claim-text">{claim.text}</div>
                          <div className="ev-claim-meta">{claim.meta}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </section>

        <section className="panel">
          <div className="panel-kicker">Source documents</div>
          <div className="panel-title">Factsheets, statements, and research files</div>
          <div style={{ display: "grid", gap: 12, marginTop: "1rem" }}>
            {display.documents.length ? (
              display.documents.map((document) => (
                <div className="queue-card" key={document.title}>
                  <div className="panel-kicker">{document.type} · {document.linked}</div>
                  <div className="panel-title" style={{ fontSize: 15, marginTop: 6 }}>{document.title}</div>
                  <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginTop: "0.5rem" }}>
                    <span className="chip">{document.age}</span>
                    {document.stale ? <span className="chip chip-amber">Stale</span> : null}
                  </div>
                </div>
              ))
            ) : (
              <div className="changes-empty">No source documents are attached to this workspace yet.</div>
            )}
          </div>
        </section>

        <section className="panel">
          <div className="panel-kicker">Benchmark and comparison mappings</div>
          <div className="panel-title">ETF to benchmark, sleeve to baseline</div>
          <table className="holdings-explorer" style={{ marginTop: "1rem" }}>
            <thead>
              <tr>
                <th>Sleeve</th>
                <th>ETF</th>
                <th>Benchmark</th>
                <th>Baseline</th>
                <th>Evidence type</th>
              </tr>
            </thead>
            <tbody>
              {display.mappings.map((mapping, index) => (
                <tr key={`${mapping.instrument}-${index}`}>
                  <td>{mapping.sleeve}</td>
                  <td>{mapping.instrument}</td>
                  <td>{mapping.benchmark}</td>
                  <td>{mapping.baseline}</td>
                  <td>{mapping.directness}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        <section className="panel">
          <div className="panel-kicker">Evidence gaps</div>
          <div className="panel-title">Missing, weak, or stale support items</div>
          <div style={{ marginTop: "1rem" }}>
            {display.gaps.length ? (
              display.gaps.map((gap) => (
                <div className="ev-gap-item" key={`${gap.object}-${gap.issue}`}>
                  <strong>{gap.object}</strong> — {gap.issue}
                </div>
              ))
            ) : (
              <div className="changes-empty">No explicit gaps were emitted. Keep the gap section visible anyway.</div>
            )}
          </div>
        </section>
      </div>
    );
  }

  function renderReportDrawer(display: CandidateReportDisplay | null) {
    if (!reportCandidateId) return null;
    const awaitingBinding = !currentReportBinding?.sourceSnapshotId && (blueprint.loading || !blueprint.data);
    const loading = Boolean(reportStatus?.loading && !display && !awaitingBinding);
    const pendingWithoutContent =
      !display
      && (awaitingBinding || reportStatus?.state === "pending" || reportStatus?.state === "unavailable" || reportStatus?.state === "idle");
    const error = reportStatus?.error;
    const activeTab = REPORT_TABS.find((tab) => tab.id === reportTab)?.id ?? "investment_case";
    const candidate = reportBlueprintCandidate;
    const sleeve = reportBlueprintSleeve;
    const compareDisplay =
      blueprintDisplay?.compare && blueprintDisplay.compare.candidates.some((row) => row.id === reportCandidateId)
        ? blueprintDisplay.compare
        : null;
    const reportMarketPath = display?.marketPath ?? candidate?.marketPath ?? null;
    const reportMarketPathSupport = display?.marketPathSupport ?? candidate?.marketPathSupport ?? null;
    const reportSourceSummary = candidate ? candidateSourceSummary(candidate) : null;
    const reportMarketSummary = candidate ? candidateMarketPathSummary(candidate) : null;
    const reportRefreshNote =
      reportStatus?.loading && display
        ? "Refreshing the bound report now. The current drawer is showing cached content while the full payload reloads."
        : null;
    const staleReportNote =
      display && reportStatus?.state === "stale_cached"
        ? "Showing cached report content while the source-bound report is prepared for the selected Explorer snapshot."
        : null;
    const stableSummary = display
      ? [
          {
            label: "Decision posture",
            value: candidate?.decisionState ?? "Under review",
            meta: compactSentence(candidate?.decisionSummary ?? display.rationale),
            tone: candidate?.decisionTone ?? "neutral",
          },
          {
            label: "Why now",
            value: compactSentence(candidate?.implicationSummary ?? display.currentImplication, "Under review") ?? "Under review",
            meta: sleeve?.sleeveRoleStatement ?? null,
            tone: "info",
          },
          {
            label: "Implementation read",
            value: candidate?.implementationSummary ?? candidate?.terBps ?? "Mixed",
            meta: [candidate?.terBps ? `TER ${candidate.terBps}` : null, candidate?.spreadProxyBps ? `Spread ${candidate.spreadProxyBps}` : null]
              .filter(Boolean)
              .join(" · ") || null,
            tone: candidate?.blockerCategory === "Implementation" ? "warn" : "neutral",
          },
          {
            label: "Source confidence",
            value: reportSourceSummary?.chips[0]?.label ?? candidate?.dataQualitySummary?.confidence ?? candidate?.sourceIntegritySummary?.state ?? "Mixed",
            meta: reportSourceSummary ? compactParts([reportSourceSummary.meta, compactSentence(reportSourceSummary.line)]) : compactSentence(candidate?.scoreBreakdown?.summary),
            tone: reportSourceSummary?.chips[0]?.tone ?? candidate?.dataQualitySummary?.confidenceTone ?? candidate?.sourceIntegritySummary?.stateTone ?? "neutral",
          },
          ...(reportMarketSummary
            ? [
                {
                  label: "Market path",
                  value: reportMarketSummary.chips[0]?.label ?? reportMarketPath?.stateLabel ?? "Not surfaced",
                  meta: compactParts([reportMarketSummary.meta, compactSentence(reportMarketPath?.objectiveNote ?? reportMarketSummary.line)]),
                  tone: reportMarketSummary.chips[0]?.tone ?? reportMarketPath?.stateTone ?? "neutral",
                },
              ]
            : []),
          {
            label: "Main restriction",
            value: candidate?.blockerCategory ? presentBlueprintBlocker(candidate.blockerCategory) : "No hard blocker",
            meta: compactSentence(candidate?.whatBlocksAction ?? display.actionBoundary),
            tone: candidate?.blockerCategory ? "warn" : "good",
          },
        ]
      : [];
    const implementationRows = display?.implementationProfile ?? [];
    const evidenceSupport = display?.evidenceSources ?? [];
    const taxAndStructureRows = implementationRows.filter((row) => /tax|distribution|domicile|currency|replication/i.test(row.label));
    const primaryDocs = display?.primaryDocuments ?? [];
    const authorityFields = display?.sourceAuthorityFields ?? [];
    const missingSupportItems = [
      ...(candidate?.sourceIntegritySummary?.missingCriticalFields ?? []).map((field) => `${field} still needs stronger support`),
      ...(candidate?.sourceIntegritySummary?.weakestFields ?? []).map((field) => `${field} remains softer than the core decision fields`),
      ...(display?.fieldIssues ?? []).slice(0, 6).map((issue) => `${issue.label}: ${issue.summary}`),
    ];
    const mandateRiskRows = [
      display?.mandateBoundary ?? null,
      ...(display?.tradeoffs ?? []).slice(0, 3),
    ].filter((value): value is string => Boolean(value));
    const implementationRiskRows = [
      ...(implementationRows.slice(0, 4).map((row) => `${row.label}: ${row.value}${row.caution ? ` · ${row.caution}` : ""}`)),
    ];
    const authorityRiskRows = [
      candidate?.sourceIntegritySummary?.summary ?? null,
      ...(display?.fieldIssues ?? []).slice(0, 4).map((issue) => `${issue.label}: ${issue.fixability}`),
    ].filter((value): value is string => Boolean(value));
    const portfolioMisuseRows = [
      candidate?.implicationSummary ?? null,
      display?.actionBoundary ?? null,
      display?.whatChangesView ?? null,
    ].filter((value): value is string => Boolean(value));
    const competitionRows = display
      ? (display.competitionBlocks.length
          ? display.competitionBlocks.map((block) => ({
              label: block.label,
              summary: block.verdict ?? block.summary,
            }))
          : display.baselineComparisons.map((row) => ({
              label: row.label,
              summary: row.verdict ?? row.summary,
            })))
      : [];
    const identityRiskRows = [
      candidate?.identitySummary ?? null,
      ...(candidate?.sourceIntegritySummary?.hardConflictFields ?? []).slice(0, 3).map((field) => `${field} still has identity or authority friction`),
    ].filter((value): value is string => Boolean(value));
    const reportCompareBuckets = groupCompareDimensions(compareDisplay);
    const comparePrimaryBuckets = reportCompareBuckets.filter((bucket) => bucket.id !== "secondary");
    const compareSecondaryDimensions = reportCompareBuckets.find((bucket) => bucket.id === "secondary")?.dimensions ?? [];
    const marketPathPromotion = reportMarketPath?.canPromote
      ? [reportMarketPath.objectiveLabel, reportMarketPath.implication].filter(Boolean).join(". ")
      : null;
    const reportCoverage = reportCandidateId
      ? (coverageAudit.data?.items ?? []).find((item) => item.candidate_id === reportCandidateId) ?? null
      : null;
    return (
      <div className="report-root">
        <div className="report-backdrop" onClick={() => setReportCandidateId(null)} />
        <div className="report-drawer visible" data-testid="report-drawer">
          <div className="report-top">
            <div>
              <div className="panel-kicker">Deep report</div>
              <h2>{candidate ? `${candidate.symbol} · ${candidate.name}` : display?.meta.title ?? "Loading report"}</h2>
              <div className="report-header-meta">
                {sleeve ? <span className="chip">{sleeve.name}</span> : null}
                {candidate?.decisionState ? <span className={pillClass(candidate.decisionTone)}>{candidate.decisionState}</span> : null}
                {candidate?.blockerCategory ? <span className="chip chip-red">{presentBlueprintBlocker(candidate.blockerCategory)}</span> : null}
              </div>
              <p>{display?.currentImplication ?? display?.rationale ?? display?.meta.copy ?? "Loading candidate report..."}</p>
              {marketPathPromotion ? <div className="report-inline-note">{marketPathPromotion}</div> : null}
              {display?.overlayMessage ? <div className="report-inline-note">{presentOverlayMessage(display.overlayMessage)}</div> : null}
            </div>
            <button className="report-close" type="button" onClick={() => setReportCandidateId(null)}>
              Close
            </button>
          </div>
          <div className="report-tabs">
            {REPORT_TABS.map((tab) => (
              <button
                className={`report-tab ${activeTab === tab.id ? "active" : ""}`}
                key={tab.id}
                type="button"
                onClick={() => setReportTab(tab.id)}
              >
                {tab.label}
              </button>
            ))}
          </div>
          <div className="report-body">
            {loading && !display ? renderReportSkeleton(REPORT_TABS.find((tab) => tab.id === activeTab)?.label ?? "report") : null}
            {pendingWithoutContent ? renderReportPendingState(reportStatus, Boolean(blueprint.loading)) : null}
            {reportRefreshNote ? <div className="surface-warning">{reportRefreshNote}</div> : null}
            {staleReportNote ? <div className="surface-warning">{staleReportNote}</div> : null}
            {error && !pendingWithoutContent ? <div className={display ? "surface-warning" : "surface-error"}>{error}</div> : null}
            {display ? (
              <>
                <div className="report-summary-strip-fixed">
                  {stableSummary.map((chip) => (
                    <div className="report-card" key={chip.label}>
                      <div className="panel-kicker">{chip.label}</div>
                      <div className={`panel-title tone-${chip.tone ?? "neutral"}`} style={{ fontSize: 20 }}>{chip.value}</div>
                      {chip.meta ? <div className="panel-copy">{chip.meta}</div> : null}
                    </div>
                  ))}
                </div>

                {activeTab === "investment_case" ? (
                  <section className="report-lens report-lens-case">
                    <div className="report-tab-header">
                      <div className="panel-kicker">Investment Case</div>
                      <div className="panel-title">Why this ETF is being judged seriously</div>
                      <div className="panel-copy">Recommendation first: job, decision boundary, and what changes the case.</div>
                    </div>
                    <div className="report-case-layout">
                      <div className="report-card report-case-main">
                        <div className="panel-kicker">Job in sleeve</div>
                        <div className="panel-title" style={{ fontSize: 24 }}>{candidate?.implicationSummary ?? display.currentImplication}</div>
                        <div className="panel-copy">{display.investmentCase}</div>
                        <div className="fact-list" style={{ marginTop: 14 }}>
                          <div className="fact-line"><strong>Current read</strong><span>{display.currentImplication}</span></div>
                          {candidate?.benchmarkFullName ? <div className="fact-line"><strong>Benchmark</strong><span>{candidate.benchmarkFullName}</span></div> : null}
                          {candidate?.exposureSummary ? <div className="fact-line"><strong>Exposure</strong><span>{candidate.exposureSummary}</span></div> : null}
                        </div>
                      </div>
                      <div className="report-case-side">
                        <div className="report-card">
                          <div className="panel-kicker">Decision boundary</div>
                          <div className="fact-list">
                            <div className="fact-line"><strong>Action boundary</strong><span>{display.actionBoundary ?? "Under review"}</span></div>
                            <div className="fact-line"><strong>What still needs cleanup</strong><span>{candidate?.whatBlocksAction ?? "No investor-facing blocker has been surfaced yet."}</span></div>
                            <div className="fact-line"><strong>What changes the view</strong><span>{display.whatChangesView ?? "No investor-facing change trigger has been surfaced yet."}</span></div>
                            {candidate?.fundingSource ? <div className="fact-line"><strong>Funding path</strong><span>{candidate.fundingSource}</span></div> : null}
                          </div>
                        </div>
                        <div className="report-card">
                          <div className="panel-kicker">Reopen or strengthen if</div>
                          <div className="fact-list">
                            <div className="fact-line"><strong>Upgrade if</strong><span>{display.upgradeCondition ?? "Not available"}</span></div>
                            <div className="fact-line"><strong>Downgrade if</strong><span>{display.downgradeCondition ?? "Not available"}</span></div>
                            <div className="fact-line"><strong>Kill if</strong><span>{display.killCondition ?? "Not available"}</span></div>
                          </div>
                        </div>
                      </div>
                    </div>
                    <div className="report-case-strip">
                      <div className="report-card">
                        <div className="panel-kicker">Why the current leader leads</div>
                        <div className="panel-copy">{display.rationale}</div>
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">What still needs cleanup</div>
                        <div className="panel-copy">{candidate?.whatBlocksAction ?? "No investor-facing blocker has been surfaced yet."}</div>
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Sleeve role</div>
                        <div className="panel-copy">{sleeve?.sleeveRoleStatement ?? sleeve?.purpose ?? "Sleeve context unavailable."}</div>
                      </div>
                    </div>
                    <div className="report-card" style={{ marginTop: 16 }}>
                    <div className="panel-kicker">Score rubric</div>
                      <div className="panel-title" style={{ fontSize: 20 }}>Recommendation score</div>
                      <div className="panel-copy" style={{ marginTop: 8 }}>
                        Keep the headline number honest: recommendation now leads, deployability stays explicit, and the full component family stays auditable underneath it.
                      </div>
                      <div style={{ marginTop: 12 }}>
                        {renderScoreFamily(display.scoreBreakdown, display.scoreComponents, sleeve?.name)}
                      </div>
                    </div>
                  </section>
                ) : null}

                {activeTab === "market_history" ? (
                  <section className="report-lens report-lens-market">
                    <div className="report-tab-header">
                      <div className="panel-kicker">Market &amp; History</div>
                      <div className="panel-title">Benchmark context and market path</div>
                      <div className="panel-copy">Use this tab to separate benchmark context, bounded market structure, and the current threshold read from background noise.</div>
                    </div>
                    <div className="report-market-layout">
                      <div className="report-card report-market-main">
                        <div className="panel-kicker">Benchmark and market read</div>
                        {display.marketHistorySummary ? <div className="panel-copy">{display.marketHistorySummary}</div> : null}
                        <div className="fact-list" style={{ marginTop: 14 }}>
                          {candidate?.benchmarkFullName ? <div className="fact-line"><strong>Benchmark</strong><span>{candidate.benchmarkFullName}</span></div> : null}
                          {candidate?.exposureSummary ? <div className="fact-line"><strong>Exposure</strong><span>{candidate.exposureSummary}</span></div> : null}
                          {candidate?.marketSupportBasis ? <div className="fact-line"><strong>Market support</strong><span>{candidate.marketSupportBasis}</span></div> : null}
                        </div>
                      </div>
                      <div className="report-market-side">
                        <div className="report-card">
                          <div className="panel-kicker">Regime windows</div>
                          {display.marketHistoryWindows.length ? display.marketHistoryWindows.map((window) => (
                            <div className="fact-line" key={`${window.label}-${window.period}`}>
                              <strong>{window.label}</strong>
                              <span>{window.period} · Fund {window.fundReturn} · Benchmark {window.benchmarkReturn} · {window.note}</span>
                            </div>
                          )) : <div className="panel-copy">No regime-window rows were emitted.</div>}
                        </div>
                        <div className="report-card">
                          <div className="panel-kicker">Current market-path read</div>
                          <div className="panel-copy">{reportMarketPath?.objectiveLabel ?? reportMarketPath?.stateLabel ?? "Market-path support is not active yet."}</div>
                          {reportMarketPath?.summaryLine ? <div className="etf-inline-meta" style={{ marginTop: 10 }}>{reportMarketPath.summaryLine}</div> : null}
                          {reportMarketPath?.suppressionLabel ? <div className="etf-inline-meta" style={{ marginTop: 10 }}>{reportMarketPath.suppressionLabel}</div> : null}
                        </div>
                      </div>
                    </div>
                    {reportMarketPathSupport ? (
                      <div className="report-card" style={{ marginTop: 16 }}>
                        <div className="panel-kicker">Observed and projected path</div>
                        <div className="panel-title" style={{ fontSize: 20 }}>Typed market-path support</div>
                        <div className="panel-copy" style={{ marginTop: 8 }}>
                          Render the bounded support object directly. Use legacy chart panels only as secondary context.
                        </div>
                        <MarketPathSupportPanel
                          support={reportMarketPathSupport}
                          showProvenance
                        />
                      </div>
                    ) : null}
                    {display.marketHistoryCharts.length ? (
                      <details className="blueprint-secondary-details" style={{ marginTop: 16 }}>
                        <summary>Additional market context</summary>
                        <div className="ia-chart-stack report-chart-stack report-chart-support" style={{ marginTop: 12 }}>
                          {display.marketHistoryCharts.map((panel) => (
                            <ChartPanel key={panel.id} panel={panel} height={200} />
                          ))}
                        </div>
                      </details>
                    ) : null}
                  </section>
                ) : null}

                {activeTab === "scenarios" ? (
                  <section className="report-lens report-lens-scenarios">
                    <div className="report-tab-header">
                      <div className="panel-kicker">Scenarios</div>
                      <div className="panel-title">What confirms, weakens, or breaks the read</div>
                      <div className="panel-copy">Scenario support stays secondary to decision truth and implementation truth.</div>
                    </div>
                    {reportMarketPathSupport?.scenario_takeaways ? (
                      <div className="report-threshold-grid" style={{ marginBottom: 16 }}>
                        <div className="report-card">
                          <div className="panel-kicker">Mild stress</div>
                          <div className="panel-copy">{reportMarketPathSupport.scenario_takeaways.favorable_case_survives_mild_stress ? "The favorable path survives mild stress." : "Mild stress already weakens the favorable path."}</div>
                        </div>
                        <div className="report-card">
                          <div className="panel-kicker">Favorable path width</div>
                          <div className="panel-copy">{reportMarketPathSupport.scenario_takeaways.favorable_case_is_narrow ? "Support relies on a narrow continuation path." : "Support is not relying on a single narrow continuation path."}</div>
                        </div>
                        <div className="report-card">
                          <div className="panel-kicker">Downside containment</div>
                          <div className="panel-copy">{reportMarketPathSupport.scenario_takeaways.downside_damage_is_contained ? "Downside damage stays contained." : "Downside damage is not well contained."}</div>
                        </div>
                        <div className="report-card">
                          <div className="panel-kicker">Stress support</div>
                          <div className="panel-copy">{reportMarketPathSupport.scenario_takeaways.stress_breaks_candidate_support ? "Stress breaks the bounded support read." : "Stress does not fully break the bounded support read."}</div>
                        </div>
                      </div>
                    ) : null}
                    <div className="report-threshold-grid">
                      <div className="report-card">
                        <div className="panel-kicker">Confirms</div>
                        <div className="panel-copy">{display.upgradeCondition ?? "Not available"}</div>
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Weakens</div>
                        <div className="panel-copy">{display.downgradeCondition ?? "Not available"}</div>
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Breaks</div>
                        <div className="panel-copy">{display.killCondition ?? "Not available"}</div>
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Current scenario read</div>
                        <div className="panel-copy">{reportMarketPath?.stateLabel ?? display.whatChangesView ?? "No explicit scenario note was emitted."}</div>
                        {reportMarketPath?.summaryLine ? <div className="etf-inline-meta" style={{ marginTop: 10 }}>{reportMarketPath.summaryLine}</div> : null}
                      </div>
                    </div>
                    {display.decisionThresholds.length ? (
                      <div className="report-card">
                        <div className="panel-kicker">Thresholds</div>
                        <div className="fact-list">
                          {display.decisionThresholds.map((threshold) => (
                            <div className="fact-line" key={threshold.label}>
                              <strong>{threshold.label}</strong>
                              <span>{threshold.value}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {reportMarketPathSupport?.scenario_summary?.length ? (
                      <div className="report-scenario-grid">
                        {reportMarketPathSupport.scenario_summary.map((scenario) => {
                          const terminalPoint = scenario.path[scenario.path.length - 1];
                          return (
                            <div className="report-card scenario-card" key={scenario.scenario_type}>
                              <div className="panel-kicker">{scenario.label}</div>
                              <div className="panel-copy">{scenario.summary}</div>
                              <div className="panel-copy">
                                <strong>Projected end:</strong> {terminalPoint ? `${terminalPoint.value.toFixed(2)} on ${terminalPoint.timestamp}` : "No terminal path point emitted."}
                              </div>
                              {scenario.usefulness_label ? (
                                <div className="etf-inline-meta" style={{ marginTop: 10 }}>{scenario.usefulness_label}</div>
                              ) : null}
                            </div>
                          );
                        })}
                      </div>
                    ) : display.scenarioBlocks.length ? (
                      <div className="report-scenario-grid">
                        {display.scenarioBlocks.map((block) => (
                          <div className="report-card scenario-card" key={block.label}>
                            <div className="panel-kicker">{block.label}</div>
                            <div className="panel-copy"><strong>Trigger:</strong> {block.trigger}</div>
                            <div className="panel-copy"><strong>Path:</strong> {block.expectedReturn}</div>
                            <div className="panel-copy"><strong>Portfolio effect:</strong> {block.portfolioEffect}</div>
                            {block.shortTerm ? <div className="etf-inline-meta" style={{ marginTop: 10 }}>{block.shortTerm}</div> : null}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="changes-empty">Scenario blocks are not available for this report yet.</div>
                    )}
                    {display.scenarioCharts.length ? (
                      <details className="blueprint-secondary-details" style={{ marginTop: 16 }}>
                        <summary>Additional scenario context</summary>
                        <div className="ia-chart-stack report-chart-stack report-chart-support" style={{ marginTop: 12 }}>
                          {display.scenarioCharts.map((panel) => (
                            <ChartPanel key={panel.id} panel={panel} height={200} />
                          ))}
                        </div>
                      </details>
                    ) : null}
                  </section>
                ) : null}

                {activeTab === "risks" ? (
                  <section className="report-lens report-lens-risks">
                    <div className="report-tab-header">
                      <div className="panel-kicker">Risks</div>
                      <div className="panel-title">Where this recommendation can still fail</div>
                      <div className="panel-copy">Keep failure lanes explicit: mandate, implementation, authority, identity, and misuse.</div>
                    </div>
                    <div className="report-risk-lanes">
                      <div className="report-card">
                        <div className="panel-kicker">Mandate risk</div>
                        {mandateRiskRows.length ? mandateRiskRows.map((row, index) => (
                          <div className="fact-line" key={`${row}-${index}`}>
                            <strong>Risk</strong>
                            <span>{row}</span>
                          </div>
                        )) : <div className="panel-copy">No explicit mandate risk was emitted.</div>}
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Implementation risk</div>
                        {implementationRiskRows.length ? implementationRiskRows.map((row, index) => (
                          <div className="fact-line" key={`${row}-${index}`}>
                            <strong>Risk</strong>
                            <span>{row}</span>
                          </div>
                        )) : <div className="panel-copy">No explicit implementation risk was emitted.</div>}
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Authority risk</div>
                        {authorityRiskRows.length ? authorityRiskRows.map((row, index) => (
                          <div className="fact-line" key={`${row}-${index}`}>
                            <strong>Risk</strong>
                            <span>{row}</span>
                          </div>
                        )) : <div className="panel-copy">No explicit authority risk was emitted.</div>}
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Identity risk</div>
                        {identityRiskRows.length ? identityRiskRows.map((row, index) => (
                          <div className="fact-line" key={`${row}-${index}`}>
                            <strong>Risk</strong>
                            <span>{row}</span>
                          </div>
                        )) : <div className="panel-copy">No explicit identity risk was emitted.</div>}
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Portfolio misuse risk</div>
                        {portfolioMisuseRows.length ? portfolioMisuseRows.map((row, index) => (
                          <div className="fact-line" key={`${row}-${index}`}>
                            <strong>Risk</strong>
                            <span>{row}</span>
                          </div>
                        )) : <div className="panel-copy">No explicit misuse risk was emitted.</div>}
                      </div>
                    </div>
                    {display.riskBlocks.length ? (
                      <div className="report-card">
                        <div className="panel-kicker">Additional risk items</div>
                        {display.riskBlocks.map((risk, index) => (
                          <div className="fact-line" key={`${risk.title}-${index}`}>
                            <strong>{risk.title}</strong>
                            <span>{risk.detail}</span>
                          </div>
                        ))}
                      </div>
                    ) : null}
                  </section>
                ) : null}

                {activeTab === "competition" ? (
                  <section className="report-lens report-lens-competition">
                    <div className="report-tab-header">
                      <div className="panel-kicker">Competition</div>
                      <div className="panel-title">Which alternative is cleaner for the sleeve</div>
                      <div className="panel-copy">Answer the investor question first: are these real substitutes, where is the loser weaker, and what would change the read?</div>
                      <div className="etf-inline-meta" style={{ marginTop: 8 }}>
                        Judge in this order: sleeve job, benchmark fidelity, implementation friction, source integrity, then bounded market-path context.
                      </div>
                    </div>
                    {compareDisplay ? (
                      <div className="report-competition-summary">
                        <div className="report-card">
                          <div className="panel-kicker">Substitution verdict</div>
                          <div className="panel-title" style={{ fontSize: 22 }}>{compareDisplay.substitutionVerdict ?? "Unavailable"}</div>
                          <div className="panel-copy">{compareDisplay.substitutionRationale ?? compareDisplay.readinessNote}</div>
                        </div>
                        <div className="report-card">
                          <div className="panel-kicker">Current leader</div>
                          <div className="panel-title" style={{ fontSize: 22 }}>{compareDisplay.winnerName}</div>
                          <div className="panel-copy">{compareDisplay.whyLeads}</div>
                        </div>
                        <div className="report-card">
                          <div className="panel-kicker">Where the weaker option is weaker</div>
                          {compareGapRows(compareDisplay).length ? (
                            <div className="fact-list" style={{ marginTop: 10 }}>
                              {compareGapRows(compareDisplay).map((row) => (
                                <div className="fact-line" key={`report-gap-${row.label}`}>
                                  <strong>{row.label}</strong>
                                  <span>{row.summary}</span>
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div className="panel-copy">{compareDisplay.whyLeads}</div>
                          )}
                        </div>
                        <div className="report-card">
                          <div className="panel-kicker">What would change the read</div>
                          <div className="panel-copy">{compareDisplay.whatWouldChange}</div>
                        </div>
                      </div>
                    ) : null}
                    <div className="report-competition-layout">
                      <div className="report-card">
                        <div className="panel-kicker">Tradeoffs</div>
                        {competitionRows.length ? competitionRows.map((comparison) => (
                          <div className="fact-line" key={comparison.label}>
                            <strong>{comparison.label}</strong>
                            <span>{comparison.summary}</span>
                          </div>
                        )) : <div className="panel-copy">{display.rationale}</div>}
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Decision buckets</div>
                        {comparePrimaryBuckets.length ? (
                          <div className="compare-bucket-grid report-compare-buckets" style={{ marginTop: 12 }}>
                            {comparePrimaryBuckets.map((bucket) => (
                              <section className="compare-bucket-card" key={bucket.id}>
                                <div className="panel-kicker">{bucket.label}</div>
                                <div className="panel-copy">{bucket.summary}</div>
                                <div className="compare-dimension-list report-compare-dimensions" style={{ marginTop: 12 }}>
                                  {bucket.dimensions.map((dimension) => (
                                    <div className="compare-dimension-row" key={dimension.id}>
                                      <div className="compare-dimension-head">
                                        <strong>{dimension.label}</strong>
                                        <span>{dimension.winnerLabel ? `Leader ${dimension.winnerLabel}` : bucket.label}</span>
                                      </div>
                                      <div className="compare-dimension-values">
                                        {compareDisplay?.candidates.map((row) => {
                                          const value = dimension.values.find((item) => item.candidateId === row.id);
                                          const isWinner = dimension.winnerLabel === row.name;
                                          return (
                                            <div className={`compare-dimension-cell${isWinner ? " winner" : ""}`} key={`${dimension.id}-${row.id}`}>
                                              <div className="compare-dimension-symbol">{row.symbol}</div>
                                              <div>{value?.value ?? "Unavailable"}</div>
                                            </div>
                                          );
                                        })}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </section>
                            ))}
                          </div>
                        ) : (
                          <div className="panel-copy">No discriminating dimensions were emitted.</div>
                        )}
                      </div>
                    </div>
                    {compareSecondaryDimensions.length ? (
                      <details className="blueprint-secondary-details">
                        <summary>Secondary dimensions</summary>
                        <div className="compare-dimension-list report-compare-dimensions" style={{ marginTop: 12 }}>
                          {compareSecondaryDimensions.map((dimension) => (
                            <div className="compare-dimension-row" key={dimension.id}>
                              <div className="compare-dimension-head">
                                <strong>{dimension.label}</strong>
                                <span>{dimension.group ?? "Secondary"}</span>
                              </div>
                              <div className="compare-dimension-values">
                                {compareDisplay?.candidates.map((row) => {
                                  const value = dimension.values.find((item) => item.candidateId === row.id);
                                  return (
                                    <div className="compare-dimension-cell" key={`${dimension.id}-${row.id}`}>
                                      <div className="compare-dimension-symbol">{row.symbol}</div>
                                      <div>{value?.value ?? "Unavailable"}</div>
                                    </div>
                                  );
                                })}
                              </div>
                            </div>
                          ))}
                        </div>
                      </details>
                    ) : null}
                    {display.competitionCharts.length ? (
                      <div className="ia-chart-stack report-chart-stack report-chart-support">
                        {display.competitionCharts.map((panel) => (
                          <ChartPanel key={panel.id} panel={panel} height={200} />
                        ))}
                      </div>
                    ) : null}
                  </section>
                ) : null}

                {activeTab === "evidence" ? (
                  <section className="report-lens report-lens-evidence">
                    <div className="report-tab-header">
                      <div className="panel-kicker">Evidence &amp; Sources</div>
                      <div className="panel-title">Provenance, authority, and missing support</div>
                      <div className="panel-copy">Keep the truth explicit here: provenance, authority, stale or missing fields, and the documents that still matter. This is where caveats belong, not the top decision path.</div>
                    </div>
                    <div className="report-evidence-layout">
                      {reportMarketPath ? (
                        <div className="report-card">
                          <div className="panel-kicker">Market-path provenance</div>
                          <div className="fact-list">
                            <div className="fact-line"><strong>Support state</strong><span>{reportMarketPath.stateLabel}</span></div>
                            {reportMarketPath.provenanceLabel ? <div className="fact-line"><strong>Backing</strong><span>{reportMarketPath.provenanceLabel}</span></div> : null}
                            {reportMarketPath.providerLabel ? <div className="fact-line"><strong>Source</strong><span>{reportMarketPath.providerLabel}</span></div> : null}
                            {reportMarketPath.qualityNote ? <div className="fact-line"><strong>Series state</strong><span>{reportMarketPath.qualityNote}</span></div> : null}
                            {reportMarketPath.generatedLabel ? <div className="fact-line"><strong>Generated</strong><span>{reportMarketPath.generatedLabel}</span></div> : null}
                            {reportMarketPath.suppressionLabel ? <div className="fact-line"><strong>Unavailable because</strong><span>{reportMarketPath.suppressionLabel}</span></div> : null}
                          </div>
                        </div>
                      ) : null}
                      {reportCoverage ? (
                        <div className="report-card">
                          <div className="panel-kicker">Coverage workflow</div>
                          <div className="fact-list">
                            <div className="fact-line"><strong>Coverage verdict</strong><span>{coverageVerdictLabel(reportCoverage.coverage_verdict)}</span></div>
                            <div className="fact-line"><strong>Support route</strong><span>{supportVerdictLabel(reportCoverage.support_verdict)}</span></div>
                            {reportCoverage.provider_symbol ? <div className="fact-line"><strong>Runtime symbol</strong><span>{reportCoverage.provider_symbol}</span></div> : null}
                            {typeof reportCoverage.direct_bars === "number" ? <div className="fact-line"><strong>Direct store depth</strong><span>{reportCoverage.direct_bars} bars</span></div> : null}
                            {typeof reportCoverage.proxy_bars === "number" ? <div className="fact-line"><strong>Proxy store depth</strong><span>{reportCoverage.proxy_bars} bars</span></div> : null}
                          </div>
                          {reportCoverage.fallback_aliases?.length ? (
                            <div className="fact-chip-row" style={{ marginTop: 12 }}>
                              {reportCoverage.fallback_aliases.map((alias) => (
                                <span className="chip chip-amber" key={`${reportCoverage.candidate_id}-${alias}`}>{alias}</span>
                              ))}
                            </div>
                          ) : null}
                        </div>
                      ) : null}
                      <div className="report-card">
                        <div className="panel-kicker">What supports this candidate</div>
                        <div className="panel-copy">{display.evidenceDepth}</div>
                        <div className="fact-list" style={{ marginTop: 14 }}>
                          {evidenceSupport.length ? evidenceSupport.map((source) => (
                            <div className="fact-line" key={`${source.label}-${source.freshness}`}>
                              <strong>{source.label}</strong>
                              <span>{source.directness} · {source.freshness}</span>
                            </div>
                          )) : <div className="panel-copy">No evidence sources were emitted.</div>}
                        </div>
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Benchmark and sleeve mapping</div>
                        <div className="fact-list">
                          {sleeve ? <div className="fact-line"><strong>Sleeve</strong><span>{sleeve.name}</span></div> : null}
                          {candidate?.benchmarkFullName ? <div className="fact-line"><strong>Benchmark</strong><span>{candidate.benchmarkFullName}</span></div> : null}
                          {candidate?.exposureSummary ? <div className="fact-line"><strong>Exposure</strong><span>{candidate.exposureSummary}</span></div> : null}
                          <div className="fact-line"><strong>Portfolio context</strong><span>{presentWeightRead(candidate ?? { currentWeight: null, weightState: null })}</span></div>
                        </div>
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Domicile, tax, and distribution</div>
                        {taxAndStructureRows.length ? (
                          <div className="fact-list">
                            {taxAndStructureRows.map((row) => (
                              <div className="fact-line" key={row.label}>
                                <strong>{row.label}</strong>
                                <span>{row.value}</span>
                              </div>
                            ))}
                          </div>
                        ) : (
                          <div className="panel-copy">No tax or structure rows were emitted.</div>
                        )}
                      </div>
                      <div className="report-card">
                        <div className="panel-kicker">Support caveats</div>
                        {missingSupportItems.length ? (
                          <div className="fact-list">
                            {missingSupportItems.map((item, index) => (
                              <div className="fact-line" key={`${item}-${index}`}>
                                <strong>Watch</strong>
                                <span>{item}</span>
                              </div>
                            ))}
                          </div>
                        ) : <div className="panel-copy">No obvious support caveats were emitted.</div>}
                      </div>
                    </div>

                    <div className="report-card">
                      <div className="panel-kicker">Primary documents</div>
                      {primaryDocs.length ? (
                        <div className="table-shell">
                          <table className="report-data-table">
                            <thead>
                              <tr><th>Doc type</th><th>Status</th><th>Retrieved at</th></tr>
                            </thead>
                            <tbody>
                              {primaryDocs.map((document, index) => (
                                <tr key={`${document.docType}-${index}`}>
                                  <td>{document.docType}</td>
                                  <td>{document.status}</td>
                                  <td>{document.retrievedAt ?? "—"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : <div className="panel-copy">No primary documents were emitted.</div>}
                    </div>

                    <div className="report-card">
                      <div className="panel-kicker">Source authority view</div>
                      {authorityFields.length ? (
                        <div className="table-shell">
                          <table className="report-data-table">
                            <thead>
                              <tr>
                                <th>Field name</th>
                                <th>Source</th>
                                <th>Authority class</th>
                                <th>Freshness</th>
                                <th>Recommendation-critical</th>
                              </tr>
                            </thead>
                            <tbody>
                              {authorityFields.map((field) => (
                                <tr key={`${field.fieldName}-${field.sourceLabel}`}>
                                  <td>{field.label}</td>
                                  <td>{field.sourceLabel}</td>
                                  <td>{field.authorityClass}</td>
                                  <td>{field.freshness}</td>
                                  <td>{field.isRecommendationCritical ? "Yes" : "No"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : <div className="panel-copy">No source authority fields were emitted.</div>}
                    </div>

                    {reportCoverage?.onboarding_checklist.length ? (
                      <details className="blueprint-secondary-details">
                        <summary>Candidate onboarding checklist</summary>
                        <div className="fact-list" style={{ marginTop: 12 }}>
                          {reportCoverage.onboarding_checklist.map((item) => (
                            <div className="fact-line" key={`${reportCoverage.candidate_id}-${item.label}`}>
                              <strong>{item.label}</strong>
                              <span>
                                <span className={pillClass(checklistTone(item.state))}>{humanizeCode(item.state)}</span>
                                {item.detail ? ` ${item.detail}` : ""}
                              </span>
                            </div>
                          ))}
                        </div>
                      </details>
                    ) : null}

                    {display.researchSupport ? (
                      <details className="blueprint-secondary-details">
                        <summary>Supporting research</summary>
                        <div style={{ marginTop: 12 }}>
                          {renderResearchSupportBody(display.researchSupport)}
                        </div>
                      </details>
                    ) : null}
                  </section>
                ) : null}
              </>
            ) : null}
          </div>
        </div>
      </div>
    );
  }

  function renderInspectorPanel(display: { inspector: InspectorLine[] }) {
    const selectedCandidate = blueprintDisplay?.sleeves
      .flatMap((s) => s.candidates)
      .find((c) => c.id === selectedCandidateId) ?? null;
    return (
      <>
        <div className="inspector-title">Context inspector</div>
        {selectedCandidate ? (
          <div className="inspector-verdict-block">
            <span className={`urgency-tag ${
              selectedCandidate.statusTone === "good" ? "urgency-monitor"
              : selectedCandidate.statusTone === "warn" ? "urgency-review"
              : selectedCandidate.statusTone === "bad" ? "urgency-act"
              : "urgency-background"}`}>
              {selectedCandidate.statusLabel}
            </span>
            <h3 style={{ margin: "10px 0 0", fontFamily: "var(--font-display)", fontSize: 28, lineHeight: 1.05 }}>
              {selectedCandidate.symbol}
            </h3>
            <p className="inspector-verdict-text">{selectedCandidate.whyNow}</p>
          </div>
        ) : null}
        <div className="inspector-list">
          {display.inspector.map((line) => (
            <div className={`inspector-item${line.tone === "bad" ? " blocker" : ""}`} key={line.label}>
              <strong>{line.label}</strong><br />{line.value}
            </div>
          ))}
        </div>
      </>
    );
  }

  return (
    <>
      <div className="shell">
        <aside className="rail">
          <div>
            <div className="brand-label">Investor workflow</div>
            <div className="brand-title">Private Investor Portfolio</div>
          </div>

          {NAV.map((item) => (
            <button
              className={`nav-button ${view === item.id ? "active" : ""}`}
              data-view={item.id}
              key={item.id}
              onClick={() => setView(item.id)}
            >
              <div className="nav-kicker">{item.kicker}</div>
              <div className="nav-title">{item.title}</div>
            </button>
          ))}

          <div className={`nav-status ${apiOk === true ? "ok" : apiOk === false ? "err" : ""}`}>
            <span className="nav-status-dot" />
            {apiOk === null ? "Connecting…" : apiOk ? "V2 API Live" : "API unreachable"}
          </div>
        </aside>

        <main className="main">
          <div className="topbar">
            <div>
              <div className="topbar-kicker">{currentMeta?.kicker ?? "Investor workflow"}</div>
              <div className="topbar-title">{currentMeta?.title ?? "CORTEX"}</div>
              <div className="topbar-copy">{currentMeta?.copy ?? "Renderer-only shell loading..."}</div>
              <div className="badge-row">
                {topbarBadges.map((badge) => (
                  <span className={badgeClass(badge.tone)} key={badge.label}>
                    {badge.label}
                  </span>
                ))}
              </div>
            </div>
            <button className="refresh" type="button" onClick={() => void refreshSurface()}>
              {refreshing ? "Refreshing..." : "Refresh surface"}
            </button>
          </div>

          {view === "portfolio" && (
            portfolioDisplay
              ? renderPortfolioSurface(portfolioDisplay)
              : portfolio.error
                ? <div className="surface-error">{portfolio.error}</div>
                : <div className="pf-empty-state">Loading Portfolio…</div>
          )}
          {view === "brief" && (
            briefDisplay
              ? renderBriefSurface(briefDisplay)
              : brief.error
                ? <div className="surface-error">{brief.error}</div>
                : <div className="pf-empty-state">Loading Daily Brief…</div>
          )}
          {view === "candidates" && (
            blueprintDisplay
              ? renderBlueprintSurface(blueprintDisplay)
              : blueprint.error
                ? <div className="surface-error">{blueprint.error}</div>
                : <div className="pf-empty-state">Loading Blueprint…</div>
          )}
          {view === "notebook" && (
            notebookDisplay
              ? renderNotebookSurface(notebookDisplay)
              : selectedCandidateId && notebookCache[selectedCandidateId]?.loading
                ? <div className="pf-empty-state">Loading Research Notebook…</div>
                : <div className="pf-empty-state">Select a candidate from Blueprint to open the notebook.</div>
          )}
          {view === "evidence" && (
            evidenceDisplay
              ? renderEvidenceSurface(evidenceDisplay)
              : selectedCandidateId && evidenceCache[selectedCandidateId]?.loading
                ? <div className="pf-empty-state">Loading Evidence Workspace…</div>
                : <div className="pf-empty-state">Select a candidate from Blueprint to open the evidence workspace.</div>
          )}
        </main>
      </div>
      {renderReportDrawer(reportDisplay)}
    </>
  );
}

export default App;
