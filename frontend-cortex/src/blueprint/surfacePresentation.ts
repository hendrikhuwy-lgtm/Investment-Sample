import type { MarketPathPresentation } from "./marketPathPresentation";

export type SurfaceTone = "good" | "warn" | "bad" | "neutral" | "info";

export type SleevePosturePresentation = {
  label: string;
  tone: SurfaceTone;
  summary: string;
  detail: string;
  blockLabel: string;
  reopenLabel: string;
  actionableCandidateCount: number;
  reviewableCandidateCount: number;
  activeSupportCandidateCount: number;
  blockedCandidateCount: number;
  leaderBlockedButReviewable: boolean;
};

export type SleevePostureCandidate = {
  decisionStateRaw: string | null | undefined;
  investorStateRaw: string | null | undefined;
  gateStateRaw: string | null | undefined;
  marketPath: MarketPathPresentation | null | undefined;
};

function humanizeToken(value: string | null | undefined): string {
  const raw = String(value ?? "").trim();
  if (!raw) return "Unavailable";
  return raw
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function normalizeSentenceCase(value: string): string {
  return value
    .replace(/(^\s*[a-z])/, (match) => match.toUpperCase())
    .replace(/([.!?]\s+)([a-z])/g, (_, prefix, char) => `${prefix}${String(char).toUpperCase()}`);
}

export function cleanBlueprintCopy(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const cleaned = raw
    .replace(/\bRight now the evidence base is still thin\./gi, "Source coverage is still thin.")
    .replace(/\bRight now the evidence base is still weak\./gi, "Source coverage is still thin.")
    .replace(/\bRight now the evidence base still has conflicts\./gi, "Source coverage still has conflicts.")
    .replace(/\bRight now the evidence base is still mixed\./gi, "Source coverage is still mixed.")
    .replace(/\bevidence quality still needs to improve\b/gi, "source coverage still needs cleanup")
    .replace(/\bidentity support is still not strong enough for action\b/gi, "instrument identity still needs cleanup before action")
    .replace(/\bRecommendation-critical fields are stale\b/gi, "Key implementation fields are stale")
    .replace(/\bRecommendation-critical fields disagree across sources\b/gi, "Key recommendation facts disagree across sources")
    .replace(/\bCritical implementation fields are missing\b/gi, "Key implementation fields are missing")
    .replace(/\bstill has benchmark lineage gaps\b/gi, "still needs cleaner benchmark lineage support")
    .replace(/\bdoes not yet expose primary listing venue\b/gi, "still lacks a clean primary listing venue record")
    .replace(/\bwith unresolved AUM\b/gi, "with unresolved AUM support")
    .replace(/\bremaining weak evidence\b/gi, "remaining thin source coverage")
    .replace(/\bIdentity is usable, but one or more lower-quality identity records were rejected as invalid\./gi, "Identity is usable, but lower-quality records were rejected.")
    .replace(/\bNon-critical fields show drift across sources\b/gi, "Lower-priority fields still drift across sources")
    .replace(/\bThe view improves if more recommendation-critical fields move onto stronger and cleaner sources\./gi, "The view improves if more key fields move onto stronger and cleaner sources.")
    .replace(/\bNo explicit block emitted\./gi, "No investor-facing blocker has been surfaced yet.")
    .replace(/\bNo explicit reopen trigger emitted\./gi, "No investor-facing reopen trigger has been surfaced yet.")
    .replace(/\bDecision boundary still under review\./gi, "Decision boundary still needs cleanup.")
    .replace(/\s+/g, " ")
    .trim();
  return normalizeSentenceCase(cleaned);
}

export function presentBlueprintDecisionState(value: string | null | undefined): string {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    blocked: "Blocked",
    shortlisted: "Reviewable",
    eligible: "Actionable",
    review: "Reviewable",
    watch: "Still in view",
    research_only: "Research only",
  };
  return map[raw] ?? humanizeToken(raw);
}

export function presentBlueprintCandidateStatus(value: string | null | undefined): string {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    "blocked candidate": "Blocked for now",
    "lead under review": "Current leader under review",
    "alternative candidate": "Alternative in view",
  };
  return map[raw] ?? humanizeToken(raw);
}

export function presentBlueprintBlocker(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return null;
  if (raw.includes("evidence") || raw.includes("source")) return "Source integrity under review";
  if (raw.includes("implementation")) return "Implementation cleanup required";
  if (raw.includes("identity")) return "Identity review required";
  if (raw.includes("mandate")) return "Sleeve mandate conflict";
  if (raw.includes("review")) return "Manual review still needed";
  return humanizeToken(raw);
}

export function presentSourceCoverageLabel(value: string | null | undefined): string {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return "Source integrity unavailable";
  if (raw === "complete" || raw.includes("source-complete")) return "Source complete";
  if (raw.includes("strong")) return "Source integrity strong";
  if (raw.includes("clean")) return "Source integrity clean";
  if (raw.includes("mixed")) return "Source integrity mixed";
  if (raw.includes("thin")) return "Source integrity thin";
  if (raw.includes("weak") || raw.includes("incomplete") || raw.includes("low")) return "Source integrity weak";
  if (raw.includes("conflicted")) return "Source integrity conflicted";
  if (raw.includes("missing")) return "Source integrity missing";
  if (raw.includes("verified")) return "Source integrity verified";
  return humanizeToken(raw);
}

export function presentEvidenceDepth(value: string | null | undefined): string {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    substantial: "Deep support",
    moderate: "Useful support",
    limited: "Support still thin",
  };
  return map[raw] ?? humanizeToken(raw);
}

export function presentRecommendationGateState(value: string | null | undefined): string {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    admissible: "Actionable",
    review_only: "Reviewable",
    blocked: "Blocked",
  };
  return map[raw] ?? humanizeToken(raw);
}

export function presentBlueprintFailureClass(value: string | null | undefined): string {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    identity_conflict: "Identity conflict",
    missing_truth: "Missing key facts",
    conflicting_truth: "Conflicting key facts",
    weak_authority_truth: "Weak-authority truth",
    stale_truth: "Stale truth",
    bounded_proxy_support: "Bounded proxy support",
    doctrine_restraint: "Process still in review",
    execution_review_required: "Execution review required",
    execution_invalid: "Execution truth too weak",
  };
  return map[raw] ?? humanizeToken(raw);
}

export function presentSleeveActionabilityState(value: string | null | undefined): string {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    ready: "Actionable",
    reviewable: "Reviewable",
    bounded: "Bounded",
    blocked: "Blocked",
  };
  return map[raw] ?? humanizeToken(raw);
}

function isActionable(candidate: SleevePostureCandidate): boolean {
  const decision = String(candidate.decisionStateRaw ?? "").trim().toLowerCase();
  const investor = String(candidate.investorStateRaw ?? "").trim().toLowerCase();
  const gate = String(candidate.gateStateRaw ?? "").trim().toLowerCase();
  return gate === "admissible" || decision === "eligible" || investor === "eligible";
}

function isReviewable(candidate: SleevePostureCandidate): boolean {
  if (isActionable(candidate)) return true;
  const decision = String(candidate.decisionStateRaw ?? "").trim().toLowerCase();
  const investor = String(candidate.investorStateRaw ?? "").trim().toLowerCase();
  const gate = String(candidate.gateStateRaw ?? "").trim().toLowerCase();
  return gate === "review_only" || decision === "review" || decision === "watch" || investor === "shortlisted";
}

export function deriveSleevePosture(candidates: SleevePostureCandidate[]): SleevePosturePresentation {
  const actionableCandidateCount = candidates.filter((candidate) => isActionable(candidate)).length;
  const reviewableCandidateCount = candidates.filter((candidate) => !isActionable(candidate) && isReviewable(candidate)).length;
  const activeSupportCandidateCount = candidates.filter((candidate) => candidate.marketPath && !candidate.marketPath.isSuppressed).length;
  const stabilizingSupportCandidateCount = candidates.filter((candidate) => {
    const usefulness = String(candidate.marketPath?.usefulness ?? "").trim().toLowerCase();
    return usefulness === "strong" || usefulness === "usable" || usefulness === "usable_with_caution";
  }).length;
  const blockedCandidateCount = Math.max(0, candidates.length - actionableCandidateCount - reviewableCandidateCount);
  const leader = candidates[0] ?? null;
  const leaderBlockedButReviewable = Boolean(
    leader &&
    !isActionable(leader) &&
    !isReviewable(leader) &&
    (actionableCandidateCount > 0 || reviewableCandidateCount > 0),
  );

  if (actionableCandidateCount > 0) {
    return {
      label: "Actionable",
      tone: "good",
      summary: `${actionableCandidateCount} candidate${actionableCandidateCount === 1 ? " is" : "s are"} clean enough to move from review into action.`,
      detail: leaderBlockedButReviewable
        ? "The current leader is blocked, but another candidate is already clean enough to act on."
        : "At least one candidate is clean enough to move from review into action now.",
      blockLabel: "What could still block action",
      reopenLabel: "What would strengthen the sleeve",
      actionableCandidateCount,
      reviewableCandidateCount,
      activeSupportCandidateCount,
      blockedCandidateCount,
      leaderBlockedButReviewable,
    };
  }

  if (reviewableCandidateCount > 0) {
    return {
      label: "Reviewable",
      tone: "info",
      summary: leaderBlockedButReviewable
        ? `The current leader is blocked, but ${reviewableCandidateCount} alternative candidate${reviewableCandidateCount === 1 ? "" : "s"} keep the sleeve open for review.`
        : `${reviewableCandidateCount} candidate${reviewableCandidateCount === 1 ? " still keeps" : "s still keep"} the sleeve in active review.`,
      detail: activeSupportCandidateCount > 0
        ? `${activeSupportCandidateCount} candidate${activeSupportCandidateCount === 1 ? " still has" : "s still have"} active market-path support behind the review.`
        : "Nothing is clean enough to act on yet, but the sleeve still has candidates worth keeping in view.",
      blockLabel: "What still needs cleanup",
      reopenLabel: "What would strengthen the sleeve",
      actionableCandidateCount,
      reviewableCandidateCount,
      activeSupportCandidateCount,
      blockedCandidateCount,
      leaderBlockedButReviewable,
    };
  }

  if (stabilizingSupportCandidateCount > 0) {
    return {
      label: "Bounded",
      tone: "warn",
      summary: "Active support exists, but every candidate still carries a hard decision block.",
      detail: `${stabilizingSupportCandidateCount} candidate${stabilizingSupportCandidateCount === 1 ? " still has" : "s still have"} usable support, so the sleeve stays bounded rather than closed for good.`,
      blockLabel: "What still keeps action closed",
      reopenLabel: "What would reopen the sleeve",
      actionableCandidateCount,
      reviewableCandidateCount,
      activeSupportCandidateCount,
      blockedCandidateCount,
      leaderBlockedButReviewable,
    };
  }

  return {
    label: "Blocked",
    tone: "bad",
    summary: activeSupportCandidateCount > 0
      ? "Action stays closed and the remaining support is still too fragile to soften that read."
      : "Action stays closed until cleaner decision support appears.",
    detail: activeSupportCandidateCount > 0
      ? "Market-path support exists, but it is still too fragile or too narrow to make the sleeve reviewable."
      : "Every candidate still lacks reviewable decision support, so the sleeve remains structurally blocked.",
    blockLabel: "What blocks action",
    reopenLabel: "What must clear first",
    actionableCandidateCount,
    reviewableCandidateCount,
    activeSupportCandidateCount,
    blockedCandidateCount,
    leaderBlockedButReviewable,
  };
}
