import type { BlueprintMarketPathSupport } from "../../../shared/v2_surface_contracts";

export type MarketPathTone = "good" | "warn" | "bad" | "neutral" | "info";

export type MarketPathPresentation = {
  usefulness: string;
  setupStateRaw: string;
  stateLabel: string;
  stateTone: MarketPathTone;
  caseLabel: string;
  objectiveLabel: string;
  objectiveNote: string;
  suppressionLabel: string | null;
  provenanceLabel: string | null;
  provenanceTone: MarketPathTone;
  qualityLabel: string | null;
  fragilityLabel: string | null;
  driftLabel: string | null;
  qualityNote: string | null;
  providerLabel: string | null;
  generatedLabel: string | null;
  timingState: string;
  timingLabel: string;
  timingReasons: string[];
  timingTone: MarketPathTone;
  implication: string | null;
  summaryLine: string | null;
  canPromote: boolean;
  isSuppressed: boolean;
  hasRenderablePath: boolean;
};

function humanizeToken(value: string | null | undefined): string {
  const raw = String(value ?? "").trim();
  if (!raw) return "Unavailable";
  return raw
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatDateTime(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function compactParts(parts: Array<string | null | undefined>): string | null {
  const rows = parts.map((part) => String(part ?? "").trim()).filter(Boolean);
  return rows.length ? rows.join(" · ") : null;
}

function listTimingReasons(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((item) => String(item ?? "").trim()).filter(Boolean);
}

function providerLabel(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const tokenMap: Record<string, string> = {
    twelve_data: "Twelve Data",
    approved_proxy: "approved proxy",
    kronos: "Kronos",
    polygon: "Polygon",
    fmp: "FMP",
  };
  return raw
    .split("+")
    .map((token) => tokenMap[token.trim()] ?? humanizeToken(token))
    .join(" + ");
}

export function presentMarketPathSuppressionReason(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return null;
  const map: Record<string, string> = {
    insufficient_history: "Not enough market history yet",
    provider_unavailable: "Market data is unavailable right now",
    symbol_mapping_failed: "Symbol mapping is not ready yet",
    stale_series: "Stored history is too stale to rely on",
    quality_degraded: "Series quality is too weak to rely on",
    model_execution_failed: "The market-path model run did not complete",
    output_suppressed: "Support was too weak to surface cleanly",
  };
  return map[raw] ?? humanizeToken(raw);
}

export function presentMarketPathUsefulness(value: string | null | undefined): string {
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    strong: "Strong market-path support",
    usable: "Bounded market-path support",
    usable_with_caution: "Review-only market-path support",
    unstable: "Fragile market-path support",
    suppressed: "No usable market-path support",
  };
  return map[raw] ?? humanizeToken(raw);
}

export function marketPathTone(value: string | null | undefined): MarketPathTone {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw === "strong") return "good";
  if (raw === "usable") return "info";
  if (raw === "usable_with_caution") return "warn";
  if (raw === "unstable") return "bad";
  if (raw === "suppressed") return "neutral";
  return "neutral";
}

export function presentTimingState(value: string | null | undefined, explicitLabel?: string | null): string {
  const label = String(explicitLabel ?? "").trim();
  if (label) return label;
  const raw = String(value ?? "").trim().toLowerCase();
  const map: Record<string, string> = {
    timing_ready: "Timing ready",
    timing_review: "Timing review",
    timing_fragile: "Timing fragile",
    timing_constrained: "Timing constrained",
    timing_unavailable: "Timing unavailable",
  };
  return map[raw] ?? "Timing not assessed";
}

export function timingStateTone(value: string | null | undefined): MarketPathTone {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw === "timing_ready") return "good";
  if (raw === "timing_review") return "info";
  if (raw === "timing_constrained") return "warn";
  if (raw === "timing_fragile") return "bad";
  if (raw === "timing_unavailable") return "neutral";
  return "neutral";
}

function marketSetupState(support: BlueprintMarketPathSupport | null | undefined): string {
  const explicit = String(support?.market_setup_state ?? "").trim().toLowerCase();
  if (explicit) return explicit;
  const usefulness = String(support?.usefulness_label ?? "").trim().toLowerCase();
  if (usefulness === "suppressed") return "unavailable";
  return support?.series_quality_summary?.uses_proxy_series ? "proxy_usable" : "direct_usable";
}

function presentMarketSetupState(support: BlueprintMarketPathSupport | null | undefined): string {
  const raw = marketSetupState(support);
  const map: Record<string, string> = {
    direct_usable: "Direct market setup",
    proxy_usable: "Proxy-backed market setup",
    degraded: "Degraded market setup",
    stale: "Stored market setup",
    unavailable: "No usable market setup",
  };
  return map[raw] ?? humanizeToken(raw);
}

function marketSetupTone(support: BlueprintMarketPathSupport | null | undefined): MarketPathTone {
  const raw = marketSetupState(support);
  if (raw === "direct_usable") return "good";
  if (raw === "proxy_usable") return "warn";
  if (raw === "degraded") return "warn";
  if (raw === "stale") return "warn";
  return "neutral";
}

export function presentMarketPathProvenance(support: BlueprintMarketPathSupport | null | undefined): string | null {
  if (!support) return null;
  const state = marketSetupState(support);
  const driving = String(support.driving_symbol ?? "").trim().toUpperCase();
  const proxy = String(support.proxy_symbol ?? "").trim().toUpperCase();
  if (state === "proxy_usable") {
    return proxy ? `Proxy via ${proxy}` : "Proxy-backed route";
  }
  if (state === "stale") {
    return support.freshness_state === "last_good" ? "Last good run" : "Stored route";
  }
  if (state === "degraded") {
    return support.liquidity_feature_mode === "price_only" ? "Price-only mode" : "Degraded route";
  }
  if (state === "unavailable") {
    return "No usable route evidence";
  }
  return driving ? `Direct ${driving}` : "Direct-series route";
}

function provenanceTone(support: BlueprintMarketPathSupport | null | undefined): MarketPathTone {
  if (!support) return "neutral";
  return marketSetupTone(support);
}

export function presentPathQuality(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return null;
  const map: Record<string, string> = {
    clean: "Clean path",
    balanced: "Balanced path",
    noisy: "Noisy path",
  };
  return map[raw] ?? `${humanizeToken(raw)} path`;
}

export function presentFragility(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return null;
  const map: Record<string, string> = {
    resilient: "Low fragility",
    watchful: "Watchful fragility",
    fragile: "High fragility",
    acute: "Acute fragility",
  };
  return map[raw] ?? `${humanizeToken(raw)} fragility`;
}

export function presentDriftDirection(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return null;
  const map: Record<string, string> = {
    toward_strengthening: "Drifting stronger",
    toward_weakening: "Drifting weaker",
    balanced: "Drift balanced",
  };
  return map[raw] ?? `${humanizeToken(raw)} drift`;
}

function presentSeriesQuality(value: string | null | undefined): string | null {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return null;
  const map: Record<string, string> = {
    healthy: "Series quality healthy",
    balanced: "Series quality balanced",
    watch: "Series quality watch",
    degraded: "Series quality too weak",
    quality_watch: "Series quality watch",
  };
  return map[raw] ?? `Series quality ${humanizeToken(raw).toLowerCase()}`;
}

function presentQualityNote(support: BlueprintMarketPathSupport | null | undefined): string | null {
  const quality = support?.series_quality_summary;
  if (!quality) return null;
  const freshness =
    typeof quality.stale_days === "number"
      ? quality.stale_days <= 2
        ? `Fresh within ${quality.stale_days} day${quality.stale_days === 1 ? "" : "s"}`
        : `${quality.stale_days} days stale`
      : null;
  return compactParts([
    presentSeriesQuality(quality.quality_label),
    freshness,
    quality.has_corporate_action_uncertainty ? "Corporate action adjustments need caution" : null,
  ]);
}

function objectiveCopy(support: BlueprintMarketPathSupport | null | undefined): {
  caseLabel: string;
  objectiveLabel: string;
  objectiveNote: string;
} {
  const usefulness = String(support?.usefulness_label ?? "").trim().toLowerCase();
  const state = marketSetupState(support);
  const usesProxy = state === "proxy_usable";
  const drift = String(support?.threshold_drift_direction ?? "").trim().toLowerCase();
  const fragility = String(support?.candidate_fragility_label ?? "").trim().toLowerCase();
  const pathQuality = String(support?.path_quality_label ?? "").trim().toLowerCase();

  if (state === "unavailable" || usefulness === "suppressed") {
    return {
      caseLabel: "No usable market setup",
      objectiveLabel: "No usable market-path support is currently available as evidence",
      objectiveNote: "Current stored history is not reliable enough to support a bounded market-path read yet.",
    };
  }
  if (state === "stale") {
    return {
      caseLabel: "Stored bounded context",
      objectiveLabel: "Stored setup stays visible, but it is no longer fresh",
      objectiveNote: "Keep the setup secondary and treat freshness as part of the risk until a live run replaces it.",
    };
  }
  if (state === "degraded") {
    return {
      caseLabel: "Degraded bounded context",
      objectiveLabel: "Support is still visible, but the route is degraded",
      objectiveNote: "The setup remains informative, but it is running with weaker route truth or thinner features than ideal.",
    };
  }
  if (usefulness === "strong" && !usesProxy && pathQuality === "clean" && (fragility === "resilient" || fragility === "watchful")) {
    return {
      caseLabel: "Support reinforces the sleeve read",
      objectiveLabel: "Support reinforces the current sleeve read",
      objectiveNote: "Direct-series structure is clean enough that the bounded path strengthens the current candidate read.",
    };
  }
  if (usefulness === "unstable" || fragility === "acute" || drift === "toward_weakening") {
    return {
      caseLabel: "Active, but too fragile",
      objectiveLabel: "Current path is active but too fragile to strengthen the candidate",
      objectiveNote: "Use the path as context only. It is still narrow, weak, or drifting toward breakage.",
    };
  }
  if (usefulness === "usable_with_caution" || usesProxy) {
    return {
      caseLabel: "Bounded proxy context",
      objectiveLabel: "Support exists, but only as bounded proxy context",
      objectiveNote: "Keep it secondary. The path is useful, but it still depends on proxy behaviour instead of direct-series authority.",
    };
  }
  return {
    caseLabel: "Bounded support only",
    objectiveLabel: "Support stays bounded and secondary",
    objectiveNote: "The path remains active, but it is not strong enough to upgrade the decision on its own.",
  };
}

export function describeMarketPathSupport(
  support: BlueprintMarketPathSupport | null | undefined,
): MarketPathPresentation | null {
  if (!support) return null;
  const usefulness = String(support.usefulness_label ?? "").trim().toLowerCase();
  const setupState = marketSetupState(support);
  const suppressionLabel = presentMarketPathSuppressionReason(support.suppression_reason);
  const provenance = presentMarketPathProvenance(support);
  const qualityLabel = presentPathQuality(support.path_quality_label);
  const fragilityLabel = presentFragility(support.candidate_fragility_label);
  const driftLabel = presentDriftDirection(support.threshold_drift_direction);
  const summaryLine = compactParts([qualityLabel, fragilityLabel, driftLabel, provenance]);
  const objective = objectiveCopy(support);
  const timingState = String(support.timing_state ?? "").trim().toLowerCase();
  const hasRenderablePath = Boolean(
    support.observed_series?.length ||
    support.projected_series?.length ||
    support.uncertainty_band?.lower_points?.length ||
    support.uncertainty_band?.upper_points?.length
  );
  return {
    usefulness,
    setupStateRaw: setupState,
    stateLabel: presentMarketSetupState(support),
    stateTone: marketSetupTone(support),
    caseLabel: objective.caseLabel,
    objectiveLabel: objective.objectiveLabel,
    objectiveNote: objective.objectiveNote,
    suppressionLabel,
    provenanceLabel: provenance,
    provenanceTone: provenanceTone(support),
    qualityLabel,
    fragilityLabel,
    driftLabel,
    qualityNote: presentQualityNote(support),
    providerLabel: providerLabel(support.provider_source),
    generatedLabel: formatDateTime(support.generated_at),
    timingState,
    timingLabel: presentTimingState(timingState, support.timing_label),
    timingReasons: listTimingReasons(support.timing_reasons),
    timingTone: timingStateTone(timingState),
    implication: String(support.candidate_implication ?? "").trim() || null,
    summaryLine,
    canPromote: setupState === "direct_usable" && usefulness === "strong" && Boolean(support.projected_series?.length),
    isSuppressed: setupState === "unavailable",
    hasRenderablePath,
  };
}
