import React from "react";

import type { BlueprintMarketPathPoint, BlueprintMarketPathSupport } from "../../../shared/v2_surface_contracts";
import {
  describeMarketPathSupport,
  presentDriftDirection,
  presentFragility,
  presentPathQuality,
  presentMarketPathSuppressionReason,
} from "./marketPathPresentation";

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

function pathD(points: BlueprintMarketPathPoint[], xForIndex: (index: number) => number, yForValue: (value: number) => number): string {
  return points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${xForIndex(index)} ${yForValue(point.value)}`)
    .join(" ");
}

function bandD(
  lowerPoints: BlueprintMarketPathPoint[],
  upperPoints: BlueprintMarketPathPoint[],
  xForIndex: (index: number) => number,
  yForValue: (value: number) => number,
): string {
  const upper = upperPoints.map((point, index) => `${index === 0 ? "M" : "L"} ${xForIndex(index)} ${yForValue(point.value)}`);
  const lower = [...lowerPoints]
    .reverse()
    .map((point, reverseIndex) => {
      const originalIndex = lowerPoints.length - reverseIndex - 1;
      return `L ${xForIndex(originalIndex)} ${yForValue(point.value)}`;
    });
  return [...upper, ...lower, "Z"].join(" ");
}

function lastPoint(points: BlueprintMarketPathPoint[] | null | undefined): BlueprintMarketPathPoint | null {
  if (!points?.length) return null;
  return points[points.length - 1] ?? null;
}

function endPointLabel(points: BlueprintMarketPathPoint[] | null | undefined): string {
  const point = lastPoint(points);
  if (!point) return "Not surfaced";
  return `${point.value.toFixed(2)} · ${point.timestamp}`;
}

function statValue(value: number | null | undefined, fallback = "Unavailable"): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return fallback;
  return value.toFixed(2);
}

function pctDistance(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "Unavailable";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function formatGenerated(value: string | null | undefined): string {
  const raw = String(value ?? "").trim();
  if (!raw) return "Unavailable";
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

function scenarioStroke(scenarioType: string | null | undefined): string {
  const raw = String(scenarioType ?? "").trim().toLowerCase();
  if (raw === "stress") return "var(--red)";
  if (raw === "downside") return "var(--orange)";
  return "var(--blue)";
}

function scenarioDash(scenarioType: string | null | undefined): string {
  const raw = String(scenarioType ?? "").trim().toLowerCase();
  if (raw === "stress") return "3 3";
  if (raw === "downside") return "5 4";
  return "7 4";
}

function toneColor(tone: string | null | undefined): string {
  if (tone === "good") return "var(--green)";
  if (tone === "warn") return "var(--orange)";
  if (tone === "bad") return "var(--red)";
  if (tone === "info") return "var(--blue)";
  return "var(--text-soft)";
}

type MarketPathTheme = {
  panelBorder: string;
  panelGlow: string;
  observedStroke: string;
  projectedStroke: string;
  projectedDash: string;
  bandFill: string;
  chartFill: string;
  legendObserved: string;
  legendProjected: string;
  legendBand: string;
  caseLabel: string;
  caseNote: string;
};

type ObjectiveRead = {
  title: string;
  note: string;
};

function visualTheme(support: BlueprintMarketPathSupport, stateTone: string | null | undefined): MarketPathTheme {
  const usefulness = String(support.usefulness_label ?? "").trim().toLowerCase();
  const usesProxy = Boolean(support.series_quality_summary?.uses_proxy_series);
  const pathQuality = String(support.path_quality_label ?? "").trim().toLowerCase();
  const fragility = String(support.candidate_fragility_label ?? "").trim().toLowerCase();
  const drift = String(support.threshold_drift_direction ?? "").trim().toLowerCase();

  if (usefulness === "suppressed") {
    return {
      panelBorder: "rgba(220, 210, 190, 0.18)",
      panelGlow: "rgba(255,255,255,0.03)",
      observedStroke: "rgba(220,210,190,0.35)",
      projectedStroke: "rgba(220,210,190,0.35)",
      projectedDash: "4 4",
      bandFill: "rgba(220,210,190,0.08)",
      chartFill: "rgba(255,255,255,0.02)",
      legendObserved: "rgba(220,210,190,0.6)",
      legendProjected: "rgba(220,210,190,0.55)",
      legendBand: "rgba(220,210,190,0.5)",
      caseLabel: "No usable market-path support is currently available as evidence",
      caseNote: "No usable projected path is available yet, so market structure should stay out of the decision read.",
    };
  }

  if (usefulness === "unstable" || fragility === "acute" || drift === "toward_weakening") {
    return {
      panelBorder: "rgba(214, 108, 64, 0.32)",
      panelGlow: "rgba(214, 108, 64, 0.08)",
      observedStroke: "var(--orange)",
      projectedStroke: "var(--red)",
      projectedDash: "5 3",
      bandFill: "rgba(214, 108, 64, 0.18)",
      chartFill: "rgba(120,40,22,0.16)",
      legendObserved: "var(--orange)",
      legendProjected: "var(--red)",
      legendBand: "rgba(214, 108, 64, 0.8)",
      caseLabel: "Current path is active but too fragile to strengthen the candidate",
      caseNote: "Use the path as context only. It is still narrow, weak, or drifting toward breakage.",
    };
  }

  if (usefulness === "strong" && !usesProxy && pathQuality === "clean" && (fragility === "resilient" || fragility === "watchful")) {
    return {
      panelBorder: "rgba(88, 160, 120, 0.30)",
      panelGlow: "rgba(88, 160, 120, 0.08)",
      observedStroke: "var(--green)",
      projectedStroke: "var(--blue)",
      projectedDash: "7 4",
      bandFill: "rgba(82, 126, 194, 0.16)",
      chartFill: "rgba(30,72,44,0.16)",
      legendObserved: "var(--green)",
      legendProjected: "var(--blue)",
      legendBand: "rgba(112, 153, 219, 0.85)",
      caseLabel: "Support reinforces the current sleeve read",
      caseNote: "Direct-series structure is clean enough that the bounded path strengthens the current candidate read.",
    };
  }

  if (usesProxy || usefulness === "usable_with_caution") {
    return {
      panelBorder: "rgba(196, 146, 60, 0.30)",
      panelGlow: "rgba(196, 146, 60, 0.08)",
      observedStroke: "var(--green)",
      projectedStroke: "var(--gold)",
      projectedDash: "6 4",
      bandFill: "rgba(196, 146, 60, 0.20)",
      chartFill: "rgba(76,58,20,0.16)",
      legendObserved: "var(--green)",
      legendProjected: "var(--gold)",
      legendBand: "rgba(219, 180, 79, 0.85)",
      caseLabel: "Support exists, but only as bounded proxy context",
      caseNote: "Keep it secondary. The path is useful, but it still depends on proxy behaviour instead of direct-series authority.",
    };
  }

  if (stateTone === "info") {
    return {
      panelBorder: "rgba(82, 126, 194, 0.28)",
      panelGlow: "rgba(82, 126, 194, 0.08)",
      observedStroke: "var(--green)",
      projectedStroke: "var(--blue)",
      projectedDash: "6 4",
      bandFill: "rgba(82, 126, 194, 0.18)",
      chartFill: "rgba(26,45,78,0.16)",
      legendObserved: "var(--green)",
      legendProjected: "var(--blue)",
      legendBand: "rgba(112, 153, 219, 0.85)",
      caseLabel: "Support stays bounded and secondary",
      caseNote: "The path remains active, but it is not strong enough to upgrade the decision on its own.",
    };
  }

  return {
    panelBorder: "rgba(196, 146, 60, 0.24)",
    panelGlow: "rgba(196, 146, 60, 0.06)",
    observedStroke: "var(--green)",
    projectedStroke: "var(--gold)",
    projectedDash: "6 4",
    bandFill: "rgba(196, 146, 60, 0.18)",
    chartFill: "rgba(255,255,255,0.03)",
    legendObserved: "var(--green)",
    legendProjected: "var(--gold)",
    legendBand: "rgba(219, 180, 79, 0.85)",
    caseLabel: "Support stays bounded and secondary",
    caseNote: "The bounded path is available, but it still needs to be read in context.",
  };
}

function objectiveRead(support: BlueprintMarketPathSupport, presentationState: string | null | undefined): ObjectiveRead {
  const usefulness = String(support.usefulness_label ?? "").trim().toLowerCase();
  const usesProxy = Boolean(support.series_quality_summary?.uses_proxy_series);
  const drift = String(support.threshold_drift_direction ?? "").trim().toLowerCase();
  const fragility = String(support.candidate_fragility_label ?? "").trim().toLowerCase();

  if (usefulness === "suppressed") {
    return {
      title: "Objective read: support unavailable",
      note: "Current stored history is not reliable enough to support a bounded market-path read yet.",
    };
  }
  if (usefulness === "strong" && !usesProxy) {
    return {
      title: "Objective read: support reinforces the sleeve role",
      note: "Direct-series structure is reinforcing the candidate instead of merely avoiding damage.",
    };
  }
  if (usefulness === "usable_with_caution" || usesProxy) {
    return {
      title: "Objective read: support exists, but stays bounded",
      note: "The path can help the case, but it still depends on proxy or cautionary structure and should remain secondary.",
    };
  }
  if (usefulness === "unstable" || fragility === "acute" || drift === "toward_weakening") {
    return {
      title: "Objective read: active, but too fragile to strengthen",
      note: "Support is visible, but the path remains fragile enough that it should not be read as reinforcement.",
    };
  }
  return {
    title: presentationState === "Strong direct support" ? "Objective read: support reinforces the sleeve role" : "Objective read: bounded support only",
    note: "The path contributes context, but it does not replace decision truth or implementation truth.",
  };
}

export function MarketPathSupportPanel({
  support,
  title = "Market-path support",
  subtitle,
  showProvenance = false,
}: {
  support: BlueprintMarketPathSupport;
  title?: string;
  subtitle?: string | null;
  showProvenance?: boolean;
}) {
  const presentation = describeMarketPathSupport(support);
  const observed = support.observed_series ?? [];
  const projected = support.projected_series ?? [];
  const lowerPoints = support.uncertainty_band?.lower_points ?? [];
  const upperPoints = support.uncertainty_band?.upper_points ?? [];
  const thresholdLines = (support.threshold_map ?? []).filter((item) => Number.isFinite(item.value));
  const observedLast = lastPoint(observed);
  const projectedLast = lastPoint(projected);
  const allValues = [
    ...observed.map((point) => point.value),
    ...projected.map((point) => point.value),
    ...lowerPoints.map((point) => point.value),
    ...upperPoints.map((point) => point.value),
    ...thresholdLines.map((point) => point.value),
  ].filter((value) => Number.isFinite(value));
  const width = 720;
  const height = 220;
  const padding = 18;
  const xCount = Math.max(2, observed.length + projected.length);
  const minValue = allValues.length ? Math.min(...allValues) : 0;
  const maxValue = allValues.length ? Math.max(...allValues) : 1;
  const range = maxValue - minValue || Math.max(Math.abs(maxValue), 1);
  const xForIndex = (index: number) => padding + (index / Math.max(1, xCount - 1)) * (width - padding * 2);
  const yForValue = (value: number) => height - padding - ((value - minValue) / range) * (height - padding * 2);
  const observedPath = observed.length ? pathD(observed, xForIndex, yForValue) : "";
  const projectedOffset = observed.length;
  const projectedPath = projected.length
    ? projected
        .map((point, index) => `${index === 0 ? "M" : "L"} ${xForIndex(projectedOffset + index)} ${yForValue(point.value)}`)
        .join(" ")
    : "";
  const bandPath =
    lowerPoints.length && upperPoints.length
      ? bandD(
          lowerPoints.map((point, index) => ({ ...point, timestamp: projected[index]?.timestamp || point.timestamp })),
          upperPoints.map((point, index) => ({ ...point, timestamp: projected[index]?.timestamp || point.timestamp })),
          (index) => xForIndex(projectedOffset + index),
          yForValue,
        )
      : "";
  const anchorIndex = Math.max(0, observed.length - 1);
  const anchorX = xForIndex(anchorIndex);
  const scenarioPaths = (support.scenario_summary ?? [])
    .filter((scenario) => Array.isArray(scenario.path) && scenario.path.length)
    .map((scenario) => ({
      scenarioType: scenario.scenario_type,
      label: scenario.label,
      summary: scenario.summary,
      endLabel: endPointLabel(scenario.path),
      path: scenario.path
        .map((point, index) => `${index === 0 ? "M" : "L"} ${xForIndex(projectedOffset + index)} ${yForValue(point.value)}`)
        .join(" "),
    }));

  const quality = support.series_quality_summary;
  const suppressionLabel = presentMarketPathSuppressionReason(support.suppression_reason);
  const projectedMovePct =
    typeof observedLast?.value === "number" &&
    Number.isFinite(observedLast.value) &&
    observedLast.value !== 0 &&
    typeof projectedLast?.value === "number" &&
    Number.isFinite(projectedLast.value)
      ? ((projectedLast.value - observedLast.value) / observedLast.value) * 100
      : null;
  const theme = visualTheme(support, presentation?.stateTone);
  const objective = objectiveRead(support, presentation?.stateLabel);
  const objectiveLine =
    "Decision objective: does current observed market structure support, weaken, or fail to strengthen this candidate over the next bounded horizon?";
  const subtitleText =
    subtitle
    || presentation?.implication
    || "Use this only as bounded market-path support, never as the decision authority.";

  return (
    <section style={{ ...panelStyle, borderColor: theme.panelBorder, background: `linear-gradient(180deg, ${theme.panelGlow} 0%, rgba(255,255,255,0.02) 100%)` }}>
      <div style={headerStyle}>
        <div>
          <div style={kickerStyle}>Bounded support</div>
          <h3 style={titleStyle}>{title}</h3>
          <p style={copyStyle}>
            {subtitleText}
          </p>
        </div>
        <div style={pillRowStyle}>
          {presentation?.timingLabel ? (
            <span style={{ ...pillStyle, color: toneColor(presentation.timingTone), borderColor: toneColor(presentation.timingTone) }}>
              {presentation.timingLabel}
            </span>
          ) : null}
          {presentation?.timingReasons.slice(0, 2).map((reason) => (
            <span key={`timing-reason-${reason}`} style={{ ...pillStyle, color: toneColor(presentation.timingTone), borderColor: toneColor(presentation.timingTone) }}>
              {timingReasonLabel(reason)}
            </span>
          ))}
          {presentation ? (
            <span style={{ ...pillStyle, color: toneColor(presentation.stateTone), borderColor: toneColor(presentation.stateTone) }}>
              {presentation.stateLabel}
            </span>
          ) : null}
          {presentation?.provenanceLabel ? (
            <span style={{ ...pillStyle, color: toneColor(presentation.provenanceTone), borderColor: toneColor(presentation.provenanceTone) }}>
              {presentation.provenanceLabel}
            </span>
          ) : null}
          {presentation?.qualityLabel ? <span style={pillStyle}>{presentation.qualityLabel}</span> : null}
          {presentation?.fragilityLabel ? <span style={pillStyle}>{presentation.fragilityLabel}</span> : null}
          {presentation?.driftLabel ? <span style={pillStyle}>{presentation.driftLabel}</span> : null}
        </div>
      </div>

      {presentation?.summaryLine ? (
        <div style={summaryNoteStyle}>{presentation.summaryLine}</div>
      ) : null}

      <div style={{ ...summaryNoteStyle, borderColor: theme.panelBorder, background: theme.chartFill, color: theme.legendProjected }}>
        <strong>{presentation?.objectiveLabel ?? theme.caseLabel}</strong>
        <span style={{ marginLeft: 8, color: "var(--text-soft)" }}>{presentation?.objectiveNote ?? theme.caseNote}</span>
      </div>

      <div style={objectiveStyle}>
        <strong>{presentation?.caseLabel ?? objective.title}</strong>
        <span>{presentation?.objectiveNote ?? objective.note} {objectiveLine}</span>
      </div>

      {presentation?.hasRenderablePath ? (
        <>
          <div style={legendRowStyle}>
            <span style={{ ...legendTagStyle, borderColor: theme.panelBorder }}>
              <span style={{ ...legendDotStyle, background: theme.legendObserved }} />
              Observed path
            </span>
            <span style={{ ...legendTagStyle, borderColor: theme.panelBorder }}>
              <span style={{ ...legendDotStyle, background: theme.legendProjected }} />
              Projected base
            </span>
            <span style={{ ...legendTagStyle, borderColor: theme.panelBorder }}>
              <span style={{ ...legendDotStyle, background: theme.legendBand }} />
              Uncertainty range
            </span>
            <span style={{ ...legendTagStyle, borderColor: theme.panelBorder }}>
              <span style={{ ...legendDotStyle, background: "var(--red)" }} />
              Stress / downside path
            </span>
            <span style={{ ...legendTagStyle, borderColor: theme.panelBorder }}>{presentation?.provenanceLabel ?? "Bounded input"}</span>
          </div>

          <div style={{ ...chartWrapStyle, borderColor: theme.panelBorder, background: theme.chartFill }}>
            <svg viewBox={`0 0 ${width} ${height}`} style={svgStyle} role="img" aria-label="Observed and projected market path">
            <rect x="0" y="0" width={width} height={height} fill={theme.chartFill} rx="16" />
            {[0.2, 0.4, 0.6, 0.8].map((ratio) => {
              const y = padding + ratio * (height - padding * 2);
              return <line key={ratio} x1={padding} y1={y} x2={width - padding} y2={y} stroke="rgba(220,210,190,0.08)" strokeWidth="1" />;
            })}
            {observed.length ? (
              <g>
                <line x1={anchorX} y1={padding} x2={anchorX} y2={height - padding} stroke="rgba(220,210,190,0.16)" strokeWidth="1" strokeDasharray="2 4" />
                <text x={Math.min(width - padding - 12, anchorX + 6)} y={padding + 10} fontSize="10" fill="rgba(220,210,190,0.7)">
                  Current anchor
                </text>
              </g>
            ) : null}
            {thresholdLines.map((threshold) => {
              const y = yForValue(threshold.value);
              const stroke =
                threshold.threshold_id === "base_case"
                  ? "var(--green)"
                  : threshold.threshold_id === "downside_case"
                    ? "var(--orange)"
                    : "var(--red)";
              return (
                <g key={threshold.threshold_id}>
                  <line x1={padding} y1={y} x2={width - padding} y2={y} stroke={stroke} strokeWidth="1.25" strokeDasharray="4 4" opacity="0.75" />
                  <text x={width - padding - 4} y={Math.max(14, y - 4)} textAnchor="end" fontSize="10" fill={stroke}>
                    {threshold.label}
                  </text>
                </g>
              );
            })}
            {bandPath ? <path d={bandPath} fill={theme.bandFill} stroke="none" /> : null}
            {observedPath ? <path d={observedPath} fill="none" stroke={theme.observedStroke} strokeWidth="2.5" strokeLinecap="round" /> : null}
            {scenarioPaths
              .filter((scenario) => String(scenario.scenarioType ?? "").trim().toLowerCase() !== "base")
              .map((scenario) => (
                <path
                  key={`scenario-${scenario.scenarioType}`}
                  d={scenario.path}
                  fill="none"
                  stroke={scenarioStroke(scenario.scenarioType)}
                  strokeWidth="1.7"
                  strokeDasharray={scenarioDash(scenario.scenarioType)}
                  strokeLinecap="round"
                  opacity="0.9"
                />
              ))}
            {projectedPath ? <path d={projectedPath} fill="none" stroke={theme.projectedStroke} strokeWidth="2.5" strokeDasharray={theme.projectedDash} strokeLinecap="round" /> : null}
            <text x={padding} y={height - 6} fontSize="10" fill="rgba(220,210,190,0.7)">
              Observed window
            </text>
            <text x={Math.max(padding, width - 108)} y={height - 6} fontSize="10" fill="rgba(220,210,190,0.7)">
              Bounded horizon
            </text>
            </svg>
          </div>
        </>
      ) : (
        <div style={{ ...emptyStateStyle, borderColor: theme.panelBorder, background: theme.chartFill }}>
          <strong>{theme.caseLabel}</strong>
          <span>{suppressionLabel || "This candidate does not have enough market-path support to render a bounded path yet."}</span>
        </div>
      )}

      <div style={statsGridStyle}>
        <div style={{ ...statCardStyle, borderColor: theme.panelBorder }}>
          <div style={statLabelStyle}>Timing state</div>
          <div style={statValueStyle}>{presentation?.timingLabel ?? "Timing not assessed"}</div>
        </div>
        <div style={{ ...statCardStyle, borderColor: theme.panelBorder }}>
          <div style={statLabelStyle}>Path quality</div>
          <div style={statValueStyle}>{presentPathQuality(support.path_quality_label) || "Unrated"}</div>
        </div>
        <div style={{ ...statCardStyle, borderColor: theme.panelBorder }}>
          <div style={statLabelStyle}>Fragility</div>
          <div style={statValueStyle}>{presentFragility(support.candidate_fragility_label) || "Unrated"}</div>
        </div>
        <div style={{ ...statCardStyle, borderColor: theme.panelBorder }}>
          <div style={statLabelStyle}>Drift</div>
          <div style={statValueStyle}>{presentDriftDirection(support.threshold_drift_direction) || "Balanced drift"}</div>
        </div>
        <div style={{ ...statCardStyle, borderColor: theme.panelBorder }}>
          <div style={statLabelStyle}>To weakening</div>
          <div style={statValueStyle}>{pctDistance(support.current_distance_to_weakening)}</div>
        </div>
        <div style={{ ...statCardStyle, borderColor: theme.panelBorder }}>
          <div style={statLabelStyle}>To strengthening</div>
          <div style={statValueStyle}>{pctDistance(support.current_distance_to_strengthening)}</div>
        </div>
        <div style={{ ...statCardStyle, borderColor: theme.panelBorder }}>
          <div style={statLabelStyle}>Projected move</div>
          <div style={statValueStyle}>{pctDistance(projectedMovePct)}</div>
        </div>
        <div style={{ ...statCardStyle, borderColor: theme.panelBorder }}>
          <div style={statLabelStyle}>Observed anchor</div>
          <div style={statValueStyle}>{statValue(observedLast?.value)}</div>
        </div>
        <div style={{ ...statCardStyle, borderColor: theme.panelBorder }}>
          <div style={statLabelStyle}>Projected end</div>
          <div style={statValueStyle}>{statValue(projectedLast?.value, "Not surfaced")}</div>
        </div>
        <div style={{ ...statCardStyle, borderColor: theme.panelBorder }}>
          <div style={statLabelStyle}>Series quality</div>
          <div style={statValueStyle}>{quality?.quality_label ? `Series ${quality.quality_label}` : "Unrated"}</div>
        </div>
      </div>

      {(support.scenario_takeaways || support.strengthening_threshold || support.weakening_threshold) ? (
        <div style={takeawayGridStyle}>
          {support.scenario_takeaways ? (
            <>
              <div style={takeawayStyle}>Mild stress: {support.scenario_takeaways.favorable_case_survives_mild_stress ? "survives" : "fails"}</div>
              <div style={takeawayStyle}>Favorable path: {support.scenario_takeaways.favorable_case_is_narrow ? "narrow" : "broad enough"}</div>
              <div style={takeawayStyle}>Downside damage: {support.scenario_takeaways.downside_damage_is_contained ? "contained" : "not contained"}</div>
              <div style={takeawayStyle}>Stress support: {support.scenario_takeaways.stress_breaks_candidate_support ? "breaks" : "holds"}</div>
            </>
          ) : null}
          {support.strengthening_threshold ? (
            <div style={takeawayStyle}>Distance to strengthening {pctDistance(support.current_distance_to_strengthening)}</div>
          ) : null}
          {support.weakening_threshold ? (
            <div style={takeawayStyle}>Distance to weakening {pctDistance(support.current_distance_to_weakening)}</div>
          ) : null}
        </div>
      ) : null}

      {(support.threshold_map ?? []).length ? (
        <div style={thresholdWrapStyle}>
          {(support.threshold_map ?? []).map((threshold) => (
            <article key={threshold.threshold_id} style={thresholdCardStyle}>
              <div style={thresholdHeaderStyle}>
                <strong>{threshold.label}</strong>
                <span>{threshold.value.toFixed(2)}</span>
              </div>
              <div style={thresholdNoteStyle}>
                {threshold.relation}
                {typeof threshold.delta_pct === "number" ? ` · ${threshold.delta_pct.toFixed(2)}%` : ""}
                {threshold.note ? ` · ${threshold.note}` : ""}
              </div>
            </article>
          ))}
        </div>
      ) : null}

      {support.scenario_summary?.length ? (
        <div style={thresholdWrapStyle}>
          {scenarioPaths.map((scenario) => (
            <article key={scenario.scenarioType} style={thresholdCardStyle}>
              <div style={thresholdHeaderStyle}>
                <strong>{scenario.label}</strong>
                <span>{scenario.endLabel}</span>
              </div>
              <div style={thresholdNoteStyle}>{scenario.summary}</div>
            </article>
          ))}
        </div>
      ) : null}

      {showProvenance ? (
        <div style={metaGridStyle}>
          {presentation?.providerLabel ? (
            <div style={metaItemStyle}>
              <strong>Source</strong>
              <span>{presentation.providerLabel}</span>
            </div>
          ) : null}
          <div style={metaItemStyle}>
            <strong>Generated</strong>
            <span>{formatGenerated(support.generated_at)}</span>
          </div>
          <div style={metaItemStyle}>
            <strong>Interval</strong>
            <span>{support.forecast_interval}</span>
          </div>
          <div style={metaItemStyle}>
            <strong>Horizon</strong>
            <span>{support.forecast_horizon} trading days</span>
          </div>
          {presentation?.qualityNote ? (
            <div style={metaItemStyle}>
              <strong>Series state</strong>
              <span>{presentation.qualityNote}</span>
            </div>
          ) : null}
          {suppressionLabel ? (
            <div style={metaItemStyle}>
              <strong>Unavailable because</strong>
              <span>{suppressionLabel}</span>
            </div>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}

const panelStyle: React.CSSProperties = {
  display: "grid",
  gap: 14,
  border: "1px solid var(--line-strong)",
  borderRadius: 16,
  padding: 16,
  background: "linear-gradient(180deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0.02) 100%)",
};

const headerStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "flex-start",
  justifyContent: "space-between",
  gap: 12,
  flexWrap: "wrap",
};

const kickerStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  letterSpacing: "0.16em",
  textTransform: "uppercase",
  color: "var(--gold)",
};

const titleStyle: React.CSSProperties = {
  margin: "4px 0 0 0",
  fontSize: 18,
  color: "var(--text)",
};

const copyStyle: React.CSSProperties = {
  margin: "8px 0 0 0",
  fontSize: 13,
  lineHeight: 1.5,
  color: "var(--text-soft)",
  maxWidth: 680,
};

const pillRowStyle: React.CSSProperties = {
  display: "flex",
  gap: 8,
  flexWrap: "wrap",
};

const pillStyle: React.CSSProperties = {
  border: "1px solid var(--line-strong)",
  borderRadius: 999,
  padding: "6px 10px",
  fontSize: 12,
  fontWeight: 700,
  color: "var(--text-soft)",
  background: "rgba(255,255,255,0.02)",
};

const summaryNoteStyle: React.CSSProperties = {
  fontSize: 12,
  color: "var(--text-soft)",
};

const objectiveStyle: React.CSSProperties = {
  display: "grid",
  gap: 4,
  padding: "10px 12px",
  borderRadius: 12,
  border: "1px solid var(--line)",
  background: "rgba(255,255,255,0.02)",
  fontSize: 12,
  color: "var(--text-soft)",
  lineHeight: 1.5,
};

const legendRowStyle: React.CSSProperties = {
  display: "flex",
  gap: 8,
  flexWrap: "wrap",
};

const legendTagStyle: React.CSSProperties = {
  display: "inline-flex",
  alignItems: "center",
  gap: 8,
  padding: "6px 10px",
  borderRadius: 999,
  border: "1px solid var(--line)",
  background: "rgba(255,255,255,0.02)",
  color: "var(--text-soft)",
  fontSize: 11,
  fontWeight: 700,
  letterSpacing: "0.03em",
};

const legendDotStyle: React.CSSProperties = {
  width: 8,
  height: 8,
  borderRadius: 999,
  display: "inline-block",
};

const chartWrapStyle: React.CSSProperties = {
  borderRadius: 16,
  overflow: "hidden",
  border: "1px solid var(--line)",
};

const svgStyle: React.CSSProperties = {
  width: "100%",
  display: "block",
};

const emptyStateStyle: React.CSSProperties = {
  display: "grid",
  gap: 6,
  border: "1px dashed var(--line-strong)",
  borderRadius: 14,
  padding: 14,
  color: "var(--text-soft)",
  background: "rgba(255,255,255,0.02)",
};

const statsGridStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
  gap: 10,
};

const statCardStyle: React.CSSProperties = {
  border: "1px solid var(--line)",
  borderRadius: 12,
  padding: "10px 12px",
  background: "rgba(255,255,255,0.02)",
};

const statLabelStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  color: "var(--text-faint)",
  textTransform: "uppercase",
  letterSpacing: "0.08em",
};

const statValueStyle: React.CSSProperties = {
  marginTop: 4,
  fontSize: 15,
  fontWeight: 700,
  color: "var(--text)",
};

const thresholdWrapStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
  gap: 10,
};

const takeawayGridStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
  gap: 10,
};

const takeawayStyle: React.CSSProperties = {
  borderRadius: 12,
  background: "rgba(255,255,255,0.03)",
  border: "1px solid var(--line)",
  padding: "10px 12px",
  fontSize: 12,
  fontWeight: 700,
  color: "var(--text-soft)",
};

const thresholdCardStyle: React.CSSProperties = {
  border: "1px solid var(--line)",
  borderRadius: 12,
  padding: "10px 12px",
  background: "rgba(255,255,255,0.02)",
};

const thresholdHeaderStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  gap: 8,
  fontSize: 13,
  color: "var(--text)",
};

const thresholdNoteStyle: React.CSSProperties = {
  marginTop: 6,
  fontSize: 12,
  lineHeight: 1.45,
  color: "var(--text-soft)",
};

const metaGridStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
  gap: 10,
};

const metaItemStyle: React.CSSProperties = {
  display: "grid",
  gap: 4,
  fontSize: 12,
  color: "var(--text-soft)",
};
