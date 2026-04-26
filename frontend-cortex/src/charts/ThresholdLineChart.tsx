import { useEffect, useRef, useState, type SyntheticEvent } from "react";
import * as echarts from "echarts";
import type {
  DailyBriefChartHoverPayload,
  DailyBriefChartPayload,
  DailyBriefChartPoint,
  DailyBriefChartThresholdLineSpec,
} from "../../../shared/v2_surface_contracts";

type Props = {
  payload: DailyBriefChartPayload;
  height?: number;
  visibleLayerIds?: string[];
  focusedLayerId?: string | null;
  focusMode?: boolean;
  focusGroupId?: string | null;
};

type Palette = ReturnType<typeof themePalette>;
type InspectionState = {
  timestamp: string;
  observedValue: number | null;
  forecastValue: number | null;
  forecastLabel: string | null;
  reviewBand: { label: string; min: number; max: number } | null;
  thresholds: Array<{ id: string; label: string; value: number }>;
  relations: string[];
  implication: string | null;
  x: number;
  y: number;
};

function hexToRgba(hex: string, alpha: number) {
  const normalized = hex.replace("#", "");
  if (normalized.length !== 6) return `rgba(255,255,255,${alpha})`;
  const value = Number.parseInt(normalized, 16);
  const r = (value >> 16) & 255;
  const g = (value >> 8) & 255;
  const b = value & 255;
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function formatAxisDate(timestamp: string) {
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) return timestamp;
  return parsed.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function formatTooltipDate(timestamp: string) {
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) return timestamp;
  return parsed.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function formatValue(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return "n/a";
  const absolute = Math.abs(value);
  if (absolute >= 1000) return value.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 2 });
  return value.toFixed(2);
}

function themePalette(theme: string | null | undefined) {
  switch (theme) {
    case "rates":
      return {
        line: "#d7c28a",
        area: "rgba(215, 194, 138, 0.08)",
        current: "#f7f0dc",
        forecast: "#9fadc4",
        forecastFill: "rgba(159, 173, 196, 0.08)",
        divider: "rgba(174, 186, 204, 0.36)",
        review: "#b99262",
        confirm: "#8ebf9c",
        break: "#c77970",
        stall: "#d1af71",
        inspect: "#fff6de",
      };
    case "credit":
      return {
        line: "#d5b39d",
        area: "rgba(213, 179, 157, 0.08)",
        current: "#f5e7dc",
        forecast: "#9aaec2",
        forecastFill: "rgba(154, 174, 194, 0.08)",
        divider: "rgba(170, 184, 201, 0.36)",
        review: "#bb8b66",
        confirm: "#8fa7c4",
        break: "#cb7b74",
        stall: "#d0aa6f",
        inspect: "#fff0e5",
      };
    case "breadth":
      return {
        line: "#9fc7b2",
        area: "rgba(159, 199, 178, 0.09)",
        current: "#eef8f1",
        forecast: "#a6b6d0",
        forecastFill: "rgba(166, 182, 208, 0.08)",
        divider: "rgba(181, 192, 213, 0.36)",
        review: "#a38d68",
        confirm: "#7fb896",
        break: "#c66c63",
        stall: "#d5b06d",
        inspect: "#f2fff7",
      };
    case "fx":
      return {
        line: "#9eb7d7",
        area: "rgba(158, 183, 215, 0.09)",
        current: "#eef4fb",
        forecast: "#b9c8db",
        forecastFill: "rgba(185, 200, 219, 0.08)",
        divider: "rgba(184, 196, 214, 0.36)",
        review: "#8da6c7",
        confirm: "#78a28c",
        break: "#cb7b74",
        stall: "#c9a870",
        inspect: "#f4f9ff",
      };
    case "commodity":
      return {
        line: "#dca874",
        area: "rgba(220, 168, 116, 0.10)",
        current: "#fbf1e6",
        forecast: "#a7b4c4",
        forecastFill: "rgba(167, 180, 196, 0.09)",
        divider: "rgba(185, 194, 204, 0.36)",
        review: "#d0a167",
        confirm: "#c97f59",
        break: "#a86c63",
        stall: "#d7b16f",
        inspect: "#fff7ee",
      };
    default:
      return {
        line: "#c5c2b9",
        area: "rgba(197, 194, 185, 0.08)",
        current: "#f0ede5",
        forecast: "#a9b2c0",
        forecastFill: "rgba(169, 178, 192, 0.08)",
        divider: "rgba(176, 186, 199, 0.34)",
        review: "#a78f6b",
        confirm: "#7ca890",
        break: "#bf746d",
        stall: "#c8a86f",
        inspect: "#faf6ec",
      };
  }
}

function gridForDensity(density: string | null | undefined) {
  if (density === "compact_line") {
    return { left: 44, right: 20, top: 14, bottom: 24 };
  }
  return { left: 46, right: 22, top: 18, bottom: 28 };
}

function pointMap(points: DailyBriefChartPoint[] | null | undefined) {
  return new Map((points ?? []).map((point) => [point.timestamp, point] as const));
}

function orderedCategories(history: DailyBriefChartPoint[], forecast: DailyBriefChartPoint[]) {
  const categories = history.map((point) => point.timestamp);
  for (const point of forecast) {
    if (!categories.includes(point.timestamp)) categories.push(point.timestamp);
  }
  return categories;
}

function tooltipPosition(point: number[], _params: unknown, _dom: unknown, _rect: unknown, size: { contentSize: number[]; viewSize: number[] }) {
  const [contentWidth, contentHeight] = size.contentSize;
  const [viewWidth, viewHeight] = size.viewSize;
  const x = point[0] > viewWidth * 0.66
    ? Math.max(10, point[0] - contentWidth - 16)
    : Math.min(point[0] + 14, viewWidth - contentWidth - 10);
  const y = Math.max(12, Math.min(point[1] - contentHeight - 12, viewHeight - contentHeight - 10));
  return [x, y];
}

function lineStyleForThreshold(line: DailyBriefChartThresholdLineSpec, palette: Palette, focused: boolean, muted: boolean) {
  let color = palette.confirm;
  let type: "solid" | "dashed" | "dotted" = "solid";
  let width = line.priority <= 1 ? 1.65 : 1.2;
  if (focused) width += 0.35;
  if (line.render_mode === "dashed_line" || line.semantic_role === "fade_line" || line.semantic_role === "break_line") {
    color = palette.break;
    type = "dashed";
  } else if (line.semantic_role === "stall_line") {
    color = palette.stall;
    type = "dotted";
  } else if (line.semantic_role === "review_line") {
    color = palette.review;
    type = "dashed";
    width = focused ? 1.45 : 1;
  }
  return {
    color,
    type,
    width,
    opacity: muted ? 0.34 : focused ? 0.98 : line.priority <= 1 ? 0.84 : 0.7,
  };
}

function visibleHoverReferenceValues(
  hoverPayload: DailyBriefChartHoverPayload | undefined,
  visibleThresholdLines: DailyBriefChartThresholdLineSpec[],
) {
  const allowed = new Set(visibleThresholdLines.map((line) => line.threshold_id));
  return (hoverPayload?.reference_values ?? []).filter((item) => allowed.has(item.threshold_id));
}

function visibleHoverRelations(
  hoverPayload: DailyBriefChartHoverPayload | undefined,
  visibleThresholdLines: DailyBriefChartThresholdLineSpec[],
  showReviewBand: boolean,
) {
  const allowed = new Set(visibleThresholdLines.map((line) => line.threshold_id));
  return (hoverPayload?.relation_statements ?? [])
    .filter((item) => {
      const thresholdId = item.threshold_id ?? null;
      if (thresholdId === "review_band") return showReviewBand;
      if (!thresholdId) return true;
      return allowed.has(thresholdId);
    })
    .sort((left, right) => left.priority - right.priority)
    .slice(0, 2)
    .map((item) => item.statement);
}

function tooltipHtml(
  payload: DailyBriefChartPayload,
  timestamp: string,
  hoverPayload: DailyBriefChartHoverPayload | undefined,
  visibleThresholdLines: DailyBriefChartThresholdLineSpec[],
  showReviewBand: boolean,
) {
  const reviewBand = showReviewBand ? (hoverPayload?.review_band ?? payload.review_context ?? payload.review_band ?? null) : null;
  const thresholdRows = visibleHoverReferenceValues(hoverPayload, visibleThresholdLines)
    .map((line) => `<div class="brief-chart-tooltip-row"><span>${line.label}</span><strong>${formatValue(line.value)}</strong></div>`)
    .join("");
  const relationTags = visibleHoverRelations(hoverPayload, visibleThresholdLines, showReviewBand);
  const implication = hoverPayload?.implication ?? payload.current_implication_label ?? payload.chart_takeaway ?? null;

	  return `
	    <div class="brief-chart-tooltip">
	      <div class="brief-chart-tooltip-date">${formatTooltipDate(timestamp)}</div>
	      ${hoverPayload?.observed_value != null ? `<div class="brief-chart-tooltip-row"><span>Observed</span><strong>${formatValue(hoverPayload.observed_value)}</strong></div>` : ""}
	      ${hoverPayload?.forecast_value != null ? `<div class="brief-chart-tooltip-row"><span>${hoverPayload.observed_value != null ? "Forecast next" : "Forecast"}</span><strong>${formatValue(hoverPayload.forecast_value)}</strong></div>` : ""}
	      ${showReviewBand && reviewBand ? `<div class="brief-chart-tooltip-row"><span>${reviewBand.label}</span><strong>${formatValue(reviewBand.min)}–${formatValue(reviewBand.max)}</strong></div>` : ""}
	      ${thresholdRows}
	      ${relationTags.map((tag) => `<div class="brief-chart-tooltip-tag">${tag}</div>`).join("")}
	      ${implication ? `<div class="brief-chart-tooltip-implication">${implication}</div>` : ""}
	    </div>
	  `;
}

function buildInspectionState(
  payload: DailyBriefChartPayload,
  timestamp: string,
  hoverPayload: DailyBriefChartHoverPayload | undefined,
  visibleThresholdLines: DailyBriefChartThresholdLineSpec[],
  showReviewBand: boolean,
  pointer: { x: number; y: number },
): InspectionState {
  const reviewSource = hoverPayload?.review_band ?? payload.review_context ?? payload.review_band;
  const reviewBand = showReviewBand && reviewSource
    ? { label: reviewSource.label, min: reviewSource.min, max: reviewSource.max }
    : null;
  const relations = visibleHoverRelations(hoverPayload, visibleThresholdLines, showReviewBand);
  const implication = hoverPayload?.implication ?? payload.current_implication_label ?? payload.chart_takeaway ?? null;

  return {
    timestamp,
    observedValue: hoverPayload?.observed_value ?? null,
    forecastValue: hoverPayload?.forecast_value ?? null,
    forecastLabel: hoverPayload?.forecast_value != null ? (hoverPayload.observed_value != null ? "Forecast next" : "Forecast") : null,
    reviewBand,
    thresholds: visibleHoverReferenceValues(hoverPayload, visibleThresholdLines).map((line) => ({ id: line.threshold_id, label: line.label, value: line.value })),
    relations,
    implication,
    x: pointer.x,
    y: pointer.y,
  };
}

function tooltipTextSummary(
  payload: DailyBriefChartPayload,
  timestamp: string,
  hoverPayload: DailyBriefChartHoverPayload | undefined,
  visibleThresholdLines: DailyBriefChartThresholdLineSpec[],
  showReviewBand: boolean,
) {
  const reviewBand = showReviewBand ? (hoverPayload?.review_band ?? payload.review_context ?? payload.review_band ?? null) : null;

  const lines = [formatTooltipDate(timestamp)];
  if (hoverPayload?.observed_value != null) lines.push(`Observed ${formatValue(hoverPayload.observed_value)}`);
  if (hoverPayload?.forecast_value != null) lines.push(`${hoverPayload.observed_value != null ? "Forecast next" : "Forecast"} ${formatValue(hoverPayload.forecast_value)}`);
  if (showReviewBand && reviewBand) lines.push(`${reviewBand.label} ${formatValue(reviewBand.min)}–${formatValue(reviewBand.max)}`);
  for (const line of visibleHoverReferenceValues(hoverPayload, visibleThresholdLines)) {
    lines.push(`${line.label} ${formatValue(line.value)}`);
  }
  const relationTags = visibleHoverRelations(hoverPayload, visibleThresholdLines, showReviewBand);
  const implication = hoverPayload?.implication ?? payload.current_implication_label ?? payload.chart_takeaway ?? null;

  return [...lines, ...relationTags, ...(implication ? [implication] : [])].join(" | ");
}

function seriesOpacity(layerId: string, focusedLayerId: string | null, emphasis = 1) {
  if (!focusedLayerId) return emphasis;
  return layerId === focusedLayerId ? emphasis : Math.max(0.26, emphasis * 0.42);
}

export function ThresholdLineChart({ payload, height = 230, visibleLayerIds, focusedLayerId = null, focusMode = false, focusGroupId = null }: Props) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [inspectionText, setInspectionText] = useState("");
  const [inspectionState, setInspectionState] = useState<InspectionState | null>(null);
  const [inspectionPinned, setInspectionPinned] = useState(false);

  useEffect(() => {
    if (!rootRef.current) return;
    const observedSeries = payload.observed_path ?? payload.observed_series ?? payload.primary_series;
    if (!observedSeries?.points?.length) return;

    const chart = echarts.init(rootRef.current, undefined, { renderer: "canvas" });
    const density = payload.chart_density_profile ?? "rich_line";
    const palette = themePalette(payload.chart_theme);
    const historyPoints = observedSeries.points ?? [];
    const forecastPath = payload.forecast_path ?? payload.forecast_series;
    const forecastPoints = forecastPath?.points ?? payload.forecast_overlay?.point_path ?? [];
    const forecastLower = payload.forecast_path?.forecast_confidence_band?.lower_band ?? payload.forecast_overlay?.lower_band ?? [];
    const forecastUpper = payload.forecast_path?.forecast_confidence_band?.upper_band ?? payload.forecast_overlay?.upper_band ?? [];
    const historyLookup = pointMap(historyPoints);
    const forecastLookup = pointMap(forecastPoints);
    const hoverLookup = new Map((payload.hover_payload_by_timestamp ?? []).map((point) => [point.timestamp, point] as const));
    const lowerLookup = pointMap(forecastLower);
    const upperLookup = pointMap(forecastUpper);
    const forecastStartTimestamp = payload.forecast_path?.forecast_start_timestamp ?? payload.forecast_overlay?.forecast_start_timestamp ?? forecastPoints[0]?.timestamp ?? null;
    const forecastVisibilityMode = payload.forecast_visibility_mode ?? (focusMode ? "emphasized" : "contextual");
    const lastHistoryTimestamp = historyPoints[historyPoints.length - 1]?.timestamp ?? null;
    const combinedTimestamps = orderedCategories(historyPoints, forecastPoints);
    const xLabelInterval = Math.max(0, Math.floor((combinedTimestamps.length - 1) / (density === "compact_line" ? 3 : 5)));
    const visibleIds = new Set(visibleLayerIds ?? []);
    const activeFocusGroup = (payload.focusable_threshold_groups ?? []).find((group) => group.group_id === focusGroupId) ?? null;
    const activeFocusMode = (payload.focus_modes ?? []).find((mode) => mode.mode_id === (focusMode ? focusGroupId : "overview")) ?? null;
    const focusVisibleIds = new Set((focusMode ? (activeFocusMode?.visible_object_ids ?? activeFocusGroup?.member_line_ids ?? visibleLayerIds) : visibleLayerIds) ?? []);
    const reviewBand = payload.review_context ?? payload.review_band ?? null;
    const thresholdLines = payload.decision_references ?? payload.threshold_lines ?? [];
    const decisionZoneVisible = !focusMode && payload.threshold_overlap_mode === "merge_to_zone" && visibleIds.has("decision_zone");
    const showReviewBand = Boolean(reviewBand && (focusMode ? focusVisibleIds.has(reviewBand.band_id) : visibleIds.has(reviewBand.band_id)));
    const visibleThresholdLines = thresholdLines.filter((line) => (focusMode ? focusVisibleIds.has(line.threshold_id) : visibleIds.has(line.threshold_id)));
    const tooltipThresholdLines = visibleThresholdLines;
    const renderableThresholdLines = focusMode
      ? visibleThresholdLines
      : visibleThresholdLines.filter(
          (line) => line.render_mode !== "legend_only" && line.render_mode !== "merged_zone",
        );
    const activeYDomain = focusMode ? activeFocusMode?.y_domain ?? activeFocusGroup?.suggested_y_domain ?? payload.focus_y_domain ?? null : null;

    const historySeriesData = combinedTimestamps.map((timestamp) => historyLookup.get(timestamp)?.value ?? null);
    const forecastSeriesData = combinedTimestamps.map((timestamp) => {
      if (!forecastPoints.length) return null;
      if (timestamp === lastHistoryTimestamp && historyLookup.has(timestamp)) {
        return historyLookup.get(timestamp)?.value ?? null;
      }
      return forecastLookup.get(timestamp)?.value ?? null;
    });
    const lowerSeriesData = combinedTimestamps.map((timestamp) => lowerLookup.get(timestamp)?.value ?? null);
    const upperSeriesData = combinedTimestamps.map((timestamp) => upperLookup.get(timestamp)?.value ?? null);
    const forecastLastTimestamp = forecastPoints[forecastPoints.length - 1]?.timestamp ?? null;
    const decisionZone = (payload.threshold_overlap_mode === "merge_to_zone" ? payload.thresholds?.trigger_zone : null) ?? null;

    const thresholdSeries = renderableThresholdLines.map((line) => {
      const focused = focusedLayerId === line.threshold_id;
      const muted = Boolean(focusedLayerId && focusedLayerId !== line.threshold_id);
      return {
        id: line.threshold_id,
        name: line.label,
        type: "line",
        symbol: "none",
        data: combinedTimestamps.map(() => line.numeric_value),
        z: 2,
        animation: false,
        tooltip: { show: false },
        lineStyle: lineStyleForThreshold(line, palette, focused, muted),
      };
    });

    const seriesEntries = [
      showReviewBand && reviewBand
        ? {
            id: reviewBand.band_id,
            type: "line",
            data: combinedTimestamps.map(() => null),
            symbol: "none",
            tooltip: { show: false },
            z: 0,
            lineStyle: { opacity: 0 },
            markArea: {
              silent: true,
              itemStyle: { color: hexToRgba(palette.review, focusedLayerId === reviewBand.band_id ? 0.13 : 0.08) },
              data: [[{ yAxis: reviewBand.min }, { yAxis: reviewBand.max }]],
            },
          }
        : null,
      decisionZoneVisible && decisionZone?.min != null && decisionZone?.max != null
        ? {
            id: "decision_zone",
            type: "line",
            data: combinedTimestamps.map(() => null),
            symbol: "none",
            tooltip: { show: false },
            z: 1,
            lineStyle: { opacity: 0 },
            markArea: {
              silent: true,
              itemStyle: { color: hexToRgba(palette.confirm, focusedLayerId === "decision_zone" ? 0.12 : 0.06) },
              data: [[{ yAxis: decisionZone.min }, { yAxis: decisionZone.max }]],
            },
          }
        : null,
      ...thresholdSeries,
      {
        id: observedSeries.series_id,
        name: observedSeries.label,
        type: "line",
        smooth: density === "rich_line",
        showSymbol: false,
        data: historySeriesData,
        z: 5,
	      lineStyle: {
	        color: palette.line,
	        width: focusedLayerId === observedSeries.series_id ? (focusMode ? 3.8 : 3.45) : density === "compact_line" ? 3.05 : 3.2,
	        cap: "round",
	        join: "round",
	        opacity: seriesOpacity(observedSeries.series_id, focusedLayerId, 1),
        },
        areaStyle: density === "rich_line" ? { color: palette.area, opacity: seriesOpacity(observedSeries.series_id, focusedLayerId, 1) } : undefined,
      },
	        (focusMode ? focusVisibleIds.has(forecastPath?.series_id ?? "forecast") : visibleIds.has(forecastPath?.series_id ?? "forecast")) && forecastStartTimestamp && forecastLastTimestamp
        ? {
            id: "forecast-zone",
            type: "line",
            data: combinedTimestamps.map(() => null),
            symbol: "none",
            tooltip: { show: false },
            z: 1,
            lineStyle: { opacity: 0 },
            markArea: {
              silent: true,
	                itemStyle: { color: hexToRgba(palette.forecast, focusMode ? 0.13 : 0.085) || palette.forecastFill },
              data: [[{ xAxis: forecastStartTimestamp }, { xAxis: forecastLastTimestamp }]],
            },
          }
        : null,
	        (focusMode ? focusVisibleIds.has(forecastPath?.series_id ?? "forecast") : visibleIds.has(forecastPath?.series_id ?? "forecast")) && forecastStartTimestamp
        ? {
            id: "forecast-divider",
            type: "line",
            data: combinedTimestamps.map(() => null),
            symbol: "none",
            tooltip: { show: false },
            z: 2,
            lineStyle: { opacity: 0 },
            markLine: {
              silent: true,
              symbol: "none",
              data: [
                {
                  xAxis: forecastStartTimestamp,
	                    lineStyle: { color: palette.divider, type: "dashed", width: focusMode ? 1.8 : 1.45, opacity: focusedLayerId === (forecastPath?.series_id ?? "forecast") ? 1 : 0.88 },
                  label: { show: false },
                },
              ],
            },
          }
        : null,
	        (focusMode ? focusVisibleIds.has(forecastPath?.series_id ?? "forecast") : visibleIds.has(forecastPath?.series_id ?? "forecast")) && forecastPoints.length
        ? {
            id: forecastPath?.series_id ?? "forecast",
            name: forecastPath?.label ?? "Forecast",
            type: "line",
            smooth: true,
            connectNulls: false,
            showSymbol: false,
            data: forecastSeriesData,
            z: 4,
	              lineStyle: {
	                color: palette.forecast,
	                width:
                  focusedLayerId === (forecastPath?.series_id ?? "forecast")
                    ? (focusMode ? 3.5 : 3.0)
                    : forecastVisibilityMode === "emphasized"
                      ? (focusMode ? 3.15 : 2.9)
                      : focusMode
                        ? 3.05
                        : density === "compact_line"
                          ? 2.6
                          : 2.75,
	                type: "dashed",
	                opacity: seriesOpacity(
                  forecastPath?.series_id ?? "forecast",
                  focusedLayerId,
                  forecastVisibilityMode === "emphasized" ? 1 : focusMode ? 1 : 0.98,
                ),
	              },
	            }
	          : null,
	        (focusMode ? focusVisibleIds.has(forecastPath?.series_id ?? "forecast") : visibleIds.has(forecastPath?.series_id ?? "forecast")) && forecastLower.length && density === "rich_line"
        ? {
            id: "forecast-lower",
            type: "line",
            symbol: "none",
            data: lowerSeriesData,
            z: 2,
            tooltip: { show: false },
            lineStyle: { color: "rgba(157, 170, 190, 0.42)", width: 0.95, type: "dotted" },
          }
        : null,
	        (focusMode ? focusVisibleIds.has(forecastPath?.series_id ?? "forecast") : visibleIds.has(forecastPath?.series_id ?? "forecast")) && forecastUpper.length && density === "rich_line"
        ? {
            id: "forecast-upper",
            type: "line",
            symbol: "none",
            data: upperSeriesData,
            z: 2,
            tooltip: { show: false },
            lineStyle: { color: "rgba(157, 170, 190, 0.42)", width: 0.95, type: "dotted" },
          }
        : null,
      payload.current_point
        ? {
            id: "current-point",
            type: "scatter",
            symbolSize: density === "compact_line" ? 16 : 18,
            z: 7,
            tooltip: { show: false },
            itemStyle: {
              color: palette.current,
              borderColor: palette.line,
              borderWidth: 2.3,
              shadowBlur: 15,
              shadowColor: hexToRgba(palette.line, 0.3),
            },
            data: [[lastHistoryTimestamp, payload.current_point.value]],
          }
        : null,
	        (focusMode ? focusVisibleIds.has(forecastPath?.series_id ?? "forecast") : visibleIds.has(forecastPath?.series_id ?? "forecast")) && forecastStartTimestamp
        ? {
            id: "forecast-start",
            type: "scatter",
            symbolSize: 10,
            z: 6,
            tooltip: { show: false },
            itemStyle: {
              color: "#0d1117",
              borderColor: palette.forecast,
              borderWidth: 1.9,
              shadowBlur: 10,
              shadowColor: hexToRgba(palette.forecast, 0.24),
            },
            data: forecastLookup.has(forecastStartTimestamp)
              ? [[forecastStartTimestamp, forecastLookup.get(forecastStartTimestamp)?.value]]
              : [],
          }
        : null,
      {
        id: "hover-history",
        type: "scatter",
        symbolSize: density === "compact_line" ? 11 : 12,
        z: 8,
        tooltip: { show: false },
        itemStyle: {
          color: palette.inspect,
          borderColor: palette.line,
          borderWidth: 1.9,
          shadowBlur: 10,
          shadowColor: hexToRgba(palette.line, 0.22),
        },
        data: [],
      },
      {
        id: "hover-forecast",
        type: "scatter",
        symbolSize: 10,
        z: 8,
        tooltip: { show: false },
        itemStyle: {
          color: "#10151d",
          borderColor: palette.forecast,
          borderWidth: 1.8,
          shadowBlur: 10,
          shadowColor: hexToRgba(palette.forecast, 0.18),
        },
        data: [],
      },
      {
        id: "capture-history",
        type: "scatter",
        symbolSize: density === "compact_line" ? 18 : 20,
        z: 1,
        silent: true,
        tooltip: { show: false },
        itemStyle: { color: "rgba(255,255,255,0.001)" },
        data: historyPoints.map((point) => [point.timestamp, point.value]),
      },
      {
        id: "capture-forecast",
        type: "scatter",
        symbolSize: 18,
        z: 1,
        silent: true,
        tooltip: { show: false },
        itemStyle: { color: "rgba(255,255,255,0.001)" },
        data: forecastPoints.map((point) => [point.timestamp, point.value]),
      },
    ].filter(Boolean);

    const option = {
      animation: false,
      grid: gridForDensity(density),
      tooltip: {
        trigger: "axis",
        triggerOn: "mousemove|click",
        showContent: false,
        transitionDuration: 0,
        confine: true,
        backgroundColor: "rgba(10, 13, 18, 0.98)",
        borderColor: "rgba(229, 223, 212, 0.12)",
        borderWidth: 1,
        padding: 0,
        className: "brief-chart-tooltip-shell",
        extraCssText: "box-shadow: 0 12px 28px rgba(0,0,0,0.28); border-radius: 12px;",
        axisPointer: {
          type: "line",
          snap: true,
          lineStyle: { color: "rgba(238, 233, 224, 0.32)", width: 1.2 },
          label: { show: false },
        },
        position: tooltipPosition,
        formatter: (params: any) => {
          const list = Array.isArray(params) ? params : [params];
          const first = list[0];
          const rawAxis = first?.axisValue;
          const timestamp = typeof rawAxis === "number" ? combinedTimestamps[rawAxis] : String(rawAxis ?? "");
          if (!timestamp) return "";
          return tooltipHtml(
            payload,
            timestamp,
            hoverLookup.get(timestamp),
            tooltipThresholdLines,
            showReviewBand,
          );
        },
      },
      xAxis: {
        type: "category",
        data: combinedTimestamps,
        boundaryGap: false,
        axisLine: { lineStyle: { color: "rgba(255,255,255,0.09)" } },
        axisTick: { show: false },
        axisLabel: {
          color: "#8f948e",
          fontSize: 10,
          margin: 10,
          interval: xLabelInterval,
          formatter: (value: string) => formatAxisDate(value),
        },
        axisPointer: {
          show: true,
          snap: true,
          label: { show: false },
        },
      },
      yAxis: {
        type: "value",
        scale: true,
        min: activeYDomain?.min,
        max: activeYDomain?.max,
        splitNumber: density === "compact_line" ? 3 : 4,
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { lineStyle: { color: "rgba(255,255,255,0.045)" } },
        axisLabel: { color: "#8f948e", fontSize: 10, margin: 8 },
      },
      series: seriesEntries,
    };

    chart.setOption(option, true);
    const seriesIds = seriesEntries.map((entry: any) => String(entry.id ?? "")).filter(Boolean);

    const updateInspection = (axisValue: string | number | null | undefined) => {
      const timestamp =
        typeof axisValue === "number" ? combinedTimestamps[axisValue] : typeof axisValue === "string" ? axisValue : null;
      if (!timestamp) {
        chart.setOption({ series: [{ id: "hover-history", data: [] }, { id: "hover-forecast", data: [] }] }, false);
        return;
      }
      const observed = historyLookup.get(timestamp);
      const forecast = forecastLookup.get(timestamp);
      chart.setOption(
        {
          series: [
            { id: "hover-history", data: observed ? [[timestamp, observed.value]] : [] },
            { id: "hover-forecast", data: forecast ? [[timestamp, forecast.value]] : [] },
          ],
        },
        false,
      );
    };

    const nearestTimestampFromX = (offsetX: number) => {
      const positions = combinedTimestamps
        .map((timestamp, index) => ({ timestamp, index, x: Number(chart.convertToPixel({ xAxisIndex: 0 }, timestamp)) }))
        .filter((item) => Number.isFinite(item.x));
      if (!positions.length) return null;
      return positions.reduce((best, item) => (
        Math.abs(item.x - offsetX) < Math.abs(best.x - offsetX) ? item : best
      ));
    };

    const showInspectionAt = (timestamp: string | null, pointer?: { x: number; y: number }) => {
      if (!timestamp) {
        chart.dispatchAction({ type: "hideTip" });
        chart.setOption({ series: [{ id: "hover-history", data: [] }, { id: "hover-forecast", data: [] }] }, false);
        setInspectionText("");
        if (!inspectionPinned) setInspectionState(null);
        return;
      }
      updateInspection(timestamp);
      const observed = historyLookup.get(timestamp);
      const forecast = forecastLookup.get(timestamp);
      const hoverPayload = hoverLookup.get(timestamp);
      const fallbackYValue = observed?.value ?? forecast?.value ?? payload.current_point?.value ?? 0;
      const fallbackPointer = pointer ?? {
        x: Number(chart.convertToPixel({ xAxisIndex: 0 }, timestamp)),
        y: typeof fallbackYValue === "number"
          ? Number(chart.convertToPixel({ yAxisIndex: 0 } as any, fallbackYValue as any))
          : 18,
      };
      const nextInspectionState = buildInspectionState(
        payload,
        timestamp,
        hoverPayload,
        tooltipThresholdLines,
        showReviewBand,
        fallbackPointer,
      );
      setInspectionText(
        tooltipTextSummary(
          payload,
          timestamp,
          hoverPayload,
          tooltipThresholdLines,
          showReviewBand,
        ),
      );
      setInspectionState(pointer || inspectionPinned ? nextInspectionState : null);
      const inspectedValue = hoverPayload?.observed_value ?? hoverPayload?.forecast_value ?? observed?.value ?? forecast?.value ?? payload.current_point?.value ?? null;
      const x = Number(chart.convertToPixel({ xAxisIndex: 0 }, timestamp));
      const y = typeof inspectedValue === "number"
        ? Number(chart.convertToPixel({ yAxisIndex: 0 }, inspectedValue))
        : pointer?.y ?? 18;
      const preferredSeriesId =
        observed?.value != null
          ? observedSeries.series_id
          : forecast?.value != null
            ? (forecastPath?.series_id ?? "forecast")
            : "current-point";
      const seriesIndex = seriesIds.indexOf(preferredSeriesId);
      const dataIndex = combinedTimestamps.indexOf(timestamp);
      if (Number.isFinite(x) && Number.isFinite(y) && dataIndex >= 0) {
        chart.dispatchAction({
          type: "showTip",
          x: pointer?.x ?? x,
          y: pointer?.y ?? y,
          ...(seriesIndex >= 0 ? { seriesIndex, dataIndex } : {}),
        });
      }
    };

    chart.on("updateAxisPointer", (event: any) => {
      const axisInfo = event?.axesInfo?.[0];
      updateInspection(axisInfo?.value);
    });

    const eventPosition = (event: { offsetX?: number; offsetY?: number; zrX?: number; zrY?: number }) => {
      const x = Number(event.offsetX ?? event.zrX);
      const y = Number(event.offsetY ?? event.zrY);
      if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
      return { x, y };
    };

    const onMouseMove = (event: { offsetX?: number; offsetY?: number; zrX?: number; zrY?: number }) => {
      const position = eventPosition(event);
      if (!position) return;
      if (inspectionPinned) return;
      const point: [number, number] = [position.x, position.y];
      if (!chart.containPixel({ gridIndex: 0 }, point)) {
        return;
      }
      const nearest = nearestTimestampFromX(position.x);
      showInspectionAt(nearest?.timestamp ?? null, position);
    };

    const onClick = (event: { offsetX?: number; offsetY?: number; zrX?: number; zrY?: number }) => {
      const position = eventPosition(event);
      if (!position) return;
      const point: [number, number] = [position.x, position.y];
      if (!chart.containPixel({ gridIndex: 0 }, point)) return;
      setInspectionPinned(true);
      const nearest = nearestTimestampFromX(position.x);
      showInspectionAt(nearest?.timestamp ?? null, position);
    };

    const onMouseLeave = () => {
      chart.dispatchAction({ type: "hideTip" });
      chart.setOption({ series: [{ id: "hover-history", data: [] }, { id: "hover-forecast", data: [] }] }, false);
      if (!inspectionPinned) {
        setInspectionText("");
        setInspectionState(null);
      }
    };

    const currentInspectionTimestamp = lastHistoryTimestamp ?? forecastStartTimestamp;
    if (currentInspectionTimestamp) {
      showInspectionAt(currentInspectionTimestamp);
    }

    chart.getZr().on("mousemove", onMouseMove);
    chart.getZr().on("click", onClick);
    chart.getZr().on("globalout", onMouseLeave);
    const onResize = () => chart.resize();
    window.addEventListener("resize", onResize);

    return () => {
      chart.getZr().off("mousemove", onMouseMove);
      chart.getZr().off("click", onClick);
      chart.getZr().off("globalout", onMouseLeave);
      window.removeEventListener("resize", onResize);
      chart.dispose();
    };
  }, [payload, height, visibleLayerIds, focusedLayerId, focusMode, focusGroupId, inspectionPinned]);

  const stopEvent = (event: SyntheticEvent<HTMLDivElement>) => {
    event.stopPropagation();
  };

  const tooltipWidth = 248;
  const tooltipHeight = 156;
  const rootWidth = rootRef.current?.clientWidth ?? 0;
  const rootHeight = rootRef.current?.clientHeight ?? height;
  const tooltipLeft = inspectionState
    ? Math.max(8, Math.min(inspectionState.x + 14, Math.max(8, rootWidth - tooltipWidth - 8)))
    : 8;
  const tooltipTop = inspectionState
    ? Math.max(8, Math.min(inspectionState.y - tooltipHeight - 8, Math.max(8, rootHeight - tooltipHeight - 8)))
    : 8;

  return (
    <div
      className="brief-threshold-chart-wrap"
      onClick={stopEvent}
      onMouseDown={stopEvent}
      onPointerDown={stopEvent}
      onDoubleClick={stopEvent}
    >
      <div ref={rootRef} style={{ width: "100%", height }} />
      {inspectionState ? (
        <div className={`brief-chart-live-tooltip ${inspectionPinned ? "is-pinned" : ""}`} style={{ left: tooltipLeft, top: tooltipTop }}>
          <div className="brief-chart-tooltip">
            <div className="brief-chart-tooltip-date">{formatTooltipDate(inspectionState.timestamp)}</div>
            {inspectionState.observedValue != null ? (
              <div className="brief-chart-tooltip-row">
                <span>Observed</span>
                <strong>{formatValue(inspectionState.observedValue)}</strong>
              </div>
            ) : null}
            {inspectionState.forecastValue != null ? (
              <div className="brief-chart-tooltip-row">
                <span>{inspectionState.forecastLabel ?? "Forecast"}</span>
                <strong>{formatValue(inspectionState.forecastValue)}</strong>
              </div>
            ) : null}
            {inspectionState.reviewBand ? (
              <div className="brief-chart-tooltip-row">
                <span>{inspectionState.reviewBand.label}</span>
                <strong>{formatValue(inspectionState.reviewBand.min)}–{formatValue(inspectionState.reviewBand.max)}</strong>
              </div>
            ) : null}
            {inspectionState.thresholds.map((line) => (
              <div className="brief-chart-tooltip-row" key={line.id}>
                <span>{line.label}</span>
                <strong>{formatValue(line.value)}</strong>
              </div>
            ))}
            {inspectionState.relations.map((relation) => (
              <div className="brief-chart-tooltip-tag" key={relation}>{relation}</div>
            ))}
            {inspectionState.implication ? (
              <div className="brief-chart-tooltip-implication">{inspectionState.implication}</div>
            ) : null}
          </div>
        </div>
      ) : null}
      <div className="brief-chart-inspection-mirror" aria-live="polite">
        {inspectionText}
      </div>
    </div>
  );
}
