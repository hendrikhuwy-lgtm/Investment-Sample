import type {
  ChartBandContract,
  ChartCalloutContract,
  ChartLogicContract,
  ChartMarkerContract,
  ChartPanelContract,
  ChartPointContract,
  ChartSeriesContract,
  ChartThresholdContract,
} from "../../../shared/v2_surface_contracts";

import type {
  ChartBandDisplay,
  ChartCalloutDisplay,
  ChartLogicDisplay,
  ChartMarkerDisplay,
  ChartPanelDisplay,
  ChartPointDisplay,
  ChartSeriesDisplay,
  ChartThresholdDisplay,
  DecompositionBarDisplay,
} from "./chartTypes";

function humanize(value: string | null | undefined): string {
  return String(value || "")
    .replace(/[_-]+/g, " ")
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase()) || "Unavailable";
}

function mapPoint(point: ChartPointContract): ChartPointDisplay {
  return {
    time: point.timestamp,
    value: point.value,
  };
}

function mapSeries(series: ChartSeriesContract | null | undefined): ChartSeriesDisplay | null {
  if (!series) return null;
  return {
    id: series.chart_id,
    type: series.series_type,
    label: series.label,
    points: series.points.map(mapPoint),
    unit: series.unit,
    sourceFamily: humanize(series.source_family),
    sourceLabel: series.source_label,
    freshnessLabel: humanize(series.freshness_state),
    trustLabel: humanize(series.trust_state),
  };
}

function mapBand(band: ChartBandContract): ChartBandDisplay {
  return {
    id: band.band_id,
    label: band.label,
    upper: band.upper_points.map(mapPoint),
    lower: band.lower_points.map(mapPoint),
    meaning: humanize(band.meaning),
    degradedLabel: band.degraded_state ? humanize(band.degraded_state) : null,
  };
}

function mapMarker(marker: ChartMarkerContract): ChartMarkerDisplay {
  return {
    id: marker.marker_id,
    time: marker.timestamp,
    label: marker.label,
    type: marker.marker_type,
    summary: marker.summary,
  };
}

function mapThreshold(threshold: ChartThresholdContract): ChartThresholdDisplay {
  return {
    id: threshold.threshold_id,
    label: threshold.label,
    value: threshold.value,
    type: threshold.threshold_type,
    actionIfCrossed: threshold.action_if_crossed,
    whatItMeans: threshold.what_it_means,
  };
}

function mapCallout(callout: ChartCalloutContract): ChartCalloutDisplay {
  return {
    id: callout.callout_id,
    label: callout.label,
    tone: callout.tone,
    detail: callout.detail,
  };
}

function adaptChartLogic(logic: ChartLogicContract): ChartLogicDisplay {
  return {
    currentValue: logic.current_value ?? null,
    previousValue: logic.previous_value ?? null,
    triggerLevel: logic.trigger_level ?? null,
    confirmAbove: logic.confirm_above ?? null,
    breakBelow: logic.break_below ?? null,
    bands: (logic.bands ?? []).map((b) => ({ label: b.label, min: b.min ?? null, max: b.max ?? null })),
    currentBand: logic.current_band ?? null,
    releaseDate: logic.release_date ?? null,
    asOfDate: logic.as_of_date ?? "",
    allocationBars: (logic.allocation_bars ?? []).map((b): DecompositionBarDisplay => ({
      label: String(b.label),
      value: Number(b.value),
      target: Number(b.target),
      low: Number(b.low),
      high: Number(b.high),
      unit: String(b.unit ?? "pct"),
    })),
  };
}

function resolveMode(panel: ChartPanelContract): string {
  const m = panel.chart_mode ?? 'market_context';
  if (m !== 'market_context') return m;
  if (panel.chart_type === 'forecast_path')    return 'forecast';
  if (panel.chart_type === 'comparison_line')  return 'comparison';
  if (panel.chart_type === 'snapshot_compare') return 'decomposition';
  return 'comparison';
}

export function adaptChartPanel(panel: ChartPanelContract): ChartPanelDisplay {
  return {
    id: panel.panel_id,
    title: panel.title,
    chartType: panel.chart_type,
    chartMode: panel.chart_mode ?? "market_context",
    inferredMode: resolveMode(panel),
    chartLogic: panel.chart_logic ? adaptChartLogic(panel.chart_logic) : null,
    primarySeries: mapSeries(panel.primary_series),
    comparisonSeries: mapSeries(panel.comparison_series ?? null),
    bands: (panel.bands ?? []).map(mapBand),
    markers: (panel.markers ?? []).map(mapMarker),
    thresholds: (panel.thresholds ?? []).map(mapThreshold),
    callouts: (panel.callouts ?? []).map(mapCallout),
    summary: panel.summary,
    whatToNotice: panel.what_to_notice,
    freshnessLabel: humanize(panel.freshness_state),
    trustLabel: humanize(panel.trust_state),
    degradedLabel: panel.degraded_state ? humanize(panel.degraded_state) : null,
    hideTimeScale: panel.chart_type === "snapshot_compare",
  };
}

export function adaptChartPanels(panels: ChartPanelContract[] | null | undefined): ChartPanelDisplay[] {
  return (panels ?? []).map(adaptChartPanel);
}
