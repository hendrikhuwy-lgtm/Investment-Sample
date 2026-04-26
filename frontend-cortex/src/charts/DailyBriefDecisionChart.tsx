import { useEffect, useMemo, useState, type SyntheticEvent } from "react";
import type { DailyBriefChartFocusGroup, DailyBriefChartFocusMode, DailyBriefChartPayload } from "../../../shared/v2_surface_contracts";
import { SignalStrip } from "./SignalStrip";
import { ThresholdLineChart } from "./ThresholdLineChart";

type Props = {
  payload: DailyBriefChartPayload;
  height?: number;
};

function summaryToneClass(tone: string | null | undefined) {
  if (tone === "confirm" || tone === "support") return "confirm";
  if (tone === "break" || tone === "warn") return "warn";
  if (tone === "review") return "review";
  return "neutral";
}

type LayerItem = {
  id: string;
  label: string;
  tone: "neutral" | "review" | "confirm" | "break";
  style: "line" | "zone" | "series";
  visibleByDefault: boolean;
  renderMode?: string | null;
};

function buildLegendLayers(payload: DailyBriefChartPayload): LayerItem[] {
  const items: LayerItem[] = [];
  const observed = payload.observed_path ?? payload.observed_series ?? payload.primary_series;
  if (observed) {
    items.push({ id: observed.series_id, label: observed.label || "Observed", tone: "neutral", style: "series", visibleByDefault: observed.visible_by_default ?? true, renderMode: "line" });
  }
  const forecast = payload.forecast_path ?? payload.forecast_series;
  if (forecast?.points?.length || payload.forecast_overlay?.point_path?.length) {
    items.push({
      id: forecast?.series_id ?? "forecast",
      label: forecast?.label ?? payload.forecast_overlay?.forecast_label ?? "Forecast",
      tone: "confirm",
      style: "series",
      visibleByDefault: forecast?.visible_by_default ?? payload.forecast_overlay?.visible_by_default ?? true,
      renderMode: "dashed_line",
    });
  }
  const review = payload.review_context ?? payload.review_band;
  if (review) {
    items.push({
      id: review.band_id,
      label: review.label,
      tone: "review",
      style: "zone",
      visibleByDefault: review.visible_by_default,
      renderMode: "merged_zone",
    });
  }
  if (payload.threshold_overlap_mode === "merge_to_zone" && payload.thresholds?.trigger_zone?.min != null && payload.thresholds?.trigger_zone?.max != null) {
    items.push({
      id: "decision_zone",
      label: String(payload.thresholds.trigger_zone.label || "Decision zone"),
      tone: "confirm",
      style: "zone",
      visibleByDefault: true,
      renderMode: "merged_zone",
    });
  }
  for (const line of payload.decision_references ?? payload.threshold_lines ?? []) {
    if (line.legend_enabled === false) continue;
    items.push({
      id: line.threshold_id,
      label: line.label,
      tone: line.semantic_role === "fade_line" || line.semantic_role === "break_line" ? "break" : line.semantic_role === "review_line" ? "review" : "confirm",
      style: "line",
      visibleByDefault: line.visible_in_overview ?? line.visible_by_default,
      renderMode: line.render_mode,
    });
  }
  const order = payload.inspectable_series_order ?? items.map((item) => item.id);
  return items.sort((left, right) => order.indexOf(left.id) - order.indexOf(right.id));
}

function buildCaptionRows(payload: DailyBriefChartPayload): Array<{ id: string; label: string; text: string; muted?: boolean }> {
  if (payload.chart_guide_items?.length) {
    return payload.chart_guide_items.map((item) => ({
      id: item.id,
      label: item.label,
      text: item.text,
      muted: item.muted,
    }));
  }
  const rows: Array<{ id: string; label: string; text: string; muted?: boolean }> = [];
  const observed = payload.observed_path ?? payload.observed_series ?? payload.primary_series;
  if (observed?.plain_language_meaning) {
    rows.push({ id: "observed", label: observed.label || "Observed", text: observed.plain_language_meaning });
  }
  const forecast = payload.forecast_path ?? payload.forecast_series;
  if (forecast?.plain_language_meaning || payload.forecast_overlay?.forecast_relative_direction) {
    rows.push({
      id: "forecast",
      label: forecast?.label ?? payload.forecast_overlay?.forecast_label ?? "Forecast",
      text: forecast?.plain_language_meaning ?? payload.chart_explainer_lines?.[1] ?? "Forecast path qualifies where the move may go next.",
    });
  }
  const review = payload.review_context ?? payload.review_band;
  if (review?.plain_language_meaning) {
    rows.push({ id: review.band_id, label: review.label, text: review.plain_language_meaning });
  }
  if (payload.threshold_overlap_mode === "merge_to_zone" && payload.thresholds?.trigger_zone?.note) {
    rows.push({ id: "decision_zone", label: String(payload.thresholds.trigger_zone.label || "Decision zone"), text: String(payload.thresholds.trigger_zone.note) });
  }
  for (const line of payload.decision_references ?? payload.threshold_lines ?? []) {
    if (line.legend_enabled === false) continue;
    rows.push({
      id: line.threshold_id,
      label: (line.visible_in_overview ?? line.visible_by_default) ? line.label : `${line.label} · focus only`,
      text: line.plain_language_meaning,
      muted: !(line.visible_in_overview ?? line.visible_by_default),
    });
  }
  if (!rows.length && payload.chart_explainer_lines?.length) {
    return payload.chart_explainer_lines.map((line, index) => ({ id: `fallback-${index}`, label: "Guide", text: line }));
  }
  return rows;
}

function resolveGuideTargetId(payload: DailyBriefChartPayload, guideId: string) {
  if (guideId === "observed") return payload.observed_series?.series_id ?? payload.primary_series?.series_id ?? null;
  if (guideId === "forecast") return payload.forecast_series?.series_id ?? "forecast";
  return guideId;
}

function findFocusGroupByLayer(payload: DailyBriefChartPayload, layerId: string | null | undefined) {
  if (!layerId) return null;
  return (payload.focusable_threshold_groups ?? []).find((group) => group.member_line_ids.includes(layerId)) ?? null;
}

export function DailyBriefDecisionChart({ payload, height = 230 }: Props) {
  const density = payload.chart_density_profile ?? "rich_line";
  const summary = payload.compact_chart_summary ?? [];
  const footerNote = payload.source_validity_footer ?? null;
  const confirmationStrip = payload.confirmation_strip ?? null;
  const eventStrip = payload.event_reaction_strip ?? null;
  const chartExplainerLines = payload.chart_explainer_lines ?? [];
  const chartTakeaway = payload.chart_takeaway ?? null;
  const showThresholdChart = payload.chart_kind === "threshold_line" && density !== "strip_only";
  const showStripOnly = density === "strip_only" && confirmationStrip;
  const legendLayers = useMemo(() => buildLegendLayers(payload), [payload]);
  const captionRows = useMemo(() => buildCaptionRows(payload), [payload]);
  const focusGroups = useMemo(() => payload.focusable_threshold_groups ?? [], [payload]);
  const focusModes = useMemo<DailyBriefChartFocusMode[]>(() => {
    if (payload.focus_modes?.length) return payload.focus_modes;
    const fallback: DailyBriefChartFocusMode[] = [
      {
        mode_id: "overview",
        mode_label: "Overview",
        primary_object_roles: ["observed_path"],
        secondary_object_roles: ["forecast_path", "review_context", "decision_reference"],
        hidden_object_roles: [],
        visible_object_ids: legendLayers.filter((item) => item.visibleByDefault).map((item) => item.id),
        legend_state: "overview",
        tooltip_role: "overview",
      },
    ];
    for (const group of focusGroups) {
      fallback.push({
        mode_id: group.group_id,
        mode_label: group.group_label,
        primary_object_roles: group.group_id === "observed_forecast_group" ? ["observed_path", "forecast_path"] : ["decision_reference"],
        secondary_object_roles: group.group_id === "observed_forecast_group" ? ["review_context"] : ["observed_path", "review_context", "forecast_path"],
        hidden_object_roles: group.group_id === "observed_forecast_group" ? ["decision_reference"] : [],
        visible_object_ids: group.member_line_ids,
        y_domain: group.suggested_y_domain,
        legend_state: "focus",
        tooltip_role: group.group_id === "observed_forecast_group" ? "path_compare" : "decision_compare",
      });
    }
    return fallback;
  }, [payload.focus_modes, legendLayers, focusGroups]);
  const payloadResetKey = useMemo(
    () =>
      [
        payload.chart_kind,
        payload.chart_question,
        payload.current_point?.timestamp ?? "",
        String(payload.current_point?.value ?? ""),
        payload.focus_default_group ?? "",
        payload.primary_focus_series ?? "",
        focusGroups.map((group) => `${group.group_id}:${group.member_line_ids.join(",")}`).join("|"),
        focusModes.map((mode) => `${mode.mode_id}:${(mode.visible_object_ids ?? []).join(",")}`).join("|"),
        legendLayers.map((item) => `${item.id}:${item.renderMode ?? "line"}`).join("|"),
      ].join("::"),
    [focusGroups, focusModes, legendLayers, payload.chart_kind, payload.chart_question, payload.current_point?.timestamp, payload.current_point?.value, payload.focus_default_group, payload.primary_focus_series],
  );
  const [chartMode, setChartMode] = useState<"overview" | "focus">("overview");
  const [activeFocusGroupId, setActiveFocusGroupId] = useState<string | null>(payload.focus_default_group ?? focusGroups[0]?.group_id ?? null);
  const [visibleLayerIds, setVisibleLayerIds] = useState<string[]>(() => legendLayers.filter((item) => item.visibleByDefault).map((item) => item.id));
  const [focusedLayerId, setFocusedLayerId] = useState<string | null>(payload.primary_focus_series ?? legendLayers[0]?.id ?? null);
  const activeModeId = chartMode === "overview" ? "overview" : activeFocusGroupId;
  const activeFocusGroup = useMemo<DailyBriefChartFocusGroup | null>(
    () => focusGroups.find((group) => group.group_id === activeFocusGroupId) ?? null,
    [focusGroups, activeFocusGroupId],
  );
  const activeFocusMode = useMemo<DailyBriefChartFocusMode | null>(
    () => focusModes.find((mode) => mode.mode_id === activeModeId) ?? null,
    [focusModes, activeModeId],
  );

  useEffect(() => {
    setChartMode("overview");
    setActiveFocusGroupId(payload.focus_default_group ?? focusGroups[0]?.group_id ?? null);
    setVisibleLayerIds(legendLayers.filter((item) => item.visibleByDefault).map((item) => item.id));
    setFocusedLayerId(payload.primary_focus_series ?? legendLayers[0]?.id ?? null);
  }, [payloadResetKey]);

  useEffect(() => {
    if (chartMode === "overview") {
      const overviewMode = focusModes.find((mode) => mode.mode_id === "overview");
      setVisibleLayerIds(overviewMode?.visible_object_ids?.length ? overviewMode.visible_object_ids : legendLayers.filter((item) => item.visibleByDefault).map((item) => item.id));
      return;
    }
    if (activeFocusMode?.visible_object_ids?.length || activeFocusGroup) {
      const nextVisible = activeFocusMode?.visible_object_ids?.length ? activeFocusMode.visible_object_ids : activeFocusGroup?.member_line_ids ?? [];
      setVisibleLayerIds(nextVisible);
      setFocusedLayerId(activeFocusGroup?.primary_line_id ?? nextVisible[0] ?? null);
    }
  }, [activeFocusGroup, activeFocusMode, chartMode, legendLayers, focusModes]);

  const haltEvent = (event: SyntheticEvent<HTMLElement>) => {
    event.stopPropagation();
  };

  const activateFocusGroup = (group: DailyBriefChartFocusGroup | null, preferredLayerId?: string | null) => {
    if (!group) return;
    setChartMode("focus");
    setActiveFocusGroupId(group.group_id);
    setVisibleLayerIds(group.member_line_ids);
    setFocusedLayerId(preferredLayerId ?? group.primary_line_id ?? group.member_line_ids[0] ?? null);
  };

  const handleLegendClick = (layerId: string) => {
    const group = findFocusGroupByLayer(payload, layerId) ?? activeFocusGroup ?? null;
    if (group) {
      activateFocusGroup(group, layerId);
      return;
    }
    setFocusedLayerId(layerId);
  };

  const handleGuideClick = (guideId: string) => {
    const targetId = resolveGuideTargetId(payload, guideId);
    const group =
      (guideId === "decision_zone"
        ? focusGroups.find((item) => item.can_split_from_zone) ?? null
        : findFocusGroupByLayer(payload, targetId)) ?? activeFocusGroup ?? null;
    if (group) {
      activateFocusGroup(group, targetId);
      return;
    }
    if (targetId) setFocusedLayerId(targetId);
  };

  const displayedCaptionRows = useMemo(() => {
    const activeIds =
      chartMode === "overview"
        ? focusModes.find((mode) => mode.mode_id === "overview")?.visible_object_ids ?? visibleLayerIds
        : activeFocusMode?.visible_object_ids ?? visibleLayerIds;
    const filtered = captionRows.filter((row) => {
      const targetId = resolveGuideTargetId(payload, row.id);
      if (!targetId) return true;
      return activeIds.includes(targetId);
    });
    return filtered.slice(0, 5);
  }, [captionRows, chartMode, focusModes, activeFocusMode, visibleLayerIds, payload]);

  return (
    <div
      className={`brief-decision-chart-shell density-${density}`}
      onClick={haltEvent}
      onMouseDown={haltEvent}
      onPointerDown={haltEvent}
      onDoubleClick={haltEvent}
    >
      <div className="brief-decision-chart-question">{payload.chart_question}</div>

      {summary.length ? (
        <div className="brief-decision-chart-summary">
          {summary.map((item) => (
            <div className={`brief-decision-chart-summary-item tone-${summaryToneClass(item.tone)}`} key={`${item.label}:${item.value}`}>
              <div className="brief-decision-chart-summary-label">{item.label}</div>
              <div className="brief-decision-chart-summary-value">{item.value}</div>
            </div>
          ))}
        </div>
      ) : null}

      <div className={`brief-decision-chart-visual ${confirmationStrip || eventStrip ? "has-strip" : "no-strip"}`}>
        {focusGroups.length ? (
          <div className="brief-decision-chart-focus-row">
              <button
                type="button"
                className={`brief-decision-chart-focus-pill ${chartMode === "overview" ? "is-active" : ""}`}
                aria-pressed={chartMode === "overview"}
                data-focus-mode="overview"
                onClick={(event) => {
                  haltEvent(event);
                  setChartMode("overview");
                setActiveFocusGroupId(payload.focus_default_group ?? focusGroups[0]?.group_id ?? null);
                setFocusedLayerId(payload.primary_focus_series ?? legendLayers[0]?.id ?? null);
              }}
            >
              Overview
            </button>
            {focusModes.filter((mode) => mode.mode_id !== "overview").map((mode) => (
              <button
                type="button"
                className={`brief-decision-chart-focus-pill ${chartMode === "focus" && activeFocusGroupId === mode.mode_id ? "is-active" : ""}`}
                key={mode.mode_id}
                aria-pressed={chartMode === "focus" && activeFocusGroupId === mode.mode_id}
                data-focus-group={mode.mode_id}
                onClick={(event) => {
                  haltEvent(event);
                  const group = focusGroups.find((item) => item.group_id === mode.mode_id) ?? null;
                  if (group) activateFocusGroup(group);
                }}
              >
                {mode.mode_label}
              </button>
            ))}
            {chartMode === "focus" && payload.focus_reason ? (
              <span className="brief-decision-chart-focus-note">{payload.focus_reason}</span>
            ) : null}
          </div>
        ) : null}

        {legendLayers.length ? (
          <div className="brief-decision-chart-legend interactive">
            {legendLayers.map((item) => (
              <button
                type="button"
                className={`brief-decision-chart-legend-item tone-${item.tone} style-${item.style} mode-${item.renderMode ?? "line"} ${visibleLayerIds.includes(item.id) ? "is-visible" : "is-hidden"} ${focusedLayerId === item.id ? "is-focused" : ""}`}
                key={item.id}
                aria-pressed={focusedLayerId === item.id}
                data-layer-id={item.id}
                onClick={(event) => {
                  haltEvent(event);
                  handleLegendClick(item.id);
                }}
                onMouseEnter={() => setFocusedLayerId(item.id)}
                onMouseLeave={() => setFocusedLayerId(chartMode === "focus" ? (activeFocusGroup?.primary_line_id ?? payload.primary_focus_series ?? null) : (payload.primary_focus_series ?? null))}
                onFocus={() => setFocusedLayerId(item.id)}
                onBlur={() => setFocusedLayerId(chartMode === "focus" ? (activeFocusGroup?.primary_line_id ?? payload.primary_focus_series ?? null) : (payload.primary_focus_series ?? null))}
              >
                <span className="brief-decision-chart-legend-mark" />
                <span>{item.label}</span>
              </button>
            ))}
          </div>
        ) : null}

        {payload.chart_suppressed_reason ? (
          <div className="brief-decision-chart-suppressed">
            <div className="brief-decision-chart-suppressed-label">Chart suppressed</div>
            <p>{payload.chart_suppressed_reason}</p>
          </div>
        ) : showThresholdChart ? (
          <div className="brief-decision-chart-frame">
            <ThresholdLineChart
              payload={payload}
              height={density === "compact_line" ? Math.min(height, 188) : height}
              visibleLayerIds={visibleLayerIds}
              focusedLayerId={focusedLayerId}
              focusMode={chartMode === "focus"}
              focusGroupId={activeFocusGroupId}
            />
          </div>
        ) : null}

        {!payload.chart_suppressed_reason && payload.chart_kind === "event_reaction_strip" && eventStrip ? (
          <SignalStrip strip={eventStrip} />
        ) : !payload.chart_suppressed_reason && payload.chart_kind === "confirmation_strip" && confirmationStrip ? (
          <SignalStrip strip={confirmationStrip} />
        ) : null}

        {(captionRows.length || chartTakeaway) ? (
          <div className="brief-decision-chart-caption">
            <div className="brief-decision-chart-caption-title">Legend</div>
            {displayedCaptionRows.length ? (
              <div className="brief-decision-chart-guide-chips">
                {displayedCaptionRows.map((row) => (
                  <button
                    type="button"
                    className={`brief-decision-chart-guide-chip ${row.muted ? "is-muted" : ""} ${focusedLayerId === resolveGuideTargetId(payload, row.id) ? "is-focused" : ""}`}
                    key={row.id}
                    onClick={(event) => {
                      haltEvent(event);
                      handleGuideClick(row.id);
                    }}
                  >
                    <span className="brief-decision-chart-guide-chip-label">{row.label}</span>
                    <span className="brief-decision-chart-guide-chip-text">{row.text}</span>
                  </button>
                ))}
              </div>
            ) : chartExplainerLines.length ? (
              <div className="brief-decision-chart-guide-chips">
                {chartExplainerLines.map((line, index) => (
                  <span className="brief-decision-chart-guide-chip fallback" key={`${index}:${line}`}>
                    <span className="brief-decision-chart-guide-chip-label">Guide</span>
                    <span className="brief-decision-chart-guide-chip-text">{line}</span>
                  </span>
                ))}
              </div>
            ) : null}
            {chartTakeaway ? (
              <div className="brief-decision-chart-caption-footer">
                <span className="brief-decision-chart-caption-label">Takeaway</span>
                <span className="brief-decision-chart-caption-text">{chartTakeaway}</span>
              </div>
            ) : null}
          </div>
        ) : null}

        {showStripOnly ? <SignalStrip strip={confirmationStrip} /> : null}
        {showThresholdChart && confirmationStrip ? <SignalStrip strip={confirmationStrip} /> : null}
      </div>

      {footerNote ? <div className="brief-decision-chart-validity">{footerNote}</div> : null}
    </div>
  );
}
