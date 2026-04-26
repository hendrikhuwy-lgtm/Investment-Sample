import { useState, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import * as Plotly from 'plotly.js-dist-min';
import type { ChartPanelDisplay } from '../../chartTypes';
import { BASE_LAYOUT, BASE_CONFIG, C } from './plotlyTheme';

type Props = { panel: ChartPanelDisplay; height?: number };

function rebase(values: number[]): number[] {
  if (!values.length) return [];
  const base = values[0];
  if (!base) return values;
  return values.map(v => (v / base) * 100);
}

export function ComparisonPlotlyChart({ panel, height = 220 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [detail, setDetail] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const primary = panel.primarySeries;
    const comp = panel.comparisonSeries;

    if (!primary || !primary.points.length) return;

    const primaryDates  = primary.points.map(p => p.time);
    const primaryValues = primary.points.map(p => p.value);
    const compDates     = comp?.points.map(p => p.time) ?? [];
    const compValues    = comp?.points.map(p => p.value) ?? [];

    const useBar = primaryValues.length <= 2;

    let data: object[];
    if (useBar) {
      const labels = primaryDates.map((_, i) => primary.points[i]?.time ?? `T${i}`);
      data = [
        {
          type: 'bar',
          x: labels, y: primaryValues,
          name: primary.label,
          marker: { color: C.current },
          hovertemplate: '%{x}: %{y:.2f}<extra></extra>',
        },
      ];
      if (compValues.length) {
        data.push({
          type: 'bar',
          x: compDates.map((_, i) => comp!.points[i]?.time ?? `T${i}`),
          y: compValues,
          name: comp!.label,
          marker: { color: C.prior },
          hovertemplate: '%{x}: %{y:.2f}<extra></extra>',
        });
      }
    } else {
      const rebPrimary = rebase(primaryValues);
      const rebComp    = rebase(compValues);
      data = [
        {
          type: 'scatter',
          mode: 'lines',
          x: primaryDates, y: rebPrimary,
          name: primary.label,
          line: { color: C.current, width: 2 },
          hovertemplate: '%{x}: %{y:.1f}<extra></extra>',
        },
      ];
      if (rebComp.length) {
        data.push({
          type: 'scatter',
          mode: 'lines',
          x: compDates, y: rebComp,
          name: comp!.label,
          line: { color: C.prior, width: 1.5, dash: 'dot' },
          hovertemplate: '%{x}: %{y:.1f}<extra></extra>',
        });
      }
    }

    // Threshold shapes
    const shapes: object[] = panel.thresholds.map(t => ({
      type: 'line',
      x0: 0, x1: 1, xref: 'paper',
      y0: t.value, y1: t.value, yref: 'y',
      line: { color: t.type.includes('off_target') ? C.stress : C.watch, width: 1, dash: 'dash' },
    }));

    // Event markers
    if (panel.markers.length) {
      data.push({
        type: 'scatter',
        mode: 'markers',
        x: panel.markers.map(m => m.time),
        y: panel.markers.map(() => null), // auto y
        marker: { symbol: 'triangle-up', size: 10, color: C.watch },
        text: panel.markers.map(m => m.label),
        hovertemplate: '%{text}<extra></extra>',
        yaxis: 'y',
      });
    }

    const layout = {
      ...BASE_LAYOUT,
      height,
      shapes,
      showlegend: !!(comp && comp.points.length),
      legend: { x: 0, y: 1, orientation: 'h', font: { size: 10, color: '#888' } },
      margin: { t: 8, r: 12, b: 24, l: 40 },
      xaxis: { ...BASE_LAYOUT.xaxis, tickfont: { size: 9 } },
      yaxis: { ...BASE_LAYOUT.yaxis, title: useBar ? '' : 'Base 100' },
      barmode: 'group',
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, data as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, height]);

  if (!panel.primarySeries || !panel.primarySeries.points.length) {
    return (
      <div className="ia-chart-panel">
        <div className="ia-chart-head"><div className="ia-chart-title">{panel.title}</div></div>
        <div className="surface-placeholder" style={{ padding: '24px 0' }}>Chart unavailable</div>
      </div>
    );
  }

  return (
    <>
      <div className="ia-chart-panel ia-chart-clickable" onClick={() => setDetail(true)}>
        <div className="ia-chart-head">
          <div>
            <div className="ia-chart-title">{panel.title}</div>
            <div className="ia-chart-meta">
              <span>{panel.freshnessLabel}</span>
              <span>{panel.trustLabel}</span>
            </div>
          </div>
          {panel.degradedLabel && <div className="ia-chart-degraded">{panel.degradedLabel}</div>}
        </div>
        <div className="ia-chart-summary">{panel.summary}</div>
        <div ref={ref} style={{ width: '100%' }} />
        <div className="ia-chart-notice">{panel.whatToNotice}</div>
      </div>
      {detail && createPortal(
        <div className="ia-chart-detail-overlay" onClick={() => setDetail(false)}>
          <div className="ia-chart-detail-modal" onClick={e => e.stopPropagation()}>
            <button className="ia-chart-detail-close" onClick={() => setDetail(false)}>×</button>
            <div ref={null} style={{ fontSize: 13, color: '#d2cec3', marginBottom: 8 }}>{panel.title}</div>
            <ComparisonPlotlyChartInner panel={panel} height={380} />
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}

// Inner component re-used in detail modal with larger height
function ComparisonPlotlyChartInner({ panel, height }: { panel: ChartPanelDisplay; height: number }) {
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el || !panel.primarySeries?.points.length) return;

    const primary = panel.primarySeries;
    const comp = panel.comparisonSeries;
    const primaryDates  = primary.points.map(p => p.time);
    const primaryValues = primary.points.map(p => p.value);
    const compDates     = comp?.points.map(p => p.time) ?? [];
    const compValues    = comp?.points.map(p => p.value) ?? [];

    const useBar = primaryValues.length <= 2;
    let data: object[];
    if (useBar) {
      data = [
        { type: 'bar', x: primaryDates, y: primaryValues, name: primary.label, marker: { color: C.current } },
        ...(compValues.length ? [{ type: 'bar', x: compDates, y: compValues, name: comp!.label, marker: { color: C.prior } }] : []),
      ];
    } else {
      const rebPrimary = rebase(primaryValues);
      const rebComp = rebase(compValues);
      data = [
        { type: 'scatter', mode: 'lines', x: primaryDates, y: rebPrimary, name: primary.label, line: { color: C.current, width: 2 } },
        ...(rebComp.length ? [{ type: 'scatter', mode: 'lines', x: compDates, y: rebComp, name: comp!.label, line: { color: C.prior, width: 1.5, dash: 'dot' } }] : []),
      ];
    }

    const shapes: object[] = panel.thresholds.map(t => ({
      type: 'line', x0: 0, x1: 1, xref: 'paper',
      y0: t.value, y1: t.value, yref: 'y',
      line: { color: C.watch, width: 1, dash: 'dash' },
    }));

    const layout = {
      ...BASE_LAYOUT, height, shapes,
      showlegend: !!(comp && comp.points.length),
      legend: { x: 0, y: 1, orientation: 'h', font: { size: 10, color: '#888' } },
      margin: { t: 8, r: 12, b: 32, l: 50 },
      barmode: 'group',
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, data as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, height]);

  return <div ref={ref} style={{ width: '100%' }} />;
}
