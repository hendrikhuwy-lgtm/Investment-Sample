import { useState, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import * as Plotly from 'plotly.js-dist-min';
import type { ChartPanelDisplay } from '../../chartTypes';
import { BASE_LAYOUT, BASE_CONFIG, C } from './plotlyTheme';

type Props = { panel: ChartPanelDisplay; height?: number };

function buildDecompData(panel: ChartPanelDisplay): { data: object[]; shapes: object[] } {
  const bars = panel.chartLogic?.allocationBars ?? [];

  const labels  = bars.map(b => b.label);
  const values  = bars.map(b => b.value);
  const targets = bars.map(b => b.target);
  const lows    = bars.map(b => b.low);
  const highs   = bars.map(b => b.high);

  const barColors = values.map((v, i) =>
    Math.abs(v - targets[i]) > 2 ? C.stress : C.confirm,
  );

  const data: object[] = [
    {
      type: 'bar',
      x: labels,
      y: values,
      marker: { color: barColors },
      name: 'Current',
      error_y: {
        type: 'data',
        symmetric: false,
        array: highs.map((h, i) => Math.max(0, h - values[i])),
        arrayminus: values.map((v, i) => Math.max(0, v - lows[i])),
        visible: true,
        color: '#555',
        thickness: 1.5,
        width: 5,
      },
      hovertemplate: '%{x}: %{y:.1f}%<extra></extra>',
    },
    // Target markers (horizontal tick)
    {
      type: 'scatter',
      mode: 'markers',
      x: labels,
      y: targets,
      marker: { symbol: 'line-ew', size: 18, color: C.watch, line: { color: C.watch, width: 2 } },
      name: 'Target',
      hovertemplate: 'Target: %{y:.1f}%<extra></extra>',
    },
  ];

  // Target lines as shapes
  const shapes: object[] = bars.map((bar, i) => ({
    type: 'line',
    xref: 'x', yref: 'y',
    x0: i - 0.4, x1: i + 0.4,
    y0: targets[i], y1: targets[i],
    line: { color: C.watch, width: 2 },
  }));

  return { data, shapes };
}

export function DecompositionPlotlyChart({ panel, height = 220 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [detail, setDetail] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const bars = panel.chartLogic?.allocationBars ?? [];
    if (!bars.length) return;

    const { data, shapes } = buildDecompData(panel);

    const layout = {
      ...BASE_LAYOUT,
      height,
      shapes,
      bargap: 0.35,
      showlegend: true,
      legend: { x: 0, y: 1, orientation: 'h', font: { size: 10, color: '#888' } },
      margin: { t: 8, r: 12, b: 40, l: 40 },
      xaxis: { ...BASE_LAYOUT.xaxis, tickfont: { size: 9 }, tickangle: -20 },
      yaxis: { ...BASE_LAYOUT.yaxis, title: '%' },
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, data as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, height]);

  if (!panel.chartLogic?.allocationBars.length) {
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
            <DecompositionPlotlyChartInner panel={panel} height={380} />
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}

function DecompositionPlotlyChartInner({ panel, height }: { panel: ChartPanelDisplay; height: number }) {
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el || !panel.chartLogic?.allocationBars.length) return;

    const { data, shapes } = buildDecompData(panel);
    const layout = {
      ...BASE_LAYOUT, height, shapes, bargap: 0.35,
      showlegend: true,
      legend: { x: 0, y: 1, orientation: 'h', font: { size: 10, color: '#888' } },
      margin: { t: 8, r: 12, b: 48, l: 50 },
      xaxis: { ...BASE_LAYOUT.xaxis, tickfont: { size: 9 }, tickangle: -25 },
      yaxis: { ...BASE_LAYOUT.yaxis, title: '%' },
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, data as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, height]);

  return <div ref={ref} style={{ width: '100%' }} />;
}
