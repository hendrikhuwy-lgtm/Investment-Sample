import { useState, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import * as Plotly from 'plotly.js-dist-min';
import type { ChartPanelDisplay } from '../../chartTypes';
import { BASE_LAYOUT, BASE_CONFIG, C } from './plotlyTheme';

type Props = { panel: ChartPanelDisplay; height?: number };

function buildForecastData(panel: ChartPanelDisplay): object[] {
  const data: object[] = [];

  // Historical comparison series (area behind forecast)
  if (panel.comparisonSeries?.points.length) {
    const hist = panel.comparisonSeries;
    data.push({
      type: 'scatter',
      mode: 'lines',
      x: hist.points.map(p => p.time),
      y: hist.points.map(p => p.value),
      name: hist.label,
      line: { color: `${C.prior}88`, width: 1.5 },
      fill: 'tozeroy',
      fillcolor: `${C.prior}11`,
      hovertemplate: '%{x}: %{y:.2f}<extra></extra>',
    });
  }

  // Confidence band (if available)
  if (panel.bands.length) {
    const band = panel.bands[0];
    if (band.lower.length && band.upper.length) {
      // Lower bound (invisible line, anchor for fill)
      data.push({
        type: 'scatter',
        mode: 'lines',
        x: band.lower.map(p => p.time),
        y: band.lower.map(p => p.value),
        line: { width: 0 },
        showlegend: false,
        hoverinfo: 'skip',
      });
      // Upper bound fills down to lower
      data.push({
        type: 'scatter',
        mode: 'lines',
        x: band.upper.map(p => p.time),
        y: band.upper.map(p => p.value),
        fill: 'tonexty',
        fillcolor: `${C.prior}28`,
        line: { width: 0 },
        name: band.label,
        hovertemplate: 'Band: %{y:.2f}<extra></extra>',
      });
    }
  }

  // Forecast path (primary series)
  if (panel.primarySeries?.points.length) {
    const forecast = panel.primarySeries;
    data.push({
      type: 'scatter',
      mode: 'lines',
      x: forecast.points.map(p => p.time),
      y: forecast.points.map(p => p.value),
      name: forecast.label,
      line: { color: C.current, width: 2 },
      hovertemplate: '%{x}: %{y:.2f}<extra></extra>',
    });
  }

  // Event markers
  if (panel.markers.length) {
    data.push({
      type: 'scatter',
      mode: 'markers',
      x: panel.markers.map(m => m.time),
      y: panel.markers.map(() => null),
      marker: { symbol: 'triangle-up', size: 10, color: C.watch },
      text: panel.markers.map(m => m.label),
      hovertemplate: '%{text}<extra></extra>',
    });
  }

  return data;
}

function buildForecastLayout(panel: ChartPanelDisplay, height: number): object {
  // Threshold shapes (breach levels)
  const shapes: object[] = panel.thresholds.map(t => ({
    type: 'line',
    x0: 0, x1: 1, xref: 'paper',
    y0: t.value, y1: t.value, yref: 'y',
    line: { color: C.stress, width: 1, dash: 'dash' },
  }));

  // Forecast start vline (first point of primary series)
  const forecastStart = panel.primarySeries?.points[0]?.time;
  if (forecastStart) {
    shapes.push({
      type: 'line',
      x0: forecastStart, x1: forecastStart,
      y0: 0, y1: 1, yref: 'paper',
      line: { color: `${C.watch}88`, width: 1, dash: 'dot' },
    });
  }

  const annotations: object[] = [];
  if (forecastStart) {
    annotations.push({
      x: forecastStart, y: 1, xref: 'x', yref: 'paper',
      text: 'Forecast', showarrow: false,
      font: { color: `${C.watch}aa`, size: 9 },
      yanchor: 'top',
    });
  }
  for (const threshold of panel.thresholds) {
    annotations.push({
      x: 1, y: threshold.value, xref: 'paper', yref: 'y',
      text: threshold.label, showarrow: false,
      font: { color: C.stress, size: 9 },
      xanchor: 'right', yanchor: 'bottom',
    });
  }

  return {
    ...BASE_LAYOUT,
    height,
    shapes,
    annotations,
    showlegend: !!(panel.comparisonSeries?.points.length || panel.bands.length),
    legend: { x: 0, y: 1, orientation: 'h', font: { size: 10, color: '#888' } },
    margin: { t: 8, r: 20, b: 28, l: 44 },
  };
}

export function ForecastPlotlyChart({ panel, height = 220 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [detail, setDetail] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const data = buildForecastData(panel);
    const layout = buildForecastLayout(panel, height);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, data as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, height]);

  if (!panel.primarySeries?.points.length) {
    return (
      <div className="ia-chart-panel">
        <div className="ia-chart-head"><div className="ia-chart-title">{panel.title}</div></div>
        <div className="surface-placeholder" style={{ padding: '24px 0' }}>Forecast unavailable</div>
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
            <ForecastPlotlyChartInner panel={panel} height={380} />
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}

function ForecastPlotlyChartInner({ panel, height }: { panel: ChartPanelDisplay; height: number }) {
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const data = buildForecastData(panel);
    const layout = buildForecastLayout(panel, height);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, data as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, height]);

  return <div ref={ref} style={{ width: '100%' }} />;
}
