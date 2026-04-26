import { useEffect, useRef } from 'react';
import * as Plotly from 'plotly.js-dist-min';
import type { ChartPanelDisplay } from '../../../chartTypes';
import { BASE_LAYOUT, BASE_CONFIG, C, FILL, bandColor } from '../plotlyTheme';

type Props = { panel: ChartPanelDisplay; height?: number };

export function RegimePlotlyDetail({ panel, height = 420 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const logic = panel.chartLogic;

  useEffect(() => {
    const el = ref.current;
    if (!el || !logic) return;

    const current = logic.currentValue ?? 0;
    const previous = logic.previousValue ?? 0;
    const bands = logic.bands ?? [];
    const currentBand = logic.currentBand ?? '';

    const historyPoints = panel.primarySeries?.points ?? [];
    const dates = historyPoints.map(p => p.time);
    const values = historyPoints.map(p => p.value);

    const allBandVals = bands.flatMap(b => [b.min, b.max].filter(v => v !== null)) as number[];
    const allVals = [...values, ...allBandVals, current, previous].filter(v => isFinite(v));
    const yMin = allVals.length ? Math.min(...allVals) : 0;
    const yMax = allVals.length ? Math.max(...allVals) : 1;
    const ySpan = yMax - yMin || 1;
    const axisYMin = yMin - ySpan * 0.05;
    const axisYMax = yMax + ySpan * 0.05;

    const xRange = dates.length >= 2 ? [dates[0], dates[dates.length - 1]] : undefined;

    const shapes: object[] = [];
    if (xRange) {
      for (const band of bands) {
        const isActive = band.label.toLowerCase() === currentBand.toLowerCase();
        const bMin = band.min ?? axisYMin;
        const bMax = band.max ?? axisYMax;
        const color = bandColor(band.label);
        shapes.push({
          type: 'rect',
          x0: xRange[0], x1: xRange[1],
          y0: bMin, y1: bMax,
          xref: 'x', yref: 'y',
          fillcolor: `${color}${isActive ? FILL.medium : FILL.light}`,
          line: { width: 0 },
          layer: 'below',
        });
      }
    }

    const annotations: object[] = bands.map(band => {
      const bMin = band.min ?? axisYMin;
      const bMax = band.max ?? axisYMax;
      const midY = (bMin + bMax) / 2;
      return {
        text: `<b>${band.label}</b>`,
        x: 1, y: midY, xref: 'paper', yref: 'y',
        xanchor: 'right', yanchor: 'middle',
        showarrow: false,
        font: { color: bandColor(band.label), size: 9 },
      };
    });

    const data: object[] = [];
    if (dates.length) {
      data.push({
        type: 'scatter',
        mode: 'lines',
        x: dates,
        y: values,
        line: { color: C.prior, width: 1.5 },
        name: panel.primarySeries?.label ?? 'History',
        hovertemplate: '%{y:.2f}<extra></extra>',
      });
    }
    if (dates.length) {
      // Prior marker
      data.push({
        type: 'scatter', mode: 'markers',
        x: [dates[Math.max(0, dates.length - 2)]],
        y: [previous],
        marker: { color: C.prior, size: 8, symbol: 'circle-open', line: { color: C.prior, width: 2 } },
        name: 'Prior',
        hovertemplate: 'Prior: %{y:.2f}<extra></extra>',
      });
      // Current marker
      data.push({
        type: 'scatter', mode: 'markers',
        x: [dates[dates.length - 1]],
        y: [current],
        marker: { color: C.current, size: 12, symbol: 'circle', line: { color: C.current, width: 2 } },
        name: 'Current',
        hovertemplate: 'Current: %{y:.2f}<extra></extra>',
      });
    }

    const noticeText = panel.whatToNotice;
    if (noticeText) {
      annotations.push({
        text: noticeText,
        x: 0.5, y: -0.08, xref: 'paper', yref: 'paper',
        xanchor: 'center', yanchor: 'top',
        showarrow: false,
        font: { color: C.textSoft, size: 10 },
      });
    }

    const layout = {
      ...BASE_LAYOUT,
      height,
      title: { text: panel.title, font: { size: 13, color: '#d2cec3' }, x: 0 },
      shapes,
      annotations,
      yaxis: { ...BASE_LAYOUT.yaxis, range: [axisYMin, axisYMax] },
      margin: { t: 36, r: 80, b: noticeText ? 48 : 20, l: 50 },
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, data as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, logic, height]);

  if (!logic) return <div className="ia-chart-panel"><div className="ia-chart-title">{panel.title}</div></div>;

  return (
    <div>
      <div ref={ref} style={{ width: '100%' }} />
      {panel.callouts.length > 0 && (
        <div className="ia-chart-thresholds" style={{ marginTop: 8 }}>
          {panel.callouts.map(c => (
            <div className="ia-chart-threshold" key={c.id}>
              <strong>{c.label}</strong>
              <span>{c.detail}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
