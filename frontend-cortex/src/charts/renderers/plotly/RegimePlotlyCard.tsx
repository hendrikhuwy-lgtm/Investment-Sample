import { useState, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import * as Plotly from 'plotly.js-dist-min';
import type { ChartPanelDisplay } from '../../chartTypes';
import { BASE_CONFIG, C, FILL, bandColor } from './plotlyTheme';
import { RegimePlotlyDetail } from './detail/RegimePlotlyDetail';

type Props = { panel: ChartPanelDisplay; height?: number };

export function RegimePlotlyCard({ panel, height = 220 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [detail, setDetail] = useState(false);
  const logic = panel.chartLogic;

  useEffect(() => {
    const el = ref.current;
    if (!el || !logic) return;

    const current = logic.currentValue ?? 0;
    const previous = logic.previousValue ?? 0;
    const bands = logic.bands ?? [];
    const currentBand = logic.currentBand ?? '';

    // Compute x range from band bounds + current/previous
    const bandVals = bands.flatMap(b => [b.min, b.max].filter(v => v !== null)) as number[];
    const allVals = [...bandVals, current, previous].filter(isFinite);
    const vizMin = allVals.length ? Math.min(...allVals) : 0;
    const vizMax = allVals.length ? Math.max(...allVals) : 1;
    const vizSpan = vizMax - vizMin || 1;
    const xMin = vizMin - vizSpan * 0.05;
    const xMax = vizMax + vizSpan * 0.05;

    const shapes: object[] = bands.map(band => {
      const isActive = band.label.toLowerCase() === currentBand.toLowerCase();
      const color = bandColor(band.label);
      return {
        type: 'rect',
        x0: band.min ?? xMin,
        x1: band.max ?? xMax,
        y0: 0, y1: 1, yref: 'paper',
        fillcolor: `${color}${isActive ? FILL.active : FILL.light}`,
        line: { width: 0 },
        layer: 'below',
      };
    });

    const annotations: object[] = bands.map(band => {
      const bMin = band.min ?? xMin;
      const bMax = band.max ?? xMax;
      const midX = (bMin + bMax) / 2;
      return {
        x: midX, y: 0.5, yref: 'paper',
        text: `<b>${band.label}</b>`,
        showarrow: false,
        font: { color: bandColor(band.label), size: 10 },
      };
    });

    const data: object[] = [
      // Prior position
      {
        type: 'scatter',
        x: [previous], y: [0.5],
        mode: 'markers',
        marker: {
          color: C.prior, size: 10,
          symbol: 'line-ns',
          line: { color: C.prior, width: 2 },
        },
        name: 'Prior',
        hovertemplate: `Prior: ${previous.toFixed(2)}<extra></extra>`,
      },
      // Current position
      {
        type: 'scatter',
        x: [current], y: [0.5],
        mode: 'markers',
        marker: {
          color: C.current, size: 14,
          symbol: 'line-ns',
          line: { color: C.current, width: 3 },
        },
        name: 'Current',
        hovertemplate: `Current: ${current.toFixed(2)}<extra></extra>`,
      },
    ];

    const layout = {
      paper_bgcolor: 'transparent',
      plot_bgcolor:  'transparent',
      font: { family: 'inherit', size: 11, color: '#d2cec3' },
      height: 80,
      margin: { t: 4, r: 12, b: 4, l: 12 },
      showlegend: false,
      shapes,
      annotations,
      xaxis: { range: [xMin, xMax], showgrid: false, zeroline: false, showticklabels: true, color: '#555' },
      yaxis: { range: [0, 1], showgrid: false, zeroline: false, showticklabels: false },
      hovermode: 'closest' as const,
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, data as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, logic]);

  void height;

  return (
    <>
      <div className="ia-chart-panel ia-chart-clickable" onClick={() => setDetail(true)}>
        <div className="ia-chart-head">
          <div className="ia-chart-title">{panel.title}</div>
          {panel.degradedLabel && <div className="ia-chart-degraded">{panel.degradedLabel}</div>}
        </div>
        <div ref={ref} style={{ width: '100%' }} />
        {panel.callouts[0] && (
          <div className="ia-chart-notice">{panel.callouts[0].detail}</div>
        )}
      </div>
      {detail && createPortal(
        <div className="ia-chart-detail-overlay" onClick={() => setDetail(false)}>
          <div className="ia-chart-detail-modal" onClick={e => e.stopPropagation()}>
            <button className="ia-chart-detail-close" onClick={() => setDetail(false)}>×</button>
            <RegimePlotlyDetail panel={panel} height={420} />
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}
