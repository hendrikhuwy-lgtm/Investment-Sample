import { useEffect, useRef } from 'react';
import * as Plotly from 'plotly.js-dist-min';
import type { ChartPanelDisplay } from '../../../chartTypes';
import { BASE_LAYOUT, BASE_CONFIG, C } from '../plotlyTheme';

type Props = { panel: ChartPanelDisplay; height?: number };

export function ThresholdPlotlyDetail({ panel, height = 420 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const logic = panel.chartLogic;

  useEffect(() => {
    const el = ref.current;
    if (!el || !logic) return;

    const current = logic.currentValue ?? 0;
    const previous = logic.previousValue ?? 0;
    const trigger = logic.triggerLevel ?? 0;
    const confirmAbove = logic.confirmAbove ?? true;

    const historyPoints = panel.primarySeries?.points ?? [];
    const dates = historyPoints.map(p => p.time);
    const values = historyPoints.map(p => p.value);

    const allVals = [...values, current, previous, trigger].filter(isFinite);
    const yMin = allVals.length ? Math.min(...allVals) : 0;
    const yMax = allVals.length ? Math.max(...allVals) : 1;
    const ySpan = yMax - yMin || Math.abs(trigger) || 1;
    const axisYMin = yMin - ySpan * 0.1;
    const axisYMax = yMax + ySpan * 0.1;

    const xRange = dates.length >= 2 ? [dates[0], dates[dates.length - 1]] : undefined;

    const shapes: object[] = [];
    // Zone rects
    if (xRange) {
      shapes.push({
        type: 'rect',
        x0: xRange[0], x1: xRange[1],
        y0: trigger, y1: axisYMax,
        xref: 'x', yref: 'y',
        fillcolor: `${confirmAbove ? C.confirm : C.stress}18`,
        line: { width: 0 },
        layer: 'below',
      });
      shapes.push({
        type: 'rect',
        x0: xRange[0], x1: xRange[1],
        y0: axisYMin, y1: trigger,
        xref: 'x', yref: 'y',
        fillcolor: `${confirmAbove ? C.stress : C.confirm}18`,
        line: { width: 0 },
        layer: 'below',
      });
    }
    // Trigger line
    shapes.push({
      type: 'line',
      x0: 0, x1: 1, xref: 'paper',
      y0: trigger, y1: trigger, yref: 'y',
      line: { color: C.watch, width: 1.5, dash: 'dash' },
    });

    const data: object[] = [];
    // History line
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
    // Prior value marker
    if (logic.previousValue !== null && dates.length) {
      data.push({
        type: 'scatter',
        mode: 'markers',
        x: [dates[Math.max(0, dates.length - 2)]],
        y: [previous],
        marker: { color: C.prior, size: 8, symbol: 'circle-open', line: { color: C.prior, width: 2 } },
        name: 'Prior',
        hovertemplate: 'Prior: %{y:.2f}<extra></extra>',
      });
    }
    // Current value marker
    if (logic.currentValue !== null && dates.length) {
      data.push({
        type: 'scatter',
        mode: 'markers',
        x: [dates[dates.length - 1]],
        y: [current],
        marker: { color: C.current, size: 12, symbol: 'circle-open', line: { color: C.current, width: 2.5 } },
        name: 'Current',
        hovertemplate: 'Current: %{y:.2f}<extra></extra>',
      });
    }

    const actionText = panel.callouts[1]?.detail ?? '';
    const annotations: object[] = [
      {
        text: `Trigger: ${trigger.toFixed(2)}`,
        x: 1, y: trigger, xref: 'paper', yref: 'y',
        xanchor: 'right', yanchor: 'bottom',
        showarrow: false,
        font: { color: C.watch, size: 10 },
      },
    ];
    if (actionText) {
      annotations.push({
        text: actionText,
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
      margin: { t: 36, r: 20, b: actionText ? 48 : 20, l: 50 },
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
      {panel.callouts[0] && (
        <div className="ia-chart-threshold" style={{ marginTop: 8 }}>
          <strong>{panel.callouts[0].label}</strong>
          <span>{panel.callouts[0].detail}</span>
        </div>
      )}
    </div>
  );
}
