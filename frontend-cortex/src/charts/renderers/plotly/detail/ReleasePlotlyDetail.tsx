import { useEffect, useRef } from 'react';
import * as Plotly from 'plotly.js-dist-min';
import type { ChartPanelDisplay } from '../../../chartTypes';
import { BASE_LAYOUT, BASE_CONFIG, C } from '../plotlyTheme';

type Props = { panel: ChartPanelDisplay; height?: number };

export function ReleasePlotlyDetail({ panel, height = 420 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const logic = panel.chartLogic;

  useEffect(() => {
    const el = ref.current;
    if (!el || !logic) return;

    const historyPoints = panel.primarySeries?.points ?? [];
    const dates = historyPoints.map(p => p.time);
    const values = historyPoints.map(p => p.value);

    if (!dates.length) return;

    const previous = logic.previousValue ?? (values.length > 1 ? values[values.length - 2] : null);

    // Bar colors: red if higher than prior (inflation rising), green if falling
    const barColors = values.map((v, i) => {
      const prior = i > 0 ? values[i - 1] : previous;
      if (prior === null) return C.neutral;
      return v > prior ? C.stress : C.confirm;
    });

    const data: object[] = [
      // Bars (stems)
      {
        type: 'bar',
        x: dates,
        y: values,
        marker: { color: barColors },
        name: panel.primarySeries?.label ?? 'Release',
        hovertemplate: '%{x}: %{y:.2f}<extra></extra>',
      },
      // Lollipop heads
      {
        type: 'scatter',
        mode: 'markers',
        x: dates,
        y: values,
        marker: { color: barColors, size: 7, symbol: 'circle' },
        showlegend: false,
        hoverinfo: 'skip',
      },
    ];

    const annotations: object[] = [];
    if (logic.releaseDate) {
      annotations.push({
        text: `Latest release`,
        x: logic.releaseDate, y: 1, xref: 'x', yref: 'paper',
        yanchor: 'top', showarrow: true, arrowhead: 1, arrowcolor: C.watch,
        font: { color: C.watch, size: 10 },
      });
    }

    const shapes: object[] = [];
    if (previous !== null) {
      shapes.push({
        type: 'line',
        x0: 0, x1: 1, xref: 'paper',
        y0: previous, y1: previous, yref: 'y',
        line: { color: C.prior, width: 1, dash: 'dot' },
      });
    }

    const layout = {
      ...BASE_LAYOUT,
      height,
      title: { text: panel.title, font: { size: 13, color: '#d2cec3' }, x: 0 },
      shapes,
      annotations,
      bargap: 0.3,
      margin: { t: 36, r: 20, b: 40, l: 50 },
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
    </div>
  );
}
