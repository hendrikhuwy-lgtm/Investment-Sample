import { useEffect, useRef } from 'react';
import * as Plotly from 'plotly.js-dist-min';
import type { ChartPanelDisplay } from '../../chartTypes';
import { BASE_LAYOUT, BASE_CONFIG } from './plotlyTheme';

type Props = { panel: ChartPanelDisplay; height?: number };

export function DegradedPlotlyFigure({ panel, height = 220 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const layout = {
      ...BASE_LAYOUT,
      height,
      annotations: [{
        text: `<b>${panel.title}</b><br><span style="color:#666;font-size:10px">${panel.degradedLabel ?? 'Data unavailable'}</span>`,
        x: 0.5, y: 0.5, xref: 'paper', yref: 'paper',
        showarrow: false,
        align: 'center',
        font: { color: '#888', size: 13 },
      }],
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, [] as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, height]);

  return (
    <div className="ia-chart-panel">
      <div ref={ref} style={{ width: '100%' }} />
    </div>
  );
}
