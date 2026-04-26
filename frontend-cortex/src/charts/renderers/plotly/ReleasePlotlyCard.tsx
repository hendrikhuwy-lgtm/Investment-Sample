import { useState, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import * as Plotly from 'plotly.js-dist-min';
import type { ChartPanelDisplay } from '../../chartTypes';
import { BASE_CONFIG, C } from './plotlyTheme';
import { ReleasePlotlyDetail } from './detail/ReleasePlotlyDetail';

type Props = { panel: ChartPanelDisplay };

export function ReleasePlotlyCard({ panel }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [detail, setDetail] = useState(false);
  const logic = panel.chartLogic;

  useEffect(() => {
    const el = ref.current;
    if (!el || !logic) return;

    const current = logic.currentValue ?? 0;
    const previous = logic.previousValue ?? 0;

    const data = [{
      type: 'indicator',
      mode: 'number+delta',
      value: current,
      delta: {
        reference: previous,
        valueformat: '+.2f',
        // For inflation/release: higher = worse (red), lower = better (green)
        increasing: { color: C.stress },
        decreasing: { color: C.confirm },
        font: { size: 16 },
      },
      number: { valueformat: '.2f', font: { color: C.current, size: 36 } },
      domain: { x: [0, 1], y: [0, 1] },
    }];

    const layout = {
      paper_bgcolor: 'transparent',
      plot_bgcolor:  'transparent',
      font: { family: 'inherit', size: 11, color: '#d2cec3' },
      height: 100,
      margin: { t: 12, r: 12, b: 12, l: 12 },
      showlegend: false,
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, data as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, logic]);

  const releaseDate = logic?.releaseDate
    ? new Date(logic.releaseDate).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
    : null;

  return (
    <>
      <div className="ia-chart-panel ia-chart-clickable" onClick={() => setDetail(true)}>
        <div className="ia-chart-head">
          <div className="ia-chart-title">{panel.title}</div>
          {panel.degradedLabel && <div className="ia-chart-degraded">{panel.degradedLabel}</div>}
        </div>
        <div ref={ref} style={{ width: '100%' }} />
        {releaseDate && <div className="ia-chart-notice">Released {releaseDate}</div>}
      </div>
      {detail && createPortal(
        <div className="ia-chart-detail-overlay" onClick={() => setDetail(false)}>
          <div className="ia-chart-detail-modal" onClick={e => e.stopPropagation()}>
            <button className="ia-chart-detail-close" onClick={() => setDetail(false)}>×</button>
            <ReleasePlotlyDetail panel={panel} height={420} />
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}
