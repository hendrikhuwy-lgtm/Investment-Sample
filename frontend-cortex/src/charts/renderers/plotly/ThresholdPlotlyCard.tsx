import { useState, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import * as Plotly from 'plotly.js-dist-min';
import type { ChartPanelDisplay } from '../../chartTypes';
import { BASE_CONFIG, C } from './plotlyTheme';
import { ThresholdPlotlyDetail } from './detail/ThresholdPlotlyDetail';

type Props = { panel: ChartPanelDisplay; height?: number };

export function ThresholdPlotlyCard({ panel, height = 220 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [detail, setDetail] = useState(false);
  const logic = panel.chartLogic;

  useEffect(() => {
    const el = ref.current;
    if (!el || !logic) return;

    const current = logic.currentValue ?? 0;
    const previous = logic.previousValue ?? 0;
    const trigger = logic.triggerLevel ?? 0;
    const confirmAbove = logic.confirmAbove ?? true;

    const allVals = [current, previous, trigger];
    const minV = Math.min(...allVals);
    const maxV = Math.max(...allVals);
    const span = maxV - minV || Math.abs(trigger) || 1;
    const axisMin = minV - span * 0.25;
    const axisMax = maxV + span * 0.25;

    const isAbove = current >= trigger;
    const statusColor = confirmAbove
      ? (isAbove ? C.confirm : C.stress)
      : (isAbove ? C.stress : C.confirm);

    const data = [{
      type: 'indicator',
      mode: 'number+gauge+delta',
      value: current,
      delta: {
        reference: previous,
        valueformat: '.2f',
        increasing: { color: confirmAbove ? C.confirm : C.stress },
        decreasing: { color: confirmAbove ? C.stress : C.confirm },
      },
      number: { valueformat: '.2f', font: { color: statusColor, size: 28 } },
      gauge: {
        shape: 'bullet',
        axis: { range: [axisMin, axisMax], tickfont: { size: 9, color: '#666' } },
        threshold: { line: { color: C.watch, width: 3 }, thickness: 0.75, value: trigger },
        steps: [
          { range: [axisMin, trigger], color: `${confirmAbove ? C.stress : C.confirm}22` },
          { range: [trigger, axisMax], color: `${confirmAbove ? C.confirm : C.stress}22` },
        ],
        bar: { color: C.current, thickness: 0.35 },
      },
    }];

    const layout = {
      paper_bgcolor: 'transparent',
      plot_bgcolor:  'transparent',
      font: { family: 'inherit', size: 11, color: '#d2cec3' },
      height: 90,
      margin: { t: 12, r: 16, b: 8, l: 80 },
      showlegend: false,
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Plotly.react(el, data as any, layout as any, BASE_CONFIG as any);
    const ro = new ResizeObserver(() => Plotly.relayout(el, { autosize: true } as any));
    ro.observe(el);
    return () => { ro.disconnect(); Plotly.purge(el); };
  }, [panel, logic]);

  void height; // height drives detail modal
  const action = panel.callouts[1]?.detail ?? '';

  return (
    <>
      <div className="ia-chart-panel ia-chart-clickable" onClick={() => setDetail(true)}>
        <div className="ia-chart-head">
          <div className="ia-chart-title">{panel.title}</div>
          {panel.degradedLabel && <div className="ia-chart-degraded">{panel.degradedLabel}</div>}
        </div>
        <div ref={ref} style={{ width: '100%' }} />
        {action && <div className="ia-chart-notice">{action}</div>}
      </div>
      {detail && createPortal(
        <div className="ia-chart-detail-overlay" onClick={() => setDetail(false)}>
          <div className="ia-chart-detail-modal" onClick={e => e.stopPropagation()}>
            <button className="ia-chart-detail-close" onClick={() => setDetail(false)}>×</button>
            <ThresholdPlotlyDetail panel={panel} height={420} />
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}
