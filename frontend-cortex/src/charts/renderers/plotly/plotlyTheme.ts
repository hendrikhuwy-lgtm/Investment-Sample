import type { Layout, Config } from 'plotly.js';

export const C = {
  confirm:  '#5a8f5a',
  watch:    '#c4923c',
  stress:   '#bf6464',
  prior:    '#7a9cb8',
  current:  '#c4923c',
  neutral:  '#555555',
  textSoft: '#888',
  bg:       'transparent',
} as const;

export const FILL = { light: '22', medium: '44', active: '66' } as const;

export const BASE_LAYOUT: Partial<Layout> = {
  paper_bgcolor: 'transparent',
  plot_bgcolor:  'transparent',
  font: { family: 'inherit', size: 11, color: '#d2cec3' },
  margin: { t: 8, r: 8, b: 8, l: 8 },
  showlegend: false,
  hovermode: 'closest',
  xaxis: { showgrid: false, zeroline: false, color: '#555' },
  yaxis: { showgrid: false, zeroline: false, color: '#555', gridcolor: 'rgba(255,255,255,0.05)' },
};

export const BASE_CONFIG: Partial<Config> = {
  displayModeBar: false,
  responsive: true,
  staticPlot: false,
};

export const BAND_COLOR: Record<string, string> = {
  calm:            C.confirm,
  tight:           C.confirm,
  soft:            C.confirm,
  'risk-on':       C.confirm,
  watch:           C.watch,
  elevated:        C.watch,
  strong:          C.watch,
  'risk-off':      C.stress,
  stress:          C.stress,
  stressed:        C.stress,
  neutral:         C.neutral,
  normal:          C.neutral,
  underperforming: '#7a5a5a',
};

export function bandColor(label: string): string {
  const key = label.toLowerCase();
  return BAND_COLOR[key] ?? C.neutral;
}
