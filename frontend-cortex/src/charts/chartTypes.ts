export type ChartSeriesDisplayType = "line" | "area" | "histogram";

export type ChartPointDisplay = {
  time: string;
  value: number;
};

export type ChartSeriesDisplay = {
  id: string;
  type: ChartSeriesDisplayType;
  label: string;
  points: ChartPointDisplay[];
  unit: string;
  sourceFamily: string;
  sourceLabel: string;
  freshnessLabel: string;
  trustLabel: string;
};

export type ChartBandDisplay = {
  id: string;
  label: string;
  upper: ChartPointDisplay[];
  lower: ChartPointDisplay[];
  meaning: string;
  degradedLabel: string | null;
};

export type ChartMarkerDisplay = {
  id: string;
  time: string;
  label: string;
  type: string;
  summary: string;
};

export type ChartThresholdDisplay = {
  id: string;
  label: string;
  value: number;
  type: string;
  actionIfCrossed: string;
  whatItMeans: string;
};

export type ChartCalloutDisplay = {
  id: string;
  label: string;
  tone: string;
  detail: string;
};

export type DecompositionBarDisplay = {
  label: string;
  value: number;
  target: number;
  low: number;
  high: number;
  unit: string;
};

export type ChartLogicDisplay = {
  currentValue: number | null;
  previousValue: number | null;
  triggerLevel: number | null;
  confirmAbove: boolean | null;
  breakBelow: boolean | null;
  bands: Array<{ label: string; min: number | null; max: number | null }>;
  currentBand: string | null;
  releaseDate: string | null;
  asOfDate: string;
  allocationBars: DecompositionBarDisplay[];
};

export type ChartPanelDisplay = {
  id: string;
  title: string;
  chartType: string;
  chartMode: string;
  inferredMode: string;
  chartLogic: ChartLogicDisplay | null;
  primarySeries: ChartSeriesDisplay | null;
  comparisonSeries: ChartSeriesDisplay | null;
  bands: ChartBandDisplay[];
  markers: ChartMarkerDisplay[];
  thresholds: ChartThresholdDisplay[];
  callouts: ChartCalloutDisplay[];
  summary: string;
  whatToNotice: string;
  freshnessLabel: string;
  trustLabel: string;
  degradedLabel: string | null;
  hideTimeScale: boolean;
};
