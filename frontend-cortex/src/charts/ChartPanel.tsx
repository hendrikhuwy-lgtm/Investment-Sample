import type { ChartPanelDisplay } from "./chartTypes";
import { ThresholdPlotlyCard }      from "./renderers/plotly/ThresholdPlotlyCard";
import { RegimePlotlyCard }          from "./renderers/plotly/RegimePlotlyCard";
import { ReleasePlotlyCard }         from "./renderers/plotly/ReleasePlotlyCard";
import { ComparisonPlotlyChart }     from "./renderers/plotly/ComparisonPlotlyChart";
import { ForecastPlotlyChart }       from "./renderers/plotly/ForecastPlotlyChart";
import { DecompositionPlotlyChart }  from "./renderers/plotly/DecompositionPlotlyChart";
import { DegradedPlotlyFigure }      from "./renderers/plotly/DegradedPlotlyFigure";

type Props = {
  panel: ChartPanelDisplay;
  height?: number;
};

export function ChartPanel({ panel, height = 220 }: Props) {
  if (panel.degradedLabel) return <DegradedPlotlyFigure panel={panel} height={height} />;

  const mode = panel.inferredMode;

  if (mode === "threshold")     return <ThresholdPlotlyCard panel={panel} height={height} />;
  if (mode === "regime")        return <RegimePlotlyCard panel={panel} height={height} />;
  if (mode === "release")       return <ReleasePlotlyCard panel={panel} />;
  if (mode === "decomposition") return <DecompositionPlotlyChart panel={panel} height={height} />;
  if (mode === "forecast")      return <ForecastPlotlyChart panel={panel} height={height} />;

  return <ComparisonPlotlyChart panel={panel} height={height} />;
}
