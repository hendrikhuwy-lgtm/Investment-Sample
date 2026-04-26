import type { DailyBriefChartStrip } from "../../../shared/v2_surface_contracts";

type Props = {
  strip: DailyBriefChartStrip;
};

function stripStatusClass(status: string) {
  if (status === "confirming") return "confirming";
  if (status === "resisting") return "resisting";
  if (status === "missing") return "missing";
  return "neutral";
}

function stripDirectionLabel(direction: string) {
  if (direction === "up") return "Up";
  if (direction === "down") return "Down";
  return "Flat";
}

function stripStatusLabel(status: string) {
  if (status === "confirming") return "Confirming";
  if (status === "resisting") return "Resisting";
  if (status === "missing") return "Missing";
  return "Neutral";
}

export function SignalStrip({ strip }: Props) {
  if (!strip.items.length) return null;
  return (
    <div className="brief-decision-strip">
      {(strip.title || strip.question) ? (
        <div className="brief-decision-strip-header">
          {strip.title ? <div className="brief-decision-strip-title">{strip.title}</div> : null}
          {strip.question ? <div className="brief-decision-strip-question">{strip.question}</div> : null}
        </div>
      ) : null}
      <div className="brief-decision-strip-items">
        {strip.items.map((item) => (
          <div className={`brief-decision-strip-item ${stripStatusClass(item.status)}`} key={item.item_id}>
            <div className="brief-decision-strip-item-top">
              <span className="brief-decision-strip-item-label">{item.label}</span>
              <span className={`brief-decision-strip-item-status ${stripStatusClass(item.status)}`}>{stripStatusLabel(item.status)}</span>
            </div>
            <div className="brief-decision-strip-item-meta">
              {item.status !== "missing" ? <span>{stripDirectionLabel(item.direction)}</span> : <span>No path</span>}
              {item.value_label ? <span>{item.value_label}</span> : null}
            </div>
            {item.note ? <div className="brief-decision-strip-item-note">{item.note}</div> : null}
          </div>
        ))}
      </div>
    </div>
  );
}
