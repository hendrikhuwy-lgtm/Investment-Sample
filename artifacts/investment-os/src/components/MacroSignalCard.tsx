import { useState } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { FreshnessBadge } from "./FreshnessBadge";
import { EvidencePanel } from "./EvidencePanel";
import { ChevronDown, ChevronRight } from "lucide-react";
import type { MacroSignal } from "@workspace/api-client-react/src/generated/api.schemas";

const SEVERITY_STYLES: Record<string, string> = {
  high: "bg-destructive/15 text-destructive border-destructive/30",
  medium: "bg-warning/15 text-warning border-warning/30",
  low: "bg-muted/30 text-muted-foreground border-border/40",
};

const LEAD_BORDER: Record<string, string> = {
  high: "border-l-destructive",
  medium: "border-l-warning",
  low: "border-l-border",
};

export function MacroSignalCard({ signal, isLead = false }: { signal: MacroSignal; isLead?: boolean }) {
  const [scenarioOpen, setScenarioOpen] = useState(false);

  const sevKey = signal.severity?.toLowerCase() ?? "low";
  const severityStyle = SEVERITY_STYLES[sevKey] ?? SEVERITY_STYLES.low;
  const leadBorder = LEAD_BORDER[sevKey] ?? "border-l-border";

  return (
    <Card className={`flex flex-col bg-card border-border/50 overflow-hidden border-l-2 ${leadBorder}`}>

      {/* ── Header ─────────────────────────────────────── */}
      <div className="px-5 pt-4 pb-3 border-b border-border/30">
        <div className="flex items-start justify-between gap-3 mb-3">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
              {signal.category}
            </span>
            <span className={`text-[9px] font-bold uppercase tracking-[0.12em] px-2 py-0.5 rounded-[2px] border ${severityStyle}`}>
              {signal.severity}
            </span>
            {isLead && (
              <span className="text-[9px] font-bold uppercase tracking-[0.12em] px-2 py-0.5 rounded-[2px] border bg-primary/10 text-primary border-primary/30">
                LEAD SIGNAL
              </span>
            )}
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <span className="text-[9px] font-mono border border-border/40 rounded-[2px] px-2 py-0.5 text-muted-foreground">
              TRUST HIGH
            </span>
            <FreshnessBadge status={signal.evidence?.[0]?.freshness ?? "latest_available"} asOf={signal.date} />
          </div>
        </div>
        <h2 className="text-[15px] font-semibold leading-snug text-foreground">
          {signal.headline}
        </h2>
      </div>

      {/* ── Lead Interpretation Banner ─────────────────── */}
      <div className="mx-5 mt-4 mb-0 p-4 bg-secondary/30 border border-border/40 rounded-sm">
        <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">
          LEAD INTERPRETATION
        </div>
        <p className="text-sm font-semibold text-foreground leading-snug">
          {signal.headline}
        </p>
        {signal.evidence?.[0] && (
          <div className="flex items-center gap-2 mt-2">
            <FreshnessBadge status={signal.evidence[0].freshness} asOf={signal.evidence[0].date} />
          </div>
        )}
      </div>

      {/* ── 2×2 Analysis Grid ──────────────────────────── */}
      <div className="p-5 grid grid-cols-2 gap-0 border border-border/30 rounded-sm mx-5 mt-4 overflow-hidden">

        {/* WHAT HAPPENED */}
        <div className="p-4 border-r border-b border-border/30 space-y-2">
          <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
            WHAT HAPPENED
          </div>
          <p className="text-xs leading-relaxed text-foreground/90">
            {signal.whatHappened}
          </p>
        </div>

        {/* WHAT IT USUALLY MEANS */}
        <div className="p-4 border-b border-border/30 space-y-2">
          <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
            WHAT IT USUALLY MEANS
          </div>
          <p className="text-xs leading-relaxed text-foreground/90">
            {signal.whatItMeans}
          </p>
        </div>

        {/* WHY IT MATTERS HERE */}
        <div className="p-4 border-r border-border/30 space-y-2">
          <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
            WHY IT MATTERS HERE
          </div>
          <p className="text-xs leading-relaxed text-foreground/90">
            {signal.investmentImplication}
          </p>
        </div>

        {/* WHAT TO DO NEXT */}
        <div className="p-4 space-y-2">
          <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
            WHAT TO DO NEXT
          </div>
          {signal.reviewAction ? (
            <p className="text-xs leading-relaxed text-foreground/90">{signal.reviewAction}</p>
          ) : (
            <p className="text-xs text-muted-foreground italic">Monitor next, not act.</p>
          )}
        </div>
      </div>

      {/* ── Boundary ───────────────────────────────────── */}
      <div className="mx-5 mt-4 p-3 border border-border/25 rounded-sm bg-secondary/10">
        <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-1">
          BOUNDARY — WHAT WOULD CHANGE THIS VIEW
        </div>
        <p className="text-xs italic text-muted-foreground leading-relaxed">{signal.boundary}</p>
      </div>

      {/* ── Review Action Box ──────────────────────────── */}
      {signal.reviewAction && (
        <div className="mx-5 mt-3 p-3 border border-primary/25 bg-primary/5 rounded-sm flex items-start justify-between gap-3">
          <div>
            <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-primary mb-1">REVIEW ACTION</div>
            <p className="text-xs text-foreground leading-snug">{signal.reviewAction}</p>
          </div>
          <span className="text-[9px] font-bold uppercase tracking-[0.1em] text-primary bg-primary/10 border border-primary/20 px-2 py-0.5 rounded-[2px] shrink-0">
            NEAR TERM
          </span>
        </div>
      )}

      {/* ── Scenario Path ──────────────────────────────── */}
      <div className="mx-5 mt-3">
        <button
          onClick={() => setScenarioOpen((v) => !v)}
          className="flex items-center gap-1.5 text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground hover:text-foreground transition-colors py-2"
        >
          {scenarioOpen ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
          SCENARIO PATH — HOW THIS COULD PLAY OUT
        </button>
        {scenarioOpen && (
          <div className="grid grid-cols-3 gap-0 border border-border/30 rounded-sm overflow-hidden mb-2">
            <div className="p-3 border-r border-border/30">
              <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-muted-foreground mb-2">BASE CASE</div>
              <p className="text-xs text-foreground/80 leading-relaxed">
                {signal.investmentImplication.slice(0, 140)}… Treat as cautious review context for affected sleeves.
              </p>
            </div>
            <div className="p-3 border-r border-border/30">
              <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-success mb-2">STRONGER CASE</div>
              <p className="text-xs text-foreground/80 leading-relaxed">
                If signals reinforce, the move should remain in the lead path with stronger confidence.
              </p>
            </div>
            <div className="p-3">
              <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-destructive mb-2">WEAKER CASE</div>
              <p className="text-xs text-foreground/80 leading-relaxed">
                If signals show reversal, this development should be retired from the lead path.
              </p>
            </div>
          </div>
        )}
      </div>

      {/* ── Evidence ───────────────────────────────────── */}
      <div className="px-5 pb-5 pt-2">
        <EvidencePanel evidence={signal.evidence} />
      </div>
    </Card>
  );
}
