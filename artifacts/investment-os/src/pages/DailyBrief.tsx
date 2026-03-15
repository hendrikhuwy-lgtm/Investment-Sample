import { useGetDailyBrief } from "@workspace/api-client-react";
import { MacroSignalCard } from "@/components/MacroSignalCard";
import { FreshnessBadge } from "@/components/FreshnessBadge";
import { Card } from "@/components/ui/card";
import { ArrowUpRight, ArrowDownRight, Minus } from "lucide-react";

export default function DailyBrief() {
  const { data: brief, isLoading, error } = useGetDailyBrief();

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="flex flex-col items-center space-y-4">
          <div className="h-6 w-6 rounded-full border-2 border-primary border-t-transparent animate-spin" />
          <p className="text-[10px] text-muted-foreground font-mono tracking-widest">LOAD_INTELLIGENCE_BRIEF...</p>
        </div>
      </div>
    );
  }

  if (error || !brief) {
    return (
      <div className="p-6 border border-destructive/30 bg-destructive/10 rounded-sm">
        <h2 className="text-destructive font-mono text-sm font-bold">BRIEF_LOAD_FAILED</h2>
        <p className="text-xs text-muted-foreground mt-1">Check API connection or try refreshing.</p>
      </div>
    );
  }

  const leadSignal = brief.macroSignals[0];
  const secondarySignals = brief.macroSignals.slice(1);

  return (
    <div className="flex flex-col space-y-0 pb-12">

      {/* ── Current Market Judgment ───────────────────────── */}
      <section className="border-b border-border/30 pb-5 mb-6">
        <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-muted-foreground mb-3">
          DAILY BRIEF // {brief.briefDate}
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-0 border border-border/30 rounded-sm overflow-hidden mb-4">
          <div className="p-4 border-r border-border/30">
            <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">DOMINANT MOVE</div>
            <p className="text-xs text-foreground leading-relaxed">{leadSignal?.whatHappened?.slice(0, 180)}…</p>
          </div>
          <div className="p-4">
            <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">BASE IMPLICATION</div>
            <p className="text-xs text-foreground leading-relaxed">{leadSignal?.investmentImplication?.slice(0, 180)}…</p>
          </div>
        </div>

        {/* Lead sleeve highlight */}
        <div className="flex items-start gap-3 text-xs">
          <span className="text-[9px] font-bold uppercase tracking-[0.16em] text-muted-foreground mt-0.5 shrink-0">LEAD SLEEVE</span>
          <div className="flex flex-wrap gap-1.5">
            <span className="px-2 py-0.5 bg-primary/15 border border-primary/30 text-primary rounded-[2px] text-[9px] font-semibold tracking-wide">
              Cash / Bills
            </span>
            <span className="px-2 py-0.5 bg-warning/15 border border-warning/30 text-warning rounded-[2px] text-[9px] font-semibold tracking-wide">
              Real Assets
            </span>
            <span className="px-2 py-0.5 bg-muted/20 border border-border/40 text-muted-foreground rounded-[2px] text-[9px] tracking-wide">
              Real assets still look most exposed in current morning note. Benchmark questions remain tied to rates, FX, and regional stress.
            </span>
          </div>
          <span className="text-[9px] text-muted-foreground shrink-0 ml-auto hidden lg:block">
            Next review: Revisit these sleeves after the next refresh.
          </span>
        </div>
      </section>

      {/* ── Key Theme Header ───────────────────────────────── */}
      <section className="mb-5">
        <h1 className="text-[22px] md:text-[28px] font-bold tracking-tight text-foreground leading-tight max-w-3xl mb-3">
          {brief.keyTheme}
        </h1>
        <p className="text-sm text-muted-foreground leading-relaxed max-w-2xl">
          {brief.summaryNarrative}
        </p>
      </section>

      {/* ── Market Snapshot Strip ──────────────────────────── */}
      <section className="mb-6 overflow-x-auto -mx-4 px-4 lg:mx-0 lg:px-0">
        <div className="flex lg:grid lg:grid-cols-4 xl:grid-cols-8 gap-2 min-w-max lg:min-w-0">
          {brief.marketSnapshot.map((point, idx) => {
            const isUp   = point.direction === "up";
            const isDown = point.direction === "down";
            const colorClass = isUp ? "text-success" : isDown ? "text-destructive" : "text-muted-foreground";

            return (
              <div key={idx} className="flex flex-col p-3 bg-secondary/15 border border-border/30 rounded-sm w-[148px] lg:w-auto flex-shrink-0 hover:bg-secondary/30 transition-colors">
                <div className="flex justify-between items-start mb-2">
                  <span className="text-[8px] font-bold uppercase tracking-[0.16em] text-muted-foreground leading-tight truncate pr-1" title={point.label}>
                    {point.label}
                  </span>
                  <FreshnessBadge status={point.freshness} asOf={point.asOf} dotOnly />
                </div>
                <div className="font-mono text-[15px] font-semibold text-foreground leading-none mb-1">{point.value}</div>
                {point.change && (
                  <div className={`flex items-center text-[10px] font-mono ${colorClass}`}>
                    {isUp   && <ArrowUpRight   className="w-2.5 h-2.5 mr-0.5" />}
                    {isDown && <ArrowDownRight  className="w-2.5 h-2.5 mr-0.5" />}
                    {!isUp && !isDown && <Minus className="w-2.5 h-2.5 mr-0.5" />}
                    {point.change}
                  </div>
                )}
                <div className="mt-1">
                  <FreshnessBadge status={point.freshness} asOf={point.asOf} />
                </div>
              </div>
            );
          })}
        </div>
      </section>

      {/* ── Main Two-Column Layout ─────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">

        {/* Left: Lead Signal (full) */}
        <div className="lg:col-span-8 space-y-5">
          <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-muted-foreground border-b border-border/30 pb-2">
            LEAD MARKET SECTION — CHART-BACKED INTERPRETATION
          </div>

          {leadSignal && <MacroSignalCard signal={leadSignal} isLead />}
        </div>

        {/* Right: Portfolio Map + Review Routing */}
        <div className="lg:col-span-4 space-y-4">

          {/* Portfolio Map */}
          <Card className="border-border/40 overflow-hidden">
            <div className="px-4 py-3 border-b border-border/30 bg-secondary/20">
              <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-foreground">PORTFOLIO MAP</div>
              <div className="text-[10px] text-muted-foreground mt-0.5">Where today's note lands</div>
            </div>
            <div className="p-4 space-y-4">

              <div>
                <div className="text-[9px] font-bold uppercase tracking-[0.16em] text-muted-foreground mb-2">HIGH IMPACT</div>
                <div className="flex flex-wrap gap-1.5">
                  <span className="px-2 py-0.5 bg-destructive/15 border border-destructive/30 text-destructive rounded-[2px] text-[9px] font-bold">Cash Bills</span>
                  <span className="px-2 py-0.5 bg-warning/15 border border-warning/30 text-warning rounded-[2px] text-[9px] font-bold">Real Assets</span>
                  <span className="px-2 py-0.5 bg-primary/15 border border-primary/30 text-primary rounded-[2px] text-[9px] font-bold">Alternatives</span>
                </div>
              </div>

              <div className="border-t border-border/30 pt-3">
                <div className="text-[9px] font-bold uppercase tracking-[0.16em] text-muted-foreground mb-2">PORTFOLIO IMPLICATIONS</div>
                <div className="text-[10px] font-semibold text-foreground mb-2">Most affected sleeves and benchmark context</div>
                <div className="space-y-2">
                  {[
                    { sleeve: "Cash Bills",   role: "Primary", note: "Cash Bills matters now because the current morning note still keeps this sleeve in focus while fresher confirmation is pending." },
                    { sleeve: "Real Assets",  role: "Primary", note: "Real Assets matters now because the current morning note still keeps this sleeve in focus while fresher confirmation is pending." },
                    { sleeve: "Alternatives", role: "Primary", note: "Alternatives matters now because the current morning note still keeps this sleeve in focus while fresher confirmation is pending." },
                  ].map(({ sleeve, role, note }) => (
                    <div key={sleeve} className="border border-border/25 rounded-sm p-2.5 bg-secondary/10">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-[10px] font-semibold text-foreground">{sleeve}</span>
                        <span className="text-[8px] font-bold uppercase tracking-[0.12em] text-muted-foreground border border-border/40 px-1.5 py-0.5 rounded-[2px]">{role}</span>
                      </div>
                      <p className="text-[10px] text-muted-foreground leading-snug">{note}</p>
                    </div>
                  ))}
                </div>
              </div>

              <div className="border-t border-border/30 pt-3">
                <div className="text-[9px] font-bold uppercase tracking-[0.16em] text-muted-foreground mb-2">BENCHMARK WATCH</div>
                <ul className="space-y-1.5">
                  {[
                    "Check bond benchmark tolerance against higher yield and credit pressure.",
                    "Check regional benchmark sensitivity to stronger USD and Asia-related stress.",
                    "Singapore rate context remains relevant for local-currency implementation review.",
                  ].map((item, i) => (
                    <li key={i} className="text-[10px] text-muted-foreground flex items-start gap-1.5">
                      <span className="text-primary mt-0.5 shrink-0">•</span>
                      {item}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          </Card>

          {/* Review Routing */}
          <Card className="border-border/40 overflow-hidden">
            <div className="px-4 py-3 border-b border-border/30 bg-secondary/20">
              <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-foreground">WHAT TO DO NEXT</div>
              <div className="text-[10px] text-muted-foreground mt-0.5">Review routing</div>
            </div>
            <div className="divide-y divide-border/25">
              {[
                { action: "Re-check Cash / Bills, Real Assets", note: "Do the next refreshes still keep these sleeves in the lead path?", timing: "IMMEDIATE", timingCls: "text-destructive border-destructive/30" },
                { action: "Use the current morning note as cautious context", note: "Does the next refresh materially change the current top developments?", timing: "MONITOR",   timingCls: "text-muted-foreground border-border/40" },
                { action: "Keep benchmark questions in review scope", note: "Current market context still points to a rates, FX, or regional benchmark review question.", timing: "REVIEW",   timingCls: "text-warning border-warning/30" },
              ].map(({ action, note, timing, timingCls }) => (
                <div key={action} className="p-3 hover:bg-secondary/20 transition-colors">
                  <div className="flex items-start justify-between gap-2 mb-1">
                    <span className="text-[10px] font-semibold text-foreground leading-snug">{action}</span>
                    <span className={`text-[8px] font-bold uppercase tracking-[0.12em] border px-1.5 py-0.5 rounded-[2px] shrink-0 ${timingCls}`}>{timing}</span>
                  </div>
                  <p className="text-[10px] text-muted-foreground leading-snug">{note}</p>
                </div>
              ))}
            </div>
          </Card>
        </div>
      </div>

      {/* ── Signal Scan ────────────────────────────────────── */}
      {secondarySignals.length > 0 && (
        <section className="mt-10 border-t border-border/30 pt-6">
          <div className="mb-4">
            <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-muted-foreground">SIGNAL SCAN</div>
            <div className="text-[11px] text-muted-foreground mt-0.5">Secondary signals worth keeping in view</div>
            <div className="text-[10px] text-muted-foreground/60 mt-0.5">These are supporting reads that can strengthen, weaken, or redirect the dominant market interpretation.</div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {secondarySignals.map((signal) => (
              <div key={signal.id} className="border border-border/30 rounded-sm bg-secondary/10 overflow-hidden hover:bg-secondary/20 transition-colors">
                <div className="px-4 pt-3 pb-2 border-b border-border/25">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <span className="text-[8px] font-bold uppercase tracking-[0.16em] text-muted-foreground">{signal.category}</span>
                      <span className={`text-[8px] font-bold uppercase tracking-[0.1em] px-1.5 py-0.5 rounded-[2px] border ${
                        signal.severity === "high" ? "text-destructive border-destructive/30 bg-destructive/10" : "text-warning border-warning/30 bg-warning/10"
                      }`}>{signal.severity}</span>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-[8px] font-mono border border-border/30 rounded-[2px] px-1.5 py-0.5 text-muted-foreground">TRUST High</span>
                      <FreshnessBadge status={signal.evidence?.[0]?.freshness ?? "latest_available"} asOf={signal.date} />
                    </div>
                  </div>
                  <h4 className="text-[12px] font-semibold text-foreground leading-snug line-clamp-2">{signal.headline}</h4>
                </div>

                <div className="px-4 py-3 grid grid-cols-2 gap-4">
                  <div>
                    <div className="text-[8px] font-bold uppercase tracking-[0.16em] text-muted-foreground mb-1">SIGNAL</div>
                    <p className="text-[10px] text-foreground/80 leading-relaxed line-clamp-3">{signal.whatHappened}</p>
                  </div>
                  <div>
                    <div className="text-[8px] font-bold uppercase tracking-[0.16em] text-muted-foreground mb-1">IMPLICATION</div>
                    <p className="text-[10px] text-foreground/80 leading-relaxed line-clamp-3">{signal.investmentImplication}</p>
                  </div>
                </div>

                <div className="px-4 pb-3 flex items-center justify-between">
                  <div>
                    <div className="text-[8px] font-bold uppercase tracking-[0.16em] text-muted-foreground mb-1">PORTFOLIO RELEVANCE</div>
                    <p className="text-[10px] text-muted-foreground leading-snug line-clamp-2">{signal.whatItMeans?.slice(0, 100)}…</p>
                  </div>
                  <button className="text-[8px] font-bold uppercase tracking-[0.14em] text-primary border border-primary/30 rounded-[2px] px-2 py-1 hover:bg-primary/10 transition-colors shrink-0 ml-4">
                    Open signal detail
                  </button>
                </div>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* ── Freshness Legend ───────────────────────────────── */}
      <div className="mt-8 pt-4 border-t border-border/20 flex flex-wrap gap-4 text-[9px] text-muted-foreground">
        <span className="font-bold uppercase tracking-[0.16em]">FRESHNESS KEY:</span>
        {[
          { label: "CURRENT",   cls: "bg-success" },
          { label: "LATEST",    cls: "bg-primary" },
          { label: "LAGGED",    cls: "bg-warning" },
          { label: "CACHED",    cls: "bg-destructive" },
        ].map(({ label, cls }) => (
          <span key={label} className="flex items-center gap-1.5">
            <span className={`w-1.5 h-1.5 rounded-full ${cls}`} />
            {label}
          </span>
        ))}
        <span className="text-muted-foreground/50">· Evidence, risks, and scenario branches remain in the signal panel so the main note stays readable.</span>
      </div>

    </div>
  );
}
