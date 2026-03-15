import { useState } from "react";
import { useGetBlueprint } from "@workspace/api-client-react";
import { CandidateCard } from "@/components/CandidateCard";
import { FreshnessBadge } from "@/components/FreshnessBadge";
import { Card } from "@/components/ui/card";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { ListChecks, LayoutGrid, List } from "lucide-react";

type ViewMode = "full" | "compact";

const READINESS_ORDER: Record<string, number> = { ready: 0, watch: 1, hold: 2, exit: 3 };
const READINESS_STYLES: Record<string, string> = {
  ready: "text-success border-success/30 bg-success/10",
  watch: "text-warning border-warning/30 bg-warning/10",
  hold:  "text-muted-foreground border-border/40 bg-secondary/20",
  exit:  "text-destructive border-destructive/30 bg-destructive/10",
};

export default function Blueprint() {
  const { data: blueprint, isLoading, error } = useGetBlueprint();
  const [viewMode, setViewMode] = useState<ViewMode>("full");

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="flex flex-col items-center space-y-3">
          <div className="h-6 w-6 rounded-full border-2 border-warning border-t-transparent animate-spin" />
          <p className="text-[10px] text-muted-foreground font-mono tracking-widest">COMPILE_BLUEPRINT...</p>
        </div>
      </div>
    );
  }

  if (error || !blueprint) {
    return (
      <div className="p-6 border border-destructive/30 bg-destructive/10 rounded-sm">
        <h2 className="text-destructive font-mono text-sm font-bold">BLUEPRINT_LOAD_FAILED</h2>
        <p className="text-xs text-muted-foreground mt-1">Check API connection or try refreshing.</p>
      </div>
    );
  }

  const { portfolioSummary } = blueprint;

  const sortedCandidates = [...blueprint.candidates].sort((a, b) => {
    const aKey = a.readiness.status.toLowerCase();
    const bKey = b.readiness.status.toLowerCase();
    return (READINESS_ORDER[aKey] ?? 9) - (READINESS_ORDER[bKey] ?? 9);
  });

  const readyCandidates  = sortedCandidates.filter((c) => c.readiness.status.toLowerCase() === "ready");
  const watchCandidates  = sortedCandidates.filter((c) => c.readiness.status.toLowerCase() === "watch");
  const holdCandidates   = sortedCandidates.filter((c) => c.readiness.status.toLowerCase() === "hold");
  const exitCandidates   = sortedCandidates.filter((c) => c.readiness.status.toLowerCase() === "exit");

  const allocationTotal =
    (portfolioSummary.equityWeight ?? 0) +
    (portfolioSummary.bondWeight ?? 0) +
    (portfolioSummary.cashWeight ?? 0);

  return (
    <div className="flex flex-col space-y-6 pb-12 animate-in fade-in duration-300">

      {/* ── Portfolio Summary Bar ──────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">

        {/* Left: Total Value + Allocation */}
        <Card className="lg:col-span-8 p-5 border-border/40 bg-card overflow-hidden">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">

            {/* Total Value */}
            <div>
              <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-muted-foreground mb-1">TOTAL PORTFOLIO VALUE</div>
              <div className="text-2xl font-mono font-bold text-foreground mb-3">{portfolioSummary.totalValue}</div>
              <div className="flex items-center gap-2">
                <FreshnessBadge status={portfolioSummary.freshness} asOf={portfolioSummary.lastUpdated} />
                <span className="text-[9px] text-muted-foreground">As of {portfolioSummary.lastUpdated}</span>
              </div>
            </div>

            {/* Allocation Bar */}
            <div>
              <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-muted-foreground mb-3">TARGET ALLOCATION</div>
              <div className="flex h-2 w-full rounded-sm overflow-hidden bg-secondary/30 mb-2">
                <div
                  style={{ width: `${(portfolioSummary.equityWeight / allocationTotal) * 100}%` }}
                  className="bg-primary"
                  title={`Equity: ${portfolioSummary.equityWeight}%`}
                />
                <div
                  style={{ width: `${(portfolioSummary.bondWeight / allocationTotal) * 100}%` }}
                  className="bg-warning"
                  title={`Bonds: ${portfolioSummary.bondWeight}%`}
                />
                <div
                  style={{ width: `${(portfolioSummary.cashWeight / allocationTotal) * 100}%` }}
                  className="bg-success"
                  title={`Cash: ${portfolioSummary.cashWeight}%`}
                />
              </div>
              <div className="flex items-center gap-4 text-[10px] font-mono text-muted-foreground">
                <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-primary" />Equity {portfolioSummary.equityWeight}%</span>
                <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-warning" />Bonds {portfolioSummary.bondWeight}%</span>
                <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-success" />Cash {portfolioSummary.cashWeight}%</span>
              </div>
            </div>
          </div>
        </Card>

        {/* Right: Decision Board Summary */}
        <Card className="lg:col-span-4 p-5 border-border/40 bg-card overflow-hidden">
          <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-muted-foreground mb-3">DECISION BOARD</div>
          <div className="grid grid-cols-2 gap-2">
            {[
              { label: "READY",   count: readyCandidates.length,  cls: "text-success border-success/30 bg-success/5" },
              { label: "WATCH",   count: watchCandidates.length,   cls: "text-warning border-warning/30 bg-warning/5" },
              { label: "HOLD",    count: holdCandidates.length,    cls: "text-muted-foreground border-border/40 bg-secondary/10" },
              { label: "EXIT",    count: exitCandidates.length,    cls: "text-destructive border-destructive/30 bg-destructive/5" },
            ].map(({ label, count, cls }) => (
              <div key={label} className={`border rounded-sm p-3 text-center ${cls}`}>
                <div className="text-xl font-mono font-bold">{count}</div>
                <div className="text-[8px] font-bold uppercase tracking-[0.16em] mt-0.5">{label}</div>
              </div>
            ))}
          </div>
          <div className="mt-3 text-[10px] text-muted-foreground">
            {sortedCandidates.length} active candidates under review
          </div>
        </Card>
      </div>

      {/* ── Design Principles ──────────────────────────────── */}
      <Accordion type="single" collapsible className="w-full border border-border/30 rounded-sm bg-secondary/10 overflow-hidden">
        <AccordionItem value="principles" className="border-none">
          <AccordionTrigger className="hover:no-underline px-4 py-3.5 hover:bg-secondary/20 transition-colors">
            <div className="flex items-center space-x-2">
              <ListChecks className="w-3.5 h-3.5 text-warning" />
              <span className="text-[10px] font-bold uppercase tracking-[0.18em] text-foreground">Portfolio Design Principles</span>
            </div>
          </AccordionTrigger>
          <AccordionContent className="px-4 pb-4">
            <ul className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {blueprint.designPrinciples.map((principle, i) => (
                <li key={i} className="flex items-start text-xs bg-background/50 p-3 rounded-sm border border-border/30">
                  <span className="text-warning mr-2.5 font-mono text-[10px] mt-0.5">0{i + 1}.</span>
                  <span className="text-muted-foreground leading-relaxed">{principle}</span>
                </li>
              ))}
            </ul>
          </AccordionContent>
        </AccordionItem>
      </Accordion>

      {/* ── Candidate Roster Header ────────────────────────── */}
      <div className="flex items-center justify-between">
        <div>
          <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-muted-foreground">CANDIDATE ROSTER</div>
          <div className="text-[11px] text-muted-foreground mt-0.5">{blueprint.candidates.length} active candidates · Thesis-first view</div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setViewMode("full")}
            className={`p-1.5 rounded-sm border transition-colors ${viewMode === "full" ? "border-primary/40 text-primary bg-primary/10" : "border-border/30 text-muted-foreground hover:text-foreground"}`}
          >
            <LayoutGrid className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={() => setViewMode("compact")}
            className={`p-1.5 rounded-sm border transition-colors ${viewMode === "compact" ? "border-primary/40 text-primary bg-primary/10" : "border-border/30 text-muted-foreground hover:text-foreground"}`}
          >
            <List className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* ── Compact View: Decision Table ───────────────────── */}
      {viewMode === "compact" && (
        <Card className="border-border/40 overflow-hidden">
          <div className="grid grid-cols-[2fr_1.5fr_1fr_1fr_1fr_1fr] border-b border-border/30 bg-secondary/30">
            {["CANDIDATE", "ROLE", "STATUS", "PRICE", "YTD", "POSITION"].map((h) => (
              <div key={h} className="px-3 py-2 text-[9px] font-bold uppercase tracking-[0.16em] text-muted-foreground border-r border-border/20 last:border-r-0">
                {h}
              </div>
            ))}
          </div>
          {sortedCandidates.map((c) => {
            const rKey = c.readiness.status.toLowerCase();
            const rStyle = READINESS_STYLES[rKey] ?? READINESS_STYLES.hold;
            const ytdPos = c.currentStatus.ytdReturn && !c.currentStatus.ytdReturn.startsWith("-");
            return (
              <div key={c.id} className="grid grid-cols-[2fr_1.5fr_1fr_1fr_1fr_1fr] border-b border-border/20 last:border-b-0 hover:bg-secondary/20 transition-colors">
                <div className="px-3 py-3 border-r border-border/20">
                  <div className="font-mono text-xs font-bold text-foreground">{c.ticker}</div>
                  <div className="text-[10px] text-muted-foreground truncate">{c.name}</div>
                </div>
                <div className="px-3 py-3 border-r border-border/20">
                  <div className="text-[10px] text-muted-foreground truncate">{c.assetClass}</div>
                  <div className="text-[9px] text-muted-foreground/60">{c.sector}</div>
                </div>
                <div className="px-3 py-3 border-r border-border/20 flex items-center">
                  <span className={`text-[9px] font-bold uppercase tracking-[0.1em] px-1.5 py-0.5 border rounded-[2px] ${rStyle}`}>
                    {c.readiness.status}
                  </span>
                </div>
                <div className="px-3 py-3 border-r border-border/20">
                  <div className="font-mono text-xs text-foreground">{c.currentStatus.price}</div>
                </div>
                <div className="px-3 py-3 border-r border-border/20">
                  <div className={`font-mono text-xs ${ytdPos ? "text-success" : "text-destructive"}`}>
                    {c.currentStatus.ytdReturn ?? "—"}
                  </div>
                </div>
                <div className="px-3 py-3">
                  <div className="font-mono text-xs text-foreground">{c.currentStatus.positionSize ?? "0%"}</div>
                  {c.allocation && (
                    <div className="text-[9px] text-muted-foreground">Target {c.allocation.target}%</div>
                  )}
                </div>
              </div>
            );
          })}
        </Card>
      )}

      {/* ── Full View: Candidate Cards ─────────────────────── */}
      {viewMode === "full" && (
        <div className="space-y-10">

          {/* Ready */}
          {readyCandidates.length > 0 && (
            <section>
              <div className="flex items-center gap-3 mb-4 pb-2 border-b border-success/20">
                <div className="w-2 h-2 rounded-full bg-success" />
                <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-success">READY — Allocation Candidates</div>
                <div className="text-[9px] text-muted-foreground ml-auto">{readyCandidates.length} candidate{readyCandidates.length !== 1 ? "s" : ""}</div>
              </div>
              <div className="space-y-5">
                {readyCandidates.map((c) => <CandidateCard key={c.id} candidate={c} />)}
              </div>
            </section>
          )}

          {/* Watch */}
          {watchCandidates.length > 0 && (
            <section>
              <div className="flex items-center gap-3 mb-4 pb-2 border-b border-warning/20">
                <div className="w-2 h-2 rounded-full bg-warning" />
                <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-warning">WATCH — Research, Do Not Act</div>
                <div className="text-[9px] text-muted-foreground ml-auto">{watchCandidates.length} candidate{watchCandidates.length !== 1 ? "s" : ""}</div>
              </div>
              <div className="space-y-5">
                {watchCandidates.map((c) => <CandidateCard key={c.id} candidate={c} />)}
              </div>
            </section>
          )}

          {/* Hold */}
          {holdCandidates.length > 0 && (
            <section>
              <div className="flex items-center gap-3 mb-4 pb-2 border-b border-border/30">
                <div className="w-2 h-2 rounded-full bg-muted-foreground" />
                <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-muted-foreground">HOLD — No Action</div>
                <div className="text-[9px] text-muted-foreground ml-auto">{holdCandidates.length} candidate{holdCandidates.length !== 1 ? "s" : ""}</div>
              </div>
              <div className="space-y-5">
                {holdCandidates.map((c) => <CandidateCard key={c.id} candidate={c} />)}
              </div>
            </section>
          )}

          {/* Exit */}
          {exitCandidates.length > 0 && (
            <section>
              <div className="flex items-center gap-3 mb-4 pb-2 border-b border-destructive/20">
                <div className="w-2 h-2 rounded-full bg-destructive" />
                <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-destructive">EXIT — Do Not Use</div>
                <div className="text-[9px] text-muted-foreground ml-auto">{exitCandidates.length} candidate{exitCandidates.length !== 1 ? "s" : ""}</div>
              </div>
              <div className="space-y-5">
                {exitCandidates.map((c) => <CandidateCard key={c.id} candidate={c} />)}
              </div>
            </section>
          )}

        </div>
      )}

      {/* ── Footer Note ────────────────────────────────────── */}
      <div className="pt-6 border-t border-border/20 text-[9px] text-muted-foreground leading-relaxed max-w-2xl">
        This blueprint is a personalized investment workbench. All decisions are subject to personal suitability review. Evidence items are dated and sourced — any claim lacking a citation should be treated as interpretive context, not confirmed fact.
      </div>

    </div>
  );
}
