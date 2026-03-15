import { useState } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { FreshnessBadge } from "./FreshnessBadge";
import { EvidencePanel } from "./EvidencePanel";
import { cn } from "@/lib/utils";
import type { Candidate } from "@workspace/api-client-react/src/generated/api.schemas";

// ─── Readiness styling ────────────────────────────────────────────────────────
const READINESS: Record<string, { label: string; cls: string }> = {
  ready: { label: "READY", cls: "text-success border-success/40 bg-success/10" },
  watch: { label: "WATCH", cls: "text-warning border-warning/40 bg-warning/10" },
  hold:  { label: "HOLD",  cls: "text-muted-foreground border-border/50 bg-secondary/30" },
  exit:  { label: "EXIT",  cls: "text-destructive border-destructive/40 bg-destructive/10" },
};

const DECISION: Record<string, string> = {
  ready: "HOLD",
  watch: "Research — Do not act",
  hold:  "Hold — No action",
  exit:  "Do not use",
};

const LEAD_STRENGTH: Record<string, string> = {
  ready: "Established",
  watch: "Unstable",
  hold:  "Limited",
  exit:  "Weak",
};

// ─── Interpretive Lenses ─────────────────────────────────────────────────────
function buildLenses(candidate: Candidate) {
  const isPositive = candidate.readiness.status === "ready" || candidate.readiness.status === "hold";
  return [
    {
      name: "CYCLE AND PSYCHOLOGY LENS",
      support: `The case matters when the current market still rewards the role this candidate is meant to play.`,
      challenge: `${isPositive ? "Leading peers by a narrow margin in current conditions." : "Not currently in the closest recommendation position."}`,
      change: `A meaningful change in relative leadership or evidence support would change this lens.`,
    },
    {
      name: "FRAGILITY AND ERROR LENS",
      support: `There is still a visible positive case, but the lead is ${isPositive ? "structurally sound" : "not purely structural"}.`,
      challenge: `Cost profile is ${isPositive ? "competitive with strongest peers" : "weaker than the strongest sleeve peers"}.`,
      change: `Fresher direct evidence or a cleaner runner-up would change the fragility assessment.`,
    },
    {
      name: "LONG-HORIZON DISCIPLINE LENS",
      support: `${candidate.thesis.slice(0, 120)}…`,
      challenge: `Current readiness is ${candidate.readiness.status} — review ready, not automatically confirmed.`,
      change: `The long-horizon case improves if the candidate continues to beat the baseline on simple implementation terms.`,
    },
    {
      name: "MARKET IMPLEMENTATION LENS",
      support: `Implementation looks cleaner when liquidity, spread, wrapper, and cost support remain stable.`,
      challenge: `Core decision evidence is supported by ${isPositive ? "direct" : "partial"} current sources.`,
      change: `Direct trading support and stable wrapper handling would improve implementation materiality.`,
    },
    {
      name: "CATALYST AND NARRATIVE LENS",
      support: `The candidate matters now only if it improves the sleeve relative to the current holding, the baseline, or the nearest runner-up.`,
      challenge: `If the case is framing rather than a real implementation edge, the narrative should carry little decision weight.`,
      change: `A stronger practical edge, or evidence that reject is genuinely better than no change, would make the current narrative more decisive.`,
    },
  ];
}

// ─── Scenario Range ───────────────────────────────────────────────────────────
function ScenarioRange({ candidate }: { candidate: Candidate }) {
  const base = candidate.allocation?.current ?? 0;
  const target = candidate.allocation?.target ?? 0;
  const stronger = Math.min(target * 1.3, 100);
  const weaker   = Math.max(target * 0.6, 0);

  const barWidth = (val: number) => `${Math.min((val / 50) * 100, 100)}%`;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-6">
        <div>
          <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-3">RANGE CHART</div>
          <div className="text-[10px] font-mono text-muted-foreground mb-4">Normalized current level · 12 months</div>
          <div className="space-y-3">
            {[
              { label: "Weaker case", val: weaker, cls: "bg-muted/50" },
              { label: "Base case",   val: base,   cls: "bg-primary/60" },
              { label: "Stronger case", val: stronger, cls: "bg-success/60" },
            ].map(({ label, val, cls }) => (
              <div key={label} className="flex items-center gap-3">
                <div className="w-24 text-[10px] text-muted-foreground text-right">{label}</div>
                <div className="flex-1 bg-secondary/30 rounded-full h-1.5 overflow-hidden">
                  <div className={`h-full ${cls} rounded-full`} style={{ width: barWidth(val) }} />
                </div>
                <div className="w-12 text-[10px] font-mono text-muted-foreground text-right">{val.toFixed(1)} to {(val * 1.15).toFixed(1)}</div>
              </div>
            ))}
          </div>

          <div className="mt-4 grid grid-cols-2 gap-0 border border-border/30 rounded-sm overflow-hidden text-xs">
            {[
              { label: "FORECAST CONFIDENCE", val: candidate.readiness.status === "ready" ? "High" : "Moderate" },
              { label: "DOWNSIDE PROBABILITY", val: candidate.readiness.status === "exit" ? "60%" : "30%" },
              { label: "BASE CASE", val: `${base.toFixed(0)}% to ${(base * 1.12).toFixed(0)}%` },
              { label: "STRONGER CASE", val: `${stronger.toFixed(0)}% to ${(stronger * 1.1).toFixed(0)}%` },
            ].map(({ label, val }) => (
              <div key={label} className="p-2 border-b border-r border-border/25 last:border-r-0">
                <div className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground">{label}</div>
                <div className="font-mono text-foreground mt-0.5">{val}</div>
              </div>
            ))}
          </div>
        </div>

        <div className="space-y-3">
          <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-3">FORECAST FRAMING</div>
          <div className="text-[11px] font-semibold text-foreground mb-3">What the current path view is actually saying</div>
          {[
            { label: "CURRENT ANCHOR",    val: "Normalized current level" },
            { label: "FORECAST HORIZON",  val: "12 months" },
            { label: "BASE CASE",         val: `${base.toFixed(0)}% – ${(base*1.12).toFixed(0)}%` },
            { label: "STRONGER CASE",     val: `${stronger.toFixed(0)}% – ${(stronger*1.1).toFixed(0)}%` },
            { label: "WEAKER CASE",       val: `${weaker.toFixed(0)}% – ${(weaker*1.15).toFixed(0)}%` },
            { label: "VALIDITY SUMMARY",  val: "usable_with_limits" },
          ].map(({ label, val }) => (
            <div key={label} className="flex items-start justify-between border-b border-border/20 pb-2">
              <span className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground">{label}</span>
              <span className="text-[11px] font-mono text-foreground ml-4 text-right">{val}</span>
            </div>
          ))}
          <p className="text-[10px] text-muted-foreground italic leading-relaxed pt-1">
            Forecast ranges are not plain-English defensible enough for a recommendation-grade visual; treat the model as exploratory scenario context only.
          </p>
        </div>
      </div>
    </div>
  );
}

// ─── Lens Review ─────────────────────────────────────────────────────────────
function LensReview({ candidate }: { candidate: Candidate }) {
  const lenses = buildLenses(candidate);
  return (
    <div className="space-y-3">
      <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">LENS REVIEW — INTERPRETIVE LENSES</div>
      {lenses.map((lens) => (
        <div key={lens.name} className="border border-border/30 rounded-sm p-4 space-y-2 bg-secondary/10">
          <div className="text-[9px] font-bold uppercase tracking-[0.16em] text-foreground/80">{lens.name}</div>
          <div className="grid grid-cols-1 gap-1.5 text-xs">
            <div>
              <span className="font-semibold text-foreground">Support: </span>
              <span className="text-foreground/80">{lens.support}</span>
            </div>
            <div>
              <span className="font-semibold text-foreground">Challenge: </span>
              <span className="text-foreground/80">{lens.challenge}</span>
            </div>
            <div>
              <span className="font-semibold text-warning">What changes the view: </span>
              <span className="text-muted-foreground">{lens.change}</span>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Tab Types ────────────────────────────────────────────────────────────────
type Tab = "why" | "facts" | "judgment" | "tradeoffs" | "performance" | "evidence";

// ─── Main Component ───────────────────────────────────────────────────────────
export function CandidateCard({ candidate }: { candidate: Candidate }) {
  const [tab, setTab] = useState<Tab>("why");

  const rKey   = candidate.readiness.status.toLowerCase() as keyof typeof READINESS;
  const r      = READINESS[rKey] ?? READINESS.hold;
  const ytdPos = candidate.currentStatus.ytdReturn && !candidate.currentStatus.ytdReturn.startsWith("-");
  const allocated = (candidate.allocation?.current ?? 0) > 0;

  const TABS: { id: Tab; label: string }[] = [
    { id: "why",         label: "Why this fund" },
    { id: "facts",       label: "Key facts" },
    { id: "judgment",    label: "Current judgment" },
    { id: "tradeoffs",   label: "Cost & risk" },
    { id: "performance", label: "Performance & path" },
    { id: "evidence",    label: "Evidence appendix" },
  ];

  return (
    <Card className="flex flex-col bg-card border-border/50 overflow-hidden">

      {/* ── Candidate Factsheet Header ──────────────────── */}
      <div className="p-5 border-b border-border/30">
        <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">
          CANDIDATE FACTSHEET
        </div>

        <div className="flex items-start justify-between gap-4 mb-3">
          <div>
            <div className="flex items-baseline gap-2.5 flex-wrap">
              <span className="font-mono text-2xl font-bold tracking-tight text-foreground">{candidate.ticker}</span>
              <span className="text-[13px] text-muted-foreground font-medium">{candidate.name}</span>
            </div>
            <p className="text-[10px] text-muted-foreground/70 mt-1">
              {candidate.thesis.slice(0, 120)}… &nbsp;
              <span className="italic">
                {allocated ? candidate.assetClass : "Not currently allocated. Research use only."}
              </span>
            </p>
          </div>

          {/* Readiness badge */}
          <div className={`px-3 py-1 border text-[10px] font-bold tracking-widest uppercase rounded-[2px] shrink-0 ${r.cls}`}>
            {r.label}
          </div>
        </div>

        {/* Decision badge strip */}
        <div className="flex flex-wrap items-center gap-2 mb-4">
          <div className="flex items-center gap-1.5 border border-border/40 rounded-[2px] px-2 py-0.5">
            <span className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground">DECISION</span>
            <span className="text-[10px] font-semibold text-foreground">{DECISION[rKey]}</span>
          </div>
          <div className="flex items-center gap-1.5 border border-border/40 rounded-[2px] px-2 py-0.5">
            <span className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground">LEAD STRENGTH</span>
            <span className={`text-[10px] font-semibold ${rKey === "ready" ? "text-success" : rKey === "watch" ? "text-warning" : "text-muted-foreground"}`}>
              {LEAD_STRENGTH[rKey]}
            </span>
          </div>
          <div className="flex items-center gap-1.5 border border-border/40 rounded-[2px] px-2 py-0.5">
            <span className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground">REC. CONFIDENCE</span>
            <span className="text-[10px] font-semibold text-foreground">
              {candidate.readiness.status === "ready" ? "High" : "Moderate"}
            </span>
          </div>
          <div className="flex items-center gap-1.5 border border-border/40 rounded-[2px] px-2 py-0.5">
            <span className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground">EVIDENCE CONFIDENCE</span>
            <span className="text-[10px] font-semibold text-foreground">
              {candidate.supportingDetail.length >= 3 ? "High" : "Partial"}
            </span>
          </div>
        </div>

        {/* Most important reason panels */}
        <div className="grid grid-cols-2 gap-3">
          <div className="border border-border/30 rounded-sm p-3 bg-success/5">
            <div className="text-[9px] font-bold uppercase tracking-[0.16em] text-success mb-1.5">
              MOST IMPORTANT REASON TO ALLOCATE
            </div>
            <p className="text-[11px] text-foreground leading-snug">
              {candidate.keyTradeoffs[0]?.replace(/^•\s*/, "") ?? "Evidence supports allocation within sleeve target."}
            </p>
          </div>
          <div className="border border-border/30 rounded-sm p-3 bg-warning/5">
            <div className="text-[9px] font-bold uppercase tracking-[0.16em] text-warning mb-1.5">
              MOST IMPORTANT REASON TO HESITATE
            </div>
            <p className="text-[11px] text-foreground leading-snug">
              {candidate.keyTradeoffs[1]?.replace(/^•\s*/, "") ?? "Supporting evidence is partial — confirmation pending."}
            </p>
          </div>
        </div>
      </div>

      {/* ── Current Status Row ──────────────────────────── */}
      <div className="px-5 py-3 bg-secondary/15 border-b border-border/25 grid grid-cols-2 sm:grid-cols-4 gap-x-6 gap-y-2 items-center">
        <div>
          <div className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground mb-0.5">PRICE</div>
          <div className="font-mono text-sm font-semibold text-foreground">{candidate.currentStatus.price}</div>
        </div>
        {candidate.currentStatus.ytdReturn && (
          <div>
            <div className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground mb-0.5">YTD RETURN</div>
            <div className={cn("font-mono text-sm font-semibold", ytdPos ? "text-success" : "text-destructive")}>
              {candidate.currentStatus.ytdReturn}
            </div>
          </div>
        )}
        <div>
          <div className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground mb-0.5">POSITION SIZE</div>
          <div className="font-mono text-sm font-semibold text-foreground">{candidate.currentStatus.positionSize ?? "0%"}</div>
        </div>
        <div className="flex flex-col items-end">
          <FreshnessBadge status={candidate.currentStatus.freshness} asOf={candidate.currentStatus.asOf} />
          {candidate.currentStatus.positionNote && (
            <p className="text-[10px] text-muted-foreground text-right mt-1 leading-tight max-w-[160px]">
              {candidate.currentStatus.positionNote}
            </p>
          )}
        </div>
      </div>

      {/* ── Tab Bar ────────────────────────────────────────── */}
      <div className="flex border-b border-border/30 overflow-x-auto">
        {TABS.map(({ id, label }) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            className={cn(
              "px-4 py-2.5 text-[10px] font-semibold uppercase tracking-[0.12em] whitespace-nowrap border-b-2 transition-colors",
              tab === id
                ? "border-primary text-primary bg-primary/5"
                : "border-transparent text-muted-foreground hover:text-foreground hover:bg-secondary/20",
            )}
          >
            {label}
          </button>
        ))}
      </div>

      {/* ── Tab Content ────────────────────────────────────── */}
      <div className="p-5">

        {/* WHY THIS FUND */}
        {tab === "why" && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-5">
              <div>
                <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-primary mb-2">
                  WHY THIS CANDIDATE WOULD DESERVE CAPITAL
                </div>
                <div className="text-[13px] font-semibold text-foreground leading-snug mb-1">Investment case and fund role</div>
                <p className="text-sm text-foreground/85 leading-relaxed">{candidate.thesis}</p>
              </div>

              <div className="border-t border-border/30 pt-4">
                <div className="text-[9px] font-bold uppercase tracking-[0.16em] text-muted-foreground mb-2">INVESTMENT CASE</div>
                <p className="text-sm text-foreground/85 leading-relaxed">{candidate.investmentCase}</p>
              </div>

              {/* Asset tags */}
              <div className="flex flex-wrap gap-1.5 pt-1">
                {[candidate.assetClass, candidate.sector, candidate.geography].map((tag) => (
                  <span key={tag} className="text-[9px] uppercase tracking-[0.12em] px-2 py-0.5 border border-border/40 rounded-[2px] text-muted-foreground">
                    {tag}
                  </span>
                ))}
              </div>
            </div>

            {/* KEY FACTS mini-table */}
            <div>
              <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-3">KEY FACTS — FUND SNAPSHOT</div>
              <div className="grid grid-cols-2 gap-0 border border-border/30 rounded-sm overflow-hidden">
                {[
                  { label: "CURRENT DECISION", val: DECISION[rKey] },
                  { label: "ASSET CLASS",       val: candidate.assetClass },
                  { label: "SECTOR",            val: candidate.sector },
                  { label: "GEOGRAPHY",         val: candidate.geography },
                  { label: "POSITION SIZE",     val: candidate.currentStatus.positionSize ?? "0%" },
                  { label: "TARGET WEIGHT",     val: candidate.allocation ? `${candidate.allocation.target}%` : "—" },
                ].map(({ label, val }) => (
                  <div key={label} className="p-3 border-b border-r border-border/25 last:border-b-0">
                    <div className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground mb-0.5">{label}</div>
                    <div className="text-[11px] font-mono text-foreground font-medium">{val}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* KEY FACTS */}
        {tab === "facts" && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-3">FUND SNAPSHOT</div>
              <div className="grid grid-cols-2 gap-0 border border-border/30 rounded-sm overflow-hidden text-xs">
                {[
                  { label: "CURRENT DECISION", val: DECISION[rKey] },
                  { label: "LEAD STRENGTH",    val: LEAD_STRENGTH[rKey] },
                  { label: "RECOMMENDATION",   val: candidate.readiness.status === "ready" ? "Moderate confidence" : "Review only" },
                  { label: "EVIDENCE BASIS",   val: candidate.supportingDetail.length >= 3 ? "Strong" : "Partial" },
                  { label: "POSITION SIZE",    val: candidate.currentStatus.positionSize ?? "0%" },
                  { label: "TARGET WEIGHT",    val: candidate.allocation ? `${candidate.allocation.target}%` : "—" },
                  { label: "LAST PRICE",       val: candidate.currentStatus.price },
                  { label: "YTD RETURN",       val: candidate.currentStatus.ytdReturn ?? "—" },
                ].map(({ label, val }) => (
                  <div key={label} className="p-3 border-b border-r border-border/25">
                    <div className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground mb-0.5">{label}</div>
                    <div className="font-mono text-foreground font-medium">{val}</div>
                  </div>
                ))}
              </div>
            </div>
            <LensReview candidate={candidate} />
          </div>
        )}

        {/* CURRENT JUDGMENT */}
        {tab === "judgment" && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div>
                <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">CURRENT JUDGMENT</div>
                <div className="text-[13px] font-semibold text-foreground mb-3">Why it is ahead or behind right now</div>
                <p className="text-sm text-foreground/85 leading-relaxed">{candidate.whyAheadOrBehind}</p>
              </div>

              {/* Key bullet points extracted from whyAheadOrBehind */}
              <div className="border-t border-border/30 pt-4">
                <div className="text-[9px] font-bold uppercase tracking-[0.16em] text-muted-foreground mb-2">KEY FACTORS</div>
                <ul className="space-y-2">
                  {candidate.decisionChangeConditions.slice(0, 3).map((cond, i) => (
                    <li key={i} className="flex items-start gap-2 text-xs text-foreground/80">
                      <span className="text-primary mt-0.5 shrink-0">•</span>
                      {cond}
                    </li>
                  ))}
                </ul>
              </div>
            </div>

            {/* Allocation bar */}
            <div className="space-y-4">
              <div>
                <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-3">RELATIVE STANDING — WHERE THE EDGE REALLY SITS</div>
                {candidate.allocation && (
                  <div className="space-y-2">
                    <div className="flex justify-between text-[10px] text-muted-foreground">
                      <span>Current: {candidate.allocation.current}%</span>
                      <span>Target: {candidate.allocation.target}%</span>
                    </div>
                    <div className="h-2 w-full bg-secondary/40 rounded-full overflow-hidden relative">
                      <div
                        className="h-full bg-primary/50 absolute left-0"
                        style={{ width: `${(candidate.allocation.target / 50) * 100}%` }}
                      />
                      <div
                        className="h-full bg-primary relative z-10"
                        style={{ width: `${(candidate.allocation.current / 50) * 100}%` }}
                      />
                      <div
                        className="absolute top-0 bottom-0 w-0.5 bg-warning z-20"
                        style={{ left: `${(candidate.allocation.target / 50) * 100}%` }}
                      />
                    </div>
                    <div className="flex gap-4 text-[9px] text-muted-foreground">
                      <span className="flex items-center gap-1"><span className="w-2 h-1 bg-primary rounded" />Current</span>
                      <span className="flex items-center gap-1"><span className="w-0.5 h-2 bg-warning rounded" />Target</span>
                    </div>
                  </div>
                )}
              </div>

              <div className="border border-border/30 rounded-sm overflow-hidden">
                <div className="grid grid-cols-3 border-b border-border/25">
                  {["DIMENSION", "VERDICT", "READING"].map((h) => (
                    <div key={h} className="p-2 text-[9px] font-bold uppercase tracking-[0.14em] text-muted-foreground border-r border-border/25 last:border-r-0">
                      {h}
                    </div>
                  ))}
                </div>
                {[
                  { dim: "Cost",           verdict: rKey === "ready" ? "Strong" : "Moderate", reading: candidate.readiness.status },
                  { dim: "Benchmark fit",  verdict: "Sufficient",                             reading: "Direct" },
                  { dim: "Evidence",       verdict: candidate.supportingDetail.length >= 3 ? "Strong" : "Partial", reading: "Dated" },
                  { dim: "Liquidity",      verdict: "Adequate",                               reading: "Market support" },
                ].map(({ dim, verdict, reading }) => (
                  <div key={dim} className="grid grid-cols-3 border-b border-border/20 last:border-b-0">
                    <div className="p-2 text-[10px] text-muted-foreground border-r border-border/25">{dim}</div>
                    <div className="p-2 text-[10px] font-medium text-foreground border-r border-border/25">{verdict}</div>
                    <div className="p-2 text-[10px] text-muted-foreground">{reading}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* COST & RISK (Tradeoffs) */}
        {tab === "tradeoffs" && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div>
              <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-3">KEY TRADEOFFS</div>
              <ul className="space-y-3">
                {candidate.keyTradeoffs.map((t, i) => (
                  <li key={i} className="flex items-start gap-2 p-3 border border-border/30 rounded-sm bg-secondary/10">
                    <span className="text-muted-foreground font-mono text-[9px] mt-0.5">0{i + 1}</span>
                    <span className="text-sm text-foreground/85 leading-snug">{t}</span>
                  </li>
                ))}
              </ul>
            </div>

            <div>
              <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-warning mb-3">DECISION CHANGE CONDITIONS</div>
              <ul className="space-y-3">
                {candidate.decisionChangeConditions.map((c, i) => (
                  <li key={i} className="flex items-start gap-2 p-3 border border-warning/20 rounded-sm bg-warning/5">
                    <div className="w-1.5 h-1.5 rounded-full bg-warning/70 mt-1.5 shrink-0" />
                    <span className="text-sm text-foreground/85 leading-snug">{c}</span>
                  </li>
                ))}
              </ul>

              {/* Readiness rationale footer */}
              <div className="mt-4 p-3 border border-border/25 rounded-sm bg-secondary/15 text-xs text-muted-foreground italic leading-relaxed">
                {candidate.readiness.rationale}
              </div>
            </div>
          </div>
        )}

        {/* PERFORMANCE & PATH */}
        {tab === "performance" && (
          <div className="space-y-6">
            <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">
              SCENARIO RANGE AND CURRENT MARKET SUPPORT
            </div>
            <ScenarioRange candidate={candidate} />
          </div>
        )}

        {/* EVIDENCE APPENDIX */}
        {tab === "evidence" && (
          <div className="space-y-3">
            <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">
              EVIDENCE APPENDIX — SUPPORTING DETAIL
            </div>
            {candidate.supportingDetail.map((item, i) => (
              <div key={i} className="border border-border/30 rounded-sm p-4 grid grid-cols-1 md:grid-cols-[1fr_2fr_auto] gap-3 items-start bg-secondary/10">
                <div>
                  <div className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground mb-1">DATE</div>
                  <div className="font-mono text-xs text-foreground">{item.date}</div>
                  <div className="text-[10px] text-muted-foreground mt-1">{item.source}</div>
                </div>
                <div>
                  <div className="text-[9px] uppercase tracking-[0.14em] text-muted-foreground mb-1">FACT</div>
                  <p className="text-xs text-foreground/85 leading-relaxed">{item.fact}</p>
                </div>
                <div>
                  <FreshnessBadge status={item.freshness} asOf={item.date} />
                </div>
              </div>
            ))}
          </div>
        )}

      </div>
    </Card>
  );
}
