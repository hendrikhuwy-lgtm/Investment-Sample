import { Router, type IRouter } from "express";

const router: IRouter = Router();

router.get("/brief/today", (_req, res) => {
  const brief = {
    briefDate: "2026-03-15",
    briefId: "brief-20260315",
    keyTheme: "Tariff escalation pressure meets a softening labor market — risk assets repricing regime uncertainty",
    summaryNarrative: "Markets are navigating a convergence of three forces: accelerating tariff implementation from the Trump administration, a labor market that is softening faster than the Fed's base case, and European fiscal expansion shifting sovereign spreads. The net effect is a risk-off bid in long duration, a rotation away from US mega-cap growth, and a currency hedge premium expanding in USD/EUR. Today's session extends the February drawdown in equities, with the S&P 500 Top 20 now -4.76% YTD versus the full S&P 500 which has absorbed the rotation into value and international.",
    marketSnapshot: [
      {
        label: "S&P 500",
        value: "5,521",
        change: "-1.4%",
        direction: "down",
        freshness: "current",
        asOf: "Mar 14, 2026 16:00 ET"
      },
      {
        label: "TOPT (Top 20 ETF)",
        value: "$29.42",
        change: "-1.11% | YTD -4.76%",
        direction: "down",
        freshness: "latest_available",
        asOf: "Mar 13, 2026 NAV"
      },
      {
        label: "10Y Treasury",
        value: "4.31%",
        change: "-8bps",
        direction: "down",
        freshness: "current",
        asOf: "Mar 14, 2026 16:00 ET"
      },
      {
        label: "USD/EUR",
        value: "1.094",
        change: "+0.6%",
        direction: "up",
        freshness: "current",
        asOf: "Mar 14, 2026 16:00 ET"
      },
      {
        label: "WTI Crude",
        value: "$68.40",
        change: "-2.1%",
        direction: "down",
        freshness: "current",
        asOf: "Mar 14, 2026 16:00 ET"
      },
      {
        label: "VIX",
        value: "22.8",
        change: "+3.2",
        direction: "up",
        freshness: "current",
        asOf: "Mar 14, 2026 16:00 ET"
      },
      {
        label: "Fed Funds (effective)",
        value: "4.33%",
        change: "unchanged",
        direction: "flat",
        freshness: "latest_available",
        asOf: "Mar 12, 2026"
      },
      {
        label: "IG Credit Spread",
        value: "112bps",
        change: "+4bps",
        direction: "up",
        freshness: "lagged",
        asOf: "Mar 13, 2026 (1-day lag)"
      }
    ],
    macroSignals: [
      {
        id: "signal-001",
        category: "TRADE POLICY",
        headline: "25% tariff on Canadian and Mexican imports effective March 15; steel and aluminum exemptions removed",
        severity: "high",
        date: "2026-03-14",
        whatHappened: "The Trump administration confirmed that the 30-day tariff pause on Canada and Mexico expires at midnight March 15, with no extension announced. Simultaneously, the USTR removed the steel and aluminum exemptions that had been in place since 2018, raising effective rates on those inputs to 25% on top of existing Section 232 measures. The announcement came at 14:30 ET Friday, catching markets in thin liquidity.",
        whatItMeans: "This is a material input cost shock for US manufacturers with integrated North American supply chains — automotive, industrials, and consumer durables are most exposed. The steel/aluminum removal specifically hits domestic producers of those finished goods who had been sourcing under exempted quotas. The political signal is that the administration views the tariff pause as a negotiating tool already exhausted, not a policy commitment.",
        investmentImplication: "Reduce exposure to US manufacturers with >30% COGS sourced from Canada/Mexico. The automotive supply chain is the most concentrated risk. Industrial ETFs (XLI) carry elevated tariff pass-through exposure. Domestically-sourced defense contractors and utilities are relative beneficiaries. The input cost pressure is stagflationary at the margin — it pressures margins without stimulating demand, which is a poor setup for equities that have priced in a soft landing.",
        boundary: "A formal trade negotiation announcement or a 60-day pause extension from USTR would substantially change this view. A retaliatory Canadian tariff package (expected but not yet announced) would escalate to a second-order risk-off signal.",
        reviewAction: "Monitor USTR announcements Monday morning. Flag any Canadian retaliatory measure as a portfolio escalation trigger.",
        evidence: [
          {
            date: "2026-03-14",
            source: "USTR Press Release 14:30 ET",
            fact: "25% tariff on all Canadian and Mexican goods effective 00:01 March 15, 2026. Section 232 steel/aluminum exemptions terminated simultaneously.",
            freshness: "current"
          },
          {
            date: "2026-03-14",
            source: "Bloomberg, Ford Motor press release",
            fact: "Ford estimated $1.2B in incremental annual costs from Canadian input exposure at 25% tariff rate.",
            freshness: "current"
          },
          {
            date: "2026-03-10",
            source: "Peterson Institute for International Economics",
            fact: "North American automotive sector sources 38% of intermediate inputs from Canada and Mexico under USMCA preferential rates.",
            freshness: "latest_available"
          }
        ]
      },
      {
        id: "signal-002",
        category: "MONETARY POLICY",
        headline: "February JOLTS: Job openings fell to 7.56M, lowest since January 2021 — labor market softening faster than Fed projections",
        severity: "medium",
        date: "2026-03-11",
        whatHappened: "The February JOLTS report (released March 11) showed job openings at 7.56 million, down from 7.74 million in January and well below the 8.0 million consensus estimate. The quits rate fell to 1.9%, matching the lowest reading since 2015. Layoffs were flat at 1.5 million. The data represents conditions through end of February — there is a one-month reporting lag.",
        whatItMeans: "The JOLTS figures signal that the labor market is cooling more rapidly than the Fed's December projections assumed. The Fed's December SEP embedded 4.3% unemployment by year-end; quits rates at this level are consistent with a faster normalization trajectory. The combination of softening labor demand (openings) and reduced worker confidence (quits) historically precedes a rise in the unemployment rate within 2-3 months.",
        investmentImplication: "This data supports a more dovish Fed trajectory than currently priced in the front end. The 2-year Treasury at 4.1% has limited downside if the market prices even 2 additional cuts this year. Duration extension in investment-grade bonds becomes more attractive on a risk-adjusted basis. Within equities, the setup favors defensive sectors (healthcare, utilities) over cyclicals and rate-sensitive growth names where the growth premium is most vulnerable.",
        boundary: "A February payrolls number above 200K with upward revision to January would substantially revise this softening thesis. Watch Friday March 6 non-farm payrolls (already released at +143K, consistent with softening read).",
        evidence: [
          {
            date: "2026-03-11",
            source: "BLS JOLTS, February 2026 release",
            fact: "Job openings: 7.56M (est. 8.0M). Quits rate: 1.9%. Layoffs: 1.5M. Data represents end-February conditions.",
            freshness: "latest_available"
          },
          {
            date: "2026-03-06",
            source: "BLS Employment Situation, February 2026",
            fact: "Non-farm payrolls +143K vs +160K estimate. Unemployment rate 4.1%. January revised down to +111K from +143K.",
            freshness: "latest_available"
          },
          {
            date: "2025-12-18",
            source: "Federal Reserve SEP, December 2025",
            fact: "Fed projected 4.3% unemployment by end-2026. Current trajectory is tracking above that pace.",
            freshness: "lagged"
          }
        ]
      },
      {
        id: "signal-003",
        category: "FISCAL POLICY — EUROPE",
        headline: "German Bundestag passes €500B infrastructure fund; EU defense spending exemption from deficit rules approved",
        severity: "medium",
        date: "2026-03-13",
        whatHappened: "The German Bundestag passed the €500B special infrastructure fund on March 13 with a 513-207 supermajority, clearing the constitutional two-thirds threshold required. The EU simultaneously announced that defense spending up to 1.5% of GDP will be excluded from the Stability and Growth Pact deficit calculations, effective immediately. This is the largest fiscal expansion in German postwar history.",
        whatItMeans: "European fiscal expansion of this scale materially shifts the growth differential between Europe and the US, which has been pricing in fiscal contraction from DOGE and tariff uncertainty. German 10-year bunds are experiencing a supply/demand recalibration — yields rose 18bps in the week of the announcement as markets absorbed the issuance implications. The euro has strengthened as capital re-rates European growth prospects and the ECB may now pause its cutting cycle earlier than expected.",
        investmentImplication: "European equities (particularly German industrials, defense, and infrastructure beneficiaries) offer the most direct exposure. IDEV (iShares Core MSCI International Developed Markets, 0.04% ER) provides broad developed-market exposure that captures European rerating. The EUR/USD move from 1.07 to 1.094 in two weeks reflects this repricing — further USD weakness is probable if US fiscal news remains uncertain while Europe executes. Hedge USD exposure in European allocations.",
        boundary: "German coalition fracture or ECB signal to offset fiscal impulse with tighter monetary stance would reverse this thesis.",
        evidence: [
          {
            date: "2026-03-13",
            source: "Bundestag vote record",
            fact: "€500B Sondervermögen passed 513-207. Two-thirds supermajority achieved. Fund operational for infrastructure spend 2026-2040.",
            freshness: "latest_available"
          },
          {
            date: "2026-03-13",
            source: "European Commission announcement",
            fact: "Defense spending up to 1.5% of GDP excluded from SGP deficit calculation effective March 13, 2026.",
            freshness: "latest_available"
          },
          {
            date: "2026-03-14",
            source: "Bloomberg bond market data",
            fact: "German 10Y Bund yield: 2.89%, up 18bps on the week. EUR/USD: 1.094, up from 1.07 on Feb 28.",
            freshness: "current"
          }
        ]
      },
      {
        id: "signal-004",
        category: "EQUITY MARKETS",
        headline: "TOPT (iShares Top 20 US Stocks ETF) -4.76% YTD through March 13, underperforming S&P 500 by 210bps",
        severity: "medium",
        date: "2026-03-13",
        whatHappened: "The iShares Top 20 US Stocks ETF (TOPT), which tracks the S&P 500 Top 20 Select Index (SPXT2SUT), has declined 4.76% year-to-date through March 13, 2026. NAV was $29.42 on March 13, down from a 52-week high of $31.85. Fund net assets are $483M with a 0.20% expense ratio. The underperformance versus the broader S&P 500 reflects the concentrated selloff in mega-cap tech and consumer discretionary — together 41.5% of TOPT by sector weight.",
        whatItMeans: "Mega-cap concentration is working against TOPT holders in a rotation environment. NVIDIA (15.6% of TOPT), Apple (13.83%), and Microsoft (12.37%) have individually seen pressure from tariff uncertainty on semiconductor supply chains, margin compression expectations, and multiple derating on AI spend scrutiny. The fund's P/E of 31.67x (as of March 12) prices in a high-growth scenario that is being challenged by both the macro backdrop and individual earnings revisions.",
        investmentImplication: "TOPT's drawdown is a symptom of the broader mega-cap rotation, not idiosyncratic fund risk. The thesis for holding concentrated large-cap remains intact if you believe AI capex sustains growth in NVDA/MSFT — but the near-term setup favors trimming concentration in the highest-multiple names. The iShares Core S&P 500 ETF (IVV, 0.03% ER) provides similar large-cap exposure with significantly more diversification and a lower multiple.",
        boundary: "A re-acceleration in AI demand signals from NVIDIA's Q1 2026 earnings (expected late May) or an Apple product cycle beat would materially restore the mega-cap growth premium.",
        evidence: [
          {
            date: "2026-03-13",
            source: "iShares.com, TOPT fund page",
            fact: "NAV $29.42. YTD return -4.76%. 52-week range $21.20–$31.85. Net assets $483M. P/E 31.67x. P/B 7.88x.",
            freshness: "latest_available"
          },
          {
            date: "2025-12-31",
            source: "iShares TOPT Fact Sheet",
            fact: "Full-year 2025 NAV return +20.45%. Top holdings: NVDA 15.6%, AAPL 13.83%, MSFT 12.37%, BRK.B 4.61%, META 4.56%.",
            freshness: "lagged"
          },
          {
            date: "2026-03-12",
            source: "iShares TOPT Portfolio Characteristics",
            fact: "Number of holdings: 21. Expense ratio: 0.20%. 30-day SEC yield: 0.42%.",
            freshness: "latest_available"
          }
        ]
      }
    ]
  };

  res.json(brief);
});

export default router;
