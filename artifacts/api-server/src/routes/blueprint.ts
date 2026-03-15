import { Router, type IRouter } from "express";

const router: IRouter = Router();

router.get("/blueprint", (_req, res) => {
  const blueprint = {
    blueprintDate: "2026-03-15",
    portfolioSummary: {
      totalValue: "$2,847,300",
      cashWeight: 8.5,
      equityWeight: 67.3,
      bondWeight: 24.2,
      lastUpdated: "2026-03-14T16:00:00Z",
      freshness: "latest_available"
    },
    designPrinciples: [
      "Evidence-first: no position without datable, sourced support",
      "Freshness-honest: every data point carries its as-of date and confidence tier",
      "Thesis before evaluation: explain what you own before explaining how it performs",
      "Boundary discipline: every conviction has a stated condition that would reverse it",
      "Cost transparency: expense ratio and implementation friction are always shown"
    ],
    candidates: [
      {
        id: "cand-001",
        ticker: "IVV",
        name: "iShares Core S&P 500 ETF",
        assetClass: "Equity — US Large Cap",
        sector: "Broad Market",
        geography: "United States",
        currentStatus: {
          price: "$564.20",
          ytdReturn: "-2.8%",
          positionSize: "32.6%",
          positionNote: "Core allocation. Within target band.",
          freshness: "latest_available",
          asOf: "Mar 14, 2026"
        },
        thesis: "Low-cost, diversified exposure to US large-cap equities is the irreducible core of a growth-oriented portfolio. IVV earns its position through unambiguous cost efficiency (0.03% ER), structural diversification across 503 holdings, and direct benchmark clarity versus the S&P 500.",
        investmentCase: "IVV is the cost-dominant vehicle for the US large-cap sleeve. At 0.03% ER, implementation friction is negligible. Benchmark fit is strong — direct S&P 500 tracking with no synthetic overlay, no counterparty risk, and quarterly distributions that simplify tax management. The S&P 500 itself remains the world's most liquid and analyzed equity benchmark, providing superior secondary market support. IVV supersedes alternatives like SPY (0.0945% ER) and VOO on institutional track record and BlackRock operational depth.",
        whyAheadOrBehind: "YTD performance at -2.8% reflects the Q1 2026 broad market compression from tariff uncertainty and Fed timeline repricing. IVV is performing in line with the benchmark — the drawdown is market, not vehicle. The core thesis is not disrupted: cost advantage, diversification quality, and benchmark fidelity all remain intact. The slight underperformance versus equal-weight indices (RSP) reflects mega-cap drag, which is an index construction feature, not a structural weakness.",
        keyTradeoffs: [
          "Market-cap weighting concentrates ~35% of exposure in top 10 names — mega-cap underperformance is fully transmitted",
          "US-only exposure creates home-bias risk if European fiscal expansion shifts the growth differential sustainably",
          "Low yield (1.3% trailing) means little income buffer in a risk-off drawdown",
          "Tax efficiency is strong but not optimal versus direct indexing at this portfolio scale"
        ],
        decisionChangeConditions: [
          "Sustained mega-cap EPS revision cycle lower would prompt a size tilt toward equal-weight or mid-cap",
          "EUR/USD exceeding 1.15 on persistent basis would prompt international reweighting above 25%",
          "A lower-cost vehicle emerging in the S&P 500 category (unlikely at 0.03%) would trigger a switch review"
        ],
        supportingDetail: [
          {
            date: "2026-03-12",
            source: "iShares CoreBuilder balanced model",
            fact: "IVV recommended at 32.6% weight in balanced (60/40) model portfolio. Weighted average expense ratio of full model: 0.05%.",
            freshness: "latest_available"
          },
          {
            date: "2026-03-14",
            source: "Bloomberg market data",
            fact: "IVV AUM: $578B. 30-day average daily volume: 3.8M shares. Bid/ask spread: 0.01%.",
            freshness: "current"
          },
          {
            date: "2026-01-15",
            source: "Morningstar ETF research",
            fact: "IVV rated Gold medalist. 10-year tracking difference vs S&P 500: -0.02% (outperformed index net of fees due to securities lending).",
            freshness: "lagged"
          }
        ],
        readiness: {
          status: "ready",
          rationale: "Hold at current weight. No trigger for size change. Cost, fit, and diversification remain strongest in class."
        },
        allocation: {
          current: 32.6,
          target: 32.0,
          weight: "32.6%"
        }
      },
      {
        id: "cand-002",
        ticker: "TOPT",
        name: "iShares Top 20 US Stocks ETF",
        assetClass: "Equity — US Mega Cap Concentrated",
        sector: "Multi-sector (tech-concentrated)",
        geography: "United States",
        currentStatus: {
          price: "$29.42",
          ytdReturn: "-4.76%",
          positionSize: "0%",
          positionNote: "Not held. Under active review for satellite allocation.",
          freshness: "latest_available",
          asOf: "Mar 13, 2026 NAV"
        },
        thesis: "TOPT offers deliberate, low-cost concentration in the 20 largest US companies by market cap — a pure-play on mega-cap compounding with a 0.20% ER. The case is that the largest companies in the world have structural advantages (network effects, AI capital access, regulatory moats) that justify premium concentration when the cycle is in their favor.",
        investmentCase: "TOPT tracks the S&P 500 Top 20 Select Index (SPXT2SUT), holding 21 positions with NVIDIA at 15.6%, Apple at 13.83%, and Microsoft at 12.37% — together 41.8% of the fund. The fund launched October 2024 and returned +20.45% NAV in its first full calendar year (2025), outperforming the broader S&P 500 by roughly 150bps. The P/E of 31.67x prices in sustained mega-cap earnings growth. The structural case rests on AI infrastructure spend (NVDA, MSFT), consumer platform lock-in (AAPL, AMZN), and financial services dominance (BRK.B, JPM). The 0.20% ER is acceptable for satellite positioning but rules out large core allocation.",
        whyAheadOrBehind: "TOPT is behind its inception thesis in Q1 2026 due to concentrated mega-cap selloff. The -4.76% YTD underperforms the broad S&P 500 by ~210bps, reflecting the rotation from high-multiple tech into value, international, and defensive sectors. The key question is whether this is temporary multiple compression (entry opportunity) or the start of a sustained earnings revision cycle. Current evidence supports the former: AI capex continues growing and earnings estimates for NVDA/MSFT have not been substantially cut. The tariff overhang is real but primarily affects hardware supply chains, not software margins.",
        keyTradeoffs: [
          "Extreme concentration: top 3 holdings are 41.8% of NAV — single-name risk is high",
          "P/E of 31.67x leaves no room for earnings disappointment — multiple compression is the primary downside path",
          "0.20% ER is 6.7x more expensive than IVV for market-cap weighted large cap exposure",
          "Tariff risk: NVDA and AAPL have material semiconductor supply chain exposure to Taiwan and China",
          "New fund (Oct 2024): limited track record, no 3Y/5Y statistics, beta unmeasured"
        ],
        decisionChangeConditions: [
          "NVIDIA Q1 2026 earnings (expected late May) beat with raised guidance would confirm AI capex thesis — entry trigger",
          "Tariff extension to semiconductors/tech hardware would materially raise supply chain risk and likely prompt exit",
          "TOPT P/E compressing below 25x (without earnings growth) would make the concentration premium less defensible",
          "If AI infrastructure spend growth rate decelerates below 20% YoY, thesis weakens materially"
        ],
        supportingDetail: [
          {
            date: "2026-03-13",
            source: "iShares.com, TOPT fund page",
            fact: "NAV $29.42. YTD -4.76%. 52-week high $31.85. Net assets $483M. P/E 31.67x. P/B 7.88x. 30-day SEC yield 0.42%.",
            freshness: "latest_available"
          },
          {
            date: "2025-12-31",
            source: "iShares TOPT Fact Sheet Q4 2025",
            fact: "Full-year 2025 NAV return +20.45%. Benchmark (S&P 500 Top 20 Select Index) returned +20.68%. Tracking difference: -23bps.",
            freshness: "lagged"
          },
          {
            date: "2026-03-12",
            source: "iShares TOPT Top Holdings",
            fact: "NVDA 15.60%, AAPL 13.83%, MSFT 12.37%, BRK.B 4.61%, META 4.56%, JPM 4.51%, TSLA 4.48%, AMZN 4.48%, LLY 4.39%, AVGO 3.77%. Top 10 = 72.60% of portfolio.",
            freshness: "latest_available"
          },
          {
            date: "2026-02-28",
            source: "iShares.com",
            fact: "30-day median bid/ask spread 0.03%. 30-day avg volume 604,251 shares. Fund inception Oct 23, 2024.",
            freshness: "latest_available"
          }
        ],
        readiness: {
          status: "watch",
          rationale: "Monitor for entry. Thesis intact but concentration risk elevated in current tariff/rotation environment. Next review trigger: NVDA Q1 2026 earnings."
        },
        allocation: {
          current: 0,
          target: 5.0,
          weight: "0% (target: 5%)"
        }
      },
      {
        id: "cand-003",
        ticker: "IDEV",
        name: "iShares Core MSCI International Developed Markets ETF",
        assetClass: "Equity — International Developed",
        sector: "Broad Market",
        geography: "Developed ex-US",
        currentStatus: {
          price: "$68.14",
          ytdReturn: "+4.2%",
          positionSize: "18.0%",
          positionNote: "Held. Performing above thesis. Review size for increase.",
          freshness: "latest_available",
          asOf: "Mar 14, 2026"
        },
        thesis: "Developed ex-US equity exposure captures the European fiscal re-rating currently underway, provides portfolio diversification against US-specific tariff and political risk, and does so at 0.04% ER — the most cost-efficient vehicle in this category. The thesis strengthens when the USD weakens and European growth accelerates relative to the US.",
        investmentCase: "IDEV tracks the MSCI World ex USA IMI Index, covering approximately 3,300 stocks across 22 developed markets. European exposure is ~65% of the fund, with Japan at ~20%. The current macro setup is unusually favorable for this allocation: (1) German fiscal stimulus (€500B infrastructure fund passed March 13) is the largest European fiscal expansion in postwar history, (2) EUR/USD has risen from 1.07 to 1.094 in two weeks as markets reprice European growth, (3) US tariff uncertainty is a relative drag on US-only portfolios. The fund at 0.04% ER offers nearly identical cost to IVV for meaningfully different exposure.",
        whyAheadOrBehind: "IDEV is ahead of its 2026 thesis. The +4.2% YTD outperformance versus the S&P 500's -2.8% represents the fastest relative outperformance since 2017. The German infrastructure package is the primary catalyst — European industrials, defense, and infrastructure names are the direct beneficiaries. The EUR appreciation adds a currency tailwind for USD-denominated investors. The risk to this thesis is an ECB pivot hawkish in response to fiscal expansion, which would partially offset the growth stimulus.",
        keyTradeoffs: [
          "Currency risk: EUR/USD moves directly affect USD returns for a dollar-based investor",
          "Concentration in Japan (~20%): yen weakness can drag even when European names outperform",
          "Less liquid than US-listed peers: bid/ask spread wider, particularly in volatile markets",
          "Political risk in Europe remains elevated — French politics, Italian spreads, energy security"
        ],
        decisionChangeConditions: [
          "ECB signaling a pause or reversal of rate cuts in response to fiscal expansion would slow European equity re-rating",
          "EUR/USD retracing below 1.07 would remove the currency tailwind and reduce the relative case",
          "US tariff resolution with Canada/Mexico followed by a growth re-acceleration would shrink the US/Europe differential"
        ],
        supportingDetail: [
          {
            date: "2026-03-12",
            source: "iShares CoreBuilder balanced model",
            fact: "IDEV recommended at 18% weight in balanced (60/40) model. Classified as Developed Markets equity.",
            freshness: "latest_available"
          },
          {
            date: "2026-03-13",
            source: "Bloomberg currency market data",
            fact: "EUR/USD: 1.094 (+0.6% day, +2.3% MTD). German 10Y Bund: 2.89% (+18bps week). European fiscal expansion primary driver.",
            freshness: "current"
          },
          {
            date: "2026-03-13",
            source: "Bundestag vote record / EC announcement",
            fact: "€500B Sondervermögen passed (513-207). EU defense spending exempted from SGP deficit rules. Largest European fiscal expansion since postwar reconstruction.",
            freshness: "latest_available"
          }
        ],
        readiness: {
          status: "ready",
          rationale: "Consider increasing to 20-22% target weight. Fiscal catalyst is active, currency tailwind persists, and relative outperformance has fundamental basis."
        },
        allocation: {
          current: 18.0,
          target: 21.0,
          weight: "18% (target: 21%)"
        }
      },
      {
        id: "cand-004",
        ticker: "IUSB",
        name: "iShares Core Universal USD Bond ETF",
        assetClass: "Fixed Income — US Broad Market",
        sector: "Multi-sector bonds",
        geography: "United States",
        currentStatus: {
          price: "$44.87",
          ytdReturn: "+1.1%",
          positionSize: "33.8%",
          positionNote: "Core bond allocation. At upper end of target band given duration extension thesis.",
          freshness: "latest_available",
          asOf: "Mar 14, 2026"
        },
        thesis: "Broad US bond market exposure at 0.06% ER serves as the portfolio's primary volatility dampener and income source. The current macro setup — a softening labor market faster than Fed projections, declining inflation, and a risk-off bid from tariff uncertainty — supports a positive duration view. Holding the long end via IUSB (which includes Treasuries, investment grade, and MBS) positions the portfolio to benefit from rate normalization.",
        investmentCase: "IUSB tracks the Bloomberg US Universal Bond Index, covering ~10,000 bonds across Treasuries, investment-grade corporate, MBS pass-throughs, and a small allocation to high yield. The portfolio in the BlackRock CoreBuilder model shows IUSB at 33.8% weight, making it the single largest position in the balanced model. The fund's credit quality is strong: AA-rated securities represent 56.22% of the fixed income portion, with AAA at 5.29% and BBB at 13.24% — well within investment-grade territory. At 0.06% ER, IUSB is cost-competitive with all major alternatives in this space.",
        whyAheadOrBehind: "IUSB is modestly ahead of its 2026 thesis. The +1.1% YTD return reflects the flight-to-quality bid as tariff uncertainty has pushed equity investors toward duration. The February JOLTS report (openings at 7.56M, quits rate at 1.9% — lowest since 2015) supports the view that the Fed will cut 2-3 times in 2026, which would be positive for the fund's duration exposure. The 10Y Treasury at 4.31% (-8bps today) reflects this ongoing bid.",
        keyTradeoffs: [
          "Duration exposure means significant mark-to-market loss if inflation re-accelerates unexpectedly",
          "MBS pass-through exposure (7.54% of model) creates prepayment optionality risk in rate-falling environment",
          "High-yield allocation (~5% of IUSB) introduces credit spread risk in a risk-off environment",
          "USD strength would reduce the relative attractiveness versus international bonds"
        ],
        decisionChangeConditions: [
          "CPI above 3.5% for two consecutive months would force a duration reduction and potential shift toward floating rate",
          "Fed signaling fewer than 2 cuts in 2026 would reduce the rate-normalization thesis",
          "Credit spreads widening beyond 150bps IG would require review of the high-yield component risk"
        ],
        supportingDetail: [
          {
            date: "2026-03-12",
            source: "iShares CoreBuilder balanced model",
            fact: "IUSB at 33.8% weight. Total portfolio weighted average expense ratio: 0.05%. Credit quality: AA 56.22%, A 17.59%, BBB 13.24%.",
            freshness: "latest_available"
          },
          {
            date: "2026-03-14",
            source: "Bloomberg Treasury market data",
            fact: "10Y Treasury yield: 4.31% (-8bps today, -22bps MTD). 2Y Treasury: 4.08%. Curve positive-sloped by 23bps.",
            freshness: "current"
          },
          {
            date: "2026-03-11",
            source: "BLS JOLTS February 2026",
            fact: "Job openings 7.56M (est 8.0M). Quits rate 1.9% — lowest since 2015. Consistent with 2-3 Fed cuts in 2026.",
            freshness: "latest_available"
          }
        ],
        readiness: {
          status: "hold",
          rationale: "Hold at 33.8%. Duration position is working. Next review: March CPI (April 10). Watch for Fed communications at March 19 FOMC meeting."
        },
        allocation: {
          current: 33.8,
          target: 34.0,
          weight: "33.8%"
        }
      },
      {
        id: "cand-005",
        ticker: "IEMG",
        name: "iShares Core MSCI Emerging Markets ETF",
        assetClass: "Equity — Emerging Markets",
        sector: "Broad Market",
        geography: "Emerging Markets",
        currentStatus: {
          price: "$44.62",
          ytdReturn: "+1.8%",
          positionSize: "6.6%",
          positionNote: "Held at satellite weight. Underweight target. China and India primary exposures.",
          freshness: "latest_available",
          asOf: "Mar 14, 2026"
        },
        thesis: "Selective EM exposure at satellite weight (6-8%) provides portfolio diversification against US-centric risks, exposure to long-run EM growth, and a valuation discount versus developed markets. IEMG at 0.09% ER is the lowest-cost broad EM vehicle with sufficient AUM and liquidity to be held at this portfolio scale.",
        investmentCase: "IEMG tracks the MSCI Emerging Markets Investable Market Index, covering ~3,000 stocks across 24 emerging markets. China is ~27% of the benchmark, India ~20%, Taiwan ~18%. The fund offers genuine diversification versus US holdings: correlation to S&P 500 is ~0.65 over 5 years, meaningfully lower than IDEV (~0.78). The valuation argument is real — EM trades at ~12x forward P/E versus ~21x for the S&P 500. The structural risk is USD strengthening, which historically is the primary headwind to EM returns in dollar terms.",
        whyAheadOrBehind: "IEMG is slightly ahead at +1.8% YTD, driven by India's continued strong performance and a stabilization in Chinese equities following January stimulus measures. However, the fund remains behind its long-term thesis due to persistent USD strength through most of 2025 and geopolitical overhangs on Taiwan exposure. The position is at 6.6% versus a target of 7-8%, which reflects deliberate caution on China risk in a tariff-escalation environment.",
        keyTradeoffs: [
          "China exposure (~27%) carries significant tariff and geopolitical risk — a Taiwan scenario is the tail risk",
          "USD strength is the structural headwind: every 5% USD appreciation roughly costs 3-4% in EM dollar returns",
          "Liquidity is lower than developed market ETFs, spreads widen in stress",
          "Political risk is genuinely diversified (India, Brazil, South Korea, South Africa) but not eliminatable"
        ],
        decisionChangeConditions: [
          "USD index (DXY) breaking above 108 on sustained basis would prompt EM weight reduction",
          "China tariff escalation beyond 25% would prompt China-ex review or outright exit",
          "India's GDP growth surprising to the downside for two consecutive quarters would reduce the conviction anchor"
        ],
        supportingDetail: [
          {
            date: "2026-03-12",
            source: "iShares CoreBuilder balanced model",
            fact: "IEMG at 6.6% weight in balanced model. Classified as Emerging Markets. ER 0.09%.",
            freshness: "latest_available"
          },
          {
            date: "2026-03-14",
            source: "Bloomberg FX data",
            fact: "DXY (USD index): 103.8, down from 108.1 peak in January 2026. USD weakening is tailwind for EM returns.",
            freshness: "current"
          }
        ],
        readiness: {
          status: "watch",
          rationale: "Monitor China tariff developments. Do not increase until USD trajectory clarifies. Review at April quarter-end rebalance."
        },
        allocation: {
          current: 6.6,
          target: 7.0,
          weight: "6.6% (target: 7%)"
        }
      }
    ]
  };

  res.json(blueprint);
});

export default router;
