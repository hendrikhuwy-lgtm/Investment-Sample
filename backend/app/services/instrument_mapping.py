from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any, Literal

from app.models.types import Citation, InstrumentCandidate, InstrumentTaxProfile, SourceRecord, TaxResidencyProfile
from app.services.tax_engine import evaluate_instrument_for_sg


def _source_record(
    source_id: str,
    url: str,
    publisher: str,
    topic: str,
    retrieved_at: datetime,
) -> SourceRecord:
    raw_hash = hashlib.sha256(f"{source_id}|{url}|{retrieved_at.isoformat()}".encode("utf-8")).hexdigest()
    tier = "primary" if publisher in {"IRS", "IRAS"} else "secondary"
    return SourceRecord(
        source_id=source_id,
        url=url,
        publisher=publisher,
        retrieved_at=retrieved_at,
        topic=topic,
        credibility_tier=tier,
        raw_hash=raw_hash,
        source_type="web",
    )


def _citation(url: str, source_id: str, retrieved_at: datetime, importance: str) -> Citation:
    return Citation(url=url, source_id=source_id, retrieved_at=retrieved_at, importance=importance)


def _build_tax_profile(candidate: InstrumentCandidate) -> InstrumentTaxProfile:
    return InstrumentTaxProfile(
        instrument_id=candidate.symbol,
        domicile=candidate.domicile,
        us_dividend_exposure=True,
        expected_withholding_rate=candidate.withholding_rate,
        us_situs_risk_flag=candidate.us_situs_risk_flag,
        expense_ratio=candidate.expense_ratio,
        liquidity_score=candidate.liquidity_score,
    )


def _score_candidates(
    residency: TaxResidencyProfile,
    candidates: list[InstrumentCandidate],
) -> list[InstrumentCandidate]:
    scored: list[InstrumentCandidate] = []
    for candidate in candidates:
        score = evaluate_instrument_for_sg(residency, _build_tax_profile(candidate))
        scored.append(candidate.model_copy(update={"tax_score": score.score}))
    return sorted(scored, key=lambda item: float(item.tax_score or 0.0), reverse=True)


def _candidate(
    *,
    symbol: str,
    name: str,
    asset_class: str,
    domicile: str,
    expense_ratio: float,
    average_volume: float | None,
    dividend_yield: float | None,
    withholding_rate: float,
    us_situs_risk_flag: bool,
    liquidity_score: float,
    retrieved_at: datetime,
    citations: list[Citation],
    yield_proxy: float | None = None,
    duration_years: float | None = None,
    allocation_weight: float | None = None,
    retail_accessible: bool | None = None,
    margin_required: bool | None = None,
    max_loss_known: bool | None = None,
    option_position: Literal["long_put"] | None = None,
    strike: float | None = None,
    expiry: str | None = None,
    premium_paid_pct_nav: float | None = None,
    annualized_carry_estimate: float | None = None,
    notes: str | None = None,
) -> InstrumentCandidate:
    return InstrumentCandidate(
        symbol=symbol,
        name=name,
        asset_class=asset_class,
        domicile=domicile,
        expense_ratio=expense_ratio,
        average_volume=average_volume,
        dividend_yield=dividend_yield,
        withholding_rate=withholding_rate,
        us_situs_risk_flag=us_situs_risk_flag,
        liquidity_score=liquidity_score,
        retrieved_at=retrieved_at,
        citations=citations,
        yield_proxy=yield_proxy,
        duration_years=duration_years,
        allocation_weight=allocation_weight,
        retail_accessible=retail_accessible,
        margin_required=margin_required,
        max_loss_known=max_loss_known,
        option_position=option_position,
        strike=strike,
        expiry=expiry,
        premium_paid_pct_nav=premium_paid_pct_nav,
        annualized_carry_estimate=annualized_carry_estimate,
        notes=notes,
    )


def _build_common_sources(retrieved_at: datetime) -> tuple[dict[str, SourceRecord], dict[str, Citation]]:
    source_specs = [
        ("irs_withholding_nra", "https://www.irs.gov/individuals/international-taxpayers/federal-income-tax-withholding-and-reporting-on-other-kinds-of-us-source-income-paid-to-nonresident-aliens", "IRS", "tax", "Primary US nonresident withholding reference"),
        ("iras_overseas_income", "https://www.iras.gov.sg/taxes/individual-income-tax/basics-of-individual-income-tax/what-is-taxable-what-is-not/income-received-from-overseas", "IRAS", "tax", "Primary Singapore overseas income and tax context reference"),
        ("vanguard_vti", "https://investor.vanguard.com/investment-products/etfs/profile/vti", "Vanguard", "global_equity", "VTI issuer profile and expense ratio"),
        ("ssga_spy", "https://www.ssga.com/us/en/intermediary/etfs/funds/spdr-sp-500-etf-trust-spy", "State Street", "global_equity", "SPY issuer profile and facts"),
        ("blackrock_ivv", "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf", "BlackRock iShares", "global_equity", "IVV issuer profile and facts"),
        ("blackrock_cspx", "https://www.ishares.com/uk/individual/en/products/253743/ishares-sp-500-b-ucits-etf-acc-fund", "BlackRock iShares", "global_equity", "CSPX UCITS profile and costs"),
        ("blackrock_iwda", "https://www.ishares.com/uk/individual/en/products/251882/ishares-msci-world-ucits-etf-acc-fund", "BlackRock iShares", "global_equity", "IWDA UCITS profile and costs"),
        ("vanguard_vwra", "https://www.vanguard.co.uk/professional/product/etf/equity/9679/ftse-all-world-ucits-etf-usd-accumulating", "Vanguard", "global_equity", "VWRA UCITS profile and costs"),
        ("vanguard_vwrd", "https://www.vanguard.co.uk/professional/product/etf/equity/9678/ftse-all-world-ucits-etf-usd-distributing", "Vanguard", "global_equity", "VWRD UCITS profile and costs"),
        ("vanguard_bnd", "https://investor.vanguard.com/investment-products/etfs/profile/bnd", "Vanguard", "ig_bonds", "BND issuer profile"),
        ("blackrock_aggu", "https://www.ishares.com/uk/individual/en/products/251767/ishares-core-global-aggregate-bond-ucits-etf", "BlackRock iShares", "ig_bonds", "AGGU UCITS profile"),
        ("blackrock_igla", "https://www.ishares.com/uk/individual/en/products/251828/ishares-global-government-bond-ucits-etf", "BlackRock iShares", "ig_bonds", "IGLA UCITS profile"),
        ("nikko_a35", "https://www.nikkoam.com.sg/etf/a35", "Nikko AM", "ig_bonds", "A35 SG bond ETF profile"),
        ("vanguard_vnq", "https://investor.vanguard.com/investment-products/etfs/profile/vnq", "Vanguard", "real_assets", "VNQ REIT ETF profile"),
        ("blackrock_iwdp", "https://www.ishares.com/uk/individual/en/products/251908/ishares-developed-markets-property-yield-ucits-etf", "BlackRock iShares", "real_assets", "IWDP UCITS property ETF profile"),
        ("ssga_gld", "https://www.ssga.com/us/en/intermediary/etfs/funds/spdr-gold-shares-gld", "State Street", "real_assets", "GLD gold ETF profile"),
        ("blackrock_sgln", "https://www.ishares.com/uk/individual/en/products/258441/ishares-physical-gold-etc", "BlackRock iShares", "real_assets", "SGLN ETC profile"),
        ("blackrock_igln", "https://www.ishares.com/uk/individual/en/products/258442/ishares-physical-gold-etc", "BlackRock iShares", "real_assets", "IGLN ETC profile"),
        ("dbmf_site", "https://www.im.natixis.com/en-us/etf/dbmf", "Natixis", "alternatives", "DBMF managed futures ETF profile"),
        ("kmlm_site", "https://kfafunds.com/kmlm/", "KFA Funds", "alternatives", "KMLM managed futures ETF profile"),
        ("cambria_tail", "https://www.cambriafunds.com/tail", "Cambria", "alternatives", "TAIL ETF profile"),
        ("alphaarchitect_caos", "https://etfsite.alphaarchitect.com/caos/", "Alpha Architect", "alternatives", "CAOS ETF profile"),
        ("agf_btal", "https://www.agf.com/us/en/investments/etfs/agf-us-market-neutral-anti-beta-fund-btal.jsp", "AGF", "alternatives", "BTAL ETF profile"),
        ("cboe_options", "https://www.cboe.com/tradable_products/sp_500/spx_options/specifications/", "Cboe", "convex", "SPX options contract specifications"),
        ("occ_options", "https://www.theocc.com/Clearance-and-Settlement/Clearing/Equity-Options-Product-Specifications", "OCC", "convex", "Equity options product specifications"),
    ]

    sources: dict[str, SourceRecord] = {}
    citations: dict[str, Citation] = {}
    for source_id, url, publisher, topic, importance in source_specs:
        sources[source_id] = _source_record(
            source_id=source_id,
            url=url,
            publisher=publisher,
            topic=topic,
            retrieved_at=retrieved_at,
        )
        citations[source_id] = _citation(
            url=url,
            source_id=source_id,
            retrieved_at=retrieved_at,
            importance=importance,
        )
    return sources, citations


def build_implementation_mapping(
    opportunities: list[dict[str, Any]] | None = None,
    retrieved_at: datetime | None = None,
) -> dict[str, Any]:
    retrieved_at = retrieved_at or datetime.now(UTC)
    sources, citations = _build_common_sources(retrieved_at)

    profile = TaxResidencyProfile(
        profile_id="sg_individual",
        tax_residency="SG",
        base_currency="SGD",
        dta_flags={"ireland_us_treaty_path": True},
        estate_risk_flags={"us_situs_cap_enabled": True},
    )

    global_equity = _score_candidates(
        profile,
        [
            _candidate(symbol="VTI", name="Vanguard Total Stock Market ETF", asset_class="global_equity", domicile="US", expense_ratio=0.0003, average_volume=4300000, dividend_yield=0.015, withholding_rate=0.30, us_situs_risk_flag=True, liquidity_score=0.99, retrieved_at=retrieved_at, citations=[citations["vanguard_vti"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="SPY", name="SPDR S&P 500 ETF Trust", asset_class="global_equity", domicile="US", expense_ratio=0.0009, average_volume=90000000, dividend_yield=0.013, withholding_rate=0.30, us_situs_risk_flag=True, liquidity_score=1.00, retrieved_at=retrieved_at, citations=[citations["ssga_spy"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="IVV", name="iShares Core S&P 500 ETF", asset_class="global_equity", domicile="US", expense_ratio=0.0003, average_volume=5000000, dividend_yield=0.013, withholding_rate=0.30, us_situs_risk_flag=True, liquidity_score=0.98, retrieved_at=retrieved_at, citations=[citations["blackrock_ivv"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="CSPX", name="iShares Core S&P 500 UCITS ETF (Acc)", asset_class="global_equity", domicile="IE", expense_ratio=0.0007, average_volume=1000000, dividend_yield=0.013, withholding_rate=0.15, us_situs_risk_flag=False, liquidity_score=0.90, retrieved_at=retrieved_at, citations=[citations["blackrock_cspx"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="IWDA", name="iShares Core MSCI World UCITS ETF", asset_class="global_equity", domicile="IE", expense_ratio=0.0020, average_volume=900000, dividend_yield=0.016, withholding_rate=0.15, us_situs_risk_flag=False, liquidity_score=0.88, retrieved_at=retrieved_at, citations=[citations["blackrock_iwda"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="VWRA", name="Vanguard FTSE All-World UCITS ETF (Acc)", asset_class="global_equity", domicile="IE", expense_ratio=0.0022, average_volume=850000, dividend_yield=0.017, withholding_rate=0.15, us_situs_risk_flag=False, liquidity_score=0.87, retrieved_at=retrieved_at, citations=[citations["vanguard_vwra"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="VWRD", name="Vanguard FTSE All-World UCITS ETF (Dist)", asset_class="global_equity", domicile="IE", expense_ratio=0.0022, average_volume=750000, dividend_yield=0.017, withholding_rate=0.15, us_situs_risk_flag=False, liquidity_score=0.86, retrieved_at=retrieved_at, citations=[citations["vanguard_vwrd"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
        ],
    )

    ig_bonds = _score_candidates(
        profile,
        [
            _candidate(symbol="BND", name="Vanguard Total Bond Market ETF", asset_class="ig_bonds", domicile="US", expense_ratio=0.0003, average_volume=7000000, dividend_yield=0.036, withholding_rate=0.30, us_situs_risk_flag=True, liquidity_score=0.95, yield_proxy=0.045, duration_years=6.1, retrieved_at=retrieved_at, citations=[citations["vanguard_bnd"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="AGGU", name="iShares Core Global Aggregate Bond UCITS ETF", asset_class="ig_bonds", domicile="IE", expense_ratio=0.0010, average_volume=350000, dividend_yield=0.031, withholding_rate=0.15, us_situs_risk_flag=False, liquidity_score=0.84, yield_proxy=0.040, duration_years=6.9, retrieved_at=retrieved_at, citations=[citations["blackrock_aggu"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="IGLA", name="iShares Global Government Bond UCITS ETF", asset_class="ig_bonds", domicile="IE", expense_ratio=0.0020, average_volume=140000, dividend_yield=0.027, withholding_rate=0.15, us_situs_risk_flag=False, liquidity_score=0.79, yield_proxy=0.034, duration_years=7.4, retrieved_at=retrieved_at, citations=[citations["blackrock_igla"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="A35", name="ABF Singapore Bond Index Fund", asset_class="ig_bonds", domicile="SG", expense_ratio=0.0024, average_volume=500000, dividend_yield=0.025, withholding_rate=0.00, us_situs_risk_flag=False, liquidity_score=0.83, yield_proxy=0.030, duration_years=7.0, retrieved_at=retrieved_at, citations=[citations["nikko_a35"], citations["iras_overseas_income"]]),
        ],
    )

    real_assets = _score_candidates(
        profile,
        [
            _candidate(symbol="VNQ", name="Vanguard Real Estate ETF", asset_class="real_assets", domicile="US", expense_ratio=0.0012, average_volume=4500000, dividend_yield=0.036, withholding_rate=0.30, us_situs_risk_flag=True, liquidity_score=0.94, retrieved_at=retrieved_at, citations=[citations["vanguard_vnq"], citations["irs_withholding_nra"], citations["iras_overseas_income"]], notes="US REIT income stream may have withholding drag for nonresident profiles."),
            _candidate(symbol="IWDP", name="iShares Developed Markets Property Yield UCITS ETF", asset_class="real_assets", domicile="IE", expense_ratio=0.0059, average_volume=120000, dividend_yield=0.031, withholding_rate=0.15, us_situs_risk_flag=False, liquidity_score=0.75, retrieved_at=retrieved_at, citations=[citations["blackrock_iwdp"], citations["irs_withholding_nra"], citations["iras_overseas_income"]], notes="Property yield exposure via UCITS wrapper."),
            _candidate(symbol="GLD", name="SPDR Gold Shares", asset_class="real_assets", domicile="US", expense_ratio=0.0040, average_volume=8000000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=True, liquidity_score=0.97, retrieved_at=retrieved_at, citations=[citations["ssga_gld"], citations["irs_withholding_nra"], citations["iras_overseas_income"]], notes="Commodity ETP structure and trust mechanics should be monitored."),
            _candidate(symbol="SGLN", name="iShares Physical Gold ETC", asset_class="real_assets", domicile="IE", expense_ratio=0.0012, average_volume=180000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=False, liquidity_score=0.78, retrieved_at=retrieved_at, citations=[citations["blackrock_sgln"], citations["iras_overseas_income"]], notes="Commodity ETP structure risk remains relevant."),
            _candidate(symbol="IGLN", name="iShares Physical Gold ETC", asset_class="real_assets", domicile="IE", expense_ratio=0.0015, average_volume=140000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=False, liquidity_score=0.74, retrieved_at=retrieved_at, citations=[citations["blackrock_igln"], citations["iras_overseas_income"]], notes="Commodity ETP structure risk remains relevant."),
        ],
    )

    alternatives = _score_candidates(
        profile,
        [
            _candidate(symbol="DBMF", name="iMGP DBi Managed Futures Strategy ETF", asset_class="alternatives", domicile="US", expense_ratio=0.0085, average_volume=350000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=True, liquidity_score=0.82, retail_accessible=True, margin_required=False, max_loss_known=True, retrieved_at=retrieved_at, citations=[citations["dbmf_site"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="KMLM", name="KFA Mount Lucas Managed Futures Index Strategy ETF", asset_class="alternatives", domicile="US", expense_ratio=0.0090, average_volume=120000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=True, liquidity_score=0.74, retail_accessible=True, margin_required=False, max_loss_known=True, retrieved_at=retrieved_at, citations=[citations["kmlm_site"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="TAIL", name="Cambria Tail Risk ETF", asset_class="alternatives", domicile="US", expense_ratio=0.0059, average_volume=380000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=True, liquidity_score=0.84, retail_accessible=True, margin_required=False, max_loss_known=True, retrieved_at=retrieved_at, citations=[citations["cambria_tail"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="CAOS", name="Alpha Architect Tail Risk ETF", asset_class="alternatives", domicile="US", expense_ratio=0.0068, average_volume=90000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=True, liquidity_score=0.70, retail_accessible=True, margin_required=False, max_loss_known=True, retrieved_at=retrieved_at, citations=[citations["alphaarchitect_caos"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
            _candidate(symbol="BTAL", name="AGF US Market Neutral Anti-Beta Fund", asset_class="alternatives", domicile="US", expense_ratio=0.0049, average_volume=280000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=True, liquidity_score=0.81, retail_accessible=True, margin_required=False, max_loss_known=True, retrieved_at=retrieved_at, citations=[citations["agf_btal"], citations["irs_withholding_nra"], citations["iras_overseas_income"]]),
        ],
    )

    convex = _score_candidates(
        profile,
        [
            _candidate(symbol="DBMF", name="Managed Futures Sleeve Example", asset_class="convex", domicile="US", expense_ratio=0.0085, average_volume=350000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=True, liquidity_score=0.82, allocation_weight=0.02, retail_accessible=True, margin_required=False, max_loss_known=True, retrieved_at=retrieved_at, citations=[citations["dbmf_site"], citations["irs_withholding_nra"], citations["iras_overseas_income"]], notes="Managed futures sleeve example at 2.0%."),
            _candidate(symbol="TAIL", name="Tail Hedge ETF Sleeve Example", asset_class="convex", domicile="US", expense_ratio=0.0059, average_volume=380000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=True, liquidity_score=0.84, allocation_weight=0.007, retail_accessible=True, margin_required=False, max_loss_known=True, retrieved_at=retrieved_at, citations=[citations["cambria_tail"], citations["irs_withholding_nra"], citations["iras_overseas_income"]], notes="Tail hedge sleeve example at 0.7%."),
            _candidate(symbol="SPY-LONG-PUT", name="SPY Long-Dated Put Example", asset_class="convex", domicile="US", expense_ratio=0.0, average_volume=50000, dividend_yield=0.0, withholding_rate=0.00, us_situs_risk_flag=True, liquidity_score=0.78, allocation_weight=0.003, retail_accessible=True, margin_required=False, max_loss_known=True, option_position="long_put", strike=600.0, expiry="2027-12-17", premium_paid_pct_nav=0.0030, annualized_carry_estimate=0.0015, retrieved_at=retrieved_at, citations=[citations["cboe_options"], citations["occ_options"], citations["irs_withholding_nra"]], notes="Long put only; max loss equals premium paid. No option writing, spreads, or undefined-loss structures."),
        ],
    )

    sg_tax_observations = [
        {
            "text": "US-domiciled ETF structures generally carry US situs estate-risk flags, while IE UCITS wrappers are modeled without US situs estate exposure under this SG profile lens.",
            "citations": [citations["irs_withholding_nra"], citations["iras_overseas_income"]],
        },
        {
            "text": "Modeled dividend withholding drag differs for US-domiciled versus IE UCITS pathways, which can influence net implementation outcomes despite similar benchmark exposure.",
            "citations": [citations["irs_withholding_nra"], citations["iras_overseas_income"], citations["blackrock_cspx"]],
        },
        {
            "text": "Liquidity proxies vary across listings and wrappers; tax-efficiency and trading-liquidity tradeoffs should be monitored jointly in implementation reviews.",
            "citations": [citations["ssga_spy"], citations["blackrock_iwda"], citations["vanguard_vwra"]],
        },
    ]

    watchlist_candidates: list[dict[str, Any]] = []
    if opportunities:
        lookup = {
            "volatility": ["TAIL", "CAOS", "BTAL"],
            "credit": ["AGGU", "IGLA"],
            "yield": ["BND", "A35"],
            "equity": ["CSPX", "IWDA", "VWRA"],
        }
        all_candidates = {candidate.symbol: candidate for candidate in [*global_equity, *ig_bonds, *alternatives, *convex, *real_assets]}
        for obs in opportunities[:3]:
            text = str(obs.get("condition_observed", "")).lower()
            keys = []
            if any(word in text for word in ["vix", "volatility", "tail"]):
                keys.append("volatility")
            if any(word in text for word in ["credit", "spread", "oas"]):
                keys.append("credit")
            if any(word in text for word in ["yield", "rates", "treasury"]):
                keys.append("yield")
            if any(word in text for word in ["equity", "sp500", "msci", "world"]):
                keys.append("equity")
            for key in keys or ["equity"]:
                for symbol in lookup.get(key, []):
                    candidate = all_candidates.get(symbol)
                    if candidate is None:
                        continue
                    watchlist_candidates.append(
                        {
                            "condition": f"Candidate instrument aligned with observed condition: {obs.get('condition_observed', '')}",
                            "symbol": candidate.symbol,
                            "name": candidate.name,
                            "time_horizon": obs.get("time_horizon", "short"),
                            "tax_score": candidate.tax_score,
                            "liquidity_score": candidate.liquidity_score,
                            "citations": [*obs.get("citations", []), *candidate.citations[:1]],
                        }
                    )
                if watchlist_candidates:
                    break

    dedup_watch: dict[str, dict[str, Any]] = {}
    for item in watchlist_candidates:
        dedup_watch[f"{item['condition']}|{item['symbol']}"] = item
    watchlist_ranked = sorted(
        dedup_watch.values(),
        key=lambda item: (float(item.get("tax_score") or 0.0), float(item.get("liquidity_score") or 0.0)),
        reverse=True,
    )[:6]

    return {
        "label": "Implementation Mapping – Illustrative Instruments",
        "disclaimer": "Illustrative candidates, not recommendations.",
        "sleeves": {
            "global_equity": {
                "title": "Global Equity",
                "candidates": global_equity,
                "sg_tax_observations": sg_tax_observations,
            },
            "ig_bonds": {"title": "IG Bonds", "candidates": ig_bonds},
            "real_assets": {"title": "Real Assets", "candidates": real_assets},
            "alternatives": {"title": "Alternatives", "candidates": alternatives},
            "convex": {"title": "Convex", "candidates": convex},
        },
        "watchlist_candidates": watchlist_ranked,
        "source_records": list(sources.values()),
    }
