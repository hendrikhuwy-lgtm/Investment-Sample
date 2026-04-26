from __future__ import annotations

from typing import Any


def _text(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


_EQUITY_INDEX_SCOPE_TYPES = {
    "equity_index",
    "broad_global_equity",
    "developed_market_equity",
    "emerging_market_equity",
    "single_country_equity",
    "real_estate_equity",
}


def _scope(
    *,
    scope_type: str,
    display_title: str,
    summary: str,
    covers: list[str],
    does_not_cover: list[str],
    sleeve_relevance: str,
    specificity: str,
    source_basis: str,
    confidence: str,
    label: str | None = None,
    index_name: str | None = None,
    market_cap_scope: str | None = None,
    country_count: str | None = None,
    constituent_count: str | None = None,
    emerging_markets_included: bool | None = None,
) -> dict[str, Any]:
    return {
        "label": label or ("Index scope" if scope_type in _EQUITY_INDEX_SCOPE_TYPES else "Exposure scope"),
        "scope_type": scope_type,
        "display_title": display_title,
        "summary": summary,
        "covers": covers,
        "does_not_cover": does_not_cover,
        "sleeve_relevance": sleeve_relevance,
        "specificity": specificity,
        "source_basis": source_basis,
        "confidence": confidence,
        "index_name": index_name,
        "coverage_statement": summary,
        "includes_statement": "; ".join(covers) if covers else None,
        "excludes_statement": "; ".join(does_not_cover) if does_not_cover else None,
        "market_cap_scope": market_cap_scope,
        "country_count": country_count,
        "constituent_count": constituent_count,
        "emerging_markets_included": emerging_markets_included,
    }


def _with_holdings(scope: dict[str, Any], holdings_count: str | None) -> dict[str, Any]:
    if not holdings_count or scope.get("constituent_count"):
        return dict(scope)
    updated = dict(scope)
    updated["constituent_count"] = holdings_count
    return updated


def _merge_legacy_scope(scope: dict[str, Any], legacy: dict[str, Any] | None) -> dict[str, Any]:
    if not legacy:
        return dict(scope)
    if scope.get("scope_type") not in {
        "equity_index",
        "broad_global_equity",
        "developed_market_equity",
        "emerging_market_equity",
        "single_country_equity",
    }:
        return dict(scope)
    merged = dict(scope)
    for key in (
        "index_name",
        "country_count",
        "constituent_count",
        "market_cap_scope",
        "emerging_markets_included",
    ):
        if merged.get(key) in (None, "") and legacy.get(key) not in (None, ""):
            merged[key] = legacy.get(key)
    return merged


def _legacy_to_scope(legacy: dict[str, Any], holdings_count: str | None) -> dict[str, Any]:
    index_name = _text(legacy.get("index_name")) or "Indexed exposure"
    summary = _text(legacy.get("coverage_statement")) or f"{index_name} exposure."
    covers = [_text(legacy.get("includes_statement")) or "Primary benchmark exposure."]
    does_not_cover = [_text(legacy.get("excludes_statement")) or "Does not by itself settle wrapper quality or trading fit."]
    return _with_holdings(
        _scope(
            scope_type="equity_index",
            display_title=index_name,
            summary=summary,
            covers=[item for item in covers if item],
            does_not_cover=[item for item in does_not_cover if item],
            sleeve_relevance="Useful for understanding the benchmark exposure, but wrapper and sleeve fit still need separate review.",
            specificity="exact",
            source_basis="candidate_registry",
            confidence="high",
            label="Index scope",
            index_name=index_name,
            market_cap_scope=_text(legacy.get("market_cap_scope")),
            country_count=_text(legacy.get("country_count")),
            constituent_count=_text(legacy.get("constituent_count")),
            emerging_markets_included=legacy.get("emerging_markets_included"),
        ),
        holdings_count,
    )


_CANDIDATE_SCOPE_REGISTRY: dict[str, dict[str, Any]] = {
    "CSPX": _scope(
        scope_type="equity_index",
        display_title="S&P 500 large-cap U.S. equity",
        summary="Tracks large-cap U.S. equity exposure represented by the S&P 500.",
        covers=["Large-cap U.S. listed companies", "Core U.S. equity beta inside the global equity sleeve"],
        does_not_cover=["U.S. small caps", "Non-U.S. developed markets", "Emerging markets"],
        sleeve_relevance="Useful only if the sleeve wants a U.S. large-cap building block rather than a broad global core line.",
        specificity="exact",
        source_basis="candidate_registry",
        confidence="high",
        label="Index scope",
        index_name="S&P 500 Index",
        market_cap_scope="Large cap",
        emerging_markets_included=False,
    ),
    "SSAC": _scope(
        scope_type="broad_global_equity",
        display_title="MSCI ACWI broad global equity",
        summary="Covers developed and emerging market equities in one broad global benchmark.",
        covers=["Developed market equities", "Emerging market equities", "Large and mid-cap companies"],
        does_not_cover=["A developed-markets-only mandate", "A separate regional or single-country satellite"],
        sleeve_relevance="Fits a broad global equity core job when the portfolio wants one line for developed plus emerging markets.",
        specificity="exact",
        source_basis="candidate_registry",
        confidence="high",
        index_name="MSCI ACWI Index",
        market_cap_scope="Large and mid cap",
        emerging_markets_included=True,
    ),
    "IWDA": _scope(
        scope_type="developed_market_equity",
        display_title="MSCI World developed-market equity",
        summary="Covers developed market equities and excludes emerging markets.",
        covers=["Developed market large and mid-cap equities", "Broad non-EM global equity exposure"],
        does_not_cover=["Emerging markets", "Small caps", "A full all-country global mandate"],
        sleeve_relevance="Fits a developed-market core role but needs a separate emerging-market line if the sleeve requires all-country coverage.",
        specificity="exact",
        source_basis="candidate_registry",
        confidence="high",
        index_name="MSCI World Index",
        market_cap_scope="Large and mid cap",
        emerging_markets_included=False,
    ),
    "VWRA": _scope(
        scope_type="broad_global_equity",
        display_title="FTSE All-World broad global equity",
        summary="Covers developed and emerging market equities through a broad all-world benchmark.",
        covers=["Developed market equities", "Emerging market equities", "Large and mid-cap companies"],
        does_not_cover=["Small-cap completion", "A region-specific satellite job"],
        sleeve_relevance="Fits the global core sleeve when one ETF should carry broad developed plus emerging equity exposure.",
        specificity="exact",
        source_basis="candidate_registry",
        confidence="high",
        index_name="FTSE All-World Index",
        market_cap_scope="Large and mid cap",
        emerging_markets_included=True,
    ),
    "VWRL": _scope(
        scope_type="broad_global_equity",
        display_title="FTSE All-World broad global equity",
        summary="Covers developed and emerging market equities through a broad all-world benchmark.",
        covers=["Developed market equities", "Emerging market equities", "Large and mid-cap companies"],
        does_not_cover=["Small-cap completion", "A region-specific satellite job"],
        sleeve_relevance="Fits the global core sleeve when one ETF should carry broad developed plus emerging equity exposure.",
        specificity="exact",
        source_basis="candidate_registry",
        confidence="high",
        index_name="FTSE All-World Index",
        market_cap_scope="Large and mid cap",
        emerging_markets_included=True,
    ),
    "VEVE": _scope(
        scope_type="developed_market_equity",
        display_title="FTSE Developed market equity",
        summary="Covers developed market equities and excludes emerging markets.",
        covers=["Developed market equities", "Large and mid-cap developed-market exposure"],
        does_not_cover=["Emerging markets", "Single-country satellite exposure"],
        sleeve_relevance="Fits a developed-market core role but does not complete the all-country global equity job by itself.",
        specificity="exact",
        source_basis="candidate_registry",
        confidence="high",
        index_name="FTSE Developed Index",
        market_cap_scope="Large and mid cap",
        emerging_markets_included=False,
    ),
    "EIMI": _scope(
        scope_type="emerging_market_equity",
        display_title="MSCI Emerging Markets IMI equity",
        summary="Covers emerging market equities, including broader market-cap coverage than a large-cap-only EM line.",
        covers=["Emerging market equities", "Large, mid, and small-cap EM exposure"],
        does_not_cover=["Developed markets", "A China-only sleeve job"],
        sleeve_relevance="Fits an emerging-market completion role rather than the main developed-market core.",
        specificity="exact",
        source_basis="candidate_registry",
        confidence="high",
        index_name="MSCI Emerging Markets IMI Index",
        market_cap_scope="Large, mid, and small cap",
        emerging_markets_included=True,
    ),
    "VFEA": _scope(
        scope_type="emerging_market_equity",
        display_title="FTSE Emerging market equity",
        summary="Covers broad emerging market equity exposure.",
        covers=["Emerging market equities", "Diversified EM country exposure"],
        does_not_cover=["Developed markets", "A China-only sleeve job"],
        sleeve_relevance="Fits an emerging-market completion role rather than the main developed-market core.",
        specificity="exact",
        source_basis="candidate_registry",
        confidence="high",
        index_name="FTSE Emerging Index",
        emerging_markets_included=True,
    ),
    "HMCH": _scope(
        scope_type="single_country_equity",
        display_title="China equity exposure",
        summary="Represents a China equity sleeve candidate rather than broad emerging-market exposure.",
        covers=["China equity exposure", "Single-country satellite role"],
        does_not_cover=["Broad emerging markets", "Developed markets", "A global equity core mandate"],
        sleeve_relevance="Fits only if the portfolio wants a dedicated China satellite rather than broader EM exposure.",
        specificity="category",
        source_basis="candidate_registry",
        confidence="medium",
        market_cap_scope="China equity",
        emerging_markets_included=True,
    ),
    "XCHA": _scope(
        scope_type="single_country_equity",
        display_title="China A-share equity exposure",
        summary="Represents a China A-share sleeve candidate rather than broad emerging-market exposure.",
        covers=["Mainland China A-share exposure", "Single-country satellite role"],
        does_not_cover=["Broad emerging markets", "Developed markets", "A global equity core mandate"],
        sleeve_relevance="Fits only if the portfolio wants a dedicated China satellite and accepts narrower country concentration.",
        specificity="category",
        source_basis="candidate_registry",
        confidence="medium",
        market_cap_scope="China A-share equity",
        emerging_markets_included=True,
    ),
    "A35": _scope(
        scope_type="bond_index",
        display_title="Singapore SGD government and quasi-sovereign bonds",
        summary="Covers Singapore dollar government and quasi-sovereign bond exposure for the safer bond sleeve.",
        covers=["Singapore government bond exposure", "Singapore dollar bond ballast", "Quasi-sovereign bond exposure where present"],
        does_not_cover=["Global aggregate bonds", "Corporate credit beta", "Equity risk"],
        sleeve_relevance="Fits a safer bond sleeve only if a Singapore dollar sovereign-heavy line is the intended job.",
        specificity="category",
        source_basis="candidate_registry",
        confidence="medium",
        index_name="ABF Singapore Bond Index",
    ),
    "AGGU": _scope(
        scope_type="aggregate_bond",
        display_title="Global aggregate investment-grade bonds",
        summary="Covers global aggregate investment-grade bond exposure across sovereign and corporate issuers.",
        covers=["Investment-grade sovereign bonds", "Investment-grade corporate bonds", "Global aggregate bond ballast"],
        does_not_cover=["High-yield credit", "Equity risk", "Local single-country bond concentration"],
        sleeve_relevance="Fits an IG bond sleeve when the job is diversified global aggregate fixed income.",
        specificity="category",
        source_basis="candidate_registry",
        confidence="high",
    ),
    "VAGU": _scope(
        scope_type="aggregate_bond",
        display_title="Global aggregate investment-grade bonds",
        summary="Covers global aggregate investment-grade bond exposure across sovereign and corporate issuers.",
        covers=["Investment-grade sovereign bonds", "Investment-grade corporate bonds", "Global aggregate bond ballast"],
        does_not_cover=["High-yield credit", "Equity risk", "Local single-country bond concentration"],
        sleeve_relevance="Fits an IG bond sleeve when the job is diversified global aggregate fixed income.",
        specificity="category",
        source_basis="candidate_registry",
        confidence="high",
    ),
    "BIL": _scope(
        scope_type="cash_treasury_bills",
        display_title="Short-duration U.S. Treasury bills",
        summary="Covers short-duration U.S. Treasury bill exposure for liquid capital and cash-like ballast.",
        covers=["Short U.S. Treasury bills", "Low-duration cash substitute exposure"],
        does_not_cover=["Long-duration bonds", "Credit risk seeking exposure", "Equity risk"],
        sleeve_relevance="Fits the cash and bills sleeve when the job is liquidity reserve with minimal duration risk.",
        specificity="exact",
        source_basis="candidate_registry",
        confidence="high",
        index_name="Bloomberg 1-3 Month T-Bill Index",
    ),
    "BILS": _scope(
        scope_type="cash_treasury_bills",
        display_title="Short-duration U.S. Treasury bills",
        summary="Covers short-duration U.S. Treasury bill exposure for liquid capital and cash-like ballast.",
        covers=["Short U.S. Treasury bills", "Low-duration cash substitute exposure"],
        does_not_cover=["Long-duration bonds", "Credit risk seeking exposure", "Equity risk"],
        sleeve_relevance="Fits the cash and bills sleeve when the job is liquidity reserve with modest bill duration.",
        specificity="category",
        source_basis="candidate_registry",
        confidence="high",
    ),
    "IB01": _scope(
        scope_type="cash_treasury_bills",
        display_title="0-1 year U.S. Treasury bond exposure",
        summary="Covers very short U.S. Treasury exposure for liquid capital with limited duration risk.",
        covers=["0-1 year U.S. Treasury exposure", "Cash-like Treasury ballast"],
        does_not_cover=["Long-duration bonds", "Corporate credit", "Equity risk"],
        sleeve_relevance="Fits the cash and bills sleeve when the job is liquid Treasury exposure with low duration.",
        specificity="category",
        source_basis="candidate_registry",
        confidence="high",
    ),
    "SGOV": _scope(
        scope_type="cash_treasury_bills",
        display_title="0-3 month U.S. Treasury bills",
        summary="Covers ultra-short U.S. Treasury bill exposure for liquid capital and cash-like ballast.",
        covers=["0-3 month U.S. Treasury bills", "Low-duration cash substitute exposure"],
        does_not_cover=["Long-duration bonds", "Corporate credit", "Equity risk"],
        sleeve_relevance="Fits the cash and bills sleeve when the job is liquidity reserve with minimal duration risk.",
        specificity="exact",
        source_basis="candidate_registry",
        confidence="high",
        index_name="ICE 0-3 Month US Treasury Securities Index",
    ),
    "CMOD": _scope(
        scope_type="commodity_basket",
        display_title="Broad commodity basket exposure",
        summary="Represents diversified commodity exposure rather than equity, bond, or cash exposure.",
        covers=["Commodity futures basket exposure", "Real-asset inflation-sensitive exposure"],
        does_not_cover=["Physical gold-only exposure", "Equity REIT exposure", "Bond income"],
        sleeve_relevance="Fits the real assets sleeve only if the intended role is broad commodity beta.",
        specificity="category",
        source_basis="candidate_registry",
        confidence="medium",
    ),
    "IWDP": _scope(
        scope_type="real_estate_equity",
        display_title="Developed-market property equity exposure",
        summary="Covers listed property and real estate equity exposure rather than physical property ownership.",
        covers=["Listed property companies", "Real estate equity exposure", "Developed-market property sleeve role"],
        does_not_cover=["Direct physical real estate", "Broad commodity exposure", "Bond income"],
        sleeve_relevance="Fits the real assets sleeve if the desired risk is listed property equity beta.",
        specificity="category",
        source_basis="candidate_registry",
        confidence="medium",
    ),
    "SGLN": _scope(
        scope_type="physical_gold",
        display_title="Physical gold exposure",
        summary="Represents physical gold exposure rather than diversified commodities or equity exposure.",
        covers=["Gold price exposure", "Precious metal store-of-value role"],
        does_not_cover=["Broad commodity baskets", "Gold miners equity", "Income-producing assets"],
        sleeve_relevance="Fits the real assets sleeve when the desired job is direct gold exposure.",
        specificity="category",
        source_basis="candidate_registry",
        confidence="high",
    ),
    "DBMF": _scope(
        scope_type="managed_futures_strategy",
        display_title="Managed futures trend-following strategy",
        summary="Represents a managed futures strategy that can hold long and short futures exposures across asset classes.",
        covers=["Trend-following futures strategy exposure", "Potential diversifying crisis-alpha behavior"],
        does_not_cover=["Static equity beta", "Static bond beta", "A transparent single index sleeve"],
        sleeve_relevance="Fits an alternatives or convex protection role only if the strategy behavior, costs, and evidence clear review.",
        specificity="strategy",
        source_basis="candidate_registry",
        confidence="medium",
    ),
    "KMLM": _scope(
        scope_type="managed_futures_strategy",
        display_title="Managed futures trend-following index strategy",
        summary="Represents a managed futures trend-following strategy across futures markets.",
        covers=["Trend-following futures exposure", "Diversifying alternative strategy role"],
        does_not_cover=["Static equity beta", "Static bond beta", "A broad commodity-only mandate"],
        sleeve_relevance="Fits an alternatives or convex protection role only if strategy evidence and wrapper implementation are acceptable.",
        specificity="strategy",
        source_basis="candidate_registry",
        confidence="medium",
    ),
    "TAIL": _scope(
        scope_type="tail_risk_strategy",
        display_title="Tail-risk strategy with Treasury collateral",
        summary="Represents a defensive tail-risk strategy, not a plain Treasury bill or equity index fund.",
        covers=["Downside hedge strategy exposure", "Options-linked protection behavior", "Short-duration Treasury collateral where present"],
        does_not_cover=["Plain short Treasury bill exposure only", "Long-only equity beta", "Broad bond income"],
        sleeve_relevance="Fits convex protection only if the portfolio needs explicit downside hedging and accepts strategy drag.",
        specificity="strategy",
        source_basis="candidate_registry",
        confidence="medium",
    ),
    "CAOS": _scope(
        scope_type="tail_risk_strategy",
        display_title="S&P 500-linked tail-risk strategy",
        summary="Represents an S&P 500-linked tail-risk strategy, not a plain S&P 500 equity holding.",
        covers=["Downside hedge strategy exposure", "S&P 500-linked options or tail-risk behavior"],
        does_not_cover=["Plain long-only S&P 500 exposure", "Broad global equity exposure", "Bond income"],
        sleeve_relevance="Fits convex protection only if the portfolio needs explicit downside hedging and accepts strategy drag.",
        specificity="strategy",
        source_basis="candidate_registry",
        confidence="medium",
    ),
}


def _taxonomy_scope(
    *,
    benchmark_full_name: str | None,
    benchmark_family: str | None,
    benchmark_key: str | None,
    exposure_label: str | None,
    asset_class: str | None,
    sleeve_key: str | None,
) -> dict[str, Any] | None:
    combined = " ".join(
        filter(
            None,
            [
                _text(benchmark_full_name),
                _text(benchmark_family),
                _text(benchmark_key),
                _text(exposure_label),
                _text(asset_class),
                _text(sleeve_key),
            ],
        )
    ).lower()
    if not combined:
        return None
    if "treasury bill" in combined or "t-bill" in combined or "short treasury" in combined or "0-3 month" in combined:
        return _scope(
            scope_type="cash_treasury_bills",
            display_title="Short-duration Treasury exposure",
            summary="Scope is represented as short-duration Treasury exposure based on the available benchmark and sleeve fields.",
            covers=["Short government bill or Treasury exposure", "Cash-like duration profile"],
            does_not_cover=["Long-duration bonds", "Equity risk"],
            sleeve_relevance="Useful when the sleeve job is liquidity reserve or cash ballast.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
        )
    if "global aggregate" in combined or "aggregate bond" in combined:
        return _scope(
            scope_type="aggregate_bond",
            display_title="Global aggregate bond exposure",
            summary="Scope is represented as aggregate investment-grade bond exposure based on available benchmark fields.",
            covers=["Sovereign and corporate investment-grade bond exposure", "Diversified bond ballast"],
            does_not_cover=["High-yield credit", "Equity risk"],
            sleeve_relevance="Useful when the sleeve job is diversified investment-grade fixed income.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
        )
    if "inflation linked" in combined or "inflation-linked" in combined:
        return _scope(
            scope_type="inflation_linked_bond",
            display_title="Inflation-linked bond exposure",
            summary="Scope is represented as inflation-linked bond exposure based on available benchmark fields.",
            covers=["Inflation-linked government bond exposure"],
            does_not_cover=["Nominal aggregate bonds", "Equity risk"],
            sleeve_relevance="Useful when the sleeve job is inflation-sensitive fixed income.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
        )
    if "gold" in combined:
        return _scope(
            scope_type="physical_gold",
            display_title="Gold exposure",
            summary="Scope is represented as gold exposure based on available benchmark and exposure fields.",
            covers=["Gold price exposure"],
            does_not_cover=["Broad commodities", "Gold miners equity"],
            sleeve_relevance="Useful when the real assets sleeve needs direct gold-like exposure.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
        )
    if "commodity" in combined:
        return _scope(
            scope_type="commodity_basket",
            display_title="Commodity basket exposure",
            summary="Scope is represented as commodity exposure based on available benchmark and exposure fields.",
            covers=["Broad commodity exposure"],
            does_not_cover=["Equity REITs", "Bond income"],
            sleeve_relevance="Useful when the real assets sleeve needs broad commodity beta.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
        )
    if "reit" in combined or "property" in combined or "real estate" in combined:
        return _scope(
            scope_type="real_estate_equity",
            display_title="Listed real estate equity exposure",
            summary="Scope is represented as listed real estate equity exposure based on available fields.",
            covers=["Listed property or REIT-like equity exposure"],
            does_not_cover=["Direct physical property ownership", "Bond income"],
            sleeve_relevance="Useful when the real assets sleeve needs listed property equity beta.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
        )
    if "managed futures" in combined or "trend following" in combined or "trend-following" in combined:
        return _scope(
            scope_type="managed_futures_strategy",
            display_title="Managed futures strategy exposure",
            summary="Scope is represented as managed futures strategy exposure because the product is not a standard long-only index ETF.",
            covers=["Rules-based or active futures strategy exposure"],
            does_not_cover=["Static equity beta", "Static bond beta"],
            sleeve_relevance="Useful only if the alternatives sleeve wants diversifying strategy exposure.",
            specificity="strategy",
            source_basis="benchmark_taxonomy",
            confidence="medium",
        )
    if "tail risk" in combined or "tail-risk" in combined or "downside" in combined:
        return _scope(
            scope_type="tail_risk_strategy",
            display_title="Tail-risk strategy exposure",
            summary="Scope is represented as tail-risk strategy exposure because the product is not a standard long-only index ETF.",
            covers=["Downside hedge strategy exposure"],
            does_not_cover=["Plain equity index exposure", "Plain Treasury bill exposure"],
            sleeve_relevance="Useful only if the sleeve explicitly wants convex protection behavior.",
            specificity="strategy",
            source_basis="benchmark_taxonomy",
            confidence="medium",
        )
    if "china" in combined:
        return _scope(
            scope_type="single_country_equity",
            display_title="China equity exposure",
            summary="Scope is represented as China equity exposure based on the available benchmark and exposure fields.",
            covers=["Single-country China equity exposure"],
            does_not_cover=["Broad emerging markets", "Developed markets"],
            sleeve_relevance="Useful only for a dedicated China satellite role.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
            emerging_markets_included=True,
        )
    if "emerging" in combined:
        return _scope(
            scope_type="emerging_market_equity",
            display_title="Emerging market equity exposure",
            summary="Scope is represented as emerging market equity exposure based on available benchmark fields.",
            covers=["Emerging market equities"],
            does_not_cover=["Developed markets"],
            sleeve_relevance="Useful as an EM completion sleeve rather than the whole global core.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
            emerging_markets_included=True,
        )
    if "all-world" in combined or "all world" in combined or "acwi" in combined:
        return _scope(
            scope_type="broad_global_equity",
            display_title="Broad global equity exposure",
            summary="Scope is represented as developed plus emerging market global equity exposure.",
            covers=["Developed market equities", "Emerging market equities"],
            does_not_cover=["A developed-markets-only mandate"],
            sleeve_relevance="Useful when the global core job needs broad all-country equity exposure.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
            emerging_markets_included=True,
        )
    if "msci world" in combined or "developed market" in combined or "developed-market" in combined or "ftse developed" in combined:
        return _scope(
            scope_type="developed_market_equity",
            display_title="Developed market equity exposure",
            summary="Scope is represented as developed market equity exposure based on available benchmark fields.",
            covers=["Developed market equities"],
            does_not_cover=["Emerging markets"],
            sleeve_relevance="Useful when the sleeve wants developed-market equity beta.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
            emerging_markets_included=False,
        )
    if "s&p 500" in combined or "sp 500" in combined:
        return _scope(
            scope_type="equity_index",
            display_title="S&P 500 large-cap U.S. equity",
            summary="Scope is represented as large-cap U.S. equity exposure based on the available benchmark fields.",
            covers=["Large-cap U.S. listed companies"],
            does_not_cover=["Non-U.S. developed markets", "Emerging markets"],
            sleeve_relevance="Useful only if the sleeve wants U.S. large-cap equity exposure.",
            specificity="category",
            source_basis="benchmark_taxonomy",
            confidence="medium",
            label="Index scope",
            index_name="S&P 500 Index",
            market_cap_scope="Large cap",
            emerging_markets_included=False,
        )
    return None


def _sleeve_fallback(
    *,
    sleeve_key: str | None,
    asset_class: str | None,
    exposure_label: str | None,
) -> dict[str, Any]:
    combined = " ".join(filter(None, [_text(sleeve_key), _text(asset_class), _text(exposure_label)])).lower()
    if "bond" in combined or "fixed" in combined:
        title = "Fixed income sleeve exposure"
        summary = "Scope is shown at fixed income asset-class level because the exact benchmark line is not explicit enough for a narrower claim."
        covers = ["Broad fixed income implementation candidate"]
        excludes = ["Equity risk", "A more precise benchmark claim"]
    elif "cash" in combined or "bill" in combined:
        title = "Cash and bills sleeve exposure"
        summary = "Scope is shown at cash and short-bill level because the exact benchmark line is not explicit enough for a narrower claim."
        covers = ["Cash-like or short bill implementation candidate"]
        excludes = ["Long-duration bonds", "Equity risk"]
    elif "real" in combined or "commodity" in combined or "gold" in combined:
        title = "Real assets sleeve exposure"
        summary = "Scope is shown at real-assets level because the exact benchmark line is not explicit enough for a narrower claim."
        covers = ["Real-asset implementation candidate"]
        excludes = ["Plain equity or bond sleeve exposure"]
    elif "alternative" in combined or "convex" in combined or "tail" in combined:
        title = "Alternative strategy exposure"
        summary = "Scope is shown at strategy level because the exact benchmark line is not explicit enough for a narrower claim."
        covers = ["Alternative or defensive strategy candidate"]
        excludes = ["Plain long-only index exposure"]
    elif "equity" in combined or "market" in combined:
        title = "Equity sleeve exposure"
        summary = "Scope is shown at broad equity sleeve level because the exact benchmark line is not explicit enough for a narrower claim."
        covers = ["Equity implementation candidate"]
        excludes = ["A more precise country, region, or index claim"]
    else:
        title = "Sleeve-level exposure"
        summary = "Scope is shown at sleeve and asset-class level because the exact benchmark line is not explicit enough for a narrower claim."
        covers = ["Current sleeve implementation candidate"]
        excludes = ["A more precise benchmark claim"]
    return _scope(
        scope_type="fallback_asset_class",
        display_title=title,
        summary=summary,
        covers=covers,
        does_not_cover=excludes,
        sleeve_relevance="Use this as a broad exposure read only; benchmark-level precision still needs stronger source fields.",
        specificity="fallback",
        source_basis="sleeve_asset_class_fallback",
        confidence="low",
    )


def resolve_index_scope_explainer(
    *,
    symbol: str,
    benchmark_full_name: str | None = None,
    benchmark_family: str | None = None,
    benchmark_key: str | None = None,
    exposure_label: str | None = None,
    asset_class: str | None = None,
    sleeve_key: str | None = None,
    holdings_count: str | None = None,
    seeded_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").strip().upper()
    registered = _CANDIDATE_SCOPE_REGISTRY.get(normalized_symbol)
    if registered:
        return _with_holdings(_merge_legacy_scope(registered, seeded_scope), holdings_count)
    if seeded_scope:
        return _legacy_to_scope(seeded_scope, holdings_count)
    taxonomy = _taxonomy_scope(
        benchmark_full_name=benchmark_full_name,
        benchmark_family=benchmark_family,
        benchmark_key=benchmark_key,
        exposure_label=exposure_label,
        asset_class=asset_class,
        sleeve_key=sleeve_key,
    )
    if taxonomy:
        return _with_holdings(taxonomy, holdings_count)
    return _sleeve_fallback(
        sleeve_key=sleeve_key,
        asset_class=asset_class,
        exposure_label=exposure_label,
    )
