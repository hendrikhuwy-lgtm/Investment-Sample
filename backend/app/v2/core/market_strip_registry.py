from __future__ import annotations

from typing import Any


_MARKET_STRIP_SPECS: dict[str, dict[str, Any]] = {
    # ── Daily market-close ────────────────────────────────────────────────────
    "^GSPC": {
        "label": "S&P 500",
        "symbol_family": "index",
        "asset_class": "equity",
        "routed_family": "market_close",
        "routed_identifier": "^GSPC",
        "quote_identifier": "^GSPC",
        "source_type": "market_close",
        "metric_definition": "S&P 500 index official daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "^DJI": {
        "label": "Dow",
        "symbol_family": "index",
        "asset_class": "equity",
        "routed_family": "market_close",
        "routed_identifier": "^DJI",
        "quote_identifier": "^DJI",
        "source_type": "market_close",
        "metric_definition": "Dow Jones Industrial Average official daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "^IXIC": {
        "label": "Nasdaq",
        "symbol_family": "index",
        "asset_class": "equity",
        "routed_family": "market_close",
        "routed_identifier": "^IXIC",
        "quote_identifier": "^IXIC",
        "source_type": "market_close",
        "metric_definition": "Nasdaq Composite official daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "^RUT": {
        "label": "Russell 2K",
        "symbol_family": "index",
        "asset_class": "equity",
        "routed_family": "market_close",
        "routed_identifier": "^RUT",
        "quote_identifier": "^RUT",
        "source_type": "market_close",
        "metric_definition": "Russell 2000 index official daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "^SPXEW": {
        "label": "S&P Equal Weight",
        "symbol_family": "index",
        "asset_class": "equity",
        "routed_family": "market_close",
        "routed_identifier": "^SPXEW",
        "quote_identifier": "^SPXEW",
        "source_type": "market_close",
        "metric_definition": "S&P 500 Equal Weight Index official daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "^VIX": {
        "label": "VIX",
        "symbol_family": "index",
        "asset_class": "volatility",
        "routed_family": "market_close",
        "routed_identifier": "^VIX",
        "quote_identifier": "^VIX",
        "source_type": "market_close",
        "metric_definition": "CBOE Volatility Index official daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "DXY": {
        "label": "FX / USD",
        "symbol_family": "dollar_index",
        "asset_class": "cash",
        "routed_family": "market_close",
        "routed_identifier": "DXY",
        "quote_identifier": "DXY",
        "source_type": "market_close",
        "metric_definition": "US Dollar Index daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "^KRX": {
        "label": "Regional",
        "symbol_family": "index",
        "asset_class": "equity",
        "routed_family": "market_close",
        "routed_identifier": "^KRX",
        "quote_identifier": "^KRX",
        "source_type": "market_close",
        "metric_definition": "KBW Nasdaq Regional Banking Index daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "^TNX": {
        "label": "Rates",
        "symbol_family": "index",
        "asset_class": "fixed_income",
        "routed_family": "market_close",
        "routed_identifier": "^TNX",
        "quote_identifier": "^TNX",
        "source_type": "market_close",
        "metric_definition": "US Treasury 10-Year yield index daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "threshold",
    },
    "^TYX": {
        "label": "UST 30Y",
        "symbol_family": "index",
        "asset_class": "fixed_income",
        "routed_family": "market_close",
        "routed_identifier": "^TYX",
        "quote_identifier": "^TYX",
        "source_type": "market_close",
        "metric_definition": "US Treasury 30-Year yield index daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "threshold",
    },
    "GC=F": {
        "label": "Gold",
        "symbol_family": "futures",
        "asset_class": "real_assets",
        "routed_family": "market_close",
        "routed_identifier": "GC=F",
        "quote_identifier": "GC=F",
        "source_type": "market_close",
        "metric_definition": "COMEX Gold futures daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "^990100-USD-STRD": {
        "label": "World Equity",
        "symbol_family": "index",
        "asset_class": "equity",
        "routed_family": "market_close",
        "routed_identifier": "^990100-USD-STRD",
        "quote_identifier": "^990100-USD-STRD",
        "source_type": "market_close",
        "metric_definition": "MSCI World Index daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "BZ=F": {
        "label": "Brent Crude",
        "symbol_family": "futures",
        "asset_class": "commodity",
        "routed_family": "market_close",
        "routed_identifier": "BZ=F",
        "quote_identifier": "BZ=F",
        "source_type": "market_close",
        "metric_definition": "ICE Brent crude futures daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "CL=F": {
        "label": "WTI Crude",
        "symbol_family": "futures",
        "asset_class": "commodity",
        "routed_family": "market_close",
        "routed_identifier": "CL=F",
        "quote_identifier": "CL=F",
        "source_type": "market_close",
        "metric_definition": "NYMEX WTI crude futures daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    "BTC-USD": {
        "label": "Bitcoin",
        "symbol_family": "crypto",
        "asset_class": "alternative",
        "routed_family": "market_close",
        "routed_identifier": "BTC-USD",
        "quote_identifier": "BTC-USD",
        "source_type": "market_close",
        "metric_definition": "BTC-USD daily close",
        "validation_policy": "exact_daily_close",
        "chart_mode": "regime",
    },
    # ── Daily official releases ───────────────────────────────────────────────
    "BAMLH0A0HYM2": {
        "label": "Credit",
        "symbol_family": "macro_series",
        "asset_class": "fixed_income",
        "data_source": "public_fred",
        "series_id": "BAMLH0A0HYM2",
        "transform": "identity",
        "unit": "spread",
        "cadence": "daily",
        "source_type": "official_release",
        "metric_definition": "ICE BofA US High Yield Option-Adjusted Spread",
        "validation_policy": "official_release",
        "chart_mode": "threshold",
    },
    "SOFR": {
        "label": "SOFR",
        "symbol_family": "macro_series",
        "asset_class": "fixed_income",
        "data_source": "public_fred",
        "series_id": "SOFR",
        "transform": "identity",
        "unit": "percent",
        "cadence": "daily",
        "source_type": "official_release",
        "metric_definition": "Secured Overnight Financing Rate",
        "validation_policy": "official_release",
        "chart_mode": "release",
    },
    "DGS2": {
        "label": "UST 2Y",
        "symbol_family": "macro_series",
        "asset_class": "fixed_income",
        "data_source": "public_fred",
        "series_id": "DGS2",
        "transform": "identity",
        "unit": "percent",
        "cadence": "daily",
        "source_type": "official_release",
        "metric_definition": "US Treasury 2-Year constant maturity yield",
        "validation_policy": "official_release",
        "chart_mode": "threshold",
    },
    "DFII10": {
        "label": "Real Yield 10Y",
        "symbol_family": "macro_series",
        "asset_class": "fixed_income",
        "data_source": "public_fred",
        "series_id": "DFII10",
        "transform": "identity",
        "unit": "percent",
        "cadence": "daily",
        "source_type": "official_release",
        "metric_definition": "US Treasury 10-Year real yield",
        "validation_policy": "official_release",
        "chart_mode": "threshold",
    },
    "NASDAQNCPAG": {
        "label": "Bonds",
        "symbol_family": "macro_series",
        "asset_class": "fixed_income",
        "data_source": "public_fred",
        "series_id": "NASDAQNCPAG",
        "transform": "identity",
        "unit": "index",
        "cadence": "daily",
        "source_type": "official_release",
        "metric_definition": "Nasdaq US Aggregate Bond Index level",
        "validation_policy": "official_release",
        "chart_mode": "regime",
    },
    # ── Weekly official releases ──────────────────────────────────────────────
    "MORTGAGE30US": {
        "label": "30Y Mortgage",
        "symbol_family": "macro_series",
        "asset_class": "fixed_income",
        "data_source": "public_fred",
        "series_id": "MORTGAGE30US",
        "transform": "identity",
        "unit": "percent",
        "cadence": "weekly",
        "source_type": "official_release",
        "metric_definition": "US 30-Year fixed mortgage average rate",
        "validation_policy": "official_release",
        "chart_mode": "threshold",
    },
    # ── Monthly official releases ─────────────────────────────────────────────
    "CPI_YOY": {
        "label": "Inflation",
        "symbol_family": "macro_series",
        "asset_class": "fixed_income",
        "data_source": "public_fred",
        "series_id": "CPIAUCSL",
        "transform": "cpi_yoy",
        "unit": "percent",
        "cadence": "monthly",
        "source_type": "official_release",
        "metric_definition": "US CPI headline year-over-year, latest official release",
        "display_style": "release_level",
        "validation_policy": "official_release",
        "chart_mode": "release",
    },
    "FEDFUNDS": {
        "label": "Fed Funds",
        "symbol_family": "macro_series",
        "asset_class": "fixed_income",
        "data_source": "public_fred",
        "series_id": "FEDFUNDS",
        "transform": "identity",
        "unit": "percent",
        "cadence": "monthly",
        "source_type": "official_release",
        "metric_definition": "Effective Federal Funds Rate, latest official monthly average",
        "display_style": "release_level",
        "validation_policy": "official_release",
        "chart_mode": "release",
    },
}

_ADDITIONAL_DAILY_BRIEF_TARGETS: dict[str, list[str]] = {
    "fx": ["USD/SGD", "EUR/USD"],
    "fx_reference": ["USD/SGD", "EUR/USD"],
    "usd_strength_fallback": ["DXY"],
}

_LOWER_IS_BETTER_SYMBOLS = {
    "CPI_YOY",
    "^VIX",
    "BAMLH0A0HYM2",
    "DXY",
    "FEDFUNDS",
    "SOFR",
    "DGS2",
    "^TNX",
    "DFII10",
    "^TYX",
    "MORTGAGE30US",
}


def market_strip_symbols() -> tuple[str, ...]:
    return tuple(_MARKET_STRIP_SPECS.keys())


def market_strip_spec(symbol: str) -> dict[str, Any]:
    normalized = str(symbol or "").strip().upper()
    spec = dict(_MARKET_STRIP_SPECS.get(normalized) or {})
    if not spec:
        return {}
    source_type = str(spec.get("source_type") or "").strip()
    if "close_validation_model" not in spec and source_type == "market_close":
        spec["close_validation_model"] = "slot_eligible_daily_close"
    if "source_authority_tier" not in spec:
        if source_type == "market_close":
            spec["source_authority_tier"] = "public_verified_close"
        elif source_type == "official_release":
            spec["source_authority_tier"] = "official_release"
        else:
            spec["source_authority_tier"] = "declared_source"
    if "metric_polarity" not in spec:
        spec["metric_polarity"] = "lower_is_better" if normalized in _LOWER_IS_BETTER_SYMBOLS else "higher_is_better"
    return spec


def market_symbol_family(symbol: str) -> str:
    spec = market_strip_spec(symbol)
    return str(spec.get("symbol_family") or "generic")


def daily_brief_targets() -> dict[str, list[str]]:
    targets: dict[str, list[str]] = {family: list(values) for family, values in _ADDITIONAL_DAILY_BRIEF_TARGETS.items()}
    targets.setdefault("quote_latest", [])
    for symbol, spec in _MARKET_STRIP_SPECS.items():
        routed_family = str(spec.get("routed_family") or "").strip()
        routed_identifier = str(spec.get("routed_identifier") or symbol).strip().upper() if routed_family else ""
        quote_identifier_raw = spec.get("quote_identifier")
        quote_identifier = str(quote_identifier_raw).strip().upper() if quote_identifier_raw not in {None, ""} else ""
        if routed_family:
            targets.setdefault(routed_family, [])
            if routed_identifier and routed_identifier not in targets[routed_family]:
                targets[routed_family].append(routed_identifier)
        if quote_identifier and routed_family == "quote_latest" and quote_identifier not in targets["quote_latest"]:
            targets["quote_latest"].append(quote_identifier)
    return targets
