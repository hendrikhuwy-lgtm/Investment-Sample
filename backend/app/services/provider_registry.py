from __future__ import annotations

from typing import Any

from app.services.symbol_resolution import KNOWN_SYMBOL_ALIASES
from app.v2.core.market_strip_registry import market_symbol_family


BLUEPRINT_FAMILY_ALIASES: dict[str, str] = {
    "market_close": "daily_market_close",
    "daily_market_close": "daily_market_close",
    "quote_latest": "latest_quote",
    "latest_quote": "latest_quote",
    "benchmark_proxy": "benchmark_proxy_history",
    "benchmark_proxy_history": "benchmark_proxy_history",
    "reference_meta": "etf_reference_metadata",
    "etf_reference_metadata": "etf_reference_metadata",
    "ohlcv_history": "ohlcv_history",
}


def canonical_blueprint_family_id(family: str | None) -> str:
    normalized = str(family or "").strip()
    return BLUEPRINT_FAMILY_ALIASES.get(normalized, normalized)


PROVIDER_CAPABILITY_MATRIX: dict[str, dict[str, Any]] = {
    "alpha_vantage": {
        "label": "Alpha Vantage",
        "confidence_tier": "secondary",
        "commercial_status": "active",
        "rate_limit_posture": "tight",
        "best_roles": ["ohlcv_history", "benchmark_proxy_backup", "fx_backup", "etf_profile"],
        "families": {
            "quote_latest": {
                "priority": "backup",
                "budget_class": "medium",
                "cadence_seconds": 1800,
                "supported_fields": ["price", "previous_close", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf", "dollar_index"],
                "commercial_status": "active",
            },
            "fx": {
                "priority": "secondary",
                "budget_class": "medium",
                "cadence_seconds": 1800,
                "supported_fields": ["value"],
                "supported_symbol_classes": ["fx_pair"],
                "commercial_status": "active",
            },
            "benchmark_proxy": {
                "priority": "secondary",
                "budget_class": "medium",
                "cadence_seconds": 86400,
                "supported_fields": ["value", "series"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf"],
                "commercial_status": "active",
            },
            "ohlcv_history": {
                "priority": "secondary",
                "budget_class": "medium",
                "cadence_seconds": 86400,
                "supported_fields": ["series", "value"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf"],
                "commercial_status": "active",
            },
            "etf_profile": {
                "priority": "primary",
                "budget_class": "medium",
                "cadence_seconds": 86400,
                "supported_fields": ["aum", "expense_ratio", "portfolio_turnover", "holdings_count",
                                     "top_10_concentration", "asset_allocation", "sector_weightings"],
                "supported_symbol_classes": ["us_etf", "etf"],
                "commercial_status": "active",
            },
        },
    },
    "fmp": {
        "label": "FinancialModelingPrep",
        "confidence_tier": "secondary",
        "commercial_status": "mixed_plan",
        "rate_limit_posture": "moderate",
        "best_roles": ["reference_meta", "fundamentals"],
        "families": {
            "quote_latest": {
                "priority": "blocked",
                "budget_class": "medium",
                "cadence_seconds": 1800,
                "supported_fields": ["price", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf"],
                "commercial_status": "blocked_by_plan",
            },
            "reference_meta": {
                "priority": "primary",
                "budget_class": "medium",
                "cadence_seconds": 86400,
                "supported_fields": ["issuer", "primary_listing_exchange", "primary_trading_currency", "aum", "holdings_count"],
                "supported_symbol_classes": ["equity", "etf"],
                "commercial_status": "active",
            },
            "fundamentals": {
                "priority": "primary",
                "budget_class": "medium",
                "cadence_seconds": 86400,
                "supported_fields": ["aum", "holdings_count", "yield_proxy"],
                "supported_symbol_classes": ["equity", "etf"],
                "commercial_status": "active",
            },
        },
    },
    "finnhub": {
        "label": "Finnhub",
        "confidence_tier": "primary",
        "commercial_status": "active",
        "rate_limit_posture": "moderate",
        "best_roles": ["news_general", "quote_latest_selective", "reference_meta_backup_us"],
        "families": {
            "quote_latest": {
                "priority": "primary",
                "budget_class": "high",
                "cadence_seconds": 900,
                "supported_fields": ["price", "previous_close", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf", "dollar_index_selective"],
                "commercial_status": "active",
            },
            "news_general": {
                "priority": "primary",
                "budget_class": "high",
                "cadence_seconds": 900,
                "supported_fields": ["headline", "published_utc", "url"],
                "supported_symbol_classes": ["broad_market"],
                "commercial_status": "active",
            },
            "fx": {
                "priority": "backup",
                "budget_class": "high",
                "cadence_seconds": 1800,
                "supported_fields": ["value"],
                "supported_symbol_classes": ["fx_pair"],
                "commercial_status": "selective",
            },
            "reference_meta": {
                "priority": "secondary",
                "budget_class": "high",
                "cadence_seconds": 86400,
                "supported_fields": ["issuer", "primary_listing_exchange", "primary_trading_currency"],
                "supported_symbol_classes": ["equity", "etf"],
                "commercial_status": "active",
            },
        },
    },
    "frankfurter": {
        "label": "Frankfurter",
        "confidence_tier": "support",
        "commercial_status": "public_available",
        "rate_limit_posture": "public_reference_only",
        "best_roles": ["fx_reference", "usd_strength_support"],
        "families": {
            "fx_reference": {
                "priority": "primary",
                "budget_class": "low",
                "cadence_seconds": 1800,
                "supported_fields": ["value"],
                "supported_symbol_classes": ["fx_pair", "usd_strength_proxy"],
                "commercial_status": "public_available",
            },
            "usd_strength_fallback": {
                "priority": "primary",
                "budget_class": "low",
                "cadence_seconds": 1800,
                "supported_fields": ["value", "previous_close", "change_pct_1d", "proxy_components"],
                "supported_symbol_classes": ["usd_strength_proxy"],
                "commercial_status": "public_available",
            },
        },
    },
    "yahoo_finance": {
        "label": "Yahoo Finance",
        "confidence_tier": "support",
        "commercial_status": "public_available",
        "rate_limit_posture": "public_market_data",
        "best_roles": ["market_close_public", "quote_latest_public", "benchmark_proxy_public"],
        "families": {
            "market_close": {
                "priority": "primary",
                "budget_class": "low",
                "cadence_seconds": 43200,
                "supported_fields": ["price", "previous_close", "absolute_change", "change_pct_1d", "observed_at"],
                "supported_symbol_classes": ["futures", "crypto", "index", "dollar_index"],
                "commercial_status": "public_available",
            },
            "quote_latest": {
                "priority": "backup",
                "budget_class": "low",
                "cadence_seconds": 900,
                "supported_fields": ["price", "previous_close", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf", "futures", "crypto", "index", "dollar_index"],
                "commercial_status": "public_available",
            },
            "benchmark_proxy": {
                "priority": "backup",
                "budget_class": "low",
                "cadence_seconds": 86400,
                "supported_fields": ["value", "series", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf", "futures", "crypto", "index"],
                "commercial_status": "public_available",
            },
        },
    },
    "polygon": {
        "label": "Polygon",
        "confidence_tier": "primary",
        "commercial_status": "active",
        "rate_limit_posture": "rate_limited_under_load",
        "best_roles": ["quote_latest_selected", "benchmark_proxy_selected"],
        "families": {
            "quote_latest": {
                "priority": "primary",
                "budget_class": "high",
                "cadence_seconds": 3600,  # was 900; hourly sufficient for daily brief charts, 4× quota savings
                "supported_fields": ["price", "open", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf"],
                "commercial_status": "active",
            },
            "ohlcv_history": {
                "priority": "backup",
                "budget_class": "high",
                "cadence_seconds": 43200,
                "supported_fields": ["series", "value"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf"],
                "commercial_status": "active",
            },
            "benchmark_proxy": {
                "priority": "primary",
                "budget_class": "high",
                "cadence_seconds": 86400,
                "supported_fields": ["price", "open", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf"],
                "commercial_status": "active",
            },
        },
    },
    "tiingo": {
        "label": "Tiingo",
        "confidence_tier": "primary",
        "commercial_status": "active",
        "rate_limit_posture": "moderate",
        "best_roles": ["ohlcv_history"],
        "families": {
            "quote_latest": {
                "priority": "backup",
                "budget_class": "high",
                "cadence_seconds": 43200,
                "supported_fields": ["price", "open", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf"],
                "commercial_status": "active",
            },
            "ohlcv_history": {
                "priority": "primary",
                "budget_class": "high",
                "cadence_seconds": 43200,
                "supported_fields": ["series", "value"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf"],
                "commercial_status": "active",
            },
            "benchmark_proxy": {
                "priority": "secondary",
                "budget_class": "high",
                "cadence_seconds": 86400,
                "supported_fields": ["price", "open", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf"],
                "commercial_status": "active",
            },
        },
    },
    "eodhd": {
        "label": "EOD Historical Data",
        "confidence_tier": "secondary",
        "commercial_status": "mixed_plan",
        "rate_limit_posture": "plan_blocked_on_selected_paths",
        "best_roles": ["reference_meta_backup", "ohlcv_history_backup"],
        "families": {
            "quote_latest": {
                "priority": "blocked",
                "budget_class": "medium",
                "cadence_seconds": 1800,
                "supported_fields": ["price", "previous_close", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf"],
                "commercial_status": "blocked_by_plan",
            },
            "benchmark_proxy": {
                "priority": "blocked",
                "budget_class": "medium",
                "cadence_seconds": 86400,
                "supported_fields": ["price", "previous_close", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf"],
                "commercial_status": "blocked_by_plan",
            },
            "ohlcv_history": {
                "priority": "backup",
                "budget_class": "medium",
                "cadence_seconds": 43200,
                "supported_fields": ["series", "value"],
                "supported_symbol_classes": ["us_equity", "us_etf"],  # .US endpoint only; non-US symbols not supported
                "commercial_status": "active",
            },
            "reference_meta": {
                "priority": "backup",
                "budget_class": "medium",
                "cadence_seconds": 86400,
                "supported_fields": ["issuer", "primary_listing_exchange", "primary_trading_currency", "aum", "holdings_count"],
                "supported_symbol_classes": ["equity", "etf"],
                "commercial_status": "active",
            },
            "fx": {
                "priority": "blocked",
                "budget_class": "medium",
                "cadence_seconds": 1800,
                "supported_fields": ["value"],
                "supported_symbol_classes": ["fx_pair"],
                "commercial_status": "blocked_by_plan",
            },
        },
    },
    "nasdaq_data_link": {
        "label": "Nasdaq Data Link",
        "confidence_tier": "strategic",
        "families": {
            "research_dataset": {"priority": "primary", "budget_class": "low", "cadence_seconds": 604800},
        },
    },
    "twelve_data": {
        "label": "Twelve Data",
        "confidence_tier": "secondary",
        "commercial_status": "active",
        "rate_limit_posture": "moderate",
        "best_roles": ["fx", "dollar_index_quotes", "quote_backup", "venue_reference_backup"],
        "families": {
            "quote_latest": {
                "priority": "secondary",
                "budget_class": "high",
                "cadence_seconds": 1800,
                "supported_fields": ["price", "previous_close", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf", "dollar_index", "index", "futures", "crypto"],
                "commercial_status": "active",
            },
            "benchmark_proxy": {
                "priority": "backup",
                "budget_class": "high",
                "cadence_seconds": 86400,
                "supported_fields": ["price", "previous_close", "absolute_change", "change_pct_1d"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf"],
                "commercial_status": "active",
            },
            "ohlcv_history": {
                "priority": "backup",
                "budget_class": "high",
                "cadence_seconds": 43200,
                "supported_fields": ["series", "value"],
                "supported_symbol_classes": ["equity", "etf", "commodity_etf"],
                "commercial_status": "active",
            },
            "fx": {
                "priority": "primary",
                "budget_class": "high",
                "cadence_seconds": 1800,
                "supported_fields": ["value"],
                "supported_symbol_classes": ["fx_pair", "dollar_index_proxy"],
                "commercial_status": "active",
            },
            "reference_meta": {
                "priority": "secondary",
                "budget_class": "medium",
                "cadence_seconds": 86400,
                "supported_fields": ["primary_listing_exchange", "primary_trading_currency"],
                "supported_symbol_classes": ["us_etf", "etf"],
                "commercial_status": "active",
            },
        },
    },
    "sec_edgar": {
        "label": "SEC EDGAR",
        "confidence_tier": "primary",
        "commercial_status": "public_available",
        "rate_limit_posture": "public_reference_only",
        "best_roles": ["etf_holdings", "filings_context"],
        "families": {
            "etf_holdings": {
                "priority": "primary",
                "budget_class": "low",
                "cadence_seconds": 604800,
                "supported_fields": ["holdings_count", "top_10_concentration", "aum",
                                     "asset_allocation", "factsheet_asof", "expense_ratio"],
                "supported_symbol_classes": ["us_etf"],
                "commercial_status": "public_available",
            },
            "filings_context": {
                "priority": "primary",
                "budget_class": "low",
                "cadence_seconds": 86400,
                "supported_fields": ["filing_date", "form_type"],
                "supported_symbol_classes": ["us_etf", "us_equity"],
                "commercial_status": "public_available",
            },
        },
    },
}

DATA_FAMILY_OWNERSHIP: dict[str, dict[str, Any]] = {
    "macro_regime_time_series": {
        "primary_provider": "fred",
        "secondary_provider": None,
        "public_fallback": ["ecb_data_api", "world_bank_indicators", "cftc_cot"],
        "legacy_fallback": [],
        "refresh_cadence_seconds": 86400,
        "quota_sensitivity": "low",
        "investor_importance": "critical",
    },
    "fx": {
        "primary_provider": "twelve_data",
        "secondary_provider": "alpha_vantage",
        "public_fallback": ["ecb_data_api"],
        "legacy_fallback": ["yahoo_finance"],
        "refresh_cadence_seconds": 1800,
        "quota_sensitivity": "high",
        "investor_importance": "critical",
    },
    "fx_reference": {
        "primary_provider": "frankfurter",
        "secondary_provider": "ecb_data_api",
        "public_fallback": ["ecb_data_api"],
        "legacy_fallback": ["twelve_data"],
        "refresh_cadence_seconds": 1800,
        "quota_sensitivity": "low",
        "investor_importance": "medium",
    },
    "usd_strength_fallback": {
        "primary_provider": "frankfurter",
        "secondary_provider": "ecb_data_api",
        "public_fallback": ["ecb_data_api"],
        "legacy_fallback": ["twelve_data"],
        "refresh_cadence_seconds": 1800,
        "quota_sensitivity": "low",
        "investor_importance": "critical",
    },
    "benchmark_proxy_history": {
        "primary_provider": "tiingo",
        "secondary_provider": "alpha_vantage",
        "public_fallback": ["yahoo_finance"],
        "legacy_fallback": ["polygon", "twelve_data", "eodhd"],
        "refresh_cadence_seconds": 86400,
        "quota_sensitivity": "medium",
        "investor_importance": "high",
    },
    "latest_quote": {
        "primary_provider": "polygon",
        "secondary_provider": "finnhub",
        "public_fallback": [],
        "legacy_fallback": ["twelve_data", "alpha_vantage", "yahoo_finance"],
        "refresh_cadence_seconds": 900,
        "quota_sensitivity": "high",
        "investor_importance": "critical",
    },
    "ohlcv_history": {
        "primary_provider": "twelve_data",
        "secondary_provider": "polygon",
        "public_fallback": [],
        "legacy_fallback": ["tiingo", "alpha_vantage", "eodhd"],
        "refresh_cadence_seconds": 43200,
        "quota_sensitivity": "medium",
        "investor_importance": "high",
    },
    "etf_reference_metadata": {
        "primary_provider": "fmp",
        "secondary_provider": "twelve_data",
        "public_fallback": [],
        "legacy_fallback": ["finnhub", "eodhd", "yahoo_finance"],
        "refresh_cadence_seconds": 86400,
        "quota_sensitivity": "medium",
        "investor_importance": "high",
    },
    "fundamentals": {
        "primary_provider": "fmp",
        "secondary_provider": "finnhub",
        "public_fallback": ["sec_edgar"],
        "legacy_fallback": ["eodhd"],
        "refresh_cadence_seconds": 86400,
        "quota_sensitivity": "medium",
        "investor_importance": "medium",
    },
    "structural_macro_context": {
        "primary_provider": "world_bank_indicators",
        "secondary_provider": "ecb_data_api",
        "public_fallback": ["cftc_cot"],
        "legacy_fallback": [],
        "refresh_cadence_seconds": 604800,
        "quota_sensitivity": "low",
        "investor_importance": "medium",
    },
    "positioning": {
        "primary_provider": "cftc_cot",
        "secondary_provider": None,
        "public_fallback": [],
        "legacy_fallback": [],
        "refresh_cadence_seconds": 604800,
        "quota_sensitivity": "low",
        "investor_importance": "medium",
    },
    "filings_context": {
        "primary_provider": "sec_edgar",
        "secondary_provider": None,
        "public_fallback": [],
        "legacy_fallback": [],
        "refresh_cadence_seconds": 86400,
        "quota_sensitivity": "low",
        "investor_importance": "medium",
    },
    "etf_profile": {
        "primary_provider": "alpha_vantage",
        "secondary_provider": "fmp",
        "public_fallback": ["sec_edgar"],
        "legacy_fallback": [],
        "refresh_cadence_seconds": 86400,
        "quota_sensitivity": "medium",
        "investor_importance": "high",
    },
    "etf_holdings": {
        "primary_provider": "sec_edgar",
        "secondary_provider": None,
        "public_fallback": [],
        "legacy_fallback": [],
        "refresh_cadence_seconds": 604800,
        "quota_sensitivity": "low",
        "investor_importance": "high",
    },
}


SURFACE_TARGET_FAMILIES: dict[str, list[str]] = {
    "daily_brief": ["market_close", "benchmark_proxy", "fx", "fx_reference", "usd_strength_fallback", "quote_latest"],
    "dashboard": ["quote_latest", "fx", "benchmark_proxy"],
    "blueprint": ["quote_latest", "reference_meta", "ohlcv_history", "benchmark_proxy", "fundamentals", "etf_profile", "etf_holdings"],
}


DATA_FAMILY_ROUTING: dict[str, list[str]] = {
    "market_close": ["yahoo_finance"],
    "quote_latest": ["polygon", "finnhub", "twelve_data", "alpha_vantage", "tiingo", "eodhd", "yahoo_finance"],
    "ohlcv_history": ["twelve_data", "polygon", "tiingo", "alpha_vantage", "eodhd"],
    "reference_meta": ["fmp", "finnhub", "twelve_data", "eodhd"],
    "fundamentals": ["fmp", "finnhub", "eodhd"],
    "benchmark_proxy": ["polygon", "alpha_vantage", "tiingo", "twelve_data", "eodhd", "yahoo_finance"],
    "fx": ["twelve_data", "alpha_vantage", "finnhub", "eodhd"],
    "fx_reference": ["frankfurter"],
    "usd_strength_fallback": ["frankfurter"],
    "research_dataset": ["nasdaq_data_link"],
    "etf_profile": ["alpha_vantage", "fmp"],
    "etf_holdings": ["sec_edgar"],
}

_FAMILY_DEFAULT_PROVIDER_ORDER: dict[str, list[str]] = {
    "market_close": ["yahoo_finance"],
    "quote_latest": ["polygon", "finnhub", "twelve_data", "alpha_vantage", "tiingo", "eodhd", "yahoo_finance"],
    "benchmark_proxy": ["polygon", "alpha_vantage", "tiingo", "twelve_data", "eodhd", "yahoo_finance"],
    "ohlcv_history": ["twelve_data", "polygon", "tiingo", "alpha_vantage", "eodhd"],
    "reference_meta": ["fmp", "finnhub", "twelve_data", "eodhd"],
    "fundamentals": ["fmp", "finnhub", "eodhd"],
    "fx": ["twelve_data", "alpha_vantage", "finnhub", "eodhd"],
    "fx_reference": ["frankfurter"],
    "usd_strength_fallback": ["frankfurter"],
    "research_dataset": ["nasdaq_data_link"],
    "etf_profile": ["alpha_vantage", "fmp"],
    "etf_holdings": ["sec_edgar"],
}

_PROVIDER_FAMILY_BLOCKS: dict[tuple[str, str], str] = {
    ("eodhd", "quote_latest"): "provider_blocked_by_plan",
    ("eodhd", "benchmark_proxy"): "provider_blocked_by_plan",
    ("eodhd", "reference_meta"): "provider_blocked_by_plan",
    ("eodhd", "fx"): "provider_blocked_by_plan",
    ("fmp", "quote_latest"): "provider_blocked_by_plan",
}

# DXY routing note:
# Direct DXY quote first tries selective quote providers. Yahoo can supply a public
# DX-Y.NYB quote path; if that remains unavailable, the effective fallback stays
# usd_strength_fallback → frankfurter (100-based proxy from EUR/USD, SGD/USD, JPY/USD).
_SYMBOL_FAMILY_PRIORITY: dict[tuple[str, str], list[str]] = {
    ("index", "quote_latest"): ["yahoo_finance", "polygon", "finnhub", "twelve_data", "alpha_vantage", "tiingo", "eodhd"],
    ("futures", "quote_latest"): ["yahoo_finance", "polygon", "finnhub", "twelve_data", "alpha_vantage", "tiingo", "eodhd"],
    ("crypto", "quote_latest"): ["yahoo_finance", "polygon", "finnhub", "twelve_data", "alpha_vantage", "tiingo", "eodhd"],
    ("dollar_index", "quote_latest"): ["twelve_data", "yahoo_finance", "alpha_vantage", "finnhub"],
    ("index", "benchmark_proxy"): ["yahoo_finance", "twelve_data", "polygon", "tiingo", "alpha_vantage", "eodhd"],
    ("futures", "benchmark_proxy"): ["yahoo_finance", "twelve_data", "polygon", "alpha_vantage", "tiingo", "eodhd"],
    ("crypto", "benchmark_proxy"): ["yahoo_finance", "twelve_data", "polygon", "alpha_vantage", "tiingo", "eodhd"],
    ("dollar_index", "benchmark_proxy"): ["yahoo_finance", "twelve_data", "alpha_vantage", "finnhub"],
    ("dollar_index", "fx"): ["twelve_data", "alpha_vantage"],
}

_NON_US_FAMILY_PRIORITY: dict[str, list[str]] = {
    "quote_latest": ["twelve_data", "yahoo_finance", "alpha_vantage", "polygon", "finnhub", "tiingo", "eodhd"],
    "ohlcv_history": ["twelve_data", "polygon", "tiingo", "alpha_vantage", "eodhd"],
    "reference_meta": ["twelve_data", "fmp", "finnhub", "eodhd"],
}

_SYMBOL_SPECIFIC_UNSUPPORTED: dict[tuple[str, str, str], str] = {
    ("polygon", "quote_latest", "DXY"): "provider_symbol_family_unsupported",
    ("polygon", "benchmark_proxy", "DXY"): "provider_symbol_family_unsupported",
    ("tiingo", "quote_latest", "DXY"): "provider_symbol_family_unsupported",
    ("tiingo", "benchmark_proxy", "DXY"): "provider_symbol_family_unsupported",
}


def capability_matrix() -> dict[str, Any]:
    return {
        "capability_contract_version": 2,
        "providers": PROVIDER_CAPABILITY_MATRIX,
        "data_family_routing": DATA_FAMILY_ROUTING,
        "surface_target_families": SURFACE_TARGET_FAMILIES,
        "data_family_ownership": DATA_FAMILY_OWNERSHIP,
    }


def providers_for_family(family: str) -> list[str]:
    return list(DATA_FAMILY_ROUTING.get(str(family), []))


def provider_family_config(provider: str, family: str) -> dict[str, Any]:
    return dict((((PROVIDER_CAPABILITY_MATRIX.get(str(provider)) or {}).get("families") or {}).get(str(family)) or {}))


_EXCHANGE_SUFFIX_REGION: dict[str, str] = {
    "US": "us",
    "LSE": "non_us",
    "SW": "non_us",
    "PA": "non_us",
    "AS": "non_us",
    "SG": "non_us",
}

_FIAT_QUOTES = {"USD", "EUR", "GBP", "JPY", "SGD", "AUD", "CAD", "CHF", "HKD", "CNH", "CNY"}

_FAMILY_FRESHNESS_MODEL: dict[str, str] = {
    "market_close": "daily_close",
    "quote_latest": "intraday_quote",
    "benchmark_proxy": "daily_history",
    "ohlcv_history": "daily_history",
    "fx": "intraday_quote",
    "fx_reference": "daily_reference",
    "usd_strength_fallback": "daily_reference",
    "reference_meta": "reference_snapshot",
    "fundamentals": "reference_snapshot",
    "etf_profile": "reference_snapshot",
    "etf_holdings": "periodic_filing",
    "filings_context": "periodic_filing",
    "research_dataset": "research_archive",
}


def _infer_symbol_profile(identifier: str | None) -> dict[str, Any]:
    symbol = str(identifier or "").strip().upper()
    alias_candidates = [str(item).strip().upper() for item in list(KNOWN_SYMBOL_ALIASES.get(symbol) or []) if str(item).strip()]
    venue_code = symbol.rsplit(".", 1)[1] if "." in symbol else next((alias.rsplit(".", 1)[1] for alias in alias_candidates if "." in alias), None)
    region = _EXCHANGE_SUFFIX_REGION.get(str(venue_code or "").upper(), "us")
    symbol_family = market_symbol_family(symbol)
    if symbol_family == "generic":
        if "/" in symbol:
            symbol_family = "fx_pair"
        elif symbol.endswith("=F"):
            symbol_family = "futures"
        elif symbol.startswith("^"):
            symbol_family = "index"
        elif symbol == "DXY":
            symbol_family = "dollar_index"
        elif "-" in symbol:
            base, quote = symbol.rsplit("-", 1)
            if quote in _FIAT_QUOTES and len(base) >= 3:
                symbol_family = "crypto"
    symbol_classes: set[str] = set()
    identifier_kind = "symbol"
    if symbol_family == "fx_pair":
        symbol_classes.add("fx_pair")
        identifier_kind = "currency_pair"
    elif symbol_family == "dollar_index":
        symbol_classes.update({"dollar_index", "dollar_index_proxy", "index"})
        identifier_kind = "macro_index"
    elif symbol_family == "index":
        symbol_classes.add("index")
        identifier_kind = "index_symbol"
    elif symbol_family == "futures":
        symbol_classes.add("futures")
        identifier_kind = "futures_symbol"
    elif symbol_family == "crypto":
        symbol_classes.add("crypto")
        identifier_kind = "crypto_pair"
    else:
        symbol_classes.update({"equity", "etf"})
        if region == "us":
            symbol_classes.update({"us_equity", "us_etf"})
        else:
            symbol_classes.update({"international_security", "non_us_security"})
        identifier_kind = "exchange_qualified_symbol" if venue_code else "symbol"
    is_proxy = symbol_family in {"dollar_index"} or symbol.startswith("^990")
    is_synthetic = symbol_family in {"dollar_index"} and symbol == "DXY"
    return {
        "symbol": symbol,
        "symbol_family": symbol_family,
        "symbol_classes": sorted(symbol_classes),
        "identifier_kind": identifier_kind,
        "region": region,
        "venue_code": str(venue_code or "").upper() or None,
        "is_proxy": is_proxy,
        "is_synthetic": is_synthetic,
    }


def provider_family_contract(provider: str, family: str) -> dict[str, Any]:
    contract = provider_family_config(provider, family)
    if not contract:
        return {}
    enriched = dict(contract)
    supported_classes = set(str(item) for item in list(enriched.get("supported_symbol_classes") or []) if str(item))
    if "supported_regions" not in enriched:
        enriched["supported_regions"] = ["us"] if supported_classes and supported_classes <= {"us_equity", "us_etf"} else ["us", "non_us", "global"]
    if "supported_identifier_kinds" not in enriched:
        enriched["supported_identifier_kinds"] = ["symbol", "exchange_qualified_symbol", "currency_pair", "index_symbol", "futures_symbol", "macro_index", "crypto_pair"]
    enriched.setdefault("provider_role", str(enriched.get("priority") or "backup"))
    enriched.setdefault("freshness_model", _FAMILY_FRESHNESS_MODEL.get(str(family or ""), "reference_snapshot"))
    enriched.setdefault("entitlement_scope", str(enriched.get("commercial_status") or "active"))
    enriched.setdefault("supports_batch", str(family or "") in {"quote_latest", "benchmark_proxy", "reference_meta", "fx"})
    enriched.setdefault(
        "max_batch_size",
        120 if str(provider or "") == "twelve_data" else 100 if str(provider or "") == "alpha_vantage" else 1,
    )
    enriched["symbol_profile_contract"] = {
        "supported_symbol_classes": sorted(supported_classes),
        "supported_identifier_kinds": list(enriched.get("supported_identifier_kinds") or []),
        "supported_regions": list(enriched.get("supported_regions") or []),
    }
    return enriched


def provider_supports_family(provider: str, family: str) -> bool:
    return bool(provider_family_config(provider, family))


def provider_support_status(provider: str, family: str, identifier: str | None = None) -> tuple[bool, str | None]:
    provider_name = str(provider or "")
    family_name = str(family or "")
    if not provider_supports_family(provider_name, family_name):
        return False, "provider_does_not_support_family"
    contract = provider_family_contract(provider_name, family_name)
    blocked_reason = _PROVIDER_FAMILY_BLOCKS.get((provider_name, family_name))
    if blocked_reason:
        return False, blocked_reason
    symbol = str(identifier or "").strip().upper()
    if not symbol:
        return True, None
    profile = _infer_symbol_profile(symbol)
    supported_identifier_kinds = {str(item) for item in list(contract.get("supported_identifier_kinds") or []) if str(item)}
    if supported_identifier_kinds and str(profile.get("identifier_kind") or "") not in supported_identifier_kinds:
        return False, "provider_identifier_kind_unsupported"
    supported_regions = {str(item) for item in list(contract.get("supported_regions") or []) if str(item)}
    profile_region = str(profile.get("region") or "")
    if supported_regions and "global" not in supported_regions and profile_region and profile_region not in supported_regions:
        return False, "provider_region_unsupported"
    supported_classes = {str(item) for item in list(contract.get("supported_symbol_classes") or []) if str(item)}
    profile_classes = {str(item) for item in list(profile.get("symbol_classes") or []) if str(item)}
    if supported_classes and not profile_classes.intersection(supported_classes):
        return False, "provider_symbol_family_unsupported"
    unsupported_reason = _SYMBOL_SPECIFIC_UNSUPPORTED.get((provider_name, family_name, symbol))
    if unsupported_reason:
        return False, unsupported_reason
    return True, None


def routed_provider_candidates(family: str, *, identifier: str | None = None) -> list[str]:
    family_name = str(family or "")
    symbol = str(identifier or "").strip().upper()
    symbol_family = market_symbol_family(symbol)
    preferred = list(_SYMBOL_FAMILY_PRIORITY.get((symbol_family, family_name)) or _FAMILY_DEFAULT_PROVIDER_ORDER.get(family_name) or DATA_FAMILY_ROUTING.get(family_name, []))
    profile = _infer_symbol_profile(symbol)
    if str(profile.get("region") or "") == "non_us" and family_name in _NON_US_FAMILY_PRIORITY:
        preferred = list(_NON_US_FAMILY_PRIORITY.get(family_name) or preferred)
    if not preferred:
        preferred = list(DATA_FAMILY_ROUTING.get(family_name, []))

    ordered: list[str] = []
    seen: set[str] = set()
    for provider_name in [*preferred, *DATA_FAMILY_ROUTING.get(family_name, [])]:
        normalized = str(provider_name or "")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        supported, _reason = provider_support_status(normalized, family_name, symbol)
        if supported:
            ordered.append(normalized)
    return ordered


def surface_families(surface: str) -> list[str]:
    return list(SURFACE_TARGET_FAMILIES.get(str(surface), []))


def family_ownership_map() -> dict[str, Any]:
    return dict(DATA_FAMILY_OWNERSHIP)
