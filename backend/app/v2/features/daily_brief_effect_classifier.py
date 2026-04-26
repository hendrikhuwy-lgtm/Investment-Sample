from __future__ import annotations

from typing import Any


_SYMBOL_BUCKETS: dict[str, tuple[str, str, list[str]]] = {
    "CPI_YOY": ("inflation_effect", "inflation", ["sleeve_ig_bonds", "sleeve_real_assets"]),
    "FEDFUNDS": ("rates_duration_effect", "policy", ["sleeve_ig_bonds", "sleeve_cash_bills"]),
    "SOFR": ("rates_duration_effect", "liquidity", ["sleeve_ig_bonds", "sleeve_cash_bills"]),
    "DGS2": ("rates_duration_effect", "duration", ["sleeve_ig_bonds", "sleeve_cash_bills"]),
    "^TNX": ("rates_duration_effect", "duration", ["sleeve_ig_bonds", "sleeve_cash_bills"]),
    "DFII10": ("rates_duration_effect", "duration", ["sleeve_ig_bonds", "sleeve_global_equity_core"]),
    "^TYX": ("rates_duration_effect", "duration", ["sleeve_ig_bonds", "sleeve_cash_bills"]),
    "MORTGAGE30US": ("rates_duration_effect", "duration", ["sleeve_ig_bonds"]),
    "BAMLH0A0HYM2": ("credit_liquidity_effect", "credit", ["sleeve_ig_bonds", "sleeve_cash_bills"]),
    "^VIX": ("market_effect", "volatility", ["sleeve_global_equity_core", "sleeve_cash_bills"]),
    "DXY": ("fx_dollar_effect", "dollar_fx", ["sleeve_cash_bills", "sleeve_global_equity_core"]),
    "^KRX": ("market_effect", "growth", ["sleeve_global_equity_core"]),
    "^GSPC": ("market_effect", "growth", ["sleeve_global_equity_core"]),
    "^DJI": ("market_effect", "growth", ["sleeve_global_equity_core"]),
    "^IXIC": ("market_effect", "growth", ["sleeve_global_equity_core"]),
    "^RUT": ("market_effect", "growth", ["sleeve_global_equity_core"]),
    "^SPXEW": ("market_effect", "growth", ["sleeve_global_equity_core"]),
    "^990100-USD-STRD": ("market_effect", "growth", ["sleeve_global_equity_core", "sleeve_developed_ex_us_optional"]),
    "NASDAQNCPAG": ("rates_duration_effect", "duration", ["sleeve_ig_bonds"]),
    "GC=F": ("commodity_real_asset_effect", "real_assets", ["sleeve_real_assets"]),
    "BZ=F": ("commodity_real_asset_effect", "energy", ["sleeve_real_assets"]),
    "CL=F": ("commodity_real_asset_effect", "energy", ["sleeve_real_assets"]),
    "BTC-USD": ("market_effect", "liquidity", ["sleeve_cash_bills", "sleeve_global_equity_core"]),
}

_EFFECT_PRIORITIES: dict[str, int] = {
    "implementation": 18,
    "policy": 17,
    "inflation": 16,
    "duration": 15,
    "credit": 15,
    "dollar_fx": 14,
    "energy": 14,
    "real_assets": 13,
    "volatility": 13,
    "growth": 11,
    "liquidity": 11,
    "market": 9,
}


def classify_effect(signal_card: dict[str, Any]) -> dict[str, Any]:
    symbol = str(signal_card.get("symbol") or signal_card.get("label") or "").strip().upper()
    label = str(signal_card.get("label") or symbol)
    signal_kind = str(signal_card.get("signal_kind") or "market")
    runtime = dict(signal_card.get("runtime_provenance") or {})
    source_kind = _source_kind(signal_kind, runtime)

    effect_type, primary_effect_bucket, mapped_sleeves = _SYMBOL_BUCKETS.get(
        symbol,
        _fallback_classification(symbol=symbol, label=label, signal_kind=signal_kind),
    )

    affected_sleeves = _dedupe([*list(signal_card.get("affected_sleeves") or []), *mapped_sleeves])
    affected_holdings = list(signal_card.get("affected_holdings") or [])
    mapping_scope = "holding" if affected_holdings else "sleeve" if affected_sleeves else "market"
    affected_candidates = _dedupe(
        [
            *list(signal_card.get("affected_candidates") or []),
            *affected_holdings,
        ]
    )

    return {
        "source_kind": source_kind,
        "effect_type": effect_type,
        "primary_effect_bucket": primary_effect_bucket,
        "mapped_sleeves": affected_sleeves,
        "affected_candidates": affected_candidates,
        "mapping_scope": mapping_scope,
        "effect_priority": _EFFECT_PRIORITIES.get(primary_effect_bucket, 8),
    }


def _source_kind(signal_kind: str, runtime: dict[str, Any]) -> str:
    family = str(runtime.get("source_family") or "").strip()
    if signal_kind == "news":
        if "policy" in family:
            return "policy_context"
        return "news_context"
    if family in {"macro_market_state", "macro_series"}:
        return "official_release"
    if family in {"market_close", "quote_latest"}:
        return "market_close"
    if family:
        return family
    if signal_kind == "macro":
        return "official_release"
    return "market_context"


def _fallback_classification(*, symbol: str, label: str, signal_kind: str) -> tuple[str, str, list[str]]:
    text = f"{symbol} {label}".lower()
    if signal_kind == "news":
        if any(term in text for term in ("ecb", "fed", "policy", "tariff", "election", "government", "fiscal")):
            return ("policy_effect", "policy", ["sleeve_ig_bonds", "sleeve_cash_bills", "sleeve_global_equity_core"])
        if any(term in text for term in ("inflation", "cpi", "ppi")):
            return ("inflation_effect", "inflation", ["sleeve_ig_bonds", "sleeve_real_assets"])
        if any(term in text for term in ("iran", "israel", "hormuz", "shipping", "sanction", "war", "conflict", "missile", "ceasefire", "strike")):
            return ("global_news_effect", "market", ["sleeve_global_equity_core", "sleeve_cash_bills", "sleeve_real_assets"])
        return ("market_effect", "market", [])
    if any(term in text for term in ("oil", "crude", "energy")):
        return ("commodity_real_asset_effect", "energy", ["sleeve_real_assets"])
    if any(term in text for term in ("gold", "silver", "commodity")):
        return ("commodity_real_asset_effect", "real_assets", ["sleeve_real_assets"])
    if any(term in text for term in ("yield", "mortgage", "rates", "duration", "bond")):
        return ("rates_duration_effect", "duration", ["sleeve_ig_bonds"])
    if any(term in text for term in ("dollar", "fx", "currency")):
        return ("fx_dollar_effect", "dollar_fx", ["sleeve_cash_bills"])
    if any(term in text for term in ("credit", "spread", "liquidity")):
        return ("credit_liquidity_effect", "credit", ["sleeve_ig_bonds", "sleeve_cash_bills"])
    return ("market_effect", "market", [])


def _dedupe(values: list[str]) -> list[str]:
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in ordered:
            ordered.append(text)
    return ordered
