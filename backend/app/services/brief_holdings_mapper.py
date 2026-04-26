from __future__ import annotations

from typing import Any


def _symbol_from_row(row: dict[str, Any]) -> str:
    return str(
        row.get("normalized_symbol")
        or row.get("raw_symbol")
        or row.get("security_name")
        or row.get("ticker")
        or ""
    ).strip()


def map_signal_to_holdings(
    signal: dict[str, Any],
    exposure_snapshot: dict[str, Any] | None,
    holdings_snapshot: list[dict[str, Any]] | None,
    instrument_metadata: dict[str, Any] | None = None,
    exposure_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _ = instrument_metadata
    implication = dict(signal.get("portfolio_implication") or {})
    matched_holdings = list(implication.get("matched_holdings") or [])
    target_sleeves = [
        *[str(item) for item in list(implication.get("primary_affected_sleeves") or []) if str(item)],
        *[str(item) for item in list(implication.get("secondary_affected_sleeves") or []) if str(item)],
    ]
    holdings_snapshot = holdings_snapshot or []
    exposure_snapshot = exposure_snapshot or {}
    exposure_context = exposure_context or {}
    total_value = float(dict((exposure_snapshot or {}).get("summary") or {}).get("total_value") or 0.0)
    signal_family = str(signal.get("signal_family") or "").lower()
    theme_weights = dict(exposure_context.get("theme_weights") or {})

    if matched_holdings:
        symbols = [
            _symbol_from_row(item)
            for item in matched_holdings
            if _symbol_from_row(item)
        ]
        affected_value = sum(float(item.get("market_value") or 0.0) for item in matched_holdings)
        affected_weight_pct = round((affected_value / total_value) * 100.0, 2) if total_value > 0 else None
        return {
            "affected_holdings": symbols[:5],
            "affected_sleeves": target_sleeves[:4],
            "impact_basis": "live_holding_direct",
            "confidence_level": "high",
            "proxy_used": False,
            "unsupported_reason": None,
            "affected_weight_pct": affected_weight_pct,
        }

    sleeve_weight_map = dict(exposure_context.get("sleeve_weight_map") or {})
    if not sleeve_weight_map:
        sleeve_rows = list((exposure_snapshot or {}).get("sleeve_concentration") or [])
        sleeve_weight_map = {
            str(item.get("sleeve") or ""): float(item.get("weight") or 0.0) for item in sleeve_rows
        }
    sleeve_weight = sum(sleeve_weight_map.get(sleeve, 0.0) for sleeve in target_sleeves)
    top_positions = list(exposure_context.get("top_positions") or (exposure_snapshot or {}).get("top_positions") or [])
    sleeve_positions_map = dict(exposure_context.get("sleeve_positions") or {})
    sleeve_positions = [
        position
        for sleeve in target_sleeves
        for position in list(sleeve_positions_map.get(sleeve) or [])
    ] or [
        position
        for position in top_positions
        if str(position.get("sleeve") or "") in target_sleeves
    ]
    if signal_family == "fx and singapore context":
        non_local_positions = [
            row
            for row in top_positions
            if str(row.get("currency") or "").upper() not in {"", "SGD"}
        ]
        if holdings_snapshot and non_local_positions:
            affected_value = sum(float(item.get("market_value") or 0.0) for item in non_local_positions[:5])
            affected_weight_pct = round((affected_value / total_value) * 100.0, 2) if total_value > 0 else None
            return {
                "affected_holdings": [_symbol_from_row(item) for item in non_local_positions[:5] if _symbol_from_row(item)],
                "affected_sleeves": target_sleeves[:4],
                "impact_basis": "live_holding_indirect",
                "confidence_level": "medium",
                "proxy_used": True,
                "unsupported_reason": "Foreign-currency positions are identifiable, but asset-level FX sensitivity is inferred from position currency rather than direct decomposition.",
                "affected_weight_pct": affected_weight_pct,
            }
        theme_weight = float(theme_weights.get("non_base_fx", theme_weights.get("non_sgd_fx", 0.0)) or 0.0)
        if theme_weight > 0:
            return {
                "affected_holdings": [],
                "affected_sleeves": target_sleeves[:4],
                "impact_basis": "live_holding_indirect" if holdings_snapshot else "live_sleeve_exposure",
                "confidence_level": "medium",
                "proxy_used": True,
                "unsupported_reason": (
                    "Foreign-currency translation is relevant from aggregated exposure, but holding-level FX decomposition is incomplete."
                    if holdings_snapshot
                    else "Live sleeve exposure shows non-base-currency risk, but holding-level FX decomposition is unavailable."
                ),
                "affected_weight_pct": round(theme_weight * 100.0, 2),
            }
    if signal_family == "em and china context" and not sleeve_positions:
        for sleeve in ("emerging_markets", "china_satellite"):
            sleeve_positions.extend(list(sleeve_positions_map.get(sleeve) or []))
        sleeve_weight = sum(
            sleeve_weight_map.get(sleeve, 0.0)
            for sleeve in set(target_sleeves + ["emerging_markets", "china_satellite"])
        )
        if sleeve_weight <= 0 and float(theme_weights.get("em_plus_china", 0.0) or 0.0) > 0:
            sleeve_weight = float(theme_weights.get("em_plus_china", 0.0) or 0.0)
    if signal_family == "rates and inflation" and not sleeve_positions:
        for sleeve in ("IG_bonds", "cash_bills", "developed_ex_us_optional"):
            sleeve_positions.extend(list(sleeve_positions_map.get(sleeve) or []))
        sleeve_weight = sum(
            sleeve_weight_map.get(sleeve, 0.0)
            for sleeve in set(target_sleeves + ["IG_bonds", "cash_bills", "developed_ex_us_optional"])
        )
        if sleeve_weight <= 0 and float(theme_weights.get("duration_plus_reserve", 0.0) or 0.0) > 0:
            sleeve_weight = float(theme_weights.get("duration_plus_reserve", 0.0) or 0.0)
    if signal_family == "credit and liquidity" and not sleeve_positions:
        for sleeve in ("IG_bonds", "alternatives", "cash_bills"):
            sleeve_positions.extend(list(sleeve_positions_map.get(sleeve) or []))
        sleeve_weight = sum(
            sleeve_weight_map.get(sleeve, 0.0)
            for sleeve in set(target_sleeves + ["IG_bonds", "alternatives", "cash_bills"])
        )
        if sleeve_weight <= 0 and float(theme_weights.get("credit", 0.0) or 0.0) > 0:
            sleeve_weight = float(theme_weights.get("credit", 0.0) or 0.0)
    if holdings_snapshot and sleeve_weight > 0:
        affected_symbols = [_symbol_from_row(item) for item in sleeve_positions[:5] if _symbol_from_row(item)]
        return {
            "affected_holdings": affected_symbols,
            "affected_sleeves": target_sleeves[:4],
            "impact_basis": "live_sleeve_exposure",
            "confidence_level": "medium",
            "proxy_used": True,
            "unsupported_reason": (
                "Direct holding mapping is incomplete, but sleeve exposure and top positions identify the likely implementation layer under pressure."
                if affected_symbols
                else "Direct holding mapping is incomplete, but sleeve exposure is available."
            ),
            "affected_weight_pct": round(sleeve_weight * 100.0, 2),
        }

    if not holdings_snapshot and sleeve_weight > 0:
        return {
            "affected_holdings": [],
            "affected_sleeves": target_sleeves[:4],
            "impact_basis": "live_sleeve_exposure",
            "confidence_level": "medium",
            "proxy_used": True,
            "unsupported_reason": "Live sleeve exposure is available, but holding-level mapping is unavailable today.",
            "affected_weight_pct": round(sleeve_weight * 100.0, 2),
        }

    if holdings_snapshot:
        return {
            "affected_holdings": [],
            "affected_sleeves": target_sleeves[:4],
            "impact_basis": "live_holding_indirect",
            "confidence_level": "low",
            "proxy_used": True,
            "unsupported_reason": "Holdings snapshot exists, but no direct or sleeve-level mapping supports a stronger consequence.",
            "affected_weight_pct": None,
        }

    if target_sleeves:
        return {
            "affected_holdings": [],
            "affected_sleeves": target_sleeves[:4],
            "impact_basis": "target_sleeve_proxy",
            "confidence_level": "low",
            "proxy_used": True,
            "unsupported_reason": "No active holdings snapshot is available; using target-sleeve design as a proxy.",
            "affected_weight_pct": None,
        }

    if str(implication.get("affected_benchmark_context") or "").strip():
        return {
            "affected_holdings": [],
            "affected_sleeves": [],
            "impact_basis": "benchmark_watch_proxy",
            "confidence_level": "low",
            "proxy_used": True,
            "unsupported_reason": "Benchmark framing is relevant, but no holdings or sleeve exposure is available for a direct portfolio read.",
            "affected_weight_pct": None,
        }

    return {
        "affected_holdings": [],
        "affected_sleeves": [],
        "impact_basis": "macro_only",
        "confidence_level": "low",
        "proxy_used": True,
        "unsupported_reason": "No direct holdings or target-sleeve consequence can be confirmed from current portfolio data.",
        "affected_weight_pct": None,
    }
