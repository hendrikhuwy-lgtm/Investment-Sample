from __future__ import annotations

from dataclasses import is_dataclass, replace
from datetime import datetime
from typing import Any

from app.v2.forecasting.service import build_forecast_bundle, build_request
from app.v2.surfaces.market_truth_support import load_surface_market_truth


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_threshold(trigger: dict[str, Any] | None) -> str | None:
    if not trigger:
        return None
    threshold = str(trigger.get("threshold") or "").strip()
    trigger_type = str(trigger.get("trigger_type") or "").replace("_", " ").strip()
    if threshold and trigger_type:
        return f"{trigger_type.title()} around {threshold}"
    if threshold:
        return f"Threshold around {threshold}"
    return None


def _scenario_effect(summary: str, portfolio_consequence: str, path: str | None) -> str:
    parts = [summary.strip(), portfolio_consequence.strip(), str(path or "").strip()]
    filtered = [part for part in parts if part]
    return " ".join(dict.fromkeys(filtered))


def _clamp(value: float, *, lower: float = 0.05, upper: float = 0.95) -> float:
    return max(lower, min(upper, value))


def _normalized_tokens(text: str) -> set[str]:
    cleaned = "".join(char.lower() if char.isalnum() else " " for char in str(text or ""))
    stop = {"the", "and", "for", "with", "that", "this", "from", "into", "stay", "stays", "current"}
    return {token for token in cleaned.split() if len(token) > 2 and token not in stop}


def _similarity(left: str | None, right: str | None) -> float:
    left_tokens = _normalized_tokens(left or "")
    right_tokens = _normalized_tokens(right or "")
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    return overlap / max(len(left_tokens), len(right_tokens))


def _compact_text(text: str | None, *, max_words: int = 18, max_chars: int = 140) -> str | None:
    value = " ".join(str(text or "").strip().split())
    if not value:
        return None
    words = value.split()
    if len(words) > max_words:
        value = " ".join(words[:max_words]).rstrip(" ,.;:") + "."
    if len(value) > max_chars:
        value = value[: max_chars - 1].rstrip(" ,.;:") + "."
    return value


def _clean_expected_path_text(text: str | None) -> str | None:
    value = _compact_text(text, max_words=14, max_chars=110)
    if not value:
        return None
    lowered = value.lower()
    if value[0] in "+-" or value[0].isdigit():
        return None
    if "%" in value and any(term in lowered for term in {"upside path", "downside path", "central path"}):
        return None
    return value.rstrip(".")


def _probability_label(score: float) -> str:
    if score >= 0.72:
        return "high"
    if score >= 0.5:
        return "moderate"
    return "low"


def _risk_label(score: float) -> str:
    if score >= 0.7:
        return "high"
    if score >= 0.45:
        return "moderate"
    return "low"


def _support_score(strength: str) -> float:
    return {
        "tight_interval_support": 0.8,
        "strong": 0.74,
        "moderate": 0.58,
        "support_only": 0.46,
        "benchmark": 0.42,
        "weak": 0.32,
    }.get(str(strength or "").strip().lower(), 0.5)


_CROSS_ASSET_RELATIONS: dict[str, list[tuple[str, str]]] = {
    "duration": [("AGG", "same"), ("DXY", "inverse"), ("GLD", "inverse")],
    "inflation": [("GLD", "same"), ("DXY", "inverse"), ("AGG", "inverse")],
    "credit": [("ACWI", "same"), ("DXY", "inverse"), ("AGG", "same")],
    "policy": [("AGG", "same"), ("DXY", "inverse"), ("ACWI", "same")],
    "dollar_fx": [("DXY", "same"), ("ACWI", "inverse"), ("GLD", "inverse")],
    "energy": [("GLD", "same"), ("DXY", "inverse")],
    "real_assets": [("GLD", "same"), ("DXY", "inverse"), ("AGG", "same")],
    "volatility": [("ACWI", "inverse"), ("AGG", "same"), ("DXY", "same")],
    "growth": [("ACWI", "same"), ("DXY", "inverse")],
    "liquidity": [("ACWI", "same"), ("AGG", "same"), ("DXY", "inverse")],
    "market": [("ACWI", "same"), ("DXY", "inverse")],
}


_FULL_WAVE_B_CHANNEL_SPECS: dict[str, list[dict[str, str]]] = {
    "duration": [
        {"symbol": "DGS2", "relation": "same", "channel": "front_end_rates"},
        {"symbol": "^TNX", "relation": "same", "channel": "nominal_yields"},
        {"symbol": "^TYX", "relation": "same", "channel": "curve_shape"},
        {"symbol": "DFII10", "relation": "same", "channel": "real_yields"},
        {"symbol": "BAMLH0A0HYM2", "relation": "same", "channel": "credit_spillover"},
        {"symbol": "^SPXEW", "relation": "inverse", "channel": "equity_breadth"},
        {"symbol": "^VIX", "relation": "same", "channel": "volatility_regime"},
    ],
    "credit": [
        {"symbol": "^990100-USD-STRD", "relation": "inverse", "channel": "equity_breadth_confirmation"},
        {"symbol": "^SPXEW", "relation": "inverse", "channel": "equity_leadership"},
        {"symbol": "^VIX", "relation": "same", "channel": "risk_regime"},
        {"symbol": "DGS2", "relation": "inverse", "channel": "rates_context"},
        {"symbol": "DFII10", "relation": "inverse", "channel": "duration_interaction"},
        {"symbol": "DXY", "relation": "same", "channel": "funding_stress"},
    ],
    "dollar_fx": [
        {"symbol": "DFII10", "relation": "same", "channel": "real_yield_linkage"},
        {"symbol": "^990100-USD-STRD", "relation": "inverse", "channel": "ex_us_equity_pressure"},
        {"symbol": "^SPXEW", "relation": "inverse", "channel": "global_risk_hurdle"},
        {"symbol": "CL=F", "relation": "inverse", "channel": "commodity_spillover"},
        {"symbol": "GC=F", "relation": "inverse", "channel": "hedge_demand"},
        {"symbol": "^VIX", "relation": "same", "channel": "risk_regime"},
    ],
    "growth": [
        {"symbol": "^990100-USD-STRD", "relation": "same", "channel": "index_path"},
        {"symbol": "^SPXEW", "relation": "same", "channel": "breadth_measures"},
        {"symbol": "^IXIC", "relation": "same", "channel": "leadership_measures"},
        {"symbol": "BAMLH0A0HYM2", "relation": "inverse", "channel": "credit_confirmation"},
        {"symbol": "DFII10", "relation": "inverse", "channel": "rates_confirmation"},
        {"symbol": "DXY", "relation": "inverse", "channel": "fx_confirmation"},
        {"symbol": "^VIX", "relation": "inverse", "channel": "volatility_regime"},
    ],
    "market": [
        {"symbol": "^990100-USD-STRD", "relation": "same", "channel": "index_path"},
        {"symbol": "^SPXEW", "relation": "same", "channel": "breadth_measures"},
        {"symbol": "^IXIC", "relation": "same", "channel": "leadership_measures"},
        {"symbol": "BAMLH0A0HYM2", "relation": "inverse", "channel": "credit_confirmation"},
        {"symbol": "DFII10", "relation": "inverse", "channel": "rates_confirmation"},
        {"symbol": "DXY", "relation": "inverse", "channel": "fx_confirmation"},
        {"symbol": "^VIX", "relation": "inverse", "channel": "volatility_regime"},
    ],
    "energy": [
        {"symbol": "CL=F", "relation": "same", "channel": "commodity_path"},
        {"symbol": "BZ=F", "relation": "same", "channel": "commodity_path"},
        {"symbol": "DXY", "relation": "inverse", "channel": "dollar_context"},
        {"symbol": "CPI_YOY", "relation": "same", "channel": "inflation_context"},
        {"symbol": "DFII10", "relation": "same", "channel": "rates_context"},
        {"symbol": "GC=F", "relation": "same", "channel": "hedge_demand"},
        {"symbol": "^VIX", "relation": "same", "channel": "geopolitical_spillover"},
    ],
    "real_assets": [
        {"symbol": "GC=F", "relation": "same", "channel": "commodity_path"},
        {"symbol": "CL=F", "relation": "same", "channel": "inflation_transmission"},
        {"symbol": "DXY", "relation": "inverse", "channel": "dollar_context"},
        {"symbol": "CPI_YOY", "relation": "same", "channel": "inflation_context"},
        {"symbol": "DFII10", "relation": "inverse", "channel": "rates_context"},
        {"symbol": "^TNX", "relation": "inverse", "channel": "bond_offset_logic"},
        {"symbol": "^VIX", "relation": "same", "channel": "hedge_demand"},
    ],
    "inflation": [
        {"symbol": "CPI_YOY", "relation": "same", "channel": "inflation_context"},
        {"symbol": "CL=F", "relation": "same", "channel": "commodity_path"},
        {"symbol": "GC=F", "relation": "same", "channel": "hedge_demand"},
        {"symbol": "DXY", "relation": "inverse", "channel": "dollar_interaction"},
        {"symbol": "DFII10", "relation": "same", "channel": "rates_context"},
        {"symbol": "^TNX", "relation": "same", "channel": "bond_sleeve_offset"},
    ],
    "policy": [
        {"symbol": "FEDFUNDS", "relation": "same", "channel": "policy_path"},
        {"symbol": "DGS2", "relation": "same", "channel": "rates_confirmation"},
        {"symbol": "^TNX", "relation": "same", "channel": "curve_confirmation"},
        {"symbol": "DXY", "relation": "same", "channel": "fx_spillover"},
        {"symbol": "^990100-USD-STRD", "relation": "inverse", "channel": "risk_sentiment"},
        {"symbol": "^VIX", "relation": "same", "channel": "volatility_regime"},
    ],
    "volatility": [
        {"symbol": "^VIX", "relation": "same", "channel": "volatility_core"},
        {"symbol": "^SPXEW", "relation": "inverse", "channel": "equity_breadth"},
        {"symbol": "^990100-USD-STRD", "relation": "inverse", "channel": "equity_index"},
        {"symbol": "BAMLH0A0HYM2", "relation": "same", "channel": "credit_confirmation"},
        {"symbol": "DXY", "relation": "same", "channel": "flight_to_safety"},
        {"symbol": "^TNX", "relation": "inverse", "channel": "rate_relief"},
    ],
}


def _clamp_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _direction_sign(value: str | None) -> int:
    normalized = str(value or "").strip().lower()
    if normalized in {"positive", "up", "bull", "bullish"}:
        return 1
    if normalized in {"negative", "down", "bear", "bearish"}:
        return -1
    return 0


def _recent_direction_sign(history: list[Any]) -> int:
    values = [_safe_float(item) for item in list(history or [])]
    values = [value for value in values if value is not None]
    if len(values) < 2:
        return 0
    change = float(values[-1] - values[-2])
    if change > 0:
        return 1
    if change < 0:
        return -1
    return 0


def _direction_alignment_score(signal_card: dict[str, Any], bundle) -> float:
    expected = _direction_sign(signal_card.get("direction"))
    if expected == 0:
        expected = _recent_direction_sign(signal_card.get("history") or [])
    forecast = _direction_sign(getattr(bundle.result, "direction", None))
    if expected == 0 or forecast == 0:
        return 0.55
    if expected == forecast:
        return 1.0
    return 0.12


def _path_strength_score(current_value: float | None, point_path: list[float]) -> float:
    if current_value in {None, 0.0} or not point_path:
        return 0.35
    move = abs(float(point_path[-1]) - float(current_value)) / max(abs(float(current_value)), 1.0)
    return _clamp_unit(move / 0.035)


def _uncertainty_width_metrics(bundle, current_value: float | None) -> tuple[float, str]:
    quantiles = dict(getattr(bundle.result, "quantiles", {}) or {})
    upper = list(quantiles.get("0.9") or [])
    lower = list(quantiles.get("0.1") or [])
    if upper and lower:
        anchor = max(abs(float(current_value or bundle.result.point_path[-1] if bundle.result.point_path else 1.0)), 1.0)
        width = abs(float(upper[-1]) - float(lower[-1])) / anchor
    elif getattr(bundle.result, "point_path", None):
        point_path = list(bundle.result.point_path or [])
        anchor = max(abs(float(current_value or point_path[-1] or 1.0)), 1.0)
        width = abs(float(point_path[-1]) - float(point_path[0])) / anchor if len(point_path) >= 2 else 0.06
    else:
        width = 0.08
    score = _clamp_unit(width / 0.12)
    if score <= 0.28:
        return score, "tight"
    if score <= 0.58:
        return score, "moderate"
    if score <= 0.85:
        return score, "wide"
    return score, "bounded"


def _path_asymmetry_score(bundle, current_value: float | None) -> float:
    quantiles = dict(getattr(bundle.result, "quantiles", {}) or {})
    upper = list(quantiles.get("0.9") or [])
    lower = list(quantiles.get("0.1") or [])
    point = list(getattr(bundle.result, "point_path", []) or [])
    if not upper or not lower or not point:
        return 0.0
    center = float(point[-1])
    upside = max(0.0, float(upper[-1]) - center)
    downside = max(0.0, center - float(lower[-1]))
    if upside + downside <= 0:
        return 0.0
    return round((upside - downside) / (upside + downside), 4)


def _trigger_distance_score(current_value: float | None, bundle) -> float | None:
    point_path = list(getattr(bundle.result, "point_path", []) or [])
    if current_value in {None, 0.0} or not point_path:
        return None
    first_step = float(point_path[0])
    return round(abs(first_step - float(current_value)) / max(abs(float(current_value)), 1.0), 4)


def _market_confirmation_score(signal_card: dict[str, Any]) -> float:
    source_context = dict(signal_card.get("source_context") or {})
    state = str(source_context.get("market_confirmation") or "limited").strip().lower()
    return {
        "strong": 0.86,
        "moderate": 0.68,
        "limited": 0.48,
        "unconfirmed": 0.24,
    }.get(state, 0.48)


def _cross_asset_confirmation_score(signal_card: dict[str, Any]) -> float:
    bucket = _bucket_key(signal_card)
    relations = list(_CROSS_ASSET_RELATIONS.get(bucket) or _CROSS_ASSET_RELATIONS["market"])
    expected_sign = _direction_sign(signal_card.get("direction"))
    if expected_sign == 0:
        expected_sign = _recent_direction_sign(signal_card.get("history") or [])
    if expected_sign == 0:
        return 0.5
    matches = 0.0
    seen = 0
    for symbol, relation in relations:
        try:
            truth = load_surface_market_truth(
                symbol=symbol,
                surface_name="daily_brief",
                endpoint_family="ohlcv_history",
                lookback=30,
                allow_live_fetch=False,
            )
        except Exception:
            continue
        points = list(getattr(truth, "points", []) or [])
        if len(points) < 2:
            continue
        change = float(points[-1].value - points[-2].value)
        actual = 1 if change > 0 else -1 if change < 0 else 0
        if actual == 0:
            continue
        relation_sign = expected_sign if relation == "same" else -expected_sign
        matches += 1.0 if actual == relation_sign else 0.0
        seen += 1
    if seen == 0:
        return _market_confirmation_score(signal_card)
    return round(_clamp_unit(matches / seen), 4)


def _regime_alignment_score(signal_card: dict[str, Any], *, persistence_score: float, support_score: float, cross_asset_confirmation_score: float) -> float:
    market_confirmation = _market_confirmation_score(signal_card)
    return round(
        _clamp_unit(
            (persistence_score * 0.35)
            + (support_score * 0.2)
            + (cross_asset_confirmation_score * 0.3)
            + (market_confirmation * 0.15)
        ),
        4,
    )


def _history_target_points(signal_card: dict[str, Any], *, scenario_depth: str) -> int:
    if scenario_depth == "significant":
        source_class = str((signal_card.get("source_context") or {}).get("source_class") or "").strip().lower()
        if source_class in {"policy_event", "geopolitical_news"}:
            return 8
        return 16
    return 8


def _should_use_full_wave_b(signal_card: dict[str, Any], *, scenario_depth: str) -> bool:
    if scenario_depth != "significant":
        return False
    source_context = dict(signal_card.get("source_context") or {})
    source_class = str(source_context.get("source_class") or "market_series").strip().lower()
    history_len = len(list(signal_card.get("history") or []))
    market_confirmation = str(source_context.get("market_confirmation") or "limited").strip().lower()
    if source_class in {"policy_event", "geopolitical_news"}:
        return history_len >= 4 or market_confirmation in {"partial", "moderate", "broad", "strong"}
    return True


def _related_series_specs(signal_card: dict[str, Any], *, scenario_depth: str) -> list[dict[str, str]]:
    bucket = _bucket_key(signal_card)
    if _should_use_full_wave_b(signal_card, scenario_depth=scenario_depth):
        return list(_FULL_WAVE_B_CHANNEL_SPECS.get(bucket) or _FULL_WAVE_B_CHANNEL_SPECS.get("market") or [])
    relations = list(_CROSS_ASSET_RELATIONS.get(bucket) or _CROSS_ASSET_RELATIONS["market"])
    return [{"symbol": symbol, "relation": relation, "channel": "cross_asset_confirmation"} for symbol, relation in relations]


def _related_series_inputs(signal_card: dict[str, Any], *, scenario_depth: str = "coverage") -> list[dict[str, Any]]:
    bucket = _bucket_key(signal_card)
    specs = _related_series_specs(signal_card, scenario_depth=scenario_depth)
    symbol_self = str(signal_card.get("symbol") or "").strip().upper()
    related: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    min_points = 4 if _should_use_full_wave_b(signal_card, scenario_depth=scenario_depth) else 8
    for spec in specs:
        symbol = str(spec.get("symbol") or "").strip().upper()
        if not symbol or symbol == symbol_self or symbol in seen_symbols:
            continue
        seen_symbols.add(symbol)
        relation = str(spec.get("relation") or "same").strip().lower() or "same"
        channel = str(spec.get("channel") or "cross_asset_confirmation").strip().lower() or "cross_asset_confirmation"
        try:
            truth = load_surface_market_truth(
                symbol=symbol,
                surface_name="daily_brief",
                endpoint_family="ohlcv_history",
                lookback=90,
                allow_live_fetch=False,
            )
        except Exception:
            continue
        history: list[float] = []
        timestamps: list[str] = []
        for point in list(getattr(truth, "points", []) or [])[-90:]:
            value = _safe_float(getattr(point, "value", None))
            if value is None:
                continue
            history.append(value)
            timestamps.append(str(getattr(point, "timestamp", "") or ""))
        if len(history) < min_points:
            continue
        latest_change = float(history[-1] - history[-2]) if len(history) >= 2 else 0.0
        latest_change_pct = ((latest_change / abs(history[-2])) * 100.0) if len(history) >= 2 and history[-2] not in {0.0, None} else 0.0
        related.append(
            {
                "symbol": symbol,
                "relation": relation,
                "channel": channel,
                "history": history,
                "timestamps": timestamps,
                "current_value": history[-1],
                "latest_change": round(latest_change, 4),
                "latest_change_pct": round(latest_change_pct, 4),
                "latest_direction": "up" if latest_change > 0 else "down" if latest_change < 0 else "flat",
            }
        )
    return related


def _binary_series(length: int, value: float) -> list[float]:
    return [round(float(value), 4)] * max(int(length or 0), 1)


def _market_confirmation_numeric(state: str) -> float:
    return {
        "none": 0.12,
        "unconfirmed": 0.22,
        "limited": 0.4,
        "partial": 0.56,
        "moderate": 0.62,
        "broad": 0.78,
        "strong": 0.84,
    }.get(str(state or "").strip().lower(), 0.4)


def _grouped_context_series(bucket: str, related_series: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    if not related_series:
        return {}
    channel_map = {
        "duration": {
            "DXY": "cross_asset_valuation",
            "AGG": "duration_confirmation",
            "GLD": "hedge_confirmation",
        },
        "inflation": {
            "GLD": "inflation_hedge",
            "DXY": "dollar_interaction",
            "AGG": "duration_offset",
        },
        "credit": {
            "ACWI": "equity_breadth_confirmation",
            "DXY": "funding_stress",
            "AGG": "duration_spillover",
        },
        "dollar_fx": {
            "DXY": "dollar_core",
            "ACWI": "global_risk_hurdle",
            "GLD": "commodity_spillover",
        },
        "growth": {
            "ACWI": "breadth_and_leadership",
            "DXY": "fx_headwind",
            "AGG": "rates_confirmation",
        },
        "energy": {
            "GLD": "hedge_demand",
            "DXY": "dollar_interaction",
        },
        "real_assets": {
            "GLD": "hedge_demand",
            "DXY": "dollar_interaction",
            "AGG": "bond_offset",
        },
        "policy": {
            "AGG": "rates_confirmation",
            "DXY": "fx_spillover",
            "ACWI": "risk_sentiment",
        },
        "volatility": {
            "ACWI": "equity_breadth",
            "AGG": "defensive_rate_response",
            "DXY": "flight_to_safety",
        },
    }
    mapping = channel_map.get(bucket, {})
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in related_series:
        channel = str(item.get("channel") or "").strip().lower() or mapping.get(str(item.get("symbol") or "").upper(), "cross_asset_confirmation")
        grouped.setdefault(channel, []).append(item)
    return grouped


def _release_distance_days(signal_card: dict[str, Any], source_context: dict[str, Any]) -> float | None:
    for key in ("release_date", "availability_date", "as_of"):
        raw = str(source_context.get(key) or signal_card.get(key) or "").strip()
        if not raw:
            continue
        try:
            release_dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            anchor_raw = str(signal_card.get("as_of") or "").strip()
            anchor_dt = datetime.fromisoformat(anchor_raw.replace("Z", "+00:00")) if anchor_raw else release_dt
            return round(abs((anchor_dt - release_dt).total_seconds()) / 86400.0, 3)
        except Exception:
            continue
    return None


def _channel_presence(grouped_context: dict[str, list[dict[str, Any]]], channels: set[str]) -> float:
    present = sum(1 for channel in channels if grouped_context.get(channel))
    total = max(len(channels), 1)
    return round(present / total, 4)


def _wave_b_family_flags(*, bucket: str, grouped_context: dict[str, list[dict[str, Any]]], source_context: dict[str, Any]) -> dict[str, float]:
    if bucket == "duration":
        return {
            "real_yield_context_flag": _channel_presence(grouped_context, {"real_yields"}),
            "curve_context_flag": _channel_presence(grouped_context, {"curve_shape", "front_end_rates", "nominal_yields"}),
            "credit_spillover_flag": _channel_presence(grouped_context, {"credit_spillover"}),
            "equity_valuation_pressure_flag": _channel_presence(grouped_context, {"equity_breadth"}),
        }
    if bucket == "credit":
        return {
            "equity_confirmation_flag": _channel_presence(grouped_context, {"equity_breadth_confirmation", "equity_leadership"}),
            "rates_context_flag": _channel_presence(grouped_context, {"rates_context", "duration_interaction"}),
            "risk_regime_flag": _channel_presence(grouped_context, {"risk_regime", "funding_stress"}),
        }
    if bucket == "dollar_fx":
        return {
            "real_yield_linkage_flag": _channel_presence(grouped_context, {"real_yield_linkage"}),
            "ex_us_equity_flag": _channel_presence(grouped_context, {"ex_us_equity_pressure", "global_risk_hurdle"}),
            "commodity_context_flag": _channel_presence(grouped_context, {"commodity_spillover", "hedge_demand"}),
            "threshold_pressure_flag": 1.0 if str(source_context.get("threshold_state") or "").strip().lower() in {"watch", "breached"} else 0.0,
        }
    if bucket in {"growth", "market"}:
        return {
            "breadth_flag": _channel_presence(grouped_context, {"breadth_measures"}),
            "leadership_flag": _channel_presence(grouped_context, {"leadership_measures"}),
            "credit_confirmation_flag": _channel_presence(grouped_context, {"credit_confirmation"}),
            "rates_confirmation_flag": _channel_presence(grouped_context, {"rates_confirmation"}),
            "fx_confirmation_flag": _channel_presence(grouped_context, {"fx_confirmation"}),
            "volatility_regime_flag": _channel_presence(grouped_context, {"volatility_regime"}),
        }
    if bucket in {"energy", "real_assets", "inflation"}:
        return {
            "dollar_context_flag": _channel_presence(grouped_context, {"dollar_context", "dollar_interaction"}),
            "inflation_context_flag": _channel_presence(grouped_context, {"inflation_context", "inflation_transmission"}),
            "rates_context_flag": _channel_presence(grouped_context, {"rates_context", "bond_offset_logic", "bond_sleeve_offset"}),
            "hedge_demand_flag": _channel_presence(grouped_context, {"hedge_demand"}),
            "geopolitical_spillover_flag": _channel_presence(grouped_context, {"geopolitical_spillover"}),
        }
    if bucket == "policy":
        return {
            "policy_path_flag": _channel_presence(grouped_context, {"policy_path"}),
            "rates_confirmation_flag": _channel_presence(grouped_context, {"rates_confirmation", "curve_confirmation"}),
            "risk_sentiment_flag": _channel_presence(grouped_context, {"risk_sentiment", "volatility_regime"}),
            "fx_spillover_flag": _channel_presence(grouped_context, {"fx_spillover"}),
        }
    if bucket == "volatility":
        return {
            "volatility_core_flag": _channel_presence(grouped_context, {"volatility_core"}),
            "equity_breadth_flag": _channel_presence(grouped_context, {"equity_breadth", "equity_index"}),
            "flight_to_safety_flag": _channel_presence(grouped_context, {"flight_to_safety"}),
            "credit_confirmation_flag": _channel_presence(grouped_context, {"credit_confirmation"}),
        }
    return {}


def _scenario_request_context(
    signal_card: dict[str, Any],
    related_series: list[dict[str, Any]],
    *,
    scenario_depth: str,
    history_target_points: int,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source_context = dict(signal_card.get("source_context") or {})
    bucket = _bucket_key(signal_card)
    history_len = max(len(list(signal_card.get("history") or [])), int(history_target_points or 1))
    freshness_age = float(source_context.get("freshness_age_days") or 99.0)
    source_class = str(source_context.get("source_class") or "market_series").lower()
    threshold_state = str(source_context.get("threshold_state") or "watch").lower()
    market_confirmation_score = _market_confirmation_numeric(str(source_context.get("market_confirmation") or "limited"))
    grouped_context = _grouped_context_series(bucket, related_series)
    official_release_flag = 1.0 if source_class == "macro_release" else 0.0
    event_flag = 1.0 if source_class in {"policy_event", "geopolitical_news"} else 0.0
    fresh_flag = 1.0 if freshness_age <= 1.5 else 0.0
    past_covariates = {
        "fresh_signal_flag": _binary_series(history_len, fresh_flag),
        "official_release_flag": _binary_series(history_len, official_release_flag),
        "event_signal_flag": _binary_series(history_len, event_flag),
        "threshold_watch_flag": _binary_series(history_len, 1.0 if threshold_state == "watch" else 0.0),
        "threshold_breached_flag": _binary_series(history_len, 1.0 if threshold_state == "breached" else 0.0),
        "market_confirmation_score": _binary_series(history_len, market_confirmation_score),
    }
    future_covariates = {
        "freshness_age_days": round(freshness_age, 3),
        "market_confirmation_score": round(market_confirmation_score, 4),
        "threshold_watch_flag": 1 if threshold_state == "watch" else 0,
        "threshold_breached_flag": 1 if threshold_state == "breached" else 0,
        "official_release_flag": int(official_release_flag),
        "event_signal_flag": int(event_flag),
    }
    if scenario_depth == "significant":
        release_distance_days = _release_distance_days(signal_card, source_context)
        related_alignment, related_contradiction, _ = _related_market_alignment(signal_card, grouped_context)
        family_flags = _wave_b_family_flags(bucket=bucket, grouped_context=grouped_context, source_context=source_context)
        past_covariates.update(
            {
                "related_alignment_score": _binary_series(history_len, related_alignment),
                "related_contradiction_score": _binary_series(history_len, related_contradiction),
                "wave_b_significant_flag": _binary_series(history_len, 1.0),
                **{key: _binary_series(history_len, value) for key, value in family_flags.items()},
            }
        )
        future_covariates.update(
            {
                "related_alignment_score": round(related_alignment, 4),
                "related_contradiction_score": round(related_contradiction, 4),
                "wave_b_significant_flag": 1,
                **{key: round(value, 4) for key, value in family_flags.items()},
            }
        )
        if release_distance_days is not None:
            future_covariates["release_distance_days"] = release_distance_days
            past_covariates["release_distance_days"] = _binary_series(history_len, release_distance_days)
    if bucket == "duration":
        future_covariates["duration_hurdle_flag"] = 1
    elif bucket == "credit":
        future_covariates["funding_stress_flag"] = 1
    elif bucket == "dollar_fx":
        future_covariates["global_hurdle_flag"] = 1
    elif bucket in {"growth", "market", "volatility"}:
        future_covariates["breadth_confirmation_flag"] = 1
    return past_covariates, future_covariates, grouped_context


def _scenario_support_strength_from_scores(*, persistence_score: float, fade_risk: float, cross_asset_confirmation_score: float, uncertainty_width_score: float) -> str:
    composite = (persistence_score * 0.4) + ((1.0 - fade_risk) * 0.2) + (cross_asset_confirmation_score * 0.25) + ((1.0 - uncertainty_width_score) * 0.15)
    if composite >= 0.72:
        return "strong"
    if composite >= 0.52:
        return "moderate"
    return "bounded"


def _forecast_intelligence(signal_card: dict[str, Any], bundle) -> dict[str, Any]:
    current_value = _safe_float(signal_card.get("current_value"))
    request = getattr(bundle, "request", None)
    grouped_context = dict(getattr(request, "grouped_context_series", {}) or {})
    future_covariates = dict(getattr(request, "future_covariates", {}) or {})
    scenario_depth = str(getattr(request, "covariates", {}).get("scenario_depth") or "").strip().lower()
    support_score = _support_score(getattr(bundle.support, "support_strength", ""))
    direction_alignment = _direction_alignment_score(signal_card, bundle)
    path_strength = _path_strength_score(current_value, list(getattr(bundle.result, "point_path", []) or []))
    uncertainty_width_score, uncertainty_label = _uncertainty_width_metrics(bundle, current_value)
    context_alignment, context_contradiction, _ = _related_market_alignment(signal_card, grouped_context)
    cross_asset_confirmation_score = _cross_asset_confirmation_score(signal_card)
    if grouped_context:
        cross_asset_confirmation_score = round(
            _clamp_unit(
                (cross_asset_confirmation_score * 0.24)
                + (context_alignment * 0.56)
                + (_market_confirmation_score(signal_card) * 0.20)
                - (context_contradiction * 0.10)
            ),
            4,
        )
    threshold_boost = 0.08 if future_covariates.get("threshold_breached_flag") else 0.04 if future_covariates.get("threshold_watch_flag") else 0.0
    wave_b_bonus = 0.08 if scenario_depth == "significant" else 0.0
    persistence_score = round(
        _clamp_unit(
            0.15
            + (direction_alignment * 0.35)
            + (support_score * 0.22)
            + (path_strength * 0.18)
            + ((1.0 - uncertainty_width_score) * 0.10)
            + (context_alignment * 0.08)
            - (context_contradiction * 0.06)
            + wave_b_bonus
        ),
        4,
    )
    fade_risk = round(
        _clamp_unit(
            0.12
            + ((1.0 - direction_alignment) * 0.42)
            + (uncertainty_width_score * 0.26)
            + ((1.0 - support_score) * 0.20)
            + (context_contradiction * 0.10)
            - (context_alignment * 0.05)
        ),
        4,
    )
    trigger_distance = _trigger_distance_score(current_value, bundle)
    trigger_distance_value = trigger_distance if trigger_distance is not None else 0.06
    trigger_pressure = round(
        _clamp_unit(
            ((1.0 - min(trigger_distance_value / 0.05, 1.0)) * 0.45)
            + (persistence_score * 0.35)
            + (support_score * 0.20)
            + threshold_boost
            + (context_alignment * 0.06)
            - (context_contradiction * 0.04)
        ),
        4,
    )
    regime_alignment_score = _regime_alignment_score(
        signal_card,
        persistence_score=persistence_score,
        support_score=support_score,
        cross_asset_confirmation_score=cross_asset_confirmation_score,
    )
    if scenario_depth == "significant":
        regime_alignment_score = round(
            _clamp_unit(
                regime_alignment_score
                + (context_alignment * 0.08)
                - (context_contradiction * 0.06)
                + (0.04 if grouped_context else 0.0)
            ),
            4,
        )
    scenario_support_strength = _scenario_support_strength_from_scores(
        persistence_score=persistence_score,
        fade_risk=fade_risk,
        cross_asset_confirmation_score=cross_asset_confirmation_score,
        uncertainty_width_score=uncertainty_width_score,
    )
    escalation_flag = bool(
        trigger_pressure >= 0.64
        and persistence_score >= 0.52
        and cross_asset_confirmation_score >= 0.42
        and str(signal_card.get("sufficiency_state") or "").strip().lower() not in {"thin", "insufficient"}
    )
    return {
        "persistence_score": persistence_score,
        "fade_risk": fade_risk,
        "trigger_distance": trigger_distance,
        "trigger_pressure": trigger_pressure,
        "path_asymmetry": _path_asymmetry_score(bundle, current_value),
        "uncertainty_width_score": uncertainty_width_score,
        "uncertainty_width_label": uncertainty_label,
        "regime_alignment_score": regime_alignment_score,
        "cross_asset_confirmation_score": cross_asset_confirmation_score,
        "scenario_support_strength": scenario_support_strength,
        "escalation_flag": escalation_flag,
    }


def _bucket_key(signal_card: dict[str, Any]) -> str:
    return str(signal_card.get("primary_effect_bucket") or "market").strip().lower() or "market"


def _source_evidence_state(*, source_class: str, market_confirmation: str, sufficiency_state: str | None) -> str:
    if source_class in {"policy_event", "geopolitical_news"}:
        if market_confirmation == "strong":
            return "event_confirmed"
        return "bounded_event"
    if source_class == "macro_release":
        return "official_release"
    if str(sufficiency_state or "").strip().lower() in {"thin", "insufficient"}:
        return "bounded_signal"
    return "market_confirmed"


def _evidence_class(signal_card: dict[str, Any], source_class: str) -> str:
    explicit = str(signal_card.get("evidence_class") or "").strip().lower()
    if explicit:
        return explicit
    if source_class == "macro_release":
        return "official_release"
    if source_class in {"policy_event", "geopolitical_news"}:
        confirmation = str((signal_card.get("source_context") or {}).get("market_confirmation") or "limited").strip().lower()
        return "market_confirmed_news" if confirmation in {"strong", "moderate", "partial", "broad"} else "reported_event_unconfirmed_market_read"
    if str(signal_card.get("source_kind") or "").strip().lower() == "market_close":
        return "public_verified_close"
    return "bounded_inference"


def _related_market_alignment(signal_card: dict[str, Any], grouped_context_series: dict[str, Any]) -> tuple[float, float, list[str]]:
    expected_sign = _direction_sign(signal_card.get("direction"))
    if expected_sign == 0:
        expected_sign = _recent_direction_sign(signal_card.get("history") or [])
    if expected_sign == 0:
        expected_sign = 1
    matches = 0
    conflicts = 0
    notes: list[str] = []
    for channel, items in dict(grouped_context_series or {}).items():
        channel_matches = 0
        channel_conflicts = 0
        for item in list(items or []):
            latest_direction = str(item.get("latest_direction") or "").strip().lower()
            actual_sign = 1 if latest_direction == "up" else -1 if latest_direction == "down" else 0
            if actual_sign == 0:
                continue
            relation = str(item.get("relation") or "same").strip().lower()
            relation_sign = expected_sign if relation == "same" else -expected_sign
            if actual_sign == relation_sign:
                channel_matches += 1
            else:
                channel_conflicts += 1
        if channel_matches == 0 and channel_conflicts == 0:
            continue
        matches += channel_matches
        conflicts += channel_conflicts
        human_channel = str(channel).replace("_", " ")
        if channel_matches >= max(1, channel_conflicts):
            notes.append(f"{human_channel} is broadly confirming the move")
        else:
            notes.append(f"{human_channel} is still resisting the move")
    total = matches + conflicts
    if total == 0:
        return 0.5, 0.0, notes
    alignment = round(_clamp_unit(matches / total), 4)
    contradiction = round(_clamp_unit(conflicts / total), 4)
    return alignment, contradiction, notes[:3]


def _scenario_fact_inputs(signal_card: dict[str, Any], bundle) -> dict[str, Any]:
    source_context = dict(signal_card.get("source_context") or {})
    source_class = str(source_context.get("source_class") or "market_series")
    market_confirmation_state = str(source_context.get("market_confirmation") or "limited").strip().lower()
    request = getattr(bundle, "request", None)
    grouped_context_series = dict(getattr(request, "grouped_context_series", {}) or {})
    related_alignment, related_contradiction, related_notes = _related_market_alignment(signal_card, grouped_context_series)
    history_points = len(list(getattr(request, "history", []) or signal_card.get("history") or []))
    freshness_age_days = float(source_context.get("freshness_age_days") or 99.0)
    freshness_state = str(signal_card.get("freshness_state") or ("fresh" if freshness_age_days <= 1.5 else "latest_valid")).strip().lower()
    evidence_class = _evidence_class(signal_card, source_class)
    release_semantics = str(source_context.get("release_semantics_state") or source_context.get("revision_state") or "").strip() or None
    external_confirmation_score = round(
        _clamp_unit((_market_confirmation_numeric(market_confirmation_state) * 0.62) + (related_alignment * 0.38)),
        4,
    )
    sparse_history_penalty = 0.0 if history_points >= 20 else 0.08 if history_points >= 12 else 0.16 if history_points >= 8 else 0.28
    return {
        "raw_move_and_comparator": str(signal_card.get("evidence_title") or signal_card.get("summary") or "").strip() or None,
        "freshness_state": freshness_state,
        "freshness_age_days": freshness_age_days,
        "evidence_class": evidence_class,
        "release_semantics": release_semantics,
        "source_validity": str(signal_card.get("freshness_label") or freshness_state).strip(),
        "market_confirmation_state": market_confirmation_state,
        "external_confirmation_score": external_confirmation_score,
        "related_market_alignment_score": related_alignment,
        "related_market_contradiction_score": related_contradiction,
        "related_market_notes": related_notes,
        "sparse_history_penalty": sparse_history_penalty,
        "isolated_signal": external_confirmation_score < 0.44 and related_alignment < 0.46,
        "broadening_into_regime": external_confirmation_score >= 0.62 and related_alignment >= 0.58,
        "history_points": history_points,
        "threshold_state": str(source_context.get("threshold_state") or "watch").strip().lower(),
        "sleeve_relevance_count": len(list(signal_card.get("affected_sleeves") or [])),
    }


def _scenario_profile(bundle, signal_card: dict[str, Any]) -> dict[str, Any]:
    source_context = dict(signal_card.get("source_context") or {})
    source_class = str(source_context.get("source_class") or "market_series")
    market_confirmation = str(source_context.get("market_confirmation") or "limited")
    support = bundle.support
    result = bundle.result
    support_strength = str(getattr(support, "support_strength", "") or "").lower()
    confidence_band = str(getattr(result, "confidence_band", "") or "").lower()
    anomaly = float(getattr(result, "anomaly_score", 0.0) or 0.0)
    direction = str(getattr(result, "direction", "mixed") or "mixed").lower()
    near_term = bundle.trigger_support[0].to_dict() if bundle.trigger_support else {}
    thesis = bundle.trigger_support[1].to_dict() if len(bundle.trigger_support) > 1 else {}
    threshold_state = str((near_term or {}).get("threshold_state") or "watch").lower()
    persistence_score = float(getattr(support, "persistence_score", 0.0) or 0.0)
    fade_risk = float(getattr(support, "fade_risk", 0.0) or 0.0)
    trigger_distance = float(getattr(support, "trigger_distance", 0.0) or 0.0)
    trigger_pressure = float(getattr(support, "trigger_pressure", 0.0) or 0.0)
    path_asymmetry = float(getattr(support, "path_asymmetry", 0.0) or 0.0)
    uncertainty_width = str(getattr(support, "uncertainty_width_label", "") or confidence_band or "bounded")
    regime_alignment_score = float(getattr(support, "regime_alignment_score", 0.0) or 0.0)
    cross_asset_confirmation_score = float(getattr(support, "cross_asset_confirmation_score", 0.0) or 0.0)
    horizon_days = int(getattr(support, "horizon", 0) or getattr(result, "horizon", 0) or 0)
    scenario_support_strength = str(getattr(support, "scenario_support_strength", None) or support_strength or "available")
    degraded_state = str(getattr(support, "degraded_state", "") or "").strip() or None
    fact_inputs = _scenario_fact_inputs(signal_card, bundle)

    confirm_base = _clamp(
        (_support_score(support_strength) * 0.18)
        + (persistence_score * 0.30)
        + ((1.0 - fade_risk) * 0.12)
        + (regime_alignment_score * 0.20)
        + (cross_asset_confirmation_score * 0.15)
        + (_market_confirmation_score(signal_card) * 0.05)
        - (anomaly * 0.10)
    )
    if str(signal_card.get("sufficiency_state") or "").strip().lower() in {"thin", "insufficient"}:
        confirm_base = _clamp(confirm_base - 0.14)

    break_base = _clamp(
        (fade_risk * 0.42)
        + ((1.0 - persistence_score) * 0.23)
        + (float(uncertainty_width in {"wide", "bounded"}) * 0.14)
        + (anomaly * 0.12)
        + ((1.0 - regime_alignment_score) * 0.09)
    )
    threshold_breach_base = _clamp(
        (trigger_pressure * 0.62)
        + ((1.0 - min(trigger_distance / 0.05 if trigger_distance else 1.0, 1.0)) * 0.18)
        + (anomaly * 0.08)
        + (0.12 if threshold_state == "breached" else 0.0)
    )

    if persistence_score >= 0.72 and fade_risk <= 0.34 and direction != "mixed":
        persistence = "durable path support"
    elif trigger_pressure >= 0.68 and persistence_score >= 0.56:
        persistence = "threshold pressure rising"
    elif fade_risk >= 0.62 or threshold_breach_base >= 0.7:
        persistence = "fragile path"
    else:
        persistence = "two-way path"

    if direction == "positive":
        path_bias = "upside bias"
    elif direction == "negative":
        path_bias = "downside bias"
    else:
        path_bias = "two-way bias"

    return {
        "bucket": _bucket_key(signal_card),
        "fact_inputs": fact_inputs,
        "forecast_inputs": {
            "point_path": list(getattr(result, "point_path", []) or []),
            "quantiles": dict(getattr(result, "quantiles", {}) or {}),
            "persistence_score": persistence_score,
            "fade_risk": fade_risk,
            "trigger_distance": trigger_distance,
            "trigger_pressure": trigger_pressure,
            "path_asymmetry": path_asymmetry,
            "uncertainty_width_score": float(getattr(support, "uncertainty_width_score", 0.0) or 0.0),
            "uncertainty_width_label": uncertainty_width,
            "regime_alignment_score": regime_alignment_score,
            "cross_asset_confirmation_score": cross_asset_confirmation_score,
            "scenario_support_strength": scenario_support_strength,
        },
        "source_class": source_class,
        "market_confirmation": market_confirmation,
        "evidence_state": _source_evidence_state(
            source_class=source_class,
            market_confirmation=market_confirmation,
            sufficiency_state=str(signal_card.get("sufficiency_state") or ""),
        ),
        "support_strength": scenario_support_strength,
        "confidence_band": confidence_band or "bounded",
        "uncertainty_width": uncertainty_width,
        "direction": direction,
        "path_bias": path_bias,
        "anomaly": anomaly,
        "confirm_base": confirm_base,
        "break_base": break_base,
        "threshold_breach_base": _clamp(threshold_breach_base),
        "persistence_vs_reversion": persistence,
        "threshold_state": threshold_state or "watch",
        "near_term_trigger": near_term,
        "thesis_trigger": thesis,
        "trigger_distance": trigger_distance,
        "trigger_pressure": trigger_pressure,
        "path_asymmetry_score": path_asymmetry,
        "regime_alignment_score": regime_alignment_score,
        "cross_asset_confirmation_score": cross_asset_confirmation_score,
        "external_confirmation_score": fact_inputs["external_confirmation_score"],
        "external_contradiction_score": fact_inputs["related_market_contradiction_score"],
        "sparse_history_penalty": fact_inputs["sparse_history_penalty"],
        "escalation_flag": bool(getattr(support, "escalation_flag", False)),
        "horizon_days": horizon_days,
        "degraded_state": degraded_state,
    }


_SCENARIO_LEAD_TEXT: dict[str, dict[str, str]] = {
    "duration": {
        "bull": "Rates stop tightening the duration hurdle.",
        "base": "The duration hurdle stays in place.",
        "bear": "Another move higher would delay bond adds.",
    },
    "inflation": {
        "bull": "Price pressure cools enough to steady duration.",
        "base": "Sticky inflation keeps duration patient.",
        "bear": "Inflation reacceleration would reset the brief.",
    },
    "credit": {
        "bull": "Credit pressure eases and carry stabilizes.",
        "base": "Credit stays selective but contained.",
        "bear": "Spread stress would tighten the risk budget.",
    },
    "policy": {
        "bull": "Policy risk settles without forcing a new move.",
        "base": "Policy still anchors the timing window.",
        "bear": "Policy repricing would reopen rate and cash review.",
    },
    "dollar_fx": {
        "bull": "Dollar pressure eases enough to steady global risk.",
        "base": "Dollar strength remains a hurdle, not a shock.",
        "bear": "A stronger dollar would tighten the global hurdle.",
    },
    "energy": {
        "bull": "Oil pressure cools and hedge urgency fades.",
        "base": "Energy stays firm enough to keep hedges relevant.",
        "bear": "A fresh oil spike would broaden the inflation shock.",
    },
    "real_assets": {
        "bull": "Hedge demand stays constructive without overheating.",
        "base": "Real assets keep their hedge role.",
        "bear": "Hedge demand weakens if the signal rolls over.",
    },
    "volatility": {
        "bull": "Volatility mean-reverts and risk conditions calm.",
        "base": "Volatility stays elevated but contained.",
        "bear": "Volatility spillover would force faster de-risking.",
    },
    "growth": {
        "bull": "Breadth improves and risk appetite broadens.",
        "base": "Equity leadership stays narrow but stable.",
        "bear": "A weaker breadth path would cut risk appetite.",
    },
    "liquidity": {
        "bull": "Liquidity-sensitive risk can stabilize.",
        "base": "Liquidity still caps how much risk can add.",
        "bear": "Liquidity stress would shrink the risk budget.",
    },
    "market": {
        "bull": "The supportive path strengthens the current read.",
        "base": "The current read still fits the path.",
        "bear": "A break path would change the brief quickly.",
    },
}

_SCENARIO_MACRO_BY_BUCKET: dict[str, dict[str, str]] = {
    "duration": {
        "bull": "Financing conditions stop worsening.",
        "base": "Financing conditions stay restrictive but stable.",
        "bear": "Financing conditions tighten again.",
    },
    "inflation": {
        "bull": "Price spillover cools rather than broadens.",
        "base": "Price pressure stays sticky, not benign.",
        "bear": "Price pressure broadens into a harder macro headwind.",
    },
    "credit": {
        "bull": "Funding conditions stabilize.",
        "base": "Funding conditions stay selective without breaking.",
        "bear": "Funding conditions tighten across risk assets.",
    },
    "policy": {
        "bull": "Policy spillover stays contained.",
        "base": "Policy keeps guiding the rate path.",
        "bear": "Policy repricing broadens into the macro hurdle.",
    },
    "dollar_fx": {
        "bull": "Dollar pressure stops tightening global conditions.",
        "base": "Dollar strength stays a headwind without becoming a shock.",
        "bear": "Dollar strength tightens financial conditions further.",
    },
    "energy": {
        "bull": "Cost-shock pressure fades.",
        "base": "Energy keeps the inflation impulse alive.",
        "bear": "The cost shock broadens through the macro backdrop.",
    },
    "real_assets": {
        "bull": "Hedge demand stays supportive without signaling disorder.",
        "base": "Hedge demand remains part of the macro mix.",
        "bear": "Hedge support weakens as the macro case softens.",
    },
    "volatility": {
        "bull": "Macro stress eases before it becomes regime-breaking.",
        "base": "Stress stays elevated but contained.",
        "bear": "Stress spills into a broader regime shift.",
    },
    "growth": {
        "bull": "Macro growth risk stabilizes.",
        "base": "Growth risk stays mixed rather than improving cleanly.",
        "bear": "Growth risk deteriorates enough to matter broadly.",
    },
    "liquidity": {
        "bull": "Liquidity conditions stabilize.",
        "base": "Liquidity remains selective.",
        "bear": "Liquidity tightens into a broader macro brake.",
    },
    "market": {
        "bull": "Macro spillover stays contained.",
        "base": "Macro spillover remains bounded.",
        "bear": "Macro spillover broadens enough to matter.",
    },
}

_SCENARIO_MICRO_BY_BUCKET: dict[str, dict[str, str]] = {
    "duration": {
        "bull": "Bond adds can move toward staged execution.",
        "base": "Keep bond adds patient; timing still matters.",
        "bear": "Delay duration adds and reopen bond review.",
    },
    "inflation": {
        "bull": "The hedge mix can stay balanced without forcing more real assets.",
        "base": "Keep duration patient and hedges alive.",
        "bear": "Lean further on hedges and delay duration adds.",
    },
    "credit": {
        "bull": "Carry can hold without a tighter risk budget.",
        "base": "Keep risk budget selective, not aggressive.",
        "bear": "Tighten the risk budget and recheck carry exposure.",
    },
    "policy": {
        "bull": "No fresh rate or cash move is forced.",
        "base": "Keep bond and cash timing tied to the current policy window.",
        "bear": "Reopen bond and cash review if repricing persists.",
    },
    "dollar_fx": {
        "bull": "Global risk can size more freely if the hurdle eases.",
        "base": "Keep ex-US risk sizing selective.",
        "bear": "Trim risk appetite if the dollar hurdle hardens again.",
    },
    "energy": {
        "bull": "Real-assets urgency fades back toward watch mode.",
        "base": "Keep hedges relevant but sized with care.",
        "bear": "Reopen real-assets protection if the oil spike extends.",
    },
    "real_assets": {
        "bull": "The hedge sleeve can hold without escalation.",
        "base": "Keep real assets as support, not the whole call.",
        "bear": "Hedge conviction weakens if the signal loses follow-through.",
    },
    "volatility": {
        "bull": "Risk posture can stay calmer without forcing de-risking.",
        "base": "Keep guardrails up, but do not overreact.",
        "bear": "Raise cash or trim risk if volatility spills further.",
    },
    "growth": {
        "bull": "Equity exposure can stay engaged if breadth improves.",
        "base": "Keep equity adds selective and leadership-aware.",
        "bear": "Reduce enthusiasm for equity risk if breadth weakens further.",
    },
    "liquidity": {
        "bull": "Liquidity-sensitive risk can stay on watch rather than review.",
        "base": "Keep adds selective while liquidity stays bounded.",
        "bear": "Shrink risk appetite if liquidity stress widens.",
    },
    "market": {
        "bull": "The sleeve consequence stays manageable.",
        "base": "Keep posture steady and selective.",
        "bear": "Escalate review if the path breaks against the current read.",
    },
}


def _scenario_case_metrics(profile: dict[str, Any], *, scenario_type: str) -> dict[str, Any]:
    confirm_base = float(profile["confirm_base"])
    break_base = float(profile["break_base"])
    breach_base = float(profile["threshold_breach_base"])
    direction = str(profile["direction"] or "mixed")
    evidence_state = str(profile.get("evidence_state") or "")
    if scenario_type == "bull":
        confirm = _clamp(confirm_base + (0.12 if direction == "positive" else 0.05))
        break_prob = _clamp(break_base - 0.12)
        breach_prob = _clamp(breach_base - 0.12)
        path_bias = "follow-through" if direction == "positive" else "relief" if direction == "negative" else "improving"
    elif scenario_type == "bear":
        confirm = _clamp(confirm_base - 0.22)
        break_prob = _clamp(break_base + 0.22)
        breach_prob = _clamp(breach_base + 0.28 + (0.08 if evidence_state == "bounded_event" else 0.0))
        path_bias = "downside follow-through" if direction == "negative" else "reversal" if direction == "positive" else "break risk"
    else:
        confirm = confirm_base
        break_prob = break_base
        breach_prob = breach_base
        path_bias = "hold"
    return {
        "path_bias": path_bias,
        "confirm_probability": _probability_label(confirm),
        "break_probability": _probability_label(break_prob),
        "threshold_breach_risk": _risk_label(breach_prob),
        "uncertainty_width": str(profile["uncertainty_width"]),
        "persistence_vs_reversion": str(profile["persistence_vs_reversion"]),
        "confirm_score": confirm,
        "break_score": break_prob,
        "breach_score": breach_prob,
    }


def _family_scenario_shape(bucket: str, profile: dict[str, Any]) -> dict[str, float]:
    fact_inputs = dict(profile.get("fact_inputs") or {})
    confirmation = float(profile.get("cross_asset_confirmation_score") or 0.0)
    trigger_pressure = float(profile.get("trigger_pressure") or 0.0)
    contradiction = float(profile.get("external_contradiction_score") or 0.0)
    broadening = bool(fact_inputs.get("broadening_into_regime"))
    isolated = bool(fact_inputs.get("isolated_signal"))
    base_boost = 0.0
    directional_push = 0.0
    if bucket == "duration":
        directional_push = 0.14 if confirmation >= 0.56 else 0.06
        base_boost = 0.06 if contradiction >= 0.38 else 0.0
    elif bucket == "credit":
        directional_push = 0.16 if broadening and trigger_pressure >= 0.48 else 0.08
        base_boost = 0.08 if isolated else 0.02
    elif bucket == "dollar_fx":
        directional_push = 0.12 if confirmation >= 0.52 else 0.06
        base_boost = 0.08 if contradiction >= 0.34 else 0.02
    elif bucket in {"growth", "market", "volatility"}:
        directional_push = 0.15 if broadening else 0.05
        base_boost = 0.1 if isolated else 0.04
    elif bucket in {"energy", "real_assets", "inflation"}:
        directional_push = 0.11 if confirmation >= 0.5 else 0.05
        base_boost = 0.08 if contradiction >= 0.34 else 0.03
    elif bucket == "policy" or str(profile.get("source_class") or "") in {"policy_event", "geopolitical_news"}:
        directional_push = 0.05 if broadening else 0.02
        base_boost = 0.18 if isolated else 0.1
    return {"base_boost": base_boost, "directional_push": directional_push}


def _normalized_support_strength(value: Any) -> str:
    text = str(value or "").strip().lower()
    if "weak" in text:
        return "weak"
    if "strong" in text:
        return "strong"
    if "moderate" in text or "usable" in text:
        return "moderate"
    if "bounded" in text:
        return "bounded"
    return "bounded"


def _scenario_likelihoods(profile: dict[str, Any]) -> dict[str, int] | None:
    strength = _normalized_support_strength(profile.get("support_strength"))
    if str(profile.get("degraded_state") or "").strip():
        return None
    fact_inputs = dict(profile.get("fact_inputs") or {})
    history_points = int(fact_inputs.get("history_points") or 0)
    external_confirmation = float(profile.get("external_confirmation_score") or fact_inputs.get("external_confirmation_score") or 0.0)
    confirmation = float(profile.get("cross_asset_confirmation_score") or 0.0)
    sparse_penalty = float(profile.get("sparse_history_penalty") or fact_inputs.get("sparse_history_penalty") or 0.0)
    if history_points < 2:
        return None
    if strength == "weak":
        return None
    if str(profile.get("evidence_state") or "") == "bounded_event" and external_confirmation < 0.38:
        return None
    if history_points < 3 and max(external_confirmation, confirmation) < 0.34:
        return None
    if history_points < 4 and strength == "bounded" and max(external_confirmation, confirmation) < 0.32 and sparse_penalty >= 0.3:
        return None

    bull_metrics = _scenario_case_metrics(profile, scenario_type="bull")
    base_metrics = _scenario_case_metrics(profile, scenario_type="base")
    bear_metrics = _scenario_case_metrics(profile, scenario_type="bear")
    confirm = float(profile.get("confirm_base") or 0.0)
    break_base = float(profile.get("break_base") or 0.0)
    breach = float(profile.get("threshold_breach_base") or 0.0)
    asym = float(profile.get("path_asymmetry_score") or 0.0)
    trigger_pressure = float(profile.get("trigger_pressure") or 0.0)
    regime = float(profile.get("regime_alignment_score") or 0.0)
    contradiction = float(profile.get("external_contradiction_score") or fact_inputs.get("related_market_contradiction_score") or 0.0)
    if history_points < 8 and strength == "bounded" and external_confirmation < 0.28 and confirmation < 0.28:
        return None
    direction = str(profile.get("direction") or "mixed").strip().lower()
    width = str(profile.get("uncertainty_width") or "bounded").strip().lower()
    width_penalty = {"tight": 0.03, "moderate": 0.08, "wide": 0.16, "bounded": 0.2}.get(width, 0.16)
    family_shape = _family_scenario_shape(str(profile.get("bucket") or "market"), profile)

    bull = (
        1.0
        + (float(bull_metrics.get("confirm_score") or confirm) * 0.72)
        + (max(asym, 0.0) * 0.52)
        + (regime * 0.22)
        + (confirmation * 0.18)
        + (external_confirmation * 0.18)
        - (contradiction * 0.16)
        - width_penalty
        - sparse_penalty
    )
    bear = (
        1.0
        + (float(bear_metrics.get("break_score") or break_base) * 0.62)
        + (float(bear_metrics.get("breach_score") or breach) * 0.3)
        + (max(-asym, 0.0) * 0.52)
        + (max(trigger_pressure - 0.45, 0.0) * 0.32)
        + (contradiction * 0.24)
        - (external_confirmation * 0.14)
        - (sparse_penalty * 0.6)
    )
    base = (
        1.12
        + ((1.0 - abs(asym)) * 0.42)
        + ((1.0 - abs(confirm - break_base)) * 0.28)
        + (0.26 if width in {"wide", "bounded"} else 0.12)
        + (0.16 if 0.28 <= trigger_pressure <= 0.76 else 0.04)
        + (sparse_penalty * 0.82)
        + width_penalty
    )

    if direction == "positive":
        bull += 0.08
        bear -= 0.05
    elif direction == "negative":
        bear += 0.08
        bull -= 0.05

    if asym >= 0:
        bull += family_shape["directional_push"]
        bear -= family_shape["directional_push"] * 0.42
    else:
        bear += family_shape["directional_push"]
        bull -= family_shape["directional_push"] * 0.42
    base += family_shape["base_boost"]

    weights = {
        "bull": max(bull, 0.4),
        "base": max(base, 0.4),
        "bear": max(bear, 0.4),
    }
    total = sum(weights.values()) or 1.0
    scaled = {key: (value / total) * 100.0 for key, value in weights.items()}
    widen_factor = {
        "strong": 0.12,
        "moderate": 0.22,
        "bounded": 0.38,
    }.get(strength, 0.48)
    widen_factor = min(0.72, max(0.08, widen_factor + (width_penalty * 0.75) + (sparse_penalty * 0.9) - (external_confirmation * 0.12)))
    widened = {key: (scaled[key] * (1.0 - widen_factor)) + (33.3333 * widen_factor) for key in ("bull", "base", "bear")}
    rounded = {key: int(value) for key, value in widened.items()}
    remainder = 100 - sum(rounded.values())
    ranked_remainders = sorted(
        ((widened[key] - rounded[key], key) for key in ("bull", "base", "bear")),
        key=lambda item: item[0],
        reverse=True,
    )
    for _, key in ranked_remainders[: max(remainder, 0)]:
        rounded[key] += 1
    if remainder < 0:
        for _, key in reversed(ranked_remainders[: abs(remainder)]):
            rounded[key] = max(0, rounded[key] - 1)
    return rounded


def _scenario_path_meaning(
    *,
    scenario_type: str,
    profile: dict[str, Any],
    metrics: dict[str, Any],
    expected_path: str,
) -> str:
    evidence_state = str(profile["evidence_state"])
    if evidence_state == "bounded_event":
        if scenario_type == "bull":
            return "Needs another round of price confirmation before it can change positioning."
        if scenario_type == "bear":
            return "Without follow-through, the event slips back into background context."
        return "The headline is still only partly reflected in prices."
    path = _clean_expected_path_text(expected_path)
    if scenario_type == "bull":
        base = "Follow-through stays more likely than an immediate reversal."
    elif scenario_type == "bear":
        base = "Break risk rises if the next watch line gives way."
    else:
        base = "The current read still fits the forecast band."
    if path:
        return f"{base} {path}."
    return base


def _scenario_lead_sentence(
    *,
    scenario_type: str,
    bucket: str,
    profile: dict[str, Any],
) -> str:
    source_class = str(profile["source_class"])
    evidence_state = str(profile["evidence_state"])
    if source_class in {"policy_event", "geopolitical_news"}:
        if evidence_state == "bounded_event":
            if scenario_type == "bull":
                return "This only becomes actionable if market confirmation broadens."
            if scenario_type == "bear":
                return "If confirmation fades, keep it in backdrop context."
            return "For now the event stays a monitored risk, not a portfolio instruction."
        if scenario_type == "bull":
            return "The event stays actionable only if confirmation keeps broadening."
        if scenario_type == "bear":
            return "A loss of confirmation would quickly weaken the event case."
        return "The event remains active because prices are still confirming it."
    return _SCENARIO_LEAD_TEXT.get(bucket, _SCENARIO_LEAD_TEXT["market"]).get(scenario_type, "The current read remains in play.")


def _scenario_macro_line(
    *,
    scenario_type: str,
    bucket: str,
    profile: dict[str, Any],
    base: str,
) -> str | None:
    source_class = str(profile["source_class"])
    evidence_state = str(profile["evidence_state"])
    if source_class in {"policy_event", "geopolitical_news"}:
        if evidence_state == "bounded_event":
            if scenario_type == "base":
                return None
            return "Macro spillover still depends on broader market confirmation."
        if scenario_type == "bull":
            return "Macro spillover stays contained while confirmation broadens."
        if scenario_type == "bear":
            return "Macro spillover matters only if the event keeps transmitting into prices."
        return "Macro spillover is active, but still tied to ongoing confirmation."
    fallback = _SCENARIO_MACRO_BY_BUCKET.get(bucket, _SCENARIO_MACRO_BY_BUCKET["market"]).get(scenario_type)
    if base and scenario_type == "base":
        return _compact_text(base, max_words=14, max_chars=120)
    return fallback


def _scenario_micro_line(
    *,
    scenario_type: str,
    bucket: str,
    profile: dict[str, Any],
    base: str,
) -> str | None:
    source_class = str(profile["source_class"])
    evidence_state = str(profile["evidence_state"])
    if source_class in {"policy_event", "geopolitical_news"}:
        if evidence_state == "bounded_event":
            if scenario_type == "bull":
                return "Keep the sleeve response on watch until cross-market confirmation improves."
            if scenario_type == "bear":
                return "If confirmation fades, leave positioning unchanged."
            return "No sleeve change is justified on headline alone."
        if scenario_type == "bull":
            return "Let sleeve action depend on confirmation across oil, FX, rates, or volatility."
        if scenario_type == "bear":
            return "Stand down quickly if the confirming price moves fade."
        return "Keep sleeve action tied to continued market confirmation."
    fallback = _SCENARIO_MICRO_BY_BUCKET.get(bucket, _SCENARIO_MICRO_BY_BUCKET["market"]).get(scenario_type)
    if base and scenario_type == "base":
        return _compact_text(base, max_words=14, max_chars=120) or fallback
    return fallback


def _scenario_short_line(
    *,
    scenario_type: str,
    profile: dict[str, Any],
) -> str | None:
    risk = str(profile["threshold_breach_risk"])
    trigger_state = str(profile["threshold_state"])
    pressure = float(profile.get("trigger_pressure") or 0.0)
    if scenario_type == "base":
        if pressure < 0.34 and risk == "low" and trigger_state == "watch":
            return "Near term, the watch line is active but path pressure is still low."
        if pressure >= 0.66:
            return f"Near term, the watch line is drawing rising pressure with {risk} breach risk."
        return f"Near term, the watch line stays active with {risk} breach risk."
    if scenario_type == "bull":
        if pressure >= 0.62:
            return f"Near term, the path is leaning toward the watch line while breach risk stays {risk}."
        return f"Near term, breach risk stays {risk} if the current band holds."
    if pressure < 0.34:
        return "Near term, the break path still needs more pressure before it can force review."
    return "Near term, a trigger failure would force faster review."


def _scenario_long_line(
    *,
    scenario_type: str,
    profile: dict[str, Any],
) -> str | None:
    persistence = str(profile["persistence_vs_reversion"])
    regime_alignment_score = float(profile.get("regime_alignment_score") or 0.0)
    cross_asset_confirmation_score = float(profile.get("cross_asset_confirmation_score") or 0.0)
    if scenario_type == "base":
        if regime_alignment_score >= 0.68 and cross_asset_confirmation_score >= 0.62:
            return "Across related markets, the current path still looks coherent enough to keep the brief intact."
        return None
    if scenario_type == "bull":
        if persistence == "durable path support":
            return "If that persistence holds, the current brief can move from watch to firmer conviction."
        if regime_alignment_score >= 0.66:
            return "If related markets keep confirming the move, the current brief can strengthen without forcing early action."
        return None
    return "If the break persists, the current brief would need a more defensive reset."


def _scenario_action_consequence(
    *,
    scenario_type: str,
    bucket: str,
    profile: dict[str, Any],
    next_action: str,
    portfolio_consequence: str,
) -> str:
    source_class = str(profile["source_class"])
    evidence_state = str(profile["evidence_state"])
    if source_class in {"policy_event", "geopolitical_news"}:
        if evidence_state == "bounded_event":
            if scenario_type == "bull":
                return "Stay in monitor mode until the signal confirms across markets."
            if scenario_type == "bear":
                return "Do not change positioning unless a new price break reactivates the event."
            return "No portfolio action yet; keep the event on watch."
        if scenario_type == "bull":
            return "Prepare a sleeve review only if the confirming price moves persist."
        if scenario_type == "bear":
            return "Step back quickly if the confirmation channel fails."
        return "Keep action tied to continued confirmation rather than the headline alone."
    action_map = {
        "duration": {
            "bull": "Prepare staged duration adds if follow-through holds.",
            "base": "Keep current bond timing; no forced move.",
            "bear": "Escalate bond review and delay adds.",
        },
        "inflation": {
            "bull": "Keep the hedge mix balanced without forcing more protection.",
            "base": "Stay patient on duration and keep hedges alive.",
            "bear": "Lean harder on hedges and delay duration adds.",
        },
        "credit": {
            "bull": "Allow carry to hold without tightening the risk budget.",
            "base": "Keep the risk budget selective, not aggressive.",
            "bear": "Tighten the risk budget and recheck credit exposure.",
        },
        "policy": {
            "bull": "Hold the current bond and cash stance.",
            "base": "Keep timing anchored to the current policy window.",
            "bear": "Reopen rate and cash review if repricing persists.",
        },
        "dollar_fx": {
            "bull": "Hold global risk sizing steady if the hurdle eases.",
            "base": "Keep ex-US sizing selective.",
            "bear": "Trim risk appetite if the dollar hurdle hardens.",
        },
        "energy": {
            "bull": "Let hedge urgency drift lower toward watch mode.",
            "base": "Keep protection relevant but measured.",
            "bear": "Reopen real-assets protection if the spike extends.",
        },
        "real_assets": {
            "bull": "Keep the hedge sleeve in support mode, not escalation mode.",
            "base": "Hold the current hedge role without adding urgency.",
            "bear": "Reduce hedge conviction if the supporting path fades.",
        },
        "volatility": {
            "bull": "Keep risk posture calmer without forcing de-risking.",
            "base": "Maintain guardrails but do not overreact.",
            "bear": "Raise cash or trim risk if volatility spills further.",
        },
        "growth": {
            "bull": "Keep equity risk engaged if breadth improves.",
            "base": "Keep equity adds selective and leadership-aware.",
            "bear": "Reduce enthusiasm for equity risk if breadth weakens further.",
        },
        "liquidity": {
            "bull": "Keep liquidity-sensitive risk on watch rather than review.",
            "base": "Stay selective while liquidity remains bounded.",
            "bear": "Shrink risk appetite if liquidity stress widens.",
        },
    }
    fallback = {
        "bull": f"Keep the current read intact while {next_action.lower()} stays sufficient.",
        "base": _compact_text(portfolio_consequence, max_words=14, max_chars=120) or "Keep the current portfolio consequence in place.",
        "bear": "Escalate review if the break path confirms.",
    }
    return action_map.get(bucket, fallback).get(scenario_type, fallback[scenario_type])


def _scenario_trigger_state(
    *,
    scenario_type: str,
    profile: dict[str, Any],
) -> str:
    if str(profile["evidence_state"]) == "bounded_event":
        if scenario_type == "bull":
            return "Needs broader cross-market confirmation."
        if scenario_type == "bear":
            return "Falls back to backdrop if confirmation fades."
        return "Watch the next session for follow-through."
    near_term = dict(profile.get("near_term_trigger") or {})
    thesis = dict(profile.get("thesis_trigger") or {})
    threshold_state = str(profile["threshold_state"])
    trigger_pressure = float(profile.get("trigger_pressure") or 0.0)
    cross_asset_confirmation_score = float(profile.get("cross_asset_confirmation_score") or 0.0)
    near_type = str(near_term.get("trigger_type") or "near term").replace("_", " ")
    if scenario_type == "bear":
        if threshold_state == "breached" and cross_asset_confirmation_score < 0.4:
            return f"{near_type.title()} watch has broken, but broader confirmation is still thin."
        return f"{near_type.title()} watch is closest to breaking; thesis path weakens next."
    if scenario_type == "bull":
        if trigger_pressure >= 0.68:
            return f"{near_type.title()} watch is under rising pressure while the thesis path improves."
        return f"{near_type.title()} watch still holds; thesis path can improve without a break."
    if thesis:
        thesis_type = str(thesis.get("trigger_type") or "thesis").replace("_", " ")
        if trigger_pressure < 0.34:
            return f"{near_type.title()} remains in {threshold_state} mode with low path pressure while {thesis_type} stays intact."
        return f"{near_type.title()} remains in {threshold_state} mode while {thesis_type} stays intact."
    return f"{near_type.title()} remains in {threshold_state} mode."


def _scenario_regime_note(*, profile: dict[str, Any], scenario_type: str) -> str | None:
    regime_alignment_score = float(profile.get("regime_alignment_score") or 0.0)
    cross_asset_confirmation_score = float(profile.get("cross_asset_confirmation_score") or 0.0)
    fact_inputs = dict(profile.get("fact_inputs") or {})
    related_notes = list(fact_inputs.get("related_market_notes") or [])
    if scenario_type == "bull":
        if regime_alignment_score >= 0.72 and cross_asset_confirmation_score >= 0.66:
            return related_notes[0].capitalize() + "." if related_notes else "Related markets are already broad enough to support the constructive path."
        if regime_alignment_score >= 0.56 and cross_asset_confirmation_score >= 0.46:
            return "The constructive path has partial regime backing, but it still needs broader cross-asset help."
        return "The constructive path still lacks enough regime breadth to become a one-way view."
    if scenario_type == "base":
        if regime_alignment_score >= 0.72 and cross_asset_confirmation_score >= 0.66:
            return related_notes[0].capitalize() + "." if related_notes else "The current regime read is coherent enough to keep the most likely path intact."
        if regime_alignment_score >= 0.56 and cross_asset_confirmation_score >= 0.46:
            return "The current regime read is only partly aligned, which keeps the most likely path conditional."
        return "The regime read is still mixed enough to keep the most likely path provisional."
    if regime_alignment_score >= 0.72 and cross_asset_confirmation_score >= 0.66:
        return "If that broad confirmation starts to fail, the adverse path becomes materially more credible."
    if regime_alignment_score >= 0.56 and cross_asset_confirmation_score >= 0.46:
        return "The adverse path still needs more regime deterioration before it can dominate."
    return "The adverse path is credible, but it still needs broader cross-asset deterioration to take over."


def _dedupe_scenario_sections(variant: dict[str, Any]) -> dict[str, Any]:
    lead = str(variant.get("lead_sentence") or "")
    action = str(variant.get("action_consequence") or "")
    for field in ("macro", "micro", "short_term", "long_term"):
        value = str(variant.get(field) or "").strip()
        if not value:
            variant[field] = None
            continue
        if _similarity(value, lead) >= 0.72 or _similarity(value, action) >= 0.72:
            variant[field] = None
        else:
            variant[field] = _compact_text(value, max_words=18, max_chars=150)
    if variant.get("macro") and variant.get("micro") and _similarity(variant["macro"], variant["micro"]) >= 0.68:
        variant["micro"] = None
    if variant.get("short_term") and variant.get("long_term") and _similarity(variant["short_term"], variant["long_term"]) >= 0.68:
        variant["long_term"] = None
    return variant


def _dedupe_scenario_variants(variants: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for index in range(1, len(variants)):
        current = variants[index]
        prior = variants[index - 1]
        current_signature = " ".join(
            [
                str(current.get("lead_sentence") or ""),
                str(current.get("action_consequence") or ""),
                str(current.get("path_meaning") or ""),
            ]
        )
        prior_signature = " ".join(
            [
                str(prior.get("lead_sentence") or ""),
                str(prior.get("action_consequence") or ""),
                str(prior.get("path_meaning") or ""),
            ]
        )
        if _similarity(current_signature, prior_signature) >= 0.68:
            current["macro"] = None
            current["micro"] = None
            current["short_term"] = None
            current["long_term"] = None
            current["path_meaning"] = _compact_text(current.get("path_meaning"), max_words=12, max_chars=96)
    return variants


def _scenario_variant(
    *,
    scenario_type: str,
    case_data: dict[str, Any],
    profile: dict[str, Any],
    signal_card: dict[str, Any],
    macro_reason: str,
    micro_reason: str,
    short_term_reason: str,
    long_term_reason: str,
    portfolio_consequence: str,
    next_action: str,
) -> dict[str, Any]:
    bucket = _bucket_key(signal_card)
    metrics = _scenario_case_metrics(profile, scenario_type=scenario_type)
    scenario_likelihoods = _scenario_likelihoods(profile)
    variant = {
        "scenario_id": f"{signal_card.get('signal_id') or 'signal'}_{scenario_type}",
        "type": scenario_type,
        "label": str(case_data.get("label") or f"{scenario_type.title()} case"),
        "scenario_name": scenario_type.title(),
        "lead_sentence": _scenario_lead_sentence(
            scenario_type=scenario_type,
            bucket=bucket,
            profile=profile,
        ),
        "portfolio_effect": _compact_text(
            str(case_data.get("summary") or portfolio_consequence or ""),
            max_words=14,
            max_chars=120,
        )
        or portfolio_consequence,
        "action_consequence": _scenario_action_consequence(
            scenario_type=scenario_type,
            bucket=bucket,
            profile=profile,
            next_action=next_action,
            portfolio_consequence=portfolio_consequence,
        ),
        "path_statement": _scenario_path_statement(
            scenario_type=scenario_type,
            profile=profile,
            metrics=metrics,
            signal_card=signal_card,
            case_data=case_data,
        ),
        "timing_window": _scenario_timing_window(
            scenario_type=scenario_type,
            profile=profile,
        ),
        "scenario_likelihood_pct": scenario_likelihoods.get(scenario_type) if scenario_likelihoods else None,
        "sleeve_consequence": _scenario_sleeve_consequence(
            scenario_type=scenario_type,
            bucket=bucket,
            profile=profile,
            micro_reason=micro_reason,
            action_consequence=_scenario_action_consequence(
                scenario_type=scenario_type,
                bucket=bucket,
                profile=profile,
                next_action=next_action,
                portfolio_consequence=portfolio_consequence,
            ),
        ),
        "action_boundary": _scenario_action_boundary(
            scenario_type=scenario_type,
            bucket=bucket,
            profile=profile,
        ),
        "upgrade_trigger": _scenario_upgrade_trigger(
            scenario_type=scenario_type,
            profile=profile,
        ),
        "downgrade_trigger": _scenario_downgrade_trigger(
            scenario_type=scenario_type,
            profile=profile,
        ),
        "support_strength": _scenario_support_strength_label(
            scenario_type=scenario_type,
            profile=profile,
            metrics=metrics,
        ),
        "regime_note": _scenario_regime_note(
            profile=profile,
            scenario_type=scenario_type,
        ),
        "confirmation_note": _scenario_confirmation_note(
            scenario_type=scenario_type,
            bucket=bucket,
            profile=profile,
        ),
        "path_meaning": _scenario_path_meaning(
            scenario_type=scenario_type,
            profile=profile,
            metrics=metrics,
            expected_path=str(case_data.get("expected_path") or ""),
        ),
        "trigger_state": _scenario_trigger_state(
            scenario_type=scenario_type,
            profile=profile,
        ),
        "path_bias": metrics["path_bias"],
        "confirm_probability": metrics["confirm_probability"],
        "break_probability": metrics["break_probability"],
        "threshold_breach_risk": metrics["threshold_breach_risk"],
        "uncertainty_width": metrics["uncertainty_width"],
        "persistence_vs_reversion": metrics["persistence_vs_reversion"],
        "evidence_state": str(profile["evidence_state"]),
        "macro": _scenario_macro_line(
            scenario_type=scenario_type,
            bucket=bucket,
            profile=profile,
            base=macro_reason,
        ),
        "micro": _scenario_micro_line(
            scenario_type=scenario_type,
            bucket=bucket,
            profile=profile,
            base=micro_reason,
        ),
        "short_term": _scenario_short_line(
            scenario_type=scenario_type,
            profile={**profile, **metrics},
        ),
        "long_term": _scenario_long_line(
            scenario_type=scenario_type,
            profile={**profile, **metrics},
        ),
    }
    return _dedupe_scenario_sections(variant)


def _scenario_path_statement(
    *,
    scenario_type: str,
    profile: dict[str, Any],
    metrics: dict[str, Any],
    signal_card: dict[str, Any],
    case_data: dict[str, Any],
) -> str:
    expected = _clean_expected_path_text(str(case_data.get("expected_path") or "").strip())
    persistence = str(metrics.get("persistence_vs_reversion") or "two-way path")
    breach_risk = str(metrics.get("threshold_breach_risk") or "moderate")
    confirmation = float(profile.get("cross_asset_confirmation_score") or 0.0)
    pressure = float(profile.get("trigger_pressure") or 0.0)
    evidence_state = str(profile.get("evidence_state") or "")
    external_confirmation = float(profile.get("external_confirmation_score") or 0.0)
    contradiction = float(profile.get("external_contradiction_score") or 0.0)
    bucket = str(profile.get("bucket") or _bucket_key(signal_card))
    if evidence_state == "bounded_event":
        if scenario_type == "bull":
            return "The constructive path needs another round of price confirmation over the next few sessions before it changes the brief."
        if scenario_type == "bear":
            return "The adverse path only becomes durable if the event starts transmitting beyond the headline and into related markets."
        return "The most likely path is still a monitored event state rather than a durable market regime shift."
    confirmation_clause = (
        "related markets are already confirming the move"
        if confirmation >= 0.66
        else "related markets are only partly confirming the move"
        if confirmation >= 0.42
        else "related markets are still resisting the move"
    )
    pressure_clause = (
        "trigger pressure is already rising"
        if pressure >= 0.66
        else "trigger pressure is still limited"
        if pressure <= 0.33
        else "trigger pressure is building"
    )
    persistence_clause = {
        "durable path support": "follow-through still looks durable",
        "threshold pressure rising": "the market is leaning toward a harder test",
        "fragile path": "the move can still fade quickly",
        "two-way path": "the move is still not one-way",
    }.get(persistence, "the move is still not one-way")
    external_clause = (
        "external confirmation is still light"
        if external_confirmation < 0.42
        else "external confirmation is usable but not broad"
        if external_confirmation < 0.62
        else "external confirmation is broad enough to keep the path credible"
    )
    if contradiction >= 0.46:
        external_clause = "some related markets are still pushing against the move"

    variant_channels = {
        "duration": {
            "bull": "Rates stop tightening further and credit stops worsening, so the duration hurdle stays high but does not become more binding.",
            "base": "Rates stay near current levels, which keeps duration adds patient while credit and valuation-sensitive assets still face the same hurdle.",
            "bear": "Rates re-tighten while credit and equity valuations lose relief, turning the duration hurdle into a broader cross-asset headwind.",
        },
        "credit": {
            "bull": "Spreads stop widening and breadth steadies, so funding stress stays contained instead of broadening into a larger risk-budget shock.",
            "base": "Spreads stay near current levels, leaving the market cautious but not yet in a fresh funding squeeze.",
            "bear": "Spreads widen again while breadth and duration fail to cushion the move, turning funding stress into a broader risk-budget problem.",
        },
        "dollar_fx": {
            "bull": "Dollar pressure eases enough for ex-US risk and commodity-sensitive assets to stop trading as if the global hurdle is tightening.",
            "base": "The dollar stays firm enough to keep the global hurdle high, but not so strong that it forces a full cross-border reset.",
            "bear": "The dollar reasserts itself through real yields and ex-US weakness, tightening the hurdle for global risk again.",
        },
        "growth": {
            "bull": "Breadth and leadership broaden enough for the rally to look like a genuine risk-on expansion rather than a narrow tape move.",
            "base": "The rally holds, but breadth and leadership stay only partly convincing, so the market remains selective rather than broadly risk-on.",
            "bear": "Breadth rolls over, leadership narrows, and cross-asset resistance returns, turning the rally back into a fragile headline move.",
        },
        "market": {
            "bull": "The move broadens across the main risk channels rather than staying isolated to the headline tape.",
            "base": "The move holds, but the market still looks selective rather than broadly committed to the same direction.",
            "bear": "The move loses sponsorship across related markets and starts slipping back toward a failed break.",
        },
        "energy": {
            "bull": "Energy pressure cools before it turns into a wider inflation or hedge-demand impulse.",
            "base": "Energy stays firm enough to keep hedge demand alive, but not strong enough to create a fresh inflation shock by itself.",
            "bear": "Energy pressure extends while inflation-sensitive and hedge-sensitive markets keep reinforcing it, turning the move into a broader macro problem.",
        },
        "real_assets": {
            "bull": "Hedge demand stays constructive without turning disorderly, so the sleeve keeps support without needing escalation.",
            "base": "Hedge demand stays relevant, but it still relies on inflation, the dollar, and bond-offset logic to keep confirming it.",
            "bear": "Hedge demand fades and the confirming markets stop lining up, which would weaken the real-assets case quickly.",
        },
        "inflation": {
            "bull": "Inflation pressure cools enough for rates and hedges to stop pressing the same constraint on duration.",
            "base": "Inflation stays sticky enough to keep duration patient, but it does not yet broaden into a harder inflation surprise.",
            "bear": "Inflation re-accelerates across related pricing channels, which would reset the bond-versus-hedge decision more aggressively.",
        },
        "policy": {
            "bull": "Policy spillover stays contained and the main exposed markets stop repricing a more restrictive path.",
            "base": "Policy remains a live timing input, but the market is still waiting for clearer confirmation across rates, FX, and risk appetite.",
            "bear": "Policy repricing spreads across rates, FX, and risk appetite, turning a policy question into a broader market hurdle.",
        },
        "volatility": {
            "bull": "Stress eases before it forces broader de-risking, so defensive demand can cool without breaking the brief.",
            "base": "Stress stays elevated but contained, which keeps guardrails in place without forcing a full defensive reset.",
            "bear": "Stress broadens across breadth, carry, and defensive rates, turning the move into a more serious regime challenge.",
        },
    }
    branch = variant_channels.get(bucket, variant_channels["market"]).get(scenario_type)
    parts = [branch]
    if scenario_type == "bull":
        if expected and _similarity(branch, expected) < 0.58:
            parts.append(f"That case improves further if {expected.lower()}.")
        parts.append(f"It broadens only when {confirmation_clause}.")
        parts.append(f"It still stops short of a stronger call while {external_clause} and {persistence_clause}.")
    elif scenario_type == "base":
        if expected and _similarity(branch, expected) < 0.58:
            parts.append(f"The current range still fits {expected.lower()}.")
        parts.append(f"It stays conditional because {confirmation_clause}, {pressure_clause}, and {persistence_clause}.")
        parts.append("It shifts toward bull if confirmation broadens and toward bear if resistance returns.")
    else:
        if expected and _similarity(branch, expected) < 0.58:
            parts.append(f"The risk increases if {expected.lower()} fails to hold.")
        parts.append(f"It gains weight when {pressure_clause} and {confirmation_clause}.")
        if breach_risk == "high":
            parts.append(f"If that happens, the current read becomes harder to defend because {external_clause}.")
        else:
            parts.append(f"It is still not dominant while {external_clause}.")
    return " ".join(parts)


def _scenario_timing_window(*, scenario_type: str, profile: dict[str, Any]) -> str:
    source_class = str(profile.get("source_class") or "market_series")
    horizon_days = int(profile.get("horizon_days") or 0)
    if source_class == "macro_release":
        return "through the next official release window"
    if source_class in {"policy_event", "geopolitical_news"}:
        return "current brief window"
    if horizon_days >= 8:
        return f"next {horizon_days} trading days"
    if scenario_type == "base":
        return "next few sessions"
    return "next one to two sessions"


def _scenario_confirmation_note(*, scenario_type: str, bucket: str, profile: dict[str, Any]) -> str | None:
    fact_inputs = dict(profile.get("fact_inputs") or {})
    source_class = str(profile.get("source_class") or "market_series")
    market_confirmation_state = str(fact_inputs.get("market_confirmation_state") or "limited").replace("_", " ")
    related_notes = list(fact_inputs.get("related_market_notes") or [])
    if source_class in {"policy_event", "geopolitical_news"}:
        if scenario_type == "bear":
            return "If spillover into rates, oil, volatility, or breadth fades, treat the event as monitored context rather than a portfolio fact."
        if scenario_type == "bull":
            return f"The constructive case confirms only if market confirmation improves from {market_confirmation_state} into a broader price response across rates, oil, volatility, or breadth."
        return f"The central case holds while market confirmation stays {market_confirmation_state} and the event does not yet broaden into a fully priced market channel."
    mapping = {
        "duration": {
            "bull": "The constructive case confirms if real yields, credit, and valuation-sensitive equities all keep respecting the same rate hurdle.",
            "base": "The central case holds while rates stay firm and credit or equity valuations do not reopen easily.",
            "bear": "The adverse case confirms if rates re-tighten while credit and equity multiples lose relief together.",
        },
        "inflation": {
            "bull": "The constructive case confirms if inflation-sensitive assets and rates stop reinforcing the inflation impulse.",
            "base": "The central case holds while inflation, rates, and hedge demand keep the same constraint in place without broadening further.",
            "bear": "The adverse case confirms if inflation-sensitive assets, rates, and hedges all reinforce a fresh inflation reacceleration.",
        },
        "credit": {
            "bull": "The constructive case confirms if spreads stop widening, breadth steadies, and duration stops echoing tighter funding conditions.",
            "base": "The central case holds while spreads stay elevated and breadth does not reopen risk appetite.",
            "bear": "The adverse case confirms if spreads widen again while breadth narrows and duration fails to offset the funding stress.",
        },
        "dollar_fx": {
            "bull": "The constructive case confirms if real yields, ex-US risk, and commodity-sensitive assets all stop trading as if the dollar hurdle is tightening.",
            "base": "The central case holds while the dollar remains firm enough to cap global risk without forcing a full reset.",
            "bear": "The adverse case confirms if real-yield pressure, ex-US weakness, and commodity spillover all reinforce the dollar path.",
        },
        "growth": {
            "bull": "The constructive case confirms if breadth broadens, leadership improves, and credit, rates, and FX stop resisting the rally.",
            "base": "The central case holds while the rally stays alive but breadth, credit, rates, and FX only partly support it.",
            "bear": "The adverse case confirms if breadth narrows again and cross-asset resistance reappears through credit, rates, or FX.",
        },
        "market": {
            "bull": "The constructive case confirms if breadth, credit, rates, and FX all reinforce the move rather than offsetting it.",
            "base": "The central case holds while the move is partly confirmed but still selective.",
            "bear": "The adverse case confirms if related markets stop reinforcing the move and start resisting it together.",
        },
        "volatility": {
            "bull": "The constructive case confirms if breadth, carry, and defensive-rate demand all calm at the same time.",
            "base": "The central case holds while volatility stays elevated but does not force a wider cross-asset break.",
            "bear": "The adverse case confirms if breadth, carry, and defensive rates all move as if the stress is broadening.",
        },
        "energy": {
            "bull": "The constructive case confirms if inflation-sensitive assets, the dollar, and hedge demand stop reinforcing the energy move.",
            "base": "The central case holds while the energy move stays firm enough to matter, but not broad enough to reset the whole macro read.",
            "bear": "The adverse case confirms if inflation-sensitive assets, the dollar, and hedge demand all reinforce the energy shock.",
        },
        "real_assets": {
            "bull": "The constructive case confirms if hedge demand stays orderly and bond-offset or dollar channels stop pushing the move harder.",
            "base": "The central case holds while hedge demand remains relevant but still depends on support from the dollar and bond-offset logic.",
            "bear": "The adverse case confirms if hedge demand fades and the confirming cross-asset channels stop lining up.",
        },
        "policy": {
            "bull": f"The constructive case confirms only if market confirmation improves from {market_confirmation_state} and rates, FX, and risk appetite stop repricing a harder policy path.",
            "base": f"The central case holds while market confirmation stays {market_confirmation_state} and the policy read remains only partly transmitted into prices.",
            "bear": f"The adverse case confirms if market confirmation improves from {market_confirmation_state} into a broader repricing across rates, FX, and risk appetite.",
        },
    }
    base = mapping.get(bucket, {}).get(
        scenario_type,
        "The scenario confirms only if related markets keep reinforcing the same path instead of offsetting it.",
    )
    if related_notes:
        return f"{base} Right now {related_notes[0]}."
    return base


def _scenario_sleeve_consequence(
    *,
    scenario_type: str,
    bucket: str,
    profile: dict[str, Any],
    micro_reason: str,
    action_consequence: str,
) -> str:
    source_class = str(profile.get("source_class") or "market_series")
    if source_class in {"policy_event", "geopolitical_news"}:
        if scenario_type == "bull":
            return "A sleeve-level response becomes more credible, but only if cross-market confirmation broadens."
        if scenario_type == "bear":
            return "Keep the portfolio consequence bounded and let the event fall back toward context."
        return "Keep the sleeve consequence in monitor mode until the event is more fully confirmed."
    if action_consequence:
        return action_consequence
    return _compact_text(micro_reason, max_words=18, max_chars=150) or _SCENARIO_MICRO_BY_BUCKET.get(bucket, _SCENARIO_MICRO_BY_BUCKET["market"]).get(scenario_type, "")


def _scenario_action_boundary(
    *,
    scenario_type: str,
    bucket: str,
    profile: dict[str, Any],
) -> str:
    source_class = str(profile.get("source_class") or "market_series")
    evidence_state = str(profile.get("evidence_state") or "")
    if source_class in {"policy_event", "geopolitical_news"} or evidence_state == "bounded_event":
        if scenario_type == "bear":
            return "Do not treat the headline alone as a portfolio fact once market confirmation fades."
        return "Do not turn this into a full portfolio move without broader price confirmation."
    if bucket == "credit":
        if scenario_type == "bull":
            return "Do not jump straight to aggressive risk adds without broader cross-asset confirmation."
        if scenario_type == "base":
            return "Treat this as a monitored restraint, not a full portfolio reset."
        return "Preserve ballast and delay risk adds until the spread move stops broadening."
    if bucket == "duration":
        return "Do not treat this alone as a full duration green light; wait for broader rate confirmation."
    if bucket in {"energy", "real_assets"}:
        return "Do not turn this into a stand-alone commodity trade call; keep it at sleeve level unless confirmation broadens."
    if bucket == "dollar_fx":
        return "Do not use this alone to force a full portfolio pivot; wait for broader risk confirmation."
    return "Do not treat this scenario alone as enough for a full portfolio reset."


def _scenario_upgrade_trigger(*, scenario_type: str, profile: dict[str, Any]) -> str | None:
    near_term = dict(profile.get("near_term_trigger") or {})
    thesis = dict(profile.get("thesis_trigger") or {})
    if scenario_type == "bear":
        return str(near_term.get("next_action_if_hit") or thesis.get("next_action_if_hit") or "").strip() or None
    return str(thesis.get("next_action_if_hit") or near_term.get("next_action_if_hit") or "").strip() or None


def _scenario_downgrade_trigger(*, scenario_type: str, profile: dict[str, Any]) -> str | None:
    near_term = dict(profile.get("near_term_trigger") or {})
    thesis = dict(profile.get("thesis_trigger") or {})
    if scenario_type == "bull":
        return str(near_term.get("next_action_if_broken") or thesis.get("next_action_if_broken") or "").strip() or None
    return str(thesis.get("next_action_if_broken") or near_term.get("next_action_if_broken") or "").strip() or None


def _scenario_support_strength_label(*, scenario_type: str, profile: dict[str, Any], metrics: dict[str, Any]) -> str:
    strength = str(profile.get("support_strength") or "available").replace("_", " ").strip().lower()
    risk = str(metrics.get("threshold_breach_risk") or "moderate").strip().lower()
    width = str(metrics.get("uncertainty_width") or "bounded").strip().lower()
    pressure = float(profile.get("trigger_pressure") or 0.0)
    confirmation = float(profile.get("cross_asset_confirmation_score") or 0.0)
    external_confirmation = float(profile.get("external_confirmation_score") or 0.0)
    contradiction = float(profile.get("external_contradiction_score") or 0.0)
    sparse_penalty = float(profile.get("sparse_history_penalty") or 0.0)
    persistence = str(metrics.get("persistence_vs_reversion") or "two-way path").strip().lower()
    strength_text = {
        "tight interval support": "This path has above-average support",
        "strong": "This path has above-average support",
        "moderate": "This path is credible but not settled",
        "support only": "This path is possible, but the evidence is still light",
        "bounded": "This path is usable, but conviction should stay bounded",
        "benchmark": "This path is directional only",
        "weak": "This path is still weak",
        "available": "This path is available",
    }.get(strength, "This path is available")
    risk_text = {
        "low": "a clean break is not the base case",
        "moderate": "a clean break remains possible",
        "high": "a clean break is becoming more plausible",
    }.get(risk, "a clean break remains possible")
    width_text = {
        "tight": "the expected range is fairly tight",
        "moderate": "the expected range is usable",
        "wide": "the expected range is still wide",
        "bounded": "the expected range is still wide",
    }.get(width, "the expected range is still wide")
    pressure_text = (
        "trigger pressure is rising"
        if pressure >= 0.66
        else "trigger pressure is still limited"
        if pressure <= 0.33
        else "trigger pressure is building"
    )
    confirmation_text = (
        "cross-asset confirmation is broad"
        if confirmation >= 0.66
        else "cross-asset confirmation is partial"
        if confirmation >= 0.42
        else "cross-asset confirmation is still light"
    )
    external_text = (
        "external confirmation is broad"
        if external_confirmation >= 0.66
        else "external confirmation is usable"
        if external_confirmation >= 0.44
        else "external confirmation is still light"
    )
    persistence_text = {
        "durable path support": "follow-through still looks durable",
        "threshold pressure rising": "the path is leaning toward a test",
        "fragile path": "the path can still fade quickly",
        "two-way path": "the path is still two-way",
    }.get(persistence, "the path is still two-way")
    extra_parts: list[str] = []
    if contradiction >= 0.42:
        extra_parts.append("some related markets are still resisting the move")
    if sparse_penalty >= 0.16:
        extra_parts.append("history is still sparse enough to keep confidence wider")
    suffix = ""
    if extra_parts:
        suffix = " " + ". ".join(part.capitalize() for part in extra_parts) + "."
    if scenario_type == "bull":
        return f"{strength_text}. Upside follow-through still needs broader confirmation because {pressure_text} and {width_text}.{suffix}"
    if scenario_type == "base":
        return f"{strength_text}. The hold path remains the center of gravity because {external_text}, {width_text}, and {persistence_text}.{suffix}"
    return f"{strength_text}. The adverse path needs wider deterioration before it takes over because {confirmation_text}, {external_text}, and {risk_text}.{suffix}"


def _scenario_block_summary(profile: dict[str, Any]) -> str:
    confirm = _probability_label(float(profile["confirm_base"]))
    break_risk = _probability_label(float(profile["break_base"]))
    uncertainty = str(profile["uncertainty_width"]).strip().lower()
    persistence = str(profile["persistence_vs_reversion"]).strip().lower()
    pressure = float(profile.get("trigger_pressure") or 0.0)
    confirmation = float(profile.get("cross_asset_confirmation_score") or 0.0)
    external_confirmation = float(profile.get("external_confirmation_score") or 0.0)
    contradiction = float(profile.get("external_contradiction_score") or 0.0)
    confirm_text = {
        "low": "the move is not yet convincing",
        "moderate": "the move could keep going",
        "high": "the move is well supported",
    }.get(confirm, "the move could keep going")
    break_text = {
        "low": "a clean reversal is not the base case",
        "moderate": "a reversal remains possible",
        "high": "a reversal risk is already meaningful",
    }.get(break_risk, "a reversal remains possible")
    uncertainty_text = {
        "tight": "the likely range is fairly tight",
        "moderate": "the likely range is usable",
        "wide": "the likely range is still wide",
        "bounded": "the likely range is still wide",
    }.get(uncertainty, "the likely range is still wide")
    persistence_text = {
        "durable path support": "the move still looks durable",
        "threshold pressure rising": "the move is nearing an important line",
        "fragile path": "the move could still fade quickly",
        "two-way path": "the move can still go either way",
    }.get(persistence, "the move can still go either way")
    confirmation_text = (
        "other markets are broadly supporting the move"
        if confirmation >= 0.66
        else "other markets are partly supporting the move"
        if confirmation >= 0.42
        else "other markets are not broadly supporting the move yet"
    )
    pressure_text = (
        "trigger pressure is rising"
        if pressure >= 0.66
        else "trigger pressure is still limited"
        if pressure <= 0.33
        else "trigger pressure is building"
    )
    resistance_text = "Some related markets are still pushing back." if contradiction >= 0.42 else ""
    confidence_text = (
        "Near-term confidence is still modest."
        if uncertainty in {"wide", "bounded"} or break_risk == "high"
        else "Near-term confidence is improving."
    )
    return _compact_text(
        " ".join(
            part
            for part in [
                f"The move is still {'not settled' if break_risk in {'moderate', 'high'} or uncertainty in {'wide', 'bounded'} else 'fairly settled'}.",
                f"Right now {confirm_text}, {persistence_text}, and {confirmation_text}.",
                resistance_text,
                confidence_text,
            ]
            if part
        ),
        max_words=40,
        max_chars=290,
    ) or "The move is still unsettled and needs more confirmation."


def _scenario_macro_text(base: str, *, scenario_type: str, path: str) -> str | None:
    text = base.strip()
    if not text:
        return None
    if scenario_type == "bull":
        return f"{text} If the path improves, the macro transmission remains contained enough for the current read to hold."
    if scenario_type == "bear":
        return f"{text} If the path worsens, the macro transmission becomes more binding and can change the brief."
    return text


def _scenario_micro_text(base: str, *, scenario_type: str, portfolio_consequence: str) -> str | None:
    text = base.strip()
    if not text:
        return None
    if scenario_type == "bull":
        return f"{text} That keeps the sleeve consequence closer to {portfolio_consequence.strip() or 'the current plan'}."
    if scenario_type == "bear":
        return f"{text} That would force a reassessment of {portfolio_consequence.strip() or 'the current sleeve consequence'}."
    return text


def _scenario_short_term_text(base: str, *, scenario_type: str, expected_path: str) -> str | None:
    text = base.strip()
    path = expected_path.strip()
    if text:
        if scenario_type == "bull":
            return f"{text} Near-term breach risk stays contained while {path or 'the path improves'}."
        if scenario_type == "bear":
            return f"{text} Near-term breach risk rises if {path or 'the path deteriorates'}."
        return f"{text} The base path stays intact unless the next trigger breaks."
    if scenario_type == "bull":
        return f"{path or 'The path improves'} with threshold-breach risk contained."
    if scenario_type == "bear":
        return f"{path or 'The path deteriorates'} if threshold-breach risk rises."
    return f"{path or 'The base path holds'} while path support stays intact."


def _scenario_long_term_text(base: str, *, scenario_type: str, confirms: str, breaks: str) -> str | None:
    text = base.strip()
    if text:
        if scenario_type == "bull":
            return f"{text} Persistence would keep confirmation tilted toward {confirms.strip() or 'the current thesis'}."
        if scenario_type == "bear":
            return f"{text} Persistence would shift the brief toward {breaks.strip() or 'a weaker thesis state'}."
        return text
    if scenario_type == "bull":
        return f"Persistence would confirm the current read if {confirms.strip() or 'the current support holds'}."
    if scenario_type == "bear":
        return f"Persistence would weaken the current read if {breaks.strip() or 'the current support fails'}."
    return "Persistence would keep the current sleeve consequence in force."


def _path_risk_note(bundle, *, signal_card: dict[str, Any]) -> str:
    profile = _scenario_profile(bundle, signal_card)
    confirm = _probability_label(float(profile["confirm_base"]))
    break_risk = _probability_label(float(profile["break_base"]))
    breach = _risk_label(float(profile["threshold_breach_base"]))
    uncertainty = str(profile["uncertainty_width"]).strip().lower()
    trigger_pressure = float(profile.get("trigger_pressure") or 0.0)
    regime_alignment = float(profile.get("regime_alignment_score") or 0.0)
    confirmation = float(profile.get("cross_asset_confirmation_score") or 0.0)
    confirm_text = {
        "low": "Follow-through is still tentative.",
        "moderate": "Follow-through remains plausible.",
        "high": "Follow-through is already well supported.",
    }.get(confirm, "Follow-through remains plausible.")
    break_text = {
        "low": "A clean reversal is not the base case.",
        "moderate": "A reversal remains possible.",
        "high": "A reversal risk is already meaningful.",
    }.get(break_risk, "A reversal remains possible.")
    breach_text = {
        "low": "The watched threshold is not close to breaking.",
        "moderate": "The watched threshold is close enough to matter.",
        "high": "The watched threshold is under real pressure.",
    }.get(breach, "The watched threshold is close enough to matter.")
    uncertainty_text = {
        "tight": "The expected range is fairly tight.",
        "moderate": "The expected range is usable.",
        "wide": "The expected range is still wide.",
        "bounded": "The expected range is still wide.",
    }.get(uncertainty, "The expected range is still wide.")
    pressure_text = (
        "threshold pressure is rising"
        if trigger_pressure >= 0.66
        else "threshold pressure is still limited"
        if trigger_pressure <= 0.33
        else "threshold pressure is building"
    )
    confirmation_text = (
        "broad cross-asset confirmation"
        if confirmation >= 0.66
        else "partial cross-asset confirmation"
        if confirmation >= 0.42
        else "light cross-asset confirmation"
    )
    regime_text = (
        "the move still looks regime-relevant"
        if regime_alignment >= 0.66
        else "the move looks partly regime-relevant"
        if regime_alignment >= 0.42
        else "the move still looks isolated"
    )
    return (
        f"Forecast support is {str(profile['support_strength']).replace('_', ' ')}. "
        f"{confirm_text} {break_text} {breach_text} {pressure_text}, {confirmation_text}, and {regime_text}. {uncertainty_text}"
    )


def build_signal_support_bundle(
    signal_card: dict[str, Any],
    *,
    why_here: str,
    portfolio_consequence: str,
    next_action: str,
    scenario_depth: str = "coverage",
) -> dict[str, Any]:
    signal_kind = str(signal_card.get("signal_kind") or "market")
    symbol = str(signal_card.get("symbol") or signal_card.get("label") or signal_card.get("signal_id") or "signal")
    effect_type = str(signal_card.get("effect_type") or "").strip() or None
    macro_reason = str(signal_card.get("why_it_matters_macro") or signal_card.get("implication") or "").strip()
    micro_reason = str(signal_card.get("why_it_matters_micro") or why_here).strip()
    short_term_reason = str(signal_card.get("why_it_matters_short_term") or "").strip()
    long_term_reason = str(signal_card.get("why_it_matters_long_term") or why_here).strip()
    confidence_class = str(signal_card.get("confidence_class") or "").strip() or None
    sufficiency_state = str(signal_card.get("sufficiency_state") or "").strip() or None
    affected_sleeves = list(signal_card.get("affected_sleeves") or [])
    affected_candidates = list(signal_card.get("affected_candidates") or [])
    current_value = _safe_float(signal_card.get("current_value"))
    history = list(signal_card.get("history") or [])
    timestamps = list(signal_card.get("timestamps") or [])
    series_family = "news" if signal_kind == "news" else "macro" if signal_kind == "macro" else "benchmark_proxy"
    history_target_points = _history_target_points(signal_card, scenario_depth=scenario_depth)
    related_series = _related_series_inputs(signal_card, scenario_depth=scenario_depth)
    past_covariates, future_covariates, grouped_context_series = _scenario_request_context(
        signal_card,
        related_series,
        scenario_depth=scenario_depth,
        history_target_points=history_target_points,
    )

    request = build_request(
        object_type="daily_brief_signal",
        object_id=str(signal_card.get("signal_id") or "signal"),
        series_family=series_family,
        series_id=str(signal_card.get("series_id") or symbol),
        horizon=10,
        frequency="daily",
        covariates={
            "symbol": symbol,
            "current_value": current_value,
            "mapping_directness": signal_card.get("mapping_directness"),
            "signal_kind": signal_kind,
            "effect_type": effect_type,
            "portfolio_consequence": portfolio_consequence,
            "next_action": next_action,
            "related_series": related_series,
            "series_group_id": _bucket_key(signal_card),
            "scenario_depth": scenario_depth,
        },
        past_covariates=past_covariates,
        future_covariates=future_covariates,
        grouped_context_series=grouped_context_series,
        history_target_points=history_target_points,
        regime_context=why_here,
        history=history,
        timestamps=timestamps,
        surface_name="daily_brief",
    )
    support_class = str(
        signal_card.get("signal_support_class")
        or ("weak_support" if signal_kind == "news" else "direct_support" if signal_card.get("mapping_directness") == "direct" else "proxy_support")
    ).strip() or "proxy_support"
    bundle = build_forecast_bundle(
        request=request,
        surface_name="daily_brief",
        candidate_id=None,
        object_label=str(signal_card.get("label") or "Signal"),
        summary=str(signal_card.get("summary") or ""),
        implication=str(signal_card.get("implication") or why_here),
        support_class=support_class,
    )
    intelligence = _forecast_intelligence(signal_card, bundle)
    if is_dataclass(bundle) and is_dataclass(bundle.support):
        bundle = replace(bundle, support=replace(bundle.support, **intelligence))
    else:
        for key, value in intelligence.items():
            setattr(bundle.support, key, value)
    near_term_trigger = bundle.trigger_support[0].to_dict() if bundle.trigger_support else None
    thesis_trigger = bundle.trigger_support[1].to_dict() if len(bundle.trigger_support) > 1 else None
    near_term_trigger_text = (
        str(near_term_trigger.get("next_action_if_hit") or "").strip()
        if near_term_trigger
        else str(signal_card.get("confirms") or "").strip()
    )
    thesis_trigger_text = (
        str(thesis_trigger.get("next_action_if_hit") or "").strip()
        if thesis_trigger
        else str(signal_card.get("implication") or why_here).strip()
    )
    threshold_summary = "; ".join(
        item
        for item in [
            _format_threshold(near_term_trigger),
            _format_threshold(thesis_trigger),
        ]
        if item
    )
    profile = _scenario_profile(bundle, signal_card)
    path_risk_note = _path_risk_note(bundle, signal_card=signal_card)
    scenario_variants = _dedupe_scenario_variants(
        [
            _scenario_variant(
                scenario_type="bull",
                case_data=dict(bundle.scenario_support.bull_case or {}),
                profile=profile,
                signal_card=signal_card,
                macro_reason=macro_reason,
                micro_reason=micro_reason,
                short_term_reason=short_term_reason,
                long_term_reason=long_term_reason,
                portfolio_consequence=portfolio_consequence,
                next_action=next_action,
            ),
            _scenario_variant(
                scenario_type="base",
                case_data=dict(bundle.scenario_support.base_case or {}),
                profile=profile,
                signal_card=signal_card,
                macro_reason=macro_reason,
                micro_reason=micro_reason,
                short_term_reason=short_term_reason,
                long_term_reason=long_term_reason,
                portfolio_consequence=portfolio_consequence,
                next_action=next_action,
            ),
            _scenario_variant(
                scenario_type="bear",
                case_data=dict(bundle.scenario_support.bear_case or {}),
                profile=profile,
                signal_card=signal_card,
                macro_reason=macro_reason,
                micro_reason=micro_reason,
                short_term_reason=short_term_reason,
                long_term_reason=long_term_reason,
                portfolio_consequence=portfolio_consequence,
                next_action=next_action,
            ),
        ]
    )
    return {
        "bundle": bundle,
        "scenario_block": {
            "signal_id": str(signal_card.get("signal_id") or "signal"),
            "label": str(signal_card.get("label") or "Signal"),
            "summary": _scenario_block_summary(profile),
            "scenarios": scenario_variants,
            "forecast_support": bundle.support.to_dict(),
            "what_confirms": bundle.scenario_support.what_confirms,
            "what_breaks": bundle.scenario_support.what_breaks,
            "threshold_summary": threshold_summary,
            "path_risk_note": path_risk_note,
            "degraded_state": bundle.support.degraded_state,
        },
        "monitoring_condition": {
            "condition_id": f"monitor_{signal_card.get('signal_id') or 'signal'}",
            "label": str(signal_card.get("label") or "Signal"),
            "why_now": (short_term_reason or str(signal_card.get("summary") or "")).strip(),
            "path_risk_note": path_risk_note,
            "near_term_trigger": near_term_trigger_text or str(signal_card.get("confirms") or ""),
            "thesis_trigger": thesis_trigger_text or str(signal_card.get("implication") or ""),
            "break_condition": str(signal_card.get("breaks") or bundle.scenario_support.what_breaks or ""),
            "portfolio_consequence": portfolio_consequence,
            "next_action": next_action,
            "affected_sleeve": affected_sleeves[0] if affected_sleeves else None,
            "affected_candidates": affected_candidates,
            "effect_type": effect_type,
            "source_kind": signal_card.get("source_kind"),
            "confidence_class": confidence_class,
            "sufficiency_state": sufficiency_state,
            "forecast_support": bundle.support.to_dict(),
            "trigger_support": near_term_trigger,
        },
    }


def build_candidate_support_bundle(
    *,
    candidate_id: str,
    symbol: str,
    label: str,
    sleeve_purpose: str,
    implication: str,
    summary: str,
    current_value: float | None = None,
    history: list[float] | None = None,
    timestamps: list[str] | None = None,
    surface_name: str = "candidate_report",
) -> dict[str, Any]:
    request = build_request(
        object_type="candidate",
        object_id=candidate_id,
        series_family="ohlcv_history",
        series_id=symbol,
        horizon=21,
        frequency="daily",
        covariates={
            "symbol": symbol,
            "current_value": current_value,
            "candidate_id": candidate_id,
            "sleeve_purpose": sleeve_purpose,
        },
        regime_context=sleeve_purpose,
        history=list(history or []),
        timestamps=list(timestamps or []),
        surface_name=surface_name,
    )
    bundle = build_forecast_bundle(
        request=request,
        surface_name=surface_name,
        candidate_id=candidate_id,
        object_label=label,
        summary=summary,
        implication=implication,
        support_class="proxy_support",
    )
    return {
        "bundle": bundle,
        "scenario_blocks": [
            {
                "type": "bull",
                "label": str(bundle.scenario_support.bull_case.get("label") or "Bull case"),
                "trigger": str(bundle.scenario_support.what_confirms or ""),
                "expected_return": str(bundle.scenario_support.bull_case.get("expected_path") or ""),
                "portfolio_effect": str(bundle.scenario_support.bull_case.get("summary") or ""),
                "short_term": str(bundle.scenario_support.bull_case.get("expected_path") or ""),
                "long_term": str(bundle.scenario_support.what_confirms or ""),
                "forecast_support": bundle.support.to_dict(),
                "what_confirms": bundle.scenario_support.what_confirms,
                "what_breaks": bundle.scenario_support.what_breaks,
                "degraded_state": bundle.support.degraded_state,
            },
            {
                "type": "base",
                "label": str(bundle.scenario_support.base_case.get("label") or "Base case"),
                "trigger": "; ".join(f"{item.trigger_type}: {item.threshold}" for item in bundle.trigger_support[:2]),
                "expected_return": str(bundle.scenario_support.base_case.get("expected_path") or ""),
                "portfolio_effect": str(bundle.scenario_support.base_case.get("summary") or ""),
                "short_term": str(bundle.scenario_support.base_case.get("expected_path") or ""),
                "long_term": str(bundle.scenario_support.what_confirms or ""),
                "forecast_support": bundle.support.to_dict(),
                "what_confirms": bundle.scenario_support.what_confirms,
                "what_breaks": bundle.scenario_support.what_breaks,
                "degraded_state": bundle.support.degraded_state,
            },
            {
                "type": "bear",
                "label": str(bundle.scenario_support.bear_case.get("label") or "Bear case"),
                "trigger": str(bundle.scenario_support.what_breaks or ""),
                "expected_return": str(bundle.scenario_support.bear_case.get("expected_path") or ""),
                "portfolio_effect": str(bundle.scenario_support.bear_case.get("summary") or ""),
                "short_term": str(bundle.scenario_support.bear_case.get("expected_path") or ""),
                "long_term": str(bundle.scenario_support.what_breaks or ""),
                "forecast_support": bundle.support.to_dict(),
                "what_confirms": bundle.scenario_support.what_confirms,
                "what_breaks": bundle.scenario_support.what_breaks,
                "degraded_state": bundle.support.degraded_state,
            },
        ],
        "decision_thresholds": [
            {
                "label": f"{item.trigger_type.replace('_', ' ').title()} threshold",
                "value": item.threshold,
                "forecast_support": bundle.support.to_dict(),
                "trigger_type": item.trigger_type,
                "threshold_state": item.threshold_state,
            }
            for item in bundle.trigger_support
        ],
        "forecast_support": bundle.support.to_dict(),
        "flip_risk_note": bundle.scenario_support.what_breaks,
    }


def monitoring_condition_for_signal(
    signal_card: dict[str, Any],
    *,
    portfolio_consequence: str,
    next_action: str,
) -> dict[str, Any]:
    return build_signal_support_bundle(
        signal_card,
        why_here=str(signal_card.get("implication") or signal_card.get("summary") or ""),
        portfolio_consequence=portfolio_consequence,
        next_action=next_action,
    )["monitoring_condition"]


def scenario_block_for_signal(signal_card: dict[str, Any], *, why_here: str) -> dict[str, Any]:
    return build_signal_support_bundle(
        signal_card,
        why_here=why_here,
        portfolio_consequence=str(signal_card.get("summary") or ""),
        next_action="Monitor",
    )["scenario_block"]
