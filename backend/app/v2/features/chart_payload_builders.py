from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from statistics import mean, pstdev
from typing import Any
from urllib.parse import quote

import requests

from app.v2.core.market_strip_registry import market_strip_spec
from app.v2.core.market_state_sources import _iso_day, _load_series_rows, _transform_rows


_LABEL_BY_SYMBOL: dict[str, str] = {
    "DGS2": "UST 2Y",
    "^TNX": "UST 10Y",
    "^TYX": "UST 30Y",
    "DFII10": "10Y Real Yield",
    "BAMLH0A0HYM2": "US Credit Spread",
    "DXY": "Dollar Index",
    "^990100-USD-STRD": "World Equity",
    "^SPXEW": "Equity Breadth",
    "^IXIC": "Nasdaq",
    "^VIX": "VIX",
    "CL=F": "WTI",
    "BZ=F": "Brent",
    "GC=F": "Gold",
    "CPI_YOY": "Inflation",
    "FEDFUNDS": "Fed Funds",
    "MORTGAGE30US": "US 30Y Mortgage",
    "NASDAQNCPAG": "US Bonds",
    "DX-Y.NYB": "Dollar Proxy",
    "UUP": "Dollar Proxy",
}

_QUESTION_BY_BUCKET: dict[str, str] = {
    "duration": "Is the rate move still restrictive enough to keep bond adds selective",
    "credit": "Are credit conditions still tight enough to keep the risk budget constrained",
    "dollar_fx": "Is the dollar still firm enough to keep the hurdle high for broad risk adds",
    "growth": "Is breadth broadening enough to support a durable risk-on read",
    "market": "Is the market move broadening enough to hold the current risk read in place",
    "energy": "Is oil still holding strongly enough to keep the inflation hedge case alive",
    "real_assets": "Is hedge demand still strong enough to keep the real-assets sleeve relevant",
    "inflation": "Is inflation still firm enough to keep easy duration adds unattractive",
    "policy": "Is the policy impulse transmitting beyond the headline and into priced market channels",
    "volatility": "Is stress still elevated enough to keep defensive guardrails active",
}

_EVENT_REACTION_SYMBOLS: dict[str, str] = {
    "CL=F": "Oil",
    "BZ=F": "Brent",
    "^VIX": "Volatility",
    "DXY": "Dollar",
    "DFII10": "Real Yields",
    "^TNX": "Rates",
    "^SPXEW": "Breadth",
    "^990100-USD-STRD": "Equity",
}

_YAHOO_PROXY_SYMBOLS: dict[str, tuple[str, ...]] = {
    "DXY": ("DX-Y.NYB", "UUP"),
}

_FAMILY_CONFIGS: dict[str, dict[str, Any]] = {
    "rates": {
        "theme": "rates",
        "history_target_points": 63,
        "selected_horizon": "3M",
        "available_horizons": ["1M", "3M"],
        "compact_min_points": 4,
        "rich_min_points": 8,
        "strip_priority": True,
        "summary_position_label": "duration pressure",
    },
    "credit": {
        "theme": "credit",
        "history_target_points": 63,
        "selected_horizon": "3M",
        "available_horizons": ["1M", "3M"],
        "compact_min_points": 4,
        "rich_min_points": 8,
        "strip_priority": True,
        "summary_position_label": "funding stress",
    },
    "breadth": {
        "theme": "breadth",
        "history_target_points": 21,
        "selected_horizon": "1M",
        "available_horizons": ["1M", "3M"],
        "compact_min_points": 4,
        "rich_min_points": 7,
        "strip_priority": True,
        "summary_position_label": "breadth confirmation",
    },
    "fx": {
        "theme": "fx",
        "history_target_points": 21,
        "selected_horizon": "1M",
        "available_horizons": ["1M", "3M"],
        "compact_min_points": 4,
        "rich_min_points": 7,
        "strip_priority": True,
        "summary_position_label": "dollar hurdle",
    },
    "commodity": {
        "theme": "commodity",
        "history_target_points": 21,
        "selected_horizon": "1M",
        "available_horizons": ["1M", "3M"],
        "compact_min_points": 4,
        "rich_min_points": 5,
        "strip_priority": True,
        "summary_position_label": "hedge relevance",
    },
    "event": {
        "theme": "event",
        "history_target_points": 0,
        "selected_horizon": "Current",
        "available_horizons": ["Current"],
        "compact_min_points": 0,
        "rich_min_points": 0,
        "strip_priority": True,
        "summary_position_label": "market reaction",
    },
    "neutral": {
        "theme": "neutral",
        "history_target_points": 21,
        "selected_horizon": "1M",
        "available_horizons": ["1M"],
        "compact_min_points": 4,
        "rich_min_points": 7,
        "strip_priority": False,
        "summary_position_label": "review context",
    },
}

_CONFIRMATION_SPECS: dict[str, list[dict[str, str]]] = {
    "rates": [
        {"symbol": "DFII10", "relation": "same", "label": "Real Yield", "note": "valuation pressure"},
        {"symbol": "BAMLH0A0HYM2", "relation": "same", "label": "Credit", "note": "funding stress"},
        {"symbol": "^SPXEW", "relation": "inverse", "label": "Breadth", "note": "equity sensitivity"},
        {"symbol": "DXY", "relation": "same", "label": "Dollar", "note": "global hurdle"},
    ],
    "credit": [
        {"symbol": "^SPXEW", "relation": "inverse", "label": "Breadth", "note": "risk appetite"},
        {"symbol": "DFII10", "relation": "same", "label": "Real Yield", "note": "rates spillover"},
        {"symbol": "DXY", "relation": "same", "label": "Dollar", "note": "funding stress"},
        {"symbol": "^VIX", "relation": "same", "label": "Volatility", "note": "stress regime"},
    ],
    "breadth": [
        {"symbol": "^SPXEW", "relation": "same", "label": "Breadth", "note": "participation"},
        {"symbol": "BAMLH0A0HYM2", "relation": "inverse", "label": "Credit", "note": "funding backdrop"},
        {"symbol": "DFII10", "relation": "inverse", "label": "Real Yield", "note": "valuation pressure"},
        {"symbol": "DXY", "relation": "inverse", "label": "Dollar", "note": "global hurdle"},
    ],
    "fx": [
        {"symbol": "DFII10", "relation": "same", "label": "Real Yield", "note": "rate linkage"},
        {"symbol": "^990100-USD-STRD", "relation": "inverse", "label": "World Equity", "note": "risk hurdle"},
        {"symbol": "CL=F", "relation": "inverse", "label": "WTI", "note": "commodity spillover"},
        {"symbol": "GC=F", "relation": "inverse", "label": "Gold", "note": "hedge demand"},
    ],
    "commodity": [
        {"symbol": "CPI_YOY", "relation": "same", "label": "Inflation", "note": "inflation transmission"},
        {"symbol": "DXY", "relation": "inverse", "label": "Dollar", "note": "dollar headwind"},
        {"symbol": "DFII10", "relation": "same", "label": "10Y Real Yield", "note": "rate pressure"},
        {"symbol": "GC=F", "relation": "same", "label": "Gold", "note": "hedge demand"},
    ],
}


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_number(value: Any) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    direct = _safe_float(raw)
    if direct is not None:
        return direct
    token = []
    started = False
    for char in raw:
        if char.isdigit() or char in {".", "-", "+"}:
            token.append(char)
            started = True
            continue
        if started:
            break
    return _safe_float("".join(token))


def _bucket_key(signal_card: dict[str, Any]) -> str:
    return str(signal_card.get("primary_effect_bucket") or "market").strip().lower() or "market"


def _source_class(signal_card: dict[str, Any]) -> str:
    return str((signal_card.get("source_context") or {}).get("source_class") or "market_series").strip().lower()


def _symbol_label(symbol: str, fallback: str | None = None) -> str:
    cleaned = str(symbol or "").strip().upper()
    if not cleaned:
        return str(fallback or "Signal")
    return _LABEL_BY_SYMBOL.get(cleaned, str(fallback or cleaned).strip())


def _parse_dt(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.isoformat()


def _round_point(timestamp: str, value: float) -> dict[str, Any]:
    return {"timestamp": timestamp, "value": round(float(value), 4)}


def _signal_history_points(signal_card: dict[str, Any], *, max_points: int = 63) -> list[dict[str, Any]]:
    values = [_safe_float(item) for item in list(signal_card.get("history") or [])]
    timestamps = [str(item) for item in list(signal_card.get("timestamps") or [])]
    points: list[dict[str, Any]] = []
    for index, value in enumerate(values):
        if value is None:
            continue
        timestamp = timestamps[index] if index < len(timestamps) and timestamps[index] else str(signal_card.get("as_of") or "")
        points.append(_round_point(timestamp, value))
    if max_points > 0 and len(points) > max_points:
        points = points[-max_points:]
    return points


def _recent_std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    try:
        return float(pstdev(values))
    except Exception:
        return 0.0


def _direction_sign(signal_card: dict[str, Any]) -> int:
    direction = str(signal_card.get("direction") or "").strip().lower()
    if direction in {"up", "positive", "bull"}:
        return 1
    if direction in {"down", "negative", "bear"}:
        return -1
    history = [_safe_float(item) for item in list(signal_card.get("history") or [])]
    values = [item for item in history if item is not None]
    if len(values) < 2:
        return 0
    return 1 if values[-1] > values[-2] else -1 if values[-1] < values[-2] else 0


def _support_bundle(signal_card: dict[str, Any]) -> dict[str, Any]:
    payload = signal_card.get("support_bundle")
    return dict(payload or {}) if isinstance(payload, dict) else {}


def _bundle(signal_card: dict[str, Any]) -> Any:
    return _support_bundle(signal_card).get("bundle")


def _bundle_request(signal_card: dict[str, Any]) -> Any:
    return getattr(_bundle(signal_card), "request", None)


def _trigger_dicts(signal_card: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    bundle = _bundle(signal_card)
    trigger_support = list(getattr(bundle, "trigger_support", []) or []) if bundle else []
    near_term = trigger_support[0].to_dict() if trigger_support and hasattr(trigger_support[0], "to_dict") else {}
    thesis = trigger_support[1].to_dict() if len(trigger_support) > 1 and hasattr(trigger_support[1], "to_dict") else {}
    if not near_term:
        near_term = dict((_support_bundle(signal_card).get("monitoring_condition") or {}).get("trigger_support") or {})
    return near_term, thesis


def _family_key(signal_card: dict[str, Any]) -> str:
    source_class = _source_class(signal_card)
    if source_class in {"policy_event", "geopolitical_news"}:
        return "event"
    bucket = _bucket_key(signal_card)
    if bucket in {"duration", "inflation"}:
        return "rates"
    if bucket == "credit":
        return "credit"
    if bucket == "dollar_fx":
        return "fx"
    if bucket in {"growth", "market", "volatility"}:
        return "breadth"
    if bucket in {"energy", "real_assets"}:
        return "commodity"
    return "neutral"


def _family_config(signal_card: dict[str, Any]) -> dict[str, Any]:
    family = _family_key(signal_card)
    return dict(_FAMILY_CONFIGS.get(family) or _FAMILY_CONFIGS["neutral"])


def _spec(signal_card: dict[str, Any]) -> dict[str, Any]:
    return dict(market_strip_spec(str(signal_card.get("symbol") or "").strip().upper()) or {})


def _truth_history_points(signal_card: dict[str, Any], *, target_points: int) -> tuple[list[dict[str, Any]], str | None]:
    spec = _spec(signal_card)
    symbol = str(signal_card.get("symbol") or "").strip().upper()
    if not symbol:
        return [], None
    if str(spec.get("data_source") or "").strip().lower() == "public_fred":
        series_id = str(spec.get("series_id") or symbol).strip().upper()
        transform = str(spec.get("transform") or "identity").strip().lower() or "identity"
        try:
            rows, _provider_name, _acquisition_mode = _load_series_rows(series_id)
            transformed = _transform_rows(rows, transform=transform)
        except Exception:
            return [], None
        points = [_round_point(_iso_day(observed_at) or "", value) for observed_at, value in transformed if value is not None]
        return (points[-target_points:] if target_points > 0 else points), None
    if "_" in symbol and symbol not in _YAHOO_PROXY_SYMBOLS:
        return [], None
    return _yahoo_history_points(symbol, target_points=target_points)


@lru_cache(maxsize=96)
def _yahoo_history_points(symbol: str, *, target_points: int = 63) -> tuple[list[dict[str, Any]], str | None]:
    def _fetch(identifier: str) -> list[dict[str, Any]]:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(identifier, safe='')}"
        try:
            response = requests.get(
                url,
                params={"range": "3mo", "interval": "1d", "includePrePost": "false", "events": "div,splits"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=8,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError):
            return []
        result = ((payload.get("chart") or {}).get("result") or [{}])[0]
        timestamps = list(result.get("timestamp") or [])
        indicators = dict(result.get("indicators") or {})
        quote_rows = ((indicators.get("quote") or [{}]) or [{}])[0]
        closes = list(quote_rows.get("close") or [])
        if not closes:
            adjclose_rows = ((indicators.get("adjclose") or [{}]) or [{}])[0]
            closes = list(adjclose_rows.get("adjclose") or [])
        points: list[dict[str, Any]] = []
        for raw_timestamp, raw_value in zip(timestamps, closes):
            value = _safe_float(raw_value)
            if value is None:
                continue
            timestamp = datetime.fromtimestamp(int(raw_timestamp), tz=UTC).isoformat()
            points.append(_round_point(timestamp, value))
        return points[-target_points:] if target_points > 0 else points

    normalized = str(symbol or "").strip().upper()
    direct = _fetch(normalized)
    if direct:
        return direct, None
    for proxy_symbol in _YAHOO_PROXY_SYMBOLS.get(normalized, ()):
        proxied = _fetch(proxy_symbol)
        if proxied:
            return proxied, f"Chart path uses {_symbol_label(proxy_symbol, 'proxy')} history."
    return [], None


def _primary_history_points(signal_card: dict[str, Any], *, target_points: int) -> tuple[list[dict[str, Any]], str | None]:
    signal_points = _signal_history_points(signal_card, max_points=target_points)
    config = _family_config(signal_card)
    compact_min = int(config.get("compact_min_points") or 4)
    if len(signal_points) >= compact_min:
        return signal_points, None
    truth_points, proxy_note = _truth_history_points(signal_card, target_points=target_points)
    if len(truth_points) > len(signal_points):
        return truth_points, proxy_note
    return signal_points, None


def _preferred_confirmation_specs(signal_card: dict[str, Any]) -> list[dict[str, str]]:
    family = _family_key(signal_card)
    return list(_CONFIRMATION_SPECS.get(family) or [])


def _series_lookup_from_bundle(signal_card: dict[str, Any]) -> dict[str, dict[str, Any]]:
    request = _bundle_request(signal_card)
    if request is None:
        return {}
    lookup: dict[str, dict[str, Any]] = {}
    covariates = dict(getattr(request, "covariates", {}) or {})
    for item in list(covariates.get("related_series") or []):
        symbol = str(item.get("symbol") or "").strip().upper()
        if symbol:
            lookup[symbol] = dict(item)
    grouped = dict(getattr(request, "grouped_context_series", {}) or {})
    for channel, items in grouped.items():
        for item in list(items or []):
            row = dict(item)
            row.setdefault("channel", channel)
            symbol = str(row.get("symbol") or "").strip().upper()
            if symbol and symbol not in lookup:
                lookup[symbol] = row
    return lookup


def _history_change(points: list[dict[str, Any]]) -> tuple[float | None, float | None, str]:
    if len(points) < 2:
        return None, None, "flat"
    current_value = float(points[-1]["value"])
    previous_value = float(points[-2]["value"])
    delta = current_value - previous_value
    delta_pct = ((delta / abs(previous_value)) * 100.0) if previous_value not in {0.0, -0.0} else None
    direction = "up" if delta > 0 else "down" if delta < 0 else "flat"
    return delta, delta_pct, direction


def _direction_matches(signal_card: dict[str, Any], *, relation: str, actual_direction: str) -> str:
    expected_sign = _direction_sign(signal_card)
    actual_sign = 1 if actual_direction == "up" else -1 if actual_direction == "down" else 0
    if actual_sign == 0:
        return "neutral"
    desired = expected_sign if relation == "same" else -expected_sign
    if desired == 0:
        return "neutral"
    return "confirming" if actual_sign == desired else "resisting"


def _change_label(*, delta: float | None, delta_pct: float | None, direction: str) -> str | None:
    prefix = "up" if direction == "up" else "down" if direction == "down" else "flat"
    if delta_pct is not None and abs(delta_pct) >= 0.1:
        return f"{prefix} {abs(delta_pct):.1f}%"
    if delta is not None:
        return f"{prefix} {abs(delta):.2f}"
    return None


def _confirmation_items(signal_card: dict[str, Any], *, limit: int = 5) -> list[dict[str, Any]]:
    specs = _preferred_confirmation_specs(signal_card)[:limit]
    if not specs:
        return []
    from_bundle = _series_lookup_from_bundle(signal_card)
    items: list[dict[str, Any]] = []
    for spec in specs:
        symbol = str(spec.get("symbol") or "").strip().upper()
        label = str(spec.get("label") or _symbol_label(symbol, symbol))
        relation = str(spec.get("relation") or "same").strip().lower() or "same"
        note = str(spec.get("note") or "").strip() or None
        item = from_bundle.get(symbol)
        points: list[dict[str, Any]] = []
        latest_direction = "flat"
        latest_change = None
        latest_change_pct = None
        if item:
            history = [_safe_float(value) for value in list(item.get("history") or [])]
            timestamps = [str(value) for value in list(item.get("timestamps") or [])]
            points = [
                _round_point(timestamps[index] if index < len(timestamps) and timestamps[index] else str(signal_card.get("as_of") or ""), value)
                for index, value in enumerate(history)
                if value is not None
            ]
            latest_change = _safe_float(item.get("latest_change"))
            latest_change_pct = _safe_float(item.get("latest_change_pct"))
            latest_direction = str(item.get("latest_direction") or "flat").strip().lower() or "flat"
        if len(points) < 2:
            points, _proxy_note = _truth_history_points({"symbol": symbol, "primary_effect_bucket": _bucket_key(signal_card)}, target_points=21)
            latest_change, latest_change_pct, latest_direction = _history_change(points)
        if len(points) < 2:
            items.append(
                {
                    "item_id": f"{signal_card.get('signal_id') or 'signal'}_{symbol.lower()}",
                    "label": label,
                    "status": "missing",
                    "direction": "flat",
                    "value_label": None,
                    "note": "history unavailable",
                }
            )
            continue
        items.append(
            {
                "item_id": f"{signal_card.get('signal_id') or 'signal'}_{symbol.lower()}",
                "label": label,
                "status": _direction_matches(signal_card, relation=relation, actual_direction=latest_direction),
                "direction": latest_direction,
                "value_label": _change_label(delta=latest_change, delta_pct=latest_change_pct, direction=latest_direction),
                "note": note,
            }
        )
    return items


def _event_reaction_items(signal_card: dict[str, Any], *, limit: int = 5) -> list[dict[str, Any]]:
    from_bundle = _series_lookup_from_bundle(signal_card)
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for symbol, label in _EVENT_REACTION_SYMBOLS.items():
        if len(items) >= limit:
            break
        if symbol in seen:
            continue
        seen.add(symbol)
        row = from_bundle.get(symbol)
        points: list[dict[str, Any]] = []
        latest_direction = "flat"
        latest_change = None
        latest_change_pct = None
        if row:
            history = [_safe_float(value) for value in list(row.get("history") or [])]
            timestamps = [str(value) for value in list(row.get("timestamps") or [])]
            points = [
                _round_point(timestamps[index] if index < len(timestamps) and timestamps[index] else str(signal_card.get("as_of") or ""), value)
                for index, value in enumerate(history)
                if value is not None
            ]
            latest_change = _safe_float(row.get("latest_change"))
            latest_change_pct = _safe_float(row.get("latest_change_pct"))
            latest_direction = str(row.get("latest_direction") or "flat").strip().lower() or "flat"
        if len(points) < 2:
            items.append(
                {
                    "item_id": f"{signal_card.get('signal_id') or 'signal'}_event_{symbol.lower()}",
                    "label": label,
                    "status": "missing",
                    "direction": "flat",
                    "value_label": None,
                    "note": "reaction missing",
                }
            )
            continue
        status = _direction_matches(signal_card, relation="same", actual_direction=latest_direction)
        items.append(
            {
                "item_id": f"{signal_card.get('signal_id') or 'signal'}_event_{symbol.lower()}",
                "label": label,
                "status": status if status != "neutral" else "neutral",
                "direction": latest_direction,
                "value_label": _change_label(delta=latest_change, delta_pct=latest_change_pct, direction=latest_direction),
                "note": "priced response" if status in {"confirming", "resisting"} else "still quiet",
            }
        )
    return items


def _confirmation_state(signal_card: dict[str, Any], items: list[dict[str, Any]]) -> str:
    effective = [item for item in items if item["status"] in {"confirming", "resisting", "neutral"}]
    if not effective:
        score = float(((signal_card.get("forecast_support") or {}).get("cross_asset_confirmation_score")) or 0.0)
        if score >= 0.66:
            return "broad"
        if score >= 0.42:
            return "partial"
        return "none"
    confirming = sum(1 for item in effective if item["status"] == "confirming")
    resisting = sum(1 for item in effective if item["status"] == "resisting")
    total = len(effective)
    if resisting >= max(2, confirming + 1):
        return "resisting"
    if confirming / max(total, 1) >= 0.66:
        return "broad"
    if confirming / max(total, 1) >= 0.34:
        return "partial"
    return "none"


def _threshold_label(bucket: str, which: str) -> str:
    labels = {
        "duration": {"review": "Review line", "confirm": "Higher-rate hold line", "break": "Relief line"},
        "credit": {"review": "Review line", "confirm": "Funding-stress line", "break": "Fade line"},
        "dollar_fx": {"review": "Review band", "confirm": "Dollar hurdle line", "break": "Fade line"},
        "growth": {"review": "Review band", "confirm": "Broadening line", "break": "Stall line"},
        "market": {"review": "Review band", "confirm": "Broadening line", "break": "Fade line"},
        "energy": {"review": "Review range", "confirm": "Inflation pressure line", "break": "Fade back into context"},
        "real_assets": {"review": "Review range", "confirm": "Hedge-demand line", "break": "Fade line"},
        "inflation": {"review": "Review line", "confirm": "Sticky-inflation line", "break": "Cooling line"},
        "volatility": {"review": "Review band", "confirm": "Stress line", "break": "Calming line"},
    }
    return labels.get(bucket, labels["market"]).get(which, "Review line")


def _band_label(base_label: str) -> str:
    label = str(base_label or "Review band").strip()
    lowered = label.lower()
    if "band" in lowered or "range" in lowered or "zone" in lowered:
        return label
    return f"{label} band"


def _threshold_zone(center: float | None, values: list[float], *, width_scale: float = 0.6) -> dict[str, Any] | None:
    if center is None:
        return None
    recent_std = _recent_std(values[-8:])
    recent_mean = mean(values[-5:]) if values else center
    base_width = max(abs(center - recent_mean) * 0.35, recent_std * width_scale, abs(center) * 0.005)
    return {"min": round(center - base_width, 4), "max": round(center + base_width, 4)}


def _threshold_payload(signal_card: dict[str, Any], points: list[dict[str, Any]]) -> dict[str, Any] | None:
    values = [float(point["value"]) for point in points]
    current_value = values[-1] if values else None
    previous_value = values[-2] if len(values) >= 2 else None
    near_term, thesis = _trigger_dicts(signal_card)
    review_line = _extract_number(near_term.get("threshold"))
    confirm_line = _extract_number(thesis.get("threshold"))
    break_line = previous_value
    bucket = _bucket_key(signal_card)
    if review_line is None and confirm_line is None and break_line is None:
        return None
    review_zone = _threshold_zone(review_line, values) if review_line is not None else None
    trigger_zone = _threshold_zone(confirm_line, values, width_scale=0.4) if confirm_line is not None else None
    return {
        "review_line": {
            "value": review_line,
            "label": _threshold_label(bucket, "review"),
            "note": str(near_term.get("next_action_if_hit") or "").strip() or None,
        }
        if review_line is not None
        else None,
        "confirm_line": {
            "value": confirm_line,
            "label": _threshold_label(bucket, "confirm"),
            "note": str(thesis.get("next_action_if_hit") or "").strip() or None,
        }
        if confirm_line is not None
        else None,
        "break_line": {
            "value": break_line,
            "label": _threshold_label(bucket, "break"),
            "note": str(near_term.get("next_action_if_broken") or thesis.get("next_action_if_broken") or "").strip() or None,
        }
        if break_line is not None
        else None,
        "current_status_line": {
            "value": current_value,
            "label": str(signal_card.get("decision_status") or "current").replace("_", " ").strip().title(),
            "note": str(signal_card.get("interpretation_subtitle") or "").strip() or None,
        }
        if current_value is not None
        else None,
        "review_zone": {
            "min": review_zone["min"],
            "max": review_zone["max"],
            "label": _band_label(_threshold_label(bucket, "review")),
            "note": None,
        }
        if review_zone is not None
        else None,
        "trigger_zone": {
            "min": trigger_zone["min"],
            "max": trigger_zone["max"],
            "label": _band_label(_threshold_label(bucket, "confirm")),
            "note": None,
        }
        if trigger_zone is not None
        else None,
    }


def _threshold_line_count(thresholds: dict[str, Any] | None) -> int:
    if not thresholds:
        return 0
    count = 0
    for key in ("review_line", "confirm_line", "break_line"):
        if (thresholds.get(key) or {}).get("value") is not None:
            count += 1
    return count


def _line_spacing_score(points: list[dict[str, Any]], thresholds: dict[str, Any] | None) -> float:
    if not points or not thresholds:
        return 0.0
    values = [float(point["value"]) for point in points]
    span = max(max(values) - min(values), max(abs(values[-1]) * 0.02, 0.25))
    threshold_values = [
        _safe_float((thresholds.get("review_line") or {}).get("value")),
        _safe_float((thresholds.get("confirm_line") or {}).get("value")),
        _safe_float((thresholds.get("break_line") or {}).get("value")),
    ]
    threshold_values = [value for value in threshold_values if value is not None]
    if len(threshold_values) < 2:
        return 1.0
    distances = [abs(right - left) / span for left, right in zip(threshold_values, threshold_values[1:])]
    return min(distances) if distances else 1.0


def _format_level(value: float | None) -> str:
    if value is None:
        return "n/a"
    absolute = abs(value)
    if absolute >= 1000:
        return f"{value:,.2f}"
    if absolute >= 100:
        return f"{value:.2f}"
    if absolute >= 10:
        return f"{value:.2f}"
    return f"{value:.2f}"


def _position_vs_review(points: list[dict[str, Any]], thresholds: dict[str, Any] | None) -> str:
    if not points or not thresholds:
        return "no clear review line"
    current_value = float(points[-1]["value"])
    review_zone = thresholds.get("review_zone") if isinstance(thresholds.get("review_zone"), dict) else None
    if review_zone and review_zone.get("min") is not None and review_zone.get("max") is not None:
        if float(review_zone["min"]) <= current_value <= float(review_zone["max"]):
            return "inside review band"
        if current_value > float(review_zone["max"]):
            return "above review band"
        return "below review band"
    review_line = _safe_float((thresholds.get("review_line") or {}).get("value"))
    if review_line is None:
        return "no clear review line"
    tolerance = max(abs(review_line) * 0.01, 0.2)
    if abs(current_value - review_line) <= tolerance:
        return "near review line"
    return "above review line" if current_value > review_line else "below review line"


def _path_state(signal_card: dict[str, Any], points: list[dict[str, Any]], thresholds: dict[str, Any] | None) -> str:
    if len(points) < 2:
        return "history too thin"
    values = [float(point["value"]) for point in points]
    current_value = values[-1]
    previous_value = values[-2]
    review_zone = thresholds.get("review_zone") if isinstance((thresholds or {}).get("review_zone"), dict) else None
    break_line = _safe_float(((thresholds or {}).get("break_line") or {}).get("value"))
    confirm_line = _safe_float(((thresholds or {}).get("confirm_line") or {}).get("value"))
    direction = _direction_sign(signal_card)
    recent_move = current_value - previous_value
    if break_line is not None and abs(current_value - break_line) <= max(abs(break_line) * 0.01, 0.2):
        return "near fade line"
    if review_zone and review_zone.get("min") is not None and review_zone.get("max") is not None:
        if float(review_zone["min"]) <= current_value <= float(review_zone["max"]):
            return "inside review range"
        if current_value > float(review_zone["max"]) and recent_move >= 0:
            return "holding above review band"
        if current_value < float(review_zone["min"]) and recent_move <= 0:
            return "fading below review band"
    if confirm_line is not None:
        if direction >= 0 and current_value >= confirm_line:
            return "above confirm line"
        if direction < 0 and current_value <= confirm_line:
            return "through confirm line"
    if direction > 0 and recent_move > 0:
        return "still holding"
    if direction < 0 and recent_move < 0:
        return "still holding"
    if abs(recent_move) <= max(abs(previous_value) * 0.0025, 0.05):
        return "stalling"
    return "fading"


def _human_confirmation_state(confirmation_state: str) -> str:
    return {
        "broad": "broad confirmation",
        "partial": "partial confirmation",
        "resisting": "markets resisting",
        "none": "confirmation still missing",
    }.get(str(confirmation_state or "none"), "confirmation still missing")


def _summary_metrics(
    signal_card: dict[str, Any],
    points: list[dict[str, Any]],
    thresholds: dict[str, Any] | None,
    confirmation_state: str,
    path_state: str,
) -> list[dict[str, Any]]:
    current_value = float(points[-1]["value"]) if points else _safe_float(signal_card.get("current_value"))
    return [
        {"label": "Current", "value": _format_level(current_value), "tone": "neutral"},
        {"label": "Position", "value": _position_vs_review(points, thresholds), "tone": "review"},
        {"label": "Confirmation", "value": _human_confirmation_state(confirmation_state), "tone": "support"},
        {"label": "Path", "value": path_state, "tone": "confirm" if "holding" in path_state or "confirm" in path_state else "warn"},
    ]


def _summary_value(summary: list[dict[str, Any]], label: str) -> str:
    for item in summary:
        if str(item.get("label") or "").strip().lower() == label.lower():
            return str(item.get("value") or "").strip()
    return ""


def _threshold_legend(thresholds: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not thresholds:
        return []
    items: list[dict[str, Any]] = []
    review_zone = thresholds.get("review_zone")
    if review_zone and review_zone.get("min") is not None and review_zone.get("max") is not None:
        label = str(review_zone.get("label") or "Review band").strip()
        lowered = label.lower()
        if "band" not in lowered and "range" not in lowered and "zone" not in lowered:
            label = f"{label} band"
        items.append({"legend_id": "review_zone", "label": label, "style": "zone", "tone": "review"})
    elif (thresholds.get("review_line") or {}).get("value") is not None:
        items.append({"legend_id": "review_line", "label": str((thresholds.get("review_line") or {}).get("label") or "Review line"), "style": "line", "tone": "review"})
    if (thresholds.get("confirm_line") or {}).get("value") is not None:
        items.append({"legend_id": "confirm_line", "label": str((thresholds.get("confirm_line") or {}).get("label") or "Confirm line"), "style": "line", "tone": "confirm"})
    if (thresholds.get("break_line") or {}).get("value") is not None:
        items.append({"legend_id": "break_line", "label": str((thresholds.get("break_line") or {}).get("label") or "Fade line"), "style": "line", "tone": "break"})
    return items[:3]


def _observed_path_explainer(signal_card: dict[str, Any]) -> str:
    family = _family_key(signal_card)
    if family == "rates":
        return "Observed path shows the actual yield path so far."
    if family == "credit":
        return "Observed path shows the actual spread path so far."
    if family == "breadth":
        return "Observed path shows whether participation is broadening or stalling."
    if family == "fx":
        return "Observed path shows whether the dollar hurdle is holding or easing."
    if family == "commodity":
        symbol = str(signal_card.get("symbol") or "").strip().upper()
        if symbol == "CL=F":
            return "Observed path shows the actual oil path so far."
        if symbol == "GC=F":
            return "Observed path shows whether hedge demand is still building or fading."
        return "Observed path shows the actual commodity path so far."
    return "Observed path shows the actual market path so far."


def _forecast_path_explainer(signal_card: dict[str, Any], forecast_overlay: dict[str, Any] | None) -> str:
    if not forecast_overlay:
        return "No forecast path is shown here because support is too weak to add one."
    family = _family_key(signal_card)
    if family == "rates":
        return "Forecast path shows where yields may go next if current rate pressure holds."
    if family == "credit":
        return "Forecast path shows where spreads may go next if current funding pressure holds."
    if family == "breadth":
        return "Forecast path shows whether breadth may keep widening if current support holds."
    if family == "fx":
        return "Forecast path shows whether dollar pressure may persist if current support holds."
    if family == "commodity":
        symbol = str(signal_card.get("symbol") or "").strip().upper()
        if symbol == "CL=F":
            return "Forecast path shows where oil may go next if current pressure holds."
        return "Forecast path shows whether the protection move may keep running if current support holds."
    return "Forecast path shows where the move may go next if current pressure holds."


def _observed_guide_text(signal_card: dict[str, Any]) -> str:
    family = _family_key(signal_card)
    if family == "rates":
        return "actual yield path so far"
    if family == "credit":
        return "actual spread path so far"
    if family == "breadth":
        return "whether participation is broadening or stalling"
    if family == "fx":
        return "whether the dollar hurdle is holding or easing"
    if family == "commodity":
        symbol = str(signal_card.get("symbol") or "").strip().upper()
        if symbol == "CL=F":
            return "actual oil path so far"
        if symbol == "GC=F":
            return "whether hedge demand is building or fading"
        return "actual commodity path so far"
    return "actual market path so far"


def _forecast_guide_text(signal_card: dict[str, Any], forecast_overlay: dict[str, Any] | None) -> str | None:
    if not forecast_overlay:
        return None
    family = _family_key(signal_card)
    if family == "rates":
        return "projected yields if current rate pressure holds"
    if family == "credit":
        return "projected spreads if current funding pressure holds"
    if family == "breadth":
        return "projected breadth if current support holds"
    if family == "fx":
        return "projected dollar path if current support holds"
    if family == "commodity":
        symbol = str(signal_card.get("symbol") or "").strip().upper()
        if symbol == "CL=F":
            return "projected oil path if current pressure holds"
        return "projected protection path if current support holds"
    return "projected path if current pressure holds"


def _review_meaning(signal_card: dict[str, Any], thresholds: dict[str, Any] | None) -> str | None:
    if not thresholds:
        return None
    bucket = _bucket_key(signal_card)
    review_zone = thresholds.get("review_zone") if isinstance(thresholds.get("review_zone"), dict) else None
    review_line = thresholds.get("review_line") if isinstance(thresholds.get("review_line"), dict) else None
    label = str((review_zone or review_line or {}).get("label") or "Review band").strip()
    if bucket == "duration":
        return f"{label} marks the range where duration pressure keeps the brief under review."
    if bucket == "credit":
        return f"{label} marks the range where funding pressure keeps the brief under review."
    if bucket in {"growth", "market"}:
        return f"{label} marks the range where the risk-on read stays under review."
    if bucket == "dollar_fx":
        return f"{label} marks the range where the dollar hurdle stays under review."
    if bucket in {"energy", "real_assets"}:
        return f"{label} marks the range where the inflation or hedge case stays under review."
    if bucket == "inflation":
        return f"{label} marks the range where the inflation read still keeps bonds under pressure."
    return f"{label} marks the range where the brief stays under review."


def _review_guide_text(signal_card: dict[str, Any], thresholds: dict[str, Any] | None) -> str | None:
    if not thresholds:
        return None
    bucket = _bucket_key(signal_card)
    if bucket == "duration":
        return "range where duration pressure keeps the brief under review"
    if bucket == "credit":
        return "range where funding pressure keeps the brief under review"
    if bucket in {"growth", "market"}:
        return "range where the risk-on read stays under review"
    if bucket == "dollar_fx":
        return "range where the dollar hurdle stays under review"
    if bucket in {"energy", "real_assets"}:
        return "range where the inflation or hedge case stays under review"
    if bucket == "inflation":
        return "range where the inflation read still keeps bonds under pressure"
    return "range where the brief stays under review"


def _confirm_meaning(signal_card: dict[str, Any], thresholds: dict[str, Any] | None) -> str | None:
    confirm_line = (thresholds or {}).get("confirm_line") if isinstance((thresholds or {}).get("confirm_line"), dict) else None
    if not confirm_line or confirm_line.get("value") is None:
        return None
    bucket = _bucket_key(signal_card)
    label = str(confirm_line.get("label") or "Confirm line").strip()
    if bucket == "duration":
        return f"{label} marks where long-bond adds stay harder to justify."
    if bucket == "credit":
        return f"{label} marks where funding conditions stay restrictive enough to keep the risk budget tight."
    if bucket in {"growth", "market"}:
        return f"{label} marks where the rally starts looking broad enough to trust."
    if bucket == "dollar_fx":
        return f"{label} marks where the dollar stays strong enough to keep the hurdle high for global risk."
    if bucket in {"energy", "real_assets"}:
        return f"{label} marks where the inflation or hedge case strengthens."
    if bucket == "inflation":
        return f"{label} marks where inflation pressure is still strong enough to keep easy bond relief delayed."
    return f"{label} marks where the current read strengthens."


def _confirm_guide_text(signal_card: dict[str, Any], thresholds: dict[str, Any] | None) -> str | None:
    confirm_line = (thresholds or {}).get("confirm_line") if isinstance((thresholds or {}).get("confirm_line"), dict) else None
    if not confirm_line or confirm_line.get("value") is None:
        return None
    bucket = _bucket_key(signal_card)
    if bucket == "duration":
        return "level where long-bond adds stay harder to justify"
    if bucket == "credit":
        return "level where funding conditions stay restrictive"
    if bucket in {"growth", "market"}:
        return "level where the rally starts looking broad enough to trust"
    if bucket == "dollar_fx":
        return "level where the dollar still acts as a hurdle"
    if bucket in {"energy", "real_assets"}:
        return "level where the inflation or hedge case strengthens"
    if bucket == "inflation":
        return "level where inflation pressure still delays easier bond relief"
    return "level where the current read strengthens"


def _break_meaning(signal_card: dict[str, Any], thresholds: dict[str, Any] | None) -> str | None:
    break_line = (thresholds or {}).get("break_line") if isinstance((thresholds or {}).get("break_line"), dict) else None
    if not break_line or break_line.get("value") is None:
        return None
    bucket = _bucket_key(signal_card)
    label = str(break_line.get("label") or "Fade line").strip()
    if bucket == "duration":
        return f"{label} marks where that rate pressure starts fading."
    if bucket == "credit":
        return f"{label} marks where funding pressure starts easing."
    if bucket in {"growth", "market"}:
        return f"{label} marks where the broadening read starts stalling."
    if bucket == "dollar_fx":
        return f"{label} marks where the dollar hurdle starts easing."
    if bucket in {"energy", "real_assets"}:
        return f"{label} marks where the move stops adding to the inflation or hedge case."
    if bucket == "inflation":
        return f"{label} marks where inflation pressure starts cooling."
    return f"{label} marks where the current read starts fading."


def _break_guide_text(signal_card: dict[str, Any], thresholds: dict[str, Any] | None) -> str | None:
    break_line = (thresholds or {}).get("break_line") if isinstance((thresholds or {}).get("break_line"), dict) else None
    if not break_line or break_line.get("value") is None:
        return None
    bucket = _bucket_key(signal_card)
    if bucket == "duration":
        return "level where rate pressure starts fading"
    if bucket == "credit":
        return "level where funding pressure starts easing"
    if bucket in {"growth", "market"}:
        return "level where the broadening read starts stalling"
    if bucket == "dollar_fx":
        return "level where the dollar hurdle starts easing"
    if bucket in {"energy", "real_assets"}:
        return "level where the move stops adding to the hedge case"
    if bucket == "inflation":
        return "level where inflation pressure starts cooling"
    return "level where the current read starts fading"


def _chart_explainer_lines(
    signal_card: dict[str, Any],
    *,
    thresholds: dict[str, Any] | None,
    forecast_overlay: dict[str, Any] | None,
) -> list[str]:
    lines = [
        _observed_guide_text(signal_card),
        _forecast_guide_text(signal_card, forecast_overlay),
        _review_guide_text(signal_card, thresholds),
        _confirm_guide_text(signal_card, thresholds),
        _break_guide_text(signal_card, thresholds),
    ]
    return [line for line in lines if str(line or "").strip()]


def _chart_guide_items(
    signal_card: dict[str, Any],
    *,
    forecast_overlay: dict[str, Any] | None,
    review_band: dict[str, Any] | None,
    threshold_lines: list[dict[str, Any]],
    overlap_mode: str,
    merged_zone: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = [
        {"id": "observed", "label": "Observed", "text": _observed_guide_text(signal_card)},
    ]
    forecast_text = _forecast_guide_text(signal_card, forecast_overlay)
    if forecast_text:
        items.append({"id": "forecast", "label": "Forecast", "text": forecast_text})
    if review_band is not None:
        items.append({"id": review_band["band_id"], "label": review_band["label"], "text": _review_guide_text(signal_card, {"review_zone": review_band}) or "range where the brief stays under review"})
    if overlap_mode == "merge_to_zone" and merged_zone is not None:
        items.append({
            "id": "decision_zone",
            "label": str(merged_zone.get("label") or "Decision zone"),
            "text": "two decision lines merged because they sit too close to separate cleanly",
        })
        return items
    for line in threshold_lines:
        semantic_role = str(line.get("semantic_role") or "").strip()
        if semantic_role in {"strengthen_line", "hold_line"}:
            text = _confirm_guide_text(signal_card, {"confirm_line": line}) or str(line.get("plain_language_meaning") or "").strip()
        elif semantic_role in {"fade_line", "break_line", "stall_line"}:
            text = _break_guide_text(signal_card, {"break_line": line}) or str(line.get("plain_language_meaning") or "").strip()
        else:
            text = str(line.get("plain_language_meaning") or "").strip()
        items.append(
            {
                "id": str(line.get("threshold_id") or line.get("id") or "threshold"),
                "label": str(line.get("label") or "Threshold"),
                "text": text,
                "muted": not bool(line.get("visible_by_default")),
            }
        )
    return [item for item in items if str(item.get("text") or "").strip()]


def _threshold_semantic_role(bucket: str, which: str) -> str:
    mapping = {
        "duration": {"confirm": "hold_line", "break": "fade_line", "review": "review_line"},
        "credit": {"confirm": "strengthen_line", "break": "fade_line", "review": "review_line"},
        "dollar_fx": {"confirm": "hold_line", "break": "fade_line", "review": "review_line"},
        "growth": {"confirm": "strengthen_line", "break": "stall_line", "review": "review_line"},
        "market": {"confirm": "strengthen_line", "break": "fade_line", "review": "review_line"},
        "energy": {"confirm": "strengthen_line", "break": "fade_line", "review": "review_line"},
        "real_assets": {"confirm": "hold_line", "break": "fade_line", "review": "review_line"},
        "inflation": {"confirm": "hold_line", "break": "fade_line", "review": "review_line"},
        "volatility": {"confirm": "hold_line", "break": "fade_line", "review": "review_line"},
    }
    return str(mapping.get(bucket, mapping["market"]).get(which, "review_line"))


def _threshold_priority(role: str) -> int:
    return {
        "strengthen_line": 1,
        "hold_line": 1,
        "stall_line": 2,
        "fade_line": 2,
        "break_line": 2,
        "review_line": 3,
    }.get(str(role or "review_line"), 3)


def _threshold_visible_default(role: str) -> bool:
    return str(role or "") != "review_line"


def _base_threshold_render_mode(role: str) -> str:
    if role in {"fade_line", "break_line", "stall_line", "review_line"}:
        return "dashed_line"
    return "line"


def _review_band_object(signal_card: dict[str, Any], thresholds: dict[str, Any] | None) -> dict[str, Any] | None:
    review_zone = (thresholds or {}).get("review_zone") if isinstance((thresholds or {}).get("review_zone"), dict) else None
    if not review_zone or review_zone.get("min") is None or review_zone.get("max") is None:
        return None
    minimum = float(review_zone["min"])
    maximum = float(review_zone["max"])
    span = abs(maximum - minimum)
    return {
        "band_id": "review_band",
        "label": str(review_zone.get("label") or "Review band").strip(),
        "min": minimum,
        "max": maximum,
        "lower_bound": minimum,
        "upper_bound": maximum,
        "plain_language_meaning": _review_meaning(signal_card, thresholds) or "Review band marks where the brief stays under review.",
        "visible_by_default": True,
        "object_role": "review_context",
        "object_type": "range_zone",
        "focus_y_domain_impact": "anchor",
        "narrow_band": span <= max(abs(maximum) * 0.0075, 0.12),
    }


def _threshold_lines(signal_card: dict[str, Any], thresholds: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not thresholds:
        return []
    bucket = _bucket_key(signal_card)
    lines: list[dict[str, Any]] = []
    review_line = (thresholds.get("review_line") or {}) if isinstance(thresholds.get("review_line"), dict) else {}
    if review_line.get("value") is not None and not isinstance((thresholds.get("review_zone") or None), dict):
        lines.append(
            {
                "id": "review_line",
                "threshold_id": "review_line",
                "label": str(review_line.get("label") or _threshold_label(bucket, "review")).strip(),
                "semantic_role": _threshold_semantic_role(bucket, "review"),
                "value": float(review_line["value"]),
                "numeric_value": float(review_line["value"]),
                "plain_language_meaning": _review_meaning(signal_card, thresholds) or "Review line marks where the brief stays under review.",
                "priority": 3,
                "visible_by_default": False,
                "render_mode": _base_threshold_render_mode(_threshold_semantic_role(bucket, "review")),
                "visual_priority": "tertiary",
                "visible_in_overview": False,
                "visible_in_focus": True,
                "hover_enabled": True,
                "legend_enabled": True,
            }
        )
    for key, which, meaning_fn in (
        ("confirm_line", "confirm", _confirm_meaning),
        ("break_line", "break", _break_meaning),
    ):
        line = (thresholds.get(key) or {}) if isinstance(thresholds.get(key), dict) else {}
        if line.get("value") is None:
            continue
        role = _threshold_semantic_role(bucket, which)
        lines.append(
            {
                "id": key,
                "threshold_id": key,
                "label": str(line.get("label") or _threshold_label(bucket, which)).strip(),
                "semantic_role": role,
                "value": float(line["value"]),
                "numeric_value": float(line["value"]),
                "plain_language_meaning": meaning_fn(signal_card, thresholds) or str(line.get("note") or "").strip() or str(line.get("label") or key).strip(),
                "priority": _threshold_priority(role),
                "visible_by_default": _threshold_visible_default(role),
                "render_mode": _base_threshold_render_mode(role),
                "visual_priority": "primary" if _threshold_priority(role) <= 1 else "secondary",
                "visible_in_overview": _threshold_visible_default(role),
                "visible_in_focus": True,
                "hover_enabled": True,
                "legend_enabled": True,
            }
        )
    return lines


def _threshold_overlap_mode(points: list[dict[str, Any]], threshold_lines: list[dict[str, Any]]) -> str:
    if len(points) < 2 or len(threshold_lines) < 2:
        return "separate_lines"
    values = [float(point["value"]) for point in points]
    span = max(max(values) - min(values), max(abs(values[-1]) * 0.02, 0.25))
    ordered = sorted(float(item["numeric_value"]) for item in threshold_lines)
    min_distance = min(abs(right - left) for left, right in zip(ordered, ordered[1:]))
    normalized = min_distance / span if span else 1.0
    if normalized <= 0.03:
        return "merge_to_zone"
    if normalized <= 0.055:
        return "hide_secondary_line_from_plot_show_in_legend"
    return "separate_lines"


def _apply_threshold_overlap(
    threshold_lines: list[dict[str, Any]],
    *,
    overlap_mode: str,
) -> list[dict[str, Any]]:
    if overlap_mode == "separate_lines" or len(threshold_lines) < 2:
        return threshold_lines
    ordered = sorted(threshold_lines, key=lambda item: (int(item.get("priority") or 9), str(item.get("threshold_id") or "")))
    visible_ids = {ordered[0]["threshold_id"]} if overlap_mode == "hide_secondary_line_from_plot_show_in_legend" else set()
    patched: list[dict[str, Any]] = []
    for item in threshold_lines:
        next_item = dict(item)
        if overlap_mode == "merge_to_zone":
            next_item["visible_by_default"] = False
            next_item["visible_in_overview"] = False
            next_item["visible_in_focus"] = True
            next_item["render_mode"] = "merged_zone"
        elif overlap_mode == "hide_secondary_line_from_plot_show_in_legend":
            next_item["visible_by_default"] = item["threshold_id"] in visible_ids
            next_item["visible_in_overview"] = item["threshold_id"] in visible_ids
            next_item["visible_in_focus"] = True
            if item["threshold_id"] not in visible_ids:
                next_item["render_mode"] = "legend_only"
        patched.append(next_item)
    return patched


def _merged_threshold_zone(threshold_lines: list[dict[str, Any]], overlap_mode: str) -> dict[str, Any] | None:
    if overlap_mode != "merge_to_zone" or len(threshold_lines) < 2:
        return None
    values = [float(item["numeric_value"]) for item in threshold_lines]
    labels = [str(item["label"] or "").strip() for item in threshold_lines[:2]]
    return {
        "min": round(min(values), 4),
        "max": round(max(values), 4),
        "label": "Decision zone",
        "note": f"{' and '.join(label for label in labels if label)} are close enough that they are shown as one zone.",
    }


def _delta_relation_label(current_value: float, threshold_value: float, label: str) -> tuple[str, str, float]:
    delta = current_value - threshold_value
    tolerance = max(abs(threshold_value) * 0.01, 0.2)
    if abs(delta) <= tolerance:
        return (f"near {label.lower()}", "near", delta)
    direction = "above" if delta > 0 else "below"
    return (f"{abs(delta):.2f} {direction} {label.lower()}", direction, delta)


def _distance_to_thresholds(
    current_value: float | None,
    threshold_lines: list[dict[str, Any]],
    review_band: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if current_value is None:
        return []
    items: list[dict[str, Any]] = []
    if review_band is not None:
        minimum = float(review_band["min"])
        maximum = float(review_band["max"])
        if minimum <= current_value <= maximum:
            relation_label = "still inside review band"
            relation = "inside"
        elif current_value > maximum:
            relation_label = f"{abs(current_value - maximum):.2f} above review band"
            relation = "above"
        else:
            relation_label = f"{abs(current_value - minimum):.2f} below review band"
            relation = "below"
        items.append(
            {
                "threshold_id": str(review_band.get("band_id") or "review_band"),
                "label": str(review_band.get("label") or "Review band"),
                "relation_label": relation_label,
                "relation": relation,
                "delta_value": 0.0 if relation == "inside" else abs(current_value - (maximum if current_value > maximum else minimum)),
            }
        )
    for line in threshold_lines:
        label, relation, delta = _delta_relation_label(current_value, float(line["numeric_value"]), str(line["label"]))
        items.append(
            {
                "threshold_id": str(line["threshold_id"]),
                "label": str(line["label"]),
                "relation_label": label,
                "relation": relation,
                "delta_value": abs(delta),
            }
        )
    return items


def _current_relation_status(
    distances: list[dict[str, Any]],
) -> str | None:
    if not distances:
        return None
    review_item = next((item for item in distances if str(item.get("threshold_id")) == "review_band"), None)
    threshold_items = [item for item in distances if str(item.get("threshold_id")) != "review_band"]
    nearest_threshold = min(threshold_items, key=lambda item: float(item.get("delta_value") or 0.0)) if threshold_items else None
    if review_item and str(review_item.get("relation")) == "inside" and nearest_threshold is not None:
        return str(nearest_threshold.get("relation_label") or review_item.get("relation_label") or "").strip() or None
    if review_item is not None:
        return str(review_item.get("relation_label") or "").strip() or None
    if nearest_threshold is not None:
        return str(nearest_threshold.get("relation_label") or "").strip() or None
    return None


def _inspection_points(
    observed_points: list[dict[str, Any]],
    forecast_points: list[dict[str, Any]],
    threshold_lines: list[dict[str, Any]],
    review_band: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    timestamps: list[str] = []
    for point in [*observed_points, *forecast_points]:
        timestamp = str(point.get("timestamp") or "").strip()
        if timestamp and timestamp not in timestamps:
            timestamps.append(timestamp)
    observed_lookup = {str(point["timestamp"]): float(point["value"]) for point in observed_points}
    forecast_lookup = {str(point["timestamp"]): float(point["value"]) for point in forecast_points}
    snapshots: list[dict[str, Any]] = []
    for timestamp in timestamps:
        observed_value = observed_lookup.get(timestamp)
        forecast_value = forecast_lookup.get(timestamp)
        inspected_value = observed_value if observed_value is not None else forecast_value
        distances = _distance_to_thresholds(inspected_value, threshold_lines, review_band)
        snapshots.append(
            {
                "timestamp": timestamp,
                "observed_value": observed_value,
                "forecast_value": forecast_value,
                "current_relation_status": _current_relation_status(distances),
                "distance_to_thresholds": distances,
            }
        )
    return snapshots


def _relation_priority_order(distances: list[dict[str, Any]]) -> list[str]:
    ordered = sorted(
        distances,
        key=lambda item: (
            1 if str(item.get("threshold_id")) == "review_band" else 0,
            float(item.get("delta_value") or 0.0),
            str(item.get("threshold_id") or ""),
        ),
    )
    return [str(item.get("threshold_id") or "") for item in ordered if str(item.get("threshold_id") or "").strip()]


def _nearest_threshold(distances: list[dict[str, Any]]) -> dict[str, Any] | None:
    threshold_items = [item for item in distances if str(item.get("threshold_id") or "") != "review_band"]
    if threshold_items:
        return min(threshold_items, key=lambda item: float(item.get("delta_value") or 0.0))
    return distances[0] if distances else None


def _forecast_relative_direction(forecast_points: list[dict[str, Any]]) -> str | None:
    if len(forecast_points) < 2:
        return None
    start = float(forecast_points[0]["value"])
    end = float(forecast_points[-1]["value"])
    delta = end - start
    tolerance = max(abs(start) * 0.0025, 0.05)
    if abs(delta) <= tolerance:
        return "staying flat"
    return "drifting higher" if delta > 0 else "drifting lower"


def _forecast_delta_label(left: float | None, right: float | None, *, fallback: str | None = None) -> str | None:
    if left is None or right is None:
        return fallback
    delta = left - right
    tolerance = max(abs(right) * 0.01, 0.2)
    if abs(delta) <= tolerance:
        return f"near {fallback or 'key line'}".strip()
    direction = "above" if delta > 0 else "below"
    return f"{abs(delta):.2f} {direction} {fallback or 'key line'}".strip()


def _forecast_strength_label(forecast_overlay: dict[str, Any] | None) -> str | None:
    if not forecast_overlay:
        return None
    direction = str(forecast_overlay.get("forecast_relative_direction") or "").strip().lower()
    if direction == "drifting higher":
        return "higher drift"
    if direction == "drifting lower":
        return "lower drift"
    if direction == "staying flat":
        return "flat outlook"
    support_strength = str(forecast_overlay.get("support_strength") or "").strip().lower()
    if support_strength:
        return f"{support_strength} outlook"
    return None


def _forecast_visibility_mode(forecast_overlay: dict[str, Any] | None) -> str:
    if not forecast_overlay or len(list(forecast_overlay.get("point_path") or [])) < 5:
        return "disabled"
    support_strength = str(forecast_overlay.get("support_strength") or "").strip().lower()
    if support_strength in {"strong", "moderate", "tight interval support"}:
        return "emphasized"
    return "contextual"


def _enrich_forecast_overlay(
    forecast_overlay: dict[str, Any] | None,
    *,
    current_point: dict[str, Any] | None,
    threshold_lines: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not forecast_overlay:
        return None
    point_path = list(forecast_overlay.get("point_path") or [])
    relative_direction = _forecast_relative_direction(point_path)
    current_value = _safe_float((current_point or {}).get("value"))
    final_forecast = _safe_float(point_path[-1]["value"]) if point_path else None
    primary_line = sorted(threshold_lines, key=lambda item: int(item.get("priority") or 9))[0] if threshold_lines else None
    key_label = str(primary_line.get("label") or "").lower() if primary_line else "key line"
    key_value = _safe_float(primary_line.get("numeric_value")) if primary_line else None
    return {
        **forecast_overlay,
        "forecast_label": "Forecast path",
        "forecast_confidence_mode": "banded" if forecast_overlay.get("lower_band") and forecast_overlay.get("upper_band") else "line_only",
        "forecast_relative_direction": relative_direction,
        "forecast_vs_current_delta": _forecast_delta_label(final_forecast, current_value, fallback="current"),
        "forecast_vs_key_line_delta": _forecast_delta_label(final_forecast, key_value, fallback=key_label),
        "forecast_strength_label": _forecast_strength_label(
            {
                **forecast_overlay,
                "forecast_relative_direction": relative_direction,
            }
        ),
        "forecast_visibility_mode": _forecast_visibility_mode(forecast_overlay),
        "forecast_comparison_label": (
            f"{relative_direction} {(_forecast_delta_label(final_forecast, key_value, fallback=key_label) or 'near the key line')}".strip()
            if relative_direction
            else _forecast_delta_label(final_forecast, key_value, fallback=key_label)
        ),
    }


def _focus_domain(values: list[float | None]) -> dict[str, Any] | None:
    usable = [float(value) for value in values if value is not None]
    if not usable:
        return None
    minimum = min(usable)
    maximum = max(usable)
    span = maximum - minimum
    anchor = max(abs(minimum), abs(maximum), 1.0)
    padding = max(span * 0.22, anchor * 0.012, 0.08)
    return {
        "min": round(minimum - padding, 4),
        "max": round(maximum + padding, 4),
    }


def _focus_group_label(signal_card: dict[str, Any], kind: str) -> str:
    family = _family_key(signal_card)
    labels = {
        ("rates", "path"): "Observed vs forecast",
        ("rates", "decision"): "Rates pressure lines",
        ("credit", "path"): "Observed vs forecast",
        ("credit", "decision"): "Credit stress lines",
        ("breadth", "path"): "Observed vs forecast",
        ("breadth", "decision"): "Breadth confirmation lines",
        ("fx", "path"): "Observed vs forecast",
        ("fx", "decision"): "Dollar hurdle lines",
        ("commodity", "path"): "Observed vs forecast",
        ("commodity", "decision"): "Commodity pressure lines",
    }
    return labels.get((family, kind), "Observed vs forecast" if kind == "path" else "Decision lines")


def _focusable_threshold_groups(
    signal_card: dict[str, Any],
    *,
    observed_series: dict[str, Any] | None,
    forecast_series: dict[str, Any] | None,
    review_band: dict[str, Any] | None,
    threshold_lines: list[dict[str, Any]],
    overlap_mode: str,
    current_point: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    observed_id = str((observed_series or {}).get("series_id") or "").strip()
    forecast_id = str((forecast_series or {}).get("series_id") or "").strip()
    review_id = str((review_band or {}).get("band_id") or "").strip()
    threshold_ids = [str(item.get("threshold_id") or "") for item in threshold_lines if str(item.get("threshold_id") or "").strip()]
    observed_points = list((observed_series or {}).get("points") or [])
    forecast_points = list((forecast_series or {}).get("points") or [])
    current_value = _safe_float((current_point or {}).get("value"))
    final_forecast = _safe_float(forecast_points[-1]["value"]) if forecast_points else None

    path_members = [item for item in [observed_id, forecast_id, review_id] if item]
    if path_members:
        groups.append(
            {
                "group_id": "observed_forecast_group",
                "group_label": _focus_group_label(signal_card, "path"),
                "member_line_ids": path_members,
                "suggested_y_domain": _focus_domain(
                    [
                        current_value,
                        final_forecast,
                        *[_safe_float(point.get("value")) for point in observed_points[-10:]],
                        *[_safe_float(point.get("value")) for point in forecast_points],
                    ]
                ),
                "primary_line_id": observed_id or None,
                "secondary_line_ids": [item for item in [forecast_id] if item] or None,
                "can_split_from_zone": False,
            }
        )

    decision_members = [item for item in [observed_id, forecast_id, review_id, *threshold_ids] if item]
    if decision_members and threshold_ids:
        threshold_values = [_safe_float(item.get("numeric_value")) for item in threshold_lines]
        groups.append(
            {
                "group_id": "decision_lines_group",
                "group_label": _focus_group_label(signal_card, "decision"),
                "member_line_ids": decision_members,
                "suggested_y_domain": _focus_domain(
                    [
                        current_value,
                        final_forecast,
                        *threshold_values,
                        _safe_float((review_band or {}).get("min")),
                        _safe_float((review_band or {}).get("max")),
                    ]
                ),
                "primary_line_id": threshold_ids[0] if threshold_ids else observed_id or None,
                "secondary_line_ids": threshold_ids[1:] or ([forecast_id] if forecast_id else None),
                "can_split_from_zone": overlap_mode in {"merge_to_zone", "hide_secondary_line_from_plot_show_in_legend"},
            }
        )
    return groups


def _focus_reason(signal_card: dict[str, Any], *, overlap_mode: str, forecast_ready: bool) -> str:
    family = _family_key(signal_card)
    subject = {
        "rates": "Focus mode separates the rate lines",
        "credit": "Focus mode separates the funding lines",
        "breadth": "Focus mode separates the breadth lines",
        "fx": "Focus mode separates the dollar lines",
        "commodity": "Focus mode separates the commodity lines",
    }.get(family, "Focus mode separates the decision lines")
    if overlap_mode in {"merge_to_zone", "hide_secondary_line_from_plot_show_in_legend"} and forecast_ready:
        return f"{subject} and makes the forecast easier to compare."
    if overlap_mode in {"merge_to_zone", "hide_secondary_line_from_plot_show_in_legend"}:
        return f"{subject} so the hidden threshold can be inspected directly."
    if forecast_ready:
        return f"{subject} and tightens the chart around the forecast and threshold relationship."
    return f"{subject} and tightens the chart around the active decision levels."


def _decision_references(
    threshold_lines: list[dict[str, Any]],
    *,
    current_vs_thresholds: list[dict[str, Any]],
    forecast_vs_thresholds: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    current_map = _relation_by_threshold_id(current_vs_thresholds)
    forecast_map = _relation_by_threshold_id(forecast_vs_thresholds)
    references: list[dict[str, Any]] = []
    for line in threshold_lines:
        threshold_id = str(line.get("threshold_id") or "").strip()
        if not threshold_id:
            continue
        references.append(
            {
                **line,
                "object_role": "decision_reference",
                "object_type": "reference_line",
                "visible_in_overview": bool(line.get("visible_in_overview", line.get("visible_by_default"))),
                "visible_in_focus": bool(line.get("visible_in_focus", True)),
                "hover_enabled": bool(line.get("hover_enabled", True)),
                "legend_enabled": bool(line.get("legend_enabled", True)),
                "current_relation_label": str((current_map.get(threshold_id) or {}).get("relation_label") or "").strip() or None,
                "forecast_relation_label": str((forecast_map.get(threshold_id) or {}).get("relation_label") or "").strip() or None,
            }
        )
    return references


def _focus_modes(
    *,
    observed_path: dict[str, Any] | None,
    forecast_path: dict[str, Any] | None,
    review_context: dict[str, Any] | None,
    decision_references: list[dict[str, Any]],
    focus_groups: list[dict[str, Any]],
    threshold_overlap_mode: str,
) -> list[dict[str, Any]]:
    observed_id = str((observed_path or {}).get("series_id") or "").strip()
    forecast_id = str((forecast_path or {}).get("series_id") or "").strip()
    review_id = str((review_context or {}).get("band_id") or "").strip()
    decision_ids = [str(item.get("threshold_id") or "") for item in decision_references if str(item.get("threshold_id") or "").strip()]
    overview_ids: list[str] = [item for item in [observed_id, forecast_id, review_id] if item]
    if threshold_overlap_mode == "merge_to_zone":
        overview_ids.append("decision_zone")
    else:
        overview_ids.extend([item for item in decision_ids if item and next((ref for ref in decision_references if ref.get("threshold_id") == item), {}).get("visible_in_overview")])

    modes: list[dict[str, Any]] = [
        {
            "mode_id": "overview",
            "mode_label": "Overview",
            "primary_object_roles": ["observed_path"],
            "secondary_object_roles": [
                role
                for role in ["forecast_path", "review_context", "decision_reference"]
                if (
                    role != "forecast_path"
                    or forecast_id
                )
            ],
            "hidden_object_roles": [],
            "visible_object_ids": overview_ids,
            "y_domain": None,
            "tooltip_role": "overview",
            "legend_state": "overview",
        }
    ]
    for group in focus_groups:
        group_id = str(group.get("group_id") or "").strip()
        if not group_id:
            continue
        if group_id == "observed_forecast_group":
            primary_roles = ["observed_path", "forecast_path"]
            secondary_roles = ["review_context"]
            hidden_roles = ["decision_reference"]
            tooltip_role = "path_compare"
        else:
            primary_roles = ["decision_reference"]
            secondary_roles = ["observed_path", "review_context", "forecast_path"]
            hidden_roles = []
            tooltip_role = "decision_compare"
        modes.append(
            {
                "mode_id": group_id,
                "mode_label": str(group.get("group_label") or "Focus").strip() or "Focus",
                "primary_object_roles": primary_roles,
                "secondary_object_roles": secondary_roles,
                "hidden_object_roles": hidden_roles,
                "visible_object_ids": list(group.get("member_line_ids") or []),
                "y_domain": group.get("suggested_y_domain"),
                "tooltip_role": tooltip_role,
                "legend_state": "focus",
            }
        )
    return modes


def _hover_payloads(
    inspection_points: list[dict[str, Any]],
    *,
    review_band: dict[str, Any] | None,
    decision_references: list[dict[str, Any]],
    chart_takeaway: str | None,
    forecast_overlay: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    last_observed_timestamp = next(
        (str(item.get("timestamp") or "") for item in reversed(inspection_points) if item.get("observed_value") is not None),
        None,
    )
    forecast_points = list((forecast_overlay or {}).get("point_path") or [])
    next_forecast_point = forecast_points[0] if forecast_points else None
    for item in inspection_points:
        timestamp = str(item.get("timestamp") or "").strip()
        if not timestamp:
            continue
        observed_value = _safe_float(item.get("observed_value"))
        forecast_value = _safe_float(item.get("forecast_value"))
        if observed_value is not None and forecast_value is None and timestamp == last_observed_timestamp and next_forecast_point is not None:
            forecast_value = _safe_float(next_forecast_point.get("value"))
        distances = list(item.get("distance_to_thresholds") or [])
        reference_values = [
            {
                "threshold_id": str(line.get("threshold_id") or ""),
                "label": str(line.get("label") or "Threshold"),
                "value": float(line.get("numeric_value")),
            }
            for line in decision_references
            if _safe_float(line.get("numeric_value")) is not None
        ]
        relation_statements: list[dict[str, Any]] = []
        for priority, relation in enumerate(
            sorted(
                distances,
                key=lambda entry: (
                    1 if str(entry.get("threshold_id")) == "review_band" else 0,
                    float(entry.get("delta_value") or 0.0),
                    str(entry.get("threshold_id") or ""),
                )
            )
        ):
            threshold_id = str(relation.get("threshold_id") or "").strip()
            if not threshold_id:
                continue
            statement = str(relation.get("relation_label") or "").strip()
            if not statement:
                continue
            relation_statements.append(
                {
                    "threshold_id": threshold_id,
                    "statement": statement,
                    "priority": priority,
                }
            )
        payloads.append(
            {
                "timestamp": timestamp,
                "observed_value": observed_value,
                "forecast_value": forecast_value,
                "review_band": (
                    {
                        "label": str(review_band.get("label") or "Review band"),
                        "min": float(review_band["min"]),
                        "max": float(review_band["max"]),
                    }
                    if review_band is not None
                    else None
                ),
                "reference_values": reference_values,
                "relation_statements": relation_statements[:2],
                "implication": (
                    str((forecast_overlay or {}).get("forecast_comparison_label") or "").strip()
                    if forecast_value is not None and str((forecast_overlay or {}).get("forecast_comparison_label") or "").strip()
                    else chart_takeaway
                ),
            }
        )
    return payloads


def _takeaway_relation_phrase(item: dict[str, Any] | None) -> str | None:
    if not item:
        return None
    label = str(item.get("label") or "").strip().lower()
    relation = str(item.get("relation") or "").strip().lower()
    delta = _safe_float(item.get("delta_value"))
    threshold_id = str(item.get("threshold_id") or "").strip()
    if threshold_id == "review_band":
        if relation == "inside":
            return "inside the review band"
        if delta is not None:
            return f"{delta:.2f} {relation} the review band"
        return str(item.get("relation_label") or "").strip() or None
    if relation == "near":
        if delta is not None and delta <= 0.05:
            return f"almost on the {label}"
        if delta is not None:
            return f"{delta:.2f} from the {label}"
        return f"near the {label}"
    if delta is not None:
        return f"{delta:.2f} {relation} the {label}"
    return str(item.get("relation_label") or "").strip() or None


def _relation_by_threshold_id(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("threshold_id") or "").strip(): item
        for item in items
        if str(item.get("threshold_id") or "").strip()
    }


def _chart_takeaway(
    signal_card: dict[str, Any],
    *,
    current_vs_thresholds: list[dict[str, Any]],
    forecast_overlay: dict[str, Any] | None,
) -> str:
    family = _family_key(signal_card)
    relation_map = _relation_by_threshold_id(current_vs_thresholds)
    review_phrase = _takeaway_relation_phrase(relation_map.get("review_band"))
    primary_phrase = _takeaway_relation_phrase(relation_map.get("confirm_line"))
    secondary_phrase = _takeaway_relation_phrase(relation_map.get("break_line"))
    forecast_clause = str((forecast_overlay or {}).get("forecast_comparison_label") or "").strip()

    lead_parts = [part for part in [review_phrase, primary_phrase, secondary_phrase] if part]
    relationship = ", ".join(lead_parts) if lead_parts else "still under review"
    forecast_sentence = f" Forecast {forecast_clause}." if forecast_clause else ""

    if family == "rates":
        return f"Rates are {relationship}, so adding more long bonds should stay selective until yields ease more clearly.{forecast_sentence}".strip()
    if family == "credit":
        return f"Credit is {relationship}, so funding conditions still look too tight for a broader risk add.{forecast_sentence}".strip()
    if family == "breadth":
        return f"Breadth is {relationship}, so broader equity adds still need better participation before they look durable.{forecast_sentence}".strip()
    if family == "fx":
        return f"The dollar is {relationship}, so global risk adds still face a real hurdle for now.{forecast_sentence}".strip()
    if family == "commodity":
        symbol = str(signal_card.get("symbol") or "").strip().upper()
        if symbol == "CL=F":
            return f"Oil is {relationship}, so the hedge case is staying alive more than it is strengthening into a stand-alone commodity call.{forecast_sentence}".strip()
        return f"The protection signal is {relationship}, so hedge demand still deserves attention for now.{forecast_sentence}".strip()
    return f"The signal is {relationship}, so the brief should stay cautious until confirmation improves.{forecast_sentence}".strip()


def _future_timestamps(last_timestamp: str | None, count: int) -> list[str]:
    anchor = _parse_dt(last_timestamp) or datetime.now(tz=UTC)
    return [_iso(anchor + timedelta(days=index + 1)) for index in range(max(count, 0))]


def _forecast_overlay(signal_card: dict[str, Any], *, density_profile: str) -> dict[str, Any] | None:
    if density_profile not in {"rich_line", "compact_line"}:
        return None
    bundle = _bundle(signal_card)
    support = getattr(bundle, "support", None) if bundle else None
    result = getattr(bundle, "result", None) if bundle else None
    if support is None or result is None:
        return None
    support_strength = str(getattr(support, "support_strength", "") or "").strip().lower()
    degraded_state = str(getattr(support, "degraded_state", "") or "").strip()
    uncertainty_width = str(getattr(support, "uncertainty_width_label", "") or "").strip().lower()
    if degraded_state:
        return None
    if support_strength not in {"strong", "moderate", "tight_interval_support"}:
        return None
    if uncertainty_width in {"wide", "bounded"} and float(getattr(support, "trigger_pressure", 0.0) or 0.0) < 0.58:
        return None
    point_path = [float(value) for value in list(getattr(result, "point_path", []) or [])]
    if len(point_path) < 5:
        return None
    quantiles = dict(getattr(result, "quantiles", {}) or {})
    lower_band = [float(value) for value in list(quantiles.get("0.1") or [])]
    upper_band = [float(value) for value in list(quantiles.get("0.9") or [])]
    base_points = _signal_history_points(signal_card, max_points=21)
    last_timestamp = base_points[-1]["timestamp"] if base_points else str(signal_card.get("as_of") or "")
    future_timestamps = _future_timestamps(last_timestamp, len(point_path))
    return {
        "point_path": [_round_point(future_timestamps[index], value) for index, value in enumerate(point_path)],
        "lower_band": (
            [_round_point(future_timestamps[index], value) for index, value in enumerate(lower_band[: len(future_timestamps)])]
            if lower_band and density_profile == "rich_line"
            else None
        ),
        "upper_band": (
            [_round_point(future_timestamps[index], value) for index, value in enumerate(upper_band[: len(future_timestamps)])]
            if upper_band and density_profile == "rich_line"
            else None
        ),
        "horizon_label": f"next {int(getattr(support, 'horizon', len(point_path)) or len(point_path))} trading days",
        "support_strength": str(getattr(support, "scenario_support_strength", None) or getattr(support, "support_strength", "") or "usable").replace("_", " ").strip(),
        "forecast_start_timestamp": future_timestamps[0] if future_timestamps else None,
        "forecast_end_timestamp": future_timestamps[-1] if future_timestamps else None,
        "visible_by_default": True,
    }


def _source_validity_footer(signal_card: dict[str, Any], *, proxy_note: str | None = None) -> str:
    source_and_validity = str(signal_card.get("source_and_validity") or "").strip()
    if source_and_validity:
        return f"{source_and_validity} {proxy_note}".strip() if proxy_note else source_and_validity
    freshness = str(signal_card.get("freshness_label") or signal_card.get("freshness_state") or "current").strip()
    evidence_class = str(signal_card.get("evidence_class") or signal_card.get("source_kind") or "market evidence").replace("_", " ").strip()
    footer = f"{freshness} · {evidence_class}"
    return f"{footer} · {proxy_note}".strip() if proxy_note else footer


def _chart_horizon(points: list[dict[str, Any]], signal_card: dict[str, Any]) -> dict[str, Any]:
    config = _family_config(signal_card)
    available = list(config.get("available_horizons") or ["1M"])
    if len(points) < 45 and "3M" in available:
        available = [item for item in available if item != "3M"]
    selected = str(config.get("selected_horizon") or "1M")
    if selected not in available:
        selected = available[0] if available else "Recent"
    return {"selected": selected, "available": available or ["Recent"]}


def _slice_points_for_horizon(points: list[dict[str, Any]], selected: str) -> list[dict[str, Any]]:
    if selected == "3M":
        return points[-63:]
    if selected == "1M":
        return points[-21:]
    return points


def _current_point(signal_card: dict[str, Any], points: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not points:
        return None
    current_value = _safe_float(signal_card.get("current_value"))
    latest = points[-1]
    return {
        "value": current_value if current_value is not None else latest["value"],
        "timestamp": latest["timestamp"],
        "label": str(signal_card.get("signal_label") or signal_card.get("label") or "Current reading"),
    }


def _threshold_question(signal_card: dict[str, Any]) -> str:
    bucket = _bucket_key(signal_card)
    if str(signal_card.get("symbol") or "").strip().upper() == "CL=F":
        return "Is oil still holding above the level that keeps the inflation hedge case alive"
    if str(signal_card.get("symbol") or "").strip().upper() == "DFII10":
        return "Are real yields still high enough to keep bond adds selective"
    if _family_key(signal_card) == "breadth":
        return "Is breadth broadening enough to support a durable risk-on read"
    return _QUESTION_BY_BUCKET.get(bucket, _QUESTION_BY_BUCKET["market"])


def _density_profile(
    signal_card: dict[str, Any],
    points: list[dict[str, Any]],
    thresholds: dict[str, Any] | None,
    confirmation_items: list[dict[str, Any]],
) -> str:
    config = _family_config(signal_card)
    if _source_class(signal_card) in {"policy_event", "geopolitical_news"}:
        usable = [item for item in confirmation_items if item["status"] in {"confirming", "resisting", "neutral"}]
        return "strip_only" if usable else "suppressed"
    if not points or not thresholds:
        usable_strip = [item for item in confirmation_items if item["status"] != "missing"]
        return "strip_only" if len(usable_strip) >= 3 else "suppressed"
    spacing_score = _line_spacing_score(points, thresholds)
    visible_thresholds = _threshold_line_count(thresholds)
    if len(points) >= int(config.get("rich_min_points") or 8) and spacing_score >= 0.08 and visible_thresholds >= 2:
        return "rich_line"
    if len(points) >= int(config.get("compact_min_points") or 4) and visible_thresholds >= 1:
        return "compact_line"
    usable_strip = [item for item in confirmation_items if item["status"] != "missing"]
    if len(usable_strip) >= 3:
        return "strip_only"
    return "suppressed"


def _comparison_series(signal_card: dict[str, Any], *, limit: int = 2) -> list[dict[str, Any]]:
    specs = _preferred_confirmation_specs(signal_card)[:limit]
    series: list[dict[str, Any]] = []
    for spec in specs:
        symbol = str(spec.get("symbol") or "").strip().upper()
        points, _proxy_note = _truth_history_points({"symbol": symbol, "primary_effect_bucket": _bucket_key(signal_card)}, target_points=21)
        if len(points) < 4:
            continue
        series.append(
            {
                "series_id": f"{signal_card.get('signal_id') or 'signal'}_{symbol.lower()}",
                "label": str(spec.get("label") or _symbol_label(symbol, symbol)),
                "unit": "level",
                "source_label": str(spec.get("note") or "").strip() or None,
                "points": points[-21:],
            }
        )
    return series


def _build_threshold_line_payload(signal_card: dict[str, Any]) -> dict[str, Any]:
    config = _family_config(signal_card)
    raw_points, proxy_note = _primary_history_points(signal_card, target_points=int(config.get("history_target_points") or 21))
    horizon = _chart_horizon(raw_points, signal_card)
    points = _slice_points_for_horizon(raw_points, horizon["selected"])
    confirmation_items = _confirmation_items(signal_card)
    confirmation_state = _confirmation_state(signal_card, confirmation_items)
    thresholds = _threshold_payload(signal_card, points) if len(points) >= 2 else None
    density = _density_profile(signal_card, points, thresholds, confirmation_items)
    path_state = _path_state(signal_card, points, thresholds) if points and thresholds else "history too thin"
    current_point = _current_point(signal_card, points)
    review_band = _review_band_object(signal_card, thresholds)
    threshold_lines = _threshold_lines(signal_card, thresholds)
    overlap_mode = _threshold_overlap_mode(points, threshold_lines)
    threshold_lines = _apply_threshold_overlap(threshold_lines, overlap_mode=overlap_mode)
    merged_zone = _merged_threshold_zone(threshold_lines, overlap_mode)
    if merged_zone is not None and thresholds is not None:
        thresholds = {
            **thresholds,
            "trigger_zone": merged_zone,
        }
    forecast_overlay = _enrich_forecast_overlay(
        _forecast_overlay(signal_card, density_profile=density),
        current_point=current_point,
        threshold_lines=threshold_lines,
    )
    summary = _summary_metrics(signal_card, points, thresholds, confirmation_state, path_state) if points else [
        {"label": "Current", "value": _format_level(_safe_float(signal_card.get("current_value"))), "tone": "neutral"},
        {"label": "Position", "value": "history too thin", "tone": "review"},
        {"label": "Confirmation", "value": _human_confirmation_state(confirmation_state), "tone": "support"},
        {"label": "Path", "value": "history too thin", "tone": "warn"},
    ]
    chart_question = _threshold_question(signal_card)
    source_validity_footer = _source_validity_footer(signal_card, proxy_note=proxy_note)
    chart_explainer_lines = _chart_explainer_lines(signal_card, thresholds=thresholds, forecast_overlay=forecast_overlay)
    if overlap_mode == "merge_to_zone" and merged_zone is not None:
        chart_explainer_lines = [
            _observed_guide_text(signal_card),
            _forecast_guide_text(signal_card, forecast_overlay),
            _review_guide_text(signal_card, thresholds),
            "Decision zone shows where the decision lines are too close to separate cleanly.",
            _break_guide_text(signal_card, thresholds),
        ]
        chart_explainer_lines = [line for line in chart_explainer_lines if str(line or "").strip()]
    distance_to_thresholds = _distance_to_thresholds(_safe_float((current_point or {}).get("value")), threshold_lines, review_band)
    forecast_final_value = _safe_float(((forecast_overlay or {}).get("point_path") or [{}])[-1].get("value")) if (forecast_overlay or {}).get("point_path") else None
    forecast_vs_thresholds = _distance_to_thresholds(forecast_final_value, threshold_lines, review_band)
    nearest_threshold = _nearest_threshold(distance_to_thresholds)
    relation_priority_order = _relation_priority_order(distance_to_thresholds)
    chart_takeaway = _chart_takeaway(
        signal_card,
        current_vs_thresholds=distance_to_thresholds,
        forecast_overlay=forecast_overlay,
    )
    observed_series = {
        "series_id": f"{signal_card.get('signal_id') or 'signal'}_observed",
        "label": "Observed",
        "unit": "level",
        "source_label": str(signal_card.get("signal_kind") or "market").replace("_", " ").strip(),
        "plain_language_meaning": _observed_path_explainer(signal_card),
        "visible_by_default": True,
        "points": points,
    } if points else None
    observed_path = {
        **observed_series,
        "object_role": "observed_path",
        "object_type": "observed_timeseries",
    } if observed_series else None
    forecast_series = {
        "series_id": f"{signal_card.get('signal_id') or 'signal'}_forecast",
        "label": str((forecast_overlay or {}).get("forecast_label") or "Forecast"),
        "unit": "level",
        "source_label": str((forecast_overlay or {}).get("support_strength") or "forecast").strip() or None,
        "plain_language_meaning": _forecast_path_explainer(signal_card, forecast_overlay),
        "visible_by_default": bool((forecast_overlay or {}).get("visible_by_default", True)),
        "points": list((forecast_overlay or {}).get("point_path") or []),
    } if forecast_overlay and list((forecast_overlay or {}).get("point_path") or []) else None
    forecast_path = {
        **forecast_series,
        "object_role": "forecast_path",
        "object_type": "forecast_timeseries",
        "forecast_start_timestamp": (forecast_overlay or {}).get("forecast_start_timestamp"),
        "forecast_end_timestamp": (forecast_overlay or {}).get("forecast_end_timestamp"),
        "forecast_relative_direction": (forecast_overlay or {}).get("forecast_relative_direction"),
        "forecast_strength_label": (forecast_overlay or {}).get("forecast_strength_label"),
        "forecast_comparison_label": (forecast_overlay or {}).get("forecast_comparison_label"),
        "forecast_visibility_mode": (forecast_overlay or {}).get("forecast_visibility_mode"),
        "forecast_confidence_band": {
            "lower_band": (forecast_overlay or {}).get("lower_band"),
            "upper_band": (forecast_overlay or {}).get("upper_band"),
        },
    } if forecast_series else None
    chart_guide_items = _chart_guide_items(
        signal_card,
        forecast_overlay=forecast_overlay,
        review_band=review_band,
        threshold_lines=threshold_lines,
        overlap_mode=overlap_mode,
        merged_zone=merged_zone,
    )
    inspection_points = _inspection_points(
        list((observed_series or {}).get("points") or []),
        list((forecast_series or {}).get("points") or []),
        threshold_lines,
        review_band,
    )
    focus_groups = _focusable_threshold_groups(
        signal_card,
        observed_series=observed_series,
        forecast_series=forecast_series,
        review_band=review_band,
        threshold_lines=threshold_lines,
        overlap_mode=overlap_mode,
        current_point=current_point,
    )
    focus_default_group = (focus_groups[1]["group_id"] if len(focus_groups) > 1 else focus_groups[0]["group_id"]) if focus_groups else None
    focus_y_domain = next((group.get("suggested_y_domain") for group in focus_groups if group.get("group_id") == focus_default_group), None)
    focus_split_available = any(bool(group.get("can_split_from_zone")) for group in focus_groups)
    inspectable_thresholds = [str(item.get("threshold_id") or "") for item in threshold_lines if str(item.get("threshold_id") or "").strip()]
    forecast_focus_ready = bool(forecast_series and len(list((forecast_series or {}).get("points") or [])) >= 5)
    focus_reason = _focus_reason(signal_card, overlap_mode=overlap_mode, forecast_ready=forecast_focus_ready)
    forecast_visibility_mode = str((forecast_overlay or {}).get("forecast_visibility_mode") or ("disabled" if not forecast_focus_ready else "contextual"))
    forecast_strength_label = str((forecast_overlay or {}).get("forecast_strength_label") or "").strip() or None
    forecast_comparison_label = str((forecast_overlay or {}).get("forecast_comparison_label") or "").strip() or None
    decision_references = _decision_references(
        threshold_lines,
        current_vs_thresholds=distance_to_thresholds,
        forecast_vs_thresholds=forecast_vs_thresholds,
    )
    focus_modes = _focus_modes(
        observed_path=observed_path,
        forecast_path=forecast_path,
        review_context=review_band,
        decision_references=decision_references,
        focus_groups=focus_groups,
        threshold_overlap_mode=overlap_mode,
    )
    hover_payload_by_timestamp = _hover_payloads(
        inspection_points,
        review_band=review_band,
        decision_references=decision_references,
        chart_takeaway=chart_takeaway,
        forecast_overlay=forecast_overlay,
    )
    inspectable_series_order = [
        item
        for item in [
            observed_series["series_id"] if observed_series else None,
            forecast_series["series_id"] if forecast_series else None,
            review_band["band_id"] if review_band else None,
            "decision_zone" if overlap_mode == "merge_to_zone" and merged_zone is not None else None,
            *[str(item.get("threshold_id")) for item in threshold_lines],
        ]
        if item
    ]
    confirmation_strip = {
        "title": "Cross-market confirmation",
        "question": "Are related markets confirming or resisting the move?",
        "items": confirmation_items,
    } if confirmation_items else None

    if density == "suppressed":
        return {
            "chart_kind": "threshold_line",
            "chart_density_profile": "suppressed",
            "chart_theme": config.get("theme") or "neutral",
            "chart_question": chart_question,
            "chart_horizon": horizon,
            "source_validity_footer": source_validity_footer,
            "chart_suppressed_reason": (
                "History is too thin and related confirmation is too limited to show a useful decision chart."
                if len(points) < 2
                else "The move does not have enough path shape or line separation to justify a full chart."
            ),
            "confirmation_state": confirmation_state,
            "compact_chart_summary": summary,
            "path_state": path_state,
            "chart_guide_items": chart_guide_items,
            "chart_explainer_lines": chart_explainer_lines,
            "chart_takeaway": chart_takeaway,
            "observed_path": observed_path,
            "forecast_path": forecast_path,
            "review_context": review_band,
            "decision_references": decision_references,
            "observed_series": observed_series,
            "forecast_series": forecast_series,
            "review_band": review_band,
            "threshold_lines": threshold_lines,
            "primary_focus_series": observed_series["series_id"] if observed_series else None,
            "inspectable_series_order": inspectable_series_order,
            "distance_to_thresholds": distance_to_thresholds,
            "inspection_points": inspection_points,
            "current_vs_thresholds": distance_to_thresholds,
            "forecast_vs_thresholds": forecast_vs_thresholds,
            "nearest_threshold": nearest_threshold,
            "relation_priority_order": relation_priority_order,
            "active_comparison_enabled": bool(forecast_series or threshold_lines),
            "threshold_overlap_mode": overlap_mode,
            "focus_split_available": focus_split_available,
            "focusable_threshold_groups": focus_groups,
            "focus_default_group": focus_default_group,
            "focus_y_domain": focus_y_domain,
            "inspectable_thresholds": inspectable_thresholds,
            "forecast_focus_ready": forecast_focus_ready,
            "focus_reason": focus_reason,
            "forecast_visibility_mode": forecast_visibility_mode,
            "forecast_strength_label": forecast_strength_label,
            "forecast_comparison_label": forecast_comparison_label,
            "focus_modes": focus_modes,
            "hover_payload_by_timestamp": hover_payload_by_timestamp,
            "current_implication_label": chart_takeaway,
            "forecast_implication_label": forecast_comparison_label,
            "chart_annotations": [path_state],
            "confirmation_strip": confirmation_strip if confirmation_strip and any(item["status"] != "missing" for item in confirmation_items) else None,
            "event_reaction_strip": None,
        }

    if density == "strip_only":
        return {
            "chart_kind": "threshold_line",
            "chart_density_profile": "strip_only",
            "chart_theme": config.get("theme") or "neutral",
            "chart_question": chart_question,
            "thresholds": thresholds,
            "current_point": current_point if points else None,
            "chart_horizon": horizon,
            "source_validity_footer": source_validity_footer,
            "chart_suppressed_reason": None,
            "confirmation_state": confirmation_state,
            "compact_chart_summary": summary,
            "threshold_legend": _threshold_legend(thresholds),
            "path_state": path_state,
            "chart_guide_items": chart_guide_items,
            "chart_explainer_lines": chart_explainer_lines,
            "chart_takeaway": chart_takeaway,
            "observed_path": observed_path,
            "forecast_path": forecast_path,
            "review_context": review_band,
            "decision_references": decision_references,
            "observed_series": observed_series,
            "forecast_series": forecast_series,
            "review_band": review_band,
            "threshold_lines": threshold_lines,
            "primary_focus_series": observed_series["series_id"] if observed_series else None,
            "inspectable_series_order": inspectable_series_order,
            "distance_to_thresholds": distance_to_thresholds,
            "inspection_points": inspection_points,
            "current_vs_thresholds": distance_to_thresholds,
            "forecast_vs_thresholds": forecast_vs_thresholds,
            "nearest_threshold": nearest_threshold,
            "relation_priority_order": relation_priority_order,
            "active_comparison_enabled": bool(forecast_series or threshold_lines),
            "threshold_overlap_mode": overlap_mode,
            "focus_split_available": focus_split_available,
            "focusable_threshold_groups": focus_groups,
            "focus_default_group": focus_default_group,
            "focus_y_domain": focus_y_domain,
            "inspectable_thresholds": inspectable_thresholds,
            "forecast_focus_ready": forecast_focus_ready,
            "focus_reason": focus_reason,
            "forecast_visibility_mode": forecast_visibility_mode,
            "forecast_strength_label": forecast_strength_label,
            "forecast_comparison_label": forecast_comparison_label,
            "focus_modes": focus_modes,
            "hover_payload_by_timestamp": hover_payload_by_timestamp,
            "current_implication_label": chart_takeaway,
            "forecast_implication_label": forecast_comparison_label,
            "chart_annotations": [path_state],
            "confirmation_strip": confirmation_strip,
            "event_reaction_strip": None,
        }

    comparison_series = _comparison_series(signal_card) if density == "rich_line" else None
    return {
        "chart_kind": "threshold_line",
        "chart_density_profile": density,
        "chart_theme": config.get("theme") or "neutral",
        "chart_question": chart_question,
        "primary_series": {
            "series_id": f"{signal_card.get('signal_id') or 'signal'}_primary",
            "label": str(signal_card.get("signal_label") or signal_card.get("label") or "Signal"),
            "unit": "level",
            "source_label": str(signal_card.get("signal_kind") or "market").replace("_", " ").strip(),
            "plain_language_meaning": _observed_path_explainer(signal_card),
            "points": points,
        },
        "observed_path": observed_path,
        "forecast_path": forecast_path,
        "review_context": review_band,
        "decision_references": decision_references,
        "observed_series": observed_series,
        "forecast_series": forecast_series,
        "comparison_series": comparison_series or None,
        "thresholds": thresholds,
        "review_band": review_band,
        "threshold_lines": threshold_lines,
        "current_point": current_point,
        "chart_horizon": horizon,
        "forecast_overlay": forecast_overlay,
        "source_validity_footer": source_validity_footer,
        "chart_suppressed_reason": None,
        "confirmation_state": confirmation_state,
        "compact_chart_summary": summary,
        "threshold_legend": _threshold_legend(thresholds),
        "path_state": path_state,
        "chart_guide_items": chart_guide_items,
        "chart_explainer_lines": chart_explainer_lines,
        "chart_takeaway": chart_takeaway,
        "primary_focus_series": observed_series["series_id"] if observed_series else None,
        "inspectable_series_order": inspectable_series_order,
        "distance_to_thresholds": distance_to_thresholds,
        "inspection_points": inspection_points,
        "current_vs_thresholds": distance_to_thresholds,
        "forecast_vs_thresholds": forecast_vs_thresholds,
        "nearest_threshold": nearest_threshold,
        "relation_priority_order": relation_priority_order,
        "active_comparison_enabled": bool(forecast_series or threshold_lines),
        "threshold_overlap_mode": overlap_mode,
        "focus_split_available": focus_split_available,
        "focusable_threshold_groups": focus_groups,
        "focus_default_group": focus_default_group,
        "focus_y_domain": focus_y_domain,
        "inspectable_thresholds": inspectable_thresholds,
        "forecast_focus_ready": forecast_focus_ready,
        "focus_reason": focus_reason,
        "forecast_visibility_mode": forecast_visibility_mode,
        "forecast_strength_label": forecast_strength_label,
        "forecast_comparison_label": forecast_comparison_label,
        "focus_modes": focus_modes,
        "hover_payload_by_timestamp": hover_payload_by_timestamp,
        "current_implication_label": chart_takeaway,
        "forecast_implication_label": forecast_comparison_label,
        "chart_annotations": [path_state],
        "confirmation_strip": confirmation_strip if confirmation_strip and any(item["status"] != "missing" for item in confirmation_items) else None,
        "event_reaction_strip": None,
    }


def _build_event_reaction_payload(signal_card: dict[str, Any]) -> dict[str, Any]:
    items = _event_reaction_items(signal_card)
    usable_items = [item for item in items if item["status"] in {"confirming", "resisting", "neutral"}]
    confirmation_state = _confirmation_state(signal_card, items)
    source_validity_footer = _source_validity_footer(signal_card)
    summary = [
        {"label": "Current", "value": "headline active", "tone": "neutral"},
        {"label": "Position", "value": "reaction strip only", "tone": "review"},
        {"label": "Confirmation", "value": _human_confirmation_state(confirmation_state), "tone": "support"},
        {"label": "Path", "value": "priced reaction" if usable_items else "headline only", "tone": "warn"},
    ]
    if len([item for item in usable_items if item["status"] in {"confirming", "resisting"}]) < 2:
        return {
            "chart_kind": "event_reaction_strip",
            "chart_density_profile": "suppressed",
            "chart_theme": "event",
            "chart_question": "Is the event still a headline, or is it becoming a priced market channel",
            "source_validity_footer": source_validity_footer,
            "chart_suppressed_reason": "The event does not have broad enough priced market reaction to chart yet.",
            "confirmation_state": confirmation_state,
            "compact_chart_summary": summary,
            "path_state": "headline only",
            "chart_explainer_lines": None,
            "chart_takeaway": "Market confirmation is still too thin to treat the event as a priced channel.",
            "chart_annotations": ["market confirmation still limited"],
            "chart_horizon": {"selected": "Current", "available": ["Current"]},
            "event_reaction_strip": None,
        }
    return {
        "chart_kind": "event_reaction_strip",
        "chart_density_profile": "strip_only",
        "chart_theme": "event",
        "chart_question": "Is the event still a headline, or is it becoming a priced market channel",
        "chart_horizon": {"selected": "Current", "available": ["Current"]},
        "forecast_overlay": None,
        "source_validity_footer": source_validity_footer,
        "chart_suppressed_reason": None,
        "confirmation_state": confirmation_state,
        "compact_chart_summary": summary,
        "path_state": "priced reaction" if confirmation_state in {"partial", "broad"} else "headline still bounded",
        "chart_explainer_lines": None,
        "chart_takeaway": (
            "The event is starting to move exposed markets, but it still needs broader confirmation to matter for the brief."
            if confirmation_state in {"partial", "broad"}
            else "The event is still more headline than priced market channel."
        ),
        "chart_annotations": ["headline moving into prices" if confirmation_state in {"partial", "broad"} else "reaction still bounded"],
        "confirmation_strip": None,
        "event_reaction_strip": {
            "title": "Market reaction",
            "question": "Is the event still a headline, or is it becoming a priced market channel?",
            "items": items[:5],
        },
    }


def build_daily_brief_chart_payload(signal_card: dict[str, Any]) -> dict[str, Any]:
    source_class = _source_class(signal_card)
    if source_class in {"policy_event", "geopolitical_news"}:
        return _build_event_reaction_payload(signal_card)
    return _build_threshold_line_payload(signal_card)
