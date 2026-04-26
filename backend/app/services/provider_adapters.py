from __future__ import annotations

import os
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import requests
from requests import HTTPError

from app.env_loader import load_local_env


load_local_env(Path(__file__).resolve().parents[3])


class ProviderAdapterError(RuntimeError):
    def __init__(self, provider_name: str, endpoint_family: str, message: str, *, error_class: str = "provider_error") -> None:
        super().__init__(message)
        self.provider_name = provider_name
        self.endpoint_family = endpoint_family
        self.error_class = error_class


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _observed_iso(value: Any) -> str:
    if value is None or value == "":
        return _now_iso()
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            return datetime.fromtimestamp(float(value), tz=UTC).isoformat()
    except Exception:
        pass
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC).isoformat()
    except Exception:
        return text


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _safe_date_text(value: Any) -> str:
    text = str(value or "").strip()
    return text or _now_iso()[:10]


def _get(url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None, timeout: int = 6) -> Any:
    response = requests.get(url, params=params, headers=headers or {"User-Agent": "investment-agent/0.1"}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _derive_change_pct(*, current_value: Any, previous_close: Any = None, open_value: Any = None, absolute_change: Any = None) -> float | None:
    current = _safe_float(current_value)
    previous = _safe_float(previous_close)
    open_price = _safe_float(open_value)
    change_value = _safe_float(absolute_change)
    try:
        if previous not in {None, 0.0} and current is not None:
            return ((current - previous) / abs(previous)) * 100.0
        if current is not None and change_value is not None and current != change_value:
            prior = current - change_value
            if prior not in {None, 0.0}:
                return (change_value / abs(prior)) * 100.0
        if open_price not in {None, 0.0} and current is not None:
            return ((current - open_price) / abs(open_price)) * 100.0
    except Exception:
        return None
    return None


def _alpha_vantage(endpoint_family: str, identifier: str) -> dict[str, Any]:
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()
    if not api_key:
        raise ProviderAdapterError("alpha_vantage", endpoint_family, "ALPHA_VANTAGE_API_KEY missing", error_class="not_configured")
    if endpoint_family == "quote_latest":
        payload = _get(
            "https://www.alphavantage.co/query",
            params={"function": "GLOBAL_QUOTE", "symbol": identifier, "apikey": api_key},
            timeout=3,
        )
        row = payload.get("Global Quote") or {}
        price = _safe_float(row.get("05. price"))
        if price is None:
            raise ProviderAdapterError("alpha_vantage", endpoint_family, "No quote returned", error_class="empty_response")
        absolute_change = _safe_float(row.get("09. change"))
        change_pct = _safe_float(str(row.get("10. change percent") or "").replace("%", ""))
        previous_close = price - absolute_change if price is not None and absolute_change is not None else None
        return {
            "value": price,
            "price": price,
            "previous_close": previous_close,
            "absolute_change": absolute_change,
            "change_pct_1d": change_pct,
            "observed_at": _now_iso(),
            "source_ref": "alphavantage:GLOBAL_QUOTE",
        }
    if endpoint_family == "fx":
        from_symbol, to_symbol = identifier.split("/")
        payload = _get(
            "https://www.alphavantage.co/query",
            params={
                "function": "CURRENCY_EXCHANGE_RATE",
                "from_currency": from_symbol,
                "to_currency": to_symbol,
                "apikey": api_key,
            },
            timeout=3,
        )
        row = payload.get("Realtime Currency Exchange Rate") or {}
        rate = _safe_float(row.get("5. Exchange Rate"))
        if rate is None:
            raise ProviderAdapterError("alpha_vantage", endpoint_family, "No FX rate returned", error_class="empty_response")
        return {"value": rate, "observed_at": row.get("6. Last Refreshed") or _now_iso(), "source_ref": "alphavantage:CURRENCY_EXCHANGE_RATE"}
    if endpoint_family in {"benchmark_proxy", "ohlcv_history"}:
        payload = _get(
            "https://www.alphavantage.co/query",
            params={"function": "TIME_SERIES_DAILY", "symbol": identifier, "apikey": api_key, "outputsize": "full"},
            timeout=3,
        )
        series = payload.get("Time Series (Daily)") or {}
        if not series:
            raise ProviderAdapterError("alpha_vantage", endpoint_family, "No daily series returned", error_class="empty_response")
        latest_date = sorted(series.keys())[-1]
        latest_row = series.get(latest_date) or {}
        close = _safe_float(latest_row.get("4. close"))
        return {"value": close, "observed_at": latest_date, "series": series, "source_ref": "alphavantage:TIME_SERIES_DAILY"}
    if endpoint_family == "etf_profile":
        payload = _get(
            "https://www.alphavantage.co/query",
            params={"function": "ETF_PROFILE", "symbol": identifier, "apikey": api_key},
            timeout=6,
        )
        if not payload or "net_assets" not in payload:
            raise ProviderAdapterError("alpha_vantage", endpoint_family, "No ETF profile returned", error_class="empty_response")
        holdings = payload.get("holdings") or []
        top10_sum = sum(_safe_float(h.get("weight")) or 0.0 for h in holdings[:10]) if holdings else None
        return {
            "value": payload,
            "net_assets": _safe_float(payload.get("net_assets")),
            "expense_ratio": _safe_float(payload.get("expense_ratio")),
            "portfolio_turnover": _safe_float(payload.get("portfolio_turnover")),
            "asset_allocation": payload.get("asset_allocation"),
            "sector_weightings": payload.get("sector_weightings"),
            "holdings": holdings,
            "holdings_count": len(holdings),
            "top_10_concentration": top10_sum,
            "observed_at": _now_iso(),
            "source_ref": "alphavantage:ETF_PROFILE",
        }
    raise ProviderAdapterError("alpha_vantage", endpoint_family, "Unsupported family", error_class="unsupported")


def _finnhub(endpoint_family: str, identifier: str) -> dict[str, Any]:
    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not api_key:
        raise ProviderAdapterError("finnhub", endpoint_family, "FINNHUB_API_KEY missing", error_class="not_configured")
    if endpoint_family == "news_general":
        category = "general"
        limit = 10
        normalized = str(identifier or "").strip()
        if "?" in normalized:
            category_part, query = normalized.split("?", 1)
            category = category_part or category
            parsed = parse_qs(query, keep_blank_values=False)
            try:
                limit = max(1, min(100, int((parsed.get("limit") or [limit])[0])))
            except Exception:
                limit = 10
        elif normalized:
            category = normalized
        payload = _get("https://finnhub.io/api/v1/news", params={"category": category, "token": api_key})
        if not isinstance(payload, list) or not payload:
            raise ProviderAdapterError("finnhub", endpoint_family, "No news returned", error_class="empty_response")
        items: list[dict[str, Any]] = []
        for row in payload[:limit]:
            if not isinstance(row, dict):
                continue
            items.append(
                {
                    "headline": str(row.get("headline") or "").strip(),
                    "source": str(row.get("source") or "Finnhub").strip() or "Finnhub",
                    "published_utc": _observed_iso(row.get("datetime")),
                    "url": str(row.get("url") or "").strip() or None,
                }
            )
        if not items:
            raise ProviderAdapterError("finnhub", endpoint_family, "No usable news rows returned", error_class="empty_response")
        return {"value": items, "observed_at": items[0].get("published_utc") or _now_iso(), "source_ref": "finnhub:news"}
    if endpoint_family == "quote_latest":
        payload = _get("https://finnhub.io/api/v1/quote", params={"symbol": identifier, "token": api_key})
        price = _safe_float(payload.get("c"))
        if price is None:
            raise ProviderAdapterError("finnhub", endpoint_family, "No quote returned", error_class="empty_response")
        timestamp = int(payload.get("t") or 0)
        observed = datetime.fromtimestamp(timestamp, tz=UTC).isoformat() if timestamp else _now_iso()
        absolute_change = _safe_float(payload.get("d"))
        change_pct = _safe_float(payload.get("dp"))
        previous_close = _safe_float(payload.get("pc"))
        open_value = _safe_float(payload.get("o"))
        return {
            "value": price,
            "price": price,
            "previous_close": previous_close,
            "absolute_change": absolute_change,
            "change_pct_1d": change_pct if change_pct is not None else _derive_change_pct(current_value=price, previous_close=previous_close, open_value=open_value, absolute_change=absolute_change),
            "open": open_value,
            "observed_at": observed,
            "source_ref": "finnhub:quote",
        }
    if endpoint_family == "fx":
        payload = _get("https://finnhub.io/api/v1/forex/rates", params={"base": identifier.split("/")[0], "token": api_key})
        quote_ccy = identifier.split("/")[1]
        rate = _safe_float((payload.get("quote") or {}).get(quote_ccy))
        if rate is None:
            raise ProviderAdapterError("finnhub", endpoint_family, "No FX rate returned", error_class="empty_response")
        return {"value": rate, "observed_at": _now_iso(), "source_ref": "finnhub:forex_rates"}
    if endpoint_family == "reference_meta":
        payload = _get("https://finnhub.io/api/v1/stock/profile2", params={"symbol": identifier, "token": api_key})
        meaningful_keys = (
            "name",
            "ticker",
            "country",
            "currency",
            "exchange",
            "ipo",
            "marketCapitalization",
            "shareOutstanding",
            "weburl",
        )
        if not isinstance(payload, dict) or not any(str(payload.get(key) or "").strip() for key in meaningful_keys):
            raise ProviderAdapterError("finnhub", endpoint_family, "No reference metadata returned", error_class="empty_response")
        return {"value": payload, "observed_at": _now_iso(), "source_ref": "finnhub:profile2"}
    raise ProviderAdapterError("finnhub", endpoint_family, "Unsupported family", error_class="unsupported")


_POLYGON_RATE_LOCK = threading.Lock()
_POLYGON_LAST_CALL_AT: float = 0.0
_POLYGON_MIN_INTERVAL = 13.0  # 5 req/min free tier → 12 s spacing; 13 s for safety


def _polygon(endpoint_family: str, identifier: str) -> dict[str, Any]:
    global _POLYGON_LAST_CALL_AT
    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        raise ProviderAdapterError("polygon", endpoint_family, "POLYGON_API_KEY missing", error_class="not_configured")
    with _POLYGON_RATE_LOCK:
        now = time.monotonic()
        gap = now - _POLYGON_LAST_CALL_AT
        if gap < _POLYGON_MIN_INTERVAL:
            raise ProviderAdapterError(
                "polygon", endpoint_family,
                f"Self-imposed rate limit: {_POLYGON_MIN_INTERVAL - gap:.1f}s remaining",
                error_class="rate_limited",
            )
        _POLYGON_LAST_CALL_AT = now
    if endpoint_family in {"quote_latest", "benchmark_proxy"}:
        payload = _get(f"https://api.polygon.io/v2/aggs/ticker/{identifier}/prev", params={"adjusted": "true", "apiKey": api_key})
        results = payload.get("results") or []
        if not results:
            raise ProviderAdapterError("polygon", endpoint_family, "No aggregate returned", error_class="empty_response")
        row = results[0]
        ts = int(row.get("t") or 0) / 1000.0
        observed = datetime.fromtimestamp(ts, tz=UTC).isoformat() if ts else _now_iso()
        close_value = _safe_float(row.get("c"))
        open_value = _safe_float(row.get("o"))
        absolute_change = None
        if close_value is not None and open_value is not None:
            absolute_change = close_value - open_value
        return {
            "value": close_value,
            "price": close_value,
            "open": open_value,
            "absolute_change": absolute_change,
            "change_pct_1d": _derive_change_pct(current_value=close_value, open_value=open_value, absolute_change=absolute_change),
            "observed_at": observed,
            "source_ref": "polygon:prev",
        }
    if endpoint_family == "ohlcv_history":
        end_date = datetime.now(UTC).date().isoformat()
        start_date = (datetime.now(UTC) - timedelta(days=3650)).date().isoformat()
        payload = _get(
            f"https://api.polygon.io/v2/aggs/ticker/{identifier}/range/1/day/{start_date}/{end_date}",
            params={"adjusted": "true", "sort": "asc", "limit": 5000, "apiKey": api_key},
        )
        results = payload.get("results") or []
        if not results:
            raise ProviderAdapterError("polygon", endpoint_family, "No history returned", error_class="empty_response")
        row = results[0]
        ts = int(row.get("t") or 0) / 1000.0
        observed = datetime.fromtimestamp(ts, tz=UTC).isoformat() if ts else _now_iso()
        return {"value": _safe_float(row.get("c")), "observed_at": observed, "series": results, "source_ref": "polygon:range"}
    raise ProviderAdapterError("polygon", endpoint_family, "Unsupported family", error_class="unsupported")


def _tiingo(endpoint_family: str, identifier: str) -> dict[str, Any]:
    api_key = os.getenv("TIINGO_API_KEY", "").strip()
    if not api_key:
        raise ProviderAdapterError("tiingo", endpoint_family, "TIINGO_API_KEY missing", error_class="not_configured")
    headers = {"Authorization": f"Token {api_key}", "Content-Type": "application/json"}
    if endpoint_family in {"quote_latest", "benchmark_proxy"}:
        payload = _get(f"https://api.tiingo.com/tiingo/daily/{identifier}/prices", params={"startDate": "2016-01-01"}, headers=headers)
        if not isinstance(payload, list) or not payload:
            raise ProviderAdapterError("tiingo", endpoint_family, "No price rows returned", error_class="empty_response")
        row = payload[-1]
        close_value = _safe_float(row.get("close"))
        open_value = _safe_float(row.get("open"))
        absolute_change = None if close_value is None or open_value is None else close_value - open_value
        return {
            "value": close_value,
            "price": close_value,
            "open": open_value,
            "absolute_change": absolute_change,
            "change_pct_1d": _derive_change_pct(current_value=close_value, open_value=open_value, absolute_change=absolute_change),
            "observed_at": row.get("date") or _now_iso(),
            "source_ref": "tiingo:daily_prices",
        }
    if endpoint_family == "ohlcv_history":
        payload = _get(
            f"https://api.tiingo.com/tiingo/daily/{identifier}/prices",
            params={"resampleFreq": "daily", "startDate": "2016-01-01"},
            headers=headers,
        )
        if not isinstance(payload, list) or not payload:
            raise ProviderAdapterError("tiingo", endpoint_family, "No history returned", error_class="empty_response")
        row = payload[-1]
        return {"value": _safe_float(row.get("close")), "observed_at": row.get("date") or _now_iso(), "series": payload, "source_ref": "tiingo:daily_prices"}
    raise ProviderAdapterError("tiingo", endpoint_family, "Unsupported family", error_class="unsupported")


_EODHD_PLAN_BLOCKED = frozenset({"quote_latest", "benchmark_proxy", "reference_meta", "fx"})


def _eodhd(endpoint_family: str, identifier: str) -> dict[str, Any]:
    if endpoint_family in _EODHD_PLAN_BLOCKED:
        raise ProviderAdapterError(
            "eodhd", endpoint_family,
            "Blocked by free-tier plan",
            error_class="plan_limited",
        )
    api_key = os.getenv("EODHD_API_KEY", "").strip()
    if not api_key:
        raise ProviderAdapterError("eodhd", endpoint_family, "EODHD_API_KEY missing", error_class="not_configured")
    normalized = identifier
    if endpoint_family in {"quote_latest", "benchmark_proxy", "reference_meta", "ohlcv_history"} and "." not in normalized:
        normalized = f"{normalized}.US"
    if endpoint_family in {"quote_latest", "benchmark_proxy"}:
        payload = _get("https://eodhd.com/api/real-time/" + normalized, params={"api_token": api_key, "fmt": "json"}, timeout=3)
        price = _safe_float(payload.get("close") or payload.get("price"))
        if price is None:
            raise ProviderAdapterError("eodhd", endpoint_family, "No quote returned", error_class="empty_response")
        previous_close = _safe_float(payload.get("previousClose"))
        absolute_change = _safe_float(payload.get("change"))
        change_pct = _safe_float(payload.get("change_p"))
        open_value = _safe_float(payload.get("open"))
        return {
            "value": price,
            "price": price,
            "previous_close": previous_close,
            "absolute_change": absolute_change,
            "change_pct_1d": change_pct if change_pct is not None else _derive_change_pct(current_value=price, previous_close=previous_close, open_value=open_value, absolute_change=absolute_change),
            "open": open_value,
            "observed_at": _observed_iso(payload.get("timestamp")),
            "source_ref": "eodhd:real-time",
        }
    if endpoint_family == "reference_meta":
        payload = _get("https://eodhd.com/api/fundamentals/" + normalized, params={"api_token": api_key, "fmt": "json"}, timeout=3)
        return {"value": payload, "observed_at": _now_iso(), "source_ref": "eodhd:fundamentals"}
    if endpoint_family == "fx":
        payload = _get("https://eodhd.com/api/real-time/FOREX." + identifier.replace("/", ""), params={"api_token": api_key, "fmt": "json"}, timeout=3)
        price = _safe_float(payload.get("close") or payload.get("price"))
        if price is None:
            raise ProviderAdapterError("eodhd", endpoint_family, "No FX rate returned", error_class="empty_response")
        return {"value": price, "observed_at": _observed_iso(payload.get("timestamp")), "source_ref": "eodhd:real-time-forex"}
    if endpoint_family == "ohlcv_history":
        payload = _get(
            "https://eodhd.com/api/eod/" + normalized,
            params={"api_token": api_key, "fmt": "json", "period": "d", "order": "a", "from": "2016-01-01"},
            timeout=3,
        )
        if not isinstance(payload, list) or not payload:
            raise ProviderAdapterError("eodhd", endpoint_family, "No history returned", error_class="empty_response")
        row = payload[0]
        return {"value": _safe_float(row.get("close")), "observed_at": row.get("date") or _now_iso(), "series": payload, "source_ref": "eodhd:eod"}
    raise ProviderAdapterError("eodhd", endpoint_family, "Unsupported family", error_class="unsupported")


_FMP_NON_US_SUFFIXES = frozenset({".LSE", ".LON", ".PA", ".XETRA", ".F", ".DE", ".HK"})


def _fmp(endpoint_family: str, identifier: str) -> dict[str, Any]:
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        raise ProviderAdapterError("fmp", endpoint_family, "FMP_API_KEY missing", error_class="not_configured")
    if endpoint_family in {"reference_meta", "quote_latest", "fundamentals"} and any(identifier.upper().endswith(sfx) for sfx in _FMP_NON_US_SUFFIXES):
        raise ProviderAdapterError("fmp", endpoint_family, f"Non-US symbol {identifier}", error_class="symbol_gap")
    if endpoint_family == "reference_meta":
        payload = _get("https://financialmodelingprep.com/stable/profile", params={"symbol": identifier, "apikey": api_key}, timeout=3)
        if not isinstance(payload, list) or not payload:
            raise ProviderAdapterError("fmp", endpoint_family, "No profile returned", error_class="empty_response")
        return {"value": payload[0], "observed_at": _now_iso(), "source_ref": "fmp:profile"}
    if endpoint_family == "ohlcv_history":
        payload = _get(
            "https://financialmodelingprep.com/stable/historical-price-eod/full",
            params={"symbol": identifier, "apikey": api_key},
            timeout=5,
        )
        rows = list(payload if isinstance(payload, list) else payload.get("historical") or payload.get("data") or [])
        if not rows:
            raise ProviderAdapterError("fmp", endpoint_family, "No history returned", error_class="empty_response")
        latest = rows[0]
        return {
            "value": _safe_float(latest.get("close")),
            "observed_at": latest.get("date") or _now_iso(),
            "series": rows,
            "source_ref": "fmp:historical_price_eod_full",
        }
    if endpoint_family in {"quote_latest", "fundamentals"}:
        payload = _get("https://financialmodelingprep.com/stable/quote", params={"symbol": identifier, "apikey": api_key}, timeout=3)
        if not isinstance(payload, list) or not payload:
            raise ProviderAdapterError("fmp", endpoint_family, "No quote returned", error_class="empty_response")
        row = payload[0]
        price = _safe_float(row.get("price"))
        previous_close = _safe_float(row.get("previousClose"))
        absolute_change = _safe_float(row.get("change"))
        change_pct = _safe_float(row.get("changesPercentage"))
        return {
            "value": price,
            "price": price,
            "previous_close": previous_close,
            "absolute_change": absolute_change,
            "change_pct_1d": change_pct if change_pct is not None else _derive_change_pct(current_value=price, previous_close=previous_close, absolute_change=absolute_change),
            "observed_at": _now_iso(),
            "source_ref": "fmp:quote",
        }
    raise ProviderAdapterError("fmp", endpoint_family, "Unsupported family", error_class="unsupported")


def _nasdaq_data_link(endpoint_family: str, identifier: str) -> dict[str, Any]:
    api_key = os.getenv("NASDAQ_DATA_LINK_API_KEY", "").strip()
    if not api_key:
        raise ProviderAdapterError("nasdaq_data_link", endpoint_family, "NASDAQ_DATA_LINK_API_KEY missing", error_class="not_configured")
    if "?" in identifier:
        resource, query = identifier.split("?", 1)
        params = {"api_key": api_key}
        for fragment in query.split("&"):
            if "=" not in fragment:
                continue
            key, value = fragment.split("=", 1)
            params[key] = value
        payload = _get(f"https://data.nasdaq.com/api/v3/datatables/{resource}.json", params=params)
        datatable = payload.get("datatable") or {}
        data_rows = datatable.get("data") or []
        latest = data_rows[0] if data_rows else []
        observed_at = latest[-1] if isinstance(latest, list) and latest else _now_iso()
        return {"value": latest, "observed_at": observed_at, "source_ref": f"nasdaq_data_link:datatable:{resource}"}
    payload = _get(f"https://data.nasdaq.com/api/v3/datasets/{identifier}.json", params={"api_key": api_key})
    dataset = payload.get("dataset") or {}
    data_rows = dataset.get("data") or []
    latest = data_rows[0] if data_rows else []
    return {"value": latest, "observed_at": latest[0] if latest else _now_iso(), "source_ref": "nasdaq_data_link:dataset"}


def _twelve_data(endpoint_family: str, identifier: str) -> dict[str, Any]:
    api_key = os.getenv("TWELVE_DATA_API_KEY", "").strip()
    if not api_key:
        raise ProviderAdapterError("twelve_data", endpoint_family, "TWELVE_DATA_API_KEY missing", error_class="not_configured")
    if endpoint_family in {"quote_latest", "benchmark_proxy"}:
        payload = _get("https://api.twelvedata.com/quote", params={"symbol": identifier, "apikey": api_key})
        price = _safe_float(payload.get("close"))
        if price is None:
            raise ProviderAdapterError("twelve_data", endpoint_family, "No quote returned", error_class="empty_response")
        previous_close = _safe_float(payload.get("previous_close"))
        absolute_change = _safe_float(payload.get("change"))
        change_pct = _safe_float(payload.get("percent_change"))
        open_value = _safe_float(payload.get("open"))
        return {
            "value": price,
            "price": price,
            "previous_close": previous_close,
            "absolute_change": absolute_change,
            "change_pct_1d": change_pct if change_pct is not None else _derive_change_pct(current_value=price, previous_close=previous_close, open_value=open_value, absolute_change=absolute_change),
            "open": open_value,
            "observed_at": payload.get("datetime") or _now_iso(),
            "source_ref": "twelvedata:quote",
        }
    if endpoint_family == "fx":
        payload = _get("https://api.twelvedata.com/exchange_rate", params={"symbol": identifier, "apikey": api_key})
        rate = _safe_float(payload.get("rate"))
        if rate is None:
            raise ProviderAdapterError("twelve_data", endpoint_family, "No FX rate returned", error_class="empty_response")
        return {"value": rate, "observed_at": _now_iso(), "source_ref": "twelvedata:exchange_rate"}
    if endpoint_family == "ohlcv_history":
        payload = _get(
            "https://api.twelvedata.com/time_series",
            params={"symbol": identifier, "interval": "1day", "outputsize": 5000, "apikey": api_key},
        )
        values = payload.get("values") or []
        if not values:
            raise ProviderAdapterError("twelve_data", endpoint_family, "No history returned", error_class="empty_response")
        row = values[0]
        return {"value": _safe_float(row.get("close")), "observed_at": row.get("datetime") or _now_iso(), "series": values, "source_ref": "twelvedata:time_series"}
    if endpoint_family == "reference_meta":
        payload = _get(
            "https://api.twelvedata.com/etf",
            params={"symbol": identifier, "apikey": api_key},
        )
        items = payload.get("data") if isinstance(payload, dict) else (payload if isinstance(payload, list) else [])
        if not items:
            raise ProviderAdapterError("twelve_data", endpoint_family, "No ETF metadata returned", error_class="empty_response")
        # Prefer US-listed USD-denominated entry
        us_items = [r for r in items if isinstance(r, dict) and str(r.get("currency") or "").upper() == "USD" and str(r.get("country") or "").lower() in {"united states", "us", "usa"}]
        row = us_items[0] if us_items else items[0]
        if not isinstance(row, dict):
            raise ProviderAdapterError("twelve_data", endpoint_family, "No usable ETF metadata row", error_class="empty_response")
        return {
            "value": row,
            "fund_name": row.get("name"),
            "primary_listing_exchange": row.get("exchange") or row.get("mic_code"),
            "primary_trading_currency": row.get("currency"),
            "domicile": row.get("country"),
            "observed_at": _now_iso(),
            "source_ref": "twelvedata:etf",
        }
    raise ProviderAdapterError("twelve_data", endpoint_family, "Unsupported family", error_class="unsupported")


_USD_STRENGTH_COMPONENTS: tuple[tuple[str, str, float], ...] = (
    ("USD", "EUR", 0.92),
    ("USD", "SGD", 1.35),
    ("USD", "JPY", 150.0),
)


def _frankfurter_rate(base: str, quote: str, *, on_date: str | None = None) -> tuple[float | None, str | None]:
    url = "https://api.frankfurter.app/latest" if not on_date else f"https://api.frankfurter.app/{on_date}"
    payload = _get(url, params={"from": base, "to": quote})
    rates = payload.get("rates") or {}
    return _safe_float(rates.get(quote)), _safe_date_text(payload.get("date"))


def _usd_strength_proxy_payload(identifier: str) -> dict[str, Any]:
    components: list[dict[str, Any]] = []
    current_values: list[float] = []
    previous_values: list[float] = []
    observed_at: str | None = None

    for base, quote, baseline in _USD_STRENGTH_COMPONENTS:
        current_rate, current_date = _frankfurter_rate(base, quote)
        if current_rate is None:
            continue
        if observed_at is None:
            observed_at = current_date
        normalized_current = current_rate / baseline
        current_values.append(normalized_current)
        previous_rate = None
        for days_back in (1, 2, 3, 4):
            try:
                prior_date = (datetime.now(UTC) - timedelta(days=days_back)).date().isoformat()
                previous_rate, _ = _frankfurter_rate(base, quote, on_date=prior_date)
            except Exception:
                previous_rate = None
            if previous_rate is not None:
                previous_values.append(previous_rate / baseline)
                break
        components.append(
            {
                "pair": f"{base}/{quote}",
                "rate": current_rate,
                "baseline": baseline,
            }
        )

    if not current_values:
        raise ProviderAdapterError("frankfurter", "usd_strength_fallback", "No USD-strength proxy components returned", error_class="empty_response")

    current_value = 100.0 * (sum(current_values) / len(current_values))
    previous_close = 100.0 * (sum(previous_values) / len(previous_values)) if previous_values else None
    absolute_change = current_value - previous_close if previous_close not in {None, 0.0} else None
    change_pct = _derive_change_pct(current_value=current_value, previous_close=previous_close, absolute_change=absolute_change)
    return {
        "value": round(current_value, 6),
        "price": round(current_value, 6),
        "previous_close": round(previous_close, 6) if previous_close is not None else None,
        "absolute_change": round(absolute_change, 6) if absolute_change is not None else None,
        "change_pct_1d": change_pct,
        "proxy_components": components,
        "proxy_method": "frankfurter_usd_cross_average",
        "observed_at": observed_at or _now_iso()[:10],
        "source_ref": f"frankfurter:usd_strength:{str(identifier or '').upper()}",
    }


def _frankfurter(endpoint_family: str, identifier: str) -> dict[str, Any]:
    if endpoint_family == "usd_strength_fallback":
        normalized = str(identifier or "").strip().upper()
        if normalized not in {"DXY", "UUP", "USD_STRENGTH"}:
            raise ProviderAdapterError("frankfurter", endpoint_family, "USD-strength fallback only supports DXY-style identifiers", error_class="unsupported")
        return _usd_strength_proxy_payload(normalized)
    if endpoint_family != "fx_reference":
        raise ProviderAdapterError("frankfurter", endpoint_family, "Unsupported family", error_class="unsupported")
    normalized = str(identifier or "").strip().upper()
    if "/" not in normalized:
        raise ProviderAdapterError("frankfurter", endpoint_family, "FX identifier must be BASE/QUOTE", error_class="unsupported")
    base, quote = normalized.split("/", 1)
    rate, observed_at = _frankfurter_rate(base, quote)
    if rate is None:
        raise ProviderAdapterError("frankfurter", endpoint_family, "No FX reference rate returned", error_class="empty_response")
    return {
        "value": rate,
        "base": base,
        "quote": quote,
        "observed_at": observed_at,
        "source_ref": "frankfurter:latest" if endpoint_family == "fx_reference" else "frankfurter:macro_fx_proxy",
    }


def _yahoo_finance(endpoint_family: str, identifier: str) -> dict[str, Any]:
    from app.services.yahoo_finance import (
        fetch_yahoo_history_series,
        fetch_yahoo_market_close_snapshot,
        fetch_yahoo_quote_snapshot,
    )

    if endpoint_family == "quote_latest":
        payload = fetch_yahoo_quote_snapshot(identifier)
        price = _safe_float(payload.get("price") or payload.get("value") or payload.get("close"))
        if price is None:
            message = str(payload.get("error") or "No Yahoo quote returned")
            error_class = "not_configured" if "not installed" in message.lower() else "empty_response"
            raise ProviderAdapterError("yahoo_finance", endpoint_family, message, error_class=error_class)
        return {
            "value": price,
            "price": price,
            "open": _safe_float(payload.get("open")),
            "high": _safe_float(payload.get("high")),
            "low": _safe_float(payload.get("low")),
            "close": _safe_float(payload.get("close")) or price,
            "volume": _safe_float(payload.get("volume")),
            "currency": payload.get("currency"),
            "previous_close": _safe_float(payload.get("previous_close")),
            "change_pct_1d": _safe_float(payload.get("change_pct_1d")),
            "observed_at": payload.get("observed_at") or _now_iso(),
            "source_ref": payload.get("source_ref") or f"https://finance.yahoo.com/quote/{identifier}",
            "source_label": payload.get("source_label") or "Yahoo Finance market quote",
            "source_kind": payload.get("source_kind") or "public_market_quote",
            "semantic_strength": payload.get("semantic_strength") or "direct",
            "provenance_strength": payload.get("provenance_strength") or "public_market_data",
            "usable_truth": bool(payload.get("usable_truth", True)),
            "provider_symbol": payload.get("resolved_symbol") or identifier,
        }
    if endpoint_family == "market_close":
        payload = fetch_yahoo_market_close_snapshot(identifier)
        price = _safe_float(payload.get("price") or payload.get("value") or payload.get("close"))
        if price is None:
            message = str(payload.get("error") or "No Yahoo market close returned")
            error_class = "not_configured" if "not installed" in message.lower() else "empty_response"
            raise ProviderAdapterError("yahoo_finance", endpoint_family, message, error_class=error_class)
        return {
            "value": price,
            "price": price,
            "open": _safe_float(payload.get("open")),
            "high": _safe_float(payload.get("high")),
            "low": _safe_float(payload.get("low")),
            "close": _safe_float(payload.get("close")) or price,
            "volume": _safe_float(payload.get("volume")),
            "currency": payload.get("currency"),
            "previous_close": _safe_float(payload.get("previous_close")),
            "change_pct_1d": _safe_float(payload.get("change_pct_1d")),
            "observed_at": payload.get("observed_at") or _now_iso(),
            "close_date": payload.get("close_date"),
            "source_ref": payload.get("source_ref") or f"https://finance.yahoo.com/quote/{identifier}/history",
            "source_label": payload.get("source_label") or "Yahoo Finance market close",
            "source_kind": payload.get("source_kind") or "public_market_close",
            "source_type": payload.get("source_type") or "market_close",
            "semantic_strength": payload.get("semantic_strength") or "direct",
            "provenance_strength": payload.get("provenance_strength") or "public_market_data",
            "usable_truth": bool(payload.get("usable_truth", True)),
            "provider_symbol": payload.get("resolved_symbol") or identifier,
        }
    if endpoint_family == "benchmark_proxy":
        payload = fetch_yahoo_history_series(identifier)
        series = payload.get("series") or []
        latest_price = _safe_float(payload.get("price") or payload.get("value") or payload.get("close"))
        if latest_price is None or not series:
            message = str(payload.get("error") or "No Yahoo history returned")
            error_class = "not_configured" if "not installed" in message.lower() else "empty_response"
            raise ProviderAdapterError("yahoo_finance", endpoint_family, message, error_class=error_class)
        return {
            "value": latest_price,
            "price": latest_price,
            "open": _safe_float(payload.get("open")),
            "close": _safe_float(payload.get("close")) or latest_price,
            "volume": _safe_float(payload.get("volume")),
            "currency": payload.get("currency"),
            "previous_close": _safe_float(payload.get("previous_close")),
            "change_pct_1d": _safe_float(payload.get("change_pct_1d")),
            "observed_at": payload.get("observed_at") or _now_iso(),
            "series": series,
            "source_ref": payload.get("source_ref") or f"https://finance.yahoo.com/quote/{identifier}/history",
            "source_label": payload.get("source_label") or "Yahoo Finance market history",
            "source_kind": payload.get("source_kind") or "public_market_history",
            "semantic_strength": payload.get("semantic_strength") or "direct",
            "provenance_strength": payload.get("provenance_strength") or "public_market_data",
            "usable_truth": bool(payload.get("usable_truth", True)),
            "provider_symbol": payload.get("resolved_symbol") or identifier,
        }
    raise ProviderAdapterError("yahoo_finance", endpoint_family, "Unsupported family", error_class="unsupported")


def _sec_edgar(endpoint_family: str, identifier: str) -> dict[str, Any]:
    if endpoint_family != "etf_holdings":
        raise ProviderAdapterError("sec_edgar", endpoint_family, "Unsupported family", error_class="unsupported")
    from app.services.sec_edgar_ingestion import ingest_etf_holdings

    payload = ingest_etf_holdings(identifier)
    if not payload:
        raise ProviderAdapterError("sec_edgar", endpoint_family, "No ETF holdings returned", error_class="empty_response")
    return payload


ADAPTERS = {
    "alpha_vantage": _alpha_vantage,
    "fmp": _fmp,
    "finnhub": _finnhub,
    "frankfurter": _frankfurter,
    "yahoo_finance": _yahoo_finance,
    "sec_edgar": _sec_edgar,
    "polygon": _polygon,
    "tiingo": _tiingo,
    "eodhd": _eodhd,
    "nasdaq_data_link": _nasdaq_data_link,
    "twelve_data": _twelve_data,
}


def fetch_provider_data(provider_name: str, endpoint_family: str, identifier: str) -> dict[str, Any]:
    handler = ADAPTERS.get(str(provider_name))
    if handler is None:
        raise ProviderAdapterError(provider_name, endpoint_family, "No adapter registered", error_class="unsupported")
    try:
        return handler(endpoint_family, identifier)
    except ProviderAdapterError:
        raise
    except HTTPError as exc:
        response = getattr(exc, "response", None)
        status_code = int(getattr(response, "status_code", 0) or 0)
        if status_code in {401, 403}:
            error_class = "auth_error"
        elif status_code == 404:
            error_class = "not_found"
        elif status_code == 429:
            error_class = "rate_limited"
        elif 500 <= status_code <= 599:
            error_class = "upstream_error"
        else:
            error_class = "provider_error"
        raise ProviderAdapterError(provider_name, endpoint_family, str(exc), error_class=error_class) from exc
    except requests.RequestException as exc:
        raise ProviderAdapterError(provider_name, endpoint_family, str(exc), error_class="provider_error") from exc
