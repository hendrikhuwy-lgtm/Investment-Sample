"""
Yahoo Finance market-data helpers.

Used for: public market quotes, bounded history, spreads, volume, liquidity proxy
NOT used for: ISIN, domicile, TER, accumulating status, or issuer/reference proofs
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

try:
    import yfinance as yf
except ImportError:
    yf = None


_YAHOO_ALIAS_MAP = {
    "WORLD_EQUITY": "^990100-USD-STRD",
    "BONDS": "AGG",
    "FX_USD": "DX-Y.NYB",
    "FX___USD": "DX-Y.NYB",
    "DXY": "DX-Y.NYB",
    "^DXY": "DX-Y.NYB",
    "USD_STRENGTH": "DX-Y.NYB",
    "GOLD": "GC=F",
    "SP500": "^GSPC",
    "S&P_500": "^GSPC",
    "NASDAQ": "^IXIC",
    "RUSSELL_2K": "^RUT",
    "EQ_BREADTH": "^SPXEW",
    "INFLATION": "TIP",
    "REGIONAL": "^KRX",
    "CREDIT": "HYG",
    "FED_FUNDS": "^IRX",
    "SOFR": "SGOV",
    "UST_2Y": "SHY",
    "RATES": "^TNX",
    "REAL_YIELD_10Y": "RINF",
    "UST_30Y": "^TYX",
    "BRENT_CRUDE": "BZ=F",
    "WTI_CRUDE": "CL=F",
    "BITCOIN": "BTC-USD",
    "30Y_MORTGAGE": "MBB",
}

_CHINA_TZ = ZoneInfo("Asia/Shanghai")


def _as_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except Exception:
        return None


def _normalize_identifier(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = raw.upper().replace("/", "_").replace(" ", "_").replace("-", "_")
    return _YAHOO_ALIAS_MAP.get(normalized, raw)


def _normalized_history_frame(history: Any, *, resolved_symbol: str) -> Any:
    if history is None or getattr(history, "empty", True):
        return None
    columns = getattr(history, "columns", None)
    if columns is not None and getattr(columns, "nlevels", 1) > 1:
        try:
            if resolved_symbol in columns.get_level_values(-1):
                history = history.xs(resolved_symbol, axis=1, level=-1)
            else:
                history = history.droplevel(-1, axis=1)
        except Exception:
            return None
    return None if getattr(history, "empty", True) else history


def _download_history_frame(resolved: str, *, period: str, interval: str, auto_adjust: bool) -> Any:
    if yf is None:
        return None
    try:
        history = yf.download(
            resolved,
            period=period,
            interval=interval,
            auto_adjust=auto_adjust,
            progress=False,
            threads=False,
        )
    except Exception:
        return None
    return _normalized_history_frame(history, resolved_symbol=resolved)


def _history_frame(identifier: str, *, period: str, interval: str, auto_adjust: bool) -> Any:
    if yf is None:
        return None
    resolved = _normalize_identifier(identifier)
    ticker_obj = yf.Ticker(resolved)
    try:
        history = ticker_obj.history(period=period, interval=interval, auto_adjust=auto_adjust)
    except Exception:
        history = None
    history = _normalized_history_frame(history, resolved_symbol=resolved)
    if history is None:
        history = _download_history_frame(
            resolved,
            period=period,
            interval=interval,
            auto_adjust=auto_adjust,
        )
    if history is None:
        return None
    return ticker_obj, history


def _observed_at(value: Any) -> str:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat()
    text = str(value or "").strip()
    if not text:
        return datetime.now(UTC).isoformat()
    return text if "T" in text else f"{text}T00:00:00+00:00"


def _ticker_currency(ticker_obj: Any) -> str | None:
    fast_info = getattr(ticker_obj, "fast_info", None)
    if hasattr(fast_info, "get"):
        currency = str(fast_info.get("currency") or "").strip()
        if currency:
            return currency.upper()
    try:
        info = getattr(ticker_obj, "info", {}) or {}
    except Exception:
        info = {}
    currency = str(info.get("currency") or "").strip()
    return currency.upper() or None


def _china_close_eligible_date(now: datetime | None = None) -> date:
    anchor = now or datetime.now(UTC)
    if anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=UTC)
    return anchor.astimezone(_CHINA_TZ).date() - timedelta(days=1)


def _history_row_date(index_value: Any) -> date | None:
    if hasattr(index_value, "to_pydatetime"):
        index_value = index_value.to_pydatetime()
    if isinstance(index_value, datetime):
        return index_value.date()
    text = str(index_value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _market_close_observed_at(day_value: date | None) -> str:
    if day_value is None:
        return datetime.now(UTC).isoformat()
    return f"{day_value.isoformat()}T00:00:00+00:00"


def fetch_yahoo_quote_snapshot(identifier: str) -> dict[str, Any]:
    """Public market-quote helper for managed provider integration."""
    if yf is None:
        return {
            "status": "failed",
            "error": "yfinance library not installed",
            "source": "yahoo_finance",
            "identifier": identifier,
        }

    resolved = _normalize_identifier(identifier)
    try:
        fetched = _history_frame(resolved, period="5d", interval="1d", auto_adjust=False)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "error": str(exc),
            "source": "yahoo_finance",
            "identifier": identifier,
        }
    if fetched is None:
        return {
            "status": "failed",
            "error": f"No market data found for ticker {resolved}",
            "source": "yahoo_finance",
            "identifier": identifier,
        }

    ticker_obj, history = fetched
    recent = history.tail(2)
    latest_row = recent.iloc[-1]
    previous_row = recent.iloc[-2] if len(recent.index) >= 2 else None

    price = _as_float(latest_row.get("Close"))
    open_value = _as_float(latest_row.get("Open"))
    high_value = _as_float(latest_row.get("High"))
    low_value = _as_float(latest_row.get("Low"))
    close_value = _as_float(latest_row.get("Close"))
    volume = _as_float(latest_row.get("Volume"))
    previous_close = _as_float(previous_row.get("Close")) if previous_row is not None else None
    change_pct_1d = None
    if price is not None and previous_close not in {None, 0}:
        change_pct_1d = ((price - previous_close) / abs(previous_close)) * 100.0

    if price is None:
        return {
            "status": "failed",
            "error": f"No close value found for ticker {resolved}",
            "source": "yahoo_finance",
            "identifier": identifier,
        }

    observed_at = _observed_at(recent.index[-1])
    return {
        "status": "success",
        "identifier": identifier,
        "resolved_symbol": resolved,
        "value": price,
        "price": price,
        "open": open_value,
        "high": high_value,
        "low": low_value,
        "close": close_value if close_value is not None else price,
        "volume": volume,
        "currency": _ticker_currency(ticker_obj),
        "observed_at": observed_at,
        "previous_close": previous_close,
        "change_pct_1d": change_pct_1d,
        "source": "yahoo_finance",
        "source_ref": f"https://finance.yahoo.com/quote/{resolved}",
        "source_url": f"https://finance.yahoo.com/quote/{resolved}",
        "source_label": "Yahoo Finance market quote",
        "source_kind": "public_market_quote",
        "semantic_strength": "direct",
        "provenance_strength": "public_market_data",
        "usable_truth": True,
    }


def fetch_yahoo_market_close_snapshot(identifier: str, *, now: datetime | None = None) -> dict[str, Any]:
    """Daily-close helper for surfaces that must avoid intraday partial bars."""
    if yf is None:
        return {
            "status": "failed",
            "error": "yfinance library not installed",
            "source": "yahoo_finance",
            "identifier": identifier,
        }

    resolved = _normalize_identifier(identifier)
    try:
        fetched = _history_frame(resolved, period="15d", interval="1d", auto_adjust=False)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "error": str(exc),
            "source": "yahoo_finance",
            "identifier": identifier,
        }
    if fetched is None:
        return {
            "status": "failed",
            "error": f"No market-close data found for ticker {resolved}",
            "source": "yahoo_finance",
            "identifier": identifier,
        }

    ticker_obj, history = fetched
    eligible_date = _china_close_eligible_date(now=now)
    rows: list[tuple[Any, Any]] = []
    for index_value, row in history.iterrows():
        row_date = _history_row_date(index_value)
        if row_date is None or row_date > eligible_date:
            continue
        rows.append((index_value, row))

    if not rows:
        return {
            "status": "failed",
            "error": f"No completed daily close available for ticker {resolved}",
            "source": "yahoo_finance",
            "identifier": identifier,
        }

    latest_index, latest_row = rows[-1]
    latest_date = _history_row_date(latest_index)
    previous_row = rows[-2][1] if len(rows) >= 2 else None

    price = _as_float(latest_row.get("Close"))
    open_value = _as_float(latest_row.get("Open"))
    high_value = _as_float(latest_row.get("High"))
    low_value = _as_float(latest_row.get("Low"))
    close_value = _as_float(latest_row.get("Close"))
    volume = _as_float(latest_row.get("Volume"))
    previous_close = _as_float(previous_row.get("Close")) if previous_row is not None else None
    change_pct_1d = None
    if price is not None and previous_close not in {None, 0}:
        change_pct_1d = ((price - previous_close) / abs(previous_close)) * 100.0

    if price is None:
        return {
            "status": "failed",
            "error": f"No close value found for ticker {resolved}",
            "source": "yahoo_finance",
            "identifier": identifier,
        }

    observed_at = _market_close_observed_at(latest_date)
    return {
        "status": "success",
        "identifier": identifier,
        "resolved_symbol": resolved,
        "value": price,
        "price": price,
        "open": open_value,
        "high": high_value,
        "low": low_value,
        "close": close_value if close_value is not None else price,
        "volume": volume,
        "currency": _ticker_currency(ticker_obj),
        "observed_at": observed_at,
        "previous_close": previous_close,
        "change_pct_1d": change_pct_1d,
        "source": "yahoo_finance",
        "source_ref": f"https://finance.yahoo.com/quote/{resolved}/history",
        "source_url": f"https://finance.yahoo.com/quote/{resolved}/history",
        "source_label": "Yahoo Finance public daily close",
        "source_kind": "public_market_close",
        "source_type": "market_close",
        "semantic_strength": "direct",
        "provenance_strength": "public_verified_close",
        "usable_truth": True,
        "close_date": latest_date.isoformat() if latest_date is not None else None,
        "eligible_as_of_china_date": eligible_date.isoformat(),
    }


def fetch_yahoo_history_series(identifier: str, *, period: str = "10y") -> dict[str, Any]:
    """Bounded daily history helper for public market proxies."""
    if yf is None:
        return {
            "status": "failed",
            "error": "yfinance library not installed",
            "source": "yahoo_finance",
            "identifier": identifier,
        }

    resolved = _normalize_identifier(identifier)
    try:
        fetched = _history_frame(resolved, period=period, interval="1d", auto_adjust=True)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "error": str(exc),
            "source": "yahoo_finance",
            "identifier": identifier,
        }
    if fetched is None:
        return {
            "status": "failed",
            "error": f"No history found for ticker {resolved}",
            "source": "yahoo_finance",
            "identifier": identifier,
        }

    ticker_obj, history = fetched
    series: list[dict[str, Any]] = []
    for index, row in history.iterrows():
        close_value = _as_float(row.get("Close"))
        if close_value is None:
            continue
        series.append(
            {
                "date": _observed_at(index)[:10],
                "open": _as_float(row.get("Open")),
                "high": _as_float(row.get("High")),
                "low": _as_float(row.get("Low")),
                "close": close_value,
                "volume": _as_float(row.get("Volume")),
            }
        )
    if not series:
        return {
            "status": "failed",
            "error": f"No usable history rows found for ticker {resolved}",
            "source": "yahoo_finance",
            "identifier": identifier,
        }

    latest = series[-1]
    previous = series[-2] if len(series) >= 2 else None
    previous_close = previous.get("close") if previous is not None else None
    change_pct_1d = None
    if previous_close not in {None, 0}:
        change_pct_1d = ((float(latest["close"]) - float(previous_close)) / abs(float(previous_close))) * 100.0

    return {
        "status": "success",
        "identifier": identifier,
        "resolved_symbol": resolved,
        "value": latest["close"],
        "price": latest["close"],
        "open": latest.get("open"),
        "close": latest["close"],
        "volume": latest.get("volume"),
        "currency": _ticker_currency(ticker_obj),
        "observed_at": f"{latest['date']}T00:00:00+00:00",
        "previous_close": previous_close,
        "change_pct_1d": change_pct_1d,
        "series": series,
        "source": "yahoo_finance",
        "source_ref": f"https://finance.yahoo.com/quote/{resolved}/history",
        "source_url": f"https://finance.yahoo.com/quote/{resolved}/history",
        "source_label": "Yahoo Finance market history",
        "source_kind": "public_market_history",
        "semantic_strength": "direct",
        "provenance_strength": "public_market_data",
        "usable_truth": True,
    }


def fetch_yahoo_market_data(ticker: str, exchange_suffix: str = ".L") -> dict[str, Any]:
    """
    Fetch market data from Yahoo Finance using yfinance library.

    Args:
        ticker: ETF ticker symbol
        exchange_suffix: Exchange suffix (".L" for London, ".SI" for Singapore, etc.)

    Returns: price, volume, average volume, bid/ask spread
    NOT for verification proofs - only for liquidity assessment
    """
    if yf is None:
        return {
            "status": "failed",
            "error": "yfinance library not installed. Run: pip install yfinance",
            "source": "yahoo_finance",
        }

    # Append exchange suffix if needed
    yahoo_ticker = ticker if "." in ticker else f"{ticker}{exchange_suffix}"

    try:
        # Fetch ticker data
        ticker_obj = yf.Ticker(yahoo_ticker)
        info = ticker_obj.info

        # Check if data was returned
        if not info or "regularMarketPrice" not in info:
            return {
                "status": "failed",
                "error": f"No data found for ticker {yahoo_ticker}",
                "source": "yahoo_finance",
                "ticker": ticker,
            }

        # Extract market data
        last_price = info.get("regularMarketPrice") or info.get("currentPrice")
        bid_price = info.get("bid")
        ask_price = info.get("ask")
        volume_day = info.get("regularMarketVolume") or info.get("volume")
        volume_avg_30d = info.get("averageDailyVolume3Month") or info.get("averageVolume")
        volume_avg_10d = info.get("averageDailyVolume10Day")

        # Calculate spread
        bid_ask_spread_abs = None
        bid_ask_spread_bps = None

        if bid_price and ask_price and bid_price > 0 and ask_price >= bid_price:
            bid_ask_spread_abs = ask_price - bid_price
            bid_ask_spread_bps = (bid_ask_spread_abs / bid_price) * 10000  # Basis points

        # Liquidity score (0-1, higher is better)
        liquidity_score = None
        if volume_avg_30d:
            # Simple heuristic: volume > 100k = 1.0, volume < 10k = 0.0
            liquidity_score = min(1.0, max(0.0, (volume_avg_30d - 10000) / 90000))

        return {
            "status": "success",
            "ticker": ticker,
            "yahoo_ticker": yahoo_ticker,
            "source": "yahoo_finance",
            "source_url": f"https://finance.yahoo.com/quote/{yahoo_ticker}",
            "retrieved_at": datetime.now(UTC).isoformat(),
            "market_data": {
                "last_price": last_price,
                "bid_price": bid_price,
                "ask_price": ask_price,
                "bid_ask_spread_abs": bid_ask_spread_abs,
                "bid_ask_spread_bps": bid_ask_spread_bps,
                "volume_day": volume_day,
                "volume_avg_30d": volume_avg_30d,
                "volume_avg_10d": volume_avg_10d,
                "liquidity_score": liquidity_score,
            },
            "proof_warning": "Yahoo Finance data CANNOT be used for ISIN, domicile, TER, or accumulating proofs",
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "source": "yahoo_finance",
            "ticker": ticker,
        }


def format_liquidity_proxy(liquidity_score: float | None, volume_avg_30d: float | None) -> str:
    """
    Format liquidity proxy for display.

    Returns: "High (1.2M avg vol)", "Medium (450K avg vol)", "Low (15K avg vol)"
    """
    if liquidity_score is None or volume_avg_30d is None:
        return "Unknown"

    if liquidity_score >= 0.7:
        level = "High"
    elif liquidity_score >= 0.3:
        level = "Medium"
    else:
        level = "Low"

    # Format volume
    if volume_avg_30d >= 1_000_000:
        vol_str = f"{volume_avg_30d / 1_000_000:.1f}M"
    elif volume_avg_30d >= 1_000:
        vol_str = f"{volume_avg_30d / 1_000:.0f}K"
    else:
        vol_str = f"{volume_avg_30d:.0f}"

    return f"{level} ({vol_str} avg vol)"
