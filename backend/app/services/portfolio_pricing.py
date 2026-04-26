from __future__ import annotations

import json
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime
from typing import Any

from app.models.types import PortfolioHolding


DEFAULT_FX_TO_SGD: dict[str, float] = {
    "SGD": 1.0,
    "USD": 1.35,
    "EUR": 1.46,
    "GBP": 1.73,
    "HKD": 0.17,
    "JPY": 0.0091,
}


FX_TICKERS: dict[str, str] = {
    "USD": "SGD=X",
    "EUR": "EURSGD=X",
    "GBP": "GBPSGD=X",
    "HKD": "HKDSGD=X",
    "JPY": "JPYSGD=X",
}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_pricing_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_price_cache (
          symbol TEXT NOT NULL,
          quote_currency TEXT NOT NULL,
          price REAL NOT NULL,
          as_of TEXT NOT NULL,
          source TEXT NOT NULL,
          PRIMARY KEY (symbol, as_of)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fx_rates_cache (
          pair TEXT NOT NULL,
          rate REAL NOT NULL,
          as_of TEXT NOT NULL,
          source TEXT NOT NULL,
          PRIMARY KEY (pair, as_of)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_price_snapshots (
          price_id TEXT PRIMARY KEY,
          security_key TEXT NOT NULL,
          normalized_symbol TEXT NOT NULL,
          raw_symbol TEXT,
          quote_currency TEXT NOT NULL,
          market_price REAL NOT NULL,
          fx_rate_to_base REAL NOT NULL,
          base_currency TEXT NOT NULL DEFAULT 'SGD',
          source TEXT NOT NULL,
          source_as_of TEXT,
          stale_flag INTEGER NOT NULL DEFAULT 0,
          retrieved_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_market_price_snapshots_symbol
        ON market_price_snapshots (security_key, retrieved_at DESC)
        """
    )
    conn.commit()


def _fetch_yahoo_quote(symbol: str, timeout: float = 1.5) -> tuple[float, str] | None:
    encoded = urllib.parse.quote(symbol)
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={encoded}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
    except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
        return None

    result = (
        payload.get("quoteResponse", {})
        .get("result", [{}])[0]
    )
    price = result.get("regularMarketPrice")
    currency = result.get("currency")
    if price is None:
        return None
    return float(price), str(currency or "USD").upper()


def _cache_price(conn: sqlite3.Connection, symbol: str, price: float, currency: str, source: str) -> None:
    ensure_pricing_tables(conn)
    conn.execute(
        """
        INSERT INTO portfolio_price_cache (symbol, quote_currency, price, as_of, source)
        VALUES (?, ?, ?, ?, ?)
        """,
        (symbol.upper(), currency.upper(), float(price), _now_iso(), source),
    )
    conn.commit()


def _latest_cached_price(conn: sqlite3.Connection, symbol: str) -> tuple[float, str, str] | None:
    ensure_pricing_tables(conn)
    row = conn.execute(
        """
        SELECT price, quote_currency, as_of
        FROM portfolio_price_cache
        WHERE symbol = ?
        ORDER BY as_of DESC
        LIMIT 1
        """,
        (symbol.upper(),),
    ).fetchone()
    if row is None:
        return None
    return float(row["price"]), str(row["quote_currency"]).upper(), str(row["as_of"])


def get_latest_price_details(
    conn: sqlite3.Connection,
    symbol: str,
    fallback_price: float,
    fallback_currency: str,
    *,
    allow_live: bool = True,
    timeout_seconds: float = 1.5,
    stale_after_hours: int = 24,
) -> dict[str, Any]:
    if allow_live:
        quote = _fetch_yahoo_quote(symbol, timeout=timeout_seconds)
        if quote is not None:
            price, currency = quote
            captured_at = _now_iso()
            _cache_price(conn, symbol, price, currency, source="yahoo_quote")
            return {
                "price": float(price),
                "currency": str(currency),
                "source": "live",
                "as_of": captured_at,
                "stale": False,
            }

    cached = _latest_cached_price(conn, symbol)
    if cached is not None:
        price, currency, as_of = cached
        try:
            age_hours = (datetime.now(UTC) - datetime.fromisoformat(as_of)).total_seconds() / 3600.0
        except Exception:  # noqa: BLE001
            age_hours = float(stale_after_hours + 1)
        return {
            "price": float(price),
            "currency": str(currency),
            "source": "cached",
            "as_of": as_of,
            "stale": age_hours > stale_after_hours,
        }

    return {
        "price": float(fallback_price),
        "currency": str(fallback_currency).upper(),
        "source": "fallback",
        "as_of": None,
        "stale": True,
    }


def get_latest_price(
    conn: sqlite3.Connection,
    symbol: str,
    fallback_price: float,
    fallback_currency: str,
    *,
    allow_live: bool = True,
    timeout_seconds: float = 1.5,
) -> tuple[float, str, str]:
    details = get_latest_price_details(
        conn,
        symbol=symbol,
        fallback_price=fallback_price,
        fallback_currency=fallback_currency,
        allow_live=allow_live,
        timeout_seconds=timeout_seconds,
    )
    return float(details["price"]), str(details["currency"]), str(details["source"])


def _cache_fx(conn: sqlite3.Connection, pair: str, rate: float, source: str) -> None:
    ensure_pricing_tables(conn)
    conn.execute(
        """
        INSERT INTO fx_rates_cache (pair, rate, as_of, source)
        VALUES (?, ?, ?, ?)
        """,
        (pair, float(rate), _now_iso(), source),
    )
    conn.commit()


def _latest_cached_fx(conn: sqlite3.Connection, pair: str) -> float | None:
    ensure_pricing_tables(conn)
    row = conn.execute(
        """
        SELECT rate
        FROM fx_rates_cache
        WHERE pair = ?
        ORDER BY as_of DESC
        LIMIT 1
        """,
        (pair.upper(),),
    ).fetchone()
    if row is None:
        return None
    return float(row["rate"])


def get_fx_to_sgd(
    conn: sqlite3.Connection,
    currency: str,
    *,
    allow_live: bool = True,
    timeout_seconds: float = 1.5,
) -> tuple[float, str]:
    code = currency.upper()
    if code == "SGD":
        return 1.0, "identity"

    ticker = FX_TICKERS.get(code)
    if allow_live and ticker:
        quote = _fetch_yahoo_quote(ticker, timeout=timeout_seconds)
        if quote is not None:
            rate, _quote_ccy = quote
            _cache_fx(conn, f"{code}/SGD", float(rate), source="yahoo_quote")
            return float(rate), "live"

    cached = _latest_cached_fx(conn, f"{code}/SGD")
    if cached is not None:
        return float(cached), "cached"

    fallback = DEFAULT_FX_TO_SGD.get(code, 1.35)
    return float(fallback), "fallback"


def value_holding_to_sgd(
    conn: sqlite3.Connection,
    holding: PortfolioHolding,
    *,
    allow_live: bool = True,
    timeout_seconds: float = 1.5,
) -> dict[str, Any]:
    price_details = get_latest_price_details(
        conn,
        symbol=holding.symbol,
        fallback_price=float(holding.cost_basis),
        fallback_currency=holding.currency,
        allow_live=allow_live,
        timeout_seconds=timeout_seconds,
    )
    price = float(price_details["price"])
    quote_ccy = str(price_details["currency"])
    price_source = str(price_details["source"])
    fx_rate, fx_source = get_fx_to_sgd(
        conn,
        quote_ccy,
        allow_live=allow_live,
        timeout_seconds=timeout_seconds,
    )
    local_value = float(holding.quantity) * float(price)
    sgd_value = local_value * float(fx_rate)
    return {
        "holding_id": holding.holding_id,
        "symbol": holding.symbol,
        "sleeve": holding.sleeve,
        "quantity": float(holding.quantity),
        "price": float(price),
        "quote_currency": quote_ccy,
        "fx_to_sgd": float(fx_rate),
        "market_value_local": round(local_value, 2),
        "market_value_sgd": round(sgd_value, 2),
        "price_source": price_source,
        "price_as_of": price_details.get("as_of"),
        "price_stale": bool(price_details.get("stale")),
        "fx_source": fx_source,
    }


def value_holdings_to_sgd(
    conn: sqlite3.Connection,
    holdings: list[PortfolioHolding],
    *,
    allow_live: bool = True,
    timeout_seconds: float = 1.5,
) -> list[dict[str, Any]]:
    return [
        value_holding_to_sgd(
            conn,
            item,
            allow_live=allow_live,
            timeout_seconds=timeout_seconds,
        )
        for item in holdings
    ]


def capture_market_price_snapshot(
    conn: sqlite3.Connection,
    *,
    security_key: str,
    normalized_symbol: str,
    raw_symbol: str,
    fallback_price: float,
    fallback_currency: str,
    base_currency: str = "SGD",
    allow_live: bool = True,
    timeout_seconds: float = 1.5,
) -> dict[str, Any]:
    ensure_pricing_tables(conn)
    price_details = get_latest_price_details(
        conn,
        symbol=normalized_symbol or raw_symbol,
        fallback_price=fallback_price,
        fallback_currency=fallback_currency,
        allow_live=allow_live,
        timeout_seconds=timeout_seconds,
    )
    fx_rate, _fx_source = get_fx_to_sgd(
        conn,
        str(price_details["currency"]),
        allow_live=allow_live,
        timeout_seconds=timeout_seconds,
    )
    retrieved_at = _now_iso()
    conn.execute(
        """
        INSERT INTO market_price_snapshots (
          price_id, security_key, normalized_symbol, raw_symbol, quote_currency, market_price,
          fx_rate_to_base, base_currency, source, source_as_of, stale_flag, retrieved_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"price_{uuid.uuid4().hex[:12]}",
            security_key,
            normalized_symbol,
            raw_symbol,
            str(price_details["currency"]),
            float(price_details["price"]),
            float(fx_rate),
            base_currency,
            str(price_details["source"]),
            price_details.get("as_of"),
            1 if bool(price_details.get("stale")) else 0,
            retrieved_at,
        ),
    )
    conn.commit()
    return {
        "market_price": float(price_details["price"]),
        "quote_currency": str(price_details["currency"]),
        "fx_rate_to_base": float(fx_rate),
        "price_source": str(price_details["source"]),
        "price_as_of": price_details.get("as_of"),
        "price_stale": bool(price_details.get("stale")),
        "retrieved_at": retrieved_at,
    }
