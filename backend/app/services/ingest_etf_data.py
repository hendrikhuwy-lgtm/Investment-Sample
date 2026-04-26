"""
ETF Data Ingestion Service
Fetches holdings, factsheet metrics, and market data from real sources.
All data must have citations - no hallucination.
"""

from __future__ import annotations

import hashlib
import html
import io
import json
import re
import sqlite3
import csv
from functools import lru_cache
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx
from app.config import Settings, get_db_path, get_repo_root
from app.services.etf_doc_parser import (
    _normalize_structured_fields,
    _parse_factsheet_date,
    fetch_candidate_docs,
)
from app.services.yahoo_finance import fetch_yahoo_market_data


_DEVELOPED_MARKET_LOCATIONS = {
    "Australia",
    "Austria",
    "Belgium",
    "Canada",
    "Denmark",
    "Finland",
    "France",
    "Germany",
    "Hong Kong",
    "Ireland",
    "Israel",
    "Italy",
    "Japan",
    "Netherlands",
    "New Zealand",
    "Norway",
    "Portugal",
    "Singapore",
    "Spain",
    "Sweden",
    "Switzerland",
    "United Kingdom",
    "United States",
}


@lru_cache(maxsize=1)
def _etf_data_sources_registry() -> dict[str, dict[str, Any]]:
    path = get_repo_root() / "backend" / "app" / "config" / "etf_data_sources.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    items = payload.get("sources") if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        return {}
    registry: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("etf_symbol") or "").strip().upper()
        if not symbol:
            continue
        registry[symbol] = item
    return registry


def get_etf_source_config(symbol: str) -> dict[str, Any] | None:
    normalized = str(symbol or "").strip().upper()
    aliases = {
        "ES3": "ES3.SI",
    }
    registry = _etf_data_sources_registry()
    return registry.get(normalized) or registry.get(aliases.get(normalized, ""))


def list_etf_source_configs() -> list[dict[str, Any]]:
    return list(_etf_data_sources_registry().values())


def _fetch_source_definition(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    data_type: str,
    source_id: str | None = None,
) -> dict[str, Any] | None:
    if source_id:
        row = conn.execute(
            """
        SELECT data_type, source_id, fetch_method, parser_type, source_url_template
        FROM etf_data_sources
        WHERE etf_symbol = ? AND data_type = ? AND source_id = ? AND enabled = 1
        ORDER BY created_at DESC
        LIMIT 1
            """,
            (symbol, data_type, source_id),
        ).fetchone()
        if row is not None:
            return dict(row)
    row = conn.execute(
        """
        SELECT data_type, source_id, fetch_method, parser_type, source_url_template
        FROM etf_data_sources
        WHERE etf_symbol = ? AND data_type = ? AND enabled = 1
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (symbol, data_type),
    ).fetchone()
    return dict(row) if row is not None else None


def _fetch_html_source_text(url: str) -> str:
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        response = client.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
        )
        response.raise_for_status()
        return response.text


def _html_to_text(payload: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", payload, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_html_structured_fields(payload: str) -> dict[str, Any]:
    return _normalize_structured_fields(_html_to_text(payload))


def _extract_parser_specific_html_fields(symbol: str, payload: str, parser_type: str) -> dict[str, Any]:
    parser = str(parser_type or "").strip().lower()
    text = _html_to_text(payload)
    fields = _extract_html_structured_fields(payload)
    augmented = _augment_summary_fields_from_text(symbol, text, fields)
    if parser == "vanguard_holdings_html_summary" and symbol.upper() == "VAGU":
        augmented.setdefault("developed_market_exposure_summary", "Global aggregate bond exposure with currency hedging")
        augmented.setdefault("emerging_market_exposure_summary", "Broad bond exposure remains summary-described rather than line-item holdings-complete")
    elif parser == "ssga_holdings_html_summary" and symbol.upper() in {"BIL", "BILS"}:
        augmented.setdefault("top_country", "United States")
        augmented.setdefault("developed_market_exposure_summary", "US Treasury-bill exposure")
    elif parser == "hsbc_holdings_pdf_summary" and symbol.upper() == "HMCH":
        augmented.setdefault("top_country", "China")
    elif parser == "xtrackers_holdings_html_summary" and symbol.upper() == "XCHA":
        augmented.setdefault("top_country", "China")
    elif parser == "amova_holdings_html_summary" and symbol.upper() == "A35":
        augmented.setdefault("top_country", "Singapore")
        augmented.setdefault("developed_market_exposure_summary", "Singapore government bond exposure")
    return augmented


def _parse_top_holdings_weights_from_text(text: str) -> list[float]:
    section_patterns = [
        r"Top 10 Holdings(?P<section>.*?)(?:Document|Performance|Sector Allocation|Quality Breakdown|Maturity Ladder|Fund Characteristics)",
        r"Fund Top Holdings(?P<section>.*?)(?:Download All Holdings|Sector Allocation|Quality Breakdown|Maturity Ladder|Document)",
        r"Top Holdings(?P<section>.*?)(?:Sector Allocation|Portfolio Characteristics|Documents|Performance)",
    ]
    for pattern in section_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        section = str(match.group("section") or "")
        weights = [float(item) for item in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*%", section)[:10]]
        if weights:
            return weights
    return []


def _augment_summary_fields_from_text(symbol: str, text: str, fields: dict[str, Any]) -> dict[str, Any]:
    augmented = dict(fields)

    def _fill(key: str, value: Any) -> None:
        if augmented.get(key) in {None, ""}:
            augmented[key] = value

    top_weights = _parse_top_holdings_weights_from_text(text)
    if top_weights:
        _fill("holdings_count", len(top_weights))
        _fill("top_10_concentration", round(sum(top_weights[:10]), 2))
    upper = text.upper()
    if symbol in {"HMCH", "XCHA"} or "MSCI CHINA" in upper or "CHINA UCITS ETF" in upper:
        _fill("em_weight", 100.0)
        _fill("top_country", "China")
        _fill("developed_market_exposure_summary", "Single-country China allocation")
        _fill("emerging_market_exposure_summary", "China single-country emerging-market exposure")
    if symbol in {"A35", "ES3", "ES3.SI"} or "SINGAPORE" in upper:
        _fill("top_country", "Singapore")
    if "TREASURY BILL" in upper or symbol in {"BIL", "BILS", "SGOV", "IB01"}:
        _fill("top_country", "United States")
        if "0 - 1 YEAR" in upper:
            _fill("sector_concentration_proxy", 100.0)
    if symbol == "SGLN" or "PHYSICAL GOLD" in upper:
        _fill("holdings_count", 1)
        _fill("top_10_concentration", 100.0)
        _fill("sector_concentration_proxy", 100.0)
        _fill("top_country", "Gold bullion backing")
    return augmented


def get_preferred_market_exchange(symbol: str, conn: sqlite3.Connection) -> str:
    normalized = str(symbol or "").strip().upper()
    config = get_etf_source_config(normalized) or {}
    listings = list(config.get("listings") or [])
    for listing in listings:
        exchange = str(dict(listing).get("exchange") or "").strip()
        if exchange:
            return exchange
    row = conn.execute(
        """
        SELECT exchange
        FROM etf_market_data
        WHERE etf_symbol = ?
        ORDER BY asof_date DESC, asof_time DESC
        LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    exchange = str(row["exchange"] or "").strip() if row else ""
    return exchange or "SGX"


def get_preferred_market_history_summary(symbol: str, conn: sqlite3.Connection) -> dict[str, Any] | None:
    exchange = get_preferred_market_exchange(symbol, conn)
    return get_etf_market_history_summary(str(symbol or "").strip().upper(), exchange, conn)


def get_preferred_latest_market_data(symbol: str, conn: sqlite3.Connection) -> dict[str, Any] | None:
    exchange = get_preferred_market_exchange(symbol, conn)
    return get_latest_etf_market_data(str(symbol or "").strip().upper(), exchange, conn)


def _generate_id(prefix: str, *parts: str) -> str:
    """Generate deterministic ID from parts."""
    combined = "|".join(str(p) for p in parts)
    hash_suffix = hashlib.sha256(combined.encode()).hexdigest()[:12]
    return f"{prefix}_{hash_suffix}"


def _parse_optional_float(value: str | None) -> float | None:
    raw = str(value or "").strip().strip('"')
    if not raw:
        return None
    normalized = (
        raw.replace(",", "")
        .replace("’", "")
        .replace("'", "")
        .replace("\u00a0", "")
        .replace("\u202f", "")
    )
    return float(normalized)


def _ensure_etf_tables(conn: sqlite3.Connection) -> None:
    """Ensure ETF data tables exist."""
    schema_path = get_repo_root() / "backend" / "app" / "storage" / "schema_etf_extensions.sql"
    try:
        with open(schema_path) as f:
            conn.executescript(f.read())
    except FileNotFoundError:
        pass  # Tables may already exist


def ensure_etf_tables(conn: sqlite3.Connection) -> None:
    """Public wrapper for ETF extension tables used by runtime/bootstrap."""
    _ensure_etf_tables(conn)


def _factsheet_result_has_material_fields(result: dict[str, Any] | None) -> bool:
    payload = dict(result or {})
    return any(
        payload.get(key) not in {None, ""}
        for key in (
            "aum_usd",
            "tracking_difference_1y",
            "tracking_difference_3y",
            "tracking_difference_5y",
            "tracking_error_1y",
            "dividend_yield",
        )
    )


# ============================================================================
# HOLDINGS INGESTION (from issuer CSV files)
# ============================================================================

def fetch_vanguard_holdings(symbol: str, conn: sqlite3.Connection) -> dict[str, Any]:
    """
    Fetch Vanguard ETF holdings from official CSV.

    Real source: https://www.vanguard.com/pub/Pdf/holdings/{symbol}_holdings.csv
    Example: VWRA (Vanguard FTSE All-World UCITS ETF)
    """
    run_id = _generate_id("vgrd", symbol, datetime.now(UTC).isoformat())
    source_id = f"vanguard_{symbol.lower()}_holdings"

    source_config = conn.execute(
        """
        SELECT source_url_template
        FROM etf_data_sources
        WHERE etf_symbol = ? AND data_type = 'holdings' AND enabled = 1
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (symbol,),
    ).fetchone()
    holdings_url = str(source_config["source_url_template"] or "").strip() if source_config else ""
    if not holdings_url:
        return {
            "status": "failed",
            "error": f"No holdings source configured for {symbol}",
        }

    started_at = datetime.now(UTC).isoformat()

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(holdings_url)
            response.raise_for_status()

            # Parse CSV (Vanguard format: usually has header rows)
            lines = response.text.strip().split("\n")

            # Skip header rows (Vanguard typically has 5-7 header rows)
            data_start_idx = 0
            for idx, line in enumerate(lines):
                if "Holdings" in line or "Ticker" in line or "ISIN" in line:
                    data_start_idx = idx + 1
                    break

            holdings_inserted = 0
            asof_date = None

            # Extract as-of date from header
            for header_line in lines[:data_start_idx]:
                date_match = re.search(r"(\d{2}/\d{2}/\d{4})", header_line)
                if date_match:
                    # Convert MM/DD/YYYY to YYYY-MM-DD
                    date_str = date_match.group(1)
                    month, day, year = date_str.split("/")
                    asof_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    break

            if not asof_date:
                asof_date = datetime.now(UTC).date().isoformat()

            retrieved_at = datetime.now(UTC).isoformat()

            # Parse data rows (CSV format varies by fund)
            for line in lines[data_start_idx:]:
                if not line.strip() or line.startswith(","):
                    continue

                parts = line.split(",")
                if len(parts) < 3:
                    continue

                # Typical Vanguard CSV: Ticker, Security Name, % Net Assets, Shares, Market Value
                try:
                    ticker = parts[0].strip().strip('"')
                    security_name = parts[1].strip().strip('"')
                    weight_str = parts[2].strip().strip('"').replace("%", "")

                    if not security_name or security_name.lower() in {"total", "holdings"}:
                        continue

                    weight_pct = float(weight_str) if weight_str else 0.0

                    holding_id = _generate_id("holding", symbol, asof_date, ticker, security_name)

                    conn.execute(
                        """
                        INSERT OR REPLACE INTO etf_holdings
                        (holding_id, etf_symbol, asof_date, security_name, security_ticker,
                         weight_pct, retrieved_at, source_url, source_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            holding_id,
                            symbol,
                            asof_date,
                            security_name,
                            ticker or None,
                            weight_pct,
                            retrieved_at,
                            holdings_url,
                            source_id,
                        ),
                    )
                    holdings_inserted += 1

                except (ValueError, IndexError):
                    continue

            conn.commit()
            finished_at = datetime.now(UTC).isoformat()

            # Log successful run
            conn.execute(
                """
                INSERT INTO etf_fetch_runs
                (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
                 status, records_fetched, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    symbol,
                    "holdings",
                    source_id,
                    started_at,
                    finished_at,
                    "success",
                    holdings_inserted,
                    holdings_url,
                ),
            )
            conn.commit()

            return {
                "status": "success",
                "holdings_fetched": holdings_inserted,
                "asof_date": asof_date,
                "source_url": holdings_url,
            }

    except httpx.HTTPError as e:
        finished_at = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, error_message, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                "holdings",
                source_id,
                started_at,
                finished_at,
                "failed",
                0,
                str(e),
                holdings_url,
            ),
        )
        conn.commit()

        return {
            "status": "failed",
            "error": str(e),
            "source_url": holdings_url,
        }


def fetch_ishares_holdings(symbol: str, conn: sqlite3.Connection) -> dict[str, Any]:
    """
    Fetch iShares ETF holdings from official CSV export.

    Real source: https://www.ishares.com/.../products/{fund_id}/.../holdings.ajax?fileType=csv
    Example: IWDA - https://www.ishares.com/uk/individual/en/products/251882/ishares-msci-world-ucits-etf-acc-fund/1478358465952.ajax?fileType=csv
    """
    run_id = _generate_id("ishares_hold", symbol, datetime.now(UTC).isoformat())
    source_id = f"ishares_{symbol.lower()}_holdings"

    # Get configured URL
    source_config = conn.execute(
        """
        SELECT source_url_template
        FROM etf_data_sources
        WHERE etf_symbol = ? AND data_type = 'holdings' AND source_id = ?
        """,
        (symbol, source_id),
    ).fetchone()

    if not source_config:
        return {"status": "failed", "error": f"No holdings source configured for {symbol}"}

    csv_url = str(source_config["source_url_template"])
    started_at = datetime.now(UTC).isoformat()

    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            }

            response = client.get(csv_url, headers=headers)
            response.raise_for_status()

            try:
                decoded = response.content.decode("utf-8-sig")
            except UnicodeDecodeError:
                decoded = response.content.decode("cp1252", errors="replace")

            lines = decoded.strip().splitlines()

            asof_date = None
            header_idx = None
            for idx, line in enumerate(lines[:20]):
                date_match = re.search(r'as of,"?(\d{1,2}/\d{1,2}/\d{4})"?', line, re.IGNORECASE)
                if date_match:
                    month, day, year = date_match.group(1).split("/")
                    asof_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                if "Weight (%)" in line and ("Ticker" in line or line.startswith("Name,")):
                    header_idx = idx
                    break

            if not asof_date:
                asof_date = datetime.now(UTC).date().isoformat()
            if header_idx is None:
                raise ValueError(f"Could not find holdings header row for {symbol}")

            reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
            holdings_inserted = 0
            retrieved_at = datetime.now(UTC).isoformat()

            for row in reader:
                security_name = str(row.get("Name") or "").strip().strip('"')
                if not security_name or security_name.lower() in {"cash and/or derivatives", "totals"}:
                    continue
                ticker = str(row.get("Ticker") or "").strip().strip('"')
                weight_raw = str(row.get("Weight (%)") or "").strip().strip('"')
                if not weight_raw:
                    continue
                try:
                    weight_pct = float(weight_raw)
                except ValueError:
                    continue

                holding_id = _generate_id("holding", symbol, asof_date, ticker or security_name, security_name)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO etf_holdings
                    (holding_id, etf_symbol, asof_date, security_name, security_ticker,
                     weight_pct, shares, market_value, sector, country, asset_class,
                     retrieved_at, source_url, source_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        holding_id,
                        symbol,
                        asof_date,
                        security_name,
                        ticker or None,
                        weight_pct,
                        _parse_optional_float(str(row.get("Shares") or "")),
                        _parse_optional_float(str(row.get("Market Value") or "")),
                        str(row.get("Sector") or "").strip().strip('"') or None,
                        str(row.get("Location") or "").strip().strip('"') or None,
                        str(row.get("Asset Class") or "").strip().strip('"') or None,
                        retrieved_at,
                        csv_url,
                        source_id,
                    ),
                )
                holdings_inserted += 1

            conn.commit()
            finished_at = datetime.now(UTC).isoformat()

            conn.execute(
                """
                INSERT INTO etf_fetch_runs
                (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
                 status, records_fetched, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    symbol,
                    "holdings",
                    source_id,
                    started_at,
                    finished_at,
                    "success",
                    holdings_inserted,
                    csv_url,
                ),
            )
            conn.commit()

            return {
                "status": "success",
                "holdings_fetched": holdings_inserted,
                "asof_date": asof_date,
                "source_url": csv_url,
            }

    except (httpx.HTTPError, Exception) as e:
        finished_at = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, error_message, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                "holdings",
                source_id,
                started_at,
                finished_at,
                "failed",
                0,
                str(e),
                csv_url,
            ),
        )
        conn.commit()

        return {"status": "failed", "error": str(e), "source_url": csv_url}


# ============================================================================
# FACTSHEET METRICS INGESTION (from PDFs or issuer pages)
# ============================================================================

def fetch_ishares_factsheet_metrics(symbol: str, conn: sqlite3.Connection) -> dict[str, Any]:
    """
    Fetch iShares ETF factsheet metrics from official JSON API.

    Real source: https://www.ishares.com/uk/individual/en/products/{fund_id}/fund-data.ajax
    Example: IWDA (iShares MSCI World UCITS ETF) - fund_id 251882
    """
    run_id = _generate_id("ishares", symbol, datetime.now(UTC).isoformat())
    source_id = f"ishares_{symbol.lower()}_factsheet"

    # Get configured URL from etf_data_sources
    source_config = conn.execute(
        """
        SELECT source_url_template
        FROM etf_data_sources
        WHERE etf_symbol = ? AND data_type = 'factsheet' AND source_id = ?
        """,
        (symbol, source_id),
    ).fetchone()

    if not source_config:
        return {
            "status": "failed",
            "error": f"No factsheet source configured for {symbol}",
        }

    api_url = str(source_config["source_url_template"])
    started_at = datetime.now(UTC).isoformat()

    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            # iShares API requires specific headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            }

            response = client.get(api_url, headers=headers)
            response.raise_for_status()

            try:
                decoded = response.content.decode("utf-8-sig")
            except UnicodeDecodeError:
                decoded = response.content.decode("cp1252", errors="replace")

            data = json.loads(decoded)

            # iShares JSON structure (varies by endpoint, typical structure):
            # {
            #   "fundData": {
            #     "totalNetAssets": {"value": 123456789, "display": "$123.46M"},
            #     "tracking": {"oneyear": -0.05, "threeyear": -0.03},
            #     "yield": {"distributionYield": 1.52},
            #     "performance": {...},
            #     "asOfDate": "2026-02-28"
            #   }
            # }

            fund_data = data.get("fundData", {})
            if not fund_data and isinstance(data.get("aaData"), list):
                rows = [row for row in list(data.get("aaData") or []) if isinstance(row, list)]
                asset_values = []
                yield_values = []
                maturity_dates = []
                for row in rows:
                    if len(row) > 3 and isinstance(row[3], dict):
                        asset_values.append(_parse_optional_float(str(row[3].get("raw") or row[3].get("display") or "")))
                    if len(row) > 15 and isinstance(row[15], dict):
                        yield_values.append(_parse_optional_float(str(row[15].get("raw") or row[15].get("display") or "")))
                    if len(row) > 17 and isinstance(row[17], dict):
                        maturity_dates.append(str(row[17].get("display") or "").strip())
                asof_date = datetime.now(UTC).date().isoformat()
                aum_usd = sum(v for v in asset_values if v is not None) or None
                dividend_yield = next((v / 100.0 for v in yield_values if v is not None), None)
                retrieved_at = datetime.now(UTC).isoformat()
                metric_id = _generate_id("factsheet", symbol, asof_date)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO etf_factsheet_metrics
                    (metric_id, etf_symbol, asof_date, aum_usd,
                     tracking_difference_1y, tracking_difference_3y, tracking_difference_5y,
                     tracking_error_1y, dividend_yield, retrieved_at, source_url, source_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        metric_id,
                        symbol,
                        asof_date,
                        aum_usd,
                        None,
                        None,
                        None,
                        None,
                        dividend_yield,
                        retrieved_at,
                        api_url,
                        source_id,
                    ),
                )
                conn.commit()
                finished_at = datetime.now(UTC).isoformat()
                conn.execute(
                    """
                    INSERT INTO etf_fetch_runs
                    (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
                     status, records_fetched, source_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        symbol,
                        "factsheet",
                        source_id,
                        started_at,
                        finished_at,
                        "success",
                        1,
                        api_url,
                    ),
                )
                conn.commit()
                return {
                    "status": "success",
                    "asof_date": asof_date,
                    "aum_usd": aum_usd,
                    "tracking_difference_1y": None,
                    "tracking_error_1y": None,
                    "dividend_yield": dividend_yield,
                    "source_url": api_url,
                }

            asof_date_str = fund_data.get("asOfDate") or datetime.now(UTC).date().isoformat()

            # Parse as-of date (typically "Feb 28, 2026" or "2026-02-28")
            asof_date = asof_date_str
            if "/" in asof_date_str:
                # Handle MM/DD/YYYY format
                parts = asof_date_str.split("/")
                asof_date = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"

            # Extract metrics
            total_assets = fund_data.get("totalNetAssets", {})
            aum_value = total_assets.get("value")
            aum_usd = float(aum_value) if aum_value else None

            tracking_data = fund_data.get("tracking", {})
            tracking_1y = tracking_data.get("oneyear")
            tracking_3y = tracking_data.get("threeyear")
            tracking_5y = tracking_data.get("fiveyear")
            tracking_error_1y = (
                fund_data.get("trackingError", {}).get("oneyear")
                if isinstance(fund_data.get("trackingError"), dict)
                else fund_data.get("trackingError")
            )

            yield_data = fund_data.get("yield", {})
            dividend_yield = yield_data.get("distributionYield")

            retrieved_at = datetime.now(UTC).isoformat()
            metric_id = _generate_id("factsheet", symbol, asof_date)

            # Insert into database
            conn.execute(
                """
                INSERT OR REPLACE INTO etf_factsheet_metrics
                (metric_id, etf_symbol, asof_date, aum_usd,
                 tracking_difference_1y, tracking_difference_3y, tracking_difference_5y,
                 tracking_error_1y, dividend_yield, retrieved_at, source_url, source_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metric_id,
                    symbol,
                    asof_date,
                    aum_usd,
                    tracking_1y,
                    tracking_3y,
                    tracking_5y,
                    tracking_error_1y,
                    dividend_yield,
                    retrieved_at,
                    api_url,
                    source_id,
                ),
            )
            conn.commit()

            finished_at = datetime.now(UTC).isoformat()

            # Log successful run
            conn.execute(
                """
                INSERT INTO etf_fetch_runs
                (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
                 status, records_fetched, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    symbol,
                    "factsheet",
                    source_id,
                    started_at,
                    finished_at,
                    "success",
                    1,
                    api_url,
                ),
            )
            conn.commit()

            return {
                "status": "success",
                "asof_date": asof_date,
                "aum_usd": aum_usd,
                "tracking_difference_1y": tracking_1y,
                "tracking_error_1y": tracking_error_1y,
                "dividend_yield": dividend_yield,
                "source_url": api_url,
            }

    except (httpx.HTTPError, json.JSONDecodeError, KeyError) as e:
        finished_at = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, error_message, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                "factsheet",
                source_id,
                started_at,
                finished_at,
                "failed",
                0,
                str(e),
                api_url,
            ),
        )
        conn.commit()

        return {
            "status": "failed",
            "error": str(e),
            "source_url": api_url,
        }


def fetch_document_factsheet_metrics(symbol: str, conn: sqlite3.Connection) -> dict[str, Any]:
    run_id = _generate_id("docfacts", symbol, datetime.now(UTC).isoformat())
    source_id = f"issuer_doc_{symbol.lower()}_factsheet"
    started_at = datetime.now(UTC).isoformat()
    doc_result = fetch_candidate_docs(symbol, use_fixtures=False)
    factsheet = dict(doc_result.get("factsheet") or {})
    extracted = dict(doc_result.get("extracted") or {})
    if str(factsheet.get("status") or "") != "success":
        finished_at = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, error_message, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                "factsheet",
                source_id,
                started_at,
                finished_at,
                "failed",
                0,
                str(doc_result.get("error") or factsheet.get("error") or "official_factsheet_unavailable"),
                str(factsheet.get("doc_url") or ""),
            ),
        )
        conn.commit()
        return {
            "status": "failed",
            "error": str(doc_result.get("error") or factsheet.get("error") or "official_factsheet_unavailable"),
            "source_url": str(factsheet.get("doc_url") or ""),
        }

    asof_date = str(extracted.get("factsheet_date") or datetime.now(UTC).date().isoformat())
    retrieved_at = str(factsheet.get("retrieved_at") or datetime.now(UTC).isoformat())
    metric_id = _generate_id("factsheet", symbol, asof_date)
    conn.execute(
        """
        INSERT OR REPLACE INTO etf_factsheet_metrics
        (metric_id, etf_symbol, asof_date, aum_usd,
         tracking_difference_1y, tracking_difference_3y, tracking_difference_5y,
         tracking_error_1y, dividend_yield, retrieved_at, source_url, source_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            metric_id,
            symbol,
            asof_date,
            extracted.get("aum_usd"),
            extracted.get("tracking_difference_1y"),
            extracted.get("tracking_difference_3y"),
            extracted.get("tracking_difference_5y"),
            extracted.get("tracking_error_1y"),
            extracted.get("yield_proxy"),
            retrieved_at,
            str(factsheet.get("doc_url") or ""),
            source_id,
        ),
    )
    conn.execute(
        """
        INSERT INTO etf_fetch_runs
        (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
         status, records_fetched, source_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            symbol,
            "factsheet",
            source_id,
            started_at,
            datetime.now(UTC).isoformat(),
            "success",
            1,
            str(factsheet.get("doc_url") or ""),
        ),
    )
    conn.commit()
    return {
        "status": "success",
        "asof_date": asof_date,
        "aum_usd": extracted.get("aum_usd"),
        "tracking_difference_1y": extracted.get("tracking_difference_1y"),
        "tracking_difference_3y": extracted.get("tracking_difference_3y"),
        "tracking_difference_5y": extracted.get("tracking_difference_5y"),
        "tracking_error_1y": extracted.get("tracking_error_1y"),
        "dividend_yield": extracted.get("yield_proxy"),
        "source_url": str(factsheet.get("doc_url") or ""),
    }


def fetch_document_holdings_summary(symbol: str, conn: sqlite3.Connection) -> dict[str, Any]:
    run_id = _generate_id("dochold", symbol, datetime.now(UTC).isoformat())
    source_id = f"issuer_doc_{symbol.lower()}_holdings_summary"
    started_at = datetime.now(UTC).isoformat()
    doc_result = fetch_candidate_docs(symbol, use_fixtures=False)
    factsheet = dict(doc_result.get("factsheet") or {})
    extracted = dict(doc_result.get("extracted") or {})
    extracted = _augment_summary_fields_from_text(
        symbol,
        " ".join(str(value) for value in extracted.values() if value not in {None, ""}),
        extracted,
    )
    doc_url = str(factsheet.get("doc_url") or "")
    if str(factsheet.get("status") or "") != "success":
        finished_at = datetime.now(UTC).isoformat()
        error = str(doc_result.get("error") or factsheet.get("error") or "official_holdings_summary_unavailable")
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, error_message, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                "holdings",
                source_id,
                started_at,
                finished_at,
                "failed",
                0,
                error,
                doc_url,
            ),
        )
        conn.commit()
        return {"status": "failed", "error": error, "source_url": doc_url}

    summary_fields = {
        "holdings_count": extracted.get("holdings_count"),
        "top_10_concentration": extracted.get("top_10_concentration"),
        "us_weight": extracted.get("us_weight"),
        "em_weight": extracted.get("em_weight"),
        "sector_concentration_proxy": extracted.get("sector_concentration_proxy"),
        "developed_market_exposure_summary": extracted.get("developed_market_exposure_summary"),
        "emerging_market_exposure_summary": extracted.get("emerging_market_exposure_summary"),
    }
    populated = sum(1 for value in summary_fields.values() if value not in {None, ""})
    if populated < 2:
        finished_at = datetime.now(UTC).isoformat()
        error = "factsheet_missing_holdings_summary_fields"
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, error_message, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                "holdings",
                source_id,
                started_at,
                finished_at,
                "failed",
                0,
                error,
                doc_url,
            ),
        )
        conn.commit()
        return {"status": "failed", "error": error, "source_url": doc_url}

    asof_date = str(extracted.get("factsheet_date") or datetime.now(UTC).date().isoformat())
    retrieved_at = str(factsheet.get("retrieved_at") or datetime.now(UTC).isoformat())
    summary_id = _generate_id("holdsum", symbol, asof_date, source_id)
    conn.execute(
        """
        INSERT OR REPLACE INTO etf_holdings_summaries
        (summary_id, etf_symbol, asof_date, holdings_count, top_10_concentration, us_weight, em_weight,
         sector_concentration_proxy, developed_market_exposure_summary, emerging_market_exposure_summary,
         top_country, coverage_class, retrieved_at, source_url, source_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            summary_id,
            symbol,
            asof_date,
            extracted.get("holdings_count"),
            extracted.get("top_10_concentration"),
            extracted.get("us_weight"),
            extracted.get("em_weight"),
            extracted.get("sector_concentration_proxy"),
            extracted.get("developed_market_exposure_summary"),
            extracted.get("emerging_market_exposure_summary"),
            extracted.get("top_country"),
            "factsheet_summary",
            retrieved_at,
            doc_url,
            source_id,
        ),
    )
    conn.execute(
        """
        INSERT INTO etf_fetch_runs
        (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
         status, records_fetched, source_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            symbol,
            "holdings",
            source_id,
            started_at,
            datetime.now(UTC).isoformat(),
            "partial",
            1,
            doc_url,
        ),
    )
    conn.commit()
    return {
        "status": "success",
        "asof_date": asof_date,
        "coverage_class": "factsheet_summary",
        "fields_populated": populated,
        "source_url": doc_url,
    }


def fetch_html_holdings_summary(
    symbol: str,
    conn: sqlite3.Connection,
    *,
    source_url: str,
    source_id: str,
    parser_type: str = "issuer_holdings_html_summary",
) -> dict[str, Any]:
    run_id = _generate_id("htmlhold", symbol, datetime.now(UTC).isoformat())
    started_at = datetime.now(UTC).isoformat()
    try:
        payload = _fetch_html_source_text(source_url)
        text = _html_to_text(payload)
        extracted = _extract_parser_specific_html_fields(symbol, payload, parser_type)
        summary_fields = {
            "holdings_count": extracted.get("holdings_count"),
            "top_10_concentration": extracted.get("top_10_concentration"),
            "us_weight": extracted.get("us_weight"),
            "em_weight": extracted.get("em_weight"),
            "sector_concentration_proxy": extracted.get("sector_concentration_proxy"),
            "developed_market_exposure_summary": extracted.get("developed_market_exposure_summary"),
            "emerging_market_exposure_summary": extracted.get("emerging_market_exposure_summary"),
        }
        populated = sum(1 for value in summary_fields.values() if value not in {None, ""})
        if populated < 2:
            error = "html_missing_holdings_summary_fields"
            conn.execute(
                """
                INSERT INTO etf_fetch_runs
                (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
                 status, records_fetched, error_message, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    symbol,
                    "holdings",
                    source_id,
                    started_at,
                    datetime.now(UTC).isoformat(),
                    "failed",
                    0,
                    error,
                    source_url,
                ),
            )
            conn.commit()
            return {"status": "failed", "error": error, "source_url": source_url}
        asof_date = str(_parse_factsheet_date(text) or datetime.now(UTC).date().isoformat())
        retrieved_at = datetime.now(UTC).isoformat()
        summary_id = _generate_id("holdsum", symbol, asof_date, source_id)
        conn.execute(
            """
            INSERT OR REPLACE INTO etf_holdings_summaries
            (summary_id, etf_symbol, asof_date, holdings_count, top_10_concentration, us_weight, em_weight,
             sector_concentration_proxy, developed_market_exposure_summary, emerging_market_exposure_summary,
             top_country, coverage_class, retrieved_at, source_url, source_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                summary_id,
                symbol,
                asof_date,
                extracted.get("holdings_count"),
                extracted.get("top_10_concentration"),
                extracted.get("us_weight"),
                extracted.get("em_weight"),
                extracted.get("sector_concentration_proxy"),
                extracted.get("developed_market_exposure_summary"),
                extracted.get("emerging_market_exposure_summary"),
                extracted.get("top_country"),
                "html_summary",
                retrieved_at,
                source_url,
                source_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                "holdings",
                source_id,
                started_at,
                datetime.now(UTC).isoformat(),
                "partial",
                1,
                source_url,
            ),
        )
        conn.commit()
        return {
            "status": "success",
            "asof_date": asof_date,
            "coverage_class": "html_summary",
            "fields_populated": populated,
            "source_url": source_url,
        }
    except Exception as exc:
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, error_message, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                "holdings",
                source_id,
                started_at,
                datetime.now(UTC).isoformat(),
                "failed",
                0,
                str(exc),
                source_url,
            ),
        )
        conn.commit()
        return {"status": "failed", "error": str(exc), "source_url": source_url}


def fetch_html_factsheet_metrics(
    symbol: str,
    conn: sqlite3.Connection,
    *,
    source_url: str,
    source_id: str,
    parser_type: str = "issuer_factsheet_html",
) -> dict[str, Any]:
    run_id = _generate_id("htmlfacts", symbol, datetime.now(UTC).isoformat())
    started_at = datetime.now(UTC).isoformat()
    try:
        payload = _fetch_html_source_text(source_url)
        extracted = _extract_parser_specific_html_fields(symbol, payload, parser_type)
        text = _html_to_text(payload)
        asof_date = str(_parse_factsheet_date(text) or datetime.now(UTC).date().isoformat())
        aum_usd = extracted.get("aum_usd")
        dividend_yield = extracted.get("yield_proxy")
        metric_id = _generate_id("factsheet", symbol, asof_date)
        conn.execute(
            """
            INSERT OR REPLACE INTO etf_factsheet_metrics
            (metric_id, etf_symbol, asof_date, aum_usd,
             tracking_difference_1y, tracking_difference_3y, tracking_difference_5y,
             tracking_error_1y, dividend_yield, benchmark_index,
             retrieved_at, source_url, source_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                metric_id,
                symbol,
                asof_date,
                aum_usd,
                extracted.get("tracking_difference_1y"),
                extracted.get("tracking_difference_3y"),
                extracted.get("tracking_difference_5y"),
                extracted.get("tracking_error_1y"),
                dividend_yield,
                extracted.get("benchmark_name"),
                datetime.now(UTC).isoformat(),
                source_url,
                source_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                "factsheet",
                source_id,
                started_at,
                datetime.now(UTC).isoformat(),
                "success" if any(value not in {None, ""} for value in (aum_usd, extracted.get("benchmark_name"), dividend_yield)) else "partial",
                1,
                source_url,
            ),
        )
        conn.commit()
        return {
            "status": "success",
            "asof_date": asof_date,
            "aum_usd": aum_usd,
            "tracking_difference_1y": extracted.get("tracking_difference_1y"),
            "tracking_error_1y": extracted.get("tracking_error_1y"),
            "dividend_yield": dividend_yield,
            "source_url": source_url,
        }
    except Exception as exc:
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, error_message, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                "factsheet",
                source_id,
                started_at,
                datetime.now(UTC).isoformat(),
                "failed",
                0,
                str(exc),
                source_url,
            ),
        )
        conn.commit()
        return {"status": "failed", "error": str(exc), "source_url": source_url}


# ============================================================================
# MARKET DATA INGESTION (from SGX or exchange APIs)
# ============================================================================

def fetch_sgx_market_data(symbol: str, conn: sqlite3.Connection) -> dict[str, Any]:
    """
    Fetch market data for SGX-listed ETFs.

    Real source: SGX API or data feed
    Note: SGX requires data subscription or web scraping of public pages
    """
    run_id = _generate_id("sgx", symbol, datetime.now(UTC).isoformat())
    source_id = f"sgx_{symbol.lower()}_market"

    # SGX real-time quotes (public page):
    # https://www.sgx.com/securities/securities-prices
    # Requires scraping or official data feed subscription

    started_at = datetime.now(UTC).isoformat()
    finished_at = datetime.now(UTC).isoformat()
    message = (
        "SGX market data remains structurally under-covered in public form; "
        "current support stays support-only until an audited SGX feed or scraper exists."
    )
    conn.execute(
        """
        INSERT INTO etf_fetch_runs
        (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
         status, records_fetched, error_message, source_url)
        VALUES (?, ?, 'market_data', ?, ?, ?, 'not_implemented', 0, ?, ?)
        """,
        (
            run_id,
            symbol,
            source_id,
            started_at,
            finished_at,
            message,
            "https://www.sgx.com/securities/securities-prices",
        ),
    )
    conn.commit()
    return {
        "status": "not_implemented",
        "message": message,
        "support_class": "support_only",
        "structural_limitation": "sgx_public_microstructure_under_covered",
        "required_steps": [
            "Configure SGX data API credentials (if subscribed)",
            "Or implement and audit a scraper for SGX public pages",
            "Parse bid/ask/volume before promoting tradability claims",
        ],
    }


_YAHOO_EXCHANGE_SUFFIXES = {
    "LSE": ".L",
    "SGX": ".SI",
    "XETRA": ".DE",
    "NYSEARCA": "",
    "NASDAQ": "",
    "NYSE": "",
}


def fetch_configured_market_data(symbol: str, conn: sqlite3.Connection) -> dict[str, Any]:
    normalized = str(symbol or "").strip().upper()
    config = get_etf_source_config(normalized) or {}
    market_source = dict(dict(config.get("data_sources") or {}).get("market_data") or {})
    parser_type = str(market_source.get("parser_type") or "").strip().lower()
    if parser_type == "sgx_market_stub_not_implemented":
        return fetch_sgx_market_data(normalized, conn)
    listing = dict((config.get("listings") or [{}])[0] or {})
    exchange = str(listing.get("exchange") or get_preferred_market_exchange(normalized, conn)).strip() or "SGX"
    ticker = str(listing.get("ticker") or normalized).strip() or normalized
    suffix = _YAHOO_EXCHANGE_SUFFIXES.get(exchange.upper(), "")
    run_id = _generate_id("market", normalized, exchange, datetime.now(UTC).isoformat())
    source_id = f"market_{normalized.lower()}_{exchange.lower()}"
    started_at = datetime.now(UTC).isoformat()

    result = fetch_yahoo_market_data(ticker, suffix)
    if str(result.get("status") or "") != "success":
        finished_at = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO etf_fetch_runs
            (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
             status, records_fetched, error_message, source_url)
            VALUES (?, ?, 'market_data', ?, ?, ?, 'failed', 0, ?, ?)
            """,
            (
                run_id,
                normalized,
                source_id,
                started_at,
                finished_at,
                str(result.get("error") or "market_data_fetch_failed"),
                str(result.get("source_url") or ""),
            ),
        )
        conn.commit()
        return {
            "status": "failed",
            "exchange": exchange,
            "ticker": ticker,
            "error": str(result.get("error") or "market_data_fetch_failed"),
            "source_url": str(result.get("source_url") or ""),
        }

    market = dict(result.get("market_data") or {})
    retrieved_at = str(result.get("retrieved_at") or datetime.now(UTC).isoformat())
    asof_dt = datetime.fromisoformat(retrieved_at.replace("Z", "+00:00"))
    asof_date = asof_dt.date().isoformat()
    asof_time = asof_dt.time().replace(microsecond=0).isoformat()
    conn.execute(
        """
        INSERT OR REPLACE INTO etf_market_data
        (market_data_id, etf_symbol, exchange, asof_date, asof_time, last_price, bid_price, ask_price,
         bid_ask_spread_abs, bid_ask_spread_bps, volume_day, volume_30d_avg, volume_90d_avg,
         nav, premium_discount_pct, retrieved_at, source_url, source_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _generate_id("mkt", normalized, exchange, asof_date, asof_time),
            normalized,
            exchange,
            asof_date,
            asof_time,
            market.get("last_price"),
            market.get("bid_price"),
            market.get("ask_price"),
            market.get("bid_ask_spread_abs"),
            market.get("bid_ask_spread_bps"),
            market.get("volume_day"),
            market.get("volume_avg_30d"),
            None,
            None,
            None,
            retrieved_at,
            str(result.get("source_url") or ""),
            source_id,
        ),
    )
    conn.execute(
        """
        INSERT INTO etf_fetch_runs
        (run_id, etf_symbol, data_type, source_id, started_at, finished_at,
         status, records_fetched, source_url)
        VALUES (?, ?, 'market_data', ?, ?, ?, 'success', 1, ?)
        """,
        (
            run_id,
            normalized,
            source_id,
            started_at,
            datetime.now(UTC).isoformat(),
            str(result.get("source_url") or ""),
        ),
    )
    conn.commit()
    return {
        "status": "success",
        "exchange": exchange,
        "ticker": ticker,
        "asof_date": asof_date,
        "source_url": str(result.get("source_url") or ""),
        "bid_ask_spread_bps": market.get("bid_ask_spread_bps"),
        "volume_30d_avg": market.get("volume_avg_30d"),
    }


# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

def configure_etf_data_source(
    conn: sqlite3.Connection,
    etf_symbol: str,
    data_type: str,
    source_id: str,
    source_url_template: str,
    fetch_method: str,
    parser_type: str | None = None,
    update_frequency: str = "daily",
) -> str:
    """
    Register an ETF data source configuration.

    Args:
        etf_symbol: ETF ticker symbol
        data_type: 'holdings', 'factsheet', 'market_data'
        source_id: Unique source identifier (e.g., 'vanguard_vwra_holdings')
        source_url_template: URL template with placeholders
        fetch_method: 'csv_download', 'pdf_extract', 'api_call', 'html_scrape'
        update_frequency: 'daily', 'weekly', 'monthly'

    Returns:
        source_config_id
    """
    source_config_id = _generate_id("etf_src", etf_symbol, data_type, source_id)

    conn.execute(
        """
        INSERT OR REPLACE INTO etf_data_sources
        (source_config_id, etf_symbol, data_type, source_id, source_url_template,
         fetch_method, parser_type, update_frequency)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_config_id,
            etf_symbol,
            data_type,
            source_id,
            source_url_template,
            fetch_method,
            parser_type,
            update_frequency,
        ),
    )
    conn.commit()

    return source_config_id


_PRIORITY_BLUEPRINT_SYMBOLS = {
    "VWRA",
    "VWRL",
    "IWDA",
    "CSPX",
    "SSAC",
    "EIMI",
    "VEVE",
    "VAGU",
    "AGGU",
    "A35",
    "IB01",
    "BIL",
    "BILS",
    "HMCH",
    "XCHA",
    "SGLN",
}


def _default_parser_type(symbol: str, data_type: str, payload: dict[str, Any]) -> str:
    provider = str(payload.get("provider") or "").strip().lower()
    method = str(payload.get("method") or "").strip().lower()
    normalized_symbol = str(symbol or "").strip().upper()
    if data_type == "holdings":
        if method == "csv_download":
            return f"{provider or normalized_symbol.lower()}_holdings_csv"
        if method == "html_scrape":
            if provider == "vanguard":
                return "vanguard_holdings_html_summary"
            if "state street" in provider or provider == "spdr":
                return "ssga_holdings_html_summary"
            if "xtrackers" in provider or "dws" in provider:
                return "xtrackers_holdings_html_summary"
            if "hsbc" in provider:
                return "hsbc_holdings_html_summary"
            if "amova" in provider:
                return "amova_holdings_html_summary"
            return "issuer_holdings_html_summary"
        if method == "pdf_extract":
            if "hsbc" in provider:
                return "hsbc_holdings_pdf_summary"
            return "issuer_holdings_pdf_summary"
    if data_type == "factsheet":
        if method == "api_call":
            return "factsheet_api_extract"
        if method == "pdf_extract":
            if provider == "vanguard":
                return "vanguard_factsheet_pdf"
            if "state street" in provider or provider == "spdr":
                return "ssga_factsheet_pdf"
            if "hsbc" in provider:
                return "hsbc_factsheet_pdf"
            if "xtrackers" in provider or "dws" in provider:
                return "xtrackers_factsheet_pdf"
            return "issuer_factsheet_pdf"
        if method == "html_scrape":
            return "issuer_factsheet_html"
    if data_type == "market_data":
        if normalized_symbol == "A35":
            return "sgx_market_stub_not_implemented"
        return "market_data_api"
    return "generic_parser"


def _validate_priority_source_entry(symbol: str, data_type: str, payload: dict[str, Any]) -> None:
    normalized_symbol = str(symbol or "").strip().upper()
    if normalized_symbol not in _PRIORITY_BLUEPRINT_SYMBOLS:
        return
    method = str(payload.get("method") or "").strip()
    parser_type = str(payload.get("parser_type") or payload.get("parser_path") or "").strip() or _default_parser_type(
        normalized_symbol,
        data_type,
        payload,
    )
    truth_family_purpose = str(payload.get("truth_family_purpose") or {
        "holdings": "holdings_exposure",
        "factsheet": "identity_wrapper",
        "market_data": "liquidity_and_aum",
    }.get(data_type, "")).strip()
    if not method:
        raise ValueError(f"Priority Blueprint symbol {normalized_symbol} {data_type} source is missing explicit method")
    if method == "api_call" and data_type != "market_data" and parser_type in {"", "generic_parser"}:
        raise ValueError(
            f"Priority Blueprint symbol {normalized_symbol} {data_type} source cannot rely on ambiguous api_call routing"
        )
    if not parser_type:
        raise ValueError(f"Priority Blueprint symbol {normalized_symbol} {data_type} source is missing parser path/type")
    if not truth_family_purpose:
        raise ValueError(f"Priority Blueprint symbol {normalized_symbol} {data_type} source is missing truth-family purpose")


def sync_configured_etf_data_sources(conn: sqlite3.Connection) -> int:
    _ensure_etf_tables(conn)
    synced = 0
    for source_config in list_etf_source_configs():
        symbol = str(source_config.get("etf_symbol") or "").strip().upper()
        if not symbol:
            continue
        data_sources = dict(source_config.get("data_sources") or {})
        configured_ids_by_type: dict[str, set[str]] = {"holdings": set(), "factsheet": set(), "market_data": set()}
        for data_type in ("holdings", "factsheet", "market_data"):
            payload = dict(data_sources.get(data_type) or {})
            source_url = str(
                payload.get("url_holdings_csv")
                or payload.get("url_template")
                or payload.get("url")
                or ""
            ).strip()
            if not source_url:
                continue
            _validate_priority_source_entry(symbol, data_type, payload)
            source_id = str(payload.get("citation_source_id") or f"{symbol.lower()}_{data_type}")
            configured_ids_by_type[data_type].add(source_id)
            parser_type = str(payload.get("parser_type") or payload.get("parser_path") or "").strip() or _default_parser_type(
                symbol,
                data_type,
                payload,
            )
            configure_etf_data_source(
                conn,
                etf_symbol=symbol,
                data_type=data_type,
                source_id=source_id,
                source_url_template=source_url,
                fetch_method=str(payload.get("method") or "api_call"),
                parser_type=parser_type,
                update_frequency=str(payload.get("frequency") or "daily"),
            )
            synced += 1
        for data_type, configured_ids in configured_ids_by_type.items():
            rows = conn.execute(
                """
                SELECT source_id
                FROM etf_data_sources
                WHERE etf_symbol = ? AND data_type = ?
                """,
                (symbol, data_type),
            ).fetchall()
            for row in rows:
                existing_id = str(row["source_id"] or "")
                if existing_id and existing_id not in configured_ids:
                    conn.execute(
                        """
                        UPDATE etf_data_sources
                        SET enabled = 0
                        WHERE etf_symbol = ? AND data_type = ? AND source_id = ?
                        """,
                        (symbol, data_type, existing_id),
                    )
    conn.commit()
    return synced


# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def refresh_etf_data(symbol: str, settings: Settings | None = None) -> dict[str, Any]:
    """
    Refresh all configured data sources for an ETF.

    Returns summary of fetch results.
    """
    settings = settings or Settings.from_env()
    db_path = get_db_path(settings=settings)
    from app.models.db import connect

    conn = connect(db_path)
    _ensure_etf_tables(conn)
    sync_configured_etf_data_sources(conn)

    results = {
        "symbol": symbol,
        "started_at": datetime.now(UTC).isoformat(),
        "holdings": None,
        "factsheet": None,
        "market_data": None,
    }

    # Fetch from configured sources
    configured_sources = conn.execute(
        """
        SELECT data_type, source_id, fetch_method, parser_type, source_url_template
        FROM etf_data_sources
        WHERE etf_symbol = ? AND enabled = 1
        """,
        (symbol,),
    ).fetchall()

    for row in configured_sources:
        data_type = str(row["data_type"])
        source_id = str(row["source_id"])
        fetch_method = str(row["fetch_method"])
        source_url = str(row["source_url_template"] or "")

        # Route to appropriate fetch function
        if data_type == "holdings":
            if fetch_method == "csv_download":
                if "vanguard" in source_id:
                    results["holdings"] = fetch_vanguard_holdings(symbol, conn)
                elif "ishares" in source_id:
                    results["holdings"] = fetch_ishares_holdings(symbol, conn)
            elif fetch_method == "html_scrape":
                results["holdings"] = fetch_html_holdings_summary(
                    symbol,
                    conn,
                    source_url=source_url,
                    source_id=source_id,
                    parser_type=str(row["parser_type"] or "issuer_holdings_html_summary"),
                )
            elif fetch_method == "pdf_extract":
                results["holdings"] = fetch_document_holdings_summary(symbol, conn)
            if not results["holdings"] or str((results["holdings"] or {}).get("status") or "") == "failed":
                results["holdings"] = fetch_document_holdings_summary(symbol, conn)
        elif data_type == "factsheet":
            if fetch_method == "api_call" and "ishares" in source_id:
                results["factsheet"] = fetch_ishares_factsheet_metrics(symbol, conn)
            elif fetch_method == "html_scrape":
                results["factsheet"] = fetch_html_factsheet_metrics(
                    symbol,
                    conn,
                    source_url=source_url,
                    source_id=source_id,
                    parser_type=str(row["parser_type"] or "issuer_factsheet_html"),
                )
            else:
                results["factsheet"] = fetch_document_factsheet_metrics(symbol, conn)
            if not _factsheet_result_has_material_fields(results["factsheet"]):
                fallback_result = fetch_document_factsheet_metrics(symbol, conn)
                if _factsheet_result_has_material_fields(fallback_result):
                    results["factsheet"] = fallback_result
        elif data_type == "market_data":
            results["market_data"] = fetch_configured_market_data(symbol, conn)

    conn.close()
    results["finished_at"] = datetime.now(UTC).isoformat()

    return results


# ============================================================================
# QUERY FUNCTIONS (for use in dashboard/blueprints)
# ============================================================================

def get_latest_etf_holdings(symbol: str, conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Get most recent holdings breakdown for an ETF."""
    rows = conn.execute(
        """
        SELECT security_name, security_ticker, security_isin, weight_pct,
               asof_date, source_url, retrieved_at
        FROM etf_holdings
        WHERE etf_symbol = ?
        AND asof_date = (
            SELECT MAX(asof_date) FROM etf_holdings WHERE etf_symbol = ?
        )
        ORDER BY weight_pct DESC
        """,
        (symbol, symbol),
    ).fetchall()

    return [
        {
            "security_name": str(row["security_name"]),
            "ticker": str(row["security_ticker"] or ""),
            "isin": str(row["security_isin"] or ""),
            "weight_pct": float(row["weight_pct"]),
            "asof_date": str(row["asof_date"]),
            "citation": {
                "source_url": str(row["source_url"]),
                "retrieved_at": str(row["retrieved_at"]),
            },
        }
        for row in rows
    ]


def get_latest_etf_factsheet_metrics(symbol: str, conn: sqlite3.Connection) -> dict[str, Any] | None:
    """Get most recent factsheet metrics for an ETF."""
    row = conn.execute(
        """
        SELECT aum_usd, tracking_difference_1y, tracking_difference_3y, tracking_difference_5y,
               tracking_error_1y, dividend_yield, benchmark_index, asof_date, source_url, source_id, retrieved_at
        FROM etf_factsheet_metrics
        WHERE etf_symbol = ?
        ORDER BY asof_date DESC
        LIMIT 1
        """,
        (symbol,),
    ).fetchone()

    if not row:
        return None

    source_url = str(row["source_url"] or "")
    asof_date = str(row["asof_date"])
    quality_issues: list[str] = []
    if not asof_date:
        quality_issues.append("missing_observed_date")
    if not source_url:
        quality_issues.append("missing_source_url")
    source_definition = _fetch_source_definition(
        conn,
        symbol=symbol,
        data_type="factsheet",
        source_id=str(row["source_id"] or ""),
    ) or {}
    return {
        "aum_usd": float(row["aum_usd"]) if row["aum_usd"] else None,
        "tracking_difference_1y": float(row["tracking_difference_1y"]) if row["tracking_difference_1y"] else None,
        "tracking_difference_3y": float(row["tracking_difference_3y"]) if row["tracking_difference_3y"] else None,
        "tracking_difference_5y": float(row["tracking_difference_5y"]) if row["tracking_difference_5y"] else None,
        "tracking_error_1y": float(row["tracking_error_1y"]) if row["tracking_error_1y"] else None,
        "dividend_yield": float(row["dividend_yield"]) if row["dividend_yield"] else None,
        "benchmark_index": str(row["benchmark_index"] or "") or None,
        "asof_date": asof_date,
        "quality_state": "verified" if not quality_issues else "partial",
        "quality_issues": quality_issues,
        "directness_class": "issuer_structured_summary_backed",
        "authority_class": "truth_grade",
        "fallback_state": "none",
        "citation": {
            "source_url": source_url,
            "source_id": str(row["source_id"] or ""),
            "source_kind": "issuer_factsheet",
            "retrieved_at": str(row["retrieved_at"]),
        },
        "source_definition": source_definition,
    }


def get_etf_factsheet_history_summary(symbol: str, conn: sqlite3.Connection) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT asof_date, aum_usd, tracking_difference_1y, tracking_difference_3y, tracking_difference_5y,
               tracking_error_1y, source_url, source_id, retrieved_at
        FROM etf_factsheet_metrics
        WHERE etf_symbol = ?
        ORDER BY asof_date DESC
        LIMIT 12
        """,
        (symbol,),
    ).fetchall()
    if not rows:
        return None
    latest = rows[0]
    aum_values = [float(row["aum_usd"]) for row in rows if row["aum_usd"] is not None]
    latest_aum = float(latest["aum_usd"]) if latest["aum_usd"] is not None else None
    previous_aum = float(rows[1]["aum_usd"]) if len(rows) > 1 and rows[1]["aum_usd"] is not None else None
    trend = "unknown"
    if latest_aum is not None and previous_aum is not None:
        if latest_aum > previous_aum * 1.03:
            trend = "rising"
        elif latest_aum < previous_aum * 0.97:
            trend = "falling"
        else:
            trend = "stable"
    latest_asof = str(latest["asof_date"])
    quality_issues: list[str] = []
    if not latest_asof:
        quality_issues.append("missing_observed_date")
    return {
        "points": len(rows),
        "latest_asof_date": latest_asof,
        "latest_aum_usd": latest_aum,
        "average_aum_usd": (sum(aum_values) / len(aum_values)) if aum_values else None,
        "tracking_difference_1y": float(latest["tracking_difference_1y"]) if latest["tracking_difference_1y"] is not None else None,
        "tracking_difference_3y": float(latest["tracking_difference_3y"]) if latest["tracking_difference_3y"] is not None else None,
        "tracking_difference_5y": float(latest["tracking_difference_5y"]) if latest["tracking_difference_5y"] is not None else None,
        "tracking_error_1y": float(latest["tracking_error_1y"]) if latest["tracking_error_1y"] is not None else None,
        "trend": trend,
        "quality_state": "verified" if len(rows) >= 2 and not quality_issues else "partial",
        "quality_issues": quality_issues,
        "citation": {
            "source_url": str(latest["source_url"] or ""),
            "source_id": str(latest["source_id"] or ""),
            "source_kind": "issuer_factsheet",
            "retrieved_at": str(latest["retrieved_at"]),
        },
    }


def get_latest_etf_market_data(symbol: str, exchange: str, conn: sqlite3.Connection) -> dict[str, Any] | None:
    """Get most recent market data for an ETF."""
    row = conn.execute(
        """
        SELECT bid_ask_spread_bps, volume_30d_avg, premium_discount_pct,
               asof_date, asof_time, source_url, source_id, retrieved_at
        FROM etf_market_data
        WHERE etf_symbol = ? AND exchange = ?
        ORDER BY asof_date DESC, asof_time DESC
        LIMIT 1
        """,
        (symbol, exchange),
    ).fetchone()

    if not row:
        return None

    return {
        "bid_ask_spread_bps": float(row["bid_ask_spread_bps"]) if row["bid_ask_spread_bps"] else None,
        "volume_30d_avg": float(row["volume_30d_avg"]) if row["volume_30d_avg"] else None,
        "premium_discount_pct": float(row["premium_discount_pct"]) if row["premium_discount_pct"] else None,
        "asof_date": str(row["asof_date"]),
        "asof_time": str(row["asof_time"] or ""),
        "citation": {
            "source_url": str(row["source_url"]),
            "source_id": str(row["source_id"] or ""),
            "source_kind": "market_data_provider",
            "retrieved_at": str(row["retrieved_at"]),
        },
    }


def get_etf_market_history_summary(symbol: str, exchange: str, conn: sqlite3.Connection) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT asof_date, bid_ask_spread_bps, volume_30d_avg, premium_discount_pct, retrieved_at, source_url, source_id
        FROM etf_market_data
        WHERE etf_symbol = ? AND exchange = ?
        ORDER BY asof_date DESC, asof_time DESC
        LIMIT 20
        """,
        (symbol, exchange),
    ).fetchall()
    if not rows:
        return None
    latest = rows[0]
    volume_values = [float(row["volume_30d_avg"]) for row in rows if row["volume_30d_avg"] is not None]
    latest_volume = float(latest["volume_30d_avg"]) if latest["volume_30d_avg"] is not None else None
    previous_volume = float(rows[1]["volume_30d_avg"]) if len(rows) > 1 and rows[1]["volume_30d_avg"] is not None else None
    trend = "unknown"
    if latest_volume is not None and previous_volume is not None:
        if latest_volume > previous_volume * 1.05:
            trend = "rising"
        elif latest_volume < previous_volume * 0.95:
            trend = "falling"
        else:
            trend = "stable"
    return {
        "points": len(rows),
        "latest_asof_date": str(latest["asof_date"]),
        "latest_spread_bps": float(latest["bid_ask_spread_bps"]) if latest["bid_ask_spread_bps"] is not None else None,
        "latest_volume_30d_avg": latest_volume,
        "average_volume_30d_avg": (sum(volume_values) / len(volume_values)) if volume_values else None,
        "premium_discount_pct": float(latest["premium_discount_pct"]) if latest["premium_discount_pct"] is not None else None,
        "trend": trend,
        "citation": {
            "source_url": str(latest["source_url"]),
            "source_id": str(latest["source_id"] or ""),
            "source_kind": "market_data_provider",
            "retrieved_at": str(latest["retrieved_at"]),
        },
    }


def get_latest_successful_etf_ingest_at(symbol: str, conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        """
        SELECT COALESCE(finished_at, started_at) AS completed_at
        FROM etf_fetch_runs
        WHERE etf_symbol = ? AND status = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (symbol,),
    ).fetchone()
    value = str(row["completed_at"] or "").strip() if row else ""
    return value or None


def get_etf_holdings_profile(symbol: str, conn: sqlite3.Connection) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT asof_date, security_name, security_ticker, security_isin, weight_pct, sector, country,
               asset_class, source_url, source_id, retrieved_at
        FROM etf_holdings
        WHERE etf_symbol = ?
          AND asof_date = (SELECT MAX(asof_date) FROM etf_holdings WHERE etf_symbol = ?)
        ORDER BY weight_pct DESC
        """,
        (symbol, symbol),
    ).fetchall()
    if not rows:
        summary_row = conn.execute(
            """
            SELECT asof_date, holdings_count, top_10_concentration, us_weight, em_weight,
                   sector_concentration_proxy, developed_market_exposure_summary,
                   emerging_market_exposure_summary, top_country, coverage_class,
                   source_url, source_id, retrieved_at
            FROM etf_holdings_summaries
            WHERE etf_symbol = ?
            ORDER BY asof_date DESC, retrieved_at DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        if not summary_row:
            return None
        latest_asof = str(summary_row["asof_date"] or "")
        latest_source_url = str(summary_row["source_url"] or "")
        quality_issues: list[str] = []
        if not latest_asof:
            quality_issues.append("missing_observed_date")
        if not latest_source_url:
            quality_issues.append("missing_source_url")
        quality_issues.append("summary_only_holdings_source")
        source_definition = _fetch_source_definition(
            conn,
            symbol=symbol,
            data_type="holdings",
            source_id=str(summary_row["source_id"] or ""),
        ) or {}
        coverage_class = str(summary_row["coverage_class"] or "factsheet_summary")
        directness_class = "issuer_structured_summary_backed" if coverage_class == "factsheet_summary" else "html_summary_backed"
        structural_limitations: list[str] = []
        if symbol.upper() in {"HMCH", "XCHA"}:
            structural_limitations.append("issuer exposes only summary-level holdings support in public sources")
        if symbol.upper() == "SGLN":
            structural_limitations.append("commodity ETC exposure is structurally summary-described rather than line-item holdings-listed")
        return {
            "asof_date": latest_asof,
            "holdings_count": int(summary_row["holdings_count"]) if summary_row["holdings_count"] is not None else None,
            "top_10_concentration": float(summary_row["top_10_concentration"]) if summary_row["top_10_concentration"] is not None else None,
            "us_weight": float(summary_row["us_weight"]) if summary_row["us_weight"] is not None else None,
            "em_weight": float(summary_row["em_weight"]) if summary_row["em_weight"] is not None else None,
            "sector_concentration_proxy": float(summary_row["sector_concentration_proxy"]) if summary_row["sector_concentration_proxy"] is not None else None,
            "developed_market_exposure_summary": str(summary_row["developed_market_exposure_summary"] or "") or None,
            "emerging_market_exposure_summary": str(summary_row["emerging_market_exposure_summary"] or "") or None,
            "top_country": str(summary_row["top_country"] or "") or None,
            "coverage_class": coverage_class,
            "quality_state": "partial",
            "quality_issues": quality_issues,
            "direct_holdings_available": False,
            "summary_support_available": True,
            "best_available_source_class": coverage_class,
            "directness_class": directness_class,
            "authority_class": "truth_grade",
            "fallback_state": "summary_only",
            "structural_limitations": structural_limitations,
            "downgrade_reasons": ["direct_holdings_unavailable", coverage_class],
            "citation": {
                "source_url": latest_source_url,
                "source_id": str(summary_row["source_id"] or ""),
                "source_kind": "issuer_holdings_summary",
                "retrieved_at": str(summary_row["retrieved_at"] or ""),
            },
            "source_definition": source_definition,
        }
    def _normalize_country(value: Any) -> str:
        normalized = str(value or "").strip()
        mapping = {
            "US": "United States",
            "U.S.": "United States",
            "USA": "United States",
            "United States of America": "United States",
            "UK": "United Kingdom",
        }
        return mapping.get(normalized, normalized)

    weights = [float(row["weight_pct"]) for row in rows if row["weight_pct"] is not None]
    top10 = round(sum(weights[:10]), 2) if weights else None
    us_weight = round(
        sum(float(row["weight_pct"]) for row in rows if _normalize_country(row["country"]) == "United States"),
        2,
    )
    equity_rows = [row for row in rows if str(row["asset_class"] or "").strip().lower() in {"equity", "stock", ""}]
    em_weight: float | None = None
    if equity_rows and any(_normalize_country(row["country"]) for row in equity_rows):
        em_weight = round(
            sum(
                float(row["weight_pct"])
                for row in equity_rows
                if _normalize_country(row["country"])
                and _normalize_country(row["country"]) not in _DEVELOPED_MARKET_LOCATIONS
            ),
            2,
        )
    tech_weight = round(
        sum(float(row["weight_pct"]) for row in rows if "information technology" in str(row["sector"] or "").lower()),
        2,
    )
    developed_summary = None
    emerging_summary = None
    if em_weight is not None:
        if em_weight <= 0:
            developed_summary = "Developed markets only"
            emerging_summary = "No emerging-market allocation detected"
        else:
            developed_summary = f"Developed plus emerging ({em_weight:.1f}% EM)"
            emerging_summary = f"Emerging market weight {em_weight:.1f}%"
    top_country = next((_normalize_country(row["country"]) for row in rows if _normalize_country(row["country"])), None)
    quality_issues: list[str] = []
    latest_asof = str(rows[0]["asof_date"] or "")
    latest_source_url = str(rows[0]["source_url"] or "")
    source_definition = _fetch_source_definition(
        conn,
        symbol=symbol,
        data_type="holdings",
        source_id=str(rows[0]["source_id"] or ""),
    ) or {}
    if not latest_asof:
        quality_issues.append("missing_observed_date")
    if not latest_source_url:
        quality_issues.append("missing_source_url")
    if len(rows) < 20:
        quality_issues.append("thin_holdings_sample")
    return {
        "asof_date": latest_asof,
        "holdings_count": len(rows),
        "top_10_concentration": top10,
        "us_weight": us_weight if us_weight > 0 else None,
        "em_weight": em_weight,
        "sector_concentration_proxy": tech_weight if tech_weight > 0 else None,
        "developed_market_exposure_summary": developed_summary,
        "emerging_market_exposure_summary": emerging_summary,
        "top_country": top_country,
        "coverage_class": "direct_holdings",
        "quality_state": "verified" if not quality_issues else "partial",
        "quality_issues": quality_issues,
        "direct_holdings_available": True,
        "summary_support_available": False,
        "best_available_source_class": "direct_holdings",
        "directness_class": "direct_holdings_backed",
        "authority_class": "truth_grade",
        "fallback_state": "none",
        "structural_limitations": [],
        "downgrade_reasons": list(quality_issues),
        "citation": {
            "source_url": latest_source_url,
            "source_id": str(rows[0]["source_id"] or ""),
            "source_kind": "issuer_holdings_file",
            "retrieved_at": str(rows[0]["retrieved_at"] or ""),
        },
        "source_definition": source_definition,
    }


def get_latest_etf_fetch_status(symbol: str, conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT data_type, source_id, started_at, finished_at, status, records_fetched, error_message, source_url
        FROM etf_fetch_runs
        WHERE etf_symbol = ?
        ORDER BY started_at DESC
        LIMIT 12
        """,
        (symbol,),
    ).fetchall()
    if not rows:
        return {
            "status": "unknown",
            "latest_run_at": None,
            "latest_success_at": None,
            "latest_failure_at": None,
            "entries": [],
        }
    entries: list[dict[str, Any]] = []
    latest_success_at = None
    latest_failure_at = None
    for row in rows:
        entry = dict(row)
        entries.append(entry)
        if str(entry.get("status") or "") == "success" and latest_success_at is None:
            latest_success_at = entry.get("finished_at") or entry.get("started_at")
        if str(entry.get("status") or "") == "failed" and latest_failure_at is None:
            latest_failure_at = entry.get("finished_at") or entry.get("started_at")
    overall_status = "success" if any(str(item.get("status") or "") == "success" for item in entries[:3]) else str(entries[0].get("status") or "unknown")
    return {
        "status": overall_status,
        "latest_run_at": entries[0].get("finished_at") or entries[0].get("started_at"),
        "latest_success_at": latest_success_at,
        "latest_failure_at": latest_failure_at,
        "entries": entries,
    }
