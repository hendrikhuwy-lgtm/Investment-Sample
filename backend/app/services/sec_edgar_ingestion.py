"""
SEC EDGAR batch ingestion service for ETF N-PORT holdings data.

IMPORTANT: This is NOT a live request-path adapter.
Schedule as a weekly background job after market close.
Never call from live API surfaces or per-user interactions.

Rate limit: ≤5 req/s (enforced via _MIN_REQUEST_INTERVAL).
Identity: mandatory User-Agent per SEC rules.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from typing import Any

import requests

from app.config import get_db_path
from app.services.blueprint_candidate_truth import upsert_field_observation
from app.services.blueprint_candidate_registry import export_live_candidate_registry
from app.services.provider_cache import put_provider_snapshot


SEC_HEADERS = {
    "User-Agent": "InvestmentAgent admin@investment-agent.local",
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov",
}

# Known CIK mappings: ticker → (CIK, issuer name)
# CIKs are zero-padded to 10 digits as stored in EDGAR.
DEFAULT_CIK_MAP: dict[str, tuple[str, str]] = {
    "SPY": ("0000884394", "SPDR S&P 500 ETF Trust"),
    "IVV": ("0001364742", "iShares Core S&P 500 ETF"),
    "VOO": ("0001229654", "Vanguard S&P 500 ETF"),
    "AGG": ("0001100663", "iShares Core US Aggregate Bond ETF"),
    "GLD": ("0001222333", "SPDR Gold Shares"),
    "ACWI": ("0001379481", "iShares MSCI ACWI ETF"),
    "VTI": ("0001111840", "Vanguard Total Stock Market ETF"),
    "QQQ": ("0001067839", "Invesco QQQ Trust"),
    "VEA": ("0001324848", "Vanguard FTSE Developed Markets ETF"),
    "VWO": ("0001133243", "Vanguard FTSE Emerging Markets ETF"),
    "BND": ("0001482544", "Vanguard Total Bond Market ETF"),
    "TLT": ("0001099290", "iShares 20+ Year Treasury Bond ETF"),
    "SHV": ("0001379400", "iShares Short Treasury Bond ETF"),
}

_RATE_LOCK = threading.Lock()
_LAST_REQUEST_AT: float = 0.0
_MIN_REQUEST_INTERVAL = 0.2  # 5 req/s max


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _safe_get(url: str, *, headers: dict[str, str] | None = None, timeout: int = 15) -> requests.Response:
    global _LAST_REQUEST_AT
    with _RATE_LOCK:
        elapsed = time.monotonic() - _LAST_REQUEST_AT
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        _LAST_REQUEST_AT = time.monotonic()
    resp = requests.get(url, headers=headers or SEC_HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp


def ensure_cik_map_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sec_edgar_cik_map (
            symbol TEXT PRIMARY KEY,
            cik TEXT NOT NULL,
            issuer_name TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def seed_default_cik_map(conn: sqlite3.Connection) -> None:
    ensure_cik_map_table(conn)
    for symbol, (cik, issuer_name) in DEFAULT_CIK_MAP.items():
        conn.execute(
            """
            INSERT INTO sec_edgar_cik_map (symbol, cik, issuer_name, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                cik = excluded.cik,
                issuer_name = excluded.issuer_name,
                updated_at = excluded.updated_at
            """,
            (symbol, cik, issuer_name, _now_iso()),
        )
    conn.commit()


def get_cik_for_symbol(conn: sqlite3.Connection, symbol: str) -> str | None:
    ensure_cik_map_table(conn)
    row = conn.execute(
        "SELECT cik FROM sec_edgar_cik_map WHERE symbol = ?", (symbol.upper(),)
    ).fetchone()
    return str(row["cik"]) if row else None


def fetch_latest_nport_url(cik: str) -> tuple[str, str] | None:
    """
    Fetch the EDGAR submissions JSON for the given CIK and return
    (document_url, filing_date) for the most recent NPORT-P filing.
    Returns None if no NPORT-P is found.
    """
    padded = cik.lstrip("0").zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{padded}.json"
    try:
        resp = _safe_get(url)
        data = resp.json()
    except Exception:
        return None

    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form") or []
    accessions = filings.get("accessionNumber") or []
    primary_docs = filings.get("primaryDocument") or []
    filing_dates = filings.get("filingDate") or []

    for i, form in enumerate(forms):
        if form in {"NPORT-P", "N-PORT"}:
            if i >= len(accessions) or i >= len(primary_docs):
                continue
            accession = accessions[i].replace("-", "")
            primary_doc = primary_docs[i]
            cik_int = int(cik.lstrip("0") or "0")
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession}/{primary_doc}"
            filing_date = filing_dates[i] if i < len(filing_dates) else None
            return doc_url, str(filing_date or "")
    return None


def parse_nport_xml(xml_content: bytes) -> dict[str, Any]:
    """
    Parse N-PORT XML and return normalised ETF fields.
    Returns an empty dict if parsing fails or no useful data is found.
    """

    def _safe_float(v: Any) -> float | None:
        try:
            return float(v) if v not in {None, ""} else None
        except Exception:
            return None

    def _find_text(parent: ET.Element, *tags: str) -> str | None:
        for tag in tags:
            for elem in parent.iter():
                local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                if local == tag and elem.text and elem.text.strip():
                    return elem.text.strip()
        return None

    try:
        root = ET.fromstring(xml_content)
    except Exception:
        return {}

    result: dict[str, Any] = {}

    # Net assets
    net_assets = _safe_float(_find_text(root, "netAssets", "totNetAssets", "totalNetAssets"))
    if net_assets is not None:
        result["aum"] = net_assets

    # Expense ratio (not always present in N-PORT)
    expense_ratio = _safe_float(_find_text(root, "annualizedGrossExpRatio", "annualTotExpRatio", "grossExpRatio"))
    if expense_ratio is not None:
        result["expense_ratio"] = expense_ratio

    # Holdings
    holdings: list[dict[str, Any]] = []
    for elem in root.iter():
        local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local == "invstOrSec":
            name = _find_text(elem, "name")
            pct_val = _safe_float(_find_text(elem, "pctVal", "pct_val"))
            holdings.append({"name": name, "pct_val": pct_val})

    if holdings:
        result["holdings_count"] = len(holdings)
        top_pcts = sorted(
            (h["pct_val"] for h in holdings if h["pct_val"] is not None),
            reverse=True,
        )[:10]
        if top_pcts:
            result["top_10_concentration"] = round(sum(top_pcts), 2)

    # Filing / report period date
    period = _find_text(root, "repPdDate", "repPeriodOfReport", "periodOfReport", "reportPeriodDate")
    if period:
        result["factsheet_asof"] = period

    return result


def ingest_etf_holdings(symbol: str, *, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    """
    Fetch and ingest ETF holdings from SEC EDGAR for a single symbol.
    Stores results in provider_cache_snapshots and candidate_field_observations.
    Returns the extracted fields dict, or {} on failure.

    DO NOT call from live request path.
    """
    close_conn = conn is None
    if conn is None:
        conn = _connection()
    try:
        seed_default_cik_map(conn)
        cik = get_cik_for_symbol(conn, symbol)
        if not cik:
            return {}

        nport_result = fetch_latest_nport_url(cik)
        if not nport_result:
            return {}
        doc_url, filing_date = nport_result

        try:
            resp = _safe_get(
                doc_url,
                headers={
                    "User-Agent": "InvestmentAgent admin@investment-agent.local",
                    "Accept-Encoding": "gzip, deflate",
                },
            )
            xml_content = resp.content
        except Exception:
            return {}

        fields = parse_nport_xml(xml_content)
        if not fields:
            return {}

        if filing_date and "factsheet_asof" not in fields:
            fields["factsheet_asof"] = filing_date

        observed_at = fields.get("factsheet_asof") or _now_iso()[:10]
        payload: dict[str, Any] = {
            **fields,
            "symbol": symbol.upper(),
            "observed_at": observed_at,
            "source_ref": f"sec_edgar:NPORT-P:{cik}",
            "cik": cik,
        }

        # Write to provider_cache_snapshots via the standard put API
        put_provider_snapshot(
            conn,
            provider_name="sec_edgar",
            endpoint_family="etf_holdings",
            cache_key=symbol.upper(),
            payload=payload,
            surface_name="blueprint",
            freshness_state="current",
            confidence_tier="primary",
            source_ref=f"sec_edgar:NPORT-P:{cik}",
            ttl_seconds=604800,  # 1 week
            cache_status="miss",
        )

        # Write field observations directly for blueprint truth resolution
        candidates = export_live_candidate_registry(conn)
        upper_symbol = symbol.upper()
        candidate_rows = [c for c in candidates if str(c.get("symbol") or "").upper() == upper_symbol]

        for field_name, field_value in fields.items():
            for candidate in candidate_rows:
                sleeve_key = str(candidate.get("sleeve_key") or "")
                if not sleeve_key:
                    continue
                missingness = "populated" if field_value not in {None, ""} else "blocked_by_source_gap"
                upsert_field_observation(
                    conn,
                    candidate_symbol=upper_symbol,
                    sleeve_key=sleeve_key,
                    field_name=field_name,
                    value=field_value,
                    source_name="sec_edgar",
                    source_url=f"sec_edgar:NPORT-P:{cik}",
                    observed_at=observed_at,
                    provenance_level="verified_nonissuer",
                    confidence_label="high",
                    parser_method="sec_edgar_ingestion:NPORT-P",
                    missingness_reason=missingness,
                )

        conn.commit()
        return fields
    finally:
        if close_conn:
            conn.close()


def run_edgar_ingestion(symbols: list[str] | None = None) -> dict[str, Any]:
    """
    Run EDGAR ingestion for all symbols in the CIK map (or a provided subset).

    Schedule weekly — NOT in the live request path.
    """
    conn = _connection()
    seed_default_cik_map(conn)

    if symbols is None:
        rows = conn.execute("SELECT symbol FROM sec_edgar_cik_map ORDER BY symbol").fetchall()
        symbols = [str(row["symbol"]) for row in rows]

    results: dict[str, Any] = {}
    for symbol in symbols:
        try:
            fields = ingest_etf_holdings(symbol, conn=conn)
            results[symbol] = {"status": "ok", "fields_extracted": list(fields.keys())}
        except Exception as exc:
            results[symbol] = {"status": "error", "error": str(exc)}
        time.sleep(0.2)  # stay within ≤5 req/s

    conn.close()
    return {"ingested": results, "total": len(symbols), "completed_at": _now_iso()}
