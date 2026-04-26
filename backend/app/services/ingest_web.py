from __future__ import annotations

import hashlib
import json
import urllib.request
from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.models.types import SourceRecord


WEB_SOURCES = [
    {
        "source_id": "fred_dgs10",
        "url": "https://fred.stlouisfed.org/series/DGS10",
        "publisher": "FRED",
        "topic": "rates",
        "credibility_tier": "primary",
    },
    {
        "source_id": "fred_t10yie",
        "url": "https://fred.stlouisfed.org/series/T10YIE",
        "publisher": "FRED",
        "topic": "inflation_expectations",
        "credibility_tier": "primary",
    },
    {
        "source_id": "oaktree_sea_change",
        "url": "https://www.oaktreecapital.com/insights/memo/sea-change",
        "publisher": "Oaktree",
        "topic": "guru_view",
        "credibility_tier": "primary",
    },
    {
        "source_id": "taleb_fat_tails",
        "url": "https://www.fooledbyrandomness.com/FatTails.html",
        "publisher": "Nassim Taleb",
        "topic": "guru_view",
        "credibility_tier": "secondary",
    },
    {
        "source_id": "iras_overseas_income",
        "url": "https://www.iras.gov.sg/taxes/individual-income-tax/basics-of-individual-income-tax/what-is-taxable-what-is-not/income-received-from-overseas",
        "publisher": "IRAS",
        "topic": "tax",
        "credibility_tier": "primary",
    },
    {
        "source_id": "irs_withholding_nra",
        "url": "https://www.irs.gov/individuals/international-taxpayers/federal-income-tax-withholding-and-reporting-on-other-kinds-of-us-source-income-paid-to-nonresident-aliens",
        "publisher": "IRS",
        "topic": "tax",
        "credibility_tier": "primary",
    },
]


def _fetch_url(url: str, timeout_seconds: int) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "investment-agent/0.1 (+objective-citation-aggregator)",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8", errors="replace")


def fetch_web_sources(settings: Settings) -> list[SourceRecord]:
    records: list[SourceRecord] = []
    for source in WEB_SOURCES:
        now = datetime.now(UTC)
        body = ""
        try:
            body = _fetch_url(source["url"], settings.web_timeout_seconds)
        except Exception as exc:  # Keep pipeline resilient.
            body = json.dumps({"error": str(exc), "url": source["url"]})

        raw_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
        record = SourceRecord(
            source_id=source["source_id"],
            url=source["url"],
            publisher=source["publisher"],
            retrieved_at=now,
            topic=source["topic"],
            credibility_tier=source["credibility_tier"],
            raw_hash=raw_hash,
            source_type="web",
        )
        records.append(record)
    return records


def important_source_note(source_id: str) -> str:
    notes: dict[str, str] = {
        "fred_dgs10": "Official macro market proxy for discount-rate regime.",
        "fred_t10yie": "Official inflation-expectation proxy used for regime shifts.",
        "oaktree_sea_change": "Primary Howard Marks cycle and risk framework input.",
        "taleb_fat_tails": "Primary Taleb fat-tail and fragility framing source.",
        "iras_overseas_income": "Primary Singapore tax context source.",
        "irs_withholding_nra": "Primary U.S. withholding context source.",
        "stooq_spy_volume": "Public index-participation proxy via liquid S&P 500 ETF volume.",
        "stooq_qqq_volume": "Public ETF-participation proxy for growth-heavy segment volume.",
        "yahoo_gspc_volume": "Public index volume proxy feed; used as fallback only when rate limits permit.",
        "yahoo_spy_volume": "Public ETF volume fallback feed; used for redundancy.",
        "vanguard_vti": "Issuer profile for US-domiciled broad-equity implementation vehicle.",
        "ssga_spy": "Issuer profile for SPY implementation details.",
        "blackrock_ivv": "Issuer profile for IVV implementation details.",
        "blackrock_cspx": "Issuer UCITS profile for CSPX implementation details.",
        "blackrock_iwda": "Issuer UCITS profile for IWDA implementation details.",
        "vanguard_vwra": "Issuer UCITS profile for VWRA implementation details.",
        "vanguard_vwrd": "Issuer UCITS profile for VWRD implementation details.",
        "vanguard_bnd": "Issuer profile for IG bond implementation proxy.",
        "blackrock_aggu": "Issuer UCITS profile for global aggregate bond implementation.",
        "blackrock_igla": "Issuer UCITS profile for government bond implementation.",
        "nikko_a35": "Issuer profile for Singapore bond ETF implementation.",
        "dbmf_site": "Issuer profile for managed futures ETF implementation.",
        "kmlm_site": "Issuer profile for managed futures ETF implementation.",
        "cambria_tail": "Issuer profile for tail-hedge ETF implementation.",
        "alphaarchitect_caos": "Issuer profile for long-volatility ETF implementation.",
        "agf_btal": "Issuer profile for anti-beta ETF implementation.",
        "cboe_options": "Primary contract specification source for SPX options.",
        "occ_options": "Primary contract specification source for US listed equity options.",
    }
    return notes.get(source_id, "Relevant source for cross-checking aggregated insight.")
