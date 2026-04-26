"""
ETF Document Parser - Extracts verification proofs from issuer PDFs.

Authoritative source for:
- ISIN
- Domicile
- Accumulating vs Distributing
- TER (Total Expense Ratio)
- Factsheet as-of date

NOT used for: market data (Yahoo Finance handles that)
"""

from __future__ import annotations

import hashlib
import html
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

# PDF parsing uses pypdf; pdfplumber remains an optional future upgrade
try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None


_KNOWN_CURRENCY_CODES = {
    "AUD",
    "CAD",
    "CHF",
    "CNH",
    "CNY",
    "EUR",
    "GBP",
    "HKD",
    "JPY",
    "NOK",
    "SEK",
    "SGD",
    "USD",
}

_MONTH_MAP = {
    "jan": "01", "january": "01",
    "feb": "02", "february": "02",
    "mar": "03", "march": "03",
    "apr": "04", "april": "04",
    "may": "05",
    "jun": "06", "june": "06",
    "jul": "07", "july": "07",
    "aug": "08", "august": "08",
    "sep": "09", "sept": "09", "september": "09",
    "oct": "10", "october": "10",
    "nov": "11", "november": "11",
    "dec": "12", "december": "12",
}


def _extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from PDF file."""
    if PdfReader is None:
        raise ImportError("pypdf required for PDF parsing: pip install pypdf")

    text_parts = []

    with open(pdf_path, "rb") as f:
        reader = PdfReader(f)
        for page in reader.pages:
            text_parts.append(page.extract_text())

    return "\n".join(text_parts)


def _extract_text_from_html(raw_html: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", raw_html)
    text = re.sub(r"(?is)<br\\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p\\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def _parse_isin(text: str) -> str | None:
    """
    Extract ISIN from PDF text.

    ISIN format: 2 letters + 10 alphanumeric (e.g., IE00BK5BQT80)
    """
    # Common patterns in factsheets:
    # "ISIN: IE00BK5BQT80"
    # "ISIN IE00BK5BQT80"
    # "IE00BK5BQT80 (ISIN)"

    patterns = [
        r"ISIN[:\s]+([A-Z]{2}[A-Z0-9]{10})",
        r"([A-Z]{2}[A-Z0-9]{10})\s+\(ISIN\)",
        r"\bISIN\b[:\s]*([A-Z]{2}[A-Z0-9]{10})",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            isin = match.group(1).upper()
            # Validate ISIN format
            if re.match(r"^[A-Z]{2}[A-Z0-9]{10}$", isin):
                return isin

    return None


def _parse_cusip(text: str) -> str | None:
    """
    Extract CUSIP from PDF text.

    CUSIP format: 9 alphanumeric characters (e.g., 78468R703)
    """
    # Common patterns in factsheets:
    # "CUSIP: 78468R703"
    # "CUSIP 78468R703"
    # "78468R703 (CUSIP)"

    patterns = [
        r"CUSIP[:\s]+([A-Z0-9]{9})(?:\s|$)",  # Must be followed by whitespace or end
        r"([A-Z0-9]{9})\s+\(CUSIP\)",
        r"\bCUSIP\b[:\s]*([A-Z0-9]{9})(?:\s|$)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            cusip = match.group(1).upper()
            # Validate CUSIP format (9 alphanumeric) and must contain at least one digit
            if re.match(r"^[A-Z0-9]{9}$", cusip) and re.search(r"\d", cusip):
                return cusip

    return None


def _parse_domicile(text: str) -> str | None:
    """
    Extract fund domicile from PDF text.

    Common domiciles: Ireland, Luxembourg, Singapore, UK, US
    """
    patterns = [
        r"Domicile[:\s]+([A-Za-z\s]+)",
        r"Domiciled in[:\s]+([A-Za-z\s]+)",
        r"Fund Domicile[:\s]+([A-Za-z\s]+)",
        r"Legal Structure.*UCITS.*domiciled in ([A-Za-z]+)",
        r"Country of domicile[:\s]+([A-Za-z\s]+)",
        r"Jurisdiction[:\s]+([A-Za-z\s]+)",
    ]

    canonical = {
        "IRELAND": "Ireland",
        "IRISH": "Ireland",
        "IE": "Ireland",
        "LUXEMBOURG": "Luxembourg",
        "LU": "Luxembourg",
        "SINGAPORE": "Singapore",
        "SG": "Singapore",
        "UNITED KINGDOM": "United Kingdom",
        "GREAT BRITAIN": "United Kingdom",
        "UK": "United Kingdom",
        "GB": "United Kingdom",
        "UNITED STATES": "United States",
        "USA": "United States",
        "US": "United States",
    }
    garbage_markers = (
        "offer or solicitation",
        "against the law",
        "where such an offer",
        "when such an offer",
        "allow you to access",
        "in which it is being accessed",
        "being accessed",
        "not intended for",
        "this document",
    )

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            domicile = " ".join(match.group(1).strip().split())
            lowered = domicile.lower()
            if any(marker in lowered for marker in garbage_markers):
                continue
            if len(domicile) > 32:
                continue
            normalized = re.sub(r"[^A-Za-z ]+", "", domicile).strip().upper()
            if normalized in canonical:
                return canonical[normalized]
            if domicile.title() in set(canonical.values()):
                return domicile.title()

    return None


def _parse_accumulating_status(text: str) -> str | None:
    """
    Determine if fund is Accumulating or Distributing.

    Returns: "accumulating", "distributing", or None
    """
    # Priority 1: Look for explicit "Share Class:" field
    share_class_match = re.search(r"Share Class[:\s]+(Accumulating|Distributing)", text, re.IGNORECASE)
    if share_class_match:
        return share_class_match.group(1).lower()

    # Priority 2: Check share class codes in fund name
    if re.search(r"\(Acc\)", text) or re.search(r"Accumulation", text):
        return "accumulating"

    if re.search(r"\(Dist\)", text) or re.search(r"Distribution", text):
        return "distributing"

    # Priority 3: Check income treatment
    if re.search(r"income.*reinvested", text, re.IGNORECASE):
        return "accumulating"

    if re.search(r"income.*distributed", text, re.IGNORECASE):
        return "distributing"

    if re.search(r"income treatment[:\s]+capitali[sz]ing", text, re.IGNORECASE):
        return "accumulating"

    if re.search(r"income treatment[:\s]+distributing", text, re.IGNORECASE):
        return "distributing"

    # Priority 4: Look for mentions (only if unambiguous)
    if re.search(r"\bAccumulating\b", text, re.IGNORECASE):
        if not re.search(r"\bDistributing\b", text, re.IGNORECASE):
            return "accumulating"

    if re.search(r"\bDistributing\b", text, re.IGNORECASE):
        if not re.search(r"\bAccumulating\b", text, re.IGNORECASE):
            return "distributing"

    return None


def _parse_ter(text: str) -> float | None:
    """
    Extract TER (Total Expense Ratio) or Ongoing Charges.

    TER typically shown as: "0.22%", "TER: 0.22%", "Ongoing charges: 0.22%"
    """
    patterns = [
        r"(?:TER|Total Expense Ratio)[:\s]+([0-9]+\.?[0-9]*)\s*%",
        r"(?:Ongoing Charges?|OCF)[:\s]+([0-9]+\.?[0-9]*)\s*%",
        r"Management Fee[:\s]+([0-9]+\.?[0-9]*)\s*%",
        r"Annual charge[:\s]+([0-9]+\.?[0-9]*)\s*%",
        r"All-?in fee[^\n]*?([0-9]+\.?[0-9]*)\s*%\s*p\.?a\.?",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            ter_pct = float(match.group(1))
            # Sanity check: TER typically 0.01% to 2.0%
            if 0.001 <= ter_pct <= 5.0:
                return ter_pct / 100.0  # Convert to decimal (0.22% → 0.0022)

    return None


def _parse_fund_name(text: str) -> str | None:
    patterns = [
        r"Fund name[:\s]+([^\n]+)",
        r"^([A-Z][^\n]+UCITS ETF[^\n]*)$",
        r"^([A-Z][^\n]+ETF[^\n]*)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            return str(match.group(1)).strip()
    first_line = next((line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("#")), "")
    return first_line or None


def _parse_listing_exchange(text: str) -> str | None:
    patterns = [
        r"(?:^|\n)\s*Stock Exchange\s*:\s*([^\n]+)",
        r"(?:^|\n)\s*Primary Listing\s*:\s*([^\n]+)",
        r"(?:^|\n)\s*Primary Exchange Name\s*:\s*([^\n]+)",
        r"(?:^|\n)\s*Exchange\s*:\s*([^\n]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            value = str(match.group(1)).strip()
            normalized = value.lower()
            if (
                "listing date" in normalized
                or "trading hours" in normalized
                or "trading currency" in normalized
                or normalized.startswith("name ")
                or normalized.startswith("bloomberg")
                or normalized.startswith("rates ")
            ):
                return None
            if "nyse arca" in normalized:
                return "NYSE Arca"
            if "nasdaq" in normalized:
                return "NASDAQ"
            if "new york stock exchange" in normalized or normalized == "nyse":
                return "NYSE"
            if "london stock" in normalized or normalized == "lse":
                return "LSE"
            if "xetra" in normalized:
                return "XETRA"
            if "hong kong" in normalized or "hkex" in normalized:
                return "HKEX"
            if "singapore exchange" in normalized or normalized == "sgx":
                return "SGX"
            if len(value) > 40 or value.startswith("("):
                return None
            return value
    return None


def _parse_primary_currency(text: str) -> str | None:
    patterns = [
        r"(?:Share Class Currency|Fund Currency)[:\s]+([A-Z]{3})\b",
        r"(?:Currency|Base currency|Trading currency)[:\s]+([A-Z]{3})\b",
        r"\(([A-Z]{3})\)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            code = str(match.group(1)).upper()
            if code in _KNOWN_CURRENCY_CODES:
                return code
    return None


def _normalize_date_capture(raw: str) -> str | None:
    text = str(raw or "").strip().replace(",", " ")
    text = re.sub(r"\s+", " ", text)
    month_word = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", text)
    if month_word:
        day = int(month_word.group(1))
        month = _MONTH_MAP.get(month_word.group(2).lower())
        year = int(month_word.group(3))
        if month:
            return f"{year}-{month}-{day:02d}"
    month_word_hyphen = re.match(r"^(\d{1,2})[-/]([A-Za-z]+)[-/](\d{4})$", text)
    if month_word_hyphen:
        day = int(month_word_hyphen.group(1))
        month = _MONTH_MAP.get(month_word_hyphen.group(2).lower())
        year = int(month_word_hyphen.group(3))
        if month:
            return f"{year}-{month}-{day:02d}"
    slash_or_dot = re.match(r"^(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})$", text)
    if slash_or_dot:
        first = int(slash_or_dot.group(1))
        second = int(slash_or_dot.group(2))
        year = int(slash_or_dot.group(3))
        if year < 100:
            year += 2000
        if first <= 12 and second > 12:
            month = first
            day = second
        else:
            day = first
            month = second
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year}-{month:02d}-{day:02d}"
    month_first = re.match(r"^([A-Za-z]+)\s+(\d{1,2})\s+(\d{4})$", text)
    if month_first:
        month = _MONTH_MAP.get(month_first.group(1).lower())
        day = int(month_first.group(2))
        year = int(month_first.group(3))
        if month:
            return f"{year}-{month}-{day:02d}"
    return None


def _parse_launch_date(text: str) -> str | None:
    patterns = [
        r"(?:Fund Inception|Inception)[:\s]+([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{2,4})",
        r"(?:Fund Inception|Inception)[:\s]+([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})",
        r"(?:Fund Inception|Inception)[:\s]+([0-9]{1,2}[-/][A-Za-z]+[-/][0-9]{4})",
        r"(?:Fund Inception|Inception)[:\s]+([A-Za-z]+\s+[0-9]{1,2},?\s+[0-9]{4})",
        r"(?:Fund launch date|Share class launch date)[:\s]+([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{2,4})",
        r"(?:Fund launch date|Share class launch date)[:\s]+([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})",
        r"(?:Fund launch date|Share class launch date)[:\s]+([0-9]{1,2}[-/][A-Za-z]+[-/][0-9]{4})",
        r"(?:Fund launch date|Share class launch date)[:\s]+([A-Za-z]+\s+[0-9]{1,2},?\s+[0-9]{4})",
        r"(?:Fund launch date|Launch date|Inception date|Date launched|Date listed|Listing date)[:\s]+([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{2,4})",
        r"(?:Fund launch date|Launch date|Inception date|Date launched|Date listed|Listing date)[:\s]+([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})",
        r"(?:Fund launch date|Launch date|Inception date|Date launched|Date listed|Listing date)[:\s]+([0-9]{1,2}[-/][A-Za-z]+[-/][0-9]{4})",
        r"(?:Fund launch date|Launch date|Inception date|Date launched|Date listed|Listing date)[:\s]+([A-Za-z]+\s+[0-9]{1,2},?\s+[0-9]{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        normalized = _normalize_date_capture(match.group(1))
        if normalized:
            return normalized
    return None


def _parse_wrapper_type(text: str) -> str | None:
    text_upper = text.upper()
    if "UCITS ETF" in text_upper:
        return "UCITS ETF"
    if "UCITS V ICAV" in text_upper:
        return "UCITS V ICAV"
    if "ETC" in text_upper:
        return "ETC"
    if "ETF" in text_upper:
        return "ETF"
    return None


def _parse_ucits_status(text: str) -> bool | None:
    upper = text.upper()
    if "UCITS" in upper:
        return True
    if "ETF" in upper or "ETC" in upper:
        return None
    return False


def _parse_replication_method(text: str) -> str | None:
    patterns = [
        r"Portfolio Methodology[:\s]+([^\n]+)",
        r"Replication(?: Method)?[:\s]+([^\n]+)",
        r"Physical\s*-\s*full replication",
        r"Physical\s*\(optimized sampling\)",
        r"Swap-based",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return str(match.group(0) if match.lastindex is None else match.group(1)).strip()
    return None


def _parse_issuer_name(text: str) -> str | None:
    garbage_markers = (
        "prospective investors",
        "principal and interest payments",
        "rating may be used to determine index classification",
        "supranational financial institutions",
        "where such an offer",
        "when such an offer",
        "against the law",
        "offer or solicitation",
        "allow you to access",
        "in which it is being accessed",
        "request_access_via_add_ons",
    )
    trusted_markers = (
        "asset management",
        "management",
        "investments",
        "investment",
        "capital",
        "vanguard",
        "blackrock",
        "ishares",
        "spdr",
        "state street",
        "alpha architect",
        "cambria",
        "natixis",
        "invesco",
        "xtrackers",
        "dws",
        "hsbc",
        "funds",
        "advisors",
        "advisor",
        "etf",
    )
    patterns = [
        r"(?:Managed by|Investment manager|Manager|Fund manager|Management company|Issued by)[:\s]+([^\n]+)",
        r"(?:Issuer|Fund provider|Promoter)[:\s]+([^\n]+)",
        r"(?:Issuer name|Company)[:\s]+([^\n]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            value = str(match.group(1)).strip()
            lowered = value.lower()
            if any(marker in lowered for marker in garbage_markers):
                continue
            if value.strip().upper() in {"US", "THE US", "USD"}:
                continue
            if len(value.split()) > 12 and not any(marker in lowered for marker in trusted_markers):
                continue
            if value and len(value) <= 120:
                return value
    return None


def _parse_benchmark_name(text: str) -> str | None:
    canonical_index_patterns = [
        r"\b((?:MSCI|FTSE|S&P|Bloomberg|ICE BofA|Markit iBoxx|BarclayHedge|Barclays Hedge|Mount Lucas)[^\n]{0,120}?Index)\b",
        r"\b(Bloomberg Commodity Index)\b",
        r"\b(DBi Managed Futures Strategy(?: ETF)? Index)\b",
        r"\b(S&P 500)\b",
    ]
    for pattern in canonical_index_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return str(match.group(1)).strip(" \t\n\r:;,.")

    patterns = [
        r"Index name[:\s]+([^\n]+)",
        r"Benchmark:\s+([^\n]+)",
        r"Benchmark(?: Information)?\s*\n\s*Benchmark:\s+([^\n]+)",
        r"track the performance\s+of the\s+([^.]+)",
        r"seeks to track the performance\s+of the\s+([^.]+)",
        r"Reference index[:\s]+([^\n]+)",
        r"Underlying index[:\s]+([^\n]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            value = str(match.group(1)).strip(" \t\n\r:;,.")
            lowered = value.lower()
            if "by investing in" in lowered or "representative sample" in lowered:
                continue
            return value
    return None


def _parse_number_of_holdings(text: str) -> int | None:
    patterns = [
        r"Number of (?:stocks|holdings)[:\s]+([0-9,]+)",
        r"Holdings[:\s]+([0-9,]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return int(str(match.group(1)).replace(",", ""))
            except ValueError:
                return None
    return None


def _parse_top_10_concentration(text: str) -> float | None:
    patterns = [
        r"Top 10 (?:holdings|positions|issuers)[:\s]+([0-9]+\.?[0-9]*)%",
        r"Top ten concentration[:\s]+([0-9]+\.?[0-9]*)%",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
    return None


def _parse_sector_concentration_proxy(text: str) -> float | None:
    patterns = [
        r"Information Technology[:\s]+([0-9]+\.?[0-9]*)%",
        r"Technology[:\s]+([0-9]+\.?[0-9]*)%",
        r"Largest sector[:\s]+([0-9]+\.?[0-9]*)%",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
    return None


def _parse_country_mix_summary(text: str) -> dict[str, Any]:
    patterns = {
        "us_weight": [
            r"United States[:\s]+([0-9]+\.?[0-9]*)%",
            r"U\.S\.[:\s]+([0-9]+\.?[0-9]*)%",
        ],
        "em_weight": [
            r"Emerging Markets[:\s]+([0-9]+\.?[0-9]*)%",
            r"Emerging market weight[:\s]+([0-9]+\.?[0-9]*)%",
        ],
    }
    out: dict[str, Any] = {
        "us_weight": None,
        "em_weight": None,
        "developed_market_exposure_summary": None,
        "emerging_market_exposure_summary": None,
    }
    for key, variants in patterns.items():
        for pattern in variants:
            match = re.search(pattern, text, re.IGNORECASE)
            if not match:
                continue
            try:
                out[key] = float(match.group(1))
                break
            except ValueError:
                continue
    em_weight = out["em_weight"]
    if em_weight is not None:
        if em_weight <= 0:
            out["developed_market_exposure_summary"] = "Developed markets only"
            out["emerging_market_exposure_summary"] = "No emerging-market allocation detected"
        else:
            out["developed_market_exposure_summary"] = f"Developed plus emerging ({em_weight:.1f}% EM)"
            out["emerging_market_exposure_summary"] = f"Emerging market weight {em_weight:.1f}%"
    return out


def _parse_cash_support_fields(text: str) -> dict[str, Any]:
    support: dict[str, Any] = {
        "weighted_average_maturity": None,
        "portfolio_quality_summary": None,
        "redemption_settlement_notes": None,
        "underlying_currency_exposure": None,
        "share_class_proven": None,
    }
    wam_patterns = [
        r"Weighted average maturity[:\s]+([0-9]+\.?[0-9]*)\s*(?:days|day)",
        r"Weighted average maturity[:\s]+([0-9]+\.?[0-9]*)\s*(?:years|year)",
        r"Average maturity[:\s]+([0-9]+\.?[0-9]*)\s*(?:days|day)",
    ]
    for pattern in wam_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        try:
            value = float(match.group(1))
        except ValueError:
            continue
        lower = match.group(0).lower()
        support["weighted_average_maturity"] = round(value / 365.0, 4) if "day" in lower else value
        break

    quality_patterns = [
        r"(?:Portfolio quality|Credit quality|Average credit quality)[:\s]+([^\n]+)",
        r"(?:Government backing|Sovereign backing)[:\s]+([^\n]+)",
    ]
    for pattern in quality_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            support["portfolio_quality_summary"] = str(match.group(1)).strip()
            break
    if support["portfolio_quality_summary"] is None:
        if re.search(r"\bTreasury bills?\b", text, re.IGNORECASE):
            support["portfolio_quality_summary"] = "US Treasury bill portfolio"
        elif re.search(r"\bgovernment\b", text, re.IGNORECASE):
            support["portfolio_quality_summary"] = "Government-backed cash or bills exposure"

    settlement_match = re.search(
        r"(?:Settlement|Redemption|Dealing)[:\s]+([^\n]+)",
        text,
        re.IGNORECASE,
    )
    if settlement_match:
        support["redemption_settlement_notes"] = str(settlement_match.group(1)).strip()

    currency_match = re.search(
        r"(?:Base currency|Trading currency|Underlying currency exposure)[:\s]+([A-Z]{3}(?:\s+[A-Za-z-]+)?)",
        text,
        re.IGNORECASE,
    )
    if currency_match:
        support["underlying_currency_exposure"] = str(currency_match.group(1)).strip()
    elif re.search(r"\bUSD\b", text):
        support["underlying_currency_exposure"] = "USD"

    support["share_class_proven"] = bool(_parse_accumulating_status(text))
    return support


def _parse_tracking_differences(text: str) -> dict[str, float | None]:
    patterns = {
        "tracking_difference_1y": r"1\s*year tracking difference[:\s]+([+-]?[0-9]+\.?[0-9]*)%",
        "tracking_difference_3y": r"3\s*year tracking difference[:\s]+([+-]?[0-9]+\.?[0-9]*)%",
        "tracking_difference_5y": r"5\s*year tracking difference[:\s]+([+-]?[0-9]+\.?[0-9]*)%",
        "tracking_difference_since_inception": r"Since inception tracking difference[:\s]+([+-]?[0-9]+\.?[0-9]*)%",
        "tracking_difference_1y_alt": r"Tracking Difference\s*\(1\s*Year\)[:\s]+([+-]?[0-9]+\.?[0-9]*)%",
        "tracking_difference_3y_alt": r"Tracking Difference\s*\(3\s*Year\)[:\s]+([+-]?[0-9]+\.?[0-9]*)%",
        "tracking_difference_5y_alt": r"Tracking Difference\s*\(5\s*Year\)[:\s]+([+-]?[0-9]+\.?[0-9]*)%",
    }
    out: dict[str, float | None] = {}
    for key, pattern in patterns.items():
        if key.endswith("_alt"):
            continue
        match = re.search(pattern, text, re.IGNORECASE)
        if not match and f"{key}_alt" in patterns:
            match = re.search(patterns[f"{key}_alt"], text, re.IGNORECASE)
        if match:
            try:
                out[key] = float(match.group(1))
            except ValueError:
                out[key] = None
        else:
            out[key] = None
    derived = _derive_blackrock_tracking_differences(text)
    for key, value in derived.items():
        if out.get(key) is None and value is not None:
            out[key] = value
    derived = _derive_table_tracking_differences(text)
    for key, value in derived.items():
        if out.get(key) is None and value is not None:
            out[key] = value
    return out


def _parse_aum_usd(text: str) -> float | None:
    structured_patterns = [
        r"Net Assets of Fund\s*\(([MB])\)\s*:\s*([0-9][0-9,]*\.?[0-9]*)\s*USD",
        r"Net Assets of Share Class\s*\(([MB])\)\s*:\s*([0-9][0-9,]*\.?[0-9]*)\s*USD",
        r"Fund size\s*\(([MB])\)\s*:\s*\$?\s*([0-9][0-9,]*\.?[0-9]*)",
        r"AUM\s*\(([MB])\)\s*:\s*\$?\s*([0-9][0-9,]*\.?[0-9]*)",
        r"Total assets\s*\((million|billion)\)\s*\$?\s*([0-9][0-9,]*\.?[0-9]*)",
        r"Share class assets\s*\((million|billion)\)\s*\$?\s*([0-9][0-9,]*\.?[0-9]*)",
    ]
    unit_multipliers = {"B": 1_000_000_000.0, "M": 1_000_000.0, "BILLION": 1_000_000_000.0, "MILLION": 1_000_000.0}
    for pattern in structured_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        try:
            value = float(str(match.group(2)).replace(",", ""))
        except ValueError:
            continue
        unit = str(match.group(1) or "").upper()
        multiplier = unit_multipliers.get(unit)
        if multiplier is not None:
            return value * multiplier

    patterns = [
        r"Net assets[:\s]+\$([0-9]+\.?[0-9]*)\s*(billion|million)\s*USD",
        r"Fund size[:\s]+\$([0-9]+\.?[0-9]*)\s*(billion|million)\s*USD",
        r"Net assets[:\s]+USD\s*([0-9]+\.?[0-9]*)\s*(billion|million)",
        r"Fund size[:\s]+USD\s*([0-9][0-9,]*\.?[0-9]*)",
        r"Total net assets[:\s]+\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
        r"Total net assets[:\s]+USD\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
        r"Fund assets[:\s]+USD\s*([0-9][0-9,]*\.?[0-9]*)\s*(billion|million)",
        r"Assets of fund[:\s]+\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
        r"Total Fund Assets[:\s]+USD\s*([0-9]+\.?[0-9]*)\s*(billion|million)",
        r"Assets under management[:\s]+USD\s*([0-9]+\.?[0-9]*)\s*(billion|million)",
        r"Assets under management[:\s]+\$?\s*USD?\s*([0-9][0-9,]*\.?[0-9]*)",
        r"Assets under management[:\s]+\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
        r"Net assets of fund[:\s]+\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
        r"Net assets of share class[:\s]+\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
        r"AUM[:\s]+USD\s*([0-9][0-9,]*\.?[0-9]*)",
        r"Fund size[:\s]+\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
        r"Net assets[:\s]+\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
        r"Assets under management[:\s]+\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
        r"AUM[:\s]+\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
        r"\bUSD\s*\$?\s*([0-9][0-9,]*\.?[0-9]*)\s*(bn|mn|billion|million|b|m)\b",
    ]
    multipliers = {
        "billion": 1_000_000_000.0,
        "million": 1_000_000.0,
        "bn": 1_000_000_000.0,
        "mn": 1_000_000.0,
        "b": 1_000_000_000.0,
        "m": 1_000_000.0,
    }
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                value = float(str(match.group(1)).replace(",", ""))
            except ValueError:
                continue
            unit = str(match.group(2)).lower() if match.lastindex and match.lastindex >= 2 else ""
            return value * multipliers.get(unit, 1.0)
    return None


def _parse_bond_metrics(text: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "effective_duration": None,
        "average_maturity": None,
        "yield_proxy": None,
        "credit_quality_mix": None,
        "government_vs_corporate_split": None,
        "interest_rate_sensitivity_proxy": None,
    }
    float_patterns = {
        "effective_duration": [
            r"Effective duration[:\s]+([0-9]+\.?[0-9]*)",
            r"Duration[:\s]+([0-9]+\.?[0-9]*)\s+years",
        ],
        "average_maturity": [
            r"Average maturity[:\s]+([0-9]+\.?[0-9]*)",
            r"Weighted average maturity[:\s]+([0-9]+\.?[0-9]*)",
        ],
        "yield_proxy": [
            r"Yield to maturity[:\s]+([0-9]+\.?[0-9]*)%",
            r"SEC yield[:\s]+([0-9]+\.?[0-9]*)%",
        ],
    }
    for key, patterns in float_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    value = float(match.group(1))
                    metrics[key] = value if key != "yield_proxy" else value / 100.0
                    break
                except ValueError:
                    continue
    mix_match = re.search(r"(?:Credit quality|Average credit quality)[:\s]+([^\n]+)", text, re.IGNORECASE)
    if mix_match:
        metrics["credit_quality_mix"] = str(mix_match.group(1)).strip()
    split_match = re.search(r"(?:Government|Sovereign).{0,40}(?:Corporate).{0,40}", text, re.IGNORECASE)
    if split_match:
        metrics["government_vs_corporate_split"] = split_match.group(0).strip()
    if metrics["effective_duration"] is not None:
        metrics["interest_rate_sensitivity_proxy"] = metrics["effective_duration"]
    return metrics


def _normalize_structured_fields(text: str) -> dict[str, Any]:
    tracking = _parse_tracking_differences(text)
    bond_metrics = _parse_bond_metrics(text)
    country_mix = _parse_country_mix_summary(text)
    cash_support = _parse_cash_support_fields(text)
    return {
        "fund_name": _parse_fund_name(text),
        "issuer": _parse_issuer_name(text),
        "primary_listing_exchange": _parse_listing_exchange(text),
        "primary_trading_currency": _parse_primary_currency(text),
        "wrapper_or_vehicle_type": _parse_wrapper_type(text),
        "ucits_status": _parse_ucits_status(text),
        "replication_method": _parse_replication_method(text),
        "benchmark_name": _parse_benchmark_name(text),
        "launch_date": _parse_launch_date(text),
        "inception_date": _parse_launch_date(text),
        "holdings_count": _parse_number_of_holdings(text),
        "top_10_concentration": _parse_top_10_concentration(text),
        "sector_concentration_proxy": _parse_sector_concentration_proxy(text),
        "aum_usd": _parse_aum_usd(text),
        **country_mix,
        **tracking,
        **bond_metrics,
        **cash_support,
    }


def _parse_factsheet_date(text: str) -> str | None:
    """
    Extract factsheet as-of date.

    Common formats:
    - "As at 31 December 2025"
    - "31 Dec 2025"
    - "Data as of: 31/12/2025"
    - "Fund facts as at 31.12.2025"
    """
    patterns = [
        r"(?:This factsheet is as of)\s+([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})",
        r"(?:Fund Facts as at|Factsheet as of|Data as of|Fund facts as at|As at|as of|as at)[:\s]+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})",
        r"(?:Fund Facts as at|Factsheet as of|Data as of|Fund facts as at|As at|as of|as at)[:\s]+([A-Za-z]+)\s+(\d{1,2})[,\s]+(\d{4})",
        r"(?:Fund Facts as at|Factsheet as of|Data as of|Fund facts as at|As at|as of|as at)[:\s]+(\d{1,2})[/-](\d{1,2})[/-](\d{4})",
        r"(?:Fund Facts as at|Factsheet as of|Data as of|Fund facts as at|As at|as of|as at)[:\s]+(\d{1,2})\.(\d{1,2})\.(\d{4})",
    ]

    for pattern in patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            groups = match.groups()

            try:
                # Pattern 1: "31 December 2025"
                if len(groups) == 3 and groups[1].isalpha():
                    day = int(groups[0])
                    month_str = groups[1].lower()
                    year = int(groups[2])
                    month = _MONTH_MAP.get(month_str)
                    if month:
                        return f"{year}-{month}-{day:02d}"

                # Pattern 1b: "February 27, 2026"
                elif len(groups) == 3 and groups[0].isalpha():
                    month = _MONTH_MAP.get(groups[0].lower())
                    day = int(groups[1])
                    year = int(groups[2])
                    if month:
                        return f"{year}-{month}-{day:02d}"

                # Pattern 2: "31/12/2025" or "31-12-2025"
                elif len(groups) == 3 and groups[0].isdigit():
                    day = int(groups[0])
                    month = int(groups[1])
                    year = int(groups[2])
                    if 1 <= month <= 12 and 1 <= day <= 31:
                        return f"{year}-{month:02d}-{day:02d}"

            except (ValueError, KeyError):
                continue

    return None


def _derive_blackrock_tracking_differences(text: str) -> dict[str, float | None]:
    match = re.search(
        r"CUMULATIVE\s*&\s*ANNUALISED PERFORMANCE.*?Share Class\s+([0-9+\-.\s]+?)\s+Benchmark\s+([0-9+\-.\s]+?)\s+(?:The figures shown relate|Source:\s*BlackRock)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return {}
    share_values = [float(item) for item in re.findall(r"[+-]?[0-9]+(?:\.[0-9]+)?", match.group(1))]
    benchmark_values = [float(item) for item in re.findall(r"[+-]?[0-9]+(?:\.[0-9]+)?", match.group(2))]
    if len(share_values) < 8 or len(benchmark_values) < 8:
        return {}
    return {
        "tracking_difference_1y": (share_values[4] - benchmark_values[4]) / 100.0,
        "tracking_difference_3y": (share_values[5] - benchmark_values[5]) / 100.0,
        "tracking_difference_5y": (share_values[6] - benchmark_values[6]) / 100.0,
        "tracking_difference_since_inception": (share_values[7] - benchmark_values[7]) / 100.0,
    }


def _performance_columns(header_line: str) -> list[str]:
    normalized = " ".join(str(header_line or "").split())
    columns: list[str] = []
    for match in re.finditer(r"QTD|YTD|1[-\s]*year|3[-\s]*year|5[-\s]*year|10[-\s]*year|1y|3y|5y|10y|Since fund inception|Since Inception", normalized, re.IGNORECASE):
        token = match.group(0).lower().replace(" ", "")
        mapping = {
            "1-year": "tracking_difference_1y",
            "1y": "tracking_difference_1y",
            "3-year": "tracking_difference_3y",
            "3y": "tracking_difference_3y",
            "5-year": "tracking_difference_5y",
            "5y": "tracking_difference_5y",
            "10-year": "tracking_difference_10y",
            "10y": "tracking_difference_10y",
            "sincefundinception": "tracking_difference_since_inception",
            "sinceinception": "tracking_difference_since_inception",
        }
        columns.append(mapping.get(token, token))
    return columns


def _performance_row_values(line: str) -> list[float]:
    return [float(item) for item in re.findall(r"[+-]?\d+(?:\.\d+)?", line)]


def _derive_table_tracking_differences(text: str) -> dict[str, float | None]:
    lines = [" ".join(line.split()) for line in text.splitlines()]
    for idx, line in enumerate(lines):
        if not line:
            continue
        if not (("1-year" in line or "1y" in line.lower()) and ("3-year" in line or "3y" in line.lower())):
            continue
        columns = _performance_columns(line)
        if not columns:
            continue
        nav_values: list[float] | None = None
        benchmark_values: list[float] | None = None
        for candidate_line in lines[idx + 1 : idx + 8]:
            if re.match(r"^NAV\b", candidate_line, re.IGNORECASE):
                row_values = _performance_row_values(candidate_line)
                if len(row_values) >= len(columns):
                    nav_values = row_values[: len(columns)]
            elif nav_values is None and re.match(r"^(Market Value|Market Price)\b", candidate_line, re.IGNORECASE):
                row_values = _performance_row_values(candidate_line)
                if len(row_values) >= len(columns):
                    nav_values = row_values[: len(columns)]
            elif re.match(r"^(Benchmark|Index)\b", candidate_line, re.IGNORECASE):
                row_values = _performance_row_values(candidate_line)
                if len(row_values) >= len(columns):
                    benchmark_values = row_values[: len(columns)]
        if nav_values is None or benchmark_values is None:
            continue
        derived: dict[str, float | None] = {}
        for position, column in enumerate(columns):
            if not column.startswith("tracking_difference_"):
                continue
            if position >= len(nav_values) or position >= len(benchmark_values):
                continue
            derived[column] = round((nav_values[position] - benchmark_values[position]) / 100.0, 6)
        if derived:
            return derived
    return {}


def fetch_and_parse_etf_doc(
    ticker: str,
    doc_url: str,
    doc_type: str,
    cache_dir: Path,
) -> dict[str, Any]:
    """
    Fetch PDF from issuer and extract verification proofs.

    Args:
        ticker: ETF ticker symbol
        doc_url: URL to PDF document
        doc_type: "factsheet", "kid", or "prospectus"
        cache_dir: Directory to cache downloaded PDFs

    Returns:
        dict with extracted fields and metadata
    """
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Generate cache filename
    url_hash = hashlib.sha256(doc_url.encode()).hexdigest()[:12]
    cache_file = cache_dir / f"{ticker}_{doc_type}_{url_hash}.pdf"

    retrieved_at = datetime.now(UTC)

    # Fetch PDF if not cached or older than 1 day
    if not cache_file.exists() or (datetime.now(UTC).timestamp() - cache_file.stat().st_mtime > 1 * 86400):
        try:
            with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                }
                response = client.get(doc_url, headers=headers)
                response.raise_for_status()

                cache_file.write_bytes(response.content)

        except httpx.HTTPError as e:
            return {
                "status": "failed",
                "error": str(e),
                "doc_url": doc_url,
            }

    # Parse PDF, with HTML/text fallback for issuer pages or HTML responses cached under PDF URLs
    try:
        text = _extract_text_from_pdf(cache_file)
    except Exception:
        raw_text = cache_file.read_text(encoding="utf-8", errors="ignore")
        lowered = raw_text[:2048].lower()
        if "<html" in lowered or "<!doctype" in lowered or "<body" in lowered:
            text = _extract_text_from_html(raw_text)
        elif raw_text.strip():
            text = raw_text.strip()
        else:
            return {
                "status": "failed",
                "error": "Document parsing failed and no text fallback was available",
                "doc_url": doc_url,
            }

    # Extract all fields
    isin = _parse_isin(text)
    cusip = _parse_cusip(text)
    domicile = _parse_domicile(text)
    accumulating_status = _parse_accumulating_status(text)
    ter = _parse_ter(text)
    factsheet_date = _parse_factsheet_date(text)

    # Use ISIN if available, otherwise CUSIP
    identifier = isin or cusip

    return {
        "status": "success",
        "doc_url": doc_url,
        "doc_type": doc_type,
        "retrieved_at": retrieved_at.isoformat(),
        "cache_file": str(cache_file),
        "extracted": {
            "isin": isin,
            "cusip": cusip,
            "identifier": identifier,
            "domicile": domicile,
            "accumulating_status": accumulating_status,
            "ter": ter,
            "factsheet_date": factsheet_date,
            **_normalize_structured_fields(text),
        },
        "proof_isin": isin is not None,
        "proof_domicile": domicile is not None,
        "proof_accumulating": accumulating_status is not None,
        "proof_ter": ter is not None,
        "proof_factsheet_fresh": _is_factsheet_fresh(factsheet_date, 120),
    }


def _is_factsheet_fresh(factsheet_date: str | None, max_age_days: int) -> bool:
    """Check if factsheet date is within acceptable age threshold."""
    if not factsheet_date:
        return False

    try:
        date_obj = datetime.fromisoformat(factsheet_date)
        age_days = (datetime.now(UTC) - date_obj.replace(tzinfo=UTC)).days
        return age_days <= max_age_days
    except (ValueError, TypeError):
        return False


def load_doc_registry() -> dict[str, Any]:
    """Load ETF doc registry from JSON config."""
    import json

    registry_path = Path(__file__).parent.parent / "config/etf_doc_registry.json"

    if not registry_path.exists():
        return {"candidates": []}

    with open(registry_path) as f:
        return json.load(f)


def _needs_structural_doc_followup(extracted: dict[str, Any]) -> bool:
    required = (
        "benchmark_name",
        "issuer",
        "launch_date",
        "primary_trading_currency",
        "primary_listing_exchange",
        "aum_usd",
    )
    return not all(extracted.get(field) not in {None, ""} for field in required)


def fetch_candidate_docs(ticker: str, cache_dir: Path | None = None, use_fixtures: bool = False) -> dict[str, Any]:
    """
    Fetch and parse all registered docs for an ETF candidate.

    Args:
        ticker: ETF ticker symbol
        cache_dir: Directory to cache PDFs
        use_fixtures: If True, use test fixtures instead of fetching real PDFs

    Returns verification proofs and extracted data.
    """
    if cache_dir is None:
        cache_dir = Path("outbox/etf_docs_cache") / ticker

    # Test mode: use local fixtures
    if use_fixtures:
        # fixtures are in backend/tests/fixtures, code is in backend/app/services
        fixture_path = Path(__file__).parent.parent.parent / "tests/fixtures" / f"sample_{ticker.lower()}_factsheet.txt"
        if fixture_path.exists():
            text = fixture_path.read_text()
            retrieved_at = datetime.now(UTC)

            # Extract all fields
            isin = _parse_isin(text)
            cusip = _parse_cusip(text)
            domicile = _parse_domicile(text)
            accumulating_status = _parse_accumulating_status(text)
            ter = _parse_ter(text)
            factsheet_date = _parse_factsheet_date(text)

            # Use ISIN if available, otherwise CUSIP
            identifier = isin or cusip

            extracted = {
                "isin": isin,
                "cusip": cusip,
                "identifier": identifier,
                "domicile": domicile,
                "accumulating_status": accumulating_status,
                "ter": ter,
                "factsheet_date": factsheet_date,
                **_normalize_structured_fields(text),
            }

            proofs = {
                "proof_isin": isin is not None,
                "proof_domicile": domicile is not None,
                "proof_accumulating": accumulating_status is not None,
                "proof_ter": ter is not None,
                "proof_factsheet_fresh": _is_factsheet_fresh(factsheet_date, 120),
            }

            verified = all(proofs.values())
            missing_proofs = [k.replace("proof_", "") for k, v in proofs.items() if not v]

            return {
                "ticker": ticker,
                "expected_isin": None,  # Not checked in fixture mode
                "factsheet": {
                    "status": "success",
                    "doc_url": str(fixture_path),
                    "doc_type": "factsheet",
                    "retrieved_at": retrieved_at.isoformat(),
                    "cache_file": str(fixture_path),
                    "extracted": extracted,
                    **proofs,
                },
                "proofs": proofs,
                "extracted": extracted,
                "isin_conflict": False,
                "verified": verified,
                "partially_verified": any(proofs.values()) and not verified,
                "verification_missing": missing_proofs,
            }
        else:
            missing_msg = (
                f"Fixture not found: {fixture_path}\n\n"
                f"To generate fixtures for {ticker}, run:\n"
                f"  python3 backend/scripts/build_factsheet_fixtures.py {ticker}\n\n"
                f"Or create manually with format:\n"
                f"  # Source: <URL>\n"
                f"  # Retrieved: <ISO date>\n"
                f"  ISIN: <identifier>\n"
                f"  Domicile: <country>\n"
                f"  Share Class: Accumulating|Distributing\n"
                f"  TER: <percentage>\n"
                f"  As of: <date>\n"
            )
            return {
                "status": "failed",
                "error": missing_msg,
                "ticker": ticker,
                "expected_fixture_path": str(fixture_path),
            }

    registry = load_doc_registry()

    # Find candidate in registry
    candidate_config = None
    for candidate in registry.get("candidates", []):
        if candidate["ticker"] == ticker:
            candidate_config = candidate
            break

    if not candidate_config:
        return {
            "status": "failed",
            "error": f"Ticker {ticker} not found in doc registry",
        }

    docs = candidate_config.get("docs", {})
    expected_isin = candidate_config.get("expected_isin")

    results = {
        "ticker": ticker,
        "expected_isin": expected_isin,
        "factsheet": None,
        "kid": None,
        "prospectus": None,
        "proofs": {
            "proof_isin": False,
            "proof_domicile": False,
            "proof_accumulating": False,
            "proof_ter": False,
            "proof_factsheet_fresh": False,
        },
        "extracted": {},
        "isin_conflict": False,
    }

    # Fetch factsheet
    factsheet_url = docs.get("factsheet_pdf_url") or docs.get("factsheet_html_url")
    if factsheet_url:
        results["factsheet"] = fetch_and_parse_etf_doc(
            ticker,
            factsheet_url,
            "factsheet",
            cache_dir,
        )

        if results["factsheet"]["status"] == "success":
            extracted = results["factsheet"]["extracted"]
            results["extracted"].update(extracted)

            # Update proofs
            for proof in ["proof_isin", "proof_domicile", "proof_accumulating", "proof_ter", "proof_factsheet_fresh"]:
                if results["factsheet"].get(proof):
                    results["proofs"][proof] = True

            # ISIN conflict check
            if extracted.get("isin") and expected_isin:
                if extracted["isin"] != expected_isin:
                    results["isin_conflict"] = True

    need_followup_docs = (not all(results["proofs"].values())) or _needs_structural_doc_followup(dict(results.get("extracted") or {}))

    # Fetch KID when core proofs are missing or structural extraction is still thin.
    if docs.get("kid_pdf_url") and need_followup_docs:
        results["kid"] = fetch_and_parse_etf_doc(
            ticker,
            docs["kid_pdf_url"],
            "kid",
            cache_dir,
        )

        if results["kid"]["status"] == "success":
            extracted_kid = results["kid"]["extracted"]
            # Merge proofs from KID
            for key, value in extracted_kid.items():
                if value and not results["extracted"].get(key):
                    results["extracted"][key] = value

            for proof in ["proof_isin", "proof_domicile", "proof_accumulating", "proof_ter"]:
                if results["kid"].get(proof):
                    results["proofs"][proof] = True

    need_followup_docs = (not all(results["proofs"].values())) or _needs_structural_doc_followup(dict(results.get("extracted") or {}))

    # Fetch prospectus as a last structured-document follow-up when the factsheet and KID
    # still leave benchmark, issuer, listing, currency, or launch fields unresolved.
    if docs.get("prospectus_pdf_url") and need_followup_docs:
        results["prospectus"] = fetch_and_parse_etf_doc(
            ticker,
            docs["prospectus_pdf_url"],
            "prospectus",
            cache_dir,
        )

        if results["prospectus"]["status"] == "success":
            extracted_prospectus = results["prospectus"]["extracted"]
            for key, value in extracted_prospectus.items():
                if value and not results["extracted"].get(key):
                    results["extracted"][key] = value

            for proof in ["proof_isin", "proof_domicile", "proof_accumulating", "proof_ter"]:
                if results["prospectus"].get(proof):
                    results["proofs"][proof] = True

    # Calculate overall verification status
    results["verified"] = all(results["proofs"].values()) and not results["isin_conflict"]
    results["partially_verified"] = any(results["proofs"].values()) and not results["verified"]

    missing_proofs = [k.replace("proof_", "") for k, v in results["proofs"].items() if not v]
    results["verification_missing"] = missing_proofs

    return results
