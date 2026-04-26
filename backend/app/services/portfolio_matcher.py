from __future__ import annotations

import hashlib
import re
from typing import Any


_SYMBOL_SANITIZER = re.compile(r"[^A-Z0-9.=/-]+")


def normalize_symbol(raw_symbol: str) -> str:
    value = str(raw_symbol or "").strip().upper()
    if not value:
        return ""
    return _SYMBOL_SANITIZER.sub("", value)


def infer_asset_type(row: dict[str, Any]) -> str:
    explicit = str(row.get("asset_type") or row.get("security_type") or "").strip().lower()
    if explicit:
        return explicit.replace(" ", "_")

    symbol = normalize_symbol(str(row.get("symbol") or row.get("ticker") or ""))
    name = str(row.get("name") or "").lower()
    if "cash" in name or symbol in {"CASH", "USD", "SGD", "EUR", "GBP"}:
        return "cash"
    if "fund" in name:
        return "mutual_fund"
    if "etf" in name or symbol in {"SPY", "IVV", "VTI", "CSPX", "IWDA", "VWRA", "BND", "AGGU"}:
        return "etf"
    return "equity"


def build_security_key(
    *,
    asset_type: str,
    normalized_symbol: str,
    currency: str,
    venue: str | None = None,
    isin: str | None = None,
    name: str | None = None,
) -> str:
    if isin:
        return f"isin:{str(isin).strip().upper()}"
    venue_key = str(venue or "").strip().upper()
    if normalized_symbol:
        parts = [asset_type.strip().lower(), normalized_symbol.strip().upper()]
        if venue_key:
            parts.append(venue_key)
        parts.append(str(currency or "").strip().upper())
        return "|".join(parts)
    fingerprint = hashlib.sha1(
        f"{asset_type}|{name or ''}|{currency or ''}".encode("utf-8")
    ).hexdigest()[:16]
    return f"synthetic:{fingerprint}"


def classify_mapping_status(
    *,
    target_sleeve: str | None,
    manual_override: bool,
    confidence: float,
) -> str:
    if manual_override:
        return "manual_override"
    if not target_sleeve:
        return "unmapped"
    if confidence < 0.75:
        return "low_confidence"
    return "auto_matched"
