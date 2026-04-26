from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


ISIN_COUNTRY_PREFIX = {
    "US": "United States",
    "IE": "Ireland",
    "GB": "United Kingdom",
    "SG": "Singapore",
    "LU": "Luxembourg",
    "FR": "France",
    "DE": "Germany",
    "JP": "Japan",
    "CN": "China",
    "HK": "Hong Kong",
    "AU": "Australia",
    "CA": "Canada",
}

COUNTRY_TO_REGION = {
    "United States": "North America",
    "Canada": "North America",
    "Ireland": "Europe",
    "United Kingdom": "Europe",
    "Luxembourg": "Europe",
    "France": "Europe",
    "Germany": "Europe",
    "Singapore": "Asia Pacific",
    "Japan": "Asia Pacific",
    "China": "Asia / Emerging",
    "Hong Kong": "Asia / Emerging",
    "Australia": "Asia Pacific",
}

VENUE_TO_COUNTRY = {
    "NASDAQ": "United States",
    "NYSE": "United States",
    "ARCA": "United States",
    "XNYS": "United States",
    "XNAS": "United States",
    "XLON": "United Kingdom",
    "LSE": "United Kingdom",
    "SGX": "Singapore",
    "XSES": "Singapore",
    "TSE": "Japan",
    "XTKS": "Japan",
    "HKEX": "Hong Kong",
}

SECTOR_RULES = {
    "Technology": {
        "industry": "Technology Platforms",
        "keywords": ["apple", "microsoft", "alphabet", "google", "meta", "nvidia", "software", "semiconductor", "cloud", "technology"],
    },
    "Consumer Discretionary": {
        "industry": "Consumer Platforms",
        "keywords": ["tesla", "amazon", "consumer", "retail", "auto", "ecommerce"],
    },
    "Financials": {
        "industry": "Banks and Insurance",
        "keywords": ["bank", "financial", "insurance", "credit"],
    },
    "Health Care": {
        "industry": "Pharma and Biotech",
        "keywords": ["health", "pharma", "biotech", "medical"],
    },
    "Industrials": {
        "industry": "Capital Goods",
        "keywords": ["industrial", "transport", "defense", "rail", "logistics"],
    },
    "Energy": {
        "industry": "Oil and Gas",
        "keywords": ["energy", "oil", "gas", "pipeline"],
    },
    "Materials": {
        "industry": "Materials and Mining",
        "keywords": ["materials", "mining", "gold", "metals"],
    },
    "Real Estate": {
        "industry": "REITs and Property",
        "keywords": ["reit", "real estate", "property"],
    },
}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_classification_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS security_classifications (
          classification_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          security_key TEXT NOT NULL,
          normalized_symbol TEXT NOT NULL,
          issuer_key TEXT,
          issuer_name TEXT,
          country TEXT,
          region TEXT,
          sector TEXT,
          industry TEXT,
          classification_source TEXT NOT NULL,
          confidence TEXT NOT NULL,
          provenance_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_security_classifications_run_security
        ON security_classifications (run_id, security_key)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS issuer_classifications (
          issuer_classification_id TEXT PRIMARY KEY,
          issuer_key TEXT NOT NULL,
          issuer_name TEXT,
          country TEXT,
          region TEXT,
          sector TEXT,
          industry TEXT,
          classification_source TEXT NOT NULL,
          confidence TEXT NOT NULL,
          provenance_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_issuer_classifications_key
        ON issuer_classifications (issuer_key)
        """
    )
    conn.commit()


def _issuer_key(row: dict[str, Any]) -> tuple[str, str]:
    issuer_name = str(row.get("security_name") or row.get("normalized_symbol") or "Unknown").strip()
    isin = str(row.get("identifier_isin") or "").strip().upper()
    if isin:
        return isin, issuer_name
    normalized_symbol = str(row.get("normalized_symbol") or "").strip().upper()
    if normalized_symbol:
        return normalized_symbol, issuer_name
    security_key = str(row.get("security_key") or "").strip()
    return security_key or issuer_name.lower().replace(" ", "_"), issuer_name


def _infer_country_region(row: dict[str, Any]) -> tuple[str, str, str, list[str]]:
    reasons: list[str] = []
    asset_type = str(row.get("asset_type") or "").lower()
    if asset_type == "cash":
        reasons.append("asset_type:cash")
        return "Base Currency", "Base Currency", "high", reasons

    isin = str(row.get("identifier_isin") or "").strip().upper()
    if len(isin) >= 2 and isin[:2] in ISIN_COUNTRY_PREFIX:
        country = ISIN_COUNTRY_PREFIX[isin[:2]]
        region = COUNTRY_TO_REGION.get(country, "Unknown")
        reasons.append(f"isin_prefix:{isin[:2]}")
        return country, region, "high", reasons

    venue = str(row.get("venue") or "").upper()
    if venue in VENUE_TO_COUNTRY:
        country = VENUE_TO_COUNTRY[venue]
        region = COUNTRY_TO_REGION.get(country, "Unknown")
        reasons.append(f"venue:{venue}")
        return country, region, "medium", reasons

    name = str(row.get("security_name") or "").lower()
    symbol = str(row.get("normalized_symbol") or "").upper()
    keyword_rules = {
        "China": ("China", "Asia / Emerging"),
        "Emerging": ("Unknown", "Asia / Emerging"),
        "Japan": ("Japan", "Asia Pacific"),
        "Singapore": ("Singapore", "Asia Pacific"),
        "Europe": ("Unknown", "Europe"),
        "US": ("United States", "North America"),
        "Treasury": ("United States", "North America"),
    }
    for token, (country, region) in keyword_rules.items():
        if token.lower() in name or token.upper() in symbol:
            reasons.append(f"name_keyword:{token.lower()}")
            return country, region, "low", reasons

    reasons.append("fallback:unknown")
    return "Unknown", "Unknown", "low", reasons


def _infer_sector_industry(row: dict[str, Any]) -> tuple[str, str, str, list[str]]:
    reasons: list[str] = []
    sleeve = str(row.get("sleeve") or "").lower()
    asset_type = str(row.get("asset_type") or "").lower()
    if asset_type == "cash" or sleeve == "cash":
        reasons.append("sleeve_or_asset:cash")
        return "Cash", "Cash Management", "high", reasons
    if sleeve == "ig_bond":
        reasons.append("sleeve:ig_bond")
        return "Fixed Income", "Investment Grade Credit", "medium", reasons
    if sleeve == "real_asset":
        reasons.append("sleeve:real_asset")
        return "Real Assets", "Real Assets", "medium", reasons
    if sleeve == "convex":
        reasons.append("sleeve:convex")
        return "Risk Management", "Convex Hedging", "medium", reasons
    name = str(row.get("security_name") or "").lower()
    for sector, rule in SECTOR_RULES.items():
        if any(keyword in name for keyword in list(rule["keywords"])):
            reasons.append(f"name_keyword:{sector.lower()}")
            return sector, str(rule["industry"]), "medium", reasons
    reasons.append("fallback:unknown")
    return "Unknown", "Unknown", "low", reasons


def rebuild_security_classifications(conn: sqlite3.Connection, *, run_id: str | None) -> dict[str, Any]:
    ensure_classification_tables(conn)
    if not run_id:
        return {"run_id": None, "items": [], "summary": {"classified_count": 0, "unknown_region_count": 0, "unknown_sector_count": 0}}

    rows = conn.execute(
        """
        SELECT run_id, security_key, normalized_symbol, security_name, asset_type, sleeve, venue, identifier_isin, currency
        FROM portfolio_holding_snapshots
        WHERE run_id = ?
        ORDER BY market_value DESC, normalized_symbol ASC
        """,
        (run_id,),
    ).fetchall()

    conn.execute("DELETE FROM security_classifications WHERE run_id = ?", (run_id,))
    now = _now_iso()
    items: list[dict[str, Any]] = []
    issuer_rows: dict[str, dict[str, Any]] = {}
    unknown_region_count = 0
    unknown_sector_count = 0
    issuer_fallback_count = 0

    for raw in rows:
        row = dict(raw)
        issuer_key, issuer_name = _issuer_key(row)
        country, region, region_confidence, region_reasons = _infer_country_region(row)
        sector, industry, sector_confidence, sector_reasons = _infer_sector_industry(row)
        existing_issuer = conn.execute(
            """
            SELECT country, region, sector, industry, classification_source, confidence
            FROM issuer_classifications
            WHERE issuer_key = ?
            LIMIT 1
            """,
            (issuer_key,),
        ).fetchone()
        classification_source = "heuristic_registry_v1"
        if existing_issuer is not None and (region == "Unknown" or sector == "Unknown"):
            if region == "Unknown" and str(existing_issuer["region"] or "Unknown") != "Unknown":
                country = str(existing_issuer["country"] or country)
                region = str(existing_issuer["region"] or region)
                region_confidence = str(existing_issuer["confidence"] or region_confidence)
                region_reasons.append("issuer_fallback")
                issuer_fallback_count += 1
            if sector == "Unknown" and str(existing_issuer["sector"] or "Unknown") != "Unknown":
                sector = str(existing_issuer["sector"] or sector)
                industry = str(existing_issuer["industry"] or industry)
                sector_confidence = str(existing_issuer["confidence"] or sector_confidence)
                sector_reasons.append("issuer_fallback")
                issuer_fallback_count += 1
            classification_source = "issuer_fallback_v1"
        confidence = (
            "high"
            if region_confidence == "high" and sector_confidence == "high"
            else "medium"
            if region_confidence == "medium" or sector_confidence == "medium"
            else "low"
        )
        provenance = {
            "country_region_reasons": region_reasons,
            "sector_industry_reasons": sector_reasons,
            "classification_scope": "security",
            "issuer_fallback_used": classification_source == "issuer_fallback_v1",
        }
        if region == "Unknown":
            unknown_region_count += 1
        if sector == "Unknown":
            unknown_sector_count += 1
        item = {
            "classification_id": f"class_{uuid.uuid4().hex[:12]}",
            "run_id": run_id,
            "security_key": str(row.get("security_key")),
            "normalized_symbol": str(row.get("normalized_symbol")),
            "issuer_key": issuer_key,
            "issuer_name": issuer_name,
            "country": country,
            "region": region,
            "sector": sector,
            "industry": industry,
            "classification_source": classification_source,
            "confidence": confidence,
            "provenance_json": provenance,
            "created_at": now,
            "updated_at": now,
        }
        conn.execute(
            """
            INSERT INTO security_classifications (
              classification_id, run_id, security_key, normalized_symbol, issuer_key, issuer_name,
              country, region, sector, industry, classification_source, confidence,
              provenance_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["classification_id"],
                item["run_id"],
                item["security_key"],
                item["normalized_symbol"],
                item["issuer_key"],
                item["issuer_name"],
                item["country"],
                item["region"],
                item["sector"],
                item["industry"],
                item["classification_source"],
                item["confidence"],
                json.dumps(item["provenance_json"]),
                item["created_at"],
                item["updated_at"],
            ),
        )
        items.append(item)
        issuer_rows[issuer_key] = {
            "issuer_name": issuer_name,
            "country": country,
            "region": region,
            "sector": sector,
            "industry": industry,
            "classification_source": classification_source,
            "confidence": confidence,
            "provenance_json": {
                "derived_from_security_key": str(row.get("security_key")),
                "issuer_fallback_used": classification_source == "issuer_fallback_v1",
            },
        }

    for issuer_key, issuer_payload in issuer_rows.items():
        conn.execute(
            """
            INSERT INTO issuer_classifications (
              issuer_classification_id, issuer_key, issuer_name, country, region, sector, industry,
              classification_source, confidence, provenance_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(issuer_key) DO UPDATE SET
              issuer_name = excluded.issuer_name,
              country = excluded.country,
              region = excluded.region,
              sector = excluded.sector,
              industry = excluded.industry,
              classification_source = excluded.classification_source,
              confidence = excluded.confidence,
              provenance_json = excluded.provenance_json,
              updated_at = excluded.updated_at
            """,
            (
                f"issuer_class_{uuid.uuid4().hex[:12]}",
                issuer_key,
                issuer_payload["issuer_name"],
                issuer_payload["country"],
                issuer_payload["region"],
                issuer_payload["sector"],
                issuer_payload["industry"],
                issuer_payload["classification_source"],
                issuer_payload["confidence"],
                json.dumps(issuer_payload["provenance_json"]),
                now,
                now,
            ),
        )

    conn.commit()
    return {
        "run_id": run_id,
        "items": items,
        "summary": {
            "classified_count": len(items),
            "unknown_region_count": unknown_region_count,
            "unknown_sector_count": unknown_sector_count,
            "issuer_fallback_count": issuer_fallback_count,
        },
    }


def list_security_classifications(conn: sqlite3.Connection, *, run_id: str | None) -> list[dict[str, Any]]:
    ensure_classification_tables(conn)
    if not run_id:
        return []
    rows = conn.execute(
        """
        SELECT classification_id, run_id, security_key, normalized_symbol, issuer_key, issuer_name,
               country, region, sector, industry, classification_source, confidence, provenance_json,
               created_at, updated_at
        FROM security_classifications
        WHERE run_id = ?
        ORDER BY normalized_symbol ASC
        """,
        (run_id,),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["provenance_json"] = json.loads(str(item.get("provenance_json") or "{}"))
        items.append(item)
    return items
