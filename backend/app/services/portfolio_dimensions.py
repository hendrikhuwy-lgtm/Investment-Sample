from __future__ import annotations

from typing import Any

import sqlite3

from app.services.classification_registry import (
    ensure_classification_tables,
    list_security_classifications,
    rebuild_security_classifications,
)
from app.services.factor_engine import build_factor_snapshot, ensure_factor_tables


def _confidence_rank(level: str) -> int:
    return {"high": 3, "medium": 2, "low": 1, "none": 0}.get(str(level or "none"), 0)


def _confidence_label(levels: list[str]) -> str:
    if not levels:
        return "none"
    if any(level == "low" for level in levels):
        return "low"
    if any(level == "medium" for level in levels):
        return "medium"
    return "high"


def build_dimension_snapshot(conn: sqlite3.Connection, *, run_id: str | None, account_id: str | None = None) -> dict[str, Any]:
    ensure_classification_tables(conn)
    ensure_factor_tables(conn)
    if not run_id:
        return {
            "run_id": None,
            "account_id": account_id,
            "region_attribution": [],
            "sector_attribution": [],
            "country_attribution": [],
            "industry_attribution": [],
            "factor_exposure": [],
            "summary": {
                "region_confidence": "none",
                "sector_confidence": "none",
                "factor_confidence": "none",
                "unknown_region_weight": 0.0,
                "unknown_sector_weight": 0.0,
                "observed_factor_count": 0,
                "inferred_factor_count": 0,
                "classified_weight": 0.0,
                "issuer_fallback_count": 0,
            },
        }

    holdings_rows = conn.execute(
        """
        SELECT security_key, normalized_symbol, security_name, market_value, sleeve, asset_type, venue, currency, identifier_isin
        FROM portfolio_holding_snapshots
        WHERE run_id = ?
          AND (? IS NULL OR account_id = ?)
        ORDER BY market_value DESC, normalized_symbol ASC
        """,
        (run_id, account_id, account_id),
    ).fetchall()
    holdings = [dict(row) for row in holdings_rows]
    total_value = sum(float(item.get("market_value") or 0.0) for item in holdings)
    if total_value <= 0:
        return {
            "run_id": run_id,
            "account_id": account_id,
            "region_attribution": [],
            "sector_attribution": [],
            "country_attribution": [],
            "industry_attribution": [],
            "factor_exposure": [],
            "summary": {
                "region_confidence": "none",
                "sector_confidence": "none",
                "factor_confidence": "none",
                "unknown_region_weight": 0.0,
                "unknown_sector_weight": 0.0,
                "observed_factor_count": 0,
                "inferred_factor_count": 0,
                "classified_weight": 0.0,
                "issuer_fallback_count": 0,
            },
        }

    rebuild_security_classifications(conn, run_id=run_id)
    classifications = list_security_classifications(conn, run_id=run_id)
    class_by_security = {str(item.get("security_key")): item for item in classifications}
    factor_rows = build_factor_snapshot(conn, run_id=run_id, holdings=holdings, classifications=classifications)

    region_values: dict[str, dict[str, Any]] = {}
    sector_values: dict[str, dict[str, Any]] = {}
    country_values: dict[str, float] = {}
    industry_values: dict[str, float] = {}
    country_labels: set[str] = set()
    industry_labels: set[str] = set()
    unknown_region_weight = 0.0
    unknown_sector_weight = 0.0
    classified_weight = 0.0
    region_conf_levels: list[str] = []
    sector_conf_levels: list[str] = []
    issuer_fallback_count = 0

    for row in holdings:
        security_key = str(row.get("security_key"))
        classification = class_by_security.get(security_key, {})
        market_value = float(row.get("market_value") or 0.0)
        weight = market_value / total_value if total_value > 0 else 0.0

        region = str(classification.get("region") or "Unknown")
        country = str(classification.get("country") or "Unknown")
        sector = str(classification.get("sector") or "Unknown")
        industry = str(classification.get("industry") or "Unknown")
        confidence = str(classification.get("confidence") or "low")
        provenance = classification.get("provenance_json") or {}
        if bool(provenance.get("issuer_fallback_used")):
            issuer_fallback_count += 1

        region_bucket = region_values.setdefault(
            region,
            {
                "label": region,
                "market_value": 0.0,
                "confidence": confidence,
                "country_labels": set(),
                "provenance_sources": set(),
            },
        )
        region_bucket["market_value"] += market_value
        region_bucket["country_labels"].add(country)
        region_bucket["provenance_sources"].update(set((provenance.get("country_region_reasons") or [])[:2]))
        if _confidence_rank(confidence) < _confidence_rank(str(region_bucket.get("confidence") or "none")):
            region_bucket["confidence"] = confidence

        sector_bucket = sector_values.setdefault(
            sector,
            {
                "label": sector,
                "market_value": 0.0,
                "confidence": confidence,
                "industry_labels": set(),
                "provenance_sources": set(),
            },
        )
        sector_bucket["market_value"] += market_value
        sector_bucket["industry_labels"].add(industry)
        sector_bucket["provenance_sources"].update(set((provenance.get("sector_industry_reasons") or [])[:2]))
        if _confidence_rank(confidence) < _confidence_rank(str(sector_bucket.get("confidence") or "none")):
            sector_bucket["confidence"] = confidence

        country_labels.add(country)
        industry_labels.add(industry)
        country_values[country] = country_values.get(country, 0.0) + market_value
        industry_values[industry] = industry_values.get(industry, 0.0) + market_value
        region_conf_levels.append(confidence)
        sector_conf_levels.append(confidence)
        if region == "Unknown":
            unknown_region_weight += weight
        else:
            classified_weight += weight
        if sector == "Unknown":
            unknown_sector_weight += weight

    region_attribution = [
        {
            "label": bucket["label"],
            "market_value": round(float(bucket["market_value"]), 2),
            "weight": round(float(bucket["market_value"]) / total_value, 6),
            "country_count": len(bucket["country_labels"]),
            "confidence": str(bucket["confidence"]),
            "provenance": sorted(bucket["provenance_sources"]),
        }
        for bucket in sorted(region_values.values(), key=lambda item: float(item["market_value"]), reverse=True)
    ]
    sector_attribution = [
        {
            "label": bucket["label"],
            "market_value": round(float(bucket["market_value"]), 2),
            "weight": round(float(bucket["market_value"]) / total_value, 6),
            "industry_count": len(bucket["industry_labels"]),
            "confidence": str(bucket["confidence"]),
            "provenance": sorted(bucket["provenance_sources"]),
        }
        for bucket in sorted(sector_values.values(), key=lambda item: float(item["market_value"]), reverse=True)
    ]
    country_attribution = [
        {
            "label": label,
            "market_value": round(value, 2),
            "weight": round(value / total_value, 6),
        }
        for label, value in sorted(country_values.items(), key=lambda item: item[1], reverse=True)
    ]
    industry_attribution = [
        {
            "label": label,
            "market_value": round(value, 2),
            "weight": round(value / total_value, 6),
        }
        for label, value in sorted(industry_values.items(), key=lambda item: item[1], reverse=True)
    ]
    factor_exposure = [
        {
            "factor": str(item.get("factor_name")),
            "score": float(item.get("exposure_value") or 0.0),
            "confidence": str(item.get("confidence") or "low"),
            "exposure_type": str(item.get("exposure_type") or "inferred"),
            "provenance": item.get("provenance_json") or {},
        }
        for item in sorted(factor_rows, key=lambda row: float(row.get("exposure_value") or 0.0), reverse=True)
        if float(item.get("exposure_value") or 0.0) > 0
    ]

    return {
        "run_id": run_id,
        "account_id": account_id,
        "region_attribution": region_attribution,
        "sector_attribution": sector_attribution,
        "country_attribution": country_attribution,
        "industry_attribution": industry_attribution,
        "factor_exposure": factor_exposure,
        "summary": {
            "region_confidence": _confidence_label(region_conf_levels),
            "sector_confidence": _confidence_label(sector_conf_levels),
            "factor_confidence": _confidence_label([str(item.get("confidence") or "low") for item in factor_rows]),
            "unknown_region_weight": round(unknown_region_weight, 6),
            "unknown_sector_weight": round(unknown_sector_weight, 6),
            "classified_weight": round(classified_weight, 6),
            "observed_factor_count": sum(1 for item in factor_rows if str(item.get("exposure_type")) == "observed"),
            "inferred_factor_count": sum(1 for item in factor_rows if str(item.get("exposure_type")) != "observed"),
            "country_count": len(country_labels - {"Unknown"}),
            "industry_count": len(industry_labels - {"Unknown"}),
            "issuer_fallback_count": issuer_fallback_count,
        },
    }
