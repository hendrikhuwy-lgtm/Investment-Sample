from __future__ import annotations

from typing import Any


_FIELD_POLICY: dict[str, dict[str, Any]] = {
    "expense_ratio": {
        "blocking": True,
        "severity": "critical",
        "stale_days": 365,
        "preferred_document_types": ["factsheet", "kid", "prospectus"],
    },
    "benchmark_key": {
        "blocking": True,
        "severity": "critical",
        "stale_days": 365,
        "preferred_document_types": [],
    },
    "benchmark_name": {
        "blocking": True,
        "severity": "critical",
        "stale_days": 365,
        "preferred_document_types": ["factsheet", "prospectus"],
    },
    "replication_method": {
        "blocking": True,
        "severity": "critical",
        "stale_days": 365,
        "preferred_document_types": ["prospectus", "factsheet"],
    },
    "primary_listing_exchange": {
        "blocking": True,
        "severity": "critical",
        "stale_days": 365,
        "preferred_document_types": ["factsheet", "prospectus"],
    },
    "primary_trading_currency": {
        "blocking": True,
        "severity": "critical",
        "stale_days": 365,
        "preferred_document_types": ["factsheet", "prospectus"],
    },
    "domicile": {
        "blocking": True,
        "severity": "critical",
        "stale_days": 365,
        "preferred_document_types": ["prospectus", "kid", "factsheet"],
    },
    "issuer": {
        "blocking": True,
        "severity": "important",
        "stale_days": 365,
        "preferred_document_types": ["prospectus", "factsheet"],
    },
    "aum": {
        "blocking": True,
        "severity": "important",
        "stale_days": 180,
        "preferred_document_types": ["factsheet"],
    },
    "liquidity_proxy": {
        "blocking": True,
        "severity": "important",
        "stale_days": 120,
        "preferred_document_types": [],
    },
    "bid_ask_spread_proxy": {
        "blocking": True,
        "severity": "critical",
        "stale_days": 90,
        "preferred_document_types": [],
    },
    "premium_discount_behavior": {
        "blocking": False,
        "severity": "important",
        "stale_days": 120,
        "preferred_document_types": ["factsheet"],
    },
    "distribution_type": {
        "blocking": False,
        "severity": "important",
        "stale_days": 365,
        "preferred_document_types": ["prospectus", "kid", "factsheet"],
    },
    "launch_date": {
        "blocking": False,
        "severity": "important",
        "stale_days": 3650,
        "preferred_document_types": ["prospectus", "factsheet"],
    },
    "tracking_difference_1y": {
        "blocking": False,
        "severity": "important",
        "stale_days": 365,
        "preferred_document_types": ["factsheet"],
    },
    "tracking_difference_3y": {
        "blocking": False,
        "severity": "important",
        "stale_days": 365,
        "preferred_document_types": ["factsheet"],
    },
    "tracking_difference_5y": {
        "blocking": False,
        "severity": "important",
        "stale_days": 365,
        "preferred_document_types": ["factsheet"],
    },
}


def field_policy(field_name: str) -> dict[str, Any]:
    return dict(_FIELD_POLICY.get(field_name, {}))


def field_stale_days(field_name: str, default: int = 365) -> int:
    return int(field_policy(field_name).get("stale_days", default))


def field_severity(field_name: str) -> str:
    return str(field_policy(field_name).get("severity") or "informational")


def field_is_blocking(field_name: str) -> bool:
    return bool(field_policy(field_name).get("blocking"))


def preferred_document_types(field_name: str) -> list[str]:
    return list(field_policy(field_name).get("preferred_document_types") or [])
