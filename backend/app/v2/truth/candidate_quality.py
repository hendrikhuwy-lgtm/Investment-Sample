from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.services.blueprint_benchmark_registry import (
    DEFAULT_BENCHMARK_ASSIGNMENTS,
    DEFAULT_SLEEVE_ASSIGNMENTS,
    canonical_benchmark_full_name,
    resolve_benchmark_assignment,
)
from app.services.blueprint_candidate_truth import (
    _field_applicable,
    compute_candidate_completeness,
    get_candidate_field_current,
    list_required_fields,
    resolve_candidate_field_truth,
)
from app.services.etf_doc_parser import load_doc_registry
from app.v2.truth.implementation_field_policy import (
    field_is_blocking,
    field_severity,
    field_stale_days,
    preferred_document_types,
)
from app.v2.blueprint_market.market_identity import candidate_id_for_symbol
from app.v2.blueprint_market.series_store import load_price_series
from app.v2.truth.market_calendar import coerce_datetime


_IMPLEMENTATION_BLOCKING_FIELDS = {
    field_name
    for field_name in {
        "expense_ratio",
        "benchmark_key",
        "benchmark_name",
        "replication_method",
        "primary_listing_exchange",
        "primary_trading_currency",
        "domicile",
        "liquidity_proxy",
        "bid_ask_spread_proxy",
        "issuer",
        "aum",
    }
    if field_is_blocking(field_name)
}
_CONFLICT_CRITICAL_FIELDS = {
    "expense_ratio",
    "benchmark_key",
    "benchmark_name",
    "replication_method",
    "bid_ask_spread_proxy",
    "primary_trading_currency",
    "domicile",
}
_BENCHMARK_FIELDS = {"benchmark_key", "benchmark_name"}
_EXECUTION_REVIEW_FIELDS = {"liquidity_proxy", "bid_ask_spread_proxy", "aum"}
_AUTHORITY_SENSITIVE_FIELDS = {
    "expense_ratio",
    "benchmark_name",
    "replication_method",
    "primary_listing_exchange",
    "primary_trading_currency",
    "domicile",
    "issuer",
    "aum",
}
_DERIVED_LINEAGE_FIELDS = {"benchmark_key"}
_DERIVATION_QUALITY_FIELDS = {"liquidity_proxy", "bid_ask_spread_proxy", "tracking_difference_1y"}
_HARD_STALE_BLOCK_FIELDS = {
    "expense_ratio",
    "benchmark_key",
    "benchmark_name",
    "replication_method",
    "primary_listing_exchange",
    "primary_trading_currency",
    "domicile",
}
_STRUCTURALLY_STABLE_IMPLEMENTATION_FIELDS = {
    "distribution_type",
    "domicile",
    "issuer",
    "launch_date",
    "primary_listing_exchange",
    "primary_trading_currency",
    "replication_method",
}
_FIELD_LABELS = {
    "expense_ratio": "Expense ratio",
    "benchmark_key": "Benchmark lineage",
    "benchmark_name": "Benchmark name",
    "replication_method": "Replication method",
    "primary_listing_exchange": "Primary exchange",
    "primary_trading_currency": "Trading currency",
    "liquidity_proxy": "Trading cost proxy",
    "bid_ask_spread_proxy": "Bid-ask spread proxy",
    "premium_discount_behavior": "Premium/discount behaviour",
    "aum": "Assets under management",
    "domicile": "Fund domicile",
    "distribution_type": "Distribution policy",
    "issuer": "Issuer",
    "launch_date": "Launch date",
    "tracking_difference_1y": "Tracking difference",
}

_NULLISH_RECONCILIATION_VALUES = {"", "null", "none", "n/a", "na", "unknown", "unavailable"}
_DOMICILE_ALIASES = {
    "IE": "IE",
    "IRELAND": "IE",
    "IRISH": "IE",
    "US": "US",
    "USA": "US",
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
    "SG": "SG",
    "SINGAPORE": "SG",
    "UK": "GB",
    "GB": "GB",
    "UNITED KINGDOM": "GB",
    "GREAT BRITAIN": "GB",
    "LU": "LU",
    "LUXEMBOURG": "LU",
}
_EXCHANGE_ALIASES = {
    "LSE": "LSE",
    "XLON": "LSE",
    "LONDON STOCK EXCHANGE": "LSE",
    "SGX": "SGX",
    "XSES": "SGX",
    "SINGAPORE EXCHANGE": "SGX",
    "NYSEARCA": "NYSEARCA",
    "NYSE ARCA": "NYSEARCA",
    "NYSE ARCA EQUITIES": "NYSEARCA",
    "NASDAQ": "NASDAQ",
    "NASDAQGS": "NASDAQ",
    "NASDAQ NMS - GLOBAL MARKET": "NASDAQ",
    "OTC": "OTC",
    "OTC MARKETS": "OTC",
    "CBOE": "CBOE",
}
_GARBAGE_RECONCILIATION_STRINGS = {
    "allow you to access the information on this website",
    "and",
}
_GARBAGE_RECONCILIATION_SUBSTRINGS = (
    "allow you to access",
    "in which it is being accessed",
    "where such an offer",
    "when such an offer",
    "against the law",
    "offer or solicitation",
    "orsolicitation",
    "request_access_via_add_ons",
)
_MARKET_HISTORY_LIQUIDITY_VALUES = {"direct_history_backed", "provider_history_backed", "proxy_history_backed"}
_RECOMMENDATION_CRITICAL_FIELDS = tuple(
    dict.fromkeys(
        [
            "expense_ratio",
            "benchmark_key",
            "benchmark_name",
            "replication_method",
            "primary_listing_exchange",
            "primary_trading_currency",
            "liquidity_proxy",
            "bid_ask_spread_proxy",
            "aum",
            "domicile",
            "distribution_type",
            "issuer",
            "launch_date",
            "tracking_difference_1y",
        ]
    )
)
_USABLE_DOCUMENT_STATUSES = {"success", "verified", "usable", "cached_valid", "available", "cached", "ready", "ok"}


_IDENTITY_STATE_RANK = {
    "verified": 0,
    "thin": 1,
    "review": 2,
    "conflict": 3,
    "missing": 4,
}
_VISIBLE_STATE_BY_INVESTOR_STATE = {
    "actionable": "eligible",
    "shortlisted": "review",
    "blocked": "blocked",
    "research_only": "research_only",
}
_VISIBLE_ALLOWED_ACTION = {
    "eligible": "approve",
    "review": "review",
    "watch": "monitor",
    "research_only": "monitor",
    "blocked": "none",
}
_BACKEND_SCORE_COMPONENT_KEYS: tuple[str, ...] = (
    "implementation",
    "source_integrity",
    "benchmark_fidelity",
    "sleeve_fit",
    "long_horizon_quality",
    "market_path_support",
    "instrument_quality",
    "portfolio_fit",
)
_CORE_SCORE_COMPONENT_KEYS: tuple[str, ...] = tuple(
    key for key in _BACKEND_SCORE_COMPONENT_KEYS if key != "market_path_support"
)
_SCORE_COMPONENT_LABELS = {
    "implementation": "Implementation",
    "source_integrity": "Source integrity",
    "benchmark_fidelity": "Benchmark fidelity",
    "sleeve_fit": "Sleeve fit",
    "long_horizon_quality": "Long-horizon quality",
    "market_path_support": "Market-path support",
    "instrument_quality": "Instrument quality",
    "portfolio_fit": "Portfolio fit",
}


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_tracking_difference_value(value: Any) -> float | None:
    number = _safe_float(value)
    if number is None:
        return None
    if abs(number) >= 0.01:
        return number / 100.0
    return number


def _json_value(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def _normalize_symbol(candidate: dict[str, Any]) -> str:
    return str(candidate.get("symbol") or "").strip().upper()


def _meaningful(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _resolved_value(resolved_truth: dict[str, dict[str, Any]], field_name: str) -> Any:
    row = dict(resolved_truth.get(field_name) or {})
    return row.get("resolved_value")


def _pick_first(candidate: dict[str, Any], resolved_truth: dict[str, dict[str, Any]], *paths: str) -> Any:
    extra = dict(candidate.get("extra") or {})
    for path in paths:
        if path.startswith("extra."):
            value = extra.get(path.split(".", 1)[1])
        else:
            value = _resolved_value(resolved_truth, path)
            if not _meaningful(value):
                value = candidate.get(path)
            if not _meaningful(value):
                value = extra.get(path)
        if _meaningful(value):
            return value
    return None


def _format_pct(value: Any) -> str | None:
    number = _safe_float(value)
    if number is None:
        return None
    return f"{number:.2%}"


def _format_bps(value: Any) -> str | None:
    number = _safe_float(value)
    if number is None:
        return None
    return f"{number:.2f} bps"


def _format_aum(value: Any) -> str | None:
    number = _safe_float(value)
    if number is None:
        return None
    if number >= 1_000_000_000:
        return f"${number / 1_000_000_000:.1f}B"
    if number >= 1_000_000:
        return f"${number / 1_000_000:.0f}M"
    return f"${number:,.0f}"


def _field_label(field_name: str) -> str:
    return _FIELD_LABELS.get(field_name, field_name.replace("_", " ").title())


def _sanitize_surface_url(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    lowered = raw.lower()
    if (
        raw.startswith("/")
        or raw.startswith("file://")
        or raw.startswith("~")
        or "/Users/" in raw
        or "tests/fixtures" in lowered
        or "expected_fixture_path" in lowered
    ):
        return None
    return raw


def _benchmark_full_name(
    candidate: dict[str, Any],
    resolved_truth: dict[str, dict[str, Any]],
    *,
    benchmark_assignment: dict[str, Any] | None = None,
) -> str | None:
    explicit_name = _pick_first(candidate, resolved_truth, "benchmark_name")
    explicit_key = _pick_first(candidate, resolved_truth, "benchmark_key")
    assignment = dict(benchmark_assignment or {})
    if _meaningful(explicit_name) and "proxy" not in str(explicit_name).strip().lower():
        return str(explicit_name).strip()
    return canonical_benchmark_full_name(
        str(explicit_key or assignment.get("benchmark_key") or "").strip() or None,
        str(explicit_name or assignment.get("benchmark_label") or "").strip() or None,
    )


def _normalize_isin(value: Any) -> str | None:
    raw = str(value or "").strip().upper()
    if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", raw):
        return raw
    return None


def _normalize_cusip(value: Any) -> str | None:
    raw = str(value or "").strip().upper()
    if re.fullmatch(r"[A-Z0-9]{9}", raw):
        return raw
    return None


def _normalize_identity_name(value: Any) -> str | None:
    raw = " ".join(str(value or "").strip().split())
    if not raw:
        return None
    lowered = raw.lower()
    upper = raw.upper()
    if upper.startswith(("ISIN:", "CUSIP:", "SEDOL:")):
        return None
    if _normalize_isin(raw) or _normalize_cusip(raw):
        return None
    if lowered.startswith("www.") or "reuters.com" in lowered:
        return None
    if "secondary market cannot" in lowered or "units / shares" in lowered:
        return None
    if "request_access_via_add_ons" in lowered:
        return None
    if len(raw) < 5:
        return None
    return raw


_RECONCILIATION_GARBAGE_VALUES = {
    "AND",
    "IN WHICH IT IS BEING ACCESSED",
    "WITH NO OPERATING HISTORY AS A RESULT PROSPECTIVE INVESTORS",
}

_DOMICILE_EQUIVALENTS = {
    "IE": "IRELAND",
    "IRELAND": "IRELAND",
    "IRL": "IRELAND",
    "US": "UNITED STATES",
    "USA": "UNITED STATES",
    "UNITED STATES": "UNITED STATES",
    "UNITED STATES OF AMERICA": "UNITED STATES",
    "SG": "SINGAPORE",
    "SINGAPORE": "SINGAPORE",
    "GB": "UNITED KINGDOM",
    "UK": "UNITED KINGDOM",
    "UNITED KINGDOM": "UNITED KINGDOM",
    "LU": "LUXEMBOURG",
    "LUXEMBOURG": "LUXEMBOURG",
}


def _normalize_reconciliation_value(field_name: str, value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    if field_name == "expense_ratio":
        numeric = _safe_float(value)
        if numeric is not None:
            return f"{numeric:.8f}".rstrip("0").rstrip(".")
    normalized = " ".join(str(value or "").strip().upper().split())
    if not normalized or normalized in _RECONCILIATION_GARBAGE_VALUES:
        return ""
    if field_name == "domicile":
        return _DOMICILE_EQUIVALENTS.get(normalized, normalized)
    if field_name == "replication_method":
        if "SWAP" in normalized or "SYNTHETIC" in normalized:
            return "SYNTHETIC"
        if "PHYSICAL" in normalized:
            return "PHYSICAL"
        if "OPTIMIZED SAMPLING" in normalized:
            return "PHYSICAL"
        if "FULL REPLICATION" in normalized:
            return "PHYSICAL"
        if "PHYSICALLY BACKED" in normalized:
            return "PHYSICAL"
    return normalized


def _tone_for_state(state: str) -> str:
    normalized = str(state or "").strip()
    if normalized in {"verified", "resolved", "strong", "actionable"}:
        return "good"
    if normalized in {"review", "mixed", "shortlisted"}:
        return "warn"
    if normalized in {"conflict", "blocked", "weak", "missing"}:
        return "bad"
    return "neutral"


def _clamp_score(value: Any, *, minimum: int = 0, maximum: int = 100) -> int:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = float(minimum)
    return max(minimum, min(maximum, int(round(numeric))))


def _score_band(score: Any) -> str:
    value = _clamp_score(score)
    if value >= 85:
        return "strong"
    if value >= 75:
        return "good"
    if value >= 65:
        return "review"
    if value >= 50:
        return "weak"
    return "blocked"


def _tone_for_band(band: str) -> str:
    if band in {"strong", "good"}:
        return "good"
    if band == "review":
        return "warn"
    if band in {"weak", "blocked"}:
        return "bad"
    return "neutral"


def _legacy_instrument_quality_label(score: Any) -> str:
    value = _clamp_score(score)
    if value >= 80:
        return "High"
    if value >= 62:
        return "Moderate"
    return "Low"


def _legacy_portfolio_fit_label(score: Any) -> str:
    value = _clamp_score(score)
    if value >= 82:
        return "Highest"
    if value >= 65:
        return "Good"
    return "Weak today"


def _weighted_average(parts: list[tuple[float, float]]) -> float:
    total_weight = 0.0
    total = 0.0
    for value, weight in parts:
        if weight <= 0:
            continue
        total += float(value) * float(weight)
        total_weight += float(weight)
    if total_weight <= 0:
        return 0.0
    return total / total_weight


def _score_from_states(value: str, mapping: dict[str, int], default: int = 50) -> int:
    normalized = str(value or "").strip().lower()
    return int(mapping.get(normalized, default))


def _age_days(value: Any) -> float | None:
    observed = coerce_datetime(value)
    if observed is None:
        return None
    return max(0.0, (datetime.now(UTC) - observed).total_seconds() / 86400.0)


def _authority_score_for_row(field_name: str, row: dict[str, Any]) -> int:
    authority_class = str(row.get("authority_class") or "missing").strip()
    base_map = {
        "issuer_primary": 95,
        "issuer_secondary": 90,
        "verified_current_truth": 84,
        "provider_or_market_summary": 62,
        "registry_seed": 52,
        "missing": 0,
    }
    score = int(base_map.get(authority_class, 78))
    if field_name in _DERIVATION_QUALITY_FIELDS or field_name == "tracking_difference_1y":
        if authority_class == "verified_current_truth":
            score = 76
    return score


def _freshness_score_for_row(field_name: str, row: dict[str, Any]) -> int:
    freshness_state = str(row.get("freshness_state") or "").strip().lower()
    if freshness_state == "missing":
        return 0
    if freshness_state == "proxy":
        return 68
    if freshness_state == "stale":
        return 35
    age = _age_days(row.get("observed_at"))
    if field_name in {"liquidity_proxy", "bid_ask_spread_proxy", "tracking_difference_1y"}:
        if age is None:
            return 72
        if age <= 5:
            return 100
        if age <= 14:
            return 86
        if age <= 45:
            return 64
        return 38
    if field_name in {"aum", "expense_ratio", "launch_date"}:
        if age is None:
            return 78
        if age <= 45:
            return 100
        if age <= 120:
            return 86
        if age <= 240:
            return 68
        return 46
    if age is None:
        return 88
    if age <= 180:
        return 100
    if age <= 540:
        return 88
    if age <= 1080:
        return 72
    return 52


def _agreement_score_for_field(field_name: str, reconciliation_item: dict[str, Any]) -> int:
    status = str(reconciliation_item.get("status") or "").strip().lower()
    if status == "hard_conflict":
        return 15
    if status == "critical_missing":
        return 35
    if status == "soft_drift":
        return 75
    if status == "stale":
        return 65
    if status == "weak_authority":
        return 80
    if status == "execution_review_required":
        return 60
    if field_name in _DERIVATION_QUALITY_FIELDS and status == "verified":
        return 90
    return 100


def _provenance_score_for_row(row: dict[str, Any]) -> int:
    source_url = str(row.get("source_url") or "").strip()
    observed_at = row.get("observed_at")
    source_name = str(row.get("source_name") or "").strip()
    document_refs = list(row.get("document_support_refs") or [])
    if (source_url or document_refs) and observed_at:
        return 100
    if source_name and observed_at:
        return 75
    if source_name:
        return 50
    return 25


def _reconciliation_index(reconciliation_report: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("field_name") or ""): dict(item)
        for item in list(reconciliation_report or [])
        if str(item.get("field_name") or "").strip()
    }


def _field_quality(
    *,
    field_name: str,
    authority_rows: dict[str, dict[str, Any]],
    reconciliation_index: dict[str, dict[str, Any]],
) -> int:
    row = dict(authority_rows.get(field_name) or {})
    if not row:
        return 0
    authority_score = _authority_score_for_row(field_name, row)
    freshness_score = _freshness_score_for_row(field_name, row)
    agreement_score = _agreement_score_for_field(field_name, reconciliation_index.get(field_name, {}))
    provenance_score = _provenance_score_for_row(row)
    return _clamp_score(
        0.45 * authority_score
        + 0.25 * freshness_score
        + 0.20 * agreement_score
        + 0.10 * provenance_score
    )


def _component_field_lists(
    *,
    fields: list[str],
    authority_rows: dict[str, dict[str, Any]],
    reconciliation_index: dict[str, dict[str, Any]],
) -> tuple[list[str], list[str], list[str], list[str]]:
    missing_fields: list[str] = []
    weak_fields: list[str] = []
    stale_fields: list[str] = []
    conflict_fields: list[str] = []
    for field_name in fields:
        row = dict(authority_rows.get(field_name) or {})
        freshness_state = str(row.get("freshness_state") or "").strip().lower()
        authority_class = str(row.get("authority_class") or "").strip().lower()
        if freshness_state == "missing" or authority_class == "missing":
            missing_fields.append(field_name)
        if authority_class in {"provider_or_market_summary", "registry_seed"}:
            weak_fields.append(field_name)
        if freshness_state == "stale":
            stale_fields.append(field_name)
        if str(dict(reconciliation_index.get(field_name) or {}).get("status") or "").strip().lower() in {"hard_conflict", "soft_drift"}:
            conflict_fields.append(field_name)
    return (
        sorted(dict.fromkeys(missing_fields)),
        sorted(dict.fromkeys(weak_fields)),
        sorted(dict.fromkeys(stale_fields)),
        sorted(dict.fromkeys(conflict_fields)),
    )


def _component_confidence(
    *,
    field_scores: list[int],
    missing_fields: list[str],
    weak_fields: list[str],
    stale_fields: list[str],
    conflict_fields: list[str],
    caps_applied: list[str],
) -> int:
    base = _weighted_average([(float(score), 1.0) for score in field_scores]) if field_scores else 60.0
    confidence = (
        float(base)
        - 8.0 * len(missing_fields)
        - 5.0 * len(weak_fields)
        - 7.0 * len(stale_fields)
        - 10.0 * len(conflict_fields)
        - 4.0 * len(caps_applied)
    )
    return _clamp_score(confidence, minimum=18, maximum=98)


def _make_component(
    *,
    key: str,
    score: float,
    reasons: list[str],
    caps_applied: list[str],
    field_drivers: list[str],
    missing_fields: list[str],
    weak_fields: list[str],
    stale_fields: list[str],
    conflict_fields: list[str],
    confidence: int,
) -> dict[str, Any]:
    final_score = _clamp_score(score)
    band = _score_band(final_score)
    label = _SCORE_COMPONENT_LABELS.get(key, key.replace("_", " ").title())
    summary = str(reasons[0] if reasons else f"{label} remains {band}.").strip()
    if summary and not summary.endswith("."):
        summary += "."
    return {
        "component_id": key,
        "key": key,
        "label": label,
        "score": final_score,
        "band": band,
        "confidence": int(confidence),
        "tone": _tone_for_band(band),
        "summary": summary,
        "reasons": reasons[:4],
        "caps_applied": caps_applied[:4],
        "field_drivers": sorted(dict.fromkeys(field_drivers)),
        "missing_fields": missing_fields[:8],
        "weak_fields": weak_fields[:8],
        "stale_fields": stale_fields[:8],
        "conflict_fields": conflict_fields[:8],
    }


def _score_from_threshold(value: float | None, *, cuts: list[tuple[float, int]], reverse: bool = False, default: int = 50) -> int:
    if value is None:
        return default
    numeric = float(value)
    if reverse:
        for threshold, score in sorted(cuts, key=lambda item: item[0]):
            if numeric <= threshold:
                return int(score)
        return int(cuts[-1][1])
    for threshold, score in sorted(cuts, key=lambda item: item[0], reverse=True):
        if numeric >= threshold:
            return int(score)
    return int(cuts[-1][1])


def _expense_ratio_quality(sleeve_key: str | None, expense_ratio: float | None) -> int:
    relaxed = str(sleeve_key or "").strip() in {"alternatives", "convex", "real_assets"}
    if relaxed:
        return _score_from_threshold(
            expense_ratio,
            cuts=[(0.0025, 95), (0.0050, 84), (0.0085, 72), (0.0125, 58), (999.0, 42)],
            reverse=True,
            default=55,
        )
    if str(sleeve_key or "").strip() in {"cash_bills", "ig_bonds"}:
        return _score_from_threshold(
            expense_ratio,
            cuts=[(0.0010, 95), (0.0018, 86), (0.0028, 74), (0.0040, 60), (999.0, 42)],
            reverse=True,
            default=50,
        )
    return _score_from_threshold(
        expense_ratio,
        cuts=[(0.0009, 96), (0.0018, 88), (0.0030, 76), (0.0050, 60), (999.0, 42)],
        reverse=True,
        default=50,
    )


def _aum_quality(aum_usd: float | None) -> int:
    return _score_from_threshold(
        aum_usd,
        cuts=[(5_000_000_000, 96), (1_000_000_000, 88), (250_000_000, 76), (75_000_000, 60), (0.0, 42)],
        default=45,
    )


def _tracking_quality(tracking_difference: float | None, *, default: int = 58) -> int:
    if tracking_difference is None:
        return default
    return _score_from_threshold(
        abs(float(tracking_difference)),
        cuts=[(0.0010, 96), (0.0025, 88), (0.0050, 76), (0.0100, 60), (999.0, 40)],
        reverse=True,
        default=default,
    )


def _launch_date_quality(value: Any) -> int:
    observed = coerce_datetime(value)
    if observed is None:
        return 52
    years = max(0.0, (datetime.now(UTC) - observed).total_seconds() / (86400.0 * 365.25))
    return _score_from_threshold(
        years,
        cuts=[(10.0, 94), (5.0, 84), (3.0, 74), (1.0, 62), (0.0, 48)],
        default=52,
    )


def _issuer_quality_score(issuer: str | None, issuer_field_quality: int) -> int:
    raw = str(issuer or "").strip().lower()
    if not raw:
        return max(36, issuer_field_quality)
    if any(token in raw for token in {"blackrock", "ishares", "vanguard", "state street", "spdr", "invesco", "amundi"}):
        return max(issuer_field_quality, 90)
    return max(issuer_field_quality, 72)


def _structure_quality(replication_method: str | None, name: str | None) -> int:
    text = " ".join(part for part in [str(replication_method or ""), str(name or "")] if part).lower()
    if not text.strip():
        return 58
    if any(token in text for token in {"swap", "synthetic", "lever", "inverse", "option", "tail risk"}):
        return 46
    if "sampling" in text:
        return 72
    return 88


def _constraint_fit_score(*, domicile: str | None, currency: str | None, distribution: str | None) -> int:
    score = 74
    domicile_norm = str(domicile or "").strip().upper()
    currency_norm = str(currency or "").strip().upper()
    distribution_norm = str(distribution or "").strip().lower()
    if domicile_norm in {"IE", "SG"}:
        score += 14
    elif domicile_norm == "US":
        score -= 6
    if currency_norm in {"USD", "SGD", "EUR"}:
        score += 4
    if "acc" in distribution_norm or "accum" in distribution_norm:
        score += 4
    elif distribution_norm:
        score -= 2
    return _clamp_score(score, minimum=35, maximum=96)


def _candidate_text_blob(*parts: Any) -> str:
    return " ".join(str(part or "").strip().lower() for part in parts if str(part or "").strip())


def _sleeve_alignment_score(
    *,
    sleeve_key: str | None,
    benchmark_key: str | None,
    benchmark_name: str | None,
    exposure_summary: str | None,
    role_in_portfolio: str | None,
    candidate_name: str | None,
) -> int:
    sleeve = str(sleeve_key or "").strip()
    benchmark = _candidate_text_blob(benchmark_key, benchmark_name)
    text = _candidate_text_blob(benchmark_key, benchmark_name, exposure_summary, role_in_portfolio, candidate_name)
    if sleeve == "global_equity_core":
        if any(token in benchmark for token in {"all_world", "acwi", "world", "sp500"}) or "global equity" in text:
            return 88 if any(token in benchmark for token in {"all_world", "acwi", "world"}) else 74
        return 50
    if sleeve == "developed_ex_us_optional":
        if "ex us" in text or "developed" in text or "msci world" in benchmark:
            return 84
        return 48
    if sleeve == "emerging_markets":
        return 88 if "emerging" in text or "ftse_em" in benchmark or "msci_em" in benchmark else 42
    if sleeve == "china_satellite":
        return 90 if "china" in text else 40
    if sleeve == "ig_bonds":
        return 88 if any(token in text for token in {"bond", "aggregate", "treasury", "fixed income"}) else 46
    if sleeve == "cash_bills":
        return 92 if any(token in text for token in {"bill", "cash", "treasury", "ultra short", "money market"}) else 42
    if sleeve == "real_assets":
        return 90 if any(token in text for token in {"gold", "commodity", "reit", "real asset"}) else 46
    if sleeve == "alternatives":
        return 88 if any(token in text for token in {"managed futures", "alternative", "trend", "macro", "tail", "commod"}) else 54
    if sleeve == "convex":
        return 92 if any(token in text for token in {"tail", "put", "convex", "hedge", "risk"}) else 38
    return 68


def _row_has_usable_value(item: dict[str, Any]) -> bool:
    if not item:
        return False
    if not _meaningful(item.get("resolved_value")):
        return False
    missingness_reason = str(item.get("missingness_reason") or "").strip()
    return not missingness_reason or missingness_reason == "populated"


def _benchmark_assignment_truth_item(
    candidate: dict[str, Any],
    field_name: str,
    benchmark_assignment: dict[str, Any] | None,
) -> dict[str, Any] | None:
    assignment = dict(benchmark_assignment or {})
    value: Any = None
    if field_name == "benchmark_key":
        value = assignment.get("benchmark_key")
    elif field_name == "benchmark_name":
        value = canonical_benchmark_full_name(
            str(assignment.get("benchmark_key") or "").strip() or None,
            str(assignment.get("benchmark_label") or "").strip() or None,
        ) or assignment.get("benchmark_label") or assignment.get("benchmark_key")
    if not _meaningful(value):
        return None
    return {
        "resolved_value": value,
        "source_name": "benchmark_registry",
        "source_url": None,
        "source_type": "benchmark_mapping",
        "observed_at": candidate.get("updated_at") or candidate.get("observed_at"),
        "value_type": "current",
        "missingness_reason": "populated",
        "provenance_level": "verified_mapping",
    }


def _candidate_seed_truth_item(candidate: dict[str, Any], field_name: str) -> dict[str, Any] | None:
    extra = dict(candidate.get("extra") or {})
    value: Any = None
    if field_name == "fund_name":
        value = _normalize_identity_name(candidate.get("fund_name") or candidate.get("name"))
    elif field_name == "isin":
        value = _normalize_isin(extra.get("isin") or candidate.get("isin"))
    elif field_name == "expense_ratio":
        value = candidate.get("expense_ratio") or candidate.get("ter")
    elif field_name == "benchmark_key":
        value = candidate.get("benchmark_key") or candidate.get("benchmark")
    elif field_name == "benchmark_name":
        value = candidate.get("benchmark_name") or candidate.get("benchmark_label") or candidate.get("benchmark_key") or candidate.get("benchmark")
    elif field_name == "replication_method":
        value = candidate.get("replication_method")
    elif field_name == "primary_listing_exchange":
        value = candidate.get("primary_listing_exchange") or extra.get("primary_listing_exchange")
    elif field_name == "primary_trading_currency":
        value = candidate.get("primary_trading_currency") or candidate.get("base_currency") or extra.get("primary_trading_currency")
    elif field_name == "liquidity_proxy":
        return None
    elif field_name == "bid_ask_spread_proxy":
        return None
    elif field_name == "aum":
        value = extra.get("aum_usd") or candidate.get("aum_usd") or candidate.get("aum")
    elif field_name == "domicile":
        value = candidate.get("domicile")
    elif field_name == "distribution_type":
        value = candidate.get("distribution_type") or candidate.get("distribution_policy") or candidate.get("share_class") or candidate.get("accumulation")
    elif field_name == "issuer":
        value = candidate.get("issuer")
    elif field_name == "launch_date":
        value = candidate.get("launch_date") or candidate.get("inception_date") or extra.get("launch_date") or extra.get("inception_date")
    elif field_name == "tracking_difference_1y":
        value = candidate.get("tracking_difference_1y") or extra.get("tracking_difference_1y")
    if not _meaningful(value):
        return None
    return {
        "resolved_value": value,
        "source_name": "candidate_registry",
        "source_url": None,
        "source_type": "registry_seed",
        "observed_at": candidate.get("updated_at") or candidate.get("observed_at"),
        "value_type": "current",
        "missingness_reason": "populated",
        "provenance_level": "seeded_fallback",
    }


def _effective_truth_item(
    candidate: dict[str, Any],
    resolved_truth: dict[str, dict[str, Any]],
    field_name: str,
    *,
    benchmark_assignment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved = dict(resolved_truth.get(field_name) or {})
    if field_name == "benchmark_key" and _row_has_usable_value(resolved):
        resolved["source_type"] = "benchmark_mapping"
        resolved["provenance_level"] = "verified_mapping"
        return resolved
    if field_name == "benchmark_name":
        benchmark_item = _benchmark_assignment_truth_item(candidate, field_name, benchmark_assignment)
        if benchmark_item:
            return benchmark_item
        if _row_has_usable_value(resolved):
            return resolved
    if field_name in _DERIVATION_QUALITY_FIELDS and _row_has_usable_value(resolved):
        source_name = str(resolved.get("source_name") or "").strip().lower()
        source_type = str(resolved.get("source_type") or "").strip().lower()
        provenance = str(resolved.get("provenance_level") or "").strip().lower()
        if source_name in {"etf_market_data", "market_route_runtime", "market_history_summary"} or source_type in {"derived_from_validated_history", "derived_market_evidence"} or provenance in {"derived_from_validated_history", "derived_market_evidence"}:
            resolved["source_type"] = "derived_from_validated_history"
            resolved["provenance_level"] = "derived_from_validated_history"
        return resolved
    if _row_has_usable_value(resolved):
        return resolved
    if field_name in {"benchmark_key", "benchmark_name"}:
        benchmark_item = _benchmark_assignment_truth_item(candidate, field_name, benchmark_assignment)
        if benchmark_item:
            return benchmark_item
    seed_item = _candidate_seed_truth_item(candidate, field_name)
    if seed_item:
        return seed_item
    return resolved


def _candidate_with_benchmark_assignment(
    candidate: dict[str, Any],
    benchmark_assignment: dict[str, Any] | None,
) -> dict[str, Any]:
    assignment = dict(benchmark_assignment or {})
    if not assignment:
        return dict(candidate)
    enriched = dict(candidate)
    if not _meaningful(enriched.get("benchmark_key")) and _meaningful(assignment.get("benchmark_key")):
        enriched["benchmark_key"] = assignment.get("benchmark_key")
    canonical_label = canonical_benchmark_full_name(
        str(assignment.get("benchmark_key") or "").strip() or None,
        str(assignment.get("benchmark_label") or "").strip() or None,
    )
    if not _meaningful(enriched.get("benchmark_name")) and _meaningful(canonical_label or assignment.get("benchmark_label")):
        enriched["benchmark_name"] = canonical_label or assignment.get("benchmark_label")
    if not _meaningful(enriched.get("benchmark_label")) and _meaningful(assignment.get("benchmark_label")):
        enriched["benchmark_label"] = assignment.get("benchmark_label")
    return enriched


def _fixability(field_name: str, status: str) -> tuple[str, str]:
    normalized_status = str(status or "").strip()
    if normalized_status == "verified":
        return "resolved", "Resolved in current truth"
    if field_name == "replication_method" and normalized_status in {"critical_missing", "weak_authority"}:
        return "source_gap", "Needs issuer-document parsing or stronger source coverage"
    if normalized_status == "stale":
        return "refreshable", "Refreshable from current sources"
    if normalized_status == "hard_conflict":
        return "reconcile", "Needs source reconciliation"
    if normalized_status == "soft_drift":
        return "review", "Needs reviewer confirmation"
    if normalized_status == "execution_review_required":
        return "execution_review", "Needs execution review before recommendation use"
    if normalized_status == "weak_authority":
        return "stronger_source", "Needs stronger source authority"
    if field_name in {"benchmark_key", "benchmark_name"} and normalized_status == "critical_missing":
        return "mapping_gap", "Fixable through benchmark mapping or registry enrichment"
    if normalized_status == "critical_missing":
        return "pipeline_gap", "Fixable through the current provider or document pipeline"
    return "review", "Needs further review"


def _reconciliation_status_for_field(
    *,
    field_name: str,
    values: set[str],
    resolved: dict[str, Any],
    severity: str,
) -> tuple[str, str]:
    freshness_state = _field_freshness(resolved, field_name) if resolved else "missing"
    authority_class = _authority_class(resolved) if resolved else "missing"
    resolved_source_name = str(resolved.get("source_name") or "").strip().lower()
    resolved_source_type = str(resolved.get("source_type") or "").strip().lower()
    resolved_provenance = str(resolved.get("provenance_level") or "").strip().lower()
    validated_market_evidence = (
        field_name == "liquidity_proxy"
        and _row_has_usable_value(resolved)
        and (
            resolved_source_name in {"market_route_runtime", "etf_market_data", "market_history_summary"}
            or resolved_source_type in {"derived_from_validated_history", "derived_market_evidence"}
            or resolved_provenance in {"derived_from_validated_history", "derived_market_evidence"}
        )
    )
    if not resolved and not values:
        return "critical_missing", f"{_field_label(field_name)} is still missing from current candidate truth."
    if validated_market_evidence:
        return "verified", "Liquidity support is validated by current market-route evidence."
    if len(values) > 1:
        if field_name in {"bid_ask_spread_proxy", "liquidity_proxy"}:
            return "execution_review_required", "Bid-ask spread proxy disagrees across sources and needs execution review."
        if field_name == "aum":
            return "soft_drift", "AUM drifts across sources and time windows."
        if field_name == "primary_listing_exchange" and str(resolved.get("source_name") or "").strip() in {"issuer_doc_parser", "issuer_doc_registry"}:
            return "soft_drift", "Primary listing exchange varies across sources, but the selected tradable line remains reviewable."
        if field_name in _CONFLICT_CRITICAL_FIELDS:
            return "hard_conflict", f"{_field_label(field_name)} disagrees across current sources."
        return "soft_drift", f"{_field_label(field_name)} drifts across sources."
    if freshness_state == "stale":
        return "stale", f"{_field_label(field_name)} is present but stale for recommendation use."
    if authority_class in {"registry_seed", "provider_or_market_summary"} and field_name in _IMPLEMENTATION_BLOCKING_FIELDS:
        return "weak_authority", f"{_field_label(field_name)} is resolved, but only from weaker authority classes."
    return "verified", f"{_field_label(field_name)} reconciles cleanly."


def _blocking_effect_for_status(field_name: str, status: str, severity: str) -> str:
    if status == "critical_missing":
        return "block"
    if status == "hard_conflict" and severity == "critical":
        return "block"
    if status == "stale":
        return "block" if field_name in _HARD_STALE_BLOCK_FIELDS else "review"
    if status in {"soft_drift", "weak_authority", "execution_review_required"}:
        return "review"
    return "none"


def _reconciliation_row_active(row: dict[str, Any]) -> bool:
    try:
        annotation = json.loads(str(row.get("override_annotation_json") or "{}"))
    except Exception:
        annotation = {}
    if not isinstance(annotation, dict):
        annotation = {}
    resolution_state = str(annotation.get("resolution_state") or "").strip().lower()
    reconciled_state = str(annotation.get("reconciled_state") or "").strip().lower()
    if resolution_state in {"quarantined", "demoted", "inactive"} or reconciled_state in {"quarantined", "demoted", "inactive"}:
        return False
    if bool(annotation.get("quarantined")) or bool(annotation.get("demoted")):
        return False
    return True


def build_reconciliation_report(
    conn: sqlite3.Connection,
    *,
    candidate: dict[str, Any],
    candidate_symbol: str,
    sleeve_key: str,
    resolved_truth: dict[str, dict[str, Any]],
    benchmark_assignment: dict[str, Any] | None = None,
    applicable_field_names: set[str] | None = None,
) -> list[dict[str, Any]]:
    applicable = set(applicable_field_names or set())
    report: list[dict[str, Any]] = []

    for field_name in _RECOMMENDATION_CRITICAL_FIELDS:
        observation_rows = conn.execute(
            """
            SELECT value_json, source_name, overwrite_priority, override_annotation_json
            FROM candidate_field_observations
            WHERE candidate_symbol = ? AND sleeve_key = ? AND field_name = ?
              AND missingness_reason = 'populated'
            ORDER BY ingested_at DESC
            LIMIT 80
            """,
            (candidate_symbol.upper(), sleeve_key, field_name),
        ).fetchall()
        values: set[str] = set()
        for row in observation_rows:
            row_dict = dict(row)
            if not _reconciliation_row_active(row_dict):
                continue
            try:
                decoded = json.loads(str(row_dict.get("value_json") or "null"))
            except Exception:
                decoded = str(row_dict.get("value_json") or "").strip()
            normalized = _normalize_reconciliation_value(field_name, decoded)
            if normalized:
                values.add(normalized)

        resolved = dict(
            resolved_truth.get(field_name)
            or _effective_truth_item(
                candidate,
                resolved_truth,
                field_name,
                benchmark_assignment=benchmark_assignment,
            )
            or {}
        )
        severity = "critical" if (not applicable or field_name in applicable) else "supporting"
        status, summary = _reconciliation_status_for_field(
            field_name=field_name,
            values=values,
            resolved=resolved,
            severity=severity,
        )
        fixability, fixability_note = _fixability(field_name, status)
        report.append(
            {
                "field_name": field_name,
                "label": _field_label(field_name),
                "status": status,
                "summary": summary,
                "severity": severity,
                "recommendation_critical": field_name in applicable if applicable else True,
                "blocking_effect": _blocking_effect_for_status(field_name, status, severity),
                "fixability": fixability,
                "fixability_note": fixability_note,
                "observation_count": len(values),
                "observed_values": sorted(values)[:5],
            }
        )
    return report


def build_reconciliation_summary(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
    sleeve_key: str,
) -> dict[str, Any]:
    observation_rows = conn.execute(
        """
        SELECT field_name, value_json, override_annotation_json
        FROM candidate_field_observations
        WHERE candidate_symbol = ? AND sleeve_key = ?
          AND missingness_reason = 'populated'
        ORDER BY overwrite_priority DESC, ingested_at DESC
        LIMIT 500
        """,
        (candidate_symbol.upper(), sleeve_key),
    ).fetchall()
    values_by_field: dict[str, set[str]] = {}
    for row in observation_rows:
        row_dict = dict(row)
        if not _reconciliation_row_active(row_dict):
            continue
        field_name = str(row_dict.get("field_name") or "").strip()
        if not field_name:
            continue
        try:
            decoded = json.loads(str(row_dict.get("value_json") or "null"))
        except Exception:
            decoded = str(row_dict.get("value_json") or "").strip()
        normalized = _normalize_reconciliation_value(field_name, decoded)
        if not normalized:
            continue
        values_by_field.setdefault(field_name, set()).add(normalized)

    hard_conflicts: list[str] = []
    review_fields: list[str] = []
    for field_name, values in values_by_field.items():
        field_status, _ = _reconciliation_status_for_field(
            field_name=field_name,
            values=values,
            resolved={},
            severity="critical",
        )
        if field_status == "hard_conflict":
            hard_conflicts.append(field_name)
        elif field_status in {"soft_drift", "weak_authority", "execution_review_required", "stale"}:
            review_fields.append(field_name)

    status = "hard_conflict" if hard_conflicts else ("verified" if values_by_field else "missing")
    summary = (
        "Recommendation-critical truth still contains unreconciled field conflicts."
        if status == "hard_conflict"
        else "Field reconciliation is currently clean."
        if status == "verified"
        else "Recommendation-critical truth is still incomplete."
    )
    return {
        "status": status,
        "summary": summary,
        "resolved_field_count": len(values_by_field),
        "hard_conflicts": sorted(hard_conflicts),
        "review_fields": sorted(review_fields),
    }


def _visible_state_from_investor_state(investor_state: str) -> str:
    normalized = str(investor_state or "").strip().lower()
    return _VISIBLE_STATE_BY_INVESTOR_STATE.get(normalized, "watch")


def _visible_allowed_action(visible_state: str) -> str:
    return _VISIBLE_ALLOWED_ACTION.get(str(visible_state or "").strip().lower(), "monitor")


def _failure_item(class_id: str, severity: str, summary: str, *, fields: list[str] | None = None) -> dict[str, Any]:
    return {
        "class_id": class_id,
        "severity": severity,
        "summary": str(summary or "").strip(),
        "fields": sorted(dict.fromkeys([str(field or "").strip() for field in list(fields or []) if str(field or "").strip()])),
    }


def _class_label(class_id: str) -> str:
    return {
        "identity_conflict": "Identity conflict",
        "missing_truth": "Missing truth",
        "conflicting_truth": "Conflicting truth",
        "benchmark_lineage_conflict": "Benchmark-lineage conflict",
        "domicile_conflict": "Domicile conflict",
        "cross_source_recommendation_conflict": "Cross-source recommendation conflict",
        "weak_authority_truth": "Weak-authority truth",
        "stale_truth": "Stale truth",
        "bounded_proxy_support": "Bounded proxy support",
        "doctrine_restraint": "Doctrine restraint",
        "execution_review_required": "Execution review required",
        "execution_invalid": "Execution truth too weak",
    }.get(class_id, class_id.replace("_", " ").strip().title())


def build_failure_class_summary(
    *,
    recommendation_gate: dict[str, Any],
    reconciliation_report: list[dict[str, Any]],
    source_authority_map: list[dict[str, Any]],
    identity_state: dict[str, Any],
    implementation_profile: dict[str, Any] | None = None,
    benchmark_assignment: dict[str, Any] | None = None,
    doctrine_restraint: dict[str, Any] | None = None,
    source_completion_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    completed_source_fields = _completed_source_fields(source_completion_summary)
    if bool(identity_state.get("blocking")):
        items.append(
            _failure_item(
                "identity_conflict",
                "block",
                str(identity_state.get("summary") or "Identity conflict remains unresolved."),
                fields=["fund_name", "isin"],
            )
        )

    critical_missing_fields = [str(field or "").strip() for field in list(recommendation_gate.get("critical_missing_fields") or []) if str(field or "").strip()]
    if critical_missing_fields:
        items.append(
            _failure_item(
                "missing_truth",
                "block",
                "Recommendation-critical truth is still missing on: " + ", ".join(field.replace("_", " ") for field in critical_missing_fields[:5]) + ".",
                fields=critical_missing_fields,
            )
        )

    blocking_conflict_fields = [
        str(item.get("field_name") or "").strip()
        for item in reconciliation_report
        if item.get("status") == "hard_conflict"
    ]
    if blocking_conflict_fields:
        summary = "Recommendation-critical facts still disagree across sources."
        conflict_class = "cross_source_recommendation_conflict"
        if all(field in _BENCHMARK_FIELDS for field in blocking_conflict_fields):
            conflict_class = "benchmark_lineage_conflict"
            summary = "Benchmark lineage facts still disagree across sources."
        elif set(blocking_conflict_fields) == {"domicile"}:
            conflict_class = "domicile_conflict"
            summary = "Domicile evidence still disagrees across sources."
        items.append(
            _failure_item(
                conflict_class,
                "block",
                summary,
                fields=blocking_conflict_fields,
            )
        )

    stale_fields = [
        str(item.get("field_name") or "").strip()
        for item in reconciliation_report
        if item.get("status") == "stale"
        and str(item.get("field_name") or "").strip() not in completed_source_fields
    ]
    if stale_fields:
        stale_severity = "block" if any(field in _HARD_STALE_BLOCK_FIELDS for field in stale_fields) else "review"
        items.append(
            _failure_item(
                "stale_truth",
                stale_severity,
                "Some recommendation-critical facts are present but too stale for a clean read.",
                fields=stale_fields,
            )
        )

    execution_review_fields = [
        str(item.get("field_name") or "").strip()
        for item in reconciliation_report
        if item.get("status") == "execution_review_required"
    ]
    if execution_review_fields:
        items.append(
            _failure_item(
                "execution_review_required",
                "review",
                "Execution proxies still disagree enough to require review, but they do not invalidate the candidate on their own.",
                fields=execution_review_fields,
            )
        )

    weak_authority_fields = [
        str(item.get("field_name") or "").strip()
        for item in reconciliation_report
        if item.get("status") == "weak_authority"
        and str(item.get("field_name") or "").strip() not in completed_source_fields
    ]
    if weak_authority_fields:
        non_benchmark_weak = [field for field in weak_authority_fields if field not in _BENCHMARK_FIELDS]
        benchmark_weak = [field for field in weak_authority_fields if field in _BENCHMARK_FIELDS]
        if non_benchmark_weak:
            items.append(
                _failure_item(
                    "weak_authority_truth",
                    "confidence_drag",
                    "Some recommendation-critical fields are present, but only from weaker authority classes.",
                    fields=non_benchmark_weak,
                )
            )
        if benchmark_weak:
            items.append(
                _failure_item(
                    "bounded_proxy_support",
                    "confidence_drag",
                    "Benchmark support is present, but still bounded by proxy or weaker-authority lineage.",
                    fields=benchmark_weak,
                )
            )

    missing_document_support = [
        str(item.get("field_name") or "").strip()
        for item in source_authority_map
        if _material_document_gap(item)
        and str(item.get("field_name") or "").strip() not in completed_source_fields
    ]
    benchmark_proxy_symbol = str(dict(benchmark_assignment or {}).get("benchmark_proxy_symbol") or "").strip()
    if missing_document_support and not any(item["class_id"] == "bounded_proxy_support" for item in items):
        if any(field in _BENCHMARK_FIELDS for field in missing_document_support) or benchmark_proxy_symbol:
            items.append(
                _failure_item(
                    "bounded_proxy_support",
                    "confidence_drag",
                    "Benchmark and lineage support remain usable, but still bounded by proxy or lighter documentation.",
                    fields=[field for field in missing_document_support if field in _BENCHMARK_FIELDS] or (["benchmark_key"] if benchmark_proxy_symbol else []),
                )
            )

    if bool(recommendation_gate.get("execution_blocking")):
        execution_profile = dict(implementation_profile or {})
        execution_evidence = dict(execution_profile.get("execution_evidence_summary") or {})
        route_validity_state = str(execution_profile.get("route_validity_state") or "").strip()
        direct_history_depth = int(execution_evidence.get("direct_history_depth") or 0)
        proxy_history_depth = int(execution_evidence.get("proxy_history_depth") or 0)
        execution_summary = "Execution quality remains too weak for investor-grade use."
        execution_fields = ["liquidity_proxy", "bid_ask_spread_proxy"]
        if route_validity_state == "missing_history" and direct_history_depth <= 0 and proxy_history_depth <= 0:
            execution_summary = (
                "Stable route is resolved, but no usable direct or proxy market history is currently available "
                "for execution validation."
            )
            execution_fields = ["direct_history_depth", "proxy_history_depth"]
        elif route_validity_state == "invalid":
            execution_summary = "Current route or market evidence is invalid for investor-grade execution use."
            execution_fields = ["route_validity_state", "liquidity_proxy", "bid_ask_spread_proxy"]
        items.append(
            _failure_item(
                "execution_invalid",
                "block",
                execution_summary,
                fields=execution_fields,
            )
        )

    if doctrine_restraint:
        items.append(
            _failure_item(
                "doctrine_restraint",
                "review",
                str(doctrine_restraint.get("summary") or "Process doctrine still keeps the candidate in review."),
            )
        )

    hard_classes = [item["class_id"] for item in items if item["severity"] == "block"]
    review_classes = [item["class_id"] for item in items if item["severity"] == "review"]
    confidence_drag_classes = [item["class_id"] for item in items if item["severity"] == "confidence_drag"]
    primary_item = next((item for item in items if item["severity"] == "block"), None)
    if primary_item is None:
        primary_item = next((item for item in items if item["severity"] == "review"), None)
    if primary_item is None:
        primary_item = next((item for item in items if item["severity"] == "confidence_drag"), None)
    return {
        "primary_class": (str(primary_item.get("class_id") or "").strip() or None) if primary_item else None,
        "primary_label": _class_label(str(primary_item.get("class_id") or "")) if primary_item else None,
        "summary": str((primary_item or {}).get("summary") or recommendation_gate.get("summary") or "").strip() or None,
        "hard_classes": hard_classes,
        "review_classes": review_classes,
        "confidence_drag_classes": confidence_drag_classes,
        "items": [
            {
                **item,
                "label": _class_label(str(item.get("class_id") or "")),
            }
            for item in items
        ],
    }
def build_visible_decision_state(
    *,
    investor_decision_state: str,
    recommendation_gate: dict[str, Any],
    identity_state: dict[str, Any],
    failure_class_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    visible_state = _visible_state_from_investor_state(investor_decision_state)
    rationale = (
        str(dict(failure_class_summary or {}).get("summary") or "").strip()
        or str(identity_state.get("summary") or "").strip()
        or str(recommendation_gate.get("summary") or "").strip()
    )
    if not rationale:
        rationale = (
            "Candidate truth and implementation are clean enough for action."
            if visible_state == "eligible"
            else "Candidate stays in review while support tightens."
            if visible_state == "review"
            else "Candidate stays in research scope only."
            if visible_state == "research_only"
            else "Candidate remains blocked until the core issues clear."
        )
    return {
        "state": visible_state,
        "allowed_action": _visible_allowed_action(visible_state),
        "rationale": rationale,
    }


def _primary_document_manifest(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        dict(item)
        for source_key in ("primary_documents", "primary_document_manifest")
        for item in list(candidate.get(source_key) or [])
        if isinstance(item, dict)
    ]
    manifest: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _append_manifest_item(
        *,
        doc_type: str,
        raw_doc_url: str | None,
        status: str,
        retrieved_at: Any,
        authority_class: str = "issuer_secondary",
        raw_cache_file: str | None = None,
        title: str | None = None,
        source: str | None = None,
        issuer: str | None = None,
        as_of_date: Any = None,
        confidence: str | None = None,
        freshness_state: str | None = None,
        document_id: str | None = None,
    ) -> None:
        doc_url = _sanitize_surface_url(raw_doc_url)
        cache_file = None
        fingerprint_seed = json.dumps(
            {
                "doc_type": doc_type,
                "doc_url": raw_doc_url,
                "cache_file": raw_cache_file,
                "status": status,
                "retrieved_at": retrieved_at,
            },
            sort_keys=True,
            ensure_ascii=True,
        )
        document_fingerprint = hashlib.sha1(fingerprint_seed.encode("utf-8")).hexdigest()[:16]
        dedupe_key = f"{doc_type}:{raw_doc_url or raw_cache_file or document_fingerprint}"
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        manifest.append(
            {
                "document_id": document_id or f"doc_{document_fingerprint}",
                "title": title or doc_type.replace("_", " ").title(),
                "doc_type": doc_type,
                "doc_url": doc_url,
                "source_url": doc_url,
                "source": source,
                "issuer": issuer,
                "status": status,
                "retrieved_at": retrieved_at,
                "as_of_date": as_of_date,
                "confidence": confidence or ("high" if str(status).strip().lower() in _USABLE_DOCUMENT_STATUSES else "low"),
                "freshness_state": freshness_state or ("current" if str(status).strip().lower() in _USABLE_DOCUMENT_STATUSES else "unavailable_with_verified_reason"),
                "authority_class": authority_class,
                "cache_file": cache_file,
                "document_fingerprint": document_fingerprint,
            }
        )

    def _infer_doc_type(url: str | None, data_type: str | None = None) -> str | None:
        lowered = str(url or "").strip().lower()
        data_type_norm = str(data_type or "").strip().lower()
        if data_type_norm == "holdings" or "holdings" in lowered:
            return "holdings_disclosure"
        if "prospectus" in lowered:
            return "prospectus"
        if "kiid" in lowered or "kid" in lowered:
            return "kid"
        if any(marker in lowered for marker in ("factsheet", "fact-sheet", "/documents/", ".pdf")):
            return "factsheet"
        return None

    for item in rows:
        doc_type = str(item.get("doc_type") or "document").strip() or "document"
        raw_doc_url = str(item.get("doc_url") or item.get("source_url") or "").strip() or None
        status = str(item.get("status") or "unknown").strip() or "unknown"
        raw_cache_file = str(item.get("cache_file") or "").strip() or None
        retrieved_at = item.get("retrieved_at")
        authority_class = str(item.get("authority_class") or ("issuer_primary" if doc_type in {"prospectus", "holdings_disclosure"} else "issuer_secondary"))
        _append_manifest_item(
            doc_type=doc_type,
            raw_doc_url=raw_doc_url,
            raw_cache_file=raw_cache_file,
            status=status,
            retrieved_at=retrieved_at,
            authority_class=authority_class,
            title=str(item.get("title") or "").strip() or None,
            source=str(item.get("source") or "").strip() or None,
            issuer=str(item.get("issuer") or "").strip() or None,
            as_of_date=item.get("as_of_date"),
            confidence=str(item.get("confidence") or "").strip() or None,
            freshness_state=str(item.get("freshness_state") or "").strip() or None,
            document_id=str(item.get("document_id") or "").strip() or None,
        )
    verification_metadata = dict(candidate.get("verification_metadata") or {})
    factsheet_summary = dict(verification_metadata.get("factsheet_summary") or {})
    factsheet_citation = dict(factsheet_summary.get("citation") or {})
    factsheet_url = str(factsheet_citation.get("source_url") or "").strip() or None
    if factsheet_url:
        _append_manifest_item(
            doc_type="factsheet",
            raw_doc_url=factsheet_url,
            status="success",
            retrieved_at=factsheet_citation.get("retrieved_at"),
        )
    for entry in list(verification_metadata.get("fetch_entries") or []):
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("source_url") or "").strip() or None
        doc_type = _infer_doc_type(url, str(entry.get("data_type") or ""))
        if not doc_type:
            continue
        _append_manifest_item(
            doc_type=doc_type,
            raw_doc_url=url,
            status=str(entry.get("status") or "unknown").strip() or "unknown",
            retrieved_at=entry.get("finished_at") or entry.get("started_at"),
            authority_class="issuer_primary" if doc_type in {"prospectus", "holdings_disclosure"} else "issuer_secondary",
        )
    for raw_url in list(candidate.get("source_links") or []):
        url = str(raw_url or "").strip() or None
        doc_type = _infer_doc_type(url)
        if not doc_type:
            continue
        _append_manifest_item(
            doc_type=doc_type,
            raw_doc_url=url,
            status="success",
            retrieved_at=None,
            authority_class="issuer_primary" if doc_type in {"prospectus", "holdings_disclosure"} else "issuer_secondary",
        )
    symbol = _normalize_symbol(candidate)
    if symbol:
        try:
            registry_rows = [
                dict(item)
                for item in list(load_doc_registry().get("candidates") or [])
                if str(dict(item).get("ticker") or "").strip().upper() == symbol
            ]
        except Exception:
            registry_rows = []
        for registry_row in registry_rows[:1]:
            docs = dict(registry_row.get("docs") or {})
            issuer = str(registry_row.get("issuer") or "").strip() or str(candidate.get("issuer") or "").strip() or None
            name = str(registry_row.get("name") or candidate.get("fund_name") or symbol).strip()
            doc_specs = (
                ("factsheet", docs.get("factsheet_pdf_url") or docs.get("factsheet_html_url")),
                ("kid", docs.get("kid_pdf_url") or docs.get("kiid_pdf_url")),
                ("prospectus", docs.get("prospectus_pdf_url") or docs.get("prospectus_url")),
                ("annual_report", docs.get("annual_report_pdf_url") or docs.get("annual_report_url")),
                ("benchmark_methodology", docs.get("benchmark_methodology_url") or docs.get("index_methodology_url")),
            )
            for doc_type, url_value in doc_specs:
                raw_url = str(url_value or "").strip() or None
                if not raw_url:
                    continue
                _append_manifest_item(
                    doc_type=doc_type,
                    raw_doc_url=raw_url,
                    status="verified",
                    retrieved_at=None,
                    authority_class="issuer_primary" if doc_type in {"prospectus", "annual_report"} else "issuer_secondary",
                    title=f"{name} {doc_type.replace('_', ' ').title()}",
                    source="issuer_doc_registry",
                    issuer=issuer,
                    confidence="high",
                    freshness_state="current",
                    document_id=f"{symbol.lower()}_{doc_type}",
                )
    return manifest


def _document_support_refs(
    field_name: str,
    *,
    source_url: str | None,
    manifest: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    preferred_types = preferred_document_types(field_name)
    refs: list[dict[str, Any]] = []
    for item in manifest:
        doc_type = str(item.get("doc_type") or "")
        doc_url = str(item.get("doc_url") or "").strip() or None
        if source_url and doc_url and doc_url == source_url:
            refs.append(dict(item))
    if refs:
        valid_refs = [ref for ref in refs if str(ref.get("status") or "").strip().lower() in _USABLE_DOCUMENT_STATUSES]
        return valid_refs or refs
    valid_preferred = [
        dict(item)
        for item in manifest
        if str(item.get("doc_type") or "") in preferred_types
        and str(item.get("status") or "").strip().lower() in _USABLE_DOCUMENT_STATUSES
    ]
    if valid_preferred:
        return valid_preferred[:3]
    return [dict(item) for item in manifest if str(item.get("doc_type") or "") in preferred_types][:3]


def _has_usable_document_ref(refs: list[dict[str, Any]]) -> bool:
    return any(str(ref.get("status") or "").strip().lower() in _USABLE_DOCUMENT_STATUSES for ref in refs)


def _material_document_gap(item: dict[str, Any]) -> bool:
    field_name = str(item.get("field_name") or "").strip()
    if not field_name or not preferred_document_types(field_name):
        return False
    if field_name in _DERIVED_LINEAGE_FIELDS or field_name in _DERIVATION_QUALITY_FIELDS:
        return False
    authority_class = str(item.get("authority_class") or "").strip()
    freshness_state = str(item.get("freshness_state") or "").strip()
    document_support_state = str(item.get("document_support_state") or "").strip()
    if document_support_state in {"backed", "derived_mapping", "derived_market_evidence"}:
        return False
    if authority_class == "missing" and freshness_state == "missing":
        return False
    if authority_class == "verified_current_truth" and field_name in _BENCHMARK_FIELDS:
        return False
    return True


def _authority_class(item: dict[str, Any]) -> str:
    name = str(item.get("source_name") or "").strip().lower()
    kind = str(item.get("source_type") or "").strip().lower()
    provenance = str(item.get("provenance_level") or "").strip().lower()
    if str(item.get("missingness_reason") or "").strip() and str(item.get("missingness_reason") or "").strip() != "populated":
        return "missing"
    if kind in {"benchmark_mapping", "mapping_authority"} or provenance in {"verified_mapping"} or name == "benchmark_registry":
        return "verified_current_truth"
    if kind in {"issuer_holdings_primary", "issuer_prospectus_primary"}:
        return "issuer_primary"
    if "issuer" in kind or "factsheet" in kind or "prospectus" in kind or "kid" in kind:
        return "issuer_secondary"
    if kind in {"derived_from_validated_history"} or provenance in {"verified_official", "manual_reviewed_override"}:
        return "verified_current_truth"
    if kind in {"verified_third_party_fallback"} or provenance == "verified_nonissuer":
        return "provider_or_market_summary"
    if "registry" in kind or "registry" in name or "seed" in name or provenance == "seeded_fallback":
        return "registry_seed"
    if "proxy" in kind:
        return "provider_or_market_summary"
    if provenance in {"inferred"}:
        return "registry_seed"
    return "verified_current_truth"


def _field_freshness(item: dict[str, Any], field_name: str) -> str:
    missingness_reason = str(item.get("missingness_reason") or "").strip()
    value_type = str(item.get("value_type") or "").strip().lower()
    provenance = str(item.get("provenance_level") or "").strip().lower()
    source_name = str(item.get("source_name") or "").strip().lower()
    if (
        field_name in {"primary_trading_currency", "primary_listing_exchange", "distribution_type"}
        and source_name in {
            "issuer_doc_parser",
            "issuer_doc_registry",
            "supplemental_candidate_metrics",
            "candidate_registry",
            "yahoo_finance",
        }
    ):
        return "current"
    if missingness_reason and missingness_reason != "populated":
        return "missing"
    if value_type == "stale":
        return "stale"
    if (
        field_name in _STRUCTURALLY_STABLE_IMPLEMENTATION_FIELDS
        and (
            provenance in {"verified_official", "manual_reviewed_override", "verified_mapping"}
            or source_name == "benchmark_registry"
        )
    ):
        return "current"
    if "proxy" in str(item.get("source_type") or "").strip().lower():
        return "proxy"
    observed_at = coerce_datetime(item.get("observed_at"))
    if observed_at is not None:
        age_days = max(0, int((datetime.now(UTC) - observed_at).total_seconds() // 86400))
        if age_days > field_stale_days(field_name, 365):
            return "stale"
    return "current"


def _execution_quote_freshness_state(value: Any) -> str:
    observed = coerce_datetime(value)
    if observed is None:
        return "unknown"
    age_days = max(0.0, (datetime.now(UTC) - observed).total_seconds() / 86400.0)
    if age_days <= 5:
        return "fresh"
    if age_days <= 30:
        return "aging"
    return "stale"


def _execution_history_depth_state(value: int | None) -> str:
    if value is None or value <= 0:
        return "missing"
    if value >= 756:
        return "strong"
    if value >= 252:
        return "usable"
    return "thin"


def _execution_confidence_state(
    *,
    quote_freshness_state: str,
    history_depth_state: str,
    spread_support_state: str,
    liquidity_support_state: str,
    volume_support_state: str,
    route_validity_state: str,
) -> str:
    if route_validity_state in {"benchmark_lineage_weak", "missing_history", "invalid"}:
        return "insufficient"
    if (
        route_validity_state == "direct_ready"
        and quote_freshness_state == "fresh"
        and history_depth_state == "strong"
        and spread_support_state == "usable"
        and liquidity_support_state in {"usable", "strong"}
    ):
        return "strong"
    if (
        route_validity_state in {"direct_ready", "proxy_ready"}
        and quote_freshness_state in {"fresh", "aging"}
        and history_depth_state in {"strong", "usable"}
        and spread_support_state in {"usable", "degraded"}
        and liquidity_support_state in {"usable", "degraded", "strong"}
        and volume_support_state in {"usable", "degraded"}
    ):
        return "usable"
    if (
        route_validity_state in {"direct_ready", "proxy_ready", "alias_review_needed", "unknown"}
        and (
            spread_support_state != "insufficient"
            or liquidity_support_state != "insufficient"
            or volume_support_state != "insufficient"
            or history_depth_state != "missing"
        )
    ):
        return "degraded"
    return "insufficient"


def _latest_completeness_snapshot(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
    sleeve_key: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT snapshot_id, candidate_symbol, sleeve_key, required_fields_total, required_fields_populated,
               critical_required_fields_missing_json, fetchable_missing_count, source_gap_missing_count,
               proxy_only_count, stale_required_count, readiness_level, computed_at
        FROM candidate_completeness_snapshots
        WHERE candidate_symbol = ? AND sleeve_key = ?
        ORDER BY computed_at DESC
        LIMIT 1
        """,
        (candidate_symbol.upper(), sleeve_key),
    ).fetchone()
    if row is None:
        return None
    item = dict(row)
    item["critical_required_fields_missing"] = json.loads(item.pop("critical_required_fields_missing_json") or "[]")
    return item


def _current_truth(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
    sleeve_key: str,
) -> dict[str, dict[str, Any]]:
    current = get_candidate_field_current(conn, candidate_symbol=candidate_symbol, sleeve_key=sleeve_key)
    if current:
        return current
    return resolve_candidate_field_truth(conn, candidate_symbol=candidate_symbol, sleeve_key=sleeve_key)


def _stored_series_depths(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
) -> dict[str, int]:
    candidate_id = candidate_id_for_symbol(candidate_symbol)
    direct_rows = load_price_series(
        conn,
        candidate_id=candidate_id,
        series_role="direct",
        interval="1day",
        ascending=True,
    )
    proxy_rows = load_price_series(
        conn,
        candidate_id=candidate_id,
        series_role="approved_proxy",
        interval="1day",
        ascending=True,
    )
    return {
        "direct_history_depth": len(direct_rows),
        "proxy_history_depth": len(proxy_rows),
    }


def build_implementation_profile(
    conn: sqlite3.Connection | None,
    candidate: dict[str, Any],
    *,
    resolved_truth: dict[str, dict[str, Any]],
    benchmark_assignment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = _primary_document_manifest(candidate)
    symbol = _normalize_symbol(candidate)
    expense_ratio = _pick_first(candidate, resolved_truth, "expense_ratio", "ter")
    benchmark_name = _benchmark_full_name(candidate, resolved_truth, benchmark_assignment=benchmark_assignment)
    replication_method = _pick_first(candidate, resolved_truth, "replication_method")
    exchange = _pick_first(candidate, resolved_truth, "extra.primary_listing_exchange", "primary_listing_exchange")
    currency = _pick_first(candidate, resolved_truth, "extra.primary_trading_currency", "primary_trading_currency", "base_currency")
    spread_proxy = _pick_first(candidate, resolved_truth, "bid_ask_spread_proxy")
    premium_discount = _pick_first(candidate, resolved_truth, "premium_discount_behavior", "premium_discount")
    aum = _pick_first(candidate, resolved_truth, "aum", "extra.aum_usd", "aum_usd")
    domicile = _pick_first(candidate, resolved_truth, "domicile")
    distribution = _pick_first(candidate, resolved_truth, "distribution_type", "share_class", "accumulation")
    launch_date = _pick_first(candidate, resolved_truth, "extra.launch_date", "launch_date", "inception_date")
    issuer = _pick_first(candidate, resolved_truth, "issuer")
    tracking_difference = _normalize_tracking_difference_value(
        _pick_first(candidate, resolved_truth, "extra.tracking_difference_1y", "tracking_difference_1y", "tracking_difference_3y", "tracking_difference_5y")
    )
    market_data_as_of = _pick_first(candidate, resolved_truth, "market_data_as_of")
    volume_30d_avg = _safe_float(_pick_first(candidate, resolved_truth, "volume_30d_avg"))
    direct_history_depth = _safe_float(_pick_first(candidate, resolved_truth, "extra.direct_history_depth", "direct_history_depth"))
    proxy_history_depth = _safe_float(_pick_first(candidate, resolved_truth, "extra.proxy_history_depth", "proxy_history_depth"))
    direct_history_points = int(direct_history_depth or 0)
    proxy_history_points = int(proxy_history_depth or 0)
    if conn is not None and (direct_history_points <= 0 or proxy_history_points <= 0):
        stored_depths = _stored_series_depths(conn, candidate_symbol=symbol)
        if direct_history_points <= 0:
            direct_history_points = int(stored_depths.get("direct_history_depth") or 0)
        if proxy_history_points <= 0:
            proxy_history_points = int(stored_depths.get("proxy_history_depth") or 0)
    history_depth = max(direct_history_points, proxy_history_points)
    liquidity_item = dict(resolved_truth.get("liquidity_proxy") or {})
    liquidity_proxy = liquidity_item.get("resolved_value") or _pick_first(candidate, resolved_truth, "liquidity_proxy")
    quote_freshness_state = _execution_quote_freshness_state(market_data_as_of)
    history_depth_state = _execution_history_depth_state(history_depth)
    spread_support_state = (
        "usable"
        if _meaningful(spread_proxy) and quote_freshness_state in {"fresh", "aging"}
        else "degraded"
        if _meaningful(spread_proxy)
        else "insufficient"
    )
    volume_support_state = (
        "usable"
        if volume_30d_avg is not None and history_depth_state in {"strong", "usable"}
        else "degraded"
        if volume_30d_avg is not None
        else "insufficient"
    )
    route_validity_state = str(
        _pick_first(candidate, resolved_truth, "route_validity_state", "extra.route_validity_state")
        or dict(candidate.get("coverage_workflow_summary") or {}).get("status")
        or ""
    ).strip() or "unknown"
    if route_validity_state == "unknown":
        if direct_history_points >= 260:
            route_validity_state = "direct_ready"
        elif proxy_history_points >= 260:
            route_validity_state = "proxy_ready"
    liquidity_source_name = str(liquidity_item.get("source_name") or "").strip().lower()
    liquidity_source_type = str(liquidity_item.get("source_type") or "").strip().lower()
    if liquidity_proxy in {"direct_history_backed", "provider_history_backed", "proxy_history_backed"}:
        if route_validity_state == "direct_ready" and history_depth_state == "strong":
            liquidity_support_state = "strong"
        elif route_validity_state in {"direct_ready", "proxy_ready"} and history_depth_state in {"strong", "usable"}:
            liquidity_support_state = "usable"
        else:
            liquidity_support_state = "degraded"
    elif liquidity_source_type == "derived_from_validated_history" or liquidity_source_name in {"market_route_runtime", "tiingo", "eod historical data", "eodhd"}:
        liquidity_support_state = "usable" if history_depth_state in {"strong", "usable"} else "degraded"
    elif _meaningful(liquidity_proxy):
        liquidity_support_state = "degraded"
    else:
        liquidity_support_state = "insufficient"
    execution_confidence = _execution_confidence_state(
        quote_freshness_state=quote_freshness_state,
        history_depth_state=history_depth_state,
        spread_support_state=spread_support_state,
        liquidity_support_state=liquidity_support_state,
        volume_support_state=volume_support_state,
        route_validity_state=route_validity_state,
    )
    mandate = benchmark_name or _pick_first(candidate, resolved_truth, "benchmark_name", "benchmark_key")

    missing: list[str] = []
    for field_name, value in (
        ("expense_ratio", expense_ratio),
        ("benchmark_name", benchmark_name),
        ("replication_method", replication_method),
        ("primary_listing_exchange", exchange),
        ("primary_trading_currency", currency),
        ("liquidity_proxy", liquidity_proxy),
        ("aum", aum),
        ("domicile", domicile),
        ("distribution_type", distribution),
        ("issuer", issuer),
    ):
        if not _meaningful(value):
            missing.append(field_name)

    execution_score = 78
    if route_validity_state in {"benchmark_lineage_weak", "missing_history", "invalid"}:
        execution_score -= 16
    elif route_validity_state == "alias_review_needed":
        execution_score -= 8
    if history_depth_state == "missing":
        execution_score -= 14
    elif history_depth_state == "thin":
        execution_score -= 8
    if not _meaningful(spread_proxy):
        execution_score -= 12
    elif spread_support_state == "degraded":
        execution_score -= 6
    if liquidity_support_state == "insufficient":
        execution_score -= 12
    elif liquidity_support_state == "degraded":
        execution_score -= 6
    if volume_support_state == "insufficient":
        execution_score -= 8
    elif volume_support_state == "degraded":
        execution_score -= 4
    if quote_freshness_state in {"stale", "unknown"}:
        execution_score -= 10
    elif quote_freshness_state == "aging":
        execution_score -= 4
    if candidate.get("liquidity_score") is not None and float(candidate.get("liquidity_score") or 0.0) < 0.65:
        execution_score -= 4
    if execution_confidence == "strong":
        execution_score += 8
    elif execution_confidence == "usable":
        execution_score += 2
    elif execution_confidence == "degraded":
        execution_score -= 6
    else:
        execution_score -= 14
    execution_score = max(18, min(92, execution_score))
    execution_suitability = "execution_efficient" if execution_score >= 72 else "execution_mixed" if execution_score >= 54 else "execution_weak"

    summary_parts = [
        f"{symbol} carries { _format_pct(expense_ratio) or 'an unresolved expense ratio' }",
        f"tracks {benchmark_name}" if _meaningful(benchmark_name) else "still has benchmark lineage gaps",
        f"lists on {exchange}" if _meaningful(exchange) else "does not yet expose primary listing venue",
        f"with { _format_aum(aum) or 'unresolved AUM' }",
    ]

    return {
        "issuer": issuer,
        "mandate_or_index": mandate,
        "replication_method": replication_method,
        "primary_listing_exchange": exchange,
        "primary_trading_currency": currency,
        "spread_proxy": _format_bps(spread_proxy),
        "premium_discount_behavior": premium_discount,
        "aum": _format_aum(aum),
        "domicile": domicile,
        "distribution_policy": distribution,
        "launch_date": launch_date,
        "issuer_name": issuer,
        "tracking_difference": _format_pct(tracking_difference),
        "execution_suitability": execution_suitability,
        "execution_score": execution_score,
        "quote_freshness_state": quote_freshness_state,
        "history_depth_state": history_depth_state,
        "spread_support_state": spread_support_state,
        "liquidity_support_state": liquidity_support_state,
        "volume_support_state": volume_support_state,
        "route_validity_state": route_validity_state,
        "execution_confidence": execution_confidence,
        "execution_evidence_summary": {
            "route_validity_state": route_validity_state,
            "direct_history_depth": direct_history_points,
            "proxy_history_depth": proxy_history_points,
            "quote_freshness_state": quote_freshness_state,
            "history_depth_state": history_depth_state,
            "spread_support_state": spread_support_state,
            "liquidity_support_state": liquidity_support_state,
            "volume_support_state": volume_support_state,
            "execution_confidence": execution_confidence,
        },
        "missing_fields": missing,
        "primary_document_manifest": manifest,
        "summary": ". ".join(summary_parts) + ".",
    }


def build_source_authority_map(
    candidate: dict[str, Any],
    *,
    resolved_truth: dict[str, dict[str, Any]],
    benchmark_assignment: dict[str, Any] | None = None,
    applicable_field_names: set[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    manifest = _primary_document_manifest(candidate)
    applicable = set(applicable_field_names or set())
    for field_name in _RECOMMENDATION_CRITICAL_FIELDS:
        item = _effective_truth_item(
            candidate,
            resolved_truth,
            field_name,
            benchmark_assignment=benchmark_assignment,
        )
        support_refs = _document_support_refs(
            field_name,
            source_url=str(item.get("source_url") or "").strip() or None,
            manifest=manifest,
        )
        authority_class = _authority_class(item)
        if field_name in _DERIVED_LINEAGE_FIELDS:
            if authority_class != "missing":
                authority_class = "verified_current_truth"
            support_refs = []
            document_support_state = "derived_mapping" if authority_class != "missing" else "missing"
        elif field_name in _DERIVATION_QUALITY_FIELDS:
            if authority_class != "missing":
                authority_class = "verified_current_truth"
            support_refs = []
            document_support_state = "derived_market_evidence" if authority_class != "missing" else "missing"
        else:
            if _has_usable_document_ref(support_refs):
                document_support_state = "backed"
            elif support_refs:
                document_support_state = "partial"
            else:
                document_support_state = "missing"
        if not item:
            rows.append(
                {
                    "field_name": field_name,
                    "label": _field_label(field_name),
                    "resolved_value": None,
                    "source_name": None,
                    "source_url": None,
                    "source_type": None,
                    "authority_class": "missing",
                    "observed_at": None,
                    "freshness_state": "missing",
                    "recommendation_critical": field_name in applicable if applicable else True,
                    "preferred_document_types": preferred_document_types(field_name),
                    "document_support_refs": support_refs,
                    "document_support_state": "missing",
                }
            )
            continue
        rows.append(
            {
                "field_name": field_name,
                "label": _field_label(field_name),
                "resolved_value": item.get("resolved_value"),
                "source_name": item.get("source_name"),
                "source_url": _sanitize_surface_url(item.get("source_url")),
                "source_type": item.get("source_type"),
                "authority_class": authority_class,
                "observed_at": item.get("observed_at"),
                "freshness_state": _field_freshness(item, field_name),
                "recommendation_critical": field_name in applicable if applicable else True,
                "preferred_document_types": preferred_document_types(field_name),
                "document_support_refs": support_refs,
                "document_support_state": document_support_state,
            }
        )
    return rows


def _source_completion_status(
    item: dict[str, Any],
    *,
    implementation_profile: dict[str, Any] | None = None,
) -> tuple[str, str]:
    field_name = str(item.get("field_name") or "").strip()
    authority_class = str(item.get("authority_class") or "").strip()
    freshness_state = str(item.get("freshness_state") or "").strip()
    source_name = str(item.get("source_name") or "").strip().lower()
    resolved_value = item.get("resolved_value")
    has_value = resolved_value is not None and str(resolved_value).strip() != ""
    execution_evidence = dict(dict(implementation_profile or {}).get("execution_evidence_summary") or {})
    direct_history_depth = int(execution_evidence.get("direct_history_depth") or 0)
    proxy_history_depth = int(execution_evidence.get("proxy_history_depth") or 0)
    history_depth = max(direct_history_depth, proxy_history_depth)

    if field_name in _DERIVATION_QUALITY_FIELDS and history_depth >= 260:
        return "equivalent_complete", "validated_market_path_equivalent"
    if authority_class != "missing" and freshness_state == "current":
        return "complete", "current_truth"
    if field_name == "launch_date" and history_depth >= 260:
        return "equivalent_complete", "verified_not_applicable_history_equivalent"
    if authority_class == "missing" or freshness_state == "missing":
        return "incomplete", "missing_truth"
    if field_name in _DERIVED_LINEAGE_FIELDS:
        return "equivalent_complete", "canonical_lineage_equivalent"
    if field_name in {"aum", "primary_trading_currency", "launch_date", "issuer"} and source_name == "yahoo finance":
        return "equivalent_complete", "verified_market_reference"
    if field_name in {
        "issuer",
        "domicile",
        "replication_method",
        "primary_listing_exchange",
        "distribution_type",
        "launch_date",
        "aum",
        "primary_trading_currency",
    } and source_name in {"candidate_payload", "candidate_registry"} and has_value:
        return "equivalent_complete", "curated_internal_reference"
    if freshness_state == "stale" and authority_class in {"issuer_primary", "issuer_secondary", "verified_current_truth"}:
        return "equivalent_complete", "stable_published_context"
    if has_value:
        return "equivalent_complete", "resolved_truth_present"
    return "incomplete", "missing_truth"


def _completed_source_fields(source_completion_summary: dict[str, Any] | None) -> set[str]:
    completed_fields: set[str] = set()
    for item in list(dict(source_completion_summary or {}).get("field_states") or []):
        if str(item.get("completion_state") or "").strip() != "incomplete":
            field_name = str(item.get("field_name") or "").strip()
            if field_name:
                completed_fields.add(field_name)
    return completed_fields


def build_source_completion_summary(
    *,
    source_authority_map: list[dict[str, Any]],
    implementation_profile: dict[str, Any] | None = None,
    reconciliation_report: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    field_states: list[dict[str, Any]] = []
    incomplete_fields: list[str] = []
    equivalent_ready_fields: list[str] = []
    not_applicable_verified_fields: list[str] = []
    resolved_by_curated_registry: list[str] = []
    completed_count = 0
    for item in source_authority_map:
        field_name = str(item.get("field_name") or "").strip()
        completion_state, completion_basis = _source_completion_status(
            item,
            implementation_profile=implementation_profile,
        )
        field_states.append(
            {
                "field_name": field_name,
                "completion_state": completion_state,
                "completion_basis": completion_basis,
            }
        )
        if completion_state == "incomplete":
            incomplete_fields.append(field_name)
            continue
        completed_count += 1
        if completion_state == "equivalent_complete":
            equivalent_ready_fields.append(field_name)
        if completion_basis == "verified_not_applicable_history_equivalent":
            not_applicable_verified_fields.append(field_name)
        if str(item.get("source_name") or "").strip().lower() == "supplemental_candidate_metrics":
            resolved_by_curated_registry.append(field_name)
    completed_fields = {
        str(item.get("field_name") or "").strip()
        for item in field_states
        if str(item.get("completion_state") or "").strip() != "incomplete"
    }
    weak_fields = sorted(
        {
            str(item.get("field_name") or "").strip()
            for item in list(reconciliation_report or [])
            if item.get("status") == "weak_authority"
            and str(item.get("field_name") or "").strip() not in completed_fields
        }
    )
    stale_fields = sorted(
        {
            str(item.get("field_name") or "").strip()
            for item in list(reconciliation_report or [])
            if item.get("status") == "stale"
            and str(item.get("field_name") or "").strip() not in completed_fields
        }
    )
    hard_conflict_fields = [
        str(item.get("field_name") or "").strip()
        for item in list(reconciliation_report or [])
        if item.get("status") == "hard_conflict"
    ]
    conflict_fields = sorted(dict.fromkeys(field for field in hard_conflict_fields if field))
    total_fields = len(source_authority_map)
    completeness_clean = completed_count >= total_fields and not incomplete_fields
    authority_clean = not weak_fields
    freshness_clean = not stale_fields
    conflict_clean = not conflict_fields
    state = "complete" if completeness_clean and authority_clean and freshness_clean and conflict_clean else "incomplete"
    summary = (
        f"All {completed_count}/{total_fields} recommendation-critical fields are source-complete."
        if state == "complete"
        else f"{completed_count}/{total_fields} recommendation-critical fields are source-complete."
    )
    if equivalent_ready_fields:
        summary += (
            f" {len(equivalent_ready_fields)} field"
            f"{'s are' if len(equivalent_ready_fields) != 1 else ' is'} satisfied through verified equivalent support."
        )
    if incomplete_fields:
        summary += (
            " Remaining incomplete fields: "
            + ", ".join(field.replace("_", " ") for field in incomplete_fields[:5])
            + "."
        )
    if weak_fields:
        summary += " Weaker-authority fields still need cleanup on: " + ", ".join(field.replace("_", " ") for field in weak_fields[:5]) + "."
    if stale_fields:
        summary += " Stale fields still need refresh on: " + ", ".join(field.replace("_", " ") for field in stale_fields[:5]) + "."
    if conflict_fields:
        summary += " Conflicts still need reconciliation on: " + ", ".join(field.replace("_", " ") for field in conflict_fields[:5]) + "."
    completion_reasons: list[str] = []
    if completeness_clean:
        completion_reasons.append("All recommendation-critical fields are complete.")
    if authority_clean:
        completion_reasons.append("No unresolved weak-authority critical fields remain.")
    if freshness_clean:
        completion_reasons.append("No unresolved stale critical fields remain.")
    if conflict_clean:
        completion_reasons.append("No unresolved critical-field conflicts remain.")
    if equivalent_ready_fields:
        completion_reasons.append(
            f"{len(equivalent_ready_fields)} field{'s are' if len(equivalent_ready_fields) != 1 else ' is'} satisfied through verified equivalent support."
        )
    if resolved_by_curated_registry:
        completion_reasons.append(
            "Curated verified registry support was used for: "
            + ", ".join(field.replace("_", " ") for field in resolved_by_curated_registry[:5])
            + "."
        )
    return {
        "state": state,
        "summary": summary,
        "critical_fields_completed": completed_count,
        "critical_fields_total": total_fields,
        "equivalent_ready_count": len(equivalent_ready_fields),
        "incomplete_fields": incomplete_fields,
        "missing_fields": incomplete_fields,
        "weak_fields": weak_fields,
        "stale_fields": stale_fields,
        "conflict_fields": conflict_fields,
        "authority_clean": authority_clean,
        "freshness_clean": freshness_clean,
        "conflict_clean": conflict_clean,
        "completeness_clean": completeness_clean,
        "equivalent_ready_fields": equivalent_ready_fields,
        "not_applicable_verified_fields": not_applicable_verified_fields,
        "resolved_by_curated_registry": sorted(dict.fromkeys(resolved_by_curated_registry)),
        "completion_reasons": completion_reasons,
        "field_states": field_states,
    }
def build_data_quality_summary(
    *,
    source_authority_map: list[dict[str, Any]],
    recommendation_gate: dict[str, Any],
    reconciliation: dict[str, Any],
    reconciliation_report: list[dict[str, Any]] | None = None,
    source_completion_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    critical_total = len(source_authority_map)
    completed_source_fields = _completed_source_fields(source_completion_summary)
    critical_ready = int(dict(source_completion_summary or {}).get("critical_fields_completed") or 0) or sum(
        1 for item in source_authority_map if item.get("freshness_state") == "current" and item.get("authority_class") != "missing"
    )
    issuer_backed = sum(1 for item in source_authority_map if str(item.get("authority_class") or "").startswith("issuer"))
    stale_critical = sum(
        1
        for item in source_authority_map
        if item.get("freshness_state") in {"stale", "missing"}
        and str(item.get("field_name") or "") not in completed_source_fields
    )
    weak_authority = sum(
        1
        for item in source_authority_map
        if str(item.get("field_name") or "") in _AUTHORITY_SENSITIVE_FIELDS
        and item.get("authority_class") in {"registry_seed", "provider_or_market_summary"}
        and str(item.get("field_name") or "") not in completed_source_fields
    )
    conflict_count = sum(
        1
        for item in list(reconciliation_report or [])
        if item.get("status") in {"hard_conflict", "soft_drift", "critical_missing", "stale", "weak_authority", "execution_review_required"}
        and str(item.get("field_name") or "") not in completed_source_fields
    )
    document_gap_count = sum(
        1
        for item in source_authority_map
        if _material_document_gap(item)
        and str(item.get("field_name") or "") not in completed_source_fields
    )
    data_confidence = str(recommendation_gate.get("data_confidence") or "mixed")
    if reconciliation.get("status") == "hard_conflict":
        data_confidence = "low"
    equivalent_ready_count = int(dict(source_completion_summary or {}).get("equivalent_ready_count") or 0)
    if str(dict(source_completion_summary or {}).get("state") or "").strip() == "complete":
        summary = f"{critical_ready}/{critical_total} recommendation-critical fields are source-complete."
        if equivalent_ready_count:
            summary += f" {equivalent_ready_count} field{'s are' if equivalent_ready_count != 1 else ' is'} satisfied through verified equivalent support."
    else:
        summary = (
            f"{critical_ready}/{critical_total} recommendation-critical fields are currently backed by resolved truth; "
            f"{issuer_backed} field{'s' if issuer_backed != 1 else ''} are issuer-backed."
        )
    if stale_critical:
        summary += f" {stale_critical} critical field{'s are' if stale_critical != 1 else ' is'} stale or missing."
    if weak_authority:
        summary += f" {weak_authority} field{'s rely' if weak_authority != 1 else ' relies'} on weaker authority classes."
    if conflict_count:
        summary += f" {conflict_count} field-level conflict or review item{'s remain' if conflict_count != 1 else ' remains'}."
    if document_gap_count:
        summary += f" {document_gap_count} critical field{'s still need' if document_gap_count != 1 else ' still needs'} preferred document support."
    return {
        "data_confidence": data_confidence,
        "critical_fields_ready": critical_ready,
        "critical_fields_total": critical_total,
        "issuer_backed_fields": issuer_backed,
        "stale_critical_fields": stale_critical,
        "document_gap_count": document_gap_count,
        "summary": summary,
    }


def _authority_row(source_authority_map: list[dict[str, Any]], field_name: str) -> dict[str, Any]:
    for item in source_authority_map:
        if str(item.get("field_name") or "").strip() == field_name:
            return dict(item)
    return {}
def build_identity_state(
    conn: sqlite3.Connection,
    *,
    candidate: dict[str, Any],
    candidate_symbol: str,
    sleeve_key: str,
    resolved_truth: dict[str, dict[str, Any]],
    benchmark_assignment: dict[str, Any] | None = None,
    source_tier_threshold: int = 15,
) -> dict[str, Any]:
    SEED_SOURCES = {"candidate_payload", "candidate_registry", "finnhub", "fmp", "financial modeling prep"}

    def _load_identity_rows(field_name: str) -> list[sqlite3.Row]:
        params = [
            candidate_symbol.upper(),
            sleeve_key,
            field_name,
            source_tier_threshold,
            *sorted(SEED_SOURCES),
        ]
        preferred_rows = conn.execute(
            f"""
            SELECT field_name, value_json, source_name, overwrite_priority, override_annotation_json
            FROM candidate_field_observations
            WHERE candidate_symbol = ? AND sleeve_key = ? AND field_name = ?
              AND missingness_reason = 'populated'
              AND COALESCE(overwrite_priority, 0) >= ?
              AND LOWER(COALESCE(source_name, '')) NOT IN ({",".join("?" for _ in SEED_SOURCES)})
            ORDER BY ingested_at DESC
            LIMIT 80
            """,
            params,
        ).fetchall()
        if preferred_rows:
            return [row for row in preferred_rows if _reconciliation_row_active(dict(row))]
        return list(
            row for row in conn.execute(
                """
                SELECT field_name, value_json, source_name, overwrite_priority, override_annotation_json
                FROM candidate_field_observations
                WHERE candidate_symbol = ? AND sleeve_key = ? AND field_name = ?
                  AND missingness_reason = 'populated'
                ORDER BY ingested_at DESC
                LIMIT 80
                """,
                (candidate_symbol.upper(), sleeve_key, field_name),
            ).fetchall()
            if _reconciliation_row_active(dict(row))
        )

    rows = [*_load_identity_rows("fund_name"), *_load_identity_rows("isin")]

    observed_names: set[str] = set()
    observed_isins: set[str] = set()
    invalid_name_count = 0
    invalid_isin_count = 0

    for row in rows:
        field_name = str(row["field_name"] or "").strip()
        try:
            decoded = json.loads(str(row["value_json"] or "null"))
        except Exception:
            decoded = str(row["value_json"] or "").strip()
        if field_name == "fund_name":
            normalized = _normalize_identity_name(decoded)
            if normalized:
                observed_names.add(normalized)
            else:
                invalid_name_count += 1
        elif field_name == "isin":
            normalized = _normalize_isin(decoded)
            if normalized:
                observed_isins.add(normalized)
            else:
                invalid_isin_count += 1

    resolved_name_item = _effective_truth_item(candidate, resolved_truth, "fund_name", benchmark_assignment=benchmark_assignment)
    resolved_isin_item = _effective_truth_item(candidate, resolved_truth, "isin", benchmark_assignment=benchmark_assignment)
    resolved_name = _normalize_identity_name(resolved_name_item.get("resolved_value")) or _normalize_identity_name(candidate.get("name"))
    resolved_isin = _normalize_isin(resolved_isin_item.get("resolved_value")) or _normalize_isin(dict(candidate.get("extra") or {}).get("isin"))
    issuer_doc_name_resolved = (
        str(resolved_name_item.get("source_name") or "").strip().lower() == "issuer_doc_parser"
        or str(resolved_name_item.get("provenance_level") or "").strip().lower() in {"issuer_doc", "verified_official"}
    )

    if resolved_name and not issuer_doc_name_resolved:
        observed_names.add(resolved_name)
    if resolved_isin:
        observed_isins.add(resolved_isin)
    trusted_resolved_isin = bool(
        resolved_isin
        and str(resolved_isin_item.get("source_name") or "").strip().lower() in {"issuer_doc_parser", "issuer_doc_registry"}
    )
    if trusted_resolved_isin:
        observed_isins = {resolved_isin}

    if len(observed_isins) > 1:
        return {
            "state": "conflict",
            "blocking": True,
            "summary": "Identity conflict: multiple ISIN values are attached to this candidate, so recommendation presentation stays blocked.",
            "resolved_name": resolved_name,
            "resolved_isin": resolved_isin,
            "name_observation_count": len(observed_names),
            "isin_observation_count": len(observed_isins),
        }
    if len(observed_names) > 1 and not issuer_doc_name_resolved:
        return {
            "state": "conflict",
            "blocking": True,
            "summary": "Identity conflict: fund name evidence disagrees across sources, so recommendation presentation stays blocked.",
            "resolved_name": resolved_name,
            "resolved_isin": resolved_isin,
            "name_observation_count": len(observed_names),
            "isin_observation_count": len(observed_isins),
        }
    rejected_record_count = invalid_name_count + invalid_isin_count
    if resolved_name or resolved_isin:
        if issuer_doc_name_resolved and resolved_isin:
            return {
                "state": "verified",
                "blocking": False,
                "summary": "Identity is verified from issuer-backed name evidence and a resolved ISIN.",
                "resolved_name": resolved_name,
                "resolved_isin": resolved_isin,
                "name_observation_count": 1,
                "isin_observation_count": len(observed_isins),
                "rejected_record_count": rejected_record_count,
            }
        if not (resolved_name and resolved_isin):
            return {
                "state": "thin",
                "blocking": False,
                "summary": "Identity is only partially supported. One of fund name or ISIN still needs stronger confirmation.",
                "resolved_name": resolved_name,
                "resolved_isin": resolved_isin,
                "name_observation_count": len(observed_names),
                "isin_observation_count": len(observed_isins),
                "rejected_record_count": rejected_record_count,
            }
        if invalid_name_count or invalid_isin_count:
            return {
                "state": "review",
                "blocking": False,
                "summary": "Identity is usable, but one or more lower-quality identity records were rejected as invalid.",
                "resolved_name": resolved_name,
                "resolved_isin": resolved_isin,
                "name_observation_count": len(observed_names),
                "isin_observation_count": len(observed_isins),
                "rejected_record_count": rejected_record_count,
            }
        return {
            "state": "verified",
            "blocking": False,
            "summary": "Fund identity is consistent across the current usable name and ISIN records.",
            "resolved_name": resolved_name,
            "resolved_isin": resolved_isin,
            "name_observation_count": len(observed_names),
            "isin_observation_count": len(observed_isins),
            "rejected_record_count": rejected_record_count,
        }
    return {
        "state": "missing",
        "blocking": True,
        "summary": "Identity support is still incomplete, so recommendation presentation stays blocked until name and ISIN support improve.",
        "resolved_name": resolved_name,
        "resolved_isin": resolved_isin,
        "name_observation_count": len(observed_names),
        "isin_observation_count": len(observed_isins),
        "rejected_record_count": rejected_record_count,
    }


def build_institutional_facts(
    candidate: dict[str, Any],
    *,
    resolved_truth: dict[str, dict[str, Any]],
    source_authority_map: list[dict[str, Any]],
    benchmark_assignment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    benchmark_full_name = _benchmark_full_name(
        candidate,
        resolved_truth,
        benchmark_assignment=benchmark_assignment,
    )
    expense_ratio = _safe_float(_pick_first(candidate, resolved_truth, "expense_ratio", "ter"))
    spread_proxy = _safe_float(_pick_first(candidate, resolved_truth, "bid_ask_spread_proxy"))
    aum_usd = _safe_float(_pick_first(candidate, resolved_truth, "aum", "extra.aum_usd", "aum_usd"))
    distribution_policy = _pick_first(candidate, resolved_truth, "distribution_type", "share_class", "accumulation")
    replication_method = _pick_first(candidate, resolved_truth, "replication_method")
    withholding_tax_posture = _pick_first(candidate, resolved_truth, "withholding_tax_posture", "tax_posture")
    estate_risk_posture = _pick_first(candidate, resolved_truth, "estate_risk_posture")
    developed_summary = _pick_first(candidate, resolved_truth, "developed_market_exposure_summary")
    emerging_summary = _pick_first(candidate, resolved_truth, "emerging_market_exposure_summary")

    exposure_parts = [
        str(part).strip()
        for part in [developed_summary, emerging_summary]
        if _meaningful(part)
    ]
    if not exposure_parts:
        fallback_asset_class = str(candidate.get("asset_class") or "").strip()
        if fallback_asset_class:
            exposure_parts = [fallback_asset_class.replace("_", " ").title()]
    exposure_summary = ". ".join(dict.fromkeys(exposure_parts)) if exposure_parts else None

    aum_authority = _authority_row(source_authority_map, "aum")
    aum_freshness = str(aum_authority.get("freshness_state") or "").strip()
    if aum_usd is None:
        aum_state = "missing"
    elif aum_freshness == "stale":
        aum_state = "stale"
    else:
        aum_state = "resolved"

    replication_risk_note = None
    replication_text = str(replication_method or "").strip().lower()
    if "swap" in replication_text or "synthetic" in replication_text:
        replication_risk_note = "Synthetic replication adds counterparty and roll dependence to the implementation read."
    elif "physical" in replication_text:
        replication_risk_note = "Physical replication keeps structure simpler, but benchmark tracking and trading cost still matter."

    tax_summary = "Current SG tax posture is not yet resolved."
    if _meaningful(withholding_tax_posture) or _meaningful(estate_risk_posture):
        tax_summary = " ".join(
            part
            for part in [
                str(withholding_tax_posture or "").strip(),
                str(estate_risk_posture or "").strip(),
            ]
            if part
        )

    return {
        "benchmark_full_name": str(benchmark_full_name).strip() if _meaningful(benchmark_full_name) else None,
        "exposure_summary": exposure_summary,
        "ter_bps": int(round(expense_ratio * 10_000)) if expense_ratio is not None else None,
        "spread_proxy_bps": spread_proxy,
        "aum_usd": aum_usd,
        "aum_state": aum_state,
        "sg_tax_posture": {
            "withholding_tax_posture": str(withholding_tax_posture).strip() if _meaningful(withholding_tax_posture) else None,
            "estate_risk_posture": str(estate_risk_posture).strip() if _meaningful(estate_risk_posture) else None,
            "summary": tax_summary,
        },
        "distribution_policy": str(distribution_policy).strip() if _meaningful(distribution_policy) else None,
        "replication_risk_note": replication_risk_note,
    }


def _authority_mix(source_authority_map: list[dict[str, Any]]) -> dict[str, int]:
    buckets = {
        "verified_current_truth": 0,
        "issuer_primary": 0,
        "issuer_secondary": 0,
        "provider_or_market_summary": 0,
        "registry_seed": 0,
        "missing": 0,
    }
    for item in source_authority_map:
        authority_class = str(item.get("authority_class") or "missing").strip()
        if authority_class not in buckets:
            authority_class = "verified_current_truth"
        buckets[authority_class] += 1
    return buckets


def _integrity_issue_counts(
    source_authority_map: list[dict[str, Any]],
    reconciliation_report: list[dict[str, Any]],
    *,
    document_gap_count: int,
) -> dict[str, int]:
    hard_conflicts = sum(1 for item in reconciliation_report if item.get("status") == "hard_conflict")
    soft_drifts = sum(1 for item in reconciliation_report if item.get("status") == "soft_drift")
    stale_fields = sum(1 for item in reconciliation_report if item.get("status") == "stale")
    missing_critical = sum(1 for item in reconciliation_report if item.get("status") == "critical_missing")
    weak_authority = sum(1 for item in reconciliation_report if item.get("status") == "weak_authority")
    execution_review_required = sum(1 for item in reconciliation_report if item.get("status") == "execution_review_required")
    review_items = hard_conflicts + soft_drifts + stale_fields + missing_critical + weak_authority + execution_review_required
    return {
        "hard_conflicts": hard_conflicts,
        "soft_drifts": soft_drifts,
        "stale_fields": stale_fields,
        "missing_critical_fields": missing_critical,
        "weak_authority_fields": weak_authority,
        "execution_review_required": execution_review_required,
        "document_gaps": int(document_gap_count or 0),
        "review_items": review_items,
    }


def _short_source_integrity_summary(
    *,
    state: str,
    critical_ready: int,
    critical_total: int,
    issue_counts: dict[str, int],
    identity_state: dict[str, Any],
) -> str:
    if state == "strong":
        return f"{critical_ready}/{critical_total} critical fields are resolved and the evidence base is clean."
    if state == "conflicted":
        return (
            f"{critical_ready}/{critical_total} critical fields are resolved, but conflicts or identity issues still block a clean recommendation read."
        )
    if state == "weak":
        return f"{critical_ready}/{critical_total} critical fields are resolved; support is still thin for recommendation use."
    if state == "missing":
        return "Critical support is still missing, so the candidate cannot carry a full recommendation read yet."
    issues: list[str] = []
    if issue_counts.get("missing_critical_fields"):
        issues.append(f"{issue_counts['missing_critical_fields']} critical field{'s' if issue_counts['missing_critical_fields'] != 1 else ''} missing")
    if issue_counts.get("stale_fields"):
        issues.append(f"{issue_counts['stale_fields']} stale")
    if issue_counts.get("weak_authority_fields"):
        issues.append(f"{issue_counts['weak_authority_fields']} weaker-source")
    if issue_counts.get("execution_review_required"):
        issues.append(f"{issue_counts['execution_review_required']} execution-review")
    if issue_counts.get("document_gaps"):
        issues.append(f"{issue_counts['document_gaps']} document gap{'s' if issue_counts['document_gaps'] != 1 else ''}")
    if str(identity_state.get("state") or "") in {"thin", "review"} and not identity_state.get("blocking"):
        issues.append("identity still needs review")
    detail = ", ".join(issues[:3]) if issues else "evidence remains mixed"
    return f"{critical_ready}/{critical_total} critical fields are resolved; {detail}."


def build_source_integrity_summary(
    *,
    source_authority_map: list[dict[str, Any]],
    data_quality_summary: dict[str, Any],
    reconciliation: dict[str, Any],
    reconciliation_report: list[dict[str, Any]],
    identity_state: dict[str, Any],
    source_completion_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if str(dict(source_completion_summary or {}).get("state") or "").strip() == "complete" and not identity_state.get("blocking") and reconciliation.get("status") != "hard_conflict":
        completed = int(dict(source_completion_summary or {}).get("critical_fields_completed") or len(source_authority_map))
        total = int(dict(source_completion_summary or {}).get("critical_fields_total") or len(source_authority_map))
        return {
            "state": "strong",
            "integrity_label": "clean",
            "summary": str(dict(source_completion_summary or {}).get("summary") or data_quality_summary.get("summary") or "").strip(),
            "critical_fields_ready": completed,
            "critical_fields_total": total,
            "issuer_backed_fields": int(data_quality_summary.get("issuer_backed_fields") or 0),
            "stale_or_missing_fields": 0,
            "weak_authority_fields": 0,
            "conflict_items": 0,
            "document_gap_count": 0,
            "authority_mix": _authority_mix(source_authority_map),
            "issue_counts": {
                "hard_conflicts": 0,
                "soft_drifts": 0,
                "stale_fields": 0,
                "missing_critical_fields": 0,
                "weak_authority_fields": 0,
                "execution_review_required": 0,
                "document_gaps": 0,
                "review_items": 0,
            },
            "hard_conflict_fields": [],
            "missing_critical_fields": [],
            "weakest_fields": [],
            "identity_state": str(identity_state.get("state") or "").strip() or None,
        }
    stale_or_missing = sum(1 for item in source_authority_map if item.get("freshness_state") in {"stale", "missing"})
    weak_authority = sum(
        1
        for item in source_authority_map
        if item.get("authority_class") in {"registry_seed", "provider_or_market_summary"}
    )
    document_gap_count = int(data_quality_summary.get("document_gap_count") or 0)
    issue_counts = _integrity_issue_counts(
        source_authority_map,
        reconciliation_report,
        document_gap_count=document_gap_count,
    )
    conflict_items = issue_counts["review_items"]
    if identity_state.get("blocking") or reconciliation.get("status") == "hard_conflict":
        state = "conflicted"
    elif str(data_quality_summary.get("data_confidence") or "") == "high" and not weak_authority and not stale_or_missing:
        state = "strong"
    elif str(data_quality_summary.get("data_confidence") or "") == "low":
        state = "weak"
    elif int(data_quality_summary.get("critical_fields_ready") or 0) == 0:
        state = "missing"
    else:
        state = "mixed"
    integrity_label = (
        "conflicted"
        if state == "conflicted"
        else "clean"
        if state == "strong"
        else "weak"
        if state == "weak"
        else "missing"
        if state == "missing"
        else "thin"
        if issue_counts["missing_critical_fields"] or issue_counts["weak_authority_fields"] or issue_counts["document_gaps"] or issue_counts["execution_review_required"]
        else "mixed"
    )
    summary = _short_source_integrity_summary(
        state=state,
        critical_ready=int(data_quality_summary.get("critical_fields_ready") or 0),
        critical_total=int(data_quality_summary.get("critical_fields_total") or 0),
        issue_counts=issue_counts,
        identity_state=identity_state,
    )
    return {
        "state": state,
        "integrity_label": integrity_label,
        "summary": summary,
        "critical_fields_ready": int(data_quality_summary.get("critical_fields_ready") or 0),
        "critical_fields_total": int(data_quality_summary.get("critical_fields_total") or 0),
        "issuer_backed_fields": int(data_quality_summary.get("issuer_backed_fields") or 0),
        "stale_or_missing_fields": stale_or_missing,
        "weak_authority_fields": weak_authority,
        "conflict_items": conflict_items,
        "document_gap_count": document_gap_count,
        "authority_mix": _authority_mix(source_authority_map),
        "issue_counts": issue_counts,
        "hard_conflict_fields": [
            str(item.get("field_name") or "")
            for item in reconciliation_report
            if item.get("status") == "hard_conflict"
        ],
        "missing_critical_fields": [
            str(item.get("field_name") or "")
            for item in reconciliation_report
            if item.get("status") == "critical_missing"
        ],
        "weakest_fields": [
            str(item.get("field_name") or "")
            for item in reconciliation_report
            if item.get("status") in {"hard_conflict", "critical_missing", "stale", "weak_authority", "execution_review_required"}
        ][:5],
        "identity_state": str(identity_state.get("state") or "").strip() or None,
    }
def build_investor_decision_state(
    *,
    gate: dict[str, Any],
    identity_state: dict[str, Any],
) -> str:
    if bool(identity_state.get("blocking")):
        return "blocked"
    gate_state = str(gate.get("gate_state") or "review_only")
    if gate_state == "blocked":
        return "blocked"
    if gate_state == "admissible":
        return "actionable"
    if gate_state == "review_only":
        return "shortlisted"
    return "research_only"


def build_blocker_category(
    *,
    gate: dict[str, Any],
    identity_state: dict[str, Any],
    reconciliation_report: list[dict[str, Any]] | None = None,
) -> str | None:
    if bool(identity_state.get("blocking")):
        return "identity"
    critical_missing = {str(field or "") for field in list(gate.get("critical_missing_fields") or [])}
    report = list(reconciliation_report or [])
    hard_conflicts = {str(item.get("field_name") or "") for item in report if item.get("status") == "hard_conflict"}
    weak_authority = {str(item.get("field_name") or "") for item in report if item.get("status") == "weak_authority"}
    execution_review = {str(item.get("field_name") or "") for item in report if item.get("status") == "execution_review_required"}
    stale_fields = {str(item.get("field_name") or "") for item in report if item.get("status") == "stale"}
    blocked_reasons = " ".join(str(reason or "") for reason in list(gate.get("blocked_reasons") or [])).lower()
    if not blocked_reasons and not critical_missing and not hard_conflicts and not weak_authority and not execution_review and not stale_fields:
        return None
    if any(field in critical_missing for field in {"domicile", "distribution_type"}):
        return "tax_wrapper"
    if any(field in critical_missing for field in _BENCHMARK_FIELDS) or any(field in hard_conflicts for field in _BENCHMARK_FIELDS):
        return "benchmark"
    if any(field in critical_missing for field in {"expense_ratio", "replication_method", "primary_listing_exchange", "primary_trading_currency", "liquidity_proxy", "bid_ask_spread_proxy", "aum"}):
        return "implementation"
    if execution_review or any(field in stale_fields for field in _EXECUTION_REVIEW_FIELDS):
        return "implementation"
    if any(field in weak_authority for field in _BENCHMARK_FIELDS):
        return "benchmark"
    if any(field in hard_conflicts for field in {"expense_ratio", "replication_method", "primary_listing_exchange", "primary_trading_currency", "liquidity_proxy", "bid_ask_spread_proxy", "aum"}):
        return "implementation"
    if "document support" in blocked_reasons or "weaker-authority" in blocked_reasons or "source" in blocked_reasons:
        return "evidence"
    if "conflict" in blocked_reasons:
        return "integrity"
    return "review"


def _identity_deployability_score(identity_state: dict[str, Any]) -> int:
    state = str(dict(identity_state or {}).get("state") or "").strip().lower()
    blocking = bool(dict(identity_state or {}).get("blocking"))
    if blocking:
        return 15
    if state == "verified":
        return 100
    if state == "review":
        return 75
    if state == "thin":
        return 40
    if state in {"conflict", "missing"}:
        return 15
    return 75


def _admissibility_deployability_score(gate: dict[str, Any]) -> int:
    gate_state = str(dict(gate or {}).get("gate_state") or "review_only").strip().lower()
    reasons = " ".join(str(reason or "") for reason in list(dict(gate or {}).get("blocked_reasons") or [])).strip()
    if gate_state == "admissible":
        return 100
    if gate_state == "blocked":
        return 20
    if reasons:
        return 45
    return 75


def _deployment_readiness_posture(
    *,
    source_integrity_score: int,
    implementation_score: int,
    admissibility_identity_score: int,
    benchmark_fidelity_score: int,
    market_path_support_score: int,
    instrument_quality_score: int,
    sleeve_fit_score: int,
    portfolio_fit_score: int,
    deployment_score: int,
    identity_score: int,
    admissibility_score: int,
) -> str:
    hard_block = bool(identity_score < 35 or admissibility_score < 40)
    if (
        not hard_block
        and source_integrity_score >= 82
        and implementation_score >= 72
        and admissibility_identity_score >= 75
        and benchmark_fidelity_score >= 78
        and market_path_support_score >= 70
        and instrument_quality_score >= 75
        and sleeve_fit_score >= 75
        and portfolio_fit_score >= 70
        and deployment_score >= 78
    ):
        return "action_ready"
    if (
        not hard_block
        and deployment_score >= 62
        and source_integrity_score >= 55
        and implementation_score >= 55
        and admissibility_identity_score >= 45
        and market_path_support_score >= 50
    ):
        return "reviewable"
    return "blocked"


def _deployment_readiness_summary(posture: str) -> str:
    if posture == "action_ready":
        return "Deployability is action-ready because admissibility, implementation, source integrity, and market-path support clear the release thresholds."
    if posture == "reviewable":
        return "Deployability remains reviewable while one or more readiness conditions still need final cleanup."
    return "Deployability stays blocked until hard conflicts or weak readiness conditions are resolved."


def _source_completion_score(source_completion_summary: dict[str, Any] | None) -> int:
    summary = dict(source_completion_summary or {})
    total = int(summary.get("critical_fields_total") or 0)
    completed = int(summary.get("critical_fields_completed") or 0)
    equivalent_ready = int(summary.get("equivalent_ready_count") or 0)
    if total <= 0:
        return 42
    completion_ratio = max(0.0, min(1.0, completed / max(1, total)))
    score = completion_ratio * 100.0
    if str(summary.get("state") or "").strip().lower() == "complete":
        score = max(score, 96.0)
    elif str(summary.get("state") or "").strip().lower() == "review":
        score = max(score, 18.0 + 70.0 * completion_ratio)
    score += min(4.0, float(equivalent_ready) * (4.0 / max(1, total)))
    return _clamp_score(score, minimum=18, maximum=100)


def _freshness_cleanliness_score(
    *,
    data_quality_summary: dict[str, Any] | None,
    source_authority_map: list[dict[str, Any]] | None,
    source_completion_summary: dict[str, Any] | None,
) -> int:
    summary = dict(data_quality_summary or {})
    total = int(summary.get("critical_fields_total") or len(_RECOMMENDATION_CRITICAL_FIELDS) or 0)
    stale_critical = int(summary.get("stale_critical_fields") or 0)
    completed_fields = _completed_source_fields(source_completion_summary)
    critical_rows = [
        dict(item)
        for item in list(source_authority_map or [])
        if str(item.get("field_name") or "").strip() in _RECOMMENDATION_CRITICAL_FIELDS
        and str(item.get("field_name") or "").strip() not in completed_fields
    ]
    if total <= 0:
        return 50
    current_count = sum(
        1
        for item in critical_rows
        if str(item.get("freshness_state") or "").strip().lower() in {"current", "fresh"}
    )
    freshness_ratio = current_count / max(1, total)
    stale_penalty = min(78.0, 24.0 * stale_critical)
    score = 0.60 * (freshness_ratio * 100.0) + 0.40 * (100.0 - stale_penalty)
    if stale_critical <= 0 and freshness_ratio >= 0.9:
        score = max(score, 94.0)
    return _clamp_score(score, minimum=18, maximum=100)


def _conflict_cleanliness_score(
    *,
    reconciliation_report: list[dict[str, Any]] | None,
    source_completion_summary: dict[str, Any] | None,
) -> int:
    completed_fields = _completed_source_fields(source_completion_summary)
    weights = {
        "hard_conflict": 34.0,
        "critical_missing": 22.0,
        "stale": 16.0,
        "weak_authority": 14.0,
        "soft_drift": 12.0,
        "execution_review_required": 10.0,
    }
    penalty = 0.0
    unresolved = 0
    for item in list(reconciliation_report or []):
        status = str(item.get("status") or "").strip().lower()
        field_name = str(item.get("field_name") or "").strip()
        if not status:
            continue
        if field_name and field_name in completed_fields and status not in {"hard_conflict"}:
            continue
        if status not in weights:
            continue
        penalty += weights[status]
        unresolved += 1
    score = 100.0 - penalty
    if unresolved == 0:
        score = max(score, 96.0)
    return _clamp_score(score, minimum=18, maximum=100)


def _truth_confidence_band(score: int | float) -> str:
    value = float(score or 0.0)
    if value >= 90:
        return "high_confidence"
    if value >= 75:
        return "good_confidence"
    if value >= 60:
        return "review_confidence"
    if value >= 45:
        return "low_confidence"
    return "unreliable"


def _truth_confidence_summary(score: int | float) -> str:
    band = _truth_confidence_band(score)
    if band == "high_confidence":
        return "Truth confidence is high because source integrity, completion, freshness, and routing are all staying clean."
    if band == "good_confidence":
        return "Truth confidence is good, but a few source, freshness, or routing details still keep the read from being fully clean."
    if band == "review_confidence":
        return "Truth confidence is only review-grade, so the recommendation should be read with bounded trust."
    if band == "low_confidence":
        return "Truth confidence is low because freshness, completeness, or routing evidence is still too weak."
    return "Truth confidence is unreliable, so the recommendation should not be promoted as a clean investor read."


def _deployability_badge(
    *,
    readiness_posture: str,
    deployability_score: int,
    truth_confidence_score: int,
    admissibility_score: int,
    identity_score: int,
) -> str:
    if readiness_posture == "action_ready" and deployability_score >= 78 and truth_confidence_score >= 75:
        return "deploy_now"
    if readiness_posture == "reviewable":
        return "review_before_deploy"
    if admissibility_score <= 20 or identity_score <= 15:
        return "blocked"
    if deployability_score >= 45 or truth_confidence_score >= 45:
        return "research_only"
    return "blocked"


def _apply_deployment_score_caps(
    raw_score: float,
    *,
    identity_score: int,
    admissibility_score: int,
    truth_confidence_score: int,
    deployability_score: int,
    source_completion_score: int,
    hard_benchmark_conflict: bool,
    missing_market_history_without_proxy: bool,
    missing_material_aum: bool,
    unresolved_stale_critical_field: bool,
) -> int:
    value = float(raw_score)
    if identity_score <= 15:
        value = min(value, 25.0)
    if admissibility_score <= 20:
        value = min(value, 35.0)
    if hard_benchmark_conflict:
        value = min(value, 45.0)
    if truth_confidence_score < 50:
        value = min(value, 59.0)
    if deployability_score < 55:
        value = min(value, 69.0)
    if missing_market_history_without_proxy:
        value = min(value, 55.0)
    if missing_material_aum:
        value = min(value, 74.0)
    if unresolved_stale_critical_field:
        value = min(value, 79.0)
    if source_completion_score < 85:
        value = min(value, 74.0)
    return _clamp_score(value, minimum=18, maximum=96)


def build_score_decomposition(
    *,
    candidate: dict[str, Any] | None = None,
    resolved_truth: dict[str, dict[str, Any]] | None = None,
    benchmark_assignment: dict[str, Any] | None = None,
    institutional_facts: dict[str, Any] | None = None,
    implementation_profile: dict[str, Any],
    gate: dict[str, Any],
    data_quality_summary: dict[str, Any],
    identity_state: dict[str, Any],
    source_authority_map: list[dict[str, Any]] | None = None,
    source_completion_summary: dict[str, Any] | None = None,
    reconciliation_report: list[dict[str, Any]] | None = None,
    blocker_category: str | None = None,
    sleeve_key: str | None = None,
    total_score: int | None = None,
) -> dict[str, Any]:
    candidate = dict(candidate or {})
    resolved_truth = dict(resolved_truth or {})
    benchmark_assignment = dict(benchmark_assignment or {})
    institutional_facts = dict(institutional_facts or {})
    authority_rows = {
        str(item.get("field_name") or ""): dict(item)
        for item in list(source_authority_map or [])
        if str(item.get("field_name") or "").strip()
    }
    reconciliation_index = _reconciliation_index(reconciliation_report)
    gate_state = str(gate.get("gate_state") or "review_only")
    gate_score = _admissibility_deployability_score(gate)
    identity_score = _identity_deployability_score(identity_state)

    symbol = str(candidate.get("symbol") or "").strip().upper()
    candidate_name = str(candidate.get("name") or symbol).strip() or symbol
    expense_ratio = _safe_float(_pick_first(candidate, resolved_truth, "expense_ratio", "ter"))
    tracking_difference = _normalize_tracking_difference_value(
        _pick_first(candidate, resolved_truth, "tracking_difference_1y", "tracking_difference_3y", "tracking_difference_5y")
    )
    aum_usd = _safe_float(_pick_first(candidate, resolved_truth, "aum", "extra.aum_usd", "aum_usd"))
    domicile = _pick_first(candidate, resolved_truth, "domicile")
    currency = _pick_first(candidate, resolved_truth, "primary_trading_currency", "base_currency")
    distribution = _pick_first(candidate, resolved_truth, "distribution_type", "share_class", "accumulation")
    issuer = _pick_first(candidate, resolved_truth, "issuer")
    replication_method = _pick_first(candidate, resolved_truth, "replication_method")
    launch_date = _pick_first(candidate, resolved_truth, "extra.launch_date", "launch_date", "inception_date")
    benchmark_key = _pick_first(candidate, resolved_truth, "benchmark_key") or benchmark_assignment.get("benchmark_key")
    benchmark_name = _benchmark_full_name(candidate, resolved_truth, benchmark_assignment=benchmark_assignment)
    exposure_summary = institutional_facts.get("exposure_summary")
    role_in_portfolio = (
        _pick_first(candidate, resolved_truth, "role_in_portfolio")
        or candidate.get("role_in_portfolio")
        or dict(candidate.get("investment_quality") or {}).get("role_in_portfolio")
        or dict(candidate.get("eligibility") or {}).get("role_in_portfolio")
    )
    default_symbol = str(DEFAULT_SLEEVE_ASSIGNMENTS.get(str(sleeve_key or "").strip()) or "").strip().upper()
    default_assignment = dict(DEFAULT_BENCHMARK_ASSIGNMENTS.get(default_symbol) or {})

    def _critical_field_score(field_name: str) -> int:
        return _field_quality(field_name=field_name, authority_rows=authority_rows, reconciliation_index=reconciliation_index)

    def _apply_component_caps(score: float, *, key: str, caps: list[str]) -> int:
        value = float(score)
        if bool(identity_state.get("blocking")):
            value = min(value, 35.0)
            caps.append("Hard identity conflict caps decision components at 35.")
        if gate_state == "blocked" and key in {"implementation", "source_integrity", "benchmark_fidelity", "sleeve_fit", "portfolio_fit"}:
            value = min(value, 40.0)
            caps.append("Hard recommendation conflict caps the component at 40.")
        if key == "source_integrity":
            if any(item.get("status") == "hard_conflict" for item in list(reconciliation_report or [])):
                value = min(value, 40.0)
                caps.append("Hard field conflict caps source integrity at 40.")
            weak_critical_count = sum(
                1
                for field_name in _RECOMMENDATION_CRITICAL_FIELDS
                if str(dict(authority_rows.get(field_name) or {}).get("authority_class") or "") in {"registry_seed", "provider_or_market_summary"}
            )
            if weak_critical_count > 2:
                value = min(value, 68.0)
                caps.append("More than two weak critical fields cap source integrity at 68.")
            if any(str(dict(authority_rows.get(field_name) or {}).get("freshness_state") or "") == "stale" for field_name in _RECOMMENDATION_CRITICAL_FIELDS):
                value = min(value, 72.0)
                caps.append("A stale critical field caps source integrity at 72.")
        if key == "implementation":
            route_state = str(implementation_profile.get("route_validity_state") or "").strip().lower()
            history_state = str(implementation_profile.get("history_depth_state") or "").strip().lower()
            if route_state == "missing_history" or (history_state == "missing" and not _safe_float(implementation_profile.get("execution_evidence_summary", {}).get("proxy_history_depth"))):
                value = min(value, 35.0)
                caps.append("Missing market history with no usable proxy caps implementation at 35.")
            if str(implementation_profile.get("quote_freshness_state") or "").strip().lower() in {"stale", "unknown"}:
                value = min(value, 72.0)
                caps.append("Stale quote freshness caps implementation at 72.")
            if str(implementation_profile.get("spread_support_state") or "").strip().lower() == "insufficient":
                value = min(value, 65.0)
                caps.append("Missing spread support caps implementation at 65.")
            if str(implementation_profile.get("liquidity_support_state") or "").strip().lower() == "insufficient":
                value = min(value, 65.0)
                caps.append("Missing liquidity depth caps implementation at 65.")
            if route_state == "proxy_ready" and str(sleeve_key or "").strip() not in {"real_assets", "alternatives"}:
                value = min(value, 78.0)
                caps.append("Proxy-only execution route caps implementation at 78.")
        if key == "benchmark_fidelity":
            benchmark_name_quality = _critical_field_score("benchmark_name")
            benchmark_key_quality = _critical_field_score("benchmark_key")
            benchmark_rows_present = bool(authority_rows.get("benchmark_key") or authority_rows.get("benchmark_name"))
            if not benchmark_rows_present or max(benchmark_name_quality, benchmark_key_quality) <= 0:
                value = min(value, 40.0)
                caps.append("Missing benchmark identity caps benchmark fidelity at 40.")
            if str(dict(reconciliation_index.get("benchmark_name") or {}).get("status") or "") == "hard_conflict":
                value = min(value, 30.0)
                caps.append("Hard benchmark conflict caps benchmark fidelity at 30.")
            if benchmark_assignment.get("benchmark_proxy_symbol") and benchmark_name_quality < 70:
                value = min(value, 72.0)
                caps.append("Proxy-only benchmark support caps benchmark fidelity at 72.")
            elif benchmark_key_quality >= 80 and benchmark_name_quality < 75:
                value = min(value, 82.0)
                caps.append("Document-light benchmark naming caps benchmark fidelity at 82.")
        if key == "sleeve_fit":
            if _sleeve_alignment_score(
                sleeve_key=sleeve_key,
                benchmark_key=str(benchmark_key or ""),
                benchmark_name=str(benchmark_name or ""),
                exposure_summary=str(exposure_summary or ""),
                role_in_portfolio=str(role_in_portfolio or ""),
                candidate_name=candidate_name,
            ) < 58:
                value = min(value, 55.0)
                caps.append("Benchmark or exposure mismatch caps sleeve fit at 55.")
            if blocker_category == "tax_wrapper":
                value = min(value, 60.0)
                caps.append("Tax or wrapper blocker caps sleeve fit at 60.")
            if str(implementation_profile.get("route_validity_state") or "").strip().lower() in {"missing_history", "invalid"}:
                value = min(value, 70.0)
                caps.append("Unsuitable execution route caps sleeve fit at 70.")
        if key == "long_horizon_quality":
            if aum_usd is None:
                value = min(value, 72.0)
                caps.append("Missing AUM caps long-horizon quality at 72.")
            if tracking_difference is None and str(benchmark_key or "").strip():
                value = min(value, 75.0)
                caps.append("Missing tracking evidence caps long-horizon quality at 75.")
            if not str(issuer or "").strip():
                value = min(value, 76.0)
                caps.append("Missing issuer durability caps long-horizon quality at 76.")
            if _structure_quality(replication_method, candidate_name) < 55 and _critical_field_score("replication_method") < 70:
                value = min(value, 68.0)
                caps.append("Complex structure without strong source support caps long-horizon quality at 68.")
        if key == "instrument_quality":
            if aum_usd is None:
                value = min(value, 68.0)
                caps.append("Missing AUM caps instrument quality at 68.")
            if _critical_field_score("issuer") < 70:
                value = min(value, 76.0)
                caps.append("Weak issuer support caps instrument quality at 76.")
            if tracking_difference is None and str(benchmark_key or "").strip():
                value = min(value, 78.0)
                caps.append("Missing tracking evidence caps instrument quality at 78.")
            if _critical_field_score("replication_method") < 65:
                value = min(value, 75.0)
                caps.append("Unknown structure or replication caps instrument quality at 75.")
        if key == "portfolio_fit":
            role_alignment = _sleeve_alignment_score(
                sleeve_key=sleeve_key,
                benchmark_key=str(benchmark_key or ""),
                benchmark_name=str(benchmark_name or ""),
                exposure_summary=str(exposure_summary or ""),
                role_in_portfolio=str(role_in_portfolio or ""),
                candidate_name=candidate_name,
            )
            if role_alignment < 58:
                value = min(value, 55.0)
                caps.append("Candidate is not cleanly in sleeve role, capping portfolio fit at 55.")
            if symbol and default_symbol and symbol != default_symbol:
                worse_vs_leader = 0
                if expense_ratio is not None and expense_ratio > _safe_float(default_assignment.get("expense_ratio") or 0) and default_assignment.get("expense_ratio") is not None:
                    worse_vs_leader += 1
                if role_alignment < 70:
                    worse_vs_leader += 1
                if str(benchmark_key or "") != str(default_assignment.get("benchmark_key") or "") and role_alignment < 72:
                    worse_vs_leader += 1
                if worse_vs_leader >= 2:
                    value = min(value, 62.0)
                    caps.append("Weaker substitution quality versus the sleeve leader caps portfolio fit at 62.")
            if gate_state == "blocked":
                value = min(value, 35.0)
                caps.append("Blocked recommendation posture caps portfolio fit at 35.")
            if str(data_quality_summary.get("data_confidence") or "").strip().lower() == "low":
                value = min(value, 70.0)
                caps.append("Weak source confidence caps portfolio fit at 70.")
            if not str(role_in_portfolio or "").strip() and not default_symbol:
                value = min(value, 74.0)
                caps.append("Missing portfolio-role context caps portfolio fit at 74.")
        return _clamp_score(value, minimum=18, maximum=96)

    implementation_fields = ["bid_ask_spread_proxy", "liquidity_proxy", "primary_listing_exchange", "primary_trading_currency", "aum"]
    implementation_missing, implementation_weak, implementation_stale, implementation_conflict = _component_field_lists(
        fields=implementation_fields,
        authority_rows=authority_rows,
        reconciliation_index=reconciliation_index,
    )
    spread_numeric = _safe_float(_pick_first(candidate, resolved_truth, "bid_ask_spread_proxy"))
    spread_quality = (
        _score_from_threshold(spread_numeric, cuts=[(2.0, 96), (5.0, 88), (10.0, 76), (20.0, 62), (999.0, 44)], reverse=True, default=58)
        if spread_numeric is not None
        else _score_from_states(
            str(implementation_profile.get("spread_support_state") or ""),
            {"usable": 76, "degraded": 58, "insufficient": 32},
            52,
        )
    )
    liquidity_depth = _weighted_average(
        [
            (
                float(
                    _score_from_states(
                        str(implementation_profile.get("liquidity_support_state") or ""),
                        {"strong": 94, "usable": 82, "degraded": 62, "insufficient": 28},
                        50,
                    )
                ),
                0.55,
            ),
            (
                float(
                    _score_from_states(
                        str(implementation_profile.get("volume_support_state") or ""),
                        {"usable": 82, "degraded": 62, "insufficient": 30},
                        52,
                    )
                ),
                0.25,
            ),
            (
                float(
                    _score_from_states(
                        str(implementation_profile.get("history_depth_state") or ""),
                        {"strong": 92, "usable": 78, "thin": 56, "missing": 18},
                        50,
                    )
                ),
                0.20,
            ),
        ]
    )
    quote_freshness = float(
        _score_from_states(
            str(implementation_profile.get("quote_freshness_state") or ""),
            {"fresh": 96, "aging": 74, "stale": 36, "unknown": 28},
            55,
        )
    )
    execution_route = float(
        _score_from_states(
            str(implementation_profile.get("route_validity_state") or ""),
            {
                "direct_ready": 94,
                "proxy_ready": 78,
                "alias_review_needed": 60,
                "benchmark_lineage_weak": 46,
                "missing_history": 18,
                "invalid": 12,
                "unknown": 52,
            },
            50,
        )
    )
    completeness_hits = 5 - sum(
        1
        for state in (
            str(implementation_profile.get("spread_support_state") or "").strip().lower(),
            str(implementation_profile.get("liquidity_support_state") or "").strip().lower(),
            str(implementation_profile.get("volume_support_state") or "").strip().lower(),
            str(implementation_profile.get("history_depth_state") or "").strip().lower(),
            str(implementation_profile.get("quote_freshness_state") or "").strip().lower(),
        )
        if state in {"insufficient", "missing", "unknown", "stale"}
    )
    trading_completeness = float(_clamp_score(42 + completeness_hits * 11))
    implementation_caps: list[str] = []
    implementation_score = _apply_component_caps(
        0.30 * spread_quality
        + 0.25 * liquidity_depth
        + 0.20 * quote_freshness
        + 0.15 * execution_route
        + 0.10 * trading_completeness,
        key="implementation",
        caps=implementation_caps,
    )
    implementation_reasons = [
        (
            "Execution route is direct-ready with usable quote freshness and history depth."
            if execution_route >= 90 and quote_freshness >= 74 and liquidity_depth >= 74
            else "Execution remains usable, but route freshness, spread, or liquidity still keep it bounded."
            if implementation_score >= 65
            else "Execution readiness is too weak because route quality, spread support, or liquidity depth are still unreliable."
        ),
        f"Spread quality reads {int(round(spread_quality))}/100 and liquidity depth reads {int(round(liquidity_depth))}/100.",
    ]
    implementation_component = _make_component(
        key="implementation",
        score=implementation_score,
        reasons=implementation_reasons,
        caps_applied=implementation_caps,
        field_drivers=["bid_ask_spread_proxy", "liquidity_proxy", "primary_listing_exchange", "primary_trading_currency"],
        missing_fields=implementation_missing,
        weak_fields=implementation_weak,
        stale_fields=implementation_stale,
        conflict_fields=implementation_conflict,
        confidence=_component_confidence(
            field_scores=[_critical_field_score(field) for field in implementation_fields],
            missing_fields=implementation_missing,
            weak_fields=implementation_weak,
            stale_fields=implementation_stale,
            conflict_fields=implementation_conflict,
            caps_applied=implementation_caps,
        ),
    )

    critical_weights = {
        "expense_ratio": 1.0,
        "benchmark_key": 1.1,
        "benchmark_name": 1.1,
        "replication_method": 0.8,
        "primary_listing_exchange": 0.8,
        "primary_trading_currency": 0.6,
        "liquidity_proxy": 0.7,
        "bid_ask_spread_proxy": 0.7,
        "aum": 0.9,
        "domicile": 0.9,
        "distribution_type": 0.6,
        "issuer": 1.0,
        "launch_date": 0.4,
        "tracking_difference_1y": 0.7,
    }
    source_integrity_fields = list(critical_weights.keys())
    source_missing, source_weak, source_stale, source_conflicts = _component_field_lists(
        fields=source_integrity_fields,
        authority_rows=authority_rows,
        reconciliation_index=reconciliation_index,
    )
    weighted_critical_quality = _weighted_average(
        [(_critical_field_score(field_name), weight) for field_name, weight in critical_weights.items()]
    )
    critical_freshness = _weighted_average(
        [(_freshness_score_for_row(field_name, dict(authority_rows.get(field_name) or {})), 1.0) for field_name in critical_weights]
    )
    conflict_cleanliness = _clamp_score(
        100
        - 42 * sum(1 for item in list(reconciliation_report or []) if item.get("status") == "hard_conflict")
        - 22 * sum(1 for item in list(reconciliation_report or []) if item.get("status") == "critical_missing")
        - 10 * sum(1 for item in list(reconciliation_report or []) if item.get("status") == "soft_drift")
        - 8 * sum(1 for item in list(reconciliation_report or []) if item.get("status") == "stale")
        - 6 * sum(1 for item in list(reconciliation_report or []) if item.get("status") == "weak_authority")
    )
    document_gap_count = int(data_quality_summary.get("document_gap_count") or 0)
    material_document_support = _clamp_score(100 - min(70, document_gap_count * 12))
    refresh_honesty = _score_from_states(
        str(data_quality_summary.get("data_confidence") or ""),
        {"high": 92, "mixed": 70, "low": 42},
        60,
    )
    source_integrity_caps: list[str] = []
    source_integrity_score = _apply_component_caps(
        0.55 * weighted_critical_quality
        + 0.15 * critical_freshness
        + 0.15 * conflict_cleanliness
        + 0.10 * material_document_support
        + 0.05 * refresh_honesty,
        key="source_integrity",
        caps=source_integrity_caps,
    )
    source_integrity_reasons = [
        (
            "Critical truth is explicit and current across the main evidence stack."
            if source_integrity_score >= 82
            else "Critical truth is usable, but freshness, authority, or document support still keep integrity below decision-grade."
            if source_integrity_score >= 60
            else "Critical truth is too weak because conflicts, missing fields, or stale authority still dominate the evidence stack."
        ),
        f"Weighted critical field quality is {int(round(weighted_critical_quality))}/100 with conflict cleanliness at {int(round(conflict_cleanliness))}/100.",
    ]
    source_integrity_component = _make_component(
        key="source_integrity",
        score=source_integrity_score,
        reasons=source_integrity_reasons,
        caps_applied=source_integrity_caps,
        field_drivers=source_integrity_fields,
        missing_fields=source_missing,
        weak_fields=source_weak,
        stale_fields=source_stale,
        conflict_fields=source_conflicts,
        confidence=_component_confidence(
            field_scores=[_critical_field_score(field) for field in source_integrity_fields],
            missing_fields=source_missing,
            weak_fields=source_weak,
            stale_fields=source_stale,
            conflict_fields=source_conflicts,
            caps_applied=source_integrity_caps,
        ),
    )

    benchmark_fields = ["benchmark_key", "benchmark_name", "tracking_difference_1y"]
    benchmark_missing, benchmark_weak, benchmark_stale, benchmark_conflicts = _component_field_lists(
        fields=benchmark_fields,
        authority_rows=authority_rows,
        reconciliation_index=reconciliation_index,
    )
    benchmark_identity_explicitness = _weighted_average(
        [
            (_critical_field_score("benchmark_key"), 0.55),
            (_critical_field_score("benchmark_name"), 0.45),
        ]
    )
    exposure_alignment = _sleeve_alignment_score(
        sleeve_key=sleeve_key,
        benchmark_key=str(benchmark_key or ""),
        benchmark_name=str(benchmark_name or ""),
        exposure_summary=str(exposure_summary or ""),
        role_in_portfolio=str(role_in_portfolio or ""),
        candidate_name=candidate_name,
    )
    tracking_quality = _weighted_average(
        [
            (_tracking_quality(tracking_difference, default=54), 0.65),
            (_critical_field_score("tracking_difference_1y"), 0.35),
        ]
    )
    benchmark_document_support = _weighted_average(
        [
            (100.0 if str(dict(authority_rows.get("benchmark_name") or {}).get("document_support_state") or "") == "backed" else 62.0, 0.6),
            (100.0 if str(dict(authority_rows.get("benchmark_key") or {}).get("document_support_state") or "") in {"backed", "derived_mapping"} else 60.0, 0.4),
        ]
    )
    lineage_consistency = _clamp_score(
        100
        - 40 * int(str(dict(reconciliation_index.get("benchmark_name") or {}).get("status") or "") == "hard_conflict")
        - 18 * int(str(dict(reconciliation_index.get("benchmark_name") or {}).get("status") or "") == "soft_drift")
        - 14 * int(str(dict(reconciliation_index.get("benchmark_name") or {}).get("status") or "") == "critical_missing")
    )
    proxy_quality = (
        84
        if benchmark_assignment.get("benchmark_proxy_symbol") and str(benchmark_assignment.get("benchmark_confidence") or "").strip().lower() == "high"
        else 74
        if benchmark_assignment.get("benchmark_proxy_symbol")
        else 92
        if benchmark_key
        else 42
    )
    benchmark_caps: list[str] = []
    benchmark_fidelity_score = _apply_component_caps(
        0.25 * benchmark_identity_explicitness
        + 0.25 * exposure_alignment
        + 0.20 * tracking_quality
        + 0.15 * benchmark_document_support
        + 0.10 * lineage_consistency
        + 0.05 * proxy_quality,
        key="benchmark_fidelity",
        caps=benchmark_caps,
    )
    benchmark_component = _make_component(
        key="benchmark_fidelity",
        score=benchmark_fidelity_score,
        reasons=[
            (
                "Benchmark identity and exposure alignment are explicit enough to support the sleeve cleanly."
                if benchmark_fidelity_score >= 82
                else "Benchmark lineage is usable, but naming explicitness, tracking evidence, or proxy reliance still bound confidence."
                if benchmark_fidelity_score >= 60
                else "Benchmark fidelity is too weak because identity, tracking, or exposure alignment still lack explicit support."
            ),
            f"Benchmark explicitness is {int(round(benchmark_identity_explicitness))}/100 and exposure alignment is {int(round(exposure_alignment))}/100.",
        ],
        caps_applied=benchmark_caps,
        field_drivers=benchmark_fields + ["role_in_portfolio"],
        missing_fields=benchmark_missing,
        weak_fields=benchmark_weak,
        stale_fields=benchmark_stale,
        conflict_fields=benchmark_conflicts,
        confidence=_component_confidence(
            field_scores=[_critical_field_score(field) for field in benchmark_fields],
            missing_fields=benchmark_missing,
            weak_fields=benchmark_weak,
            stale_fields=benchmark_stale,
            conflict_fields=benchmark_conflicts,
            caps_applied=benchmark_caps,
        ),
    )

    sleeve_fit_fields = ["benchmark_key", "benchmark_name", "domicile", "primary_trading_currency", "distribution_type"]
    sleeve_missing, sleeve_weak, sleeve_stale, sleeve_conflicts = _component_field_lists(
        fields=sleeve_fit_fields,
        authority_rows=authority_rows,
        reconciliation_index=reconciliation_index,
    )
    sleeve_role_alignment = exposure_alignment
    wrapper_currency_tax_fit = _constraint_fit_score(domicile=domicile, currency=currency, distribution=distribution)
    implementation_suitability = implementation_score
    risk_profile_fit = _structure_quality(replication_method, candidate_name)
    concentration_fit = 86 if str(sleeve_key or "").strip() not in {"convex", "alternatives"} else 72
    sleeve_fit_caps: list[str] = []
    sleeve_fit_score = _apply_component_caps(
        0.35 * sleeve_role_alignment
        + 0.20 * exposure_alignment
        + 0.15 * wrapper_currency_tax_fit
        + 0.15 * implementation_suitability
        + 0.10 * risk_profile_fit
        + 0.05 * concentration_fit,
        key="sleeve_fit",
        caps=sleeve_fit_caps,
    )
    sleeve_fit_component = _make_component(
        key="sleeve_fit",
        score=sleeve_fit_score,
        reasons=[
            (
                "The candidate fits the sleeve job cleanly on exposure, wrapper, and implementation."
                if sleeve_fit_score >= 82
                else "The candidate can still do the sleeve job, but wrapper, benchmark, or execution fit still need review."
                if sleeve_fit_score >= 60
                else "The candidate does not yet fit the sleeve cleanly enough on exposure, wrapper, or implementation."
            ),
            f"Sleeve role alignment is {int(round(sleeve_role_alignment))}/100 with wrapper and currency fit at {int(round(wrapper_currency_tax_fit))}/100.",
        ],
        caps_applied=sleeve_fit_caps,
        field_drivers=sleeve_fit_fields + ["role_in_portfolio"],
        missing_fields=sleeve_missing,
        weak_fields=sleeve_weak,
        stale_fields=sleeve_stale,
        conflict_fields=sleeve_conflicts,
        confidence=_component_confidence(
            field_scores=[_critical_field_score(field) for field in sleeve_fit_fields],
            missing_fields=sleeve_missing,
            weak_fields=sleeve_weak,
            stale_fields=sleeve_stale,
            conflict_fields=sleeve_conflicts,
            caps_applied=sleeve_fit_caps,
        ),
    )

    long_horizon_fields = ["expense_ratio", "tracking_difference_1y", "aum", "issuer", "replication_method", "domicile", "distribution_type", "launch_date"]
    long_missing, long_weak, long_stale, long_conflicts = _component_field_lists(
        fields=long_horizon_fields,
        authority_rows=authority_rows,
        reconciliation_index=reconciliation_index,
    )
    cost_drag_quality = _expense_ratio_quality(sleeve_key, expense_ratio)
    tracking_reliability = _weighted_average(
        [(_tracking_quality(tracking_difference, default=56), 0.6), (_critical_field_score("tracking_difference_1y"), 0.4)]
    )
    source_durability = _weighted_average(
        [
            (_critical_field_score("expense_ratio"), 0.2),
            (_critical_field_score("benchmark_name"), 0.2),
            (_critical_field_score("issuer"), 0.2),
            (_critical_field_score("domicile"), 0.2),
            (_critical_field_score("aum"), 0.2),
        ]
    )
    structure_durability = _weighted_average(
        [
            (_structure_quality(replication_method, candidate_name), 0.45),
            (_constraint_fit_score(domicile=domicile, currency=currency, distribution=distribution), 0.55),
        ]
    )
    liquidity_durability = _weighted_average(
        [
            (_aum_quality(aum_usd), 0.5),
            (liquidity_depth, 0.5),
        ]
    )
    issuer_durability = _issuer_quality_score(str(issuer or ""), _critical_field_score("issuer"))
    simplicity = _weighted_average(
        [
            (_structure_quality(replication_method, candidate_name), 0.7),
            (_launch_date_quality(launch_date), 0.3),
        ]
    )
    long_horizon_caps: list[str] = []
    long_horizon_quality_score = _apply_component_caps(
        0.20 * cost_drag_quality
        + 0.20 * tracking_reliability
        + 0.15 * source_durability
        + 0.15 * structure_durability
        + 0.10 * liquidity_durability
        + 0.10 * issuer_durability
        + 0.10 * simplicity,
        key="long_horizon_quality",
        caps=long_horizon_caps,
    )
    long_horizon_component = _make_component(
        key="long_horizon_quality",
        score=long_horizon_quality_score,
        reasons=[
            (
                "Long-horizon holding quality is supported by cost, durability, and cleaner structure."
                if long_horizon_quality_score >= 82
                else "Long-horizon quality is usable, but cost drag, tracking durability, or structure still keep it bounded."
                if long_horizon_quality_score >= 60
                else "Long-horizon quality remains too weak because cost, durability, or structure evidence is still too thin."
            ),
            f"Cost drag quality is {int(round(cost_drag_quality))}/100 and tracking reliability is {int(round(tracking_reliability))}/100.",
        ],
        caps_applied=long_horizon_caps,
        field_drivers=long_horizon_fields,
        missing_fields=long_missing,
        weak_fields=long_weak,
        stale_fields=long_stale,
        conflict_fields=long_conflicts,
        confidence=_component_confidence(
            field_scores=[_critical_field_score(field) for field in long_horizon_fields],
            missing_fields=long_missing,
            weak_fields=long_weak,
            stale_fields=long_stale,
            conflict_fields=long_conflicts,
            caps_applied=long_horizon_caps,
        ),
    )

    instrument_fields = ["expense_ratio", "tracking_difference_1y", "aum", "issuer", "replication_method", "domicile", "distribution_type", "launch_date"]
    instrument_missing, instrument_weak, instrument_stale, instrument_conflicts = _component_field_lists(
        fields=instrument_fields,
        authority_rows=authority_rows,
        reconciliation_index=reconciliation_index,
    )
    cost_efficiency = cost_drag_quality
    instrument_tracking_quality = tracking_reliability
    fund_scale = _aum_quality(aum_usd)
    transparency = _weighted_average(
        [
            (_structure_quality(replication_method, candidate_name), 0.5),
            (100.0 if _primary_document_manifest(candidate) else 56.0, 0.5),
        ]
    )
    issuer_quality = issuer_durability
    domicile_quality = _constraint_fit_score(domicile=domicile, currency=currency, distribution=distribution)
    age_stability = _launch_date_quality(launch_date)
    instrument_caps: list[str] = []
    instrument_quality_score = _apply_component_caps(
        0.25 * cost_efficiency
        + 0.20 * instrument_tracking_quality
        + 0.15 * fund_scale
        + 0.15 * transparency
        + 0.10 * issuer_quality
        + 0.10 * domicile_quality
        + 0.05 * age_stability,
        key="instrument_quality",
        caps=instrument_caps,
    )
    instrument_quality_component = _make_component(
        key="instrument_quality",
        score=instrument_quality_score,
        reasons=[
            (
                "Instrument quality is strong on cost, scale, and structure."
                if instrument_quality_score >= 82
                else "Instrument quality is usable, but scale, tracking, or structure evidence still need cleanup."
                if instrument_quality_score >= 60
                else "Instrument quality remains too weak because fund scale, tracking, or structure support is still not strong enough."
            ),
            f"Cost efficiency is {int(round(cost_efficiency))}/100 and fund scale is {int(round(fund_scale))}/100.",
        ],
        caps_applied=instrument_caps,
        field_drivers=instrument_fields,
        missing_fields=instrument_missing,
        weak_fields=instrument_weak,
        stale_fields=instrument_stale,
        conflict_fields=instrument_conflicts,
        confidence=_component_confidence(
            field_scores=[_critical_field_score(field) for field in instrument_fields],
            missing_fields=instrument_missing,
            weak_fields=instrument_weak,
            stale_fields=instrument_stale,
            conflict_fields=instrument_conflicts,
            caps_applied=instrument_caps,
        ),
    )

    portfolio_fields = ["benchmark_key", "benchmark_name", "primary_trading_currency", "domicile", "distribution_type", "aum"]
    portfolio_missing, portfolio_weak, portfolio_stale, portfolio_conflicts = _component_field_lists(
        fields=portfolio_fields,
        authority_rows=authority_rows,
        reconciliation_index=reconciliation_index,
    )
    portfolio_context_missing = not str(role_in_portfolio or "").strip() and not default_symbol
    marginal_role = sleeve_role_alignment
    overlap_diversification = 88 if symbol == default_symbol else 78 if str(benchmark_key or "") == str(default_assignment.get("benchmark_key") or "") else 66
    risk_budget_fit = _weighted_average([(risk_profile_fit, 0.5), (implementation_score, 0.5)])
    substitution_quality = 92 if symbol == default_symbol else 78 if str(benchmark_key or "") == str(default_assignment.get("benchmark_key") or "") else 62
    liquidity_at_target = _weighted_average([(liquidity_depth, 0.6), (_aum_quality(aum_usd), 0.4)])
    investor_constraints_fit = _constraint_fit_score(domicile=domicile, currency=currency, distribution=distribution)
    portfolio_caps: list[str] = []
    portfolio_fit_score = _apply_component_caps(
        0.30 * marginal_role
        + 0.20 * overlap_diversification
        + 0.15 * risk_budget_fit
        + 0.15 * substitution_quality
        + 0.10 * liquidity_at_target
        + 0.10 * investor_constraints_fit,
        key="portfolio_fit",
        caps=portfolio_caps,
    )
    portfolio_confidence = _component_confidence(
        field_scores=[_critical_field_score(field) for field in portfolio_fields],
        missing_fields=portfolio_missing,
        weak_fields=portfolio_weak,
        stale_fields=portfolio_stale,
        conflict_fields=portfolio_conflicts,
        caps_applied=portfolio_caps,
    )
    if portfolio_context_missing:
        portfolio_confidence = min(portfolio_confidence, 46)
    portfolio_fit_component = _make_component(
        key="portfolio_fit",
        score=portfolio_fit_score,
        reasons=[
            (
                "Portfolio fit is strong because the candidate adds clean sleeve-role value at usable size."
                if portfolio_fit_score >= 80
                else "Portfolio fit is still usable, but role clarity, substitution quality, or liquidity at target weight stay bounded."
                if portfolio_fit_score >= 60
                else "Portfolio fit is too weak because the candidate does not yet add a clean marginal role inside the sleeve."
            ),
            f"Marginal role contribution is {int(round(marginal_role))}/100 and substitution quality is {int(round(substitution_quality))}/100.",
        ],
        caps_applied=portfolio_caps,
        field_drivers=portfolio_fields + ["role_in_portfolio"],
        missing_fields=portfolio_missing,
        weak_fields=portfolio_weak,
        stale_fields=portfolio_stale,
        conflict_fields=portfolio_conflicts,
        confidence=portfolio_confidence,
    )

    components = [
        implementation_component,
        source_integrity_component,
        benchmark_component,
        sleeve_fit_component,
        long_horizon_component,
        instrument_quality_component,
        portfolio_fit_component,
    ]
    admissibility_identity_score = _clamp_score(min(identity_score, gate_score), minimum=15, maximum=100)
    recommendation_merit_score = _clamp_score(
        _weighted_average(
            [
                (sleeve_fit_component["score"], 0.30),
                (instrument_quality_component["score"], 0.25),
                (long_horizon_component["score"], 0.20),
                (benchmark_component["score"], 0.15),
                (portfolio_fit_component["score"], 0.10),
            ]
        ),
        minimum=18,
        maximum=100,
    )
    provisional_market_path_support_score = 50
    deployability_score = _clamp_score(
        _weighted_average(
            [
                (source_integrity_component["score"], 0.35),
                (implementation_component["score"], 0.30),
                (provisional_market_path_support_score, 0.20),
                (admissibility_identity_score, 0.15),
            ]
        ),
        minimum=18,
        maximum=100,
    )
    source_completion_score = _source_completion_score(source_completion_summary)
    freshness_cleanliness_score = _freshness_cleanliness_score(
        data_quality_summary=data_quality_summary,
        source_authority_map=source_authority_map,
        source_completion_summary=source_completion_summary,
    )
    conflict_cleanliness_score = _conflict_cleanliness_score(
        reconciliation_report=reconciliation_report,
        source_completion_summary=source_completion_summary,
    )
    truth_confidence_score = _clamp_score(
        _weighted_average(
            [
                (source_integrity_component["score"], 0.35),
                (source_completion_score, 0.20),
                (freshness_cleanliness_score, 0.15),
                (conflict_cleanliness_score, 0.15),
                (provisional_market_path_support_score, 0.15),
            ]
        ),
        minimum=18,
        maximum=100,
    )
    raw_recommendation_score = _weighted_average(
        [
            (recommendation_merit_score, 0.80),
            (deployability_score, 0.20),
        ]
    )
    hard_benchmark_conflict = any(
        item.get("status") == "hard_conflict"
        and str(item.get("field_name") or "").strip() in _BENCHMARK_FIELDS
        for item in list(reconciliation_report or [])
    )
    proxy_history_depth = _safe_float(dict(implementation_profile.get("execution_evidence_summary") or {}).get("proxy_history_depth"))
    route_state = str(implementation_profile.get("route_validity_state") or "").strip().lower()
    history_state = str(implementation_profile.get("history_depth_state") or "").strip().lower()
    missing_market_history_without_proxy = route_state == "missing_history" or (
        history_state == "missing" and proxy_history_depth <= 0.0
    )
    missing_material_aum = aum_usd is None or float(aum_usd) <= 0.0
    unresolved_stale_critical_field = int(dict(data_quality_summary or {}).get("stale_critical_fields") or 0) > 0
    recommendation_score = _apply_deployment_score_caps(
        raw_recommendation_score,
        identity_score=identity_score,
        admissibility_score=gate_score,
        truth_confidence_score=truth_confidence_score,
        deployability_score=int(deployability_score),
        source_completion_score=source_completion_score,
        hard_benchmark_conflict=hard_benchmark_conflict,
        missing_market_history_without_proxy=missing_market_history_without_proxy,
        missing_material_aum=missing_material_aum,
        unresolved_stale_critical_field=unresolved_stale_critical_field,
    )
    if total_score is None:
        total_score = recommendation_score
    else:
        recommendation_score = _clamp_score(total_score, minimum=18, maximum=96)
    confidence_penalty = _clamp_score(
        (
            100
            - _weighted_average([(float(component["confidence"]), 1.0) for component in components])
            + 2.0 * sum(len(list(component.get("caps_applied") or [])) for component in components)
        )
        / 3.5,
        minimum=0,
        maximum=28,
    )
    readiness_posture = _deployment_readiness_posture(
        source_integrity_score=source_integrity_component["score"],
        implementation_score=implementation_component["score"],
        admissibility_identity_score=admissibility_identity_score,
        benchmark_fidelity_score=benchmark_component["score"],
        market_path_support_score=provisional_market_path_support_score,
        instrument_quality_score=instrument_quality_component["score"],
        sleeve_fit_score=sleeve_fit_component["score"],
        portfolio_fit_score=portfolio_fit_component["score"],
        deployment_score=int(deployability_score),
        identity_score=identity_score,
        admissibility_score=gate_score,
    )
    deployability_badge = _deployability_badge(
        readiness_posture=readiness_posture,
        deployability_score=int(deployability_score),
        truth_confidence_score=int(truth_confidence_score),
        admissibility_score=gate_score,
        identity_score=identity_score,
    )
    return {
        "total_score": int(recommendation_score),
        "score_model_version": "recommendation_score_v3",
        "admissibility_score": gate_score,
        "admissibility_identity_score": admissibility_identity_score,
        "implementation_score": implementation_component["score"],
        "source_integrity_score": source_integrity_component["score"],
        "evidence_score": source_integrity_component["score"],
        "sleeve_fit_score": sleeve_fit_component["score"],
        "identity_score": identity_score,
        "benchmark_fidelity_score": benchmark_component["score"],
        "long_horizon_quality_score": long_horizon_component["score"],
        "instrument_quality_score": instrument_quality_component["score"],
        "portfolio_fit_score": portfolio_fit_component["score"],
        "recommendation_merit_score": int(recommendation_merit_score),
        "investment_merit_score": int(recommendation_merit_score),
        "deployability_score": int(deployability_score),
        "truth_confidence_score": int(truth_confidence_score),
        "source_completion_score": int(source_completion_score),
        "freshness_cleanliness_score": int(freshness_cleanliness_score),
        "conflict_cleanliness_score": int(conflict_cleanliness_score),
        "truth_confidence_band": _truth_confidence_band(truth_confidence_score),
        "truth_confidence_summary": _truth_confidence_summary(truth_confidence_score),
        "recommendation_score": int(recommendation_score),
        "optimality_score": int(recommendation_merit_score),
        "readiness_score": int(deployability_score),
        "deployment_score": int(deployability_score),
        "confidence_penalty": int(confidence_penalty),
        "readiness_posture": readiness_posture,
        "readiness_summary": _deployment_readiness_summary(readiness_posture),
        "deployability_badge": deployability_badge,
        "hard_benchmark_conflict": hard_benchmark_conflict,
        "summary": (
            "Recommendation score weights recommendation merit 80% and deployability 20%, while truth confidence controls how strongly the read can be trusted and promoted."
            if int(recommendation_score) >= 72
            else "Recommendation score remains capped because truth confidence, deployability, or hard routing and evidence controls are still constraining the read."
        ),
        "components": components,
    }


def market_path_support_component(market_path_support: dict[str, Any] | None) -> dict[str, Any]:
    support = dict(market_path_support or {})
    quality_summary = dict(support.get("series_quality_summary") or {})
    route_state = str(support.get("route_state") or support.get("eligibility_state") or "").strip().lower()
    usefulness = str(support.get("usefulness_label") or "").strip().lower()
    freshness_state = str(support.get("freshness_state") or "").strip().lower()
    quality_label = str(quality_summary.get("quality_label") or support.get("path_quality_label") or "").strip().lower()
    direct_vs_proxy = 92
    if bool(support.get("uses_proxy_series")):
        direct_vs_proxy = 68
    if route_state in {"unavailable", "failed", "disabled", "unsupported"}:
        direct_vs_proxy = 22
    route_validity = _score_from_states(
        route_state,
        {
            "direct": 94,
            "direct_usable": 92,
            "eligible": 84,
            "proxy": 72,
            "proxy_usable": 72,
            "approved_proxy": 70,
            "last_good_artifact": 54,
            "stale": 50,
            "degraded": 46,
            "unavailable": 18,
            "failed": 14,
            "unsupported": 12,
        },
        76 if usefulness in {"strong", "usable"} else 48,
    )
    bars_present = float(quality_summary.get("bars_present") or 0.0)
    history_depth = _score_from_threshold(
        bars_present,
        cuts=[(756, 94), (252, 84), (126, 68), (30, 48), (0, 22)],
        default=40,
    )
    history_depth_quality = _weighted_average(
        [
            (history_depth, 0.7),
            (
                float(
                    _score_from_states(
                        quality_label,
                        {"excellent": 96, "good": 84, "usable": 72, "thin": 48, "degraded": 44, "broken": 20},
                        60,
                    )
                ),
                0.3,
            ),
        ]
    )
    current_attempt = _score_from_states(
        freshness_state,
        {"fresh": 96, "current": 94, "stale": 58, "last_good": 54, "unavailable": 18},
        52,
    )
    provider_terminal_confidence = 82
    if not dict(support.get("truth_manifest") or {}).get("model_family"):
        provider_terminal_confidence = 54
    if not support.get("sampling_summary"):
        provider_terminal_confidence -= 12
    if freshness_state in {"last_good", "stale"}:
        provider_terminal_confidence -= 8
    market_path_fields = ["route_state", "usefulness_label", "freshness_state", "series_quality_summary", "sampling_summary"]
    missing_fields = [field for field in market_path_fields if not support.get(field)]
    weak_fields = ["uses_proxy_series"] if bool(support.get("uses_proxy_series")) else []
    stale_fields = ["freshness_state"] if freshness_state in {"stale", "last_good"} else []
    conflict_fields: list[str] = []
    caps_applied: list[str] = []
    score = 0.30 * route_validity + 0.25 * history_depth_quality + 0.20 * current_attempt + 0.15 * direct_vs_proxy + 0.10 * provider_terminal_confidence
    if route_state in {"unavailable", "failed", "unsupported"} or usefulness == "suppressed":
        score = min(score, 25.0)
        caps_applied.append("Missing history or suppressed route caps market-path support at 25.")
    elif bool(support.get("uses_proxy_series")) and bars_present <= 0:
        score = min(score, 72.0)
        caps_applied.append("Proxy-only path with no direct history caps market-path support at 72.")
    elif freshness_state in {"stale", "last_good"}:
        score = min(score, 76.0)
        caps_applied.append("Stale but usable path caps market-path support at 76.")
    final_score = _clamp_score(score, minimum=18, maximum=96)
    confidence = _component_confidence(
        field_scores=[route_validity, int(round(history_depth_quality)), current_attempt, direct_vs_proxy, provider_terminal_confidence],
        missing_fields=missing_fields,
        weak_fields=weak_fields,
        stale_fields=stale_fields,
        conflict_fields=conflict_fields,
        caps_applied=caps_applied,
    )
    return _make_component(
        key="market_path_support",
        score=final_score,
        reasons=[
            (
                "Market-path support is direct, fresh, and auditable enough to remain a bounded support layer."
                if final_score >= 82
                else "Market-path support is usable, but proxy routing, stale freshness, or thinner history keep it bounded."
                if final_score >= 60
                else "Market-path support is too weak because the route, freshness, or history depth are not decision-grade."
            ),
            str(support.get("candidate_implication") or "Market-path support stays secondary and auditable only when route and freshness remain explicit."),
        ],
        caps_applied=caps_applied,
        field_drivers=["route_state", "series_quality_summary", "freshness_state", "sampling_summary"],
        missing_fields=missing_fields,
        weak_fields=weak_fields,
        stale_fields=stale_fields,
        conflict_fields=conflict_fields,
        confidence=confidence,
    )


def enrich_score_decomposition_with_market_path_support(
    score_decomposition: dict[str, Any] | None,
    market_path_support: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not score_decomposition:
        return None
    enriched = dict(score_decomposition)
    components = [dict(component) for component in list(enriched.get("components") or [])]
    components = [
        component
        for component in components
        if str(component.get("component_id") or component.get("key") or "") != "market_path_support"
    ]
    market_component = market_path_support_component(market_path_support)
    components.append(market_component)
    enriched["components"] = components
    enriched["market_path_support_score"] = int(market_component["score"])
    component_index = {
        str(component.get("component_id") or component.get("key") or ""): dict(component)
        for component in components
    }
    admissibility_score = int(enriched.get("admissibility_score") or 0)
    identity_score = int(enriched.get("identity_score") or 0)
    admissibility_identity_score = _clamp_score(
        min(
            int(enriched.get("admissibility_identity_score") or admissibility_score or 0),
            admissibility_score or 0,
            identity_score or 0,
        ),
        minimum=15,
        maximum=100,
    )
    recommendation_merit_score = _clamp_score(
        _weighted_average(
            [
                (float(component_index.get("sleeve_fit", {}).get("score") or 0.0), 0.30),
                (float(component_index.get("instrument_quality", {}).get("score") or 0.0), 0.25),
                (float(component_index.get("long_horizon_quality", {}).get("score") or 0.0), 0.20),
                (float(component_index.get("benchmark_fidelity", {}).get("score") or 0.0), 0.15),
                (float(component_index.get("portfolio_fit", {}).get("score") or 0.0), 0.10),
            ]
        ),
        minimum=18,
        maximum=100,
    )
    source_completion_score = _clamp_score(
        float(enriched.get("source_completion_score") or 42.0),
        minimum=18,
        maximum=100,
    )
    freshness_cleanliness_score = _clamp_score(
        float(enriched.get("freshness_cleanliness_score") or 50.0),
        minimum=18,
        maximum=100,
    )
    conflict_cleanliness_score = _clamp_score(
        float(enriched.get("conflict_cleanliness_score") or 50.0),
        minimum=18,
        maximum=100,
    )
    deployability_score = _clamp_score(
        _weighted_average(
            [
                (float(component_index.get("source_integrity", {}).get("score") or 0.0), 0.35),
                (float(component_index.get("implementation", {}).get("score") or 0.0), 0.30),
                (float(component_index.get("market_path_support", {}).get("score") or 0.0), 0.20),
                (float(admissibility_identity_score), 0.15),
            ]
        ),
        minimum=18,
        maximum=100,
    )
    truth_confidence_score = _clamp_score(
        _weighted_average(
            [
                (float(component_index.get("source_integrity", {}).get("score") or 0.0), 0.35),
                (float(source_completion_score), 0.20),
                (float(freshness_cleanliness_score), 0.15),
                (float(conflict_cleanliness_score), 0.15),
                (float(component_index.get("market_path_support", {}).get("score") or 0.0), 0.15),
            ]
        ),
        minimum=18,
        maximum=100,
    )
    raw_recommendation_score = _weighted_average(
        [
            (float(recommendation_merit_score), 0.80),
            (float(deployability_score), 0.20),
        ]
    )
    missing_material_aum = any(
        "aum" == str(field or "").strip().lower()
        for field in list(component_index.get("instrument_quality", {}).get("missing_fields") or [])
        + list(component_index.get("long_horizon_quality", {}).get("missing_fields") or [])
    )
    recommendation_score = _apply_deployment_score_caps(
        raw_recommendation_score,
        identity_score=identity_score,
        admissibility_score=admissibility_score,
        truth_confidence_score=int(truth_confidence_score),
        deployability_score=int(deployability_score),
        source_completion_score=int(source_completion_score),
        hard_benchmark_conflict=bool(enriched.get("hard_benchmark_conflict")),
        missing_market_history_without_proxy=bool(
            str(dict(market_path_support or {}).get("route_state") or "").strip().lower() in {"unavailable", "failed"}
            and not bool(dict(market_path_support or {}).get("uses_proxy_series"))
        ),
        missing_material_aum=missing_material_aum,
        unresolved_stale_critical_field=bool(
            "stale" in " ".join(str(cap or "").strip().lower() for cap in list(component_index.get("source_integrity", {}).get("caps_applied") or []))
        ),
    )
    enriched["admissibility_identity_score"] = int(admissibility_identity_score)
    enriched["recommendation_merit_score"] = int(recommendation_merit_score)
    enriched["investment_merit_score"] = int(recommendation_merit_score)
    enriched["deployability_score"] = int(deployability_score)
    enriched["truth_confidence_score"] = int(truth_confidence_score)
    enriched["source_completion_score"] = int(source_completion_score)
    enriched["freshness_cleanliness_score"] = int(freshness_cleanliness_score)
    enriched["conflict_cleanliness_score"] = int(conflict_cleanliness_score)
    enriched["truth_confidence_band"] = _truth_confidence_band(truth_confidence_score)
    enriched["truth_confidence_summary"] = _truth_confidence_summary(truth_confidence_score)
    enriched["recommendation_score"] = int(recommendation_score)
    enriched["optimality_score"] = int(recommendation_merit_score)
    enriched["readiness_score"] = int(deployability_score)
    enriched["deployment_score"] = int(deployability_score)
    enriched["total_score"] = int(recommendation_score)
    enriched["confidence_penalty"] = _clamp_score(
        (
            100.0
            - _weighted_average(
                [
                    (float(dict(component).get("confidence") or 0.0), 1.0)
                    for component in components
                ]
            )
            + 2.0 * sum(len(list(dict(component).get("caps_applied") or [])) for component in components)
        )
        / 3.5,
        minimum=0,
        maximum=28,
    )
    enriched["readiness_posture"] = _deployment_readiness_posture(
        source_integrity_score=int(enriched.get("source_integrity_score") or 0),
        implementation_score=int(enriched.get("implementation_score") or 0),
        admissibility_identity_score=int(admissibility_identity_score),
        benchmark_fidelity_score=int(enriched.get("benchmark_fidelity_score") or 0),
        market_path_support_score=int(enriched.get("market_path_support_score") or 0),
        instrument_quality_score=int(enriched.get("instrument_quality_score") or 0),
        sleeve_fit_score=int(enriched.get("sleeve_fit_score") or 0),
        portfolio_fit_score=int(enriched.get("portfolio_fit_score") or 0),
        deployment_score=int(deployability_score),
        identity_score=identity_score,
        admissibility_score=admissibility_score,
    )
    enriched["deployability_badge"] = _deployability_badge(
        readiness_posture=str(enriched.get("readiness_posture") or "blocked"),
        deployability_score=int(deployability_score),
        truth_confidence_score=int(truth_confidence_score),
        admissibility_score=admissibility_score,
        identity_score=identity_score,
    )
    enriched["readiness_summary"] = _deployment_readiness_summary(str(enriched.get("readiness_posture") or "blocked"))
    enriched["score_model_version"] = "recommendation_score_v3"
    enriched["summary"] = (
        "Recommendation score weights recommendation merit 80% and deployability 20%, while truth confidence controls how strongly the read can be trusted and promoted."
        if int(enriched.get("recommendation_score") or enriched.get("total_score") or 0) >= 72
        else "Recommendation score remains capped because truth confidence, deployability, or hard routing and evidence controls are still constraining the read."
    )
    return enriched


def build_score_rubric(
    *,
    sleeve_key: str | None,
    score_decomposition: dict[str, Any] | None,
) -> dict[str, Any] | None:
    score = dict(score_decomposition or {})
    if not score:
        return None
    weighting_profiles: dict[str, list[tuple[str, str]]] = {
        "global_equity_core": [
            ("benchmark_fidelity_score", "high"),
            ("implementation_score", "high"),
            ("source_integrity_score", "high"),
            ("sleeve_fit_score", "high"),
            ("instrument_quality_score", "high"),
            ("long_horizon_quality_score", "medium"),
            ("portfolio_fit_score", "medium"),
            ("market_path_support_score", "low"),
        ],
        "developed_ex_us_optional": [
            ("benchmark_fidelity_score", "high"),
            ("implementation_score", "high"),
            ("source_integrity_score", "high"),
            ("sleeve_fit_score", "high"),
            ("instrument_quality_score", "high"),
            ("long_horizon_quality_score", "medium"),
            ("portfolio_fit_score", "medium"),
            ("market_path_support_score", "low"),
        ],
        "emerging_markets": [
            ("benchmark_fidelity_score", "high"),
            ("source_integrity_score", "high"),
            ("implementation_score", "high"),
            ("sleeve_fit_score", "high"),
            ("instrument_quality_score", "medium"),
            ("long_horizon_quality_score", "medium"),
            ("portfolio_fit_score", "medium"),
            ("market_path_support_score", "low"),
        ],
        "china_satellite": [
            ("benchmark_fidelity_score", "high"),
            ("sleeve_fit_score", "high"),
            ("source_integrity_score", "high"),
            ("implementation_score", "medium"),
            ("instrument_quality_score", "medium"),
            ("long_horizon_quality_score", "medium"),
            ("portfolio_fit_score", "medium"),
            ("market_path_support_score", "low"),
        ],
        "ig_bonds": [
            ("benchmark_fidelity_score", "high"),
            ("implementation_score", "high"),
            ("source_integrity_score", "high"),
            ("sleeve_fit_score", "medium"),
            ("instrument_quality_score", "medium"),
            ("long_horizon_quality_score", "medium"),
            ("portfolio_fit_score", "medium"),
            ("market_path_support_score", "low"),
        ],
        "cash_bills": [
            ("implementation_score", "high"),
            ("source_integrity_score", "high"),
            ("benchmark_fidelity_score", "medium"),
            ("sleeve_fit_score", "medium"),
            ("instrument_quality_score", "medium"),
            ("long_horizon_quality_score", "medium"),
            ("portfolio_fit_score", "medium"),
            ("market_path_support_score", "low"),
        ],
        "real_assets": [
            ("sleeve_fit_score", "high"),
            ("implementation_score", "high"),
            ("source_integrity_score", "high"),
            ("instrument_quality_score", "medium"),
            ("long_horizon_quality_score", "medium"),
            ("benchmark_fidelity_score", "medium"),
            ("portfolio_fit_score", "medium"),
            ("market_path_support_score", "low"),
        ],
        "alternatives": [
            ("sleeve_fit_score", "high"),
            ("implementation_score", "high"),
            ("source_integrity_score", "high"),
            ("instrument_quality_score", "medium"),
            ("long_horizon_quality_score", "medium"),
            ("benchmark_fidelity_score", "medium"),
            ("portfolio_fit_score", "medium"),
            ("market_path_support_score", "low"),
        ],
        "convex": [
            ("sleeve_fit_score", "high"),
            ("implementation_score", "high"),
            ("source_integrity_score", "high"),
            ("instrument_quality_score", "medium"),
            ("long_horizon_quality_score", "medium"),
            ("benchmark_fidelity_score", "medium"),
            ("portfolio_fit_score", "medium"),
            ("market_path_support_score", "low"),
        ],
    }
    ordered = weighting_profiles.get(
        str(sleeve_key or "").strip(),
        [
            ("sleeve_fit_score", "high"),
            ("implementation_score", "high"),
            ("source_integrity_score", "high"),
            ("benchmark_fidelity_score", "high"),
            ("instrument_quality_score", "medium"),
            ("long_horizon_quality_score", "medium"),
            ("portfolio_fit_score", "medium"),
            ("market_path_support_score", "low"),
        ],
    )
    label_map = {
        "sleeve_fit_score": "Sleeve fit",
        "implementation_score": "Implementation",
        "source_integrity_score": "Source integrity",
        "identity_score": "Identity",
        "benchmark_fidelity_score": "Benchmark fidelity",
        "market_path_support_score": "Market-path support",
        "long_horizon_quality_score": "Long-horizon quality",
        "instrument_quality_score": "Instrument quality",
        "portfolio_fit_score": "Portfolio fit",
        "recommendation_merit_score": "Recommendation merit",
        "investment_merit_score": "Investment merit",
        "deployability_score": "Deployability",
        "truth_confidence_score": "Truth confidence",
        "recommendation_score": "Recommendation score",
        "total_score": "Recommendation score",
        "confidence_penalty": "Confidence penalty",
    }
    component_key_map = {
        "implementation_score": "implementation",
        "source_integrity_score": "source_integrity",
        "sleeve_fit_score": "sleeve_fit",
        "identity_score": "identity",
        "benchmark_fidelity_score": "benchmark_fidelity",
        "market_path_support_score": "market_path_support",
        "long_horizon_quality_score": "long_horizon_quality",
        "instrument_quality_score": "instrument_quality",
        "portfolio_fit_score": "portfolio_fit",
    }
    components_by_id = {
        str(component.get("component_id") or ""): dict(component)
        for component in list(score.get("components") or [])
    }
    families: list[dict[str, Any]] = []
    for family_id, weighting_bucket in ordered:
        component = components_by_id.get(component_key_map.get(family_id, ""), {})
        families.append(
            {
                "family_id": family_id,
                "label": label_map.get(family_id, family_id.replace("_", " ").title()),
                "score": score.get(family_id),
                "weighting_bucket": weighting_bucket,
                "bounded_role": "support_only" if family_id == "market_path_support_score" else None,
                "summary": str(component.get("summary") or "").strip()
                or (
                    "Market-path support stays secondary and cannot dominate the recommendation score."
                    if family_id == "market_path_support_score"
                    else f"{label_map.get(family_id, family_id)} contributes to the deterministic score."
                ),
            }
        )
    return {
        "weighting_profile": str(sleeve_key or "default"),
        "summary": str(score.get("summary") or "Recommendation score remains deterministic and auditable across explicit families."),
        "confidence_penalty": score.get("confidence_penalty"),
        "dimension_priority_order": [family_id for family_id, _ in ordered],
        "families": families,
    }


def build_recommendation_gate(
    *,
    completeness: dict[str, Any],
    implementation_profile: dict[str, Any],
    reconciliation: dict[str, Any],
    identity_state: dict[str, Any] | None = None,
    source_authority_map: list[dict[str, Any]] | None = None,
    reconciliation_report: list[dict[str, Any]] | None = None,
    source_completion_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    completed_source_fields = _completed_source_fields(source_completion_summary)
    authority_rows = [item for item in list(source_authority_map or []) if item.get("recommendation_critical")]
    present_authority_fields = {
        str(item.get("field_name") or "")
        for item in authority_rows
        if item.get("recommendation_critical") and item.get("freshness_state") != "missing"
    }
    critical_missing = [
        field
        for field in list(completeness.get("critical_required_fields_missing") or [])
        if (field in _IMPLEMENTATION_BLOCKING_FIELDS or field in _CONFLICT_CRITICAL_FIELDS)
        and field not in present_authority_fields
        and field not in completed_source_fields
    ]
    authority_missing = [
        str(item.get("field_name") or "")
        for item in authority_rows
        if item.get("recommendation_critical") and item.get("freshness_state") == "missing"
        and str(item.get("field_name") or "") not in completed_source_fields
    ]
    stale_blocking = [
        str(item.get("field_name") or "")
        for item in authority_rows
        if item.get("field_name") in _HARD_STALE_BLOCK_FIELDS and item.get("freshness_state") == "stale"
        and str(item.get("field_name") or "") not in completed_source_fields
    ]
    weak_authority = [
        str(item.get("field_name") or "")
        for item in authority_rows
        if item.get("field_name") in _AUTHORITY_SENSITIVE_FIELDS
        and item.get("authority_class") in {"registry_seed", "provider_or_market_summary"}
        and item.get("freshness_state") == "current"
        and str(item.get("field_name") or "") not in completed_source_fields
    ]
    missing_document_support = [
        str(item.get("field_name") or "")
        for item in authority_rows
        if _material_document_gap(item)
        and str(item.get("field_name") or "") not in completed_source_fields
    ]
    critical_missing = sorted(dict.fromkeys(critical_missing + authority_missing))
    stale_count = int(completeness.get("stale_required_count") or 0)
    proxy_count = int(completeness.get("proxy_only_count") or 0)
    readiness = str(completeness.get("readiness_level") or "research_visible")
    execution_suitability = str(implementation_profile.get("execution_suitability") or "execution_mixed")
    route_validity_state = str(implementation_profile.get("route_validity_state") or "").strip()
    history_depth_state = str(implementation_profile.get("history_depth_state") or "").strip()
    execution_confidence = str(implementation_profile.get("execution_confidence") or "").strip()
    blocked_reasons: list[str] = []
    relevant_reconciliation_rows = [item for item in list(reconciliation_report or []) if item.get("recommendation_critical")]
    field_conflicts = [item for item in relevant_reconciliation_rows if item.get("blocking_effect") == "block"]
    review_items = [item for item in relevant_reconciliation_rows if item.get("blocking_effect") == "review"]
    document_gap_benchmark = [field for field in missing_document_support if field == "benchmark_name"]
    review_only_reasons: list[str] = []

    if critical_missing:
        blocked_reasons.append(
            "Critical implementation fields are missing: " + ", ".join(field.replace("_", " ") for field in critical_missing[:5]) + "."
        )
    if stale_blocking:
        blocked_reasons.append(
            "Recommendation-critical fields are stale: " + ", ".join(field.replace("_", " ") for field in stale_blocking[:5]) + "."
        )
    if reconciliation.get("status") == "hard_conflict":
        blocked_reasons.append(str(reconciliation.get("summary") or "Critical source conflicts remain unresolved."))
    if bool(dict(identity_state or {}).get("blocking")):
        blocked_reasons.append(str(dict(identity_state or {}).get("summary") or "Identity conflict remains unresolved."))
    if field_conflicts:
        blocked_reasons.append(
            "Field-level conflicts remain on: " + ", ".join(str(item.get("field_name") or "").replace("_", " ") for item in field_conflicts[:5]) + "."
        )
    if stale_count and not stale_blocking and str(dict(source_completion_summary or {}).get("state") or "").strip() != "complete":
        review_only_reasons.append(f"{stale_count} required field{'s are' if stale_count != 1 else ' is'} stale.")
    if proxy_count and readiness == "recommendation_ready":
        review_only_reasons.append("Recommendation readiness is bounded by proxy-only fields.")
    execution_blocking = execution_suitability == "execution_weak"
    if execution_blocking and route_validity_state in {"direct_ready", "proxy_ready", "benchmark_lineage_weak"} and history_depth_state in {"strong", "usable"} and execution_confidence in {"usable", "degraded"}:
        execution_blocking = False
        review_only_reasons.append("Current quote or family freshness is still missing, but route and history support are strong enough for review.")
    if execution_blocking:
        blocked_reasons.append("Implementation quality remains too weak for an investor-grade recommendation.")
    if any(item.get("status") == "execution_review_required" for item in relevant_reconciliation_rows):
        review_only_reasons.append("Execution proxy support still needs review before recommendation use.")

    if critical_missing or stale_blocking or reconciliation.get("status") == "hard_conflict" or field_conflicts or execution_blocking or bool(dict(identity_state or {}).get("blocking")):
        gate_state = "blocked"
    elif (
        readiness in {"recommendation_ready", "shortlist_ready"}
        and not stale_count
        and not proxy_count
        and not weak_authority
        and not review_items
        and not missing_document_support
        and not review_only_reasons
    ):
        gate_state = "admissible"
    else:
        gate_state = "review_only"

    data_confidence = "high"
    if blocked_reasons:
        data_confidence = "mixed" if gate_state == "review_only" else "low"
    elif proxy_count or weak_authority or review_items or missing_document_support:
        data_confidence = "mixed"

    return {
        "gate_state": gate_state,
        "readiness_level": readiness,
        "execution_blocking": execution_blocking,
        "critical_missing_fields": critical_missing,
        "blocked_reasons": blocked_reasons
        + (
            [
                "Recommendation remains review-only because preferred document support is still missing on: "
                + ", ".join(field.replace("_", " ") for field in missing_document_support[:5])
                + "."
            ]
            if gate_state == "review_only" and missing_document_support
            else []
        )
        + (
            [
                "Recommendation remains review-only because weaker-authority fields still need stronger sourcing: "
                + ", ".join(field.replace("_", " ") for field in weak_authority[:5])
                + "."
            ]
            if gate_state == "review_only" and weak_authority
            else []
        )
        + (
            [
                "Recommendation remains review-only because benchmark lineage is still bounded by proxy or lighter document support: "
                + ", ".join(field.replace("_", " ") for field in document_gap_benchmark[:5])
                + "."
            ]
            if gate_state == "review_only" and document_gap_benchmark
            else []
        )
        + (
            [
                "Recommendation remains review-only because the current support still needs review on: "
                + "; ".join(review_only_reasons[:3])
            ]
            if gate_state == "review_only" and review_only_reasons
            else []
        ),
        "data_confidence": data_confidence,
        "execution_suitability": execution_suitability,
        "summary": (
            "Recommendation admissibility is clear."
            if gate_state == "admissible"
            else (
                "Recommendation remains review-only while freshness or bounded support tighten."
                if gate_state == "review_only" and str(dict(source_completion_summary or {}).get("state") or "").strip() == "complete"
                else "Recommendation remains review-only while source authority, freshness, or bounded proxy support tighten."
                if gate_state == "review_only"
                else (
                    "Recommendation is blocked by unresolved identity conflict."
                    if bool(dict(identity_state or {}).get("blocking"))
                    else "Recommendation is blocked until conflicting recommendation-critical truth is reconciled."
                    if field_conflicts or reconciliation.get("status") == "hard_conflict"
                    else "Recommendation is blocked until stale recommendation-critical truth is refreshed."
                    if stale_blocking
                    else "Recommendation is blocked until missing implementation truth is restored."
                    if critical_missing
                    else "Recommendation is blocked until implementation quality is strong enough for investor use."
                )
            )
        ),
    }
def build_candidate_truth_context(conn: sqlite3.Connection, candidate: dict[str, Any]) -> dict[str, Any]:
    symbol = _normalize_symbol(candidate)
    sleeve_key = str(candidate.get("sleeve_key") or "").strip()
    benchmark_assignment = resolve_benchmark_assignment(conn, candidate=candidate, sleeve_key=sleeve_key)
    candidate_for_truth = _candidate_with_benchmark_assignment(candidate, benchmark_assignment)
    current_truth = _current_truth(conn, candidate_symbol=symbol, sleeve_key=sleeve_key)
    completeness = _latest_completeness_snapshot(conn, candidate_symbol=symbol, sleeve_key=sleeve_key)
    if completeness is None:
        completeness = compute_candidate_completeness(conn, candidate={**candidate_for_truth, "symbol": symbol, "sleeve_key": sleeve_key}, now=datetime.now(UTC))
    applicable_field_names = {
        str(requirement.get("field_name") or "")
        for requirement in list_required_fields(conn, sleeve_key)
        if _field_applicable(candidate_for_truth, str(requirement.get("field_name") or ""), str(requirement.get("applicability_rule") or "always"))
    }
    implementation_profile = build_implementation_profile(
        conn,
        candidate_for_truth,
        resolved_truth=current_truth,
        benchmark_assignment=benchmark_assignment,
    )
    source_authority_map = build_source_authority_map(
        candidate_for_truth,
        resolved_truth=current_truth,
        benchmark_assignment=benchmark_assignment,
        applicable_field_names=applicable_field_names,
    )
    identity_state = build_identity_state(
        conn,
        candidate=candidate_for_truth,
        candidate_symbol=symbol,
        sleeve_key=sleeve_key,
        resolved_truth=current_truth,
        benchmark_assignment=benchmark_assignment,
    )
    reconciliation_report = build_reconciliation_report(
        conn,
        candidate=candidate_for_truth,
        candidate_symbol=symbol,
        sleeve_key=sleeve_key,
        resolved_truth=current_truth,
        benchmark_assignment=benchmark_assignment,
        applicable_field_names=applicable_field_names,
    )
    reconciliation = build_reconciliation_summary(conn, candidate_symbol=symbol, sleeve_key=sleeve_key)
    source_completion_summary = build_source_completion_summary(
        source_authority_map=source_authority_map,
        implementation_profile=implementation_profile,
        reconciliation_report=reconciliation_report,
    )
    gate = build_recommendation_gate(
        completeness=completeness,
        implementation_profile=implementation_profile,
        reconciliation=reconciliation,
        identity_state=identity_state,
        source_authority_map=source_authority_map,
        reconciliation_report=reconciliation_report,
        source_completion_summary=source_completion_summary,
    )
    data_quality = build_data_quality_summary(
        source_authority_map=source_authority_map,
        recommendation_gate=gate,
        reconciliation=reconciliation,
        reconciliation_report=reconciliation_report,
        source_completion_summary=source_completion_summary,
    )
    institutional_facts = build_institutional_facts(
        candidate_for_truth,
        resolved_truth=current_truth,
        source_authority_map=source_authority_map,
        benchmark_assignment=benchmark_assignment,
    )
    source_integrity_summary = build_source_integrity_summary(
        source_authority_map=source_authority_map,
        data_quality_summary=data_quality,
        reconciliation=reconciliation,
        reconciliation_report=reconciliation_report,
        identity_state=identity_state,
        source_completion_summary=source_completion_summary,
    )
    investor_decision_state = build_investor_decision_state(
        gate=gate,
        identity_state=identity_state,
    )
    failure_class_summary = build_failure_class_summary(
        recommendation_gate=gate,
        reconciliation_report=reconciliation_report,
        source_authority_map=source_authority_map,
        identity_state=identity_state,
        implementation_profile=implementation_profile,
        benchmark_assignment=benchmark_assignment,
        source_completion_summary=source_completion_summary,
    )
    visible_decision_state = build_visible_decision_state(
        investor_decision_state=investor_decision_state,
        recommendation_gate=gate,
        identity_state=identity_state,
        failure_class_summary=failure_class_summary,
    )
    blocker_category = build_blocker_category(
        gate=gate,
        identity_state=identity_state,
        reconciliation_report=reconciliation_report,
    )
    score_decomposition = build_score_decomposition(
        candidate=candidate_for_truth,
        resolved_truth=current_truth,
        benchmark_assignment=benchmark_assignment,
        institutional_facts=institutional_facts,
        implementation_profile=implementation_profile,
        gate=gate,
        data_quality_summary=data_quality,
        identity_state=identity_state,
        source_authority_map=source_authority_map,
        source_completion_summary=source_completion_summary,
        reconciliation_report=reconciliation_report,
        blocker_category=blocker_category,
        sleeve_key=sleeve_key,
    )
    return {
        "benchmark_assignment": benchmark_assignment,
        "resolved_truth": current_truth,
        "completeness": completeness,
        "implementation_profile": implementation_profile,
        "primary_document_manifest": _primary_document_manifest(candidate),
        "identity_state": identity_state,
        "institutional_facts": institutional_facts,
        "reconciliation": reconciliation,
        "reconciliation_report": reconciliation_report,
        "source_authority_map": source_authority_map,
        "source_completion_summary": source_completion_summary,
        "data_quality": data_quality,
        "recommendation_gate": gate,
        "source_integrity_summary": source_integrity_summary,
        "investor_decision_state": investor_decision_state,
        "visible_decision_state": visible_decision_state,
        "blocker_category": blocker_category,
        "failure_class_summary": failure_class_summary,
        "score_decomposition": score_decomposition,
    }
