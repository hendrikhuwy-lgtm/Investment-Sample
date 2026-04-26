from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.models.types import LiquidityProfile


_USABLE_HISTORY_DEPTH_DAYS = 252
_STRONG_HISTORY_DEPTH_DAYS = 756
_FRESH_QUOTE_MAX_AGE_DAYS = 5
_AGING_QUOTE_MAX_AGE_DAYS = 30


def evaluate_liquidity_profile(*, candidate: dict[str, Any], sleeve_key: str) -> dict[str, Any]:
    liquidity_score = _safe_float(candidate.get("liquidity_score"))
    spread_bps = _safe_float(candidate.get("bid_ask_spread_proxy"))
    average_volume = _safe_float(candidate.get("volume_30d_avg"))
    liquidity_proxy = str(candidate.get("liquidity_proxy") or "").strip()
    market_data_as_of = str(candidate.get("market_data_as_of") or "").strip()
    coverage = dict(candidate.get("coverage_workflow_summary") or {})
    direct_history_depth = _safe_int(coverage.get("direct_history_depth"))
    proxy_history_depth = _safe_int(coverage.get("proxy_history_depth"))
    instrument_type = str(candidate.get("instrument_type") or "")
    replication_method = str(candidate.get("replication_method") or "").strip().lower()
    warnings: list[str] = []
    blockers: list[str] = []
    provenance: list[str] = []
    evidence_basis: list[str] = []
    missing_inputs: list[str] = []
    quote_freshness_state = _quote_freshness_state(market_data_as_of)
    history_depth = direct_history_depth if direct_history_depth is not None else proxy_history_depth
    history_depth_state = _history_depth_state(history_depth)
    route_validity_state = _route_validity_state(coverage)

    if liquidity_score is None:
        if spread_bps is not None or average_volume is not None or liquidity_proxy:
            liquidity_status = "limited_evidence"
            evidence_basis.append("partial_liquidity_inputs")
        else:
            liquidity_status = "unknown"
        blockers.append("liquidity score unavailable")
        missing_inputs.append("liquidity_score")
    elif liquidity_score >= 0.85:
        liquidity_status = "strong"
        provenance.append("candidate.liquidity_score")
        evidence_basis.append("candidate.liquidity_score")
    elif liquidity_score >= 0.65:
        liquidity_status = "adequate"
        provenance.append("candidate.liquidity_score")
        evidence_basis.append("candidate.liquidity_score")
    else:
        liquidity_status = "weak"
        provenance.append("candidate.liquidity_score")
        evidence_basis.append("candidate.liquidity_score")

    if spread_bps is None:
        spread_status = "unknown"
        blockers.append("spread proxy unavailable")
        missing_inputs.append("bid_ask_spread_proxy")
    elif spread_bps <= 8:
        spread_status = "tight"
        provenance.append("candidate.bid_ask_spread_proxy")
        evidence_basis.append("candidate.bid_ask_spread_proxy")
    elif spread_bps <= 20:
        spread_status = "acceptable"
        provenance.append("candidate.bid_ask_spread_proxy")
        evidence_basis.append("candidate.bid_ask_spread_proxy")
    else:
        spread_status = "wide"
        provenance.append("candidate.bid_ask_spread_proxy")
        evidence_basis.append("candidate.bid_ask_spread_proxy")

    if average_volume is not None:
        provenance.append("candidate.volume_30d_avg")
        evidence_basis.append("candidate.volume_30d_avg")
    elif liquidity_proxy:
        evidence_basis.append("candidate.liquidity_proxy")
    else:
        missing_inputs.append("volume_30d_avg")

    if "synthetic" in replication_method or "swap" in replication_method:
        warnings.append("Synthetic or swap-based structure warrants counterparty and execution review.")
    if sleeve_key == "ig_bonds" and spread_bps is None:
        warnings.append("Bond ETF execution proxy is incomplete because bid/ask spread data is unavailable.")
    if instrument_type in {"money_market_fund_sg", "cash_account_sg"}:
        warnings.append("Fund or cash implementation may rely on dealing-cycle liquidity rather than continuous exchange liquidity.")
    if sleeve_key in {"real_assets", "alternatives"} and liquidity_score is not None and liquidity_score < 0.72:
        warnings.append("Diversifier sleeve candidate has weaker secondary-market liquidity than core ETFs.")
    if instrument_type == "long_put_overlay_strategy":
        warnings.append("Execution depends on listed option market depth and permissions rather than ETF secondary liquidity.")
    if route_validity_state == "alias_review_needed":
        warnings.append("Execution route still depends on alias review, so direct market-path certainty is limited.")
    if quote_freshness_state in {"stale", "unknown"}:
        warnings.append("Quote freshness is not strong enough to treat execution support as clean.")
    if history_depth_state in {"thin", "missing"}:
        warnings.append("Stored market-history depth is still too thin for stronger execution confidence.")

    spread_support_state = (
        "usable"
        if spread_bps is not None and quote_freshness_state in {"fresh", "aging"}
        else "degraded"
        if spread_bps is not None
        else "insufficient"
    )
    volume_support_state = (
        "usable"
        if average_volume is not None and history_depth_state in {"strong", "usable"}
        else "degraded"
        if average_volume is not None
        else "insufficient"
    )
    execution_confidence = _execution_confidence(
        quote_freshness_state=quote_freshness_state,
        history_depth_state=history_depth_state,
        spread_support_state=spread_support_state,
        volume_support_state=volume_support_state,
        route_validity_state=route_validity_state,
    )

    capacity_comment = _capacity_comment(
        liquidity_status=liquidity_status,
        spread_status=spread_status,
        liquidity_proxy=liquidity_proxy,
        instrument_type=instrument_type,
    )
    execution_comment = _execution_comment(
        liquidity_status=liquidity_status,
        spread_status=spread_status,
        liquidity_proxy=liquidity_proxy,
        instrument_type=instrument_type,
    )
    explanation = _liquidity_explanation(
        liquidity_status=liquidity_status,
        spread_status=spread_status,
        liquidity_proxy=liquidity_proxy,
        average_volume=average_volume,
        missing_inputs=missing_inputs,
    )

    profile = LiquidityProfile(
        liquidity_status=liquidity_status,
        spread_status=spread_status,
        capacity_comment=capacity_comment,
        execution_comment=execution_comment,
        days_to_liquidate_estimate=None,
        warnings=warnings,
        blockers=list(dict.fromkeys(blockers)),
        provenance=list(dict.fromkeys(provenance + (["candidate.liquidity_proxy"] if liquidity_proxy else []))),
        explanation=explanation,
        evidence_basis=list(dict.fromkeys(evidence_basis)),
        missing_inputs=list(dict.fromkeys(missing_inputs)),
        quote_freshness_state=quote_freshness_state,
        history_depth_state=history_depth_state,
        spread_support_state=spread_support_state,
        volume_support_state=volume_support_state,
        route_validity_state=route_validity_state,
        execution_confidence=execution_confidence,
    )
    return profile.model_dump(mode="json")


def _capacity_comment(*, liquidity_status: str, spread_status: str, liquidity_proxy: str, instrument_type: str) -> str:
    if liquidity_status == "strong":
        return "Current liquidity inputs support strong capacity for typical hnwi_sg ticket sizes."
    if liquidity_status == "adequate":
        return "Current liquidity inputs support ordinary implementation review, but larger tickets should still be staged carefully."
    if liquidity_status == "weak":
        return "Current liquidity inputs suggest weaker execution depth relative to core sleeve candidates."
    if instrument_type in {"money_market_fund_sg", "cash_account_sg"}:
        return "Capacity depends more on fund dealing or banking channels than exchange turnover."
    if liquidity_proxy:
        return f"Capacity could not be scored directly; available proxy says: {liquidity_proxy}."
    return "Liquidity capacity cannot be assessed from current inputs."


def _execution_comment(*, liquidity_status: str, spread_status: str, liquidity_proxy: str, instrument_type: str) -> str:
    if instrument_type == "long_put_overlay_strategy":
        return "Execution realism depends on option market depth, contract selection, and account permissions."
    if spread_status == "tight":
        return "Spread proxy is tight relative to current blueprint inputs."
    if spread_status == "acceptable":
        return "Spread proxy is acceptable for implementation review but not friction-free."
    if spread_status == "wide":
        return "Spread proxy is wide enough to justify extra care on order timing and size."
    if liquidity_proxy:
        return f"Execution comment is based on qualitative liquidity proxy only: {liquidity_proxy}."
    return "Execution realism is unknown because spread and volume proxies are incomplete."


def _liquidity_explanation(
    *,
    liquidity_status: str,
    spread_status: str,
    liquidity_proxy: str,
    average_volume: float | None,
    missing_inputs: list[str],
) -> str:
    if liquidity_status == "strong":
        return "Liquidity assessment is supported by a high liquidity score and available market-trading evidence."
    if liquidity_status == "adequate":
        return "Liquidity assessment is supported by a usable liquidity score, but execution still depends on ordinary market depth and order staging."
    if liquidity_status == "weak":
        return "Liquidity assessment is source-backed and indicates weaker execution depth than a core implementation vehicle."
    if liquidity_status == "limited_evidence":
        pieces: list[str] = []
        if spread_status != "unknown":
            pieces.append(f"spread evidence is {spread_status}")
        if average_volume is not None:
            pieces.append("recent volume evidence is available")
        if liquidity_proxy:
            pieces.append("qualitative liquidity proxy is available")
        joined = ", ".join(pieces) if pieces else "partial liquidity evidence is available"
        return f"Liquidity could only be assessed with limited evidence because {joined}, while direct liquidity scoring is missing."
    if missing_inputs:
        return f"Liquidity remains unknown because required inputs are missing: {', '.join(sorted(set(missing_inputs)))}."
    return "Liquidity remains unknown because no reliable direct or proxy liquidity evidence is available."


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:  # noqa: BLE001
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:  # noqa: BLE001
        return None


def _quote_freshness_state(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    try:
        observed_at = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=UTC)
        age_days = max(0.0, (datetime.now(UTC) - observed_at.astimezone(UTC)).total_seconds() / 86400.0)
    except ValueError:
        return "unknown"
    if age_days <= _FRESH_QUOTE_MAX_AGE_DAYS:
        return "fresh"
    if age_days <= _AGING_QUOTE_MAX_AGE_DAYS:
        return "aging"
    return "stale"


def _history_depth_state(value: int | None) -> str:
    if value is None or value <= 0:
        return "missing"
    if value >= _STRONG_HISTORY_DEPTH_DAYS:
        return "strong"
    if value >= _USABLE_HISTORY_DEPTH_DAYS:
        return "usable"
    return "thin"


def _route_validity_state(coverage: dict[str, Any]) -> str:
    status = str(coverage.get("status") or "").strip()
    if status in {"direct_ready", "proxy_ready", "alias_review_needed"}:
        return status
    if status in {"benchmark_lineage_weak", "missing_history"}:
        return "invalid"
    return "unknown"


def _execution_confidence(
    *,
    quote_freshness_state: str,
    history_depth_state: str,
    spread_support_state: str,
    volume_support_state: str,
    route_validity_state: str,
) -> str:
    if route_validity_state == "invalid":
        return "insufficient"
    if (
        route_validity_state == "direct_ready"
        and quote_freshness_state == "fresh"
        and history_depth_state == "strong"
        and spread_support_state == "usable"
    ):
        return "strong"
    if (
        route_validity_state in {"direct_ready", "proxy_ready"}
        and quote_freshness_state in {"fresh", "aging"}
        and history_depth_state in {"strong", "usable"}
        and spread_support_state in {"usable", "degraded"}
        and volume_support_state in {"usable", "degraded"}
    ):
        return "usable"
    if (
        route_validity_state in {"direct_ready", "proxy_ready", "alias_review_needed"}
        and (spread_support_state != "insufficient" or volume_support_state != "insufficient" or history_depth_state != "missing")
    ):
        return "degraded"
    return "insufficient"
