from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.isdigit():
            epoch_value = float(text)
            if epoch_value > 10_000_000:
                return datetime.fromtimestamp(epoch_value, tz=UTC)
    except Exception:
        pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        try:
            parsed = datetime.fromisoformat(f"{text}T00:00:00+00:00")
        except Exception:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def age_seconds(value: Any) -> float | None:
    parsed = _parse_dt(value)
    if parsed is None:
        return None
    return max(0.0, (datetime.now(UTC) - parsed).total_seconds())


def freshness_state_for_age(age: float | None, cadence_seconds: int | None) -> str:
    if age is None:
        return "unknown"
    cadence = max(300, int(cadence_seconds or 1800))
    if age <= cadence:
        return "current"
    if age <= cadence * 3:
        return "aging"
    return "stale"


def _family_effective_cadence(provider_family: str | None, cadence_seconds: int | None, observed_at: Any) -> int:
    family = str(provider_family or "").strip().lower()
    cadence = max(300, int(cadence_seconds or 1800))
    observed = _parse_dt(observed_at)
    now = datetime.now(UTC)
    if family in {"benchmark_proxy", "benchmark_proxy_history", "ohlcv_history"}:
        cadence = max(cadence, 24 * 60 * 60)
        if now.weekday() >= 5 or (now.weekday() == 0 and now.hour < 14):
            cadence = max(cadence, 72 * 60 * 60)
        return cadence
    if family in {"latest_quote", "quote_latest"}:
        # Quote families should be tighter than daily history, but weekend and pre-open
        # sessions should not be treated as hard stale when only the last official close exists.
        cadence = max(cadence, 6 * 60 * 60)
        if observed is not None:
            if now.weekday() >= 5 or (now.weekday() == 0 and now.hour < 14):
                cadence = max(cadence, 72 * 60 * 60)
            elif now.hour < 14:
                cadence = max(cadence, 24 * 60 * 60)
        return cadence
    return cadence


def _freshness_model(provider_family: str | None) -> str:
    family = str(provider_family or "").strip().lower()
    if family in {"latest_quote", "quote_latest", "fx"}:
        return "intraday_quote"
    if family in {"market_close"}:
        return "daily_close"
    if family in {"fx_reference", "usd_strength_fallback"}:
        return "daily_reference"
    if family in {"benchmark_proxy", "benchmark_proxy_history", "ohlcv_history"}:
        return "daily_history"
    if family in {"etf_holdings", "filings_context"}:
        return "periodic_filing"
    return "reference_snapshot"


def _operational_freshness_state(provider_family: str | None, freshness_state: str, observed_at: Any, fetched_at: Any) -> str:
    state = str(freshness_state or "")
    if state in {"current", "aging", "unavailable", "unknown"}:
        return state
    family = str(provider_family or "").strip().lower()
    age = age_seconds(observed_at) or age_seconds(fetched_at)
    if age is None:
        return state
    now = datetime.now(UTC)
    if family == "market_close":
        grace = 72 * 60 * 60 if now.weekday() >= 5 or (now.weekday() == 0 and now.hour < 14) else 36 * 60 * 60
        return "expected_lag" if age <= grace else state
    if family in {"fx_reference", "usd_strength_fallback"}:
        grace = 96 * 60 * 60 if now.weekday() >= 5 else 48 * 60 * 60
        return "expected_lag" if age <= grace else state
    return state


def coverage_status_from_error(error_state: str | None) -> str:
    error = str(error_state or "").strip().lower()
    if not error:
        return "complete"
    if any(token in error for token in ("auth_error", "rate_limited", "budget_block")):
        return "missing_fetchable"
    if any(token in error for token in ("not_found", "symbol_gap", "empty_response")):
        return "missing_source_gap"
    if any(token in error for token in ("provider_error", "upstream_error")):
        return "partial"
    return "partial"


def error_family(error_state: str | None, *, fallback_aliases: list[str] | None = None) -> str | None:
    error = str(error_state or "").strip().lower()
    if not error:
        return None
    aliases = [str(item).strip() for item in list(fallback_aliases or []) if str(item).strip()]
    if "402" in error or "payment required" in error:
        return "plan_limited"
    if "403" in error and "auth" in error:
        return "endpoint_blocked"
    if "404" in error or "not_found" in error:
        return "symbol_gap" if aliases else "missing_source_gap"
    if "empty_response" in error:
        return "empty_response"
    if "provider_error" in error or "upstream_error" in error:
        return "provider_error"
    if "rate_limited" in error or "429" in error:
        return "rate_limited"
    return "unknown_failure"


def confidence_label(
    *,
    source_tier: str | None,
    freshness_state: str,
    fallback_used: bool,
    coverage_status: str,
    provider_diversity: int = 1,
    specificity_score: float = 0.0,
) -> tuple[str, str]:
    score = 0.0
    tier = str(source_tier or "secondary").lower()
    score += {"primary": 3.0, "secondary": 2.0, "strategic": 2.0, "public": 1.5}.get(tier, 1.0)
    score += {"current": 2.0, "aging": 1.0, "stale": -1.0, "unknown": -0.5}.get(freshness_state, -0.5)
    if fallback_used:
        score -= 1.0
    score += min(1.5, max(0.0, float(provider_diversity) - 1.0) * 0.75)
    score += max(0.0, float(specificity_score))
    if coverage_status in {"missing_source_gap", "partial"}:
        score -= 1.0
    if coverage_status == "missing_fetchable":
        score -= 0.5
    if score >= 5.0:
        return "high", "Fresh source-backed data with acceptable recency and good provider support."
    if score >= 3.0:
        return "medium", "Useful source-backed data, but either aging, fallback-assisted, or less specific."
    return "low", "Coverage, freshness, or source quality is not strong enough for higher confidence."


def governance_state(
    *,
    freshness_state: str,
    fallback_used: bool,
    coverage_status: str,
) -> str:
    if coverage_status == "missing_source_gap":
        return "missing_source_gap"
    if coverage_status == "missing_fetchable":
        return "missing_fetchable"
    if coverage_status == "partial":
        return "partial"
    if fallback_used:
        return "fallback_current" if freshness_state in {"current", "aging"} else "fallback_stale"
    if freshness_state == "current":
        return "source_backed_current"
    if freshness_state == "aging":
        return "source_backed_aging"
    return "source_backed_stale"


def build_governance_record(
    *,
    source_name: str | None,
    provider_family: str | None,
    fetched_at: Any,
    observed_at: Any,
    cadence_seconds: int | None,
    fallback_used: bool,
    cache_status: str | None,
    error_state: str | None,
    source_tier: str | None,
    coverage_status: str | None = None,
    provider_diversity: int = 1,
    specificity_score: float = 0.0,
) -> dict[str, Any]:
    resolved_coverage = str(coverage_status or coverage_status_from_error(error_state))
    effective_cadence = _family_effective_cadence(provider_family, cadence_seconds, observed_at)
    freshness = freshness_state_for_age(age_seconds(observed_at) or age_seconds(fetched_at), effective_cadence)
    operational_freshness = _operational_freshness_state(provider_family, freshness, observed_at, fetched_at)
    confidence_freshness = "aging" if operational_freshness == "expected_lag" else freshness
    confidence, confidence_reason = confidence_label(
        source_tier=source_tier,
        freshness_state=confidence_freshness,
        fallback_used=bool(fallback_used),
        coverage_status=resolved_coverage,
        provider_diversity=provider_diversity,
        specificity_score=specificity_score,
    )
    gov_state = governance_state(
        freshness_state=confidence_freshness,
        fallback_used=bool(fallback_used),
        coverage_status=resolved_coverage,
    )
    if operational_freshness == "expected_lag":
        gov_state = "fallback_expected_lag" if fallback_used else "source_backed_expected_lag"
    return {
        "source_name": source_name,
        "provider_family": provider_family,
        "fetched_at": fetched_at,
        "observed_at": observed_at,
        "freshness_state": freshness,
        "operational_freshness_state": operational_freshness,
        "freshness_model": _freshness_model(provider_family),
        "confidence_label": confidence,
        "confidence_reason": confidence_reason,
        "fallback_used": bool(fallback_used),
        "cache_status": cache_status,
        "error_state": error_state,
        "coverage_status": resolved_coverage,
        "governance_state": gov_state,
    }


def summarize_surface_sufficiency(
    items: list[dict[str, Any]],
    *,
    surface_name: str,
) -> dict[str, Any]:
    current = 0
    aging = 0
    stale = 0
    fallback = 0
    gaps = 0
    partial = 0
    for item in items:
        gov = dict(item.get("governance") or {})
        state = str(gov.get("governance_state") or "")
        if state == "source_backed_current":
            current += 1
        elif state == "source_backed_aging":
            aging += 1
        elif state in {"source_backed_stale", "fallback_stale"}:
            stale += 1
        elif state == "fallback_current":
            fallback += 1
        elif state == "partial":
            partial += 1
        elif state in {"missing_fetchable", "missing_source_gap"}:
            gaps += 1
    if surface_name == "blueprint":
        if current >= 4 and gaps == 0 and stale <= 1:
            status = "recommendation_ready"
        elif current + aging + fallback >= 3:
            status = "review_ready"
        else:
            status = "blocked_by_missing_critical_data"
    elif surface_name == "dashboard":
        if current >= 3 and gaps == 0:
            status = "enough_for_holdings_freshness"
        elif current + aging + fallback >= 2:
            status = "enough_for_benchmark_watch"
        else:
            status = "not_enough_for_full_confidence"
    else:
        if current >= 4 and partial == 0 and gaps == 0:
            status = "enough_for_portfolio_relevance"
        elif current + aging + fallback >= 3:
            status = "enough_for_interpretation"
        else:
            status = "enough_for_monitoring"
    return {
        "status": status,
        "current_count": current,
        "aging_count": aging,
        "stale_count": stale,
        "fallback_count": fallback,
        "gap_count": gaps,
        "partial_count": partial,
    }


def consistency_warning(*, family: str, surfaces: dict[str, dict[str, Any]]) -> str | None:
    observed_ats = {
        surface: str(item.get("observed_at") or "")
        for surface, item in surfaces.items()
        if str(item.get("observed_at") or "").strip()
    }
    parsed = {surface: _parse_dt(value) for surface, value in observed_ats.items()}
    normalized_days = {
        surface: (dt.date().isoformat() if dt is not None else value)
        for surface, value in observed_ats.items()
        for dt in [parsed.get(surface)]
    }
    if len(set(normalized_days.values())) > 1:
        return f"{family} context is not aligned across surfaces: " + ", ".join(
            f"{surface}={value}" for surface, value in observed_ats.items()
        )
    freshness = {
        surface: str(dict(item.get("governance") or {}).get("freshness_state") or "")
        for surface, item in surfaces.items()
    }
    if len({value for value in freshness.values() if value}) > 1:
        return f"{family} freshness differs across surfaces: " + ", ".join(f"{surface}={value}" for surface, value in freshness.items())
    return None
