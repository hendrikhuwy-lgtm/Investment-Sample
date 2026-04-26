from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any


_SOURCE_CLASS_POLICY: dict[str, dict[str, float]] = {
    "market_series": {"half_life_days": 0.9, "lead_window_days": 1.4, "backdrop_after_days": 2.5},
    "benchmark_move": {"half_life_days": 1.0, "lead_window_days": 1.6, "backdrop_after_days": 3.0},
    "macro_release": {"half_life_days": 3.0, "lead_window_days": 4.0, "backdrop_after_days": 9.0},
    "policy_event": {"half_life_days": 0.7, "lead_window_days": 1.3, "backdrop_after_days": 2.0},
    "geopolitical_news": {"half_life_days": 0.6, "lead_window_days": 1.1, "backdrop_after_days": 2.0},
}

_GROUP_LABELS: dict[str, str] = {
    "rates_duration": "Rates and Duration",
    "inflation_real_assets": "Inflation and Real Assets",
    "fx_dollar": "FX and Dollar",
    "credit_liquidity": "Credit and Liquidity",
    "policy_release": "Policy and Official Releases",
    "growth_equity_risk": "Growth and Equity Risk",
    "volatility_risk_regime": "Volatility and Risk Regime",
    "geopolitical_global_news": "Geopolitical and Global News",
    "implementation_execution": "Implementation and Execution",
}


def assess_driver_relevance(driver: dict[str, Any]) -> dict[str, Any]:
    source_context = dict(driver.get("source_context") or {})
    source_class = str(source_context.get("source_class") or "market_series")
    policy = dict(_SOURCE_CLASS_POLICY.get(source_class) or _SOURCE_CLASS_POLICY["market_series"])
    age_days = _age_days(driver, source_context)
    freshness_relevance_score = _freshness_relevance_score(age_days=age_days, policy=policy)
    threshold_state = _threshold_state(driver)
    novelty_class, reactivation_reason = _novelty_class(
        driver=driver,
        source_context=source_context,
        age_days=age_days,
        policy=policy,
        threshold_state=threshold_state,
    )
    current_action_delta, portfolio_read_delta, decision_delta_score = _decision_deltas(
        driver=driver,
        novelty_class=novelty_class,
        freshness_score=freshness_relevance_score,
    )
    is_backdrop = _is_backdrop(
        driver=driver,
        source_context=source_context,
        age_days=age_days,
        policy=policy,
        novelty_class=novelty_class,
        freshness_score=freshness_relevance_score,
        decision_delta_score=decision_delta_score,
    )
    lead_lane = _lead_lane(
        driver=driver,
        novelty_class=novelty_class,
        freshness_score=freshness_relevance_score,
        decision_delta_score=decision_delta_score,
        is_backdrop=is_backdrop,
    )
    lead_relevance_score = round(
        float(driver.get("prominence_score") or 0.0)
        + freshness_relevance_score
        + decision_delta_score
        + _novelty_bonus(novelty_class)
        - (4.0 if lead_lane == "regime_context" else 0.0),
        1,
    )
    return {
        "freshness_half_life_days": round(float(policy.get("half_life_days") or 0.0), 2),
        "freshness_age_days": round(age_days, 2),
        "freshness_relevance_score": round(freshness_relevance_score, 1),
        "novelty_class": novelty_class,
        "reactivation_reason": reactivation_reason,
        "threshold_state": threshold_state,
        "current_action_delta": current_action_delta,
        "portfolio_read_delta": portfolio_read_delta,
        "decision_change_potential": round(float(driver.get("decision_change_potential") or 0.0) + decision_delta_score, 1),
        "lead_relevance_score": lead_relevance_score,
        "lead_lane": lead_lane,
        "is_regime_context": lead_lane == "regime_context",
    }


def build_signal_stack_groups(drivers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for driver in drivers:
        group_id = _group_bucket_key(driver)
        grouped.setdefault(group_id, []).append(driver)

    groups: list[dict[str, Any]] = []
    for group_id, items in grouped.items():
        ordered = sorted(
            items,
            key=lambda item: (
                float(item.get("lead_relevance_score") or item.get("decision_relevance_score") or 0.0),
                float(item.get("freshness_relevance_score") or 0.0),
            ),
            reverse=True,
        )
        representative = next(
            (
                item
                for item in ordered
                if str(item.get("visibility_role") or "") in {"representative", "lead"}
            ),
            ordered[0] if ordered else None,
        )
        groups.append(
            {
                "group_id": group_id,
                "label": _GROUP_LABELS.get(group_id, group_id.replace("_", " ").title()),
                "summary": _group_summary(group_id, ordered),
                "representative": representative,
                "count": len(ordered),
                "signals": ordered,
            }
        )
    groups.sort(
        key=lambda group: max(
            (float(item.get("lead_relevance_score") or item.get("decision_relevance_score") or 0.0) for item in group["signals"]),
            default=0.0,
        ),
        reverse=True,
    )
    return groups


def _group_bucket_key(driver: dict[str, Any]) -> str:
    return str(
        driver.get("aspect_bucket")
        or driver.get("duplication_group")
        or "implementation_execution"
    )


def _age_days(driver: dict[str, Any], source_context: dict[str, Any]) -> float:
    runtime = dict(driver.get("runtime_provenance") or {})
    provider_execution = dict(driver.get("provider_execution") or {})
    truth_envelope = dict(runtime.get("truth_envelope") or {})
    source_class = str(source_context.get("source_class") or "market_series")
    observed = (
        _parse_datetime(provider_execution.get("fetched_at")) if source_class in {"market_series", "benchmark_move"} else None
    ) or (
        _parse_datetime(truth_envelope.get("retrieved_at_utc")) if source_class in {"market_series", "benchmark_move"} else None
    ) or (
        _parse_datetime(runtime.get("observed_at")) if source_class in {"market_series", "benchmark_move"} else None
    ) or (
        _parse_datetime(truth_envelope.get("as_of_utc")) if source_class in {"market_series", "benchmark_move"} else None
    ) or (
        _parse_datetime(source_context.get("availability_date"))
        or _parse_datetime(source_context.get("release_date"))
        or _parse_datetime(driver.get("as_of"))
    )
    if observed is None:
        return 99.0
    now = datetime.now(timezone.utc)
    delta = now - observed
    return max(0.0, delta.total_seconds() / 86400.0)


def _freshness_relevance_score(*, age_days: float, policy: dict[str, float]) -> float:
    half_life = max(float(policy.get("half_life_days") or 0.0), 0.15)
    decay = math.exp(-math.log(2.0) * age_days / half_life)
    return 22.0 * decay


def _threshold_state(driver: dict[str, Any]) -> str:
    monitoring = dict(driver.get("monitoring_condition") or {})
    trigger_support = dict(monitoring.get("trigger_support") or {})
    state = str(trigger_support.get("threshold_state") or "").strip().lower()
    if state:
        return state
    path_note = str(driver.get("path_risk_note") or "").lower()
    if "breach risk is high" in path_note:
        return "watch"
    return "stable"


def _novelty_class(
    *,
    driver: dict[str, Any],
    source_context: dict[str, Any],
    age_days: float,
    policy: dict[str, float],
    threshold_state: str,
) -> tuple[str, str | None]:
    source_class = str(source_context.get("source_class") or "market_series")
    market_confirmation = str(source_context.get("market_confirmation") or "limited")
    significance_delta = float(source_context.get("significance_delta") or 0.0)
    direction = str(driver.get("direction") or "neutral")
    lead_window_days = float(policy.get("lead_window_days") or 1.0)

    if threshold_state == "breached":
        return "threshold_break", "A watched threshold was breached today."
    if age_days <= lead_window_days:
        if source_class == "macro_release":
            return "new", None
        if source_class in {"policy_event", "geopolitical_news"}:
            if market_confirmation == "strong":
                return "new", None
            return "continuation", None
        if significance_delta >= 2.0 or str(driver.get("magnitude") or "") == "significant":
            return "escalation", None
        if direction == "neutral":
            return "continuation", None
        return "new", None
    if threshold_state == "watch":
        return "reactivated", "Fresh threshold pressure is pulling an older theme back into today’s brief."
    if market_confirmation == "strong" and significance_delta >= 0.8:
        return "reactivated", "Fresh market confirmation is reactivating an older theme."
    if direction == "down" and significance_delta >= 1.2:
        return "reversal", None
    return "continuation", None


def _decision_deltas(
    *,
    driver: dict[str, Any],
    novelty_class: str,
    freshness_score: float,
) -> tuple[str, str, float]:
    actionability = str(driver.get("actionability_class") or "")
    next_action = str(driver.get("next_action") or "Monitor")
    affected_holdings = list(driver.get("affected_holdings") or [])
    affected_candidates = list(driver.get("affected_candidates") or [])
    affected_sleeves = list(driver.get("affected_sleeves") or [])
    bucket = str(driver.get("primary_effect_bucket") or "market")

    if affected_holdings:
        action_delta = "changes a mapped holding review today"
        score = 18.0
    elif "review" in next_action.lower():
        action_delta = "reopens active sleeve review today"
        score = 15.0
    elif actionability == "sleeve_decision" and affected_sleeves:
        action_delta = "changes sleeve timing and hurdle today"
        score = 11.0
    elif affected_candidates or affected_sleeves:
        action_delta = "changes implementation watch, not execution yet"
        score = 7.0
    else:
        action_delta = "adds context but does not change today’s action frame"
        score = 2.0

    if novelty_class == "continuation":
        score -= 5.0
    elif novelty_class in {"reactivated", "threshold_break", "reversal"}:
        score += 5.0
    elif novelty_class in {"new", "escalation"}:
        score += 3.0

    if freshness_score < 6.0:
        score -= 4.0

    portfolio_delta = {
        "duration": "changes the bond-sleeve timing hurdle",
        "inflation": "changes the duration-versus-real-assets balance",
        "energy": "changes the real-assets inflation-hedge read",
        "real_assets": "changes hedge quality across real-assets sleeves",
        "credit": "changes the carry and risk-budget read",
        "dollar_fx": "changes the hurdle for global risk-taking",
        "policy": "changes the policy backdrop used in sleeve timing",
        "growth": "changes the growth-risk leadership read",
        "volatility": "changes the risk-regime guardrail, not the core stance",
    }.get(bucket, "changes the current portfolio read only modestly")

    return action_delta, portfolio_delta, max(score, 0.0)


def _is_backdrop(
    *,
    driver: dict[str, Any],
    source_context: dict[str, Any],
    age_days: float,
    policy: dict[str, float],
    novelty_class: str,
    freshness_score: float,
    decision_delta_score: float,
) -> bool:
    if novelty_class in {"reactivated", "threshold_break", "new", "escalation", "reversal"}:
        return False
    if freshness_score <= 4.0:
        return True
    if age_days >= float(policy.get("backdrop_after_days") or 99.0):
        return True
    if str(source_context.get("source_class") or "") == "macro_release" and decision_delta_score < 11.0:
        return True
    if str(driver.get("actionability_class") or "") == "contextual_monitor":
        return True
    return False


def _lead_lane(
    *,
    driver: dict[str, Any],
    novelty_class: str,
    freshness_score: float,
    decision_delta_score: float,
    is_backdrop: bool,
) -> str:
    if is_backdrop:
        return "regime_context"
    if str(driver.get("sufficiency_state") or "") in {"thin", "insufficient"}:
        return "support_context"
    if novelty_class in {"continuation"} and decision_delta_score < 12.0:
        return "support_context"
    if freshness_score >= 8.0 and decision_delta_score >= 10.0:
        return "daily_lead"
    return "support_context"


def _novelty_bonus(novelty_class: str) -> float:
    return {
        "threshold_break": 12.0,
        "reactivated": 8.0,
        "reversal": 7.0,
        "escalation": 6.0,
        "new": 5.0,
        "continuation": -5.0,
    }.get(novelty_class, 0.0)


def _group_summary(group_id: str, items: list[dict[str, Any]]) -> str:
    fresh = sum(1 for item in items if str(item.get("novelty_class") or "") in {"new", "escalation", "reactivated", "threshold_break", "reversal"})
    backdrop = sum(1 for item in items if str(item.get("lead_lane") or "") == "regime_context")
    label = _GROUP_LABELS.get(group_id, group_id.replace("_", " "))
    event_summary = _event_group_summary(items)
    if event_summary:
        return f"{label}: {event_summary}"
    if fresh and backdrop:
        return f"{label}: {fresh} active driver{'s' if fresh != 1 else ''}, {backdrop} backdrop item{'s' if backdrop != 1 else ''}."
    if fresh:
        return f"{label}: {fresh} active secondary driver{'s' if fresh != 1 else ''}."
    return f"{label}: {len(items)} background or support item{'s' if len(items) != 1 else ''}."


def _event_group_summary(items: list[dict[str, Any]]) -> str | None:
    clusters = {
        str(item.get("event_cluster_id") or "").strip()
        for item in items
        if str(item.get("event_cluster_id") or "").strip()
    }
    if not clusters:
        return None
    channels: list[str] = []
    assets: list[str] = []
    for item in items:
        for channel in list(item.get("market_channels") or []):
            text = str(channel or "").replace("_", " ").strip()
            if text and text not in channels:
                channels.append(text)
        for asset in list(item.get("confirmation_assets") or []):
            text = str(asset or "").strip()
            if text and text not in assets:
                assets.append(text)
    cluster_text = f"{len(clusters)} event cluster{'s' if len(clusters) != 1 else ''}"
    if assets:
        return f"{cluster_text}; watch {', '.join(assets[:4])} for confirmation."
    if channels:
        return f"{cluster_text}; watch {', '.join(channels[:4])} transmission."
    return cluster_text


def _parse_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    candidate = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{candidate}T00:00:00+00:00")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
