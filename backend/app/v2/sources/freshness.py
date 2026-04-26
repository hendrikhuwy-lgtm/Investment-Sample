from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.v2.sources.types import FreshnessEvaluation, FreshnessPolicy


def coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        if len(normalized) == 10:
            try:
                parsed = datetime.fromisoformat(f"{normalized}T00:00:00+00:00")
            except ValueError:
                return None
        else:
            return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def evaluate_freshness(
    observed_at: Any,
    *,
    retrieved_at: Any = None,
    policy: FreshnessPolicy,
    now: datetime | None = None,
) -> FreshnessEvaluation:
    current_time = now or datetime.now(UTC)
    observed_dt = coerce_datetime(observed_at)
    retrieved_dt = coerce_datetime(retrieved_at)
    anchor = observed_dt or retrieved_dt
    if policy.not_applicable:
        return FreshnessEvaluation(
            state="not_applicable",
            age_seconds=None,
            observed_at=observed_dt,
            retrieved_at=retrieved_dt,
            policy=policy,
        )
    if anchor is None:
        return FreshnessEvaluation(
            state="unknown",
            age_seconds=None,
            observed_at=observed_dt,
            retrieved_at=retrieved_dt,
            policy=policy,
        )
    age_seconds = max(0, int((current_time - anchor).total_seconds()))
    state = "fresh"
    if policy.expires_seconds is not None and age_seconds > policy.expires_seconds:
        state = "expired"
    elif policy.stale_seconds is not None and age_seconds > policy.stale_seconds:
        state = "stale"
    elif policy.fresh_seconds is not None and age_seconds > policy.fresh_seconds:
        state = "aging"
    return FreshnessEvaluation(
        state=state,
        age_seconds=age_seconds,
        observed_at=observed_dt,
        retrieved_at=retrieved_dt,
        policy=policy,
    )

