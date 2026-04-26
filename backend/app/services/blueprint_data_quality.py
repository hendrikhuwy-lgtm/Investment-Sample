from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.config import Settings


def _parse_date(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if len(text) == 10:
            return datetime.fromisoformat(f"{text}T00:00:00+00:00")
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def classify_freshness(
    *,
    observed_at: Any,
    now: datetime,
    fresh_hours: float,
    aging_hours: float | None = None,
    stale_hours: float | None = None,
) -> str:
    parsed = _parse_date(observed_at)
    if parsed is None:
        return "unknown"
    age_hours = max(0.0, (now - parsed).total_seconds() / 3600.0)
    aging_cutoff = aging_hours if aging_hours is not None else fresh_hours * 2.0
    stale_cutoff = stale_hours if stale_hours is not None else fresh_hours * 4.0
    if age_hours <= fresh_hours:
        return "fresh"
    if age_hours <= aging_cutoff:
        return "aging"
    if age_hours <= stale_cutoff:
        return "stale"
    return "quarantined"


def summarize_blueprint_data_quality(
    *,
    candidates: list[dict[str, Any]],
    citations: list[dict[str, Any]],
    regime_context: dict[str, Any],
    settings: Settings,
    now: datetime,
) -> dict[str, Any]:
    metrics: list[dict[str, Any]] = []

    factsheet_states: list[str] = []
    for candidate in candidates:
        observed_at = candidate.get("factsheet_asof") or candidate.get("last_verified_at")
        fallback_used = not bool(candidate.get("factsheet_asof")) and bool(candidate.get("last_verified_at"))
        state = classify_freshness(
            observed_at=observed_at,
            now=now,
            fresh_hours=float(settings.blueprint_factsheet_max_age_days * 24),
            aging_hours=float(settings.blueprint_factsheet_max_age_days * 24 * 1.5),
            stale_hours=float(settings.blueprint_factsheet_max_age_days * 24 * 2),
        )
        factsheet_states.append(state)
        metrics.append(
            {
                "metric_key": f"{candidate.get('symbol')}_factsheet",
                "metric_name": f"{candidate.get('symbol')} factsheet freshness",
                "classification": state,
                "observed_at": observed_at,
                "provenance": ["candidate_factsheet_asof", "candidate_last_verified_at"],
                "fallback_used": fallback_used,
                "fallback_source": "last_verified_at" if fallback_used else None,
                "quarantined_from_scoring": state == "quarantined",
                "stale_reason": "factsheet freshness exceeded policy threshold" if state in {"stale", "quarantined"} else None,
            }
        )

    macro_states: list[str] = []
    for monitor in list(regime_context.get("monitor_records") or []):
        observed_at = monitor.get("observed_at") or monitor.get("retrieved_at")
        fallback_used = bool(monitor.get("fallback_used")) or (not bool(monitor.get("observed_at")) and bool(monitor.get("retrieved_at")))
        state = classify_freshness(
            observed_at=observed_at,
            now=now,
            fresh_hours=float(settings.blueprint_macro_freshness_hours),
            aging_hours=float(settings.blueprint_macro_freshness_hours * 2),
            stale_hours=float(settings.blueprint_macro_freshness_hours * 4),
        )
        macro_states.append(state)
        metrics.append(
            {
                "metric_key": str(monitor.get("metric_key") or "unknown"),
                "metric_name": str(monitor.get("metric_name") or monitor.get("metric_key") or "Unknown metric"),
                "classification": state,
                "observed_at": observed_at,
                "provenance": ["regime_monitor"],
                "fallback_used": fallback_used,
                "fallback_source": "retrieved_at" if not monitor.get("observed_at") and monitor.get("retrieved_at") else ("cached_previous_value" if monitor.get("fallback_used") else None),
                "quarantined_from_scoring": state == "quarantined",
                "stale_reason": (
                    f"observed_at older than {settings.blueprint_macro_freshness_hours * 4} hours"
                    if state == "quarantined"
                    else ("observed_at exceeded freshness threshold" if state == "stale" else None)
                ),
            }
        )

    citation_states: list[str] = []
    for citation in citations:
        state = classify_freshness(
            observed_at=citation.get("retrieved_at"),
            now=now,
            fresh_hours=float(settings.blueprint_citation_health_max_age_days * 24),
            aging_hours=float(settings.blueprint_citation_health_max_age_days * 24 * 2),
            stale_hours=float(settings.blueprint_citation_health_max_age_days * 24 * 4),
        )
        citation_states.append(state)

    ordered = ["fresh", "aging", "stale", "quarantined", "unknown"]

    def _worst(values: list[str]) -> str:
        if not values:
            return "unknown"
        present = {value for value in values}
        for level in reversed(ordered):
            if level in present:
                return level
        return "unknown"

    worst = _worst(factsheet_states + macro_states + citation_states)
    quarantined_metrics = [item["metric_name"] for item in metrics if item["classification"] == "quarantined"]
    stale_metrics = [item["metric_name"] for item in metrics if item["classification"] in {"stale", "quarantined"}]
    aging_metrics = [item["metric_name"] for item in metrics if item["classification"] == "aging"]

    fallback_flags = [str(item.get("metric_name") or item.get("metric_key") or "unknown") for item in metrics if item.get("fallback_used")]
    exclusions = [str(item.get("metric_name") or item.get("metric_key") or "unknown") for item in metrics if item.get("quarantined_from_scoring")]
    confidence = "high"
    if quarantined_metrics:
        confidence = "low"
    elif stale_metrics or fallback_flags:
        confidence = "medium"

    banner = None
    if quarantined_metrics:
        banner = f"Data quality warning: {len(quarantined_metrics)} metrics are quarantined from downstream scoring."
    elif stale_metrics:
        banner = f"Data quality warning: {len(stale_metrics)} metrics are stale."
    elif fallback_flags:
        banner = f"Data quality notice: {len(fallback_flags)} metrics are using fallback timestamps or cached prior observations."

    healthy_refresh_points = [
        _parse_date(item.get("observed_at"))
        for item in metrics
        if item.get("classification") in {"fresh", "aging"} and item.get("observed_at")
    ]
    last_successful_refresh = max(
        [point.isoformat() for point in healthy_refresh_points if point is not None] or [None]
    )

    return {
        "freshness": worst,
        "confidence": confidence,
        "freshness_score": max(
            0,
            100
            - (len(aging_metrics) * 5)
            - (len(stale_metrics) * 12)
            - (len(quarantined_metrics) * 20),
        ),
        "stale_metrics_count": len(stale_metrics),
        "quarantined_metrics_count": len(quarantined_metrics),
        "fallback_metrics_count": len(fallback_flags),
        "quarantine_flags": quarantined_metrics,
        "fallback_flags": fallback_flags,
        "exclusions": exclusions,
        "broken_citation_count": 0,
        "last_successful_refresh": last_successful_refresh,
        "banner": banner,
        "policy": {
            "factsheet_max_age_days": settings.blueprint_factsheet_max_age_days,
            "macro_freshness_hours": settings.blueprint_macro_freshness_hours,
            "citation_health_max_age_days": settings.blueprint_citation_health_max_age_days,
            "fallback_max_age_hours": settings.blueprint_data_fallback_max_age_hours,
            "quarantined_metrics_excluded_from_scoring": True,
        },
        "metrics": metrics,
    }
