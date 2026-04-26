from __future__ import annotations

from typing import Any, Mapping


EVIDENCE_EVENT_TYPES = {
    "truth_change",
    "evidence_document_added",
    "evidence_claim_added",
    "evidence_mapping_added",
    "evidence_gap_added",
    "tax_assumption_added",
    "source_integrity_changed",
    "document_support_changed",
    "data_confidence_changed",
    "candidate_source_strengthened",
    "candidate_source_weakened",
    "source_completion_changed",
    "truth_confidence_changed",
    "index_scope_added",
    "quick_brief_evidence_added",
}

DECISION_EVENT_TYPES = {
    "leader_changed",
    "recommendation_state_changed",
    "score_band_improved",
    "score_band_weakened",
    "candidate_score_moved",
    "candidate_deployability_changed",
    "decision_changed",
    "deployment_state_changed",
}

PORTFOLIO_DRIFT_EVENT_TYPES = {
    "portfolio_fit",
    "sleeve_posture_changed",
    "portfolio_drift_changed",
    "funding_path_changed",
}

TIMING_EVENT_TYPES = {
    "market_path_strengthened",
    "market_path_weakened",
    "candidate_market_strengthened",
    "candidate_market_weakened",
    "timing_state_changed",
    "forecast_support_strengthened",
    "forecast_support_weakened",
    "forecast_trigger_threshold_crossed",
    "forecast_anomaly_opened",
    "forecast_anomaly_resolved",
}

MARKET_IMPACT_EVENT_TYPES = {
    "market_implication_changed",
}

FRESHNESS_EVENT_TYPES = {
    "freshness_risk",
}

UPGRADE_EVENT_TYPES = {
    "forecast_support_strengthened",
    "forecast_anomaly_resolved",
    "recommendation_review_recorded",
    "blocker_cleared",
    "score_band_improved",
    "market_path_strengthened",
    "candidate_source_strengthened",
    "index_scope_added",
    "quick_brief_evidence_added",
}

DOWNGRADE_EVENT_TYPES = {
    "forecast_support_weakened",
    "forecast_trigger_threshold_crossed",
    "forecast_anomaly_opened",
    "blocker_opened",
    "score_band_weakened",
    "market_path_weakened",
    "candidate_source_weakened",
}

BLOCKER_EVENT_TYPES = {
    "boundary_change",
    "blocker",
    "blocker_opened",
    "blocker_cleared",
}

SYSTEM_EVENT_TYPES = {
    "rebuild",
    "runtime_degraded",
    "background_refresh_failed",
}

STATE_RANK = {
    "blocked": 0,
    "research only": 1,
    "research_only": 1,
    "monitor": 2,
    "watch": 2,
    "review": 3,
    "reviewable": 3,
    "review required": 3,
    "review_required": 3,
    "eligible": 4,
    "eligible now": 4,
    "eligible_now": 4,
    "ready": 5,
}


def _normalized(value: str | None | Any) -> str:
    return str(value or "").strip().lower()


def _narrative(event: Mapping[str, Any]) -> str:
    return " ".join(
        part for part in [
            _normalized(event.get("summary")),
            _normalized(event.get("reason_summary")),
            _normalized(event.get("implication_summary")),
            _normalized(event.get("previous_state")),
            _normalized(event.get("current_state")),
        ]
        if part
    )


def _state_rank(value: str | None) -> int | None:
    normalized = _normalized(value)
    if not normalized:
        return None
    return STATE_RANK.get(normalized)


def _direction(event_type: str, previous_state: str | None, current_state: str | None, narrative: str) -> str:
    if event_type in UPGRADE_EVENT_TYPES:
        return "upgrade"
    if event_type in DOWNGRADE_EVENT_TYPES or event_type in BLOCKER_EVENT_TYPES:
        return "downgrade"

    previous_rank = _state_rank(previous_state)
    current_rank = _state_rank(current_state)
    if previous_rank is not None and current_rank is not None:
        if current_rank > previous_rank:
            return "upgrade"
        if current_rank < previous_rank:
            return "downgrade"

    if any(token in narrative for token in ["eligible now", "evidence sufficient", "cleared", "resolved", "strengthened"]):
        return "upgrade"
    if any(token in narrative for token in ["blocked", "blocker", "downgrade", "weakened", "aging", "stale", "conflict"]):
        return "downgrade"
    return "neutral"


def classify_event(event: Mapping[str, Any]) -> dict[str, Any]:
    event_type = _normalized(event.get("event_type"))
    previous_state = event.get("previous_state")
    current_state = event.get("current_state")
    narrative = _narrative(event)

    is_blocker_change = (
        event_type in BLOCKER_EVENT_TYPES
        or "blocked" in _normalized(current_state)
        or (
            "blocker" in narrative
            and not (
                any(token in narrative for token in ["cleared blocker", "blocker cleared", "resolved blocker"])
                or ("blocker" in narrative and "cleared" in narrative)
                or ("blocker" in narrative and "resolved" in narrative)
            )
        )
    )

    if event_type in DECISION_EVENT_TYPES:
        category = "decision"
    elif is_blocker_change:
        category = "blocker"
    elif event_type in MARKET_IMPACT_EVENT_TYPES:
        category = "market_impact"
    elif event_type in PORTFOLIO_DRIFT_EVENT_TYPES:
        category = "portfolio_drift"
    elif event_type in TIMING_EVENT_TYPES:
        category = "timing"
    elif event_type in FRESHNESS_EVENT_TYPES or any(token in narrative for token in ["freshness", "stale", "aging", "degraded"]):
        category = "freshness_risk"
    elif event_type in EVIDENCE_EVENT_TYPES or any(token in narrative for token in ["evidence", "source", "document", "truth"]):
        category = "source_evidence"
    elif event_type in SYSTEM_EVENT_TYPES:
        category = "system"
    elif any(token in narrative for token in ["sleeve", "allocation", "capital", "target"]):
        category = "portfolio_drift"
    else:
        category = "system" if event_type in SYSTEM_EVENT_TYPES else "source_evidence"

    ui_category = {
        "blocker": "blocker_changes",
        "source_evidence": "evidence",
        "decision": "decision",
        "market_impact": "market_impact",
        "portfolio_drift": "portfolio_drift",
        "timing": "timing",
        "freshness_risk": "freshness_risk",
        "system": "system",
    }.get(category, category)

    direction = _direction(event_type, previous_state, current_state, narrative)
    return {
        "category": category,
        "ui_category": ui_category,
        "direction": direction,
        "is_blocker_change": is_blocker_change,
    }


def matches_category(event: Mapping[str, Any], category: str) -> bool:
    normalized = _normalized(category) or "all"
    if normalized == "all":
        return True
    if normalized == "requires_review":
        return bool(event.get("requires_review"))
    if normalized == "upgrades":
        return _normalized(event.get("direction")) == "upgrade"
    if normalized == "downgrades":
        return _normalized(event.get("direction")) == "downgrade"
    if normalized == "blocker_changes":
        return bool(event.get("is_blocker_change"))
    if normalized == "audit_only":
        materiality_class = _normalized(event.get("materiality_class"))
        materiality_status = _normalized(event.get("materiality_status"))
        render_mode = _normalized(event.get("render_mode"))
        return (
            materiality_class == "audit_only"
            or materiality_status in {"unresolved_driver_missing", "raw_movement_only"}
            or render_mode in {"compact_audit", "grouped_audit", "hidden_audit"}
        )
    if normalized == "blocker":
        return bool(event.get("is_blocker_change")) or _normalized(event.get("category")) == "blocker"
    if normalized == "evidence":
        return _normalized(event.get("category")) == "source_evidence" or _normalized(event.get("ui_category")) == "evidence"
    if normalized == "source_evidence":
        return _normalized(event.get("category")) == "source_evidence" or _normalized(event.get("ui_category")) == "evidence"
    if normalized == "sleeve":
        return _normalized(event.get("category")) in {"decision", "portfolio_drift"} or _normalized(event.get("ui_category")) == "sleeve"
    if normalized == "portfolio_drift":
        return _normalized(event.get("category")) == "portfolio_drift" or _normalized(event.get("ui_category")) == "sleeve"
    return _normalized(event.get("ui_category")) == normalized
