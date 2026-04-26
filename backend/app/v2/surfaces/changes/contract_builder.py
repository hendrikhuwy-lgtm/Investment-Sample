from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.v2.core.change_ledger import get_diffs
from app.v2.core.holdings_overlay import apply_overlay
from app.v2.core.domain_objects import utc_now_iso
from app.v2.surfaces.changes.blueprint_daily_scan import latest_blueprint_daily_source_scan
from app.v2.surfaces.common import empty_section, ready_section, surface_state
from app.v2.surfaces.changes.classification import classify_event, matches_category
from app.v2.surfaces.changes.decision_read_model import build_decision_read_fields
from app.v2.surfaces.changes.query import DEFAULT_CHANGES_TIMEZONE, ChangeFeedQuery, normalize_query


_CONTRACT_VERSION = "0.3.0"
_AVAILABLE_CATEGORIES = [
    "all",
    "requires_review",
    "upgrades",
    "downgrades",
    "blocker_changes",
    "evidence",
    "sleeve",
    "freshness_risk",
    "decision",
    "market_impact",
    "portfolio_drift",
    "source_evidence",
    "blocker",
    "timing",
    "audit_only",
    "system",
]

_REPORT_TAB_FOR_SURFACE: dict[str, str] = {
    "blueprint_explorer": "investment_case",
    "candidate_report": "investment_case",
    "compare": "investment_case",
    "daily_brief": "evidence",
    "portfolio": "portfolio_fit",
    "changes": "evidence",
}

_SUPPRESSED_EVENT_TYPES = {"rebuild"}
_FULL_INVESTOR_MATERIALITY = {
    "material",
    "material_source_backed",
    "material_portfolio_backed",
    "historical_source_backed",
}
_MAIN_MATERIALITY_CLASSES = {"investor_material", "review_material"}
_AUDIT_MATERIALITY_CLASSES = {"audit_only", "system_only"}
_AUDIT_RENDER_MODES = {"compact_audit", "grouped_audit", "hidden_audit"}


def _net_impact(event_types: list[str]) -> str:
    normalized = {str(event_type or "").strip().lower() for event_type in event_types}
    if {"truth_change", "boundary_change", "forecast_trigger_threshold_crossed"} & normalized:
        return "material"
    if {"forecast_support_weakened", "forecast_support_strengthened", "forecast_anomaly_opened", "forecast_anomaly_resolved"} & normalized:
        return "minor"
    if normalized and normalized <= {"interpretation_change"}:
        return "minor"
    if normalized:
        return "minor"
    return "none"


def _impact_level(event_type: str) -> str:
    normalized = str(event_type or "").strip().lower()
    if normalized in {
        "truth_change",
        "boundary_change",
        "forecast_trigger_threshold_crossed",
        "recommendation_state_changed",
        "leader_changed",
        "deployment_state_changed",
        "decision_changed",
        "blocker_opened",
    }:
        return "high"
    if normalized in {
        "interpretation_change",
        "forecast_support_weakened",
        "forecast_support_strengthened",
        "forecast_anomaly_opened",
        "forecast_anomaly_resolved",
        "blocker_cleared",
        "score_band_improved",
        "score_band_weakened",
        "candidate_score_moved",
        "candidate_deployability_changed",
        "market_path_strengthened",
        "market_path_weakened",
        "candidate_market_strengthened",
        "candidate_market_weakened",
        "candidate_source_strengthened",
        "candidate_source_weakened",
        "source_completion_changed",
        "truth_confidence_changed",
        "index_scope_added",
        "quick_brief_evidence_added",
        "sleeve_posture_changed",
        "portfolio_drift_changed",
        "funding_path_changed",
        "source_integrity_changed",
        "document_support_changed",
    }:
        return "medium"
    return "low"


def _report_tab_for_surface(surface_id: str) -> str:
    return _REPORT_TAB_FOR_SURFACE.get(str(surface_id or "").strip(), "investment_case")


def _event_trading_day(event: dict[str, Any], timezone: str) -> str | None:
    raw = str(event.get("changed_at_utc") or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(ZoneInfo(timezone or DEFAULT_CHANGES_TIMEZONE)).date().isoformat()


def _actionability(level: str, requires_review: bool) -> str:
    if level == "high":
        return "review" if requires_review else "act_now"
    if level == "medium":
        return "review" if requires_review else "monitor"
    return "monitor" if requires_review else "no_action"


def _enrich_event(event: dict[str, Any], surface_id: str, *, timezone: str = DEFAULT_CHANGES_TIMEZONE) -> dict[str, Any]:
    event_type = str(event.get("event_type") or "")
    level = str(event.get("impact_level") or _impact_level(event_type) or "low")
    requires_review = bool(event.get("requires_review")) or level in {"high", "medium"}

    previous_state = event.get("previous_state")
    current_state = event.get("current_state")
    implication_summary = event.get("implication_summary") or event.get("summary")
    portfolio_consequence = event.get("portfolio_consequence")
    next_action = event.get("next_action") or (
        "Review this change before the next rebalancing decision." if requires_review else None
    )
    what_would_reverse = event.get("what_would_reverse") or (
        "A return to the prior evidence state or boundary condition would reverse this change."
        if requires_review else None
    )
    report_tab = event.get("report_tab") or _report_tab_for_surface(surface_id)

    deep_link_target = event.get("deep_link_target")
    if not deep_link_target and event.get("candidate_id"):
        deep_link_target = {
            "target_type": "candidate_report",
            "target_id": event.get("candidate_id"),
            "tab": report_tab,
            "section": report_tab,
            "anchor": event.get("event_id"),
        }

    if isinstance(deep_link_target, dict):
        deep_link_target = {
            **deep_link_target,
            "section": deep_link_target.get("section") or report_tab,
            "anchor": deep_link_target.get("anchor") or event.get("event_id"),
        }

    classified = classify_event(
        {
            **event,
            "previous_state": previous_state,
            "current_state": current_state,
            "implication_summary": implication_summary,
            "requires_review": requires_review,
        }
    )
    decision_fields = build_decision_read_fields(
        {
            **event,
            "previous_state": previous_state,
            "current_state": current_state,
            "implication_summary": implication_summary,
            "portfolio_consequence": portfolio_consequence,
            "next_action": next_action,
            "what_would_reverse": what_would_reverse,
            "deep_link_target": deep_link_target,
            "report_tab": report_tab,
        },
        surface_id=surface_id,
        category=classified["category"],
        direction=classified["direction"],
        severity=level,
        requires_review=requires_review,
    )

    return {
        **event,
        **decision_fields,
        "previous_state": previous_state,
        "current_state": current_state,
        "implication_summary": implication_summary,
        "portfolio_consequence": portfolio_consequence,
        "next_action": next_action,
        "what_would_reverse": what_would_reverse,
        "requires_review": requires_review,
        "report_tab": report_tab,
        "impact_level": level,
        "severity": level,
        "actionability": _actionability(level, requires_review),
        "trading_day": _event_trading_day(event, timezone),
        "driver": {
            "family": event.get("change_trigger") or classified["category"],
            "name": event.get("reason_summary") or event_type,
            "previous_value": previous_state,
            "current_value": current_state,
            "change": event.get("score_delta") or None,
            "threshold": None,
        },
        "affected": {
            "candidate_id": event.get("candidate_id"),
            "symbol": event.get("symbol") or None,
            "sleeve_id": event.get("sleeve_id"),
        },
        "implication": {
            "summary": implication_summary or event.get("summary") or "",
            "why_it_matters": portfolio_consequence or implication_summary or event.get("summary") or "",
            "next_step": next_action or "",
            "reversal_condition": what_would_reverse,
        },
        "evidence_refs": event.get("evidence_refs") or [],
        "source_freshness": event.get("source_freshness") or {},
        "deep_link_target": deep_link_target,
        "category": classified["category"],
        "ui_category": classified["ui_category"],
        "direction": classified["direction"],
        "is_blocker_change": classified["is_blocker_change"],
    }


def _is_visible_event(event: dict[str, Any]) -> bool:
    return str(event.get("event_type") or "").strip().lower() not in _SUPPRESSED_EVENT_TYPES


def _scope_events(events: list[dict[str, Any]], query: ChangeFeedQuery) -> list[dict[str, Any]]:
    scoped = events
    if query.candidate_id:
        scoped = [event for event in scoped if str(event.get("candidate_id") or "").strip() == query.candidate_id]
    return scoped


def _is_main_material_event(event: dict[str, Any]) -> bool:
    detail = dict(event.get("change_detail") or {})
    materiality_class = str(event.get("materiality_class") or detail.get("materiality_class") or "").strip()
    render_mode = str(event.get("render_mode") or detail.get("render_mode") or "").strip()
    materiality_status = str(event.get("materiality_status") or detail.get("materiality_status") or "").strip()
    if render_mode in _AUDIT_RENDER_MODES or materiality_class in _AUDIT_MATERIALITY_CLASSES:
        return False
    if materiality_status in {"suppressed_not_material", "raw_movement_only", "unresolved_driver_missing"}:
        return False
    return materiality_class in _MAIN_MATERIALITY_CLASSES or materiality_status in _FULL_INVESTOR_MATERIALITY


def _is_audit_event(event: dict[str, Any]) -> bool:
    detail = dict(event.get("change_detail") or {})
    materiality_class = str(event.get("materiality_class") or detail.get("materiality_class") or "").strip()
    render_mode = str(event.get("render_mode") or detail.get("render_mode") or "").strip()
    materiality_status = str(event.get("materiality_status") or detail.get("materiality_status") or "").strip()
    return (
        materiality_class == "audit_only"
        or render_mode in _AUDIT_RENDER_MODES
        or materiality_status in {"unresolved_driver_missing", "raw_movement_only"}
    )


def _is_suppressed_event(event: dict[str, Any]) -> bool:
    detail = dict(event.get("change_detail") or {})
    materiality_class = str(event.get("materiality_class") or detail.get("materiality_class") or "").strip()
    render_mode = str(event.get("render_mode") or detail.get("render_mode") or "").strip()
    materiality_status = str(event.get("materiality_status") or detail.get("materiality_status") or "").strip()
    return materiality_class == "suppressed" or render_mode == "suppressed" or materiality_status == "suppressed_not_material"


def _build_summary(material_events: list[dict[str, Any]], *, audit_events: list[dict[str, Any]], suppressed_count: int) -> dict[str, int | bool]:
    material_upgrades = sum(1 for event in material_events if str(event.get("direction") or "").strip().lower() == "upgrade")
    material_downgrades = sum(1 for event in material_events if str(event.get("direction") or "").strip().lower() == "downgrade")
    return {
        "total_changes": len(material_events),
        "upgrades": material_upgrades,
        "downgrades": material_downgrades,
        "blocker_changes": sum(1 for event in material_events if bool(event.get("is_blocker_change"))),
        "requires_review": sum(1 for event in material_events if bool(event.get("requires_review"))),
        "material_changes": len(material_events),
        "material_upgrades": material_upgrades,
        "material_downgrades": material_downgrades,
        "audit_only_count": len(audit_events),
        "suppressed_count": suppressed_count,
        "no_material_change": len(material_events) == 0,
    }


def _available_sleeves(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[tuple[str | None, str], int] = {}
    for event in events:
        sleeve_name = str(event.get("sleeve_name") or "").strip() or "Blueprint"
        sleeve_id = str(event.get("sleeve_id") or "").strip() or None
        key = (sleeve_id, sleeve_name)
        counts[key] = counts.get(key, 0) + 1
    return [
        {
            "sleeve_id": sleeve_id,
            "sleeve_name": sleeve_name,
            "count": count,
        }
        for (sleeve_id, sleeve_name), count in sorted(counts.items(), key=lambda item: item[0][1].lower())
    ]


def _audit_group_key(event: dict[str, Any]) -> tuple[str, str, str]:
    detail = dict(event.get("change_detail") or {})
    audit_detail = dict(detail.get("audit_detail") or event.get("audit_detail") or {})
    reason = str(
        audit_detail.get("missing_driver_reason")
        or detail.get("missing_driver_reason")
        or event.get("missing_driver_reason")
        or "source driver not preserved"
    ).strip()
    category = str(event.get("category") or "audit").strip() or "audit"
    family = "historical_review_movements" if category in {"decision", "timing"} else f"historical_{category}_movements"
    return (family, category, reason)


def _compact_audit_event(event: dict[str, Any]) -> dict[str, Any]:
    detail = dict(event.get("change_detail") or {})
    audit_detail = dict(detail.get("audit_detail") or event.get("audit_detail") or {})
    transition = detail.get("state_transition") or event.get("state_transition") or {}
    return {
        "event_id": event.get("event_id"),
        "ticker": event.get("symbol"),
        "sleeve_id": event.get("sleeve_id"),
        "sleeve_name": event.get("sleeve_name"),
        "event_type": event.get("event_type"),
        "category": event.get("category"),
        "changed_at_utc": event.get("changed_at_utc"),
        "event_age_hours": event.get("event_age_hours"),
        "closure_status": event.get("closure_status"),
        "materiality_status": event.get("materiality_status"),
        "materiality_class": event.get("materiality_class"),
        "transition": {
            "from": transition.get("from") if isinstance(transition, dict) else event.get("previous_state"),
            "to": transition.get("to") if isinstance(transition, dict) else event.get("current_state"),
        },
        "missing_driver_reason": audit_detail.get("missing_driver_reason") or detail.get("missing_driver_reason"),
    }


def _build_audit_groups(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for event in events:
        grouped.setdefault(_audit_group_key(event), []).append(event)
    groups: list[dict[str, Any]] = []
    for index, ((family, category, reason), group_events) in enumerate(
        sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0][0], item[0][1]))
    ):
        count = len(group_events)
        title = "Historical review movements without preserved drivers"
        if category not in {"decision", "timing"}:
            title = f"Historical {category.replace('_', ' ')} movements without preserved drivers".capitalize()
        groups.append(
            {
                "group_id": f"{family}_{index}",
                "title": title,
                "count": count,
                "summary": f"{count} historical review state movement{'s' if count != 1 else ''} available as audit context, not current investment signals.",
                "missing_driver_reason": reason,
                "render_mode": "grouped_audit",
                "materiality_class": "audit_only",
                "events_returned": min(count, 8),
                "has_more_events": count > 8,
                "events": [_compact_audit_event(event) for event in group_events[:8]],
            }
        )
    return groups


def _matches_filters(event: dict[str, Any], query: ChangeFeedQuery) -> bool:
    if query.sleeve_id and str(event.get("sleeve_id") or "").strip() != query.sleeve_id:
        return False
    if query.needs_review is not None and bool(event.get("requires_review")) is not query.needs_review:
        return False
    if not matches_category(event, query.category):
        return False
    return True


def _paginate(events: list[dict[str, Any]], query: ChangeFeedQuery) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    total_matching = len(events)
    start = 0
    if query.cursor:
        try:
            start = max(0, int(query.cursor))
        except ValueError:
            start = 0
    if query.limit is None:
        page = events[start:]
        returned = len(page)
        return page, {
            "limit": None,
            "returned": returned,
            "total_matching": total_matching,
            "has_more": False,
            "next_cursor": None,
        }

    end = start + query.limit
    page = events[start:end]
    has_more = end < total_matching
    return page, {
        "limit": query.limit,
        "returned": len(page),
        "total_matching": total_matching,
        "has_more": has_more,
        "next_cursor": str(end) if has_more else None,
    }


def _latest_event_metadata(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {
            "feed_freshness_state": "empty",
            "latest_event_at": None,
            "latest_event_age_days": None,
        }
    latest_raw = str(events[0].get("changed_at_utc") or "").strip()
    latest_dt = datetime.fromisoformat(latest_raw.replace("Z", "+00:00"))
    if latest_dt.tzinfo is None:
        latest_dt = latest_dt.replace(tzinfo=UTC)
    else:
        latest_dt = latest_dt.astimezone(UTC)
    age_days = max(0.0, (datetime.now(UTC) - latest_dt).total_seconds() / 86400.0)
    freshness_state = "current" if age_days <= 1.0 else "stale"
    return {
        "feed_freshness_state": freshness_state,
        "latest_event_at": latest_dt.isoformat(),
        "latest_event_age_days": round(age_days, 2),
    }


def _parse_utc(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _event_age_hours(event: dict[str, Any]) -> float | None:
    parsed = _parse_utc(event.get("changed_at_utc"))
    if parsed is None:
        return None
    return round(max(0.0, (datetime.now(UTC) - parsed).total_seconds() / 3600.0), 2)


def _scan_has_fresh_no_material_result(scan: dict[str, Any] | None) -> bool:
    if not scan:
        return False
    status = str(scan.get("status") or "").strip().lower()
    return status in {"success", "partial"} and bool(scan.get("no_material_change"))


def _annotate_event_lifecycle(
    event: dict[str, Any],
    query: ChangeFeedQuery,
    daily_source_scan: dict[str, Any] | None,
) -> dict[str, Any]:
    detail = dict(event.get("change_detail") or {})
    age_hours = detail.get("event_age_hours")
    if not isinstance(age_hours, int | float):
        age_hours = _event_age_hours(event)

    scan_status = str((daily_source_scan or {}).get("status") or "").strip() or None
    scan_day = str((daily_source_scan or {}).get("trading_day") or "").strip() or None
    scan_dt = _parse_utc((daily_source_scan or {}).get("latest_scan_at") or (daily_source_scan or {}).get("finished_at"))
    event_dt = _parse_utc(event.get("changed_at_utc"))
    event_day = str(event.get("trading_day") or "").strip() or None
    materiality_status = str(detail.get("materiality_status") or "material").strip()
    render_mode = str(detail.get("render_mode") or event.get("render_mode") or "").strip()
    materiality_class = str(detail.get("materiality_class") or event.get("materiality_class") or "").strip()
    if not materiality_class:
        materiality_class = (
            "audit_only"
            if render_mode in _AUDIT_RENDER_MODES or materiality_status in {"unresolved_driver_missing", "raw_movement_only"}
            else "suppressed"
            if materiality_status == "suppressed_not_material" or render_mode == "suppressed"
            else "review_material"
            if materiality_status in _FULL_INVESTOR_MATERIALITY
            else "system_only"
        )

    if query.surface_id == "blueprint_explorer" and daily_source_scan:
        is_current = bool(event_day and scan_day and event_day == scan_day)
        if _scan_has_fresh_no_material_result(daily_source_scan) and event_dt and scan_dt and event_dt <= scan_dt:
            is_current = False
        if materiality_class not in _MAIN_MATERIALITY_CLASSES:
            is_current = False
    else:
        is_current = age_hours is None or age_hours <= 24.0

    closure_status = str(detail.get("closure_status") or "").strip() or "open_review"
    if not is_current:
        if materiality_status in {"suppressed_not_material", "raw_movement_only"}:
            closure_status = "suppressed_not_material"
        elif event_day and scan_day and event_day != scan_day:
            closure_status = "stale_historical"
        elif isinstance(age_hours, int | float) and age_hours > 24.0:
            closure_status = "stale_historical"
        elif materiality_status == "unresolved_driver_missing":
            closure_status = "unresolved_driver_missing"
        elif closure_status == "open_actionable":
            closure_status = "open_review"

    detail.update(
        {
            "is_current": is_current,
            "event_age_hours": age_hours,
            "source_scan_status": scan_status,
            "closure_status": closure_status,
            "materiality_class": materiality_class,
        }
    )
    return {
        **event,
        "is_current": is_current,
        "event_age_hours": age_hours,
        "source_scan_status": scan_status,
        "closure_status": closure_status,
        "materiality_status": materiality_status,
        "materiality_class": materiality_class,
        "materiality_reason": detail.get("materiality_reason"),
        "change_detail": detail,
    }


def _passes_feed_visibility(
    event: dict[str, Any],
    query: ChangeFeedQuery,
    daily_source_scan: dict[str, Any] | None,
) -> bool:
    if not _is_visible_event(event):
        return False
    if _is_suppressed_event(event) or not _is_main_material_event(event):
        return False
    if query.window == "today":
        if event.get("is_current") is False:
            return False
        if _scan_has_fresh_no_material_result(daily_source_scan):
            event_dt = _parse_utc(event.get("changed_at_utc"))
            scan_dt = _parse_utc((daily_source_scan or {}).get("latest_scan_at") or (daily_source_scan or {}).get("finished_at"))
            if event_dt and scan_dt and event_dt <= scan_dt:
                return False
    return True


def _passes_audit_visibility(event: dict[str, Any], query: ChangeFeedQuery) -> bool:
    if not _is_visible_event(event) or _is_suppressed_event(event) or not _is_audit_event(event):
        return False
    if query.window == "today":
        return False
    return True


def _runtime_feed_state(current_state: str) -> str:
    if current_state == "empty":
        return current_state
    try:
        from app.v2.runtime.service import runtime_jobs_payload

        runtime = runtime_jobs_payload()
        if runtime.get("scheduler_enabled") and not runtime.get("worker_alive"):
            return "degraded_runtime"
    except Exception:
        return current_state
    return current_state


def build(
    surface_id: str,
    since_utc: str | None = None,
    *,
    window: str | None = None,
    candidate_id: str | None = None,
    sleeve_id: str | None = None,
    category: str | None = None,
    needs_review: bool | None = None,
    limit: int | None = None,
    cursor: str | None = None,
    timezone: str | None = None,
) -> dict[str, object]:
    query = normalize_query(
        surface_id=surface_id,
        since_utc=since_utc,
        window=window,
        candidate_id=candidate_id,
        sleeve_id=sleeve_id,
        category=category,
        needs_review=needs_review,
        limit=limit,
        cursor=cursor,
        timezone=timezone,
    )
    daily_source_scan = (
        latest_blueprint_daily_source_scan(query.timezone)
        if query.surface_id == "blueprint_explorer"
        else None
    )
    historical_raw_events = [event.model_dump(mode="json") for event in get_diffs(query.surface_id, None)]
    historical_enriched_events = [
        _annotate_event_lifecycle(
            _enrich_event(event, query.surface_id, timezone=query.timezone),
            query,
            daily_source_scan,
        )
        for event in historical_raw_events
    ]
    historical_material_events = [
        event
        for event in historical_enriched_events
        if _passes_feed_visibility(event, query, daily_source_scan)
    ]
    historical_audit_events = [
        event
        for event in historical_enriched_events
        if _passes_audit_visibility(event, query)
    ]
    historical_scoped_events = _scope_events(historical_material_events, query)
    raw_events = [event.model_dump(mode="json") for event in get_diffs(query.surface_id, query.since_utc)]
    enriched_events = [
        _annotate_event_lifecycle(
            _enrich_event(event, query.surface_id, timezone=query.timezone),
            query,
            daily_source_scan,
        )
        for event in raw_events
    ]
    material_events = [
        event
        for event in enriched_events
        if _passes_feed_visibility(event, query, daily_source_scan)
    ]
    audit_events = [
        event
        for event in enriched_events
        if _passes_audit_visibility(event, query)
    ]
    suppressed_count = sum(1 for event in enriched_events if _is_suppressed_event(event))
    scoped_material_events = _scope_events(material_events, query)
    scoped_audit_events = _scope_events(audit_events, query)
    scoped_events = scoped_audit_events if query.category == "audit_only" else scoped_material_events
    summary = _build_summary(scoped_material_events, audit_events=scoped_audit_events, suppressed_count=suppressed_count)
    available_sleeves = _available_sleeves(scoped_material_events + scoped_audit_events)
    audit_groups = _build_audit_groups(scoped_audit_events)
    filtered_events = [event for event in scoped_events if _matches_filters(event, query)]
    change_events, pagination = _paginate(filtered_events, query)
    latest_event_meta = _latest_event_metadata(historical_scoped_events)
    latest_event_meta["feed_freshness_state"] = _runtime_feed_state(
        str(latest_event_meta.get("feed_freshness_state") or "empty")
    )
    base_contract = {
        "contract_version": _CONTRACT_VERSION,
        "surface_id": query.surface_id,
        "generated_at": utc_now_iso(),
        "freshness_state": "stored_valid_context",
        "surface_state": surface_state(
            "ready" if change_events else "empty",
            reason_codes=[] if change_events else ["no_changes"],
            summary="Changes feed contains investor-meaningful deltas." if change_events else "No changes were recorded for the selected surface and window.",
        ),
        "section_states": {
            "changes_feed": ready_section() if change_events else empty_section("no_changes", "No changes match the selected surface and time window."),
        },
        "change_events": change_events,
        "net_impact": _net_impact([str(event.get("event_type") or "") for event in change_events]),
        "since_utc": query.since_utc,
        "window": query.window,
        "timezone": query.timezone,
        "effective_since_utc": query.since_utc,
        "daily_source_scan": daily_source_scan,
        "summary": summary,
        "audit_groups": audit_groups,
        "available_sleeves": available_sleeves,
        "available_categories": _AVAILABLE_CATEGORIES,
        **latest_event_meta,
        "filters_applied": {
            "category": query.category,
            "sleeve_id": query.sleeve_id,
            "candidate_id": query.candidate_id,
            "needs_review": query.needs_review,
            "limit": query.limit,
            "cursor": query.cursor,
            "timezone": query.timezone,
        },
        "pagination": pagination,
    }
    return apply_overlay(base_contract, holdings=None)
