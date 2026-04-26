from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.config import get_db_path
from app.v2.core.change_ledger import get_diffs, record_change
from app.v2.storage.surface_snapshot_store import latest_surface_snapshot, previous_surface_snapshot
from app.v2.surfaces.changes.query import DEFAULT_CHANGES_TIMEZONE, normalize_timezone


_SURFACE_ID = "blueprint_explorer"
_OBJECT_ID = "blueprint_explorer"


def _now() -> datetime:
    return datetime.now(UTC)


def _now_iso() -> str:
    return _now().isoformat()


def _zone(timezone: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(normalize_timezone(timezone))
    except ZoneInfoNotFoundError:
        return ZoneInfo(DEFAULT_CHANGES_TIMEZONE)


def _trading_day(timezone: str | None, *, at: datetime | None = None) -> str:
    current = (at or _now()).astimezone(_zone(timezone))
    return current.date().isoformat()


def _trading_day_start_utc(timezone: str | None, *, at: datetime | None = None) -> str:
    zone = _zone(timezone)
    current = (at or _now()).astimezone(zone)
    local_start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    return local_start.astimezone(UTC).isoformat()


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_blueprint_daily_source_scans (
          scan_id TEXT PRIMARY KEY,
          trading_day TEXT NOT NULL,
          timezone TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          status TEXT NOT NULL,
          source_freshness_state TEXT NOT NULL,
          emitted_event_count INTEGER NOT NULL DEFAULT 0,
          material_candidate_count INTEGER NOT NULL DEFAULT 0,
          no_material_change INTEGER NOT NULL DEFAULT 1,
          failure_reasons_json TEXT NOT NULL DEFAULT '[]',
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_v2_blueprint_daily_source_scans_day
        ON v2_blueprint_daily_source_scans (trading_day DESC, created_at DESC)
        """
    )
    conn.commit()


def _scan_payload(
    *,
    trading_day: str,
    timezone: str,
    started_at: str | None,
    finished_at: str | None,
    status: str,
    source_freshness_state: str,
    emitted_event_count: int,
    material_candidate_count: int,
    no_material_change: bool,
    latest_scan_at: str | None,
    failure_reasons: list[str],
) -> dict[str, Any]:
    return {
        "trading_day": trading_day,
        "timezone": timezone,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "source_freshness_state": source_freshness_state,
        "emitted_event_count": emitted_event_count,
        "material_candidate_count": material_candidate_count,
        "no_material_change": no_material_change,
        "latest_scan_at": latest_scan_at,
        "failure_reasons": failure_reasons,
    }


def _persist_scan(payload: dict[str, Any]) -> dict[str, Any]:
    scan_id = f"{payload['trading_day']}::{payload['timezone']}"
    with _connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO v2_blueprint_daily_source_scans (
              scan_id, trading_day, timezone, started_at, finished_at, status,
              source_freshness_state, emitted_event_count, material_candidate_count,
              no_material_change, failure_reasons_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scan_id,
                payload.get("trading_day"),
                payload.get("timezone"),
                payload.get("started_at"),
                payload.get("finished_at"),
                payload.get("status"),
                payload.get("source_freshness_state"),
                int(payload.get("emitted_event_count") or 0),
                int(payload.get("material_candidate_count") or 0),
                1 if payload.get("no_material_change") else 0,
                json.dumps(list(payload.get("failure_reasons") or []), ensure_ascii=True),
                _now_iso(),
            ),
        )
        conn.commit()
    return payload


def latest_blueprint_daily_source_scan(timezone: str | None = None) -> dict[str, Any]:
    normalized_timezone = normalize_timezone(timezone)
    today = _trading_day(normalized_timezone)
    with _connection() as conn:
        today_row = conn.execute(
            """
            SELECT * FROM v2_blueprint_daily_source_scans
            WHERE trading_day = ? AND timezone = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (today, normalized_timezone),
        ).fetchone()
        fallback_today_row = conn.execute(
            """
            SELECT * FROM v2_blueprint_daily_source_scans
            WHERE trading_day = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (today,),
        ).fetchone()
        latest_row = conn.execute(
            """
            SELECT * FROM v2_blueprint_daily_source_scans
            ORDER BY created_at DESC
            LIMIT 1
            """,
        ).fetchone()
    row = today_row or fallback_today_row or latest_row
    if row is None:
        return _scan_payload(
            trading_day=today,
            timezone=normalized_timezone,
            started_at=None,
            finished_at=None,
            status="not_run",
            source_freshness_state="empty",
            emitted_event_count=0,
            material_candidate_count=0,
            no_material_change=True,
            latest_scan_at=None,
            failure_reasons=[],
        )
    item = dict(row)
    failures = json.loads(str(item.get("failure_reasons_json") or "[]"))
    if today_row is None and fallback_today_row is None:
        return _scan_payload(
            trading_day=today,
            timezone=normalized_timezone,
            started_at=None,
            finished_at=None,
            status="not_run",
            source_freshness_state="stale",
            emitted_event_count=0,
            material_candidate_count=0,
            no_material_change=True,
            latest_scan_at=str(item.get("finished_at") or item.get("created_at") or ""),
            failure_reasons=[],
        )
    return _scan_payload(
        trading_day=str(item.get("trading_day") or today),
        timezone=str(item.get("timezone") or normalized_timezone),
        started_at=str(item.get("started_at") or "") or None,
        finished_at=str(item.get("finished_at") or "") or None,
        status=str(item.get("status") or "not_run"),
        source_freshness_state=str(item.get("source_freshness_state") or "empty"),
        emitted_event_count=int(item.get("emitted_event_count") or 0),
        material_candidate_count=int(item.get("material_candidate_count") or 0),
        no_material_change=bool(item.get("no_material_change")),
        latest_scan_at=str(item.get("finished_at") or item.get("created_at") or "") or None,
        failure_reasons=[str(reason) for reason in failures if str(reason or "").strip()],
    )


def _candidate_index(contract: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for sleeve in list((contract or {}).get("sleeves") or []):
        sleeve_id = str(sleeve.get("sleeve_id") or "").strip() or None
        sleeve_name = str(sleeve.get("sleeve_name") or "").strip() or None
        for candidate in list(sleeve.get("candidates") or []):
            candidate_id = str(candidate.get("candidate_id") or "").strip()
            if candidate_id:
                indexed[candidate_id] = {
                    "candidate": candidate,
                    "sleeve_id": sleeve_id,
                    "sleeve_name": sleeve_name,
                }
    return indexed


def _sleeve_index(contract: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    return {
        str(sleeve.get("sleeve_id") or "").strip(): sleeve
        for sleeve in list((contract or {}).get("sleeves") or [])
        if str(sleeve.get("sleeve_id") or "").strip()
    }


def _dig(source: dict[str, Any], path: list[str]) -> Any:
    current: Any = source
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _score(candidate: dict[str, Any]) -> float | None:
    for path in (
        ["score_summary", "average_score"],
        ["score_decomposition", "recommendation_score"],
        ["score_decomposition", "total_score"],
        ["score"],
    ):
        value = _num(_dig(candidate, list(path)))
        if value is not None:
            return value
    return None


def _deployability(candidate: dict[str, Any]) -> float | None:
    for key in ("deployability_score", "deployment_score", "readiness_score"):
        value = _num(_dig(candidate, ["score_decomposition", key]))
        if value is not None:
            return value
    return None


def _truth_confidence(candidate: dict[str, Any]) -> float | None:
    return _num(_dig(candidate, ["score_decomposition", "truth_confidence_score"]))


def _source_ready(candidate: dict[str, Any]) -> int | None:
    for path in (
        ["source_completion_summary", "critical_fields_completed"],
        ["source_integrity_summary", "critical_fields_ready"],
        ["data_quality_summary", "critical_fields_ready"],
    ):
        value = _num(_dig(candidate, list(path)))
        if value is not None:
            return int(value)
    return None


def _source_state(candidate: dict[str, Any]) -> str:
    return str(
        _dig(candidate, ["source_completion_summary", "state"])
        or _dig(candidate, ["source_integrity_summary", "state"])
        or ""
    ).strip().lower()


def _market_support(candidate: dict[str, Any]) -> str:
    return str(
        _dig(candidate, ["market_path_support", "support_strength"])
        or _dig(candidate, ["forecast_support", "support_strength"])
        or ""
    ).strip().lower()


def _support_rank(value: str) -> int:
    return {
        "strong": 4,
        "moderate": 3,
        "support_only": 2,
        "unstable": 1,
        "weak": 0,
    }.get(str(value or "").strip().lower(), -1)


def _timing_state(candidate: dict[str, Any]) -> str:
    return str(
        _dig(candidate, ["market_path_support", "timing_state"])
        or _dig(candidate, ["market_path_support", "path_state"])
        or _dig(candidate, ["market_path_support", "state"])
        or ""
    ).strip().lower()


def _market_path_source_summary(candidate: dict[str, Any], *, symbol: str, sleeve_name: str | None, strengthened: bool) -> str:
    support = dict(candidate.get("market_path_support") or candidate.get("forecast_support") or {})
    sleeve = str(sleeve_name or "this sleeve").strip()
    for key in (
        "candidate_implication",
        "decision_impact_text",
        "regime_check_text",
        "support_reason",
        "route_label",
    ):
        text = str(support.get(key) or "").strip()
        if text:
            return text
    quality = dict(support.get("series_quality_summary") or {})
    provider = str(support.get("provider_source") or support.get("provider_label") or quality.get("provider_symbol") or "").strip()
    direction = "improved" if strengthened else "weakened"
    if provider:
        return f"{sleeve} timing support {direction} in the latest market path refresh from {provider}."
    return f"{sleeve} timing support {direction}, but the source packet does not expose a specific market driver for {symbol}."


def _has_index_scope(candidate: dict[str, Any]) -> bool:
    return bool(_dig(candidate, ["quick_brief_snapshot", "index_scope_explainer"]))


def _has_quick_evidence(candidate: dict[str, Any]) -> bool:
    quick = candidate.get("quick_brief_snapshot")
    if not isinstance(quick, dict):
        return False
    for key in ("fund_profile", "listing_profile", "document_coverage", "source_integrity_pack", "evidence_pack"):
        if quick.get(key):
            return True
    return False


def _candidate_symbol(candidate: dict[str, Any], candidate_id: str) -> str:
    return str(candidate.get("symbol") or candidate_id).strip()


def _daily_duplicate(
    *,
    event_type: str,
    candidate_id: str | None,
    sleeve_id: str | None,
    trigger: str,
    previous_state: str | None,
    current_state: str | None,
    trading_day_start_utc: str,
) -> bool:
    for event in get_diffs(_SURFACE_ID, trading_day_start_utc):
        if str(event.event_type or "") != event_type:
            continue
        if str(event.candidate_id or "") != str(candidate_id or ""):
            continue
        if str(event.sleeve_id or "") != str(sleeve_id or ""):
            continue
        if str(event.change_trigger or "") != trigger:
            continue
        if str(event.previous_state or "") != str(previous_state or ""):
            continue
        if str(event.current_state or "") != str(current_state or ""):
            continue
        return True
    return False


def _emit(
    *,
    event_type: str,
    summary: str,
    candidate_id: str | None,
    sleeve_id: str | None,
    sleeve_name: str | None,
    previous_state: str | None,
    current_state: str | None,
    implication_summary: str,
    portfolio_consequence: str,
    next_action: str,
    what_would_reverse: str,
    impact_level: str,
    requires_review: bool,
    trigger: str,
    trading_day_start_utc: str,
    report_tab: str = "investment_case",
) -> bool:
    if _daily_duplicate(
        event_type=event_type,
        candidate_id=candidate_id,
        sleeve_id=sleeve_id,
        trigger=trigger,
        previous_state=previous_state,
        current_state=current_state,
        trading_day_start_utc=trading_day_start_utc,
    ):
        return False
    record_change(
        event_type=event_type,
        surface_id=_SURFACE_ID,
        summary=summary,
        candidate_id=candidate_id,
        sleeve_id=sleeve_id,
        sleeve_name=sleeve_name,
        change_trigger=trigger,
        reason_summary=trigger,
        previous_state=previous_state,
        current_state=current_state,
        implication_summary=implication_summary,
        portfolio_consequence=portfolio_consequence,
        next_action=next_action,
        what_would_reverse=what_would_reverse,
        requires_review=requires_review,
        report_tab=report_tab,
        impact_level=impact_level,
        deep_link_target={
            "target_type": "candidate_report" if candidate_id else "blueprint_explorer",
            "target_id": candidate_id or sleeve_id or _OBJECT_ID,
            "tab": report_tab,
            "section": report_tab,
        },
    )
    return True


def run_blueprint_daily_source_scan(timezone: str | None = None) -> dict[str, Any]:
    normalized_timezone = normalize_timezone(timezone)
    started_at = _now_iso()
    trading_day = _trading_day(normalized_timezone)
    trading_day_start = _trading_day_start_utc(normalized_timezone)
    emitted = 0
    material_candidates: set[str] = set()
    failure_reasons: list[str] = []
    try:
        latest = latest_surface_snapshot(surface_id=_SURFACE_ID, object_id=_OBJECT_ID)
        previous = previous_surface_snapshot(surface_id=_SURFACE_ID, object_id=_OBJECT_ID)
        if not latest:
            failure_reasons.append("missing_latest_blueprint_explorer_snapshot")
            payload = _scan_payload(
                trading_day=trading_day,
                timezone=normalized_timezone,
                started_at=started_at,
                finished_at=_now_iso(),
                status="failed",
                source_freshness_state="empty",
                emitted_event_count=0,
                material_candidate_count=0,
                no_material_change=True,
                latest_scan_at=_now_iso(),
                failure_reasons=failure_reasons,
            )
            return _persist_scan(payload)
        if not previous:
            failure_reasons.append("missing_previous_comparable_snapshot")

        current_contract = dict(latest.get("contract") or {})
        previous_contract = dict((previous or {}).get("contract") or {})
        previous_candidates = _candidate_index(previous_contract)
        current_candidates = _candidate_index(current_contract)

        for candidate_id, current_info in current_candidates.items():
            previous_info = previous_candidates.get(candidate_id)
            if not previous_info:
                continue
            current_candidate = dict(current_info.get("candidate") or {})
            previous_candidate = dict(previous_info.get("candidate") or {})
            sleeve_id = str(current_info.get("sleeve_id") or "") or None
            sleeve_name = str(current_info.get("sleeve_name") or "") or None
            symbol = _candidate_symbol(current_candidate, candidate_id)
            score_current = _score(current_candidate)
            score_previous = _score(previous_candidate)
            if score_current is not None and score_previous is not None and abs(score_current - score_previous) >= 3:
                # Raw score movement is retained in the candidate surface, but it is not a source-backed
                # Blueprint change by itself. The visible Changes feed should start from a real driver.
                pass

            deploy_current = _deployability(current_candidate)
            deploy_previous = _deployability(previous_candidate)
            if deploy_current is not None and deploy_previous is not None and abs(deploy_current - deploy_previous) >= 4:
                if _emit(
                    event_type="candidate_deployability_changed",
                    summary=f"{symbol} deployability moved {deploy_previous:.0f} to {deploy_current:.0f}.",
                    candidate_id=candidate_id,
                    sleeve_id=sleeve_id,
                    sleeve_name=sleeve_name,
                    previous_state=f"{deploy_previous:.0f}",
                    current_state=f"{deploy_current:.0f}",
                    implication_summary="Deployability changed enough to alter how quickly the candidate can be acted on.",
                    portfolio_consequence="This affects action timing, not just research ranking.",
                    next_action="Check the implementation and source confidence sections before treating this as deployable.",
                    what_would_reverse="A return to the prior deployability score would reverse this event.",
                    impact_level="medium",
                    requires_review=True,
                    trigger="deployability_delta",
                    trading_day_start_utc=trading_day_start,
                ):
                    emitted += 1
                    material_candidates.add(candidate_id)

            confidence_current = _truth_confidence(current_candidate)
            confidence_previous = _truth_confidence(previous_candidate)
            if confidence_current is not None and confidence_previous is not None and abs(confidence_current - confidence_previous) >= 5:
                if _emit(
                    event_type="truth_confidence_changed",
                    summary=f"{symbol} truth confidence moved {confidence_previous:.0f} to {confidence_current:.0f}.",
                    candidate_id=candidate_id,
                    sleeve_id=sleeve_id,
                    sleeve_name=sleeve_name,
                    previous_state=f"{confidence_previous:.0f}",
                    current_state=f"{confidence_current:.0f}",
                    implication_summary="Source confidence changed enough to alter recommendation trust.",
                    portfolio_consequence="This changes evidence burden before any ETF preference is promoted.",
                    next_action="Review source confidence and evidence fields before changing the candidate read.",
                    what_would_reverse="A return to the prior truth confidence score would reverse this event.",
                    impact_level="medium",
                    requires_review=True,
                    trigger="truth_confidence_delta",
                    trading_day_start_utc=trading_day_start,
                    report_tab="evidence",
                ):
                    emitted += 1
                    material_candidates.add(candidate_id)

            source_current = _source_ready(current_candidate)
            source_previous = _source_ready(previous_candidate)
            if source_current is not None and source_previous is not None and source_current != source_previous:
                if _emit(
                    event_type="source_completion_changed",
                    summary=f"{symbol} source completion moved {source_previous} to {source_current} critical fields.",
                    candidate_id=candidate_id,
                    sleeve_id=sleeve_id,
                    sleeve_name=sleeve_name,
                    previous_state=str(source_previous),
                    current_state=str(source_current),
                    implication_summary="Recommendation-critical source coverage changed.",
                    portfolio_consequence="This changes how much source review the candidate needs.",
                    next_action="Check the source confidence and quick brief evidence before changing preference.",
                    what_would_reverse="A return to the prior critical-field count would reverse this event.",
                    impact_level="medium",
                    requires_review=True,
                    trigger="source_completion",
                    trading_day_start_utc=trading_day_start,
                    report_tab="evidence",
                ):
                    emitted += 1
                    material_candidates.add(candidate_id)

            current_source_state = _source_state(current_candidate)
            previous_source_state = _source_state(previous_candidate)
            if current_source_state and previous_source_state and current_source_state != previous_source_state:
                if _emit(
                    event_type="candidate_source_strengthened" if current_source_state in {"complete", "strong", "clean"} else "candidate_source_weakened",
                    summary=f"{symbol} source state moved from {previous_source_state} to {current_source_state}.",
                    candidate_id=candidate_id,
                    sleeve_id=sleeve_id,
                    sleeve_name=sleeve_name,
                    previous_state=previous_source_state,
                    current_state=current_source_state,
                    implication_summary="The candidate source read changed materially.",
                    portfolio_consequence="This changes confidence in the row before it changes allocation.",
                    next_action="Review source details before using this candidate as a substitute.",
                    what_would_reverse="A return to the prior source state would reverse this event.",
                    impact_level="medium",
                    requires_review=current_source_state not in {"complete", "strong", "clean"},
                    trigger="source_state",
                    trading_day_start_utc=trading_day_start,
                    report_tab="evidence",
                ):
                    emitted += 1
                    material_candidates.add(candidate_id)

            if _has_index_scope(current_candidate) and not _has_index_scope(previous_candidate):
                if _emit(
                    event_type="index_scope_added",
                    summary=f"{symbol} index or exposure scope became available.",
                    candidate_id=candidate_id,
                    sleeve_id=sleeve_id,
                    sleeve_name=sleeve_name,
                    previous_state="missing",
                    current_state="available",
                    implication_summary="The quick brief can now explain what exposure the investor is actually buying.",
                    portfolio_consequence="This reduces source ambiguity but does not by itself authorize deployment.",
                    next_action="Use the quick brief scope section before comparing the ETF against same-job peers.",
                    what_would_reverse="If the scope explainer disappears or conflicts with source truth, this event reverses.",
                    impact_level="medium",
                    requires_review=False,
                    trigger="index_scope_materialized",
                    trading_day_start_utc=trading_day_start,
                    report_tab="evidence",
                ):
                    emitted += 1
                    material_candidates.add(candidate_id)

            if _has_quick_evidence(current_candidate) and not _has_quick_evidence(previous_candidate):
                if _emit(
                    event_type="quick_brief_evidence_added",
                    summary=f"{symbol} quick brief evidence became available.",
                    candidate_id=candidate_id,
                    sleeve_id=sleeve_id,
                    sleeve_name=sleeve_name,
                    previous_state="missing",
                    current_state="available",
                    implication_summary="More candidate evidence is now available in the first-read workflow.",
                    portfolio_consequence="This improves reviewability but still requires comparison before action.",
                    next_action="Open the quick brief if the row is competing for the next sleeve decision.",
                    what_would_reverse="If quick brief evidence becomes unavailable or stale, this event reverses.",
                    impact_level="medium",
                    requires_review=False,
                    trigger="quick_brief_evidence_materialized",
                    trading_day_start_utc=trading_day_start,
                    report_tab="evidence",
                ):
                    emitted += 1
                    material_candidates.add(candidate_id)

            current_market = _market_support(current_candidate)
            previous_market = _market_support(previous_candidate)
            if current_market and previous_market and _support_rank(current_market) != _support_rank(previous_market):
                strengthened = _support_rank(current_market) > _support_rank(previous_market)
                trigger = _market_path_source_summary(
                    current_candidate,
                    symbol=symbol,
                    sleeve_name=sleeve_name,
                    strengthened=strengthened,
                )
                if _emit(
                    event_type="market_path_strengthened" if strengthened else "market_path_weakened",
                    summary=f"{symbol} market path {'strengthened' if strengthened else 'weakened'} from {previous_market} to {current_market}.",
                    candidate_id=candidate_id,
                    sleeve_id=sleeve_id,
                    sleeve_name=sleeve_name,
                    previous_state=previous_market,
                    current_state=current_market,
                    implication_summary="Market path support changed enough to affect timing context.",
                    portfolio_consequence="This changes timing pressure, not source authority.",
                    next_action="Treat as timing context and confirm against candidate evidence before changing preference.",
                    what_would_reverse="A reversal in market path support would reverse this event.",
                    impact_level="medium",
                    requires_review=False,
                    trigger=trigger,
                    trading_day_start_utc=trading_day_start,
                    report_tab="scenarios",
                ):
                    emitted += 1
                    material_candidates.add(candidate_id)

            current_timing = _timing_state(current_candidate)
            previous_timing = _timing_state(previous_candidate)
            if current_timing and previous_timing and current_timing != previous_timing:
                strengthened = _support_rank(current_timing) > _support_rank(previous_timing)
                trigger = _market_path_source_summary(
                    current_candidate,
                    symbol=symbol,
                    sleeve_name=sleeve_name,
                    strengthened=strengthened,
                )
                if _emit(
                    event_type="timing_state_changed",
                    summary=f"{symbol} timing state changed from {previous_timing} to {current_timing}.",
                    candidate_id=candidate_id,
                    sleeve_id=sleeve_id,
                    sleeve_name=sleeve_name,
                    previous_state=previous_timing,
                    current_state=current_timing,
                    implication_summary="Timing state changed enough to alter current review priority.",
                    portfolio_consequence="This changes when to review the candidate, not what it owns.",
                    next_action="Check market path and scenario details before changing action priority.",
                    what_would_reverse="A return to the prior timing state would reverse this event.",
                    impact_level="medium",
                    requires_review=False,
                    trigger=trigger,
                    trading_day_start_utc=trading_day_start,
                    report_tab="scenarios",
                ):
                    emitted += 1
                    material_candidates.add(candidate_id)

        previous_sleeves = _sleeve_index(previous_contract)
        current_sleeves = _sleeve_index(current_contract)
        for sleeve_id, current_sleeve in current_sleeves.items():
            previous_sleeve = previous_sleeves.get(sleeve_id) or {}
            current_priority = str(current_sleeve.get("sleeve_actionability_state") or current_sleeve.get("visible_state") or "").strip().lower()
            previous_priority = str(previous_sleeve.get("sleeve_actionability_state") or previous_sleeve.get("visible_state") or "").strip().lower()
            if current_priority and previous_priority and current_priority != previous_priority:
                sleeve_name = str(current_sleeve.get("sleeve_name") or sleeve_id).strip()
                if _emit(
                    event_type="portfolio_drift_changed",
                    summary=f"{sleeve_name} action priority moved from {previous_priority} to {current_priority}.",
                    candidate_id=None,
                    sleeve_id=sleeve_id,
                    sleeve_name=sleeve_name,
                    previous_state=previous_priority,
                    current_state=current_priority,
                    implication_summary="The sleeve-level action posture changed.",
                    portfolio_consequence="This changes which sleeve deserves review first.",
                    next_action="Review the sleeve lane before inspecting individual candidate reports.",
                    what_would_reverse="A return to the prior sleeve posture would reverse this event.",
                    impact_level="high",
                    requires_review=current_priority not in {"ready", "deployable"},
                    trigger="sleeve_actionability",
                    trading_day_start_utc=trading_day_start,
                ):
                    emitted += 1

        finished_at = _now_iso()
        payload = _scan_payload(
            trading_day=trading_day,
            timezone=normalized_timezone,
            started_at=started_at,
            finished_at=finished_at,
            status="partial" if failure_reasons else "success",
            source_freshness_state="fresh" if latest else "empty",
            emitted_event_count=emitted,
            material_candidate_count=len(material_candidates),
            no_material_change=emitted == 0,
            latest_scan_at=finished_at,
            failure_reasons=failure_reasons,
        )
        return _persist_scan(payload)
    except Exception as exc:  # noqa: BLE001
        payload = _scan_payload(
            trading_day=trading_day,
            timezone=normalized_timezone,
            started_at=started_at,
            finished_at=_now_iso(),
            status="failed",
            source_freshness_state="degraded_runtime",
            emitted_event_count=emitted,
            material_candidate_count=len(material_candidates),
            no_material_change=emitted == 0,
            latest_scan_at=_now_iso(),
            failure_reasons=[str(exc)],
        )
        return _persist_scan(payload)


def main() -> None:
    print(json.dumps(run_blueprint_daily_source_scan(), ensure_ascii=True, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
