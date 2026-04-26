from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.v2.core.change_ledger import get_diffs, record_change


def _normalized(value: Any) -> str:
    return str(value or "").strip()


def _score_band(score: Any) -> str:
    try:
        numeric = float(score)
    except (TypeError, ValueError):
        return "unknown"
    if numeric >= 75:
        return "strong"
    if numeric >= 55:
        return "usable"
    return "weak"


def _support_rank(value: Any) -> int:
    normalized = str(value or "").strip().lower()
    return {
        "strong": 4,
        "moderate": 3,
        "support_only": 2,
        "unstable": 1,
        "weak": 0,
    }.get(normalized, -1)


def _state(value: dict[str, Any] | None) -> str:
    return str((value or {}).get("state") or "").strip().lower()


def _gate_blocks(gate_state: Any) -> bool:
    normalized = str(gate_state or "").strip().lower()
    return normalized in {"blocked", "ineligible"}


def _recent_duplicate(
    *,
    surface_id: str,
    event_type: str,
    candidate_id: str | None,
    sleeve_id: str | None,
    previous_state: str | None,
    current_state: str | None,
    summary: str,
    hours: int = 72,
) -> bool:
    local_now = datetime.now(UTC).astimezone(ZoneInfo("Asia/Singapore"))
    since = local_now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC).isoformat()
    for event in get_diffs(surface_id, since):
        if str(event.event_type or "") != event_type:
            continue
        if str(event.candidate_id or "") != str(candidate_id or ""):
            continue
        if str(event.sleeve_id or "") != str(sleeve_id or ""):
            continue
        if str(event.previous_state or "") != str(previous_state or ""):
            continue
        if str(event.current_state or "") != str(current_state or ""):
            continue
        if str(event.summary or "") != summary:
            continue
        return True
    return False


def _safe_record_change(
    *,
    event_type: str,
    surface_id: str,
    summary: str,
    candidate_id: str | None = None,
    sleeve_id: str | None = None,
    sleeve_name: str | None = None,
    previous_state: str | None = None,
    current_state: str | None = None,
    change_trigger: str | None = None,
    reason_summary: str | None = None,
    implication_summary: str | None = None,
    portfolio_consequence: str | None = None,
    next_action: str | None = None,
    what_would_reverse: str | None = None,
    impact_level: str = "medium",
    requires_review: bool | None = None,
    report_tab: str = "investment_case",
    deep_link_target: dict[str, object] | None = None,
) -> None:
    if _recent_duplicate(
        surface_id=surface_id,
        event_type=event_type,
        candidate_id=candidate_id,
        sleeve_id=sleeve_id,
        previous_state=previous_state,
        current_state=current_state,
        summary=summary,
    ):
        return
    record_change(
        event_type=event_type,
        surface_id=surface_id,
        summary=summary,
        candidate_id=candidate_id,
        sleeve_id=sleeve_id,
        sleeve_name=sleeve_name,
        change_trigger=change_trigger,
        reason_summary=reason_summary,
        previous_state=previous_state,
        current_state=current_state,
        implication_summary=implication_summary,
        portfolio_consequence=portfolio_consequence,
        next_action=next_action,
        what_would_reverse=what_would_reverse,
        impact_level=impact_level,
        requires_review=requires_review,
        report_tab=report_tab,
        deep_link_target=deep_link_target,
    )


def _score_component_change(previous_candidate: dict[str, Any], current_candidate: dict[str, Any]) -> str | None:
    previous_components = {
        str(component.get("component_id") or component.get("label") or "").strip(): component
        for component in list((previous_candidate.get("score_summary") or {}).get("components") or [])
        if str(component.get("component_id") or component.get("label") or "").strip()
    }
    strongest_delta: tuple[float, str] | None = None
    for component in list((current_candidate.get("score_summary") or {}).get("components") or []):
        key = str(component.get("component_id") or component.get("label") or "").strip()
        previous = previous_components.get(key)
        if not previous:
            continue
        try:
            current_score = float(component.get("score"))
            previous_score = float(previous.get("score"))
        except (TypeError, ValueError):
            continue
        delta = current_score - previous_score
        if abs(delta) < 3:
            continue
        label = str(component.get("label") or key).strip()
        if strongest_delta is None or abs(delta) > abs(strongest_delta[0]):
            strongest_delta = (delta, label)
    if strongest_delta is None:
        return None
    delta, label = strongest_delta
    return f"{label} {'improved' if delta > 0 else 'weakened'} enough to affect review priority."


def _timing_trigger(symbol: str, sleeve_name: str | None, strengthening: bool) -> str:
    verb = "strengthened" if strengthening else "weakened"
    sleeve = sleeve_name or "the sleeve"
    return f"The latest timing refresh {verb} {sleeve} market support for {symbol}."


def _state_change_trigger(
    symbol: str,
    sleeve_name: str | None,
    current_state: str | None,
    current_candidate: dict[str, Any],
) -> str:
    sleeve = sleeve_name or "the sleeve"
    gate_state = str((current_candidate.get("recommendation_gate") or {}).get("gate_state") or "").strip()
    source_state = str((current_candidate.get("source_integrity_summary") or {}).get("state") or "").strip()
    completion_state = str((current_candidate.get("source_completion_summary") or {}).get("state") or "").strip()
    if gate_state:
        return f"{symbol}'s recommendation gate is now {gate_state} inside {sleeve}."
    if source_state:
        return f"{symbol}'s source integrity is now {source_state} inside {sleeve}."
    if completion_state:
        return f"{symbol}'s source completion is now {completion_state} inside {sleeve}."
    if current_state:
        return f"{sleeve} review priority changed for {symbol}, but the snapshot does not expose a more specific driver."
    return f"The snapshot records a review priority change for {symbol}, but does not expose a more specific driver."


def _candidate_index(contract: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for sleeve in list((contract or {}).get("sleeves") or []):
        sleeve_id = str(sleeve.get("sleeve_id") or "").strip() or None
        sleeve_name = str(sleeve.get("sleeve_name") or "").strip() or None
        for candidate in list(sleeve.get("candidates") or []):
            candidate_id = str(candidate.get("candidate_id") or "").strip()
            if not candidate_id:
                continue
            index[candidate_id] = {
                "candidate": candidate,
                "sleeve_id": sleeve_id,
                "sleeve_name": sleeve_name,
            }
    return index


def emit_explorer_snapshot_changes(
    previous_contract: dict[str, Any] | None,
    current_contract: dict[str, Any],
) -> None:
    previous_contract = previous_contract or {}
    current_contract = current_contract or {}
    previous_sleeves = {
        str(sleeve.get("sleeve_id") or "").strip(): sleeve
        for sleeve in list(previous_contract.get("sleeves") or [])
        if str(sleeve.get("sleeve_id") or "").strip()
    }
    current_sleeves = {
        str(sleeve.get("sleeve_id") or "").strip(): sleeve
        for sleeve in list(current_contract.get("sleeves") or [])
        if str(sleeve.get("sleeve_id") or "").strip()
    }

    for sleeve_id, current_sleeve in current_sleeves.items():
        previous_sleeve = previous_sleeves.get(sleeve_id) or {}
        sleeve_name = str(current_sleeve.get("sleeve_name") or "").strip() or None
        previous_lead = str(previous_sleeve.get("lead_candidate_id") or "").strip() or None
        current_lead = str(current_sleeve.get("lead_candidate_id") or "").strip() or None
        if previous_lead and current_lead and previous_lead != current_lead:
            summary = f"{sleeve_name or sleeve_id} leader changed from {previous_lead} to {current_lead}."
            implication = f"{current_lead} is now the front-runner for {sleeve_name or sleeve_id}."
            consequence = f"This changes which ETF currently sets the decision benchmark for {sleeve_name or sleeve_id}."
            _safe_record_change(
                event_type="leader_changed",
                surface_id="blueprint_explorer",
                summary=summary,
                candidate_id=current_lead,
                sleeve_id=sleeve_id,
                sleeve_name=sleeve_name,
                previous_state=previous_lead,
                current_state=current_lead,
                change_trigger=f"{sleeve_name or sleeve_id} lead candidate changed from {previous_lead} to {current_lead}.",
                reason_summary=f"The lead ETF sets the first peer benchmark for {sleeve_name or sleeve_id}.",
                implication_summary=implication,
                portfolio_consequence=consequence,
                next_action="Review the lead candidate first before comparing remaining sleeve alternatives.",
                what_would_reverse="A cleaner alternative retaking the lead would reverse this change.",
                impact_level="medium",
                requires_review=True,
                deep_link_target={"target_type": "candidate_report", "target_id": current_lead, "tab": "investment_case"},
            )

        previous_posture = str(previous_sleeve.get("visible_state") or "").strip() or None
        current_posture = str(current_sleeve.get("visible_state") or "").strip() or None
        if previous_posture and current_posture and previous_posture != current_posture:
            summary = f"{sleeve_name or sleeve_id} posture changed from {previous_posture} to {current_posture}."
            implication = str(current_sleeve.get("implication_summary") or "").strip() or None
            _safe_record_change(
                event_type="sleeve_posture_changed",
                surface_id="blueprint_explorer",
                summary=summary,
                sleeve_id=sleeve_id,
                sleeve_name=sleeve_name,
                previous_state=previous_posture,
                current_state=current_posture,
                change_trigger=f"{sleeve_name or sleeve_id} sleeve posture changed from {previous_posture} to {current_posture}.",
                reason_summary=f"Sleeve posture changes review priority before it changes ETF deployment.",
                implication_summary=implication,
                portfolio_consequence=f"The sleeve now carries a {current_posture} posture in the workflow.",
                next_action="Use the lane to check whether the lead candidate still matches the sleeve job.",
                what_would_reverse="A return to the prior sleeve posture would reverse this change.",
                impact_level="medium",
                requires_review=current_posture not in {"eligible", "ready"},
            )

    previous_candidates = _candidate_index(previous_contract)
    current_candidates = _candidate_index(current_contract)
    emitted_candidates = 0
    for candidate_id, current_info in current_candidates.items():
        if emitted_candidates >= 18:
            break
        previous_info = previous_candidates.get(candidate_id)
        if not previous_info:
            continue
        current_candidate = current_info["candidate"]
        previous_candidate = previous_info["candidate"]
        sleeve_id = current_info["sleeve_id"]
        sleeve_name = current_info["sleeve_name"]
        symbol = str(current_candidate.get("symbol") or candidate_id).strip()
        name = str(current_candidate.get("name") or symbol).strip()
        current_state = _state(current_candidate.get("visible_decision_state"))
        previous_state = _state(previous_candidate.get("visible_decision_state"))
        current_gate = str((current_candidate.get("recommendation_gate") or {}).get("gate_state") or "").strip().lower()
        previous_gate = str((previous_candidate.get("recommendation_gate") or {}).get("gate_state") or "").strip().lower()
        current_blocked = tuple(current_candidate.get("recommendation_gate", {}).get("blocked_reasons") or [])
        previous_blocked = tuple(previous_candidate.get("recommendation_gate", {}).get("blocked_reasons") or [])
        current_score = (current_candidate.get("score_summary") or {}).get("average_score", current_candidate.get("score"))
        previous_score = (previous_candidate.get("score_summary") or {}).get("average_score", previous_candidate.get("score"))
        current_score_band = _score_band(current_score)
        previous_score_band = _score_band(previous_score)
        current_support = str((current_candidate.get("forecast_support") or {}).get("support_strength") or "").strip().lower()
        previous_support = str((previous_candidate.get("forecast_support") or {}).get("support_strength") or "").strip().lower()
        implication = str(current_candidate.get("implication_summary") or current_candidate.get("why_now") or "").strip() or None

        if previous_state and current_state and previous_state != current_state:
            summary = f"{symbol} moved from {previous_state} to {current_state} in {sleeve_name or 'the sleeve'}."
            consequence = f"This {'strengthens' if current_state in {'eligible', 'ready'} else 'weakens'} the case for {name} in {sleeve_name or 'the sleeve'}."
            _safe_record_change(
                event_type="recommendation_state_changed",
                surface_id="blueprint_explorer",
                summary=summary,
                candidate_id=candidate_id,
                sleeve_id=sleeve_id,
                sleeve_name=sleeve_name,
                previous_state=previous_state,
                current_state=current_state,
                change_trigger=_state_change_trigger(symbol, sleeve_name, current_state, current_candidate),
                reason_summary=f"{name} needs to keep enough sleeve fit, implementation quality, evidence, and peer advantage before allocation priority changes.",
                implication_summary=implication,
                portfolio_consequence=consequence,
                next_action="Recheck the candidate row and quick brief before changing sleeve preference.",
                what_would_reverse="A return to the prior recommendation state would reverse this change.",
                impact_level="high",
                requires_review=current_state not in {"eligible", "ready"},
                deep_link_target={"target_type": "candidate_report", "target_id": candidate_id, "tab": "investment_case"},
            )
            emitted_candidates += 1
            continue

        if previous_gate != current_gate or previous_blocked != current_blocked:
            blocker_cleared = _gate_blocks(previous_gate) and not _gate_blocks(current_gate)
            event_type = "blocker_cleared" if blocker_cleared else "blocker_opened"
            summary = (
                f"{symbol} cleared a recommendation blocker in {sleeve_name or 'the sleeve'}."
                if blocker_cleared
                else f"{symbol} opened a recommendation blocker in {sleeve_name or 'the sleeve'}."
            )
            consequence = (
                f"This reduces review burden for {name}."
                if blocker_cleared
                else f"This raises review burden for {name} before it can be preferred."
            )
            _safe_record_change(
                event_type=event_type,
                surface_id="blueprint_explorer",
                summary=summary,
                candidate_id=candidate_id,
                sleeve_id=sleeve_id,
                sleeve_name=sleeve_name,
                previous_state=previous_gate or None,
                current_state=current_gate or None,
                change_trigger=(
                    f"{symbol}'s investment blocker cleared in {sleeve_name or 'the sleeve'}."
                    if blocker_cleared
                    else f"{symbol}'s investment blocker appeared in {sleeve_name or 'the sleeve'}."
                ),
                reason_summary="Blocker state controls whether the candidate can be reviewed for deployment at all.",
                implication_summary=implication,
                portfolio_consequence=consequence,
                next_action="Check the current blocker reason before promoting or rejecting the candidate.",
                what_would_reverse="A change in recommendation-gate blockers would reverse this event.",
                impact_level="high" if not blocker_cleared else "medium",
                requires_review=not blocker_cleared,
                deep_link_target={"target_type": "candidate_report", "target_id": candidate_id, "tab": "investment_case"},
            )
            emitted_candidates += 1
            continue

        if previous_score_band != "unknown" and current_score_band != "unknown" and previous_score_band != current_score_band:
            improving = current_score_band in {"usable", "strong"} and previous_score_band != "strong"
            event_type = "score_band_improved" if improving else "score_band_weakened"
            summary = f"{symbol} moved from {previous_score_band} to {current_score_band} recommendation quality."
            consequence = f"This {'improves' if improving else 'weakens'} how convincingly {name} can compete in {sleeve_name or 'the sleeve'}."
            score_driver = _score_component_change(previous_candidate, current_candidate)
            _safe_record_change(
                event_type=event_type,
                surface_id="blueprint_explorer",
                summary=summary,
                candidate_id=candidate_id,
                sleeve_id=sleeve_id,
                sleeve_name=sleeve_name,
                previous_state=previous_score_band,
                current_state=current_score_band,
                change_trigger=score_driver or f"{sleeve_name or 'The sleeve'} review priority changed for {symbol}, but the snapshot does not expose a specific component driver.",
                reason_summary=f"Score changes matter only if {name} still holds up on sleeve fit, implementation quality, evidence, and peer comparison.",
                implication_summary=implication,
                portfolio_consequence=consequence,
                next_action="Use the candidate row first, then open the quick brief if the reason for the score shift matters.",
                what_would_reverse="A move back across the same score band boundary would reverse this change.",
                impact_level="medium",
                requires_review=current_score_band != "strong",
                deep_link_target={"target_type": "candidate_report", "target_id": candidate_id, "tab": "investment_case"},
            )
            emitted_candidates += 1
            continue

        if previous_support and current_support and _support_rank(previous_support) != _support_rank(current_support):
            strengthening = _support_rank(current_support) > _support_rank(previous_support)
            event_type = "market_path_strengthened" if strengthening else "market_path_weakened"
            summary = f"{symbol} market setup {'improved' if strengthening else 'weakened'} in {sleeve_name or 'the sleeve'}."
            consequence = (
                f"This gives {name} slightly more timing support, but does not override wrapper or evidence questions."
                if strengthening
                else f"This removes timing support as a reason to prefer {name} right now."
            )
            _safe_record_change(
                event_type=event_type,
                surface_id="blueprint_explorer",
                summary=summary,
                candidate_id=candidate_id,
                sleeve_id=sleeve_id,
                sleeve_name=sleeve_name,
                previous_state=previous_support,
                current_state=current_support,
                change_trigger=_timing_trigger(symbol, sleeve_name, strengthening),
                reason_summary=f"Timing can change review priority for {name}, but it cannot justify deployment without sleeve need, evidence, implementation quality, and peer comparison.",
                implication_summary=implication,
                portfolio_consequence=consequence,
                next_action="Treat this as timing context, not recommendation authority on its own.",
                what_would_reverse="A reversal in market-path support would reverse this change.",
                impact_level="low",
                requires_review=False,
                deep_link_target={"target_type": "candidate_report", "target_id": candidate_id, "tab": "scenarios"},
            )
            emitted_candidates += 1


def emit_candidate_report_changes(
    candidate_id: str,
    previous_contract: dict[str, Any] | None,
    current_contract: dict[str, Any],
) -> None:
    previous_contract = previous_contract or {}
    current_contract = current_contract or {}
    symbol = str(current_contract.get("symbol") or current_contract.get("candidate_symbol") or candidate_id).strip()
    current_state_raw = current_contract.get("visible_decision_state")
    previous_state_raw = previous_contract.get("visible_decision_state")
    current_state = str((current_state_raw or {}).get("state") or "").strip().lower()
    previous_state = str((previous_state_raw or {}).get("state") or "").strip().lower()
    current_gate = str((current_contract.get("recommendation_gate") or {}).get("gate_state") or "").strip().lower()
    previous_gate = str((previous_contract.get("recommendation_gate") or {}).get("gate_state") or "").strip().lower()
    current_blocked = tuple((current_contract.get("recommendation_gate") or {}).get("blocked_reasons") or [])
    previous_blocked = tuple((previous_contract.get("recommendation_gate") or {}).get("blocked_reasons") or [])
    current_score = (current_contract.get("score_summary") or {}).get("average_score")
    previous_score = (previous_contract.get("score_summary") or {}).get("average_score")
    current_band = _score_band(current_score)
    previous_band = _score_band(previous_score)
    current_implication = str(
        current_contract.get("current_implication")
        or ((current_contract.get("quick_brief") or {}).get("summary"))
        or ((current_contract.get("quick_brief") or {}).get("why_this_matters"))
        or ""
    ).strip() or None

    if previous_state and current_state and previous_state != current_state:
        _safe_record_change(
            event_type="recommendation_state_changed",
            surface_id="candidate_report",
            summary=f"{symbol} moved from {previous_state} to {current_state}.",
            candidate_id=candidate_id,
            previous_state=previous_state,
            current_state=current_state,
            change_trigger=f"{symbol}'s candidate report decision state changed from {previous_state} to {current_state}.",
            reason_summary="Decision state changes matter only after mandate fit, implementation quality, evidence, and peer comparison are checked.",
            implication_summary=current_implication,
            portfolio_consequence=f"This {'strengthens' if current_state in {'eligible', 'ready'} else 'weakens'} the current ETF case.",
            next_action="Use the investment-case tab first, then compare if the ETF is still competing for the sleeve.",
            what_would_reverse="A return to the prior decision state would reverse this change.",
            impact_level="high",
            requires_review=current_state not in {"eligible", "ready"},
            deep_link_target={"target_type": "candidate_report", "target_id": candidate_id, "tab": "investment_case"},
        )

    if previous_gate != current_gate or previous_blocked != current_blocked:
        blocker_cleared = _gate_blocks(previous_gate) and not _gate_blocks(current_gate)
        _safe_record_change(
            event_type="blocker_cleared" if blocker_cleared else "blocker_opened",
            surface_id="candidate_report",
            summary=f"{symbol} {'cleared' if blocker_cleared else 'opened'} a recommendation blocker.",
            candidate_id=candidate_id,
            previous_state=previous_gate or None,
            current_state=current_gate or None,
            change_trigger=(
                f"{symbol}'s investment blocker cleared."
                if blocker_cleared else
                f"{symbol}'s investment blocker appeared."
            ),
            reason_summary="Blocker state controls whether the ETF can be reviewed for deployment at all.",
            implication_summary=current_implication,
            portfolio_consequence=(
                "This lowers the review burden on the ETF."
                if blocker_cleared else
                "This adds review burden before the ETF can be preferred."
            ),
            next_action="Check the recommendation gate and blocker reasons in the quick brief.",
            what_would_reverse="A change in blocker state would reverse this event.",
            impact_level="high" if not blocker_cleared else "medium",
            requires_review=not blocker_cleared,
            deep_link_target={"target_type": "candidate_report", "target_id": candidate_id, "tab": "investment_case"},
        )

    if previous_band != "unknown" and current_band != "unknown" and previous_band != current_band:
        improving = current_band in {"usable", "strong"} and previous_band != "strong"
        _safe_record_change(
            event_type="score_band_improved" if improving else "score_band_weakened",
            surface_id="candidate_report",
            summary=f"{symbol} moved from {previous_band} to {current_band} recommendation quality.",
            candidate_id=candidate_id,
            previous_state=previous_band,
            current_state=current_band,
            change_trigger=f"{symbol}'s report score quality changed from {previous_band} to {current_band}.",
            reason_summary="Score quality changes review priority only if the ETF still works on sleeve fit, implementation quality, and evidence.",
            implication_summary=current_implication,
            portfolio_consequence=(
                "This improves the ETF's overall standing."
                if improving else
                "This weakens the ETF's overall standing."
            ),
            next_action="Use the score family and quick brief together before changing preference.",
            what_would_reverse="A move back across the same score-band boundary would reverse this event.",
            impact_level="medium",
            requires_review=current_band != "strong",
            deep_link_target={"target_type": "candidate_report", "target_id": candidate_id, "tab": "investment_case"},
        )
