from __future__ import annotations

from typing import Any


ACTIVE_UNIVERSE_STATES = {
    "recommended_primary",
    "recommended_backup",
    "watchlist_only",
    "research_only",
    "rejected_inferior_to_selected",
}


def build_candidate_universe(
    *,
    current_payload: dict[str, Any],
    previous_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    previous_lookup = _candidate_state_lookup(previous_payload or {})
    current_lookup = _candidate_state_lookup(current_payload)
    sleeves: dict[str, list[dict[str, Any]]] = {}
    entered: list[dict[str, Any]] = []
    exited: list[dict[str, Any]] = []
    retained: list[dict[str, Any]] = []
    under_review: list[dict[str, Any]] = []

    for key, record in current_lookup.items():
        prior = previous_lookup.get(key)
        recommendation_state = str(record.get("recommendation_state") or "research_only")
        if prior is None:
            change_state = "added"
        elif str(prior.get("recommendation_state") or "") != recommendation_state:
            change_state = "under_review"
        else:
            change_state = "retained"
        if recommendation_state not in ACTIVE_UNIVERSE_STATES:
            change_state = "under_review"
        item = {
            "candidate_name": record.get("name"),
            "candidate_symbol": record.get("symbol"),
            "sleeve": record.get("sleeve_key"),
            "change_state": change_state,
            "change_reasons": _change_reasons(previous=prior, current=record),
            "effective_confidence": record.get("recommendation_confidence"),
            "recommendation_state": recommendation_state,
            "recommendation_eligible": recommendation_state in {"recommended_primary", "recommended_backup"},
            "research_only": recommendation_state == "research_only",
            "universe_role": _universe_role(record),
        }
        sleeves.setdefault(str(record.get("sleeve_key") or ""), []).append(item)
        if change_state == "added":
            entered.append(item)
        if change_state == "under_review" or recommendation_state in {"watchlist_only", "research_only"}:
            under_review.append(item)
        if change_state == "retained":
            retained.append(item)

    for key, record in previous_lookup.items():
        if key in current_lookup:
            continue
        item = {
            "candidate_name": record.get("name"),
            "candidate_symbol": record.get("symbol"),
            "sleeve": record.get("sleeve_key"),
            "change_state": "removed",
            "change_reasons": _removal_reasons(record),
            "effective_confidence": record.get("recommendation_confidence"),
            "recommendation_state": "removed_from_deliverable_set",
            "recommendation_eligible": False,
            "research_only": False,
            "universe_role": "removed",
        }
        exited.append(item)

    return {
        "by_sleeve": sleeves,
        "entered": entered,
        "exited": exited,
        "retained": retained,
        "under_review": under_review,
        "summary": {
            "current_candidate_count": len(current_lookup),
            "entered_count": len(entered),
            "exited_count": len(exited),
            "retained_count": len(retained),
            "under_review_count": len(under_review),
        },
    }


def build_candidate_universe_diff(
    *,
    current_payload: dict[str, Any],
    previous_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    universe = build_candidate_universe(current_payload=current_payload, previous_payload=previous_payload)
    return {
        "entered": universe.get("entered") or [],
        "exited": universe.get("exited") or [],
        "under_review": universe.get("under_review") or [],
        "summary": universe.get("summary") or {},
    }


def _candidate_state_lookup(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for sleeve in list(payload.get("sleeves") or []):
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        for candidate in list(sleeve.get("candidates") or []):
            quality = dict(candidate.get("investment_quality") or {})
            decision_record = dict(candidate.get("decision_record") or {})
            state = str(quality.get("recommendation_state") or decision_record.get("final_decision_state") or "research_only")
            if state == "removed_from_deliverable_set":
                continue
            out[f"{sleeve_key}::{str(candidate.get('symbol') or '').upper()}"] = {
                "symbol": candidate.get("symbol"),
                "name": candidate.get("name"),
                "sleeve_key": sleeve_key,
                "recommendation_state": state,
                "recommendation_confidence": quality.get("recommendation_confidence"),
                "benchmark_fit_type": dict(candidate.get("benchmark_assignment") or {}).get("benchmark_fit_type"),
                "benchmark_authority_level": dict(candidate.get("benchmark_assignment") or {}).get("benchmark_authority_level"),
                "primary_pressure_type": dict(candidate.get("eligibility") or {}).get("primary_pressure_type"),
                "readiness_level": dict(candidate.get("data_completeness") or {}).get("readiness_level"),
                "decision_record": decision_record,
            }
    return out


def _change_reasons(*, previous: dict[str, Any] | None, current: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    current_state = str(current.get("recommendation_state") or "")
    previous_state = str((previous or {}).get("recommendation_state") or "")
    if previous is None:
        reasons.append("new market relevance")
    if current_state != previous_state and current_state in {"recommended_primary", "recommended_backup"}:
        reasons.append("improved implementation suitability")
    if str(current.get("benchmark_fit_type") or "") == "strong_fit" and str((previous or {}).get("benchmark_fit_type") or "") != "strong_fit":
        reasons.append("new benchmark support")
    if str(current.get("primary_pressure_type") or "") == "liquidity":
        reasons.append("liquidity deterioration")
    if str(current.get("primary_pressure_type") or "") in {"structure", "tax_wrapper"}:
        reasons.append("governance concern")
    if str(current.get("readiness_level") or "") == "research_visible":
        reasons.append("stale or weak evidence")
    if str(dict(current.get("decision_record") or {}).get("final_decision_state") or "") == "rejected_policy_failure":
        reasons.append("no longer aligned with sleeve mandate")
    if not reasons:
        reasons.append("retained because it remains relevant and not dominated by stronger evidence-backed peers")
    return list(dict.fromkeys(reasons))[:4]


def _removal_reasons(record: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    primary = str(record.get("primary_pressure_type") or "")
    decision_state = str(dict(record.get("decision_record") or {}).get("final_decision_state") or "")
    if decision_state == "rejected_policy_failure":
        reasons.append("no longer aligned with sleeve mandate")
    if primary == "benchmark":
        reasons.append("new benchmark support favored stronger alternatives")
    elif primary == "liquidity":
        reasons.append("liquidity deterioration")
    elif primary in {"structure", "tax_wrapper"}:
        reasons.append("governance concern")
    if not reasons:
        reasons.append("replaced by stronger candidate")
    return list(dict.fromkeys(reasons))[:4]


def _universe_role(record: dict[str, Any]) -> str:
    state = str(record.get("recommendation_state") or "research_only")
    if state in {"recommended_primary", "recommended_backup"}:
        return "deliverable"
    if state == "watchlist_only":
        return "watchlist"
    if state == "research_only":
        return "research"
    return "review"
