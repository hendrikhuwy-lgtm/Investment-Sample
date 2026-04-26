from __future__ import annotations

from typing import Any


DELIVERABLE_ACTIVE_STATES = {"recommended_primary", "recommended_backup", "watchlist_only", "research_only"}


def build_deliverable_candidates(
    *,
    current_payload: dict[str, Any],
    previous_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    previous_lookup = _candidate_state_lookup(previous_payload or {})
    current_lookup = _candidate_state_lookup(current_payload)
    sleeves: dict[str, list[dict[str, Any]]] = {}
    entered: list[dict[str, Any]] = []
    exited: list[dict[str, Any]] = []

    for key, record in current_lookup.items():
        prior = previous_lookup.get(key)
        current_state = str(record.get("recommendation_state") or "research_only")
        if prior is None:
            change_state = "added"
        elif str(prior.get("recommendation_state") or "") != current_state:
            change_state = "under_review" if current_state in {"watchlist_only", "research_only"} else "retained"
        else:
            change_state = "retained"
        if current_state not in DELIVERABLE_ACTIVE_STATES:
            change_state = "under_review"
        item = {
            "candidate_name": record.get("name"),
            "candidate_symbol": record.get("symbol"),
            "sleeve": record.get("sleeve_key"),
            "change_state": change_state,
            "change_reasons": _change_reasons(previous=prior, current=record),
            "effective_confidence": record.get("recommendation_confidence"),
            "recommendation_eligible": current_state in {"recommended_primary", "recommended_backup"},
            "research_only": current_state == "research_only",
        }
        sleeves.setdefault(str(record.get("sleeve_key") or ""), []).append(item)
        if change_state == "added":
            entered.append(item)

    for key, record in previous_lookup.items():
        if key in current_lookup:
            continue
        exited.append(
            {
                "candidate_name": record.get("name"),
                "candidate_symbol": record.get("symbol"),
                "sleeve": record.get("sleeve_key"),
                "change_state": "removed",
                "change_reasons": _removal_reasons(record),
                "effective_confidence": record.get("recommendation_confidence"),
                "recommendation_eligible": False,
                "research_only": False,
            }
        )

    return {
        "by_sleeve": sleeves,
        "entered": entered,
        "exited": exited,
        "summary": {
            "current_deliverable_count": len(current_lookup),
            "entered_count": len(entered),
            "exited_count": len(exited),
        },
    }


def build_deliverable_candidates_diff(*, current_payload: dict[str, Any], previous_payload: dict[str, Any] | None) -> dict[str, Any]:
    data = build_deliverable_candidates(current_payload=current_payload, previous_payload=previous_payload)
    return {
        "entered": data.get("entered") or [],
        "exited": data.get("exited") or [],
        "summary": data.get("summary") or {},
    }


def _candidate_state_lookup(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for sleeve in list(payload.get("sleeves") or []):
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        for candidate in list(sleeve.get("candidates") or []):
            quality = dict(candidate.get("investment_quality") or {})
            state = str(quality.get("recommendation_state") or "research_only")
            if state == "removed_from_deliverable_set":
                continue
            out[f"{sleeve_key}::{str(candidate.get('symbol') or '').upper()}"] = {
                "symbol": candidate.get("symbol"),
                "name": candidate.get("name"),
                "sleeve_key": sleeve_key,
                "recommendation_state": state,
                "recommendation_confidence": quality.get("recommendation_confidence"),
                "benchmark_fit_type": dict(candidate.get("benchmark_assignment") or {}).get("benchmark_fit_type"),
                "primary_pressure_type": dict(candidate.get("eligibility") or {}).get("primary_pressure_type"),
                "decision_record": dict(candidate.get("decision_record") or {}),
            }
    return out


def _change_reasons(*, previous: dict[str, Any] | None, current: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if previous is None:
        reasons.append("new market relevance")
    if str(current.get("benchmark_fit_type") or "") == "strong_fit" and str((previous or {}).get("benchmark_fit_type") or "") != "strong_fit":
        reasons.append("new benchmark support")
    if str(current.get("primary_pressure_type") or "") == "liquidity":
        reasons.append("liquidity deterioration")
    if str(current.get("primary_pressure_type") or "") == "benchmark":
        reasons.append("changed macro regime relevance")
    if str(dict(current.get("decision_record") or {}).get("final_decision_state") or "") == "rejected_policy_failure":
        reasons.append("no longer aligned with sleeve mandate")
    if not reasons:
        reasons.append("retained because it remains relevant and not dominated by stronger evidence-backed peers")
    return list(dict.fromkeys(reasons))[:4]


def _removal_reasons(record: dict[str, Any]) -> list[str]:
    reasons = []
    primary = str(record.get("primary_pressure_type") or "")
    if primary == "benchmark":
        reasons.append("new benchmark support favored stronger alternatives")
    elif primary == "liquidity":
        reasons.append("liquidity deterioration")
    elif primary in {"structure", "tax_wrapper"}:
        reasons.append("governance concern")
    else:
        reasons.append("replaced by stronger candidate")
    return reasons
