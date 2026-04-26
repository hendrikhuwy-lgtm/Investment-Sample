from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.services.blueprint_decision_semantics import build_confidence_snapshot
from app.services.blueprint_decision_semantics import resolve_user_facing_decision_state
from app.services.blueprint_recommendation_diff import build_recommendation_diff
from app.services.upstream_truth_contract import normalize_source_state_base


CORE_PASSIVE_SLEEVES = {"global_equity_core", "developed_ex_us_optional", "emerging_markets", "china_satellite", "ig_bonds", "cash_bills"}


def ensure_recommendation_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sleeve_recommendations (
          recommendation_id TEXT PRIMARY KEY,
          snapshot_id TEXT,
          sleeve_key TEXT NOT NULL,
          our_pick_symbol TEXT,
          top_candidates_json TEXT NOT NULL,
          acceptable_candidates_json TEXT NOT NULL,
          caution_candidates_json TEXT NOT NULL,
          why_this_pick_wins TEXT,
          what_would_change_the_pick TEXT,
          missing_data_json TEXT NOT NULL,
          score_version TEXT NOT NULL,
          as_of_date TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recommendation_events (
          event_id TEXT PRIMARY KEY,
          snapshot_id TEXT,
          sleeve_key TEXT NOT NULL,
          candidate_symbol TEXT NOT NULL,
          prior_rank INTEGER,
          new_rank INTEGER,
          prior_badge TEXT,
          new_badge TEXT,
          score_version TEXT NOT NULL,
          ips_version TEXT,
          governance_summary_json TEXT,
          market_state_snapshot_json TEXT,
          detail_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(recommendation_events)").fetchall()}
    for column, ddl in (
        ("ips_version", "TEXT"),
        ("governance_summary_json", "TEXT"),
        ("market_state_snapshot_json", "TEXT"),
    ):
        if column not in existing:
            conn.execute(f"ALTER TABLE recommendation_events ADD COLUMN {column} {ddl}")
    conn.commit()


def rank_sleeve_candidates(
    *,
    sleeve_key: str,
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ranked = sorted(
        candidates,
        key=lambda item: (
            _decision_order(str(dict(item.get("decision_record") or {}).get("final_decision_state") or "research_only")),
            _eligibility_order(str(dict(item.get("investment_quality") or {}).get("eligibility_state") or "data_incomplete")),
            _truth_rank(item),
            -float(dict(item.get("investment_quality") or {}).get("composite_score") or -1e9),
            -float(dict(item.get("sg_lens") or {}).get("score") or -1e9),
            str(item.get("symbol") or ""),
        ),
    )
    eligible = [item for item in ranked if str(dict(item.get("investment_quality") or {}).get("eligibility_state") or "") in {"eligible", "eligible_with_caution"}]
    total = len(ranked)
    for index, candidate in enumerate(ranked, start=1):
        quality = dict(candidate.get("investment_quality") or {})
        quality["rank_in_sleeve"] = index if eligible and candidate in eligible else None
        quality["percentile_in_sleeve"] = round((1 - ((index - 1) / max(1, total))) * 100.0, 2)
        badge, state, user_facing_state = _assign_badge(candidate=candidate, sleeve_key=sleeve_key, rank=index, total=total)
        quality["badge"] = badge
        quality["recommendation_state"] = state
        quality["user_facing_state"] = user_facing_state
        candidate["investment_quality"] = quality
    _annotate_recommendation_context(sleeve_key=sleeve_key, ranked=ranked)
    summary = _build_sleeve_recommendation(sleeve_key=sleeve_key, ranked=ranked)
    return ranked, summary


def persist_sleeve_recommendations(conn: sqlite3.Connection, *, snapshot_id: str | None, summaries: list[dict[str, Any]]) -> None:
    ensure_recommendation_tables(conn)
    if snapshot_id is not None:
        conn.execute("DELETE FROM sleeve_recommendations WHERE snapshot_id = ?", (snapshot_id,))
    for summary in summaries:
        conn.execute(
            """
            INSERT INTO sleeve_recommendations (
              recommendation_id, snapshot_id, sleeve_key, our_pick_symbol, top_candidates_json,
              acceptable_candidates_json, caution_candidates_json, why_this_pick_wins,
              what_would_change_the_pick, missing_data_json, score_version, as_of_date, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"sleeve_reco_{uuid.uuid4().hex[:12]}",
                snapshot_id,
                summary.get("sleeve_key"),
                summary.get("our_pick_symbol"),
                _to_json(summary.get("top_candidates") or []),
                _to_json(summary.get("acceptable_candidates") or []),
                _to_json(summary.get("caution_candidates") or []),
                summary.get("why_this_pick_wins"),
                summary.get("what_would_change_the_pick"),
                _to_json(summary.get("missing_data") or []),
                summary.get("score_version") or "quality_v1_existing_data_only",
                summary.get("as_of_date") or datetime.now(UTC).date().isoformat(),
                datetime.now(UTC).isoformat(),
            ),
        )
    conn.commit()


def build_recommendation_events(*, prior_payload: dict[str, Any] | None, current_payload: dict[str, Any]) -> list[dict[str, Any]]:
    prior = _flatten_quality(prior_payload or {})
    current = _flatten_quality(current_payload)
    events: list[dict[str, Any]] = []
    for key, after in current.items():
        before = prior.get(key)
        if before is None:
            continue
        before_candidate = _find_candidate(
            current_payload=prior_payload or {},
            sleeve_key=key.split("::", 1)[0],
            candidate_symbol=key.split("::", 1)[1],
        )
        after_candidate = _find_candidate(
            current_payload=current_payload,
            sleeve_key=key.split("::", 1)[0],
            candidate_symbol=key.split("::", 1)[1],
        )
        change_driver = _classify_change_driver(
            before=before,
            after=after,
            before_candidate=before_candidate,
            after_candidate=after_candidate,
        )
        if (
            before.get("rank_in_sleeve") == after.get("rank_in_sleeve")
            and before.get("badge") == after.get("badge")
            and before.get("recommendation_state") == after.get("recommendation_state")
            and not list(change_driver.get("changed_dimensions") or [])
            and str(change_driver.get("before_benchmark_effect") or "") == str(change_driver.get("after_benchmark_effect") or "")
            and str(change_driver.get("before_readiness") or "") == str(change_driver.get("after_readiness") or "")
        ):
            continue
        sleeve_key, candidate_symbol = key.split("::", 1)
        events.append(
            {
                "event_id": f"recommendation_event_{uuid.uuid4().hex[:12]}",
                "snapshot_id": None,
                "sleeve_key": sleeve_key,
                "candidate_symbol": candidate_symbol,
                "prior_rank": before.get("rank_in_sleeve"),
                "new_rank": after.get("rank_in_sleeve"),
                "prior_badge": before.get("badge"),
                "new_badge": after.get("badge"),
                "score_version": after.get("score_version") or "quality_v1_existing_data_only",
                "ips_version": str(dict(current_payload.get("blueprint_meta") or {}).get("ips_linkage", {}).get("ips_version") or ""),
                "governance_summary": {
                    "truth_summary": dict(current_payload.get("blueprint_meta") or {}).get("truth_summary") or {},
                    "data_quality": dict(current_payload.get("blueprint_meta") or {}).get("data_quality") or {},
                    "benchmark_registry": dict(current_payload.get("blueprint_meta") or {}).get("benchmark_registry") or {},
                },
                "market_state_snapshot": {
                    "candidate_symbol": candidate_symbol,
                    "current_candidate": after,
                    "prior_candidate": before,
                },
                "score_delta_summary": _score_delta_summary(before=before, after=after),
                "explanation_snapshot": _explanation_snapshot(
                    before_candidate=before_candidate,
                    after_candidate=after_candidate,
                ),
                "confidence_snapshot": _confidence_snapshot(
                    before_candidate=before_candidate,
                    after_candidate=after_candidate,
                ),
                "detail": {
                    "before": before,
                    "after": after,
                    "score_delta_summary": _score_delta_summary(before=before, after=after),
                    "explanation_snapshot": _explanation_snapshot(
                        before_candidate=before_candidate,
                        after_candidate=after_candidate,
                    ),
                    "confidence_snapshot": _confidence_snapshot(
                        before_candidate=before_candidate,
                        after_candidate=after_candidate,
                    ),
                    "change_driver": change_driver,
                    "recommendation_diff": build_recommendation_diff(
                        {
                            "recommendation_state": before.get("recommendation_state"),
                            "benchmark_fit_type": dict((before_candidate or {}).get("benchmark_assignment") or {}).get("benchmark_fit_type"),
                            "readiness_level": dict((before_candidate or {}).get("data_completeness") or {}).get("readiness_level"),
                            "composite_score": before.get("composite_score"),
                            "recommendation_confidence": before.get("recommendation_confidence"),
                            "candidate_universe_changed": _sleeve_candidate_symbols(prior_payload or {}, sleeve_key) != _sleeve_candidate_symbols(current_payload, sleeve_key),
                            "candidate_universe_reason": "candidate universe changed around the sleeve"
                            if _sleeve_candidate_symbols(prior_payload or {}, sleeve_key) != _sleeve_candidate_symbols(current_payload, sleeve_key)
                            else "",
                            "rejection_reasons": list(dict((before_candidate or {}).get("decision_record") or {}).get("rejection_reasons") or []),
                        },
                        {
                            "recommendation_state": after.get("recommendation_state"),
                            "benchmark_fit_type": dict((after_candidate or {}).get("benchmark_assignment") or {}).get("benchmark_fit_type"),
                            "readiness_level": dict((after_candidate or {}).get("data_completeness") or {}).get("readiness_level"),
                            "composite_score": after.get("composite_score"),
                            "recommendation_confidence": after.get("recommendation_confidence"),
                            "candidate_universe_changed": _sleeve_candidate_symbols(prior_payload or {}, sleeve_key) != _sleeve_candidate_symbols(current_payload, sleeve_key),
                            "candidate_universe_reason": "candidate universe changed around the sleeve"
                            if _sleeve_candidate_symbols(prior_payload or {}, sleeve_key) != _sleeve_candidate_symbols(current_payload, sleeve_key)
                            else "",
                            "rejection_reasons": list(dict((after_candidate or {}).get("decision_record") or {}).get("rejection_reasons") or []),
                        },
                    ),
                },
            }
        )
    return events


def persist_recommendation_events(conn: sqlite3.Connection, *, snapshot_id: str | None, events: list[dict[str, Any]]) -> None:
    ensure_recommendation_tables(conn)
    for event in events:
        conn.execute(
            """
            INSERT INTO recommendation_events (
              event_id, snapshot_id, sleeve_key, candidate_symbol, prior_rank, new_rank,
              prior_badge, new_badge, score_version, ips_version, governance_summary_json,
              market_state_snapshot_json, detail_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.get("event_id"),
                snapshot_id,
                event.get("sleeve_key"),
                event.get("candidate_symbol"),
                event.get("prior_rank"),
                event.get("new_rank"),
                event.get("prior_badge"),
                event.get("new_badge"),
                event.get("score_version"),
                event.get("ips_version"),
                _to_json(event.get("governance_summary") or {}),
                _to_json(
                    {
                        **dict(event.get("market_state_snapshot") or {}),
                        "score_delta_summary": event.get("score_delta_summary") or {},
                        "explanation_snapshot": event.get("explanation_snapshot") or {},
                    }
                ),
                _to_json(
                    {
                        **dict(event.get("detail") or {}),
                        "score_delta_summary": event.get("score_delta_summary") or {},
                        "explanation_snapshot": event.get("explanation_snapshot") or {},
                    }
                ),
                datetime.now(UTC).isoformat(),
            ),
        )
    conn.commit()


def list_recommendation_events(conn: sqlite3.Connection, *, limit: int = 100) -> list[dict[str, Any]]:
    ensure_recommendation_tables(conn)
    rows = conn.execute(
        """
        SELECT event_id, snapshot_id, sleeve_key, candidate_symbol, prior_rank, new_rank,
               prior_badge, new_badge, score_version, ips_version, governance_summary_json,
               market_state_snapshot_json, detail_json, created_at
        FROM recommendation_events
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (max(1, min(limit, 500)),),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in ("governance_summary_json", "market_state_snapshot_json", "detail_json"):
            try:
                item[key[:-5]] = json.loads(str(item.pop(key) or "{}"))
            except Exception:
                item[key[:-5]] = {}
        items.append(item)
    return items


def _assign_badge(*, candidate: dict[str, Any], sleeve_key: str, rank: int, total: int) -> tuple[str, str, str]:
    quality = dict(candidate.get("investment_quality") or {})
    completeness = dict(candidate.get("data_completeness") or {})
    decision_record = dict(candidate.get("decision_record") or {})
    eligibility = str(quality.get("eligibility_state") or "data_incomplete")
    score = float(quality.get("composite_score") or 0.0) if quality.get("composite_score") is not None else None
    confidence = str(quality.get("data_confidence") or "low")
    source_state = str(candidate.get("source_state") or "unknown")
    readiness_level = str(completeness.get("readiness_level") or "research_visible")
    decision_state = str(decision_record.get("final_decision_state") or "")
    policy_gate_state = str(decision_record.get("policy_gate_state") or "")
    data_quality_state = str(decision_record.get("data_quality_state") or "")
    unresolved_required = str(decision_record.get("required_gate_resolution_state") or "") == "unresolved"

    if decision_state == "rejected_policy_failure" or policy_gate_state == "fail":
        recommendation_state = "rejected_policy_failure"
        user_facing_state = "blocked_by_policy"
        return ("caution", recommendation_state, user_facing_state)
    if decision_state == "blocked_by_unresolved_gate" or unresolved_required:
        recommendation_state = "blocked_by_unresolved_gate"
        user_facing_state = "blocked_by_unresolved_gate"
        return ("not_ranked", recommendation_state, user_facing_state)
    if decision_state in {"blocked_by_missing_required_evidence", "rejected_data_insufficient"} or not bool(quality.get("composite_score_valid", True)):
        recommendation_state = "blocked_by_missing_required_evidence"
        user_facing_state = "blocked_by_missing_required_evidence"
        return ("not_ranked" if eligibility == "data_incomplete" else "caution", recommendation_state, user_facing_state)
    if data_quality_state in {"fail", "failed", "unknown_due_to_missing_inputs"}:
        recommendation_state = "blocked_by_missing_required_evidence"
        user_facing_state = "blocked_by_missing_required_evidence"
        return ("not_ranked", recommendation_state, user_facing_state)
    if eligibility in {"ineligible", "data_incomplete"}:
        recommendation_state = "blocked_by_missing_required_evidence" if eligibility == "data_incomplete" else "rejected_policy_failure"
        user_facing_state = "blocked_by_missing_required_evidence" if eligibility == "data_incomplete" else "blocked_by_policy"
        return ("not_ranked" if eligibility == "data_incomplete" else "caution", recommendation_state, user_facing_state)
    source_state = normalize_source_state_base(source_state)
    if source_state in {"stale_live", "broken_source", "strategy_placeholder", "policy_placeholder"} or str(candidate.get("freshness_state") or "") in {"stale", "quarantined"}:
        recommendation_state = "blocked_by_missing_required_evidence"
        user_facing_state = "blocked_by_missing_required_evidence"
        return ("caution", recommendation_state, user_facing_state)
    if sleeve_key in CORE_PASSIVE_SLEEVES and source_state not in {"source_validated", "aging"}:
        recommendation_state = "blocked_by_missing_required_evidence"
        user_facing_state = "blocked_by_missing_required_evidence"
        return ("caution", recommendation_state, user_facing_state)
    if readiness_level == "research_visible":
        recommendation_state = "research_only"
        user_facing_state = "research_ready_but_not_recommendable"
        return ("caution", recommendation_state, user_facing_state)
    if rank == 1 and (score or 0.0) >= 60 and confidence in {"high", "medium"}:
        recommendation_state = "recommended_primary"
    elif rank == 2 and (score or 0.0) >= 60 and confidence in {"high", "medium"}:
        recommendation_state = "recommended_backup"
    elif (score or 0.0) >= 50 and confidence in {"high", "medium"}:
        recommendation_state = "rejected_inferior_to_selected"
    elif (score or 0.0) >= 45:
        recommendation_state = "watchlist_only"
    else:
        recommendation_state = "research_only"

    user_facing_state = resolve_user_facing_decision_state(
        policy_gate_state=policy_gate_state,
        required_gate_resolution_state=str(decision_record.get("required_gate_resolution_state") or "resolved"),
        data_quality_state=data_quality_state,
        scoring_state=str(decision_record.get("scoring_state") or ""),
        recommendation_state=recommendation_state,
        readiness_level=readiness_level,
        recommendation_confidence=confidence,
    )
    badge = {
        "fully_clean_recommendable": "best_in_class",
        "best_available_with_limits": "recommended",
        "research_ready_but_not_recommendable": "acceptable",
        "blocked_by_policy": "caution",
        "blocked_by_missing_required_evidence": "not_ranked",
        "blocked_by_unresolved_gate": "not_ranked",
    }.get(user_facing_state, "caution")
    return (badge, recommendation_state, user_facing_state)


def _build_sleeve_recommendation(*, sleeve_key: str, ranked: list[dict[str, Any]]) -> dict[str, Any]:
    eligible = [item for item in ranked if str(dict(item.get("investment_quality") or {}).get("eligibility_state") or "") in {"eligible", "eligible_with_caution"}]
    recommended_top = [
        item
        for item in eligible
        if str(dict(item.get("investment_quality") or {}).get("recommendation_state") or "") in {"recommended_primary", "recommended_backup"}
    ]
    operationally_usable = [
        item
        for item in eligible
        if str(item.get("action_readiness") or "") in {"usable_now", "usable_with_limits"}
    ]
    top = eligible[:3]
    acceptable = [item for item in ranked if str(dict(item.get("investment_quality") or {}).get("recommendation_state") or "") == "watchlist_only"][:3]
    caution = [
        item
        for item in ranked
        if str(dict(item.get("investment_quality") or {}).get("recommendation_state") or "") in {
            "rejected_policy_failure",
            "rejected_data_insufficient",
            "rejected_inferior_to_selected",
            "research_only",
            "removed_from_deliverable_set",
        }
    ][:3]
    primary = next(
        (
            item
            for item in recommended_top
            if str(dict(item.get("investment_quality") or {}).get("recommendation_state") or "") == "recommended_primary"
        ),
        None,
    )
    our_pick = primary or (recommended_top[0] if recommended_top else None) or (operationally_usable[0] if operationally_usable else None)
    winner_context = dict((our_pick or {}).get("recommendation_context") or {})
    missing = []
    review_priority: list[dict[str, Any]] = []
    for candidate in ranked:
        missing.extend(str(item) for item in list(dict(candidate.get("investment_lens") or {}).get("unknowns_that_matter") or []) if str(item).strip())
        readiness = dict(candidate.get("decision_readiness") or {})
        for pressure in list(readiness.get("pressures") or [])[:2]:
            review_priority.append(
                {
                    "symbol": candidate.get("symbol"),
                    "pressure_type": pressure.get("pressure_type"),
                    "severity": pressure.get("severity"),
                    "label": pressure.get("label"),
                    "detail": pressure.get("detail"),
                    "trend": pressure.get("trend"),
                }
            )
    missing = list(dict.fromkeys(missing))[:8]
    why = "No candidate currently clears policy, data-quality, and confidence requirements for an active recommendation."
    what_changes = "Recommendation would change if stronger cost, liquidity, structure, tax, or confidence evidence appears."
    if our_pick is not None:
        quality = dict(our_pick.get("investment_quality") or {})
        thesis = dict(quality.get("thesis") or {})
        recommendation_result = dict(our_pick.get("recommendation_result") or {})
        why = "; ".join(list(thesis.get("why_it_wins") or [])[:3]) or str(quality.get("investment_thesis") or "Current top pick leads on active quality dimensions.")
        usability_memo = dict(our_pick.get("usability_memo") or {})
        if usability_memo.get("state") == "usable_with_limits":
            why = f"{why} Current use is still limitation-aware: {str(usability_memo.get('summary') or '').strip()}"
        if winner_context.get("lead_drivers"):
            why = f"{why} Lead drivers: {', '.join(str(item) for item in list(winner_context.get('lead_drivers') or [])[:3])}."
        if bool(recommendation_result.get("no_change_is_best")):
            why = f"{why} No change remains the better practical decision until replacement edge clears switching friction."
        what_changes = (
            f"A peer could replace the pick if {', '.join(str(item) for item in list(winner_context.get('flip_conditions') or [])[:3])}."
            if winner_context.get("flip_conditions")
            else "A peer could replace the pick if it remains eligible and materially improves quality on stable dimensions such as cost, liquidity, structure, tax, or verified performance inputs."
        )
    return {
        "sleeve_key": sleeve_key,
        "our_pick_symbol": str(our_pick.get("symbol") or "") if our_pick else None,
        "our_pick": _candidate_summary(our_pick) if our_pick else None,
        "top_candidates": [_candidate_summary(item) for item in top],
        "acceptable_candidates": [_candidate_summary(item) for item in acceptable],
        "caution_candidates": [_candidate_summary(item) for item in caution],
        "why_this_pick_wins": why,
        "what_would_change_the_pick": what_changes,
        "missing_data": missing,
        "common_blockers": _common_blockers(ranked),
        "review_priority": review_priority[:6],
        "nearest_challenger": winner_context.get("nearest_challenger"),
        "winner_stability": winner_context.get("stability"),
        "lead_type": winner_context.get("lead_type"),
        "lead_drivers": list(winner_context.get("lead_drivers") or [])[:4],
        "flip_conditions": list(winner_context.get("flip_conditions") or [])[:4],
        "review_escalation_level": dict((our_pick or {}).get("review_escalation") or {}).get("level") if our_pick else None,
        "score_version": str(dict((our_pick or {}).get("investment_quality") or {}).get("score_version") or "quality_v1_existing_data_only"),
        "as_of_date": str(dict((our_pick or {}).get("investment_quality") or {}).get("as_of_date") or datetime.now(UTC).date().isoformat()),
        "active_recommendation_states": [
            str(dict(item.get("investment_quality") or {}).get("recommendation_state") or "")
            for item in recommended_top[:2]
        ],
        "operational_usability_states": [
            str(item.get("action_readiness") or "")
            for item in operationally_usable[:3]
        ],
    }


def _candidate_summary(candidate: dict[str, Any]) -> dict[str, Any]:
    quality = dict(candidate.get("investment_quality") or {})
    readiness = dict(candidate.get("data_completeness") or {})
    eligibility = dict(candidate.get("eligibility") or {})
    return {
        "symbol": candidate.get("symbol"),
        "name": candidate.get("name"),
        "rank_in_sleeve": quality.get("rank_in_sleeve"),
        "badge": quality.get("badge"),
        "recommendation_state": quality.get("recommendation_state"),
        "composite_score": quality.get("composite_score"),
        "eligibility_state": quality.get("eligibility_state"),
        "data_confidence": quality.get("data_confidence"),
        "readiness_level": readiness.get("readiness_level"),
        "action_readiness": candidate.get("action_readiness"),
        "primary_pressure_type": eligibility.get("primary_pressure_type"),
        "benchmark_effect_type": dict(candidate.get("benchmark_assignment") or {}).get("benchmark_effect_type"),
        "recommendation_stability": dict(candidate.get("recommendation_context") or {}).get("stability"),
        "review_escalation_level": dict(candidate.get("review_escalation") or {}).get("level"),
    }


def _common_blockers(candidates: list[dict[str, Any]]) -> list[str]:
    items: list[str] = []
    for candidate in candidates:
        items.extend(str(item) for item in list(dict(candidate.get("investment_quality") or {}).get("eligibility_blockers") or []) if str(item).strip())
    counts: dict[str, int] = {}
    for item in items:
        counts[item] = counts.get(item, 0) + 1
    return [item for item, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:6]]


def _truth_rank(candidate: dict[str, Any]) -> int:
    source_state = normalize_source_state_base(str(candidate.get("display_source_state") or candidate.get("source_state") or "unknown"))
    freshness_state = str(candidate.get("freshness_state") or "unknown")
    if source_state == "source_validated" and freshness_state in {"fresh", "aging"}:
        return 0
    if source_state in {"manual_seed"} and freshness_state in {"fresh", "aging", "unknown"}:
        return 1
    if source_state == "aging":
        return 1
    if source_state == "stale_live" or freshness_state == "stale":
        return 2
    if source_state in {"broken_source", "strategy_placeholder", "policy_placeholder"} or freshness_state == "quarantined":
        return 3
    return 4


def _eligibility_order(state: str) -> int:
    return {
        "eligible": 0,
        "eligible_with_caution": 1,
        "data_incomplete": 2,
        "ineligible": 3,
    }.get(str(state), 4)


def _decision_order(state: str) -> int:
    return {
        "recommended_primary": 0,
        "recommended_backup": 1,
        "watchlist_only": 2,
        "rejected_inferior_to_selected": 3,
        "research_only": 4,
        "rejected_data_insufficient": 5,
        "rejected_policy_failure": 6,
        "removed_from_deliverable_set": 7,
    }.get(str(state), 8)


def _flatten_quality(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for sleeve in list(payload.get("sleeves") or []):
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        for candidate in list(sleeve.get("candidates") or []):
            key = f"{sleeve_key}::{str(candidate.get('symbol') or '')}"
            out[key] = dict(candidate.get("investment_quality") or {})
    return out


def _find_candidate(*, current_payload: dict[str, Any], sleeve_key: str, candidate_symbol: str) -> dict[str, Any] | None:
    for sleeve in list(current_payload.get("sleeves") or []):
        if str(sleeve.get("sleeve_key") or "") != sleeve_key:
            continue
        for candidate in list(sleeve.get("candidates") or []):
            if str(candidate.get("symbol") or "").upper() == candidate_symbol.upper():
                return dict(candidate)
    return None


def _sleeve_candidate_symbols(payload: dict[str, Any], sleeve_key: str) -> set[str]:
    for sleeve in list(payload.get("sleeves") or []):
        if str(sleeve.get("sleeve_key") or "") != sleeve_key:
            continue
        return {
            str(candidate.get("symbol") or "").upper()
            for candidate in list(sleeve.get("candidates") or [])
            if str(candidate.get("symbol") or "").strip()
        }
    return set()


def _score_delta_summary(*, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    dimensions = (
        "composite_score",
        "cost_score",
        "liquidity_score",
        "structure_score",
        "tax_score",
        "performance_score",
        "risk_adjusted_score",
        "governance_confidence_score",
    )
    deltas: dict[str, Any] = {}
    changed_dimensions: list[str] = []
    for field in dimensions:
        before_value = _safe_float(before.get(field))
        after_value = _safe_float(after.get(field))
        if before_value is None and after_value is None:
            continue
        delta = None if before_value is None or after_value is None else round(after_value - before_value, 2)
        deltas[field] = {"before": before_value, "after": after_value, "delta": delta}
        if delta not in {None, 0.0}:
            changed_dimensions.append(field)
    return {
        "changed_dimensions": changed_dimensions,
        "dimensions": deltas,
    }


def _explanation_snapshot(*, before_candidate: dict[str, Any] | None, after_candidate: dict[str, Any] | None) -> dict[str, Any]:
    before_quality = dict((before_candidate or {}).get("investment_quality") or {})
    after_quality = dict((after_candidate or {}).get("investment_quality") or {})
    before_eligibility = dict((before_candidate or {}).get("eligibility") or {})
    after_eligibility = dict((after_candidate or {}).get("eligibility") or {})
    return {
        "before": {
            "structured_summary": before_quality.get("structured_summary"),
            "weakness_families": before_quality.get("weakness_families") or [],
            "primary_pressure_type": before_eligibility.get("primary_pressure_type"),
            "benchmark_effect_type": dict((before_candidate or {}).get("benchmark_assignment") or {}).get("benchmark_effect_type"),
            "readiness_level": dict((before_candidate or {}).get("data_completeness") or {}).get("readiness_level"),
            "pressure_snapshot": list(before_eligibility.get("pressures") or []),
        },
        "after": {
            "structured_summary": after_quality.get("structured_summary"),
            "weakness_families": after_quality.get("weakness_families") or [],
            "primary_pressure_type": after_eligibility.get("primary_pressure_type"),
            "benchmark_effect_type": dict((after_candidate or {}).get("benchmark_assignment") or {}).get("benchmark_effect_type"),
            "readiness_level": dict((after_candidate or {}).get("data_completeness") or {}).get("readiness_level"),
            "pressure_snapshot": list(after_eligibility.get("pressures") or []),
        },
    }


def _confidence_snapshot(*, before_candidate: dict[str, Any] | None, after_candidate: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "before": build_confidence_snapshot(candidate=before_candidate or {}) if before_candidate else {},
        "after": build_confidence_snapshot(candidate=after_candidate or {}) if after_candidate else {},
    }


def _classify_change_driver(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    before_candidate: dict[str, Any] | None,
    after_candidate: dict[str, Any] | None,
) -> dict[str, Any]:
    before_benchmark = dict((before_candidate or {}).get("benchmark_assignment") or {})
    after_benchmark = dict((after_candidate or {}).get("benchmark_assignment") or {})
    before_completeness = dict((before_candidate or {}).get("data_completeness") or {})
    after_completeness = dict((after_candidate or {}).get("data_completeness") or {})
    changed_dimensions = list((_score_delta_summary(before=before, after=after).get("changed_dimensions") or []))
    if str(before_benchmark.get("benchmark_effect_type") or "") != str(after_benchmark.get("benchmark_effect_type") or ""):
        driver = "benchmark_support_change"
    elif str(before_completeness.get("readiness_level") or "") != str(after_completeness.get("readiness_level") or "") and not changed_dimensions:
        driver = "data_or_readiness_change"
    elif changed_dimensions:
        driver = "quality_score_change"
    else:
        driver = "ranking_only_change"
    return {
        "driver": driver,
        "changed_dimensions": changed_dimensions,
        "before_readiness": before_completeness.get("readiness_level"),
        "after_readiness": after_completeness.get("readiness_level"),
        "before_benchmark_effect": before_benchmark.get("benchmark_effect_type"),
        "after_benchmark_effect": after_benchmark.get("benchmark_effect_type"),
    }


def _annotate_recommendation_context(*, sleeve_key: str, ranked: list[dict[str, Any]]) -> None:
    eligible = [
        item
        for item in ranked
        if str(dict(item.get("investment_quality") or {}).get("eligibility_state") or "") in {"eligible", "eligible_with_caution"}
    ]
    winner = eligible[0] if eligible else None
    challenger = eligible[1] if len(eligible) > 1 else None
    lead_drivers, flip_conditions, lead_gap = _compare_winner_and_challenger(winner=winner, challenger=challenger)
    stability, lead_type = _classify_stability(winner=winner, challenger=challenger, lead_gap=lead_gap)
    for candidate in ranked:
        symbol = str(candidate.get("symbol") or "")
        role = "trailing_candidate"
        if winner is not None and symbol == str(winner.get("symbol") or ""):
            role = "winner"
        elif challenger is not None and symbol == str(challenger.get("symbol") or ""):
            role = "nearest_challenger"
        candidate["recommendation_context"] = {
            "role": role,
            "winner_symbol": winner.get("symbol") if winner else None,
            "nearest_challenger": _candidate_summary(challenger) if challenger else None,
            "lead_drivers": lead_drivers if role == "winner" else [],
            "flip_conditions": flip_conditions if role in {"winner", "nearest_challenger"} else [],
            "lead_gap": lead_gap,
            "stability": stability,
            "lead_type": lead_type,
            "why_now": (
                "Current winner remains ahead on sleeve expression, benchmark trust, and readiness."
                if role == "winner"
                else "This candidate remains the nearest challenger to the current winner."
                if role == "nearest_challenger"
                else "This candidate is not currently in the closest recommendation flip path."
            ),
        }


def _compare_winner_and_challenger(*, winner: dict[str, Any] | None, challenger: dict[str, Any] | None) -> tuple[list[str], list[str], float]:
    if winner is None:
        return [], [], 0.0
    winner_quality = dict((winner or {}).get("investment_quality") or {})
    challenger_quality = dict((challenger or {}).get("investment_quality") or {})
    winner_score = float(winner_quality.get("composite_score") or 0.0)
    challenger_score = float(challenger_quality.get("composite_score") or 0.0) if challenger else 0.0
    lead_gap = round(winner_score - challenger_score, 2)
    lead_drivers: list[str] = []
    flip_conditions: list[str] = []

    winner_benchmark = dict((winner or {}).get("benchmark_assignment") or {})
    challenger_benchmark = dict((challenger or {}).get("benchmark_assignment") or {})
    if str(winner_benchmark.get("benchmark_authority_level") or "") != str(challenger_benchmark.get("benchmark_authority_level") or ""):
        lead_drivers.append("winner has stronger benchmark authority")
        flip_conditions.append("challenger gains stronger benchmark authority or winner loses benchmark trust")
    if str(dict((winner or {}).get("data_completeness") or {}).get("readiness_level") or "") != str(dict((challenger or {}).get("data_completeness") or {}).get("readiness_level") or ""):
        lead_drivers.append("winner clears a stronger readiness tier")
        flip_conditions.append("winner readiness drops or challenger clears the same readiness tier")
    if lead_gap > 0:
        lead_drivers.append(f"winner keeps a {lead_gap:.2f} composite-score lead")
        flip_conditions.append("challenger closes the current composite-score gap")
    winner_expr = dict((winner or {}).get("sleeve_expression") or {})
    challenger_expr = dict((challenger or {}).get("sleeve_expression") or {})
    if str(winner_expr.get("fit_type") or "") != str(challenger_expr.get("fit_type") or ""):
        lead_drivers.append("winner has cleaner sleeve-expression fit")
        flip_conditions.append("challenger improves sleeve-expression fit or winner becomes a larger compromise")
    if not lead_drivers:
        lead_drivers.append("winner remains ahead mainly on cumulative quality and readiness")
    if not flip_conditions:
        flip_conditions.append("challenger materially improves on sleeve fit, readiness, or benchmark trust")
    return list(dict.fromkeys(lead_drivers))[:4], list(dict.fromkeys(flip_conditions))[:4], lead_gap


def _classify_stability(*, winner: dict[str, Any] | None, challenger: dict[str, Any] | None, lead_gap: float) -> tuple[str, str]:
    if winner is None:
        return "unstable", "none"
    winner_pressures = list(dict((winner or {}).get("eligibility") or {}).get("pressures") or [])
    readiness = str(dict((winner or {}).get("data_completeness") or {}).get("readiness_level") or "research_visible")
    if readiness != "recommendation_ready":
        return "unstable", "fragile"
    if any(str(item.get("severity") or "") == "critical" for item in winner_pressures):
        return "fragile", "fragile"
    if challenger is None:
        return "robust", "structural"
    if lead_gap >= 7:
        return "robust", "structural"
    if lead_gap >= 3:
        return "watch_stable", "temporary"
    if lead_gap > 0:
        return "fragile", "fragile"
    return "unstable", "fragile"


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _to_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True)
