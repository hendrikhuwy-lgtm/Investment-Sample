from __future__ import annotations

import sqlite3

from app.config import get_settings
from app.services.blueprint_benchmark_registry import enrich_benchmark_assignment
from app.services.blueprint_candidate_eligibility import evaluate_candidate_eligibility
from app.services.blueprint_recommendations import build_recommendation_events
from app.services.blueprint_store import create_blueprint_snapshot, diff_blueprint_snapshots
from app.services.portfolio_blueprint import build_portfolio_blueprint_payload


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_blueprint_payload_contains_quality_scores_and_recommendations() -> None:
    payload = build_portfolio_blueprint_payload()
    assert payload["blueprint_meta"]["recommendation_summary"]["eligible_count"] >= 1
    assert payload["blueprint_meta"]["score_models"]
    assert payload["blueprint_meta"]["benchmark_registry"]["entries"]
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    assert sleeve["recommendation"]["score_version"]
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    assert candidate["investment_quality"]["score_version"]
    assert candidate["investment_quality"]["investment_thesis"]
    assert candidate["benchmark_assignment"]["benchmark_key"]
    assert candidate["benchmark_assignment"]["benchmark_kind"] in {"proxy", "direct", "sleeve_default"}
    assert candidate["benchmark_assignment"]["benchmark_effect_type"]
    assert candidate["benchmark_assignment"]["benchmark_explanation"]
    if sleeve["recommendation"]["our_pick_symbol"]:
        chosen = next(item for item in sleeve["candidates"] if item["symbol"] == sleeve["recommendation"]["our_pick_symbol"])
        assert chosen["investment_quality"]["rank_in_sleeve"] == 1
        assert chosen["investment_quality"]["badge"] in {"best_in_class", "recommended"}
    else:
        assert "No eligible candidate currently clears the recommendation gate." in sleeve["recommendation"]["why_this_pick_wins"]


def test_ineligible_candidate_is_not_promoted_to_top_badge() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "cash_bills")
    placeholder_rows = list(sleeve.get("policy_placeholders") or []) + list(sleeve.get("strategy_placeholders") or [])
    candidate = next(item for item in placeholder_rows if item["symbol"] == "UCITS_MMF_PLACEHOLDER")
    assert candidate["source_state"] == "policy_placeholder"
    assert candidate.get("action_readiness") == "blocked_by_data_quality"
    assert "placeholder" in str(candidate.get("source_state_note") or "").lower()


def test_snapshot_diff_includes_recommendation_changes() -> None:
    conn = _conn()
    try:
        payload_a = build_portfolio_blueprint_payload()
        snapshot_a = create_blueprint_snapshot(conn, blueprint_payload=payload_a, note="baseline")
        payload_b = build_portfolio_blueprint_payload()
        sleeve = next(item for item in payload_b["sleeves"] if item["sleeve_key"] == "global_equity_core")
        top = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
        alt = next(item for item in sleeve["candidates"] if item["symbol"] == "CSPX")
        top["investment_quality"]["badge"] = "acceptable"
        top["investment_quality"]["rank_in_sleeve"] = 3
        alt["investment_quality"]["badge"] = "best_in_class"
        alt["investment_quality"]["rank_in_sleeve"] = 1
        sleeve["recommendation"]["our_pick_symbol"] = "CSPX"
        snapshot_b = create_blueprint_snapshot(conn, blueprint_payload=payload_b, note="re-ranked")
        diff = diff_blueprint_snapshots(conn, snapshot_a=snapshot_a["snapshot_id"], snapshot_b=snapshot_b["snapshot_id"])
        assert diff["diff"]["recommendation_changes"]
        assert diff["diff"]["sleeve_pick_changes"]
        assert any(item["after"] == "CSPX" for item in diff["diff"]["sleeve_pick_changes"])
    finally:
        conn.close()


def test_score_models_expose_benchmark_aware_phase_two_requirements() -> None:
    payload = build_portfolio_blueprint_payload()
    models = {item["score_version"]: item for item in payload["blueprint_meta"]["score_models"]}
    v2 = models["quality_v2_performance_enabled"]
    assert "benchmark_proxy" in v2["requires"]
    assert "benchmark_relative_returns" in v2["requires"]
    assert "tracking_error" in v2["requires"]


def test_core_sleeve_candidate_with_linked_not_validated_truth_is_data_incomplete() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    candidate["source_state"] = "source_linked_not_validated"
    candidate["freshness_state"] = "fresh"
    candidate["benchmark_assignment"]["benchmark_confidence"] = "high"
    candidate["benchmark_assignment"]["validation_status"] = "validated"
    candidate["investment_lens"]["comparison_readiness"] = {"status": "ready", "blockers": []}
    candidate["latest_fetch_status"] = {"status": "failed"}
    result = evaluate_candidate_eligibility(
        candidate=candidate,
        sleeve_key="global_equity_core",
        settings=get_settings(),
        now=__import__("datetime").datetime.now(__import__("datetime").UTC),
    )
    assert result["eligibility_state"] in {"data_incomplete", "ineligible"}
    assert any("linked but not validated" in item for item in result["eligibility_blockers"])
    assert result["pressures"]
    assert result["primary_pressure_type"] in {"readiness", "data", "benchmark"}


def test_proxy_benchmark_cases_are_explained_conservatively() -> None:
    assignment = enrich_benchmark_assignment(
        {
            "benchmark_key": "FTSE_ALL_WORLD",
            "benchmark_label": "FTSE All-World proxy",
            "benchmark_proxy_symbol": "ACWI",
            "benchmark_source_type": "proxy_etf",
            "benchmark_confidence": "medium",
            "assignment_source": "registry_symbol",
            "validation_status": "proxy_matched",
            "validation_notes": ["Proxy benchmark matched only."],
            "allowed_proxy_flag": True,
        },
        sleeve_key="global_equity_core",
    )
    assert assignment["benchmark_kind"] == "proxy"
    assert assignment["benchmark_effect_type"] == "benchmark_fit_proxy_acceptable"
    assert "proxy" in str(assignment["benchmark_explanation"]).lower()
    assert "fully decisive" in str(assignment["recommendation_confidence_effect"]).lower()


def test_pressure_types_are_separated_and_ranked() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    candidate["source_state"] = "source_linked_not_validated"
    candidate["freshness_state"] = "stale"
    candidate["benchmark_assignment"]["validation_status"] = "proxy_matched"
    candidate["benchmark_assignment"]["benchmark_effect_type"] = "benchmark_fit_proxy_acceptable"
    candidate["benchmark_assignment"]["benchmark_explanation"] = "Proxy mapping is acceptable but still weakens confidence."
    result = evaluate_candidate_eligibility(
        candidate=candidate,
        sleeve_key="global_equity_core",
        settings=get_settings(),
        now=__import__("datetime").datetime.now(__import__("datetime").UTC),
    )
    pressures = list(result.get("pressures") or [])
    assert len(pressures) >= 2
    assert pressures[0]["pressure_type"] in {"readiness", "data", "benchmark"}
    assert {item["pressure_type"] for item in pressures}.issuperset({"benchmark", "data"})


def test_thesis_output_is_structured_and_investor_useful() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    quality = dict(candidate.get("investment_quality") or {})
    assert quality.get("fit_for_sleeve")
    assert isinstance(quality.get("better_than_peers") or [], list)
    assert isinstance(quality.get("main_limitations") or [], list)
    assert isinstance(quality.get("confidence_improvers") or [], list)
    assert quality.get("readiness_label") in {"research_visible", "review_ready", "shortlist_ready", "recommendation_ready"}
    assert quality.get("sleeve_expression_summary")


def test_each_candidate_has_explicit_sleeve_expression_logic() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    expression = dict(candidate.get("sleeve_expression") or {})
    assert expression.get("sleeve_purpose")
    assert expression.get("benchmark_role")
    assert expression.get("implementation_priorities")
    assert expression.get("fit_type") in {"direct_fit", "qualified_fit", "partial_fit", "mismatch"}
    assert expression.get("summary")


def test_benchmark_fit_materially_affects_recommendation_confidence() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    strong_confidence = str(dict(candidate.get("investment_quality") or {}).get("recommendation_confidence") or "")

    candidate["benchmark_assignment"]["benchmark_fit_type"] = "weak_proxy"
    candidate["benchmark_assignment"]["benchmark_authority_level"] = "insufficient"
    candidate["benchmark_assignment"]["validation_status"] = "mismatch"

    from app.services.blueprint_investment_quality import build_investment_quality_score

    rescored = build_investment_quality_score(
        candidate=candidate,
        sleeve_key="global_equity_core",
        sleeve_candidates=list(sleeve.get("candidates") or []),
        eligibility=dict(candidate.get("eligibility") or {}),
        performance_metrics=dict(candidate.get("performance_metrics") or {}),
    )
    assert strong_confidence in {"high", "medium", "low"}
    assert rescored["recommendation_confidence"] == "low"


def test_factsheet_tracking_difference_is_used_when_proxy_history_is_partial() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    metrics = dict(candidate.get("performance_metrics") or {})
    assert candidate["benchmark_assignment"]["benchmark_fit_type"] == "strong_fit"
    assert metrics.get("tracking_difference_1y") is not None or metrics.get("tracking_difference_3y") is not None


def test_pressures_have_state_not_just_type() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    candidate["source_state"] = "source_linked_not_validated"
    candidate["freshness_state"] = "stale"
    candidate["benchmark_assignment"]["validation_status"] = "proxy_matched"
    candidate["benchmark_assignment"]["benchmark_fit_type"] = "acceptable_proxy"
    candidate["benchmark_assignment"]["benchmark_authority_level"] = "limited"
    history = [
        {
            "detail": {
                "confidence_snapshot": {
                    "after": {
                        "pressure_snapshot": [
                            {"pressure_type": "data", "severity": "important"},
                            {"pressure_type": "benchmark", "severity": "important"},
                        ]
                    }
                }
            }
        }
    ]
    result = evaluate_candidate_eligibility(
        candidate=candidate,
        sleeve_key="global_equity_core",
        settings=get_settings(),
        now=__import__("datetime").datetime.now(__import__("datetime").UTC),
        candidate_history=history,
    )
    pressures = list(result.get("pressures") or [])
    assert pressures
    data_pressure = next(item for item in pressures if item["pressure_type"] == "data")
    assert data_pressure["trend"] in {"stable", "worsening", "emerging"}
    assert data_pressure["persistence"] in {"repeated", "persistent", "structural", "isolated"}
    assert data_pressure["review_relevance"] in {"low", "medium", "high"}


def test_recommendation_history_explains_why_candidate_changed() -> None:
    before = build_portfolio_blueprint_payload()
    after = build_portfolio_blueprint_payload()
    sleeve = next(item for item in after["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    candidate["benchmark_assignment"]["benchmark_effect_type"] = "benchmark_fit_weak"
    candidate["data_completeness"]["readiness_level"] = "review_ready"
    candidate["investment_quality"]["composite_score"] = max(0, float(candidate["investment_quality"]["composite_score"]) - 8)
    candidate["investment_quality"]["rank_in_sleeve"] = 3
    candidate["investment_quality"]["badge"] = "acceptable"
    events = build_recommendation_events(prior_payload=before, current_payload=after)
    assert events
    match = next((item for item in events if item["candidate_symbol"] == "VWRA" and item["sleeve_key"] == "global_equity_core"), None)
    assert match is not None
    assert match["detail"]["change_driver"]["driver"] in {"benchmark_support_change", "quality_score_change", "data_or_readiness_change"}
    assert "score_delta_summary" in match["detail"]
    assert "explanation_snapshot" in match["detail"]
    assert "confidence_snapshot" in match["detail"]


def test_sleeve_without_pick_explains_dominant_missing_categories() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(
        (
            item
            for item in payload["sleeves"]
            if not dict(item.get("recommendation") or {}).get("our_pick_symbol")
        ),
        None,
    )
    assert sleeve is not None
    recommendation = dict(sleeve.get("recommendation") or {})
    assert recommendation.get("our_pick_symbol") in {None, "", "No current pick"}
    readiness_summary = recommendation.get("readiness_summary") or {}
    assert isinstance(readiness_summary.get("dominant_missing_categories") or [], list)
    assert recommendation.get("no_current_pick_reason")
    if readiness_summary.get("dominant_missing_categories"):
        joined = str(recommendation.get("no_current_pick_reason") or "").lower()
        assert "dominant missing categories" in joined
    assert isinstance(readiness_summary.get("dominant_pressure_types") or [], list)


def test_recommendation_flip_conditions_and_stability_are_visible() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    recommendation = dict(sleeve.get("recommendation") or {})
    if recommendation.get("our_pick_symbol"):
        assert recommendation.get("winner_stability") in {"robust", "watch_stable", "fragile", "unstable"}
        assert isinstance(recommendation.get("lead_drivers") or [], list)
        assert isinstance(recommendation.get("flip_conditions") or [], list)


def test_payload_exposes_escalation_confidence_history_and_investor_consequence() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    assert dict(candidate.get("review_escalation") or {}).get("level") in {"informational", "watch", "review", "urgent_review"}
    assert dict(candidate.get("confidence_history") or {}).get("summary")
    consequence = dict(candidate.get("investor_consequence_summary") or {})
    assert consequence.get("implementation_quality_effect")
    assert consequence.get("benchmark_comparison_effect")
    assert consequence.get("investment_trust_effect")


def test_failed_refresh_degrades_action_readiness_even_with_prior_validated_data() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    candidate["source_state"] = "source_validated"
    candidate["freshness_state"] = "fresh"
    candidate["latest_fetch_status"] = {"status": "failed"}
    candidate["investment_quality"]["badge"] = "best_in_class"
    candidate["investment_quality"]["eligibility_state"] = "eligible"

    from app.services.portfolio_blueprint import _candidate_truth_state

    truth = _candidate_truth_state(
        candidate,
        settings=get_settings(),
        now=__import__("datetime").datetime.now(__import__("datetime").UTC),
    )
    assert truth["display_source_state"] == "refresh_failed_using_last_validated"
    assert truth["action_readiness"] != "action_ready_for_shortlist"
