from __future__ import annotations

import sqlite3

from app.config import Settings
from app.services.blueprint_data_quality import summarize_blueprint_data_quality
from app.services.blueprint_decisions import (
    ensure_blueprint_decision_tables,
    list_candidate_decision_events,
    list_candidate_decisions,
    upsert_candidate_decision,
)
from app.services.citation_health import ensure_citation_health_tables, summarize_citation_health
from app.services.portfolio_blueprint import build_portfolio_blueprint_payload


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_data_quality_summary_quarantines_old_metrics() -> None:
    settings = Settings.from_env()
    summary = summarize_blueprint_data_quality(
        candidates=[{"symbol": "VWRA", "factsheet_asof": "2025-01-01"}],
        citations=[{"source_id": "x", "url": "https://example.com", "retrieved_at": "2025-01-01T00:00:00+00:00"}],
        regime_context={"monitor_records": [{"metric_key": "DGS10", "metric_name": "US 10Y Treasury yield", "observed_at": "2025-01-01T00:00:00+00:00"}]},
        settings=settings,
        now=__import__("datetime").datetime(2026, 3, 6, tzinfo=__import__("datetime").timezone.utc),
    )
    assert summary["freshness"] in {"stale", "quarantined"}
    assert summary["stale_metrics_count"] >= 1
    assert "quarantined_metrics_count" in summary
    assert "fallback_metrics_count" in summary
    assert "policy" in summary
    assert "exclusions" in summary


def test_citation_health_defaults_to_unknown_without_network_refresh() -> None:
    conn = _conn()
    try:
        ensure_citation_health_tables(conn)
        summary = summarize_citation_health(
            conn,
            [{"source_id": "foo", "url": "https://example.com", "retrieved_at": "2026-03-01T00:00:00+00:00"}],
            settings=Settings.from_env(),
            force_refresh=False,
        )
        assert summary["overall_status"] == "unknown"
        assert summary["counts"]["unknown"] == 1
        assert "retention_counts" in summary
        assert "hashed_documents_count" in summary
    finally:
        conn.close()


def test_candidate_decision_state_is_persisted() -> None:
    conn = _conn()
    try:
        ensure_blueprint_decision_tables(conn)
        decision = upsert_candidate_decision(
            conn,
            sleeve_key="global_equity_core",
            candidate_symbol="VWRA",
            status="approved",
            note="Reviewed for current shortlist.",
            actor_id="local_actor",
        )
        assert decision["status"] == "approved"
        decisions = list_candidate_decisions(conn)
        assert decisions[("global_equity_core", "VWRA")]["note"] == "Reviewed for current shortlist."
    finally:
        conn.close()


def test_candidate_decision_requires_rationale_for_approval_and_rejection() -> None:
    conn = _conn()
    try:
        ensure_blueprint_decision_tables(conn)
        for status in ("approved", "rejected", "manual_override"):
            try:
                upsert_candidate_decision(
                    conn,
                    sleeve_key="global_equity_core",
                    candidate_symbol="VWRA",
                    status=status,
                    note="",
                    actor_id="local_actor",
                )
            except ValueError as exc:
                assert "requires rationale" in str(exc)
            else:
                raise AssertionError(f"{status} should require rationale")
    finally:
        conn.close()


def test_candidate_decision_events_are_listed() -> None:
    conn = _conn()
    try:
        ensure_blueprint_decision_tables(conn)
        upsert_candidate_decision(
            conn,
            sleeve_key="global_equity_core",
            candidate_symbol="VWRA",
            status="proposed",
            note="Initial shortlist review.",
            actor_id="local_actor",
        )
        upsert_candidate_decision(
            conn,
            sleeve_key="global_equity_core",
            candidate_symbol="VWRA",
            status="approved",
            note="Approved after implementation review.",
            actor_id="local_actor",
        )
        events = list_candidate_decision_events(conn)
        bucket = events[("global_equity_core", "VWRA")]
        assert len(bucket) == 2
        assert bucket[0]["new_status"] == "approved"
        assert bucket[1]["new_status"] == "proposed"
    finally:
        conn.close()


def test_candidate_decision_event_can_capture_score_snapshot() -> None:
    conn = _conn()
    try:
        ensure_blueprint_decision_tables(conn)
        decision = upsert_candidate_decision(
            conn,
            sleeve_key="global_equity_core",
            candidate_symbol="VWRA",
            status="approved",
            note="Approved with score context.",
            actor_id="local_actor",
            score_snapshot={"score_version": "quality_v1_existing_data_only", "composite_score": 88.0},
            recommendation_snapshot={"our_pick_symbol": "VWRA"},
        )
        assert decision["status"] == "approved"
        events = list_candidate_decision_events(conn)[("global_equity_core", "VWRA")]
        assert "quality_v1_existing_data_only" in str(events[0]["score_snapshot_json"])
        assert "VWRA" in str(events[0]["recommendation_snapshot_json"])
    finally:
        conn.close()


def test_blueprint_payload_exposes_release3_release4_fields() -> None:
    payload = build_portfolio_blueprint_payload()
    assert "data_quality" in payload["blueprint_meta"]
    assert "citation_health" in payload["blueprint_meta"]
    assert "regime_transition_context" in payload["blueprint_meta"]
    assert "retention_counts" in payload["blueprint_meta"]["citation_health"]
    assert "hashed_documents_count" in payload["blueprint_meta"]["citation_health"]
    assert "exclusions" in payload["blueprint_meta"]["data_quality"]
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    assert candidate.get("decision_state")
    advanced_pack = dict(candidate.get("investment_lens", {}).get("advanced_pack") or {})
    assert "citation_health" in advanced_pack
    assert "data_quality" in advanced_pack
    assert "decision_state" in advanced_pack
    assert "decision_history" in advanced_pack
    assert "recommendation_summary" in payload["blueprint_meta"]
    assert "score_models" in payload["blueprint_meta"]
    assert candidate.get("investment_quality")

