from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import update


VISIBLE_DETAIL_FIELDS = [
    "summary",
    "trigger",
    "source_evidence",
    "reason",
    "portfolio_consequence",
    "next_action",
    "reversal_condition",
]

BANNED_VISIBLE_PHRASES = [
    "visible decision state changed",
    "sleeve question is whether",
    "investment question is whether",
    "recommendation authority",
    "bounded support",
    "timing context",
    "score band boundary",
    "score-band boundary",
    "old schema artifact",
    "candidate row",
    "quick brief",
    "use the candidate row first",
    "support_only",
    "support only",
    "usable",
    "strong review",
    "moved from review only to ordinary review",
    "near term market conditions improved, but that only strengthens the review case",
]


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TestClient:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_v2.sqlite3"))
    from app.v2.app import app

    with TestClient(app) as test_client:
        yield test_client


def _visible_detail_text(detail: dict[str, object]) -> str:
    return " ".join(str(detail.get(key) or "") for key in VISIBLE_DETAIL_FIELDS).lower()


def _assert_clean_visible_detail(detail: dict[str, object]) -> None:
    visible_text = _visible_detail_text(detail)
    assert not any(phrase in visible_text for phrase in BANNED_VISIBLE_PHRASES)
    row_values = [str(detail.get(key) or "") for key in VISIBLE_DETAIL_FIELDS[1:]]
    assert all(value.strip() for value in row_values)
    normalized = {" ".join(value.lower().split()) for value in row_values}
    assert len(normalized) == len(row_values)


def _assert_compact_audit_detail(detail: dict[str, object]) -> None:
    assert detail["render_mode"] == "grouped_audit"
    assert detail["materiality_class"] == "audit_only"
    assert detail["summary"].startswith("Historical review movement.")
    assert "source driver was not preserved" in str(detail["summary"])
    assert detail.get("trigger") is None
    assert detail.get("source_evidence") is None
    assert detail.get("reason") is None
    assert detail.get("portfolio_consequence") is None
    assert detail.get("next_action") is None
    assert detail.get("reversal_condition") is None
    assert detail.get("reversal_conditions") is None
    assert detail.get("missing_driver_reason")
    assert isinstance(detail.get("driver_packet"), dict)
    assert detail["driver_packet"]["preserved"] is False
    assert isinstance(detail.get("primary_trigger"), dict)
    assert detail["primary_trigger"]["preserved"] is False
    assert detail["primary_trigger"]["display_label"]
    assert isinstance(detail.get("candidate_impact"), dict)
    assert isinstance(detail.get("audit_detail"), dict)
    assert not any(phrase in _visible_detail_text(detail) for phrase in BANNED_VISIBLE_PHRASES)


def _record_changes() -> None:
    from app.v2.core.change_ledger import record_change

    record_change(
        event_type="truth_change",
        surface_id="blueprint_explorer",
        candidate_id="candidate_instrument_idev",
        sleeve_id="sleeve_global_equity_core",
        sleeve_name="Global Equity",
        previous_state="Review before deploy",
        current_state="Eligible now",
        change_trigger="Global Equity sleeve drifted below target while IDEV evidence sufficiency improved.",
        reason_summary="The sleeve is underweight and the evidence blocker cleared at the same time.",
        implication_summary="IDEV cleared its evidence blocker and moved to Eligible.",
        summary="IDEV evidence cleared.",
        portfolio_consequence="IDEV is now a cleaner first call for the next Global Equity deployment.",
        next_action="Review funding size and mandate fit before approving deployment.",
        what_would_reverse="A renewed evidence blocker or closed funding path would reverse this change.",
        impact_level="high",
    )
    record_change(
        event_type="boundary_change",
        surface_id="blueprint_explorer",
        candidate_id="candidate_instrument_vea",
        sleeve_id="sleeve_global_equity_core",
        sleeve_name="Global Equity",
        previous_state="Eligible but secondary",
        current_state="Blocked — FX constraint",
        implication_summary="FX blocker added.",
        summary="VEA downgraded because FX blocker was added.",
        impact_level="high",
    )
    record_change(
        event_type="freshness_risk",
        surface_id="blueprint_explorer",
        candidate_id="candidate_instrument_gld",
        sleeve_id="sleeve_real_assets",
        sleeve_name="Real Assets",
        previous_state="Fresh — 18d",
        current_state="Aging — 31d",
        implication_summary="Recommendation confidence is reduced due to stale evidence.",
        summary="GLD evidence crossed the 30-day freshness threshold.",
        impact_level="low",
    )
    record_change(
        event_type="truth_change",
        surface_id="candidate_report",
        candidate_id="candidate_instrument_idev",
        sleeve_id="sleeve_global_equity_core",
        sleeve_name="Global Equity",
        previous_state="Partial evidence",
        current_state="Evidence sufficient",
        implication_summary="Candidate report evidence was refreshed and is now sufficient.",
        summary="Candidate report refreshed for IDEV.",
        impact_level="high",
    )


def test_changes_builder_emits_summary_filters_and_pagination(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder.sqlite3"))
    _record_changes()

    from app.v2.surfaces.changes.contract_builder import build

    contract = build("blueprint_explorer", window="7d", limit=2)

    assert contract["surface_id"] == "blueprint_explorer"
    assert contract["window"] == "7d"
    assert contract["effective_since_utc"]
    assert contract["summary"]["total_changes"] == 3
    assert contract["summary"]["upgrades"] == 1
    assert contract["summary"]["downgrades"] >= 1
    assert contract["summary"]["blocker_changes"] == 1
    assert contract["pagination"]["limit"] == 2
    assert contract["pagination"]["returned"] == 2
    assert contract["pagination"]["total_matching"] == 3
    assert contract["pagination"]["has_more"] is True
    assert "Global Equity" in {row["sleeve_name"] for row in contract["available_sleeves"]}
    assert contract["feed_freshness_state"] in {"current", "stale", "empty", "degraded_runtime"}


def test_changes_builder_category_filters_use_backend_classification(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder_category.sqlite3"))
    _record_changes()

    from app.v2.surfaces.changes.contract_builder import build

    blockers = build("blueprint_explorer", window="7d", category="blocker_changes")
    assert blockers["change_events"]
    assert all(event["is_blocker_change"] for event in blockers["change_events"])

    upgrades = build("blueprint_explorer", window="7d", category="upgrades")
    assert upgrades["change_events"]
    assert all(event["direction"] == "upgrade" for event in upgrades["change_events"])

    freshness = build("blueprint_explorer", window="7d", category="freshness_risk")
    assert freshness["change_events"]
    assert all(event["ui_category"] == "freshness_risk" for event in freshness["change_events"])


def test_changes_builder_exposes_decision_read_fields(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder_decision_read.sqlite3"))
    _record_changes()

    from app.v2.surfaces.changes.contract_builder import build

    contract = build("blueprint_explorer", window="7d", category="source_evidence", limit=1)
    assert contract["change_events"]
    event = contract["change_events"][0]
    assert event["category"] == "source_evidence"
    assert event["occurred_at"]
    assert event["effective_at"]
    assert event["title"]
    assert event["why_it_matters"]
    assert event["actionability"] in {"review", "monitor", "no_action", "act_now"}
    assert event["scope"] in {"candidate", "sleeve", "portfolio", "system"}
    assert isinstance(event["evidence_refs"], list)


def test_changes_builder_embeds_inline_change_detail(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder_inline_detail.sqlite3"))
    _record_changes()

    from app.v2.surfaces.changes.contract_builder import build

    contract = build("blueprint_explorer", window="7d", category="upgrades", limit=1)
    assert contract["change_events"]
    detail = contract["change_events"][0]["change_detail"]

    assert detail["event_id"] == contract["change_events"][0]["event_id"]
    assert detail["summary"]
    assert not detail["summary"].startswith("IDEV moved from")
    assert detail["summary"].startswith("Global Equity sleeve drifted below target")
    assert "IDEV can be judged more on portfolio fit and implementation quality" in detail["summary"]
    assert detail["state_transition"] == {"from": "Review before deploy", "to": "Eligible now"}
    assert detail["trigger"] == "Global Equity sleeve drifted below target while IDEV evidence support improved."
    assert detail["source_evidence"]
    assert detail["source_evidence"] != detail["trigger"]
    assert detail["reason"] == "The sleeve is underweight and the evidence gap cleared at the same time."
    assert detail["portfolio_consequence"] == "IDEV is now a cleaner first call for the next Global Equity deployment."
    assert detail["next_action"] == "Review funding size and mandate fit before approving deployment."
    assert detail["reversal_condition"] == "A renewed evidence gap or closed funding path would reverse this change."
    assert detail["reversal_conditions"] == detail["reversal_condition"]
    assert detail["driver_label"] == "sleeve position + funding"
    assert detail["render_mode"] == "full_investor_explanation"
    assert detail["materiality_status"] == "material_source_backed"
    assert detail["materiality_class"] in {"investor_material", "review_material"}
    assert detail["driver_packet"]["preserved"] is True
    assert detail["primary_trigger"]["preserved"] is True
    assert detail["primary_trigger"]["trigger_type"] in {"portfolio", "source"}
    assert detail["primary_trigger"]["display_label"] == detail["trigger"]
    assert detail["candidate_impact"]["affected_candidate_id"] == "candidate_instrument_idev"
    assert detail["candidate_impact"]["impact_direction"] == "strengthened"
    assert detail["candidate_impact"]["why_it_matters"] == detail["reason"]
    assert detail["closure_status"] in {"open_actionable", "open_review"}
    assert contract["change_events"][0]["primary_trigger"]["display_label"] == detail["primary_trigger"]["display_label"]
    assert contract["change_events"][0]["candidate_impact"]["next_action"] == detail["next_action"]
    assert isinstance(detail["event_age_hours"], (int, float))
    _assert_clean_visible_detail(detail)
    assert detail["score_delta"] == {"from": None, "to": None}
    assert detail["affected_candidate"]["candidate_id"] == "candidate_instrument_idev"
    assert detail["affected_candidate"]["symbol"] == "IDEV"
    assert detail["affected_candidate"]["sleeve_id"] == "sleeve_global_equity_core"
    assert detail["source_freshness"]["state"] in {"unknown", "current", "stale", "last_good", "degraded_runtime"}
    assert detail["source_freshness"]["latest_event_at"]
    assert detail["links"]["candidate_recommendation_href"]
    assert detail["links"]["report_href"]


def test_timing_change_without_source_driver_renders_compact_audit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder_timing_language.sqlite3"))
    from app.v2.core.change_ledger import record_change
    from app.v2.surfaces.changes.contract_builder import build

    record_change(
        event_type="market_path_weakened",
        surface_id="blueprint_explorer",
        candidate_id="candidate_instrument_eimi",
        sleeve_id="sleeve_emerging_markets",
        sleeve_name="Emerging Markets",
        previous_state="support_only",
        current_state="cautious",
        implication_summary=(
            "The sleeve question is whether iShares Core MSCI EM IMI UCITS ETF is the cleanest way to add "
            "emerging-market growth exposure. Recommendation remains review-only while freshness or bounded "
            "support tighten. Right now the evidence base is clean."
        ),
        portfolio_consequence="Treat this as timing context, not recommendation authority on its own.",
        next_action="Treat this as timing context, not recommendation authority on its own.",
        what_would_reverse="A reversal in market-path support would reverse this change.",
        summary="EIMI market setup weakened in Emerging Markets.",
        impact_level="low",
        requires_review=False,
    )

    contract = build("blueprint_explorer", window="7d", category="timing", limit=1)
    assert contract["change_events"] == []
    assert contract["summary"]["total_changes"] == 0
    assert contract["summary"]["audit_only_count"] == 1
    assert contract["audit_groups"][0]["count"] == 1

    audit_contract = build("blueprint_explorer", window="7d", category="audit_only", limit=1)
    detail = audit_contract["change_events"][0]["change_detail"]

    _assert_compact_audit_detail(detail)
    assert detail["materiality_status"] == "unresolved_driver_missing"
    visible_text = _visible_detail_text(detail)
    assert "cautious" not in visible_text
    assert "timing support weakened" not in visible_text
    assert "do not fund EIMI today" not in visible_text
    assert "$" not in visible_text
    assert "%" not in visible_text


def test_raw_convex_protection_score_upgrade_is_compact_audit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder_convex_upgrade.sqlite3"))
    from app.v2.core.change_ledger import record_change
    from app.v2.surfaces.changes.contract_builder import build

    record_change(
        event_type="score_band_improved",
        surface_id="blueprint_explorer",
        candidate_id="candidate_instrument_kmlm",
        sleeve_id="sleeve_convex",
        sleeve_name="Convex Protection",
        previous_state="usable",
        current_state="strong",
        implication_summary=(
            "The sleeve question is whether KFA Mount Lucas Managed Futures Index Strategy ETF is the cleanest way "
            "to provide downside protection when risk breaks sharply."
        ),
        portfolio_consequence="This improves how convincingly KMLM can compete in Convex Protection.",
        next_action="Use the candidate row first, then open the quick brief if the reason for the score shift matters.",
        what_would_reverse="A move back across the same score band boundary would reverse this change.",
        summary="KMLM moved from usable to strong recommendation quality.",
        impact_level="medium",
        requires_review=False,
    )

    contract = build("blueprint_explorer", window="7d", category="upgrades", limit=1)
    assert contract["change_events"] == []
    assert contract["summary"]["total_changes"] == 0


def test_timing_upgrade_without_specific_source_driver_is_compact_audit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder_veve_timing.sqlite3"))
    from app.v2.core.change_ledger import record_change
    from app.v2.surfaces.changes.contract_builder import build

    record_change(
        event_type="market_path_strengthened",
        surface_id="blueprint_explorer",
        candidate_id="candidate_instrument_veve",
        sleeve_id="sleeve_developed_ex_us_optional_split",
        sleeve_name="Developed ex US Optional Split",
        previous_state="cautious",
        current_state="moderate",
        implication_summary="VEVE timing support improved, but deployment has not been confirmed.",
        portfolio_consequence="Treat this as timing context, not recommendation authority on its own.",
        next_action="Use the candidate row first, then open the quick brief if the reason matters.",
        what_would_reverse="A move back across the same score band boundary would reverse this change.",
        summary="VEVE moved from review only to ordinary review.",
        impact_level="low",
        requires_review=False,
    )

    contract = build("blueprint_explorer", window="7d", category="timing", limit=1)
    assert contract["change_events"] == []
    assert contract["summary"]["total_changes"] == 0
    assert contract["summary"]["audit_only_count"] == 1
    assert contract["audit_groups"][0]["count"] == 1

    audit_contract = build("blueprint_explorer", window="7d", category="audit_only", limit=1)
    detail = audit_contract["change_events"][0]["change_detail"]

    _assert_compact_audit_detail(detail)
    assert detail["materiality_status"] == "unresolved_driver_missing"
    assert "timing support improved" not in _visible_detail_text(detail)
    assert "candidate row" not in _visible_detail_text(detail)
    assert "quick brief" not in _visible_detail_text(detail)
    assert "score band boundary" not in _visible_detail_text(detail)


def test_timing_upgrade_with_specific_source_driver_renders_full_card(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder_veve_specific_timing.sqlite3"))
    from app.v2.core.change_ledger import record_change
    from app.v2.surfaces.changes.contract_builder import build

    record_change(
        event_type="market_path_strengthened",
        surface_id="blueprint_explorer",
        candidate_id="candidate_instrument_veve",
        sleeve_id="sleeve_developed_ex_us_optional_split",
        sleeve_name="Developed ex US Optional Split",
        previous_state="cautious",
        current_state="moderate",
        change_trigger="Developed market breadth improved in the latest market path refresh.",
        implication_summary="VEVE timing support improved, but deployment has not been confirmed.",
        portfolio_consequence="Treat this as timing context, not recommendation authority on its own.",
        next_action="Use the candidate row first, then open the quick brief if the reason matters.",
        what_would_reverse="A move back across the same score band boundary would reverse this change.",
        summary="VEVE moved from review only to ordinary review.",
        impact_level="low",
        requires_review=False,
    )

    contract = build("blueprint_explorer", window="7d", category="timing", limit=1)
    detail = contract["change_events"][0]["change_detail"]

    assert detail["render_mode"] == "full_investor_explanation"
    assert detail["materiality_status"] == "material_source_backed"
    assert detail["materiality_class"] in {"investor_material", "review_material"}
    assert detail["primary_trigger"]["preserved"] is True
    assert detail["primary_trigger"]["display_label"] == detail["trigger"]
    assert detail["candidate_impact"]["affected_dimension"] in {"timing", "market_path"}
    assert detail["summary"].startswith("Developed market breadth improved")
    assert not detail["summary"].startswith("VEVE moved")
    assert detail["source_evidence"] == "Source packet reports that developed market breadth improved in the latest market path refresh."
    assert "No allocation change yet" in detail["portfolio_consequence"]
    assert "Continue review and compare VEVE" in detail["next_action"]
    assert "score band" not in detail["reversal_condition"].lower()
    _assert_clean_visible_detail(detail)


def test_explicit_changes_routes_return_filtered_payloads(client: TestClient) -> None:
    _record_changes()

    three_day = client.get("/api/v2/surfaces/blueprint/explorer/changes?window=3d&limit=6")
    assert three_day.status_code == 200
    three_day_payload = three_day.json()
    assert three_day_payload["window"] == "3d"
    assert three_day_payload["effective_since_utc"]

    explorer = client.get("/api/v2/surfaces/blueprint/explorer/changes?window=7d&category=blocker_changes")
    assert explorer.status_code == 200
    explorer_payload = explorer.json()
    assert explorer_payload["surface_id"] == "blueprint_explorer"
    assert explorer_payload["filters_applied"]["category"] == "blocker_changes"
    assert all(event["is_blocker_change"] for event in explorer_payload["change_events"])

    candidate = client.get("/api/v2/surfaces/candidates/candidate_instrument_idev/changes?window=7d")
    assert candidate.status_code == 200
    candidate_payload = candidate.json()
    assert candidate_payload["surface_id"] == "candidate_report"
    assert candidate_payload["filters_applied"]["candidate_id"] == "candidate_instrument_idev"
    assert candidate_payload["change_events"]
    assert all(event["candidate_id"] == "candidate_instrument_idev" for event in candidate_payload["change_events"])


def test_rebuild_events_are_suppressed_from_visible_feed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder_rebuild.sqlite3"))
    from app.v2.core.change_ledger import record_change
    from app.v2.surfaces.changes.contract_builder import build

    record_change(
        event_type="rebuild",
        surface_id="blueprint_explorer",
        summary="Blueprint Explorer contract rebuilt.",
        impact_level="low",
    )
    record_change(
        event_type="truth_change",
        surface_id="blueprint_explorer",
        candidate_id="candidate_instrument_idev",
        summary="IDEV evidence cleared.",
        impact_level="high",
    )

    contract = build("blueprint_explorer", window="7d")
    assert contract["summary"]["total_changes"] == 1
    assert len(contract["change_events"]) == 1
    assert contract["change_events"][0]["event_type"] == "truth_change"


def _persist_no_material_blueprint_scan() -> None:
    from app.v2.surfaces.changes.blueprint_daily_scan import _persist_scan

    now = datetime.now(UTC)
    _persist_scan(
        {
            "trading_day": now.astimezone(ZoneInfo("Asia/Singapore")).date().isoformat(),
            "timezone": "Asia/Singapore",
            "started_at": now.isoformat(),
            "finished_at": (now + timedelta(seconds=1)).isoformat(),
            "status": "success",
            "source_freshness_state": "fresh",
            "emitted_event_count": 0,
            "material_candidate_count": 0,
            "no_material_change": True,
            "latest_scan_at": (now + timedelta(seconds=1)).isoformat(),
            "failure_reasons": [],
        }
    )


def test_no_material_today_suppresses_generic_current_movement(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder_no_material_today.sqlite3"))
    from app.v2.core.change_ledger import record_change
    from app.v2.surfaces.changes.contract_builder import build

    record_change(
        event_type="market_path_strengthened",
        surface_id="blueprint_explorer",
        candidate_id="candidate_instrument_veve",
        sleeve_id="sleeve_developed_ex_us_optional_split",
        sleeve_name="Developed ex US Optional Split",
        previous_state="review only",
        current_state="ordinary review",
        implication_summary="VEVE timing support improved, but deployment has not been confirmed.",
        summary="VEVE moved from review only to ordinary review.",
        impact_level="low",
        requires_review=False,
    )
    _persist_no_material_blueprint_scan()

    contract = build("blueprint_explorer", window="today", category="timing", limit=6)

    assert contract["daily_source_scan"]["no_material_change"] is True
    assert contract["summary"]["total_changes"] == 0
    assert contract["change_events"] == []


def test_historical_cards_are_marked_non_current(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "changes_builder_historical.sqlite3"))
    from app.v2.core.change_ledger import ChangeEventRecord, _engine, record_change
    from app.v2.surfaces.changes.contract_builder import build

    event_id = record_change(
        event_type="truth_change",
        surface_id="blueprint_explorer",
        candidate_id="candidate_instrument_idev",
        sleeve_id="sleeve_global_equity_core",
        sleeve_name="Global Equity",
        previous_state="Review before deploy",
        current_state="Eligible now",
        change_trigger="Global Equity sleeve drifted below target while IDEV evidence support improved.",
        implication_summary="IDEV cleared its evidence blocker and moved to Eligible.",
        summary="IDEV evidence cleared.",
        portfolio_consequence="IDEV is now a cleaner first call for the next Global Equity deployment.",
        impact_level="high",
    )
    with _engine().begin() as connection:
        connection.execute(
            update(ChangeEventRecord)
            .where(ChangeEventRecord.event_id == event_id)
            .values(changed_at_utc=datetime.now(UTC) - timedelta(days=2))
        )
    _persist_no_material_blueprint_scan()

    contract = build("blueprint_explorer", window="7d", category="upgrades", limit=1)
    assert contract["change_events"]
    detail = contract["change_events"][0]["change_detail"]

    assert detail["is_current"] is False
    assert detail["event_age_hours"] >= 24
    assert detail["closure_status"] == "stale_historical"
    assert detail["source_scan_status"] == "success"
