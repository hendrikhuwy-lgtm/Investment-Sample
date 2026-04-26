from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from app.models.db import connect, init_db
from app.services.account_service import list_accounts
from app.services.limit_engine import evaluate_limit_breaches, list_latest_limit_breaches, sync_limit_reviews
from app.services.review_items import list_review_items, upsert_review_item
from app.services.review_workflow import (
    apply_escalation_rules,
    assign_review_item,
    ensure_review_workflow_tables,
    get_review_item_detail,
    list_review_events,
    resolve_review_item_structured,
)
from app.services.staleness_policy import classify_staleness_impact, list_availability_history, record_availability_snapshot


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "app" / "storage" / "schema.sql"


def _conn(tmp_path: Path):
    conn = connect(tmp_path / "governance.sqlite3")
    init_db(conn, SCHEMA_PATH)
    ensure_review_workflow_tables(conn)
    return conn


def test_limit_breaches_create_and_then_resolve_reviews(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        exposures = {
            "summary": {
                "total_value": 100.0,
                "top_5_concentration": 0.60,
            },
            "top_positions": [
                {"security_key": "sec_1", "normalized_symbol": "CSPX", "security_name": "CSPX", "weight": 0.20},
            ],
            "top_issuers": [
                {"issuer_key": "issuer_1", "issuer_label": "Issuer 1", "weight": 0.22},
            ],
            "sleeve_concentration": [
                {"sleeve": "convex", "weight": 0.12},
                {"sleeve": "cash", "weight": 0.01},
            ],
            "stale_priced_weight": {"weight": 0.11},
            "unmapped_weight": {"weight": 0.03},
        }
        blueprint_compare = {
            "comparison_rows": [
                {
                    "sleeve_key": "global_equity",
                    "sleeve_name": "Global Equity",
                    "deviation": 0.08,
                    "current_weight": 0.58,
                    "band_min": 0.45,
                    "band_max": 0.55,
                    "rebalance_candidate": True,
                    "breach_severity": "high",
                }
            ]
        }

        breaches = evaluate_limit_breaches(
            conn,
            run_id="run_active",
            snapshot_id="snap_1",
            blueprint_id="bp_1",
            exposures=exposures,
            blueprint_compare=blueprint_compare,
        )
        sync_limit_reviews(conn, breaches=breaches)
        open_reviews = [item for item in list_review_items(conn) if item["category"] == "limit_breach"]

        assert breaches["summary"]["breach_count"] >= 4
        assert any(item["label"] == "Stale priced weight" and item["severity"] == "critical" for item in breaches["items"])
        assert open_reviews
        assert all(item["status"] == "open" for item in open_reviews)

        cleared = evaluate_limit_breaches(
            conn,
            run_id="run_active",
            snapshot_id="snap_2",
            blueprint_id="bp_1",
            exposures={
                "summary": {"total_value": 100.0, "top_5_concentration": 0.30},
                "top_positions": [{"security_key": "sec_1", "normalized_symbol": "CSPX", "security_name": "CSPX", "weight": 0.10}],
                "top_issuers": [{"issuer_key": "issuer_1", "issuer_label": "Issuer 1", "weight": 0.10}],
                "sleeve_concentration": [{"sleeve": "convex", "weight": 0.05}, {"sleeve": "cash", "weight": 0.05}],
                "stale_priced_weight": {"weight": 0.0},
                "unmapped_weight": {"weight": 0.0},
            },
            blueprint_compare={"comparison_rows": []},
        )
        sync_limit_reviews(conn, breaches=cleared)
        latest = list_latest_limit_breaches(conn, run_id="run_active")
        limit_reviews = [item for item in list_review_items(conn) if item["category"] == "limit_breach"]

        assert latest["items"] == []
        assert limit_reviews
        assert all(item["status"] == "resolved" for item in limit_reviews)
    finally:
        conn.close()


def test_staleness_policy_and_availability_history_track_transitions(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        impact = classify_staleness_impact(
            {"summary": {"stale_priced_weight": 0.07, "stale_priced_value": 7000.0}},
            {"price_as_of_date": "2026-03-05"},
        )
        assert impact["nav_confidence_flag"] == "degraded"
        assert impact["pricing_mode"] == "mixed_or_stale"

        record_availability_snapshot(
            conn,
            availability={
                "portfolio": "degraded_stale_prices",
                "blueprint": "healthy",
                "daily_brief": "healthy",
                "issues": ["Stale price weight above warning threshold."],
            },
            run_id="run_1",
        )
        record_availability_snapshot(
            conn,
            availability={
                "portfolio": "healthy",
                "blueprint": "healthy",
                "daily_brief": "healthy",
                "issues": [],
            },
            run_id="run_2",
        )
        history = list_availability_history(conn, limit=10)
        portfolio_rows = [item for item in history if item["service_domain"] == "portfolio"]

        assert portfolio_rows
        assert portfolio_rows[0]["status"] == "healthy"
        assert any(item["status"] == "degraded_stale_prices" and item["exited_at"] is not None for item in portfolio_rows)
    finally:
        conn.close()


def test_review_workflow_captures_assignment_resolution_and_escalation(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        upsert_review_item(
            conn,
            review_key="review::workflow",
            category="stale_price",
            severity="high",
            linked_object_type="limit_breach",
            linked_object_id="breach_1",
            source_run_id="run_1",
            notes="Stale prices exceed threshold.",
            due_in_days=0,
        )
        review = next(item for item in list_review_items(conn) if item["review_key"] == "review::workflow")

        assigned = assign_review_item(
            conn,
            review_id=review["review_id"],
            assignee_name="PM",
            assignee_role="Portfolio Manager",
            actor="system",
        )
        resolved = resolve_review_item_structured(
            conn,
            review_id=review["review_id"],
            resolution_type="accepted_risk",
            resolution_summary="Validated as temporary source lag.",
            resolution_notes="Monitor next close.",
            actor="PM",
        )
        detail = get_review_item_detail(conn, review["review_id"])

        assert assigned["status"] == "assigned"
        assert resolved["status"] == "resolved"
        assert detail is not None
        assert detail["resolved_by"] == "PM"
        assert len(list_review_events(conn, review["review_id"])) >= 2

        upsert_review_item(
            conn,
            review_key="review::escalate",
            category="critical",
            severity="critical",
            linked_object_type="limit_breach",
            linked_object_id="breach_2",
            source_run_id="run_2",
            notes="Critical breach remains open.",
            due_in_days=0,
        )
        overdue = next(item for item in list_review_items(conn) if item["review_key"] == "review::escalate")
        conn.execute(
            "UPDATE review_items SET due_date = ? WHERE review_id = ?",
            ((datetime.now(UTC) - timedelta(days=1)).date().isoformat(), overdue["review_id"]),
        )
        conn.commit()

        escalated = apply_escalation_rules(conn)
        escalated_detail = get_review_item_detail(conn, overdue["review_id"])

        assert escalated
        assert escalated_detail is not None
        assert escalated_detail["status"] == "escalated"
        assert any(event["new_status"] == "escalated" for event in escalated_detail["events"])
    finally:
        conn.close()


def test_account_summary_only_uses_active_upload(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        now = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO portfolio_upload_runs (
              run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
              raw_row_count, parsed_row_count, normalized_position_count, total_market_value, stale_price_count,
              mapping_issue_count, warning_count, warnings_json, errors_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, 1, 1, ?, 0, 0, 0, '[]', '[]')
            """,
            ("run_old", now, "2026-03-01", "old.csv", "old.csv", "succeeded", 0, 50.0),
        )
        conn.execute(
            """
            INSERT INTO portfolio_upload_runs (
              run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
              raw_row_count, parsed_row_count, normalized_position_count, total_market_value, stale_price_count,
              mapping_issue_count, warning_count, warnings_json, errors_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, 1, 2, ?, 0, 0, 0, '[]', '[]')
            """,
            ("run_active", now, "2026-03-06", "active.csv", "active.csv", "succeeded", 1, 300.0),
        )
        conn.executemany(
            """
            INSERT INTO portfolio_holding_snapshots (
              snapshot_row_id, run_id, uploaded_at, holdings_as_of_date, price_as_of_date, account_id, security_key,
              raw_symbol, normalized_symbol, security_name, asset_type, currency, quantity, cost_basis, market_price,
              market_value, fx_rate_to_base, base_currency, sleeve, mapping_status, price_source, price_stale, venue, identifier_isin
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "row_old_1", "run_old", now, "2026-03-01", "2026-03-01", "ACC-1", "sec_old",
                    "OLD", "OLD", "Old Holding", "etf", "USD", 1.0, 10.0, 10.0,
                    50.0, 1.35, "SGD", "global_equity", "auto_matched", "feed", 0, "XNYS", None,
                ),
                (
                    "row_active_1", "run_active", now, "2026-03-06", "2026-03-06", "ACC-1", "sec_new_1",
                    "AAA", "AAA", "Active Holding A", "etf", "USD", 1.0, 10.0, 10.0,
                    100.0, 1.35, "SGD", "global_equity", "auto_matched", "feed", 0, "XNYS", None,
                ),
                (
                    "row_active_2", "run_active", now, "2026-03-06", "2026-03-06", "ACC-2", "sec_new_2",
                    "BBB", "BBB", "Active Holding B", "etf", "USD", 1.0, 10.0, 10.0,
                    200.0, 1.35, "SGD", "ig_bond", "auto_matched", "feed", 0, "XNYS", None,
                ),
            ],
        )
        conn.commit()

        accounts = list_accounts(conn)
        acc1 = next(item for item in accounts if item["account_id"] == "ACC-1")
        acc2 = next(item for item in accounts if item["account_id"] == "ACC-2")

        assert len(accounts) == 2
        assert acc1["total_value"] == 100.0
        assert acc1["position_count"] == 1
        assert acc2["total_value"] == 200.0

        conn.execute("UPDATE portfolio_upload_runs SET is_active = 0")
        conn.commit()

        assert list_accounts(conn) == []
    finally:
        conn.close()
