from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from app.models.db import connect, init_db
from app.services.auth_service import authenticate_user, create_session, create_user, ensure_auth_tables, get_session_user, list_users, update_user
from app.services.exposure_aggregator import build_exposure_snapshot
from app.services.portfolio_delta import compute_latest_portfolio_delta
from app.services.review_items import ensure_review_tables, list_review_items, upsert_review_item
from app.services.scenario_registry import compare_scenarios, create_scenario, ensure_scenario_registry_tables, list_scenarios, update_scenario


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "app" / "storage" / "schema.sql"


def _conn(tmp_path: Path):
    conn = connect(tmp_path / "platform_quality.sqlite3")
    init_db(conn, SCHEMA_PATH)
    ensure_auth_tables(conn)
    ensure_review_tables(conn)
    ensure_scenario_registry_tables(conn)
    return conn


def test_auth_session_round_trip(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        primary = authenticate_user(conn, username="John", password="HU123")
        user = authenticate_user(conn, username="admin", password="admin")
        assert primary is not None
        assert user is not None
        raw_token, session = create_session(conn, user_id=user["user_id"], source_ip="127.0.0.1", user_agent="pytest")
        resolved = get_session_user(conn, raw_token)
        assert session["session_id"].startswith("session_")
        assert resolved is not None
        assert resolved["user"]["username"] == "admin"
        assert "admin" in resolved["user"]["roles"]
    finally:
        conn.close()


def test_scenario_registry_versions_and_comparison(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        created = create_scenario(
            conn,
            scenario_name="Test credit widening",
            source_rationale="Synthetic governance test",
            policy_notes="initial",
            shocks={"global_equity": -0.12, "ig_bond": -0.05},
            probability_weight=0.2,
            confidence_rating="medium",
            reviewed_by="admin",
        )
        updated = update_scenario(
            conn,
            scenario_id=str(created["scenario_id"]),
            patch={"probability_weight": 0.25, "confidence_rating": "high", "shocks": {"global_equity": -0.10, "ig_bond": -0.04}},
            actor="admin",
        )
        rows = [item for item in list_scenarios(conn) if str(item.get("scenario_id")) == str(created["scenario_id"])]
        comparisons = compare_scenarios(
            conn,
            current_weights={"global_equity": 0.6, "ig_bond": 0.3},
            prior_weights={"global_equity": 0.5, "ig_bond": 0.4},
        )
        assert updated["active_version"]["version_label"] == "1.1"
        assert rows
        assert comparisons["current_vs_prior_version"]
    finally:
        conn.close()


def test_account_scoped_exposures_delta_and_reviews(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        now = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO portfolio_upload_runs (
              run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
              raw_row_count, parsed_row_count, normalized_position_count, total_market_value, stale_price_count,
              mapping_issue_count, warning_count, warnings_json, errors_json
            ) VALUES (?, ?, ?, ?, ?, ?, 1, 0, 2, 2, 2, 300.0, 0, 0, 0, '[]', '[]')
            """,
            ("run_active", now, "2026-03-07", "test.csv", "test.csv", "succeeded"),
        )
        conn.execute(
            """
            INSERT INTO portfolio_upload_runs (
              run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
              raw_row_count, parsed_row_count, normalized_position_count, total_market_value, stale_price_count,
              mapping_issue_count, warning_count, warnings_json, errors_json
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 2, 2, 2, 250.0, 0, 0, 0, '[]', '[]')
            """,
            ("run_prior", now, "2026-03-06", "prior.csv", "prior.csv", "succeeded"),
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
                ("a1", "run_active", now, "2026-03-07", "2026-03-07", "ACC-1", "sec_1", "AAA", "AAA", "AAA", "etf", "USD", 10.0, 10.0, 12.0, 120.0, 1.0, "SGD", "global_equity", "auto_matched", "feed", 0, "XNYS", None),
                ("a2", "run_active", now, "2026-03-07", "2026-03-07", "ACC-2", "sec_2", "BBB", "BBB", "BBB", "etf", "USD", 10.0, 10.0, 18.0, 180.0, 1.0, "SGD", "ig_bond", "auto_matched", "feed", 0, "XNYS", None),
                ("p1", "run_prior", now, "2026-03-06", "2026-03-06", "ACC-1", "sec_1", "AAA", "AAA", "AAA", "etf", "USD", 8.0, 10.0, 11.0, 88.0, 1.0, "SGD", "global_equity", "auto_matched", "feed", 0, "XNYS", None),
                ("p2", "run_prior", now, "2026-03-06", "2026-03-06", "ACC-2", "sec_2", "BBB", "BBB", "BBB", "etf", "USD", 10.0, 10.0, 16.0, 160.0, 1.0, "SGD", "ig_bond", "auto_matched", "feed", 0, "XNYS", None),
            ],
        )
        exposure = build_exposure_snapshot(conn, run_id="run_active", account_id="ACC-1")
        delta = compute_latest_portfolio_delta(conn, latest_run_id="run_active", previous_run_id="run_prior", account_id="ACC-1")
        upsert_review_item(
            conn,
            review_key="review::acc1",
            category="mapping_issue",
            severity="medium",
            linked_object_type="portfolio",
            linked_object_id="sec_1",
            source_run_id="run_active",
            account_id="ACC-1",
            notes="Scoped to account 1",
        )
        assert exposure["summary"]["total_value"] == 120.0
        assert delta["summary"]["increased_count"] == 1
        assert len(list_review_items(conn, account_id="ACC-1")) == 1
    finally:
        conn.close()


def test_user_admin_create_and_update(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        created = create_user(
            conn,
            username="analyst",
            display_name="Analyst User",
            email="analyst@example.com",
            password="secret123",
            roles=["reviewer"],
        )
        updated = update_user(
            conn,
            user_id=str(created["user_id"]),
            status="inactive",
            roles=["read_only"],
        )
        users = list_users(conn)
        assert any(item["username"] == "analyst" for item in users)
        assert updated["status"] == "inactive"
        assert updated["roles"] == ["read_only"]
    finally:
        conn.close()
