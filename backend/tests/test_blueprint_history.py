from __future__ import annotations

import sqlite3

from app.services.blueprint_store import create_blueprint_snapshot, diff_blueprint_snapshots, list_blueprint_snapshots
from app.services.portfolio_blueprint import build_portfolio_blueprint_payload


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_blueprint_snapshot_history_and_diff_capture_candidate_and_policy_changes() -> None:
    conn = _conn()
    try:
        payload_a = build_portfolio_blueprint_payload()
        snapshot_a = create_blueprint_snapshot(conn, blueprint_payload=payload_a, note="baseline")

        payload_b = build_portfolio_blueprint_payload()
        payload_b["blueprint_meta"]["version"] = "2026-03-03"
        first_sleeve = payload_b["sleeves"][0]
        first_sleeve["policy_weight_range"]["target"] = float(first_sleeve["policy_weight_range"]["target"]) + 1.0
        first_candidate = first_sleeve["candidates"][0]
        first_candidate["verification_status"] = "partially_verified"
        second_candidate = dict(first_candidate)
        second_candidate["symbol"] = "ZZZZ"
        second_candidate["name"] = "Temporary Compare Candidate"
        first_sleeve["candidates"] = [*first_sleeve["candidates"], second_candidate]
        snapshot_b = create_blueprint_snapshot(conn, blueprint_payload=payload_b, note="policy adjusted")

        history = list_blueprint_snapshots(conn)
        assert len(history) == 2

        diff = diff_blueprint_snapshots(conn, snapshot_a=snapshot_a["snapshot_id"], snapshot_b=snapshot_b["snapshot_id"])
        assert "global_equity_core::ZZZZ" in diff["diff"]["added_candidates"]
        assert diff["diff"]["weight_range_changes"]
        assert any("version:" in item for item in diff["diff"]["policy_changes"])
        assert diff["diff"]["verification_status_changes"]
    finally:
        conn.close()
