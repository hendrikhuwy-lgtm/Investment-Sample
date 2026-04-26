from __future__ import annotations

from app.models.db import connect
from app.services.scenario_registry import compare_scenarios, list_scenario_comparison_history, record_scenario_comparisons


def test_scenario_comparison_snapshot_history(tmp_path) -> None:
    conn = connect(tmp_path / "scenario.sqlite3")
    try:
        comparisons = compare_scenarios(
            conn,
            current_weights={"global_equity": 0.5, "ig_bond": 0.2, "cash": 0.1, "real_asset": 0.1, "alt": 0.07, "convex": 0.03},
            prior_weights={"global_equity": 0.55, "ig_bond": 0.18, "cash": 0.1, "real_asset": 0.1, "alt": 0.04, "convex": 0.03},
        )
        snapshots = record_scenario_comparisons(
            conn,
            current_run_id="run_now",
            prior_run_id="run_prior",
            comparisons=comparisons,
        )
        history = list_scenario_comparison_history(conn, limit=20)
        assert snapshots
        assert history
        assert history[0]["current_run_id"] == "run_now"
    finally:
        conn.close()
