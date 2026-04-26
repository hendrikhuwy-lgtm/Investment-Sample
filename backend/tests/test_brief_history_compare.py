from __future__ import annotations

from app.models.db import connect
from app.services.brief_history_compare import build_history_compare, record_regime_history


def test_scenario_history_comparison(tmp_path) -> None:
    conn = connect(tmp_path / "history.sqlite3")
    try:
        record_regime_history(
            conn,
            brief_run_id="run_1",
            as_of_ts="2026-03-06T00:00:00+00:00",
            long_state="normal",
            short_state="watch",
            change_summary="baseline",
        )
        record_regime_history(
            conn,
            brief_run_id="run_2",
            as_of_ts="2026-03-07T00:00:00+00:00",
            long_state="watch",
            short_state="alert",
            change_summary="stress broadening",
        )
        comparison = build_history_compare(conn, current_run_id="run_2")
        assert comparison["current"]["brief_run_id"] == "run_2"
        assert comparison["prior"]["brief_run_id"] == "run_1"
        assert comparison["changes"]
    finally:
        conn.close()
