from __future__ import annotations

from app.models.db import connect
from app.services.brief_benchmark import get_current_benchmark
from app.services.ips_generator import build_ips_snapshot, latest_ips_snapshot, persist_ips_snapshot


def test_ips_generation_and_persistence(tmp_path) -> None:
    conn = connect(tmp_path / "ips.sqlite3")
    try:
        benchmark = get_current_benchmark(conn)
        snapshot = build_ips_snapshot(conn, benchmark=benchmark, rebalancing_policy={"calendar_review_cadence": "monthly"}, cma_version="2026.03")
        persisted = persist_ips_snapshot(
            conn,
            brief_run_id="run_test",
            snapshot=snapshot,
            benchmark_definition_id=benchmark["benchmark_definition_id"],
            cma_version="2026.03",
        )
        latest = latest_ips_snapshot(conn, "run_test")
        assert persisted["ips_snapshot_id"] == latest["ips_snapshot_id"]
        assert latest["payload"]["benchmark"]["benchmark_name"] == benchmark["benchmark_name"]
        assert latest["payload"]["rebalancing_rules"]["calendar_review_cadence"] == "monthly"
    finally:
        conn.close()
