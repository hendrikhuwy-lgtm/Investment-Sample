from __future__ import annotations

from app.models.db import connect
from app.services.brief_benchmark import build_comparison_context, get_current_benchmark
from app.services.cma_engine import build_expected_return_range_section


def test_benchmark_composition_math(tmp_path) -> None:
    conn = connect(tmp_path / "benchmark.sqlite3")
    try:
        benchmark = get_current_benchmark(conn)
        cma = build_expected_return_range_section(conn)
        comparison = build_comparison_context(benchmark, cma)
        total_weight = sum(float(item["weight"]) for item in comparison["components"])
        assert round(total_weight, 6) == 1.0
        assert comparison["expected_return_min"] <= comparison["expected_return_max"]
        assert comparison["benchmark_definition_id"]
    finally:
        conn.close()
