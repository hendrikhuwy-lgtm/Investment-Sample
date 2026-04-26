from __future__ import annotations

from app.models.db import connect
from app.services.cma_engine import build_expected_return_range_section, current_cma_version


def test_cma_range_generation(tmp_path) -> None:
    conn = connect(tmp_path / "cma.sqlite3")
    try:
        section = build_expected_return_range_section(conn)
        assert current_cma_version(conn) == "2026.03"
        assert section["assumption_date"] == "2026-03-01"
        assert section["items"]
        equity = next(item for item in section["items"] if item["sleeve_key"] == "global_equity")
        assert equity["expected_return_min"] < equity["expected_return_max"]
        assert section["caveat"]
    finally:
        conn.close()
