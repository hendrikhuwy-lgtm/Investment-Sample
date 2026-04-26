from __future__ import annotations

from app.models.db import connect
from app.services.brief_dca import build_dca_guidance, get_current_dca_policy


def test_dca_routing_logic(tmp_path) -> None:
    conn = connect(tmp_path / "dca.sqlite3")
    try:
        policy = get_current_dca_policy(conn)
        guidance = build_dca_guidance(policy)
        assert guidance["routing_mode"] == "drift_correcting"
        assert guidance["neutral_conditions"]
        assert guidance["drift_conditions"]
        assert guidance["stress_conditions"]
    finally:
        conn.close()
