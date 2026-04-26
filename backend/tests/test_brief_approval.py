from __future__ import annotations

from app.models.db import connect
from app.services.brief_approval import approve_brief, create_or_refresh_approval, reject_brief
from app.services.brief_delivery_state import latest_ack_state, record_ack_event


def test_approval_state_transitions_and_ack(tmp_path) -> None:
    conn = connect(tmp_path / "approval.sqlite3")
    try:
        generated = create_or_refresh_approval(conn, brief_run_id="run_approval")
        assert generated["approval_status"] == "generated"
        approved = approve_brief(conn, "run_approval", "reviewer_a")
        assert approved["approval_status"] == "approved"
        rejected = reject_brief(conn, "run_approval", "reviewer_b", "Need revisions")
        assert rejected["approval_status"] == "rejected"
        record_ack_event(
            conn,
            brief_run_id="run_approval",
            recipient="user@test.local",
            ack_state="acknowledged",
            actor="reviewer_b",
            details={"source": "test"},
        )
        ack = latest_ack_state(conn, "run_approval", "user@test.local")
        assert ack is not None
        assert ack["ack_state"] == "acknowledged"
    finally:
        conn.close()
