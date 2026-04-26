from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from app.config import Settings
from app.models.db import connect
from app.schedulers import daily_brief
from app.services.brief_approval import approve_brief, create_or_refresh_approval


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "daily_brief.sqlite3"),
        smtp_host="smtp.test.local",
        smtp_port=587,
        smtp_user="user",
        smtp_password="pass",
        alert_from="alerts@test.local",
        alert_to="dad@test.local",
        mcp_live_required=False,
        refresh_live_cache_on_brief=False,
    )


def _seed_brief_run(db_path: Path, run_id: str, *, approval_status: str = "generated") -> None:
    conn = connect(db_path)
    try:
        daily_brief.init_db(conn, daily_brief.SCHEMA_PATH)
        conn.execute(
            """
            INSERT INTO daily_brief_runs (
              brief_run_id, source_run_id, generated_at, status, brief_mode, audience_preset, delivery_state,
              approval_required, summary, diagnostics_json, content_version, policy_pack_version,
              benchmark_definition_version, cma_version, chart_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                run_id,
                "2026-02-20T02:00:00+00:00",
                "ok",
                "daily",
                "pm",
                "generated",
                1,
                "Seeded Daily Brief run",
                "{}",
                "2026.02.20",
                "2026.02.20",
                "2026.02",
                "2026.02",
                "2026.02.20",
            ),
        )
        create_or_refresh_approval(conn, brief_run_id=run_id, status=approval_status)
        conn.commit()
    finally:
        conn.close()


def test_daily_brief_sends_once_per_day(monkeypatch, tmp_path: Path) -> None:
    send_calls: list[tuple[str, str]] = []
    md_path = tmp_path / "brief.md"
    html_path = tmp_path / "brief.html"
    md_path.write_text("# md", encoding="utf-8")
    html_path.write_text("<html></html>", encoding="utf-8")

    def fake_generate(
        settings: Settings,
        force_cache_only: bool,
        conn=None,
        brief_mode: str | None = None,
        audience_preset: str | None = None,
    ) -> dict:
        return {
            "daily_log": {
                "log_id": "log_1",
                "created_at": datetime.now(UTC).isoformat(),
                "macro_state_summary": "Daily Brief",
                "short_term_alert_state": "Watch",
                "portfolio_snapshot_id": "snapshot_1",
                "regime_classification": "Watch",
                "top_risk_flags": [],
                "top_opportunity_flags": [],
                "personal_alignment_score": 75.0,
            },
            "macro_result": {
                "subject": "Daily Brief",
                "run_id": "run_1",
                "md_path": str(md_path),
                "html_path": str(html_path),
                "cached_used": True,
                "mcp_connected_count": 0,
                "mcp_total_count": 30,
                "citations_count": 12,
                "alert_count": 1,
            },
        }

    def fake_send(settings: Settings, subject: str, markdown_body: str, html_body: str) -> None:
        send_calls.append((subject, settings.alert_to))

    monkeypatch.setattr(daily_brief, "generate_daily_log", fake_generate)
    monkeypatch.setattr(daily_brief, "send_narrated_brief_email", fake_send)

    settings = _settings(tmp_path)
    now_utc = datetime(2026, 2, 17, 2, 0, tzinfo=UTC)
    first = daily_brief.run_daily_brief_once(settings=settings, now_utc=now_utc)
    second = daily_brief.run_daily_brief_once(settings=settings, now_utc=now_utc)

    assert first["status"] == "sent"
    assert second["status"] == "generated_no_send"
    assert second["reason"] == "already_sent_today"
    assert len(send_calls) == 1

    db_path = settings.resolved_db_path(daily_brief.PROJECT_ROOT)
    conn = connect(db_path)
    try:
        statuses = [row[0] for row in conn.execute("SELECT status FROM email_runs ORDER BY id ASC").fetchall()]
    finally:
        conn.close()
    assert statuses == ["sent", "generated_no_send"]


def test_daily_brief_no_email_mode_records_generation(monkeypatch, tmp_path: Path) -> None:
    md_path = tmp_path / "brief.md"
    html_path = tmp_path / "brief.html"
    md_path.write_text("# md", encoding="utf-8")
    html_path.write_text("<html></html>", encoding="utf-8")

    def fake_generate(
        settings: Settings,
        force_cache_only: bool,
        conn=None,
        brief_mode: str | None = None,
        audience_preset: str | None = None,
    ) -> dict:
        return {
            "daily_log": {
                "log_id": "log_2",
                "created_at": datetime.now(UTC).isoformat(),
                "macro_state_summary": "Daily Brief",
                "short_term_alert_state": "Normal",
                "portfolio_snapshot_id": "snapshot_2",
                "regime_classification": "Normal",
                "top_risk_flags": [],
                "top_opportunity_flags": [],
                "personal_alignment_score": 80.0,
            },
            "macro_result": {
                "subject": "Daily Brief",
                "run_id": "run_2",
                "md_path": str(md_path),
                "html_path": str(html_path),
                "cached_used": False,
                "mcp_connected_count": 3,
                "mcp_total_count": 30,
                "citations_count": 20,
                "alert_count": 0,
            },
        }

    monkeypatch.setattr(daily_brief, "generate_daily_log", fake_generate)
    monkeypatch.setattr(
        daily_brief,
        "send_narrated_brief_email",
        lambda settings, subject, markdown_body, html_body: (_ for _ in ()).throw(
            AssertionError("send_narrated_brief_email should not be called in no-email mode")
        ),
    )

    settings = _settings(tmp_path)
    now_utc = datetime(2026, 2, 18, 2, 0, tzinfo=UTC)
    result = daily_brief.run_daily_brief_once(
        settings=settings,
        now_utc=now_utc,
        send_email=False,
    )
    assert result["status"] == "generated_no_send"


def test_daily_brief_blocks_send_when_approval_required(monkeypatch, tmp_path: Path) -> None:
    send_calls: list[tuple[str, str]] = []
    md_path = tmp_path / "brief.md"
    html_path = tmp_path / "brief.html"
    md_path.write_text("# md", encoding="utf-8")
    html_path.write_text("<html></html>", encoding="utf-8")

    def fake_generate(
        settings: Settings,
        force_cache_only: bool,
        conn=None,
        brief_mode: str | None = None,
        audience_preset: str | None = None,
    ) -> dict:
        return {
            "daily_log": {
                "log_id": "log_3",
                "created_at": datetime.now(UTC).isoformat(),
                "macro_state_summary": "Daily Brief",
                "short_term_alert_state": "Watch",
                "portfolio_snapshot_id": "snapshot_3",
                "regime_classification": "Watch",
                "top_risk_flags": [],
                "top_opportunity_flags": [],
                "personal_alignment_score": 82.0,
            },
            "macro_result": {
                "subject": "Daily Brief",
                "run_id": "run_need_approval",
                "md_path": str(md_path),
                "html_path": str(html_path),
                "cached_used": False,
                "mcp_connected_count": 2,
                "mcp_total_count": 30,
                "citations_count": 10,
                "alert_count": 1,
            },
        }

    def fake_send(settings: Settings, subject: str, markdown_body: str, html_body: str) -> None:
        send_calls.append((subject, settings.alert_to))

    monkeypatch.setattr(daily_brief, "generate_daily_log", fake_generate)
    monkeypatch.setattr(daily_brief, "send_narrated_brief_email", fake_send)

    settings = replace(_settings(tmp_path), daily_brief_require_approval_before_send=True)
    result = daily_brief.run_daily_brief_once(
        settings=settings,
        now_utc=datetime(2026, 2, 19, 2, 0, tzinfo=UTC),
    )

    assert result["status"] == "generated_no_send"
    assert result["reason"] == "approval_required_absent"
    assert send_calls == []


def test_daily_brief_sends_after_approval_is_recorded(monkeypatch, tmp_path: Path) -> None:
    send_calls: list[tuple[str, str]] = []
    md_path = tmp_path / "brief.md"
    html_path = tmp_path / "brief.html"
    md_path.write_text("# md", encoding="utf-8")
    html_path.write_text("<html><body>brief</body></html>", encoding="utf-8")

    def fake_generate(
        settings: Settings,
        force_cache_only: bool,
        conn=None,
        brief_mode: str | None = None,
        audience_preset: str | None = None,
    ) -> dict:
        return {
            "daily_log": {
                "log_id": "log_4",
                "created_at": datetime.now(UTC).isoformat(),
                "macro_state_summary": "Daily Brief",
                "short_term_alert_state": "Alert",
                "portfolio_snapshot_id": "snapshot_4",
                "regime_classification": "Alert",
                "top_risk_flags": [],
                "top_opportunity_flags": [],
                "personal_alignment_score": 85.0,
            },
            "macro_result": {
                "subject": "Daily Brief",
                "run_id": "run_approval_flow",
                "md_path": str(md_path),
                "html_path": str(html_path),
                "cached_used": False,
                "mcp_connected_count": 4,
                "mcp_total_count": 30,
                "citations_count": 18,
                "alert_count": 2,
            },
        }

    def fake_send(settings: Settings, subject: str, markdown_body: str, html_body: str) -> None:
        send_calls.append((subject, settings.alert_to))

    settings = replace(_settings(tmp_path), daily_brief_require_approval_before_send=True)
    _seed_brief_run(Path(settings.db_path), "run_approval_flow")

    monkeypatch.setattr(daily_brief, "generate_daily_log", fake_generate)
    monkeypatch.setattr(daily_brief, "send_narrated_brief_email", fake_send)

    first = daily_brief.run_daily_brief_once(
        settings=settings,
        now_utc=datetime(2026, 2, 20, 2, 0, tzinfo=UTC),
    )
    assert first["status"] == "generated_no_send"
    assert first["reason"] == "approval_required_absent"

    conn = connect(Path(settings.db_path))
    try:
        approve_brief(conn, "run_approval_flow", "reviewer_a", "Approved after workflow review.")
    finally:
        conn.close()

    second = daily_brief.run_daily_brief_once(
        settings=settings,
        now_utc=datetime(2026, 2, 20, 2, 30, tzinfo=UTC),
    )
    assert second["status"] == "sent"
    assert send_calls == [("Daily Brief", settings.alert_to)]

    conn = connect(Path(settings.db_path))
    try:
        run_row = conn.execute(
            "SELECT delivery_state FROM daily_brief_runs WHERE brief_run_id = ?",
            ("run_approval_flow",),
        ).fetchone()
        approval_row = conn.execute(
            "SELECT approval_status FROM daily_brief_approvals WHERE brief_run_id = ?",
            ("run_approval_flow",),
        ).fetchone()
    finally:
        conn.close()

    assert run_row is not None
    assert approval_row is not None
    assert run_row[0] == "sent"
    assert approval_row[0] == "sent"
