from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _table_names(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    finally:
        conn.close()
    return {str(row[0]) for row in rows}


def test_v2_app_bootstraps_runtime_tables(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "boot_v2.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))

    from app.v2.app import app

    with TestClient(app) as client:
        response = client.get("/api/v2/health")
        assert response.status_code == 200

    tables = _table_names(db_path)
    assert "provider_cache_snapshots" in tables
    assert "provider_family_success" in tables
    assert "public_upstream_snapshots" in tables
    assert "symbol_resolution_registry" in tables
    assert "blueprint_canonical_instruments" in tables
    assert "etf_factsheet_metrics" in tables
    assert "etf_holdings_summaries" in tables
    assert "etf_data_sources" in tables
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT extra_json FROM blueprint_canonical_instruments WHERE symbol = 'IWDP' LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert '"launch_date": "2007-11-20"' in str(row[0])


def test_v2_auth_challenges_when_enabled(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "auth_v2.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))
    monkeypatch.setenv("IA_AUTH_ENABLED", "1")
    monkeypatch.setenv("IA_AUTH_USERNAME", "investor")
    monkeypatch.setenv("IA_AUTH_PASSWORD", "correct-password")

    from app.v2.app import app

    with TestClient(app) as client:
        unauthenticated = client.get("/api/v2/health")
        assert unauthenticated.status_code == 401
        assert unauthenticated.headers["www-authenticate"] == 'Basic realm="Cortex"'

        authenticated = client.get("/api/v2/health", auth=("investor", "correct-password"))
        assert authenticated.status_code == 200


def test_v2_production_auth_fails_closed_without_credentials(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "auth_required_v2.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))
    monkeypatch.setenv("IA_ENV", "production")
    monkeypatch.setenv("IA_AUTH_ENABLED", "1")
    monkeypatch.delenv("IA_AUTH_PASSWORD", raising=False)
    monkeypatch.delenv("IA_AUTH_BEARER_TOKEN", raising=False)

    from app.v2.app import app

    with pytest.raises(RuntimeError, match="Authentication is enabled"):
        with TestClient(app):
            pass


def test_run_startup_warmup_queues_refresh_and_marks_complete(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "boot_warmup.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))

    import app.v2.runtime.service as runtime_service

    monkeypatch.setattr(runtime_service, "_STARTUP_WARMUP_COMPLETED", False)
    monkeypatch.setattr(runtime_service, "_is_test_mode", lambda: False)
    monkeypatch.setattr(
        runtime_service,
        "_queue_startup_refreshes",
        lambda force_refresh=False: [{"surface_name": "daily_brief", "status": "queued"}],
    )
    monkeypatch.setattr(
        runtime_service,
        "_warm_surface_snapshots",
        lambda: {
            "surface_results": [{"surface_id": "daily_brief", "status": "ok"}],
            "ok": True,
        },
    )
    monkeypatch.setattr(
        runtime_service,
        "_warm_benchmark_truth",
        lambda: {"status": "ok", "results": []},
    )
    monkeypatch.setattr(runtime_service, "_run_job", lambda spec, force_refresh=False: {"status": "ok"})

    payload = runtime_service.run_startup_warmup()

    assert payload["status"] == "ok"
    assert payload["refresh_results"] == [{"surface_name": "daily_brief", "status": "queued"}]
    assert runtime_service._STARTUP_WARMUP_COMPLETED is True


def test_runtime_status_payload_exposes_serving_mode_and_active_frontend(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_status.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))

    import app.v2.runtime.service as runtime_service

    monkeypatch.setattr(runtime_service, "_STARTUP_WARMUP_COMPLETED", True)
    monkeypatch.setattr(runtime_service, "_scheduler_enabled", lambda: False)
    monkeypatch.setattr(runtime_service, "runtime_jobs_payload", lambda: {"worker_alive": False, "jobs": []})
    monkeypatch.setattr(runtime_service, "_configured_providers_by_family", lambda: {"quote": ["polygon"]})
    monkeypatch.setattr(
        runtime_service,
        "build_surface_readiness_payload",
        lambda conn, settings: {
            "generated_at": "2026-04-10T00:00:00+00:00",
            "surfaces": [{"surface_name": "daily_brief", "state": "ready"}],
        },
    )

    payload = runtime_service.runtime_status_payload()

    assert payload["serving_mode"] == "warm_snapshots_plus_on_demand"
    assert payload["active_frontend"] == "frontend-cortex"
    assert payload["alert_count"] == 0


def test_blueprint_runtime_refresh_runs_candidate_enrichment(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_blueprint.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))

    import app.v2.runtime.service as runtime_service
    import app.v2.donors.providers as provider_donors
    import app.services.blueprint_refresh_monitor as refresh_monitor

    class StubDonor:
        def __init__(self, conn, settings):
            self.conn = conn
            self.settings = settings

        def refresh_blueprint(self, *, force_refresh: bool = False):
            return {"surface_name": "blueprint", "items": [], "sufficiency": {}}

    monkeypatch.setattr(provider_donors, "SQLiteProviderDonor", StubDonor)
    monkeypatch.setattr(
        refresh_monitor,
        "run_blueprint_candidate_refresh",
        lambda conn, *, settings, trigger_source, symbols=None: {
            "status": "succeeded",
            "success_count": 4,
            "failure_count": 0,
        },
    )

    payload = runtime_service._run_surface_refresh("blueprint", force_refresh=False)

    assert payload["surface_name"] == "blueprint"
    assert payload["candidate_refresh"] == {
        "status": "succeeded",
        "success_count": 4,
        "failure_count": 0,
    }


def test_runtime_job_specs_include_blueprint_market_lanes_when_enabled(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_market_specs.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))
    monkeypatch.setenv("IA_BLUEPRINT_MARKET_SCHEDULER_ENABLED", "1")
    monkeypatch.setenv("IA_BLUEPRINT_MARKET_PATH_ENABLED", "1")

    import app.v2.runtime.service as runtime_service

    specs = runtime_service._job_specs()
    job_ids = {spec.job_id for spec in specs}

    assert "blueprint_market_series_refresh" in job_ids
    assert "blueprint_market_forecast_refresh" in job_ids
    assert "blueprint_market_identity_audit" in job_ids


def test_runtime_blueprint_market_job_summarizes_counts(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_market_job.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))

    import app.v2.runtime.service as runtime_service

    spec = runtime_service.RuntimeJobSpec(
        job_id="blueprint_market_forecast_refresh",
        kind="blueprint_market_forecast_refresh",
        target="forecast_refresh",
        interval_seconds=86400,
        description="forecast refresh",
    )
    monkeypatch.setattr(
        runtime_service,
        "_run_blueprint_market_job",
        lambda kind: {
            "status": "ok",
            "scope": {"eligible_count": 6},
            "refreshed_count": 2,
            "skipped_count": 3,
            "suppressed_count": 1,
            "failure_count": 0,
        },
    )

    payload = runtime_service._run_job(spec)

    assert payload["last_status"] == "ok"
    assert payload["payload_summary"] == {
        "eligible_count": 6,
        "refreshed_count": 2,
        "skipped_count": 3,
        "suppressed_count": 1,
        "failure_count": 0,
        "stale_count": 0,
        "degraded_count": 0,
        "broken_count": 0,
        "served_last_good_count": 0,
    }
