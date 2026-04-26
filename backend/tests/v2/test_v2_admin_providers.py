from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TestClient:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "admin_v2.sqlite3"))
    from app.v2.app import app

    with TestClient(app) as test_client:
        yield test_client


def test_capability_matrix_exposes_active_universe(client: TestClient) -> None:
    response = client.get("/api/v2/admin/providers/capability-matrix")
    assert response.status_code == 200
    payload = response.json()
    assert "capability_matrix" in payload
    assert "provider_status_registry" in payload
    assert "active_candidate_universe" in payload
    assert payload["active_candidate_universe"]["active_symbol_count"] >= 1


def test_provider_readiness_surface_shape(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.provider_refresh as provider_refresh

    monkeypatch.setattr(
        provider_refresh,
        "build_cached_external_upstream_payload",
        lambda conn, settings, surface_name=None: {
            "providers": [],
            "active_targets": {"quote_latest": ["SPY"]},
            "summary": {"provider_count": 0},
            "family_success": [],
            "activation_report": {"summary": {}},
        },
    )
    response = client.get("/api/v2/admin/providers/readiness?surface=blueprint")
    assert response.status_code == 200
    payload = response.json()
    assert payload["surface"] == "blueprint"
    assert payload["active_targets"]["quote_latest"] == ["SPY"]
    assert "active_candidate_universe" in payload


def test_provider_cache_status_shape(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.provider_refresh as provider_refresh

    monkeypatch.setattr(
        provider_refresh,
        "build_cached_external_upstream_payload",
        lambda conn, settings, surface_name=None: {
            "active_targets": {"benchmark_proxy": ["SPY", "TLT"]},
            "summary": {"provider_count": 0},
        },
    )
    response = client.get("/api/v2/admin/providers/cache-status?surface=daily-brief")
    assert response.status_code == 200
    payload = response.json()
    assert payload["surface"] == "daily_brief"
    assert payload["active_targets"]["benchmark_proxy"] == ["SPY", "TLT"]
    assert "snapshot_versions" in payload
    assert "family_success" in payload


def test_provider_refresh_routes_call_v2_donor(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.donors.providers as provider_donors

    monkeypatch.setattr(
        provider_donors.SQLiteProviderDonor,
        "refresh_blueprint",
        lambda self, force_refresh=False: {
            "surface_name": "blueprint",
            "refreshed_at": "2026-04-03T00:00:00+00:00",
            "items": [],
        },
    )
    response = client.post("/api/v2/admin/providers/refresh/blueprint?force_refresh=true")
    assert response.status_code == 200
    payload = response.json()
    assert payload["surface_name"] == "blueprint"
    assert "active_candidate_universe" in payload
