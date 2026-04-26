from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TestClient:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "wave3.sqlite3"))
    from app.v2.app import app

    with TestClient(app) as test_client:
        yield test_client


def test_governance_and_runtime_routes(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.provider_refresh as provider_refresh
    import app.v2.donors.providers as provider_donors

    monkeypatch.setattr(
        provider_refresh,
        "build_cached_external_upstream_payload",
        lambda conn, settings, surface_name=None: {
            "providers": [],
            "active_targets": {"quote_latest": ["SPY"]},
            "summary": {
                "provider_count": 0,
                "surface_issue_count": 1 if surface_name == "blueprint" else 0,
                "issues": ["candidate_truth:coverage_gap"] if surface_name == "blueprint" else [],
                "governance": {
                    "status": "review_ready" if surface_name != "blueprint" else "blocked_by_missing_critical_data",
                    "current_count": 2,
                    "stale_count": 0,
                    "gap_count": 1 if surface_name == "blueprint" else 0,
                },
            },
            "family_success": [],
            "activation_report": {"summary": {}, "source_family_coverage": {}},
        },
    )
    monkeypatch.setattr(
        provider_donors.SQLiteProviderDonor,
        "refresh_blueprint",
        lambda self, force_refresh=False: {"surface_name": "blueprint", "items": [], "sufficiency": {}},
    )

    authority = client.get("/api/v2/admin/governance/source-authority")
    assert authority.status_code == 200
    authority_payload = authority.json()
    assert authority_payload["summary"]["family_count"] >= 1
    assert any(item["family_name"] == "latest_quote" for item in authority_payload["families"])

    family = client.get("/api/v2/admin/governance/family/latest_quote")
    assert family.status_code == 200
    assert family.json()["family_name"] == "latest_quote"

    readiness = client.get("/api/v2/admin/governance/surface-readiness")
    assert readiness.status_code == 200
    readiness_payload = readiness.json()
    assert any(item["surface_name"] == "blueprint" and item["state"] == "blocked" for item in readiness_payload["surfaces"])

    jobs = client.get("/api/v2/admin/runtime/jobs")
    assert jobs.status_code == 200
    job_payload = jobs.json()
    assert any(item["job_id"] == "refresh_blueprint" for item in job_payload["jobs"])

    refresh = client.post("/api/v2/admin/runtime/refresh/blueprint")
    assert refresh.status_code == 200
    refresh_payload = refresh.json()
    assert refresh_payload["job_id"] == "refresh_blueprint"
    assert refresh_payload["last_status"] == "ok"


def test_portfolio_upload_status_and_surface_contract(client: TestClient) -> None:
    csv_text = "symbol,name,quantity,cost_basis,currency,account_id\nSPY,SPY ETF,10,500,USD,broker\n"
    upload = client.post(
        "/api/v2/portfolio/uploads",
        json={
            "csv_text": csv_text,
            "filename": "holdings.csv",
            "allow_live_pricing": False,
        },
    )
    assert upload.status_code == 200
    upload_payload = upload.json()
    run_id = upload_payload["run_id"]
    assert upload_payload["upload_detail"]["run_id"] == run_id

    status = client.get("/api/v2/portfolio/status")
    assert status.status_code == 200
    status_payload = status.json()
    assert status_payload["active_upload"]["run_id"] == run_id
    assert status_payload["mapping_summary"]["stale_price_count"] >= 0
    assert status_payload["base_currency"] == "SGD"

    overrides = client.post(
        "/api/v2/portfolio/mapping/overrides",
        json={"symbol": "SPY", "sleeve": "cash"},
    )
    assert overrides.status_code == 200
    assert overrides.json()["saved"] is True

    surface = client.get("/api/v2/surfaces/portfolio")
    assert surface.status_code == 200
    surface_payload = surface.json()
    assert surface_payload["active_upload"]["run_id"] == run_id
    assert "portfolio_source_state" in surface_payload
    assert "upload_sync" in surface_payload["section_states"]
    assert surface_payload["mapping_summary"]["override_count"] >= 1


def test_feedback_routes_persist_reviews(client: TestClient) -> None:
    response = client.post(
        "/api/v2/admin/feedback/recommendations/candidate_instrument_cmod/review",
        json={
            "decision_label": "Still preferred",
            "review_outcome": "confirmed",
            "notes": "Threshold held and evidence remained current.",
            "what_changed_view": "Support stayed intact.",
            "overconfident_forecast_support": False,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["review_outcome"] == "confirmed"

    reviews = client.get("/api/v2/admin/feedback/recommendations")
    assert reviews.status_code == 200
    reviews_payload = reviews.json()
    assert reviews_payload["summary"]["review_count"] == 1
    assert reviews_payload["reviews"][0]["decision_label"] == "Still preferred"

    forecast = client.get("/api/v2/admin/feedback/forecast")
    assert forecast.status_code == 200
    assert "providers" in forecast.json()

    triggers = client.get("/api/v2/admin/feedback/triggers")
    assert triggers.status_code == 200
    assert "trigger_count" in triggers.json()


def test_governance_conflicts_include_candidate_field_conflicts(client: TestClient) -> None:
    import sqlite3

    from app.config import get_db_path
    from app.services.blueprint_candidate_truth import resolve_candidate_field_truth, upsert_field_observation

    with sqlite3.connect(get_db_path()) as conn:
        conn.row_factory = sqlite3.Row
        upsert_field_observation(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            field_name="expense_ratio",
            value=0.0022,
            source_name="issuer_doc_parser",
            observed_at="2026-04-01T00:00:00+00:00",
            provenance_level="verified_official",
            value_type="verified",
        )
        upsert_field_observation(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            field_name="expense_ratio",
            value=0.0040,
            source_name="provider_summary",
            observed_at="2026-04-01T00:00:00+00:00",
            provenance_level="verified_nonissuer",
            value_type="verified",
        )
        resolve_candidate_field_truth(conn, candidate_symbol="VWRA", sleeve_key="global_equity_core")
        conn.commit()

    payload = client.get("/api/v2/admin/governance/conflicts")
    assert payload.status_code == 200
    body = payload.json()
    assert body["candidate_field_conflict_count"] >= 1
    assert "field_conflicts" in body
    assert any(
        item.get("conflict_type") == "candidate_field_conflict"
        and item.get("candidate_symbol") == "VWRA"
        and item.get("field_name") == "expense_ratio"
        and "authority_mix" in item
        for item in body["conflicts"]
    )


def test_replay_route_returns_latest_promoted_snapshot(client: TestClient) -> None:
    report = client.get("/api/v2/surfaces/candidates/candidate_instrument_vwra/report")
    assert report.status_code == 200

    replay = client.get("/api/v2/admin/replay/surfaces/candidate_report/candidate_instrument_vwra")
    assert replay.status_code == 200
    payload = replay.json()
    assert payload["surface_id"] == "candidate_report"
    assert "decision_inputs" in payload
    assert "source_fingerprints" in payload["decision_inputs"]
