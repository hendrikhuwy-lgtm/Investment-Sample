from __future__ import annotations

import inspect
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.v2.router import router as v2_router


app_under_test = FastAPI()
app_under_test.include_router(v2_router)
client = TestClient(app_under_test)


@pytest.fixture(autouse=True)
def isolated_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "investment_agent.sqlite3"))


def _walk_keys(value: object) -> Iterator[str]:
    if isinstance(value, dict):
        for key, item in value.items():
            yield str(key)
            yield from _walk_keys(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_keys(item)


def test_blueprint_explorer_route_returns_contract_shape() -> None:
    assert "blueprint_payload_assembler" not in str(inspect.stack(context=0))

    response = client.get("/api/v2/surfaces/blueprint/explorer")

    assert response.status_code == 200
    payload = response.json()
    assert payload["surface_id"] == "blueprint_explorer"
    assert "freshness_state" in payload
    assert payload["holdings_overlay_present"] is False
    assert isinstance(payload["sleeves"], list)
    assert payload["sleeves"]
    assert "blueprint_payload_assembler" not in response.text
    assert "gate_result" not in set(_walk_keys(payload))
    assert "review_intensity_decision" not in set(_walk_keys(payload))
    assert "deep_review_result" not in set(_walk_keys(payload))


def test_candidate_report_route_returns_contract_shape_for_known_candidate() -> None:
    explorer = client.get("/api/v2/surfaces/blueprint/explorer")
    candidate_id = explorer.json()["sleeves"][0]["lead_candidate_id"]

    assert "blueprint_payload_assembler" not in str(inspect.stack(context=0))

    response = client.get(f"/api/v2/surfaces/candidates/{candidate_id}/report")

    assert response.status_code == 200
    payload = response.json()
    assert payload["surface_id"] == "candidate_report"
    assert payload["candidate_id"] == candidate_id
    assert payload["holdings_overlay_present"] is False
    assert "gate_result" not in set(_walk_keys(payload))
    assert "review_intensity_decision" not in set(_walk_keys(payload))
    assert "deep_review_result" not in set(_walk_keys(payload))


def test_blueprint_explorer_stable_ids_across_calls() -> None:
    first = client.get("/api/v2/surfaces/blueprint/explorer")
    second = client.get("/api/v2/surfaces/blueprint/explorer")

    assert first.status_code == 200
    assert second.status_code == 200
    first_ids = [row["lead_candidate_id"] for row in first.json()["sleeves"]]
    second_ids = [row["lead_candidate_id"] for row in second.json()["sleeves"]]
    assert first_ids == second_ids
