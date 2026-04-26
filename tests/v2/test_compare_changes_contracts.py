from __future__ import annotations

import inspect
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.v2.core.change_ledger import get_diffs, record_change
from app.v2.router import changes as changes_route
from app.v2.router import compare as compare_route
from app.v2.router import router as v2_router
from app.v2.surfaces.changes import contract_builder as changes_contract_builder
from app.v2.surfaces.compare import contract_builder as compare_contract_builder


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


def test_compare_route_returns_200_and_required_fields() -> None:
    assert "blueprint_payload_assembler" not in str(inspect.stack(context=0))

    response = client.get("/api/v2/surfaces/compare", params={"candidate_a": "VWRA", "candidate_b": "IWDA"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["surface_id"] == "compare"
    assert "who_leads" in payload
    assert "why_leads" in payload
    assert "dimensions" in payload
    assert isinstance(payload["dimensions"], list)
    assert payload["holdings_overlay_present"] is False


def test_changes_route_returns_200_and_required_fields() -> None:
    response = client.get("/api/v2/surfaces/changes", params={"surface_id": "blueprint_explorer"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["surface_id"] == "blueprint_explorer"
    assert "change_events" in payload
    assert "net_impact" in payload
    assert isinstance(payload["change_events"], list)


def test_change_ledger_round_trip() -> None:
    event_id = record_change("truth_change", "blueprint_explorer", "Factsheet evidence refreshed.")
    diffs = get_diffs("blueprint_explorer")

    assert event_id
    assert diffs
    matching = next(event for event in diffs if event.event_id == event_id)
    assert matching.event_type == "truth_change"
    assert matching.summary == "Factsheet evidence refreshed."


def test_compare_and_changes_routes_do_not_import_blueprint_payload_assembler() -> None:
    compare_route_source = inspect.getsource(compare_route)
    compare_builder_source = inspect.getsource(compare_contract_builder)
    changes_route_source = inspect.getsource(changes_route)
    changes_builder_source = inspect.getsource(changes_contract_builder)

    assert "blueprint_payload_assembler" not in compare_route_source
    assert "blueprint_payload_assembler" not in compare_builder_source
    assert "blueprint_payload_assembler" not in changes_route_source
    assert "blueprint_payload_assembler" not in changes_builder_source

    compare_response = client.get("/api/v2/surfaces/compare", params={"candidate_a": "VWRA", "candidate_b": "IWDA"})
    changes_response = client.get("/api/v2/surfaces/changes", params={"surface_id": "blueprint_explorer"})

    assert compare_response.status_code == 200
    assert changes_response.status_code == 200
    forbidden = {"gate_result", "review_intensity_decision", "prompt_schema", "retry_count"}
    assert forbidden.isdisjoint(set(_walk_keys(compare_response.json())))
    assert forbidden.isdisjoint(set(_walk_keys(changes_response.json())))
