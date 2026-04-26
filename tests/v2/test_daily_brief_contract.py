from __future__ import annotations

import inspect
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.v2.router import daily_brief as daily_brief_route
from app.v2.router import router as v2_router
from app.v2.surfaces.daily_brief import contract_builder


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


def test_daily_brief_route_returns_contract_shape() -> None:
    response = client.get("/api/v2/surfaces/daily-brief")

    assert response.status_code == 200
    payload = response.json()
    assert payload["surface_id"] == "daily_brief"
    assert "what_changed" in payload
    assert "why_it_matters_economically" in payload
    assert "review_posture" in payload
    assert "evidence_and_trust" in payload


def test_daily_brief_route_keeps_overlay_null_without_holdings() -> None:
    response = client.get("/api/v2/surfaces/daily-brief")

    assert response.status_code == 200
    payload = response.json()
    assert payload["portfolio_overlay"] is None
    assert payload["holdings_overlay_present"] is False


def test_daily_brief_route_does_not_import_blueprint_payload_assembler() -> None:
    route_source = inspect.getsource(daily_brief_route)
    builder_source = inspect.getsource(contract_builder)

    assert "blueprint_payload_assembler" not in route_source
    assert "blueprint_payload_assembler" not in builder_source


def test_daily_brief_route_excludes_banned_fields() -> None:
    response = client.get("/api/v2/surfaces/daily-brief")

    assert response.status_code == 200
    payload = response.json()
    forbidden = {"gate_result", "review_intensity_decision", "prompt_schema", "retry_count"}
    assert forbidden.isdisjoint(set(_walk_keys(payload)))
