from __future__ import annotations

import inspect
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.v2.router import evidence_workspace as evidence_workspace_route
from app.v2.router import notebook as notebook_route
from app.v2.router import router as v2_router
from app.v2.surfaces.evidence_workspace import contract_builder as evidence_workspace_builder
from app.v2.surfaces.notebook import contract_builder as notebook_builder


app_under_test = FastAPI()
app_under_test.include_router(v2_router)
client = TestClient(app_under_test)


@pytest.fixture(autouse=True)
def isolated_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "investment_agent.sqlite3"))


def _candidate_id() -> str:
    return "VWRA"


def test_notebook_route_returns_200_and_required_fields() -> None:
    candidate_id = _candidate_id()
    assert "blueprint_payload_assembler" not in str(inspect.stack(context=0))

    response = client.get(f"/api/v2/surfaces/candidates/{candidate_id}/notebook")

    assert response.status_code == 200
    payload = response.json()
    assert payload["surface_id"] == "notebook"
    assert payload["candidate_id"].startswith("candidate_")
    assert "evidence_sections" in payload
    assert "evidence_depth" in payload
    assert "blueprint_payload_assembler" not in response.text


def test_evidence_workspace_route_returns_200_and_required_fields() -> None:
    candidate_id = _candidate_id()
    assert "blueprint_payload_assembler" not in str(inspect.stack(context=0))

    response = client.get(f"/api/v2/surfaces/candidates/{candidate_id}/evidence")

    assert response.status_code == 200
    payload = response.json()
    assert payload["surface_id"] == "evidence_workspace"
    assert payload["candidate_id"].startswith("candidate_")
    assert "evidence_pack" in payload
    assert "source_citations" in payload
    assert "completeness_score" in payload
    assert "blueprint_payload_assembler" not in response.text


def test_notebook_and_evidence_routes_do_not_import_blueprint_payload_assembler() -> None:
    notebook_route_source = inspect.getsource(notebook_route)
    notebook_builder_source = inspect.getsource(notebook_builder)
    evidence_route_source = inspect.getsource(evidence_workspace_route)
    evidence_builder_source = inspect.getsource(evidence_workspace_builder)

    assert "blueprint_payload_assembler" not in notebook_route_source
    assert "blueprint_payload_assembler" not in notebook_builder_source
    assert "blueprint_payload_assembler" not in evidence_route_source
    assert "blueprint_payload_assembler" not in evidence_builder_source
