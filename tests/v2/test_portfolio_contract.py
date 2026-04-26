from __future__ import annotations

import inspect
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import get_db_path
from app.models.db import connect
from app.services.portfolio_state import save_snapshot, upsert_holding
from app.v2.router import portfolio as portfolio_route
from app.v2.router import router as v2_router
from app.v2.surfaces.portfolio import contract_builder


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


def _seed_holdings() -> None:
    conn = connect(get_db_path())
    try:
        upsert_holding(
            conn,
            {
                "holding_id": "holding_iwda",
                "symbol": "IWDA",
                "name": "iShares Core MSCI World UCITS ETF",
                "quantity": 12,
                "cost_basis": 100,
                "currency": "USD",
                "sleeve": "global_equity",
                "account_type": "broker",
            },
        )
        upsert_holding(
            conn,
            {
                "holding_id": "holding_aggu",
                "symbol": "AGGU",
                "name": "iShares Core Global Aggregate Bond UCITS ETF",
                "quantity": 10,
                "cost_basis": 80,
                "currency": "USD",
                "sleeve": "ig_bond",
                "account_type": "broker",
            },
        )
        save_snapshot(
            conn,
            total_value=2000.0,
            sleeve_weights={
                "global_equity": 0.60,
                "ig_bond": 0.30,
                "cash": 0.10,
            },
            concentration_metrics={"top_position_weight": 0.35},
            convex_coverage_ratio=0.0,
            tax_drag_estimate=0.0,
            notes="seeded for v2 portfolio route test",
        )
    finally:
        conn.close()


def test_portfolio_route_returns_200() -> None:
    response = client.get("/api/v2/surfaces/portfolio")

    assert response.status_code == 200


def test_portfolio_route_contains_required_fields() -> None:
    _seed_holdings()

    response = client.get("/api/v2/surfaces/portfolio", params={"account_id": "default"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["surface_id"] == "portfolio"
    assert "account_id" in payload
    assert "mandate_state" in payload
    assert "sleeve_drift_summary" in payload
    assert "holdings_overlay_present" in payload
    assert payload["holdings_overlay_present"] is True


def test_portfolio_route_keeps_optional_consequences_present() -> None:
    _seed_holdings()

    response = client.get("/api/v2/surfaces/portfolio")

    assert response.status_code == 200
    payload = response.json()
    assert "blueprint_consequence" in payload
    assert "daily_brief_consequence" in payload
    assert payload["blueprint_consequence"] is None or isinstance(payload["blueprint_consequence"], str)
    assert payload["daily_brief_consequence"] is None or isinstance(payload["daily_brief_consequence"], str)


def test_portfolio_route_excludes_banned_fields_and_banned_imports() -> None:
    route_source = inspect.getsource(portfolio_route)
    builder_source = inspect.getsource(contract_builder)

    assert "blueprint_payload_assembler" not in str(inspect.stack(context=0))
    assert "blueprint_payload_assembler" not in route_source
    assert "blueprint_payload_assembler" not in builder_source

    response = client.get("/api/v2/surfaces/portfolio")

    assert response.status_code == 200
    payload = response.json()
    forbidden = {"gate_result", "review_intensity_decision", "prompt_schema"}
    assert forbidden.isdisjoint(set(_walk_keys(payload)))
