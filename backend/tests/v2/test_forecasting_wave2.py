from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient


_BLANK_ENV_KEYS = [
    "FINNHUB_API_KEY",
    "FRED_API_KEY",
    "POLYGON_API_KEY",
    "EODHD_API_KEY",
    "TIINGO_API_KEY",
    "ALPHA_VANTAGE_API_KEY",
    "TWELVE_DATA_API_KEY",
    "FMP_API_KEY",
    "IA_TIMESFM_BASE_URL",
    "IA_CHRONOS_BASE_URL",
    "IA_MOIRAI_BASE_URL",
    "IA_LAGLLAMA_BASE_URL",
]


@pytest.fixture
def isolated_forecast_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    db_path = tmp_path / "forecast_wave2.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))
    monkeypatch.setenv("IA_FORECAST_BENCHMARK_MODE", "0")
    for key in _BLANK_ENV_KEYS:
        monkeypatch.setenv(key, "")

    import app.v2.forecasting.service as forecast_service

    monkeypatch.setattr(
        forecast_service,
        "_fetch_symbol_history",
        lambda *, symbol, surface_name: (
            [100.0, 101.5, 102.2, 103.1, 104.0, 104.8],
            [
                "2026-03-27T00:00:00+00:00",
                "2026-03-28T00:00:00+00:00",
                "2026-03-29T00:00:00+00:00",
                "2026-03-30T00:00:00+00:00",
                "2026-03-31T00:00:00+00:00",
                "2026-04-01T00:00:00+00:00",
            ],
            "test_history",
        ),
    )
    return db_path


def _stable_candidate_id(identifier: str = "VWRA") -> str:
    from app.v2.donors.instrument_truth import get_instrument_truth

    truth = get_instrument_truth(identifier)
    return f"candidate_{truth.instrument_id}"


def test_forecast_capability_matrix_route_has_fallback(isolated_forecast_env: Path) -> None:
    from app.v2.app import app

    with TestClient(app) as client:
        response = client.get("/api/v2/admin/forecast/capability-matrix")
    assert response.status_code == 200
    payload = response.json()
    providers = {row["provider"] for row in payload["capabilities"]}
    assert "deterministic_baseline" in providers
    assert {"timesfm", "chronos", "moirai", "lagllama"} <= providers
    assert "timegpt" not in providers
    assert "readiness" in payload
    assert "latest_probes" in payload
    assert "latest_runs" in payload
    assert "recent_evaluations" in payload


def test_forecast_bundle_persists_with_explicit_degraded_fallback(isolated_forecast_env: Path) -> None:
    from app.v2.features.forecast_feature_service import build_candidate_support_bundle
    from app.v2.forecasting.store import latest_scenario_support, latest_trigger_states, list_latest_runs

    candidate_id = "candidate_instrument_vwra"
    payload = build_candidate_support_bundle(
        candidate_id=candidate_id,
        symbol="VWRA",
        label="VWRA",
        sleeve_purpose="Global equity core",
        implication="Keep the core allocation under review.",
        summary="Core sleeve remains the anchor implementation.",
        current_value=104.8,
        history=[100.0, 101.5, 102.2, 103.1, 104.0, 104.8],
        timestamps=[
            "2026-03-27T00:00:00+00:00",
            "2026-03-28T00:00:00+00:00",
            "2026-03-29T00:00:00+00:00",
            "2026-03-30T00:00:00+00:00",
            "2026-03-31T00:00:00+00:00",
            "2026-04-01T00:00:00+00:00",
        ],
        surface_name="candidate_report",
    )

    assert payload["forecast_support"]["provider"] == "deterministic_baseline"
    assert payload["forecast_support"]["degraded_state"] == "provider_unavailable"
    assert payload["scenario_blocks"]
    assert payload["decision_thresholds"]
    assert list_latest_runs(candidate_id=candidate_id)
    assert latest_scenario_support(candidate_id) is not None
    assert latest_trigger_states(candidate_id)


def test_daily_brief_contract_emits_forecast_supported_sections(
    isolated_forecast_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.v2.core.domain_objects import EvidencePack, MacroTruth, MarketDataPoint, MarketSeriesTruth
    import app.v2.surfaces.daily_brief.contract_builder as brief_builder

    evidence = [
        EvidencePack(
            evidence_id="pack_market",
            thesis="Market truth",
            summary="Market support pack",
            freshness="fresh_full_rebuild",
        )
    ]
    monkeypatch.setattr(
        brief_builder,
        "_load_market_truths",
        lambda: [
            MarketSeriesTruth(
                series_id="series_acwi",
                label="ACWI",
                frequency="daily",
                units="index",
                points=[
                    MarketDataPoint(at="2026-03-31T00:00:00+00:00", value=100.0),
                    MarketDataPoint(at="2026-04-01T00:00:00+00:00", value=103.0),
                ],
                evidence=evidence,
                as_of="2026-04-01T00:00:00+00:00",
            )
        ],
    )
    monkeypatch.setattr(
        brief_builder,
        "_load_macro_truths",
        lambda: (
            [
                MacroTruth(
                    macro_id="macro_rates",
                    regime="Rates steady",
                    summary="Rates remain elevated but stable.",
                    indicators={"current_value": 4.2, "previous_value": 4.1},
                    evidence=evidence,
                    as_of="2026-04-01T00:00:00+00:00",
                )
            ],
            "macro_adapter",
        ),
    )
    monkeypatch.setattr(brief_builder, "_load_news_truths", lambda: [])

    contract = brief_builder.build()

    assert contract["scenario_blocks"]
    assert contract["monitoring_conditions"]
    assert contract["scenario_blocks"][0]["forecast_support"] is not None
    assert contract["monitoring_conditions"][0]["trigger_support"] is not None
    assert contract["section_states"]["scenarios"]["state"] in {"ready", "degraded"}


def test_candidate_report_and_portfolio_emit_wave2_forecast_fields(
    isolated_forecast_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.v2.core.domain_objects import MarketDataPoint, MarketSeriesTruth, PortfolioTruth
    import app.v2.surfaces.blueprint.report_contract_builder as report_builder
    import app.v2.surfaces.portfolio.contract_builder as portfolio_builder

    monkeypatch.setattr(
        report_builder,
        "_market_truth",
        lambda symbol: MarketSeriesTruth(
            series_id=f"series_{symbol.lower()}",
            label=f"{symbol} market context",
            frequency="daily",
            units="price",
            points=[
                MarketDataPoint(at="2026-03-31T00:00:00+00:00", value=100.0),
                MarketDataPoint(at="2026-04-01T00:00:00+00:00", value=102.5),
            ],
            as_of="2026-04-01T00:00:00+00:00",
        ),
    )
    monkeypatch.setattr(
        portfolio_builder,
        "get_portfolio_truth",
        lambda account_id: PortfolioTruth(
            portfolio_id=account_id,
            name="Default",
            base_currency="USD",
            holdings=[
                {"symbol": "VWRA", "name": "Vanguard FTSE All-World", "sleeve": "global_equity"},
                {"symbol": "AGGU", "name": "Bond Aggregate", "sleeve": "ig_bond"},
            ],
            exposures={"global_equity": 0.42, "ig_bond": 0.30, "cash": 0.12},
            cash_weight=0.12,
            as_of="2026-04-01T00:00:00+00:00",
        ),
    )

    report = report_builder.build("VWRA")
    portfolio = portfolio_builder.build("default")

    assert report["forecast_support"] is not None
    assert report["scenario_blocks"]
    assert report["scenario_blocks"][0]["forecast_support"] is not None
    assert report["decision_thresholds"][0]["forecast_support"] is not None
    assert portfolio["forecast_watchlist"]
    assert portfolio["forecast_watchlist"][0]["forecast_support"] is not None


def test_notebook_evidence_and_changes_integrate_forecast_records(
    isolated_forecast_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.v2.core.change_ledger import record_change
    from app.v2.features.forecast_feature_service import build_candidate_support_bundle
    from app.v2.storage.notebook_store import create_entry
    from app.v2.forecasting.store import add_notebook_forecast_reference, list_latest_runs
    from app.v2.surfaces.changes.contract_builder import build as build_changes
    from app.v2.surfaces.evidence_workspace.contract_builder import build as build_evidence
    from app.v2.surfaces.notebook.contract_builder import build as build_notebook

    stable_candidate_id = _stable_candidate_id("VWRA")
    forecast_payload = build_candidate_support_bundle(
        candidate_id=stable_candidate_id,
        symbol="VWRA",
        label="VWRA",
        sleeve_purpose="Global equity core",
        implication="Scenario support exists for the candidate report.",
        summary="Persist forecast support for notebook and evidence surfaces.",
        current_value=104.8,
        history=[100.0, 101.0, 102.0, 103.0, 104.0, 104.8],
        timestamps=[
            "2026-03-27T00:00:00+00:00",
            "2026-03-28T00:00:00+00:00",
            "2026-03-29T00:00:00+00:00",
            "2026-03-30T00:00:00+00:00",
            "2026-03-31T00:00:00+00:00",
            "2026-04-01T00:00:00+00:00",
        ],
        surface_name="candidate_report",
    )
    forecast_run_id = list_latest_runs(candidate_id=stable_candidate_id, limit=1)[0]["forecast_run_id"]

    entry = create_entry(
        stable_candidate_id,
        linked_object_type="candidate",
        linked_object_id=stable_candidate_id,
        linked_object_label="VWRA",
        title="VWRA note",
        thesis="Persist forecast refs alongside thesis memory.",
        assumptions="Forecasts remain support only.",
        invalidation="Threshold support weakens.",
        watch_items="Scenario band and trigger state.",
        reflections="Initial note.",
        next_review_date="2026-05-01",
    )
    add_notebook_forecast_reference(
        entry["entry_id"],
        forecast_run_id=forecast_run_id,
        reference_label="Near-term threshold watch",
        threshold_summary=forecast_payload["decision_thresholds"][0]["value"],
    )
    record_change(
        event_type="forecast_trigger_threshold_crossed",
        surface_id="candidate_report",
        summary="Forecast trigger crossed for VWRA.",
        candidate_id=stable_candidate_id,
        previous_state="watch",
        current_state="breached",
        implication_summary="Scenario support escalated the monitoring threshold.",
        report_tab="scenarios",
        impact_level="high",
        requires_review=True,
    )

    notebook_contract = build_notebook("VWRA")
    evidence_contract = build_evidence("VWRA")
    changes_contract = build_changes("candidate_report")

    assert notebook_contract["active_draft"]["forecast_refs"]
    assert evidence_contract["forecast_support_items"]
    assert any(document["document_type"] == "forecast_support" for document in evidence_contract["documents"])
    assert any(event["event_type"] == "forecast_trigger_threshold_crossed" for event in changes_contract["change_events"])
