from __future__ import annotations

from pathlib import Path

import pytest


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
def isolated_chart_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "chart_contracts.sqlite3"))
    for key in _BLANK_ENV_KEYS:
        monkeypatch.setenv(key, "")

    import app.v2.forecasting.service as forecast_service

    monkeypatch.setattr(
        forecast_service,
        "_fetch_symbol_history",
        lambda *, symbol, surface_name: (
            [100.0, 101.0, 102.4, 103.1, 104.3, 105.0],
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


def _market_truth(series_id: str, label: str):
    from app.v2.core.domain_objects import EvidencePack, MarketDataPoint, MarketSeriesTruth

    evidence = [
        EvidencePack(
            evidence_id=f"evidence_{series_id}",
            thesis=f"{label} chart support",
            summary="Deterministic chart test support",
            freshness="fresh_full_rebuild",
            facts={
                "source_family": "ohlcv_history",
                "source_label": "Test provider",
                "freshness_state": "fresh_full_rebuild",
                "trust_state": "direct_support",
            },
        )
    ]
    return MarketSeriesTruth(
        series_id=series_id,
        label=label,
        frequency="daily",
        units="price",
        points=[
            MarketDataPoint(at="2026-03-27T00:00:00+00:00", value=100.0),
            MarketDataPoint(at="2026-03-28T00:00:00+00:00", value=101.5),
            MarketDataPoint(at="2026-03-29T00:00:00+00:00", value=102.0),
            MarketDataPoint(at="2026-03-30T00:00:00+00:00", value=103.4),
            MarketDataPoint(at="2026-03-31T00:00:00+00:00", value=104.2),
            MarketDataPoint(at="2026-04-01T00:00:00+00:00", value=105.0),
        ],
        evidence=evidence,
        as_of="2026-04-01T00:00:00+00:00",
    )


def test_daily_brief_emits_chart_panels(
    isolated_chart_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.v2.core.domain_objects import EvidencePack, MacroTruth
    import app.v2.surfaces.daily_brief.contract_builder as brief_builder

    evidence = [
        EvidencePack(
            evidence_id="pack_macro",
            thesis="Macro truth",
            summary="Macro support pack",
            freshness="fresh_full_rebuild",
        )
    ]

    monkeypatch.setattr(
        brief_builder,
        "_load_market_truths",
        lambda: [
            _market_truth("series_acwi", "ACWI"),
            _market_truth("series_dxy", "DXY"),
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

    assert contract["macro_chart_panels"]
    assert contract["cross_asset_chart_panels"]
    assert contract["fx_chart_panels"]
    assert contract["signal_chart_panels"]
    assert contract["scenario_chart_panels"]
    assert "series" not in contract["macro_chart_panels"][0]
    assert "provider_name" not in contract["signal_chart_panels"][0]["panel"]


def test_candidate_report_and_portfolio_emit_chart_contracts(
    isolated_chart_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.v2.core.domain_objects import PortfolioTruth
    import app.v2.surfaces.blueprint.report_contract_builder as report_builder
    import app.v2.surfaces.portfolio.contract_builder as portfolio_builder

    monkeypatch.setattr(
        report_builder,
        "_market_truth",
        lambda symbol, endpoint_family="ohlcv_history": _market_truth(f"series_{symbol.lower()}", symbol),
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
                {"symbol": "AGGU", "name": "Global aggregate bond", "sleeve": "ig_bond"},
            ],
            exposures={"global_equity": 0.45, "ig_bond": 0.27, "cash": 0.15},
            cash_weight=0.15,
            as_of="2026-04-01T00:00:00+00:00",
        ),
    )

    report = report_builder.build("VWRA")
    portfolio = portfolio_builder.build("default")

    assert report["market_history_charts"]
    assert report["scenario_charts"]
    assert report["competition_charts"]
    assert report["market_history_charts"][0]["primary_series"]["points"]
    assert portfolio["allocation_chart_panels"]
    assert len(portfolio["allocation_chart_panels"]) >= 2
    assert portfolio["allocation_chart_panels"][0]["thresholds"]


def test_blueprint_candidate_rows_emit_detail_chart_panels(
    isolated_chart_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.surfaces.blueprint.explorer_contract_builder as explorer_builder

    monkeypatch.setattr(explorer_builder, "ensure_candidate_registry_tables", lambda conn: None)
    monkeypatch.setattr(explorer_builder, "seed_default_candidate_registry", lambda conn: None)
    monkeypatch.setattr(
        explorer_builder,
        "export_live_candidate_registry",
        lambda conn: [
            {
                "symbol": "VWRA",
                "name": "Vanguard FTSE All-World",
                "sleeve_key": "global_equity_core",
                "expense_ratio": 0.0022,
                "aum_usd": 5_000_000_000,
                "liquidity_score": 0.8,
            }
        ],
    )
    monkeypatch.setattr(
        explorer_builder,
        "_market_truth",
        lambda symbol, endpoint_family="ohlcv_history": _market_truth(f"series_{symbol.lower()}", symbol),
    )

    contract = explorer_builder.build()
    first_candidate = contract["sleeves"][0]["candidates"][0]

    assert first_candidate["detail_chart_panels"]
    assert first_candidate["detail_chart_panels"][0]["chart_type"] == "comparison_line"
    assert "series" not in first_candidate["detail_chart_panels"][0]


def test_daily_brief_shared_chart_contract_declares_object_roles() -> None:
    shared_contract = Path(__file__).resolve().parents[3] / "shared" / "v2_surface_contracts.ts"
    source = shared_contract.read_text(encoding="utf-8")

    assert "observed_path?: DailyBriefChartObservedPath" in source
    assert "forecast_path?: DailyBriefChartForecastPath" in source
    assert "review_context?: DailyBriefChartReviewBand" in source
    assert "decision_references?: DailyBriefChartDecisionReference[] | null" in source
    assert "focus_modes?: DailyBriefChartFocusMode[] | null" in source
    assert "hover_payload_by_timestamp?: DailyBriefChartHoverPayload[] | null" in source
