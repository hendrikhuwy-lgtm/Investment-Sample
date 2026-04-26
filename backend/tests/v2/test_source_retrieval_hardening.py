from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_daily_brief_targets_align_with_canonical_market_strip() -> None:
    from app.v2.core.market_strip_registry import daily_brief_targets, market_strip_symbols

    targets = daily_brief_targets()

    assert {"ACWI", "AGG", "DXY", "GLD"}.issubset(set(market_strip_symbols()))
    assert {"ACWI", "AGG", "GLD"}.issubset(set(targets["benchmark_proxy"]))
    assert {"ACWI", "AGG", "DXY", "GLD"}.issubset(set(targets["quote_latest"]))
    assert targets["fx_reference"] == ["USD/SGD", "EUR/USD"]


def test_provider_support_status_marks_dxy_symbol_family_rules() -> None:
    from app.services.provider_registry import provider_support_status

    assert provider_support_status("polygon", "quote_latest", "DXY") == (
        False,
        "provider_symbol_family_unsupported",
    )
    assert provider_support_status("tiingo", "quote_latest", "DXY") == (
        False,
        "provider_symbol_family_unsupported",
    )
    assert provider_support_status("eodhd", "quote_latest", "DXY") == (
        False,
        "provider_blocked_by_plan",
    )
    assert provider_support_status("twelve_data", "quote_latest", "DXY") == (True, None)


def test_market_price_adapter_prefers_provider_with_movement_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.sources.market_price_adapter as adapter

    monkeypatch.setenv("POLYGON_API_KEY", "demo")
    monkeypatch.setenv("FINNHUB_API_KEY", "demo")
    monkeypatch.setattr(adapter, "_freshest_snapshot_for_ticker", lambda ticker: None)
    monkeypatch.setattr(adapter, "routed_provider_candidates", lambda family, identifier=None: ["polygon", "finnhub"])
    monkeypatch.setattr(adapter, "provider_support_status", lambda provider, family, identifier=None: (True, None))

    def _fake_fetch(provider_name: str, endpoint_family: str, identifier: str):
        if provider_name == "polygon":
            return {"value": 100.0, "observed_at": "2026-04-06T00:00:00+00:00", "source_ref": "polygon:prev"}
        return {
            "value": 101.0,
            "previous_close": 100.0,
            "change_pct_1d": 1.0,
            "observed_at": "2026-04-06T00:00:00+00:00",
            "source_ref": "finnhub:quote",
        }

    monkeypatch.setattr(adapter, "fetch_provider_data", _fake_fetch)

    payload = adapter.fetch("ACWI")

    assert payload["provider_name"] == "finnhub"
    assert payload["price"] == 101.0
    assert payload["change_pct_1d"] == 1.0
    assert payload["retrieval_path"] == "direct_live"


def test_market_price_adapter_uses_bounded_dxy_proxy_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.sources.market_price_adapter as adapter

    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    monkeypatch.setattr(adapter, "_freshest_snapshot_for_ticker", lambda ticker: None)
    monkeypatch.setattr(adapter, "routed_provider_candidates", lambda family, identifier=None: ["frankfurter"] if family == "usd_strength_fallback" else [])
    monkeypatch.setattr(
        adapter,
        "provider_support_status",
        lambda provider, family, identifier=None: (True, None) if (provider, family) == ("frankfurter", "usd_strength_fallback") else (False, "provider_symbol_family_unsupported"),
    )
    monkeypatch.setattr(
        adapter,
        "fetch_provider_data",
        lambda provider, family, identifier: {
            "value": 101.0,
            "previous_close": 100.0,
            "change_pct_1d": 1.0,
            "observed_at": "2026-04-06",
        },
    )

    payload = adapter.fetch("DXY")

    assert payload["provider_name"] == "frankfurter"
    assert payload["retrieval_path"] == "fallback_derived"
    assert payload["provider_execution"]["source_family"] == "usd_strength_fallback"
    assert payload["provider_execution"]["authority_level"] == "derived"
    assert payload["provider_execution"]["sufficiency_state"] == "proxy_bounded"


def test_market_price_adapter_persists_frankfurter_public_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "market_price.sqlite3"))

    import sqlite3
    import app.v2.sources.market_price_adapter as adapter

    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    monkeypatch.setattr(adapter, "_freshest_snapshot_for_ticker", lambda ticker: None)
    monkeypatch.setattr(adapter, "routed_provider_candidates", lambda family, identifier=None: ["frankfurter"] if family == "usd_strength_fallback" else [])
    monkeypatch.setattr(
        adapter,
        "provider_support_status",
        lambda provider, family, identifier=None: (True, None) if (provider, family) == ("frankfurter", "usd_strength_fallback") else (False, "provider_symbol_family_unsupported"),
    )
    monkeypatch.setattr(
        adapter,
        "fetch_provider_data",
        lambda provider, family, identifier: {
            "value": 101.0,
            "previous_close": 100.0,
            "change_pct_1d": 1.0,
            "observed_at": "2026-04-06",
            "proxy_components": [{"pair": "USD/JPY", "rate": 145.0}],
        },
    )

    payload = adapter.fetch("DXY", surface_name="daily_brief")

    conn = sqlite3.connect(tmp_path / "market_price.sqlite3")
    try:
        row = conn.execute(
            """
            SELECT provider_key, family_name, observed_at
            FROM public_upstream_snapshots
            WHERE provider_key = 'frankfurter'
            ORDER BY fetched_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert payload["provider_name"] == "frankfurter"
    assert row == ("frankfurter", "usd_strength_fallback", "2026-04-06")


def test_chart_contracts_derives_two_points_from_previous_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "chart.sqlite3"))

    import app.v2.contracts.chart_contracts as chart_contracts

    monkeypatch.setattr(
        chart_contracts,
        "fetch_routed_family",
        lambda conn, **kwargs: {
            "provider_name": "twelve_data",
            "source_name": "Twelve Data",
            "endpoint_family": "quote_latest",
            "identifier": kwargs["identifier"],
            "value": 104.0,
            "previous_close": 100.0,
            "change_pct_1d": 4.0,
            "observed_at": "2026-04-06T00:00:00+00:00",
            "cache_status": "miss",
            "freshness_state": "fresh_full_rebuild",
            "retrieval_path": "routed_live",
        },
    )

    truth, payload = chart_contracts.load_routed_market_series(
        surface_name="daily_brief",
        endpoint_family="quote_latest",
        identifier="DXY",
        label="DXY",
    )

    assert payload["retrieval_path"] == "routed_live"
    assert len(truth.points) == 2
    assert truth.points[0].value == 100.0
    assert truth.points[1].value == 104.0


def test_chart_contracts_backfill_provider_execution_for_cached_quotes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "chart_cached.sqlite3"))

    import app.v2.contracts.chart_contracts as chart_contracts

    monkeypatch.setattr(
        chart_contracts,
        "_cached_family_payload",
        lambda conn, **kwargs: {
            "provider_name": "finnhub",
            "source_name": "Finnhub",
            "endpoint_family": "quote_latest",
            "identifier": kwargs["identifier"],
            "price": 99.0,
            "previous_close": 98.0,
            "observed_at": "2026-04-06T00:00:00+00:00",
            "cache_status": "hit",
            "freshness_state": "stored_valid_context",
            "retrieval_path": "routed_cache",
        },
    )

    truth, payload = chart_contracts.load_routed_market_series(
        surface_name="blueprint",
        endpoint_family="quote_latest",
        identifier="IWDP",
        label="IWDP",
        allow_live_fetch=False,
    )

    execution = payload["provider_execution"]

    assert payload["provider_name"] == "finnhub"
    assert execution["provider_name"] == "finnhub"
    assert execution["usable_truth"] is True
    assert execution["sufficiency_state"] == "movement_capable"
    assert truth.evidence[0].facts["source_provider"] == "finnhub"


def test_finnhub_reference_meta_empty_payload_is_not_treated_as_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.services.provider_adapters as provider_adapters

    monkeypatch.setenv("FINNHUB_API_KEY", "demo")
    monkeypatch.setattr(provider_adapters, "_get", lambda *args, **kwargs: {})

    with pytest.raises(provider_adapters.ProviderAdapterError) as exc:
        provider_adapters.fetch_provider_data("finnhub", "reference_meta", "IWDP")

    assert exc.value.error_class == "empty_response"


def test_doc_parser_extracts_ishares_net_assets_of_fund_phrase() -> None:
    from app.services.etf_doc_parser import _parse_aum_usd

    text = """
    PRODUCT INFORMATION
    Net Assets of Share Class (M) : 1,177.31 USD
    KEY FACTS
    Net Assets of Fund (M) : 1,743.40 USD
    """

    assert _parse_aum_usd(text) == pytest.approx(1_743_400_000.0)


def test_doc_parser_extracts_launch_date_phrase() -> None:
    from app.services.etf_doc_parser import _parse_launch_date

    text = """
    FUND SNAPSHOT
    Fund launch date 20 November 2007
    Portfolio characteristics follow below.
    """

    assert _parse_launch_date(text) == "2007-11-20"


def test_doc_parser_extracts_us_style_and_hyphenated_launch_dates() -> None:
    from app.services.etf_doc_parser import _parse_launch_date

    assert _parse_launch_date("Fund Information\nInception Date 05/25/2007\n") == "2007-05-25"
    assert _parse_launch_date("Fund Launch Date : 20-Feb-2019\n") == "2019-02-20"


def test_doc_parser_extracts_plain_fund_size_usd_phrase() -> None:
    from app.services.etf_doc_parser import _parse_aum_usd

    text = """
    Fund facts
    Inception date 26 January 2011
    Fund Size USD 1,281,263,968
    """

    assert _parse_aum_usd(text) == pytest.approx(1_281_263_968.0)


def test_doc_parser_derives_blackrock_tracking_difference_from_performance_table() -> None:
    from app.services.etf_doc_parser import _parse_tracking_differences

    text = """
    CUMULATIVE & ANNUALISED PERFORMANCE
    CUMULATIVE (%) ANNUALISED (% p.a.)
    1m 3m 6m YTD 1y 3y 5y Since Inception
    Share Class 0.73 3.82 9.62 2.99 21.41 20.63 12.54 10.90
    Benchmark 0.73 3.82 9.61 2.99 21.33 20.58 12.46 10.94
    Source: BlackRock
    """

    parsed = _parse_tracking_differences(text)

    assert parsed["tracking_difference_1y"] == pytest.approx(0.0008)
    assert parsed["tracking_difference_3y"] == pytest.approx(0.0005)
    assert parsed["tracking_difference_5y"] == pytest.approx(0.0008)


def test_doc_parser_fixture_extracts_current_xcha_launch_and_aum() -> None:
    from app.services.etf_doc_parser import fetch_candidate_docs

    result = fetch_candidate_docs("XCHA", use_fixtures=True)
    extracted = dict(result.get("extracted") or {})

    assert extracted["launch_date"] == "2010-06-24"
    assert extracted["aum_usd"] == pytest.approx(2_200_000_000.0)
    assert extracted["primary_trading_currency"] == "USD"


def test_refresh_etf_data_uses_document_factsheet_for_iwdp(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "etf_refresh.sqlite3"))

    import app.services.ingest_etf_data as ingest

    monkeypatch.setattr(
        ingest,
        "fetch_document_factsheet_metrics",
        lambda symbol, conn: {
            "status": "success",
            "asof_date": "2026-04-07",
            "aum_usd": 1_743_400_000.0,
            "tracking_difference_1y": None,
            "tracking_difference_3y": None,
            "tracking_difference_5y": None,
            "tracking_error_1y": None,
            "dividend_yield": None,
            "source_url": "https://www.blackrock.com/example.pdf",
        },
    )
    monkeypatch.setattr(
        ingest,
        "fetch_configured_market_data",
        lambda symbol, conn: {
            "status": "success",
            "exchange": "LSE",
            "ticker": symbol,
            "asof_date": "2026-04-07",
            "source_url": "https://finance.yahoo.com/quote/IWDP.L",
            "bid_ask_spread_bps": 16.0,
            "volume_30d_avg": 24_000,
        },
    )

    result = ingest.refresh_etf_data("IWDP")

    assert result["factsheet"]["status"] == "success"
    assert result["factsheet"]["aum_usd"] == pytest.approx(1_743_400_000.0)


def test_refresh_registry_candidate_truth_persists_launch_date_and_benchmark_mapping(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "candidate_truth.sqlite3"))

    import sqlite3

    import app.services.blueprint_candidate_registry as registry
    from app.config import get_db_path
    from app.services.blueprint_candidate_truth import get_candidate_field_current

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    registry.ensure_candidate_registry_tables(conn)
    registry.seed_default_candidate_registry(conn)
    candidate = next(item for item in registry.export_live_candidate_registry(conn) if str(item.get("symbol") or "").upper() == "IWDP")
    sleeve_key = str(candidate.get("sleeve_key") or "")

    monkeypatch.setattr(
        registry,
        "fetch_candidate_docs",
        lambda symbol, use_fixtures=True: {
            "status": "success",
            "verified": True,
            "factsheet": {
                "status": "success",
                "doc_url": "https://issuer.example/factsheet.pdf",
                "retrieved_at": "2026-04-07T00:00:00+00:00",
            },
            "extracted": {
                "launch_date": "2007-11-20",
            },
        },
    )
    monkeypatch.setattr(registry, "get_etf_factsheet_history_summary", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_preferred_market_history_summary", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_preferred_latest_market_data", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_latest_etf_fetch_status", lambda symbol, conn: {"status": "success", "entries": []})
    monkeypatch.setattr(registry, "get_etf_holdings_profile", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_latest_successful_etf_ingest_at", lambda symbol, conn: None)
    monkeypatch.setattr(
        registry,
        "build_sg_tax_truth",
        lambda **kwargs: {
            "estate_risk_posture": "bounded",
            "withholding_tax_posture": "bounded",
        },
    )

    result = registry.refresh_registry_candidate_truth(conn, symbol="IWDP")
    current = get_candidate_field_current(conn, candidate_symbol="IWDP", sleeve_key=sleeve_key)

    assert result["updated"] is True
    assert current["benchmark_name"]["resolved_value"] == "Global REIT proxy"
    assert current["benchmark_name"]["source_name"] == "benchmark_registry"
    assert current["launch_date"]["resolved_value"] == "2007-11-20"
    assert current["launch_date"]["source_name"] == "issuer_doc_parser"


def test_refresh_registry_candidate_truth_uses_seed_values_when_doc_fields_are_null(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "candidate_truth.sqlite3"))

    import sqlite3

    import app.services.blueprint_candidate_registry as registry
    from app.config import get_db_path
    from app.services.blueprint_candidate_truth import get_candidate_field_current

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    registry.ensure_candidate_registry_tables(conn)
    registry.seed_default_candidate_registry(conn)
    conn.execute(
        """
        UPDATE blueprint_canonical_instruments
        SET extra_json = ?
        WHERE symbol = 'IB01'
        """,
        (
            json.dumps(
                {
                    "aum_usd": 25_229_390_000.0,
                    "tracking_difference_1y": 0.0,
                    "primary_listing_exchange": "LSE",
                    "primary_trading_currency": "USD",
                }
            ),
        ),
    )
    conn.commit()
    candidate = next(item for item in registry.export_live_candidate_registry(conn) if str(item.get("symbol") or "").upper() == "IB01")
    sleeve_key = str(candidate.get("sleeve_key") or "")

    monkeypatch.setattr(
        registry,
        "fetch_candidate_docs",
        lambda symbol, use_fixtures=True: {
            "status": "success",
            "verified": False,
            "factsheet": {
                "status": "success",
                "doc_url": "https://issuer.example/factsheet.pdf",
                "retrieved_at": "2026-04-07T00:00:00+00:00",
            },
            "extracted": {
                "aum_usd": None,
                "tracking_difference_1y": None,
                "primary_listing_exchange": None,
                "primary_trading_currency": None,
            },
        },
    )
    monkeypatch.setattr(registry, "get_etf_factsheet_history_summary", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_preferred_market_history_summary", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_preferred_latest_market_data", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_latest_etf_fetch_status", lambda symbol, conn: {"status": "success", "entries": []})
    monkeypatch.setattr(registry, "get_etf_holdings_profile", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_latest_successful_etf_ingest_at", lambda symbol, conn: None)
    monkeypatch.setattr(
        registry,
        "build_sg_tax_truth",
        lambda **kwargs: {
            "estate_risk_posture": "bounded",
            "withholding_tax_posture": "bounded",
        },
    )

    registry.refresh_registry_candidate_truth(conn, symbol="IB01")
    current = get_candidate_field_current(conn, candidate_symbol="IB01", sleeve_key=sleeve_key)

    assert current["aum"]["resolved_value"] == 25_229_390_000.0
    assert current["aum"]["source_name"] == "candidate_registry"
    assert current["tracking_difference_1y"]["resolved_value"] == 0.0
    assert current["primary_listing_exchange"]["resolved_value"] == "LSE"
    assert current["primary_trading_currency"]["resolved_value"] == "USD"


def test_refresh_registry_candidate_truth_normalizes_small_percent_unit_tracking_difference(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "candidate_truth_tracking.sqlite3"))

    import sqlite3

    import app.services.blueprint_candidate_registry as registry
    from app.config import get_db_path
    from app.services.blueprint_candidate_truth import get_candidate_field_current

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    registry.ensure_candidate_registry_tables(conn)
    registry.seed_default_candidate_registry(conn)
    candidate = next(item for item in registry.export_live_candidate_registry(conn) if str(item.get("symbol") or "").upper() == "IWDA")
    sleeve_key = str(candidate.get("sleeve_key") or "")

    monkeypatch.setattr(
        registry,
        "fetch_candidate_docs",
        lambda symbol, use_fixtures=True: {
            "status": "success",
            "verified": True,
            "factsheet": {
                "status": "success",
                "doc_url": "https://issuer.example/factsheet.pdf",
                "retrieved_at": "2026-04-07T00:00:00+00:00",
            },
            "extracted": {
                "tracking_difference_1y": -0.05,
            },
        },
    )
    monkeypatch.setattr(registry, "get_etf_factsheet_history_summary", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_preferred_market_history_summary", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_preferred_latest_market_data", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_latest_etf_fetch_status", lambda symbol, conn: {"status": "success", "entries": []})
    monkeypatch.setattr(registry, "get_etf_holdings_profile", lambda symbol, conn: None)
    monkeypatch.setattr(registry, "get_latest_successful_etf_ingest_at", lambda symbol, conn: None)
    monkeypatch.setattr(
        registry,
        "build_sg_tax_truth",
        lambda **kwargs: {
            "estate_risk_posture": "bounded",
            "withholding_tax_posture": "bounded",
        },
    )

    registry.refresh_registry_candidate_truth(conn, symbol="IWDA")
    current = get_candidate_field_current(conn, candidate_symbol="IWDA", sleeve_key=sleeve_key)

    assert current["tracking_difference_1y"]["resolved_value"] == pytest.approx(-0.0005)


def test_interpretation_marks_missing_movement_as_unknown() -> None:
    from app.v2.core.domain_objects import InstrumentTruth, MarketSeriesTruth
    from app.v2.core.interpretation_engine import interpret

    signal, card = interpret(
        InstrumentTruth(
            instrument_id="instrument_acwi",
            symbol="ACWI",
            name="ACWI",
            asset_class="equity",
        ),
        MarketSeriesTruth(
            series_id="market:acwi",
            label="ACWI",
            frequency="daily",
            units="price",
            points=[],
            evidence=[],
        ),
    )

    assert signal.direction == "unknown"
    assert signal.magnitude == "unknown"
    assert signal.metadata["movement_state"] == "input_constrained"
    assert "not established" in signal.summary
    assert card.signals[0].direction == "unknown"


def test_forecast_provider_candidates_prefer_ready_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.v2.forecasting.capabilities import ForecastRequest
    import app.v2.forecasting.service as forecast_service

    monkeypatch.setattr(forecast_service, "provider_available", lambda provider: True)
    monkeypatch.setattr(
        forecast_service,
        "readiness_rows",
        lambda: [
            {"provider": "chronos", "ready": True},
            {"provider": "timesfm", "ready": False},
        ],
    )

    request = ForecastRequest(
        request_id="forecast_request_demo",
        object_type="market_series",
        object_id="market_acwi",
        series_family="benchmark_proxy",
        series_id="ACWI",
        horizon=5,
        frequency="daily",
        covariates={"symbol": "ACWI"},
        history=[100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
        timestamps=[
            "2026-03-30T00:00:00+00:00",
            "2026-03-31T00:00:00+00:00",
            "2026-04-01T00:00:00+00:00",
            "2026-04-02T00:00:00+00:00",
            "2026-04-03T00:00:00+00:00",
            "2026-04-04T00:00:00+00:00",
        ],
        requested_at="2026-04-06T00:00:00+00:00",
    )

    assert forecast_service._provider_candidates(request, surface_name="daily_brief") == ["chronos"]


def test_daily_brief_quote_family_remains_budget_critical() -> None:
    from app.services.provider_refresh import _should_skip_for_budget

    assert _should_skip_for_budget("polygon", "quote_latest", "daily_brief", "critical_only") == (False, None)


def test_blueprint_etf_enrichment_families_remain_budget_critical() -> None:
    from app.services.provider_refresh import _should_skip_for_budget

    assert _should_skip_for_budget("alpha_vantage", "etf_profile", "blueprint", "critical_only") == (False, None)
    assert _should_skip_for_budget("sec_edgar", "etf_holdings", "blueprint", "critical_only") == (False, None)


def test_provider_refresh_normalize_payload_allows_dict_value_for_reference_meta() -> None:
    from app.services.provider_refresh import _normalize_payload

    payload = _normalize_payload(
        "finnhub",
        "reference_meta",
        "IWDP",
        {
            "value": {"exchange": "LSE", "currency": "USD"},
            "observed_at": "2026-04-07T00:00:00+00:00",
        },
    )

    assert payload["value"] == {"exchange": "LSE", "currency": "USD"}
    assert payload["price"] is None


def test_runtime_scheduler_defaults_are_more_active_for_budget_critical_surfaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.runtime.service as runtime_service

    monkeypatch.delenv("IA_RUNTIME_REFRESH_DAILY_BRIEF_SECONDS", raising=False)
    monkeypatch.delenv("IA_RUNTIME_REFRESH_DASHBOARD_SECONDS", raising=False)
    monkeypatch.delenv("IA_RUNTIME_REFRESH_BLUEPRINT_SECONDS", raising=False)
    monkeypatch.delenv("IA_RUNTIME_SCHEDULER_STARTUP_DELAY_SECONDS", raising=False)

    assert runtime_service._surface_interval("daily_brief") == 300
    assert runtime_service._surface_interval("dashboard") == 300
    assert runtime_service._surface_interval("blueprint") == 21600
    assert runtime_service._startup_delay_seconds() == 10


def test_provider_activation_counts_public_snapshot_family_as_active(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "provider_activation.sqlite3"))

    import sqlite3

    from app.services.provider_activation import build_provider_activation_report
    from app.services.public_upstream_snapshots import put_public_upstream_snapshot

    conn = sqlite3.connect(tmp_path / "provider_activation.sqlite3")
    conn.row_factory = sqlite3.Row
    put_public_upstream_snapshot(
        conn,
        provider_key="frankfurter",
        family_name="usd_strength_fallback",
        surface_usage=["daily_brief"],
        payload={"provider_key": "frankfurter", "status": "ok"},
        source_url="https://api.frankfurter.app/latest",
        observed_at="2026-04-06",
        freshness_state="fresh_full_rebuild",
        error_state=None,
    )

    report = build_provider_activation_report(conn)
    conn.close()

    frankfurter = next(item for item in report["providers"] if item["provider_name"] == "frankfurter")
    coverage = report["source_family_coverage"]["usd_strength_fallback"]

    assert "usd_strength_fallback" in frankfurter["active_families"]
    assert coverage["coverage_state"] == "healthy"


def test_registry_refresh_merges_live_doc_without_losing_fixture_aum() -> None:
    from app.services.blueprint_candidate_registry import _merge_doc_results

    fixture = {
        "verified": True,
        "factsheet": {"doc_url": "/tmp/xcha_fixture.txt"},
        "extracted": {
            "aum_usd": 2_200_000_000.0,
            "launch_date": "2010-06-24",
            "primary_trading_currency": "USD",
        },
    }
    live = {
        "verified": True,
        "factsheet": {"doc_url": "https://example.com/xcha.pdf"},
        "extracted": {
            "aum_usd": None,
            "launch_date": "2010-06-24",
            "primary_trading_currency": "USD",
            "factsheet_date": "2026-02-27",
        },
    }

    merged = _merge_doc_results(fixture, live)

    assert merged is not None
    assert merged["factsheet"]["doc_url"] == "https://example.com/xcha.pdf"
    assert merged["extracted"]["aum_usd"] == pytest.approx(2_200_000_000.0)
    assert merged["extracted"]["factsheet_date"] == "2026-02-27"
