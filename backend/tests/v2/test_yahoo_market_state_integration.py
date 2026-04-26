from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def test_yahoo_provider_is_narrowly_scoped_to_market_families() -> None:
    from app.services.provider_registry import provider_supports_family, providers_for_family, routed_provider_candidates

    assert provider_supports_family("yahoo_finance", "market_close") is True
    assert provider_supports_family("yahoo_finance", "quote_latest") is True
    assert provider_supports_family("yahoo_finance", "benchmark_proxy") is True

    assert provider_supports_family("yahoo_finance", "reference_meta") is False
    assert provider_supports_family("yahoo_finance", "fundamentals") is False
    assert provider_supports_family("yahoo_finance", "fx") is False
    assert provider_supports_family("yahoo_finance", "fx_reference") is False
    assert provider_supports_family("yahoo_finance", "usd_strength_fallback") is False

    assert "yahoo_finance" in providers_for_family("market_close")
    assert "yahoo_finance" in providers_for_family("quote_latest")
    assert "yahoo_finance" in providers_for_family("benchmark_proxy")
    assert "yahoo_finance" in routed_provider_candidates("quote_latest", identifier="DXY")
    assert routed_provider_candidates("market_close", identifier="^GSPC")[0] == "yahoo_finance"


def test_yahoo_market_close_snapshot_uses_last_completed_china_day(monkeypatch) -> None:
    import pandas as pd

    from app.services import yahoo_finance

    frame = pd.DataFrame(
        {
            "Open": [6587.66, 6601.93, 6500.0],
            "High": [6618.12, 6601.93, 6510.0],
            "Low": [6579.72, 6534.55, 6490.0],
            "Close": [6611.83, 6550.29, 6510.0],
            "Volume": [3906440000, 5066760000, 1000],
        },
        index=pd.DatetimeIndex(
            [
                "2026-04-06 00:00:00-04:00",
                "2026-04-07 00:00:00-04:00",
                "2026-04-08 00:00:00-04:00",
            ]
        ),
    )

    monkeypatch.setattr(
        yahoo_finance,
        "_history_frame",
        lambda identifier, *, period, interval, auto_adjust: (object(), frame),
    )
    monkeypatch.setattr(yahoo_finance, "_ticker_currency", lambda ticker_obj: "USD")

    payload = yahoo_finance.fetch_yahoo_market_close_snapshot(
        "^GSPC",
        now=datetime(2026, 4, 8, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert payload["close_date"] == "2026-04-07"
    assert round(float(payload["price"]), 2) == 6550.29
    assert round(float(payload["previous_close"]), 2) == 6611.83
    assert payload["source_type"] == "market_close"


def test_market_price_adapter_treats_yahoo_as_no_key_provider(monkeypatch) -> None:
    import app.v2.sources.market_price_adapter as adapter

    for key in (
        "FINNHUB_API_KEY",
        "ALPHA_VANTAGE_API_KEY",
        "POLYGON_API_KEY",
        "EODHD_API_KEY",
        "TIINGO_API_KEY",
        "TWELVE_DATA_API_KEY",
        "FMP_API_KEY",
    ):
        monkeypatch.delenv(key, raising=False)

    configured = adapter._configured_quote_providers()

    assert "yahoo_finance" in configured


def test_market_price_adapter_uses_yahoo_without_api_key(monkeypatch) -> None:
    import app.v2.sources.market_price_adapter as adapter

    for key in (
        "FINNHUB_API_KEY",
        "ALPHA_VANTAGE_API_KEY",
        "POLYGON_API_KEY",
        "EODHD_API_KEY",
        "TIINGO_API_KEY",
        "TWELVE_DATA_API_KEY",
        "FMP_API_KEY",
    ):
        monkeypatch.delenv(key, raising=False)

    monkeypatch.setattr(adapter, "_freshest_snapshot_for_ticker", lambda ticker: None)
    monkeypatch.setattr(adapter, "routed_provider_candidates", lambda family, identifier=None: ["polygon", "yahoo_finance"])
    monkeypatch.setattr(adapter, "provider_support_status", lambda provider, family, identifier=None: (True, None))
    monkeypatch.setattr(
        adapter,
        "fetch_provider_data",
        lambda provider, family, identifier: {
            "price": 658.93,
            "previous_close": 655.85,
            "change_pct_1d": 0.47,
            "observed_at": "2026-04-07T00:00:00+00:00",
            "provider_symbol": "SPY",
        } if provider == "yahoo_finance" else (_ for _ in ()).throw(AssertionError("keyed provider should be skipped")),
    )

    payload = adapter.fetch("SPY", surface_name="daily_brief")

    assert payload["provider_name"] == "yahoo_finance"
    assert payload["price"] == 658.93
    assert payload["retrieval_path"] == "direct_live"
    assert payload["provider_execution"]["live_or_cache"] == "live"
    assert payload["provider_execution"]["source_family"] == "quote_latest"


def test_market_price_adapter_prefers_yahoo_direct_dxy_before_frankfurter_fallback(monkeypatch) -> None:
    import app.v2.sources.market_price_adapter as adapter

    for key in (
        "FINNHUB_API_KEY",
        "ALPHA_VANTAGE_API_KEY",
        "POLYGON_API_KEY",
        "EODHD_API_KEY",
        "TIINGO_API_KEY",
        "TWELVE_DATA_API_KEY",
        "FMP_API_KEY",
    ):
        monkeypatch.delenv(key, raising=False)

    calls: list[tuple[str, str, str]] = []
    monkeypatch.setattr(adapter, "_freshest_snapshot_for_ticker", lambda ticker: None)
    monkeypatch.setattr(
        adapter,
        "routed_provider_candidates",
        lambda family, identifier=None: ["twelve_data", "yahoo_finance"] if family == "quote_latest" else ["frankfurter"],
    )
    monkeypatch.setattr(adapter, "provider_support_status", lambda provider, family, identifier=None: (True, None))

    def _fake_fetch(provider: str, family: str, identifier: str) -> dict[str, object]:
        calls.append((provider, family, identifier))
        if provider == "yahoo_finance" and family == "quote_latest":
            return {
                "price": 98.68,
                "previous_close": 98.67,
                "change_pct_1d": 0.01,
                "observed_at": "2026-04-07T00:00:00+00:00",
                "provider_symbol": "DX-Y.NYB",
            }
        raise AssertionError("Frankfurter fallback should not be used when Yahoo direct quote succeeds")

    monkeypatch.setattr(adapter, "fetch_provider_data", _fake_fetch)

    payload = adapter.fetch("DXY", surface_name="daily_brief")

    assert payload["provider_name"] == "yahoo_finance"
    assert payload["retrieval_path"] == "direct_live"
    assert payload["provider_execution"]["source_family"] == "quote_latest"
    assert calls == [("yahoo_finance", "quote_latest", "DXY")]


def test_yahoo_benchmark_proxy_normalizes_history_payload(monkeypatch) -> None:
    from app.services import provider_adapters, yahoo_finance

    monkeypatch.setattr(
        yahoo_finance,
        "fetch_yahoo_history_series",
        lambda identifier, period="10y": {
            "status": "success",
            "identifier": identifier,
            "resolved_symbol": "SPY",
            "price": 658.93,
            "close": 658.93,
            "previous_close": 655.85,
            "currency": "USD",
            "observed_at": "2026-04-07T00:00:00+00:00",
            "change_pct_1d": 0.47,
            "series": [
                {"date": "2026-04-06", "close": 655.85},
                {"date": "2026-04-07", "close": 658.93},
            ],
            "source_ref": "https://finance.yahoo.com/quote/SPY/history",
            "source_label": "Yahoo Finance market history",
            "source_kind": "public_market_history",
            "semantic_strength": "direct",
            "provenance_strength": "public_market_data",
            "usable_truth": True,
        },
    )

    payload = provider_adapters.fetch_provider_data("yahoo_finance", "benchmark_proxy", "SPY")

    assert payload["price"] == 658.93
    assert payload["change_pct_1d"] == 0.47
    assert len(payload["series"]) == 2
    assert payload["provider_symbol"] == "SPY"


def test_yahoo_market_close_normalizes_close_payload(monkeypatch) -> None:
    from app.services import provider_adapters, yahoo_finance

    monkeypatch.setattr(
        yahoo_finance,
        "fetch_yahoo_market_close_snapshot",
        lambda identifier: {
            "status": "success",
            "identifier": identifier,
            "resolved_symbol": "^GSPC",
            "price": 6550.29,
            "close": 6550.29,
            "previous_close": 6611.83,
            "currency": "USD",
            "observed_at": "2026-04-07T00:00:00+00:00",
            "close_date": "2026-04-07",
            "change_pct_1d": -0.99,
            "source_ref": "https://finance.yahoo.com/quote/%5EGSPC/history",
            "source_label": "Yahoo Finance market close",
            "source_kind": "public_market_close",
            "source_type": "market_close",
            "semantic_strength": "direct",
            "provenance_strength": "public_market_data",
            "usable_truth": True,
        },
    )

    payload = provider_adapters.fetch_provider_data("yahoo_finance", "market_close", "^GSPC")

    assert payload["price"] == 6550.29
    assert payload["change_pct_1d"] == -0.99
    assert payload["close_date"] == "2026-04-07"
    assert payload["source_type"] == "market_close"
    assert payload["provider_symbol"] == "^GSPC"


def test_chart_contracts_preserve_public_verified_close_for_market_close() -> None:
    from app.v2.contracts.chart_contracts import _with_provider_execution

    payload = _with_provider_execution(
        {
            "provider_name": "yahoo_finance",
            "provider_symbol": "^GSPC",
            "price": 6550.29,
            "change_pct_1d": -0.99,
            "observed_at": "2026-04-07T00:00:00+00:00",
            "fetched_at": "2026-04-08T00:05:00+00:00",
            "retrieval_path": "direct_live",
            "provenance_strength": "public_verified_close",
        },
        endpoint_family="market_close",
        identifier="^GSPC",
    )

    execution = dict(payload.get("provider_execution") or {})
    envelope = dict(payload.get("truth_envelope") or {})

    assert execution["provenance_strength"] == "public_verified_close"
    assert envelope["reference_period"] == "2026-04-07"
    assert envelope["period_clock_class"] == "daily_market_close"
