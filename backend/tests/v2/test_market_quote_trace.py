from __future__ import annotations

import logging

from app.services.provider_adapters import ProviderAdapterError
from app.v2.core.domain_objects import MarketSeriesTruth, utc_now_iso


def test_market_price_adapter_logs_missing_provider_keys(monkeypatch, caplog) -> None:
    import app.v2.sources.market_price_adapter as adapter

    monkeypatch.setenv("IA_TRACE_MARKET_QUOTES", "1")
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
    monkeypatch.setattr(
        adapter,
        "fetch_provider_data",
        lambda provider_name, endpoint_family, identifier: (
            (_ for _ in ()).throw(
                ProviderAdapterError(provider_name, endpoint_family, "no public quote returned", error_class="empty_response")
            )
        ),
    )

    with caplog.at_level(logging.INFO):
        payload = adapter.fetch("ACWI")

    assert payload["ticker"] == "ACWI"
    assert payload["price"] is None
    assert "provider_configured_but_key_missing" in caplog.text
    assert "no_live_provider_and_no_cached_snapshot" in caplog.text


def test_daily_brief_logs_routed_payload_and_uses_quote_fallback_when_thin(monkeypatch, caplog) -> None:
    import app.v2.surfaces.daily_brief.contract_builder as brief_builder

    monkeypatch.setenv("IA_TRACE_MARKET_QUOTES", "1")

    def _fake_routed_market_series(**kwargs):
        symbol = kwargs["identifier"]
        return (
            MarketSeriesTruth(
                series_id=f"benchmark_proxy:{symbol.lower()}",
                label=symbol,
                frequency="daily",
                units="price",
                points=[],
                evidence=[],
                as_of=utc_now_iso(),
            ),
            {
                "provider_name": None,
                "cache_status": "unavailable",
                "error_state": "polygon:not_configured",
                "fallback_used": False,
            },
        )

    monkeypatch.setattr(brief_builder, "load_routed_market_series", _fake_routed_market_series)
    monkeypatch.setattr(
        brief_builder,
        "get_market_adapter",
        lambda: type(
            "_Adapter",
            (),
            {
                "fetch": staticmethod(
                    lambda symbol: {
                        "ticker": symbol,
                        "price": 100.0,
                        "previous_close": 99.0,
                        "change_pct_1d": 1.01,
                        "provider_name": "finnhub",
                        "retrieval_path": "direct_live",
                    }
                )
            },
        )(),
    )

    with caplog.at_level(logging.INFO):
        truths = brief_builder._load_market_truths()

    assert truths
    assert "daily_brief.market_symbol.routed_result" in caplog.text
    assert '"fallback_to_quote_adapter": true' in caplog.text.lower()
    assert "routed_unavailable" in caplog.text
    assert "daily_brief.market_symbol.quote_fallback_used" in caplog.text


def test_fetch_routed_family_logs_provider_failure_chain(monkeypatch, caplog) -> None:
    import sqlite3

    from app.services import provider_refresh

    monkeypatch.setenv("IA_TRACE_MARKET_QUOTES", "1")
    for key in ("POLYGON_API_KEY", "TIINGO_API_KEY", "EODHD_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(provider_refresh, "provider_supports_family", lambda provider_name, endpoint_family: True)

    def _fake_fetch_with_cache(conn, **kwargs):
        raise ProviderAdapterError(kwargs["provider_name"], kwargs["endpoint_family"], "missing key", error_class="not_configured")

    monkeypatch.setattr(provider_refresh, "fetch_with_cache", _fake_fetch_with_cache)

    with caplog.at_level(logging.INFO):
        payload = provider_refresh.fetch_routed_family(
            sqlite3.connect(":memory:"),
            surface_name="daily_brief",
            endpoint_family="benchmark_proxy",
            identifier="ACWI",
            triggered_by_job="test_trace",
        )

    assert payload["cache_status"] == "unavailable"
    assert payload["error_state"] == "twelve_data:not_configured"
    assert "provider_refresh.provider.evaluate" in caplog.text
    assert "provider_refresh.provider.failed" in caplog.text
    assert "provider_refresh.fetch_routed_family.unavailable" in caplog.text
