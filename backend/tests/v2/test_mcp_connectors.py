from __future__ import annotations

from fastapi.testclient import TestClient


def test_connector_catalog_contains_finnhub_and_nasdaq() -> None:
    from app.services.mcp_connectors import load_connector_candidates

    rows = load_connector_candidates()
    names = {str(row.get("provider_name") or "").strip() for row in rows}
    assert "finnhub" in names
    assert "nasdaq_data_link" in names


def test_news_adapter_uses_provider_backed_finnhub(monkeypatch) -> None:
    from app.v2.sources import news_adapter

    monkeypatch.setenv("FINNHUB_API_KEY", "test-key")

    def _fake_fetch(provider_name: str, endpoint_family: str, identifier: str) -> dict:
        assert provider_name == "finnhub"
        assert endpoint_family == "news_general"
        assert identifier.startswith("general?limit=")
        return {
            "value": [
                {
                    "headline": "Test headline",
                    "source": "Finnhub",
                    "published_utc": "2026-04-03T00:00:00+00:00",
                    "url": "https://example.test/news",
                }
            ]
        }

    monkeypatch.setattr(news_adapter, "fetch_provider_data", _fake_fetch)
    items = news_adapter.fetch(limit=1)
    assert len(items) == 1
    assert items[0]["headline"] == "Test headline"


def test_v2_mcp_adapter_fetches_provider_backed_payload(monkeypatch) -> None:
    from app.v2.app import app
    import app.services.mcp_connectors as mcp_connectors

    monkeypatch.setenv("FINNHUB_API_KEY", "test-key")

    def _fake_fetch(provider_name: str, endpoint_family: str, identifier: str) -> dict:
        assert provider_name == "finnhub"
        assert endpoint_family == "quote_latest"
        assert identifier == "AAPL"
        return {
            "value": 255.92,
            "observed_at": "2026-04-03T00:00:00+00:00",
            "source_ref": "finnhub:quote",
        }

    monkeypatch.setattr(mcp_connectors, "fetch_provider_data", _fake_fetch)

    with TestClient(app) as client:
        response = client.post(
            "/api/v2/mcp/adapter/finnhub-market-data",
            json={
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "fetch",
                    "arguments": {
                        "endpoint_family": "quote_latest",
                        "identifier": "AAPL",
                    },
                },
            },
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["result"]["ok"] is True
    assert payload["result"]["provider_name"] == "finnhub"
    assert payload["result"]["payload"]["source_ref"] == "finnhub:quote"


def test_v2_mcp_connectors_run_reports_live_ok(monkeypatch) -> None:
    from app.v2.app import app
    import app.services.mcp_connectors as mcp_connectors

    monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
    monkeypatch.setenv("NASDAQ_DATA_LINK_API_KEY", "test-key")

    def _fake_fetch(provider_name: str, endpoint_family: str, identifier: str) -> dict:
        if provider_name == "finnhub":
            return {
                "value": [{"headline": "Markets higher"}],
                "observed_at": "2026-04-03T00:00:00+00:00",
                "source_ref": "finnhub:news",
            }
        if provider_name == "nasdaq_data_link":
            return {
                "value": ["2023-01-03", "SPY", 931982116.0],
                "observed_at": "2023-01-03",
                "source_ref": "nasdaq_data_link:datatable:ETFG/FUND",
            }
        raise AssertionError(provider_name)

    monkeypatch.setattr(mcp_connectors, "fetch_provider_data", _fake_fetch)

    with TestClient(app) as client:
        response = client.get("/api/v2/mcp/connectors/run")
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] >= 2
    assert payload["live_ok_count"] >= 2
