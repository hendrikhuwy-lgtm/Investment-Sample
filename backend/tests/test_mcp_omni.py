from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.config import Settings
from app.models.types import MCPItem, SourceRecord
from app.services import real_email_brief as reb
from app.services.ingest_mcp import MCPIngestionResult, ingest_mcp_omni
from app.services.real_email_brief import OmniBriefError


def _sample_series() -> tuple[dict, list, bool]:
    now = datetime.now(UTC)
    series = {
        "DGS10": {
            "label": "US 10Y Treasury Yield",
            "latest_date": "2026-02-12",
            "latest_value": 4.09,
            "prior_date": "2026-02-05",
            "prior_value": 4.21,
            "change_5obs": -0.12,
            "sparkline": "▁▂▃▄",
            "range_bar": "████░░░░",
            "points": [4.2, 4.1, 4.0, 4.09],
        },
        "T10YIE": {
            "label": "US 10Y Breakeven Inflation",
            "latest_date": "2026-02-13",
            "latest_value": 2.27,
            "prior_date": "2026-02-06",
            "prior_value": 2.34,
            "change_5obs": -0.07,
            "sparkline": "▁▂▂▄",
            "range_bar": "███░░░░░",
            "points": [2.34, 2.31, 2.29, 2.27],
        },
        "SP500": {
            "label": "S&P 500 Index",
            "latest_date": "2026-02-13",
            "latest_value": 6836.17,
            "prior_date": "2026-02-06",
            "prior_value": 6932.30,
            "change_5obs": -96.13,
            "sparkline": "█▇▆▅",
            "range_bar": "█████░░░",
            "points": [6932, 6890, 6860, 6836],
        },
        "VIXCLS": {
            "label": "CBOE VIX",
            "latest_date": "2026-02-12",
            "latest_value": 20.82,
            "prior_date": "2026-02-05",
            "prior_value": 21.77,
            "change_5obs": -0.95,
            "sparkline": "▅▆▅▄",
            "range_bar": "██████░░",
            "points": [21.77, 21.2, 20.9, 20.82],
        },
        "BAMLH0A0HYM2": {
            "label": "US High Yield OAS",
            "latest_date": "2026-02-12",
            "latest_value": 2.92,
            "prior_date": "2026-02-05",
            "prior_value": 2.97,
            "change_5obs": -0.05,
            "sparkline": "▃▂▂▁",
            "range_bar": "██████░░",
            "points": [2.97, 2.95, 2.93, 2.92],
        },
    }
    citations = [
        reb._citation(  # noqa: SLF001
            url=f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={code}",
            source_id=f"fred_{code.lower()}",
            retrieved_at=now,
            importance=f"Official FRED series {code}",
            cached=False,
        )
        for code in ["DGS10", "T10YIE", "SP500", "VIXCLS", "BAMLH0A0HYM2"]
    ]
    return series, citations, False


def _sample_mcp_result() -> MCPIngestionResult:
    snapshot = {
        "server_id": "example/server",
        "server_name": "Example Server",
        "publisher": "example",
        "endpoint_url": "https://example.com/mcp",
        "endpoint_type": "http",
        "retrieved_at": datetime.now(UTC),
        "cached": False,
        "capability": {
            "handshake_ok": True,
            "tools_count": 1,
            "resources_count": 1,
            "auth_used": False,
            "errors": [],
        },
        "items": [
            MCPItem(
                server_id="example/server",
                item_type="resource",
                uri="https://example.com/resource",
                title="Risk outlook",
                content="Macro risk and liquidity stress commentary",
                metadata={},
            )
        ],
        "raw_hash": "abc123",
        "error": None,
    }
    from app.models.types import MCPServerSnapshot

    return MCPIngestionResult(
        source_records=[
            SourceRecord(
                source_id="mcp_example/server",
                url="https://example.com/mcp",
                publisher="example",
                retrieved_at=datetime.now(UTC),
                topic="mcp_omni",
                credibility_tier="secondary",
                raw_hash="abc123",
                source_type="mcp",
            )
        ],
        snapshots=[MCPServerSnapshot(**snapshot)],
        errors=[],
        total_servers=1,
        connected_servers=1,
        cached_used=False,
    )


def _sample_web_records() -> list[SourceRecord]:
    return [
        SourceRecord(
            source_id="fred_dgs10",
            url="https://fred.stlouisfed.org/series/DGS10",
            publisher="FRED",
            retrieved_at=datetime.now(UTC),
            topic="rates",
            credibility_tier="primary",
            raw_hash="hash1",
            source_type="web",
        )
    ]


def test_mcp_ingestion_resilience(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "app.services.ingest_mcp.parse_registry_snapshot",
        lambda _path: [
            {"server": {"name": "ok/server", "remotes": [{"type": "streamable-http", "url": "https://ok.example/mcp"}]}},
            {"server": {"name": "bad/server", "remotes": [{"type": "streamable-http", "url": "https://fail.example/mcp"}]}},
        ],
    )

    class DummyClient:
        def __init__(self, endpoint: str, auth_token: str | None = None) -> None:
            self.endpoint = endpoint
            self.auth_token = auth_token

    monkeypatch.setattr(
        "app.services.ingest_mcp.connect_server",
        lambda endpoint, auth, settings: DummyClient(endpoint["url"], auth),
    )
    monkeypatch.setattr(
        "app.services.ingest_mcp.evaluate_source",
        lambda **kwargs: type("Policy", (), {"allowed": True, "reason": ""})(),
    )

    def fake_handshake(client):
        if "fail" in client.endpoint:
            return {
                "handshake_ok": False,
                "tools_count": 0,
                "resources_count": 0,
                "auth_used": False,
                "errors": ["connection failed"],
                "handshake": {},
                "tools": [],
                "resources": [],
            }
        return {
            "handshake_ok": True,
            "tools_count": 0,
            "resources_count": 0,
            "auth_used": False,
            "errors": [],
            "handshake": {},
            "tools": [],
            "resources": [],
        }

    monkeypatch.setattr("app.services.ingest_mcp.handshake_and_capabilities", fake_handshake)
    monkeypatch.setattr(
        "app.services.ingest_mcp.fetch_server_snapshot",
        lambda client, capabilities: {"items": [], "errors": capabilities.get("errors", [])},
    )

    settings = Settings(db_path=str(tmp_path / "test.sqlite"), mcp_priority_mode="all")
    result = ingest_mcp_omni(settings=settings)

    assert result.total_servers == 2
    assert result.connected_servers == 1
    assert len(result.source_records) == 2
    assert any(error["server_id"] == "bad/server" for error in result.errors)


def test_citation_gate_blocks_uncited(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(reb, "_refresh_live_cache", lambda settings: (True, "ok"))
    monkeypatch.setattr(reb, "_load_series_from_cache", lambda force_cache_only: _sample_series())
    monkeypatch.setattr(reb, "ingest_mcp_omni", lambda settings=None, **kwargs: _sample_mcp_result())
    monkeypatch.setattr(reb, "fetch_web_sources", lambda settings: _sample_web_records())

    with pytest.raises(OmniBriefError):
        reb.generate_mcp_omni_email_brief(
            settings=Settings(db_path=str(tmp_path / "test.sqlite")),
            force_cache_only=True,
            inject_uncited_section=True,
        )


def test_cache_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(reb, "_refresh_live_cache", lambda settings: (False, "failed"))
    monkeypatch.setattr(reb, "_load_series_from_cache", lambda force_cache_only: _sample_series()[:2] + (True,))
    monkeypatch.setattr(reb, "ingest_mcp_omni", lambda settings=None, **kwargs: _sample_mcp_result())
    monkeypatch.setattr(reb, "fetch_web_sources", lambda settings: _sample_web_records())

    result = reb.generate_mcp_omni_email_brief(
        settings=Settings(db_path=str(tmp_path / "test.sqlite")),
        force_cache_only=True,
    )

    assert result["cached_used"] is True


def test_brief_output_files_exist(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(reb, "_refresh_live_cache", lambda settings: (True, "ok"))
    monkeypatch.setattr(reb, "_load_series_from_cache", lambda force_cache_only: _sample_series())
    monkeypatch.setattr(reb, "ingest_mcp_omni", lambda settings=None, **kwargs: _sample_mcp_result())
    monkeypatch.setattr(reb, "fetch_web_sources", lambda settings: _sample_web_records())

    result = reb.generate_mcp_omni_email_brief(
        settings=Settings(db_path=str(tmp_path / "test.sqlite")),
        force_cache_only=True,
    )

    assert Path(result["md_path"]).exists()
    assert Path(result["html_path"]).exists()
    assert Path(result["pdf_path"]).exists()


def test_refresh_report_distinguishes_source_lag_from_refresh_failure() -> None:
    now = datetime.now(UTC)
    lagged_row = {
        "metric": "US 10Y Treasury Yield",
        "citation": reb._citation(  # noqa: SLF001
            url="https://fred.stlouisfed.org/series/DGS10",
            source_id="fred_dgs10",
            retrieved_at=now,
            importance="FRED DGS10",
            observed_at="2026-03-08",
            lag_days=1,
            lag_class="lagged",
            lag_cause="expected_publication_lag",
        ),
    }
    failed_row = {
        "metric": "VIX",
        "citation": reb._citation(  # noqa: SLF001
            url="https://fred.stlouisfed.org/series/VIXCLS",
            source_id="fred_vixcls",
            retrieved_at=now,
            importance="FRED VIXCLS",
            observed_at="2026-03-07",
            lag_days=3,
            lag_class="stale",
            lag_cause="unexpected_ingestion_lag",
        ),
    }

    report = reb._build_refresh_report(  # noqa: SLF001
        brief_run_id="run_test",
        run_started_at=now - timedelta(minutes=5),
        run_finished_at=now,
        refresh_attempted=True,
        refresh_ok=False,
        refresh_msg="upstream failed",
        graph_rows=[lagged_row, failed_row],
        volume_rows=[],
        sti_proxy_row=None,
        vea_proxy_row=None,
        data_recency_summary=reb._build_data_recency_summary([lagged_row, failed_row]),  # noqa: SLF001
        provider_refresh=None,
    )

    reason_codes = {item["metric"]: item["reason_code"] for item in report["series_reports"]}
    assert reason_codes["US 10Y Treasury Yield"] == "latest_available_source_lag"
    assert reason_codes["VIX"] == "refresh_failed_used_cache"
    assert report["failed_refresh_series"]
