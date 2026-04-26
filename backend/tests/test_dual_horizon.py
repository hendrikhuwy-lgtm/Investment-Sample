from __future__ import annotations

import sqlite3
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.config import Settings
from app.models.types import MCPItem, SourceRecord
from app.services import real_email_brief as reb
from app.services.ingest_mcp import MCPIngestionResult
from app.services.real_email_brief import OmniBriefError


def _sample_series() -> tuple[dict, list, bool]:
    now = datetime.now(UTC)
    series = {
        "DGS10": {"latest_date": "2026-02-12", "latest_value": 4.09, "change_5obs": -0.12, "sparkline": "▁▂▃", "range_bar": "████░░", "points": [4.2, 4.1, 4.0]},
        "T10YIE": {"latest_date": "2026-02-13", "latest_value": 2.27, "change_5obs": -0.07, "sparkline": "▁▂▃", "range_bar": "████░░", "points": [2.4, 2.3, 2.2]},
        "SP500": {"latest_date": "2026-02-13", "latest_value": 6836.17, "change_5obs": -96.13, "sparkline": "▅▄▃", "range_bar": "████░░", "points": [7000, 6900, 6836]},
        "VIXCLS": {"latest_date": "2026-02-12", "latest_value": 20.82, "change_5obs": -0.95, "sparkline": "▃▄▅", "range_bar": "████░░", "points": [21.7, 21.1, 20.8]},
        "BAMLH0A0HYM2": {"latest_date": "2026-02-12", "latest_value": 2.92, "change_5obs": -0.05, "sparkline": "▅▄▃", "range_bar": "████░░", "points": [3.0, 2.95, 2.92]},
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
    return series, citations, True


def _sample_mcp_result() -> MCPIngestionResult:
    from app.models.types import MCPServerSnapshot

    snapshot = MCPServerSnapshot(
        server_id="example/server",
        server_name="Example Server",
        publisher="example",
        endpoint_url="https://example.com/mcp",
        endpoint_type="http",
        retrieved_at=datetime.now(UTC),
        cached=False,
        capability={"handshake_ok": True, "tools_count": 1, "resources_count": 1, "auth_used": False, "errors": []},
        items=[
            MCPItem(
                server_id="example/server",
                item_type="resource",
                uri="https://example.com/resource",
                title="Central bank policy risk update",
                content="Central bank and liquidity risk notes.",
                metadata={},
            )
        ],
        raw_hash="abc",
        error=None,
    )
    return MCPIngestionResult(
        source_records=[
            SourceRecord(
                source_id="mcp_example/server",
                url="https://example.com/mcp",
                publisher="example",
                retrieved_at=datetime.now(UTC),
                topic="mcp_omni",
                credibility_tier="secondary",
                raw_hash="abc",
                source_type="mcp",
            )
        ],
        snapshots=[snapshot],
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


def _sample_dual_rows() -> list[dict]:
    citation = reb._citation(  # noqa: SLF001
        url="https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS",
        source_id="fred_vixcls",
        retrieved_at=datetime.now(UTC),
        importance="Official FRED VIX",
        cached=True,
    )
    return [
        {
            "series_code": "VIXCLS",
            "metric": "CBOE VIX (2026-02-12)",
            "latest_date": "2026-02-12",
            "citation": citation,
            "long_horizon": {
                "latest": 20.82,
                "change_1y": 3.2,
                "sparkline_3y_weekly": "▁▂▃▄▅",
                "rolling_vol_5y": 0.19,
                "percentile_5y": 84.0,
                "percentile_10y": 78.0,
                "range_bar_10y": "████████░░░░",
                "regime_classification": "high percentile regime",
            },
            "short_horizon": {
                "change_5obs": -0.95,
                "momentum_20obs": 2.2,
                "sparkline_60d": "▁▂▂▃▄▅▆",
                "range_bar_60d": "█████████░░",
                "percentile_60d": 91.0,
                "direction_tag": "upward",
                "zscore_20obs": 2.4,
            },
        }
    ]


def _base_monkeypatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reb, "_refresh_live_cache", lambda settings: (False, "cached"))
    monkeypatch.setattr(reb, "_load_series_from_cache", lambda force_cache_only: _sample_series())
    monkeypatch.setattr(reb, "ingest_mcp_omni", lambda settings=None, **kwargs: _sample_mcp_result())
    monkeypatch.setattr(reb, "fetch_web_sources", lambda settings: _sample_web_records())
    monkeypatch.setattr(reb, "_build_dual_horizon_graph_rows", lambda series, citations: _sample_dual_rows())


def test_dual_horizon_present(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _base_monkeypatch(monkeypatch)

    result = reb.generate_mcp_omni_email_brief(
        settings=Settings(db_path=str(tmp_path / "dual.sqlite")),
        force_cache_only=True,
    )
    text = Path(result["md_path"]).read_text(encoding="utf-8")
    assert "Policy context (long horizon)" in text
    assert "Monitoring and opportunities (short horizon)" in text
    assert "5y pct" in text and "10y pct" in text


def test_short_alerts_generated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _base_monkeypatch(monkeypatch)
    db_path = tmp_path / "alerts.sqlite"
    settings = Settings(db_path=str(db_path))

    result = reb.generate_mcp_omni_email_brief(settings=settings, force_cache_only=True)
    assert result["alert_count"] >= 1

    conn = sqlite3.connect(str(db_path))
    try:
        count = conn.execute("SELECT COUNT(*) FROM alert_events WHERE run_id IS NOT NULL").fetchone()[0]
    finally:
        conn.close()
    assert count >= 1


def test_opportunity_requires_citations(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _base_monkeypatch(monkeypatch)
    monkeypatch.setattr(
        reb,
        "_generate_opportunity_observations",
        lambda graph_rows, tax_citations: [
            {
                "condition_observed": "uncited",
                "confirmation_data": "uncited",
                "time_horizon": "short",
                "confidence": 0.5,
                "citations": [],
            }
        ],
    )

    with pytest.raises(OmniBriefError):
        reb.generate_mcp_omni_email_brief(
            settings=Settings(db_path=str(tmp_path / "opp.sqlite")),
            force_cache_only=True,
        )


def test_separation_enforced(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _base_monkeypatch(monkeypatch)
    result = reb.generate_mcp_omni_email_brief(
        settings=Settings(db_path=str(tmp_path / "sep.sqlite")),
        force_cache_only=True,
    )
    html = Path(result["html_path"]).read_text(encoding="utf-8")
    assert "Policy context (long horizon)" in html
    assert "Monitoring and opportunities (short horizon)" in html
    assert "3y Trend (w)" in html
    assert "60d pct" in html


def test_no_directive_language(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _base_monkeypatch(monkeypatch)
    result = reb.generate_mcp_omni_email_brief(
        settings=Settings(db_path=str(tmp_path / "lang.sqlite")),
        force_cache_only=True,
    )
    text = Path(result["md_path"]).read_text(encoding="utf-8").lower()
    forbidden = [r"\bbuy\b", r"\bsell\b", r"\breduce exposure\b", r"\bincrease exposure\b", r"\brebalance now\b"]
    for pattern in forbidden:
        assert not re.search(pattern, text)
