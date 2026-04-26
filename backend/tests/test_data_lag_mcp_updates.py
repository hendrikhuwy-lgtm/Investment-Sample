from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from app.config import Settings
from app.models.db import connect, init_db
from app.models.types import Citation, MCPItem, MCPServerSnapshot, MCPServerCapability, SourceRecord
from app.services.ingest_mcp import MCPIngestionResult
from app.services import real_email_brief as reb


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "app" / "storage" / "schema.sql"


def _fred_citation(code: str, observed_at: str, lag_days: int) -> Citation:
    lag_class = "fresh" if lag_days <= 1 else "lagged" if lag_days <= 4 else "stale"
    return Citation(
        url=f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={code}",
        source_id=f"fred_{code.lower()}",
        retrieved_at=datetime(2026, 2, 19, 2, 0, tzinfo=UTC),
        importance=f"Official FRED series {code}; retrieval=live",
        observed_at=observed_at,
        lag_days=lag_days,
        lag_class=lag_class,
    )


def _sample_series() -> tuple[dict[str, dict[str, float | str | list[float]]], list[Citation], bool]:
    series = {
        "DGS10": {"latest_date": "2026-02-16", "latest_value": 4.05, "change_5obs": -0.12, "points": [4.2, 4.1, 4.05]},
        "T10YIE": {"latest_date": "2026-02-16", "latest_value": 2.29, "change_5obs": 0.03, "points": [2.2, 2.25, 2.29]},
        "SP500": {"latest_date": "2026-02-16", "latest_value": 6881.31, "change_5obs": 45.2, "points": [6800, 6840, 6881]},
        "VIXCLS": {"latest_date": "2026-02-16", "latest_value": 20.29, "change_5obs": -0.95, "points": [21.4, 20.8, 20.29]},
        "BAMLH0A0HYM2": {"latest_date": "2026-02-16", "latest_value": 2.94, "change_5obs": 0.02, "points": [2.9, 2.92, 2.94]},
    }
    citations = [
        _fred_citation("DGS10", "2026-02-16", 3),
        _fred_citation("T10YIE", "2026-02-16", 3),
        _fred_citation("SP500", "2026-02-16", 3),
        _fred_citation("VIXCLS", "2026-02-16", 3),
        _fred_citation("BAMLH0A0HYM2", "2026-02-16", 3),
    ]
    return series, citations, True


def _sample_graph_rows(series: dict[str, dict[str, float | str | list[float]]], citations: list[Citation]) -> list[dict]:
    by_source = {citation.source_id: citation for citation in citations}
    rows: list[dict] = []
    for code, label, _ in reb.FRED_SERIES:
        source_id = f"fred_{code.lower()}"
        citation = by_source[source_id]
        rows.append(
            {
                "series_code": code,
                "metric": f"{label} ({series[code]['latest_date']})",
                "latest_date": series[code]["latest_date"],
                "as_of": citation.observed_at,
                "lag_days": citation.lag_days,
                "lag_class": citation.lag_class,
                "daily_change_cue": f"1d delta: n/a, series not updated since {citation.observed_at}",
                "citation": citation,
                "long_horizon": {
                    "latest": series[code]["latest_value"],
                    "change_1y": 0.0,
                    "sparkline_3y_weekly": "▁▂▃▄",
                    "rolling_vol_5y": 0.12,
                    "percentile_5y": 70.0,
                    "percentile_10y": 65.0,
                    "range_bar_10y": "██████░░░░",
                    "regime_classification": "upper percentile regime",
                },
                "short_horizon": {
                    "change_5obs": series[code]["change_5obs"],
                    "momentum_20obs": series[code]["change_5obs"],
                    "sparkline_60d": "▁▂▃▄",
                    "range_bar_60d": "████░░░░",
                    "percentile_60d": 72.0,
                    "direction_tag": "upward",
                    "zscore_20obs": 1.8,
                },
            }
        )
    return rows


def _sample_web_records() -> list[SourceRecord]:
    return [
        SourceRecord(
            source_id="fred_dgs10",
            url="https://fred.stlouisfed.org/series/DGS10",
            publisher="FRED",
            retrieved_at=datetime.now(UTC),
            topic="rates",
            credibility_tier="primary",
            raw_hash="hash_fred_dgs10",
            source_type="web",
        )
    ]


def _seed_mcp_items(
    *,
    db_path: Path,
    run_id: str,
    server_id: str,
    title: str,
    content: str,
    prior_run_id: str | None = None,
    same_prior_item: bool = False,
) -> None:
    conn = connect(db_path)
    init_db(conn, SCHEMA_PATH)
    now = datetime(2026, 2, 19, 2, 0, tzinfo=UTC).isoformat()
    item_url = "https://provider.example/item"
    item_id = hashlib.sha256(f"{server_id}|{title}|{item_url}".encode("utf-8")).hexdigest()
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    payload = json.dumps({"title": title, "content": content})
    conn.execute(
        """
        INSERT INTO mcp_items (
          server_id, mcp_server_id, run_id, item_id, retrieved_at, raw_hash, content_hash,
          item_type, uri, url, title, published_at, snippet, item_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            server_id,
            server_id,
            run_id,
            item_id,
            now,
            hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            content_hash,
            "resource",
            "mcp://item/1",
            item_url,
            title,
            "2026-02-18",
            content[:120],
            payload,
        ),
    )
    if prior_run_id:
        prior_content = content if same_prior_item else "prior different content"
        prior_payload = json.dumps({"title": title, "content": prior_content})
        conn.execute(
            """
            INSERT INTO mcp_items (
              server_id, mcp_server_id, run_id, item_id, retrieved_at, raw_hash, content_hash,
              item_type, uri, url, title, published_at, snippet, item_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                server_id,
                server_id,
                prior_run_id,
                item_id,
                datetime(2026, 2, 18, 2, 0, tzinfo=UTC).isoformat(),
                hashlib.sha256(prior_payload.encode("utf-8")).hexdigest(),
                hashlib.sha256(prior_content.encode("utf-8")).hexdigest(),
                "resource",
                "mcp://item/1",
                item_url,
                title,
                "2026-02-17",
                prior_content[:120],
                prior_payload,
            ),
        )
    conn.commit()
    conn.close()


def _mcp_result(run_id: str, server_id: str = "finance/provider") -> MCPIngestionResult:
    snapshot = MCPServerSnapshot(
        server_id=server_id,
        server_name="Finance Provider",
        publisher="finance",
        endpoint_url="https://provider.example/mcp",
        endpoint_type="http",
        retrieved_at=datetime(2026, 2, 19, 2, 0, tzinfo=UTC),
        cached=False,
        capability=MCPServerCapability(
            handshake_ok=True,
            tools_count=1,
            resources_count=1,
            auth_used=False,
            errors=[],
        ),
        items=[
            MCPItem(
                server_id=server_id,
                item_type="resource",
                uri="mcp://item/1",
                title="Liquidity and credit risk note",
                content="Risk and liquidity context",
                metadata={},
            )
        ],
        raw_hash="raw_hash",
        error=None,
    )
    return MCPIngestionResult(
        source_records=[],
        snapshots=[snapshot],
        errors=[],
        total_servers=1,
        connected_servers=1,
        cached_used=False,
        connectable_servers=1,
        live_success_count=1,
        live_success_ratio=1.0,
        run_id=run_id,
    )


def _base_monkeypatch(monkeypatch, tmp_path: Path, *, with_prior_same_item: bool) -> None:
    db_path = tmp_path / "brief.sqlite"

    monkeypatch.setattr(reb, "_refresh_live_cache", lambda settings: (False, "cached"))
    monkeypatch.setattr(reb, "_load_series_from_cache", lambda force_cache_only: _sample_series())
    monkeypatch.setattr(reb, "fetch_web_sources", lambda settings: _sample_web_records())

    series, citations, _ = _sample_series()
    monkeypatch.setattr(reb, "_build_dual_horizon_graph_rows", lambda _series, _citations: _sample_graph_rows(series, citations))

    def _fake_ingest(settings=None, **kwargs):
        current_run_id = "mcp_run_current"
        prior_run_id = "mcp_run_prior" if with_prior_same_item else None
        _seed_mcp_items(
            db_path=db_path,
            run_id=current_run_id,
            server_id="finance/provider",
            title="Credit and liquidity update",
            content="current content",
            prior_run_id=prior_run_id,
            same_prior_item=with_prior_same_item,
        )
        return _mcp_result(run_id=current_run_id)

    monkeypatch.setattr(reb, "ingest_mcp_omni", _fake_ingest)


def test_compute_lag_days_lagged_class() -> None:
    retrieved = datetime(2026, 2, 19, 12, 0, tzinfo=UTC)
    lag_days, lag_class = reb.compute_lag_days("2026-02-16", retrieved, timezone="Asia Singapore")
    assert lag_days == 3
    assert lag_class == "lagged"


def test_executive_snapshot_includes_as_of_lag(monkeypatch, tmp_path: Path) -> None:
    _base_monkeypatch(monkeypatch, tmp_path, with_prior_same_item=False)
    result = reb.generate_mcp_omni_email_brief(
        settings=Settings(db_path=str(tmp_path / "brief.sqlite")),
        force_cache_only=True,
    )
    markdown = Path(result["md_path"]).read_text(encoding="utf-8")
    assert "as of 2026-02-16, lag 3 day(s)" in markdown


def test_mcp_updates_new_items_today(monkeypatch, tmp_path: Path) -> None:
    _base_monkeypatch(monkeypatch, tmp_path, with_prior_same_item=False)
    result = reb.generate_mcp_omni_email_brief(
        settings=Settings(db_path=str(tmp_path / "brief.sqlite")),
        force_cache_only=True,
    )
    markdown = Path(result["md_path"]).read_text(encoding="utf-8")
    assert "New MCP items today" in markdown


def test_mcp_updates_explicit_no_new_items(monkeypatch, tmp_path: Path) -> None:
    _base_monkeypatch(monkeypatch, tmp_path, with_prior_same_item=True)
    result = reb.generate_mcp_omni_email_brief(
        settings=Settings(db_path=str(tmp_path / "brief.sqlite")),
        force_cache_only=True,
    )
    markdown = Path(result["md_path"]).read_text(encoding="utf-8")
    assert "No new MCP items since prior run" in markdown
