from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from app.models.db import connect, init_db
from app.services.liquidity_model import build_liquidity_snapshot
from app.services.portfolio_dimensions import build_dimension_snapshot


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "app" / "storage" / "schema.sql"


def _conn(tmp_path: Path):
    conn = connect(tmp_path / "analytics_precision.sqlite3")
    init_db(conn, SCHEMA_PATH)
    return conn


def test_dimension_snapshot_returns_provenance_unknown_buckets_and_factor_metadata(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        now = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO portfolio_upload_runs (
              run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
              raw_row_count, parsed_row_count, normalized_position_count, total_market_value, stale_price_count,
              mapping_issue_count, warning_count, warnings_json, errors_json
            ) VALUES (?, ?, ?, ?, ?, ?, 1, 0, 3, 3, 3, 3000, 0, 0, 0, '[]', '[]')
            """,
            ("run_dimensions", now, "2026-03-07", "sample.csv", "sample.csv", "succeeded"),
        )
        conn.executemany(
            """
            INSERT INTO portfolio_holding_snapshots (
              snapshot_row_id, run_id, uploaded_at, holdings_as_of_date, price_as_of_date, account_id, security_key,
              raw_symbol, normalized_symbol, security_name, asset_type, currency, quantity, cost_basis, market_price,
              market_value, fx_rate_to_base, base_currency, sleeve, mapping_status, price_source, price_stale, venue, identifier_isin
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "row_1", "run_dimensions", now, "2026-03-07", "2026-03-07", "ACC-1", "sec_us",
                    "AAPL", "AAPL", "Apple Inc.", "equity", "USD", 10.0, 100.0, 150.0,
                    1500.0, 1.35, "SGD", "global_equity", "auto_matched", "feed", 0, "NASDAQ", "US0378331005",
                ),
                (
                    "row_2", "run_dimensions", now, "2026-03-07", "2026-03-07", "ACC-1", "sec_bond",
                    "AGGU", "AGGU", "Global Aggregate Bond UCITS ETF", "bond", "USD", 5.0, 100.0, 120.0,
                    900.0, 1.35, "SGD", "ig_bond", "auto_matched", "feed", 0, "LSE", "IE00BDBRDM35",
                ),
                (
                    "row_3", "run_dimensions", now, "2026-03-07", "2026-03-07", "ACC-1", "sec_unknown",
                    "MYST", "MYST", "Mystery Asset", "alt", "USD", 2.0, 100.0, 300.0,
                    600.0, 1.35, "SGD", "alt", "auto_matched", "feed", 0, "", "",
                ),
            ],
        )
        conn.commit()

        payload = build_dimension_snapshot(conn, run_id="run_dimensions")

        assert payload["summary"]["unknown_region_weight"] > 0
        assert payload["summary"]["unknown_sector_weight"] > 0
        assert payload["summary"]["classified_weight"] > 0
        assert any(item["label"] == "North America" and item["confidence"] in {"high", "medium"} for item in payload["region_attribution"])
        assert any(item["label"] == "Technology" for item in payload["sector_attribution"])
        assert any(item["label"] == "United States" for item in payload["country_attribution"])
        assert any(item["label"] == "Technology Platforms" for item in payload["industry_attribution"])
        assert any(item["factor"] == "style_growth" and "provenance" in item for item in payload["factor_exposure"])
    finally:
        conn.close()


def test_liquidity_snapshot_emits_warnings_with_provenance(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        now = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO portfolio_upload_runs (
              run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
              raw_row_count, parsed_row_count, normalized_position_count, total_market_value, stale_price_count,
              mapping_issue_count, warning_count, warnings_json, errors_json
            ) VALUES (?, ?, ?, ?, ?, ?, 1, 0, 2, 2, 2, 2500000, 0, 0, 0, '[]', '[]')
            """,
            ("run_liquidity", now, "2026-03-07", "liq.csv", "liq.csv", "succeeded"),
        )
        conn.executemany(
            """
            INSERT INTO portfolio_holding_snapshots (
              snapshot_row_id, run_id, uploaded_at, holdings_as_of_date, price_as_of_date, account_id, security_key,
              raw_symbol, normalized_symbol, security_name, asset_type, currency, quantity, cost_basis, market_price,
              market_value, fx_rate_to_base, base_currency, sleeve, mapping_status, price_source, price_stale, venue, identifier_isin
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "row_liq_1", "run_liquidity", now, "2026-03-07", "2026-03-07", "ACC-1", "sec_cash",
                    "CASH", "CASH", "Cash Reserve", "cash", "SGD", 1.0, 1.0, 1.0,
                    500000.0, 1.0, "SGD", "cash", "auto_matched", "feed", 0, "SGX", None,
                ),
                (
                    "row_liq_2", "run_liquidity", now, "2026-03-07", "2026-03-07", "ACC-1", "sec_alt",
                    "ALTX", "ALTX", "Illiquid Alternatives Vehicle", "alt", "USD", 1.0, 1.0, 1.0,
                    2000000.0, 1.35, "SGD", "alt", "auto_matched", "feed", 0, "", None,
                ),
            ],
        )
        conn.commit()

        payload = build_liquidity_snapshot(conn, run_id="run_liquidity")

        assert payload["summary"]["warning_count"] >= 1
        assert payload["summary"]["illiquid_weight"] > 0
        assert any(item["normalized_symbol"] == "ALTX" for item in payload["warnings"])
        assert any(item["exit_difficulty_band"] == "hard" for item in payload["warnings"])
        assert any(item["source_name"] for item in payload["items"])
        assert any("rule" in item["provenance_json"] for item in payload["items"])
    finally:
        conn.close()
