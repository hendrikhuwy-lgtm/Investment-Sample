#!/usr/bin/env python3
"""
Bootstrap ETF data sources from configuration file.
Populates etf_data_sources table with real source URLs.
"""

import json
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import Settings, get_db_path
from app.models.db import connect
from app.services.ingest_etf_data import configure_etf_data_source, _ensure_etf_tables


def load_etf_sources_config() -> dict:
    """Load ETF data sources configuration from JSON."""
    config_path = Path(__file__).parent.parent / "app/config/etf_data_sources.json"

    if not config_path.exists():
        raise FileNotFoundError(f"ETF sources config not found: {config_path}")

    with open(config_path) as f:
        return json.load(f)


def bootstrap_etf_data_sources(settings: Settings | None = None) -> None:
    """Initialize etf_data_sources table from configuration."""
    settings = settings or Settings.from_env()
    db_path = get_db_path(settings=settings)
    conn = connect(db_path)

    _ensure_etf_tables(conn)

    config = load_etf_sources_config()

    sources_configured = 0

    for etf_config in config.get("sources", []):
        symbol = etf_config["etf_symbol"]
        name = etf_config["name"]
        data_sources = etf_config.get("data_sources", {})

        print(f"\nConfiguring {symbol} ({name})...")

        # Configure holdings source
        if "holdings" in data_sources:
            holdings_src = data_sources["holdings"]
            url = holdings_src.get("url") or holdings_src.get("url_template") or holdings_src.get("url_holdings_csv")
            source_id = holdings_src["citation_source_id"]

            if url:
                configure_etf_data_source(
                    conn=conn,
                    etf_symbol=symbol,
                    data_type="holdings",
                    source_id=source_id,
                    source_url_template=url,
                    fetch_method=holdings_src["method"],
                    update_frequency=holdings_src.get("frequency", "monthly"),
                )
                print(f"  ✓ Holdings source: {holdings_src['provider']}")
                sources_configured += 1

        # Configure factsheet source
        if "factsheet" in data_sources:
            factsheet_src = data_sources["factsheet"]
            url = factsheet_src.get("url") or factsheet_src.get("url_template")
            source_id = factsheet_src["citation_source_id"]

            if url:
                configure_etf_data_source(
                    conn=conn,
                    etf_symbol=symbol,
                    data_type="factsheet",
                    source_id=source_id,
                    source_url_template=url,
                    fetch_method=factsheet_src["method"],
                    update_frequency=factsheet_src.get("frequency", "monthly"),
                )
                print(f"  ✓ Factsheet source: {factsheet_src['provider']}")
                sources_configured += 1

        # Configure market data source
        if "market_data" in data_sources:
            market_src = data_sources["market_data"]
            url = market_src.get("url") or market_src.get("url_template")
            source_id = market_src["citation_source_id"]

            if url:
                configure_etf_data_source(
                    conn=conn,
                    etf_symbol=symbol,
                    data_type="market_data",
                    source_id=source_id,
                    source_url_template=url,
                    fetch_method=market_src["method"],
                    update_frequency=market_src.get("frequency", "daily"),
                )
                print(f"  ✓ Market data source: {market_src['provider']}")
                sources_configured += 1

    conn.close()

    print(f"\n✅ Configured {sources_configured} data sources for {len(config.get('sources', []))} ETFs")
    print("\nNext steps:")
    print("1. Implement fetch functions for each provider (iShares API, Vanguard PDF, etc.)")
    print("2. Run: python backend/scripts/refresh_etf_data.py IWDA")
    print("3. Query: SELECT * FROM etf_holdings WHERE etf_symbol = 'IWDA';")


if __name__ == "__main__":
    bootstrap_etf_data_sources()
