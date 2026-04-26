#!/usr/bin/env python3
"""
Test ETF data fetching from real sources.
Demonstrates complete pipeline: source configuration → fetch → query.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import Settings, get_db_path
from app.models.db import connect
from app.services.ingest_etf_data import (
    get_latest_etf_factsheet_metrics,
    get_latest_etf_holdings,
    refresh_etf_data,
)


def test_iwda_fetch():
    """Test fetching iShares MSCI World ETF (IWDA) data."""
    symbol = "IWDA"

    print(f"\n{'='*60}")
    print(f"Testing ETF Data Fetch: {symbol}")
    print(f"{'='*60}\n")

    # Fetch from real sources
    print("Fetching from iShares API and CSV...")
    results = refresh_etf_data(symbol)

    print(f"\nFetch Results:")
    print(f"  Started: {results['started_at']}")
    print(f"  Finished: {results['finished_at']}")

    # Holdings
    if results.get("holdings"):
        holdings = results["holdings"]
        print(f"\n  Holdings:")
        print(f"    Status: {holdings.get('status')}")
        if holdings.get('status') == 'success':
            print(f"    Records fetched: {holdings.get('holdings_fetched')}")
            print(f"    As-of date: {holdings.get('asof_date')}")
            print(f"    Source: {holdings.get('source_url')}")
        else:
            print(f"    Error: {holdings.get('error')}")

    # Factsheet
    if results.get("factsheet"):
        factsheet = results["factsheet"]
        print(f"\n  Factsheet:")
        print(f"    Status: {factsheet.get('status')}")
        if factsheet.get('status') == 'success':
            print(f"    AUM (USD): ${factsheet.get('aum_usd'):,.2f}" if factsheet.get('aum_usd') else "    AUM: N/A")
            print(f"    Tracking diff (1y): {factsheet.get('tracking_difference_1y')}%" if factsheet.get('tracking_difference_1y') else "    Tracking diff: N/A")
            print(f"    Dividend yield: {factsheet.get('dividend_yield')}%" if factsheet.get('dividend_yield') else "    Dividend yield: N/A")
            print(f"    As-of date: {factsheet.get('asof_date')}")
            print(f"    Source: {factsheet.get('source_url')}")
        else:
            print(f"    Error: {factsheet.get('error')}")

    # Query back from database
    print(f"\n{'='*60}")
    print("Querying stored data from database...")
    print(f"{'='*60}\n")

    settings = Settings.from_env()
    db_path = get_db_path(settings=settings)
    conn = connect(db_path)

    # Query holdings
    holdings_data = get_latest_etf_holdings(symbol, conn)
    if holdings_data:
        print(f"Holdings (top 10 by weight):")
        for i, holding in enumerate(holdings_data[:10], 1):
            print(f"  {i}. {holding['security_name']} ({holding['ticker']}): {holding['weight_pct']:.2f}%")
        print(f"     Total holdings: {len(holdings_data)}")
        print(f"     Citation: {holdings_data[0]['citation']['source_url']}")
    else:
        print("No holdings data found in database")

    # Query factsheet metrics
    factsheet_data = get_latest_etf_factsheet_metrics(symbol, conn)
    if factsheet_data:
        print(f"\nFactsheet Metrics:")
        if factsheet_data.get('aum_usd'):
            print(f"  AUM: ${factsheet_data['aum_usd']:,.2f}")
        if factsheet_data.get('tracking_difference_1y') is not None:
            print(f"  Tracking difference (1y): {factsheet_data['tracking_difference_1y']}%")
        if factsheet_data.get('dividend_yield'):
            print(f"  Dividend yield: {factsheet_data['dividend_yield']}%")
        print(f"  As-of date: {factsheet_data['asof_date']}")
        print(f"  Citation: {factsheet_data['citation']['source_url']}")
    else:
        print("\nNo factsheet data found in database")

    conn.close()

    print(f"\n{'='*60}")
    print("✅ ETF data pipeline test complete")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    test_iwda_fetch()
