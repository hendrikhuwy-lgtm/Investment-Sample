#!/usr/bin/env python3
"""
Production ETF Verification Script

Complete end-to-end verification using:
1. Issuer PDF fixtures (for testing) OR real PDFs (when URLs configured)
2. Yahoo Finance for market data
3. Strict 5-proof verification checklist
4. No hallucination - missing data downgrades status

Usage:
    python3 production_verify_etf.py IWDA
    python3 production_verify_etf.py IWDA --use-fixtures  # Test mode
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.etf_doc_parser import fetch_candidate_docs
from app.services.yahoo_finance import fetch_yahoo_market_data, format_liquidity_proxy


def production_verify(ticker: str, use_fixtures: bool = False) -> dict:
    """
    Run production verification for ETF candidate.

    Returns complete verified candidate payload for blueprint integration.
    """
    print(f"\n{'='*70}")
    print(f"ETF VERIFICATION: {ticker}")
    print(f"Mode: {'TEST (fixtures)' if use_fixtures else 'PRODUCTION (real PDFs)'}")
    print(f"{'='*70}\n")

    # Step 1: Fetch and parse issuer documentation
    print("Step 1: Issuer Documentation Verification")
    print("-" * 70)

    doc_result = fetch_candidate_docs(ticker, use_fixtures=use_fixtures)

    if doc_result.get("status") == "failed":
        print(f"❌ FAILED: {doc_result.get('error')}")
        return doc_result

    extracted = doc_result.get("extracted", {})
    proofs = doc_result.get("proofs", {})

    # Display extracted proofs
    proof_items = [
        ("ISIN", extracted.get("isin")),
        ("Domicile", extracted.get("domicile")),
        ("Accumulating", extracted.get("accumulating_status")),
        ("TER", f"{extracted.get('ter') * 100:.2f}%" if extracted.get("ter") else None),
        ("Factsheet date", extracted.get("factsheet_date")),
    ]

    for label, value in proof_items:
        proof_key = f"proof_{label.lower().replace(' ', '_')}"
        if label == "TER":
            proof_key = "proof_ter"
        elif label == "Accumulating":
            proof_key = "proof_accumulating"
        elif label == "Factsheet date":
            proof_key = "proof_factsheet_fresh"

        status = "✓" if proofs.get(proof_key) else "✗"
        display_value = value if value else "NOT FOUND"
        print(f"  {status} {label}: {display_value}")

    # Check ISIN conflict
    if doc_result.get("isin_conflict"):
        print(f"\n  ⚠️  ISIN CONFLICT")
        print(f"      Expected: {doc_result.get('expected_isin')}")
        print(f"      Extracted: {extracted.get('isin')}")

    # Step 2: Fetch market data from Yahoo Finance
    print("\nStep 2: Market Data (Yahoo Finance - Liquidity Only)")
    print("-" * 70)

    # Determine exchange suffix
    if ticker.endswith(".SI"):
        exchange_suffix = ""  # Already has suffix
    else:
        exchange_suffix = ".L"  # Default to London

    yahoo_result = fetch_yahoo_market_data(ticker, exchange_suffix)

    if yahoo_result["status"] == "success":
        market = yahoo_result["market_data"]
        print(f"  ✓ Ticker: {yahoo_result['yahoo_ticker']}")

        if market.get("last_price"):
            print(f"  ✓ Price: ${market['last_price']:.2f}")

        if market.get("bid_ask_spread_bps"):
            print(f"  ✓ Spread: {market['bid_ask_spread_bps']:.1f} bps")

        if market.get("volume_avg_30d"):
            print(f"  ✓ Volume (30d avg): {market['volume_avg_30d']:,.0f}")

        liquidity_proxy = format_liquidity_proxy(
            market.get("liquidity_score"),
            market.get("volume_avg_30d")
        )
        print(f"  ✓ Liquidity proxy: {liquidity_proxy}")

        print("\n  ⚠️  Yahoo data NOT used for verification proofs")
    else:
        print(f"  ✗ Yahoo Finance failed: {yahoo_result.get('error')}")
        liquidity_proxy = "Unknown"

    # Step 3: Final verification status
    print(f"\n{'='*70}")
    print("VERIFICATION STATUS")
    print(f"{'='*70}\n")

    verified = doc_result.get("verified", False)
    partially_verified = doc_result.get("partially_verified", False)
    missing_proofs = doc_result.get("verification_missing", [])

    if verified:
        print("  ✅ VERIFIED")
        print("     All 5 proofs satisfied from issuer documentation")
        print("     Candidate eligible for 'verified only' view")
    elif partially_verified:
        print("  ⚠️  PARTIALLY VERIFIED")
        print(f"     Missing proofs: {', '.join(missing_proofs)}")
        print("     Candidate visible but marked unverified")
    else:
        print("  ❌ UNVERIFIED")
        if doc_result.get("isin_conflict"):
            print("     Reason: ISIN conflict detected")
        else:
            print(f"     Missing proofs: {', '.join(missing_proofs)}")
        print("     Candidate hidden in 'verified only' view")

    # Step 4: Generate blueprint payload
    print(f"\n{'='*70}")
    print("BLUEPRINT PAYLOAD")
    print(f"{'='*70}\n")

    candidate_payload = {
        "symbol": ticker,
        "name": f"{ticker} ETF",  # Would come from registry
        "instrument_type": "etf_ucits" if extracted.get("domicile") == "Ireland" else "etf",
        "verified": verified,
        "partially_verified": partially_verified,
        "verification_missing": missing_proofs,
        "isin_conflict": doc_result.get("isin_conflict", False),
        "proofs": proofs,
        "isin": extracted.get("isin"),
        "domicile": extracted.get("domicile"),
        "accumulation_or_distribution": extracted.get("accumulating_status"),
        "expense_ratio": extracted.get("ter"),
        "factsheet_date": extracted.get("factsheet_date"),
        "liquidity_proxy": liquidity_proxy,
        "market_data": {
            "last_price": market.get("last_price") if yahoo_result["status"] == "success" else None,
            "bid_ask_spread_bps": market.get("bid_ask_spread_bps") if yahoo_result["status"] == "success" else None,
            "volume_avg_30d": market.get("volume_avg_30d") if yahoo_result["status"] == "success" else None,
        } if yahoo_result["status"] == "success" else None,
        "citations": [
            {
                "source": "issuer_factsheet",
                "url": doc_result.get("factsheet", {}).get("doc_url"),
                "retrieved_at": doc_result.get("factsheet", {}).get("retrieved_at"),
                "purpose": "ISIN, domicile, TER, accumulating status verification",
            },
            {
                "source": "yahoo_finance",
                "url": yahoo_result.get("source_url") if yahoo_result["status"] == "success" else None,
                "retrieved_at": yahoo_result.get("retrieved_at"),
                "purpose": "Market data (liquidity proxy only)",
            },
        ],
    }

    print(json.dumps(candidate_payload, indent=2))

    # Save to file
    output_dir = Path("outbox/etf_verification")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{ticker}_verification.json"
    output_file.write_text(json.dumps(candidate_payload, indent=2))

    print(f"\n✅ Saved to: {output_file}")

    return candidate_payload


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Verify ETF candidate")
    parser.add_argument("ticker", help="ETF ticker symbol (e.g., IWDA, VWRA)")
    parser.add_argument("--use-fixtures", action="store_true", help="Use test fixtures instead of real PDFs")
    args = parser.parse_args()

    result = production_verify(args.ticker, use_fixtures=args.use_fixtures)

    # Exit code based on verification status
    if result.get("verified"):
        sys.exit(0)
    else:
        sys.exit(1)
