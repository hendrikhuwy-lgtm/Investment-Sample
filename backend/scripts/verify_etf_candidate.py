#!/usr/bin/env python3
"""
Verify ETF candidate using issuer PDFs + Yahoo Finance.

Demonstrates complete verification pipeline:
1. Fetch issuer PDFs (factsheet, KID)
2. Extract ISIN, domicile, TER, accumulating status, factsheet date
3. Fetch Yahoo Finance for market data (liquidity proxy)
4. Combine into verification result
5. Mark as Verified only if all 5 proofs satisfied
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.etf_doc_parser import fetch_candidate_docs
from app.services.yahoo_finance import fetch_yahoo_market_data, format_liquidity_proxy


def verify_candidate(ticker: str) -> dict:
    """
    Run full verification for an ETF candidate.

    Returns verification status and complete data package.
    """
    print(f"\n{'='*70}")
    print(f"ETF Candidate Verification: {ticker}")
    print(f"{'='*70}\n")

    # Step 1: Fetch and parse issuer docs
    print("Step 1: Fetching issuer documentation...")
    doc_results = fetch_candidate_docs(ticker)

    if doc_results.get("status") == "failed":
        print(f"  ❌ Failed: {doc_results.get('error')}")
        return doc_results

    print(f"  ✓ Fetched: {ticker}")

    if doc_results.get("factsheet"):
        fs = doc_results["factsheet"]
        print(f"    Factsheet: {fs['status']}")
        if fs["status"] == "success":
            print(f"      URL: {fs['doc_url']}")
            print(f"      Cache: {fs['cache_file']}")

    if doc_results.get("kid"):
        kid = doc_results["kid"]
        print(f"    KID: {kid['status']}")

    # Step 2: Display extracted proofs
    print("\nStep 2: Verification Proofs (from issuer PDFs only)")
    extracted = doc_results.get("extracted", {})
    proofs = doc_results.get("proofs", {})

    proof_display = {
        "proof_isin": ("ISIN", extracted.get("isin")),
        "proof_domicile": ("Domicile", extracted.get("domicile")),
        "proof_accumulating": ("Accumulating", extracted.get("accumulating_status")),
        "proof_ter": ("TER", f"{extracted.get('ter') * 100:.2f}%" if extracted.get("ter") else None),
        "proof_factsheet_fresh": ("Factsheet date", extracted.get("factsheet_date")),
    }

    for proof_key, (label, value) in proof_display.items():
        status = "✓" if proofs.get(proof_key) else "✗"
        value_str = value if value else "NOT FOUND"
        print(f"  {status} {label}: {value_str}")

    # Check for ISIN conflict
    if doc_results.get("isin_conflict"):
        print(f"\n  ⚠️  ISIN CONFLICT DETECTED")
        print(f"      Expected: {doc_results.get('expected_isin')}")
        print(f"      Extracted: {extracted.get('isin')}")
        print(f"      Status: UNVERIFIED (requires manual review)")

    # Step 3: Fetch Yahoo Finance market data
    print("\nStep 3: Fetching market data (Yahoo Finance)")
    yahoo_data = fetch_yahoo_market_data(ticker)

    if yahoo_data["status"] == "success":
        market = yahoo_data["market_data"]
        print(f"  ✓ Yahoo ticker: {yahoo_data['yahoo_ticker']}")
        print(f"    Last price: ${market.get('last_price'):.2f}" if market.get('last_price') else "    Last price: N/A")
        print(f"    Bid/Ask spread: {market.get('bid_ask_spread_bps'):.1f} bps" if market.get('bid_ask_spread_bps') else "    Spread: N/A")
        print(f"    Volume (30d avg): {market.get('volume_avg_30d'):,.0f}" if market.get('volume_avg_30d') else "    Volume: N/A")
        liquidity_proxy = format_liquidity_proxy(market.get('liquidity_score'), market.get('volume_avg_30d'))
        print(f"    Liquidity proxy: {liquidity_proxy}")
        print(f"\n    ⚠️  Note: Yahoo data NOT used for verification proofs")
    else:
        print(f"  ❌ Yahoo Finance failed: {yahoo_data.get('error')}")

    # Step 4: Final verification status
    print(f"\n{'='*70}")
    print("Verification Status")
    print(f"{'='*70}\n")

    verified = doc_results.get("verified", False)
    partially_verified = doc_results.get("partially_verified", False)
    missing_proofs = doc_results.get("verification_missing", [])

    if verified:
        print("  ✅ VERIFIED")
        print("     All 5 proofs satisfied from issuer documentation")
    elif partially_verified:
        print("  ⚠️  PARTIALLY VERIFIED")
        print(f"     Missing proofs: {', '.join(missing_proofs)}")
        print("     Candidate visible but marked as unverified")
    else:
        print("  ❌ UNVERIFIED")
        print(f"     Missing proofs: {', '.join(missing_proofs)}")
        print("     Candidate will not appear in 'verified only' view")

    # Step 5: Generate candidate payload
    candidate_payload = {
        "ticker": ticker,
        "name": doc_results.get("ticker"),  # Would come from registry
        "verification_status": "verified" if verified else ("partially_verified" if partially_verified else "unverified"),
        "verified": verified,
        "partially_verified": partially_verified,
        "verification_missing": missing_proofs,
        "isin_conflict": doc_results.get("isin_conflict", False),
        "proofs": proofs,
        "extracted_from_issuer_docs": extracted,
        "market_data_from_yahoo": yahoo_data.get("market_data") if yahoo_data["status"] == "success" else None,
        "liquidity_proxy": format_liquidity_proxy(
            yahoo_data.get("market_data", {}).get("liquidity_score"),
            yahoo_data.get("market_data", {}).get("volume_avg_30d"),
        ) if yahoo_data["status"] == "success" else "Unknown",
        "citations": [
            {
                "source": "issuer_factsheet",
                "url": doc_results.get("factsheet", {}).get("doc_url"),
                "retrieved_at": doc_results.get("factsheet", {}).get("retrieved_at"),
                "purpose": "ISIN, domicile, TER, accumulating status, factsheet date verification",
            },
            {
                "source": "yahoo_finance",
                "url": yahoo_data.get("source_url") if yahoo_data["status"] == "success" else None,
                "retrieved_at": yahoo_data.get("retrieved_at"),
                "purpose": "Market data (liquidity proxy only, NOT for verification)",
            },
        ],
    }

    print(f"\nCandidate Payload (JSON):")
    print(json.dumps(candidate_payload, indent=2))

    return candidate_payload


if __name__ == "__main__":
    ticker = sys.argv[1] if len(sys.argv) > 1 else "IWDA"
    result = verify_candidate(ticker)

    # Write to file
    output_file = Path("outbox/etf_verification_result.json")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(result, indent=2))

    print(f"\n✅ Results written to: {output_file}")
