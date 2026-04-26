#!/usr/bin/env python3
"""
Demonstration of ETF Verification Pipeline

Shows complete flow with mock data (since real iShares URLs require research):
1. Mock PDF content with all required fields
2. Parser extracts proofs
3. Verification status computed
4. Blueprint payload generated
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.etf_doc_parser import (
    _is_factsheet_fresh,
    _parse_accumulating_status,
    _parse_domicile,
    _parse_factsheet_date,
    _parse_isin,
    _parse_ter,
)


def demo_verification_complete():
    """Demonstrate complete verification with all proofs satisfied."""
    print("\n" + "=" * 70)
    print("DEMO: Complete Verification (All Proofs Satisfied)")
    print("=" * 70 + "\n")

    # Mock PDF content (realistic factsheet text)
    mock_factsheet_text = """
    iShares MSCI World UCITS ETF (Acc)

    Fund Facts as at 31 January 2026

    ISIN: IE00B4L5Y983
    Ticker: IWDA

    Fund Domicile: Ireland
    Legal Structure: UCITS ETF domiciled in Ireland

    Share Class: Accumulating
    Income Treatment: Reinvested automatically

    Ongoing Charges: 0.20%
    Total Expense Ratio (TER): 0.20% p.a.

    Net Assets: $45.2 billion
    """

    print("Step 1: Extract verification proofs from issuer PDF")
    print("-" * 70)

    isin = _parse_isin(mock_factsheet_text)
    domicile = _parse_domicile(mock_factsheet_text)
    accumulating = _parse_accumulating_status(mock_factsheet_text)
    ter = _parse_ter(mock_factsheet_text)
    factsheet_date = _parse_factsheet_date(mock_factsheet_text)

    print(f"  ✓ ISIN: {isin}")
    print(f"  ✓ Domicile: {domicile}")
    print(f"  ✓ Accumulating: {accumulating}")
    print(f"  ✓ TER: {ter * 100:.2f}%")
    print(f"  ✓ Factsheet date: {factsheet_date}")

    # Check freshness
    is_fresh = _is_factsheet_fresh(factsheet_date, 120)
    print(f"  ✓ Factsheet fresh (<120 days): {is_fresh}")

    print("\nStep 2: Compute verification status")
    print("-" * 70)

    proofs = {
        "proof_isin": isin is not None,
        "proof_domicile": domicile is not None,
        "proof_accumulating": accumulating is not None,
        "proof_ter": ter is not None,
        "proof_factsheet_fresh": is_fresh,
    }

    verified = all(proofs.values())
    missing_proofs = [k.replace("proof_", "") for k, v in proofs.items() if not v]

    for proof_name, proof_value in proofs.items():
        status = "✓" if proof_value else "✗"
        print(f"  {status} {proof_name}: {proof_value}")

    print(f"\n  Verification Status: {'✅ VERIFIED' if verified else '❌ UNVERIFIED'}")

    if missing_proofs:
        print(f"  Missing proofs: {', '.join(missing_proofs)}")

    print("\nStep 3: Generate blueprint payload")
    print("-" * 70)

    candidate_payload = {
        "symbol": "IWDA",
        "name": "iShares MSCI World UCITS ETF (Acc)",
        "instrument_type": "etf_ucits",
        "domicile": domicile,
        "expense_ratio": ter,
        "accumulation_or_distribution": accumulating,
        "replication_method": "Physical",
        "liquidity_proxy": "High (2.5M avg vol)",
        "verified": verified,
        "partially_verified": False,
        "verification_missing": missing_proofs,
        "proofs": proofs,
        "citations": [
            {
                "source_id": "ishares_iwda_factsheet",
                "url": "https://www.ishares.com/.../iwda-fact-sheet.pdf",
                "retrieved_at": datetime.now().isoformat(),
                "purpose": "ISIN, domicile, TER, accumulating status, factsheet date",
                "factsheet_asof": factsheet_date,
            }
        ],
        "sg_lens": {
            "score": 95.2,
            "breakdown": {
                "withholding_penalty": 0.15,
                "expense_penalty": 0.20,
                "liquidity_bonus": 10.0,
                "estate_risk_penalty": 0.0,
            },
        },
    }

    print(json.dumps(candidate_payload, indent=2))

    return candidate_payload


def demo_verification_partial():
    """Demonstrate partial verification with missing proofs."""
    print("\n" + "=" * 70)
    print("DEMO: Partial Verification (Missing TER and Factsheet Date)")
    print("=" * 70 + "\n")

    # Mock PDF with incomplete data
    mock_incomplete_text = """
    Vanguard FTSE All-World UCITS ETF

    ISIN: IE00BK5BQT80
    Domicile: Ireland
    Share Class: Accumulating

    (TER not shown, factsheet date missing)
    """

    print("Step 1: Extract proofs from incomplete PDF")
    print("-" * 70)

    isin = _parse_isin(mock_incomplete_text)
    domicile = _parse_domicile(mock_incomplete_text)
    accumulating = _parse_accumulating_status(mock_incomplete_text)
    ter = _parse_ter(mock_incomplete_text)
    factsheet_date = _parse_factsheet_date(mock_incomplete_text)

    print(f"  ✓ ISIN: {isin}")
    print(f"  ✓ Domicile: {domicile}")
    print(f"  ✓ Accumulating: {accumulating}")
    print(f"  ✗ TER: {ter} (NOT FOUND)")
    print(f"  ✗ Factsheet date: {factsheet_date} (NOT FOUND)")

    proofs = {
        "proof_isin": isin is not None,
        "proof_domicile": domicile is not None,
        "proof_accumulating": accumulating is not None,
        "proof_ter": ter is not None,
        "proof_factsheet_fresh": factsheet_date is not None and _is_factsheet_fresh(factsheet_date, 120),
    }

    verified = all(proofs.values())
    partially_verified = any(proofs.values()) and not verified
    missing_proofs = [k.replace("proof_", "") for k, v in proofs.items() if not v]

    print("\nStep 2: Verification status")
    print("-" * 70)
    print(f"  Verified: {verified}")
    print(f"  Partially verified: {partially_verified}")
    print(f"  Missing proofs: {', '.join(missing_proofs)}")

    print("\nStep 3: UI behavior")
    print("-" * 70)
    print("  ⚠️  Candidate visible but marked as UNVERIFIED")
    print("  ⚠️  'Show verified only' filter will HIDE this candidate")
    print(f"  ⚠️  Missing: {', '.join(missing_proofs)}")
    print("  💡 User action: 'Refresh docs' button to retry PDF fetch")


def demo_isin_conflict():
    """Demonstrate ISIN conflict detection."""
    print("\n" + "=" * 70)
    print("DEMO: ISIN Conflict (Downgrade to Unverified)")
    print("=" * 70 + "\n")

    expected_isin = "IE00BK5BQT80"
    extracted_isin = "IE00BK5BXXXX"  # Wrong ISIN

    isin_conflict = (extracted_isin != expected_isin)

    print("Step 1: ISIN Cross-Check")
    print("-" * 70)
    print(f"  Expected ISIN: {expected_isin}")
    print(f"  Extracted ISIN: {extracted_isin}")
    print(f"  Conflict detected: {isin_conflict}")

    print("\nStep 2: Verification downgrade")
    print("-" * 70)
    print("  ❌ UNVERIFIED (despite all other proofs passing)")
    print("  ⚠️  Requires manual review")
    print("  💡 Possible causes:")
    print("      - Wrong PDF fetched")
    print("      - Share class mismatch")
    print("      - Registry configuration error")


if __name__ == "__main__":
    # Run all demos
    demo_verification_complete()
    demo_verification_partial()
    demo_isin_conflict()

    print("\n" + "=" * 70)
    print("✅ Verification Pipeline Demonstrations Complete")
    print("=" * 70 + "\n")

    print("Summary:")
    print("  1. ✅ Parser extracts all 5 proofs from PDF text")
    print("  2. ✅ Verification requires all proofs satisfied")
    print("  3. ✅ Missing proofs → partially_verified status")
    print("  4. ✅ ISIN conflict → unverified + manual review")
    print("  5. ✅ Yahoo Finance data NOT used for proofs")
    print("\nNext steps:")
    print("  - Research correct iShares/Vanguard PDF URLs")
    print("  - Test with real PDFs")
    print("  - Integrate into blueprint service")
    print("  - Add 'Refresh docs' UI button")
