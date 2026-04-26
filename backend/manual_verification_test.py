#!/usr/bin/env python3
"""
Manual verification test to validate the verification logic fixes.
Runs without pytest dependency.
"""

from datetime import UTC, datetime
from app.services.verification import verify_candidate_proofs, _normalize_ter


def test_distributing_etf_verified():
    """Distributing ETF with matching citation should verify."""
    print("\n[TEST] Distributing ETF verification...")

    candidate = {
        "symbol": "VEVE",
        "isin": "IE00BK5BQV03",
        "domicile": "IE",
        "accumulation_or_distribution": "distributing",
        "expense_ratio": 0.0012,
        "instrument_type": "etf_ucits",
    }

    citations = [
        {
            "url": "https://example.com/veve",
            "tier": "primary",
            "proof_identifier": "IE00BK5BQV03",
            "proof_domicile": "IE",
            "proof_share_class": "distributing",
            "proof_ter": 0.0012,
            "factsheet_asof": "2026-02-28",
        }
    ]

    result = verify_candidate_proofs(
        candidate=candidate,
        citations=citations,
        now=datetime(2026, 3, 2, tzinfo=UTC),
        freshness_days_threshold=120,
    )

    assert result["proof_share_class_match"] is True, "Share class match should be True"
    assert result["verification_status"] == "verified", f"Expected verified, got {result['verification_status']}"
    print("✅ PASS: Distributing ETF can be verified")


def test_accumulating_etf_verified():
    """Accumulating ETF with matching citation should verify."""
    print("\n[TEST] Accumulating ETF verification...")

    candidate = {
        "symbol": "VWRA",
        "isin": "IE00BK5BQT80",
        "domicile": "IE",
        "accumulation_or_distribution": "accumulating",
        "expense_ratio": 0.0022,
        "instrument_type": "etf_ucits",
    }

    citations = [
        {
            "url": "https://example.com/vwra",
            "tier": "primary",
            "proof_identifier": "IE00BK5BQT80",
            "proof_domicile": "IE",
            "proof_share_class": "accumulating",
            "proof_ter": 0.0022,
            "factsheet_asof": "2026-02-28",
        }
    ]

    result = verify_candidate_proofs(
        candidate=candidate,
        citations=citations,
        now=datetime(2026, 3, 2, tzinfo=UTC),
        freshness_days_threshold=120,
    )

    assert result["proof_share_class_match"] is True, "Share class match should be True"
    assert result["verification_status"] == "verified", f"Expected verified, got {result['verification_status']}"
    print("✅ PASS: Accumulating ETF still verifies")


def test_ter_normalization():
    """TER normalization handles percent vs fraction forms."""
    print("\n[TEST] TER normalization...")

    # Fraction form should stay unchanged
    assert _normalize_ter(0.0022) == 0.0022, "Fraction form should remain unchanged"
    assert _normalize_ter(0.0020) == 0.0020

    # Percent form should convert to fraction
    assert _normalize_ter(0.22) == 0.0022, "Percent form should convert to fraction"
    assert _normalize_ter(0.20) == 0.0020
    assert _normalize_ter(1.50) == 0.0150

    # Threshold test
    assert _normalize_ter(0.09) == 0.09, "Below threshold should not convert"
    assert _normalize_ter(0.10) == 0.10, "At threshold should not convert"
    assert _normalize_ter(0.11) == 0.0011, "Above threshold should convert"

    # None handling
    assert _normalize_ter(None) is None, "None should return None"

    print("✅ PASS: TER normalization works correctly")


def test_ter_proof_with_percent_citation():
    """TER proof should pass when citation is in percent form."""
    print("\n[TEST] TER proof with percent citation...")

    candidate = {
        "symbol": "TEST",
        "isin": "IE00TEST0001",
        "domicile": "IE",
        "accumulation_or_distribution": "accumulating",
        "expense_ratio": 0.0022,  # Fraction form
        "instrument_type": "etf_ucits",
    }

    citations = [
        {
            "url": "https://example.com/test",
            "tier": "primary",
            "proof_identifier": "IE00TEST0001",
            "proof_domicile": "IE",
            "proof_share_class": "accumulating",
            "proof_ter": 0.22,  # Percent form
            "factsheet_asof": "2026-02-28",
        }
    ]

    result = verify_candidate_proofs(
        candidate=candidate,
        citations=citations,
        now=datetime(2026, 3, 2, tzinfo=UTC),
        freshness_days_threshold=120,
    )

    assert result["proof_ter"] is True, "TER proof should pass despite different formats"
    assert result["verification_status"] == "verified"
    print("✅ PASS: TER proof handles percent form correctly")


def test_cusip_proof_us_etf():
    """US ETF with CUSIP should verify."""
    print("\n[TEST] CUSIP support for US ETF...")

    candidate = {
        "symbol": "BIL",
        "cusip": "78468R703",
        "domicile": "US",
        "accumulation_or_distribution": "accumulating",
        "expense_ratio": 0.0014,
        "instrument_type": "etf_us",
    }

    citations = [
        {
            "url": "https://example.com/bil",
            "tier": "primary",
            "proof_identifier": "78468R703",
            "proof_domicile": "US",
            "proof_share_class": "accumulating",
            "proof_ter": 0.0014,
            "factsheet_asof": "2026-02-28",
        }
    ]

    result = verify_candidate_proofs(
        candidate=candidate,
        citations=citations,
        now=datetime(2026, 3, 2, tzinfo=UTC),
        freshness_days_threshold=120,
    )

    assert result["proof_isin"] is True, "Identifier proof should pass (covers CUSIP)"
    assert result["verification_status"] == "verified"
    print("✅ PASS: CUSIP identifier support works")


def test_share_class_mismatch_fails():
    """Share class mismatch should fail proof."""
    print("\n[TEST] Share class mismatch detection...")

    candidate = {
        "symbol": "TEST",
        "isin": "IE00TEST0001",
        "domicile": "IE",
        "accumulation_or_distribution": "accumulating",
        "expense_ratio": 0.0020,
        "instrument_type": "etf_ucits",
    }

    citations = [
        {
            "url": "https://example.com/test",
            "tier": "primary",
            "proof_identifier": "IE00TEST0001",
            "proof_domicile": "IE",
            "proof_share_class": "distributing",  # Mismatch
            "proof_ter": 0.0020,
            "factsheet_asof": "2026-02-28",
        }
    ]

    result = verify_candidate_proofs(
        candidate=candidate,
        citations=citations,
        now=datetime(2026, 3, 2, tzinfo=UTC),
        freshness_days_threshold=120,
    )

    assert result["proof_share_class_match"] is False, "Share class mismatch should fail"
    assert "share_class_proven" in result["verification_missing"]
    assert result["verification_status"] == "partially_verified"
    print("✅ PASS: Share class mismatch correctly detected")


def main():
    print("=" * 70)
    print("MANUAL VERIFICATION TEST SUITE")
    print("Testing verification logic fixes")
    print("=" * 70)

    tests = [
        test_accumulating_etf_verified,
        test_distributing_etf_verified,
        test_ter_normalization,
        test_ter_proof_with_percent_citation,
        test_cusip_proof_us_etf,
        test_share_class_mismatch_fails,
    ]

    passed = 0
    failed = 0

    for test_func in tests:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            print(f"❌ FAIL: {e}")
            failed += 1
        except Exception as e:
            print(f"❌ ERROR: {e}")
            failed += 1

    print("\n" + "=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 70)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
