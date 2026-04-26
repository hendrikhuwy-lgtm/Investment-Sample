#!/usr/bin/env python3
"""
ETF Verification Tests - Verification Logic Fixes

Tests for verification logic fixes:
1. Distributing ETFs can be verified (not just accumulating)
2. TER normalization (percent vs fraction)
3. ISIN and CUSIP identifier support
4. Share class matching (accumulating OR distributing)
5. Fixture error messages are informative
"""

from datetime import UTC, datetime

import pytest

from app.services.verification import verify_candidate_proofs, _normalize_ter


class TestShareClassProof:
    """Test proof_share_class_match works for both accumulating and distributing."""

    def test_accumulating_etf_verified(self):
        """Accumulating ETF with matching citation should verify."""
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

        assert result["proof_isin"] is True
        assert result["proof_domicile"] is True
        assert result["proof_share_class_match"] is True
        assert result["proof_ter"] is True
        assert result["proof_factsheet_fresh"] is True
        assert result["verification_status"] == "verified"
        assert result["verification_missing"] == []

    def test_distributing_etf_verified(self):
        """Distributing ETF with matching citation should verify."""
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

        assert result["proof_share_class_match"] is True
        assert result["verification_status"] == "verified"
        assert "share_class_proven" not in result["verification_missing"]

    def test_share_class_mismatch_fails(self):
        """Share class mismatch should fail proof."""
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

        assert result["proof_share_class_match"] is False
        assert "share_class_proven" in result["verification_missing"]
        assert result["verification_status"] == "partially_verified"


class TestTERNormalization:
    """Test TER normalization handles percent vs fraction forms."""

    def test_normalize_ter_fraction_form(self):
        """TER in fraction form (0.0022) should remain unchanged."""
        assert _normalize_ter(0.0022) == 0.0022
        assert _normalize_ter(0.0020) == 0.0020
        assert _normalize_ter(0.0500) == 0.0500
        assert _normalize_ter(0.0001) == 0.0001

    def test_normalize_ter_percent_form(self):
        """TER in percent form (0.22) should convert to fraction."""
        assert _normalize_ter(0.22) == 0.0022
        assert _normalize_ter(0.20) == 0.0020
        assert _normalize_ter(5.00) == 0.0500
        assert _normalize_ter(1.50) == 0.0150

    def test_normalize_ter_threshold(self):
        """TER > 0.10 (10%) triggers percent-to-fraction conversion."""
        assert _normalize_ter(0.09) == 0.09  # Below threshold, stays
        assert _normalize_ter(0.10) == 0.10  # At threshold, stays
        assert _normalize_ter(0.11) == 0.0011  # Above threshold, converts
        assert _normalize_ter(0.15) == 0.0015

    def test_normalize_ter_none(self):
        """None should return None."""
        assert _normalize_ter(None) is None

    def test_ter_proof_with_percent_citation(self):
        """TER proof should pass when citation is in percent form."""
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

        assert result["proof_ter"] is True
        assert result["verification_status"] == "verified"

    def test_ter_proof_tolerance(self):
        """TER proof should use 1 basis point tolerance (0.0001)."""
        candidate = {
            "symbol": "TEST",
            "isin": "IE00TEST0001",
            "domicile": "IE",
            "accumulation_or_distribution": "accumulating",
            "expense_ratio": 0.0022,
            "instrument_type": "etf_ucits",
        }

        # Within tolerance (0.09 basis points)
        citations_close = [
            {
                "url": "https://example.com/test",
                "tier": "primary",
                "proof_identifier": "IE00TEST0001",
                "proof_domicile": "IE",
                "proof_share_class": "accumulating",
                "proof_ter": 0.00221,  # 0.09 basis point diff
                "factsheet_asof": "2026-02-28",
            }
        ]

        result = verify_candidate_proofs(
            candidate=candidate,
            citations=citations_close,
            now=datetime(2026, 3, 2, tzinfo=UTC),
            freshness_days_threshold=120,
        )

        assert result["proof_ter"] is True

    def test_ter_proof_outside_tolerance(self):
        """TER proof should fail when outside tolerance."""
        candidate = {
            "symbol": "TEST",
            "isin": "IE00TEST0001",
            "domicile": "IE",
            "accumulation_or_distribution": "accumulating",
            "expense_ratio": 0.0022,
            "instrument_type": "etf_ucits",
        }

        # Outside tolerance (2 basis points)
        citations_far = [
            {
                "url": "https://example.com/test",
                "tier": "primary",
                "proof_identifier": "IE00TEST0001",
                "proof_domicile": "IE",
                "proof_share_class": "accumulating",
                "proof_ter": 0.0042,  # 20 basis points off
                "factsheet_asof": "2026-02-28",
            }
        ]

        result = verify_candidate_proofs(
            candidate=candidate,
            citations=citations_far,
            now=datetime(2026, 3, 2, tzinfo=UTC),
            freshness_days_threshold=120,
        )

        assert result["proof_ter"] is False


class TestIdentifierProof:
    """Test ISIN and CUSIP identifier support."""

    def test_isin_proof_ucits_etf(self):
        """UCITS ETF with ISIN should verify."""
        candidate = {
            "symbol": "IWDA",
            "isin": "IE00B4L5Y983",
            "domicile": "IE",
            "accumulation_or_distribution": "accumulating",
            "expense_ratio": 0.0020,
            "instrument_type": "etf_ucits",
        }

        citations = [
            {
                "url": "https://example.com/iwda",
                "tier": "primary",
                "proof_identifier": "IE00B4L5Y983",
                "proof_domicile": "IE",
                "proof_share_class": "accumulating",
                "proof_ter": 0.0020,
                "factsheet_asof": "2026-01-31",
            }
        ]

        result = verify_candidate_proofs(
            candidate=candidate,
            citations=citations,
            now=datetime(2026, 3, 2, tzinfo=UTC),
            freshness_days_threshold=120,
        )

        assert result["proof_isin"] is True
        assert result["verification_status"] == "verified"

    def test_cusip_proof_us_etf(self):
        """US ETF with CUSIP should verify."""
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

        assert result["proof_isin"] is True  # Note: proof_isin covers all identifier types
        assert result["verification_status"] == "verified"

    def test_both_isin_and_cusip(self):
        """ETF with both ISIN and CUSIP should verify on either match."""
        candidate = {
            "symbol": "TEST",
            "isin": "IE00TEST0001",
            "cusip": "TEST12345",
            "domicile": "IE",
            "accumulation_or_distribution": "accumulating",
            "expense_ratio": 0.0020,
            "instrument_type": "etf_ucits",
        }

        # Citation has CUSIP only
        citations_cusip = [
            {
                "url": "https://example.com/test",
                "tier": "primary",
                "proof_identifier": "TEST12345",
                "proof_domicile": "IE",
                "proof_share_class": "accumulating",
                "proof_ter": 0.0020,
                "factsheet_asof": "2026-02-28",
            }
        ]

        result = verify_candidate_proofs(
            candidate=candidate,
            citations=citations_cusip,
            now=datetime(2026, 3, 2, tzinfo=UTC),
            freshness_days_threshold=120,
        )

        assert result["proof_isin"] is True

        # Citation has ISIN only
        citations_isin = [
            {
                "url": "https://example.com/test",
                "tier": "primary",
                "proof_identifier": "IE00TEST0001",
                "proof_domicile": "IE",
                "proof_share_class": "accumulating",
                "proof_ter": 0.0020,
                "factsheet_asof": "2026-02-28",
            }
        ]

        result2 = verify_candidate_proofs(
            candidate=candidate,
            citations=citations_isin,
            now=datetime(2026, 3, 2, tzinfo=UTC),
            freshness_days_threshold=120,
        )

        assert result2["proof_isin"] is True

    def test_identifier_mismatch_fails(self):
        """Wrong identifier should fail proof."""
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
                "proof_identifier": "IE00WRONG999",  # Wrong ISIN
                "proof_domicile": "IE",
                "proof_share_class": "accumulating",
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

        assert result["proof_isin"] is False
        assert "isin_proven" in result["verification_missing"]


class TestFixtureFallback:
    """Test fixture missing behavior is informative."""

    def test_missing_fixture_error_message(self):
        """Missing fixture should return clear error with instructions."""
        from app.services.etf_doc_parser import fetch_candidate_docs

        result = fetch_candidate_docs("NONEXISTENT", use_fixtures=True)

        assert result["status"] == "failed"
        assert "Fixture not found" in result["error"]
        assert "build_factsheet_fixtures.py" in result["error"]
        assert "expected_fixture_path" in result
        assert "NONEXISTENT" in result["error"]

    def test_existing_fixture_success(self):
        """Existing fixture should load successfully."""
        from app.services.etf_doc_parser import fetch_candidate_docs

        result = fetch_candidate_docs("VWRA", use_fixtures=True)

        assert result["status"] != "failed"
        assert result.get("verified") is True
        assert result["extracted"]["isin"] == "IE00BK5BQT80"


class TestNonETFInstruments:
    """Test that non-ETF instruments are handled correctly."""

    def test_cash_account_partial_verification(self):
        """Cash accounts should always be partially_verified."""
        candidate = {
            "symbol": "SGD_CASH",
            "instrument_type": "cash_account_sg",
        }

        result = verify_candidate_proofs(
            candidate=candidate,
            citations=[],
            now=datetime(2026, 3, 2, tzinfo=UTC),
            freshness_days_threshold=120,
        )

        assert result["verification_status"] == "partially_verified"
        assert "proof_scope_etf_only" in result["verification_missing"]
        assert "yield_not_sourced" in result["verification_missing"]

    def test_long_put_overlay_partial_verification(self):
        """Options strategies should be partially_verified."""
        candidate = {
            "symbol": "SPX_PUTS",
            "instrument_type": "long_put_overlay_strategy",
        }

        result = verify_candidate_proofs(
            candidate=candidate,
            citations=[],
            now=datetime(2026, 3, 2, tzinfo=UTC),
            freshness_days_threshold=120,
        )

        assert result["verification_status"] == "partially_verified"
        assert "proof_scope_etf_only" in result["verification_missing"]
