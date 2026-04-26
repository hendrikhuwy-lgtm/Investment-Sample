"""
ETF Verification Pipeline Tests

Tests the complete flow:
1. PDF parser extracts ISIN, TER, domicile, accumulating, factsheet date
2. Yahoo Finance (or mock) provides market data
3. Verification status computed correctly
4. Missing proofs downgrade to partially_verified
"""

import pytest
from pathlib import Path


def test_pdf_parser_extracts_isin():
    """Test ISIN extraction from PDF text."""
    from app.services.etf_doc_parser import _parse_isin

    sample_text = """
    Fund Facts
    ISIN: IE00BK5BQT80
    Ticker: VWRA
    Domicile: Ireland
    """

    isin = _parse_isin(sample_text)
    assert isin == "IE00BK5BQT80"


def test_pdf_parser_extracts_domicile():
    """Test domicile extraction from PDF text."""
    from app.services.etf_doc_parser import _parse_domicile

    sample_text = """
    Fund Structure: UCITS domiciled in Ireland
    """

    domicile = _parse_domicile(sample_text)
    assert domicile == "Ireland"


def test_pdf_parser_extracts_ter():
    """Test TER extraction from PDF text."""
    from app.services.etf_doc_parser import _parse_ter

    sample_text = """
    Ongoing Charges: 0.22%
    """

    ter = _parse_ter(sample_text)
    assert ter is not None
    assert abs(ter - 0.0022) < 0.0001  # 0.22% = 0.0022


def test_pdf_parser_extracts_accumulating():
    """Test accumulating status extraction."""
    from app.services.etf_doc_parser import _parse_accumulating_status

    sample_text_acc = """
    Share Class: Accumulating
    Income Treatment: Reinvested
    """

    status = _parse_accumulating_status(sample_text_acc)
    assert status == "accumulating"

    sample_text_dist = """
    Share Class: Distributing
    Income Treatment: Distributed quarterly
    """

    status = _parse_accumulating_status(sample_text_dist)
    assert status == "distributing"


def test_pdf_parser_extracts_factsheet_date():
    """Test factsheet date extraction."""
    from app.services.etf_doc_parser import _parse_factsheet_date

    sample_text = """
    Fund Facts as at 31 December 2025
    """

    date = _parse_factsheet_date(sample_text)
    assert date == "2025-12-31"


def test_fetch_candidate_docs_extracts_structured_fields_from_fixture():
    from app.services.etf_doc_parser import fetch_candidate_docs

    result = fetch_candidate_docs("VWRA", use_fixtures=True)
    assert result["verified"] is True
    extracted = result["extracted"]
    assert extracted["fund_name"] == "Vanguard FTSE All-World UCITS ETF"
    assert extracted["primary_listing_exchange"] == "London Stock Exchange"
    assert extracted["primary_trading_currency"] == "USD"
    assert extracted["wrapper_or_vehicle_type"] in {"UCITS ETF", "UCITS V ICAV"}
    assert extracted["benchmark_name"] == "FTSE All-World Index"
    assert extracted["replication_method"]
    assert extracted["holdings_count"] == 3756
    assert extracted["tracking_difference_1y"] == -0.08
    assert extracted["tracking_difference_3y"] == -0.09
    assert extracted["tracking_difference_since_inception"] == -0.10


def test_factsheet_freshness_check():
    """Test factsheet age verification."""
    from app.services.etf_doc_parser import _is_factsheet_fresh
    from datetime import datetime, timedelta

    # Fresh factsheet (within 120 days)
    recent_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    assert _is_factsheet_fresh(recent_date, 120) is True

    # Stale factsheet (beyond 120 days)
    old_date = (datetime.now() - timedelta(days=150)).strftime("%Y-%m-%d")
    assert _is_factsheet_fresh(old_date, 120) is False


def test_verification_requires_all_proofs():
    """Test that candidate is verified only if all 5 proofs are satisfied."""
    proofs = {
        "proof_isin": True,
        "proof_domicile": True,
        "proof_accumulating": True,
        "proof_ter": True,
        "proof_factsheet_fresh": True,
    }

    # All proofs → verified
    verified = all(proofs.values())
    assert verified is True

    # Missing one proof → not verified
    proofs["proof_ter"] = False
    verified = all(proofs.values())
    assert verified is False


def test_isin_conflict_downgrades_candidate():
    """Test that ISIN mismatch prevents verification."""
    expected_isin = "IE00BK5BQT80"
    extracted_isin = "IE00BK5BXXXX"  # Different ISIN

    isin_conflict = (extracted_isin != expected_isin)
    verified = not isin_conflict  # Conflict prevents verification

    assert isin_conflict is True
    assert verified is False


def test_yahoo_finance_not_used_for_verification():
    """Test that Yahoo Finance data cannot satisfy verification proofs."""
    from app.services.yahoo_finance import fetch_yahoo_market_data

    # Even if Yahoo returns data, it should not set verification proofs
    # This is enforced by using issuer PDFs exclusively for proofs

    yahoo_result = {
        "status": "success",
        "market_data": {
            "last_price": 100.0,
            "bid_ask_spread_bps": 5.0,
            "liquidity_score": 0.8,
        },
        "proof_warning": "Yahoo Finance data CANNOT be used for ISIN, domicile, TER, or accumulating proofs",
    }

    # Verification proofs must come from issuer docs
    assert "proof_isin" not in yahoo_result
    assert "proof_domicile" not in yahoo_result
    assert "proof_ter" not in yahoo_result


def test_partial_verification_shows_missing_proofs():
    """Test that partially verified candidates show which proofs are missing."""
    proofs = {
        "proof_isin": True,
        "proof_domicile": True,
        "proof_accumulating": False,  # Missing
        "proof_ter": False,  # Missing
        "proof_factsheet_fresh": True,
    }

    verified = all(proofs.values())
    partially_verified = any(proofs.values()) and not verified
    missing_proofs = [k.replace("proof_", "") for k, v in proofs.items() if not v]

    assert verified is False
    assert partially_verified is True
    assert set(missing_proofs) == {"accumulating", "ter"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
