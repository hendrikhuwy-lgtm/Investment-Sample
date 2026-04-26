from __future__ import annotations

from datetime import UTC, datetime

from app.services.verification import verify_candidate_proofs


def _base_candidate() -> dict:
    return {
        "symbol": "VWRA",
        "instrument_type": "etf_ucits",
        "domicile": "IE",
        "accumulation_or_distribution": "accumulating",
        "expense_ratio": 0.0022,
        "share_class_id": "VWRA",
    }


def test_candidate_missing_isin_proof_is_not_verified() -> None:
    candidate = _base_candidate()
    citations = [
        {
            "url": "https://issuer.example/fund",
            "tier": "primary",
            "proof_domicile": "IE",
            "proof_share_class": "accumulating",
            "proof_ter": 0.0022,
            "factsheet_asof": "2026-02-15",
        }
    ]
    result = verify_candidate_proofs(candidate, citations, datetime(2026, 3, 2, tzinfo=UTC), 120)
    assert result["verification_status"] != "verified"
    assert "isin_proven" in result["verification_missing"]


def test_candidate_missing_factsheet_date_is_not_verified() -> None:
    candidate = _base_candidate()
    citations = [
        {
            "url": "https://issuer.example/fund",
            "tier": "primary",
            "proof_identifier": "VWRA",
            "proof_domicile": "IE",
            "proof_share_class": "accumulating",
            "proof_ter": 0.0022,
        }
    ]
    result = verify_candidate_proofs(candidate, citations, datetime(2026, 3, 2, tzinfo=UTC), 120)
    assert result["verification_status"] != "verified"
    assert "factsheet_fresh" in result["verification_missing"]


def test_candidate_with_all_proofs_and_fresh_date_is_verified() -> None:
    candidate = _base_candidate()
    citations = [
        {
            "url": "https://issuer.example/fund",
            "tier": "primary",
            "proof_identifier": "VWRA",
            "proof_domicile": "IE",
            "proof_share_class": "accumulating",
            "proof_ter": 0.0022,
            "factsheet_asof": "2026-02-20",
        }
    ]
    result = verify_candidate_proofs(candidate, citations, datetime(2026, 3, 2, tzinfo=UTC), 120)
    assert result["verification_status"] == "verified"
    assert result["verification_missing"] == []
