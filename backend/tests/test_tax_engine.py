from app.models.types import InstrumentTaxProfile, TaxResidencyProfile
from app.services.tax_engine import build_sg_tax_truth, compare_equivalent_exposures


def test_tax_engine_prefers_lower_withholding_and_less_estate_risk() -> None:
    profile = TaxResidencyProfile(
        profile_id="sg",
        tax_residency="SG",
        base_currency="SGD",
        dta_flags={"x": True},
        estate_risk_flags={"us_situs_cap_enabled": True},
    )

    candidates = [
        InstrumentTaxProfile(
            instrument_id="us",
            domicile="US",
            us_dividend_exposure=True,
            expected_withholding_rate=0.30,
            us_situs_risk_flag=True,
            expense_ratio=0.0003,
            liquidity_score=0.99,
        ),
        InstrumentTaxProfile(
            instrument_id="ie",
            domicile="IE",
            us_dividend_exposure=True,
            expected_withholding_rate=0.15,
            us_situs_risk_flag=False,
            expense_ratio=0.0007,
            liquidity_score=0.90,
        ),
    ]

    ranked = compare_equivalent_exposures(profile, candidates)
    assert ranked[0].instrument_id == "ie"


def test_build_sg_tax_truth_formats_posture_and_wrapper_notes() -> None:
    payload = build_sg_tax_truth(
        domicile="IE",
        expected_withholding_rate=0.15,
        us_situs_risk_flag=False,
        accumulation_or_distribution="accumulating",
        instrument_type="etf_ucits",
    )
    assert "15.00%" in str(payload["withholding_tax_posture"])
    assert "No US situs" in str(payload["estate_risk_posture"])
    assert "Ireland-domiciled ETF wrapper" in str(payload["wrapper_notes"])
    assert "Accumulating share class" in str(payload["distribution_mechanics"])
