from __future__ import annotations

from dataclasses import dataclass

from app.models.types import InstrumentTaxProfile, TaxResidencyProfile


@dataclass(frozen=True)
class TaxScore:
    instrument_id: str
    score: float
    withholding_drag: float
    estate_risk_penalty: float
    rationale: str


def evaluate_instrument_for_sg(
    residency: TaxResidencyProfile, instrument: InstrumentTaxProfile
) -> TaxScore:
    if residency.tax_residency != "SG":
        raise ValueError("This engine currently supports SG profile only")

    withholding_penalty = instrument.expected_withholding_rate * 100
    expense_penalty = instrument.expense_ratio * 100
    liquidity_bonus = instrument.liquidity_score * 10
    estate_risk_penalty = 10.0 if instrument.us_situs_risk_flag else 0.0

    score = max(
        0.0,
        100.0 - withholding_penalty - expense_penalty - estate_risk_penalty + liquidity_bonus,
    )

    rationale = (
        f"withholding={instrument.expected_withholding_rate:.2%}, "
        f"expense={instrument.expense_ratio:.2%}, liquidity={instrument.liquidity_score:.2f}, "
        f"estate_risk={instrument.us_situs_risk_flag}"
    )

    return TaxScore(
        instrument_id=instrument.instrument_id,
        score=round(score, 2),
        withholding_drag=round(withholding_penalty, 2),
        estate_risk_penalty=estate_risk_penalty,
        rationale=rationale,
    )


def compare_equivalent_exposures(
    residency: TaxResidencyProfile,
    candidates: list[InstrumentTaxProfile],
) -> list[TaxScore]:
    scored = [evaluate_instrument_for_sg(residency, instrument) for instrument in candidates]
    return sorted(scored, key=lambda item: item.score, reverse=True)


def needs_tax_drag_alert(previous_drag: float, current_drag: float, threshold_bps: float = 20) -> bool:
    return (current_drag - previous_drag) * 100 >= threshold_bps


def build_sg_tax_truth(
    *,
    domicile: str | None,
    expected_withholding_rate: float | None,
    us_situs_risk_flag: bool,
    accumulation_or_distribution: str | None = None,
    instrument_type: str | None = None,
) -> dict[str, str | float | None]:
    normalized_domicile = str(domicile or "").upper()
    withholding_posture: str | None
    if expected_withholding_rate is None:
        withholding_posture = None
    elif expected_withholding_rate <= 0.0:
        withholding_posture = "No expected withholding drag under current SG lens assumptions."
    else:
        withholding_posture = f"Expected withholding drag approximately {expected_withholding_rate:.2%} for SG lens review."

    estate_risk_posture = "US situs estate risk applies." if us_situs_risk_flag else "No US situs estate-risk flag."

    wrapper_notes: str | None = None
    if instrument_type in {"etf_ucits", "etf_us"}:
        if normalized_domicile == "IE":
            wrapper_notes = "Ireland-domiciled ETF wrapper is generally preferred for SG implementation review."
        elif normalized_domicile == "US":
            wrapper_notes = "US-domiciled ETF wrapper increases SG estate and withholding review importance."
        elif normalized_domicile:
            wrapper_notes = f"{normalized_domicile}-domiciled wrapper requires sleeve-specific SG review."

    distribution_mechanics: str | None = None
    normalized_distribution = str(accumulation_or_distribution or "").strip().lower()
    if normalized_distribution == "accumulating":
        distribution_mechanics = "Accumulating share class; income is reinvested."
    elif normalized_distribution in {"distributing", "distribution"}:
        distribution_mechanics = "Distributing share class; income is paid out."

    return {
        "withholding_tax_posture": withholding_posture,
        "estate_risk_posture": estate_risk_posture,
        "wrapper_notes": wrapper_notes,
        "distribution_mechanics": distribution_mechanics,
    }
