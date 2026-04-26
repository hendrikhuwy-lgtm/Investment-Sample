from __future__ import annotations

from typing import Any

from app.models.types import ConcentrationPolicy, RiskControlResult

EQUITY_CONCENTRATION_SLEEVES = {
    "global_equity_core",
    "developed_ex_us_optional",
    "emerging_markets",
    "china_satellite",
}
BOND_AND_CASH_SLEEVES = {"ig_bonds", "cash_bills"}


def get_concentration_policy(profile_type: str = "hnwi_sg") -> ConcentrationPolicy:
    normalized = str(profile_type or "hnwi_sg").strip().lower()
    if normalized != "hnwi_sg":
        normalized = "hnwi_sg"
    return ConcentrationPolicy(profile_type="hnwi_sg")


def evaluate_concentration_controls(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    target_weight_pct: float | None,
    policy: ConcentrationPolicy,
    warning_buffer: float = 0.90,
) -> list[dict[str, Any]]:
    results: list[RiskControlResult] = []

    single_fund_limit = _fraction_to_pct(policy.single_fund_max_weight)
    if single_fund_limit is None or target_weight_pct is None:
        results.append(
            RiskControlResult(
                status="unknown",
                metric_name="Single fund weight",
                current_value=target_weight_pct,
                policy_limit=single_fund_limit,
                rationale="Target sleeve weight is unavailable for single-fund concentration review.",
                blockers=["Target sleeve weight missing"] if target_weight_pct is None else [],
                provenance=["model-derived from sleeve target weight and hnwi_sg concentration policy"],
            )
        )
    else:
        results.append(
            _evaluate_upper_bound(
                metric_name="Single fund weight",
                current_value=target_weight_pct,
                limit_pct=single_fund_limit,
                warning_buffer=warning_buffer,
                rationale_pass="Target sleeve weight remains within diversified single-fund policy.",
                rationale_warn="Target sleeve weight is close to the single-fund concentration limit for the hnwi_sg profile.",
                rationale_fail="Target sleeve weight exceeds the single-fund concentration limit for the hnwi_sg profile.",
                provenance=["model-derived from sleeve target weight and hnwi_sg concentration policy"],
            )
        )

    country_weight = _optional_pct(candidate.get("us_weight_pct"))
    country_limit = _fraction_to_pct(policy.single_country_max_weight)
    if sleeve_key in {"global_equity_core", "developed_ex_us_optional"}:
        if country_weight is None:
            results.append(
                RiskControlResult(
                    status="unknown",
                    metric_name="Dominant country weight proxy",
                    current_value=None,
                    policy_limit=country_limit,
                    rationale="Country-concentration control cannot be evaluated from the current candidate metadata.",
                    blockers=["US weight missing"],
                    provenance=["candidate_metadata.us_weight_pct missing"],
                )
            )
        else:
            results.append(
                _evaluate_upper_bound(
                    metric_name="Dominant country weight proxy",
                    current_value=country_weight,
                    limit_pct=country_limit,
                    warning_buffer=warning_buffer,
                    rationale_pass="Available country-weight proxy remains inside the concentration guardrail.",
                    rationale_warn="Available country-weight proxy is approaching the concentration guardrail.",
                    rationale_fail="Available country-weight proxy exceeds the concentration guardrail.",
                    provenance=["candidate_metadata.us_weight_pct", "model-derived against hnwi_sg concentration policy"],
                )
            )

    sector_weight = _optional_pct(candidate.get("tech_weight_pct"))
    sector_limit = _fraction_to_pct(policy.single_sector_max_weight)
    if sleeve_key in EQUITY_CONCENTRATION_SLEEVES:
        if sector_weight is None:
            results.append(
                RiskControlResult(
                    status="unknown",
                    metric_name="Sector concentration proxy",
                    current_value=None,
                    policy_limit=sector_limit,
                    rationale="Sector concentration cannot be evaluated because sector tilt data is unavailable.",
                    blockers=["sector tilt data missing"],
                    provenance=["candidate_metadata.tech_weight_pct missing"],
                )
            )
        else:
            results.append(
                _evaluate_upper_bound(
                    metric_name="Sector concentration proxy",
                    current_value=sector_weight,
                    limit_pct=sector_limit,
                    warning_buffer=warning_buffer,
                    rationale_pass="Available sector-weight proxy remains within the sector concentration guardrail.",
                    rationale_warn="Available sector-weight proxy is close to the sector concentration guardrail.",
                    rationale_fail="Available sector-weight proxy exceeds the sector concentration guardrail.",
                    provenance=["candidate_metadata.tech_weight_pct", "model-derived against hnwi_sg concentration policy"],
                )
            )

    top10_weight = _optional_pct(candidate.get("top10_concentration_pct"))
    top10_limit = _fraction_to_pct(policy.top10_concentration_warning)
    if top10_weight is None and sleeve_key in EQUITY_CONCENTRATION_SLEEVES:
        results.append(
            RiskControlResult(
                status="unknown",
                metric_name="Top-10 concentration",
                current_value=None,
                policy_limit=top10_limit,
                rationale="Top-10 concentration cannot be compared because issuer concentration data is unavailable.",
                blockers=["issuer concentration unavailable", "top-10 concentration missing"],
                provenance=["candidate_metadata.top10_concentration_pct missing"],
            )
        )
    elif top10_weight is not None:
        if sleeve_key in BOND_AND_CASH_SLEEVES:
            if top10_weight >= top10_limit:
                status = "warn"
                rationale = "Top-10 concentration is elevated, but this is treated as a review signal rather than a hard block for bond or cash sleeves."
            else:
                status = "pass"
                rationale = "Top-10 concentration remains acceptable for bond or cash sleeve review."
            results.append(
                RiskControlResult(
                    status=status,
                    metric_name="Top-10 concentration",
                    current_value=top10_weight,
                    policy_limit=top10_limit,
                    rationale=rationale,
                    blockers=[],
                    provenance=["candidate_metadata.top10_concentration_pct", "sleeve-specific bond/cash calibration"],
                )
            )
        else:
            results.append(
                _evaluate_upper_bound(
                    metric_name="Top-10 concentration",
                    current_value=top10_weight,
                    limit_pct=top10_limit,
                    warning_buffer=warning_buffer,
                    rationale_pass="Top-10 concentration remains inside the warning band for the hnwi_sg profile.",
                    rationale_warn="Top-10 concentration is approaching the warning band for the hnwi_sg profile.",
                    rationale_fail="Top-10 concentration exceeds the warning band for the hnwi_sg profile.",
                    provenance=["candidate_metadata.top10_concentration_pct", "model-derived against hnwi_sg concentration policy"],
                )
            )

    if sleeve_key in {"global_equity_core"}:
        em_weight = _optional_pct(candidate.get("em_weight_pct"))
        em_min = _fraction_to_pct(policy.em_weight_band_min)
        em_max = _fraction_to_pct(policy.em_weight_band_max)
        if em_weight is None:
            results.append(
                RiskControlResult(
                    status="unknown",
                    metric_name="EM weight band",
                    current_value=None,
                    policy_limit=f"{em_min:.1f}-{em_max:.1f}%" if em_min is not None and em_max is not None else None,
                    rationale="EM weight band cannot be evaluated because EM weight is unavailable.",
                    blockers=["EM weight missing"],
                    provenance=["candidate_metadata.em_weight_pct missing"],
                )
            )
        elif em_min is not None and em_max is not None:
            if em_weight < em_min or em_weight > em_max:
                status = "fail"
                rationale = "EM weight sits outside the configured band for the hnwi_sg profile."
            elif em_weight <= em_min * (1 + (1 - warning_buffer)) or em_weight >= em_max * warning_buffer:
                status = "warn"
                rationale = "EM weight is near the configured band edge for the hnwi_sg profile."
            else:
                status = "pass"
                rationale = "EM weight sits inside the configured band for the hnwi_sg profile."
            results.append(
                RiskControlResult(
                    status=status,
                    metric_name="EM weight band",
                    current_value=em_weight,
                    policy_limit=f"{em_min:.1f}-{em_max:.1f}%",
                    rationale=rationale,
                    blockers=[],
                    provenance=["candidate_metadata.em_weight_pct", "model-derived against hnwi_sg concentration policy"],
                )
            )

    return [item.model_dump(mode="json") for item in results]


def summarize_concentration_status(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        return {"status": "unknown", "pass": 0, "warn": 0, "fail": 0, "unknown": 0}
    counts = {"pass": 0, "warn": 0, "fail": 0, "unknown": 0}
    for item in results:
        status = str(item.get("status") or "unknown")
        counts[status if status in counts else "unknown"] += 1
    overall = "pass"
    if counts["fail"]:
        overall = "fail"
    elif counts["warn"]:
        overall = "warn"
    elif counts["unknown"] and not counts["pass"]:
        overall = "unknown"
    elif counts["unknown"]:
        overall = "warn"
    return {"status": overall, **counts}


def _evaluate_upper_bound(
    *,
    metric_name: str,
    current_value: float,
    limit_pct: float | None,
    warning_buffer: float,
    rationale_pass: str,
    rationale_warn: str,
    rationale_fail: str,
    provenance: list[str],
) -> RiskControlResult:
    if limit_pct is None:
        return RiskControlResult(
            status="unknown",
            metric_name=metric_name,
            current_value=current_value,
            policy_limit=None,
            rationale="Policy limit is unavailable for this control.",
            blockers=["policy limit missing"],
            provenance=provenance,
        )
    if current_value > limit_pct:
        status = "fail"
        rationale = rationale_fail
    elif current_value >= limit_pct * warning_buffer:
        status = "warn"
        rationale = rationale_warn
    else:
        status = "pass"
        rationale = rationale_pass
    return RiskControlResult(
        status=status,
        metric_name=metric_name,
        current_value=current_value,
        policy_limit=limit_pct,
        rationale=rationale,
        blockers=[],
        provenance=provenance,
    )


def _optional_pct(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:  # noqa: BLE001
        return None


def _fraction_to_pct(value: float | None) -> float | None:
    if value is None:
        return None
    return float(value) * 100.0
