from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.models.types import (
    InstrumentTaxProfile,
    PersonalPortfolioDiagnostic,
    PortfolioHolding,
    TaxResidencyProfile,
)
from app.services.portfolio_state import DEFAULT_POLICY_WEIGHTS, holding_market_value, holdings_total_value
from app.services.stress_engine import run_stress_scenarios
from app.services.tax_engine import evaluate_instrument_for_sg


IE_SYMBOLS = {"CSPX", "IWDA", "VWRA", "VWRD", "AGGU", "IGLA", "IWDP", "SGLN", "IGLN"}
SG_SYMBOLS = {"A35"}


def _weights_by_sleeve(holdings: list[PortfolioHolding]) -> tuple[dict[str, float], float]:
    total_value = holdings_total_value(holdings)
    bucket = {key: 0.0 for key in DEFAULT_POLICY_WEIGHTS}
    for holding in holdings:
        bucket[holding.sleeve] = bucket.get(holding.sleeve, 0.0) + holding_market_value(holding)
    if total_value <= 0:
        return {key: 0.0 for key in bucket}, 0.0
    return {key: value / total_value for key, value in bucket.items()}, total_value


def compute_allocation_drift(
    holdings: list[PortfolioHolding],
    policy_weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    policy = policy_weights or DEFAULT_POLICY_WEIGHTS
    actual, total_value = _weights_by_sleeve(holdings)
    drift = {sleeve: round(actual.get(sleeve, 0.0) - float(target), 4) for sleeve, target in policy.items()}
    return {
        "total_value": total_value,
        "actual_weights": actual,
        "policy_weights": policy,
        "drift": drift,
    }


def _region_for_holding(holding: PortfolioHolding) -> str:
    symbol = holding.symbol.upper()
    if holding.sleeve == "cash":
        return "Cash"
    if symbol in SG_SYMBOLS or holding.currency.upper() == "SGD":
        return "Singapore"
    if symbol in IE_SYMBOLS:
        return "Global UCITS"
    if symbol in {"VWRA", "VWRD", "IWDA"}:
        return "Global Developed"
    return "United States"


def compute_concentration_metrics(holdings: list[PortfolioHolding]) -> dict[str, Any]:
    total_value = holdings_total_value(holdings)
    if total_value <= 0:
        return {
            "top5_positions_pct": 0.0,
            "single_name_risk_pct": 0.0,
            "region_exposure_pct": {},
            "currency_exposure_pct": {},
            "top_positions": [],
        }

    positions = []
    region_values: dict[str, float] = {}
    currency_values: dict[str, float] = {}
    for holding in holdings:
        value = holding_market_value(holding)
        positions.append((holding.symbol, value))
        region = _region_for_holding(holding)
        region_values[region] = region_values.get(region, 0.0) + value
        currency = holding.currency.upper()
        currency_values[currency] = currency_values.get(currency, 0.0) + value

    positions.sort(key=lambda item: item[1], reverse=True)
    top5_value = sum(value for _, value in positions[:5])
    max_single = positions[0][1] if positions else 0.0
    top_positions = [
        {"symbol": symbol, "weight_pct": round((value / total_value) * 100.0, 2)}
        for symbol, value in positions[:5]
    ]
    return {
        "top5_positions_pct": round((top5_value / total_value) * 100.0, 2),
        "single_name_risk_pct": round((max_single / total_value) * 100.0, 2),
        "region_exposure_pct": {
            key: round((value / total_value) * 100.0, 2)
            for key, value in sorted(region_values.items(), key=lambda item: item[1], reverse=True)
        },
        "currency_exposure_pct": {
            key: round((value / total_value) * 100.0, 2)
            for key, value in sorted(currency_values.items(), key=lambda item: item[1], reverse=True)
        },
        "top_positions": top_positions,
    }


def _instrument_tax_profile(holding: PortfolioHolding) -> InstrumentTaxProfile:
    symbol = holding.symbol.upper()
    if symbol in IE_SYMBOLS:
        domicile = "IE"
        withholding = 0.15
        estate_risk = False
    elif symbol in SG_SYMBOLS:
        domicile = "SG"
        withholding = 0.0
        estate_risk = False
    else:
        domicile = "US"
        withholding = 0.30 if holding.sleeve in {"global_equity", "real_asset", "ig_bond"} else 0.0
        estate_risk = True

    expense_ratio = 0.002 if holding.sleeve in {"global_equity", "ig_bond"} else 0.004
    if holding.sleeve in {"alt", "convex"}:
        expense_ratio = 0.007
    if holding.sleeve == "cash":
        expense_ratio = 0.0005

    return InstrumentTaxProfile(
        instrument_id=holding.holding_id,
        domicile=domicile,
        us_dividend_exposure=holding.sleeve in {"global_equity", "real_asset", "ig_bond"},
        expected_withholding_rate=withholding,
        us_situs_risk_flag=estate_risk,
        expense_ratio=expense_ratio,
        liquidity_score=0.80,
    )


def compute_tax_drag_estimate(holdings: list[PortfolioHolding]) -> dict[str, Any]:
    total_value = holdings_total_value(holdings)
    if total_value <= 0:
        return {"tax_drag_estimate_pct_nav": 0.0, "weighted_tax_score": 0.0, "instrument_breakdown": []}

    profile = TaxResidencyProfile(
        profile_id="sg_personal_portfolio",
        tax_residency="SG",
        base_currency="SGD",
        dta_flags={"ireland_us_treaty_path": True},
        estate_risk_flags={"us_situs_cap_enabled": True},
    )

    weighted_score = 0.0
    weighted_drag = 0.0
    breakdown: list[dict[str, Any]] = []
    for holding in holdings:
        value = holding_market_value(holding)
        if value <= 0:
            continue
        weight = value / total_value
        score = evaluate_instrument_for_sg(profile, _instrument_tax_profile(holding))
        weighted_score += score.score * weight
        # Approximate drag as withholding+expense+risk penalty in percentage points.
        drag_pct_points = score.withholding_drag + (score.estate_risk_penalty * 0.25)
        weighted_drag += drag_pct_points * weight
        breakdown.append(
            {
                "symbol": holding.symbol,
                "weight_pct": round(weight * 100.0, 2),
                "tax_score": score.score,
                "drag_proxy_pct_points": round(drag_pct_points, 2),
                "rationale": score.rationale,
            }
        )

    return {
        "tax_drag_estimate_pct_nav": round(weighted_drag / 100.0, 4),
        "weighted_tax_score": round(weighted_score, 2),
        "instrument_breakdown": breakdown,
    }


def compute_convex_coverage(holdings: list[PortfolioHolding]) -> dict[str, Any]:
    total_value = holdings_total_value(holdings)
    if total_value <= 0:
        return {
            "convex_notional": 0.0,
            "hedge_coverage_pct": 0.0,
            "premium_pct_nav": 0.0,
            "target_check": {
                "managed_futures": {"target": 0.02, "actual": 0.0, "within_target": False},
                "tail_hedge": {"target": 0.007, "actual": 0.0, "within_target": False},
                "long_put": {"target": 0.003, "actual": 0.0, "within_target": False},
            },
            "compliant": False,
            "margin_required": False,
            "max_loss_known": True,
        }

    managed_value = 0.0
    tail_value = 0.0
    put_value = 0.0
    convex_value = 0.0
    for holding in holdings:
        value = holding_market_value(holding)
        if holding.sleeve != "convex":
            continue
        symbol = holding.symbol.upper()
        convex_value += value
        if symbol in {"DBMF", "KMLM"}:
            managed_value += value
        elif symbol in {"TAIL", "CAOS", "BTAL"}:
            tail_value += value
        elif "PUT" in symbol:
            put_value += value
        else:
            managed_value += value

    managed_w = managed_value / total_value
    tail_w = tail_value / total_value
    put_w = put_value / total_value
    tol = 0.0025
    target_check = {
        "managed_futures": {"target": 0.02, "actual": round(managed_w, 4), "within_target": abs(managed_w - 0.02) <= tol},
        "tail_hedge": {"target": 0.007, "actual": round(tail_w, 4), "within_target": abs(tail_w - 0.007) <= tol},
        "long_put": {"target": 0.003, "actual": round(put_w, 4), "within_target": abs(put_w - 0.003) <= tol},
    }
    compliant = all(item["within_target"] for item in target_check.values())
    return {
        "convex_notional": round(convex_value, 2),
        "hedge_coverage_pct": round((convex_value / total_value) * 100.0, 2),
        "premium_pct_nav": round((put_value / total_value) * 100.0, 2),
        "target_check": target_check,
        "compliant": compliant,
        "margin_required": False,
        "max_loss_known": True,
    }


def compute_regime_alignment_score(
    holdings: list[PortfolioHolding],
    macro_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    macro = macro_context or {}
    long_state = str(macro.get("long_state", "Normal"))
    short_state = str(macro.get("short_state", "Normal"))
    graph_rows = list(macro.get("graph_metadata", []))
    actual_weights, _ = _weights_by_sleeve(holdings)

    score = 100.0
    factors: list[str] = []
    eq_w = float(actual_weights.get("global_equity", 0.0))
    convex_w = float(actual_weights.get("convex", 0.0))
    ig_w = float(actual_weights.get("ig_bond", 0.0))

    if long_state.lower() == "alert" and eq_w > 0.55:
        penalty = min(20.0, (eq_w - 0.55) * 80.0)
        score -= penalty
        factors.append("Long-horizon alert with elevated equity exposure reduces alignment.")
    if short_state.lower() == "alert" and convex_w < 0.02:
        score -= 8.0
        factors.append("Short-horizon alert with lower convex weight reduces alignment.")
    if convex_w >= 0.02:
        score += 3.0
        factors.append("Convex sleeve presence supports stress-resilience alignment.")
    if ig_w < 0.10 and long_state.lower() != "normal":
        score -= 4.0
        factors.append("Lower IG bond share in non-normal regime may reduce ballast alignment.")

    for row in graph_rows:
        if str(row.get("series_code", "")).upper() == "VIXCLS":
            long = row.get("long_horizon", {})
            pct = float(long.get("percentile_5y", 0.0))
            if pct >= 80 and eq_w > 0.50:
                score -= 5.0
                factors.append("High volatility percentile with high equity weight moderates alignment score.")
        if str(row.get("series_code", "")).upper() == "BAMLH0A0HYM2":
            long = row.get("long_horizon", {})
            pct = float(long.get("percentile_5y", 0.0))
            if pct >= 75 and ig_w < 0.15:
                score -= 3.0
                factors.append("Elevated credit stress percentile with low IG allocation lowers alignment.")

    bounded = max(0.0, min(100.0, score))
    if not factors:
        factors.append("Current exposures are broadly consistent with regime diagnostics.")

    return {
        "score": round(bounded, 2),
        "factors": factors,
        "long_state": long_state,
        "short_state": short_state,
    }


def build_personal_portfolio_diagnostic(
    holdings: list[PortfolioHolding],
    macro_context: dict[str, Any] | None = None,
    policy_weights: dict[str, float] | None = None,
) -> PersonalPortfolioDiagnostic:
    allocation = compute_allocation_drift(holdings, policy_weights=policy_weights)
    concentration = compute_concentration_metrics(holdings)
    tax_drag = compute_tax_drag_estimate(holdings)
    convex = compute_convex_coverage(holdings)
    alignment = compute_regime_alignment_score(holdings, macro_context=macro_context)
    scenarios = run_stress_scenarios(allocation["actual_weights"])

    return PersonalPortfolioDiagnostic(
        created_at=datetime.now(UTC),
        total_value=float(allocation["total_value"]),
        policy_weights=allocation["policy_weights"],
        actual_weights=allocation["actual_weights"],
        allocation_drift=allocation["drift"],
        concentration_metrics=concentration,
        tax_drag_estimate=float(tax_drag["tax_drag_estimate_pct_nav"]),
        convex_coverage=convex,
        regime_alignment_score=float(alignment["score"]),
        regime_alignment_diagnostic=alignment,
        stress_scenarios=scenarios,
    )
