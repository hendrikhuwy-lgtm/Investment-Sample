from __future__ import annotations

from dataclasses import dataclass

from app.models.types import ConvexSleevePosition


@dataclass(frozen=True)
class ConvexValidationResult:
    valid: bool
    total_weight: float
    errors: list[str]


def validate_retail_safe_convex(positions: list[ConvexSleevePosition]) -> ConvexValidationResult:
    errors: list[str] = []

    total_weight = sum(position.allocation_weight for position in positions)
    if round(total_weight, 6) > 0.03:
        errors.append("Convex sleeve exceeds 3% target allocation")

    type_weights = {"managed_futures_etf": 0.0, "tail_hedge_fund": 0.0, "long_put_option": 0.0}

    for position in positions:
        type_weights[position.instrument_type] += position.allocation_weight

        if not position.retail_accessible:
            errors.append(f"{position.symbol} is not retail-accessible")
        if position.margin_required:
            errors.append(f"{position.symbol} requires margin; forbidden by policy")
        if not position.max_loss_known:
            errors.append(f"{position.symbol} has unknown max loss; forbidden by policy")

    if abs(type_weights["managed_futures_etf"] - 0.02) > 0.005:
        errors.append("Managed futures sleeve should be near 2.0%")
    if abs(type_weights["tail_hedge_fund"] - 0.007) > 0.005:
        errors.append("Tail hedge sleeve should be near 0.7%")
    if abs(type_weights["long_put_option"] - 0.003) > 0.003:
        errors.append("Long put sleeve should be near 0.3%")

    return ConvexValidationResult(valid=not errors, total_weight=total_weight, errors=errors)


def carry_budget_breach(carry_annualized: float, budget: float = 0.008) -> bool:
    return carry_annualized > budget
