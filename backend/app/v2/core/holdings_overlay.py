from __future__ import annotations

import copy

from app.v2.core.domain_objects import CandidateAssessment, PortfolioTruth


def apply_overlay(base_contract: dict, holdings: object) -> dict:
    """
    Applies portfolio holdings overlay to a base surface contract.
    When holdings is None: returns base_contract UNCHANGED.
    When holdings present: adds holdings_overlay fields to a copy of base_contract.
    Must never mutate base_contract.
    """
    result = copy.deepcopy(base_contract)
    if holdings is None:
        result.setdefault("holdings_overlay_present", False)
        return result

    result["holdings_overlay_present"] = True
    result["holdings_overlay"] = copy.deepcopy(holdings)
    return result


def apply_holdings_overlay(candidate: CandidateAssessment, portfolio: PortfolioTruth | None) -> CandidateAssessment:
    if portfolio is None:
        return candidate

    matched_holding = next(
        (holding for holding in portfolio.holdings if str(holding.get("symbol") or "").upper() == candidate.instrument.symbol.upper()),
        None,
    )
    overlay = dict(candidate.holdings_context)
    overlay["portfolio_id"] = portfolio.portfolio_id
    overlay["is_current_holding"] = matched_holding is not None
    overlay["current_weight"] = float(matched_holding.get("weight") or 0.0) if matched_holding else 0.0
    overlay["current_cost_basis"] = matched_holding.get("cost_basis") if matched_holding else None
    overlay["turnover_note"] = (
        "Already held; any change should clear a higher evidence bar."
        if matched_holding
        else "Not currently held; portfolio funding source must still be explicit."
    )
    return candidate.model_copy(update={"holdings_context": overlay})
