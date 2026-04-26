from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.v2.core.domain_objects import PortfolioTruth


def translate(holdings_data: Any) -> "PortfolioTruth":
    """Translates holdings/pricing data from portfolio_state → PortfolioTruth."""
    raise NotImplementedError("Translator not yet implemented")
