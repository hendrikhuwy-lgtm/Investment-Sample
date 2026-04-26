from __future__ import annotations

from collections import defaultdict

from app.config import get_db_path
from app.models.db import connect
from app.services.portfolio_state import holdings_total_value
from app.v2.core.domain_objects import PortfolioTruth
from app.v2.donors.portfolio import SQLitePortfolioDonor


def _portfolio_id(account_id: str) -> str:
    normalized = str(account_id or "default").strip().lower().replace(" ", "_").replace("-", "_")
    return f"portfolio_{normalized or 'default'}"


def _holdings_payload(holdings: list[object]) -> list[dict[str, object]]:
    return [holding.model_dump(mode="json") for holding in holdings]


def _exposures_from_holdings(holdings: list[object]) -> dict[str, float]:
    total_value = holdings_total_value(holdings)
    if total_value <= 0:
        return {}

    sleeve_values: dict[str, float] = defaultdict(float)
    for holding in holdings:
        sleeve_values[str(holding.sleeve)] += float(holding.quantity) * float(holding.cost_basis)

    return {
        sleeve_id: round(value / total_value, 4)
        for sleeve_id, value in sorted(sleeve_values.items())
        if value > 0
    }


def get_portfolio_truth(account_id: str) -> PortfolioTruth:
    """Returns PortfolioTruth from the portfolio_state donor with null-safe holdings handling."""
    conn = connect(get_db_path())
    try:
        donor = SQLitePortfolioDonor(conn)
        holdings = donor.list_holdings()
        if not holdings:
            return PortfolioTruth(
                portfolio_id=_portfolio_id(account_id),
                name=f"{account_id or 'Default'} Portfolio",
                base_currency="USD",
            )

        snapshot = donor.latest_snapshot()
        exposures = dict(snapshot.sleeve_weights) if snapshot is not None else _exposures_from_holdings(holdings)
        base_currency = next(
            (
                str(holding.currency or "").upper()
                for holding in holdings
                if str(holding.currency or "").strip()
            ),
            "USD",
        )
        as_of = (
            snapshot.created_at.isoformat()
            if snapshot is not None
            else max(holding.updated_at for holding in holdings).isoformat()
        )

        return PortfolioTruth(
            portfolio_id=_portfolio_id(account_id),
            name=f"{account_id or 'Default'} Portfolio",
            base_currency=base_currency,
            holdings=_holdings_payload(holdings),
            exposures=exposures,
            cash_weight=float(exposures.get("cash", 0.0)),
            as_of=as_of,
        )
    finally:
        conn.close()
