from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from app.models.types import PortfolioHolding, PortfolioSnapshot
from app.services.portfolio_state import latest_snapshot, list_holdings, list_snapshots


@dataclass(slots=True)
class SQLitePortfolioDonor:
    """Read-only donor wrapper for holdings and saved portfolio snapshots."""

    conn: sqlite3.Connection

    def list_holdings(self) -> list[PortfolioHolding]:
        return list_holdings(self.conn)

    def latest_snapshot(self) -> PortfolioSnapshot | None:
        return latest_snapshot(self.conn)

    def list_snapshots(self, *, limit: int = 30) -> list[PortfolioSnapshot]:
        return list_snapshots(self.conn, limit=limit)

