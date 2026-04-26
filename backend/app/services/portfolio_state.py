from __future__ import annotations

import json
import csv
import uuid
import io
from datetime import UTC, datetime
from typing import Any

import sqlite3

from app.models.types import PortfolioHolding, PortfolioSnapshot


DEFAULT_POLICY_WEIGHTS: dict[str, float] = {
    "global_equity": 0.50,
    "ig_bond": 0.20,
    "cash": 0.10,
    "real_asset": 0.10,
    "alt": 0.07,
    "convex": 0.03,
}

DEFAULT_SYMBOL_SLEEVE_MAP: dict[str, str] = {
    "CSPX": "global_equity",
    "IWDA": "global_equity",
    "VWRA": "global_equity",
    "VWRD": "global_equity",
    "VTI": "global_equity",
    "SPY": "global_equity",
    "IVV": "global_equity",
    "BND": "ig_bond",
    "AGGU": "ig_bond",
    "IGLA": "ig_bond",
    "A35": "ig_bond",
    "VNQ": "real_asset",
    "IWDP": "real_asset",
    "GLD": "real_asset",
    "SGLN": "real_asset",
    "IGLN": "real_asset",
    "DBMF": "convex",
    "KMLM": "convex",
    "TAIL": "convex",
    "CAOS": "convex",
    "BTAL": "alt",
}

VALID_SLEEVES = {"global_equity", "ig_bond", "real_asset", "alt", "convex", "cash"}
VALID_ACCOUNT_TYPES = {"taxable", "broker", "other"}
SLEEVE_ALIASES: dict[str, str] = {
    "global_equity": "global_equity",
    "equity": "global_equity",
    "global equity": "global_equity",
    "growth": "global_equity",
    "stocks": "global_equity",
    "ig_bond": "ig_bond",
    "ig bond": "ig_bond",
    "ig bonds": "ig_bond",
    "bond": "ig_bond",
    "bonds": "ig_bond",
    "fixed income": "ig_bond",
    "income": "ig_bond",
    "real_asset": "real_asset",
    "real asset": "real_asset",
    "real assets": "real_asset",
    "reits": "real_asset",
    "commodities": "real_asset",
    "gold": "real_asset",
    "alt": "alt",
    "alternative": "alt",
    "alternatives": "alt",
    "convex": "convex",
    "tail": "convex",
    "tail risk": "convex",
    "cash": "cash",
    "cash_bills": "cash",
    "cash & bills": "cash",
    "cash and bills": "cash",
    "bills": "cash",
}


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return any(str(row[1]) == column_name for row in rows)


def ensure_portfolio_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_holdings (
          holding_id TEXT PRIMARY KEY,
          symbol TEXT NOT NULL,
          name TEXT NOT NULL,
          quantity REAL NOT NULL,
          cost_basis REAL NOT NULL,
          currency TEXT NOT NULL,
          sleeve TEXT NOT NULL,
          account_type TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
          snapshot_id TEXT PRIMARY KEY,
          created_at TEXT NOT NULL,
          snapshot_date TEXT,
          holdings_as_of_date TEXT,
          price_as_of_date TEXT,
          upload_run_id TEXT,
          total_value REAL NOT NULL,
          sleeve_weights_json TEXT NOT NULL,
          concentration_metrics_json TEXT NOT NULL,
          convex_coverage_ratio REAL NOT NULL,
          tax_drag_estimate REAL NOT NULL,
          stale_price_count INTEGER NOT NULL DEFAULT 0,
          mapping_issue_count INTEGER NOT NULL DEFAULT 0,
          notes TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_sleeve_overrides (
          symbol TEXT PRIMARY KEY,
          sleeve TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    for column_name, definition in (
        ("snapshot_date", "TEXT"),
        ("holdings_as_of_date", "TEXT"),
        ("price_as_of_date", "TEXT"),
        ("upload_run_id", "TEXT"),
        ("stale_price_count", "INTEGER NOT NULL DEFAULT 0"),
        ("mapping_issue_count", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if not _column_exists(conn, "portfolio_snapshots", column_name):
            conn.execute(f'ALTER TABLE "portfolio_snapshots" ADD COLUMN "{column_name}" {definition}')
    conn.commit()


def list_sleeve_overrides(conn: sqlite3.Connection) -> dict[str, str]:
    ensure_portfolio_tables(conn)
    rows = conn.execute(
        "SELECT symbol, sleeve FROM portfolio_sleeve_overrides ORDER BY symbol ASC"
    ).fetchall()
    return {str(row["symbol"]).upper(): str(row["sleeve"]) for row in rows}


def normalize_sleeve(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    normalized = SLEEVE_ALIASES.get(raw.lower())
    if normalized in VALID_SLEEVES:
        return normalized
    return raw if raw in VALID_SLEEVES else None


def normalize_account_type(value: str | None, *, default: str = "broker") -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    if raw in VALID_ACCOUNT_TYPES:
        return raw
    if "tax" in raw:
        return "taxable"
    if "broker" in raw:
        return "broker"
    return "other"


def put_sleeve_override(conn: sqlite3.Connection, symbol: str, sleeve: str) -> None:
    ensure_portfolio_tables(conn)
    normalized_sleeve = normalize_sleeve(sleeve)
    if normalized_sleeve is None:
        raise ValueError(f"Unsupported sleeve override: {sleeve}")
    conn.execute(
        """
        INSERT INTO portfolio_sleeve_overrides (symbol, sleeve, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            sleeve=excluded.sleeve,
            updated_at=excluded.updated_at
        """,
        (symbol.upper(), normalized_sleeve, datetime.now(UTC).isoformat()),
    )
    conn.commit()


def resolve_sleeve(conn: sqlite3.Connection, symbol: str, provided: str | None = None) -> str:
    if provided:
        normalized = normalize_sleeve(provided)
        if normalized:
            return normalized
    overrides = list_sleeve_overrides(conn)
    code = symbol.upper()
    if code in overrides:
        return overrides[code]
    return DEFAULT_SYMBOL_SLEEVE_MAP.get(code, "global_equity")


def _holding_from_row(row: sqlite3.Row | sqlite3.Cursor | Any) -> PortfolioHolding:
    normalized_symbol = str(row["symbol"]).upper()
    sleeve = normalize_sleeve(str(row["sleeve"])) or DEFAULT_SYMBOL_SLEEVE_MAP.get(normalized_symbol, "global_equity")
    return PortfolioHolding(
        holding_id=str(row["holding_id"]),
        symbol=normalized_symbol,
        name=str(row["name"]),
        quantity=float(row["quantity"]),
        cost_basis=float(row["cost_basis"]),
        currency=str(row["currency"]),
        sleeve=sleeve,
        account_type=normalize_account_type(str(row["account_type"])),
        created_at=datetime.fromisoformat(str(row["created_at"])),
        updated_at=datetime.fromisoformat(str(row["updated_at"])),
    )


def list_holdings(conn: sqlite3.Connection) -> list[PortfolioHolding]:
    ensure_portfolio_tables(conn)
    rows = conn.execute(
        """
        SELECT holding_id, symbol, name, quantity, cost_basis, currency, sleeve, account_type, created_at, updated_at
        FROM portfolio_holdings
        ORDER BY updated_at DESC
        """
    ).fetchall()
    return [_holding_from_row(row) for row in rows]


def upsert_holding(conn: sqlite3.Connection, payload: dict[str, Any]) -> PortfolioHolding:
    ensure_portfolio_tables(conn)
    now = datetime.now(UTC).isoformat()
    holding_id = str(payload.get("holding_id") or f"holding_{uuid.uuid4().hex[:12]}")

    existing = conn.execute(
        "SELECT holding_id, created_at FROM portfolio_holdings WHERE holding_id = ?",
        (holding_id,),
    ).fetchone()
    created_at = str(existing["created_at"]) if existing is not None else now

    symbol = str(payload.get("symbol", "")).upper()
    raw_sleeve = payload.get("sleeve")
    provided_sleeve = None
    if isinstance(raw_sleeve, str) and raw_sleeve.strip():
        provided_sleeve = raw_sleeve.strip()
    sleeve = resolve_sleeve(conn, symbol=symbol, provided=provided_sleeve)
    account_type = normalize_account_type(str(payload.get("account_type", "broker")))

    conn.execute(
        """
        INSERT INTO portfolio_holdings (
            holding_id, symbol, name, quantity, cost_basis, currency, sleeve, account_type, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(holding_id) DO UPDATE SET
            symbol=excluded.symbol,
            name=excluded.name,
            quantity=excluded.quantity,
            cost_basis=excluded.cost_basis,
            currency=excluded.currency,
            sleeve=excluded.sleeve,
            account_type=excluded.account_type,
            updated_at=excluded.updated_at
        """,
        (
            holding_id,
            symbol,
            str(payload.get("name", "")),
            float(payload.get("quantity", 0.0)),
            float(payload.get("cost_basis", 0.0)),
            str(payload.get("currency", "USD")).upper(),
            sleeve,
            account_type,
            created_at,
            now,
        ),
    )
    conn.commit()
    row = conn.execute(
        """
        SELECT holding_id, symbol, name, quantity, cost_basis, currency, sleeve, account_type, created_at, updated_at
        FROM portfolio_holdings
        WHERE holding_id = ?
        """,
        (holding_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Failed to upsert holding {holding_id}")
    return _holding_from_row(row)


def delete_holding(conn: sqlite3.Connection, holding_id: str) -> bool:
    ensure_portfolio_tables(conn)
    cur = conn.execute("DELETE FROM portfolio_holdings WHERE holding_id = ?", (holding_id,))
    conn.commit()
    return cur.rowcount > 0


def holding_market_value(holding: PortfolioHolding) -> float:
    return max(0.0, float(holding.quantity) * float(holding.cost_basis))


def holdings_total_value(holdings: list[PortfolioHolding]) -> float:
    return sum(holding_market_value(item) for item in holdings)


def latest_snapshot(conn: sqlite3.Connection) -> PortfolioSnapshot | None:
    ensure_portfolio_tables(conn)
    row = conn.execute(
        """
        SELECT snapshot_id, created_at, total_value, sleeve_weights_json, concentration_metrics_json,
               convex_coverage_ratio, tax_drag_estimate, notes
        FROM portfolio_snapshots
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return PortfolioSnapshot(
        snapshot_id=str(row["snapshot_id"]),
        created_at=datetime.fromisoformat(str(row["created_at"])),
        total_value=float(row["total_value"]),
        sleeve_weights=json.loads(str(row["sleeve_weights_json"])),
        concentration_metrics=json.loads(str(row["concentration_metrics_json"])),
        convex_coverage_ratio=float(row["convex_coverage_ratio"]),
        tax_drag_estimate=float(row["tax_drag_estimate"]),
        notes=str(row["notes"]) if row["notes"] is not None else None,
    )


def list_snapshots(conn: sqlite3.Connection, limit: int = 30) -> list[PortfolioSnapshot]:
    ensure_portfolio_tables(conn)
    rows = conn.execute(
        """
        SELECT snapshot_id, created_at, total_value, sleeve_weights_json, concentration_metrics_json,
               convex_coverage_ratio, tax_drag_estimate, notes
        FROM portfolio_snapshots
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (max(1, limit),),
    ).fetchall()
    out: list[PortfolioSnapshot] = []
    for row in rows:
        out.append(
            PortfolioSnapshot(
                snapshot_id=str(row["snapshot_id"]),
                created_at=datetime.fromisoformat(str(row["created_at"])),
                total_value=float(row["total_value"]),
                sleeve_weights=json.loads(str(row["sleeve_weights_json"])),
                concentration_metrics=json.loads(str(row["concentration_metrics_json"])),
                convex_coverage_ratio=float(row["convex_coverage_ratio"]),
                tax_drag_estimate=float(row["tax_drag_estimate"]),
                notes=str(row["notes"]) if row["notes"] is not None else None,
            )
        )
    return out


def save_snapshot(
    conn: sqlite3.Connection,
    *,
    total_value: float,
    sleeve_weights: dict[str, float],
    concentration_metrics: dict[str, Any],
    convex_coverage_ratio: float,
    tax_drag_estimate: float,
    notes: str | None = None,
) -> PortfolioSnapshot:
    ensure_portfolio_tables(conn)
    snapshot_id = f"snapshot_{uuid.uuid4().hex[:12]}"
    created_at = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO portfolio_snapshots (
            snapshot_id, created_at, total_value, sleeve_weights_json, concentration_metrics_json,
            convex_coverage_ratio, tax_drag_estimate, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            created_at,
            float(total_value),
            json.dumps(sleeve_weights),
            json.dumps(concentration_metrics),
            float(convex_coverage_ratio),
            float(tax_drag_estimate),
            notes,
        ),
    )
    conn.commit()
    return PortfolioSnapshot(
        snapshot_id=snapshot_id,
        created_at=datetime.fromisoformat(created_at),
        total_value=float(total_value),
        sleeve_weights=sleeve_weights,
        concentration_metrics=concentration_metrics,
        convex_coverage_ratio=float(convex_coverage_ratio),
        tax_drag_estimate=float(tax_drag_estimate),
        notes=notes,
    )


def import_holdings_csv(
    conn: sqlite3.Connection,
    csv_text: str,
    *,
    default_currency: str = "USD",
    default_account_type: str = "broker",
    allow_sleeve_override: bool = True,
) -> dict[str, Any]:
    ensure_portfolio_tables(conn)
    reader = csv.DictReader(io.StringIO(csv_text))
    created: list[PortfolioHolding] = []
    errors: list[str] = []

    required = {"symbol", "name", "quantity", "cost_basis"}
    if reader.fieldnames is None:
        return {"created": [], "errors": ["CSV header row is missing."]}

    lower_headers = {item.strip().lower() for item in reader.fieldnames}
    missing = required - lower_headers
    if missing:
        return {"created": [], "errors": [f"CSV missing required columns: {', '.join(sorted(missing))}"]}

    for idx, row in enumerate(reader, start=2):
        try:
            normalized = {str(key).strip().lower(): value for key, value in row.items() if key is not None}
            symbol = str(normalized.get("symbol", "")).strip().upper()
            if not symbol:
                raise ValueError("symbol is empty")
            payload = {
                "holding_id": str(normalized.get("holding_id", "") or None),
                "symbol": symbol,
                "name": str(normalized.get("name", "")).strip() or symbol,
                "quantity": float(normalized.get("quantity", 0.0)),
                "cost_basis": float(normalized.get("cost_basis", 0.0)),
                "currency": str(normalized.get("currency", default_currency)).strip().upper() or default_currency,
                "sleeve": str(normalized.get("sleeve", "")).strip() if allow_sleeve_override else "",
                "account_type": str(normalized.get("account_type", default_account_type)).strip() or default_account_type,
            }
            if payload["quantity"] <= 0:
                raise ValueError("quantity must be > 0")
            if payload["cost_basis"] <= 0:
                raise ValueError("cost_basis must be > 0")
            created.append(upsert_holding(conn, payload))
        except Exception as exc:
            errors.append(f"line {idx}: {exc}")

    return {
        "created": [item.model_dump(mode="json") for item in created],
        "errors": errors,
    }


def build_portfolio_state_context(current_holdings: list[dict[str, Any]]) -> dict[str, Any]:
    """Compatibility stub — provides a coverage_state summary for blueprint_pipeline."""
    if not current_holdings:
        return {"coverage_state": "no_holdings", "holding_count": 0, "symbols": []}
    symbols = [str(h.get("symbol") or h.get("ticker") or "") for h in current_holdings if h]
    symbols = [s for s in symbols if s]
    return {
        "coverage_state": "holdings_present" if symbols else "no_holdings",
        "holding_count": len(current_holdings),
        "symbols": symbols,
    }
