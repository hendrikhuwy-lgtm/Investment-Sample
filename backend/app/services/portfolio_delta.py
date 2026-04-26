from __future__ import annotations

import sqlite3
from typing import Any


def _snapshot_rows(conn: sqlite3.Connection, run_id: str | None, *, account_id: str | None = None) -> list[dict[str, Any]]:
    if not run_id:
        return []
    rows = conn.execute(
        """
        SELECT account_id, security_key, normalized_symbol, security_name, asset_type, currency, quantity,
               cost_basis, market_price, market_value, sleeve, mapping_status, price_stale
        FROM portfolio_holding_snapshots
        WHERE run_id = ?
          AND (? IS NULL OR account_id = ?)
        ORDER BY market_value DESC, normalized_symbol ASC
        """,
        (run_id, account_id, account_id),
    ).fetchall()
    return [dict(row) for row in rows]


def _position_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = f"{row.get('account_id') or 'default'}::{row.get('security_key')}"
        out[key] = row
    return out


def compute_latest_portfolio_delta(
    conn: sqlite3.Connection,
    *,
    latest_run_id: str | None,
    previous_run_id: str | None,
    account_id: str | None = None,
) -> dict[str, Any]:
    latest_rows = _snapshot_rows(conn, latest_run_id, account_id=account_id)
    previous_rows = _snapshot_rows(conn, previous_run_id, account_id=account_id)
    latest_idx = _position_index(latest_rows)
    previous_idx = _position_index(previous_rows)
    latest_total = sum(float(item.get("market_value") or 0.0) for item in latest_rows)
    previous_total = sum(float(item.get("market_value") or 0.0) for item in previous_rows)

    new_positions: list[dict[str, Any]] = []
    exited_positions: list[dict[str, Any]] = []
    increased_positions: list[dict[str, Any]] = []
    reduced_positions: list[dict[str, Any]] = []
    all_movers: list[dict[str, Any]] = []
    stale_price_warnings: list[dict[str, Any]] = []
    mapping_issues: list[dict[str, Any]] = []

    all_keys = set(latest_idx) | set(previous_idx)
    for key in all_keys:
        latest = latest_idx.get(key)
        previous = previous_idx.get(key)
        if latest and not previous:
            new_positions.append(latest)
        elif previous and not latest:
            exited_positions.append(previous)
        elif latest and previous:
            quantity_delta = float(latest.get("quantity") or 0.0) - float(previous.get("quantity") or 0.0)
            market_value_delta = float(latest.get("market_value") or 0.0) - float(previous.get("market_value") or 0.0)
            if quantity_delta > 0:
                increased_positions.append({**latest, "quantity_delta": round(quantity_delta, 6)})
            elif quantity_delta < 0:
                reduced_positions.append({**latest, "quantity_delta": round(quantity_delta, 6)})
            latest_weight = (float(latest.get("market_value") or 0.0) / latest_total) if latest_total > 0 else 0.0
            previous_weight = (float(previous.get("market_value") or 0.0) / previous_total) if previous_total > 0 else 0.0
            all_movers.append(
                {
                    "security_key": latest.get("security_key"),
                    "normalized_symbol": latest.get("normalized_symbol"),
                    "security_name": latest.get("security_name"),
                    "market_value_delta": round(market_value_delta, 2),
                    "weight_change": round(latest_weight - previous_weight, 6),
                    "current_market_value": round(float(latest.get("market_value") or 0.0), 2),
                    "current_weight": round(latest_weight, 6),
                }
            )

    for latest in latest_rows:
        if int(latest.get("price_stale") or 0) == 1:
            stale_price_warnings.append(
                {
                    "security_key": latest.get("security_key"),
                    "normalized_symbol": latest.get("normalized_symbol"),
                    "security_name": latest.get("security_name"),
                    "market_price": latest.get("market_price"),
                }
            )
        status = str(latest.get("mapping_status") or "unmapped")
        if status in {"unmapped", "low_confidence"}:
            mapping_issues.append(
                {
                    "security_key": latest.get("security_key"),
                    "normalized_symbol": latest.get("normalized_symbol"),
                    "security_name": latest.get("security_name"),
                    "mapping_status": status,
                    "sleeve": latest.get("sleeve"),
                }
            )

    largest_market_value_movers = sorted(
        all_movers,
        key=lambda item: abs(float(item.get("market_value_delta") or 0.0)),
        reverse=True,
    )[:5]
    biggest_weight_changes = sorted(
        all_movers,
        key=lambda item: abs(float(item.get("weight_change") or 0.0)),
        reverse=True,
    )[:5]

    return {
        "latest_run_id": latest_run_id,
        "previous_run_id": previous_run_id,
        "account_id": account_id,
        "new_positions": new_positions,
        "exited_positions": exited_positions,
        "increased_positions": increased_positions,
        "reduced_positions": reduced_positions,
        "largest_market_value_movers": largest_market_value_movers,
        "biggest_weight_changes": biggest_weight_changes,
        "stale_price_warnings": stale_price_warnings,
        "mapping_issues": mapping_issues,
        "summary": {
            "new_count": len(new_positions),
            "exited_count": len(exited_positions),
            "increased_count": len(increased_positions),
            "reduced_count": len(reduced_positions),
            "stale_price_count": len(stale_price_warnings),
            "mapping_issue_count": len(mapping_issues),
        },
    }
