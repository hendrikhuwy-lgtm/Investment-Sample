from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_change_attribution_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS change_attribution_records (
          attribution_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          security_key TEXT,
          normalized_symbol TEXT,
          attribution_type TEXT NOT NULL,
          confidence TEXT NOT NULL,
          trade_date TEXT,
          settlement_date TEXT,
          pending_settlement INTEGER NOT NULL DEFAULT 0,
          detail TEXT,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_change_attribution_run
        ON change_attribution_records (run_id, attribution_type)
        """
    )
    conn.commit()


def build_change_attribution(conn: sqlite3.Connection, *, delta: dict[str, Any], run_id: str | None, account_id: str | None = None) -> dict[str, Any]:
    ensure_change_attribution_table(conn)
    if not run_id:
        return {"run_id": None, "items": [], "summary": {"trade_driven": 0, "price_driven": 0, "unknown": 0}}
    if account_id is None:
        conn.execute("DELETE FROM change_attribution_records WHERE run_id = ?", (run_id,))
    items: list[dict[str, Any]] = []

    def add(item: dict[str, Any], attribution_type: str, confidence: str, detail: str) -> None:
        payload = {
            "attribution_id": f"attr_{uuid.uuid4().hex[:12]}",
            "run_id": run_id,
            "security_key": item.get("security_key"),
            "normalized_symbol": item.get("normalized_symbol"),
            "attribution_type": attribution_type,
            "confidence": confidence,
            "trade_date": None,
            "settlement_date": None,
            "pending_settlement": False,
            "detail": detail,
            "created_at": _now_iso(),
        }
        if account_id is None:
            conn.execute(
                """
                INSERT INTO change_attribution_records (
                  attribution_id, run_id, security_key, normalized_symbol, attribution_type, confidence,
                  trade_date, settlement_date, pending_settlement, detail, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["attribution_id"], payload["run_id"], payload["security_key"], payload["normalized_symbol"],
                    payload["attribution_type"], payload["confidence"], payload["trade_date"], payload["settlement_date"],
                    1 if payload["pending_settlement"] else 0, payload["detail"], payload["created_at"],
                ),
            )
        items.append(payload)

    for item in list(delta.get("new_positions") or []):
        add(item, "trade_driven", "high", "New position relative to prior snapshot.")
    for item in list(delta.get("exited_positions") or []):
        add(item, "trade_driven", "high", "Exited position relative to prior snapshot.")
    for item in list(delta.get("increased_positions") or []):
        add(item, "trade_driven", "high", "Quantity increased versus prior snapshot.")
    for item in list(delta.get("reduced_positions") or []):
        add(item, "trade_driven", "high", "Quantity reduced versus prior snapshot.")
    for item in list(delta.get("largest_market_value_movers") or []):
        key = str(item.get("security_key"))
        if any(str(existing.get("security_key")) == key for existing in items):
            continue
        add(item, "price_driven", "medium", "Market value moved without a detected quantity change.")

    if account_id is None:
        conn.commit()
    summary = {
        "trade_driven": sum(1 for item in items if item["attribution_type"] == "trade_driven"),
        "price_driven": sum(1 for item in items if item["attribution_type"] == "price_driven"),
        "unknown": sum(1 for item in items if item["attribution_type"] == "unknown"),
    }
    return {"run_id": run_id, "account_id": account_id, "items": items, "summary": summary}
