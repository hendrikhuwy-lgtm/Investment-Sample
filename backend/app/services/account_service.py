from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.services.review_items import ensure_review_tables


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_account_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS account_entities (
          account_id TEXT PRIMARY KEY,
          account_name TEXT,
          custodian_name TEXT,
          base_currency TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          account_type TEXT,
          is_active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dashboard_refresh_metadata (
          section_name TEXT PRIMARY KEY,
          last_refreshed_at TEXT,
          refresh_mode TEXT,
          updated_at TEXT NOT NULL
        )
        """
    )
    existing_columns = conn.execute('PRAGMA table_info("account_entities")').fetchall()
    for column_name, definition in (
        ("account_name", "TEXT"),
        ("status", "TEXT NOT NULL DEFAULT 'active'"),
        ("account_type", "TEXT"),
        ("is_active", "INTEGER NOT NULL DEFAULT 1"),
    ):
        if not any(str(row[1]) == column_name for row in existing_columns):
            conn.execute(f'ALTER TABLE "account_entities" ADD COLUMN "{column_name}" {definition}')
    conn.commit()


def sync_account_entities(conn: sqlite3.Connection) -> None:
    ensure_account_tables(conn)
    rows = conn.execute("SELECT DISTINCT account_id, base_currency FROM portfolio_holding_snapshots").fetchall()
    now = _now_iso()
    for row in rows:
        account_id = str(row["account_id"] or "").strip()
        if not account_id:
            continue
        conn.execute(
            """
            INSERT INTO account_entities (account_id, custodian_name, base_currency, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
              base_currency = COALESCE(excluded.base_currency, account_entities.base_currency),
              updated_at = excluded.updated_at
            """,
            (account_id, None, row["base_currency"], now, now),
        )
    conn.commit()


def list_accounts(conn: sqlite3.Connection, *, account_id: str | None = None) -> list[dict[str, Any]]:
    ensure_account_tables(conn)
    ensure_review_tables(conn)
    active_run = conn.execute(
        """
        SELECT run_id
        FROM portfolio_upload_runs
        WHERE coalesce(is_deleted, 0) = 0 AND coalesce(is_active, 0) = 1
        ORDER BY uploaded_at DESC
        LIMIT 1
        """
    ).fetchone()
    active_run_id = str(active_run["run_id"]) if active_run is not None else None
    if not active_run_id:
        return []
    rows = conn.execute(
        """
        SELECT account_id, account_name, custodian_name, base_currency, status, account_type, is_active, created_at, updated_at
        FROM account_entities
        WHERE (? IS NULL OR account_id = ?)
        ORDER BY account_id ASC
        """,
        (account_id, account_id),
    ).fetchall()
    accounts: list[dict[str, Any]] = []
    for row in rows:
        exposure = conn.execute(
            """
            SELECT COUNT(*) AS position_count, COALESCE(SUM(market_value), 0) AS total_value
            FROM portfolio_holding_snapshots
            WHERE account_id = ?
              AND (? IS NULL OR run_id = ?)
            """,
            (str(row["account_id"]), active_run_id, active_run_id),
        ).fetchone()
        reviews = conn.execute(
            """
            SELECT COUNT(*) AS open_review_count
            FROM review_items
            WHERE account_id = ?
              AND status NOT IN ('resolved', 'dismissed')
            """,
            (str(row["account_id"]),),
        ).fetchone()
        liquidity = conn.execute(
            """
            SELECT COUNT(*) AS low_liquidity_count
            FROM liquidity_snapshots
            WHERE run_id = ?
              AND security_key IN (
                SELECT security_key
                FROM portfolio_holding_snapshots
                WHERE run_id = ?
                  AND account_id = ?
              )
              AND liquidity_bucket IN ('low', 'unknown')
            """,
            (active_run_id, active_run_id, str(row["account_id"])),
        ).fetchone()
        accounts.append(
            {
                "account_id": str(row["account_id"]),
                "account_name": row["account_name"],
                "custodian_name": row["custodian_name"],
                "base_currency": row["base_currency"],
                "status": row["status"],
                "account_type": row["account_type"],
                "is_active": bool(row["is_active"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "position_count": int(exposure["position_count"] or 0),
                "total_value": float(exposure["total_value"] or 0.0),
                "open_review_count": int((reviews["open_review_count"] or 0) if reviews is not None else 0),
                "low_liquidity_count": int((liquidity["low_liquidity_count"] or 0) if liquidity is not None else 0),
            }
        )
    return accounts
