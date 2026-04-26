from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_liquidity_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS liquidity_snapshots (
          liquidity_id TEXT PRIMARY KEY,
          run_id TEXT,
          security_key TEXT,
          normalized_symbol TEXT,
          liquidity_bucket TEXT NOT NULL,
          trading_volume_proxy REAL,
          days_to_exit_proxy REAL,
          confidence_flag TEXT NOT NULL,
          source_name TEXT,
          source_observed_at TEXT,
          provenance_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_liquidity_snapshots_run
        ON liquidity_snapshots (run_id, liquidity_bucket)
        """
    )
    conn.commit()


def _classify_liquidity(row: dict[str, Any]) -> tuple[str, float, float, str, str, str, dict[str, Any]]:
    asset_type = str(row.get("asset_type") or "").lower()
    market_value = float(row.get("market_value") or 0.0)
    symbol = str(row.get("normalized_symbol") or "").upper()
    sleeve = str(row.get("sleeve") or "").lower()
    if asset_type == "cash" or sleeve == "cash":
        return "high", 5_000_000.0, 0.25, "easy", "high", "asset_type_rule", {"rule": "cash_is_immediately_liquid"}
    if asset_type == "etf":
        bucket = "high" if symbol in {"SPY", "IVV", "CSPX", "VWRA", "IWDA", "A35", "AGGU"} else "medium"
        days = 1.0 if bucket == "high" else max(1.0, market_value / 750_000.0)
        confidence = "medium" if bucket == "high" else "low"
        exit_band = "easy" if bucket == "high" else "moderate"
        return bucket, 1_000_000.0 if bucket == "high" else 400_000.0, round(days, 2), exit_band, confidence, "etf_proxy_rule", {
            "rule": "etf_proxy_liquidity",
            "symbol": symbol,
        }
    if asset_type in {"equity", "mutual_fund"}:
        bucket = "medium" if market_value <= 750_000 else "low"
        days = max(1.0, market_value / 500_000.0)
        exit_band = "moderate" if bucket == "medium" else "hard"
        return bucket, 500_000.0, round(days, 2), exit_band, "low", "equity_proxy_rule", {"rule": "equity_market_value_proxy"}
    if asset_type == "bond" or sleeve == "ig_bond":
        bucket = "medium" if market_value <= 1_000_000 else "low"
        days = max(1.5, market_value / 600_000.0)
        exit_band = "moderate" if bucket == "medium" else "hard"
        return bucket, 350_000.0, round(days, 2), exit_band, "low", "bond_proxy_rule", {"rule": "bond_secondary_liquidity_proxy"}
    return "unknown", 0.0, 5.0, "hard", "low", "fallback_rule", {"rule": "unknown_asset_type"}


def build_liquidity_snapshot(conn: sqlite3.Connection, *, run_id: str | None, account_id: str | None = None) -> dict[str, Any]:
    ensure_liquidity_tables(conn)
    if not run_id:
        return {
            "run_id": None,
            "items": [],
            "warnings": [],
            "summary": {
                "high": 0,
                "medium": 0,
                "low": 0,
                "unknown": 0,
                "warning_count": 0,
                "avg_days_to_exit": 0.0,
                "lowest_confidence": "none",
            },
        }
    if account_id is None:
        conn.execute("DELETE FROM liquidity_snapshots WHERE run_id = ?", (run_id,))
    rows = conn.execute(
        """
        SELECT security_key, normalized_symbol, asset_type, market_value, sleeve, price_as_of_date, account_id
        FROM portfolio_holding_snapshots
        WHERE run_id = ?
          AND (? IS NULL OR account_id = ?)
        """,
        (run_id, account_id, account_id),
    ).fetchall()
    items: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    total_value = sum(float(row["market_value"] or 0.0) for row in rows)
    illiquid_weight = 0.0
    exit_band_counts = {"easy": 0, "moderate": 0, "hard": 0}
    for row in rows:
        bucket, volume, days_to_exit, exit_band, confidence, source_name, provenance = _classify_liquidity(dict(row))
        market_value = float(row["market_value"] or 0.0)
        if bucket in {"low", "unknown"} and total_value > 0:
            illiquid_weight += market_value / total_value
        exit_band_counts[exit_band] = exit_band_counts.get(exit_band, 0) + 1
        payload = {
            "liquidity_id": f"liq_{uuid.uuid4().hex[:12]}",
            "run_id": run_id,
            "account_id": row["account_id"],
            "security_key": row["security_key"],
            "normalized_symbol": row["normalized_symbol"],
            "liquidity_bucket": bucket,
            "trading_volume_proxy": volume,
            "days_to_exit_proxy": round(days_to_exit, 2),
            "exit_difficulty_band": exit_band,
            "confidence_flag": confidence,
            "source_name": source_name,
            "source_observed_at": row["price_as_of_date"],
            "provenance_json": provenance,
            "created_at": _now_iso(),
        }
        if account_id is None:
            conn.execute(
                """
                INSERT INTO liquidity_snapshots (
                  liquidity_id, run_id, security_key, normalized_symbol, liquidity_bucket,
                  trading_volume_proxy, days_to_exit_proxy, confidence_flag, source_name,
                  source_observed_at, provenance_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["liquidity_id"], payload["run_id"], payload["security_key"], payload["normalized_symbol"],
                    payload["liquidity_bucket"], payload["trading_volume_proxy"], payload["days_to_exit_proxy"],
                    payload["confidence_flag"], payload["source_name"], payload["source_observed_at"],
                    json.dumps(payload["provenance_json"]), payload["created_at"],
                ),
            )
        items.append(payload)
        if bucket in {"low", "unknown"} or days_to_exit >= 3.0:
            warnings.append(
                {
                    "security_key": payload["security_key"],
                    "normalized_symbol": payload["normalized_symbol"],
                    "liquidity_bucket": payload["liquidity_bucket"],
                    "days_to_exit_proxy": payload["days_to_exit_proxy"],
                    "exit_difficulty_band": payload["exit_difficulty_band"],
                    "confidence_flag": payload["confidence_flag"],
                    "source_name": payload["source_name"],
                    "reason": (
                        "Estimated exit difficulty exceeds operational comfort threshold."
                        if days_to_exit >= 3.0
                        else "Liquidity classification is low confidence or low liquidity."
                    ),
                    "provenance": payload["provenance_json"],
                }
            )
    if account_id is None:
        conn.commit()
    summary = {
        bucket: sum(1 for item in items if item["liquidity_bucket"] == bucket)
        for bucket in ("high", "medium", "low", "unknown")
    }
    lowest_confidence = "high"
    for level in ("low", "medium", "high"):
        if any(item["confidence_flag"] == level for item in items):
            lowest_confidence = level
            break
    summary.update(
        {
            "warning_count": len(warnings),
            "avg_days_to_exit": round(
                sum(float(item["days_to_exit_proxy"] or 0.0) for item in items) / max(len(items), 1),
                2,
            ),
            "lowest_confidence": lowest_confidence if items else "none",
            "illiquid_weight": round(illiquid_weight, 6),
            "exit_band_easy": exit_band_counts.get("easy", 0),
            "exit_band_moderate": exit_band_counts.get("moderate", 0),
            "exit_band_hard": exit_band_counts.get("hard", 0),
        }
    )
    return {"run_id": run_id, "account_id": account_id, "items": items, "warnings": warnings, "summary": summary}
