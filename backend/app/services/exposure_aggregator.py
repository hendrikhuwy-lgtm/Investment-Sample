from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_exposure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS exposure_snapshots (
          exposure_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          snapshot_id TEXT,
          exposure_type TEXT NOT NULL,
          scope_key TEXT NOT NULL,
          label TEXT NOT NULL,
          market_value REAL NOT NULL DEFAULT 0,
          weight REAL NOT NULL DEFAULT 0,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_exposure_snapshots_run_type
        ON exposure_snapshots (run_id, exposure_type, weight DESC)
        """
    )
    conn.commit()


def _issuer_key(row: dict[str, Any]) -> tuple[str, str]:
    label = str(row.get("security_name") or row.get("normalized_symbol") or "Unknown").strip() or "Unknown"
    identifier = str(row.get("identifier_isin") or "").strip().upper()
    if identifier:
        return identifier, label
    symbol = str(row.get("normalized_symbol") or "").strip().upper()
    if symbol:
        return symbol, label
    security_key = str(row.get("security_key") or "").strip()
    return security_key or label.lower().replace(" ", "_"), label


def _round(value: float) -> float:
    return round(float(value or 0.0), 6)


def build_exposure_snapshot(
    conn: sqlite3.Connection,
    *,
    run_id: str | None,
    snapshot_id: str | None = None,
    account_id: str | None = None,
) -> dict[str, Any]:
    ensure_exposure_tables(conn)
    if not run_id:
        return {
            "run_id": None,
            "snapshot_id": snapshot_id,
            "as_of": None,
            "summary": {
                "total_value": 0.0,
                "stale_priced_value": 0.0,
                "stale_priced_weight": 0.0,
                "unmapped_value": 0.0,
                "unmapped_weight": 0.0,
                "top_5_concentration": 0.0,
                "currency_count": 0,
                "issuer_count": 0,
            },
            "top_positions": [],
            "top_issuers": [],
            "issuer_concentration": [],
            "sleeve_concentration": [],
            "currency_exposure": [],
            "stale_priced_weight": {"market_value": 0.0, "weight": 0.0},
            "unmapped_weight": {"market_value": 0.0, "weight": 0.0},
        }

    rows = conn.execute(
        """
        SELECT security_key, normalized_symbol, security_name, currency, market_value, sleeve,
               mapping_status, price_stale, identifier_isin
        FROM portfolio_holding_snapshots
        WHERE run_id = ?
          AND (? IS NULL OR account_id = ?)
        ORDER BY market_value DESC, normalized_symbol ASC
        """,
        (run_id, account_id, account_id),
    ).fetchall()
    holdings = [dict(row) for row in rows]
    total_value = sum(float(item.get("market_value") or 0.0) for item in holdings)
    uploaded_row = conn.execute(
        """
        SELECT holdings_as_of_date
        FROM portfolio_upload_runs
        WHERE run_id = ?
        LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    as_of = str(uploaded_row["holdings_as_of_date"]) if uploaded_row is not None else None

    issuer_values: dict[str, dict[str, Any]] = {}
    sleeve_values: dict[str, float] = {}
    currency_values: dict[str, float] = {}
    stale_value = 0.0
    unmapped_value = 0.0
    top_positions = []

    for item in holdings:
        market_value = float(item.get("market_value") or 0.0)
        weight = (market_value / total_value) if total_value > 0 else 0.0
        top_positions.append(
            {
                "security_key": item.get("security_key"),
                "normalized_symbol": item.get("normalized_symbol"),
                "security_name": item.get("security_name"),
                "market_value": round(market_value, 2),
                "weight": _round(weight),
                "currency": item.get("currency"),
                "sleeve": item.get("sleeve"),
                "mapping_status": item.get("mapping_status"),
                "price_stale": bool(item.get("price_stale")),
            }
        )
        issuer_key, issuer_label = _issuer_key(item)
        issuer_bucket = issuer_values.setdefault(issuer_key, {"issuer_key": issuer_key, "issuer_label": issuer_label, "market_value": 0.0})
        issuer_bucket["market_value"] += market_value
        sleeve_key = str(item.get("sleeve") or "unmapped")
        sleeve_values[sleeve_key] = sleeve_values.get(sleeve_key, 0.0) + market_value
        currency = str(item.get("currency") or "UNK").upper()
        currency_values[currency] = currency_values.get(currency, 0.0) + market_value
        if bool(item.get("price_stale")):
            stale_value += market_value
        if str(item.get("mapping_status") or "unmapped") in {"unmapped", "low_confidence"}:
            unmapped_value += market_value

    top_positions = top_positions[:10]
    top_5_concentration = sum(item["weight"] for item in top_positions[:5])
    top_issuers = sorted(
        (
            {
                "issuer_key": bucket["issuer_key"],
                "issuer_label": bucket["issuer_label"],
                "market_value": round(float(bucket["market_value"]), 2),
                "weight": _round((float(bucket["market_value"]) / total_value) if total_value > 0 else 0.0),
            }
            for bucket in issuer_values.values()
        ),
        key=lambda item: float(item["market_value"]),
        reverse=True,
    )
    sleeve_concentration = sorted(
        (
            {
                "sleeve": sleeve,
                "market_value": round(value, 2),
                "weight": _round((value / total_value) if total_value > 0 else 0.0),
            }
            for sleeve, value in sleeve_values.items()
        ),
        key=lambda item: float(item["market_value"]),
        reverse=True,
    )
    currency_exposure = sorted(
        (
            {
                "currency": currency,
                "market_value": round(value, 2),
                "weight": _round((value / total_value) if total_value > 0 else 0.0),
            }
            for currency, value in currency_values.items()
        ),
        key=lambda item: float(item["market_value"]),
        reverse=True,
    )

    conn.execute("DELETE FROM exposure_snapshots WHERE run_id = ?", (run_id,))
    created_at = _now_iso()

    def persist(exposure_type: str, scope_key: str, label: str, market_value: float, weight: float, metadata: dict[str, Any] | None = None) -> None:
        conn.execute(
            """
            INSERT INTO exposure_snapshots (
              exposure_id, run_id, snapshot_id, exposure_type, scope_key, label,
              market_value, weight, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"exposure_{uuid.uuid4().hex[:12]}",
                run_id,
                snapshot_id,
                exposure_type,
                scope_key,
                label,
                round(float(market_value or 0.0), 2),
                _round(weight),
                json.dumps(metadata or {}),
                created_at,
            ),
        )

    for item in top_positions:
        persist("position", str(item["security_key"]), str(item["normalized_symbol"]), float(item["market_value"]), float(item["weight"]), {
            "security_name": item["security_name"],
            "currency": item["currency"],
            "sleeve": item["sleeve"],
            "mapping_status": item["mapping_status"],
            "price_stale": item["price_stale"],
        })
    for item in top_issuers:
        persist("issuer", str(item["issuer_key"]), str(item["issuer_label"]), float(item["market_value"]), float(item["weight"]))
    for item in sleeve_concentration:
        persist("sleeve", str(item["sleeve"]), str(item["sleeve"]), float(item["market_value"]), float(item["weight"]))
    for item in currency_exposure:
        persist("currency", str(item["currency"]), str(item["currency"]), float(item["market_value"]), float(item["weight"]))
    persist(
        "stale_priced_weight",
        "portfolio",
        "Stale priced weight",
        stale_value,
        (stale_value / total_value) if total_value > 0 else 0.0,
    )
    persist(
        "unmapped_weight",
        "portfolio",
        "Unmapped weight",
        unmapped_value,
        (unmapped_value / total_value) if total_value > 0 else 0.0,
    )
    persist(
        "top_5_concentration",
        "portfolio",
        "Top 5 concentration",
        sum(item["market_value"] for item in top_positions[:5]),
        top_5_concentration,
    )
    conn.commit()

    return {
        "run_id": run_id,
        "account_id": account_id,
        "snapshot_id": snapshot_id,
        "as_of": as_of,
        "summary": {
            "total_value": round(total_value, 2),
            "stale_priced_value": round(stale_value, 2),
            "stale_priced_weight": _round((stale_value / total_value) if total_value > 0 else 0.0),
            "unmapped_value": round(unmapped_value, 2),
            "unmapped_weight": _round((unmapped_value / total_value) if total_value > 0 else 0.0),
            "top_5_concentration": _round(top_5_concentration),
            "currency_count": len(currency_exposure),
            "issuer_count": len(top_issuers),
        },
        "top_positions": top_positions,
        "top_issuers": top_issuers[:10],
        "issuer_concentration": top_issuers[:10],
        "sleeve_concentration": sleeve_concentration,
        "currency_exposure": currency_exposure,
        "stale_priced_weight": {
            "market_value": round(stale_value, 2),
            "weight": _round((stale_value / total_value) if total_value > 0 else 0.0),
        },
        "unmapped_weight": {
            "market_value": round(unmapped_value, 2),
            "weight": _round((unmapped_value / total_value) if total_value > 0 else 0.0),
        },
    }


def get_latest_exposure_snapshot(
    conn: sqlite3.Connection,
    *,
    run_id: str | None,
    snapshot_id: str | None = None,
) -> dict[str, Any]:
    return build_exposure_snapshot(conn, run_id=run_id, snapshot_id=snapshot_id)
