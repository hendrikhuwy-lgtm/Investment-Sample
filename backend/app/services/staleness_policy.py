from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_sla_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS staleness_sla_policies (
          policy_id TEXT PRIMARY KEY,
          asset_class TEXT NOT NULL,
          source_type TEXT NOT NULL,
          max_lag_days_warning INTEGER NOT NULL,
          max_lag_days_breach INTEGER NOT NULL,
          nav_blocking INTEGER NOT NULL DEFAULT 0,
          escalation_enabled INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    existing = conn.execute("SELECT 1 FROM staleness_sla_policies LIMIT 1").fetchone()
    if existing is None:
        now = _now_iso()
        defaults = [
            ("equity", "market_price", 1, 3, 0, 1),
            ("etf", "market_price", 1, 3, 0, 1),
            ("bond", "market_price", 2, 5, 0, 1),
            ("cash", "market_price", 1, 2, 1, 1),
        ]
        for asset_class, source_type, warn, breach, block, esc in defaults:
            conn.execute(
                """
                INSERT INTO staleness_sla_policies (
                  policy_id, asset_class, source_type, max_lag_days_warning, max_lag_days_breach,
                  nav_blocking, escalation_enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (f"sla_{uuid.uuid4().hex[:12]}", asset_class, source_type, warn, breach, block, esc, now, now),
            )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS availability_history (
          history_id TEXT PRIMARY KEY,
          service_domain TEXT NOT NULL,
          status TEXT NOT NULL,
          issue_count INTEGER NOT NULL DEFAULT 0,
          entered_at TEXT NOT NULL,
          exited_at TEXT,
          duration_seconds INTEGER,
          root_cause TEXT,
          run_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_availability_history_domain
        ON availability_history (service_domain, entered_at DESC)
        """
    )
    conn.commit()


def list_sla_policies(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_sla_tables(conn)
    rows = conn.execute(
        """
        SELECT policy_id, asset_class, source_type, max_lag_days_warning, max_lag_days_breach,
               nav_blocking, escalation_enabled, created_at, updated_at
        FROM staleness_sla_policies
        ORDER BY asset_class, source_type
        """
    ).fetchall()
    return [dict(row) for row in rows]


def update_sla_policy(conn: sqlite3.Connection, *, policy_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    ensure_sla_tables(conn)
    allowed = {"max_lag_days_warning", "max_lag_days_breach", "nav_blocking", "escalation_enabled"}
    assignments = []
    values: list[Any] = []
    for key, value in patch.items():
        if key not in allowed:
            continue
        assignments.append(f"{key} = ?")
        values.append(value)
    if assignments:
        assignments.append("updated_at = ?")
        values.append(_now_iso())
        values.append(policy_id)
        conn.execute(f"UPDATE staleness_sla_policies SET {', '.join(assignments)} WHERE policy_id = ?", tuple(values))
        conn.commit()
    row = conn.execute("SELECT * FROM staleness_sla_policies WHERE policy_id = ? LIMIT 1", (policy_id,)).fetchone()
    if row is None:
        raise ValueError("SLA policy not found.")
    return dict(row)


def classify_staleness_impact(exposures: dict[str, Any], portfolio_summary: dict[str, Any]) -> dict[str, Any]:
    stale_weight = float((exposures.get("summary") or {}).get("stale_priced_weight") or 0.0)
    stale_value = float((exposures.get("summary") or {}).get("stale_priced_value") or 0.0)
    if stale_weight >= 0.10:
        nav_confidence = "blocked"
    elif stale_weight >= 0.05:
        nav_confidence = "degraded"
    else:
        nav_confidence = "normal"
    return {
        "stale_priced_market_value": round(stale_value, 2),
        "stale_priced_weight": round(stale_weight, 6),
        "nav_confidence_flag": nav_confidence,
        "pricing_mode": "official_close" if stale_weight == 0 else "mixed_or_stale",
        "valuation_time": portfolio_summary.get("price_as_of_date"),
        "degradation_reason_summary": (
            "No stale-price impact."
            if stale_weight == 0
            else f"{stale_weight * 100:.1f}% of portfolio is valued on stale or fallback prices."
        ),
    }


def record_availability_snapshot(
    conn: sqlite3.Connection,
    *,
    availability: dict[str, Any],
    run_id: str | None = None,
) -> None:
    ensure_sla_tables(conn)
    now = _now_iso()
    for domain in ("portfolio", "blueprint", "daily_brief"):
        status = str(availability.get(domain) or "unknown")
        issue_count = len(list(availability.get("issues") or []))
        open_row = conn.execute(
            """
            SELECT history_id, status, entered_at
            FROM availability_history
            WHERE service_domain = ? AND exited_at IS NULL
            ORDER BY entered_at DESC
            LIMIT 1
            """,
            (domain,),
        ).fetchone()
        if open_row is not None and str(open_row["status"]) == status:
            continue
        if open_row is not None:
            entered_at = datetime.fromisoformat(str(open_row["entered_at"]))
            duration_seconds = int((datetime.now(UTC) - entered_at).total_seconds())
            conn.execute(
                """
                UPDATE availability_history
                SET exited_at = ?, duration_seconds = ?, root_cause = ?
                WHERE history_id = ?
                """,
                (now, duration_seconds, "; ".join(list(availability.get("issues") or [])), str(open_row["history_id"])),
            )
        conn.execute(
            """
            INSERT INTO availability_history (
              history_id, service_domain, status, issue_count, entered_at, exited_at, duration_seconds, root_cause, run_id
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?)
            """,
            (f"availability_{uuid.uuid4().hex[:12]}", domain, status, issue_count, now, "; ".join(list(availability.get("issues") or [])), run_id),
        )
    conn.commit()


def list_availability_history(conn: sqlite3.Connection, *, limit: int = 100) -> list[dict[str, Any]]:
    ensure_sla_tables(conn)
    rows = conn.execute(
        """
        SELECT history_id, service_domain, status, issue_count, entered_at, exited_at, duration_seconds, root_cause, run_id
        FROM availability_history
        ORDER BY entered_at DESC
        LIMIT ?
        """,
        (max(1, limit),),
    ).fetchall()
    return [dict(row) for row in rows]
