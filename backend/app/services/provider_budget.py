from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.services.provider_registry import PROVIDER_CAPABILITY_MATRIX


def _now() -> datetime:
    return datetime.now(UTC)


def _iso_now() -> str:
    return _now().isoformat()


def ensure_provider_budget_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_usage_ledger (
          ledger_id TEXT PRIMARY KEY,
          provider_name TEXT NOT NULL,
          endpoint_family TEXT NOT NULL,
          date_bucket TEXT NOT NULL,
          month_bucket TEXT NOT NULL,
          call_count INTEGER NOT NULL DEFAULT 1,
          estimated_cost_unit REAL NOT NULL DEFAULT 1.0,
          success INTEGER NOT NULL DEFAULT 1,
          triggered_by_job TEXT,
          triggered_by_surface TEXT,
          cache_hit INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_provider_usage_provider_month
        ON provider_usage_ledger (provider_name, month_bucket, endpoint_family)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_budget_policy (
          provider_name TEXT PRIMARY KEY,
          daily_soft_budget REAL NOT NULL,
          monthly_soft_budget REAL NOT NULL,
          monthly_hard_budget REAL NOT NULL,
          reserve_percentage REAL NOT NULL DEFAULT 0.15,
          critical_use_only_threshold REAL NOT NULL DEFAULT 0.85,
          blocked_threshold REAL NOT NULL DEFAULT 1.0,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_surface_budget_policy (
          provider_name TEXT NOT NULL,
          surface_name TEXT NOT NULL,
          daily_reserved_budget REAL NOT NULL DEFAULT 0,
          monthly_reserved_budget REAL NOT NULL DEFAULT 0,
          importance_weight REAL NOT NULL DEFAULT 1.0,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (provider_name, surface_name)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_health_state (
          provider_name TEXT PRIMARY KEY,
          last_successful_fetch_at TEXT,
          last_failure_at TEXT,
          failure_streak INTEGER NOT NULL DEFAULT 0,
          current_mode TEXT NOT NULL DEFAULT 'normal',
          quota_state TEXT NOT NULL DEFAULT 'normal',
          last_error TEXT,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_cache_snapshots (
          snapshot_id TEXT PRIMARY KEY,
          provider_name TEXT NOT NULL,
          endpoint_family TEXT NOT NULL,
          cache_key TEXT NOT NULL,
          surface_name TEXT,
          payload_json TEXT NOT NULL,
          fetched_at TEXT NOT NULL,
          expires_at TEXT,
          freshness_state TEXT NOT NULL,
          confidence_tier TEXT,
          source_ref TEXT,
          cache_status TEXT NOT NULL,
          fallback_used INTEGER NOT NULL DEFAULT 0,
          error_state TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_provider_cache_key
        ON provider_cache_snapshots (provider_name, endpoint_family, cache_key, surface_name)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_surface_snapshot_versions (
          surface_name TEXT NOT NULL,
          family_name TEXT NOT NULL,
          snapshot_version TEXT NOT NULL,
          latest_observed_at TEXT,
          latest_fetched_at TEXT,
          provider_mix_json TEXT NOT NULL DEFAULT '[]',
          updated_at TEXT NOT NULL,
          PRIMARY KEY (surface_name, family_name)
        )
        """
    )
    conn.commit()
    seed_provider_budget_policies(conn)


def seed_provider_budget_policies(conn: sqlite3.Connection) -> None:
    defaults = {
        "alpha_vantage": (80, 1800, 2200),
        "fmp": (120, 3000, 4000),
        "finnhub": (100, 2000, 2800),
        "polygon": (100, 2000, 2600),
        "tiingo": (100, 2200, 3000),
        "eodhd": (120, 2500, 3200),
        "nasdaq_data_link": (10, 120, 160),
        "twelve_data": (120, 2500, 3200),
    }
    surface_defaults = {
        "daily_brief": {"reserve": 0.20, "weight": 0.90},
        "dashboard": {"reserve": 0.45, "weight": 1.25},
        "blueprint": {"reserve": 0.30, "weight": 1.10},
    }
    now = _iso_now()
    for provider_name in PROVIDER_CAPABILITY_MATRIX.keys():
        daily, monthly_soft, monthly_hard = defaults.get(provider_name, (50, 1000, 1500))
        conn.execute(
            """
            INSERT INTO provider_budget_policy (
              provider_name, daily_soft_budget, monthly_soft_budget, monthly_hard_budget,
              reserve_percentage, critical_use_only_threshold, blocked_threshold, updated_at
            )
            VALUES (?, ?, ?, ?, 0.15, 0.85, 1.0, ?)
            ON CONFLICT(provider_name) DO NOTHING
            """,
            (provider_name, daily, monthly_soft, monthly_hard, now),
        )
        conn.execute(
            """
            INSERT INTO provider_health_state (
              provider_name, failure_streak, current_mode, quota_state, updated_at
            )
            VALUES (?, 0, 'normal', 'normal', ?)
            ON CONFLICT(provider_name) DO NOTHING
            """,
            (provider_name, now),
        )
        for surface_name, config in surface_defaults.items():
            reserve = float(config["reserve"])
            conn.execute(
                """
                INSERT INTO provider_surface_budget_policy (
                  provider_name, surface_name, daily_reserved_budget, monthly_reserved_budget,
                  importance_weight, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider_name, surface_name) DO NOTHING
                """,
                (
                    provider_name,
                    surface_name,
                    round(daily * reserve, 2),
                    round(monthly_soft * reserve, 2),
                    float(config["weight"]),
                    now,
                ),
            )
    conn.commit()


def _usage_totals(conn: sqlite3.Connection, provider_name: str) -> tuple[float, float]:
    now = _now()
    date_bucket = now.date().isoformat()
    month_bucket = now.strftime("%Y-%m")
    daily = conn.execute(
        """
        SELECT COALESCE(SUM(call_count * estimated_cost_unit), 0)
        FROM provider_usage_ledger
        WHERE provider_name = ? AND date_bucket = ?
        """,
        (provider_name, date_bucket),
    ).fetchone()[0]
    monthly = conn.execute(
        """
        SELECT COALESCE(SUM(call_count * estimated_cost_unit), 0)
        FROM provider_usage_ledger
        WHERE provider_name = ? AND month_bucket = ?
        """,
        (provider_name, month_bucket),
    ).fetchone()[0]
    return float(daily or 0), float(monthly or 0)


def _compute_budget_mode(policy: dict[str, Any], daily_used: float, monthly_used: float) -> str:
    monthly_soft = float(policy["monthly_soft_budget"] or 0)
    monthly_hard = float(policy["monthly_hard_budget"] or 0)
    ratio_soft = (monthly_used / monthly_soft) if monthly_soft else 0.0
    ratio_hard = (monthly_used / monthly_hard) if monthly_hard else 0.0
    if ratio_hard >= float(policy["blocked_threshold"]):
        return "blocked"
    if ratio_soft >= float(policy["critical_use_only_threshold"]):
        return "critical_only"
    if ratio_soft >= 0.6:
        return "conserve"
    return "normal"


def _surface_reservation(
    conn: sqlite3.Connection, provider_name: str, surface_name: str
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT provider_name, surface_name, daily_reserved_budget, monthly_reserved_budget, importance_weight, updated_at
        FROM provider_surface_budget_policy
        WHERE provider_name = ? AND surface_name = ?
        """,
        (provider_name, surface_name),
    ).fetchone()
    return dict(row) if row is not None else None


def peek_provider_budget_state(conn: sqlite3.Connection, provider_name: str) -> dict[str, Any]:
    ensure_provider_budget_tables(conn)
    row = conn.execute(
        """
        SELECT *
        FROM provider_budget_policy
        WHERE provider_name = ?
        """,
        (provider_name,),
    ).fetchone()
    if row is None:
        return {"provider_name": provider_name, "mode": "normal"}
    policy = dict(row)
    daily_used, monthly_used = _usage_totals(conn, provider_name)
    mode = _compute_budget_mode(policy, daily_used, monthly_used)
    return {
        "provider_name": provider_name,
        "mode": mode,
        "daily_used": daily_used,
        "monthly_used": monthly_used,
        "policy": policy,
    }


def peek_surface_budget_state(
    conn: sqlite3.Connection, provider_name: str, surface_name: str
) -> dict[str, Any]:
    ensure_provider_budget_tables(conn)
    base = peek_provider_budget_state(conn, provider_name)
    reservation = _surface_reservation(conn, provider_name, surface_name) or {}
    daily_used, monthly_used = _usage_totals(conn, provider_name)
    daily_reserved = float(reservation.get("daily_reserved_budget") or 0.0)
    monthly_reserved = float(reservation.get("monthly_reserved_budget") or 0.0)
    mode = str(base.get("mode") or "normal")
    if mode == "normal":
        if monthly_reserved and monthly_used >= monthly_reserved * 0.85:
            mode = "critical_only"
        elif daily_reserved and daily_used >= daily_reserved * 0.85:
            mode = "conserve"
    return {
        "provider_name": provider_name,
        "surface_name": surface_name,
        "mode": mode,
        "daily_used": daily_used,
        "monthly_used": monthly_used,
        "reservation": reservation,
        "provider_policy": base.get("policy"),
    }


def get_provider_budget_state(conn: sqlite3.Connection, provider_name: str) -> dict[str, Any]:
    state = peek_provider_budget_state(conn, provider_name)
    mode = str(state.get("mode") or "normal")
    conn.execute(
        """
        UPDATE provider_health_state
        SET current_mode = ?, quota_state = ?, updated_at = ?
        WHERE provider_name = ?
        """,
        (mode, mode, _iso_now(), provider_name),
    )
    conn.commit()
    return state


def list_surface_budget_policies(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_provider_budget_tables(conn)
    rows = conn.execute(
        """
        SELECT provider_name, surface_name, daily_reserved_budget, monthly_reserved_budget, importance_weight, updated_at
        FROM provider_surface_budget_policy
        ORDER BY surface_name, provider_name
        """
    ).fetchall()
    return [dict(row) for row in rows]


def upsert_surface_snapshot_version(
    conn: sqlite3.Connection,
    *,
    surface_name: str,
    family_name: str,
    snapshot_version: str,
    latest_observed_at: str | None,
    latest_fetched_at: str | None,
    provider_mix: list[str],
) -> None:
    ensure_provider_budget_tables(conn)
    conn.execute(
        """
        INSERT INTO provider_surface_snapshot_versions (
          surface_name, family_name, snapshot_version, latest_observed_at, latest_fetched_at, provider_mix_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(surface_name, family_name)
        DO UPDATE SET
          snapshot_version = excluded.snapshot_version,
          latest_observed_at = excluded.latest_observed_at,
          latest_fetched_at = excluded.latest_fetched_at,
          provider_mix_json = excluded.provider_mix_json,
          updated_at = excluded.updated_at
        """,
        (
            surface_name,
            family_name,
            snapshot_version,
            latest_observed_at,
            latest_fetched_at,
            json_dumps_sorted(provider_mix),
            _iso_now(),
        ),
    )
    conn.commit()


def list_surface_snapshot_versions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_provider_budget_tables(conn)
    rows = conn.execute(
        """
        SELECT surface_name, family_name, snapshot_version, latest_observed_at, latest_fetched_at, provider_mix_json, updated_at
        FROM provider_surface_snapshot_versions
        ORDER BY surface_name, family_name
        """
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        try:
            item["provider_mix"] = json.loads(str(item.get("provider_mix_json") or "[]"))
        except Exception:
            item["provider_mix"] = []
        items.append(item)
    return items


def json_dumps_sorted(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def record_provider_usage(
    conn: sqlite3.Connection,
    *,
    provider_name: str,
    endpoint_family: str,
    estimated_cost_unit: float = 1.0,
    success: bool = True,
    triggered_by_job: str | None = None,
    triggered_by_surface: str | None = None,
    cache_hit: bool = False,
) -> None:
    ensure_provider_budget_tables(conn)
    now = _now()
    conn.execute(
        """
        INSERT INTO provider_usage_ledger (
          ledger_id, provider_name, endpoint_family, date_bucket, month_bucket, call_count,
          estimated_cost_unit, success, triggered_by_job, triggered_by_surface, cache_hit, created_at
        )
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"provider_ledger_{uuid.uuid4().hex[:12]}",
            provider_name,
            endpoint_family,
            now.date().isoformat(),
            now.strftime("%Y-%m"),
            float(estimated_cost_unit),
            1 if success else 0,
            triggered_by_job,
            triggered_by_surface,
            1 if cache_hit else 0,
            now.isoformat(),
        ),
    )
    conn.commit()


def record_provider_health(
    conn: sqlite3.Connection,
    *,
    provider_name: str,
    success: bool,
    error: str | None = None,
) -> None:
    ensure_provider_budget_tables(conn)
    row = conn.execute(
        "SELECT failure_streak FROM provider_health_state WHERE provider_name = ?",
        (provider_name,),
    ).fetchone()
    streak = int(row["failure_streak"]) if row else 0
    now = _iso_now()
    if success:
        conn.execute(
            """
            UPDATE provider_health_state
            SET last_successful_fetch_at = ?, failure_streak = 0, last_error = NULL, updated_at = ?
            WHERE provider_name = ?
            """,
            (now, now, provider_name),
        )
    else:
        conn.execute(
            """
            UPDATE provider_health_state
            SET last_failure_at = ?, failure_streak = ?, last_error = ?, updated_at = ?
            WHERE provider_name = ?
            """,
            (now, streak + 1, error, now, provider_name),
        )
    conn.commit()


def list_provider_health(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_provider_budget_tables(conn)
    rows = conn.execute(
        """
        SELECT provider_name, last_successful_fetch_at, last_failure_at, failure_streak,
               current_mode, quota_state, last_error, updated_at
        FROM provider_health_state
        ORDER BY provider_name
        """
    ).fetchall()
    return [dict(row) for row in rows]
