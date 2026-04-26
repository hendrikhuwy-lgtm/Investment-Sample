from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return {str(row[1]) for row in rows}


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, ddl: str) -> None:
    if column_name in _table_columns(conn, table_name):
        return
    conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN {column_name} {ddl}')


def ensure_blueprint_market_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_market_identities (
          identity_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          instrument_id TEXT NOT NULL,
          symbol TEXT NOT NULL,
          provider_symbol TEXT NOT NULL,
          provider_asset_class TEXT,
          exchange_mic TEXT,
          quote_currency TEXT,
          series_role TEXT NOT NULL,
          adjustment_mode TEXT NOT NULL,
          timezone TEXT NOT NULL,
          primary_interval TEXT NOT NULL,
          preferred_lookback_days INTEGER NOT NULL,
          forecast_eligibility TEXT NOT NULL,
          proxy_relationship TEXT,
          resolution_method TEXT,
          resolution_confidence REAL,
          resolved_from TEXT,
          last_verified_at TEXT,
          forecast_driving_series INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(candidate_id, series_role, primary_interval)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_candidate_market_identities_candidate
        ON candidate_market_identities (candidate_id, forecast_driving_series DESC, series_role)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_price_series (
          row_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          instrument_id TEXT NOT NULL,
          series_role TEXT NOT NULL,
          timestamp_utc TEXT NOT NULL,
          interval TEXT NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume REAL,
          amount REAL,
          provider TEXT NOT NULL,
          provider_symbol TEXT NOT NULL,
          adjusted_flag INTEGER NOT NULL DEFAULT 0,
          freshness_ts TEXT NOT NULL,
          quality_flags_json TEXT NOT NULL DEFAULT '[]',
          series_quality_summary_json TEXT NOT NULL DEFAULT '{}',
          ingest_run_id TEXT NOT NULL,
          UNIQUE(candidate_id, series_role, interval, timestamp_utc)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_candidate_price_series_candidate
        ON candidate_price_series (candidate_id, series_role, interval, timestamp_utc DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_price_series_runs (
          ingest_run_id TEXT PRIMARY KEY,
          run_type TEXT NOT NULL,
          candidate_id TEXT NOT NULL,
          series_role TEXT NOT NULL,
          provider TEXT NOT NULL,
          status TEXT NOT NULL,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          bars_written INTEGER NOT NULL DEFAULT 0,
          failure_class TEXT,
          details_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_candidate_price_series_runs_candidate
        ON candidate_price_series_runs (candidate_id, series_role, started_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_market_forecast_runs (
          forecast_run_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          series_role TEXT NOT NULL DEFAULT 'direct',
          model_name TEXT NOT NULL,
          model_version TEXT NOT NULL,
          input_series_version TEXT NOT NULL,
          run_status TEXT NOT NULL,
          usefulness_label TEXT NOT NULL,
          suppression_reason TEXT,
          generated_at TEXT NOT NULL,
          details_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_candidate_market_forecast_runs_candidate
        ON candidate_market_forecast_runs (candidate_id, generated_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_market_forecast_artifacts (
          artifact_id TEXT PRIMARY KEY,
          forecast_run_id TEXT NOT NULL,
          candidate_id TEXT NOT NULL,
          input_series_version TEXT NOT NULL,
          market_path_support_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_candidate_market_forecast_artifacts_candidate
        ON candidate_market_forecast_artifacts (candidate_id, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_candidate_market_forecast_artifacts_series_version
        ON candidate_market_forecast_artifacts (candidate_id, input_series_version)
        """
    )
    _ensure_column(conn, "candidate_market_forecast_runs", "series_role", "TEXT NOT NULL DEFAULT 'direct'")
    conn.commit()


def upsert_market_identity(conn: sqlite3.Connection, identity: dict[str, Any]) -> dict[str, Any]:
    ensure_blueprint_market_tables(conn)
    payload = dict(identity)
    now_iso = _now_iso()
    payload.setdefault("identity_id", f"candidate_market_identity_{uuid.uuid4().hex[:12]}")
    payload.setdefault("created_at", now_iso)
    payload["updated_at"] = now_iso
    conn.execute(
        """
        INSERT INTO candidate_market_identities (
          identity_id, candidate_id, instrument_id, symbol, provider_symbol,
          provider_asset_class, exchange_mic, quote_currency, series_role,
          adjustment_mode, timezone, primary_interval, preferred_lookback_days,
          forecast_eligibility, proxy_relationship, resolution_method,
          resolution_confidence, resolved_from, last_verified_at,
          forecast_driving_series, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id, series_role, primary_interval)
        DO UPDATE SET
          instrument_id = excluded.instrument_id,
          symbol = excluded.symbol,
          provider_symbol = excluded.provider_symbol,
          provider_asset_class = excluded.provider_asset_class,
          exchange_mic = excluded.exchange_mic,
          quote_currency = excluded.quote_currency,
          adjustment_mode = excluded.adjustment_mode,
          timezone = excluded.timezone,
          preferred_lookback_days = excluded.preferred_lookback_days,
          forecast_eligibility = excluded.forecast_eligibility,
          proxy_relationship = excluded.proxy_relationship,
          resolution_method = excluded.resolution_method,
          resolution_confidence = excluded.resolution_confidence,
          resolved_from = excluded.resolved_from,
          last_verified_at = excluded.last_verified_at,
          forecast_driving_series = excluded.forecast_driving_series,
          updated_at = excluded.updated_at
        """,
        (
            payload["identity_id"],
            payload["candidate_id"],
            payload["instrument_id"],
            payload["symbol"],
            payload["provider_symbol"],
            payload.get("provider_asset_class"),
            payload.get("exchange_mic"),
            payload.get("quote_currency"),
            payload["series_role"],
            payload["adjustment_mode"],
            payload["timezone"],
            payload["primary_interval"],
            int(payload["preferred_lookback_days"]),
            payload["forecast_eligibility"],
            payload.get("proxy_relationship"),
            payload.get("resolution_method"),
            payload.get("resolution_confidence"),
            payload.get("resolved_from"),
            payload.get("last_verified_at"),
            1 if payload.get("forecast_driving_series") else 0,
            payload["created_at"],
            payload["updated_at"],
        ),
    )
    conn.commit()
    return payload


def list_market_identities(conn: sqlite3.Connection, candidate_id: str) -> list[dict[str, Any]]:
    ensure_blueprint_market_tables(conn)
    rows = conn.execute(
        """
        SELECT *
        FROM candidate_market_identities
        WHERE candidate_id = ?
        ORDER BY forecast_driving_series DESC, series_role ASC
        """,
        (str(candidate_id),),
    ).fetchall()
    return [dict(row) for row in rows]


def record_series_run_start(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    series_role: str,
    provider: str,
    run_type: str,
) -> str:
    ensure_blueprint_market_tables(conn)
    ingest_run_id = f"candidate_series_run_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO candidate_price_series_runs (
          ingest_run_id, run_type, candidate_id, series_role, provider, status, started_at
        ) VALUES (?, ?, ?, ?, ?, 'running', ?)
        """,
        (ingest_run_id, run_type, candidate_id, series_role, provider, _now_iso()),
    )
    conn.commit()
    return ingest_run_id


def finish_series_run(
    conn: sqlite3.Connection,
    *,
    ingest_run_id: str,
    status: str,
    bars_written: int,
    failure_class: str | None,
    details: dict[str, Any] | None = None,
) -> None:
    ensure_blueprint_market_tables(conn)
    conn.execute(
        """
        UPDATE candidate_price_series_runs
        SET status = ?,
            finished_at = ?,
            bars_written = ?,
            failure_class = ?,
            details_json = ?
        WHERE ingest_run_id = ?
        """,
        (
            status,
            _now_iso(),
            int(bars_written),
            failure_class,
            json.dumps(details or {}, sort_keys=True, ensure_ascii=True),
            ingest_run_id,
        ),
    )
    conn.commit()


def upsert_price_series_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    ensure_blueprint_market_tables(conn)
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO candidate_price_series (
          row_id, candidate_id, instrument_id, series_role, timestamp_utc, interval,
          open, high, low, close, volume, amount, provider, provider_symbol,
          adjusted_flag, freshness_ts, quality_flags_json, series_quality_summary_json, ingest_run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id, series_role, interval, timestamp_utc)
        DO UPDATE SET
          open = excluded.open,
          high = excluded.high,
          low = excluded.low,
          close = excluded.close,
          volume = excluded.volume,
          amount = excluded.amount,
          provider = excluded.provider,
          provider_symbol = excluded.provider_symbol,
          adjusted_flag = excluded.adjusted_flag,
          freshness_ts = excluded.freshness_ts,
          quality_flags_json = excluded.quality_flags_json,
          series_quality_summary_json = excluded.series_quality_summary_json,
          ingest_run_id = excluded.ingest_run_id
        """,
        [
            (
                row.get("row_id") or f"candidate_price_series_{uuid.uuid4().hex[:12]}",
                row["candidate_id"],
                row["instrument_id"],
                row["series_role"],
                row["timestamp_utc"],
                row["interval"],
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                row.get("volume"),
                row.get("amount"),
                row["provider"],
                row["provider_symbol"],
                1 if row.get("adjusted_flag") else 0,
                row["freshness_ts"],
                json.dumps(list(row.get("quality_flags") or []), sort_keys=True, ensure_ascii=True),
                json.dumps(dict(row.get("series_quality_summary") or {}), sort_keys=True, ensure_ascii=True),
                row["ingest_run_id"],
            )
            for row in rows
        ],
    )
    conn.commit()


def load_price_series(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    series_role: str,
    interval: str = "1day",
    limit: int | None = None,
    ascending: bool = True,
) -> list[dict[str, Any]]:
    ensure_blueprint_market_tables(conn)
    order = "ASC" if ascending else "DESC"
    sql = """
        SELECT *
        FROM candidate_price_series
        WHERE candidate_id = ? AND series_role = ? AND interval = ?
        ORDER BY timestamp_utc {order}
    """.format(order=order)
    params: list[Any] = [candidate_id, series_role, interval]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    rows = conn.execute(sql, params).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        try:
            item["quality_flags"] = json.loads(str(item.get("quality_flags_json") or "[]"))
        except Exception:
            item["quality_flags"] = []
        try:
            item["series_quality_summary"] = json.loads(str(item.get("series_quality_summary_json") or "{}"))
        except Exception:
            item["series_quality_summary"] = {}
        items.append(item)
    return items


def load_reusable_price_series(
    conn: sqlite3.Connection,
    *,
    provider_symbol: str,
    series_role: str,
    interval: str,
    exclude_candidate_id: str | None = None,
) -> dict[str, Any] | None:
    ensure_blueprint_market_tables(conn)
    params: list[Any] = [str(provider_symbol), str(series_role), str(interval)]
    where = """
        provider_symbol = ?
        AND series_role = ?
        AND interval = ?
    """
    if exclude_candidate_id:
        where += " AND candidate_id <> ?"
        params.append(str(exclude_candidate_id))
    row = conn.execute(
        f"""
        SELECT candidate_id, COUNT(*) AS bar_count, MAX(timestamp_utc) AS latest_timestamp
        FROM candidate_price_series
        WHERE {where}
        GROUP BY candidate_id
        ORDER BY bar_count DESC, latest_timestamp DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        return None
    source_candidate_id = str(row["candidate_id"] or "")
    rows = load_price_series(
        conn,
        candidate_id=source_candidate_id,
        series_role=str(series_role),
        interval=str(interval),
        ascending=True,
    )
    if not rows:
        return None
    return {
        "source_candidate_id": source_candidate_id,
        "bar_count": int(row["bar_count"] or 0),
        "latest_timestamp": row["latest_timestamp"],
        "rows": rows,
    }


def latest_series_version(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    series_role: str,
    interval: str = "1day",
) -> str | None:
    ensure_blueprint_market_tables(conn)
    row = conn.execute(
        """
        SELECT ingest_run_id
        FROM candidate_price_series
        WHERE candidate_id = ? AND series_role = ? AND interval = ?
        ORDER BY timestamp_utc DESC
        LIMIT 1
        """,
        (candidate_id, series_role, interval),
    ).fetchone()
    return str(row[0]) if row is not None and row[0] else None


def record_forecast_run(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    series_role: str,
    model_name: str,
    model_version: str,
    input_series_version: str,
    run_status: str,
    usefulness_label: str,
    suppression_reason: str | None,
    details: dict[str, Any] | None = None,
) -> str:
    ensure_blueprint_market_tables(conn)
    forecast_run_id = f"candidate_market_forecast_run_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO candidate_market_forecast_runs (
          forecast_run_id, candidate_id, series_role, model_name, model_version,
          input_series_version, run_status, usefulness_label, suppression_reason,
          generated_at, details_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            forecast_run_id,
            candidate_id,
            series_role,
            model_name,
            model_version,
            input_series_version,
            run_status,
            usefulness_label,
            suppression_reason,
            _now_iso(),
            json.dumps(details or {}, sort_keys=True, ensure_ascii=True),
        ),
    )
    conn.commit()
    return forecast_run_id


def persist_forecast_artifact(
    conn: sqlite3.Connection,
    *,
    forecast_run_id: str,
    candidate_id: str,
    input_series_version: str,
    market_path_support: dict[str, Any],
) -> None:
    ensure_blueprint_market_tables(conn)
    conn.execute(
        """
        INSERT INTO candidate_market_forecast_artifacts (
          artifact_id, forecast_run_id, candidate_id, input_series_version,
          market_path_support_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id, input_series_version)
        DO UPDATE SET
          forecast_run_id = excluded.forecast_run_id,
          market_path_support_json = excluded.market_path_support_json,
          created_at = excluded.created_at
        """,
        (
            f"candidate_market_forecast_artifact_{uuid.uuid4().hex[:12]}",
            forecast_run_id,
            candidate_id,
            input_series_version,
            json.dumps(market_path_support, sort_keys=True, ensure_ascii=True),
            _now_iso(),
        ),
    )
    conn.commit()


def latest_forecast_artifact(conn: sqlite3.Connection, *, candidate_id: str) -> dict[str, Any] | None:
    ensure_blueprint_market_tables(conn)
    row = conn.execute(
        """
        SELECT artifact_id, forecast_run_id, candidate_id, input_series_version,
               market_path_support_json, created_at
        FROM candidate_market_forecast_artifacts
        WHERE candidate_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (candidate_id,),
    ).fetchone()
    if row is None:
        return None
    item = dict(row)
    try:
        item["market_path_support"] = json.loads(str(item.get("market_path_support_json") or "{}"))
    except Exception:
        item["market_path_support"] = {}
    return item
