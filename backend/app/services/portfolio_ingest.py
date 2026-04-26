from __future__ import annotations

import csv
import io
import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.services.portfolio_matcher import (
    build_security_key,
    classify_mapping_status,
    infer_asset_type,
    normalize_symbol,
)
from app.services.portfolio_pricing import capture_market_price_snapshot
from app.services.portfolio_state import (
    list_sleeve_overrides,
    normalize_account_type,
    resolve_sleeve,
    save_snapshot,
    upsert_holding,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return any(str(row[1]) == column_name for row in rows)


def ensure_portfolio_control_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_upload_runs (
          run_id TEXT PRIMARY KEY,
          uploaded_at TEXT NOT NULL,
          holdings_as_of_date TEXT NOT NULL,
          filename TEXT,
          source_name TEXT,
          status TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 0,
          is_deleted INTEGER NOT NULL DEFAULT 0,
          deleted_at TEXT,
          deleted_reason TEXT,
          snapshot_id TEXT,
          raw_row_count INTEGER NOT NULL DEFAULT 0,
          parsed_row_count INTEGER NOT NULL DEFAULT 0,
          normalized_position_count INTEGER NOT NULL DEFAULT 0,
          total_market_value REAL NOT NULL DEFAULT 0,
          stale_price_count INTEGER NOT NULL DEFAULT 0,
          mapping_issue_count INTEGER NOT NULL DEFAULT 0,
          warning_count INTEGER NOT NULL DEFAULT 0,
          warnings_json TEXT NOT NULL DEFAULT '[]',
          errors_json TEXT NOT NULL DEFAULT '[]'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_holding_snapshots (
          snapshot_row_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          uploaded_at TEXT NOT NULL,
          holdings_as_of_date TEXT NOT NULL,
          price_as_of_date TEXT,
          account_id TEXT NOT NULL,
          security_key TEXT NOT NULL,
          raw_symbol TEXT NOT NULL,
          normalized_symbol TEXT NOT NULL,
          security_name TEXT NOT NULL,
          asset_type TEXT NOT NULL,
          currency TEXT NOT NULL,
          quantity REAL NOT NULL,
          cost_basis REAL NOT NULL,
          market_price REAL,
          market_value REAL,
          fx_rate_to_base REAL,
          base_currency TEXT NOT NULL DEFAULT 'SGD',
          sleeve TEXT,
          mapping_status TEXT NOT NULL DEFAULT 'unmapped',
          price_source TEXT,
          price_stale INTEGER NOT NULL DEFAULT 0,
          venue TEXT,
          identifier_isin TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_mapping_issues (
          issue_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          account_id TEXT,
          security_key TEXT,
          issue_type TEXT NOT NULL,
          severity TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    for column_name, definition in (
        ("filename", "TEXT"),
        ("is_active", "INTEGER NOT NULL DEFAULT 0"),
        ("is_deleted", "INTEGER NOT NULL DEFAULT 0"),
        ("deleted_at", "TEXT"),
        ("deleted_reason", "TEXT"),
        ("snapshot_id", "TEXT"),
        ("normalized_position_count", "INTEGER NOT NULL DEFAULT 0"),
        ("total_market_value", "REAL NOT NULL DEFAULT 0"),
        ("stale_price_count", "INTEGER NOT NULL DEFAULT 0"),
        ("mapping_issue_count", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if not _column_exists(conn, "portfolio_upload_runs", column_name):
            conn.execute(f'ALTER TABLE "portfolio_upload_runs" ADD COLUMN "{column_name}" {definition}')
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_portfolio_upload_runs_active
        ON portfolio_upload_runs (is_active, is_deleted, uploaded_at DESC)
        """
    )
    _backfill_upload_registry(conn)
    conn.commit()


def _backfill_upload_registry(conn: sqlite3.Connection) -> None:
    if not _column_exists(conn, "portfolio_upload_runs", "run_id"):
        return
    rows = conn.execute(
        """
        SELECT run_id, snapshot_id, parsed_row_count, normalized_position_count, total_market_value,
               stale_price_count, mapping_issue_count, filename
        FROM portfolio_upload_runs
        WHERE coalesce(is_deleted, 0) = 0
        ORDER BY uploaded_at DESC
        """
    ).fetchall()
    if not rows:
        return

    active_row = conn.execute(
        """
        SELECT run_id
        FROM portfolio_upload_runs
        WHERE coalesce(is_deleted, 0) = 0 AND coalesce(is_active, 0) = 1
        LIMIT 1
        """
    ).fetchone()
    if active_row is None:
        fallback = conn.execute(
            """
            SELECT run_id
            FROM portfolio_upload_runs
            WHERE coalesce(is_deleted, 0) = 0 AND coalesce(parsed_row_count, 0) > 0
            ORDER BY uploaded_at DESC
            LIMIT 1
            """
        ).fetchone()
        if fallback is not None:
            conn.execute(
                'UPDATE portfolio_upload_runs SET is_active = CASE WHEN run_id = ? THEN 1 ELSE 0 END WHERE coalesce(is_deleted, 0) = 0',
                (str(fallback["run_id"]),),
            )

    for row in rows:
        run_id = str(row["run_id"])
        snapshot_rows = conn.execute(
            """
            SELECT market_value, price_stale, mapping_status
            FROM portfolio_holding_snapshots
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchall()
        normalized_position_count = int(row["normalized_position_count"] or 0)
        total_market_value = float(row["total_market_value"] or 0.0)
        stale_price_count = int(row["stale_price_count"] or 0)
        mapping_issue_count = int(row["mapping_issue_count"] or 0)
        filename = row["filename"]
        snapshot_id = row["snapshot_id"]

        computed_positions = len(snapshot_rows)
        computed_value = round(sum(float(item["market_value"] or 0.0) for item in snapshot_rows), 2)
        computed_stale = sum(1 for item in snapshot_rows if int(item["price_stale"] or 0) == 1)
        computed_mapping = sum(
            1 for item in snapshot_rows if str(item["mapping_status"] or "unmapped") in {"unmapped", "low_confidence"}
        )
        needs_summary = (
            normalized_position_count != computed_positions
            or abs(total_market_value - computed_value) > 0.01
            or stale_price_count != computed_stale
            or mapping_issue_count != computed_mapping
            or (filename is None)
        )
        if needs_summary:
            conn.execute(
                """
                UPDATE portfolio_upload_runs
                SET normalized_position_count = ?,
                    total_market_value = ?,
                    stale_price_count = ?,
                    mapping_issue_count = ?,
                    filename = COALESCE(filename, source_name, run_id)
                WHERE run_id = ?
                """,
                (
                    computed_positions,
                    computed_value,
                    computed_stale,
                    computed_mapping,
                    run_id,
                ),
            )
        if snapshot_id is None and snapshot_rows:
            linked = conn.execute(
                """
                SELECT snapshot_id
                FROM portfolio_snapshots
                WHERE upload_run_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            if linked is not None:
                conn.execute(
                    "UPDATE portfolio_upload_runs SET snapshot_id = ? WHERE run_id = ?",
                    (str(linked["snapshot_id"]), run_id),
                )


def _rows_from_structured_text(raw_text: str) -> tuple[list[dict[str, Any]], list[str]]:
    text = str(raw_text or "").replace("\ufeff", "").strip()
    if not text:
        return [], ["Upload body is empty."]

    if text.startswith("[") or text.startswith("{"):
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = None
        if payload is not None:
            if isinstance(payload, list):
                rows = [row for row in payload if isinstance(row, dict)]
                return rows, []
            if isinstance(payload, dict):
                candidate_rows = payload.get("rows") or payload.get("holdings") or payload.get("data")
                if isinstance(candidate_rows, list):
                    rows = [row for row in candidate_rows if isinstance(row, dict)]
                    return rows, []
                return [payload], []

    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    if reader.fieldnames is None:
        return [], ["Header row is missing."]
    rows = []
    for row in reader:
        rows.append(
            {
                str(key).replace("\ufeff", "").strip().lower(): value
                for key, value in row.items()
                if key is not None
            }
        )
    return rows, []


def latest_upload_run_id(conn: sqlite3.Connection) -> str | None:
    ensure_portfolio_control_tables(conn)
    row = conn.execute(
        """
        SELECT run_id
        FROM portfolio_upload_runs
        WHERE is_deleted = 0 AND is_active = 1
        LIMIT 1
        """
    ).fetchone()
    return str(row["run_id"]) if row is not None else None


def previous_upload_run_id(conn: sqlite3.Connection, current_run_id: str | None) -> str | None:
    ensure_portfolio_control_tables(conn)
    params: tuple[Any, ...] = ()
    sql = "SELECT run_id FROM portfolio_upload_runs WHERE is_deleted = 0"
    if current_run_id:
        sql += " AND run_id <> ?"
        params = (current_run_id,)
    sql += " ORDER BY uploaded_at DESC LIMIT 1"
    row = conn.execute(sql, params).fetchone()
    return str(row["run_id"]) if row is not None else None


def latest_upload_run(conn: sqlite3.Connection) -> dict[str, Any] | None:
    ensure_portfolio_control_tables(conn)
    row = conn.execute(
        """
        SELECT run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
               deleted_at, deleted_reason, snapshot_id, raw_row_count, parsed_row_count,
               normalized_position_count, total_market_value, stale_price_count, mapping_issue_count,
               warning_count, warnings_json, errors_json
        FROM portfolio_upload_runs
        WHERE is_deleted = 0 AND is_active = 1
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return {
        "run_id": str(row["run_id"]),
        "uploaded_at": str(row["uploaded_at"]),
        "holdings_as_of_date": str(row["holdings_as_of_date"]),
        "filename": row["filename"],
        "source_name": row["source_name"],
        "status": str(row["status"]),
        "is_active": bool(row["is_active"]),
        "is_deleted": bool(row["is_deleted"]),
        "deleted_at": row["deleted_at"],
        "deleted_reason": row["deleted_reason"],
        "snapshot_id": row["snapshot_id"],
        "raw_row_count": int(row["raw_row_count"] or 0),
        "parsed_row_count": int(row["parsed_row_count"] or 0),
        "normalized_position_count": int(row["normalized_position_count"] or 0),
        "total_market_value": float(row["total_market_value"] or 0.0),
        "stale_price_count": int(row["stale_price_count"] or 0),
        "mapping_issue_count": int(row["mapping_issue_count"] or 0),
        "warning_count": int(row["warning_count"] or 0),
        "warnings": json.loads(str(row["warnings_json"] or "[]")),
        "errors": json.loads(str(row["errors_json"] or "[]")),
    }


def _upload_run_from_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": str(row["run_id"]),
        "uploaded_at": str(row["uploaded_at"]),
        "holdings_as_of_date": str(row["holdings_as_of_date"]),
        "filename": row["filename"],
        "source_name": row["source_name"],
        "status": str(row["status"]),
        "is_active": bool(row["is_active"]),
        "is_deleted": bool(row["is_deleted"]),
        "deleted_at": row["deleted_at"],
        "deleted_reason": row["deleted_reason"],
        "snapshot_id": row["snapshot_id"],
        "raw_row_count": int(row["raw_row_count"] or 0),
        "parsed_row_count": int(row["parsed_row_count"] or 0),
        "normalized_position_count": int(row["normalized_position_count"] or 0),
        "total_market_value": float(row["total_market_value"] or 0.0),
        "stale_price_count": int(row["stale_price_count"] or 0),
        "mapping_issue_count": int(row["mapping_issue_count"] or 0),
        "warning_count": int(row["warning_count"] or 0),
        "warnings": json.loads(str(row["warnings_json"] or "[]")),
        "errors": json.loads(str(row["errors_json"] or "[]")),
    }


def list_portfolio_uploads(conn: sqlite3.Connection, *, include_deleted: bool = False) -> list[dict[str, Any]]:
    ensure_portfolio_control_tables(conn)
    where = "" if include_deleted else "WHERE is_deleted = 0"
    rows = conn.execute(
        f"""
        SELECT run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
               deleted_at, deleted_reason, snapshot_id, raw_row_count, parsed_row_count,
               normalized_position_count, total_market_value, stale_price_count, mapping_issue_count,
               warning_count, warnings_json, errors_json
        FROM portfolio_upload_runs
        {where}
        ORDER BY is_active DESC, uploaded_at DESC
        """
    ).fetchall()
    return [_upload_run_from_row(row) for row in rows]


def get_portfolio_upload_detail(conn: sqlite3.Connection, run_id: str) -> dict[str, Any] | None:
    ensure_portfolio_control_tables(conn)
    row = conn.execute(
        """
        SELECT run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
               deleted_at, deleted_reason, snapshot_id, raw_row_count, parsed_row_count,
               normalized_position_count, total_market_value, stale_price_count, mapping_issue_count,
               warning_count, warnings_json, errors_json
        FROM portfolio_upload_runs
        WHERE run_id = ?
        LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    upload = _upload_run_from_row(row)
    snapshot_rows = latest_snapshot_rows(conn, run_id=run_id)
    issues = conn.execute(
        """
        SELECT issue_id, account_id, security_key, issue_type, severity, detail, created_at
        FROM portfolio_mapping_issues
        WHERE run_id = ?
        ORDER BY severity DESC, created_at DESC
        """,
        (run_id,),
    ).fetchall()
    upload["positions"] = snapshot_rows
    upload["issues"] = [dict(item) for item in issues]
    return upload


def _valid_uploads_for_promotion(conn: sqlite3.Connection, *, exclude_run_id: str | None = None) -> list[dict[str, Any]]:
    params: list[Any] = []
    sql = """
        SELECT run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
               deleted_at, deleted_reason, snapshot_id, raw_row_count, parsed_row_count,
               normalized_position_count, total_market_value, stale_price_count, mapping_issue_count,
               warning_count, warnings_json, errors_json
        FROM portfolio_upload_runs
        WHERE is_deleted = 0 AND parsed_row_count > 0
    """
    if exclude_run_id:
        sql += " AND run_id <> ?"
        params.append(exclude_run_id)
    sql += " ORDER BY uploaded_at DESC"
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [_upload_run_from_row(row) for row in rows]


def _set_active_run(conn: sqlite3.Connection, run_id: str | None) -> None:
    ensure_portfolio_control_tables(conn)
    conn.execute('UPDATE portfolio_upload_runs SET is_active = 0 WHERE is_deleted = 0')
    if run_id:
        conn.execute(
            'UPDATE portfolio_upload_runs SET is_active = 1 WHERE run_id = ? AND is_deleted = 0',
            (run_id,),
        )
    conn.commit()


def _synchronize_active_legacy_state(conn: sqlite3.Connection, run_id: str | None) -> None:
    if run_id is None:
        conn.execute("DELETE FROM portfolio_holdings")
        conn.commit()
        return
    _replace_legacy_holdings(conn, latest_snapshot_rows(conn, run_id=run_id))


def activate_portfolio_upload(conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
    detail = get_portfolio_upload_detail(conn, run_id)
    if detail is None or detail.get("is_deleted"):
        raise ValueError("Upload not found or deleted.")
    if int(detail.get("parsed_row_count") or 0) <= 0:
        raise ValueError("Cannot activate an upload with no normalized positions.")
    _set_active_run(conn, run_id)
    _synchronize_active_legacy_state(conn, run_id)
    return {
        "active_run_id": run_id,
        "active_upload": get_portfolio_upload_detail(conn, run_id),
    }


def delete_portfolio_upload(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    deleted_reason: str | None = None,
) -> dict[str, Any]:
    detail = get_portfolio_upload_detail(conn, run_id)
    if detail is None:
        raise ValueError("Upload not found.")
    if detail.get("is_deleted"):
        return {
            "deleted_run_id": run_id,
            "active_run_id": latest_upload_run_id(conn),
            "active_upload": latest_upload_run(conn),
        }

    now = _now_iso()
    conn.execute(
        """
        UPDATE portfolio_upload_runs
        SET is_deleted = 1,
            is_active = 0,
            status = CASE WHEN status = 'failed' THEN status ELSE 'deleted' END,
            deleted_at = ?,
            deleted_reason = ?
        WHERE run_id = ?
        """,
        (now, deleted_reason, run_id),
    )
    conn.execute(
        """
        UPDATE review_items
        SET status = 'archived',
            updated_at = ?
        WHERE source_run_id = ? AND status = 'open'
        """,
        (now, run_id),
    )
    conn.commit()

    promoted = _valid_uploads_for_promotion(conn, exclude_run_id=run_id)
    next_active_run_id = promoted[0]["run_id"] if promoted else None
    _set_active_run(conn, next_active_run_id)
    _synchronize_active_legacy_state(conn, next_active_run_id)
    return {
        "deleted_run_id": run_id,
        "active_run_id": next_active_run_id,
        "active_upload": latest_upload_run(conn),
    }


def latest_snapshot_rows(conn: sqlite3.Connection, *, run_id: str | None = None, account_id: str | None = None) -> list[dict[str, Any]]:
    ensure_portfolio_control_tables(conn)
    effective_run_id = run_id or latest_upload_run_id(conn)
    if not effective_run_id:
        return []
    rows = conn.execute(
        """
        SELECT snapshot_row_id, run_id, uploaded_at, holdings_as_of_date, price_as_of_date,
               account_id, security_key, raw_symbol, normalized_symbol, security_name,
               asset_type, currency, quantity, cost_basis, market_price, market_value,
               fx_rate_to_base, base_currency, sleeve, mapping_status, price_source,
               price_stale, venue, identifier_isin
        FROM portfolio_holding_snapshots
        WHERE run_id = ?
          AND (? IS NULL OR account_id = ?)
        ORDER BY market_value DESC, normalized_symbol ASC
        """,
        (effective_run_id, account_id, account_id),
    ).fetchall()
    return [dict(row) for row in rows]


def _replace_legacy_holdings(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM portfolio_holdings")
    conn.commit()
    for row in rows:
        upsert_holding(
            conn,
            {
                "symbol": row["normalized_symbol"],
                "name": row["security_name"],
                "quantity": row["quantity"],
                "cost_basis": row["cost_basis"],
                "currency": row["currency"],
                "sleeve": row.get("sleeve"),
                "account_type": normalize_account_type(row.get("account_id") or "broker"),
            },
        )


def _record_mapping_issue(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    account_id: str,
    security_key: str,
    issue_type: str,
    severity: str,
    detail: str,
) -> None:
    conn.execute(
        """
        INSERT INTO portfolio_mapping_issues (issue_id, run_id, account_id, security_key, issue_type, severity, detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"issue_{uuid.uuid4().hex[:12]}",
            run_id,
            account_id,
            security_key,
            issue_type,
            severity,
            detail,
            _now_iso(),
        ),
    )


def _create_aggregate_snapshot(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    uploaded_at: str,
    holdings_as_of_date: str,
    rows: list[dict[str, Any]],
) -> str:
    total_value = round(sum(float(item.get("market_value") or 0.0) for item in rows), 2)
    sleeve_values: dict[str, float] = {}
    for item in rows:
        sleeve = str(item.get("sleeve") or "unmapped")
        sleeve_values[sleeve] = sleeve_values.get(sleeve, 0.0) + float(item.get("market_value") or 0.0)
    sleeve_weights = {
        sleeve: (value / total_value) if total_value > 0 else 0.0
        for sleeve, value in sleeve_values.items()
    }
    top_positions = sorted((float(item.get("market_value") or 0.0) for item in rows), reverse=True)
    concentration_metrics = {
        "position_count": len(rows),
        "largest_position_pct": round((top_positions[0] / total_value), 6) if total_value > 0 and top_positions else 0.0,
        "top5_pct": round((sum(top_positions[:5]) / total_value), 6) if total_value > 0 and top_positions else 0.0,
    }
    snapshot = save_snapshot(
        conn,
        total_value=total_value,
        sleeve_weights=sleeve_weights,
        concentration_metrics=concentration_metrics,
        convex_coverage_ratio=0.0,
        tax_drag_estimate=0.0,
        notes=f"Derived from portfolio upload {run_id}.",
    )
    price_as_of_date = max((str(item.get("price_as_of_date") or "") for item in rows if item.get("price_as_of_date")), default=None)
    stale_price_count = sum(1 for item in rows if int(item.get("price_stale") or 0) == 1)
    mapping_issue_count = sum(
        1 for item in rows if str(item.get("mapping_status") or "unmapped") in {"unmapped", "low_confidence"}
    )
    conn.execute(
        """
        UPDATE portfolio_snapshots
        SET snapshot_date = ?,
            holdings_as_of_date = ?,
            price_as_of_date = ?,
            upload_run_id = ?,
            stale_price_count = ?,
            mapping_issue_count = ?
        WHERE snapshot_id = ?
        """,
        (
            holdings_as_of_date,
            holdings_as_of_date,
            price_as_of_date,
            run_id,
            stale_price_count,
            mapping_issue_count,
            snapshot.snapshot_id,
        ),
    )
    conn.commit()
    return snapshot.snapshot_id


def import_holdings_csv_to_snapshot(
    conn: sqlite3.Connection,
    csv_text: str,
    *,
    default_currency: str = "USD",
    default_account_type: str = "broker",
    allow_sleeve_override: bool = True,
    source_name: str = "manual_csv_upload",
    filename: str | None = None,
    allow_live_pricing: bool = True,
) -> dict[str, Any]:
    ensure_portfolio_control_tables(conn)
    source_rows, source_errors = _rows_from_structured_text(csv_text)
    if source_errors:
        return {"created": [], "errors": source_errors, "warnings": [], "run_id": None}
    if not source_rows:
        return {"created": [], "errors": ["No readable rows found in upload."], "warnings": [], "run_id": None}

    normalized_headers = {str(item).strip().lower() for item in source_rows[0].keys()}
    if "symbol" not in normalized_headers and "ticker" not in normalized_headers:
        return {"created": [], "errors": ["Upload is missing required symbol or ticker column."], "warnings": [], "run_id": None}

    run_id = f"upload_{uuid.uuid4().hex[:12]}"
    uploaded_at = _now_iso()
    warnings: list[str] = []
    errors: list[str] = []
    created_rows: list[dict[str, Any]] = []
    overrides = list_sleeve_overrides(conn)
    holdings_as_of_date: str | None = None
    raw_count = 0

    conn.execute("DELETE FROM portfolio_mapping_issues WHERE run_id = ?", (run_id,))

    for idx, row in enumerate(source_rows, start=2):
        raw_count += 1
        try:
            raw_symbol = str(row.get("symbol") or row.get("ticker") or "").strip()
            normalized_symbol = normalize_symbol(raw_symbol)
            if not normalized_symbol:
                raise ValueError("symbol is empty")
            security_name = str(row.get("name") or row.get("security_name") or normalized_symbol).strip() or normalized_symbol
            quantity = float(row.get("quantity") or row.get("shares") or 0.0)
            cost_basis = float(row.get("cost_basis") or row.get("price") or row.get("average_cost") or 0.0)
            if quantity <= 0:
                raise ValueError("quantity must be > 0")
            if cost_basis <= 0:
                raise ValueError("cost_basis must be > 0")
            currency = str(row.get("currency") or default_currency).strip().upper() or default_currency
            account_id = str(row.get("account_id") or default_account_type).strip() or default_account_type
            asset_type = infer_asset_type(row)
            venue = str(row.get("venue") or row.get("exchange") or "").strip().upper() or None
            isin = str(row.get("isin") or row.get("identifier_isin") or "").strip().upper() or None
            security_key = build_security_key(
                asset_type=asset_type,
                normalized_symbol=normalized_symbol,
                currency=currency,
                venue=venue,
                isin=isin,
                name=security_name,
            )
            holdings_as_of_date = str(
                row.get("holdings_as_of_date")
                or row.get("as_of_date")
                or row.get("date")
                or holdings_as_of_date
                or uploaded_at[:10]
            )
            provided_sleeve = str(row.get("sleeve") or "").strip() if allow_sleeve_override else ""
            manual_override = security_key in {f"{key}" for key in []}
            has_symbol_override = normalized_symbol in overrides
            sleeve = resolve_sleeve(conn, normalized_symbol, provided_sleeve or None)
            confidence = 1.0 if provided_sleeve or has_symbol_override else (0.85 if sleeve else 0.0)
            mapping_status = classify_mapping_status(
                target_sleeve=sleeve,
                manual_override=manual_override,
                confidence=confidence,
            )
            pricing = capture_market_price_snapshot(
                conn,
                security_key=security_key,
                normalized_symbol=normalized_symbol,
                raw_symbol=raw_symbol,
                fallback_price=cost_basis,
                fallback_currency=currency,
                allow_live=allow_live_pricing,
            )
            market_value = float(quantity) * float(pricing["market_price"]) * float(pricing["fx_rate_to_base"])
            snapshot_row = {
                "snapshot_row_id": f"holding_snapshot_{uuid.uuid4().hex[:12]}",
                "run_id": run_id,
                "uploaded_at": uploaded_at,
                "holdings_as_of_date": holdings_as_of_date,
                "price_as_of_date": pricing.get("price_as_of") or pricing.get("retrieved_at"),
                "account_id": account_id,
                "security_key": security_key,
                "raw_symbol": raw_symbol,
                "normalized_symbol": normalized_symbol,
                "security_name": security_name,
                "asset_type": asset_type,
                "currency": currency,
                "quantity": quantity,
                "cost_basis": cost_basis,
                "market_price": float(pricing["market_price"]),
                "market_value": round(market_value, 2),
                "fx_rate_to_base": float(pricing["fx_rate_to_base"]),
                "base_currency": "SGD",
                "sleeve": sleeve,
                "mapping_status": mapping_status,
                "price_source": pricing["price_source"],
                "price_stale": 1 if bool(pricing.get("price_stale")) else 0,
                "venue": venue,
                "identifier_isin": isin,
            }
            conn.execute(
                """
                INSERT INTO portfolio_holding_snapshots (
                  snapshot_row_id, run_id, uploaded_at, holdings_as_of_date, price_as_of_date,
                  account_id, security_key, raw_symbol, normalized_symbol, security_name,
                  asset_type, currency, quantity, cost_basis, market_price, market_value,
                  fx_rate_to_base, base_currency, sleeve, mapping_status, price_source,
                  price_stale, venue, identifier_isin
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_row["snapshot_row_id"],
                    snapshot_row["run_id"],
                    snapshot_row["uploaded_at"],
                    snapshot_row["holdings_as_of_date"],
                    snapshot_row["price_as_of_date"],
                    snapshot_row["account_id"],
                    snapshot_row["security_key"],
                    snapshot_row["raw_symbol"],
                    snapshot_row["normalized_symbol"],
                    snapshot_row["security_name"],
                    snapshot_row["asset_type"],
                    snapshot_row["currency"],
                    snapshot_row["quantity"],
                    snapshot_row["cost_basis"],
                    snapshot_row["market_price"],
                    snapshot_row["market_value"],
                    snapshot_row["fx_rate_to_base"],
                    snapshot_row["base_currency"],
                    snapshot_row["sleeve"],
                    snapshot_row["mapping_status"],
                    snapshot_row["price_source"],
                    snapshot_row["price_stale"],
                    snapshot_row["venue"],
                    snapshot_row["identifier_isin"],
                ),
            )
            created_rows.append(snapshot_row)
            if mapping_status in {"unmapped", "low_confidence"}:
                _record_mapping_issue(
                    conn,
                    run_id=run_id,
                    account_id=account_id,
                    security_key=security_key,
                    issue_type="mapping",
                    severity="high" if mapping_status == "unmapped" else "medium",
                    detail=f"{normalized_symbol} is {mapping_status}.",
                )
            if snapshot_row["price_stale"]:
                _record_mapping_issue(
                    conn,
                    run_id=run_id,
                    account_id=account_id,
                    security_key=security_key,
                    issue_type="stale_price",
                    severity="medium",
                    detail=f"{normalized_symbol} valuation is stale or fallback-based.",
                )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"line {idx}: {exc}")

    if not created_rows and errors:
        conn.execute(
            """
            INSERT INTO portfolio_upload_runs (
              run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
              raw_row_count, parsed_row_count, normalized_position_count, total_market_value,
              stale_price_count, mapping_issue_count, warning_count, warnings_json, errors_json
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                uploaded_at,
                holdings_as_of_date or uploaded_at[:10],
                filename or source_name,
                source_name,
                "failed",
                raw_count,
                0,
                0,
                0.0,
                0,
                0,
                len(warnings),
                json.dumps(warnings),
                json.dumps(errors),
            ),
        )
        conn.commit()
        return {"created": [], "errors": errors, "warnings": warnings, "run_id": run_id}

    snapshot_id = _create_aggregate_snapshot(
        conn,
        run_id=run_id,
        uploaded_at=uploaded_at,
        holdings_as_of_date=holdings_as_of_date or uploaded_at[:10],
        rows=created_rows,
    )
    total_market_value = round(sum(float(item.get("market_value") or 0.0) for item in created_rows), 2)
    stale_price_count = sum(1 for item in created_rows if int(item.get("price_stale") or 0) == 1)
    mapping_issue_count = sum(
        1 for item in created_rows if str(item.get("mapping_status") or "unmapped") in {"unmapped", "low_confidence"}
    )
    normalized_position_count = len(created_rows)

    conn.execute(
        """
        INSERT INTO portfolio_upload_runs (
          run_id, uploaded_at, holdings_as_of_date, filename, source_name, status, is_active, is_deleted,
          deleted_at, deleted_reason, snapshot_id, raw_row_count, parsed_row_count, normalized_position_count,
          total_market_value, stale_price_count, mapping_issue_count, warning_count, warnings_json, errors_json
        ) VALUES (?, ?, ?, ?, ?, ?, 1, 0, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            uploaded_at,
            holdings_as_of_date or uploaded_at[:10],
            filename or source_name,
            source_name,
            "partial" if errors else "ok",
            snapshot_id,
            raw_count,
            len(created_rows),
            normalized_position_count,
            total_market_value,
            stale_price_count,
            mapping_issue_count,
            len(warnings),
            json.dumps(warnings),
            json.dumps(errors),
        ),
    )
    conn.execute("UPDATE portfolio_upload_runs SET is_active = 0 WHERE run_id <> ?", (run_id,))
    conn.commit()
    _synchronize_active_legacy_state(conn, run_id)
    return {
        "run_id": run_id,
        "holdings_as_of_date": holdings_as_of_date or uploaded_at[:10],
        "snapshot_id": snapshot_id,
        "created": created_rows,
        "errors": errors,
        "warnings": warnings,
    }
