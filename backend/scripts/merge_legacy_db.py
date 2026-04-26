#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.config import get_db_path, get_legacy_db_path  # noqa: E402


MERGE_TIMESTAMP_COLUMNS = (
    "finished_at",
    "retrieved_at",
    "attempted_at",
    "updated_at",
    "created_at",
    "asof_ts",
)

VERIFY_TABLES = (
    "daily_logs",
    "email_runs",
    "mcp_connectivity_runs",
    "mcp_items",
    "outbox_artifacts",
)


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _table_names(conn: sqlite3.Connection, schema: str = "main") -> list[str]:
    rows = conn.execute(
        f"""
        SELECT name
        FROM {schema}.sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def _table_columns(conn: sqlite3.Connection, table: str, schema: str = "main") -> list[dict[str, Any]]:
    rows = conn.execute(f"PRAGMA {schema}.table_info({_quote_ident(table)})").fetchall()
    return [{"name": str(row[1]), "pk_order": int(row[5] or 0)} for row in rows]


def _table_count(conn: sqlite3.Connection, table: str, schema: str = "main") -> int:
    if table not in _table_names(conn, schema=schema):
        return 0
    row = conn.execute(f"SELECT COUNT(*) FROM {schema}.{_quote_ident(table)}").fetchone()
    return int(row[0] if row is not None else 0)


def _verification_counts(conn: sqlite3.Connection, schema: str = "main") -> dict[str, int]:
    return {table: _table_count(conn, table, schema=schema) for table in VERIFY_TABLES}


def _best_timestamp_column(columns: list[str]) -> str | None:
    for candidate in MERGE_TIMESTAMP_COLUMNS:
        if candidate in columns:
            return candidate
    return None


def _normalize_ts(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat()
    except Exception:  # noqa: BLE001
        return text


def _legacy_is_newer(current_row: sqlite3.Row, legacy_row: sqlite3.Row, ts_col: str | None) -> bool:
    if ts_col is None:
        return False
    current_ts = _normalize_ts(current_row[ts_col])
    legacy_ts = _normalize_ts(legacy_row[ts_col])
    if not legacy_ts:
        return False
    if not current_ts:
        return True
    return legacy_ts >= current_ts


def _insert_row(conn: sqlite3.Connection, table: str, columns: list[str], row: sqlite3.Row, schema: str = "main") -> None:
    cols_sql = ", ".join(_quote_ident(col) for col in columns)
    placeholders = ", ".join(["?"] * len(columns))
    values = [row[col] for col in columns]
    conn.execute(
        f"INSERT INTO {schema}.{_quote_ident(table)} ({cols_sql}) VALUES ({placeholders})",
        values,
    )


def _update_row(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    pk_cols: list[str],
    row: sqlite3.Row,
    schema: str = "main",
) -> None:
    update_cols = [col for col in columns if col not in pk_cols]
    if not update_cols:
        return
    set_sql = ", ".join(f"{_quote_ident(col)} = ?" for col in update_cols)
    where_sql = " AND ".join(f"{_quote_ident(col)} = ?" for col in pk_cols)
    values = [row[col] for col in update_cols] + [row[col] for col in pk_cols]
    conn.execute(
        f"UPDATE {schema}.{_quote_ident(table)} SET {set_sql} WHERE {where_sql}",
        values,
    )


def _merge_table_by_pk(
    conn: sqlite3.Connection,
    table: str,
    common_cols: list[str],
    pk_cols: list[str],
) -> dict[str, int]:
    where_sql = " AND ".join(f"{_quote_ident(col)} = ?" for col in pk_cols)
    select_existing_sql = (
        f"SELECT {', '.join(_quote_ident(col) for col in common_cols)} "
        f"FROM main.{_quote_ident(table)} WHERE {where_sql} LIMIT 1"
    )
    ts_col = _best_timestamp_column(common_cols)

    inserted = 0
    updated = 0
    skipped = 0
    legacy_rows = conn.execute(
        f"SELECT {', '.join(_quote_ident(col) for col in common_cols)} FROM legacy.{_quote_ident(table)}"
    ).fetchall()

    for legacy_row in legacy_rows:
        pk_values = [legacy_row[col] for col in pk_cols]
        current = conn.execute(select_existing_sql, pk_values).fetchone()
        if current is None:
            _insert_row(conn, table, common_cols, legacy_row, schema="main")
            inserted += 1
            continue
        if _legacy_is_newer(current, legacy_row, ts_col):
            _update_row(conn, table, common_cols, pk_cols, legacy_row, schema="main")
            updated += 1
        else:
            skipped += 1

    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def _merge_table_without_pk(conn: sqlite3.Connection, table: str, common_cols: list[str]) -> dict[str, int]:
    ts_col = _best_timestamp_column(common_cols)
    inserted = 0
    replaced = 0
    skipped = 0

    if ts_col is None:
        legacy_rows = conn.execute(
            f"SELECT {', '.join(_quote_ident(col) for col in common_cols)} FROM legacy.{_quote_ident(table)}"
        ).fetchall()
        for legacy_row in legacy_rows:
            _insert_row(conn, table, common_cols, legacy_row, schema="main")
            inserted += 1
        return {"inserted": inserted, "updated": replaced, "skipped": skipped}

    key_cols = [col for col in common_cols if col != ts_col]
    if not key_cols:
        legacy_rows = conn.execute(
            f"SELECT {', '.join(_quote_ident(col) for col in common_cols)} FROM legacy.{_quote_ident(table)}"
        ).fetchall()
        for legacy_row in legacy_rows:
            _insert_row(conn, table, common_cols, legacy_row, schema="main")
            inserted += 1
        return {"inserted": inserted, "updated": replaced, "skipped": skipped}

    target_rows = conn.execute(
        f"""
        SELECT rowid, {', '.join(_quote_ident(col) for col in common_cols)}
        FROM main.{_quote_ident(table)}
        """
    ).fetchall()

    target_index: dict[tuple[Any, ...], tuple[int, str]] = {}
    for row in target_rows:
        key = tuple(row[col] for col in key_cols)
        rowid = int(row["rowid"])
        target_index[key] = (rowid, _normalize_ts(row[ts_col]))

    legacy_rows = conn.execute(
        f"SELECT {', '.join(_quote_ident(col) for col in common_cols)} FROM legacy.{_quote_ident(table)}"
    ).fetchall()

    for legacy_row in legacy_rows:
        key = tuple(legacy_row[col] for col in key_cols)
        legacy_ts = _normalize_ts(legacy_row[ts_col])
        existing = target_index.get(key)
        if existing is None:
            _insert_row(conn, table, common_cols, legacy_row, schema="main")
            inserted += 1
            continue
        rowid, current_ts = existing
        if legacy_ts and (not current_ts or legacy_ts >= current_ts):
            conn.execute(f"DELETE FROM main.{_quote_ident(table)} WHERE rowid = ?", (rowid,))
            _insert_row(conn, table, common_cols, legacy_row, schema="main")
            replaced += 1
        else:
            skipped += 1

    return {"inserted": inserted, "updated": replaced, "skipped": skipped}


def _merge_shared_tables(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    main_tables = set(_table_names(conn, schema="main"))
    legacy_tables = set(_table_names(conn, schema="legacy"))
    shared_tables = sorted(main_tables & legacy_tables)

    merged: dict[str, dict[str, int]] = {}
    for table in shared_tables:
        source_cols = _table_columns(conn, table, schema="legacy")
        target_cols = _table_columns(conn, table, schema="main")
        source_names = [item["name"] for item in source_cols]
        target_name_set = {item["name"] for item in target_cols}
        common_cols = [name for name in source_names if name in target_name_set]
        if not common_cols:
            merged[table] = {"inserted": 0, "updated": 0, "skipped": 0}
            continue

        pk_cols = [
            item["name"]
            for item in sorted(target_cols, key=lambda value: value["pk_order"])
            if item["pk_order"] > 0 and item["name"] in common_cols
        ]
        if pk_cols:
            merged[table] = _merge_table_by_pk(conn, table, common_cols, pk_cols)
        else:
            merged[table] = _merge_table_without_pk(conn, table, common_cols)
    return merged


def _bak_path(source_path: Path) -> Path:
    base = source_path.with_suffix(source_path.suffix + ".bak")
    if not base.exists():
        return base
    stamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    return source_path.with_suffix(source_path.suffix + f".bak.{stamp}")


def merge_legacy_db(
    legacy_path: Path | None = None,
    canonical_path: Path | None = None,
    rename_legacy: bool = True,
) -> dict[str, Any]:
    source = (legacy_path or get_legacy_db_path()).resolve()
    target = (canonical_path or get_db_path()).resolve()

    if not source.exists():
        return {
            "status": "skipped",
            "reason": "legacy_missing",
            "legacy_path": str(source),
            "canonical_path": str(target),
        }
    if source == target:
        return {
            "status": "skipped",
            "reason": "same_path",
            "legacy_path": str(source),
            "canonical_path": str(target),
        }

    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        shutil.copy2(source, target)
        backup_path = None
        if rename_legacy:
            backup = _bak_path(source)
            source.rename(backup)
            backup_path = str(backup)
        conn_verify = sqlite3.connect(str(target))
        try:
            after_counts = _verification_counts(conn_verify, schema="main")
        finally:
            conn_verify.close()
        return {
            "status": "copied",
            "legacy_path": str(source),
            "canonical_path": str(target),
            "legacy_backup_path": backup_path,
            "counts_before": {name: 0 for name in VERIFY_TABLES},
            "counts_after": after_counts,
            "merged_tables": {},
        }

    conn = sqlite3.connect(f"file:{target}?mode=rwc", uri=True)
    conn.row_factory = sqlite3.Row
    backup_path: str | None = None
    try:
        counts_before = _verification_counts(conn, schema="main")
        conn.execute("ATTACH DATABASE ? AS legacy", (f"file:{source}?mode=ro",))
        merged_tables = _merge_shared_tables(conn)
        conn.commit()
        conn.execute("DETACH DATABASE legacy")
        counts_after = _verification_counts(conn, schema="main")
    finally:
        conn.close()

    if rename_legacy:
        backup = _bak_path(source)
        source.rename(backup)
        backup_path = str(backup)

    return {
        "status": "merged",
        "legacy_path": str(source),
        "canonical_path": str(target),
        "legacy_backup_path": backup_path,
        "counts_before": counts_before,
        "counts_after": counts_after,
        "merged_tables": merged_tables,
    }


def _main() -> int:
    parser = argparse.ArgumentParser(description="Merge legacy backend SQLite DB into canonical runtime DB.")
    parser.add_argument(
        "--legacy",
        type=Path,
        default=None,
        help="Legacy DB path (default backend/storage/investment_agent.sqlite3).",
    )
    parser.add_argument(
        "--canonical",
        type=Path,
        default=None,
        help="Canonical DB path (default IA_DB_PATH or storage/investment_agent.sqlite3).",
    )
    parser.add_argument(
        "--keep-legacy",
        action="store_true",
        help="Do not rename legacy DB to .bak after merge.",
    )
    args = parser.parse_args()

    result = merge_legacy_db(
        legacy_path=args.legacy,
        canonical_path=args.canonical,
        rename_legacy=not args.keep_legacy,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())

