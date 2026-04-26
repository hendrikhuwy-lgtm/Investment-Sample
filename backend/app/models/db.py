from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

from app.models.migrations import apply_schema_migrations

def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout = 15000")
    return conn


def init_db(conn: sqlite3.Connection, schema_path: Path) -> dict[str, Any]:
    pre_report = apply_schema_migrations(conn)
    sql = schema_path.read_text(encoding="utf-8")
    try:
        conn.executescript(sql)
    except sqlite3.OperationalError as exc:
        # Existing databases may be behind current schema columns/indexes.
        message = str(exc).lower()
        if "no such column" not in message:
            raise
        apply_schema_migrations(conn)
        conn.executescript(sql)
    post_report = apply_schema_migrations(conn)
    conn.commit()
    return {"pre": pre_report, "post": post_report}


def execute_many(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
    conn.executemany(sql, rows)
    conn.commit()
