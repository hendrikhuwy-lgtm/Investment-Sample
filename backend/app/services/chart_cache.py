from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTBOX_DIR = PROJECT_ROOT / "outbox"
CHART_DIR = OUTBOX_DIR / "daily_brief_charts"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_chart_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chart_artifacts (
          chart_artifact_id TEXT PRIMARY KEY,
          brief_run_id TEXT NOT NULL,
          chart_key TEXT NOT NULL,
          title TEXT NOT NULL,
          artifact_path TEXT NOT NULL,
          artifact_format TEXT NOT NULL DEFAULT 'svg',
          source_as_of TEXT,
          freshness_note TEXT,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_chart_artifacts_run
        ON chart_artifacts (brief_run_id, chart_key, created_at DESC)
        """
    )
    conn.commit()


def store_chart_artifact(
    conn: sqlite3.Connection,
    *,
    brief_run_id: str,
    chart_key: str,
    title: str,
    svg: str,
    source_as_of: str | None,
    freshness_note: str | None,
) -> dict[str, str]:
    ensure_chart_tables(conn)
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{brief_run_id}_{chart_key}.svg"
    path = CHART_DIR / filename
    path.write_text(svg, encoding="utf-8")
    created_at = _now_iso()
    conn.execute(
        """
        INSERT INTO chart_artifacts (
          chart_artifact_id, brief_run_id, chart_key, title, artifact_path, artifact_format,
          source_as_of, freshness_note, created_at
        ) VALUES (?, ?, ?, ?, ?, 'svg', ?, ?, ?)
        """,
        (
            f"chart_{uuid.uuid4().hex[:12]}",
            brief_run_id,
            chart_key,
            title,
            str(path),
            source_as_of,
            freshness_note,
            created_at,
        ),
    )
    conn.commit()
    return {
        "chart_key": chart_key,
        "title": title,
        "artifact_path": str(path),
        "artifact_format": "svg",
        "source_as_of": source_as_of or "",
        "freshness_note": freshness_note or "",
        "created_at": created_at,
    }


def list_chart_artifacts(conn: sqlite3.Connection, brief_run_id: str) -> list[dict[str, str]]:
    ensure_chart_tables(conn)
    rows = conn.execute(
        """
        SELECT chart_key, title, artifact_path, artifact_format, source_as_of, freshness_note, created_at
        FROM chart_artifacts
        WHERE brief_run_id = ?
        ORDER BY created_at ASC, chart_key ASC
        """,
        (brief_run_id,),
    ).fetchall()
    return [dict(row) for row in rows]
