from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.services.signals import SIGNAL_METHODOLOGY_VERSION, SIGNAL_THRESHOLD_REGISTRY


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_regime_methodology_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_methodology_registry (
          metric_key TEXT PRIMARY KEY,
          watch_threshold REAL,
          alert_threshold REAL,
          methodology_note TEXT NOT NULL,
          threshold_kind TEXT NOT NULL DEFAULT 'observational',
          source_name TEXT,
          source_url TEXT,
          observed_at TEXT,
          provenance_level TEXT NOT NULL DEFAULT 'developer_seed',
          confidence_label TEXT NOT NULL DEFAULT 'low',
          methodology_version TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def seed_regime_methodology_registry(conn: sqlite3.Connection) -> None:
    ensure_regime_methodology_tables(conn)
    existing = {
        str(row["metric_key"])
        for row in conn.execute("SELECT metric_key FROM regime_methodology_registry").fetchall()
    }
    now = _now_iso()
    for metric_key, config in SIGNAL_THRESHOLD_REGISTRY.items():
        if metric_key in existing:
            continue
        conn.execute(
            """
            INSERT INTO regime_methodology_registry (
              metric_key, watch_threshold, alert_threshold, methodology_note, threshold_kind,
              source_name, source_url, observed_at, provenance_level, confidence_label,
              methodology_version, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                metric_key,
                config.get("watch_threshold"),
                config.get("alert_threshold"),
                str(config.get("methodology_note") or "Observational regime threshold."),
                "observational",
                "Daily Brief signal methodology seed",
                "https://policy.example/internal/regime/developer-seed",
                datetime.now(UTC).date().isoformat(),
                "developer_seed",
                "low",
                SIGNAL_METHODOLOGY_VERSION,
                now,
            ),
        )
    conn.commit()


def list_regime_methodology(conn: sqlite3.Connection) -> dict[str, Any]:
    seed_regime_methodology_registry(conn)
    rows = conn.execute(
        """
        SELECT metric_key, watch_threshold, alert_threshold, methodology_note, threshold_kind,
               source_name, source_url, observed_at, provenance_level, confidence_label,
               methodology_version, updated_at
        FROM regime_methodology_registry
        ORDER BY metric_key ASC
        """
    ).fetchall()
    items = [dict(row) for row in rows]
    version = max((str(item.get("methodology_version") or SIGNAL_METHODOLOGY_VERSION) for item in items), default=SIGNAL_METHODOLOGY_VERSION)
    return {"version": version, "thresholds": {str(item["metric_key"]): item for item in items}, "items": items}


def upsert_regime_methodology(
    conn: sqlite3.Connection,
    *,
    metric_key: str,
    watch_threshold: float | None,
    alert_threshold: float | None,
    methodology_note: str,
    threshold_kind: str = "observational",
    source_name: str | None = None,
    source_url: str | None = None,
    observed_at: str | None = None,
    provenance_level: str = "provisional",
    confidence_label: str = "medium",
    methodology_version: str | None = None,
) -> dict[str, Any]:
    seed_regime_methodology_registry(conn)
    updated_at = _now_iso()
    version = methodology_version or SIGNAL_METHODOLOGY_VERSION
    conn.execute(
        """
        INSERT INTO regime_methodology_registry (
          metric_key, watch_threshold, alert_threshold, methodology_note, threshold_kind,
          source_name, source_url, observed_at, provenance_level, confidence_label,
          methodology_version, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(metric_key) DO UPDATE SET
          watch_threshold=excluded.watch_threshold,
          alert_threshold=excluded.alert_threshold,
          methodology_note=excluded.methodology_note,
          threshold_kind=excluded.threshold_kind,
          source_name=excluded.source_name,
          source_url=excluded.source_url,
          observed_at=excluded.observed_at,
          provenance_level=excluded.provenance_level,
          confidence_label=excluded.confidence_label,
          methodology_version=excluded.methodology_version,
          updated_at=excluded.updated_at
        """,
        (
            metric_key,
            watch_threshold,
            alert_threshold,
            methodology_note,
            threshold_kind,
            source_name,
            source_url,
            observed_at,
            provenance_level,
            confidence_label,
            version,
            updated_at,
        ),
    )
    conn.commit()
    payload = list_regime_methodology(conn)["thresholds"][metric_key]
    return dict(payload)
