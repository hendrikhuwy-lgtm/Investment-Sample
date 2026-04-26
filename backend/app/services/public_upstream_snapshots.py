from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


def _now() -> datetime:
    return datetime.now(UTC)


def _iso_now() -> str:
    return _now().isoformat()


def ensure_public_upstream_snapshot_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public_upstream_snapshots (
          snapshot_id TEXT PRIMARY KEY,
          provider_key TEXT NOT NULL,
          family_name TEXT NOT NULL,
          surface_usage_json TEXT NOT NULL DEFAULT '[]',
          payload_json TEXT NOT NULL DEFAULT '{}',
          source_url TEXT,
          observed_at TEXT,
          fetched_at TEXT NOT NULL,
          freshness_state TEXT NOT NULL,
          error_state TEXT,
          snapshot_version TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_public_upstream_snapshots_lookup
        ON public_upstream_snapshots (provider_key, family_name, fetched_at DESC)
        """
    )
    conn.commit()


def put_public_upstream_snapshot(
    conn: sqlite3.Connection,
    *,
    provider_key: str,
    family_name: str,
    surface_usage: list[str],
    payload: dict[str, Any],
    source_url: str | None,
    observed_at: str | None,
    freshness_state: str,
    error_state: str | None = None,
) -> str:
    ensure_public_upstream_snapshot_tables(conn)
    now = _iso_now()
    snapshot_version = f"{provider_key}:{family_name}:{str(observed_at or now).replace(':', '').replace('-', '')[:24]}"
    conn.execute(
        """
        INSERT INTO public_upstream_snapshots (
          snapshot_id, provider_key, family_name, surface_usage_json, payload_json, source_url,
          observed_at, fetched_at, freshness_state, error_state, snapshot_version, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"public_upstream_snapshot_{uuid.uuid4().hex[:12]}",
            provider_key,
            family_name,
            json.dumps(sorted(set(surface_usage)), ensure_ascii=True),
            json.dumps(payload, ensure_ascii=True, sort_keys=True),
            source_url,
            observed_at,
            now,
            freshness_state,
            error_state,
            snapshot_version,
            now,
        ),
    )
    conn.commit()
    return snapshot_version


def latest_public_upstream_snapshots(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_public_upstream_snapshot_tables(conn)
    rows = conn.execute(
        """
        SELECT provider_key, family_name, MAX(fetched_at) AS latest_fetched_at
        FROM public_upstream_snapshots
        GROUP BY provider_key, family_name
        """
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        latest = conn.execute(
            """
            SELECT *
            FROM public_upstream_snapshots
            WHERE provider_key = ? AND family_name = ? AND fetched_at = ?
            LIMIT 1
            """,
            (row["provider_key"], row["family_name"], row["latest_fetched_at"]),
        ).fetchone()
        if latest is None:
            continue
        item = dict(latest)
        try:
            item["surface_usage"] = json.loads(str(item.get("surface_usage_json") or "[]"))
        except Exception:
            item["surface_usage"] = []
        try:
            item["payload"] = json.loads(str(item.get("payload_json") or "{}"))
        except Exception:
            item["payload"] = {}
        items.append(item)
    return items


def public_upstream_health_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    snapshots = latest_public_upstream_snapshots(conn)
    by_provider: dict[str, dict[str, Any]] = {}
    for item in snapshots:
        provider_key = str(item.get("provider_key") or "")
        bucket = by_provider.setdefault(
            provider_key,
            {
                "provider_key": provider_key,
                "last_successful_fetch_at": None,
                "snapshot_count": 0,
                "families": [],
                "latest_observed_at": None,
                "latest_fetched_at": None,
                "freshness_states": set(),
                "error_state": None,
                "snapshot_versions": [],
            },
        )
        bucket["snapshot_count"] += 1
        bucket["families"].append(str(item.get("family_name") or ""))
        bucket["snapshot_versions"].append(str(item.get("snapshot_version") or ""))
        bucket["freshness_states"].add(str(item.get("freshness_state") or "unknown"))
        fetched_at = str(item.get("fetched_at") or "")
        observed_at = str(item.get("observed_at") or "")
        if bucket["latest_fetched_at"] is None or fetched_at > str(bucket["latest_fetched_at"]):
            bucket["latest_fetched_at"] = fetched_at
            bucket["last_successful_fetch_at"] = fetched_at
        if bucket["latest_observed_at"] is None or observed_at > str(bucket["latest_observed_at"]):
            bucket["latest_observed_at"] = observed_at
        if item.get("error_state"):
            bucket["error_state"] = str(item.get("error_state") or "")
    for bucket in by_provider.values():
        bucket["families"] = sorted(set(bucket["families"]))
        bucket["snapshot_versions"] = sorted(set(bucket["snapshot_versions"]))
        bucket["freshness_states"] = sorted(bucket["freshness_states"])
    return {"providers": list(by_provider.values())}
