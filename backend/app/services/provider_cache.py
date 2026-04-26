from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.services.provider_budget import ensure_provider_budget_tables

_CHINA_TZ = ZoneInfo("Asia/Shanghai")


def _now() -> datetime:
    return datetime.now(UTC)


def _next_daily_brief_slot_boundary(*, now: datetime) -> datetime:
    local = now.astimezone(_CHINA_TZ)
    if local.hour < 8:
        slot_local = local.replace(hour=8, minute=0, second=0, microsecond=0)
    elif local.hour < 20:
        slot_local = local.replace(hour=20, minute=0, second=0, microsecond=0)
    else:
        next_day = local + timedelta(days=1)
        slot_local = next_day.replace(hour=8, minute=0, second=0, microsecond=0)
    return slot_local.astimezone(UTC)


def _slot_aligned_expiry(
    *,
    surface_name: str | None,
    endpoint_family: str,
    now: datetime,
) -> datetime | None:
    if str(surface_name or "") == "daily_brief" and str(endpoint_family or "") == "market_close":
        return _next_daily_brief_slot_boundary(now=now)
    return None


def get_cached_provider_snapshot(
    conn: sqlite3.Connection,
    *,
    provider_name: str,
    endpoint_family: str,
    cache_key: str,
    surface_name: str | None = None,
    max_age_seconds: int | None = None,
    allow_expired: bool = False,
) -> dict[str, Any] | None:
    ensure_provider_budget_tables(conn)
    row = conn.execute(
        """
        SELECT *
        FROM provider_cache_snapshots
        WHERE provider_name = ? AND endpoint_family = ? AND cache_key = ? AND COALESCE(surface_name, '') = COALESCE(?, '')
        LIMIT 1
        """,
        (provider_name, endpoint_family, cache_key, surface_name),
    ).fetchone()
    if row is None:
        return None
    item = dict(row)
    if not allow_expired:
        expires_at = str(item.get("expires_at") or "").strip()
        if expires_at:
            try:
                expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                if expires_dt.tzinfo is None:
                    expires_dt = expires_dt.replace(tzinfo=UTC)
                else:
                    expires_dt = expires_dt.astimezone(UTC)
                if expires_dt <= _now():
                    return None
            except Exception:
                return None
    fetched_at = item.get("fetched_at")
    if max_age_seconds is not None and fetched_at:
        try:
            age = (_now() - datetime.fromisoformat(str(fetched_at))).total_seconds()
            if age > max_age_seconds:
                return None
        except Exception:
            return None
    try:
        item["payload"] = json.loads(str(item.get("payload_json") or "{}"))
    except Exception:
        item["payload"] = {}
    return item


def put_provider_snapshot(
    conn: sqlite3.Connection,
    *,
    provider_name: str,
    endpoint_family: str,
    cache_key: str,
    payload: dict[str, Any],
    surface_name: str | None,
    freshness_state: str,
    confidence_tier: str,
    source_ref: str | None,
    ttl_seconds: int,
    cache_status: str = "miss",
    fallback_used: bool = False,
    error_state: str | None = None,
) -> None:
    ensure_provider_budget_tables(conn)
    now = _now()
    slot_expiry = _slot_aligned_expiry(
        surface_name=surface_name,
        endpoint_family=endpoint_family,
        now=now,
    )
    expires_dt = slot_expiry or (now + timedelta(seconds=max(60, int(ttl_seconds))))
    expires_at = expires_dt.isoformat()
    conn.execute(
        """
        INSERT INTO provider_cache_snapshots (
          snapshot_id, provider_name, endpoint_family, cache_key, surface_name, payload_json,
          fetched_at, expires_at, freshness_state, confidence_tier, source_ref, cache_status,
          fallback_used, error_state
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider_name, endpoint_family, cache_key, surface_name)
        DO UPDATE SET
          payload_json = excluded.payload_json,
          fetched_at = excluded.fetched_at,
          expires_at = excluded.expires_at,
          freshness_state = excluded.freshness_state,
          confidence_tier = excluded.confidence_tier,
          source_ref = excluded.source_ref,
          cache_status = excluded.cache_status,
          fallback_used = excluded.fallback_used,
          error_state = excluded.error_state
        """,
        (
            f"provider_snapshot_{uuid.uuid4().hex[:12]}",
            provider_name,
            endpoint_family,
            cache_key,
            surface_name,
            json.dumps(payload, ensure_ascii=True, sort_keys=True),
            now.isoformat(),
            expires_at,
            freshness_state,
            confidence_tier,
            source_ref,
            cache_status,
            1 if fallback_used else 0,
            error_state,
        ),
    )
    conn.commit()


def list_provider_snapshots(
    conn: sqlite3.Connection,
    *,
    surface_name: str | None = None,
    endpoint_family: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    ensure_provider_budget_tables(conn)
    clauses = []
    params: list[Any] = []
    if surface_name is not None:
        clauses.append("COALESCE(surface_name, '') = COALESCE(?, '')")
        params.append(surface_name)
    if endpoint_family is not None:
        clauses.append("endpoint_family = ?")
        params.append(endpoint_family)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        f"""
        SELECT *
        FROM provider_cache_snapshots
        {where}
        ORDER BY fetched_at DESC
        LIMIT ?
        """,
        (*params, int(limit)),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        try:
            item["payload"] = json.loads(str(item.get("payload_json") or "{}"))
        except Exception:
            item["payload"] = {}
        items.append(item)
    return items
