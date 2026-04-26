from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from statistics import median
from typing import Any

from app.services.data_governance import _parse_dt, error_family
from app.services.provider_budget import ensure_provider_budget_tables


def _now() -> datetime:
    return datetime.now(UTC)


def _iso_now() -> str:
    return _now().isoformat()


def ensure_provider_family_success_tables(conn: sqlite3.Connection) -> None:
    ensure_provider_budget_tables(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_family_events (
          event_id TEXT PRIMARY KEY,
          provider_name TEXT NOT NULL,
          surface_name TEXT NOT NULL,
          family_name TEXT NOT NULL,
          identifier TEXT,
          target_universe_json TEXT NOT NULL DEFAULT '[]',
          success INTEGER NOT NULL DEFAULT 0,
          error_class TEXT,
          cache_hit INTEGER NOT NULL DEFAULT 0,
          freshness_state TEXT,
          fallback_used INTEGER NOT NULL DEFAULT 0,
          age_seconds REAL,
          root_error_class TEXT,
          effective_error_class TEXT,
          suppression_reason TEXT,
          triggered_by_job TEXT,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_provider_family_events_lookup
        ON provider_family_events (provider_name, surface_name, family_name, created_at)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_family_success (
          provider_name TEXT NOT NULL,
          surface_name TEXT NOT NULL,
          family_name TEXT NOT NULL,
          target_universe_json TEXT NOT NULL DEFAULT '[]',
          success_count INTEGER NOT NULL DEFAULT 0,
          failure_count INTEGER NOT NULL DEFAULT 0,
          empty_response_count INTEGER NOT NULL DEFAULT 0,
          endpoint_blocked_count INTEGER NOT NULL DEFAULT 0,
          plan_limited_count INTEGER NOT NULL DEFAULT 0,
          symbol_gap_count INTEGER NOT NULL DEFAULT 0,
          stale_snapshot_count INTEGER NOT NULL DEFAULT 0,
          current_snapshot_count INTEGER NOT NULL DEFAULT 0,
          median_freshness_seconds REAL,
          last_successful_family_refresh TEXT,
          last_failed_family_refresh TEXT,
          reliability_score REAL NOT NULL DEFAULT 0,
          current_tier TEXT NOT NULL DEFAULT 'backup_only',
          current_terminal_state TEXT,
          current_terminal_cause TEXT,
          last_error_class TEXT,
          last_root_error_class TEXT,
          last_effective_error_class TEXT,
          last_suppression_reason TEXT,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (provider_name, surface_name, family_name)
        )
        """
    )
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(provider_family_success)").fetchall()}
    if "endpoint_blocked_count" not in columns:
        conn.execute("ALTER TABLE provider_family_success ADD COLUMN endpoint_blocked_count INTEGER NOT NULL DEFAULT 0")
    if "plan_limited_count" not in columns:
        conn.execute("ALTER TABLE provider_family_success ADD COLUMN plan_limited_count INTEGER NOT NULL DEFAULT 0")
    if "last_root_error_class" not in columns:
        conn.execute("ALTER TABLE provider_family_success ADD COLUMN last_root_error_class TEXT")
    if "last_effective_error_class" not in columns:
        conn.execute("ALTER TABLE provider_family_success ADD COLUMN last_effective_error_class TEXT")
    if "last_suppression_reason" not in columns:
        conn.execute("ALTER TABLE provider_family_success ADD COLUMN last_suppression_reason TEXT")
    if "current_terminal_state" not in columns:
        conn.execute("ALTER TABLE provider_family_success ADD COLUMN current_terminal_state TEXT")
    if "current_terminal_cause" not in columns:
        conn.execute("ALTER TABLE provider_family_success ADD COLUMN current_terminal_cause TEXT")
    event_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(provider_family_events)").fetchall()}
    if "root_error_class" not in event_columns:
        conn.execute("ALTER TABLE provider_family_events ADD COLUMN root_error_class TEXT")
    if "effective_error_class" not in event_columns:
        conn.execute("ALTER TABLE provider_family_events ADD COLUMN effective_error_class TEXT")
    if "suppression_reason" not in event_columns:
        conn.execute("ALTER TABLE provider_family_events ADD COLUMN suppression_reason TEXT")
    conn.commit()


def _target_json(target_universe: list[str] | None) -> str:
    return json.dumps(sorted({str(item).upper() for item in list(target_universe or []) if str(item).strip()}), ensure_ascii=True)


def record_provider_family_event(
    conn: sqlite3.Connection,
    *,
    provider_name: str,
    surface_name: str,
    family_name: str,
    identifier: str,
    target_universe: list[str] | None,
    success: bool,
    error_class: str | None,
    cache_hit: bool,
    freshness_state: str | None,
    fallback_used: bool,
    age_seconds: float | None,
    root_error_class: str | None = None,
    effective_error_class: str | None = None,
    suppression_reason: str | None = None,
    triggered_by_job: str | None,
) -> None:
    ensure_provider_family_success_tables(conn)
    conn.execute(
        """
        INSERT INTO provider_family_events (
          event_id, provider_name, surface_name, family_name, identifier, target_universe_json,
          success, error_class, cache_hit, freshness_state, fallback_used, age_seconds,
          root_error_class, effective_error_class, suppression_reason, triggered_by_job, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"provider_family_event_{uuid.uuid4().hex[:12]}",
            provider_name,
            surface_name,
            family_name,
            identifier,
            _target_json(target_universe),
            1 if success else 0,
            error_class,
            1 if cache_hit else 0,
            freshness_state,
            1 if fallback_used else 0,
            age_seconds,
            root_error_class,
            effective_error_class,
            suppression_reason,
            triggered_by_job,
            _iso_now(),
        ),
    )
    conn.commit()


def _tier_for_summary(summary: dict[str, Any]) -> str:
    reliability = float(summary.get("reliability_score") or 0.0)
    failure_count = int(summary.get("failure_count") or 0)
    current_count = int(summary.get("current_snapshot_count") or 0)
    empty_count = int(summary.get("empty_response_count") or 0)
    endpoint_blocked_count = int(summary.get("endpoint_blocked_count") or 0)
    plan_limited_count = int(summary.get("plan_limited_count") or 0)
    symbol_gap_count = int(summary.get("symbol_gap_count") or 0)
    # Recovery checks first — real snapshots override past failure history
    if reliability >= 0.75 and current_count >= 2:
        return "primary_active"
    if reliability >= 0.55 and current_count >= 1:
        return "secondary_active"
    # Disabled only when there is genuinely nothing usable
    if endpoint_blocked_count >= max(3, current_count + 1) or plan_limited_count >= max(3, current_count + 1):
        return "disabled_for_family"
    if failure_count >= 4 and current_count == 0:
        return "disabled_for_family"
    if empty_count >= max(4, current_count + 2):
        return "disabled_for_family"
    if symbol_gap_count >= max(3, current_count + 1):
        return "backup_only"
    return "backup_only"


def _terminal_result(
    *,
    success: bool,
    freshness_state: str | None,
    root_error_class: str | None,
    effective_error_class: str | None,
    suppression_reason: str | None,
    error_class: str | None = None,
) -> tuple[str, str | None]:
    freshness = str(freshness_state or "").strip()
    cause = str(root_error_class or effective_error_class or error_class or "").strip() or None
    suppression = str(suppression_reason or "").strip().lower()
    if success:
        if freshness in {"stale"}:
            return "stale_context_only", "stale_only"
        return "current_success", None
    if "quarant" in suppression:
        if cause == "budget_block":
            return "quarantined", "all_routes_quarantined"
        return "quarantined", cause or "manual_quarantine"
    if cause == "budget_block":
        if "stale" in suppression:
            return "stale_context_only", "stale_only"
        if "route" in suppression or "eligible" in suppression:
            return "current_failure", "no_eligible_route"
        if "manual" in suppression:
            return "current_failure", "manual_quarantine"
    return "current_failure", cause or "unknown_failure"


def recompute_provider_family_success(
    conn: sqlite3.Connection,
    *,
    surface_name: str,
    family_name: str,
) -> None:
    ensure_provider_family_success_tables(conn)
    rows = conn.execute(
        """
        SELECT provider_name, success, error_class, freshness_state, age_seconds, target_universe_json, created_at
               , root_error_class, effective_error_class, suppression_reason
        FROM provider_family_events
        WHERE surface_name = ? AND family_name = ?
        ORDER BY created_at DESC
        LIMIT 500
        """,
        (surface_name, family_name),
    ).fetchall()
    by_provider: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        provider_name = str(item.get("provider_name") or "")
        bucket = by_provider.setdefault(
            provider_name,
            {
                "provider_name": provider_name,
                "surface_name": surface_name,
                "family_name": family_name,
                "target_universe_json": item.get("target_universe_json") or "[]",
                "success_count": 0,
                "failure_count": 0,
                "empty_response_count": 0,
                "endpoint_blocked_count": 0,
                "plan_limited_count": 0,
                "symbol_gap_count": 0,
                "stale_snapshot_count": 0,
                "current_snapshot_count": 0,
                "ages": [],
                "last_successful_family_refresh": None,
                "last_failed_family_refresh": None,
                "last_error_class": None,
                "last_root_error_class": None,
                "last_effective_error_class": None,
                "last_suppression_reason": None,
                "current_terminal_state": None,
                "current_terminal_cause": None,
            },
        )
        created_at = str(item.get("created_at") or "")
        freshness_state = str(item.get("freshness_state") or "")
        terminal_state, terminal_cause = _terminal_result(
            success=bool(item.get("success")),
            freshness_state=freshness_state,
            root_error_class=str(item.get("root_error_class") or "") or None,
            effective_error_class=str(item.get("effective_error_class") or "") or None,
            suppression_reason=str(item.get("suppression_reason") or "") or None,
            error_class=str(item.get("error_class") or "") or None,
        )
        if bucket["current_terminal_state"] is None:
            bucket["current_terminal_state"] = terminal_state
            bucket["current_terminal_cause"] = terminal_cause
        if item.get("success"):
            bucket["success_count"] += 1
            if freshness_state in {"current", "expected_lag"}:
                bucket["current_snapshot_count"] += 1
            elif freshness_state == "stale":
                bucket["stale_snapshot_count"] += 1
            if bucket["last_successful_family_refresh"] is None:
                bucket["last_successful_family_refresh"] = created_at
        else:
            effective_error = str(item.get("effective_error_class") or item.get("error_class") or "")
            root_error = str(item.get("root_error_class") or item.get("error_class") or "")
            err = error_family(root_error or effective_error)
            if effective_error == "budget_block":
                if bucket["last_effective_error_class"] is None:
                    bucket["last_effective_error_class"] = effective_error
                if bucket["last_root_error_class"] is None:
                    bucket["last_root_error_class"] = root_error or None
                if bucket["last_suppression_reason"] is None:
                    bucket["last_suppression_reason"] = str(item.get("suppression_reason") or "") or None
                if bucket["last_error_class"] is None:
                    bucket["last_error_class"] = root_error or effective_error or None
                if bucket["last_failed_family_refresh"] is None:
                    bucket["last_failed_family_refresh"] = created_at
                continue
            if err == "rate_limited":
                bucket["rate_limited_count"] = bucket.get("rate_limited_count", 0) + 1
                # do NOT increment failure_count — transient quota, not broken endpoint
            else:
                bucket["failure_count"] += 1
                if err == "empty_response":
                    bucket["empty_response_count"] += 1
                elif err == "endpoint_blocked":
                    bucket["endpoint_blocked_count"] += 1
                elif err == "plan_limited":
                    bucket["plan_limited_count"] += 1
                elif err in {"symbol_gap", "missing_source_gap"}:
                    bucket["symbol_gap_count"] += 1
            if bucket["last_failed_family_refresh"] is None:
                bucket["last_failed_family_refresh"] = created_at
            if bucket["last_error_class"] is None:
                bucket["last_error_class"] = root_error or effective_error or str(item.get("error_class") or "")
            if bucket["last_root_error_class"] is None:
                bucket["last_root_error_class"] = root_error or None
            if bucket["last_effective_error_class"] is None:
                bucket["last_effective_error_class"] = effective_error or None
            if bucket["last_suppression_reason"] is None:
                bucket["last_suppression_reason"] = str(item.get("suppression_reason") or "") or None
        age_seconds = item.get("age_seconds")
        if age_seconds is not None:
            try:
                bucket["ages"].append(float(age_seconds))
            except Exception:
                pass

    # Also fold in current snapshot freshness directly so the registry reflects actual cached family state.
    snapshot_rows = conn.execute(
        """
        SELECT provider_name, payload_json, fetched_at, freshness_state, error_state
        FROM provider_cache_snapshots
        WHERE surface_name = ? AND endpoint_family = ?
        """,
        (surface_name, family_name),
    ).fetchall()
    for row in snapshot_rows:
        item = dict(row)
        provider_name = str(item.get("provider_name") or "")
        bucket = by_provider.setdefault(
            provider_name,
            {
                "provider_name": provider_name,
                "surface_name": surface_name,
                "family_name": family_name,
                "target_universe_json": "[]",
                "success_count": 0,
                "failure_count": 0,
                "empty_response_count": 0,
                "endpoint_blocked_count": 0,
                "plan_limited_count": 0,
                "symbol_gap_count": 0,
                "stale_snapshot_count": 0,
                "current_snapshot_count": 0,
                "ages": [],
                "last_successful_family_refresh": None,
                "last_failed_family_refresh": None,
                "last_error_class": None,
                "last_root_error_class": None,
                "last_effective_error_class": None,
                "last_suppression_reason": None,
                "current_terminal_state": None,
                "current_terminal_cause": None,
            },
        )
        try:
            payload = json.loads(str(item.get("payload_json") or "{}"))
        except Exception:
            payload = {}
        governance = dict(payload.get("governance") or {})
        freshness_state = str(governance.get("operational_freshness_state") or item.get("freshness_state") or "")
        if freshness_state in {"current", "expected_lag"}:
            bucket["current_snapshot_count"] += 1
        elif freshness_state == "stale":
            bucket["stale_snapshot_count"] += 1
        observed_at = _parse_dt(payload.get("observed_at")) or _parse_dt(item.get("fetched_at"))
        if observed_at is not None:
            bucket["ages"].append(max(0.0, (_now() - observed_at).total_seconds()))
        if item.get("error_state") and bucket["last_error_class"] is None:
            bucket["last_error_class"] = str(item.get("error_state") or "")

    # Replace the aggregate rows for this surface/family.
    conn.execute(
        "DELETE FROM provider_family_success WHERE surface_name = ? AND family_name = ?",
        (surface_name, family_name),
    )
    for bucket in by_provider.values():
        success_count = int(bucket["success_count"])
        failure_count = int(bucket["failure_count"])
        current_count = int(bucket["current_snapshot_count"])
        stale_count = int(bucket["stale_snapshot_count"])
        empty_count = int(bucket["empty_response_count"])
        endpoint_blocked_count = int(bucket["endpoint_blocked_count"])
        plan_limited_count = int(bucket["plan_limited_count"])
        symbol_gap_count = int(bucket["symbol_gap_count"])
        rate_limited_count = int(bucket.get("rate_limited_count", 0))
        total = success_count + failure_count
        base_score = (success_count / total) if total else 0.0
        freshness_bonus = 0.15 if current_count >= max(1, stale_count) else 0.0
        stale_penalty = min(0.25, stale_count * 0.03)
        empty_penalty = min(0.25, empty_count * 0.04)
        blocked_penalty = min(0.20, endpoint_blocked_count * 0.05)
        plan_penalty = min(0.20, plan_limited_count * 0.05)
        gap_penalty = min(0.20, symbol_gap_count * 0.04)
        rate_limited_penalty = min(0.10, rate_limited_count * 0.02)
        reliability = max(0.0, min(1.0, base_score + freshness_bonus - stale_penalty - empty_penalty - blocked_penalty - plan_penalty - gap_penalty - rate_limited_penalty))
        summary = {
            "success_count": success_count,
            "failure_count": failure_count,
            "current_snapshot_count": current_count,
            "stale_snapshot_count": stale_count,
            "empty_response_count": empty_count,
            "endpoint_blocked_count": endpoint_blocked_count,
            "plan_limited_count": plan_limited_count,
            "symbol_gap_count": symbol_gap_count,
            "reliability_score": reliability,
        }
        tier = _tier_for_summary(summary)
        median_freshness = median(bucket["ages"]) if bucket["ages"] else None
        conn.execute(
            """
            INSERT INTO provider_family_success (
              provider_name, surface_name, family_name, target_universe_json, success_count, failure_count,
              empty_response_count, endpoint_blocked_count, plan_limited_count, symbol_gap_count, stale_snapshot_count, current_snapshot_count,
              median_freshness_seconds, last_successful_family_refresh, last_failed_family_refresh,
              reliability_score, current_tier, current_terminal_state, current_terminal_cause,
              last_error_class, last_root_error_class, last_effective_error_class, last_suppression_reason, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bucket["provider_name"],
                surface_name,
                family_name,
                bucket["target_universe_json"],
                success_count,
                failure_count,
                empty_count,
                endpoint_blocked_count,
                plan_limited_count,
                symbol_gap_count,
                stale_count,
                current_count,
                median_freshness,
                bucket["last_successful_family_refresh"],
                bucket["last_failed_family_refresh"],
                reliability,
                tier,
                bucket["current_terminal_state"],
                bucket["current_terminal_cause"],
                bucket["last_error_class"],
                bucket["last_root_error_class"],
                bucket["last_effective_error_class"],
                bucket["last_suppression_reason"],
                _iso_now(),
            ),
        )
    conn.commit()


def list_provider_family_success(
    conn: sqlite3.Connection,
    *,
    surface_name: str | None = None,
    family_name: str | None = None,
) -> list[dict[str, Any]]:
    ensure_provider_family_success_tables(conn)
    clauses = []
    params: list[Any] = []
    if surface_name is not None:
        clauses.append("surface_name = ?")
        params.append(surface_name)
    if family_name is not None:
        clauses.append("family_name = ?")
        params.append(family_name)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        f"""
        SELECT provider_name, surface_name, family_name, target_universe_json, success_count, failure_count,
               empty_response_count, endpoint_blocked_count, plan_limited_count, symbol_gap_count, stale_snapshot_count, current_snapshot_count,
               median_freshness_seconds, last_successful_family_refresh, last_failed_family_refresh,
               reliability_score, current_tier, current_terminal_state, current_terminal_cause,
               last_error_class, last_root_error_class, last_effective_error_class, last_suppression_reason, updated_at
        FROM provider_family_success
        {where}
        ORDER BY surface_name, family_name, reliability_score DESC, current_snapshot_count DESC, provider_name
        """,
        params,
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        try:
            item["target_universe"] = json.loads(str(item.get("target_universe_json") or "[]"))
        except Exception:
            item["target_universe"] = []
        item["routing_status"] = item.get("current_tier")
        items.append(item)
    return items


def compare_family_providers(
    conn: sqlite3.Connection,
    *,
    surface_name: str,
    family_name: str,
) -> dict[str, Any]:
    rows = list_provider_family_success(conn, surface_name=surface_name, family_name=family_name)
    ranked = sorted(
        rows,
        key=lambda item: (
            float(item.get("reliability_score") or 0.0),
            int(item.get("current_snapshot_count") or 0),
            -float(item.get("median_freshness_seconds") or 10**12),
        ),
        reverse=True,
    )
    return {
        "surface_name": surface_name,
        "family_name": family_name,
        "best_current_provider": ranked[0] if ranked else None,
        "second_best_provider": ranked[1] if len(ranked) > 1 else None,
        "weakest_current_provider": ranked[-1] if ranked else None,
        "recommended_routing_change": (
            "promote_second_provider"
            if len(ranked) >= 2 and str((ranked[0] or {}).get("current_tier")) != "primary_active"
            else "demote_weak_provider"
            if ranked and str((ranked[-1] or {}).get("current_tier")) == "disabled_for_family"
            else "keep_current_routing"
        ),
        "rows": ranked,
    }
