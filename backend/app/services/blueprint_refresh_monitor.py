from __future__ import annotations

import json
import sqlite3
import uuid
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

from app.config import Settings
from app.services.blueprint_candidate_registry import (
    ensure_candidate_registry_tables,
    list_active_candidate_registry,
    refresh_registry_candidate_truth,
    seed_default_candidate_registry,
)
from app.services.ingest_etf_data import refresh_etf_data


_REFRESH_SCOPE_STATE_LOCK = threading.Lock()
_REFRESH_SCOPE_GUARDS: dict[str, threading.Lock] = {}
_REFRESH_SCOPE_STATE: dict[str, dict[str, Any]] = {}

BLUEPRINT_WRITE_REFRESH_SCOPE = "blueprint_write_refresh"
_STALE_RUNNING_REFRESH_MINUTES = 20
_NO_SCOPE_STALE_RUNNING_GRACE_MINUTES = 1


def ensure_blueprint_refresh_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_refresh_runs (
          run_id TEXT PRIMARY KEY,
          trigger_source TEXT NOT NULL,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          status TEXT NOT NULL,
          candidate_count INTEGER NOT NULL DEFAULT 0,
          success_count INTEGER NOT NULL DEFAULT 0,
          failure_count INTEGER NOT NULL DEFAULT 0,
          refreshed_symbols_json TEXT NOT NULL DEFAULT '[]',
          failed_symbols_json TEXT NOT NULL DEFAULT '[]',
          details_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_refresh_runs_started
        ON blueprint_refresh_runs (started_at DESC)
        """
    )
    conn.commit()
    reconcile_stale_blueprint_refresh_runs(conn)


def claim_refresh_scope(scope: str, *, owner: str) -> dict[str, Any]:
    normalized_scope = str(scope or "").strip() or "refresh_scope"
    normalized_owner = str(owner or "").strip() or "unknown_owner"
    with _REFRESH_SCOPE_STATE_LOCK:
        guard = _REFRESH_SCOPE_GUARDS.setdefault(normalized_scope, threading.Lock())
        existing = dict(_REFRESH_SCOPE_STATE.get(normalized_scope) or {})
    acquired = guard.acquire(blocking=False)
    if not acquired:
        return {
            "acquired": False,
            "status": "already_running",
            "scope": normalized_scope,
            "active_refresh": existing or None,
        }
    claimed_at = datetime.now(UTC).isoformat()
    state = {
        "scope": normalized_scope,
        "owner": normalized_owner,
        "claimed_at": claimed_at,
        "last_updated_at": claimed_at,
        "status": "running",
    }
    with _REFRESH_SCOPE_STATE_LOCK:
        _REFRESH_SCOPE_STATE[normalized_scope] = state
    return {
        "acquired": True,
        "status": "running",
        "scope": normalized_scope,
        "active_refresh": dict(state),
    }


def refresh_scope_snapshot(scope: str) -> dict[str, Any] | None:
    normalized_scope = str(scope or "").strip() or "refresh_scope"
    with _REFRESH_SCOPE_STATE_LOCK:
        state = _REFRESH_SCOPE_STATE.get(normalized_scope)
        return dict(state) if state else None


def release_refresh_scope(scope: str, *, owner: str, result_status: str | None = None) -> None:
    normalized_scope = str(scope or "").strip() or "refresh_scope"
    normalized_owner = str(owner or "").strip() or "unknown_owner"
    with _REFRESH_SCOPE_STATE_LOCK:
        guard = _REFRESH_SCOPE_GUARDS.setdefault(normalized_scope, threading.Lock())
        state = dict(_REFRESH_SCOPE_STATE.get(normalized_scope) or {})
        state.update(
            {
                "owner": normalized_owner,
                "status": str(result_status or "finished"),
                "finished_at": datetime.now(UTC).isoformat(),
                "last_updated_at": datetime.now(UTC).isoformat(),
            }
        )
        _REFRESH_SCOPE_STATE[normalized_scope] = state
        _REFRESH_SCOPE_STATE.pop(normalized_scope, None)
    guard.release()


def reconcile_stale_blueprint_refresh_runs(conn: sqlite3.Connection) -> dict[str, Any]:
    ensure_blueprint_refresh_tables.__wrapped__ = getattr(ensure_blueprint_refresh_tables, "__wrapped__", ensure_blueprint_refresh_tables)
    now_utc = datetime.now(UTC)
    cutoff = now_utc - timedelta(minutes=_STALE_RUNNING_REFRESH_MINUTES)
    active_scope = refresh_scope_snapshot(BLUEPRINT_WRITE_REFRESH_SCOPE) or {}
    active_claimed_at = str(active_scope.get("claimed_at") or "").strip()
    no_scope_cutoff = now_utc - timedelta(minutes=_NO_SCOPE_STALE_RUNNING_GRACE_MINUTES)
    rows = conn.execute(
        """
        SELECT run_id, started_at, details_json
        FROM blueprint_refresh_runs
        WHERE status = 'running' AND finished_at IS NULL
        ORDER BY started_at DESC
        """
    ).fetchall()
    updated = 0
    for row in rows:
        started_at_text = str(row["started_at"] or "").strip()
        try:
            started_at = datetime.fromisoformat(started_at_text.replace("Z", "+00:00"))
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=UTC)
            else:
                started_at = started_at.astimezone(UTC)
        except Exception:
            started_at = None
        if active_claimed_at and started_at_text == active_claimed_at:
            continue
        if started_at is not None:
            if active_claimed_at and started_at >= cutoff:
                continue
            if not active_claimed_at and started_at >= no_scope_cutoff:
                continue
        elif not active_claimed_at:
            pass
        else:
            continue
        details = {}
        try:
            details = json.loads(str(row["details_json"] or "{}"))
        except Exception:
            details = {}
        details.update(
            {
                "cleanup_reason": "stale_running_row_reconciled",
                "cleanup_finished_at": datetime.now(UTC).isoformat(),
            }
        )
        conn.execute(
            """
            UPDATE blueprint_refresh_runs
            SET status = 'abandoned',
                finished_at = ?,
                details_json = ?
            WHERE run_id = ? AND status = 'running' AND finished_at IS NULL
            """,
            (datetime.now(UTC).isoformat(), json.dumps(details, sort_keys=True), str(row["run_id"] or "")),
        )
        updated += int(conn.total_changes > 0)
    if updated:
        conn.commit()
    return {"reconciled_count": updated}


def record_blueprint_refresh_skip(
    conn: sqlite3.Connection,
    *,
    trigger_source: str,
    reason: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_blueprint_refresh_tables(conn)
    run_id = f"bp_refresh_{uuid.uuid4().hex[:12]}"
    now_iso = datetime.now(UTC).isoformat()
    payload = dict(details or {})
    payload.setdefault("skip_reason", str(reason or "already_running"))
    conn.execute(
        """
        INSERT INTO blueprint_refresh_runs (
          run_id, trigger_source, started_at, finished_at, status, candidate_count,
          success_count, failure_count, refreshed_symbols_json, failed_symbols_json, details_json
        ) VALUES (?, ?, ?, ?, 'skipped', 0, 0, 0, '[]', '[]', ?)
        """,
        (run_id, trigger_source, now_iso, now_iso, json.dumps(payload, sort_keys=True)),
    )
    conn.commit()
    return {
        "run_id": run_id,
        "status": "skipped",
        "reason": reason,
        "started_at": now_iso,
        "finished_at": now_iso,
        "details": payload,
    }


def run_blueprint_candidate_refresh(
    conn: sqlite3.Connection,
    *,
    settings: Settings,
    trigger_source: str,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    ensure_blueprint_refresh_tables(conn)
    ensure_candidate_registry_tables(conn)
    seed_default_candidate_registry(conn)
    run_id = f"bp_refresh_{uuid.uuid4().hex[:12]}"
    started_at = datetime.now(UTC)
    requested = {str(item or "").strip().upper() for item in list(symbols or []) if str(item or "").strip()}
    candidates = [
        item for item in list_active_candidate_registry(conn)
        if not requested or str(item.get("symbol") or "").strip().upper() in requested
    ]
    refreshed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    conn.execute(
        """
        INSERT INTO blueprint_refresh_runs (
          run_id, trigger_source, started_at, status, candidate_count
        ) VALUES (?, ?, ?, 'running', ?)
        """,
        (run_id, trigger_source, started_at.isoformat(), len(candidates)),
    )
    conn.commit()

    try:
        for candidate in candidates:
            symbol = str(candidate.get("symbol") or "").strip().upper()
            instrument_type = str(candidate.get("instrument_type") or "")
            try:
                if instrument_type in {"etf_ucits", "etf_us"}:
                    refresh_etf_data(symbol, settings=settings)
                truth = refresh_registry_candidate_truth(conn, symbol=symbol, activate_market_series=True)
                refreshed.append(
                    {
                        "symbol": symbol,
                        "source_state": truth.get("source_state"),
                        "factsheet_asof": truth.get("factsheet_asof"),
                        "market_data_asof": truth.get("market_data_asof"),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                failed.append({"symbol": symbol, "error": str(exc)})
        status = "succeeded" if not failed else ("partial" if refreshed else "failed")
        return _finish_run(
            conn,
            run_id=run_id,
            status=status,
            refreshed=refreshed,
            failed=failed,
            details={
                "trigger_source": trigger_source,
                "stale_after_hours": int(settings.blueprint_refresh_stale_after_hours),
            },
        )
    except Exception as exc:  # noqa: BLE001
        return _finish_run(
            conn,
            run_id=run_id,
            status="failed",
            refreshed=refreshed,
            failed=failed + [{"symbol": "*", "error": str(exc)}],
            details={"trigger_source": trigger_source},
        )


def latest_blueprint_refresh_status(conn: sqlite3.Connection, *, settings: Settings) -> dict[str, Any]:
    ensure_blueprint_refresh_tables(conn)
    row = conn.execute(
        """
        SELECT run_id, trigger_source, started_at, finished_at, status, candidate_count, success_count,
               failure_count, refreshed_symbols_json, failed_symbols_json, details_json
        FROM blueprint_refresh_runs
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return {
            "status": "never_run",
            "stale_state": "stale",
            "alert_banner": "Blueprint refresh has not run yet. Candidate truth may still reflect static registry data.",
            "last_success_at": None,
        }
    item = dict(row)
    for key in ("refreshed_symbols_json", "failed_symbols_json", "details_json"):
        try:
            item[key[:-5]] = json.loads(str(item.pop(key) or ("{}" if key == "details_json" else "[]")))
        except Exception:
            item[key[:-5]] = {} if key == "details_json" else []
    last_success_row = conn.execute(
        """
        SELECT finished_at
        FROM blueprint_refresh_runs
        WHERE status IN ('succeeded', 'partial') AND finished_at IS NOT NULL
        ORDER BY finished_at DESC
        LIMIT 1
        """
    ).fetchone()
    last_success_at = str(last_success_row[0]) if last_success_row is not None else None
    stale_state = "fresh"
    alert_banner = None
    if not last_success_at:
        stale_state = "stale"
        alert_banner = "Blueprint refresh has no successful run on record. Treat rankings as review-only."
    else:
        try:
            dt = datetime.fromisoformat(last_success_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            age = datetime.now(UTC) - dt
            if age > timedelta(hours=max(1, int(settings.blueprint_refresh_stale_after_hours))):
                stale_state = "stale"
                alert_banner = "Blueprint refresh is overdue. Candidate freshness and ranking trust should be treated as degraded."
            elif age > timedelta(hours=max(1, int(settings.blueprint_refresh_stale_after_hours // 2 or 1))):
                stale_state = "aging"
        except Exception:
            stale_state = "unknown"
            alert_banner = "Blueprint refresh timing could not be parsed."
    item["last_success_at"] = last_success_at
    item["stale_state"] = stale_state
    item["alert_banner"] = alert_banner
    return item


def _finish_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    status: str,
    refreshed: list[dict[str, Any]],
    failed: list[dict[str, Any]],
    details: dict[str, Any],
) -> dict[str, Any]:
    finished_at = datetime.now(UTC).isoformat()
    conn.execute(
        """
        UPDATE blueprint_refresh_runs
        SET finished_at = ?,
            status = ?,
            success_count = ?,
            failure_count = ?,
            refreshed_symbols_json = ?,
            failed_symbols_json = ?,
            details_json = ?
        WHERE run_id = ?
        """,
        (
            finished_at,
            status,
            len(refreshed),
            len(failed),
            json.dumps(refreshed, sort_keys=True),
            json.dumps(failed, sort_keys=True),
            json.dumps(details, sort_keys=True),
            run_id,
        ),
    )
    conn.commit()
    return {
        "run_id": run_id,
        "status": status,
        "started_at": conn.execute("SELECT started_at FROM blueprint_refresh_runs WHERE run_id = ?", (run_id,)).fetchone()[0],
        "finished_at": finished_at,
        "candidate_count": len(refreshed) + len(failed),
        "success_count": len(refreshed),
        "failure_count": len(failed),
        "refreshed_symbols": refreshed,
        "failed_symbols": failed,
    }
