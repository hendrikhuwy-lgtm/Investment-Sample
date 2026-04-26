from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, date, datetime
from typing import Any

from app.services.review_items import upsert_review_item


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_limit_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS limit_profiles (
          limit_id TEXT PRIMARY KEY,
          blueprint_id TEXT,
          strategy_id TEXT,
          limit_type TEXT NOT NULL,
          scope TEXT NOT NULL,
          threshold_value REAL NOT NULL,
          warning_threshold REAL,
          breach_severity TEXT NOT NULL DEFAULT 'medium',
          enabled INTEGER NOT NULL DEFAULT 1,
          effective_from TEXT,
          effective_to TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_limit_profiles_scope
        ON limit_profiles (COALESCE(blueprint_id, ''), COALESCE(strategy_id, ''), limit_type, scope)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS limit_breaches (
          breach_id TEXT PRIMARY KEY,
          limit_id TEXT NOT NULL,
          snapshot_id TEXT,
          run_id TEXT,
          scope_key TEXT,
          label TEXT,
          current_value REAL NOT NULL,
          threshold_value REAL NOT NULL,
          warning_threshold REAL,
          severity TEXT NOT NULL,
          breach_status TEXT NOT NULL,
          first_detected_at TEXT NOT NULL,
          last_detected_at TEXT NOT NULL,
          resolved_at TEXT,
          linked_review_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_limit_breaches_open
        ON limit_breaches (limit_id, COALESCE(run_id, ''), COALESCE(scope_key, ''), breach_status)
        WHERE breach_status IN ('warning', 'breached')
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_limit_breaches_run
        ON limit_breaches (run_id, severity, last_detected_at DESC)
        """
    )
    conn.commit()


def ensure_default_limit_profiles(
    conn: sqlite3.Connection,
    *,
    blueprint_id: str | None,
) -> list[dict[str, Any]]:
    ensure_limit_tables(conn)
    now = _now_iso()
    defaults = [
        ("single_position_max_weight", "portfolio", 0.15, 0.12, "high"),
        ("top_5_concentration_max", "portfolio", 0.55, 0.48, "high"),
        ("issuer_max_weight", "portfolio", 0.20, 0.16, "high"),
        ("sleeve_band_breach", "portfolio", 0.0, None, "medium"),
        ("convex_max_weight", "sleeve:convex", 0.10, 0.08, "medium"),
        ("cash_min_weight", "sleeve:cash", 0.02, 0.03, "medium"),
        ("stale_price_block_for_nav", "portfolio", 0.10, 0.05, "critical"),
        ("unmapped_weight_max", "portfolio", 0.02, 0.01, "high"),
    ]
    for limit_type, scope, threshold, warning, severity in defaults:
        row = conn.execute(
            """
            SELECT limit_id
            FROM limit_profiles
            WHERE COALESCE(blueprint_id, '') = COALESCE(?, '')
              AND limit_type = ?
              AND scope = ?
            LIMIT 1
            """,
            (blueprint_id, limit_type, scope),
        ).fetchone()
        if row is None:
            conn.execute(
                """
                INSERT INTO limit_profiles (
                  limit_id, blueprint_id, strategy_id, limit_type, scope, threshold_value,
                  warning_threshold, breach_severity, enabled, effective_from, effective_to,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"limit_{uuid.uuid4().hex[:12]}",
                    blueprint_id,
                    None,
                    limit_type,
                    scope,
                    threshold,
                    warning,
                    severity,
                    1,
                    date.today().isoformat(),
                    None,
                    now,
                    now,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE limit_profiles
                SET threshold_value = ?,
                    warning_threshold = ?,
                    breach_severity = ?,
                    enabled = 1,
                    updated_at = ?
                WHERE limit_id = ?
                """,
                (
                    threshold,
                    warning,
                    severity,
                    now,
                    str(row["limit_id"]),
                ),
            )
    conn.commit()
    return list_current_limit_profiles(conn, blueprint_id=blueprint_id)


def list_current_limit_profiles(conn: sqlite3.Connection, *, blueprint_id: str | None = None) -> list[dict[str, Any]]:
    ensure_limit_tables(conn)
    params: list[Any] = []
    where = "WHERE enabled = 1"
    if blueprint_id is not None:
        where += " AND COALESCE(blueprint_id, '') = COALESCE(?, '')"
        params.append(blueprint_id)
    rows = conn.execute(
        f"""
        SELECT limit_id, blueprint_id, strategy_id, limit_type, scope, threshold_value,
               warning_threshold, breach_severity, enabled, effective_from, effective_to,
               created_at, updated_at
        FROM limit_profiles
        {where}
        ORDER BY limit_type ASC
        """,
        tuple(params),
    ).fetchall()
    return [
        {
            "limit_id": str(row["limit_id"]),
            "blueprint_id": row["blueprint_id"],
            "strategy_id": row["strategy_id"],
            "limit_type": str(row["limit_type"]),
            "scope": str(row["scope"]),
            "threshold_value": float(row["threshold_value"]),
            "warning_threshold": None if row["warning_threshold"] is None else float(row["warning_threshold"]),
            "breach_severity": str(row["breach_severity"]),
            "enabled": bool(row["enabled"]),
            "effective_from": row["effective_from"],
            "effective_to": row["effective_to"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def _evaluate_upper_bound(current_value: float, threshold: float, warning: float | None) -> str:
    if current_value >= threshold:
        return "breached"
    if warning is not None and current_value > warning:
        return "warning"
    return "ok"


def _evaluate_lower_bound(current_value: float, threshold: float, warning: float | None) -> str:
    if current_value <= threshold:
        return "breached"
    if warning is not None and current_value < warning:
        return "warning"
    return "ok"


def _upsert_breach(
    conn: sqlite3.Connection,
    *,
    limit_id: str,
    snapshot_id: str | None,
    run_id: str | None,
    scope_key: str,
    label: str,
    current_value: float,
    threshold_value: float,
    warning_threshold: float | None,
    severity: str,
    breach_status: str,
) -> dict[str, Any]:
    existing = conn.execute(
        """
        SELECT breach_id, first_detected_at
        FROM limit_breaches
        WHERE limit_id = ? AND COALESCE(run_id, '') = COALESCE(?, '') AND COALESCE(scope_key, '') = COALESCE(?, '')
          AND breach_status IN ('warning', 'breached')
        LIMIT 1
        """,
        (limit_id, run_id, scope_key),
    ).fetchone()
    breach_id = str(existing["breach_id"]) if existing is not None else f"breach_{uuid.uuid4().hex[:12]}"
    first_detected_at = str(existing["first_detected_at"]) if existing is not None else _now_iso()
    last_detected_at = _now_iso()
    conn.execute(
        """
        INSERT INTO limit_breaches (
          breach_id, limit_id, snapshot_id, run_id, scope_key, label, current_value, threshold_value,
          warning_threshold, severity, breach_status, first_detected_at, last_detected_at, resolved_at, linked_review_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(breach_id) DO UPDATE SET
          snapshot_id=excluded.snapshot_id,
          run_id=excluded.run_id,
          scope_key=excluded.scope_key,
          label=excluded.label,
          current_value=excluded.current_value,
          threshold_value=excluded.threshold_value,
          warning_threshold=excluded.warning_threshold,
          severity=excluded.severity,
          breach_status=excluded.breach_status,
          last_detected_at=excluded.last_detected_at,
          resolved_at=excluded.resolved_at
        """,
        (
            breach_id,
            limit_id,
            snapshot_id,
            run_id,
            scope_key,
            label,
            round(float(current_value), 6),
            round(float(threshold_value), 6),
            None if warning_threshold is None else round(float(warning_threshold), 6),
            severity,
            breach_status,
            first_detected_at,
            last_detected_at,
            None,
            None,
        ),
    )
    return {
        "breach_id": breach_id,
        "limit_id": limit_id,
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "scope_key": scope_key,
        "label": label,
        "current_value": round(float(current_value), 6),
        "threshold_value": round(float(threshold_value), 6),
        "warning_threshold": None if warning_threshold is None else round(float(warning_threshold), 6),
        "severity": severity,
        "breach_status": breach_status,
        "first_detected_at": first_detected_at,
        "last_detected_at": last_detected_at,
        "resolved_at": None,
    }


def _resolve_cleared_breaches(
    conn: sqlite3.Connection,
    *,
    run_id: str | None,
    active_keys: set[tuple[str, str]],
) -> None:
    rows = conn.execute(
        """
        SELECT breach_id, limit_id, scope_key
        FROM limit_breaches
        WHERE COALESCE(run_id, '') = COALESCE(?, '') AND breach_status IN ('warning', 'breached')
        """,
        (run_id,),
    ).fetchall()
    now = _now_iso()
    for row in rows:
        key = (str(row["limit_id"]), str(row["scope_key"] or ""))
        if key in active_keys:
            continue
        conn.execute(
            """
            UPDATE limit_breaches
            SET breach_status = 'resolved', resolved_at = ?, last_detected_at = ?
            WHERE breach_id = ?
            """,
            (now, now, str(row["breach_id"])),
        )


def evaluate_limit_breaches(
    conn: sqlite3.Connection,
    *,
    run_id: str | None,
    snapshot_id: str | None,
    blueprint_id: str | None,
    exposures: dict[str, Any],
    blueprint_compare: dict[str, Any],
) -> dict[str, Any]:
    profiles = ensure_default_limit_profiles(conn, blueprint_id=blueprint_id)
    if not run_id or float((exposures.get("summary") or {}).get("total_value") or 0.0) <= 0.0:
        _resolve_cleared_breaches(conn, run_id=run_id, active_keys=set())
        conn.commit()
        return {
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "profiles": profiles,
            "items": [],
            "summary": {
                "warning_count": 0,
                "breach_count": 0,
                "critical_count": 0,
                "high_count": 0,
            },
        }
    active_keys: set[tuple[str, str]] = set()
    breaches: list[dict[str, Any]] = []

    for profile in profiles:
        limit_type = str(profile["limit_type"])
        scope = str(profile["scope"])
        threshold = float(profile["threshold_value"])
        warning = None if profile["warning_threshold"] is None else float(profile["warning_threshold"])
        severity = str(profile["breach_severity"])

        if limit_type == "single_position_max_weight":
            for item in list(exposures.get("top_positions") or []):
                state = _evaluate_upper_bound(float(item.get("weight") or 0.0), threshold, warning)
                if state == "ok":
                    continue
                active_keys.add((profile["limit_id"], str(item.get("security_key"))))
                breaches.append(
                    _upsert_breach(
                        conn,
                        limit_id=profile["limit_id"],
                        snapshot_id=snapshot_id,
                        run_id=run_id,
                        scope_key=str(item.get("security_key")),
                        label=str(item.get("normalized_symbol") or item.get("security_name") or "Position"),
                        current_value=float(item.get("weight") or 0.0),
                        threshold_value=threshold,
                        warning_threshold=warning,
                        severity="high" if state == "breached" else "medium",
                        breach_status=state,
                    )
                )
        elif limit_type == "top_5_concentration_max":
            value = float((exposures.get("summary") or {}).get("top_5_concentration") or 0.0)
            state = _evaluate_upper_bound(value, threshold, warning)
            if state != "ok":
                active_keys.add((profile["limit_id"], "portfolio"))
                breaches.append(
                    _upsert_breach(
                        conn,
                        limit_id=profile["limit_id"],
                        snapshot_id=snapshot_id,
                        run_id=run_id,
                        scope_key="portfolio",
                        label="Top 5 concentration",
                        current_value=value,
                        threshold_value=threshold,
                        warning_threshold=warning,
                        severity="high" if state == "breached" else "medium",
                        breach_status=state,
                    )
                )
        elif limit_type == "issuer_max_weight":
            for item in list(exposures.get("top_issuers") or []):
                state = _evaluate_upper_bound(float(item.get("weight") or 0.0), threshold, warning)
                if state == "ok":
                    continue
                active_keys.add((profile["limit_id"], str(item.get("issuer_key"))))
                breaches.append(
                    _upsert_breach(
                        conn,
                        limit_id=profile["limit_id"],
                        snapshot_id=snapshot_id,
                        run_id=run_id,
                        scope_key=str(item.get("issuer_key")),
                        label=str(item.get("issuer_label") or "Issuer"),
                        current_value=float(item.get("weight") or 0.0),
                        threshold_value=threshold,
                        warning_threshold=warning,
                        severity=severity if state == "breached" else "medium",
                        breach_status=state,
                    )
                )
        elif limit_type == "sleeve_band_breach":
            for row in list(blueprint_compare.get("comparison_rows") or []):
                if row.get("rebalance_candidate"):
                    deviation = abs(float(row.get("deviation") or 0.0))
                    active_keys.add((profile["limit_id"], str(row.get("sleeve_key"))))
                    breaches.append(
                        _upsert_breach(
                            conn,
                            limit_id=profile["limit_id"],
                            snapshot_id=snapshot_id,
                            run_id=run_id,
                            scope_key=str(row.get("sleeve_key")),
                            label=str(row.get("sleeve_name") or row.get("sleeve_key") or "Sleeve"),
                            current_value=deviation,
                            threshold_value=max(
                                abs(float(row.get("current_weight") or 0.0) - float(row.get("band_min") or 0.0)),
                                abs(float(row.get("current_weight") or 0.0) - float(row.get("band_max") or 0.0)),
                            ),
                            warning_threshold=0.0,
                            severity="high" if str(row.get("breach_severity")) == "high" else "medium",
                            breach_status="breached",
                        )
                    )
        elif limit_type == "convex_max_weight":
            current = next((item for item in list(exposures.get("sleeve_concentration") or []) if str(item.get("sleeve")) == "convex"), None)
            value = float((current or {}).get("weight") or 0.0)
            state = _evaluate_upper_bound(value, threshold, warning)
            if state != "ok":
                active_keys.add((profile["limit_id"], "convex"))
                breaches.append(
                    _upsert_breach(
                        conn,
                        limit_id=profile["limit_id"],
                        snapshot_id=snapshot_id,
                        run_id=run_id,
                        scope_key="convex",
                        label="Convex sleeve",
                        current_value=value,
                        threshold_value=threshold,
                        warning_threshold=warning,
                        severity=severity if state == "breached" else "medium",
                        breach_status=state,
                    )
                )
        elif limit_type == "cash_min_weight":
            current = next((item for item in list(exposures.get("sleeve_concentration") or []) if str(item.get("sleeve")) == "cash"), None)
            value = float((current or {}).get("weight") or 0.0)
            state = _evaluate_lower_bound(value, threshold, warning)
            if state != "ok":
                active_keys.add((profile["limit_id"], "cash"))
                breaches.append(
                    _upsert_breach(
                        conn,
                        limit_id=profile["limit_id"],
                        snapshot_id=snapshot_id,
                        run_id=run_id,
                        scope_key="cash",
                        label="Cash sleeve",
                        current_value=value,
                        threshold_value=threshold,
                        warning_threshold=warning,
                        severity=severity if state == "breached" else "medium",
                        breach_status=state,
                    )
                )
        elif limit_type == "stale_price_block_for_nav":
            value = float((exposures.get("stale_priced_weight") or {}).get("weight") or 0.0)
            state = _evaluate_upper_bound(value, threshold, warning)
            if state != "ok":
                active_keys.add((profile["limit_id"], "portfolio"))
                breaches.append(
                    _upsert_breach(
                        conn,
                        limit_id=profile["limit_id"],
                        snapshot_id=snapshot_id,
                        run_id=run_id,
                        scope_key="portfolio",
                        label="Stale priced weight",
                        current_value=value,
                        threshold_value=threshold,
                        warning_threshold=warning,
                        severity="critical" if state == "breached" else "medium",
                        breach_status=state,
                    )
                )
        elif limit_type == "unmapped_weight_max":
            value = float((exposures.get("unmapped_weight") or {}).get("weight") or 0.0)
            state = _evaluate_upper_bound(value, threshold, warning)
            if state != "ok":
                active_keys.add((profile["limit_id"], "portfolio"))
                breaches.append(
                    _upsert_breach(
                        conn,
                        limit_id=profile["limit_id"],
                        snapshot_id=snapshot_id,
                        run_id=run_id,
                        scope_key="portfolio",
                        label="Unmapped weight",
                        current_value=value,
                        threshold_value=threshold,
                        warning_threshold=warning,
                        severity=severity if state == "breached" else "medium",
                        breach_status=state,
                    )
                )

    _resolve_cleared_breaches(conn, run_id=run_id, active_keys=active_keys)
    conn.commit()
    return {
        "run_id": run_id,
        "snapshot_id": snapshot_id,
        "profiles": profiles,
        "items": sorted(
            breaches,
            key=lambda item: (
                {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(str(item.get("severity")), 4),
                -abs(float(item.get("current_value") or 0.0)),
            ),
        ),
        "summary": {
            "warning_count": sum(1 for item in breaches if str(item.get("breach_status")) == "warning"),
            "breach_count": sum(1 for item in breaches if str(item.get("breach_status")) == "breached"),
            "critical_count": sum(1 for item in breaches if str(item.get("severity")) == "critical"),
            "high_count": sum(1 for item in breaches if str(item.get("severity")) == "high"),
        },
    }


def list_latest_limit_breaches(conn: sqlite3.Connection, *, run_id: str | None) -> dict[str, Any]:
    ensure_limit_tables(conn)
    rows = conn.execute(
        """
        SELECT breach_id, limit_id, snapshot_id, run_id, scope_key, label, current_value, threshold_value,
               warning_threshold, severity, breach_status, first_detected_at, last_detected_at, resolved_at, linked_review_id
        FROM limit_breaches
        WHERE COALESCE(run_id, '') = COALESCE(?, '') AND breach_status IN ('warning', 'breached')
        ORDER BY
          CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
          ABS(current_value) DESC
        """,
        (run_id,),
    ).fetchall()
    items = [dict(row) for row in rows]
    return {
        "run_id": run_id,
        "items": items,
        "summary": {
            "warning_count": sum(1 for item in items if str(item.get("breach_status")) == "warning"),
            "breach_count": sum(1 for item in items if str(item.get("breach_status")) == "breached"),
            "critical_count": sum(1 for item in items if str(item.get("severity")) == "critical"),
            "high_count": sum(1 for item in items if str(item.get("severity")) == "high"),
        },
    }


def sync_limit_reviews(conn: sqlite3.Connection, *, breaches: dict[str, Any]) -> None:
    active_keys: set[str] = set()
    for item in list(breaches.get("items") or []):
        review_key = f"limit_breach::{item.get('limit_id')}::{item.get('scope_key')}"
        active_keys.add(review_key)
        state = str(item.get("breach_status") or "warning")
        severity = str(item.get("severity") or "medium")
        label = str(item.get("label") or item.get("scope_key") or "Limit")
        upsert_review_item(
            conn,
            review_key=review_key,
            category="limit_breach",
            severity=severity if state == "breached" else "medium",
            linked_object_type="limit_breach",
            linked_object_id=str(item.get("breach_id")),
            source_run_id=item.get("run_id"),
            notes=(
                f"{label} is {state}: current {float(item.get('current_value') or 0.0) * 100:.1f}% "
                f"vs threshold {float(item.get('threshold_value') or 0.0) * 100:.1f}%."
            ),
            due_in_days=0 if severity in {"critical", "high"} else 1,
        )
    stale_rows = conn.execute(
        """
        SELECT review_key
        FROM review_items
        WHERE category = 'limit_breach' AND status <> 'resolved'
        """
    ).fetchall()
    for row in stale_rows:
        review_key = str(row["review_key"])
        if review_key in active_keys:
            continue
        existing = conn.execute(
            "SELECT notes FROM review_items WHERE review_key = ? LIMIT 1",
            (review_key,),
        ).fetchone()
        notes = str((existing or {}).get("notes") or "").strip() if isinstance(existing, dict) else str(existing["notes"] or "").strip()
        suffix = "Auto-resolved after control refresh."
        merged_notes = f"{notes} {suffix}".strip()
        conn.execute(
            """
            UPDATE review_items
            SET status = 'resolved',
                notes = ?,
                updated_at = ?
            WHERE review_key = ?
            """,
            (merged_notes, _now_iso(), review_key),
        )
    conn.commit()
