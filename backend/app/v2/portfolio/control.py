from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.services.portfolio_ingest import (
    _create_aggregate_snapshot,
    _synchronize_active_legacy_state,
    activate_portfolio_upload,
    delete_portfolio_upload,
    ensure_portfolio_control_tables,
    get_portfolio_upload_detail,
    import_holdings_csv_to_snapshot,
    latest_snapshot_rows,
    latest_upload_run,
    latest_upload_run_id,
    list_portfolio_uploads,
)
from app.services.portfolio_state import list_sleeve_overrides, put_sleeve_override


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _account_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        account_id = str(row.get("account_id") or "broker")
        bucket = grouped.setdefault(
            account_id,
            {
                "account_id": account_id,
                "position_count": 0,
                "market_value": 0.0,
                "currencies": set(),
                "wrapper_label": account_id.replace("_", " ").title(),
            },
        )
        bucket["position_count"] += 1
        bucket["market_value"] += float(row.get("market_value") or 0.0)
        currency = str(row.get("currency") or "").strip().upper()
        if currency:
            bucket["currencies"].add(currency)
    result = []
    for item in grouped.values():
        result.append(
            {
                "account_id": item["account_id"],
                "position_count": item["position_count"],
                "market_value": round(float(item["market_value"]), 2),
                "currencies": sorted(item["currencies"]),
                "wrapper_label": item["wrapper_label"],
            }
        )
    return sorted(result, key=lambda row: (str(row["account_id"]),))


def _unresolved_mapping_rows(upload_detail: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not upload_detail:
        return []
    issues = list(upload_detail.get("issues") or [])
    positions_by_key = {
        str(position.get("security_key") or ""): dict(position)
        for position in list(upload_detail.get("positions") or [])
    }
    rows: list[dict[str, Any]] = []
    for issue in issues:
        security_key = str(issue.get("security_key") or "")
        position = positions_by_key.get(security_key, {})
        rows.append(
            {
                "issue_id": issue.get("issue_id"),
                "symbol": position.get("normalized_symbol") or position.get("raw_symbol"),
                "name": position.get("security_name") or security_key,
                "account_id": position.get("account_id"),
                "sleeve": position.get("sleeve"),
                "mapping_status": position.get("mapping_status"),
                "issue_type": issue.get("issue_type"),
                "severity": issue.get("severity"),
                "detail": issue.get("detail"),
                "security_key": security_key,
            }
        )
    return rows


def _mapping_quality_label(unresolved_count: int, stale_price_count: int, *, has_upload: bool) -> str:
    if not has_upload:
        return "Awaiting holdings upload"
    if unresolved_count == 0 and stale_price_count == 0:
        return "Resolved and priced"
    if unresolved_count > 0:
        return "Mapping review required"
    return "Pricing review required"


def _portfolio_source_state(active_upload: dict[str, Any] | None) -> dict[str, Any]:
    if active_upload is None:
        return {
            "state": "empty",
            "reason_codes": ["no_holdings"],
            "summary": "No active holdings upload is available yet.",
        }
    unresolved = int(active_upload.get("mapping_issue_count") or 0)
    stale_prices = int(active_upload.get("stale_price_count") or 0)
    if unresolved > 0 or stale_prices > 0:
        reason_codes: list[str] = []
        if unresolved > 0:
            reason_codes.append("unresolved_mappings")
        if stale_prices > 0:
            reason_codes.append("stale_pricing")
        return {
            "state": "degraded",
            "reason_codes": reason_codes,
            "summary": "Portfolio upload is active, but mapping or stale-price issues still need review.",
        }
    return {
        "state": "ready",
        "reason_codes": [],
        "summary": "Portfolio upload and mapping state are ready for investor-facing use.",
    }


def build_portfolio_status(conn: sqlite3.Connection, *, account_id: str = "default") -> dict[str, Any]:
    ensure_portfolio_control_tables(conn)
    active_upload = latest_upload_run(conn)
    active_run_id = str(active_upload.get("run_id") or "") if active_upload else None
    upload_detail = get_portfolio_upload_detail(conn, active_run_id) if active_run_id else None
    positions = latest_snapshot_rows(conn, run_id=active_run_id, account_id=None) if active_run_id else []
    unresolved_rows = _unresolved_mapping_rows(upload_detail)
    base_currency = next(
        (
            str(row.get("base_currency") or "").upper()
            for row in positions
            if str(row.get("base_currency") or "").strip()
        ),
        "SGD",
    )
    overrides = list_sleeve_overrides(conn)
    mapping_summary = {
        "quality_label": _mapping_quality_label(
            int(active_upload.get("mapping_issue_count") or 0) if active_upload else 0,
            int(active_upload.get("stale_price_count") or 0) if active_upload else 0,
            has_upload=active_upload is not None,
        ),
        "unresolved_count": int(active_upload.get("mapping_issue_count") or 0) if active_upload else 0,
        "stale_price_count": int(active_upload.get("stale_price_count") or 0) if active_upload else 0,
        "override_count": len(overrides),
    }
    return {
        "generated_at": _now_iso(),
        "account_id": account_id,
        "portfolio_source_state": _portfolio_source_state(active_upload),
        "active_upload": active_upload,
        "upload_history": list_portfolio_uploads(conn, include_deleted=False)[:8],
        "mapping_summary": mapping_summary,
        "unresolved_mapping_rows": unresolved_rows[:50],
        "account_summary": _account_summary(positions),
        "base_currency": base_currency,
        "mapping_overrides": [
            {"symbol": symbol, "sleeve": sleeve}
            for symbol, sleeve in sorted(overrides.items())
        ],
    }


def create_upload(
    conn: sqlite3.Connection,
    *,
    csv_text: str,
    filename: str | None,
    source_name: str,
    default_currency: str,
    default_account_type: str,
    allow_live_pricing: bool,
) -> dict[str, Any]:
    result = import_holdings_csv_to_snapshot(
        conn,
        csv_text,
        default_currency=default_currency,
        default_account_type=default_account_type,
        source_name=source_name,
        filename=filename,
        allow_live_pricing=allow_live_pricing,
    )
    run_id = result.get("run_id")
    detail = get_portfolio_upload_detail(conn, str(run_id)) if run_id else None
    return {
        **result,
        "upload_detail": detail,
        "portfolio_status": build_portfolio_status(conn),
    }


def activate_upload(conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
    payload = activate_portfolio_upload(conn, run_id)
    payload["portfolio_status"] = build_portfolio_status(conn)
    return payload


def delete_upload(conn: sqlite3.Connection, run_id: str, *, deleted_reason: str | None = None) -> dict[str, Any]:
    payload = delete_portfolio_upload(conn, run_id, deleted_reason=deleted_reason)
    payload["portfolio_status"] = build_portfolio_status(conn)
    return payload


def list_mapping_issues(conn: sqlite3.Connection, *, run_id: str | None = None) -> list[dict[str, Any]]:
    effective_run_id = run_id or latest_upload_run_id(conn)
    if not effective_run_id:
        return []
    detail = get_portfolio_upload_detail(conn, effective_run_id)
    return _unresolved_mapping_rows(detail)


def list_mapping_overrides(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    overrides = list_sleeve_overrides(conn)
    return [{"symbol": symbol, "sleeve": sleeve} for symbol, sleeve in sorted(overrides.items())]


def put_mapping_override(conn: sqlite3.Connection, *, symbol: str, sleeve: str) -> dict[str, Any]:
    ensure_portfolio_control_tables(conn)
    put_sleeve_override(conn, symbol, sleeve)
    active_run_id = latest_upload_run_id(conn)
    if active_run_id:
        conn.execute(
            """
            UPDATE portfolio_holding_snapshots
            SET sleeve = ?, mapping_status = 'manual_override'
            WHERE run_id = ? AND normalized_symbol = ?
            """,
            (sleeve, active_run_id, symbol.upper()),
        )
        rows = latest_snapshot_rows(conn, run_id=active_run_id)
        if rows:
            uploaded_at = str(rows[0].get("uploaded_at") or _now_iso())
            holdings_as_of_date = str(rows[0].get("holdings_as_of_date") or uploaded_at[:10])
            _create_aggregate_snapshot(
                conn,
                run_id=active_run_id,
                uploaded_at=uploaded_at,
                holdings_as_of_date=holdings_as_of_date,
                rows=rows,
            )
            _synchronize_active_legacy_state(conn, active_run_id)
        issue_rows = conn.execute(
            """
            SELECT security_key
            FROM portfolio_holding_snapshots
            WHERE run_id = ? AND normalized_symbol = ?
            """,
            (active_run_id, symbol.upper()),
        ).fetchall()
        security_keys = [str(row["security_key"]) for row in issue_rows if str(row["security_key"] or "")]
        if security_keys:
            placeholders = ",".join("?" for _ in security_keys)
            conn.execute(
                f"""
                DELETE FROM portfolio_mapping_issues
                WHERE run_id = ? AND issue_type = 'mapping' AND security_key IN ({placeholders})
                """,
                (active_run_id, *security_keys),
            )
            conn.execute(
                """
                UPDATE portfolio_upload_runs
                SET mapping_issue_count = (
                  SELECT COUNT(*)
                  FROM portfolio_mapping_issues
                  WHERE run_id = ? AND issue_type = 'mapping'
                )
                WHERE run_id = ?
                """,
                (active_run_id, active_run_id),
            )
            conn.commit()
    return {
        "saved": True,
        "symbol": symbol.upper(),
        "sleeve": sleeve,
        "portfolio_status": build_portfolio_status(conn),
    }
