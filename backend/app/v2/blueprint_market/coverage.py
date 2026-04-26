from __future__ import annotations

import sqlite3
from typing import Any

from app.services.symbol_resolution import ensure_symbol_resolution_tables, resolve_provider_identifiers
from app.v2.blueprint_market.market_identity import candidate_id_for_symbol, ensure_candidate_market_identities
from app.v2.blueprint_market.series_refresh_service import check_candidate_series_freshness
from app.v2.blueprint_market.series_store import load_price_series


_HISTORY_READY_BARS = 260
_STABLE_PROXY_HISTORY_BARS = 1000
_STABLE_ROUTE_REASONS = {
    "ucits_exchange_qualified_preferred",
    "sgx_exchange_qualified_preferred",
    "exchange_qualified_alias",
    "route_candidate_promoted",
}

_BENCHMARK_FIELDS = {"benchmark_key", "benchmark_name"}


def _checklist_item(item_id: str, label: str, state: str, detail: str | None = None) -> dict[str, Any]:
    return {
        "item_id": item_id,
        "label": label,
        "state": state,
        "detail": detail,
    }


def _status_summary(
    status: str,
    *,
    symbol: str,
    direct_history_depth: int,
    proxy_history_depth: int,
    benchmark_lineage_weak: bool,
    alias_review_needed: bool,
    kronos_eligible: bool,
) -> str:
    if status == "direct_ready":
        return (
            f"{symbol} has direct stored history and clean enough benchmark support for Blueprint runtime and "
            f"{'Kronos staging' if kronos_eligible else 'bounded market-path staging'}."
        )
    if status == "proxy_ready":
        return (
            f"{symbol} is currently usable through an approved proxy path. Direct history is still thinner than target, "
            "so provenance should stay explicit."
        )
    if status == "alias_review_needed":
        return (
            f"{symbol} still needs alias verification because the resolved provider symbol path is not yet strong enough "
            f"to rely on without review. Direct history depth is {direct_history_depth} bars."
        )
    if status == "benchmark_lineage_weak":
        return (
            f"{symbol} has market history, but benchmark lineage is still too weak for a cleaner investor-facing coverage label."
        )
    if direct_history_depth or proxy_history_depth:
        return (
            f"{symbol} has partial stored history, but it is still short of the direct-ready threshold and needs further warmup."
        )
    return f"{symbol} does not yet have enough stored history for a clean Blueprint market-path workflow."


def _coverage_status(
    *,
    direct_ready: bool,
    proxy_ready: bool,
    alias_review_needed: bool,
    benchmark_lineage_weak: bool,
) -> str:
    if direct_ready and not benchmark_lineage_weak and not alias_review_needed:
        return "direct_ready"
    if proxy_ready and not benchmark_lineage_weak:
        return "proxy_ready"
    if alias_review_needed:
        return "alias_review_needed"
    if not direct_ready and not proxy_ready:
        return "missing_history"
    if benchmark_lineage_weak:
        return "benchmark_lineage_weak"
    return "missing_history"


def _benchmark_lineage_weak_state(
    truth_context: dict[str, Any],
    candidate_row: dict[str, Any],
) -> bool:
    report = [
        dict(item)
        for item in list(dict(truth_context or {}).get("reconciliation_report") or [])
        if str(item.get("field_name") or "").strip() in _BENCHMARK_FIELDS
    ]
    statuses = {str(item.get("status") or "").strip() for item in report if str(item.get("status") or "").strip()}
    if statuses & {"hard_conflict", "critical_missing", "stale", "weak_authority"}:
        return True
    if statuses:
        return False
    return False


def _latest_series_run(conn: sqlite3.Connection, *, candidate_id: str, series_role: str) -> dict[str, Any]:
    try:
        row = conn.execute(
            """
            SELECT provider_name, status, failure_class, details_json, started_at, finished_at
            FROM candidate_price_series_runs
            WHERE candidate_id = ? AND series_role = ?
            ORDER BY started_at DESC, rowid DESC
            LIMIT 1
            """,
            (candidate_id, series_role),
        ).fetchone()
    except sqlite3.OperationalError:
        return {}
    if row is None:
        return {}
    return {
        "provider_name": row["provider_name"],
        "status": row["status"],
        "failure_class": row["failure_class"],
        "details": {},
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
    }


def build_candidate_coverage_summary(
    conn: sqlite3.Connection,
    candidate_row: dict[str, Any],
    truth_context: dict[str, Any],
    *,
    candidate_id: str | None = None,
    market_path_support: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_symbol_resolution_tables(conn)
    symbol = str(candidate_row.get("symbol") or "").strip().upper()
    sleeve_key = str(candidate_row.get("sleeve_key") or "").strip()
    stable_candidate_id = candidate_id or candidate_id_for_symbol(symbol)
    alias_resolution = resolve_provider_identifiers(
        conn,
        provider_name="twelve_data",
        endpoint_family="ohlcv_history",
        identifier=symbol,
        asset_type=str(candidate_row.get("asset_class") or candidate_row.get("instrument_type") or "").strip() or None,
        region=str(candidate_row.get("domicile") or "").strip() or None,
    )
    identities = ensure_candidate_market_identities(conn, stable_candidate_id)
    direct_identity = next((item for item in identities if str(item.get("series_role") or "") == "direct"), None)
    proxy_identity = next((item for item in identities if str(item.get("series_role") or "") == "approved_proxy"), None)
    direct_rows = (
        load_price_series(conn, candidate_id=stable_candidate_id, series_role="direct", interval="1day", ascending=True)
        if direct_identity
        else []
    )
    proxy_rows = (
        load_price_series(conn, candidate_id=stable_candidate_id, series_role="approved_proxy", interval="1day", ascending=True)
        if proxy_identity
        else []
    )
    direct_quality = (
        check_candidate_series_freshness(conn, candidate_id=stable_candidate_id, series_role="direct")
        if direct_identity
        else {"series_quality_summary": None}
    )
    proxy_quality = (
        check_candidate_series_freshness(conn, candidate_id=stable_candidate_id, series_role="approved_proxy")
        if proxy_identity
        else {"series_quality_summary": None}
    )
    failure_class_summary = dict(truth_context.get("failure_class_summary") or {})
    hard_classes = {str(value) for value in list(failure_class_summary.get("hard_classes") or [])}
    benchmark_lineage_weak = _benchmark_lineage_weak_state(truth_context, candidate_row)
    direct_history_depth = len(direct_rows)
    proxy_history_depth = len(proxy_rows)
    direct_ready = direct_history_depth >= _HISTORY_READY_BARS
    proxy_ready = proxy_history_depth >= _HISTORY_READY_BARS and not direct_ready
    resolution_confidence = float(alias_resolution.get("resolution_confidence") or 0.0)
    provider_symbol = str(alias_resolution.get("provider_symbol") or "").strip().upper()
    exchange_qualified_symbol = str(alias_resolution.get("exchange_qualified_symbol") or "").strip().upper()
    route_reason = str(alias_resolution.get("resolution_reason") or "").strip().lower()
    stable_route = bool(
        provider_symbol
        and (
            route_reason in _STABLE_ROUTE_REASONS
            or (provider_symbol == exchange_qualified_symbol and resolution_confidence >= 0.8)
        )
    )
    stable_proxy_route = bool(
        proxy_ready
        and (
            resolution_confidence >= 0.85
            or (
                provider_symbol
                and exchange_qualified_symbol
                and provider_symbol == exchange_qualified_symbol
                and proxy_history_depth >= _STABLE_PROXY_HISTORY_BARS
                and route_reason in {
                    "ucits_exchange_qualified_preferred",
                    "sgx_exchange_qualified_preferred",
                    "exchange_qualified_alias",
                    "route_candidate_promoted",
                }
            )
        )
    )
    alias_review_needed = bool(
        (list(alias_resolution.get("fallback_aliases") or []) or str(alias_resolution.get("provider_symbol") or "") != symbol)
        and not direct_ready
        and not stable_proxy_route
        and not stable_route
        and resolution_confidence < 0.9
    )
    direct_run = _latest_series_run(conn, candidate_id=stable_candidate_id, series_role="direct")
    proxy_run = _latest_series_run(conn, candidate_id=stable_candidate_id, series_role="approved_proxy")
    support = dict(market_path_support or {})
    kronos_eligible = str(support.get("eligibility_state") or "") == "eligible"
    data_quality = dict(truth_context.get("data_quality") or {})
    source_integrity = dict(truth_context.get("source_integrity_summary") or {})
    metadata_complete = bool(symbol and str(candidate_row.get("name") or "").strip() and sleeve_key)
    exchange_verified = bool(
        str((direct_identity or {}).get("exchange_mic") or "").strip()
        or str(alias_resolution.get("exchange_qualified_symbol") or "").strip()
    )
    benchmark_verified = bool(
        str(candidate_row.get("benchmark_key") or "").strip()
        and not benchmark_lineage_weak
    )
    store_warmed = bool(direct_rows or proxy_rows)
    truth_audit_complete = not hard_classes and str(data_quality.get("data_confidence") or "").strip().lower() != "low"
    checklist = [
        _checklist_item(
            "metadata_complete",
            "Metadata complete",
            "ready" if metadata_complete else "missing",
            str(candidate_row.get("name") or symbol),
        ),
        _checklist_item(
            "symbol_resolved",
            "Symbol resolved",
            "ready" if alias_resolution.get("provider_symbol") else "review",
            f"Twelve Data symbol {alias_resolution.get('provider_symbol') or symbol}",
        ),
        _checklist_item(
            "exchange_verified",
            "Exchange verified",
            "ready" if exchange_verified else "review",
            str((direct_identity or {}).get("exchange_mic") or alias_resolution.get("exchange_qualified_symbol") or "Exchange still thin"),
        ),
        _checklist_item(
            "benchmark_lineage_verified",
            "Benchmark lineage verified",
            "ready" if benchmark_verified else "review",
            str(candidate_row.get("benchmark_key") or "Benchmark lineage still thin"),
        ),
        _checklist_item(
            "direct_history_present",
            "Direct history present",
            "ready" if direct_ready else "review",
            f"{direct_history_depth} direct bars",
        ),
        _checklist_item(
            "approved_proxy_reviewed",
            "Approved proxy reviewed",
            "ready" if proxy_ready else "not_needed" if direct_ready else "review",
            f"{proxy_history_depth} proxy bars",
        ),
        _checklist_item(
            "store_warmed",
            "Canonical store warmed",
            "ready" if store_warmed else "missing",
            "Stored history available" if store_warmed else "No stored history yet",
        ),
        _checklist_item(
            "kronos_eligible",
            "Kronos eligible",
            "ready" if kronos_eligible else "review",
            str(support.get("suppression_reason") or support.get("usefulness_label") or "No support yet"),
        ),
        _checklist_item(
            "truth_audit_complete",
            "Truth audit complete",
            "ready" if truth_audit_complete else "review",
            str(failure_class_summary.get("summary") or source_integrity.get("summary") or "Truth still bounded"),
        ),
        _checklist_item(
            "sleeve_staging_ready",
            "Sleeve staging ready",
            "ready" if sleeve_key else "missing",
            sleeve_key or "Sleeve key is missing",
        ),
    ]
    status = _coverage_status(
        direct_ready=direct_ready,
        proxy_ready=proxy_ready,
        alias_review_needed=alias_review_needed,
        benchmark_lineage_weak=benchmark_lineage_weak,
    )
    current_history_provider = (
        str(direct_rows[-1].get("provider") or "").strip()
        if direct_rows
        else str(proxy_rows[-1].get("provider") or "").strip()
        if proxy_rows
        else None
    )
    coverage_workflow_summary = {
        "status": status,
        "summary": _status_summary(
            status,
            symbol=symbol,
            direct_history_depth=direct_history_depth,
            proxy_history_depth=proxy_history_depth,
            benchmark_lineage_weak=benchmark_lineage_weak,
            alias_review_needed=alias_review_needed,
            kronos_eligible=kronos_eligible,
        ),
        "checklist": checklist,
        "current_runtime_provider": "twelve_data",
        "current_history_provider": current_history_provider,
        "direct_history_depth": direct_history_depth,
        "proxy_history_depth": proxy_history_depth,
        "benchmark_lineage_weak": benchmark_lineage_weak,
        "alias_review_needed": alias_review_needed,
        "route_stability_state": "stable" if stable_route else "review_needed" if alias_review_needed else "native",
        "symbol_alias_registry": {
            "direct_symbol": alias_resolution.get("direct_symbol") or symbol,
            "exchange_qualified_symbol": alias_resolution.get("exchange_qualified_symbol"),
            "provider_symbol": alias_resolution.get("provider_symbol") or symbol,
            "provider_alias": alias_resolution.get("provider_alias"),
            "manual_override": alias_resolution.get("manual_override"),
            "verification_source": alias_resolution.get("verification_source"),
            "resolution_confidence": resolution_confidence,
            "resolution_reason": alias_resolution.get("resolution_reason"),
            "fallback_aliases": list(alias_resolution.get("fallback_aliases") or []),
        },
        "direct_quality": dict(direct_quality.get("series_quality_summary") or {}),
        "proxy_quality": dict(proxy_quality.get("series_quality_summary") or {}),
        "direct_history_attempted": bool(direct_run),
        "direct_history_attempt_status": direct_run.get("status"),
        "direct_history_failure_class": direct_run.get("failure_class"),
        "direct_history_provider": direct_run.get("provider_name"),
        "proxy_history_attempted": bool(proxy_run),
        "proxy_history_attempt_status": proxy_run.get("status"),
        "proxy_history_failure_class": proxy_run.get("failure_class"),
        "proxy_history_provider": proxy_run.get("provider_name"),
    }
    return {
        "coverage_status": status,
        "coverage_workflow_summary": coverage_workflow_summary,
        "current_runtime_provider": "twelve_data",
        "direct_history_depth": direct_history_depth,
        "proxy_history_depth": proxy_history_depth,
        "direct_ready": direct_ready,
        "proxy_ready": proxy_ready,
        "missing_history": not bool(direct_rows or proxy_rows),
        "benchmark_lineage_weak": benchmark_lineage_weak,
        "alias_review_needed": alias_review_needed,
        "direct_identity": direct_identity,
        "proxy_identity": proxy_identity,
    }
