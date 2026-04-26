from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.v2.truth.candidate_quality import build_candidate_truth_context
from app.services.provider_family_success import list_provider_family_success
from app.services.provider_activation import build_provider_activation_report
from app.services.provider_cache import list_provider_snapshots
from app.services.provider_refresh import build_cached_external_upstream_payload
from app.services.provider_registry import DATA_FAMILY_OWNERSHIP, DATA_FAMILY_ROUTING, SURFACE_TARGET_FAMILIES


_FAMILY_ALIAS_TO_CANONICAL = {
    "quote_latest": "latest_quote",
    "benchmark_proxy": "benchmark_proxy_history",
    "reference_meta": "etf_reference_metadata",
}
_CANONICAL_TO_ENDPOINT = {value: key for key, value in _FAMILY_ALIAS_TO_CANONICAL.items()}
_CONTRADICTION_TOLERANCE = {
    "latest_quote": 0.03,
    "fx": 0.02,
    "benchmark_proxy_history": 0.05,
    "ohlcv_history": 0.05,
}
_SURFACE_FAMILY_ALIAS = {
    "daily_brief": {"latest_quote": "quote_latest", "benchmark_proxy_history": "benchmark_proxy"},
    "dashboard": {"latest_quote": "quote_latest", "benchmark_proxy_history": "benchmark_proxy"},
    "blueprint": {"etf_reference_metadata": "reference_meta"},
}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _canonical_family_name(family: str | None) -> str:
    return _FAMILY_ALIAS_TO_CANONICAL.get(str(family or "").strip(), str(family or "").strip())


def _endpoint_family_name(family: str | None) -> str:
    return _CANONICAL_TO_ENDPOINT.get(str(family or "").strip(), str(family or "").strip())


def _surface_family_name(surface_name: str, family_name: str) -> str:
    return (_SURFACE_FAMILY_ALIAS.get(surface_name) or {}).get(family_name, family_name)


def _extract_numeric_value(payload: dict[str, Any]) -> float | None:
    candidates = [
        payload.get("price"),
        payload.get("last"),
        payload.get("value"),
        payload.get("market_price"),
        payload.get("current_value"),
        payload.get("close"),
    ]
    for value in candidates:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number == number:
            return number
    return None


def _snapshot_items(conn: sqlite3.Connection, *, endpoint_family: str | None = None) -> list[dict[str, Any]]:
    rows = list_provider_snapshots(conn, endpoint_family=endpoint_family, limit=500)
    return [dict(row) for row in rows]


def _family_conflicts(activation_report: dict[str, Any], family_name: str, snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    coverage = dict(dict(activation_report.get("source_family_coverage") or {}).get(family_name) or {})
    coverage_state = str(coverage.get("coverage_state") or "")
    if coverage_state and coverage_state not in {"healthy"}:
        conflicts.append(
            {
                "conflict_type": "coverage_state",
                "family_name": family_name,
                "severity": "high" if coverage_state in {"missing", "configured_no_current_data"} else "medium",
                "message": f"{family_name} is currently {coverage_state}.",
                "providers": list(coverage.get("active_providers") or []),
            }
        )

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in snapshots:
        cache_key = str(row.get("cache_key") or "")
        if cache_key:
            grouped.setdefault(cache_key, []).append(row)

    tolerance = _CONTRADICTION_TOLERANCE.get(family_name, 0.03)
    for cache_key, rows in grouped.items():
        if len(rows) < 2:
            continue
        values: list[tuple[str, float]] = []
        for row in rows:
            value = _extract_numeric_value(dict(row.get("payload") or {}))
            if value is None:
                continue
            values.append((str(row.get("provider_name") or ""), value))
        if len(values) < 2:
            continue
        numeric_values = [item[1] for item in values]
        min_value = min(numeric_values)
        max_value = max(numeric_values)
        baseline = max(abs(min_value), abs(max_value), 1e-9)
        spread = abs(max_value - min_value) / baseline
        if spread >= tolerance:
            conflicts.append(
                {
                    "conflict_type": "contradictory_live_truth",
                    "family_name": family_name,
                    "severity": "medium",
                    "message": f"{family_name} providers disagree beyond tolerance for {cache_key}.",
                    "cache_key": cache_key,
                    "spread_ratio": round(spread, 6),
                    "providers": [
                        {
                            "provider_name": provider_name,
                            "value": round(value, 8),
                        }
                        for provider_name, value in values
                    ],
                }
            )
    return conflicts


def _candidate_field_conflicts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT DISTINCT candidate_symbol, sleeve_key
        FROM candidate_field_current
        ORDER BY candidate_symbol, sleeve_key
        """
    ).fetchall()
    conflicts: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        symbol = str(item.get("candidate_symbol") or "").strip().upper()
        sleeve_key = str(item.get("sleeve_key") or "").strip()
        if not symbol or not sleeve_key:
            continue
        truth_context = build_candidate_truth_context(
            conn,
            {
                "symbol": symbol,
                "sleeve_key": sleeve_key,
            },
        )
        authority_by_field = {
            str(field.get("field_name") or ""): dict(field)
            for field in list(truth_context.get("source_authority_map") or [])
        }
        for field in list(truth_context.get("reconciliation_report") or []):
            status = str(field.get("status") or "")
            if status in {"verified"}:
                continue
            authority = authority_by_field.get(str(field.get("field_name") or ""), {})
            conflicts.append(
                {
                    "conflict_type": "candidate_field_conflict",
                    "family_name": "candidate_truth",
                    "severity": str(field.get("severity") or "important"),
                    "message": str(field.get("summary") or ""),
                    "candidate_symbol": symbol,
                    "sleeve_key": sleeve_key,
                    "field_name": field.get("field_name"),
                    "label": field.get("label"),
                    "status": status,
                    "blocking_effect": field.get("blocking_effect"),
                    "source_names": list(field.get("source_names") or []),
                    "resolved_value": field.get("resolved_value"),
                    "observed_values": list(field.get("observed_values") or []),
                    "recommendation_critical": bool(field.get("recommendation_critical")),
                    "authority_mix": {
                        "authority_class": authority.get("authority_class"),
                        "freshness_state": authority.get("freshness_state"),
                        "document_support_state": authority.get("document_support_state"),
                    },
                    "value_count": int(field.get("value_count") or 0),
                }
            )
    return conflicts


def build_source_authority_payload(conn: sqlite3.Connection, settings: Settings) -> dict[str, Any]:
    activation_report = build_provider_activation_report(conn)
    cached_payload = build_cached_external_upstream_payload(conn, settings)
    family_success_rows = [dict(row) for row in list_provider_family_success(conn)]
    family_success_map: dict[str, list[dict[str, Any]]] = {}
    for row in family_success_rows:
        family_success_map.setdefault(str(row.get("family_name") or ""), []).append(row)
    family_rows: list[dict[str, Any]] = []
    all_conflicts: list[dict[str, Any]] = []
    coverage_map = dict(activation_report.get("source_family_coverage") or {})
    for family_name, ownership in DATA_FAMILY_OWNERSHIP.items():
        endpoint_family = _endpoint_family_name(family_name)
        family_snapshots = _snapshot_items(conn, endpoint_family=endpoint_family)
        conflicts = _family_conflicts(activation_report, family_name, family_snapshots)
        all_conflicts.extend(conflicts)
        coverage = dict(coverage_map.get(family_name) or {})
        success_rows = list(family_success_map.get(family_name) or [])
        family_rows.append(
            {
                "family_name": family_name,
                "endpoint_family": endpoint_family,
                "primary_provider": ownership.get("primary_provider"),
                "secondary_provider": ownership.get("secondary_provider"),
                "public_fallback": list(ownership.get("public_fallback") or []),
                "legacy_fallback": list(ownership.get("legacy_fallback") or []),
                "refresh_cadence_seconds": ownership.get("refresh_cadence_seconds"),
                "investor_importance": ownership.get("investor_importance"),
                "configured_providers": list(coverage.get("configured_providers") or []),
                "active_providers": list(coverage.get("active_providers") or []),
                "coverage_state": coverage.get("coverage_state"),
                "authority": coverage.get("authority"),
                "snapshot_count": len(family_snapshots),
                "current_terminal_states": [
                    {
                        "provider_name": row.get("provider_name"),
                        "current_terminal_state": row.get("current_terminal_state"),
                        "current_terminal_cause": row.get("current_terminal_cause"),
                    }
                    for row in success_rows
                ],
                "conflict_count": len(conflicts),
                "conflicts": conflicts,
            }
        )
    field_conflicts = _candidate_field_conflicts(conn)
    return {
        "generated_at": _now_iso(),
        "families": family_rows,
        "summary": {
            "family_count": len(family_rows),
            "healthy_family_count": sum(1 for item in family_rows if str(item.get("coverage_state") or "") == "healthy"),
            "degraded_family_count": sum(
                1
                for item in family_rows
                if str(item.get("coverage_state") or "") not in {"healthy", ""}
            ),
            "current_failure_family_count": sum(
                1
                for item in family_rows
                if any(
                    str(state.get("current_terminal_state") or "") == "current_failure"
                    for state in list(item.get("current_terminal_states") or [])
                )
            ),
            "conflict_count": len(all_conflicts) + len(field_conflicts),
            "candidate_field_conflict_count": len(field_conflicts),
        },
        "provider_summary": dict(activation_report.get("summary") or {}),
        "surface_summary": dict(cached_payload.get("summary") or {}),
        "candidate_field_conflicts": field_conflicts,
    }


def build_family_authority_payload(conn: sqlite3.Connection, settings: Settings, family: str) -> dict[str, Any]:
    family_name = _canonical_family_name(family)
    if family_name not in DATA_FAMILY_OWNERSHIP:
        raise KeyError(family_name)
    activation_report = build_provider_activation_report(conn)
    coverage = dict(dict(activation_report.get("source_family_coverage") or {}).get(family_name) or {})
    endpoint_family = _endpoint_family_name(family_name)
    snapshots = _snapshot_items(conn, endpoint_family=endpoint_family)
    conflicts = _family_conflicts(activation_report, family_name, snapshots)
    ownership = dict(DATA_FAMILY_OWNERSHIP.get(family_name) or {})
    family_success = [dict(row) for row in list_provider_family_success(conn, family_name=family_name)]
    return {
        "generated_at": _now_iso(),
        "family_name": family_name,
        "endpoint_family": endpoint_family,
        "surface_targets": [
            surface_name
            for surface_name, families in SURFACE_TARGET_FAMILIES.items()
            if endpoint_family in families or family_name in families
        ],
        "routing_order": list(DATA_FAMILY_ROUTING.get(endpoint_family) or []),
        "authority_policy": {
            "primary_provider": ownership.get("primary_provider"),
            "secondary_provider": ownership.get("secondary_provider"),
            "public_fallback": list(ownership.get("public_fallback") or []),
            "legacy_fallback": list(ownership.get("legacy_fallback") or []),
            "refresh_cadence_seconds": ownership.get("refresh_cadence_seconds"),
            "investor_importance": ownership.get("investor_importance"),
            "fail_closed_when": [
                "missing_fetchable",
                "missing_source_gap",
                "contradictory_live_truth",
                "source_backed_stale",
            ],
        },
        "coverage": coverage,
        "family_success": family_success,
        "conflicts": conflicts,
        "recent_snapshots": [
            {
                "provider_name": row.get("provider_name"),
                "cache_key": row.get("cache_key"),
                "fetched_at": row.get("fetched_at"),
                "freshness_state": row.get("freshness_state"),
                "confidence_tier": row.get("confidence_tier"),
                "error_state": row.get("error_state"),
                "fallback_used": bool(row.get("fallback_used")),
                "payload_summary": {
                    "identifier": dict(row.get("payload") or {}).get("identifier"),
                    "value": _extract_numeric_value(dict(row.get("payload") or {})),
                    "source_ref": row.get("source_ref"),
                },
            }
            for row in snapshots[:40]
        ],
    }


def build_surface_readiness_payload(conn: sqlite3.Connection, settings: Settings) -> dict[str, Any]:
    surfaces: list[dict[str, Any]] = []
    for surface_name in ("daily_brief", "dashboard", "blueprint"):
        payload = build_cached_external_upstream_payload(conn, settings, surface_name=surface_name)
        summary = dict(payload.get("summary") or {})
        governance = dict(summary.get("governance") or {})
        issues = list(summary.get("issues") or [])
        diversity = dict(summary.get("source_diversity") or {})
        critical_families = list(diversity.get("critical_families") or [])
        has_unavailable = any(str(item.get("state") or "") == "unavailable" for item in critical_families)
        has_single_source = any(str(item.get("state") or "") == "single_source" for item in critical_families)
        runtime_live_state = "ready"
        if has_unavailable:
            runtime_live_state = "blocked"
        elif issues or has_single_source:
            runtime_live_state = "degraded"
        slot_validity = dict(summary.get("slot_validity") or {})
        slot_state = str(slot_validity.get("state") or "")
        state = runtime_live_state
        surface_issues = issues
        if surface_name == "daily_brief" and slot_state:
            state = {
                "slot_valid": "ready",
                "slot_valid_degraded": "degraded",
                "slot_failed": "blocked",
            }.get(slot_state, runtime_live_state)
            surface_issues = list(slot_validity.get("issues") or issues)
        surfaces.append(
            {
                "surface_name": surface_name,
                "state": state,
                "issue_count": len(surface_issues),
                "issues": surface_issues,
                "governance_status": governance.get("status"),
                "current_count": governance.get("current_count"),
                "stale_count": governance.get("stale_count"),
                "gap_count": governance.get("gap_count"),
                "active_targets": payload.get("active_targets"),
                "slot_state": slot_state or None,
                "slot_issues": slot_validity.get("issues"),
                "slot_key": slot_validity.get("slot_key"),
                "slot_label": slot_validity.get("slot_label"),
                "slot_started_at": slot_validity.get("slot_started_at"),
                "slot_ends_at": slot_validity.get("slot_ends_at"),
                "runtime_live_state": runtime_live_state,
                "runtime_live_issue_count": int(summary.get("surface_issue_count") or 0),
                "runtime_live_issues": issues,
            }
        )
    return {
        "generated_at": _now_iso(),
        "surfaces": surfaces,
        "summary": {
            "ready_count": sum(1 for item in surfaces if item["state"] == "ready"),
            "degraded_count": sum(1 for item in surfaces if item["state"] == "degraded"),
            "blocked_count": sum(1 for item in surfaces if item["state"] == "blocked"),
        },
    }


def build_conflict_payload(conn: sqlite3.Connection, settings: Settings) -> dict[str, Any]:
    activation_report = build_provider_activation_report(conn)
    family_conflicts: list[dict[str, Any]] = []
    for family_name in DATA_FAMILY_OWNERSHIP:
        endpoint_family = _endpoint_family_name(family_name)
        family_conflicts.extend(_family_conflicts(activation_report, family_name, _snapshot_items(conn, endpoint_family=endpoint_family)))

    surface_readiness = build_surface_readiness_payload(conn, settings)
    surface_conflicts: list[dict[str, Any]] = []
    for surface in list(surface_readiness.get("surfaces") or []):
        if str(surface.get("state") or "") == "ready":
            continue
        surface_conflicts.append(
            {
                "conflict_type": "surface_not_ready",
                "family_name": None,
                "severity": "high" if surface.get("state") == "blocked" else "medium",
                "message": f"{surface.get('surface_name')} is {surface.get('state')}.",
                "surface_name": surface.get("surface_name"),
                "issues": surface.get("issues"),
            }
        )

    candidate_field_conflicts = _candidate_field_conflicts(conn)
    conflicts = family_conflicts + surface_conflicts + candidate_field_conflicts

    return {
        "generated_at": _now_iso(),
        "conflict_count": len(conflicts),
        "candidate_field_conflict_count": len(candidate_field_conflicts),
        "family_conflicts": family_conflicts,
        "surface_conflicts": surface_conflicts,
        "field_conflicts": candidate_field_conflicts,
        "conflicts": conflicts,
    }
