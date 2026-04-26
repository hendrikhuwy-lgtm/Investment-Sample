from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.services.external_upstreams import build_provider_status_registry
from app.services.provider_budget import (
    ensure_provider_budget_tables,
    list_provider_health,
    peek_provider_budget_state,
)
from app.services.provider_family_success import (
    ensure_provider_family_success_tables,
    list_provider_family_success,
)
from app.services.provider_registry import DATA_FAMILY_OWNERSHIP, PROVIDER_CAPABILITY_MATRIX
from app.services.public_upstream_snapshots import public_upstream_health_summary


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _canonical_family_name(family: str) -> str:
    return {
        "quote_latest": "latest_quote",
        "benchmark_proxy": "benchmark_proxy_history",
        "reference_meta": "etf_reference_metadata",
    }.get(str(family or ""), str(family or ""))


def _family_authority(provider_name: str, family_name: str) -> str:
    ownership = dict(DATA_FAMILY_OWNERSHIP.get(family_name) or {})
    if provider_name in {
        str(ownership.get("primary_provider") or ""),
        str(ownership.get("secondary_provider") or ""),
    }:
        return "truth_grade"
    return "support_grade"


def _configured_family_names(provider_name: str) -> list[str]:
    families = dict(dict(PROVIDER_CAPABILITY_MATRIX.get(provider_name) or {}).get("families") or {})
    normalized = [_canonical_family_name(family_name) for family_name in families]
    return sorted(dict.fromkeys([name for name in normalized if name]))


def _activation_state(
    *,
    status: str,
    configured: bool,
    requires_api_key: bool,
    active_families: list[str],
    last_error: str,
) -> str:
    normalized_status = str(status or "")
    if active_families:
        return "active"
    if normalized_status.startswith("planned") or normalized_status == "not_installed_optional":
        return "non_viable"
    if requires_api_key and not configured:
        return "unconfigured"
    if configured and last_error:
        return "degraded"
    if configured:
        return "configured"
    return "non_viable"


def build_provider_activation_report(conn: sqlite3.Connection) -> dict[str, Any]:
    ensure_provider_budget_tables(conn)
    ensure_provider_family_success_tables(conn)

    status_registry = [dict(item) for item in build_provider_status_registry()]
    health_by_provider = {
        str(item.get("provider_name") or ""): dict(item)
        for item in list_provider_health(conn)
    }
    public_health_by_provider = {
        str(item.get("provider_key") or ""): dict(item)
        for item in list(public_upstream_health_summary(conn).get("providers") or [])
    }
    family_success_rows = [dict(item) for item in list_provider_family_success(conn)]
    family_rows_by_provider: dict[str, list[dict[str, Any]]] = {}
    for row in family_success_rows:
        family_rows_by_provider.setdefault(str(row.get("provider_name") or ""), []).append(row)

    providers: list[dict[str, Any]] = []
    provider_index: dict[str, dict[str, Any]] = {}

    for entry in status_registry:
        provider_name = str(entry.get("provider_key") or "")
        family_rows = list(family_rows_by_provider.get(provider_name) or [])
        health = health_by_provider.get(provider_name) or {}
        public_health = public_health_by_provider.get(provider_name) or {}
        budget_state = (
            peek_provider_budget_state(conn, provider_name)
            if provider_name in PROVIDER_CAPABILITY_MATRIX
            else {"mode": "normal"}
        )

        configured_families = _configured_family_names(provider_name)
        public_families = [str(item).strip() for item in list(public_health.get("families") or []) if str(item).strip()]
        reported_families = [str(row.get("family_name") or "") for row in family_rows if str(row.get("family_name") or "")]
        all_families = sorted(dict.fromkeys(configured_families + public_families + reported_families))

        active_rows = [
            row
            for row in family_rows
            if str(row.get("current_tier") or "") in {"primary_active", "secondary_active"}
            or int(row.get("current_snapshot_count") or 0) > 0
        ]
        active_families = sorted(
            dict.fromkeys([str(row.get("family_name") or "") for row in active_rows if str(row.get("family_name") or "")])
        )
        public_active_families = [
            family_name
            for family_name in public_families
            if public_health.get("last_successful_fetch_at")
            and not public_health.get("error_state")
            and any(str(state) in {"current", "aging", "expected_lag"} for state in list(public_health.get("freshness_states") or []))
        ]
        active_families = sorted(dict.fromkeys(active_families + public_active_families))
        inactive_families = [family_name for family_name in all_families if family_name not in set(active_families)]
        truth_grade_active_families = [
            family_name for family_name in active_families if _family_authority(provider_name, family_name) == "truth_grade"
        ]
        support_grade_active_families = [
            family_name for family_name in active_families if family_name not in set(truth_grade_active_families)
        ]
        last_error = str(health.get("last_error") or public_health.get("error_state") or "")
        payload = {
            "provider_name": provider_name,
            "label": str(entry.get("label") or provider_name),
            "status": str(entry.get("status") or "unknown"),
            "configured": bool(entry.get("configured")),
            "requires_api_key": bool(entry.get("requires_api_key")),
            "free_access": bool(entry.get("free_access")),
            "detail": str(entry.get("detail") or ""),
            "activation_env": entry.get("activation_env"),
            "activation_state": _activation_state(
                status=str(entry.get("status") or ""),
                configured=bool(entry.get("configured")),
                requires_api_key=bool(entry.get("requires_api_key")),
                active_families=active_families,
                last_error=last_error,
            ),
            "current_mode": str(budget_state.get("mode") or "normal"),
            "failure_streak": int(health.get("failure_streak") or 0),
            "last_error": last_error or None,
            "last_successful_fetch_at": health.get("last_successful_fetch_at") or public_health.get("last_successful_fetch_at"),
            "latest_observed_at": public_health.get("latest_observed_at"),
            "snapshot_count": int(public_health.get("snapshot_count") or 0),
            "active_families": active_families,
            "inactive_families": inactive_families,
            "truth_grade_active_families": truth_grade_active_families,
            "support_grade_active_families": support_grade_active_families,
            "canonical_truth_enabled": bool(truth_grade_active_families),
        }
        providers.append(payload)
        provider_index[provider_name] = payload

    family_coverage: dict[str, dict[str, Any]] = {}
    for family_name, ownership in DATA_FAMILY_OWNERSHIP.items():
        candidate_providers = [
            str(name).strip()
            for name in [
                ownership.get("primary_provider"),
                ownership.get("secondary_provider"),
                *list(ownership.get("public_fallback") or []),
                *list(ownership.get("legacy_fallback") or []),
            ]
            if str(name).strip()
        ]
        configured_providers = [
            provider_name
            for provider_name in candidate_providers
            if bool((provider_index.get(provider_name) or {}).get("configured"))
            or not bool((provider_index.get(provider_name) or {}).get("requires_api_key"))
        ]
        active_providers = [
            provider_name
            for provider_name in candidate_providers
            if family_name in set(list((provider_index.get(provider_name) or {}).get("active_families") or []))
        ]
        truth_grade_active = [
            provider_name
            for provider_name in active_providers
            if _family_authority(provider_name, family_name) == "truth_grade"
        ]
        authority = "truth_grade" if truth_grade_active else "support_grade" if active_providers else "unknown"
        coverage_state = (
            "healthy"
            if truth_grade_active
            else "partial"
            if active_providers
            else "configured_no_current_data"
            if configured_providers
            else "missing"
        )
        family_coverage[family_name] = {
            "family_name": family_name,
            "authority": authority,
            "coverage_state": coverage_state,
            "primary_provider": ownership.get("primary_provider"),
            "secondary_provider": ownership.get("secondary_provider"),
            "configured_provider_count": len(configured_providers),
            "active_provider_count": len(active_providers),
            "configured_providers": configured_providers,
            "active_providers": active_providers,
        }

    summary = {
        "generated_at": _now_iso(),
        "provider_count": len(providers),
        "active_provider_count": sum(1 for item in providers if str(item.get("activation_state") or "") == "active"),
        "configured_provider_count": sum(1 for item in providers if bool(item.get("configured"))),
        "canonical_truth_provider_count": sum(1 for item in providers if bool(item.get("canonical_truth_enabled"))),
        "degraded_provider_count": sum(1 for item in providers if str(item.get("activation_state") or "") == "degraded"),
        "unconfigured_provider_count": sum(1 for item in providers if str(item.get("activation_state") or "") == "unconfigured"),
    }
    return {
        "generated_at": summary["generated_at"],
        "summary": summary,
        "providers": providers,
        "source_family_coverage": family_coverage,
    }
