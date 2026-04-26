from __future__ import annotations

from typing import Any, Literal


ContractStateKind = Literal["ready", "degraded", "empty", "blocked"]


def surface_state(
    state: ContractStateKind,
    *,
    reason_codes: list[str] | None = None,
    summary: str,
) -> dict[str, Any]:
    return {
        "state": state,
        "reason_codes": list(reason_codes or []),
        "summary": summary,
    }


def section_state(
    state: ContractStateKind,
    *,
    reason_code: str,
    reason_text: str,
) -> dict[str, str]:
    return {
        "state": state,
        "reason_code": reason_code,
        "reason_text": reason_text,
    }


def ready_section() -> dict[str, str]:
    return section_state("ready", reason_code="ready", reason_text="Section is fully populated.")


def degraded_section(reason_code: str, reason_text: str) -> dict[str, str]:
    return section_state("degraded", reason_code=reason_code, reason_text=reason_text)


def empty_section(reason_code: str, reason_text: str) -> dict[str, str]:
    return section_state("empty", reason_code=reason_code, reason_text=reason_text)


def runtime_provenance(
    *,
    source_family: str | None,
    provider_used: str | None = None,
    path_used: str | None = None,
    provider_execution: dict[str, Any] | None = None,
    truth_envelope: dict[str, Any] | None = None,
    freshness: str | None = None,
    authority_kind: str | None = None,
    provenance_strength: str | None = None,
    insufficiency_reason: str | None = None,
    derived_or_proxy: bool | None = None,
) -> dict[str, Any]:
    execution = dict(provider_execution or {})
    envelope = dict(truth_envelope or {})
    acquisition_mode = str(envelope.get("acquisition_mode") or "").strip() or None
    resolved_provider = str(provider_used or execution.get("provider_name") or "").strip() or None
    resolved_path = str(path_used or execution.get("path_used") or "").strip() or None
    resolved_live_or_cache = str(execution.get("live_or_cache") or acquisition_mode or "").strip() or None
    if resolved_live_or_cache is None and resolved_path:
        if "cache" in resolved_path:
            resolved_live_or_cache = "cache"
        elif "fallback" in resolved_path:
            resolved_live_or_cache = "fallback"
        else:
            resolved_live_or_cache = "live"
    resolved_authority = str(authority_kind or execution.get("authority_level") or "").strip() or None
    resolved_freshness = str(freshness or execution.get("freshness_class") or "").strip() or None
    resolved_usable_truth = execution.get("usable_truth")
    resolved_sufficiency_state = str(execution.get("sufficiency_state") or "").strip() or None
    resolved_data_mode = str(execution.get("data_mode") or "").strip() or None
    resolved_derived = bool(
        derived_or_proxy
        if derived_or_proxy is not None
        else resolved_authority == "derived"
        or "proxy" in str(provenance_strength or "")
        or "proxy" in str(execution.get("semantic_grade") or "")
        or "proxy" in str(resolved_path or "")
    )
    resolved_strength = str(
        provenance_strength
        or execution.get("provenance_strength")
        or (
            "derived_or_proxy"
            if resolved_derived
            else resolved_authority
            or ("cache_continuity" if resolved_live_or_cache == "cache" else "live_authoritative")
        )
    )
    resolved_reason = str(insufficiency_reason or execution.get("insufficiency_reason") or envelope.get("degradation_reason") or "").strip() or None
    return {
        "source_family": str(source_family or "").strip() or None,
        "provider_used": resolved_provider,
        "path_used": resolved_path,
        "live_or_cache": resolved_live_or_cache,
        "freshness": resolved_freshness,
        "provenance_strength": resolved_strength,
        "source_authority_tier": resolved_strength,
        "derived_or_proxy": resolved_derived,
        "usable_truth": resolved_usable_truth,
        "sufficiency_state": resolved_sufficiency_state,
        "data_mode": resolved_data_mode,
        "authority_level": resolved_authority,
        "observed_at": str(execution.get("observed_at") or envelope.get("as_of_utc") or "").strip() or None,
        "insufficiency_reason": resolved_reason,
        "truth_envelope": envelope or None,
    }
