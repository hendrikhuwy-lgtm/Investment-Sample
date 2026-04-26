from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


_RUNTIME_TRUTH: dict[str, dict[str, Any]] = {}


def reset_runtime_truth() -> None:
    _RUNTIME_TRUTH.clear()


def _default_provenance_strength(*, live_or_cache: str, usable_truth: bool, semantic_grade: str | None) -> str:
    if not usable_truth:
        return "degraded"
    grade = str(semantic_grade or "")
    if "derived" in grade or "proxy" in grade:
        return "derived_or_proxy"
    if str(live_or_cache) == "live":
        return "live_authoritative"
    if str(live_or_cache) == "cache":
        return "cache_continuity"
    return "fallback"


def record_runtime_truth(
    *,
    source_id: str,
    source_family: str,
    field_name: str,
    symbol_or_entity: str,
    provider_used: str | None,
    path_used: str,
    live_or_cache: str,
    usable_truth: bool,
    freshness: str | None,
    insufficiency_reason: str | None,
    semantic_grade: str | None,
    provenance_strength: str | None = None,
    configured: bool = True,
    investor_surface: str | None = None,
    status_reason: str | None = None,
    attempt_succeeded: bool | None = None,
) -> None:
    now_iso = _now_iso()
    row = _RUNTIME_TRUTH.setdefault(
        str(source_id),
        {
            "source_id": str(source_id),
            "source_family": str(source_family),
            "configured": bool(configured),
            "last_attempted": None,
            "last_succeeded": None,
            "last_usable_payload": None,
            "last_investor_facing_use": None,
            "current_status_reason": "unseen",
            "latest_trace": None,
        },
    )
    row["configured"] = bool(row.get("configured")) or bool(configured)
    row["source_family"] = str(source_family)
    row["last_attempted"] = now_iso
    if attempt_succeeded or (attempt_succeeded is None and usable_truth):
        row["last_succeeded"] = now_iso
    if usable_truth:
        row["last_usable_payload"] = now_iso
    if investor_surface:
        row["last_investor_facing_use"] = now_iso
        row["last_investor_surface"] = str(investor_surface)
    row["current_status_reason"] = (
        str(status_reason or insufficiency_reason or ("usable_truth" if usable_truth else "insufficient_truth"))
    )
    resolved_provenance_strength = str(
        provenance_strength
        or _default_provenance_strength(
            live_or_cache=live_or_cache,
            usable_truth=usable_truth,
            semantic_grade=semantic_grade,
        )
    )
    row["last_provenance_strength"] = resolved_provenance_strength
    row["latest_trace"] = {
        "field_name": str(field_name),
        "symbol_or_entity": str(symbol_or_entity),
        "provider_used": str(provider_used or "") or None,
        "path_used": str(path_used),
        "live_or_cache": str(live_or_cache),
        "usable_truth": bool(usable_truth),
        "freshness": str(freshness or "") or None,
        "insufficiency_reason": str(insufficiency_reason or "") or None,
        "semantic_grade": str(semantic_grade or "") or None,
        "provenance_strength": resolved_provenance_strength,
    }


def record_investor_use(*, source_id: str, investor_surface: str) -> None:
    row = _RUNTIME_TRUTH.setdefault(
        str(source_id),
        {
            "source_id": str(source_id),
            "source_family": str(source_id),
            "configured": True,
            "last_attempted": None,
            "last_succeeded": None,
            "last_usable_payload": None,
            "last_investor_facing_use": None,
            "current_status_reason": "unseen",
            "latest_trace": None,
        },
    )
    row["last_investor_facing_use"] = _now_iso()
    row["last_investor_surface"] = str(investor_surface)


def runtime_truth_rows(*, default_sources: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    rows = {key: deepcopy(value) for key, value in _RUNTIME_TRUTH.items()}
    for item in list(default_sources or []):
        source_id = str(item.get("source_id") or "").strip()
        if not source_id or source_id in rows:
            continue
        rows[source_id] = {
            **dict(item),
            "source_id": source_id,
            "source_family": str(item.get("source_family") or source_id),
            "configured": bool(item.get("configured", True)),
            "last_attempted": None,
            "last_succeeded": None,
            "last_usable_payload": None,
            "last_investor_facing_use": None,
            "current_status_reason": str(item.get("current_status_reason") or "unseen"),
            "latest_trace": None,
        }
    return sorted(rows.values(), key=lambda item: str(item.get("source_id") or ""))
