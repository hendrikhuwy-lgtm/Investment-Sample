from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from app.models.db import connect
from app.config import get_db_path, get_settings


router = APIRouter(prefix="/api/v2", tags=["v2"])

# ---------------------------------------------------------------------------
# Two-level surface cache: memory (sub-ms) → DB snapshot (ms) → live build
# ---------------------------------------------------------------------------
# Level 1 — in-process dict: lives for the server lifetime, sub-millisecond.
# Level 2 — SQLite snapshot: survives server restarts, ~5ms read.
# Level 3 — live build: expensive, done once then cached at both levels.
# Background thread keeps the cache warm without blocking any request.

_MEM_CACHE: dict[str, tuple[datetime, dict[str, Any]]] = {}  # key → (stored_at, contract)
_MEM_LOCK = threading.Lock()
_REBUILD_IN_FLIGHT: set[str] = set()
_REBUILD_LOCK = threading.Lock()
_CHINA_TZ = ZoneInfo("Asia/Shanghai")


def _mem_get_any(key: str) -> tuple[dict[str, Any], datetime] | None:
    """Return (contract, stored_at) regardless of age, or None if absent."""
    with _MEM_LOCK:
        entry = _MEM_CACHE.get(key)
    return entry  # (stored_at, contract) or None


def _mem_set(key: str, contract: dict[str, Any]) -> None:
    with _MEM_LOCK:
        _MEM_CACHE[key] = (datetime.now(UTC), contract)


def _contract_generated_at(contract: dict[str, Any], fallback: datetime) -> datetime:
    raw = str(contract.get("generated_at") or "").strip()
    if not raw:
        return fallback
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return fallback
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _contract_effective_time(contract: dict[str, Any], fallback: datetime) -> datetime:
    return _contract_generated_at(contract, fallback)


def _db_should_replace_mem(
    *,
    mem_contract: dict[str, Any],
    mem_stored_at: datetime,
    db_contract: dict[str, Any],
    db_created_at: datetime,
) -> bool:
    mem_effective_time = _contract_effective_time(mem_contract, mem_stored_at)
    db_effective_time = _contract_effective_time(db_contract, db_created_at)
    if db_effective_time > mem_effective_time:
        return True
    if db_effective_time < mem_effective_time:
        return False
    mem_snapshot_id = str(mem_contract.get("surface_snapshot_id") or "").strip()
    db_snapshot_id = str(db_contract.get("surface_snapshot_id") or "").strip()
    return bool(db_snapshot_id and db_snapshot_id != mem_snapshot_id and db_created_at > mem_stored_at)


def _db_get_any(surface_id: str, object_id: str) -> tuple[dict[str, Any], datetime] | None:
    """Return (contract, created_at) from DB regardless of age, or None."""
    try:
        from app.v2.storage.surface_snapshot_store import latest_surface_snapshot
        cached = latest_surface_snapshot(surface_id=surface_id, object_id=object_id)
        if cached is None:
            return None
        contract = dict(cached.get("contract") or {})
        snapshot_id = str(cached.get("snapshot_id") or "").strip()
        if snapshot_id and not str(contract.get("surface_snapshot_id") or "").strip():
            contract["surface_snapshot_id"] = snapshot_id
        created_raw = cached.get("created_at", "")
        created = datetime.fromisoformat(created_raw)
        if created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
        return contract, created
    except Exception:
        return None


def _fire_rebuild(key: str, fn: Callable[[], None]) -> None:
    """Kick off fn() in a daemon thread, one concurrent rebuild per key."""
    with _REBUILD_LOCK:
        if key in _REBUILD_IN_FLIGHT:
            return
        _REBUILD_IN_FLIGHT.add(key)

    def _worker() -> None:
        try:
            fn()
        except Exception:
            # Background warmups must never leak into the interactive request path.
            pass
        finally:
            with _REBUILD_LOCK:
                _REBUILD_IN_FLIGHT.discard(key)

    threading.Thread(target=_worker, daemon=True).start()


def _surface_slot_boundary(surface_id: str, *, now: datetime) -> datetime | None:
    if str(surface_id or "") != "daily_brief":
        return None
    local = now.astimezone(_CHINA_TZ)
    if local.hour >= 20:
        slot_local = local.replace(hour=20, minute=0, second=0, microsecond=0)
    elif local.hour >= 8:
        slot_local = local.replace(hour=8, minute=0, second=0, microsecond=0)
    else:
        previous = local - timedelta(days=1)
        slot_local = previous.replace(hour=20, minute=0, second=0, microsecond=0)
    return slot_local.astimezone(UTC)


def _serve_cached(
    key: str,
    surface_id: str,
    object_id: str,
    max_seconds: int,
    rebuild_fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """
    Always serve from cache if any data exists (regardless of TTL).
    Fire a background rebuild when data is stale.
    Only build synchronously on true cold start (no cached data at all).
    """
    now = datetime.now(UTC)
    slot_boundary = _surface_slot_boundary(surface_id, now=now)

    # Level 1 — memory (sub-ms)
    mem_entry = _mem_get_any(key)
    if mem_entry is not None:
        stored_at, contract = mem_entry
        db_entry = _db_get_any(surface_id, object_id)
        if db_entry is not None:
            db_contract, created_at = db_entry
            if _db_should_replace_mem(
                mem_contract=contract,
                mem_stored_at=stored_at,
                db_contract=db_contract,
                db_created_at=created_at,
            ):
                _mem_set(key, db_contract)
                db_age = (now - created_at).total_seconds()
                if db_age > max_seconds:
                    _fire_rebuild(key, lambda: rebuild_fn())
                return db_contract
        contract_time = _contract_effective_time(contract, stored_at)
        if slot_boundary is not None and contract_time < slot_boundary:
            return rebuild_fn()
        age = (now - stored_at).total_seconds()
        if age > max_seconds:
            _fire_rebuild(key, lambda: rebuild_fn())
        return contract

    # Level 2 — DB snapshot (ms)
    db_entry = _db_get_any(surface_id, object_id)
    if db_entry is not None:
        contract, created_at = db_entry
        contract_time = _contract_effective_time(contract, created_at)
        if slot_boundary is not None and contract_time < slot_boundary:
            return rebuild_fn()
        _mem_set(key, contract)  # promote to memory
        age = (now - created_at).total_seconds()
        if age > max_seconds:
            _fire_rebuild(key, lambda: rebuild_fn())
        return contract

    # Level 3 — cold start: build synchronously, cache result
    return rebuild_fn()


def _cache_state_payload(
    *,
    state: str,
    stored_at: datetime,
    max_seconds: int,
    stale: bool,
    revalidating: bool,
) -> dict[str, Any]:
    return {
        "state": state,
        "stale": stale,
        "revalidating": revalidating,
        "cached_at": stored_at.isoformat(),
        "max_age_seconds": max_seconds,
        "summary": (
            "Served from live build."
            if state == "live_build"
            else "Served from memory cache."
            if state == "memory"
            else "Served from persisted surface snapshot."
        ),
    }


def _serve_cached_with_state(
    key: str,
    surface_id: str,
    object_id: str,
    max_seconds: int,
    rebuild_fn: Callable[[], dict[str, Any]],
    *,
    allow_background_rebuild: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    now = datetime.now(UTC)
    mem_entry = _mem_get_any(key)
    if mem_entry is not None:
        stored_at, contract = mem_entry
        db_entry = _db_get_any(surface_id, object_id)
        if db_entry is not None:
            db_contract, created_at = db_entry
            if _db_should_replace_mem(
                mem_contract=contract,
                mem_stored_at=stored_at,
                db_contract=db_contract,
                db_created_at=created_at,
            ):
                _mem_set(key, db_contract)
                db_age = (now - created_at).total_seconds()
                db_stale = db_age > max_seconds
                if db_stale and allow_background_rebuild:
                    _fire_rebuild(key, lambda: rebuild_fn())
                return db_contract, _cache_state_payload(
                    state="snapshot",
                    stored_at=created_at,
                    max_seconds=max_seconds,
                    stale=db_stale,
                    revalidating=db_stale and allow_background_rebuild,
                )
        age = (now - stored_at).total_seconds()
        stale = age > max_seconds
        if stale and allow_background_rebuild:
            _fire_rebuild(key, lambda: rebuild_fn())
        return contract, _cache_state_payload(
            state="memory",
            stored_at=stored_at,
            max_seconds=max_seconds,
            stale=stale,
            revalidating=stale and allow_background_rebuild,
        )

    db_entry = _db_get_any(surface_id, object_id)
    if db_entry is not None:
        contract, created_at = db_entry
        _mem_set(key, contract)
        age = (now - created_at).total_seconds()
        stale = age > max_seconds
        if stale and allow_background_rebuild:
            _fire_rebuild(key, lambda: rebuild_fn())
        return contract, _cache_state_payload(
            state="snapshot",
            stored_at=created_at,
            max_seconds=max_seconds,
            stale=stale,
            revalidating=stale and allow_background_rebuild,
        )

    contract = rebuild_fn()
    built_at = datetime.now(UTC)
    return contract, _cache_state_payload(
        state="live_build",
        stored_at=built_at,
        max_seconds=max_seconds,
        stale=False,
        revalidating=False,
    )


def _annotate_report_cache_metadata(contract: dict[str, Any], cache_state: dict[str, Any]) -> dict[str, Any]:
    generated_at = str(contract.get("generated_at") or datetime.now(UTC).isoformat())
    route_state = str(cache_state.get("state") or "live_build")
    loading_hint = dict(contract.get("report_loading_hint") or {})
    loading_hint["route_cache_state"] = route_state
    return {
        **contract,
        "route_cache_state": cache_state,
        "report_cache_state": route_state,
        "report_generated_at": generated_at,
        "report_source_snapshot_at": str(cache_state.get("cached_at") or generated_at),
        "report_loading_hint": loading_hint,
    }


def _report_binding_cache_key(
    candidate_id: str,
    *,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None = None,
    source_contract_version: str | None = None,
) -> str:
    sleeve_part = str(sleeve_key or "").strip() or "unbound_sleeve"
    snapshot_part = str(source_snapshot_id or "").strip() or "unbound"
    version_part = str(source_contract_version or "").strip() or "unknown"
    return f"candidate_report::{candidate_id}::{sleeve_part}::{snapshot_part}::{version_part}"


def _report_binding_payload(
    *,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None,
    source_generated_at: str | None,
    source_contract_version: str | None,
) -> dict[str, str | None]:
    return {
        "sleeve_key": str(sleeve_key or "").strip() or None,
        "source_snapshot_id": str(source_snapshot_id or "").strip() or None,
        "source_generated_at": str(source_generated_at or "").strip() or None,
        "source_contract_version": str(source_contract_version or "").strip() or None,
    }


def _report_cache_state(
    *,
    state: str,
    stale: bool = False,
    revalidating: bool = False,
    summary: str,
    max_seconds: int = 300,
    cached_at: datetime | None = None,
) -> dict[str, Any]:
    stored_at = cached_at or datetime.now(UTC)
    return {
        "state": state,
        "stale": stale,
        "revalidating": revalidating,
        "cached_at": stored_at.isoformat(),
        "max_age_seconds": max_seconds,
        "summary": summary,
    }


def _report_contract_matches_binding(
    contract: dict[str, Any],
    *,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None,
    source_generated_at: str | None,
    source_contract_version: str | None,
    exact: bool = True,
) -> bool:
    requested_snapshot = str(source_snapshot_id or "").strip()
    requested_sleeve = str(sleeve_key or "").strip()
    requested_version = str(source_contract_version or "").strip()
    requested_generated_at = str(source_generated_at or "").strip()
    actual_snapshot = str(contract.get("bound_source_snapshot_id") or "").strip()
    actual_sleeve = str(contract.get("sleeve_key") or contract.get("sleeve_id") or "").strip()
    actual_version = str(contract.get("source_contract_version") or "").strip()
    actual_generated_at = str(contract.get("bound_source_generated_at") or "").strip()
    if requested_sleeve:
        normalized_requested_sleeve = requested_sleeve.removeprefix("sleeve_")
        normalized_actual_sleeve = actual_sleeve.removeprefix("sleeve_")
        if normalized_actual_sleeve != normalized_requested_sleeve:
            return False
    if requested_snapshot and actual_snapshot != requested_snapshot:
        return False
    if exact and not requested_snapshot and actual_snapshot:
        return False
    if requested_version and actual_version != requested_version:
        return False
    if exact and not requested_version and actual_version:
        return False
    if requested_generated_at and actual_generated_at and actual_generated_at != requested_generated_at:
        return False
    return True


def _created_at_from_snapshot(snapshot: dict[str, Any]) -> datetime:
    raw = str(snapshot.get("created_at") or snapshot.get("generated_at") or "").strip()
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        parsed = datetime.now(UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _promote_bound_report_to_memory(
    contract: dict[str, Any],
    *,
    stable_candidate_id: str,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None,
    source_contract_version: str | None,
) -> None:
    _mem_set(
        _report_binding_cache_key(
            stable_candidate_id,
            sleeve_key=sleeve_key,
            source_snapshot_id=source_snapshot_id,
            source_contract_version=source_contract_version,
        ),
        contract,
    )


def _cached_report_from_memory(
    *,
    stable_candidate_id: str,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None,
    source_generated_at: str | None,
    source_contract_version: str | None,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    key = _report_binding_cache_key(
        stable_candidate_id,
        sleeve_key=sleeve_key,
        source_snapshot_id=source_snapshot_id,
        source_contract_version=source_contract_version,
    )
    mem_entry = _mem_get_any(key)
    if mem_entry is None:
        return None
    stored_at, contract = mem_entry
    if not _report_contract_matches_binding(
        contract,
        sleeve_key=sleeve_key,
        source_snapshot_id=source_snapshot_id,
        source_generated_at=source_generated_at,
        source_contract_version=source_contract_version,
    ):
        return None
    cache_state = _report_cache_state(
        state="memory",
        cached_at=stored_at,
        summary="Served from source-bound memory cache.",
    )
    return contract, cache_state


def _candidate_report_snapshots(stable_candidate_id: str, *, limit: int = 80) -> list[dict[str, Any]]:
    try:
        from app.v2.storage.surface_snapshot_store import list_surface_snapshots

        return list_surface_snapshots(surface_id="candidate_report", object_id=stable_candidate_id, limit=limit)
    except Exception:
        return []


def _cached_report_from_snapshot(
    *,
    stable_candidate_id: str,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None,
    source_generated_at: str | None,
    source_contract_version: str | None,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    for snapshot in _candidate_report_snapshots(stable_candidate_id):
        contract = dict(snapshot.get("contract") or {})
        if not _report_contract_matches_binding(
            contract,
            sleeve_key=sleeve_key,
            source_snapshot_id=source_snapshot_id,
            source_generated_at=source_generated_at,
            source_contract_version=source_contract_version,
        ):
            continue
        snapshot_id = str(snapshot.get("snapshot_id") or "").strip()
        if snapshot_id and not str(contract.get("surface_snapshot_id") or "").strip():
            contract["surface_snapshot_id"] = snapshot_id
        cache_state = _report_cache_state(
            state="snapshot",
            cached_at=_created_at_from_snapshot(snapshot),
            summary="Served from exact source-bound report snapshot.",
        )
        _promote_bound_report_to_memory(
            contract,
            stable_candidate_id=stable_candidate_id,
            sleeve_key=sleeve_key,
            source_snapshot_id=source_snapshot_id,
            source_contract_version=source_contract_version,
        )
        return contract, cache_state
    return None


def _embedded_report_from_explorer_snapshot(
    *,
    stable_candidate_id: str,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None,
    source_generated_at: str | None,
    source_contract_version: str | None,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    if not source_snapshot_id:
        return None
    try:
        from app.v2.storage.surface_snapshot_store import get_surface_snapshot

        explorer_snapshot = get_surface_snapshot(source_snapshot_id)
    except Exception:
        return None
    if explorer_snapshot is None:
        return None
    contract = dict(explorer_snapshot.get("contract") or {})
    for sleeve in list(contract.get("sleeves") or []):
        current_sleeve = str(dict(sleeve or {}).get("sleeve_key") or dict(sleeve or {}).get("sleeve_id") or "").strip()
        requested_sleeve = str(sleeve_key or "").strip()
        requested_sleeve_norm = requested_sleeve[7:] if requested_sleeve.startswith("sleeve_") else requested_sleeve
        current_sleeve_norm = current_sleeve[7:] if current_sleeve.startswith("sleeve_") else current_sleeve
        if requested_sleeve_norm and current_sleeve_norm and current_sleeve_norm != requested_sleeve_norm:
            continue
        for candidate in list(dict(sleeve or {}).get("candidates") or []):
            candidate_row = dict(candidate or {})
            if str(candidate_row.get("candidate_id") or "") != stable_candidate_id:
                continue
            report_snapshot = dict(candidate_row.get("report_snapshot") or {})
            if not report_snapshot:
                return None
            report_snapshot.setdefault("bound_source_snapshot_id", source_snapshot_id)
            report_snapshot.setdefault("bound_source_generated_at", source_generated_at)
            report_snapshot.setdefault("source_contract_version", source_contract_version)
            if not _report_contract_matches_binding(
                report_snapshot,
                source_snapshot_id=source_snapshot_id,
                source_generated_at=source_generated_at,
                source_contract_version=source_contract_version,
            ):
                return None
            cache_state = _report_cache_state(
                state="embedded_snapshot",
                cached_at=_created_at_from_snapshot(explorer_snapshot),
                summary="Served from embedded Explorer report snapshot.",
            )
            _promote_bound_report_to_memory(
                report_snapshot,
                stable_candidate_id=stable_candidate_id,
                sleeve_key=sleeve_key,
                source_snapshot_id=source_snapshot_id,
                source_contract_version=source_contract_version,
            )
            return report_snapshot, cache_state
    return None


def _latest_stale_candidate_report(
    *,
    stable_candidate_id: str,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None,
    source_contract_version: str | None,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    for snapshot in _candidate_report_snapshots(stable_candidate_id, limit=20):
        contract = dict(snapshot.get("contract") or {})
        if str(contract.get("surface_id") or "") != "candidate_report":
            continue
        requested_sleeve = str(sleeve_key or "").strip()
        actual_sleeve = str(contract.get("sleeve_key") or contract.get("sleeve_id") or "").strip()
        requested_sleeve_norm = requested_sleeve[7:] if requested_sleeve.startswith("sleeve_") else requested_sleeve
        actual_sleeve_norm = actual_sleeve[7:] if actual_sleeve.startswith("sleeve_") else actual_sleeve
        if requested_sleeve_norm and actual_sleeve_norm and actual_sleeve_norm != requested_sleeve_norm:
            continue
        if source_contract_version and str(contract.get("source_contract_version") or "").strip() not in {
            "",
            str(source_contract_version).strip(),
        }:
            continue
        contract["status"] = "stale_cached"
        contract["report_cache_state"] = "stale_cached"
        contract["binding_state"] = "stale_snapshot_fallback"
        contract["requested_source_snapshot_id"] = str(source_snapshot_id or "").strip() or None
        contract["requested_source_contract_version"] = str(source_contract_version or "").strip() or None
        cache_state = _report_cache_state(
            state="stale_cached",
            stale=True,
            revalidating=True,
            cached_at=_created_at_from_snapshot(snapshot),
            summary="Served from a stale candidate report while the source-bound report is prepared.",
        )
        return contract, cache_state
    return None


def _source_snapshot_exists(source_snapshot_id: str | None) -> bool:
    if not source_snapshot_id:
        return True
    try:
        from app.v2.storage.surface_snapshot_store import get_surface_snapshot

        snapshot = get_surface_snapshot(source_snapshot_id)
        return snapshot is not None
    except Exception:
        return False


def _build_pending_report_payload(
    *,
    stable_candidate_id: str,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None,
    source_generated_at: str | None,
    source_contract_version: str | None,
    status: str = "report_pending",
    reason: str = "building_bound_report",
    message: str = "Report is being prepared from the selected Explorer snapshot.",
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    return {
        "status": status,
        "surface_id": "candidate_report",
        "candidate_id": stable_candidate_id,
        "sleeve_key": str(sleeve_key or "").strip() or None,
        "generated_at": now,
        "source_binding": _report_binding_payload(
            sleeve_key=sleeve_key,
            source_snapshot_id=source_snapshot_id,
            source_generated_at=source_generated_at,
            source_contract_version=source_contract_version,
        ),
        "reason": reason,
        "message": message,
        "retry_after_ms": 1500 if status == "report_pending" else None,
        "report_cache_state": status,
        "binding_state": "pending" if status == "report_pending" else "unavailable",
        "route_cache_state": _report_cache_state(
            state=status,
            summary=message,
            stale=False,
            revalidating=status == "report_pending",
        ),
        "report_loading_hint": {
            "strategy": "stable_pending",
            "summary": message,
            "route_cache_state": status,
        },
    }


def _kick_candidate_report_build(
    *,
    candidate_id: str,
    stable_candidate_id: str,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None,
    source_generated_at: str | None,
    source_contract_version: str | None,
    report_build_mode: str = "snapshot_only",
) -> None:
    key = _report_binding_cache_key(
        stable_candidate_id,
        sleeve_key=sleeve_key,
        source_snapshot_id=source_snapshot_id,
        source_contract_version=source_contract_version,
    )
    _fire_rebuild(
        key,
        lambda: _build_candidate_report(
            candidate_id,
            sleeve_key=sleeve_key,
            source_snapshot_id=source_snapshot_id,
            source_generated_at=source_generated_at,
            source_contract_version=source_contract_version,
            report_build_mode=report_build_mode,
        ),
    )


def _candidate_report_fast_payload(
    candidate_id: str,
    *,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None,
    source_generated_at: str | None,
    source_contract_version: str | None,
    allow_background_build: bool = True,
    refresh: bool = False,
) -> dict[str, Any]:
    stable_candidate_id = _stable_candidate_id(candidate_id)
    cached = _cached_report_from_memory(
        stable_candidate_id=stable_candidate_id,
        sleeve_key=sleeve_key,
        source_snapshot_id=source_snapshot_id,
        source_generated_at=source_generated_at,
        source_contract_version=source_contract_version,
    )
    if cached is None:
        cached = _cached_report_from_snapshot(
            stable_candidate_id=stable_candidate_id,
            sleeve_key=sleeve_key,
            source_snapshot_id=source_snapshot_id,
            source_generated_at=source_generated_at,
            source_contract_version=source_contract_version,
        )
    if cached is None:
        cached = _embedded_report_from_explorer_snapshot(
            stable_candidate_id=stable_candidate_id,
            sleeve_key=sleeve_key,
            source_snapshot_id=source_snapshot_id,
            source_generated_at=source_generated_at,
            source_contract_version=source_contract_version,
        )
    if cached is not None:
        contract, cache_state = cached
        if refresh and allow_background_build:
            _kick_candidate_report_build(
                candidate_id=candidate_id,
                stable_candidate_id=stable_candidate_id,
                sleeve_key=sleeve_key,
                source_snapshot_id=source_snapshot_id,
                source_generated_at=source_generated_at,
                source_contract_version=source_contract_version,
                report_build_mode="refresh",
            )
            cache_state = {**cache_state, "revalidating": True, "summary": "Served cached report while explicit refresh runs in background."}
        return _annotate_report_cache_metadata(contract, cache_state)

    if source_snapshot_id and not _source_snapshot_exists(source_snapshot_id):
        return _build_pending_report_payload(
            stable_candidate_id=stable_candidate_id,
            sleeve_key=sleeve_key,
            source_snapshot_id=source_snapshot_id,
            source_generated_at=source_generated_at,
            source_contract_version=source_contract_version,
            status="report_unavailable",
            reason="source_snapshot_unavailable",
            message="Report is not available because the selected Explorer snapshot is no longer available.",
        )

    stale = _latest_stale_candidate_report(
        stable_candidate_id=stable_candidate_id,
        sleeve_key=sleeve_key,
        source_snapshot_id=source_snapshot_id,
        source_contract_version=source_contract_version,
    )
    if allow_background_build:
        _kick_candidate_report_build(
            candidate_id=candidate_id,
            stable_candidate_id=stable_candidate_id,
            sleeve_key=sleeve_key,
            source_snapshot_id=source_snapshot_id,
            source_generated_at=source_generated_at,
            source_contract_version=source_contract_version,
            report_build_mode="refresh" if refresh else "snapshot_only",
        )
    if stale is not None:
        contract, cache_state = stale
        return _annotate_report_cache_metadata(contract, cache_state)

    return _build_pending_report_payload(
        stable_candidate_id=stable_candidate_id,
        sleeve_key=sleeve_key,
        source_snapshot_id=source_snapshot_id,
        source_generated_at=source_generated_at,
        source_contract_version=source_contract_version,
    )


class NotebookEntryCreateRequest(BaseModel):
    linked_object_type: str = "candidate"
    linked_object_id: str | None = None
    linked_object_label: str | None = None
    title: str
    thesis: str
    assumptions: str = ""
    invalidation: str = ""
    watch_items: str = ""
    reflections: str = ""
    next_review_date: str | None = None


class NotebookEntryUpdateRequest(BaseModel):
    linked_object_type: str | None = None
    linked_object_id: str | None = None
    linked_object_label: str | None = None
    title: str | None = None
    thesis: str | None = None
    assumptions: str | None = None
    invalidation: str | None = None
    watch_items: str | None = None
    reflections: str | None = None
    next_review_date: str | None = None


class NotebookForecastReferenceCreateRequest(BaseModel):
    forecast_run_id: str
    reference_label: str
    threshold_summary: str | None = None


class EvidenceDocumentCreateRequest(BaseModel):
    linked_object_type: str = "candidate"
    linked_object_id: str | None = None
    linked_object_label: str | None = None
    title: str
    document_type: str = "document"
    url: str | None = None
    retrieved_utc: str | None = None
    freshness_state: str = "stored_valid_context"
    stale: bool = False


class EvidenceClaimCreateRequest(BaseModel):
    object_type: str = "candidate"
    object_id: str | None = None
    object_label: str | None = None
    claim_text: str
    claim_meta: str = ""
    directness: str = "proxy"
    freshness_state: str = "stored_valid_context"


class EvidenceMappingCreateRequest(BaseModel):
    sleeve_label: str
    instrument_label: str
    benchmark_label: str
    baseline_label: str
    directness: str = "bounded"


class EvidenceTaxAssumptionCreateRequest(BaseModel):
    label: str
    value: str


class EvidenceGapCreateRequest(BaseModel):
    object_label: str
    issue_text: str


class PortfolioUploadCreateRequest(BaseModel):
    csv_text: str
    filename: str | None = None
    source_name: str = "manual_csv_upload"
    default_currency: str = "USD"
    default_account_type: str = "broker"
    allow_live_pricing: bool = True


class PortfolioMappingOverrideRequest(BaseModel):
    symbol: str
    sleeve: str


class RecommendationReviewCreateRequest(BaseModel):
    decision_label: str
    review_outcome: str
    notes: str = ""
    what_changed_view: str = ""
    false_positive: bool = False
    false_negative: bool = False
    stale_evidence_miss: bool = False
    overconfident_forecast_support: bool = False


class BlueprintMarketPathRefreshRequest(BaseModel):
    candidate_id: str | None = None
    sleeve_key: str | None = None
    stale_only: bool = False
    verify_symbol_mapping_only: bool = False
    refresh_forecasts: bool = False
    forecast_stale_only: bool = True
    audit_only: bool = False


def _stable_candidate_id(candidate_id: str) -> str:
    from app.v2.donors.instrument_truth import get_instrument_truth

    truth = get_instrument_truth(candidate_id)
    return f"candidate_{truth.instrument_id}"


def _connection() -> sqlite3.Connection:
    return connect(get_db_path())


def _normalize_surface_name(surface: str | None) -> str | None:
    if surface is None:
        return None
    normalized = str(surface).strip().lower().replace("-", "_")
    if normalized in {"daily_brief", "blueprint", "dashboard"}:
        return normalized
    raise HTTPException(status_code=400, detail={"message": f"Unsupported provider admin surface: {surface}"})


def _prepare_candidate_registry(conn: sqlite3.Connection) -> None:
    from app.services.blueprint_candidate_registry import (
        ensure_candidate_registry_tables,
        export_candidate_registry,
        seed_default_candidate_registry,
    )

    ensure_candidate_registry_tables(conn)
    if not export_candidate_registry(conn):
        seed_default_candidate_registry(conn)


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "layer": "track_b_v2"}


@router.get("/admin/providers/capability-matrix")
def provider_capability_matrix() -> dict[str, object]:
    from app.services.blueprint_candidate_registry import active_candidate_universe_summary
    from app.services.external_upstreams import build_provider_status_registry
    from app.services.provider_registry import capability_matrix

    with _connection() as conn:
        _prepare_candidate_registry(conn)
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "capability_matrix": capability_matrix(),
            "provider_status_registry": build_provider_status_registry(),
            "active_candidate_universe": active_candidate_universe_summary(conn),
        }


@router.get("/admin/providers/readiness")
def provider_readiness(surface: str | None = None) -> dict[str, object]:
    from app.services.blueprint_candidate_registry import active_candidate_universe_summary
    from app.services.provider_refresh import build_cached_external_upstream_payload

    normalized_surface = _normalize_surface_name(surface)
    with _connection() as conn:
        _prepare_candidate_registry(conn)
        payload = build_cached_external_upstream_payload(conn, get_settings(), surface_name=normalized_surface)
        payload["active_candidate_universe"] = active_candidate_universe_summary(conn)
        payload["surface"] = normalized_surface
        return payload


@router.get("/admin/providers/cache-status")
def provider_cache_status(surface: str | None = None) -> dict[str, object]:
    from app.services.blueprint_candidate_registry import active_candidate_universe_summary
    from app.services.provider_budget import list_surface_snapshot_versions
    from app.services.provider_cache import list_provider_snapshots
    from app.services.provider_family_success import list_provider_family_success
    from app.services.provider_refresh import build_cached_external_upstream_payload

    normalized_surface = _normalize_surface_name(surface)
    with _connection() as conn:
        _prepare_candidate_registry(conn)
        payload = build_cached_external_upstream_payload(conn, get_settings(), surface_name=normalized_surface)
        snapshots = list_provider_snapshots(conn, surface_name=normalized_surface, limit=300)
        snapshot_versions = list_surface_snapshot_versions(conn)
        if normalized_surface is not None:
            snapshot_versions = [
                item for item in snapshot_versions if str(item.get("surface_name") or "") == normalized_surface
            ]
        family_success = list_provider_family_success(conn, surface_name=normalized_surface)
        family_snapshot_counts: dict[str, int] = {}
        for row in snapshots:
            family_name = str(row.get("endpoint_family") or "")
            family_snapshot_counts[family_name] = family_snapshot_counts.get(family_name, 0) + 1
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "surface": normalized_surface,
            "active_targets": payload.get("active_targets"),
            "snapshot_count": len(snapshots),
            "family_snapshot_counts": family_snapshot_counts,
            "snapshot_versions": snapshot_versions,
            "family_success": family_success,
            "summary": payload.get("summary"),
            "active_candidate_universe": active_candidate_universe_summary(conn),
        }


@router.get("/admin/governance/source-authority")
def governance_source_authority() -> dict[str, object]:
    from app.v2.governance.service import build_source_authority_payload

    with _connection() as conn:
        _prepare_candidate_registry(conn)
        return build_source_authority_payload(conn, get_settings())


@router.get("/admin/governance/family/{family}")
def governance_family(family: str) -> dict[str, object]:
    from app.v2.governance.service import build_family_authority_payload

    with _connection() as conn:
        _prepare_candidate_registry(conn)
        try:
            return build_family_authority_payload(conn, get_settings(), family)
        except KeyError:
            raise HTTPException(status_code=404, detail={"message": f"Unknown source family: {family}"}) from None


@router.get("/admin/governance/surface-readiness")
def governance_surface_readiness() -> dict[str, object]:
    from app.v2.governance.service import build_surface_readiness_payload

    with _connection() as conn:
        _prepare_candidate_registry(conn)
        return build_surface_readiness_payload(conn, get_settings())


@router.get("/admin/governance/conflicts")
def governance_conflicts() -> dict[str, object]:
    from app.v2.governance.service import build_conflict_payload

    with _connection() as conn:
        _prepare_candidate_registry(conn)
        return build_conflict_payload(conn, get_settings())


@router.get("/admin/replay/surfaces/{surface_id}/{object_id:path}")
def replay_surface_snapshot(surface_id: str, object_id: str) -> dict[str, object]:
    from app.v2.storage.surface_snapshot_store import latest_surface_snapshot

    snapshot = latest_surface_snapshot(surface_id=surface_id, object_id=object_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail={"message": f"No replay snapshot found for {surface_id}:{object_id}"})
    return snapshot


@router.get("/admin/runtime/status")
def runtime_status() -> dict[str, object]:
    from app.v2.runtime.service import runtime_status_payload

    return runtime_status_payload()


@router.get("/admin/runtime/jobs")
def runtime_jobs() -> dict[str, object]:
    from app.v2.runtime.service import runtime_jobs_payload

    return runtime_jobs_payload()


@router.get("/admin/runtime/alerts")
def runtime_alerts() -> dict[str, object]:
    from app.v2.runtime.service import runtime_alerts_payload

    return runtime_alerts_payload()


@router.post("/admin/runtime/refresh/{surface}")
def runtime_refresh_surface(surface: str, force_refresh: bool = True) -> dict[str, object]:
    from app.v2.runtime.service import trigger_surface_refresh

    normalized_surface = _normalize_surface_name(surface)
    if normalized_surface is None:
        raise HTTPException(status_code=400, detail={"message": f"Unsupported runtime refresh surface: {surface}"})
    try:
        return trigger_surface_refresh(normalized_surface, force_refresh=force_refresh)
    except KeyError:
        raise HTTPException(status_code=404, detail={"message": f"Unknown runtime surface: {surface}"}) from None


@router.post("/admin/runtime/refresh/family/{family}")
def runtime_refresh_family(family: str, force_refresh: bool = True) -> dict[str, object]:
    from app.v2.runtime.service import trigger_family_refresh

    return trigger_family_refresh(family, force_refresh=force_refresh)


@router.post("/admin/ingestion/edgar")
def trigger_edgar_ingestion(symbols: list[str] | None = None) -> dict[str, object]:
    """
    Trigger SEC EDGAR N-PORT ingestion for tracked ETF symbols.
    This is a batch operation (not in the live request path) — runs synchronously and may take 30–120s.
    Provide ?symbols=SPY&symbols=IVV to restrict to a subset; omit to run all tracked symbols.
    """
    from app.services.sec_edgar_ingestion import run_edgar_ingestion

    return run_edgar_ingestion(symbols=symbols)


@router.get("/admin/forecast/capability-matrix")
def forecast_capability_matrix() -> dict[str, object]:
    from app.v2.forecasting.service import forecast_admin_payload

    return forecast_admin_payload()


@router.get("/admin/forecast/readiness")
def forecast_readiness() -> dict[str, object]:
    from app.v2.forecasting.service import forecast_readiness_payload

    return forecast_readiness_payload()


@router.get("/admin/forecast/runtime")
def forecast_runtime() -> dict[str, object]:
    from app.v2.forecasting.runtime_manager import forecast_runtime_status

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "providers": forecast_runtime_status(),
    }


@router.post("/admin/forecast/deferred-start")
def forecast_deferred_start() -> dict[str, object]:
    from app.v2.forecasting.runtime_manager import start_forecast_runtime_worker

    threading.Thread(
        target=start_forecast_runtime_worker,
        kwargs={"force": True},
        name="forecast-deferred-start",
        daemon=True,
    ).start()
    return {
        "status": "queued",
        "mode": "deferred_after_surface_ready",
        "queued_at": datetime.now(UTC).isoformat(),
    }


@router.post("/admin/forecast/probe")
def forecast_probe() -> dict[str, object]:
    from app.v2.forecasting.service import forecast_probe_payload

    return forecast_probe_payload()


@router.post("/admin/forecast/probe/{provider}")
def forecast_probe_provider(provider: str) -> dict[str, object]:
    from app.v2.forecasting.service import forecast_probe_payload

    return forecast_probe_payload(provider=provider)


def _refresh_provider_surface(surface_name: str, *, force_refresh: bool) -> dict[str, object]:
    from app.services.blueprint_candidate_registry import active_candidate_universe_summary
    from app.services.blueprint_refresh_monitor import (
        BLUEPRINT_WRITE_REFRESH_SCOPE,
        claim_refresh_scope,
        record_blueprint_refresh_skip,
        refresh_scope_snapshot,
        release_refresh_scope,
    )
    from app.v2.donors.providers import SQLiteProviderDonor

    scope_claim: dict[str, Any] | None = None
    if surface_name == "blueprint":
        scope_claim = claim_refresh_scope(BLUEPRINT_WRITE_REFRESH_SCOPE, owner="v2_manual_provider_refresh")
        if not scope_claim.get("acquired"):
            with _connection() as conn:
                _prepare_candidate_registry(conn)
                skip_record = record_blueprint_refresh_skip(
                    conn,
                    trigger_source="v2_manual_provider_refresh",
                    reason="already_running",
                    details={
                        "scope": BLUEPRINT_WRITE_REFRESH_SCOPE,
                        "active_refresh": refresh_scope_snapshot(BLUEPRINT_WRITE_REFRESH_SCOPE),
                    },
                )
                return {
                    "status": "skipped",
                    "reason": "already_running",
                    "scope": BLUEPRINT_WRITE_REFRESH_SCOPE,
                    "active_refresh": refresh_scope_snapshot(BLUEPRINT_WRITE_REFRESH_SCOPE),
                    "skip_record": skip_record,
                    "active_candidate_universe": active_candidate_universe_summary(conn),
                }

    with _connection() as conn:
        try:
            _prepare_candidate_registry(conn)
            donor = SQLiteProviderDonor(conn, get_settings())
            if surface_name == "daily_brief":
                payload = donor.refresh_daily_brief(force_refresh=force_refresh)
            elif surface_name == "blueprint":
                payload = donor.refresh_blueprint(force_refresh=force_refresh)
            elif surface_name == "dashboard":
                payload = donor.refresh_dashboard(force_refresh=force_refresh)
            else:
                raise HTTPException(status_code=400, detail={"message": f"Unsupported provider refresh surface: {surface_name}"})
            payload["active_candidate_universe"] = active_candidate_universe_summary(conn)
            return payload
        finally:
            if scope_claim and scope_claim.get("acquired"):
                release_refresh_scope(
                    BLUEPRINT_WRITE_REFRESH_SCOPE,
                    owner="v2_manual_provider_refresh",
                    result_status="finished",
                )


@router.post("/admin/providers/refresh/daily-brief")
def refresh_daily_brief_providers(force_refresh: bool = False) -> dict[str, object]:
    return _refresh_provider_surface("daily_brief", force_refresh=force_refresh)


@router.post("/admin/providers/refresh/blueprint")
def refresh_blueprint_providers(force_refresh: bool = False) -> dict[str, object]:
    return _refresh_provider_surface("blueprint", force_refresh=force_refresh)


@router.post("/admin/candidates/refresh/issuer-docs")
def refresh_candidate_issuer_docs(force: bool = False) -> dict[str, object]:
    from app.services.blueprint_candidate_registry import export_live_candidate_registry, refresh_registry_candidate_truth
    from app.services.blueprint_refresh_monitor import (
        BLUEPRINT_WRITE_REFRESH_SCOPE,
        claim_refresh_scope,
        record_blueprint_refresh_skip,
        refresh_scope_snapshot,
        release_refresh_scope,
    )
    from app.v2.sources.issuer_factsheet_adapter import ISSUER_DOC_TARGET_FIELDS, get_missing_issuer_doc_fields

    def _coverage_snapshot(conn, rows: list[dict[str, object]]) -> dict[str, int]:
        coverage = {field: 0 for field in ISSUER_DOC_TARGET_FIELDS}
        for row in rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            sleeve_key = str(row.get("sleeve_key") or "").strip()
            missing = set(
                get_missing_issuer_doc_fields(
                    conn,
                    candidate_symbol=symbol,
                    sleeve_key=sleeve_key,
                    target_fields=ISSUER_DOC_TARGET_FIELDS,
                )
            )
            for field in ISSUER_DOC_TARGET_FIELDS:
                if field not in missing:
                    coverage[field] += 1
        return coverage

    scope_claim = claim_refresh_scope(BLUEPRINT_WRITE_REFRESH_SCOPE, owner="v2_manual_issuer_doc_refresh")
    if not scope_claim.get("acquired"):
        with _connection() as conn:
            skip_record = record_blueprint_refresh_skip(
                conn,
                trigger_source="v2_manual_issuer_doc_refresh",
                reason="already_running",
                details={
                    "scope": BLUEPRINT_WRITE_REFRESH_SCOPE,
                    "active_refresh": refresh_scope_snapshot(BLUEPRINT_WRITE_REFRESH_SCOPE),
                },
            )
            return {
                "generated_at": datetime.now(UTC).isoformat(),
                "status": "skipped",
                "reason": "already_running",
                "scope": BLUEPRINT_WRITE_REFRESH_SCOPE,
                "active_refresh": refresh_scope_snapshot(BLUEPRINT_WRITE_REFRESH_SCOPE),
                "skip_record": skip_record,
            }

    with _connection() as conn:
        try:
            _prepare_candidate_registry(conn)
            live_rows = export_live_candidate_registry(conn)
            coverage_before = _coverage_snapshot(conn, live_rows)
            symbols_to_refresh: set[str] = set()
            for row in live_rows:
                symbol = str(row.get("symbol") or "").strip().upper()
                sleeve_key = str(row.get("sleeve_key") or "").strip()
                if not symbol or not sleeve_key:
                    continue
                missing = get_missing_issuer_doc_fields(
                    conn,
                    candidate_symbol=symbol,
                    sleeve_key=sleeve_key,
                    target_fields=ISSUER_DOC_TARGET_FIELDS,
                )
                if force or missing:
                    symbols_to_refresh.add(symbol)
            refreshed: list[dict[str, object]] = []
            for symbol in sorted(symbols_to_refresh):
                refreshed.append(refresh_registry_candidate_truth(conn, symbol=symbol, activate_market_series=True))
            coverage_after = _coverage_snapshot(conn, live_rows)
            observations_added = sum(
                max(0, coverage_after.get(field, 0) - coverage_before.get(field, 0))
                for field in ISSUER_DOC_TARGET_FIELDS
            )
            return {
                "generated_at": datetime.now(UTC).isoformat(),
                "candidates_processed": len(symbols_to_refresh),
                "observations_added": observations_added,
                "coverage_before": coverage_before,
                "coverage_after": coverage_after,
                "refreshed_symbols": sorted(symbols_to_refresh),
                "results": refreshed,
            }
        finally:
            release_refresh_scope(
                BLUEPRINT_WRITE_REFRESH_SCOPE,
                owner="v2_manual_issuer_doc_refresh",
                result_status="finished",
            )


@router.post("/admin/blueprint/market-path/refresh")
def refresh_blueprint_market_path(payload: BlueprintMarketPathRefreshRequest) -> dict[str, object]:
    from app.v2.blueprint_market import (
        operator_market_series_refresh,
        run_market_forecast_refresh_lane,
        run_market_identity_gap_audit,
    )

    with _connection() as conn:
        _prepare_candidate_registry(conn)
        audit_result = None
        series_result = None
        forecast_result = None
        if payload.audit_only:
            audit_result = run_market_identity_gap_audit(
                conn,
                candidate_id=payload.candidate_id,
                sleeve_key=payload.sleeve_key,
            )
        else:
            series_result = operator_market_series_refresh(
                conn,
                candidate_id=payload.candidate_id,
                sleeve_key=payload.sleeve_key,
                stale_only=payload.stale_only,
                verify_symbol_mapping_only=payload.verify_symbol_mapping_only,
            )
            if payload.refresh_forecasts and not payload.verify_symbol_mapping_only:
                forecast_result = run_market_forecast_refresh_lane(
                    candidate_id=payload.candidate_id,
                    sleeve_key=payload.sleeve_key,
                    stale_only=payload.forecast_stale_only,
                )
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "candidate_id": payload.candidate_id,
            "sleeve_key": payload.sleeve_key,
            "audit_only": payload.audit_only,
            "refresh_forecasts": payload.refresh_forecasts,
            "forecast_stale_only": payload.forecast_stale_only,
            **(audit_result or series_result or {}),
            "series_refresh": series_result,
            "forecast_refresh": forecast_result,
            "identity_audit": audit_result,
        }


@router.get("/admin/blueprint/market-path/coverage-audit")
def blueprint_market_path_coverage_audit(
    candidate_id: str | None = None,
    sleeve_key: str | None = None,
) -> dict[str, object]:
    from app.v2.blueprint_market.coverage import build_candidate_coverage_summary
    from app.v2.blueprint_market import build_candidate_market_path_support
    from app.v2.blueprint_market.series_refresh_service import list_blueprint_market_candidates
    from app.v2.truth.candidate_quality import build_candidate_truth_context

    with _connection() as conn:
        _prepare_candidate_registry(conn)
        candidates = list_blueprint_market_candidates(conn, candidate_id=candidate_id, sleeve_key=sleeve_key)
        items: list[dict[str, object]] = []
        direct_ready = 0
        proxy_ready = 0
        missing_history = 0
        suppressed = 0
        alias_review = 0
        benchmark_weak = 0
        for row in candidates:
            symbol = str(row.get("symbol") or "").strip().upper()
            stable_candidate_id = f"candidate_instrument_{symbol.lower()}"
            truth_context = build_candidate_truth_context(conn, {**row, "symbol": symbol, "sleeve_key": row.get("sleeve_key")})
            market_path_support = build_candidate_market_path_support(stable_candidate_id, allow_refresh=False)
            support_dict = dict(market_path_support or {})
            coverage_summary = build_candidate_coverage_summary(
                conn,
                row,
                truth_context,
                candidate_id=stable_candidate_id,
                market_path_support=support_dict,
            )
            support_usefulness = str(support_dict.get("usefulness_label") or "suppressed")
            support_proxy = bool(dict(support_dict.get("series_quality_summary") or {}).get("uses_proxy_series"))
            if support_usefulness == "suppressed":
                suppressed += 1
            if bool(coverage_summary.get("direct_ready")):
                direct_ready += 1
            elif bool(coverage_summary.get("proxy_ready")):
                proxy_ready += 1
            elif bool(coverage_summary.get("missing_history")):
                missing_history += 1
            if bool(coverage_summary.get("alias_review_needed")):
                alias_review += 1
            if bool(coverage_summary.get("benchmark_lineage_weak")):
                benchmark_weak += 1
            coverage_workflow = dict(coverage_summary.get("coverage_workflow_summary") or {})
            symbol_alias_registry = dict(coverage_workflow.get("symbol_alias_registry") or {})
            items.append(
                {
                    "candidate_id": stable_candidate_id,
                    "symbol": symbol,
                    "name": str(row.get("name") or symbol).strip(),
                    "sleeve_key": str(row.get("sleeve_key") or "").strip(),
                    "provider_symbol": symbol_alias_registry.get("provider_symbol"),
                    "fallback_aliases": list(symbol_alias_registry.get("fallback_aliases") or []),
                    "symbol_alias_registry": symbol_alias_registry,
                    "direct_identity": coverage_summary.get("direct_identity"),
                    "proxy_identity": coverage_summary.get("proxy_identity"),
                    "direct_bars": coverage_summary.get("direct_history_depth"),
                    "proxy_bars": coverage_summary.get("proxy_history_depth"),
                    "current_runtime_provider": coverage_summary.get("current_runtime_provider"),
                    "direct_quality": dict(coverage_workflow.get("direct_quality") or {}),
                    "proxy_quality": dict(coverage_workflow.get("proxy_quality") or {}),
                    "visible_decision_state": dict(truth_context.get("visible_decision_state") or {}),
                    "failure_class_summary": dict(truth_context.get("failure_class_summary") or {}),
                    "market_path_support": support_dict or None,
                    "onboarding_checklist": list(coverage_workflow.get("checklist") or []),
                    "coverage_status": coverage_summary.get("coverage_status"),
                    "coverage_workflow_summary": coverage_workflow,
                    "coverage_verdict": coverage_summary.get("coverage_status"),
                    "support_verdict": (
                        "proxy_backed"
                        if support_proxy and support_usefulness != "suppressed"
                        else "direct_backed"
                        if support_usefulness != "suppressed"
                        else "suppressed"
                    ),
                }
            )
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "candidate_id": candidate_id,
            "sleeve_key": sleeve_key,
            "summary": {
                "candidate_count": len(items),
                "direct_ready_count": direct_ready,
                "proxy_ready_count": proxy_ready,
                "missing_history_count": missing_history,
                "suppressed_count": suppressed,
                "alias_review_count": alias_review,
                "benchmark_lineage_weak_count": benchmark_weak,
            },
            "items": items,
        }


@router.post("/admin/providers/refresh/dashboard")
def refresh_dashboard_providers(force_refresh: bool = False) -> dict[str, object]:
    return _refresh_provider_surface("dashboard", force_refresh=force_refresh)


@router.get("/mcp/connectors")
def mcp_connectors() -> dict[str, object]:
    from app.services.mcp_connectors import connector_status_payload, load_connector_candidates

    rows = load_connector_candidates()
    connectors = [connector_status_payload(row) for row in rows]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "count": len(connectors),
        "connectors": connectors,
    }


@router.get("/mcp/connectors/run")
def mcp_connectors_run() -> dict[str, object]:
    from app.services.mcp_connectors import load_connector_candidates, probe_connector

    connectors = [probe_connector(row) for row in load_connector_candidates()]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "count": len(connectors),
        "run_attempted_count": sum(1 for row in connectors if bool(row.get("run_attempted"))),
        "live_ok_count": sum(1 for row in connectors if bool(row.get("live_ok"))),
        "configured_count": sum(1 for row in connectors if bool(row.get("configured"))),
        "missing_env_count": sum(1 for row in connectors if str(row.get("error_class")) == "missing_env"),
        "connectors": connectors,
    }


@router.api_route("/mcp/adapter/{connector_slug}", methods=["GET", "POST"])
async def mcp_connector_adapter(connector_slug: str, request: Request) -> dict[str, object]:
    from app.services.mcp_connectors import (
        connector_fetch,
        connector_resources_payload,
        connector_source_url,
        connector_status_payload,
        connector_tools_payload,
        find_connector_candidate,
        jsonrpc_error,
        jsonrpc_ok,
    )
    from app.services.provider_adapters import ProviderAdapterError

    row = find_connector_candidate(connector_slug)
    if row is None:
        raise HTTPException(status_code=404, detail={"message": f"Unknown connector slug: {connector_slug}"})

    status_payload = connector_status_payload(row)
    if request.method == "GET":
        return {
            "transport": "http",
            "connector": status_payload["name"],
            "provider_name": status_payload["provider_name"],
            "provider_mode": status_payload["mode"],
            "supported_families": status_payload["supported_families"],
            "retrieved_at": status_payload["retrieved_at"],
        }

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    if not isinstance(payload, dict):
        payload = {}
    rpc_id = payload.get("id", 1)
    method = str(payload.get("method") or "").strip()
    params = payload.get("params")
    params = params if isinstance(params, dict) else {}

    if method == "initialize":
        return jsonrpc_ok(
            rpc_id,
            {
                "protocolVersion": "2024-11-05",
                "serverInfo": {
                    "name": f"investment-agent/{connector_slug}",
                    "version": "0.2.0",
                },
                "capabilities": {
                    "resources": {},
                    "tools": {"listChanged": False},
                },
                "provider_mode": status_payload["mode"],
            },
        )

    if method == "tools/list":
        return jsonrpc_ok(rpc_id, {"tools": connector_tools_payload(row)})

    if method == "resources/list":
        return jsonrpc_ok(rpc_id, {"resources": connector_resources_payload(row)})

    if method == "resources/read":
        uri = str(params.get("uri") or "")
        if uri.endswith("/status"):
            return jsonrpc_ok(rpc_id, connector_status_payload(row))
        if uri.endswith("/source"):
            return jsonrpc_ok(
                rpc_id,
                {
                    "name": status_payload["name"],
                    "provider_name": status_payload["provider_name"],
                    "publisher": status_payload["publisher"],
                    "category": status_payload["category"],
                    "priority": status_payload["priority"],
                    "supported_families": status_payload["supported_families"],
                    "source_url": connector_source_url(row),
                    "retrieved_at": datetime.now(UTC).isoformat(),
                },
            )
        return jsonrpc_error(rpc_id, -32602, f"Unknown uri: {uri}")

    if method in {"search", "resources/search"}:
        query = str(params.get("query") or "").strip()
        return jsonrpc_ok(
            rpc_id,
            {
                "items": [
                    {
                        "title": f"connector:{status_payload['name']}",
                        "summary": f"Query='{query}' matched connector runtime metadata",
                        "provider_mode": status_payload["mode"],
                        "source_url": connector_source_url(row),
                        "retrieved_at": datetime.now(UTC).isoformat(),
                    }
                ]
            },
        )

    if method == "tools/call":
        name = str(params.get("name") or "").strip()
        arguments = params.get("arguments")
        arguments = arguments if isinstance(arguments, dict) else {}
        if name == "search":
            query = str(arguments.get("query") or "").strip()
            return jsonrpc_ok(
                rpc_id,
                {
                    "items": [
                        {
                            "title": f"connector:{status_payload['name']}",
                            "summary": f"Search tool query='{query}' returned runtime metadata",
                            "provider_mode": status_payload["mode"],
                            "source_url": connector_source_url(row),
                            "retrieved_at": datetime.now(UTC).isoformat(),
                        }
                    ]
                },
            )
        if name == "fetch":
            endpoint_family = str(arguments.get("endpoint_family") or "").strip()
            identifier = str(arguments.get("identifier") or "").strip() or None
            limit_raw = arguments.get("limit")
            limit = int(limit_raw) if isinstance(limit_raw, int) else None
            try:
                provider_payload = connector_fetch(row, endpoint_family, identifier, limit=limit)
            except ProviderAdapterError as exc:
                return jsonrpc_ok(
                    rpc_id,
                    {
                        "ok": False,
                        "provider_name": status_payload["provider_name"],
                        "endpoint_family": endpoint_family,
                        "identifier": identifier,
                        "error_class": exc.error_class,
                        "message": str(exc),
                    },
                )
            return jsonrpc_ok(
                rpc_id,
                {
                    "ok": True,
                    "provider_name": status_payload["provider_name"],
                    "endpoint_family": endpoint_family,
                    "identifier": identifier,
                    "payload": provider_payload,
                },
            )
        return jsonrpc_error(rpc_id, -32602, f"Unsupported tool: {name}")

    return jsonrpc_error(rpc_id, -32601, f"Unknown method: {method}")


def _build_blueprint() -> dict[str, Any]:
    from app.v2.surfaces.blueprint.explorer_contract_builder import build
    result = build()
    _mem_set("blueprint_explorer", result)
    return result


def _build_candidate_report(
    candidate_id: str,
    *,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None = None,
    source_generated_at: str | None = None,
    source_contract_version: str | None = None,
    report_build_mode: str = "snapshot_only",
) -> dict[str, Any]:
    from app.v2.surfaces.blueprint.report_contract_builder import build

    stable_candidate_id = _stable_candidate_id(candidate_id)
    result = build(
        candidate_id,
        source_snapshot_id=source_snapshot_id,
        source_generated_at=source_generated_at,
        source_contract_version=source_contract_version,
        sleeve_key=sleeve_key,
        report_build_mode=report_build_mode,
    )
    _promote_bound_report_to_memory(
        result,
        stable_candidate_id=stable_candidate_id,
        sleeve_key=sleeve_key or str(result.get("sleeve_key") or result.get("sleeve_id") or "").strip() or None,
        source_snapshot_id=source_snapshot_id,
        source_contract_version=source_contract_version,
    )
    return result


def _parse_compare_ids(ids: str | None) -> list[str]:
    rows = [str(value or "").strip() for value in str(ids or "").split(",")]
    candidate_ids = [row for row in rows if row]
    if len(candidate_ids) < 2:
        raise HTTPException(status_code=400, detail={"message": "Blueprint compare requires at least two candidate ids."})
    return candidate_ids[:3]


@router.get("/surfaces/blueprint/explorer")
def blueprint_explorer(report_candidate_id: str | None = None) -> dict[str, object]:
    from app.v2.surfaces.blueprint.explorer_contract_builder import build

    # Only cache the default explorer view (no specific report candidate)
    if not report_candidate_id:
        contract, cache_state = _serve_cached_with_state(
            "blueprint_explorer", "blueprint_explorer", "blueprint_explorer",
            max_seconds=300, rebuild_fn=_build_blueprint, allow_background_rebuild=False,
        )
        if not contract.get("surface_snapshot_id"):
            contract = _build_blueprint()
            cache_state = {
                "state": "live_build",
                "stale": False,
                "revalidating": False,
                "cached_at": datetime.now(UTC).isoformat(),
                "max_age_seconds": 300,
                "summary": "Served from live build.",
            }
        return {
            **contract,
            "route_cache_state": cache_state,
        }

    return build(report_candidate_id=report_candidate_id)


@router.get("/blueprint/compare")
def blueprint_compare(ids: str, sleeve_id: str | None = None) -> dict[str, object]:
    from app.v2.surfaces.blueprint.compare_contract_builder import build

    return build(_parse_compare_ids(ids), sleeve_id=sleeve_id)


@router.get("/surfaces/blueprint/compare")
def blueprint_compare_surface(ids: str, sleeve_id: str | None = None) -> dict[str, object]:
    from app.v2.surfaces.blueprint.compare_contract_builder import build

    return build(_parse_compare_ids(ids), sleeve_id=sleeve_id)


def _binding_matches_report(
    contract: dict[str, Any],
    *,
    source_snapshot_id: str | None,
    source_generated_at: str | None,
    source_contract_version: str | None,
) -> bool:
    if source_snapshot_id and str(contract.get("bound_source_snapshot_id") or "") != str(source_snapshot_id):
        return False
    if source_contract_version and str(contract.get("source_contract_version") or "") != str(source_contract_version):
        return False
    if source_generated_at:
        try:
            expected = datetime.fromisoformat(str(source_generated_at).replace("Z", "+00:00"))
            actual = datetime.fromisoformat(str(contract.get("report_generated_at") or contract.get("generated_at") or "").replace("Z", "+00:00"))
            if expected.tzinfo is None:
                expected = expected.replace(tzinfo=UTC)
            if actual.tzinfo is None:
                actual = actual.replace(tzinfo=UTC)
            if actual.astimezone(UTC) < expected.astimezone(UTC):
                return False
        except Exception:
            return False
    return True


@router.get("/surfaces/candidates/{candidate_id}/report")
def candidate_report(
    candidate_id: str,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None = None,
    source_generated_at: str | None = None,
    source_contract_version: str | None = None,
    refresh: bool = False,
) -> dict[str, object]:
    return _candidate_report_fast_payload(
        candidate_id,
        sleeve_key=sleeve_key,
        source_snapshot_id=source_snapshot_id,
        source_generated_at=source_generated_at,
        source_contract_version=source_contract_version,
        refresh=refresh,
    )


@router.get("/surfaces/candidates/{candidate_id}/report-plain", response_class=PlainTextResponse)
def candidate_report_plain(
    candidate_id: str,
    sleeve_key: str | None = None,
    source_snapshot_id: str | None = None,
    source_generated_at: str | None = None,
    source_contract_version: str | None = None,
    refresh: bool = False,
) -> PlainTextResponse:
    payload = _candidate_report_fast_payload(
        candidate_id,
        sleeve_key=sleeve_key,
        source_snapshot_id=source_snapshot_id,
        source_generated_at=source_generated_at,
        source_contract_version=source_contract_version,
        refresh=refresh,
    )
    return PlainTextResponse(
        json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")),
        media_type="application/json",
        headers={"Cache-Control": "no-store", "Connection": "close"},
    )


@router.get("/surfaces/candidates/{candidate_id}/notebook")
def notebook(candidate_id: str) -> dict[str, object]:
    from app.v2.surfaces.notebook.contract_builder import build

    return build(candidate_id)


@router.get("/surfaces/candidates/{candidate_id}/notebook/history")
def notebook_history(candidate_id: str, limit: int = 24) -> dict[str, object]:
    from app.v2.storage.notebook_store import list_history

    stable_candidate_id = _stable_candidate_id(candidate_id)
    return {
        "candidate_id": stable_candidate_id,
        "history": list_history(candidate_id=stable_candidate_id, limit=limit),
    }


@router.get("/surfaces/candidates/{candidate_id}/evidence")
def evidence_workspace(candidate_id: str) -> dict[str, object]:
    from app.v2.surfaces.evidence_workspace.contract_builder import build

    return build(candidate_id)


def _build_daily_brief() -> dict[str, Any]:
    from app.v2.surfaces.daily_brief.contract_builder import build
    result = build()
    _mem_set("daily_brief", result)
    return result


def _build_and_snapshot_daily_brief() -> dict[str, Any]:
    from app.v2.storage.surface_snapshot_store import record_surface_snapshot
    result = _build_daily_brief()
    record_surface_snapshot(
        surface_id="daily_brief",
        object_id="daily_brief",
        snapshot_kind="surface_contract",
        state_label=str(result.get("freshness_state") or ""),
        data_confidence=str(result.get("data_confidence") or ""),
        decision_confidence=str(result.get("decision_confidence") or ""),
        generated_at=str(result.get("generated_at") or ""),
        contract=result,
        input_summary={},
    )
    return result


def _daily_brief_loading_contract() -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    summary = "Daily Brief refresh is running. Cached content will replace this loading state as soon as the surface snapshot is ready."
    section_state = {
        "state": "degraded",
        "reason_code": "refresh_in_progress",
        "reason_text": summary,
    }
    return {
        "contract_version": "0.3.0",
        "surface_id": "daily_brief",
        "generated_at": now,
        "freshness_state": "degraded_monitoring_mode",
        "holdings_overlay_present": False,
        "surface_state": {
            "state": "degraded",
            "reason_codes": ["refresh_in_progress"],
            "summary": summary,
        },
        "section_states": {
            "market_state": section_state,
            "signal_stack": section_state,
            "regime_context": section_state,
            "portfolio_impact": section_state,
            "review_triggers": section_state,
            "monitoring_conditions": section_state,
            "scenarios": section_state,
            "evidence": section_state,
            "diagnostics": section_state,
        },
        "what_changed": [],
        "why_it_matters_economically": "Daily Brief is preparing the current market and macro read.",
        "why_it_matters_here": "Portfolio implications will appear as soon as the refreshed brief snapshot is ready.",
        "review_posture": "monitor",
        "what_confirms_or_breaks": "Waiting for the current brief inputs to finish loading.",
        "data_confidence": "low",
        "decision_confidence": "low",
        "evidence_and_trust": {
            "freshness_state": "execution_failed_or_incomplete",
            "source_count": 0,
            "completeness_score": 0.0,
        },
        "portfolio_overlay": None,
        "portfolio_overlay_context": {
            "state": "unavailable",
            "summary": "Portfolio overlay will attach after the brief refresh completes.",
        },
        "market_state_cards": [],
        "macro_chart_panels": [],
        "cross_asset_chart_panels": [],
        "fx_chart_panels": [],
        "signal_chart_panels": [],
        "scenario_chart_panels": [],
        "signal_stack": [],
        "signal_stack_groups": [],
        "regime_context_drivers": [],
        "monitoring_conditions": [],
        "contingent_drivers": [],
        "portfolio_impact_rows": [],
        "review_triggers": [],
        "scenario_blocks": [],
        "evidence_bars": [],
        "data_timeframes": [],
        "diagnostics": [
            {"label": "Brief state", "value": "Refresh in progress"},
            {"label": "Generated", "value": now},
        ],
        "route_cache_state": {
            "state": "pending",
            "stale": False,
            "revalidating": True,
            "cached_at": now,
            "max_age_seconds": 600,
            "summary": "No cached Daily Brief snapshot was available, so a refresh was queued.",
        },
    }


def _daily_brief_cached_entry() -> tuple[dict[str, Any], datetime] | None:
    mem_entry = _mem_get_any("daily_brief")
    if mem_entry is not None:
        stored_at, contract = mem_entry
        return contract, stored_at

    db_entry = _db_get_any("daily_brief", "daily_brief")
    if db_entry is None:
        return None

    contract, created_at = db_entry
    _mem_set("daily_brief", contract)
    return contract, created_at


def _daily_brief_needs_rebuild(contract: dict[str, Any], stored_at: datetime, *, max_seconds: int = 600) -> bool:
    now = datetime.now(UTC)
    slot_boundary = _surface_slot_boundary("daily_brief", now=now)
    contract_time = _contract_effective_time(contract, stored_at)
    if slot_boundary is not None and contract_time < slot_boundary:
        return True
    return (now - stored_at).total_seconds() > max_seconds


@router.get("/surfaces/daily-brief")
def daily_brief(force: int = 0) -> dict[str, object]:
    if int(force or 0):
        _fire_rebuild("daily_brief", lambda: _build_and_snapshot_daily_brief())
        cached = _daily_brief_cached_entry()
        if cached is not None:
            contract, _ = cached
            return contract
        return _daily_brief_loading_contract()

    cached = _daily_brief_cached_entry()
    if cached is not None:
        contract, stored_at = cached
        if _daily_brief_needs_rebuild(contract, stored_at):
            _fire_rebuild("daily_brief", lambda: _build_and_snapshot_daily_brief())
        return contract

    _fire_rebuild("daily_brief", lambda: _build_and_snapshot_daily_brief())
    return _daily_brief_loading_contract()


def _build_and_snapshot_portfolio(account_id: str) -> dict[str, Any]:
    from app.v2.storage.surface_snapshot_store import record_surface_snapshot
    from app.v2.surfaces.portfolio.contract_builder import build
    result = build(account_id)
    key = f"portfolio_{account_id}"
    _mem_set(key, result)
    record_surface_snapshot(
        surface_id="portfolio",
        object_id=key,
        snapshot_kind="surface_contract",
        state_label=str(result.get("freshness_state") or ""),
        data_confidence="medium",
        decision_confidence="medium",
        generated_at=str(result.get("generated_at") or ""),
        contract=result,
        input_summary={},
    )
    return result


@router.get("/surfaces/portfolio")
def portfolio(account_id: str = "default") -> dict[str, object]:
    key = f"portfolio_{account_id}"
    return _serve_cached(
        key, "portfolio", key,
        max_seconds=180, rebuild_fn=lambda: _build_and_snapshot_portfolio(account_id),
    )


@router.get("/portfolio/status")
def portfolio_status(account_id: str = "default") -> dict[str, object]:
    from app.v2.portfolio.control import build_portfolio_status

    with _connection() as conn:
        return build_portfolio_status(conn, account_id=account_id)


@router.get("/portfolio/uploads")
def portfolio_uploads(include_deleted: bool = False) -> dict[str, object]:
    from app.services.portfolio_ingest import ensure_portfolio_control_tables, list_portfolio_uploads

    with _connection() as conn:
        ensure_portfolio_control_tables(conn)
        rows = list_portfolio_uploads(conn, include_deleted=include_deleted)
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "uploads": rows,
            "count": len(rows),
        }


@router.get("/portfolio/uploads/{run_id}")
def portfolio_upload_detail(run_id: str) -> dict[str, object]:
    from app.services.portfolio_ingest import get_portfolio_upload_detail

    with _connection() as conn:
        detail = get_portfolio_upload_detail(conn, run_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="portfolio_upload_not_found")
        return detail


@router.post("/portfolio/uploads")
def create_portfolio_upload(payload: PortfolioUploadCreateRequest) -> dict[str, object]:
    from app.v2.portfolio.control import create_upload

    with _connection() as conn:
        return create_upload(
            conn,
            csv_text=payload.csv_text,
            filename=payload.filename,
            source_name=payload.source_name,
            default_currency=payload.default_currency,
            default_account_type=payload.default_account_type,
            allow_live_pricing=payload.allow_live_pricing,
        )


@router.post("/portfolio/uploads/{run_id}/activate")
def activate_portfolio_upload_route(run_id: str) -> dict[str, object]:
    from app.v2.portfolio.control import activate_upload

    with _connection() as conn:
        try:
            return activate_upload(conn, run_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"message": str(exc)}) from exc


@router.post("/portfolio/uploads/{run_id}/delete")
def delete_portfolio_upload_route(run_id: str, deleted_reason: str | None = None) -> dict[str, object]:
    from app.v2.portfolio.control import delete_upload

    with _connection() as conn:
        try:
            return delete_upload(conn, run_id, deleted_reason=deleted_reason)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"message": str(exc)}) from exc


@router.get("/portfolio/mapping/issues")
def portfolio_mapping_issues(run_id: str | None = None) -> dict[str, object]:
    from app.v2.portfolio.control import list_mapping_issues

    with _connection() as conn:
        rows = list_mapping_issues(conn, run_id=run_id)
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "run_id": run_id,
            "issues": rows,
            "count": len(rows),
        }


@router.get("/portfolio/mapping/overrides")
def portfolio_mapping_overrides() -> dict[str, object]:
    from app.v2.portfolio.control import list_mapping_overrides

    with _connection() as conn:
        rows = list_mapping_overrides(conn)
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "overrides": rows,
            "count": len(rows),
        }


@router.post("/portfolio/mapping/overrides")
def save_portfolio_mapping_override(payload: PortfolioMappingOverrideRequest) -> dict[str, object]:
    from app.v2.portfolio.control import put_mapping_override

    with _connection() as conn:
        try:
            return put_mapping_override(conn, symbol=payload.symbol, sleeve=payload.sleeve)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"message": str(exc)}) from exc


@router.get("/surfaces/compare")
def compare(candidate_a: str, candidate_b: str) -> dict[str, object]:
    from app.v2.surfaces.compare.contract_builder import build

    return build(candidate_a, candidate_b)


@router.get("/surfaces/changes")
def changes(
    surface_id: str,
    since_utc: str | None = None,
    window: str | None = None,
    timezone: str | None = None,
    candidate_id: str | None = None,
    sleeve_id: str | None = None,
    category: str | None = None,
    needs_review: bool | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> dict[str, object]:
    from app.v2.surfaces.changes.contract_builder import build

    try:
        return build(
            surface_id,
            since_utc,
            window=window,
            timezone=timezone,
            candidate_id=candidate_id,
            sleeve_id=sleeve_id,
            category=category,
            needs_review=needs_review,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"message": str(exc)}) from exc


@router.get("/surfaces/blueprint/explorer/changes")
def blueprint_explorer_changes(
    since_utc: str | None = None,
    window: str | None = None,
    timezone: str | None = None,
    sleeve_id: str | None = None,
    category: str | None = None,
    needs_review: bool | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> dict[str, object]:
    from app.v2.surfaces.changes.contract_builder import build

    try:
        return build(
            "blueprint_explorer",
            since_utc,
            window=window,
            timezone=timezone,
            sleeve_id=sleeve_id,
            category=category,
            needs_review=needs_review,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"message": str(exc)}) from exc


@router.post("/surfaces/blueprint/explorer/changes/scan")
def blueprint_explorer_changes_scan(timezone: str | None = None) -> dict[str, object]:
    from app.v2.surfaces.changes.blueprint_daily_scan import run_blueprint_daily_source_scan

    return run_blueprint_daily_source_scan(timezone=timezone)


@router.get("/surfaces/candidates/{candidate_id}/changes")
def candidate_changes(
    candidate_id: str,
    since_utc: str | None = None,
    window: str | None = None,
    timezone: str | None = None,
    sleeve_id: str | None = None,
    category: str | None = None,
    needs_review: bool | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> dict[str, object]:
    from app.v2.surfaces.changes.contract_builder import build

    try:
        return build(
            "candidate_report",
            since_utc,
            window=window,
            timezone=timezone,
            candidate_id=candidate_id,
            sleeve_id=sleeve_id,
            category=category,
            needs_review=needs_review,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"message": str(exc)}) from exc


@router.get("/admin/feedback/forecast")
def feedback_forecast(provider: str | None = None, limit: int = 60) -> dict[str, object]:
    from app.v2.feedback.store import forecast_feedback_payload

    return forecast_feedback_payload(provider=provider, limit=limit)


@router.get("/admin/feedback/triggers")
def feedback_triggers(limit: int = 80) -> dict[str, object]:
    from app.v2.feedback.store import trigger_feedback_payload

    return trigger_feedback_payload(limit=limit)


@router.get("/admin/feedback/recommendations")
def feedback_recommendations(candidate_id: str | None = None, limit: int = 100) -> dict[str, object]:
    from app.v2.feedback.store import recommendation_feedback_payload

    return recommendation_feedback_payload(candidate_id=candidate_id, limit=limit)


@router.post("/admin/feedback/recommendations/{candidate_id}/review")
def create_recommendation_review(candidate_id: str, payload: RecommendationReviewCreateRequest) -> dict[str, object]:
    from app.v2.core.change_ledger import record_change
    from app.v2.feedback.store import create_recommendation_review as create_review

    review = create_review(
        candidate_id=_stable_candidate_id(candidate_id),
        decision_label=payload.decision_label,
        review_outcome=payload.review_outcome,
        notes=payload.notes,
        what_changed_view=payload.what_changed_view,
        false_positive=payload.false_positive,
        false_negative=payload.false_negative,
        stale_evidence_miss=payload.stale_evidence_miss,
        overconfident_forecast_support=payload.overconfident_forecast_support,
    )
    record_change(
        event_type="recommendation_review_recorded",
        surface_id="changes",
        summary=f"Recommendation review recorded for {review['candidate_id']}.",
        candidate_id=review["candidate_id"],
        change_trigger="recommendation_feedback",
        reason_summary=payload.review_outcome,
        current_state=payload.decision_label,
        implication_summary=payload.what_changed_view or payload.notes or payload.review_outcome,
        report_tab="investment_case",
        impact_level="low",
        deep_link_target={
            "target_type": "candidate_report",
            "target_id": review["candidate_id"],
            "tab": "investment_case",
            "section": "feedback",
        },
    )
    return review


@router.post("/surfaces/candidates/{candidate_id}/notebook/entries")
async def create_notebook_entry(candidate_id: str, payload: NotebookEntryCreateRequest) -> dict[str, object]:
    from app.v2.storage.notebook_store import create_entry
    from app.v2.core.change_ledger import record_change

    stable_candidate_id = _stable_candidate_id(candidate_id)
    entry = create_entry(
        stable_candidate_id,
        linked_object_type=payload.linked_object_type,
        linked_object_id=payload.linked_object_id or stable_candidate_id,
        linked_object_label=payload.linked_object_label or stable_candidate_id,
        title=payload.title,
        thesis=payload.thesis,
        assumptions=payload.assumptions,
        invalidation=payload.invalidation,
        watch_items=payload.watch_items,
        reflections=payload.reflections,
        next_review_date=payload.next_review_date,
    )
    record_change(
        event_type="notebook_entry_created",
        surface_id="notebook",
        summary=f"Notebook entry created for {stable_candidate_id}.",
        candidate_id=stable_candidate_id,
        change_trigger="notebook_create",
        reason_summary=payload.title,
        current_state="draft",
        implication_summary=payload.thesis,
        report_tab="investment_case",
        impact_level="low",
        deep_link_target={"target_type": "notebook", "target_id": stable_candidate_id},
    )
    return entry


@router.patch("/surfaces/notebook/entries/{entry_id}")
async def update_notebook_entry(entry_id: str, payload: NotebookEntryUpdateRequest) -> dict[str, object]:
    from app.v2.storage.notebook_store import update_entry
    from app.v2.core.change_ledger import record_change

    entry = update_entry(entry_id, payload.model_dump(exclude_none=True))
    if entry is None:
        raise HTTPException(status_code=404, detail="notebook_entry_not_found")
    record_change(
        event_type="notebook_entry_updated",
        surface_id="notebook",
        summary=f"Notebook entry updated for {entry['candidate_id']}.",
        candidate_id=entry["candidate_id"],
        change_trigger="notebook_update",
        reason_summary=entry["title"],
        current_state=entry["status"],
        implication_summary=entry["thesis"],
        report_tab="investment_case",
        impact_level="low",
        deep_link_target={"target_type": "notebook", "target_id": entry["candidate_id"]},
    )
    return entry


@router.post("/surfaces/notebook/entries/{entry_id}/forecast-refs")
async def add_notebook_forecast_ref(entry_id: str, payload: NotebookForecastReferenceCreateRequest) -> dict[str, object]:
    from app.v2.forecasting.store import add_notebook_forecast_reference
    from app.v2.core.change_ledger import record_change
    from app.v2.storage.notebook_store import _connection as notebook_connection

    with notebook_connection() as conn:
        row = conn.execute(
            "SELECT * FROM v2_notebook_entries WHERE entry_id = ?",
            (entry_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="notebook_entry_not_found")
    entry = dict(row)
    ref = add_notebook_forecast_reference(
        entry_id,
        forecast_run_id=payload.forecast_run_id,
        reference_label=payload.reference_label,
        threshold_summary=payload.threshold_summary,
    )
    record_change(
        event_type="notebook_forecast_ref_added",
        surface_id="notebook",
        summary=f"Notebook forecast reference added for {entry['candidate_id']}.",
        candidate_id=entry["candidate_id"],
        change_trigger="notebook_forecast_ref_add",
        reason_summary=payload.reference_label,
        implication_summary=payload.threshold_summary or payload.reference_label,
        report_tab="scenarios",
        impact_level="low",
        deep_link_target={"target_type": "notebook", "target_id": entry["candidate_id"]},
    )
    return ref


@router.post("/surfaces/notebook/entries/{entry_id}/finalize")
async def finalize_notebook_entry(entry_id: str) -> dict[str, object]:
    from app.v2.storage.notebook_store import set_status
    from app.v2.core.change_ledger import record_change

    entry = set_status(entry_id, "finalized")
    if entry is None:
        raise HTTPException(status_code=404, detail="notebook_entry_not_found")
    record_change(
        event_type="notebook_entry_finalized",
        surface_id="notebook",
        summary=f"Notebook entry finalized for {entry['candidate_id']}.",
        candidate_id=entry["candidate_id"],
        change_trigger="notebook_finalize",
        current_state="finalized",
        implication_summary=entry["thesis"],
        report_tab="investment_case",
        impact_level="low",
        deep_link_target={"target_type": "notebook", "target_id": entry["candidate_id"]},
    )
    return entry


@router.post("/surfaces/notebook/entries/{entry_id}/archive")
async def archive_notebook_entry(entry_id: str) -> dict[str, object]:
    from app.v2.storage.notebook_store import set_status
    from app.v2.core.change_ledger import record_change

    entry = set_status(entry_id, "archived")
    if entry is None:
        raise HTTPException(status_code=404, detail="notebook_entry_not_found")
    record_change(
        event_type="notebook_entry_archived",
        surface_id="notebook",
        summary=f"Notebook entry archived for {entry['candidate_id']}.",
        candidate_id=entry["candidate_id"],
        change_trigger="notebook_archive",
        current_state="archived",
        implication_summary=entry["thesis"],
        report_tab="investment_case",
        impact_level="low",
        deep_link_target={"target_type": "notebook", "target_id": entry["candidate_id"]},
    )
    return entry


@router.delete("/surfaces/notebook/entries/{entry_id}")
async def delete_notebook_entry(entry_id: str) -> dict[str, object]:
    from app.v2.storage.notebook_store import delete_entry

    deleted = delete_entry(entry_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="notebook_entry_not_found")
    return {"deleted": True, "entry_id": entry_id}


@router.post("/surfaces/candidates/{candidate_id}/evidence/documents")
async def create_evidence_document(candidate_id: str, payload: EvidenceDocumentCreateRequest) -> dict[str, object]:
    from app.v2.storage.evidence_store import add_document
    from app.v2.core.change_ledger import record_change

    stable_candidate_id = _stable_candidate_id(candidate_id)
    document = add_document(
        stable_candidate_id,
        linked_object_type=payload.linked_object_type,
        linked_object_id=payload.linked_object_id or stable_candidate_id,
        linked_object_label=payload.linked_object_label or stable_candidate_id,
        title=payload.title,
        document_type=payload.document_type,
        url=payload.url,
        retrieved_utc=payload.retrieved_utc,
        freshness_state=payload.freshness_state,
        stale=payload.stale,
    )
    record_change(
        event_type="evidence_document_added",
        surface_id="evidence_workspace",
        summary=f"Evidence document added for {stable_candidate_id}.",
        candidate_id=stable_candidate_id,
        change_trigger="evidence_document_add",
        reason_summary=payload.title,
        implication_summary=payload.document_type,
        report_tab="evidence",
        impact_level="low",
        deep_link_target={"target_type": "evidence_workspace", "target_id": stable_candidate_id},
    )
    return document


@router.post("/surfaces/candidates/{candidate_id}/evidence/claims")
async def create_evidence_claim(candidate_id: str, payload: EvidenceClaimCreateRequest) -> dict[str, object]:
    from app.v2.storage.evidence_store import add_claim
    from app.v2.core.change_ledger import record_change

    stable_candidate_id = _stable_candidate_id(candidate_id)
    claim = add_claim(
        stable_candidate_id,
        object_type=payload.object_type,
        object_id=payload.object_id or stable_candidate_id,
        object_label=payload.object_label or stable_candidate_id,
        claim_text=payload.claim_text,
        claim_meta=payload.claim_meta,
        directness=payload.directness,
        freshness_state=payload.freshness_state,
    )
    record_change(
        event_type="evidence_claim_added",
        surface_id="evidence_workspace",
        summary=f"Evidence claim added for {stable_candidate_id}.",
        candidate_id=stable_candidate_id,
        change_trigger="evidence_claim_add",
        reason_summary=payload.claim_text,
        implication_summary=payload.claim_meta,
        report_tab="evidence",
        impact_level="low",
        deep_link_target={"target_type": "evidence_workspace", "target_id": stable_candidate_id},
    )
    return claim


@router.post("/surfaces/candidates/{candidate_id}/evidence/mappings")
async def create_evidence_mapping(candidate_id: str, payload: EvidenceMappingCreateRequest) -> dict[str, object]:
    from app.v2.storage.evidence_store import add_mapping
    from app.v2.core.change_ledger import record_change

    stable_candidate_id = _stable_candidate_id(candidate_id)
    mapping = add_mapping(
        stable_candidate_id,
        sleeve_label=payload.sleeve_label,
        instrument_label=payload.instrument_label,
        benchmark_label=payload.benchmark_label,
        baseline_label=payload.baseline_label,
        directness=payload.directness,
    )
    record_change(
        event_type="evidence_mapping_added",
        surface_id="evidence_workspace",
        summary=f"Evidence mapping added for {stable_candidate_id}.",
        candidate_id=stable_candidate_id,
        change_trigger="evidence_mapping_add",
        reason_summary=payload.instrument_label,
        implication_summary=payload.benchmark_label,
        report_tab="evidence",
        impact_level="low",
        deep_link_target={"target_type": "evidence_workspace", "target_id": stable_candidate_id},
    )
    return mapping


@router.post("/surfaces/candidates/{candidate_id}/evidence/tax-assumptions")
async def create_evidence_tax_assumption(candidate_id: str, payload: EvidenceTaxAssumptionCreateRequest) -> dict[str, object]:
    from app.v2.storage.evidence_store import add_tax_assumption
    from app.v2.core.change_ledger import record_change

    stable_candidate_id = _stable_candidate_id(candidate_id)
    assumption = add_tax_assumption(stable_candidate_id, label=payload.label, value=payload.value)
    record_change(
        event_type="tax_assumption_added",
        surface_id="evidence_workspace",
        summary=f"Tax assumption added for {stable_candidate_id}.",
        candidate_id=stable_candidate_id,
        change_trigger="tax_assumption_add",
        reason_summary=payload.label,
        implication_summary=payload.value,
        report_tab="evidence",
        impact_level="low",
        deep_link_target={"target_type": "evidence_workspace", "target_id": stable_candidate_id},
    )
    return assumption


@router.post("/surfaces/candidates/{candidate_id}/evidence/gaps")
async def create_evidence_gap(candidate_id: str, payload: EvidenceGapCreateRequest) -> dict[str, object]:
    from app.v2.storage.evidence_store import add_gap
    from app.v2.core.change_ledger import record_change

    stable_candidate_id = _stable_candidate_id(candidate_id)
    gap = add_gap(stable_candidate_id, object_label=payload.object_label, issue_text=payload.issue_text)
    record_change(
        event_type="evidence_gap_added",
        surface_id="evidence_workspace",
        summary=f"Evidence gap added for {stable_candidate_id}.",
        candidate_id=stable_candidate_id,
        change_trigger="evidence_gap_add",
        reason_summary=payload.object_label,
        implication_summary=payload.issue_text,
        report_tab="evidence",
        impact_level="medium",
        requires_review=True,
        deep_link_target={"target_type": "evidence_workspace", "target_id": stable_candidate_id},
    )
    return gap
    requested_sleeve_norm = requested_sleeve[7:] if requested_sleeve.startswith("sleeve_") else requested_sleeve
    actual_sleeve_norm = actual_sleeve[7:] if actual_sleeve.startswith("sleeve_") else actual_sleeve
    if requested_sleeve_norm and actual_sleeve_norm and actual_sleeve_norm != requested_sleeve_norm:
        return False
