from __future__ import annotations

import logging
import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from app.config import get_db_path, get_repo_root, get_settings
from app.models.db import connect
from app.services.provider_registry import SURFACE_TARGET_FAMILIES, surface_families
from app.v2.forecasting.service import forecast_readiness_payload
from app.v2.governance.service import build_conflict_payload, build_surface_readiness_payload


logger = logging.getLogger(__name__)

_LOCK = threading.RLock()
_STOP_EVENT = threading.Event()
_WORKER: threading.Thread | None = None
_JOB_STATE: dict[str, dict[str, Any]] = {}
_SURFACE_DEFAULT_INTERVAL = {
    "daily_brief": 12 * 60 * 60,
    "dashboard": 5 * 60,
    "blueprint": 6 * 60 * 60,
}
_FAMILY_ALIAS_TO_ENDPOINT = {
    "latest_quote": "quote_latest",
    "benchmark_proxy_history": "benchmark_proxy",
    "etf_reference_metadata": "reference_meta",
}
_JOB_ORDER = ("daily_brief", "dashboard", "blueprint")
_STARTUP_WARMUP_COMPLETED = False
_ENV_BY_PROVIDER = {
    "alpha_vantage": "ALPHA_VANTAGE_API_KEY",
    "eodhd": "EODHD_API_KEY",
    "finnhub": "FINNHUB_API_KEY",
    "fmp": "FMP_API_KEY",
    "frankfurter": None,
    "polygon": "POLYGON_API_KEY",
    "tiingo": "TIINGO_API_KEY",
    "twelve_data": "TWELVE_DATA_API_KEY",
}


@dataclass(frozen=True)
class RuntimeJobSpec:
    job_id: str
    kind: str
    target: str
    interval_seconds: int
    description: str


def _now() -> datetime:
    return datetime.now(UTC)


def _now_iso() -> str:
    return _now().isoformat()


def _is_test_mode() -> bool:
    return bool(os.getenv("PYTEST_CURRENT_TEST"))


def _scheduler_enabled() -> bool:
    return os.getenv("IA_RUNTIME_SCHEDULER_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}


def _blueprint_market_scheduler_enabled() -> bool:
    settings = get_settings()
    return bool(getattr(settings, "blueprint_market_scheduler_enabled", False)) and bool(
        getattr(settings, "blueprint_market_path_enabled", False)
    )


def _configured_providers_by_family() -> dict[str, list[str]]:
    from app.services.provider_registry import DATA_FAMILY_ROUTING

    configured: dict[str, list[str]] = {}
    for family_name, providers in DATA_FAMILY_ROUTING.items():
        active = []
        for provider_name in providers:
            env_name = _ENV_BY_PROVIDER.get(provider_name)
            if env_name is None or os.getenv(env_name, "").strip():
                active.append(provider_name)
        if active:
            configured[family_name] = active
    return configured


def _loop_seconds() -> int:
    try:
        return max(5, int(os.getenv("IA_RUNTIME_SCHEDULER_LOOP_SECONDS", "15")))
    except Exception:
        return 15


def _startup_delay_seconds() -> int:
    try:
        return max(5, int(os.getenv("IA_RUNTIME_SCHEDULER_STARTUP_DELAY_SECONDS", "10")))
    except Exception:
        return 10


def _surface_interval(surface_name: str) -> int:
    if surface_name == "daily_brief":
        from app.services.daily_brief_slots import settings_slot_hours, slot_interval_seconds

        return max(60, slot_interval_seconds(settings_slot_hours(get_settings())))
    env_name = f"IA_RUNTIME_REFRESH_{surface_name.upper()}_SECONDS"
    default = _SURFACE_DEFAULT_INTERVAL.get(surface_name, 3600)
    try:
        return max(60, int(os.getenv(env_name, str(default))))
    except Exception:
        return default


def _next_run_at(spec: RuntimeJobSpec, *, reference: datetime | None = None) -> str:
    current = reference or _now()
    if spec.kind == "surface_refresh" and spec.target == "daily_brief":
        from app.services.daily_brief_slots import next_slot_start, settings_slot_hours

        return next_slot_start(current, hours=settings_slot_hours(get_settings())).isoformat()
    return (current + timedelta(seconds=spec.interval_seconds)).isoformat()


def _blueprint_market_interval_seconds(kind: str) -> int:
    settings = get_settings()
    mapping = {
        "series_refresh": int(getattr(settings, "blueprint_market_series_refresh_seconds", 86_400) or 86_400),
        "forecast_refresh": int(getattr(settings, "blueprint_market_forecast_refresh_seconds", 86_400) or 86_400),
        "identity_audit": int(getattr(settings, "blueprint_market_identity_audit_seconds", 604_800) or 604_800),
    }
    fallback = mapping.get(kind, 86_400)
    env_name = f"IA_BLUEPRINT_MARKET_{kind.upper()}_SECONDS"
    try:
        return max(300, int(os.getenv(env_name, str(fallback))))
    except Exception:
        return fallback


def _job_specs() -> list[RuntimeJobSpec]:
    specs = [
        RuntimeJobSpec(
            job_id=f"refresh_{surface_name}",
            kind="surface_refresh",
            target=surface_name,
            interval_seconds=_surface_interval(surface_name),
            description=f"Refresh {surface_name.replace('_', ' ')} provider snapshots.",
        )
        for surface_name in _JOB_ORDER
    ]
    specs.append(
        RuntimeJobSpec(
            job_id="forecast_probe",
            kind="forecast_probe",
            target="forecast",
            interval_seconds=max(45, int(os.getenv("IA_FORECAST_AUTOSTART_RECHECK_SECONDS", "45"))),
            description="Refresh forecast provider readiness and probe records.",
        )
    )
    specs.append(
        RuntimeJobSpec(
            job_id="blueprint_daily_source_scan",
            kind="blueprint_daily_source_scan",
            target="blueprint_explorer",
            interval_seconds=max(3600, int(os.getenv("IA_BLUEPRINT_DAILY_SOURCE_SCAN_SECONDS", "43200"))),
            description="Diff latest Blueprint Explorer snapshots and emit material daily Changes events.",
        )
    )
    if _blueprint_market_scheduler_enabled():
        specs.extend(
            [
                RuntimeJobSpec(
                    job_id="blueprint_market_series_refresh",
                    kind="blueprint_market_series_refresh",
                    target="series_refresh",
                    interval_seconds=_blueprint_market_interval_seconds("series_refresh"),
                    description="Refresh canonical Blueprint Candidate market series on the scheduled lane.",
                ),
                RuntimeJobSpec(
                    job_id="blueprint_market_forecast_refresh",
                    kind="blueprint_market_forecast_refresh",
                    target="forecast_refresh",
                    interval_seconds=_blueprint_market_interval_seconds("forecast_refresh"),
                    description="Generate stale Blueprint Candidate Kronos support bundles on the scheduled lane.",
                ),
                RuntimeJobSpec(
                    job_id="blueprint_market_identity_audit",
                    kind="blueprint_market_identity_audit",
                    target="identity_audit",
                    interval_seconds=_blueprint_market_interval_seconds("identity_audit"),
                    description="Audit Blueprint Candidate market identities, freshness, and gap states.",
                ),
            ]
        )
    return specs


def _connection() -> sqlite3.Connection:
    return connect(get_db_path())


def _run_surface_refresh(surface_name: str, *, force_refresh: bool) -> dict[str, Any]:
    from app.services.blueprint_refresh_monitor import (
        BLUEPRINT_WRITE_REFRESH_SCOPE,
        claim_refresh_scope,
        record_blueprint_refresh_skip,
        refresh_scope_snapshot,
        release_refresh_scope,
        run_blueprint_candidate_refresh,
    )
    from app.v2.donors.providers import SQLiteProviderDonor

    scope_claim: dict[str, Any] | None = None
    if surface_name == "blueprint":
        scope_claim = claim_refresh_scope(BLUEPRINT_WRITE_REFRESH_SCOPE, owner="runtime_scheduler_blueprint_refresh")
        if not scope_claim.get("acquired"):
            with _connection() as conn:
                skip_record = record_blueprint_refresh_skip(
                    conn,
                    trigger_source="runtime_scheduler_blueprint_refresh",
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
                }

    with _connection() as conn:
        try:
            donor = SQLiteProviderDonor(conn, get_settings())
            if surface_name == "daily_brief":
                return donor.refresh_daily_brief(force_refresh=force_refresh)
            if surface_name == "dashboard":
                return donor.refresh_dashboard(force_refresh=force_refresh)
            if surface_name == "blueprint":
                payload = donor.refresh_blueprint(force_refresh=force_refresh)
                candidate_refresh = run_blueprint_candidate_refresh(
                    conn,
                    settings=get_settings(),
                    trigger_source="runtime_scheduler" if not force_refresh else "runtime_scheduler_force",
                )
                payload["candidate_refresh"] = {
                    "status": candidate_refresh.get("status"),
                    "success_count": candidate_refresh.get("success_count"),
                    "failure_count": candidate_refresh.get("failure_count"),
                }
                return payload
        finally:
            if scope_claim and scope_claim.get("acquired"):
                release_refresh_scope(
                    BLUEPRINT_WRITE_REFRESH_SCOPE,
                    owner="runtime_scheduler_blueprint_refresh",
                    result_status="finished",
                )
    raise KeyError(surface_name)


def _run_blueprint_market_job(kind: str) -> dict[str, Any]:
    from app.v2.blueprint_market.blueprint_candidate_forecast_service import run_market_forecast_refresh_lane
    from app.v2.blueprint_market.series_refresh_service import run_market_identity_gap_audit, run_market_series_refresh_lane

    with _connection() as conn:
        if kind == "series_refresh":
            return run_market_series_refresh_lane(conn, stale_only=True)
        if kind == "forecast_refresh":
            return run_market_forecast_refresh_lane(stale_only=True)
        if kind == "identity_audit":
            return run_market_identity_gap_audit(conn)
    raise KeyError(kind)


def _run_job(spec: RuntimeJobSpec, *, force_refresh: bool = False) -> dict[str, Any]:
    started_at = _now_iso()
    outcome: dict[str, Any]
    try:
        if spec.kind == "surface_refresh":
            payload = _run_surface_refresh(spec.target, force_refresh=force_refresh)
            outcome = {
                "status": "ok",
                "summary": f"{spec.target} refresh completed.",
                "payload_summary": {
                    "surface_name": payload.get("surface_name"),
                    "item_count": len(list(payload.get("items") or [])),
                    "sufficiency": dict(payload.get("sufficiency") or {}),
                },
            }
        elif spec.kind == "blueprint_daily_source_scan":
            from app.v2.surfaces.changes.blueprint_daily_scan import run_blueprint_daily_source_scan

            payload = run_blueprint_daily_source_scan()
            outcome = {
                "status": "ok" if payload.get("status") in {"success", "partial"} else "partial",
                "summary": "Blueprint daily source scan completed.",
                "payload_summary": {
                    "trading_day": payload.get("trading_day"),
                    "emitted_event_count": int(payload.get("emitted_event_count") or 0),
                    "material_candidate_count": int(payload.get("material_candidate_count") or 0),
                    "no_material_change": bool(payload.get("no_material_change")),
                },
            }
        elif spec.kind.startswith("blueprint_market_"):
            payload = _run_blueprint_market_job(spec.target)
            outcome = {
                "status": "ok" if str(payload.get("status") or "") in {"ok", "succeeded"} else "partial",
                "summary": f"Blueprint market job {spec.target} completed.",
                "payload_summary": {
                    "eligible_count": int(dict(payload.get("scope") or {}).get("eligible_count") or 0),
                    "refreshed_count": int(payload.get("refreshed_count") or 0),
                    "served_last_good_count": int(payload.get("served_last_good_count") or 0),
                    "skipped_count": int(payload.get("skipped_count") or 0),
                    "suppressed_count": int(payload.get("suppressed_count") or 0),
                    "failure_count": int(payload.get("failure_count") or 0),
                    "stale_count": int(payload.get("stale_count") or 0),
                    "degraded_count": int(payload.get("degraded_count") or 0),
                    "broken_count": int(payload.get("broken_count") or 0),
                },
            }
        else:
            payload = forecast_readiness_payload()
            outcome = {
                "status": "ok",
                "summary": "Forecast readiness refreshed.",
                "payload_summary": {
                    "ready_count": int(payload.get("ready_count") or 0),
                    "external_support_active": bool(payload.get("external_support_active")),
                },
            }
    except Exception as exc:  # noqa: BLE001
        logger.exception("V2 runtime job failed", extra={"job_id": spec.job_id})
        outcome = {
            "status": "error",
            "summary": str(exc),
            "payload_summary": {},
        }

    finished_at = _now_iso()
    with _LOCK:
        current = dict(_JOB_STATE.get(spec.job_id) or {})
        failure_streak = int(current.get("failure_streak") or 0)
        if outcome["status"] == "ok":
            failure_streak = 0
        else:
            failure_streak += 1
        _JOB_STATE[spec.job_id] = {
            "job_id": spec.job_id,
            "kind": spec.kind,
            "target": spec.target,
            "description": spec.description,
            "interval_seconds": spec.interval_seconds,
            "last_started_at": started_at,
            "last_finished_at": finished_at,
            "last_status": outcome["status"],
            "last_summary": outcome["summary"],
            "payload_summary": outcome["payload_summary"],
            "failure_streak": failure_streak,
            "next_run_at": _next_run_at(spec, reference=_now()),
        }
        return dict(_JOB_STATE[spec.job_id])


def _ensure_job_state() -> None:
    with _LOCK:
        startup_next_run_at = (_now() + timedelta(seconds=_startup_delay_seconds())).isoformat()
        for spec in _job_specs():
            if spec.job_id in _JOB_STATE:
                continue
            next_run_at = (
                _next_run_at(spec, reference=_now())
                if spec.kind == "surface_refresh" and spec.target == "daily_brief"
                else startup_next_run_at
            )
            _JOB_STATE[spec.job_id] = {
                "job_id": spec.job_id,
                "kind": spec.kind,
                "target": spec.target,
                "description": spec.description,
                "interval_seconds": spec.interval_seconds,
                "last_started_at": None,
                "last_finished_at": None,
                "last_status": "idle",
                "last_summary": "Not run yet.",
                "payload_summary": {},
                "failure_streak": 0,
                "next_run_at": next_run_at,
            }


def _due_jobs() -> list[RuntimeJobSpec]:
    _ensure_job_state()
    now = _now()
    due: list[RuntimeJobSpec] = []
    with _LOCK:
        for spec in _job_specs():
            next_run_at = str(dict(_JOB_STATE.get(spec.job_id) or {}).get("next_run_at") or "")
            try:
                parsed = datetime.fromisoformat(next_run_at)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
            except Exception:
                parsed = now
            if parsed <= now:
                due.append(spec)
    return due


def _worker_loop() -> None:
    while not _STOP_EVENT.is_set():
        for spec in _due_jobs():
            _run_job(spec)
        _STOP_EVENT.wait(_loop_seconds())


def _warm_surface_snapshots() -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    builders = (
        ("daily_brief", "app.v2.surfaces.daily_brief.contract_builder", "build"),
        ("blueprint_explorer", "app.v2.surfaces.blueprint.explorer_contract_builder", "build"),
    )
    for surface_id, module_name, builder_name in builders:
        try:
            module = __import__(module_name, fromlist=[builder_name])
            builder = getattr(module, builder_name)
            contract = builder()
            results.append(
                {
                    "surface_id": surface_id,
                    "status": "ok",
                    "generated_at": contract.get("generated_at"),
                    "snapshot_id": contract.get("surface_snapshot_id"),
                }
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("V2 startup surface warmup failed", extra={"surface_id": surface_id})
            results.append({"surface_id": surface_id, "status": "error", "message": str(exc)})
    return {
        "surface_results": results,
        "ok": all(item.get("status") == "ok" for item in results),
    }


def _warm_benchmark_truth() -> dict[str, Any]:
    try:
        from app.v2.sources.benchmark_truth_adapter import warm_history_cache

        results = warm_history_cache(surface_name="candidate_report")
    except Exception as exc:  # noqa: BLE001
        logger.exception("V2 startup benchmark warmup failed")
        return {"status": "error", "message": str(exc), "results": []}
    return {
        "status": "ok" if all(bool(item.get("usable_history")) for item in results) else "partial",
        "results": results,
    }


def _queue_startup_refreshes(*, force_refresh: bool = False) -> list[dict[str, Any]]:
    queued: list[dict[str, Any]] = []

    def _runner(surface_name: str) -> None:
        try:
            trigger_surface_refresh(surface_name, force_refresh=force_refresh)
        except Exception:  # noqa: BLE001
            logger.exception("V2 startup refresh queue failed", extra={"surface_name": surface_name})

    for surface_name in _JOB_ORDER:
        threading.Thread(
            target=_runner,
            args=(surface_name,),
            name=f"v2-startup-refresh-{surface_name}",
            daemon=True,
        ).start()
        queued.append({"surface_name": surface_name, "status": "queued"})
    return queued


def run_startup_warmup() -> dict[str, Any]:
    global _STARTUP_WARMUP_COMPLETED
    if _is_test_mode():
        return {"status": "skipped", "reason": "test_mode"}
    with _LOCK:
        if _STARTUP_WARMUP_COMPLETED:
            return {"status": "skipped", "reason": "already_completed"}
    refresh_results = _queue_startup_refreshes(force_refresh=False)
    forecast_spec = next(spec for spec in _job_specs() if spec.job_id == "forecast_probe")
    _run_job(forecast_spec, force_refresh=False)
    benchmark_results = _warm_benchmark_truth()
    snapshot_results = _warm_surface_snapshots()
    payload = {
        "status": "ok" if snapshot_results.get("ok") and benchmark_results.get("status") == "ok" else "partial",
        "completed_at": _now_iso(),
        "refresh_results": refresh_results,
        "benchmark_results": benchmark_results,
        "snapshot_results": snapshot_results,
    }
    with _LOCK:
        _STARTUP_WARMUP_COMPLETED = True
    return payload


def start_runtime_worker() -> None:
    global _WORKER
    if _is_test_mode() or not _scheduler_enabled():
        return
    with _LOCK:
        if _WORKER is not None and _WORKER.is_alive():
            return
        _STOP_EVENT.clear()
        _ensure_job_state()
        _WORKER = threading.Thread(target=_worker_loop, name="v2-runtime-scheduler", daemon=True)
        _WORKER.start()


def stop_runtime_worker() -> None:
    global _WORKER
    _STOP_EVENT.set()
    with _LOCK:
        _WORKER = None


def runtime_jobs_payload() -> dict[str, Any]:
    _ensure_job_state()
    with _LOCK:
        jobs = [dict(_JOB_STATE[job_id]) for job_id in sorted(_JOB_STATE)]
    return {
        "generated_at": _now_iso(),
        "scheduler_enabled": _scheduler_enabled(),
        "worker_alive": bool(_WORKER and _WORKER.is_alive()),
        "jobs": jobs,
    }


def runtime_alerts_payload() -> dict[str, Any]:
    with _connection() as conn:
        readiness = build_surface_readiness_payload(conn, get_settings())
        conflicts = build_conflict_payload(conn, get_settings())
    forecast = forecast_readiness_payload()
    alerts: list[dict[str, Any]] = []
    for surface in list(readiness.get("surfaces") or []):
        if str(surface.get("state") or "") == "ready":
            continue
        alerts.append(
            {
                "alert_type": "surface_readiness",
                "severity": "high" if surface.get("state") == "blocked" else "medium",
                "surface_name": surface.get("surface_name"),
                "message": f"{surface.get('surface_name')} is {surface.get('state')}.",
                "issues": surface.get("issues"),
            }
        )
    for item in list(conflicts.get("conflicts") or [])[:20]:
        alerts.append(
            {
                "alert_type": item.get("conflict_type"),
                "severity": item.get("severity"),
                "surface_name": item.get("surface_name"),
                "message": item.get("message"),
                "family_name": item.get("family_name"),
            }
        )
    if not bool(forecast.get("external_support_active")):
        alerts.append(
            {
                "alert_type": "forecast_degraded",
                "severity": "medium",
                "surface_name": None,
                "message": "Forecast support is currently serving deterministic fallback only.",
            }
        )
    return {
        "generated_at": _now_iso(),
        "alert_count": len(alerts),
        "alerts": alerts,
    }


def runtime_status_payload() -> dict[str, Any]:
    jobs = runtime_jobs_payload()
    with _connection() as conn:
        readiness = build_surface_readiness_payload(conn, get_settings())
    degraded_surfaces = [
        item
        for item in list(readiness.get("surfaces") or [])
        if str(item.get("state") or "") != "ready"
    ]
    failed_jobs = [
        item
        for item in list(jobs.get("jobs") or [])
        if str(item.get("last_status") or "") == "error" or int(item.get("failure_streak") or 0) > 0
    ]
    env_path = get_repo_root() / "ops" / "env" / ".env.local"
    return {
        "generated_at": _now_iso(),
        "active_app": "app.v2.app:app",
        "active_env_path": str(env_path),
        "active_env_loaded": env_path.exists(),
        "scheduler_enabled": _scheduler_enabled(),
        "refresh_enabled": _scheduler_enabled(),
        "worker_alive": jobs["worker_alive"],
        "startup_warmup_completed": _STARTUP_WARMUP_COMPLETED,
        "serving_mode": (
            "scheduled_refresh"
            if _scheduler_enabled() and jobs["worker_alive"]
            else "warm_snapshots_plus_on_demand"
            if _STARTUP_WARMUP_COMPLETED
            else "cold_on_demand"
        ),
        "active_frontend": "frontend-cortex",
        "configured_providers_by_family": _configured_providers_by_family(),
        "job_count": len(list(jobs.get("jobs") or [])),
        "surface_readiness": readiness,
        "alert_count": len(degraded_surfaces) + len(failed_jobs),
    }


def trigger_surface_refresh(surface_name: str, *, force_refresh: bool = True) -> dict[str, Any]:
    spec = next((item for item in _job_specs() if item.kind == "surface_refresh" and item.target == surface_name), None)
    if spec is None:
        raise KeyError(surface_name)
    return _run_job(spec, force_refresh=force_refresh)


def trigger_family_refresh(family_name: str, *, force_refresh: bool = True) -> dict[str, Any]:
    normalized_family = str(family_name or "").strip()
    endpoint_family = _FAMILY_ALIAS_TO_ENDPOINT.get(normalized_family, normalized_family)
    affected_surfaces = [
        surface_name
        for surface_name in SURFACE_TARGET_FAMILIES
        if endpoint_family in set(surface_families(surface_name))
    ]
    results = [trigger_surface_refresh(surface_name, force_refresh=force_refresh) for surface_name in affected_surfaces]
    return {
        "generated_at": _now_iso(),
        "family_name": normalized_family,
        "endpoint_family": endpoint_family,
        "affected_surfaces": affected_surfaces,
        "results": results,
    }
