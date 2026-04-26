from __future__ import annotations

import logging
import os
import shlex
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.config import get_repo_root
from app.v2.forecasting.registry import all_providers, adapter_for, provider_meta


logger = logging.getLogger(__name__)

_LOCK = threading.RLock()
_WORKER: threading.Thread | None = None
_STOP_EVENT = threading.Event()
_PROCESSES: dict[str, subprocess.Popen[str]] = {}
_PROCESS_META: dict[str, dict[str, Any]] = {}
_LOG_HANDLES: dict[str, Any] = {}


@dataclass(frozen=True)
class LocalProviderSpec:
    provider: str
    repo_dir: Path
    venv_uvicorn: Path
    module: str = "server:app"


def _env_true(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _runtime_enabled() -> bool:
    return _env_true("IA_FORECAST_AUTOSTART_ENABLED", "1")


def _include_benchmarks() -> bool:
    return _env_true("IA_FORECAST_AUTOSTART_INCLUDE_BENCHMARKS", "1")


def _recheck_seconds() -> int:
    try:
        return max(10, int(os.getenv("IA_FORECAST_AUTOSTART_RECHECK_SECONDS", "45")))
    except Exception:
        return 45


def _warmup_seconds() -> int:
    try:
        return max(5, int(os.getenv("IA_FORECAST_AUTOSTART_WARMUP_SECONDS", "90")))
    except Exception:
        return 90


def _is_test_mode() -> bool:
    return bool(os.getenv("PYTEST_CURRENT_TEST"))


def _local_specs() -> dict[str, LocalProviderSpec]:
    home = Path.home()
    return {
        "chronos": LocalProviderSpec(
            provider="chronos",
            repo_dir=home / "chronos-server",
            venv_uvicorn=home / "chronos-server" / ".venv" / "bin" / "uvicorn",
        ),
        "timesfm": LocalProviderSpec(
            provider="timesfm",
            repo_dir=home / "timesfm",
            venv_uvicorn=home / "timesfm" / ".venv" / "bin" / "uvicorn",
        ),
        "moirai": LocalProviderSpec(
            provider="moirai",
            repo_dir=home / "uni2ts",
            venv_uvicorn=home / "uni2ts" / ".venv" / "bin" / "uvicorn",
        ),
        "lagllama": LocalProviderSpec(
            provider="lagllama",
            repo_dir=home / "lag-llama",
            venv_uvicorn=home / "lag-llama" / ".venv" / "bin" / "uvicorn",
        ),
    }


def _tracked_providers() -> list[str]:
    return all_providers(include_benchmarks=_include_benchmarks())


def _parse_base_url(url: str) -> tuple[str, int] | None:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.hostname or parsed.port is None:
        return None
    return parsed.hostname, int(parsed.port)


def _is_local_url(url: str) -> bool:
    parsed = _parse_base_url(url)
    if parsed is None:
        return False
    host, _port = parsed
    return host in {"127.0.0.1", "localhost"}


def _port_open(url: str, *, timeout_seconds: float = 0.5) -> bool:
    parsed = _parse_base_url(url)
    if parsed is None:
        return False
    host, port = parsed
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def _log_dir() -> Path:
    path = get_repo_root() / "outbox" / "forecast_sidecars"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _start_command(provider: str) -> str | None:
    env_name = f"IA_{provider.upper()}_START_CMD"
    explicit = os.getenv(env_name, "").strip()
    if explicit:
        return explicit

    adapter = adapter_for(provider)
    spec = _local_specs().get(provider)
    if adapter is None or spec is None:
        return None
    base_url = adapter.base_url()
    if not base_url or not _is_local_url(base_url):
        return None
    parsed = _parse_base_url(base_url)
    if parsed is None or not spec.repo_dir.exists():
        return None
    host, port = parsed
    uvicorn_bin = spec.venv_uvicorn if spec.venv_uvicorn.exists() else shutil.which("uvicorn")
    if not uvicorn_bin:
        return None
    return (
        f"{shlex.quote(str(uvicorn_bin))} {shlex.quote(spec.module)} "
        f"--host {shlex.quote(str(host))} --port {int(port)} --app-dir {shlex.quote(str(spec.repo_dir))}"
    )


def _process_record(provider: str) -> dict[str, Any]:
    proc = _PROCESSES.get(provider)
    meta = dict(_PROCESS_META.get(provider) or {})
    running = bool(proc and proc.poll() is None)
    return {
        "provider": provider,
        "pid": None if proc is None else proc.pid,
        "running": running,
        **meta,
    }


def _launch_provider(provider: str) -> dict[str, Any] | None:
    adapter = adapter_for(provider)
    spec = _local_specs().get(provider)
    if adapter is None or spec is None:
        return None
    cmd = _start_command(provider)
    if not cmd:
        return None
    log_path = _log_dir() / f"{provider}.log"
    old_handle = _LOG_HANDLES.pop(provider, None)
    if old_handle is not None:
        try:
            old_handle.close()
        except Exception:
            logger.exception("Failed closing stale forecast sidecar log", extra={"provider": provider})
    log_handle = open(log_path, "a", encoding="utf-8")
    process = subprocess.Popen(
        ["bash", "-lc", cmd],
        cwd=str(spec.repo_dir),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    _PROCESSES[provider] = process
    _LOG_HANDLES[provider] = log_handle
    _PROCESS_META[provider] = {
        "base_url": adapter.base_url(),
        "local_repo_dir": str(spec.repo_dir),
        "command": cmd,
        "log_path": str(log_path),
        "started_at": time.time(),
        "autostart_managed": True,
    }
    logger.info("Started forecast provider sidecar", extra={"provider": provider, "pid": process.pid})
    return _process_record(provider)


def ensure_forecast_sidecars_started() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with _LOCK:
        for provider in _tracked_providers():
            adapter = adapter_for(provider)
            if adapter is None:
                rows.append({"provider": provider, "autostart_managed": False, "reason": "no_adapter"})
                continue
            base_url = adapter.base_url()
            if not adapter.configured() or not _is_local_url(base_url):
                rows.append(
                    {
                        "provider": provider,
                        "autostart_managed": False,
                        "base_url": base_url,
                        "reason": "not_local_or_not_configured",
                    }
                )
                continue
            if _port_open(base_url):
                rows.append(
                    {
                        **_process_record(provider),
                        "provider": provider,
                        "base_url": base_url,
                        "port_open": True,
                        "autostart_managed": bool(_PROCESS_META.get(provider)),
                    }
                )
                continue
            proc = _PROCESSES.get(provider)
            meta = _PROCESS_META.get(provider) or {}
            if proc is not None and proc.poll() is None:
                if time.time() - float(meta.get("started_at") or 0.0) < _warmup_seconds():
                    rows.append(
                        {
                            **_process_record(provider),
                            "provider": provider,
                            "base_url": base_url,
                            "port_open": False,
                            "autostart_managed": True,
                            "reason": "warming_up",
                        }
                    )
                    continue
            launched = _launch_provider(provider)
            rows.append(
                launched
                or {
                    "provider": provider,
                    "base_url": base_url,
                    "port_open": False,
                    "autostart_managed": False,
                    "reason": "no_start_command",
                }
            )
    return rows


def forecast_runtime_status() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with _LOCK:
        for provider in _tracked_providers():
            adapter = adapter_for(provider)
            meta = provider_meta(provider)
            base_url = adapter.base_url() if adapter is not None else ""
            command = _start_command(provider)
            rows.append(
                {
                    **_process_record(provider),
                    "provider": provider,
                    "configured": bool(adapter and adapter.configured()),
                    "base_url": base_url,
                    "port_open": _port_open(base_url) if base_url else False,
                    "ready_target": bool(meta and not bool(meta.get("benchmark_only"))),
                    "benchmark_only": bool(meta.get("benchmark_only")),
                    "local_target": _is_local_url(base_url) if base_url else False,
                    "autostart_enabled": _runtime_enabled(),
                    "command_available": bool(command),
                }
            )
    return rows


def _worker_loop() -> None:
    while not _STOP_EVENT.is_set():
        try:
            ensure_forecast_sidecars_started()
        except Exception:
            logger.exception("Forecast sidecar ensure loop failed")
        _STOP_EVENT.wait(_recheck_seconds())


def start_forecast_runtime_worker(*, force: bool = False) -> None:
    global _WORKER
    if _is_test_mode() or (not force and not _runtime_enabled()):
        return
    with _LOCK:
        if _WORKER is not None and _WORKER.is_alive():
            return
        _STOP_EVENT.clear()
        ensure_forecast_sidecars_started()
        _WORKER = threading.Thread(target=_worker_loop, name="forecast-sidecar-runtime", daemon=True)
        _WORKER.start()


def stop_forecast_runtime_worker() -> None:
    global _WORKER
    _STOP_EVENT.set()
    with _LOCK:
        for provider, process in list(_PROCESSES.items()):
            if process.poll() is None:
                try:
                    process.terminate()
                except Exception:
                    logger.exception("Failed terminating forecast sidecar", extra={"provider": provider})
        for provider, handle in list(_LOG_HANDLES.items()):
            try:
                handle.close()
            except Exception:
                logger.exception("Failed closing forecast sidecar log", extra={"provider": provider})
        _PROCESSES.clear()
        _PROCESS_META.clear()
        _LOG_HANDLES.clear()
        _WORKER = None
