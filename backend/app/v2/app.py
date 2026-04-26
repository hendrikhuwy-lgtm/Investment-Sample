"""Minimal V2-only FastAPI app — serves only /api/v2/* routes without loading legacy services."""
from __future__ import annotations

import threading
import os
import base64
import hmac
import importlib
import socket
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles

from app.v2.forecasting.runtime_manager import start_forecast_runtime_worker, stop_forecast_runtime_worker
from app.v2.runtime.bootstrap import ensure_v2_runtime_bootstrap
from app.v2.runtime.service import run_startup_warmup, start_runtime_worker, stop_runtime_worker
from app.v2.router import router

_repo_root = Path(__file__).resolve().parents[3]
_cortex_frontend_stop = threading.Event()
_cortex_frontend_lock = threading.Lock()
_cortex_frontend_thread: threading.Thread | None = None
_cortex_frontend_process: subprocess.Popen[bytes] | None = None


def _env_enabled(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _production_mode() -> bool:
    for name in ("IA_ENV", "APP_ENV", "ENVIRONMENT", "NODE_ENV"):
        if os.getenv(name, "").strip().lower() in {"prod", "production"}:
            return True
    return False


def _test_mode() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ or _env_enabled("IA_TEST_MODE", "0")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except ValueError:
        return default


def _cortex_canonical_frontend_url() -> str | None:
    raw = (
        os.getenv("IA_CORTEX_FRONTEND_CANONICAL_URL")
        or os.getenv("IA_CORTEX_FRONTEND_DEV_URL")
        or ""
    ).strip()
    if raw:
        return raw.rstrip("/")
    if not _cortex_frontend_autostart_enabled():
        return None
    return f"http://{_connect_host(_cortex_frontend_host())}:{_cortex_frontend_port()}/cortex"


def _cortex_redirect_url(request: Request, full_path: str) -> str | None:
    canonical = _cortex_canonical_frontend_url()
    if not canonical:
        return None
    parsed = urlparse(canonical)
    if not parsed.scheme or not parsed.netloc:
        return None
    if parsed.netloc == request.headers.get("host", ""):
        return None
    base_path = parsed.path.rstrip("/") or "/cortex"
    suffix = full_path.strip("/")
    path = f"{base_path}/{suffix}" if suffix else f"{base_path}/"
    query = request.url.query
    return f"{parsed.scheme}://{parsed.netloc}{path}{'?' + query if query else ''}"


def _cortex_frontend_host() -> str:
    return os.getenv("IA_CORTEX_FRONTEND_HOST", "127.0.0.1").strip() or "127.0.0.1"


def _cortex_frontend_port() -> int:
    return _env_int("IA_CORTEX_FRONTEND_PORT", 5177)


def _cortex_frontend_autostart_enabled() -> bool:
    default = "0" if _production_mode() or _test_mode() else "1"
    return _env_enabled("IA_CORTEX_FRONTEND_AUTOSTART", default)


def _connect_host(host: str) -> str:
    return "127.0.0.1" if host in {"0.0.0.0", "::"} else host


def _tcp_port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((_connect_host(host), port), timeout=0.5):
            return True
    except OSError:
        return False


def _cortex_process_running() -> bool:
    return _cortex_frontend_process is not None and _cortex_frontend_process.poll() is None


def _start_cortex_frontend_dev(host: str, port: int) -> None:
    global _cortex_frontend_process

    frontend_root = _repo_root / "frontend-cortex"
    package_json = frontend_root / "package.json"
    if not package_json.exists():
        return

    log_path = _repo_root / "outbox" / "logs" / "cortex-frontend-dev.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    npm_bin = os.getenv("IA_CORTEX_FRONTEND_NPM", "npm").strip() or "npm"
    env = os.environ.copy()
    env.setdefault("BROWSER", "none")
    log_file = log_path.open("ab")
    try:
        _cortex_frontend_process = subprocess.Popen(
            [npm_bin, "run", "dev", "--", "--host", host, "--port", str(port)],
            cwd=str(frontend_root),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError:
        _cortex_frontend_process = None
    finally:
        log_file.close()


def _ensure_cortex_frontend_running() -> None:
    host = _cortex_frontend_host()
    port = _cortex_frontend_port()
    if _tcp_port_open(host, port):
        return
    with _cortex_frontend_lock:
        if _tcp_port_open(host, port) or _cortex_process_running():
            return
        _start_cortex_frontend_dev(host, port)


def _watch_cortex_frontend() -> None:
    interval = max(1, _env_int("IA_CORTEX_FRONTEND_WATCHDOG_INTERVAL_SECONDS", 5))
    while not _cortex_frontend_stop.is_set():
        _ensure_cortex_frontend_running()
        _cortex_frontend_stop.wait(interval)


def _start_cortex_frontend_watchdog() -> None:
    global _cortex_frontend_thread
    if not _cortex_frontend_autostart_enabled():
        return
    if _cortex_frontend_thread is not None and _cortex_frontend_thread.is_alive():
        return
    _cortex_frontend_stop.clear()
    _cortex_frontend_thread = threading.Thread(
        target=_watch_cortex_frontend,
        name="cortex-frontend-watchdog",
        daemon=True,
    )
    _cortex_frontend_thread.start()


def _stop_cortex_frontend_watchdog() -> None:
    global _cortex_frontend_process
    _cortex_frontend_stop.set()
    with _cortex_frontend_lock:
        process = _cortex_frontend_process
        _cortex_frontend_process = None
    if process is None or process.poll() is not None:
        return
    if not _env_enabled("IA_CORTEX_FRONTEND_STOP_WITH_BACKEND", "1"):
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _auth_enabled() -> bool:
    default = "1" if _production_mode() else "0"
    return _env_enabled("IA_AUTH_ENABLED", default)


def _auth_username() -> str:
    return os.getenv("IA_AUTH_USERNAME", "investor").strip() or "investor"


def _auth_password() -> str:
    return os.getenv("IA_AUTH_PASSWORD", "").strip()


def _auth_bearer_token() -> str:
    return os.getenv("IA_AUTH_BEARER_TOKEN", "").strip()


def _validate_auth_config() -> None:
    if not _auth_enabled():
        return
    if not _auth_password() and not _auth_bearer_token():
        raise RuntimeError(
            "Authentication is enabled but no credential is configured. "
            "Set IA_AUTH_PASSWORD for browser login or IA_AUTH_BEARER_TOKEN for API clients."
        )


def _protected_path(path: str) -> bool:
    if path == "/api/v2/health" and _env_enabled("IA_AUTH_PUBLIC_HEALTH", "0"):
        return False
    return path.startswith("/api/v2") or path == "/cortex" or path.startswith("/cortex/")


def _constant_time_equal(left: str, right: str) -> bool:
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))


def _authorized(request: Request) -> bool:
    authorization = request.headers.get("authorization", "").strip()
    bearer_token = _auth_bearer_token()
    if bearer_token and authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()
        return _constant_time_equal(supplied, bearer_token)

    password = _auth_password()
    if not password or not authorization.lower().startswith("basic "):
        return False
    try:
        decoded = base64.b64decode(authorization[6:].strip(), validate=True).decode("utf-8")
    except Exception:
        return False
    username, separator, supplied_password = decoded.partition(":")
    if not separator:
        return False
    return _constant_time_equal(username, _auth_username()) and _constant_time_equal(supplied_password, password)


def _auth_challenge_response(request: Request) -> Response:
    headers = {"WWW-Authenticate": 'Basic realm="Cortex"'}
    if request.url.path.startswith("/api/"):
        return JSONResponse(
            {"detail": "authentication_required"},
            status_code=401,
            headers=headers,
        )
    return Response("Authentication required", status_code=401, headers=headers)


def _cors_origins() -> list[str]:
    local_origins = [
        "http://localhost:4173",
        "http://localhost:5173",
        "http://localhost:5175",
        "http://localhost:5174",
        "http://localhost:5177",
        "http://127.0.0.1:4173",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5175",
        "http://127.0.0.1:5174",
        "http://127.0.0.1:5177",
    ]
    configured = [
        value.strip().rstrip("/")
        for value in os.getenv("IA_CORS_ORIGINS", "").split(",")
        if value.strip()
    ]
    return list(dict.fromkeys([*local_origins, *configured]))


def _boot_background_services() -> None:
    if not _env_enabled("IA_FORECAST_DEFER_UNTIL_SURFACE_READY", "1"):
        start_forecast_runtime_worker()
    if _env_enabled("IA_RUNTIME_SCHEDULER_ENABLED", "0"):
        start_runtime_worker()
        run_startup_warmup()


def _preload_surface_modules() -> None:
    for module_name in (
        "app.v2.translators.market_signal_translator",
        "app.v2.translators.instrument_truth_translator",
        "app.v2.surfaces.portfolio.contract_builder",
        "app.v2.surfaces.daily_brief.contract_builder",
        "app.v2.surfaces.blueprint.explorer_contract_builder",
        "app.v2.surfaces.changes.contract_builder",
        "app.v2.surfaces.compare.contract_builder",
    ):
        importlib.import_module(module_name)


def _validate_cortex_production_bundle() -> None:
    if not _production_mode():
        return
    if _cortex_frontend_autostart_enabled():
        raise RuntimeError("IA_CORTEX_FRONTEND_AUTOSTART must be 0 in production.")
    index_path = _cortex_dist / "index.html"
    if not index_path.exists():
        raise RuntimeError(
            "Cortex production bundle is missing. Run scripts/build_cortex_production.sh before starting production."
        )


@asynccontextmanager
async def _lifespan(_: FastAPI):
    _validate_auth_config()
    _validate_cortex_production_bundle()
    ensure_v2_runtime_bootstrap()
    _preload_surface_modules()
    _start_cortex_frontend_watchdog()
    threading.Thread(target=_boot_background_services, name="v2-background-bootstrap", daemon=True).start()
    yield
    _stop_cortex_frontend_watchdog()
    stop_runtime_worker()
    stop_forecast_runtime_worker()


app = FastAPI(title="Investment Agent V2", version="0.2.0", lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def require_cortex_auth(request: Request, call_next):
    if request.method == "OPTIONS" or not _auth_enabled() or not _protected_path(request.url.path):
        return await call_next(request)
    if _authorized(request):
        return await call_next(request)
    return _auth_challenge_response(request)


app.include_router(router)

_cortex_dist = _repo_root / "frontend-cortex" / "dist"
if _cortex_dist.exists():
    app.mount("/cortex/assets", StaticFiles(directory=str(_cortex_dist / "assets")), name="cortex_assets")

    @app.get("/cortex", include_in_schema=False)
    @app.get("/cortex/", include_in_schema=False)
    @app.get("/cortex/{full_path:path}", include_in_schema=False)
    async def serve_cortex(request: Request, full_path: str = ""):
        redirect_url = _cortex_redirect_url(request, full_path)
        if redirect_url:
            return RedirectResponse(redirect_url, status_code=307)
        return FileResponse(
            str(_cortex_dist / "index.html"),
            headers={"Cache-Control": "no-store"},
        )
