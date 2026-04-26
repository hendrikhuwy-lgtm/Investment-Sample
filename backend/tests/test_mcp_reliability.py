from __future__ import annotations

import json
import socket
import threading
import time
import urllib.error
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from app.config import Settings
from app.services.ingest_mcp import (
    MCPLiveCoverageError,
    classify_endpoint_type,
    ingest_mcp_omni,
    parse_registry_snapshot,
    resolve_server_endpoints,
)
from app.services.mcp_client import MCPClient, _classify_error, _resolve_ca_bundle_candidates, ipv6_health_probe


def test_endpoint_type_classification() -> None:
    assert classify_endpoint_type("https://example.com/events/sse", "streamable-http") == "sse"
    assert classify_endpoint_type("wss://example.com/mcp", "websocket") == "websocket"
    assert classify_endpoint_type("https://example.com/mcp", "streamable-http") == "http"
    assert classify_endpoint_type("custom://example.com/mcp", "custom") == "unknown_remote_type"


def test_endpoint_ordering_prefers_http_then_sse_then_websocket() -> None:
    record = {
        "server": {
            "name": "x/server",
            "remotes": [
                {"type": "websocket", "url": "wss://example.com/socket"},
                {"type": "sse", "url": "https://example.com/sse"},
                {"type": "streamable-http", "url": "https://example.com/mcp"},
            ],
        }
    }

    resolved = resolve_server_endpoints(record)
    assert [item["endpoint_type"] for item in resolved] == ["http", "sse", "websocket"]


def test_dns_error_classification() -> None:
    exc = socket.gaierror(8, "nodename nor servname provided, or not known")
    error_class, detail = _classify_error(
        exc,
        family=socket.AF_INET,
        via_proxy=False,
        ipv4_succeeded=False,
    )
    assert error_class == "dns_err"
    assert "errno=8" in detail


def test_http_401_classification_maps_to_auth_required() -> None:
    exc = urllib.error.HTTPError(
        url="https://example.com/mcp",
        code=401,
        msg="Unauthorized",
        hdrs={},
        fp=None,
    )
    error_class, detail = _classify_error(
        exc,
        family=None,
        via_proxy=False,
        ipv4_succeeded=False,
    )
    assert error_class == "auth_required"
    assert "status=401" in detail


def test_parse_registry_snapshot_dedupes_server_name(tmp_path: Path) -> None:
    snapshot = tmp_path / "registry.json"
    snapshot.write_text(
        json.dumps(
            {
                "servers": [
                    {"server": {"name": "dup/server", "remotes": [{"type": "streamable-http", "url": "https://one.example/mcp"}]}},
                    {"server": {"name": "dup/server", "remotes": [{"type": "streamable-http", "url": "https://two.example/mcp"}]}},
                ]
            }
        ),
        encoding="utf-8",
    )
    parsed = parse_registry_snapshot(snapshot)
    assert len(parsed) == 1
    assert parsed[0]["server"]["name"] == "dup/server"


def test_proxy_mode_force_direct_ignores_proxy_values() -> None:
    client = MCPClient(
        endpoint="https://example.com/mcp",
        proxy_mode="force_direct",
        http_proxy="http://proxy.local:8080",
        https_proxy="http://proxy.local:8080",
    )
    assert client._proxy_for_request() is None  # noqa: SLF001


def test_proxy_mode_force_proxy_requires_proxy() -> None:
    client = MCPClient(
        endpoint="https://example.com/mcp",
        proxy_mode="force_proxy",
        http_proxy="",
        https_proxy="",
    )
    with pytest.raises(RuntimeError):
        client._proxy_for_request()  # noqa: SLF001


def test_resolve_ca_bundle_candidates_prefers_existing_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    explicit = tmp_path / "explicit.pem"
    explicit.write_text("dummy", encoding="utf-8")
    env_ca = tmp_path / "env.pem"
    env_ca.write_text("dummy", encoding="utf-8")

    monkeypatch.setenv("SSL_CERT_FILE", str(env_ca))
    monkeypatch.setattr(
        "app.services.mcp_client.ssl.get_default_verify_paths",
        lambda: type(
            "VerifyPaths",
            (),
            {
                "cafile": str(tmp_path / "missing-default.pem"),
                "openssl_cafile": str(tmp_path / "missing-openssl.pem"),
            },
        )(),
    )

    candidates = _resolve_ca_bundle_candidates(str(explicit))
    assert candidates[0] == str(explicit)
    assert str(env_ca) in candidates


def test_mcp_client_uses_configured_ca_bundle(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cafile = tmp_path / "bundle.pem"
    cafile.write_text("dummy", encoding="utf-8")

    calls: list[str | None] = []

    class DummyContext:
        def wrap_socket(self, sock, server_hostname=None):  # noqa: ANN001
            _ = server_hostname
            return sock

    def fake_create_default_context(*, cafile=None):  # noqa: ANN001
        calls.append(cafile)
        return DummyContext()

    monkeypatch.setattr("app.services.mcp_client.ssl.create_default_context", fake_create_default_context)
    monkeypatch.setattr("app.services.mcp_client.os.path.exists", lambda path: path == str(cafile))
    monkeypatch.setattr(
        "app.services.mcp_client.ssl.get_default_verify_paths",
        lambda: type("VerifyPaths", (), {"cafile": None, "openssl_cafile": None})(),
    )
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)

    client = MCPClient(endpoint="https://example.com/mcp", tls_ca_bundle=str(cafile))
    _ = client._get_ssl_context()  # noqa: SLF001
    assert calls and calls[0] == str(cafile)


def test_force_proxy_handshake_reports_proxy_missing() -> None:
    client = MCPClient(
        endpoint="https://example.com/mcp",
        endpoint_type="http",
        proxy_mode="force_proxy",
        http_proxy="",
        https_proxy="",
    )
    result = client.handshake()
    assert result.ok is False
    assert result.error_class == "proxy_missing"


def test_ipv6_health_logic_reports_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [
        (socket.AF_INET6, socket.SOCK_STREAM, 0, "", ("::1", 443, 0, 0))
    ])

    class DummySock:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def settimeout(self, timeout: float) -> None:
            _ = timeout

        def connect(self, sockaddr) -> None:
            _ = sockaddr
            raise OSError("network unreachable")

        def close(self) -> None:
            return

    monkeypatch.setattr(socket, "socket", lambda *args, **kwargs: DummySock())

    unhealthy, error = ipv6_health_probe(timeout_seconds=0.1)
    assert unhealthy is True
    assert error is not None


def test_websocket_only_server_is_classified_unsupported_ws(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "app.services.ingest_mcp.parse_registry_snapshot",
        lambda _path: [
            {
                "server": {
                    "name": "ws/only",
                    "title": "WS Only",
                    "remotes": [{"type": "websocket", "url": "wss://example.invalid/mcp"}],
                }
            }
        ],
    )

    settings = Settings(
        db_path=str(tmp_path / "test.sqlite"),
        mcp_live_required=False,
        mcp_priority_mode="all",
    )
    result = ingest_mcp_omni(settings=settings, enforce_live_gate=False)

    assert result.connectable_servers == 0
    assert result.live_success_count == 0
    assert result.error_class_counts.get("unknown_endpoint_type") == 1
    assert any(item.get("error_class") == "unknown_endpoint_type" for item in result.errors)


def test_live_coverage_gate_raises_when_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "app.services.ingest_mcp.parse_registry_snapshot",
        lambda _path: [
            {
                "server": {
                    "name": "ws/only",
                    "title": "WS Only",
                    "remotes": [{"type": "websocket", "url": "wss://example.invalid/mcp"}],
                }
            }
        ],
    )

    settings = Settings(
        db_path=str(tmp_path / "test.sqlite"),
        mcp_live_required=True,
        mcp_min_success_ratio=0.70,
        mcp_min_success_count=1,
        mcp_priority_mode="all",
    )

    with pytest.raises(MCPLiveCoverageError):
        ingest_mcp_omni(settings=settings, enforce_live_gate=True)


def test_per_server_budget_stops_repeated_attempts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "app.services.ingest_mcp.parse_registry_snapshot",
        lambda _path: [
            {
                "server": {
                    "name": "budget/server",
                    "remotes": [
                        {"type": "streamable-http", "url": "https://one.example/mcp"},
                        {"type": "streamable-http", "url": "https://two.example/mcp"},
                    ],
                }
            }
        ],
    )

    class DummyClient:
        auth_token = None

    monkeypatch.setattr(
        "app.services.ingest_mcp.connect_server",
        lambda endpoint, auth, settings: DummyClient(),
    )

    calls = {"n": 0}

    def fake_handshake(_client):
        calls["n"] += 1
        time.sleep(0.05)
        return {
            "handshake_ok": False,
            "tools_count": 0,
            "resources_count": 0,
            "auth_used": False,
            "errors": ["timeout in test"],
            "error_class": "connect_timeout",
            "error_detail": "timeout in test",
            "endpoint_host": "one.example",
            "endpoint_port": 443,
            "ip_family_attempted": "ipv4",
            "handshake": {},
            "tools": [],
            "resources": [],
            "transport": "http",
        }

    monkeypatch.setattr("app.services.ingest_mcp.handshake_and_capabilities", fake_handshake)

    settings = Settings(
        db_path=str(tmp_path / "test.sqlite"),
        mcp_live_required=False,
        mcp_server_budget_seconds=0.02,
        mcp_max_workers=1,
        mcp_priority_mode="all",
    )

    result = ingest_mcp_omni(settings=settings, enforce_live_gate=False)

    assert calls["n"] == 1
    assert result.live_success_count == 0


def test_auth_required_without_token_excluded_from_connectable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "app.services.ingest_mcp.parse_registry_snapshot",
        lambda _path: [
            {"server": {"name": "auth/server", "remotes": [{"type": "streamable-http", "url": "https://auth.example/mcp"}]}}
        ],
    )

    class DummyClient:
        auth_token = None

    monkeypatch.setattr("app.services.ingest_mcp.connect_server", lambda *args, **kwargs: DummyClient())
    monkeypatch.setattr(
        "app.services.ingest_mcp.handshake_and_capabilities",
        lambda _client: {
            "handshake_ok": False,
            "tools_count": 0,
            "resources_count": 0,
            "auth_used": False,
            "errors": ["status=401"],
            "error_class": "auth_required",
            "error_detail": "status=401",
            "endpoint_host": "auth.example",
            "endpoint_port": 443,
            "ip_family_attempted": "ipv4",
            "handshake": {},
            "tools": [],
            "resources": [],
            "transport": "http",
            "response_status": 401,
        },
    )

    settings = Settings(
        db_path=str(tmp_path / "test.sqlite"),
        mcp_live_required=False,
        mcp_max_workers=1,
        mcp_priority_mode="all",
    )
    result = ingest_mcp_omni(settings=settings, enforce_live_gate=False)

    assert result.connectable_servers == 0
    assert result.error_class_counts.get("auth_required") == 1


def test_dns_failure_excluded_from_connectable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "app.services.ingest_mcp.parse_registry_snapshot",
        lambda _path: [
            {"server": {"name": "dns/server", "remotes": [{"type": "streamable-http", "url": "https://dns.example/mcp"}]}}
        ],
    )

    class DummyClient:
        auth_token = None

    monkeypatch.setattr("app.services.ingest_mcp.connect_server", lambda *args, **kwargs: DummyClient())
    monkeypatch.setattr(
        "app.services.ingest_mcp.handshake_and_capabilities",
        lambda _client: {
            "handshake_ok": False,
            "tools_count": 0,
            "resources_count": 0,
            "auth_used": False,
            "errors": ["nodename nor servname provided"],
            "error_class": "dns_err",
            "error_detail": "nodename nor servname provided",
            "endpoint_host": "dns.example",
            "endpoint_port": 443,
            "ip_family_attempted": "ipv4",
            "handshake": {},
            "tools": [],
            "resources": [],
            "transport": "http",
            "response_status": None,
        },
    )

    settings = Settings(
        db_path=str(tmp_path / "test.sqlite"),
        mcp_live_required=False,
        mcp_max_workers=1,
        mcp_priority_mode="all",
    )
    result = ingest_mcp_omni(settings=settings, enforce_live_gate=False)

    assert result.connectable_servers == 0
    assert result.error_class_counts.get("dns_err") == 1


class _LocalMCPHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        body = json.dumps({"status": "ok", "transport": "http"}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) if length > 0 else b"{}")
        rpc_id = payload.get("id", 1)
        method = payload.get("method", "")

        if method == "initialize":
            result = {"protocolVersion": "2024-11-05"}
        elif method == "tools/list":
            result = {"tools": []}
        elif method == "resources/list":
            result = {"resources": []}
        elif method in {"search", "resources/search", "tools/call", "resources/read"}:
            result = {}
        else:
            result = {}

        body = json.dumps({"jsonrpc": "2.0", "id": rpc_id, "result": result}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        _ = (fmt, args)


@contextmanager
def _run_local_mcp_server():
    try:
        server = HTTPServer(("127.0.0.1", 0), _LocalMCPHandler)
    except PermissionError as exc:
        pytest.skip(f"local socket bind not permitted in this environment: {exc}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def test_local_http_server_integration(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    with _run_local_mcp_server() as server:
        url = f"http://127.0.0.1:{server.server_port}/mcp"
        monkeypatch.setattr(
            "app.services.ingest_mcp.parse_registry_snapshot",
            lambda _path: [
                {"server": {"name": "local/server", "title": "Local", "remotes": [{"type": "streamable-http", "url": url}]}}
            ],
        )

        settings = Settings(
            db_path=str(tmp_path / "test.sqlite"),
            mcp_live_required=False,
            proxy_mode="force_direct",
            mcp_max_workers=1,
            mcp_priority_mode="all",
        )

        result = ingest_mcp_omni(settings=settings, enforce_live_gate=False)

        assert result.total_servers == 1
        assert result.connectable_servers == 1
        assert result.live_success_count == 1


def test_priority_mode_prioritized_only_filters_registry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "app.services.ingest_mcp.parse_registry_snapshot",
        lambda _path: [
            {"server": {"name": "keep/server", "title": "Keep", "remotes": [{"type": "streamable-http", "url": "http://127.0.0.1:9/mcp"}]}},
            {"server": {"name": "drop/server", "title": "Drop", "remotes": [{"type": "streamable-http", "url": "http://127.0.0.1:9/mcp"}]}},
        ],
    )

    settings = Settings(
        db_path=str(tmp_path / "test.sqlite"),
        mcp_live_required=False,
        mcp_priority_mode="prioritized_only",
        mcp_priority_servers="keep/server",
        mcp_max_workers=1,
    )

    result = ingest_mcp_omni(settings=settings, enforce_live_gate=False)
    assert result.total_servers == 1
