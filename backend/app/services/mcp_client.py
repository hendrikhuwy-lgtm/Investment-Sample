from __future__ import annotations

import base64
import json
import os
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.config import load_version_contract

CLIENT_BACKEND_VERSION = str(load_version_contract().get("backend_version") or "0.1.0")


def _resolve_ca_bundle_candidates(explicit_path: str | None) -> list[str]:
    candidates: list[str] = []

    def add(path: str | None) -> None:
        if not path:
            return
        value = str(path).strip()
        if not value or value in candidates:
            return
        if os.path.exists(value):
            candidates.append(value)

    add(explicit_path)
    add(os.getenv("SSL_CERT_FILE", ""))
    verify_paths = ssl.get_default_verify_paths()
    add(getattr(verify_paths, "cafile", None))
    add(getattr(verify_paths, "openssl_cafile", None))
    add("/etc/ssl/cert.pem")
    add("/private/etc/ssl/cert.pem")
    return candidates


def _family_name(family: int | None) -> str:
    if family == socket.AF_INET:
        return "ipv4"
    if family == socket.AF_INET6:
        return "ipv6"
    return "unspecified"


def _format_error(exc: BaseException) -> str:
    errno = getattr(exc, "errno", None)
    if errno is not None:
        return f"errno={errno} {type(exc).__name__}: {exc}"
    return f"{type(exc).__name__}: {exc}"


def _classify_error(
    exc: BaseException,
    *,
    family: int | None,
    via_proxy: bool,
    ipv4_succeeded: bool,
) -> tuple[str, str]:
    if isinstance(exc, RuntimeError) and str(exc) == "proxy_missing":
        return "proxy_missing", _format_error(exc)

    if isinstance(exc, socket.gaierror):
        return "dns_err", _format_error(exc)

    if isinstance(exc, ssl.SSLError):
        return "tls_err", _format_error(exc)

    if isinstance(exc, socket.timeout):
        return "connect_timeout", _format_error(exc)

    if isinstance(exc, urllib.error.HTTPError):
        if exc.code in {401, 403}:
            return "auth_required", f"status={exc.code} {_format_error(exc)}"
        if exc.code == 406:
            return "protocol_mismatch", f"status={exc.code} {_format_error(exc)}"
        return "http_err", f"status={exc.code} {_format_error(exc)}"

    if isinstance(exc, urllib.error.URLError):
        reason = exc.reason
        if isinstance(reason, BaseException):
            return _classify_error(reason, family=family, via_proxy=via_proxy, ipv4_succeeded=ipv4_succeeded)
        text = str(reason)
        if "nodename nor servname" in text or "name or service not known" in text:
            return "dns_err", _format_error(exc)
        if "timed out" in text:
            return "connect_timeout", _format_error(exc)
        if via_proxy:
            return "proxy_err", _format_error(exc)
        return "http_err", _format_error(exc)

    if isinstance(exc, ConnectionRefusedError):
        return "connect_refused", _format_error(exc)

    errno = getattr(exc, "errno", None)
    if errno in {61, 111}:  # macOS/Linux ECONNREFUSED
        return "connect_refused", _format_error(exc)

    if errno in {8}:
        return "dns_err", _format_error(exc)

    if family == socket.AF_INET6 and ipv4_succeeded:
        return "ipv6_route_err", _format_error(exc)

    if via_proxy:
        return "proxy_err", _format_error(exc)

    return "http_err", _format_error(exc)


def _parse_proxy_url(proxy_url: str) -> tuple[str, int, str | None]:
    parsed = urllib.parse.urlparse(proxy_url)
    host = parsed.hostname
    if not host:
        raise ValueError(f"invalid proxy url: {proxy_url}")

    port = parsed.port or (443 if parsed.scheme == "https" else 8080)
    auth_header = None
    if parsed.username:
        user = urllib.parse.unquote(parsed.username)
        password = urllib.parse.unquote(parsed.password or "")
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        auth_header = f"Proxy-Authorization: Basic {token}\r\n"
    return host, port, auth_header


def _host_matches_no_proxy(host: str, no_proxy: str) -> bool:
    entries = [item.strip().lower() for item in no_proxy.split(",") if item.strip()]
    host_lower = host.lower()
    for entry in entries:
        if entry == "*":
            return True
        if host_lower == entry:
            return True
        if host_lower.endswith(f".{entry}"):
            return True
    return False


@dataclass
class MCPClientResult:
    ok: bool
    data: Any
    error: str | None = None
    error_class: str | None = None
    error_detail: str | None = None
    endpoint_host: str | None = None
    endpoint_port: int | None = None
    ip_family_attempted: str | None = None
    response_status: int | None = None


class MCPClient:
    """Best-effort MCP client abstraction.

    Supports HTTP JSON-RPC, SSE probing, proxy-aware requests,
    and IPv4/IPv6 raw-socket fallback when urlopen path fails.
    """

    def __init__(
        self,
        endpoint: str,
        endpoint_type: str = "http",
        auth_token: str | None = None,
        connect_timeout_seconds: int = 8,
        read_timeout_seconds: int = 12,
        max_retries: int = 2,
        http_proxy: str = "",
        https_proxy: str = "",
        no_proxy: str = "",
        proxy_mode: str = "auto",
        ipv6_unhealthy: bool = False,
        tls_ca_bundle: str = "",
        tls_verify: bool = True,
    ) -> None:
        self.endpoint = endpoint
        self.endpoint_type = endpoint_type
        self.auth_token = auth_token
        self.connect_timeout_seconds = connect_timeout_seconds
        self.read_timeout_seconds = read_timeout_seconds
        self.max_retries = max_retries

        self.http_proxy = http_proxy
        self.https_proxy = https_proxy
        self.no_proxy = no_proxy
        self.proxy_mode = (proxy_mode or "auto").strip().lower()
        self.ipv6_unhealthy = ipv6_unhealthy
        self.tls_ca_bundle = tls_ca_bundle
        self.tls_verify = tls_verify

        self.scheme = "https"
        self.host = ""
        self.port = 443
        self.path = "/"
        self._set_endpoint(endpoint)

        self._ipv4_succeeded = False
        self._ssl_context: ssl.SSLContext | None = None

    def _set_endpoint(self, endpoint: str) -> None:
        self.endpoint = endpoint
        parsed = urllib.parse.urlparse(endpoint)
        self.scheme = parsed.scheme or "https"
        self.host = parsed.hostname or ""
        self.port = parsed.port or (443 if self.scheme == "https" else 80)
        self.path = parsed.path or "/"
        if parsed.query:
            self.path = f"{self.path}?{parsed.query}"

    def _get_ssl_context(self) -> ssl.SSLContext:
        if self._ssl_context is not None:
            return self._ssl_context

        if not self.tls_verify:
            ctx = ssl._create_unverified_context()
            self._ssl_context = ctx
            return ctx

        for cafile in _resolve_ca_bundle_candidates(self.tls_ca_bundle):
            try:
                ctx = ssl.create_default_context(cafile=cafile)
                self._ssl_context = ctx
                return ctx
            except Exception:
                continue

        ctx = ssl.create_default_context()
        self._ssl_context = ctx
        return ctx

    def _headers(self) -> dict[str, str]:
        headers = {
            "User-Agent": "investment-agent-mcp-client/0.1",
            "Accept": "application/json, text/event-stream",
        }
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        return headers

    def _proxy_for_request(self) -> str | None:
        if self.proxy_mode == "force_direct":
            return None

        if self.proxy_mode == "auto" and self.host and _host_matches_no_proxy(self.host, self.no_proxy):
            return None

        if self.scheme == "https":
            proxy = self.https_proxy or self.http_proxy
        else:
            proxy = self.http_proxy

        if self.proxy_mode == "force_proxy" and not proxy:
            raise RuntimeError("proxy_missing")

        return proxy or None

    def _build_urllib_opener(self, proxy_url: str | None) -> urllib.request.OpenerDirector:
        https_handler = urllib.request.HTTPSHandler(context=self._get_ssl_context())
        if self.proxy_mode == "force_direct":
            handler = urllib.request.ProxyHandler({})
            return urllib.request.build_opener(handler, https_handler)

        if proxy_url:
            proxies = {"http": self.http_proxy or proxy_url, "https": self.https_proxy or proxy_url}
            handler = urllib.request.ProxyHandler(proxies)
            return urllib.request.build_opener(handler, https_handler)

        # Explicitly direct to avoid ambient env interference in deterministic diagnostics.
        handler = urllib.request.ProxyHandler({})
        return urllib.request.build_opener(handler, https_handler)

    def _decode_body(self, body: bytes) -> Any:
        text = body.decode("utf-8", errors="replace")
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"text": text}

    def _send_via_urllib(
        self,
        method: str,
        payload: dict[str, Any] | None,
        stream_probe: bool,
        probe_bytes: int,
    ) -> MCPClientResult:
        try:
            proxy_url = self._proxy_for_request()
        except RuntimeError as exc:
            detail = str(exc)
            return MCPClientResult(
                ok=False,
                data=None,
                error=detail,
                error_class="proxy_missing" if detail == "proxy_missing" else "proxy_err",
                error_detail=detail,
                endpoint_host=self.host,
                endpoint_port=self.port,
            )

        body = None
        headers = self._headers()
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        opener = self._build_urllib_opener(proxy_url)
        timeout = max(self.connect_timeout_seconds, self.read_timeout_seconds)
        if stream_probe:
            timeout = min(timeout, 2.0)

        current_url = self.endpoint
        max_redirects = 3

        for _ in range(max_redirects + 1):
            req = urllib.request.Request(current_url, data=body, headers=headers, method=method)
            try:
                with opener.open(req, timeout=timeout) as response:
                    status = int(getattr(response, "status", 200) or 200)
                    if status != 200:
                        return MCPClientResult(
                            ok=False,
                            data=None,
                            error=f"http status {status}",
                            error_class="http_err",
                            error_detail=f"status={status}",
                            endpoint_host=self.host,
                            endpoint_port=self.port,
                            response_status=status,
                        )

                    if current_url != self.endpoint:
                        self._set_endpoint(current_url)

                    if stream_probe:
                        sample = response.read(probe_bytes)
                        if not sample:
                            return MCPClientResult(
                                ok=False,
                                data=None,
                                error="sse_no_data",
                                error_class="http_err",
                                error_detail="sse probe returned zero bytes",
                                endpoint_host=self.host,
                                endpoint_port=self.port,
                            )
                        return MCPClientResult(
                            ok=True,
                            data={
                                "transport": "sse",
                                "reachable": True,
                                "sample_bytes_count": len(sample),
                                "snippet_hash": __import__("hashlib").sha256(sample).hexdigest(),
                                "retrieved_at": datetime.now(UTC).isoformat(),
                            },
                            endpoint_host=self.host,
                            endpoint_port=self.port,
                            response_status=status,
                        )

                    data = self._decode_body(response.read())
                    return MCPClientResult(
                        ok=True,
                        data=data,
                        endpoint_host=self.host,
                        endpoint_port=self.port,
                        response_status=status,
                    )
            except urllib.error.HTTPError as exc:
                location = exc.headers.get("Location")
                if exc.code in {301, 302, 303, 307, 308} and location:
                    current_url = urllib.parse.urljoin(current_url, location)
                    continue
                error_class, detail = _classify_error(
                    exc,
                    family=None,
                    via_proxy=proxy_url is not None,
                    ipv4_succeeded=self._ipv4_succeeded,
                )
                return MCPClientResult(
                    ok=False,
                    data=None,
                    error=str(exc),
                    error_class=error_class,
                    error_detail=detail,
                    endpoint_host=self.host,
                    endpoint_port=self.port,
                    response_status=getattr(exc, "code", None),
                )
            except Exception as exc:  # noqa: BLE001
                error_class, detail = _classify_error(
                    exc,
                    family=None,
                    via_proxy=proxy_url is not None,
                    ipv4_succeeded=self._ipv4_succeeded,
                )
                return MCPClientResult(
                    ok=False,
                    data=None,
                    error=str(exc),
                    error_class=error_class,
                    error_detail=detail,
                    endpoint_host=self.host,
                    endpoint_port=self.port,
                )

        return MCPClientResult(
            ok=False,
            data=None,
            error="redirect_loop",
            error_class="http_err",
            error_detail="redirect loop detected",
            endpoint_host=self.host,
            endpoint_port=self.port,
        )

    def _resolve_addresses(self, family: int) -> list[tuple[Any, ...]]:
        return socket.getaddrinfo(self.host, self.port, family, socket.SOCK_STREAM)

    def _recv_http_response(self, sock: socket.socket, max_bytes: int) -> tuple[int, bytes]:
        chunks: list[bytes] = []
        read = 0
        while read < max_bytes:
            part = sock.recv(min(4096, max_bytes - read))
            if not part:
                break
            chunks.append(part)
            read += len(part)
        blob = b"".join(chunks)
        header_blob, _, body = blob.partition(b"\r\n\r\n")
        header_lines = header_blob.splitlines()
        if not header_lines:
            raise RuntimeError("invalid_http_response")
        status_line = header_lines[0].decode("iso-8859-1", errors="replace")
        parts = status_line.split(" ")
        if len(parts) < 2:
            raise RuntimeError(f"invalid_http_status_line: {status_line}")
        return int(parts[1]), body

    def _build_http_request(self, method: str, body: bytes | None, use_absolute_url: bool) -> bytes:
        target = self.endpoint if use_absolute_url else self.path
        headers = self._headers()
        headers["Host"] = self.host
        headers["Connection"] = "close"
        if body is not None:
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(body))

        lines = [f"{method} {target} HTTP/1.1"]
        for key, value in headers.items():
            lines.append(f"{key}: {value}")
        lines.append("")
        lines.append("")
        head = "\r\n".join(lines).encode("utf-8")
        return head + (body or b"")

    def _open_socket(self, addr: tuple[Any, ...], timeout_seconds: float) -> socket.socket:
        family, socktype, proto, _, sockaddr = addr
        sock = socket.socket(family, socktype, proto)
        sock.settimeout(timeout_seconds)
        sock.connect(sockaddr)
        return sock

    def _connect_first_resolved(
        self,
        addrs: list[tuple[Any, ...]],
        timeout_seconds: float,
        limit: int = 3,
    ) -> socket.socket:
        last_error: BaseException | None = None
        for addr in addrs[: max(1, limit)]:
            try:
                return self._open_socket(addr, timeout_seconds)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        if last_error is None:
            raise RuntimeError("no_resolved_addresses")
        raise last_error

    def _connect_https_proxy_tunnel(
        self,
        family: int,
        timeout_seconds: float,
    ) -> tuple[socket.socket, bool]:
        proxy_url = self._proxy_for_request()
        if not proxy_url:
            raise RuntimeError("proxy_missing")

        proxy_host, proxy_port, auth_header = _parse_proxy_url(proxy_url)
        addrs = socket.getaddrinfo(proxy_host, proxy_port, family, socket.SOCK_STREAM)
        last_error: BaseException | None = None

        for addr in addrs[:3]:
            proxy_sock: socket.socket | None = None
            try:
                proxy_sock = self._open_socket(addr, timeout_seconds)
                connect_req = (
                    f"CONNECT {self.host}:{self.port} HTTP/1.1\r\n"
                    f"Host: {self.host}:{self.port}\r\n"
                    f"Connection: keep-alive\r\n"
                    + (auth_header or "")
                    + "\r\n"
                ).encode("utf-8")
                proxy_sock.sendall(connect_req)

                header = b""
                start = time.time()
                while b"\r\n\r\n" not in header and len(header) < 8192:
                    if time.time() - start > timeout_seconds:
                        raise socket.timeout("proxy CONNECT timeout")
                    part = proxy_sock.recv(1024)
                    if not part:
                        break
                    header += part

                if b"\r\n\r\n" not in header:
                    raise RuntimeError("proxy CONNECT response malformed")

                status_line = header.split(b"\r\n", 1)[0].decode("iso-8859-1", errors="replace")
                status_parts = status_line.split(" ")
                code = int(status_parts[1]) if len(status_parts) >= 2 and status_parts[1].isdigit() else 0
                if code != 200:
                    raise RuntimeError(f"proxy CONNECT status {code}")

                return proxy_sock, True
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if proxy_sock is not None:
                    try:
                        proxy_sock.close()
                    except Exception:
                        pass

        if last_error is None:
            raise RuntimeError("proxy CONNECT failed")
        raise last_error

    def _send_via_raw_socket(
        self,
        *,
        family: int,
        method: str,
        payload: dict[str, Any] | None,
        stream_probe: bool,
        probe_bytes: int,
    ) -> MCPClientResult:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        timeout = max(self.connect_timeout_seconds, 0.5)

        proxy_url = None
        try:
            proxy_url = self._proxy_for_request()
        except RuntimeError as exc:
            detail = str(exc)
            return MCPClientResult(
                ok=False,
                data=None,
                error=detail,
                error_class="proxy_missing" if detail == "proxy_missing" else "proxy_err",
                error_detail=detail,
                endpoint_host=self.host,
                endpoint_port=self.port,
                ip_family_attempted=_family_name(family),
            )

        try:
            use_proxy = proxy_url is not None
            ssl_socket = False
            if self.scheme == "https" and use_proxy:
                sock, via_proxy = self._connect_https_proxy_tunnel(family, timeout)
                use_proxy = via_proxy
                ssl_socket = True
            elif use_proxy:
                proxy_host, proxy_port, _auth = _parse_proxy_url(proxy_url)
                addrs = socket.getaddrinfo(proxy_host, proxy_port, family, socket.SOCK_STREAM)
                sock = self._connect_first_resolved(addrs, timeout, limit=3)
            else:
                addrs = self._resolve_addresses(family)
                sock = self._connect_first_resolved(addrs, timeout, limit=3)

            if self.scheme == "https" and ssl_socket:
                ctx = self._get_ssl_context()
                sock = ctx.wrap_socket(sock, server_hostname=self.host)
            elif self.scheme == "https" and not use_proxy:
                ctx = self._get_ssl_context()
                sock = ctx.wrap_socket(sock, server_hostname=self.host)

            read_timeout = max(self.read_timeout_seconds, 0.5)
            if stream_probe:
                read_timeout = min(read_timeout, 2.0)
            sock.settimeout(read_timeout)
            use_absolute_url = bool(use_proxy and self.scheme == "http")
            request_bytes = self._build_http_request(method, body, use_absolute_url=use_absolute_url)
            sock.sendall(request_bytes)
            status, body_bytes = self._recv_http_response(sock, max_bytes=max(probe_bytes if stream_probe else 524288, 8192))
            sock.close()

            if status != 200:
                return MCPClientResult(
                    ok=False,
                    data=None,
                    error=f"http status {status}",
                    error_class="http_err",
                    error_detail=f"status={status}",
                    endpoint_host=self.host,
                    endpoint_port=self.port,
                    ip_family_attempted=_family_name(family),
                    response_status=status,
                )

            if stream_probe:
                if not body_bytes:
                    return MCPClientResult(
                        ok=False,
                        data=None,
                        error="sse_no_data",
                        error_class="http_err",
                        error_detail="sse probe returned zero bytes",
                        endpoint_host=self.host,
                        endpoint_port=self.port,
                        ip_family_attempted=_family_name(family),
                    )
                return MCPClientResult(
                    ok=True,
                    data={
                        "transport": "sse",
                        "reachable": True,
                        "sample_bytes_count": len(body_bytes),
                        "snippet_hash": __import__("hashlib").sha256(body_bytes).hexdigest(),
                        "retrieved_at": datetime.now(UTC).isoformat(),
                    },
                    endpoint_host=self.host,
                    endpoint_port=self.port,
                    ip_family_attempted=_family_name(family),
                    response_status=status,
                )

            data = self._decode_body(body_bytes)
            if family == socket.AF_INET:
                self._ipv4_succeeded = True
            return MCPClientResult(
                ok=True,
                data=data,
                endpoint_host=self.host,
                endpoint_port=self.port,
                ip_family_attempted=_family_name(family),
                response_status=status,
            )
        except Exception as exc:  # noqa: BLE001
            error_class, detail = _classify_error(
                exc,
                family=family,
                via_proxy=proxy_url is not None,
                ipv4_succeeded=self._ipv4_succeeded,
            )
            return MCPClientResult(
                ok=False,
                data=None,
                error=str(exc),
                error_class=error_class,
                error_detail=detail,
                endpoint_host=self.host,
                endpoint_port=self.port,
                ip_family_attempted=_family_name(family),
            )

    def _request(
        self,
        method: str,
        payload: dict[str, Any] | None = None,
        *,
        stream_probe: bool = False,
        probe_bytes: int = 65536,
    ) -> MCPClientResult:
        if self.endpoint_type == "websocket":
            return MCPClientResult(
                ok=False,
                data=None,
                error="websocket transport not enabled",
                error_class="unsupported_ws",
                error_detail="websocket transport not enabled",
                endpoint_host=self.host,
                endpoint_port=self.port,
            )

        attempts: list[MCPClientResult] = []

        normal = self._send_via_urllib(method, payload, stream_probe, probe_bytes)
        if normal.ok:
            return normal
        attempts.append(normal)

        v4 = self._send_via_raw_socket(
            family=socket.AF_INET,
            method=method,
            payload=payload,
            stream_probe=stream_probe,
            probe_bytes=probe_bytes,
        )
        if v4.ok:
            return v4
        attempts.append(v4)

        if not self.ipv6_unhealthy:
            v6 = self._send_via_raw_socket(
                family=socket.AF_INET6,
                method=method,
                payload=payload,
                stream_probe=stream_probe,
                probe_bytes=probe_bytes,
            )
            if v6.ok:
                return v6
            attempts.append(v6)

        # Prefer a more specific class if present.
        preferred_order = [
            "proxy_missing",
            "proxy_err",
            "dns_err",
            "connect_timeout",
            "connect_refused",
            "tls_err",
            "ipv6_route_err",
            "http_err",
        ]
        for klass in preferred_order:
            for attempt in attempts:
                if attempt.error_class == klass:
                    return attempt

        return attempts[-1]

    def _jsonrpc(self, method_name: str, params: dict[str, Any] | None = None) -> MCPClientResult:
        payload = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000) % 1000000,
            "method": method_name,
            "params": params or {},
        }
        result = self._request("POST", payload)
        if not result.ok:
            return result

        data = result.data
        if isinstance(data, dict):
            if "error" in data:
                return MCPClientResult(
                    ok=False,
                    data=None,
                    error=str(data["error"]),
                    error_class="http_err",
                    error_detail=str(data["error"]),
                    endpoint_host=result.endpoint_host,
                    endpoint_port=result.endpoint_port,
                    ip_family_attempted=result.ip_family_attempted,
                )
            if "result" in data:
                return MCPClientResult(
                    ok=True,
                    data=data["result"],
                    endpoint_host=result.endpoint_host,
                    endpoint_port=result.endpoint_port,
                    ip_family_attempted=result.ip_family_attempted,
                    response_status=result.response_status,
                )
        return result

    def handshake(self) -> MCPClientResult:
        if self.endpoint_type == "websocket":
            return MCPClientResult(
                ok=False,
                data=None,
                error="websocket transport not enabled",
                error_class="unsupported_ws",
                error_detail="websocket transport not enabled",
                endpoint_host=self.host,
                endpoint_port=self.port,
            )

        if self.endpoint_type == "sse":
            return self._request("GET", stream_probe=True, probe_bytes=65536)

        probe = self._request("GET")
        if probe.ok:
            payload = probe.data if isinstance(probe.data, dict) else {"payload": probe.data}
            payload["transport"] = "http"
            return MCPClientResult(
                ok=True,
                data=payload,
                endpoint_host=probe.endpoint_host,
                endpoint_port=probe.endpoint_port,
                ip_family_attempted=probe.ip_family_attempted,
                response_status=probe.response_status,
            )

        init = self._jsonrpc(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "investment-agent",
                    "version": CLIENT_BACKEND_VERSION,
                },
            },
        )
        if not init.ok:
            init = self._jsonrpc("initialize", {"client": "investment-agent"})
        if not init.ok:
            return init

        payload = init.data if isinstance(init.data, dict) else {"payload": init.data}
        payload["transport"] = "http"
        return MCPClientResult(
            ok=True,
            data=payload,
            endpoint_host=init.endpoint_host,
            endpoint_port=init.endpoint_port,
            ip_family_attempted=init.ip_family_attempted,
            response_status=init.response_status,
        )

    def list_tools(self) -> MCPClientResult:
        return self._jsonrpc("tools/list")

    def list_resources(self) -> MCPClientResult:
        return self._jsonrpc("resources/list")

    def read_resource(self, uri: str) -> MCPClientResult:
        return self._jsonrpc("resources/read", {"uri": uri})

    def search(self, query: str) -> MCPClientResult:
        for method_name, params in [
            ("search", {"query": query}),
            ("resources/search", {"query": query}),
            ("tools/call", {"name": "search", "arguments": {"query": query}}),
        ]:
            result = self._jsonrpc(method_name, params)
            if result.ok:
                return result
        return MCPClientResult(
            ok=False,
            data=None,
            error="search not supported",
            error_class="http_err",
            error_detail="search not supported",
            endpoint_host=self.host,
            endpoint_port=self.port,
        )


def ipv6_health_probe(timeout_seconds: float = 1.5) -> tuple[bool, str | None]:
    """Best-effort IPv6 reachability probe.

    Uses cloudflare.com as a dual-stack target. Failure does not raise.
    """
    try:
        addrs = socket.getaddrinfo("cloudflare.com", 443, socket.AF_INET6, socket.SOCK_STREAM)
        if not addrs:
            return True, "no_ipv6_records"
        family, socktype, proto, _, sockaddr = addrs[0]
        sock = socket.socket(family, socktype, proto)
        sock.settimeout(timeout_seconds)
        try:
            sock.connect(sockaddr)
        finally:
            sock.close()
        return False, None
    except Exception as exc:  # noqa: BLE001
        return True, _format_error(exc)
