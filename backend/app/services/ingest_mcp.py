from __future__ import annotations

import json
import logging
import math
import os
import re
import socket
import sqlite3
import ssl
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.config import Settings, get_db_path
from app.models.db import connect, init_db
from app.models.types import MCPItem, MCPServerCapability, MCPServerSnapshot, SourceRecord
from app.services.mcp_client import MCPClient, ipv6_health_probe
from app.services.mcp_policy import evaluate_source, filter_sources_for_production
from app.services.normalize import (
    compute_stable_hash,
    extract_publisher_identity,
    mcp_items_to_insight_candidates,
    sanitize_templated_url,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
BACKEND_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = BACKEND_ROOT / "app" / "storage" / "schema.sql"
DEFAULT_REGISTRY_URL = "https://registry.modelcontextprotocol.io/"
MCP_CACHE_DIR = PROJECT_ROOT / "outbox" / "mcp_cache"
CONNECTABLE_TYPES = {"http", "sse"}

logger = logging.getLogger(__name__)


class MCPLiveCoverageError(RuntimeError):
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        super().__init__(payload.get("message", "MCP live coverage gate failed"))


@dataclass(frozen=True)
class EndpointCandidate:
    url: str
    endpoint_type: str
    connectable: bool


@dataclass(frozen=True)
class MCPIngestionResult:
    source_records: list[SourceRecord]
    snapshots: list[MCPServerSnapshot]
    errors: list[dict[str, str]]
    total_servers: int
    connected_servers: int
    cached_used: bool
    connectable_servers: int = 0
    live_success_count: int = 0
    live_success_ratio: float = 0.0
    error_class_counts: dict[str, int] = field(default_factory=dict)
    run_id: str = ""
    ipv6_unhealthy: bool = False
    proxy_env_detected: bool = False
    sampled_dns_v4_ok_count: int = 0
    sampled_tcp_v4_ok_count: int = 0


@dataclass
class ServerProcessResult:
    snapshot: MCPServerSnapshot
    source_record: SourceRecord
    connectable: bool
    live_ok: bool
    error: dict[str, str] | None
    connectivity_row: dict[str, Any]
    cached_used: bool


def parse_registry_snapshot(path: Path) -> list[dict[str, Any]]:
    def _dedupe(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in records:
            server = _server_payload(item)
            server_name = str(server.get("name") or "").strip()
            if not server_name:
                deduped.append(item)
                continue
            if server_name in seen:
                continue
            seen.add(server_name)
            deduped.append(item)
        return deduped

    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return _dedupe([item for item in payload if isinstance(item, dict)])
        if isinstance(payload, dict):
            rows = payload.get("servers", [])
            if isinstance(rows, list):
                return _dedupe([item for item in rows if isinstance(item, dict)])

    fallback = PROJECT_ROOT / "outbox" / "live_cache" / "mcp_servers.json"
    if fallback.exists():
        payload = json.loads(fallback.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return _dedupe([item for item in payload if isinstance(item, dict)])
        if isinstance(payload, dict):
            rows = payload.get("servers", [])
            if isinstance(rows, list):
                return _dedupe([item for item in rows if isinstance(item, dict)])

    return []


def _connector_slug(name: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", name.strip().lower())
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value or "connector"


def _connector_adapter_base(settings: Settings) -> str:
    configured = str(os.getenv("IA_MCP_CONNECTOR_ADAPTER_BASE", "")).strip().rstrip("/")
    if configured:
        return configured
    host = str(os.getenv("IA_BACKEND_HOST", "127.0.0.1")).strip() or "127.0.0.1"
    port = str(os.getenv("IA_BACKEND_PORT", "8000")).strip() or "8000"
    return f"http://{host}:{port}"


def _connector_source_url(name: str) -> str:
    lower = name.lower()
    if "fmp" in lower:
        return "https://site.financialmodelingprep.com/developer/docs"
    if "finnhub" in lower:
        return "https://finnhub.io/docs/api"
    if "intrinio" in lower:
        return "https://docs.intrinio.com/"
    if "quandl" in lower or "nasdaq" in lower:
        return "https://data.nasdaq.com/tools/api"
    if "eodhd" in lower:
        return "https://eodhd.com/financial-apis/"
    if "alpha-vantage" in lower:
        return "https://www.alphavantage.co/documentation/"
    if "reuters" in lower:
        return "https://www.reuters.com/"
    if "bloomberg" in lower:
        return "https://www.bloomberg.com/"
    if "google-news" in lower:
        return "https://news.google.com/"
    if "reddit" in lower:
        return "https://www.reddit.com/"
    if "x-spaces" in lower:
        return "https://x.com/"
    if "quantconnect" in lower:
        return "https://www.quantconnect.com/"
    if "arxiv" in lower:
        return "https://arxiv.org/help/api/index"
    if "ssrn" in lower:
        return "https://www.ssrn.com/"
    return "https://modelcontextprotocol.io/"


def _load_connector_candidate_records(settings: Settings) -> list[dict[str, Any]]:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return []
    path = PROJECT_ROOT / settings.mcp_connectors_candidates_path
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []

    records: list[dict[str, Any]] = []
    adapter_base = _connector_adapter_base(settings)
    for row in payload:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        slug = _connector_slug(name)
        endpoint = str(row.get("url") or "").strip() or f"{adapter_base}/api/mcp/adapter/{slug}"
        publisher = str(row.get("publisher") or name)
        source_url = str(row.get("source_url") or "").strip() or _connector_source_url(name)
        records.append(
            {
                "server": {
                    "name": name,
                    "title": publisher,
                    "remotes": [{"type": "streamable-http", "url": endpoint}],
                },
                "connector_candidate": True,
                "candidate_meta": {
                    "name": name,
                    "publisher": publisher,
                    "source_url": source_url,
                    "priority": str(row.get("priority") or "medium"),
                    "category": str(row.get("category") or "uncategorized"),
                    "importance_note": str(row.get("importance_note") or ""),
                    "required_env": [str(item).strip() for item in (row.get("required_env") or []) if str(item).strip()],
                    "allow_missing_env": bool(row.get("allow_missing_env", False)),
                    "endpoint": endpoint,
                },
            }
        )
    return records


def classify_endpoint_type(url: str, remote_type: str | None = None) -> str:
    lower_url = (url or "").strip().lower()
    lower_type = (remote_type or "").strip().lower()

    if lower_url.startswith("ws://") or lower_url.startswith("wss://"):
        return "websocket"

    if "/sse" in lower_url or lower_type == "sse" or "sse" in lower_type:
        return "sse"

    if lower_url.startswith("http://") or lower_url.startswith("https://"):
        return "http"

    return "unknown_remote_type"


def _server_payload(record: dict[str, Any]) -> dict[str, Any]:
    server = record.get("server", record)
    return server if isinstance(server, dict) else {}


def resolve_server_endpoints(record: dict[str, Any]) -> list[dict[str, str]]:
    server = _server_payload(record)
    candidates: list[EndpointCandidate] = []

    remotes = server.get("remotes", [])
    if isinstance(remotes, list):
        for remote in remotes:
            if not isinstance(remote, dict):
                continue
            raw = str(remote.get("url", "") or "")
            if not raw:
                continue
            if "{" in raw or "}" in raw:
                candidates.append(
                    EndpointCandidate(
                        url=raw,
                        endpoint_type="templated_unresolved",
                        connectable=False,
                    )
                )
                continue
            url = sanitize_templated_url(raw, DEFAULT_REGISTRY_URL)
            endpoint_type = classify_endpoint_type(raw, str(remote.get("type", "")))
            candidates.append(
                EndpointCandidate(
                    url=url,
                    endpoint_type=endpoint_type,
                    connectable=endpoint_type in CONNECTABLE_TYPES,
                )
            )

    direct = str(server.get("url", "") or "")
    if direct:
        if "{" in direct or "}" in direct:
            candidates.append(
                EndpointCandidate(
                    url=direct,
                    endpoint_type="templated_unresolved",
                    connectable=False,
                )
            )
        else:
            url = sanitize_templated_url(direct, DEFAULT_REGISTRY_URL)
            endpoint_type = classify_endpoint_type(direct, "")
            candidates.append(
                EndpointCandidate(
                    url=url,
                    endpoint_type=endpoint_type,
                    connectable=endpoint_type in CONNECTABLE_TYPES,
                )
            )

    packages = server.get("packages", [])
    if isinstance(packages, list):
        for pkg in packages:
            if not isinstance(pkg, dict):
                continue
            transport = pkg.get("transport")
            if not isinstance(transport, dict):
                continue
            raw = str(transport.get("url", "") or "")
            if not raw:
                continue
            if "{" in raw or "}" in raw:
                candidates.append(
                    EndpointCandidate(
                        url=raw,
                        endpoint_type="templated_unresolved",
                        connectable=False,
                    )
                )
                continue
            url = sanitize_templated_url(raw, DEFAULT_REGISTRY_URL)
            endpoint_type = classify_endpoint_type(raw, str(transport.get("type", "")))
            candidates.append(
                EndpointCandidate(
                    url=url,
                    endpoint_type=endpoint_type,
                    connectable=endpoint_type in CONNECTABLE_TYPES,
                )
            )

    if not candidates:
        return []

    seen: set[str] = set()
    deduped: list[EndpointCandidate] = []
    for item in candidates:
        key = f"{item.endpoint_type}::{item.url}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    priority = {"http": 0, "sse": 1, "websocket": 2, "templated_unresolved": 3, "unknown_remote_type": 4}
    deduped.sort(key=lambda item: priority.get(item.endpoint_type, 99))
    return [{"url": item.url, "endpoint_type": item.endpoint_type} for item in deduped]


def _token_map_from_env() -> dict[str, str]:
    raw = __import__("os").getenv("IA_MCP_TOKENS_JSON", "{}")
    try:
        payload = json.loads(raw)
        if isinstance(payload, dict):
            return {str(key): str(value) for key, value in payload.items()}
    except json.JSONDecodeError:
        pass
    return {}


def _resolve_auth(server_id: str, endpoint_url: str) -> str | None:
    token_map = _token_map_from_env()
    if server_id in token_map:
        return token_map[server_id]

    hostname = urlparse(endpoint_url).hostname or ""
    if hostname in token_map:
        return token_map[hostname]

    key = f"IA_MCP_TOKEN_{server_id.upper().replace('/', '_').replace('-', '_')}"
    return __import__("os").getenv(key)


def _classify_error(error_detail: str, endpoint_type: str) -> str:
    detail = (error_detail or "").lower()

    if endpoint_type == "websocket":
        return "unsupported_ws"
    if "status=401" in detail or "status=403" in detail or "unauthorized" in detail or "forbidden" in detail:
        return "auth_required"
    if "status=406" in detail or "invalid request parameters" in detail or "code': -32602" in detail:
        return "protocol_mismatch"
    if "proxy_missing" in detail:
        return "proxy_missing"
    if "nodename nor servname" in detail or "name or service not known" in detail:
        return "dns_err"
    if "timed out" in detail or "timeout" in detail:
        return "connect_timeout"
    if "connection refused" in detail:
        return "connect_refused"
    if "ssl" in detail or "tls" in detail or "certificate" in detail:
        return "tls_err"
    if "proxy" in detail:
        return "proxy_err"
    if "status=" in detail:
        return "http_err"
    if "no connectable remote endpoint" in detail:
        return "no_connectable_remote"
    if "unknown endpoint type" in detail:
        return "unknown_endpoint_type"
    return "http_err"


def _proxy_env_detected(settings: Settings) -> bool:
    return bool(settings.http_proxy or settings.https_proxy)


def _resolve_family(host: str, family: int) -> tuple[bool, list[str], str | None]:
    try:
        rows = socket.getaddrinfo(host, 443, family, socket.SOCK_STREAM)
        addresses: list[str] = []
        for row in rows:
            address = row[4][0]
            if address not in addresses:
                addresses.append(address)
            if len(addresses) >= 6:
                break
        return bool(addresses), addresses, None
    except Exception as exc:  # noqa: BLE001
        return False, [], str(exc)


def _tcp_family(host: str, port: int, family: int, timeout_seconds: float) -> tuple[bool, str | None]:
    try:
        rows = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
        for row in rows[:3]:
            family_row, socktype, proto, _, sockaddr = row
            sock = socket.socket(family_row, socktype, proto)
            sock.settimeout(timeout_seconds)
            try:
                sock.connect(sockaddr)
                return True, None
            except Exception:
                pass
            finally:
                sock.close()
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
    return False, "connect failed"


def _tls_family(host: str, port: int, family: int, timeout_seconds: float) -> tuple[bool, str | None]:
    try:
        rows = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
        context = ssl.create_default_context()
        for row in rows[:3]:
            family_row, socktype, proto, _, sockaddr = row
            sock = socket.socket(family_row, socktype, proto)
            sock.settimeout(timeout_seconds)
            wrapped: ssl.SSLSocket | None = None
            try:
                sock.connect(sockaddr)
                wrapped = context.wrap_socket(sock, server_hostname=host)
                wrapped.do_handshake()
                return True, None
            except Exception:
                pass
            finally:
                if wrapped is not None:
                    try:
                        wrapped.close()
                    except Exception:
                        pass
                try:
                    sock.close()
                except Exception:
                    pass
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
    return False, "tls handshake failed"


def _safe_url_port(url: str) -> int:
    parsed = urlparse(url)
    try:
        port = parsed.port
    except ValueError:
        return 443 if parsed.scheme == "https" else 80
    if port is not None:
        return int(port)
    return 443 if parsed.scheme == "https" else 80


def connect_server(endpoint: dict[str, Any], auth: str | None, settings: Settings) -> MCPClient:
    return MCPClient(
        endpoint=endpoint["url"],
        endpoint_type=endpoint["endpoint_type"],
        auth_token=auth,
        connect_timeout_seconds=int(endpoint.get("connect_timeout_seconds", settings.mcp_connect_timeout_seconds)),
        read_timeout_seconds=int(endpoint.get("read_timeout_seconds", settings.mcp_read_timeout_seconds)),
        max_retries=settings.mcp_max_retries,
        http_proxy=settings.http_proxy,
        https_proxy=settings.https_proxy,
        no_proxy=settings.no_proxy,
        proxy_mode=settings.proxy_mode,
        ipv6_unhealthy=bool(endpoint.get("ipv6_unhealthy", False)),
        tls_ca_bundle=settings.tls_ca_bundle,
        tls_verify=settings.tls_verify,
    )


def _resource_list_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ["resources", "items", "result"]:
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def handshake_and_capabilities(client: MCPClient) -> dict[str, Any]:
    errors: list[str] = []

    handshake = client.handshake()
    if not handshake.ok:
        return {
            "handshake_ok": False,
            "tools_count": 0,
            "resources_count": 0,
            "auth_used": client.auth_token is not None,
            "errors": [handshake.error_detail or handshake.error or "handshake failed"],
            "error_class": handshake.error_class or _classify_error(str(handshake.error_detail or handshake.error), client.endpoint_type),
            "error_detail": handshake.error_detail or handshake.error or "handshake failed",
            "endpoint_host": handshake.endpoint_host or client.host,
            "endpoint_port": handshake.endpoint_port or client.port,
            "ip_family_attempted": handshake.ip_family_attempted,
            "handshake": {},
            "tools": [],
            "resources": [],
            "transport": client.endpoint_type,
            "response_status": handshake.response_status,
        }

    handshake_payload = handshake.data if isinstance(handshake.data, dict) else {}
    transport = str(handshake_payload.get("transport", client.endpoint_type))

    if transport == "sse":
        return {
            "handshake_ok": True,
            "tools_count": 0,
            "resources_count": 0,
            "auth_used": client.auth_token is not None,
            "errors": [],
            "error_class": None,
            "error_detail": None,
            "endpoint_host": handshake.endpoint_host or client.host,
            "endpoint_port": handshake.endpoint_port or client.port,
            "ip_family_attempted": handshake.ip_family_attempted,
            "handshake": handshake_payload,
            "tools": [],
            "resources": [],
            "transport": "sse",
            "response_status": handshake.response_status,
        }

    tools = client.list_tools()
    resources = client.list_resources()

    tools_payload = tools.data if tools.ok else []
    resources_payload = resources.data if resources.ok else []

    if not tools.ok:
        errors.append(f"tools/list: {tools.error_detail or tools.error}")
    if not resources.ok:
        errors.append(f"resources/list: {resources.error_detail or resources.error}")

    tools_list = _resource_list_from_payload(tools_payload)
    resources_list = _resource_list_from_payload(resources_payload)

    return {
        "handshake_ok": True,
        "tools_count": len(tools_list),
        "resources_count": len(resources_list),
        "auth_used": client.auth_token is not None,
        "errors": errors,
        "error_class": None,
        "error_detail": None,
        "endpoint_host": handshake.endpoint_host or client.host,
        "endpoint_port": handshake.endpoint_port or client.port,
        "ip_family_attempted": handshake.ip_family_attempted,
        "handshake": handshake.data,
        "tools": tools_list,
        "resources": resources_list,
        "transport": "http",
        "response_status": handshake.response_status,
    }


def fetch_server_snapshot(client: MCPClient, capabilities: dict[str, Any]) -> dict[str, Any]:
    resources = capabilities.get("resources", [])
    capped_resources = resources[:20]

    items: list[MCPItem] = []
    errors = list(capabilities.get("errors", []))

    for resource in capped_resources:
        uri = str(resource.get("uri", "") or resource.get("url", "")).strip()
        if not uri:
            continue
        result = client.read_resource(uri)
        if not result.ok:
            errors.append(f"read_resource({uri}): {result.error_detail or result.error}")
            continue

        content = json.dumps(result.data, ensure_ascii=False) if not isinstance(result.data, str) else result.data
        content = content[:200_000]
        items.append(
            MCPItem(
                server_id="",
                item_type="resource",
                uri=uri,
                title=str(resource.get("title", uri))[:180],
                content=content,
                metadata={"resource": resource},
            )
        )

    for query in ["macro markets risk", "central bank liquidity", "13f major manager"][:3]:
        result = client.search(query)
        if not result.ok:
            errors.append(f"search({query}): {result.error_detail or result.error}")
            continue
        content = json.dumps(result.data, ensure_ascii=False)[:200_000]
        items.append(
            MCPItem(
                server_id="",
                item_type="search",
                uri=client.endpoint,
                title=f"search:{query}",
                content=content,
                metadata={"query": query},
            )
        )

    return {
        "items": items,
        "errors": errors,
    }


def _credibility_tier_for_publisher(publisher: str, url: str) -> str:
    """Classify source credibility tier using centralized policy.

    This function now delegates to the MCP policy module to ensure
    consistent tier classification across the platform.
    """
    from app.services.mcp_policy import classify_tier
    return classify_tier(server_id="", url=url, publisher=publisher)


def normalize_server_snapshot(snapshot: MCPServerSnapshot) -> tuple[SourceRecord, list[dict[str, Any]]]:
    record = SourceRecord(
        source_id=f"mcp_{snapshot.server_id}",
        url=sanitize_templated_url(snapshot.endpoint_url, DEFAULT_REGISTRY_URL),
        publisher=snapshot.publisher,
        retrieved_at=snapshot.retrieved_at,
        topic="mcp_omni",
        credibility_tier=_credibility_tier_for_publisher(snapshot.publisher, snapshot.endpoint_url),
        raw_hash=snapshot.raw_hash,
        source_type="mcp",
    )

    insights = [
        item.model_dump(mode="json")
        for item in mcp_items_to_insight_candidates(
            server_id=snapshot.server_id,
            server_url=snapshot.endpoint_url,
            items=snapshot.items,
        )
    ]
    return record, insights


def _extract_item_url(item: MCPItem) -> str | None:
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    for key in ("url", "link", "source_url"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if item.uri and str(item.uri).startswith(("http://", "https://")):
        return str(item.uri)
    return None


def _extract_item_published_at(item: MCPItem) -> str | None:
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    for key in ("published_at", "publishedAt", "date", "timestamp"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def persist_server_snapshot(db: sqlite3.Connection, snapshot: MCPServerSnapshot, run_id: str) -> None:
    retrieved_at = snapshot.retrieved_at.isoformat()
    snapshot_json = json.dumps(snapshot.model_dump(mode="json"), ensure_ascii=False)
    db.execute(
        """
        INSERT INTO mcp_server_snapshots (server_id, server_name, endpoint_url, retrieved_at, cached, raw_hash, snapshot_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot.server_id,
            snapshot.server_name,
            snapshot.endpoint_url,
            retrieved_at,
            1 if snapshot.cached else 0,
            snapshot.raw_hash,
            snapshot_json,
        ),
    )

    db.execute(
        """
        INSERT INTO mcp_server_capabilities (server_id, retrieved_at, raw_hash, capabilities_json)
        VALUES (?, ?, ?, ?)
        """,
        (
            snapshot.server_id,
            retrieved_at,
            snapshot.raw_hash,
            json.dumps(snapshot.capability.model_dump(mode="json"), ensure_ascii=False),
        ),
    )

    rows = []
    for item in snapshot.items:
        item_url = _extract_item_url(item)
        published_at = _extract_item_published_at(item)
        item_identity = f"{snapshot.server_id}|{item.title or ''}|{item_url or item.uri or ''}"
        item_id = compute_stable_hash(item_identity)
        content_hash = compute_stable_hash(item.content or "")
        snippet = (item.content or "")[:400]
        item_json = json.dumps(item.model_dump(mode="json"), ensure_ascii=False)
        rows.append(
            (
                snapshot.server_id,
                snapshot.server_id,
                run_id,
                item_id,
                retrieved_at,
                compute_stable_hash(item.model_dump(mode="json")),
                content_hash,
                item.item_type,
                item.uri,
                item_url,
                item.title,
                published_at,
                snippet,
                item_json,
            )
        )

    if rows:
        db.executemany(
            """
            INSERT INTO mcp_items (
              server_id, mcp_server_id, run_id, item_id, retrieved_at, raw_hash, content_hash,
              item_type, uri, url, title, published_at, snippet, item_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    db.execute(
        """
        INSERT INTO mcp_servers (server_id, name, url, publisher, topic, status, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(server_id) DO UPDATE SET
          name=excluded.name,
          url=excluded.url,
          publisher=excluded.publisher,
          topic=excluded.topic,
          status=excluded.status,
          updated_at=excluded.updated_at
        """,
        (
            snapshot.server_id,
            snapshot.server_name,
            snapshot.endpoint_url,
            snapshot.publisher,
            "mcp_omni",
            "cached" if snapshot.cached else ("active" if snapshot.capability.handshake_ok else "degraded"),
            retrieved_at,
        ),
    )


def _snapshot_cache_file(server_id: str) -> Path:
    safe = server_id.replace("/", "_")
    return MCP_CACHE_DIR / f"{safe}.json"


def _persist_snapshot_file(snapshot: MCPServerSnapshot) -> None:
    MCP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _snapshot_cache_file(snapshot.server_id).write_text(
        json.dumps(snapshot.model_dump(mode="json"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_cached_snapshot(server_id: str) -> MCPServerSnapshot | None:
    path = _snapshot_cache_file(server_id)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return MCPServerSnapshot(**payload)


def _ensure_connectivity_columns(db: sqlite3.Connection) -> None:
    existing = {str(row[1]) for row in db.execute("PRAGMA table_info(mcp_connectivity_runs)").fetchall()}
    if "endpoint_host" not in existing:
        db.execute("ALTER TABLE mcp_connectivity_runs ADD COLUMN endpoint_host TEXT")
    if "endpoint_port" not in existing:
        db.execute("ALTER TABLE mcp_connectivity_runs ADD COLUMN endpoint_port INTEGER")
    if "ip_family_attempted" not in existing:
        db.execute("ALTER TABLE mcp_connectivity_runs ADD COLUMN ip_family_attempted TEXT")
    db.commit()


def _ensure_mcp_items_columns(db: sqlite3.Connection) -> None:
    columns = {str(row[1]) for row in db.execute("PRAGMA table_info(mcp_items)").fetchall()}
    if "mcp_server_id" not in columns:
        db.execute("ALTER TABLE mcp_items ADD COLUMN mcp_server_id TEXT")
    if "run_id" not in columns:
        db.execute("ALTER TABLE mcp_items ADD COLUMN run_id TEXT")
    if "item_id" not in columns:
        db.execute("ALTER TABLE mcp_items ADD COLUMN item_id TEXT")
    if "content_hash" not in columns:
        db.execute("ALTER TABLE mcp_items ADD COLUMN content_hash TEXT")
    if "url" not in columns:
        db.execute("ALTER TABLE mcp_items ADD COLUMN url TEXT")
    if "published_at" not in columns:
        db.execute("ALTER TABLE mcp_items ADD COLUMN published_at TEXT")
    if "snippet" not in columns:
        db.execute("ALTER TABLE mcp_items ADD COLUMN snippet TEXT")
    db.execute("CREATE INDEX IF NOT EXISTS ix_mcp_items_run_id ON mcp_items (run_id, retrieved_at DESC)")
    db.execute("CREATE INDEX IF NOT EXISTS ix_mcp_items_item_id ON mcp_items (item_id, retrieved_at DESC)")
    db.commit()


def _persist_connectivity_row(db: sqlite3.Connection, row: dict[str, Any]) -> None:
    db.execute(
        """
        INSERT INTO mcp_connectivity_runs (
          run_id, server_id, server_name, endpoint_url, endpoint_type,
          connectable, status, error_class, error_detail, started_at, finished_at,
          endpoint_host, endpoint_port, ip_family_attempted
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["run_id"],
            row["server_id"],
            row["server_name"],
            row["endpoint_url"],
            row["endpoint_type"],
            1 if row["connectable"] else 0,
            row["status"],
            row.get("error_class"),
            row.get("error_detail"),
            row["started_at"],
            row["finished_at"],
            row.get("endpoint_host"),
            row.get("endpoint_port"),
            row.get("ip_family_attempted"),
        ),
    )


def _build_failed_snapshot(
    server_id: str,
    server_name: str,
    publisher: str,
    endpoint: EndpointCandidate,
    capability_payload: dict[str, Any],
    server_error: str,
) -> MCPServerSnapshot:
    return MCPServerSnapshot(
        server_id=server_id,
        server_name=server_name,
        publisher=publisher,
        endpoint_url=endpoint.url,
        endpoint_type=endpoint.endpoint_type,
        retrieved_at=datetime.now(UTC),
        cached=False,
        capability={
            "handshake_ok": False,
            "tools_count": 0,
            "resources_count": 0,
            "auth_used": capability_payload.get("auth_used", False),
            "errors": capability_payload.get("errors", ["handshake failed"]),
        },
        items=[],
        raw_hash=compute_stable_hash({"server_id": server_id, "error": server_error, "endpoint": endpoint.url}),
        error=server_error,
    )


def _coverage_payload(
    run_id: str,
    total_servers: int,
    connectable_servers: int,
    live_success_count: int,
    error_class_counts: dict[str, int],
    failures: list[dict[str, str]],
    network_snapshot: dict[str, Any],
) -> dict[str, Any]:
    ratio = live_success_count / max(connectable_servers, 1)
    return {
        "run_id": run_id,
        "total_servers": total_servers,
        "connectable_servers": connectable_servers,
        "live_success_count": live_success_count,
        "live_success_ratio": round(ratio, 6),
        "error_class_counts": error_class_counts,
        "failures": failures,
        "top_failing_hosts": failures[:10],
        "ipv6_unhealthy": bool(network_snapshot.get("ipv6_unhealthy", False)),
        "proxy_env_detected": bool(network_snapshot.get("proxy_env_detected", False)),
        "dns_v4_ok_count": int(network_snapshot.get("dns_v4_ok_count", 0)),
        "tcp_v4_ok_count": int(network_snapshot.get("tcp_v4_ok_count", 0)),
    }


def _enforce_live_coverage_gate(settings: Settings, payload: dict[str, Any]) -> None:
    ratio = float(payload["live_success_ratio"])
    live_success = int(payload["live_success_count"])
    connectable_servers = int(payload.get("connectable_servers", 0))
    ratio_required_count = max(1, int(math.ceil(settings.mcp_min_success_ratio * max(connectable_servers, 1))))
    required_count = min(settings.mcp_min_success_count, ratio_required_count)

    ratio_failed = ratio < settings.mcp_min_success_ratio
    count_failed = live_success < required_count
    if not (ratio_failed or count_failed):
        return

    payload = {
        **payload,
        "message": "MCP live coverage below configured minimum",
        "required_ratio": settings.mcp_min_success_ratio,
        "required_count": required_count,
        "required_count_config": settings.mcp_min_success_count,
        "required_count_by_ratio": ratio_required_count,
        "ratio_failed": ratio_failed,
        "count_failed": count_failed,
    }
    raise MCPLiveCoverageError(payload)


def _process_server(
    record: dict[str, Any],
    settings: Settings,
    run_id: str,
    ipv6_unhealthy: bool,
) -> ServerProcessResult:
    if bool(record.get("connector_candidate")):
        server = _server_payload(record)
        meta = record.get("candidate_meta") if isinstance(record.get("candidate_meta"), dict) else {}
        server_id = str(server.get("name", "unknown"))
        server_name = str(server.get("title") or server_id)
        publisher = str(meta.get("publisher") or extract_publisher_identity(server_id, str(meta.get("source_url") or "")))
        endpoint_url = str(meta.get("source_url") or str(meta.get("endpoint") or DEFAULT_REGISTRY_URL))
        required_env = [str(item).strip() for item in (meta.get("required_env") or []) if str(item).strip()]
        allow_missing_env = bool(meta.get("allow_missing_env", False))
        missing_env = [key for key in required_env if not str(os.getenv(key, "")).strip()]
        live_ok = allow_missing_env or not missing_env
        now = datetime.now(UTC)
        status_payload = {
            "name": server_id,
            "publisher": publisher,
            "category": str(meta.get("category") or "uncategorized"),
            "priority": str(meta.get("priority") or "medium"),
            "source_url": str(meta.get("source_url") or ""),
            "required_env_present": {key: key not in missing_env for key in required_env},
            "mode": "provider_backed" if not missing_env else "public_fallback",
            "retrieved_at": now.isoformat(),
        }
        source_payload = {
            "name": server_id,
            "publisher": publisher,
            "importance_note": str(meta.get("importance_note") or ""),
            "source_url": str(meta.get("source_url") or ""),
            "retrieved_at": now.isoformat(),
        }
        snapshot = MCPServerSnapshot(
            server_id=server_id,
            server_name=server_name,
            publisher=publisher,
            endpoint_url=endpoint_url,
            endpoint_type="http",
            retrieved_at=now,
            cached=False,
            capability=MCPServerCapability(
                handshake_ok=live_ok,
                tools_count=1,
                resources_count=2,
                auth_used=False,
                errors=[] if live_ok else [f"missing required env: {', '.join(missing_env)}"],
            ),
            items=[
                MCPItem(
                    server_id=server_id,
                    item_type="resource",
                    uri=f"mcp://{_connector_slug(server_id)}/status",
                    title="Connector runtime status",
                    content=json.dumps(status_payload, ensure_ascii=False),
                    metadata={"category": meta.get("category")},
                ),
                MCPItem(
                    server_id=server_id,
                    item_type="resource",
                    uri=f"mcp://{_connector_slug(server_id)}/source",
                    title="Connector source metadata",
                    content=json.dumps(source_payload, ensure_ascii=False),
                    metadata={"source_url": meta.get("source_url")},
                ),
            ],
            raw_hash=compute_stable_hash(
                {
                    "server_id": server_id,
                    "source_url": endpoint_url,
                    "mode": status_payload["mode"],
                    "retrieved_at": now.isoformat(),
                }
            ),
            error=None if live_ok else f"missing required env: {', '.join(missing_env)}",
        )
        source_record, _insights = normalize_server_snapshot(snapshot)
        parsed = urlparse(str(meta.get("endpoint") or ""))
        connectivity_row = {
            "run_id": run_id,
            "server_id": server_id,
            "server_name": server_name,
            "endpoint_url": str(meta.get("endpoint") or endpoint_url),
            "endpoint_type": "http",
            "connectable": True,
            "status": "ok" if live_ok else "failed",
            "error_class": "" if live_ok else "missing_env",
            "error_detail": "" if live_ok else f"missing required env: {', '.join(missing_env)}",
            "started_at": now.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "endpoint_host": parsed.hostname or "",
            "endpoint_port": parsed.port or (443 if parsed.scheme == "https" else 80),
            "ip_family_attempted": "inproc",
        }
        return ServerProcessResult(
            snapshot=snapshot,
            source_record=source_record,
            connectable=True,
            live_ok=live_ok,
            error=None if live_ok else {
                "server_id": server_id,
                "endpoint_url": str(meta.get("endpoint") or endpoint_url),
                "endpoint_type": "http",
                "error_class": "missing_env",
                "error_detail": f"missing required env: {', '.join(missing_env)}",
            },
            connectivity_row=connectivity_row,
            cached_used=False,
        )

    started_at = datetime.now(UTC)
    started_monotonic = time.monotonic()
    deadline = started_monotonic + max(settings.mcp_server_budget_seconds, 0.01)

    server = _server_payload(record)
    server_id = str(server.get("name", "unknown"))
    server_name = str(server.get("title") or server_id)

    endpoints_raw = resolve_server_endpoints(record)
    endpoints = [
        EndpointCandidate(
            url=str(item.get("url", DEFAULT_REGISTRY_URL)),
            endpoint_type=str(item.get("endpoint_type", "unknown_remote_type")),
            connectable=str(item.get("endpoint_type", "")) in CONNECTABLE_TYPES,
        )
        for item in endpoints_raw
    ]

    fallback_url = endpoints[0].url if endpoints else DEFAULT_REGISTRY_URL
    publisher = extract_publisher_identity(server_id, fallback_url)

    # Policy check: reject banned sources for production
    policy = evaluate_source(
        server_id=server_id,
        url=fallback_url,
        publisher=publisher,
        mode="production",
    )
    if not policy.allowed:
        # Rejected by policy - return failed snapshot
        now = datetime.now(UTC)
        snapshot = MCPServerSnapshot(
            server_id=server_id,
            server_name=server_name,
            publisher=publisher,
            endpoint_url=fallback_url,
            endpoint_type="unknown",
            retrieved_at=now,
            cached=False,
            capability=MCPServerCapability(
                handshake_ok=False,
                tools_count=0,
                resources_count=0,
                auth_used=False,
                errors=[f"Policy rejection: {policy.reason}"],
            ),
            items=[],
            raw_hash=compute_stable_hash({"server_id": server_id, "policy_rejection": policy.reason}),
            error=f"Policy rejection: {policy.reason}",
        )
        source_record, _insights = normalize_server_snapshot(snapshot)
        connectivity_row = {
            "run_id": run_id,
            "server_id": server_id,
            "server_name": server_name,
            "endpoint_url": fallback_url,
            "endpoint_type": "policy_rejected",
            "connectable": False,
            "status": "failed",
            "error_class": "policy_rejection",
            "error_detail": policy.reason,
            "started_at": now.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "endpoint_host": "",
            "endpoint_port": 0,
            "ip_family_attempted": "",
        }
        return ServerProcessResult(
            snapshot=snapshot,
            source_record=source_record,
            connectable=False,
            live_ok=False,
            error={
                "server_id": server_id,
                "endpoint_url": fallback_url,
                "endpoint_type": "policy_rejected",
                "error_class": "policy_rejection",
                "error_detail": policy.reason,
            },
            connectivity_row=connectivity_row,
            cached_used=False,
        )

    selected_endpoint = endpoints[0] if endpoints else EndpointCandidate(
        url=DEFAULT_REGISTRY_URL,
        endpoint_type="unknown_remote_type",
        connectable=False,
    )

    connectable_candidates = [candidate for candidate in endpoints if candidate.connectable]
    connectable = bool(connectable_candidates)
    auth_missing_blocked = False

    capability_payload: dict[str, Any] = {
        "handshake_ok": False,
        "tools_count": 0,
        "resources_count": 0,
        "auth_used": False,
        "errors": [],
        "error_class": None,
        "error_detail": None,
        "endpoint_host": None,
        "endpoint_port": None,
        "ip_family_attempted": None,
        "handshake": {},
        "tools": [],
        "resources": [],
        "transport": selected_endpoint.endpoint_type,
    }

    fetched_items: list[MCPItem] = []
    error_class: str | None = None
    server_error = ""
    live_ok = False

    if not endpoints:
        error_class = "no_connectable_remote"
        server_error = "no connectable remote endpoint"
        capability_payload["errors"] = [server_error]
    elif not connectable_candidates:
        if any(candidate.endpoint_type == "templated_unresolved" for candidate in endpoints):
            error_class = "templated_unresolved"
            server_error = "templated endpoint unresolved"
        else:
            error_class = "unknown_endpoint_type"
            server_error = "unknown endpoint type"
        capability_payload["errors"] = [server_error]
    else:
        for endpoint in connectable_candidates:
            selected_endpoint = endpoint

            if time.monotonic() > deadline:
                error_class = "connect_timeout"
                server_error = "server budget exceeded before endpoint attempts"
                break

            if endpoint.endpoint_type == "websocket":
                error_class = "unsupported_ws"
                server_error = "websocket transport not supported"
                capability_payload["errors"] = [server_error]
                capability_payload["error_class"] = error_class
                capability_payload["error_detail"] = server_error
                continue

            auth = _resolve_auth(server_id, endpoint.url)
            remaining = max(0.5, deadline - time.monotonic())
            endpoint_payload = {
                "url": endpoint.url,
                "endpoint_type": endpoint.endpoint_type,
                "ipv6_unhealthy": ipv6_unhealthy,
                "connect_timeout_seconds": min(settings.mcp_connect_timeout_seconds, max(1, int(remaining))),
                "read_timeout_seconds": min(settings.mcp_read_timeout_seconds, max(1, int(remaining))),
            }
            client = connect_server(endpoint_payload, auth, settings)
            capability_payload = handshake_and_capabilities(client)

            if capability_payload.get("handshake_ok"):
                live_ok = True
                if endpoint.endpoint_type == "http" and (deadline - time.monotonic()) > 0.5:
                    fetch_payload = fetch_server_snapshot(client, capability_payload)
                    fetched_items = fetch_payload["items"]
                    capability_payload["errors"] = fetch_payload["errors"]
                break

            error_class = str(capability_payload.get("error_class") or _classify_error(
                str(capability_payload.get("error_detail") or capability_payload.get("errors", [""])[0]),
                endpoint.endpoint_type,
            ))
            server_error = str(capability_payload.get("error_detail") or capability_payload.get("errors", ["handshake failed"])[0])
            response_status = int(capability_payload.get("response_status") or 0)
            if (response_status in {401, 403} or error_class == "auth_required") and not auth:
                auth_missing_blocked = True

    if not live_ok and not server_error:
        server_error = "; ".join(str(item) for item in capability_payload.get("errors", []))

    if not live_ok and not error_class:
        error_class = _classify_error(server_error, selected_endpoint.endpoint_type)

    if not live_ok and auth_missing_blocked:
        connectable = False
        error_class = "auth_required"
        if not server_error:
            server_error = "authentication required; token missing"
    elif not live_ok and error_class == "protocol_mismatch":
        connectable = False
    elif not live_ok and error_class == "dns_err":
        connectable = False

    cached_used = False
    if not live_ok:
        cached_snapshot = _load_cached_snapshot(server_id)
        if cached_snapshot and not settings.mcp_live_required:
            cached_used = True
            snapshot = cached_snapshot.model_copy(
                update={
                    "cached": True,
                    "retrieved_at": datetime.now(UTC),
                    "error": server_error,
                }
            )
        else:
            snapshot = _build_failed_snapshot(
                server_id=server_id,
                server_name=server_name,
                publisher=publisher,
                endpoint=selected_endpoint,
                capability_payload=capability_payload,
                server_error=server_error,
            )
    else:
        items = [item.model_copy(update={"server_id": server_id}) for item in fetched_items]
        snapshot_payload = {
            "server_id": server_id,
            "server_name": server_name,
            "publisher": publisher,
            "endpoint_url": selected_endpoint.url,
            "endpoint_type": selected_endpoint.endpoint_type,
            "retrieved_at": datetime.now(UTC).isoformat(),
            "capability": capability_payload,
            "items": [item.model_dump(mode="json") for item in items],
        }
        snapshot = MCPServerSnapshot(
            server_id=server_id,
            server_name=server_name,
            publisher=publisher,
            endpoint_url=selected_endpoint.url,
            endpoint_type=selected_endpoint.endpoint_type,
            retrieved_at=datetime.now(UTC),
            cached=False,
            capability={
                "handshake_ok": True,
                "tools_count": capability_payload.get("tools_count", 0),
                "resources_count": capability_payload.get("resources_count", 0),
                "auth_used": capability_payload.get("auth_used", False),
                "errors": capability_payload.get("errors", []),
            },
            items=items,
            raw_hash=compute_stable_hash(snapshot_payload),
            error=None,
        )

    source_record, _insight_candidates = normalize_server_snapshot(snapshot)

    row = {
        "run_id": run_id,
        "server_id": server_id,
        "server_name": server_name,
        "endpoint_url": selected_endpoint.url,
        "endpoint_type": selected_endpoint.endpoint_type,
        "connectable": connectable,
        "status": "ok" if live_ok else "failed",
        "error_class": None if live_ok else error_class,
        "error_detail": None if live_ok else server_error,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "endpoint_host": capability_payload.get("endpoint_host") or (urlparse(selected_endpoint.url).hostname or ""),
        "endpoint_port": capability_payload.get("endpoint_port") or _safe_url_port(selected_endpoint.url),
        "ip_family_attempted": capability_payload.get("ip_family_attempted"),
    }

    failure = None
    if not live_ok:
        failure = {
            "server_id": server_id,
            "endpoint_type": selected_endpoint.endpoint_type,
            "error_class": str(error_class or "http_err"),
            "error": server_error,
            "error_detail": server_error,
            "endpoint_host": str(row.get("endpoint_host") or ""),
            "endpoint_port": str(row.get("endpoint_port") or ""),
            "ip_family_attempted": str(row.get("ip_family_attempted") or ""),
            "cached": "true" if snapshot.cached else "false",
        }

    return ServerProcessResult(
        snapshot=snapshot,
        source_record=source_record,
        connectable=connectable,
        live_ok=live_ok,
        error=failure,
        connectivity_row=row,
        cached_used=cached_used,
    )


def mcp_network_diagnostics(settings: Settings | None = None, limit: int = 10) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    hosts = diagnostic_registry_hosts(settings=settings, limit=limit)
    if not hosts:
        hosts = ["registry.modelcontextprotocol.io"]

    ipv6_unhealthy, ipv6_probe_error = ipv6_health_probe()

    host_checks: list[dict[str, Any]] = []
    dns_v4_ok_count = 0
    dns_v6_ok_count = 0
    tcp_v4_ok_count = 0
    tcp_v6_ok_count = 0
    tls_v4_ok_count = 0
    tls_v6_ok_count = 0
    dns_resolution_failures: list[dict[str, Any]] = []
    tcp_connectivity_failures: list[dict[str, Any]] = []
    tls_handshake_failures: list[dict[str, Any]] = []

    for host in hosts:
        dns_v4_ok, addresses_v4, dns_v4_error = _resolve_family(host, socket.AF_INET)
        dns_v6_ok, addresses_v6, dns_v6_error = _resolve_family(host, socket.AF_INET6)
        tcp_v4_ok, tcp_v4_error = _tcp_family(host, 443, socket.AF_INET, timeout_seconds=2.0) if dns_v4_ok else (False, dns_v4_error)
        tcp_v6_ok, tcp_v6_error = _tcp_family(host, 443, socket.AF_INET6, timeout_seconds=2.0) if dns_v6_ok else (False, dns_v6_error)
        tls_v4_ok, tls_v4_error = _tls_family(host, 443, socket.AF_INET, timeout_seconds=2.0) if tcp_v4_ok else (False, tcp_v4_error)
        tls_v6_ok, tls_v6_error = _tls_family(host, 443, socket.AF_INET6, timeout_seconds=2.0) if tcp_v6_ok else (False, tcp_v6_error)

        dns_v4_ok_count += 1 if dns_v4_ok else 0
        dns_v6_ok_count += 1 if dns_v6_ok else 0
        tcp_v4_ok_count += 1 if tcp_v4_ok else 0
        tcp_v6_ok_count += 1 if tcp_v6_ok else 0
        tls_v4_ok_count += 1 if tls_v4_ok else 0
        tls_v6_ok_count += 1 if tls_v6_ok else 0

        if not dns_v4_ok and not dns_v6_ok:
            dns_resolution_failures.append(
                {
                    "host": host,
                    "dns_v4_error": dns_v4_error,
                    "dns_v6_error": dns_v6_error,
                }
            )
        elif not tcp_v4_ok and not tcp_v6_ok:
            tcp_connectivity_failures.append(
                {
                    "host": host,
                    "tcp_443_v4_error": tcp_v4_error,
                    "tcp_443_v6_error": tcp_v6_error,
                }
            )
        elif not tls_v4_ok and not tls_v6_ok:
            tls_handshake_failures.append(
                {
                    "host": host,
                    "tls_443_v4_error": tls_v4_error,
                    "tls_443_v6_error": tls_v6_error,
                }
            )

        host_checks.append(
            {
                "host": host,
                "dns_v4_ok": dns_v4_ok,
                "addresses_v4": addresses_v4,
                "dns_v4_error": dns_v4_error,
                "dns_v6_ok": dns_v6_ok,
                "addresses_v6": addresses_v6,
                "dns_v6_error": dns_v6_error,
                "tcp_443_v4_ok": tcp_v4_ok,
                "tcp_443_v4_error": tcp_v4_error,
                "tcp_443_v6_ok": tcp_v6_ok,
                "tcp_443_v6_error": tcp_v6_error,
                "tls_443_v4_ok": tls_v4_ok,
                "tls_443_v4_error": tls_v4_error,
                "tls_443_v6_ok": tls_v6_ok,
                "tls_443_v6_error": tls_v6_error,
            }
        )

    proxy_env_detected = _proxy_env_detected(settings)
    if settings.proxy_mode == "force_proxy" and not proxy_env_detected:
        environment_verdict = "proxy_required"
    elif dns_v4_ok_count == 0 and dns_v6_ok_count == 0:
        environment_verdict = "dns_broken"
    elif tcp_v4_ok_count > 0 and (ipv6_unhealthy or (dns_v6_ok_count > 0 and tcp_v6_ok_count == 0)):
        environment_verdict = "ipv6_broken"
    elif tcp_v4_ok_count > 0 or tcp_v6_ok_count > 0:
        environment_verdict = "ok"
    else:
        environment_verdict = "dns_broken"

    if settings.proxy_mode == "force_proxy":
        network_profile = "proxy_required"
    elif dns_v4_ok_count == 0 and dns_v6_ok_count == 0:
        network_profile = "dns_broken"
    elif tcp_v4_ok_count > 0 and tcp_v6_ok_count > 0:
        network_profile = "dual_stack_working"
    elif tcp_v4_ok_count > 0 and (ipv6_unhealthy or tcp_v6_ok_count == 0):
        network_profile = "ipv6_broken"
    elif tcp_v4_ok_count > 0:
        network_profile = "ipv4_only_working"
    else:
        network_profile = "dns_broken"

    return {
        "hosts": host_checks,
        "dns_v4_ok_count": dns_v4_ok_count,
        "dns_v6_ok_count": dns_v6_ok_count,
        "tcp_v4_ok_count": tcp_v4_ok_count,
        "tcp_v6_ok_count": tcp_v6_ok_count,
        "tls_v4_ok_count": tls_v4_ok_count,
        "tls_v6_ok_count": tls_v6_ok_count,
        "dns_resolution_failures": dns_resolution_failures,
        "tcp_connectivity_failures": tcp_connectivity_failures,
        "tls_handshake_failures": tls_handshake_failures,
        "ipv6_unhealthy": ipv6_unhealthy,
        "ipv6_probe_error": ipv6_probe_error,
        "proxy_env_detected": proxy_env_detected,
        "proxy_mode": settings.proxy_mode,
        "network_profile": network_profile,
        "environment_verdict": environment_verdict,
    }


def _apply_priority_scope(records: list[dict[str, Any]], settings: Settings) -> list[dict[str, Any]]:
    mode = (settings.mcp_priority_mode or "all").strip().lower()
    prioritized = settings.prioritized_mcp_server_set()
    if mode == "all" or not prioritized:
        return records

    def _name(record: dict[str, Any]) -> str:
        return str(_server_payload(record).get("name", "")).strip()

    priority_rows = [record for record in records if _name(record) in prioritized]
    non_priority_rows = [record for record in records if _name(record) not in prioritized]

    if mode == "prioritized_only":
        return priority_rows
    if mode == "prioritized_first":
        return priority_rows + non_priority_rows
    return records


def ingest_mcp_omni(
    settings: Settings | None = None,
    enforce_live_gate: bool = False,
) -> MCPIngestionResult:
    settings = settings or Settings.from_env()
    registry_path = PROJECT_ROOT / settings.mcp_registry_snapshot_path
    server_records = parse_registry_snapshot(registry_path)
    server_records = _apply_priority_scope(server_records, settings)
    connector_records = _load_connector_candidate_records(settings)
    mode = (settings.mcp_priority_mode or "").strip().lower()
    if connector_records and mode == "connectors_only":
        server_records = connector_records
    elif connector_records:
        seen_names: set[str] = set()
        merged: list[dict[str, Any]] = []
        for row in [*server_records, *connector_records]:
            name = str(_server_payload(row).get("name", "")).strip()
            if name and name in seen_names:
                continue
            if name:
                seen_names.add(name)
            merged.append(row)
        server_records = merged

    db_path = get_db_path(settings=settings)
    conn = connect(db_path)
    init_db(conn, SCHEMA_PATH)
    _ensure_connectivity_columns(conn)
    _ensure_mcp_items_columns(conn)

    run_id = uuid.uuid4().hex
    source_records: list[SourceRecord] = []
    snapshots: list[MCPServerSnapshot] = []
    errors: list[dict[str, str]] = []
    error_class_counts: dict[str, int] = defaultdict(int)

    network_snapshot = mcp_network_diagnostics(settings=settings, limit=10)
    ipv6_unhealthy = bool(network_snapshot.get("ipv6_unhealthy", False))

    workers = max(1, min(settings.mcp_max_workers, max(1, len(server_records))))
    processed: list[ServerProcessResult] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(_process_server, record, settings, run_id, ipv6_unhealthy)
            for record in server_records
        ]
        for future in as_completed(futures):
            processed.append(future.result())

    connectable_servers = 0
    live_success_count = 0
    cached_used = False

    for result in processed:
        _persist_snapshot_file(result.snapshot)
        persist_server_snapshot(conn, result.snapshot, run_id=run_id)
        _persist_connectivity_row(conn, result.connectivity_row)

        source_records.append(result.source_record)
        snapshots.append(result.snapshot)

        if result.connectable:
            connectable_servers += 1
        if result.live_ok:
            live_success_count += 1
        if result.cached_used:
            cached_used = True

        if result.error:
            errors.append(result.error)
            klass = str(result.error.get("error_class") or "http_err")
            error_class_counts[klass] += 1
            logger.warning(
                "mcp_connectivity_failed server_id=%s endpoint_type=%s error_class=%s detail=%s",
                result.error.get("server_id"),
                result.error.get("endpoint_type"),
                klass,
                result.error.get("error_detail"),
            )

    conn.commit()
    conn.close()

    coverage = _coverage_payload(
        run_id=run_id,
        total_servers=len(server_records),
        connectable_servers=connectable_servers,
        live_success_count=live_success_count,
        error_class_counts=dict(error_class_counts),
        failures=errors,
        network_snapshot=network_snapshot,
    )

    if enforce_live_gate and settings.mcp_live_required:
        _enforce_live_coverage_gate(settings, coverage)

    return MCPIngestionResult(
        source_records=source_records,
        snapshots=snapshots,
        errors=errors,
        total_servers=len(server_records),
        connected_servers=live_success_count,
        cached_used=cached_used,
        connectable_servers=connectable_servers,
        live_success_count=live_success_count,
        live_success_ratio=float(coverage["live_success_ratio"]),
        error_class_counts=dict(error_class_counts),
        run_id=run_id,
        ipv6_unhealthy=bool(coverage.get("ipv6_unhealthy", False)),
        proxy_env_detected=bool(coverage.get("proxy_env_detected", False)),
        sampled_dns_v4_ok_count=int(coverage.get("dns_v4_ok_count", 0)),
        sampled_tcp_v4_ok_count=int(coverage.get("tcp_v4_ok_count", 0)),
    )


def fetch_mcp_sources(settings: Settings | None = None) -> list[SourceRecord]:
    return ingest_mcp_omni(settings=settings, enforce_live_gate=False).source_records


def latest_mcp_run_summary(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    db_path = get_db_path(settings=settings)
    conn = connect(db_path)
    init_db(conn, SCHEMA_PATH)
    _ensure_connectivity_columns(conn)

    row = conn.execute(
        """
        SELECT run_id
        FROM mcp_connectivity_runs
        ORDER BY finished_at DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        conn.close()
        return {
            "run_id": None,
            "total_servers": 0,
            "connectable_servers": 0,
            "live_success_count": 0,
            "live_success_ratio": 0.0,
            "error_class_counts": {},
            "top_failing_hosts": [],
            "top_success_servers": [],
        }

    run_id = str(row["run_id"])
    rows = conn.execute(
        """
        SELECT server_id, endpoint_url, endpoint_type, connectable, status,
               error_class, error_detail, endpoint_host, endpoint_port, ip_family_attempted
        FROM mcp_connectivity_runs
        WHERE run_id = ?
        ORDER BY finished_at DESC
        """,
        (run_id,),
    ).fetchall()
    conn.close()

    total_servers = len(rows)
    connectable_servers = sum(1 for item in rows if int(item["connectable"] or 0) == 1)
    live_success_count = sum(1 for item in rows if str(item["status"]) == "ok")
    live_success_ratio = live_success_count / max(connectable_servers, 1)

    counts: dict[str, int] = defaultdict(int)
    top_failing_hosts: list[dict[str, Any]] = []
    top_success_servers: list[dict[str, Any]] = []
    for item in rows:
        if str(item["status"]) == "ok":
            if len(top_success_servers) < 10:
                top_success_servers.append(
                    {
                        "server_id": str(item["server_id"]),
                        "endpoint_host": str(item["endpoint_host"] or ""),
                        "endpoint_port": int(item["endpoint_port"] or 0),
                        "endpoint_type": str(item["endpoint_type"] or ""),
                        "status": "ok",
                        "ip_family_attempted": str(item["ip_family_attempted"] or ""),
                    }
                )
            continue
        klass = str(item["error_class"] or "http_err")
        counts[klass] += 1
        if len(top_failing_hosts) < 10:
            top_failing_hosts.append(
                {
                    "server_id": str(item["server_id"]),
                    "endpoint_host": str(item["endpoint_host"] or ""),
                    "endpoint_port": int(item["endpoint_port"] or 0),
                    "endpoint_type": str(item["endpoint_type"] or ""),
                    "error_class": klass,
                    "error_detail": str(item["error_detail"] or ""),
                    "ip_family_attempted": str(item["ip_family_attempted"] or ""),
                }
            )

    return {
        "run_id": run_id,
        "total_servers": total_servers,
        "connectable_servers": connectable_servers,
        "live_success_count": live_success_count,
        "live_success_ratio": round(live_success_ratio, 6),
        "error_class_counts": dict(counts),
        "top_failing_hosts": top_failing_hosts,
        "top_success_servers": top_success_servers,
    }


def diagnostic_registry_hosts(settings: Settings | None = None, limit: int = 10) -> list[str]:
    settings = settings or Settings.from_env()
    registry_path = PROJECT_ROOT / settings.mcp_registry_snapshot_path
    records = parse_registry_snapshot(registry_path)
    records = _apply_priority_scope(records, settings)

    hosts: list[str] = []
    seen: set[str] = set()

    for record in records:
        endpoints = resolve_server_endpoints(record)
        for endpoint in endpoints:
            host = urlparse(str(endpoint.get("url", ""))).hostname
            if not host:
                continue
            if host in seen:
                continue
            seen.add(host)
            hosts.append(host)
            break
        if len(hosts) >= limit:
            break

    return hosts
