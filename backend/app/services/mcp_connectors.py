from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from typing import Any

from app.config import Settings, get_repo_root
from app.services.provider_adapters import ProviderAdapterError, fetch_provider_data


_DEFAULT_PROVIDER_ROWS: dict[str, dict[str, Any]] = {
    "finnhub": {
        "name": "Finnhub Market Data",
        "provider_name": "finnhub",
        "publisher": "Finnhub",
        "category": "market_data",
        "priority": "high",
        "importance_note": "Primary live quote and news connector for investor-facing market and signal surfaces.",
        "required_env": ["FINNHUB_API_KEY"],
        "allow_missing_env": False,
        "supported_families": ["quote_latest", "fx", "reference_meta", "news_general"],
        "default_identifiers": {
            "quote_latest": "AAPL",
            "fx": "USD/SGD",
            "reference_meta": "AAPL",
            "news_general": "general?limit=5",
        },
    },
    "nasdaq_data_link": {
        "name": "Nasdaq Data Link Research",
        "provider_name": "nasdaq_data_link",
        "publisher": "Nasdaq Data Link",
        "category": "research_data",
        "priority": "high",
        "importance_note": "Research dataset connector used for ETF and structural reference datasets where entitlement is available.",
        "required_env": ["NASDAQ_DATA_LINK_API_KEY"],
        "allow_missing_env": False,
        "supported_families": ["research_dataset"],
        "default_identifiers": {
            "research_dataset": "ETFG/FUND?ticker=SPY",
        },
    },
}


def normalize_connector_env_key(name: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "_", name.strip().upper())
    return value.strip("_")


def connector_slug(name: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", name.strip().lower())
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value or "connector"


def load_connector_candidates(settings: Settings | None = None) -> list[dict[str, Any]]:
    active_settings = settings or Settings.from_env()
    path = get_repo_root() / active_settings.mcp_connectors_candidates_path
    if not path.exists():
        return [dict(row) for row in _DEFAULT_PROVIDER_ROWS.values()]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return [dict(row) for row in _DEFAULT_PROVIDER_ROWS.values()]
    if not isinstance(payload, list):
        return [dict(row) for row in _DEFAULT_PROVIDER_ROWS.values()]
    rows = [dict(item) for item in payload if isinstance(item, dict)]
    return rows or [dict(row) for row in _DEFAULT_PROVIDER_ROWS.values()]


def _provider_name(row: dict[str, Any]) -> str:
    explicit = str(row.get("provider_name") or "").strip().lower()
    if explicit:
        return explicit
    name = str(row.get("name") or "").strip().lower()
    if "finnhub" in name:
        return "finnhub"
    if "nasdaq" in name or "quandl" in name:
        return "nasdaq_data_link"
    return name.replace(" ", "_")


def find_connector_candidate(name_or_slug: str, settings: Settings | None = None) -> dict[str, Any] | None:
    needle = str(name_or_slug or "").strip().lower()
    if not needle:
        return None
    for row in load_connector_candidates(settings=settings):
        names = {
            str(row.get("name") or "").strip().lower(),
            connector_slug(str(row.get("name") or "")),
            _provider_name(row),
        }
        if needle in names:
            return row
    return None


def find_connector_by_keywords(*keywords: str, settings: Settings | None = None) -> dict[str, Any] | None:
    normalized_keywords = [str(item).strip().lower() for item in keywords if str(item).strip()]
    if not normalized_keywords:
        return None
    for row in load_connector_candidates(settings=settings):
        haystack = " ".join(
            [
                str(row.get("name") or ""),
                str(row.get("provider_name") or ""),
                str(row.get("publisher") or ""),
                str(row.get("category") or ""),
                str(row.get("source_url") or ""),
            ]
        ).lower()
        if all(keyword in haystack for keyword in normalized_keywords):
            return row
    return None


def required_env_status(row: dict[str, Any]) -> dict[str, bool]:
    required_env = [str(item).strip() for item in (row.get("required_env") or []) if str(item).strip()]
    return {name: bool(os.getenv(name, "").strip()) for name in required_env}


def connector_mode(row: dict[str, Any]) -> str:
    env_status = required_env_status(row)
    if env_status and all(env_status.values()):
        return "provider_backed"
    if bool(row.get("allow_missing_env", False)):
        return "public_fallback"
    return "credentials_missing"


def connector_source_url(row: dict[str, Any]) -> str:
    explicit = str(row.get("source_url") or "").strip()
    if explicit:
        return explicit
    provider_name = _provider_name(row)
    if provider_name == "finnhub":
        return "https://finnhub.io/docs/api"
    if provider_name == "nasdaq_data_link":
        return "https://data.nasdaq.com/tools/api"
    return "https://modelcontextprotocol.io/"


def connector_supported_families(row: dict[str, Any]) -> list[str]:
    families = [str(item).strip() for item in (row.get("supported_families") or []) if str(item).strip()]
    if families:
        return families
    provider_name = _provider_name(row)
    defaults = _DEFAULT_PROVIDER_ROWS.get(provider_name, {})
    return [str(item).strip() for item in (defaults.get("supported_families") or []) if str(item).strip()]


def connector_default_identifier(row: dict[str, Any], endpoint_family: str) -> str | None:
    family_key = str(endpoint_family or "").strip()
    explicit = row.get("default_identifiers") or {}
    if isinstance(explicit, dict):
        value = str(explicit.get(family_key) or "").strip()
        if value:
            return value
    defaults = (_DEFAULT_PROVIDER_ROWS.get(_provider_name(row), {}).get("default_identifiers") or {})
    value = str(defaults.get(family_key) or "").strip()
    return value or None


def connector_resources_payload(row: dict[str, Any]) -> list[dict[str, str]]:
    slug = connector_slug(str(row.get("name") or "connector"))
    return [
        {
            "uri": f"mcp://{slug}/status",
            "title": "Connector runtime status",
            "mimeType": "application/json",
        },
        {
            "uri": f"mcp://{slug}/source",
            "title": "Connector source metadata",
            "mimeType": "application/json",
        },
    ]


def connector_status_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": str(row.get("name") or ""),
        "slug": connector_slug(str(row.get("name") or "")),
        "provider_name": _provider_name(row),
        "publisher": str(row.get("publisher") or ""),
        "category": str(row.get("category") or "uncategorized"),
        "priority": str(row.get("priority") or "medium"),
        "mode": connector_mode(row),
        "required_env_present": required_env_status(row),
        "supported_families": connector_supported_families(row),
        "source_url": connector_source_url(row),
        "retrieved_at": datetime.now(UTC).isoformat(),
    }


def connector_tools_payload(row: dict[str, Any]) -> list[dict[str, Any]]:
    supported_families = connector_supported_families(row)
    return [
        {
            "name": "search",
            "description": "Search connector runtime status and source metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
        },
        {
            "name": "fetch",
            "description": "Fetch provider-backed data for this connector.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "endpoint_family": {"type": "string", "enum": supported_families},
                    "identifier": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
                "required": ["endpoint_family"],
            },
        },
    ]


def connector_fetch(
    row: dict[str, Any],
    endpoint_family: str,
    identifier: str | None = None,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    provider_name = _provider_name(row)
    supported_families = connector_supported_families(row)
    if endpoint_family not in supported_families:
        raise ProviderAdapterError(provider_name, endpoint_family, f"Unsupported family for connector: {endpoint_family}", error_class="unsupported")

    resolved_identifier = str(identifier or connector_default_identifier(row, endpoint_family) or "").strip()
    if endpoint_family == "news_general":
        base_identifier = resolved_identifier or "general"
        if limit is not None:
            if "?" in base_identifier:
                resolved_identifier = f"{base_identifier}&limit={int(limit)}"
            else:
                resolved_identifier = f"{base_identifier}?limit={int(limit)}"
        else:
            resolved_identifier = base_identifier
    elif not resolved_identifier:
        raise ProviderAdapterError(provider_name, endpoint_family, "Missing identifier for connector fetch", error_class="missing_identifier")

    return fetch_provider_data(provider_name, endpoint_family, resolved_identifier)


def probe_connector(row: dict[str, Any]) -> dict[str, Any]:
    status = connector_status_payload(row)
    result = {
        **status,
        "configured": status["mode"] != "credentials_missing",
        "run_attempted": False,
        "live_ok": False,
        "items_count": 0,
        "error_class": "",
        "error_detail": "",
        "probe_summary": None,
    }
    if status["mode"] == "credentials_missing":
        result["error_class"] = "missing_env"
        result["error_detail"] = "Missing required env vars for provider-backed connector."
        return result

    families = connector_supported_families(row)
    if not families:
        result["error_class"] = "unsupported"
        result["error_detail"] = "Connector does not declare any supported families."
        return result

    family = families[0]
    identifier = connector_default_identifier(row, family)
    result["run_attempted"] = True
    try:
        payload = connector_fetch(row, family, identifier)
    except ProviderAdapterError as exc:
        result["error_class"] = exc.error_class
        result["error_detail"] = str(exc)
        return result

    value = payload.get("value")
    if isinstance(value, list):
        items_count = len(value)
        preview = value[:1]
    elif isinstance(value, dict):
        items_count = len(value)
        preview = dict(list(value.items())[:3])
    else:
        items_count = 1 if value not in {None, ""} else 0
        preview = value

    result["live_ok"] = items_count > 0
    result["items_count"] = items_count
    result["probe_summary"] = {
        "endpoint_family": family,
        "identifier": identifier,
        "observed_at": payload.get("observed_at"),
        "source_ref": payload.get("source_ref"),
        "preview": preview,
    }
    if not result["live_ok"]:
        result["error_class"] = "empty_response"
        result["error_detail"] = "Connector fetch returned no usable payload."
    return result


def connector_is_provider_backed(*keywords: str, settings: Settings | None = None) -> bool:
    row = find_connector_by_keywords(*keywords, settings=settings)
    return bool(row) and connector_mode(row) == "provider_backed"


def jsonrpc_ok(rpc_id: object, payload: dict | list) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": rpc_id, "result": payload}


def jsonrpc_error(rpc_id: object, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": rpc_id, "error": {"code": code, "message": message}}
