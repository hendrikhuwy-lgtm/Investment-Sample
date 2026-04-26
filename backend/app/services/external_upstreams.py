from __future__ import annotations

import importlib.util
import os
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from typing import Any

import requests

from app.config import Settings
from app.services.blueprint_candidate_registry import export_live_candidate_registry
from app.services.data_governance import build_governance_record
from app.services.portfolio_ingest import latest_snapshot_rows
from app.services.provider_registry import PROVIDER_CAPABILITY_MATRIX, family_ownership_map
from app.services.public_upstream_snapshots import ensure_public_upstream_snapshot_tables, put_public_upstream_snapshot


_CACHE_TTL_SECONDS = 60 * 60 * 6
_CACHE: dict[str, Any] = {"generated_at": 0.0, "payload": None}

_ECB_HEADERS = {"User-Agent": "investment-agent/0.1"}
_SEC_HEADERS = {"User-Agent": "investment-agent/0.1 contact@local.invalid"}
_GENERIC_HEADERS = {"User-Agent": "investment-agent/0.1"}


def _now() -> datetime:
    return datetime.now(UTC)


def _iso_now() -> str:
    return _now().isoformat()


def _public_family_name(provider_key: str) -> str:
    return {
        "ecb_data_api": "fx",
        "world_bank_indicators": "structural_macro_context",
        "cftc_cot": "positioning",
        "sec_edgar": "filings_context",
    }.get(str(provider_key), "structural_macro_context")


def _public_source_url(context: dict[str, Any]) -> str | None:
    for item in list(context.get("items") or []):
        citation = dict(item.get("citation") or {})
        url = str(citation.get("url") or "").strip()
        if url:
            return url
    return None


def _public_freshness_state(provider_key: str, observed_at: str | None) -> str:
    family = _public_family_name(provider_key)
    cadence_seconds = int((family_ownership_map().get(family) or {}).get("refresh_cadence_seconds") or 86400)
    governance = build_governance_record(
        source_name=provider_key,
        provider_family=family,
        fetched_at=_iso_now(),
        observed_at=observed_at,
        cadence_seconds=cadence_seconds,
        fallback_used=False,
        cache_status="miss",
        error_state=None,
        source_tier="public",
    )
    return str(governance.get("freshness_state") or "unknown")


def _persist_public_context(conn: sqlite3.Connection | None, context: dict[str, Any]) -> None:
    if conn is None:
        return
    ensure_public_upstream_snapshot_tables(conn)
    provider_key = str(context.get("provider_key") or "")
    family_name = _public_family_name(provider_key)
    put_public_upstream_snapshot(
        conn,
        provider_key=provider_key,
        family_name=family_name,
        surface_usage=["daily_brief", "blueprint", "dashboard"],
        payload=context,
        source_url=_public_source_url(context),
        observed_at=str(context.get("observed_at") or "") or None,
        freshness_state=_public_freshness_state(provider_key, str(context.get("observed_at") or "") or None),
        error_state=None if str(context.get("status") or "") == "ok" else str(context.get("status") or "unavailable"),
    )


def _safe_get_json(url: str, *, headers: dict[str, str] | None = None, timeout: int = 12) -> Any:
    response = requests.get(url, headers=headers or _GENERIC_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _safe_get_text(url: str, *, headers: dict[str, str] | None = None, timeout: int = 12) -> str:
    response = requests.get(url, headers=headers or _GENERIC_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def _lag_days(observed_at: str | None) -> int | None:
    if not observed_at:
        return None
    try:
        observed = datetime.fromisoformat(str(observed_at).replace("Z", "+00:00"))
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=UTC)
        return max(0, (_now() - observed).days)
    except Exception:
        try:
            observed = datetime.fromisoformat(f"{observed_at}T00:00:00+00:00")
            return max(0, (_now() - observed).days)
        except Exception:
            return None


def _ecb_latest_value(flow: str) -> tuple[str, float] | None:
    payload = _safe_get_json(
        f"https://data-api.ecb.europa.eu/service/data/{flow}?format=jsondata",
        headers=_ECB_HEADERS,
    )
    structure = payload.get("structure") or {}
    dims = (((structure.get("dimensions") or {}).get("observation") or [{}])[0].get("values") or [])
    datasets = payload.get("dataSets") or []
    if not datasets:
        return None
    series_map = (datasets[0].get("series") or {})
    if not series_map:
        return None
    series = next(iter(series_map.values()))
    observations = series.get("observations") or {}
    if not observations:
        return None
    latest_idx = max(int(key) for key in observations.keys())
    latest = observations.get(str(latest_idx)) or []
    latest_value = latest[0] if latest else None
    latest_date = ((dims[latest_idx] or {}).get("id") if latest_idx < len(dims) else None) or None
    if latest_date is None or latest_value is None:
        return None
    return str(latest_date), float(latest_value)


def _fetch_ecb_context() -> dict[str, Any]:
    eurusd = _ecb_latest_value("EXR/D.USD.EUR.SP00.A")
    eursgd = _ecb_latest_value("EXR/D.SGD.EUR.SP00.A")
    if not eurusd and not eursgd:
        return {
            "provider_key": "ecb_data_api",
            "status": "unavailable",
            "headline": "ECB context unavailable.",
            "items": [],
        }
    items: list[dict[str, Any]] = []
    observed_dates = [item[0] for item in [eurusd, eursgd] if item]
    if eurusd:
        items.append(
            {
                "metric": "EURUSD_ECB",
                "label": "ECB EUR/USD reference",
                "value": eurusd[1],
                "observed_at": eurusd[0],
                "lag_days": _lag_days(eurusd[0]),
                "summary": f"ECB EUR/USD reference last printed {eurusd[1]:.4f}.",
                "citation": {
                    "source_id": "ecb_exr_usd_eur",
                    "url": "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=jsondata",
                    "publisher": "ECB",
                    "retrieved_at": _iso_now(),
                },
            }
        )
    if eursgd:
        items.append(
            {
                "metric": "EURSGD_ECB",
                "label": "ECB EUR/SGD reference",
                "value": eursgd[1],
                "observed_at": eursgd[0],
                "lag_days": _lag_days(eursgd[0]),
                "summary": f"ECB EUR/SGD reference last printed {eursgd[1]:.4f}.",
                "citation": {
                    "source_id": "ecb_exr_sgd_eur",
                    "url": "https://data-api.ecb.europa.eu/service/data/EXR/D.SGD.EUR.SP00.A?format=jsondata",
                    "publisher": "ECB",
                    "retrieved_at": _iso_now(),
                },
            }
        )
    return {
        "provider_key": "ecb_data_api",
        "status": "ok",
        "headline": "ECB reference rates add non-US FX context for EUR/USD and EUR/SGD translation.",
        "items": items,
        "observed_at": max(observed_dates) if observed_dates else None,
    }


def _latest_world_bank_value(country: str, indicator: str) -> dict[str, Any] | None:
    payload = _safe_get_json(
        f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=8",
        headers=_GENERIC_HEADERS,
    )
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        return None
    for row in payload[1]:
        if row.get("value") is not None:
            return row
    return None


def _fetch_world_bank_context() -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for country, label in [("SGP", "Singapore"), ("CHN", "China"), ("WLD", "World")]:
        row = _latest_world_bank_value(country, "NY.GDP.MKTP.KD.ZG")
        if not row:
            continue
        year = str(row.get("date") or "")
        value = float(row.get("value"))
        items.append(
            {
                "metric": f"WBGDP_{country}",
                "label": f"{label} real GDP growth",
                "value": value,
                "observed_at": year,
                "lag_days": None,
                "summary": f"World Bank latest available {label.lower()} real GDP growth is {value:.1f}% ({year}).",
                "citation": {
                    "source_id": f"world_bank_{country.lower()}_gdp_growth",
                    "url": f"https://api.worldbank.org/v2/country/{country}/indicator/NY.GDP.MKTP.KD.ZG?format=json",
                    "publisher": "World Bank",
                    "retrieved_at": _iso_now(),
                },
            }
        )
    return {
        "provider_key": "world_bank_indicators",
        "status": "ok" if items else "unavailable",
        "headline": "World Bank structural indicators add strategic growth context for Singapore, China, and world demand.",
        "items": items,
        "observed_at": max((str(item.get("observed_at") or "") for item in items), default=None),
    }


def _fetch_cftc_market_snapshot(label: str, pattern: str) -> dict[str, Any] | None:
    payload = _safe_get_json(
        "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
        f"?$select=market_and_exchange_names,report_date_as_yyyy_mm_dd,noncomm_positions_long_all,"
        f"noncomm_positions_short_all,change_in_noncomm_long_all,change_in_noncomm_short_all"
        f"&$where=market_and_exchange_names like '{pattern}'&$order=report_date_as_yyyy_mm_dd DESC&$limit=1",
        headers=_GENERIC_HEADERS,
    )
    if not isinstance(payload, list) or not payload:
        return None
    row = payload[0]
    long_pos = float(row.get("noncomm_positions_long_all") or 0)
    short_pos = float(row.get("noncomm_positions_short_all") or 0)
    long_change = float(row.get("change_in_noncomm_long_all") or 0)
    short_change = float(row.get("change_in_noncomm_short_all") or 0)
    net = long_pos - short_pos
    net_delta = long_change - short_change
    observed = str(row.get("report_date_as_yyyy_mm_dd") or "").split("T")[0]
    return {
        "metric": f"CFTC_{label.upper()}",
        "label": label,
        "value": net,
        "observed_at": observed,
        "lag_days": _lag_days(observed),
        "summary": f"{label} non-commercial net positioning is {net:,.0f}, with weekly net change {net_delta:,.0f}.",
        "citation": {
            "source_id": f"cftc_{label.lower().replace(' ', '_')}",
            "url": "https://publicreporting.cftc.gov/",
            "publisher": "CFTC",
            "retrieved_at": _iso_now(),
        },
    }


def _fetch_cftc_context() -> dict[str, Any]:
    items = [
        _fetch_cftc_market_snapshot("USD index positioning", "%U.S. DOLLAR INDEX%"),
        _fetch_cftc_market_snapshot("10Y Treasury positioning", "%10-YEAR U.S. TREASURY NOTES%"),
    ]
    items = [item for item in items if item]
    return {
        "provider_key": "cftc_cot",
        "status": "ok" if items else "unavailable",
        "headline": "CFTC positioning adds crowding context for dollar and duration exposure.",
        "items": items,
        "observed_at": max((str(item.get("observed_at") or "") for item in items), default=None),
    }


def _fetch_sec_context(conn: sqlite3.Connection | None) -> dict[str, Any]:
    feed_text = _safe_get_text(
        "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=13F-HR&owner=include&count=10&output=atom",
        headers=_SEC_HEADERS,
    )
    root = ET.fromstring(feed_text)
    entries = root.findall("{http://www.w3.org/2005/Atom}entry")
    candidate_count = 0
    holdings_count = 0
    if conn is not None:
        try:
            candidates = export_live_candidate_registry(conn)
            candidate_count = sum(1 for item in candidates if str(item.get("domicile") or "").upper() == "US")
        except Exception:
            candidate_count = 0
        try:
            holdings = latest_snapshot_rows(conn)
            holdings_count = sum(1 for item in holdings if str(item.get("normalized_symbol") or "").strip())
        except Exception:
            holdings_count = 0
    return {
        "provider_key": "sec_edgar",
        "status": "ok",
        "headline": "SEC EDGAR adds real filings and 13F activity context for holdings and U.S.-linked candidates.",
        "items": [
            {
                "metric": "SEC_13F_CURRENT",
                "label": "Recent 13F activity",
                "value": len(entries),
                "observed_at": _iso_now(),
                "lag_days": 0,
                "summary": f"SEC current 13F feed returned {len(entries)} recent entries. US-linked candidates: {candidate_count}. Holdings coverage candidates: {holdings_count}.",
                "citation": {
                    "source_id": "sec_13f_feed",
                    "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=13F-HR&owner=include&count=10&output=atom",
                    "publisher": "SEC",
                    "retrieved_at": _iso_now(),
                },
            }
        ],
        "observed_at": _iso_now(),
    }


def _optional_provider_status(
    key: str,
    label: str,
    env_name: str,
    sections: list[str],
    *,
    signup_url: str | None = None,
    docs_url: str | None = None,
) -> dict[str, Any]:
    configured = bool(os.getenv(env_name, "").strip())
    return {
        "provider_key": key,
        "label": label,
        "status": "configured_optional" if configured else "unconfigured_optional",
        "configured": configured,
        "requires_api_key": True,
        "free_access": True,
        "integrated_sections": sections,
        "detail": f"{label} turns on when {env_name} is configured.",
        "activation_env": env_name,
        "signup_url": signup_url,
        "docs_url": docs_url,
        **_routed_provider_metadata(key),
    }


def _openbb_status() -> dict[str, Any]:
    installed = importlib.util.find_spec("openbb") is not None
    return {
        "provider_key": "openbb_provider_bridge",
        "label": "OpenBB provider bridge",
        "status": "installed_optional" if installed else "not_installed_optional",
        "configured": installed,
        "requires_api_key": False,
        "free_access": True,
        "integrated_sections": ["daily_brief", "blueprint", "dashboard"],
        "detail": "Optional provider abstraction layer for SEC, ECB, World Bank, CFTC, and other upstreams.",
        "install_hint": "cd /Users/huwenyihendrik/Projects/investment-agent/backend && .venv/bin/python -m pip install openbb",
        "docs_url": "https://docs.openbb.co/platform/api_keys",
        "source_scope": "optional_bridge",
        "routed_provider": False,
        "routed_families": [],
    }


def _planned_public_status(key: str, label: str, detail: str, sections: list[str]) -> dict[str, Any]:
    return {
        "provider_key": key,
        "label": label,
        "status": "planned_public",
        "configured": False,
        "requires_api_key": False,
        "free_access": True,
        "integrated_sections": sections,
        "detail": detail,
        "source_scope": "planned_public",
        "routed_provider": False,
        "routed_families": [],
    }


def _routed_provider_metadata(provider_key: str) -> dict[str, Any]:
    if provider_key not in PROVIDER_CAPABILITY_MATRIX:
        return {
            "source_scope": "public_context_only",
            "routed_provider": False,
            "routed_families": [],
        }
    families = sorted(
        family_name
        for family_name in dict(PROVIDER_CAPABILITY_MATRIX.get(provider_key, {}).get("families") or {})
        if family_name
    )
    return {
        "source_scope": "routed_provider",
        "routed_provider": True,
        "routed_families": families,
    }


def _provider_summary(providers: list[dict[str, Any]]) -> dict[str, Any]:
    live = sum(1 for item in providers if str(item.get("status")) in {"ok", "configured_optional", "installed_optional"})
    public_live = sum(1 for item in providers if not bool(item.get("requires_api_key")) and str(item.get("status")) == "ok")
    configured_optional = sum(1 for item in providers if str(item.get("status")) in {"configured_optional", "installed_optional"})
    issues = [item.get("label") for item in providers if str(item.get("status")).startswith("unavailable")]
    return {
        "provider_count": len(providers),
        "live_count": live,
        "public_live_count": public_live,
        "configured_optional_count": configured_optional,
        "issues": issues,
    }


def build_provider_status_registry() -> list[dict[str, Any]]:
    return [
        {
            "provider_key": "ecb_data_api",
            "label": "ECB data API",
            "status": "public_available",
            "configured": True,
            "requires_api_key": False,
            "free_access": True,
            "integrated_sections": ["daily_brief", "blueprint", "dashboard"],
            "detail": "Public non-US FX and rates context source.",
            **_routed_provider_metadata("ecb_data_api"),
        },
        {
            "provider_key": "world_bank_indicators",
            "label": "World Bank Indicators",
            "status": "public_available",
            "configured": True,
            "requires_api_key": False,
            "free_access": True,
            "integrated_sections": ["daily_brief", "blueprint", "dashboard"],
            "detail": "Public structural macro context source.",
            **_routed_provider_metadata("world_bank_indicators"),
        },
        {
            "provider_key": "cftc_cot",
            "label": "CFTC COT",
            "status": "public_available",
            "configured": True,
            "requires_api_key": False,
            "free_access": True,
            "integrated_sections": ["daily_brief", "blueprint", "dashboard"],
            "detail": "Public futures positioning and crowding context source.",
            **_routed_provider_metadata("cftc_cot"),
        },
        {
            "provider_key": "sec_edgar",
            "label": "SEC EDGAR",
            "status": "public_available",
            "configured": True,
            "requires_api_key": False,
            "free_access": True,
            "integrated_sections": ["daily_brief", "blueprint", "dashboard"],
            "detail": "Public U.S. filings and 13F activity source.",
            **_routed_provider_metadata("sec_edgar"),
        },
        {
            "provider_key": "frankfurter",
            "label": "Frankfurter",
            "status": "public_available",
            "configured": True,
            "requires_api_key": False,
            "free_access": True,
            "integrated_sections": ["daily_brief"],
            "detail": "Public FX reference source kept narrow to fx_reference and USD-strength support.",
            **_routed_provider_metadata("frankfurter"),
        },
        _optional_provider_status(
            "alpha_vantage",
            "Alpha Vantage",
            "ALPHA_VANTAGE_API_KEY",
            ["daily_brief", "blueprint", "dashboard"],
            signup_url="https://www.alphavantage.co/support/#api-key",
            docs_url="https://www.alphavantage.co/documentation/",
        ),
        _optional_provider_status(
            "fmp",
            "FinancialModelingPrep",
            "FMP_API_KEY",
            ["daily_brief", "blueprint", "dashboard"],
            signup_url="https://site.financialmodelingprep.com/developer/docs/dashboard",
            docs_url="https://site.financialmodelingprep.com/developer/docs",
        ),
        _optional_provider_status(
            "finnhub",
            "Finnhub",
            "FINNHUB_API_KEY",
            ["daily_brief", "blueprint", "dashboard"],
            signup_url="https://finnhub.io/register",
            docs_url="https://finnhub.io/docs/api",
        ),
        _optional_provider_status(
            "nasdaq_data_link",
            "Nasdaq Data Link",
            "NASDAQ_DATA_LINK_API_KEY",
            ["daily_brief", "blueprint", "dashboard"],
            signup_url="https://data.nasdaq.com/sign-up",
            docs_url="https://docs.data.nasdaq.com/",
        ),
        _optional_provider_status(
            "eodhd",
            "EOD Historical Data",
            "EODHD_API_KEY",
            ["daily_brief", "blueprint", "dashboard"],
            signup_url="https://eodhd.com/financial-apis/quick-start-with-our-financial-data-apis",
            docs_url="https://eodhd.com/financial-apis/",
        ),
        _optional_provider_status(
            "polygon",
            "Polygon",
            "POLYGON_API_KEY",
            ["daily_brief", "blueprint", "dashboard"],
            signup_url="https://polygon.io/dashboard/signup",
            docs_url="https://polygon.io/docs/rest/quickstart",
        ),
        _optional_provider_status(
            "tiingo",
            "Tiingo",
            "TIINGO_API_KEY",
            ["daily_brief", "blueprint", "dashboard"],
            signup_url="https://api.tiingo.com/",
            docs_url="https://www.tiingo.com/documentation/general/overview",
        ),
        _optional_provider_status(
            "twelve_data",
            "Twelve Data",
            "TWELVE_DATA_API_KEY",
            ["daily_brief", "blueprint", "dashboard"],
            signup_url="https://twelvedata.com/apikey",
            docs_url="https://twelvedata.com/docs",
        ),
        _openbb_status(),
        _planned_public_status(
            "central_bank_regulatory_adapter",
            "Central-bank and regulatory adapter",
            "Public policy-statement and regulatory-release adapter path reserved for future ingestion without weakening the current brief.",
            ["daily_brief", "blueprint", "dashboard"],
        ),
        _planned_public_status(
            "arxiv_research_adapter",
            "ArXiv research adapter",
            "Public research-feed bridge reserved for future strategic context and methodology expansion.",
            ["daily_brief", "blueprint", "dashboard"],
        ),
        _planned_public_status(
            "ssrn_research_adapter",
            "SSRN research adapter",
            "Public working-paper bridge reserved for future strategic context and methodology expansion.",
            ["daily_brief", "blueprint", "dashboard"],
        ),
    ]


def build_external_upstream_payload(
    settings: Settings,
    *,
    conn: sqlite3.Connection | None = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    now = time.time()
    if not force_refresh and _CACHE.get("payload") is not None and now - float(_CACHE.get("generated_at") or 0) < _CACHE_TTL_SECONDS:
        cached_payload = dict(_CACHE["payload"])
        if conn is not None:
            for context in list(cached_payload.get("daily_brief_context") or []):
                if isinstance(context, dict):
                    _persist_public_context(conn, context)
        return cached_payload

    public_contexts = []
    providers: list[dict[str, Any]] = []

    for loader, label in [
        (_fetch_ecb_context, "ECB data API"),
        (_fetch_world_bank_context, "World Bank Indicators"),
        (_fetch_cftc_context, "CFTC COT"),
        (lambda: _fetch_sec_context(conn), "SEC EDGAR"),
    ]:
        try:
            context = loader()
            _persist_public_context(conn, context)
            public_contexts.append(context)
            providers.append(
                {
                    "provider_key": context.get("provider_key"),
                    "label": label,
                    "status": context.get("status"),
                    "configured": True,
                    "requires_api_key": False,
                    "free_access": True,
                    "integrated_sections": ["daily_brief", "blueprint", "dashboard"],
                    "detail": context.get("headline"),
                    "observed_at": context.get("observed_at"),
                }
            )
        except Exception as exc:  # noqa: BLE001
            providers.append(
                {
                    "provider_key": label.lower().replace(" ", "_"),
                    "label": label,
                    "status": "unavailable",
                    "configured": True,
                    "requires_api_key": False,
                    "free_access": True,
                    "integrated_sections": ["daily_brief", "blueprint", "dashboard"],
                    "detail": str(exc),
                }
            )

    existing_keys = {str(item.get("provider_key") or "") for item in providers}
    for item in build_provider_status_registry():
        if str(item.get("provider_key") or "") not in existing_keys:
            providers.append(item)

    payload = {
        "generated_at": _iso_now(),
        "summary": _provider_summary(providers),
        "providers": providers,
        "daily_brief_context": public_contexts,
        "blueprint_context": [
            item for item in public_contexts if str(item.get("provider_key")) in {"world_bank_indicators", "sec_edgar", "ecb_data_api"}
        ],
        "dashboard_context": [
            item for item in public_contexts if str(item.get("provider_key")) in {"ecb_data_api", "cftc_cot", "sec_edgar"}
        ],
    }
    _CACHE["generated_at"] = now
    _CACHE["payload"] = dict(payload)
    return payload
