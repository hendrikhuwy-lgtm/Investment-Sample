from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import requests

from app.services.provider_adapters import ProviderAdapterError, fetch_provider_data
from app.v2.donors.source_freshness import FreshnessClass, FreshnessState
from app.v2.sources.execution_envelope import build_provider_execution
from app.v2.sources.freshness import coerce_datetime
from app.v2.sources.freshness_registry import register_source
from app.v2.sources.runtime_truth import record_runtime_truth


source_tier: str = "1B"
_SOURCE_ID = "news"
_USER_AGENT = "investment-agent-v2/0.1"
_LAST_FETCHED_AT: str | None = None
_LAST_PUBLISHED_AT: str | None = None


def _now() -> datetime:
    return datetime.now(UTC)


def _remember(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    global _LAST_FETCHED_AT, _LAST_PUBLISHED_AT

    if items:
        _LAST_FETCHED_AT = _now().isoformat()
        published = [coerce_datetime(item.get("published_utc")) for item in items]
        published = [value for value in published if value is not None]
        if published:
            _LAST_PUBLISHED_AT = max(published).astimezone(UTC).isoformat()
    return items


def _normalize_item(headline: Any, source: Any, published_utc: Any, url: Any) -> dict[str, str | None]:
    published = coerce_datetime(published_utc)
    return {
        "headline": str(headline or "").strip(),
        "source": str(source or "").strip() or "unknown",
        "published_utc": published.astimezone(UTC).isoformat() if published is not None else None,
        "url": str(url or "").strip() or None,
    }


def _fetch_finnhub(limit: int) -> list[dict[str, Any]]:
    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not api_key:
        return []
    try:
        payload = fetch_provider_data("finnhub", "news_general", f"general?limit={max(1, limit)}")
    except ProviderAdapterError:
        return []
    items = payload.get("value") if isinstance(payload, dict) else []
    return list(items) if isinstance(items, list) else []


def _parse_gdelt_timestamp(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.strptime(text, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        parsed = coerce_datetime(text)
    return parsed.astimezone(UTC).isoformat() if parsed is not None else None


def _fetch_gdelt(limit: int) -> list[dict[str, Any]]:
    query = "(finance OR markets OR inflation OR rates)"
    try:
        response = requests.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={
                "query": query,
                "mode": "ArtList",
                "format": "json",
                "sort": "DateDesc",
                "maxrecords": max(1, limit),
            },
            headers={"User-Agent": _USER_AGENT},
            timeout=6,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return []

    items: list[dict[str, Any]] = []
    for row in (payload.get("articles") or [])[: max(0, limit)] if isinstance(payload, dict) else []:
        if not isinstance(row, dict):
            continue
        items.append(
            _normalize_item(
                row.get("title"),
                row.get("sourceCommonName") or row.get("domain"),
                _parse_gdelt_timestamp(row.get("seendate")),
                row.get("url"),
            )
        )
    return items


def fallback() -> list[dict[str, Any]]:
    return []


def unavailable_payload() -> dict[str, Any]:
    return {"error": "news_unavailable", "items": []}


def fetch(limit: int = 10, *, surface_name: str | None = None) -> list[dict[str, Any]]:
    normalized_limit = max(0, int(limit))
    items = _fetch_finnhub(normalized_limit)
    if items:
        traced = [
            {
                **dict(item),
                "provider_execution": build_provider_execution(
                    provider_name="finnhub",
                    source_family="news",
                    identifier=f"limit:{normalized_limit}",
                    observed_at=str(dict(item).get("published_utc") or "").strip() or None,
                    path_used="direct_live",
                    live_or_cache="live",
                    usable_truth=True,
                    semantic_grade="headline_timestamp_present",
                    freshness_class="current",
                    sufficiency_state="headline_timestamp_present",
                    data_mode="live",
                    authority_level="direct",
                ),
            }
            for item in items
        ]
        record_runtime_truth(
            source_id=_SOURCE_ID,
            source_family="news",
            field_name="headline",
            symbol_or_entity=f"limit:{normalized_limit}",
            provider_used="finnhub",
            path_used="direct_live",
            live_or_cache="live",
            usable_truth=True,
            freshness="current",
            insufficiency_reason=None,
            semantic_grade="headline_timestamp_present",
            investor_surface=surface_name,
            attempt_succeeded=True,
        )
        return _remember(traced)

    items = _fetch_gdelt(normalized_limit)
    if items:
        traced = [
            {
                **dict(item),
                "provider_execution": build_provider_execution(
                    provider_name="gdelt",
                    source_family="news",
                    identifier=f"limit:{normalized_limit}",
                    observed_at=str(dict(item).get("published_utc") or "").strip() or None,
                    path_used="public_fallback",
                    live_or_cache="live",
                    usable_truth=True,
                    semantic_grade="fallback_live",
                    freshness_class="current",
                    sufficiency_state="headline_timestamp_present",
                    data_mode="live",
                    authority_level="derived",
                    provenance_strength="derived_or_proxy",
                    insufficiency_reason="finnhub_unavailable",
                ),
            }
            for item in items
        ]
        record_runtime_truth(
            source_id=_SOURCE_ID,
            source_family="news",
            field_name="headline",
            symbol_or_entity=f"limit:{normalized_limit}",
            provider_used="gdelt",
            path_used="public_fallback",
            live_or_cache="live",
            usable_truth=True,
            freshness="current",
            insufficiency_reason="finnhub_unavailable",
            semantic_grade="fallback_live",
            investor_surface=surface_name,
            attempt_succeeded=True,
        )
        return _remember(traced)

    _remember([])
    record_runtime_truth(
        source_id=_SOURCE_ID,
        source_family="news",
        field_name="headline",
        symbol_or_entity=f"limit:{normalized_limit}",
        provider_used=None,
        path_used="fallback",
        live_or_cache="fallback",
        usable_truth=False,
        freshness="unavailable",
        insufficiency_reason="news_unavailable",
        semantic_grade="unavailable",
        investor_surface=surface_name,
        attempt_succeeded=False,
    )
    return list(unavailable_payload()["items"])


def freshness_state() -> FreshnessState:
    last_fetched = coerce_datetime(_LAST_FETCHED_AT)
    last_published = coerce_datetime(_LAST_PUBLISHED_AT)
    anchor = last_fetched or last_published
    if anchor is None:
        return FreshnessState(
            source_id=_SOURCE_ID,
            freshness_class=FreshnessClass.EXECUTION_FAILED_OR_INCOMPLETE,
            last_updated_utc=None,
            staleness_seconds=None,
        )

    age_seconds = max(0, int((_now() - anchor).total_seconds()))
    if age_seconds <= 60 * 60:
        freshness_class = FreshnessClass.FRESH_FULL_REBUILD
    elif age_seconds <= 6 * 60 * 60:
        freshness_class = FreshnessClass.FRESH_PARTIAL_REBUILD
    elif age_seconds <= 24 * 60 * 60:
        freshness_class = FreshnessClass.STORED_VALID_CONTEXT
    else:
        freshness_class = FreshnessClass.DEGRADED_MONITORING_MODE
    return FreshnessState(
        source_id=_SOURCE_ID,
        freshness_class=freshness_class,
        last_updated_utc=anchor.astimezone(UTC).isoformat(),
        staleness_seconds=age_seconds,
    )


class NewsAdapter:
    source_id = _SOURCE_ID
    tier = source_tier

    def fetch(self, limit: int = 10, *, surface_name: str | None = None) -> list[dict[str, Any]]:
        return fetch(limit=limit, surface_name=surface_name)

    def fallback(self) -> list[dict[str, Any]]:
        return fallback()

    def freshness_state(self) -> FreshnessState:
        return freshness_state()


register_source(_SOURCE_ID, adapter=__import__(__name__, fromlist=["fetch"]))
