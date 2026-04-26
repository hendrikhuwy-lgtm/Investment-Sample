from __future__ import annotations

import hashlib
from typing import Any

from app.v2.core.domain_objects import EvidenceCitation, EvidencePack, MarketSeriesTruth
from app.v2.features.daily_brief_event_cluster import build_event_cluster
from app.v2.sources.freshness_registry import get_freshness


def _freshness_payload(source_id: str) -> dict[str, Any]:
    freshness = get_freshness(source_id)
    return {
        "source_id": freshness.source_id,
        "freshness_class": freshness.freshness_class.value,
        "last_updated_utc": freshness.last_updated_utc,
        "staleness_seconds": freshness.staleness_seconds,
    }


def _series_id(item: dict[str, Any]) -> str:
    source = str(item.get("source") or "unknown").strip().lower().replace(" ", "_")
    basis = "|".join(
        [
            str(item.get("source") or ""),
            str(item.get("headline") or ""),
            str(item.get("published_utc") or ""),
            str(item.get("url") or ""),
        ]
    )
    digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:12]
    return f"news:{source or 'unknown'}:{digest}"


def translate(raw_items: Any) -> list[MarketSeriesTruth]:
    items = list(raw_items or [])
    if not items:
        return []

    freshness_state = _freshness_payload("news")
    source_id = "news_adapter"
    translated: list[MarketSeriesTruth] = []

    for raw in items:
        item = dict(raw or {})
        headline = str(item.get("headline") or "").strip()
        source = str(item.get("source") or "").strip() or "unknown"
        published_utc = str(item.get("published_utc") or freshness_state.get("last_updated_utc") or "").strip()
        provider_execution = dict(item.get("provider_execution") or {})
        event_source_class = _event_source_class(headline)
        event_metadata = build_event_cluster(
            label=headline,
            source_class=event_source_class,
            bucket="policy" if event_source_class == "policy_event" else "market",
            seed_metadata=item,
        )
        evidence = EvidencePack(
            evidence_id=f"evidence_{_series_id(item).replace(':', '_')}",
            thesis=headline or f"{source} news event",
            summary="Translated news source payload for V2 market-series truth.",
            freshness=str(freshness_state.get("freshness_class") or "unknown"),
            citations=[
                EvidenceCitation(
                    source_id=source_id,
                    label=source,
                    url=str(item.get("url") or "").strip() or None,
                )
            ],
            facts={
                "ticker": source,
                "current_value": None,
                "one_day_change_pct": None,
                "regime_label": headline or None,
                "freshness_state": freshness_state,
                "source_id": source_id,
                "provider_execution": provider_execution or None,
                "usable_truth": provider_execution.get("usable_truth"),
                "sufficiency_state": provider_execution.get("sufficiency_state"),
                "data_mode": provider_execution.get("data_mode"),
                "authority_level": provider_execution.get("authority_level"),
                **event_metadata,
            },
            observed_at=published_utc,
        )
        translated.append(
            MarketSeriesTruth(
                series_id=_series_id(item),
                label=headline or f"{source} news",
                frequency="event",
                units="headline",
                points=[],
                evidence=[evidence],
                as_of=published_utc,
            )
        )

    return translated


def _event_source_class(headline: str) -> str:
    lowered = str(headline or "").lower()
    if any(term in lowered for term in {"fed", "ecb", "boj", "pboc", "tariff", "election", "government", "fiscal", "policy", "central bank", "rate decision"}):
        return "policy_event"
    return "geopolitical_news"
