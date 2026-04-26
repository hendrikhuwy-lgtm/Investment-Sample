from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from app.v2.sources.registry import get_source_definition
from app.v2.sources.source_policy import source_policy_map


_REGISTERED_SOURCE_KEYS = (
    "market_price",
    "benchmark_truth",
    "macro",
    "news",
    "issuer_factsheet",
    "blueprint_candidate_registry",
    "blueprint_candidate_truth",
    "blueprint_benchmark_assignment",
    "etf_document_verification",
    "etf_market_state",
    "provider_surface_context",
    "portfolio_holdings",
    "portfolio_snapshot",
)


def _serialize_freshness_policy(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return dict(model_dump(mode="json"))
    dict_method = getattr(value, "dict", None)
    if callable(dict_method):
        return dict(dict_method())
    if is_dataclass(value):
        return dict(asdict(value))
    if isinstance(value, dict):
        return dict(value)
    raise TypeError(f"Unsupported freshness policy type: {type(value)!r}")


def truth_family_registry() -> dict[str, Any]:
    sources: dict[str, dict[str, Any]] = {}
    for key in _REGISTERED_SOURCE_KEYS:
        definition = get_source_definition(key)
        sources[key] = {
            "key": definition.key,
            "name": definition.name,
            "tier": definition.tier,
            "surface": definition.surface,
            "donor": definition.donor,
            "connector_kind": definition.connector_kind,
            "authoritative_fields": list(definition.authoritative_fields),
            "freshness_policy": _serialize_freshness_policy(definition.freshness_policy),
        }
    return {
        "sources": sources,
        "source_policy": source_policy_map(),
    }
