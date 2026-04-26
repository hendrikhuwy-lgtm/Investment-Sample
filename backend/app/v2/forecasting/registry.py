from __future__ import annotations

from typing import Any

from app.v2.forecasting.adapters import chronos_adapter, lagllama_adapter, moirai_adapter, timesfm_adapter
from app.v2.forecasting.capabilities import ForecastCapability, ForecastRequest


_PROVIDERS = {
    "timesfm": {
        "adapter": timesfm_adapter,
        "tier": "tier_a",
        "managed": False,
        "benchmark_only": False,
    },
    "chronos": {
        "adapter": chronos_adapter,
        "tier": "tier_a",
        "managed": False,
        "benchmark_only": False,
    },
    "moirai": {
        "adapter": moirai_adapter,
        "tier": "tier_b",
        "managed": False,
        "benchmark_only": True,
    },
    "lagllama": {
        "adapter": lagllama_adapter,
        "tier": "tier_b",
        "managed": False,
        "benchmark_only": True,
    },
}


def capability_matrix() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for provider, meta in _PROVIDERS.items():
        adapter = meta["adapter"]
        capability = ForecastCapability(
            provider=provider,
            model_name=adapter.model_name(),
            tier=str(meta["tier"]),
            managed=bool(meta["managed"]),
            benchmark_only=bool(meta["benchmark_only"]),
            configured=bool(adapter.configured()),
            reason_code=None if adapter.configured() else "provider_unavailable",
            endpoint=getattr(adapter, "PROVIDER", provider),
            base_url=getattr(adapter, "base_url", lambda: None)(),
        )
        rows.append(capability.to_dict())
    rows.append(
        ForecastCapability(
            provider="deterministic_baseline",
            model_name="rule_based_support",
            tier="fallback",
            managed=False,
            benchmark_only=False,
            configured=True,
            ready=True,
            endpoint="internal",
            base_url=None,
        ).to_dict()
    )
    return rows


def configured_providers(*, include_benchmarks: bool = False) -> list[str]:
    rows: list[str] = []
    for provider, meta in _PROVIDERS.items():
        if not include_benchmarks and bool(meta["benchmark_only"]):
            continue
        if meta["adapter"].configured():
            rows.append(provider)
    return rows


def benchmark_providers() -> list[str]:
    return [provider for provider, meta in _PROVIDERS.items() if bool(meta["benchmark_only"]) and meta["adapter"].configured()]


def all_providers(*, include_benchmarks: bool = True) -> list[str]:
    return [
        provider
        for provider, meta in _PROVIDERS.items()
        if include_benchmarks or not bool(meta["benchmark_only"])
    ]


def provider_meta(provider: str) -> dict[str, Any]:
    return dict(_PROVIDERS.get(str(provider)) or {})


def provider_sequence_for_request(request: ForecastRequest, *, surface_name: str) -> list[str]:
    family = str(request.series_family or "").strip().lower()
    if surface_name == "daily_brief":
        return ["chronos", "timesfm"]
    if surface_name in {"candidate_report", "blueprint_explorer", "compare"}:
        return ["chronos", "timesfm"]
    if surface_name == "portfolio":
        return ["chronos", "timesfm"]
    return ["chronos", "timesfm"]


def adapter_for(provider: str):
    return (_PROVIDERS.get(str(provider)) or {}).get("adapter")


def provider_available(provider: str) -> bool:
    if provider == "deterministic_baseline":
        return True
    adapter = adapter_for(provider)
    return bool(adapter and adapter.configured())
