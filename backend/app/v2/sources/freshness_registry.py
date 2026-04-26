from __future__ import annotations

from typing import Any

from app.v2.donors.source_freshness import FreshnessClass, FreshnessState


def _unknown_freshness(source_id: str) -> FreshnessState:
    return FreshnessState(
        source_id=source_id,
        freshness_class=FreshnessClass.EXECUTION_FAILED_OR_INCOMPLETE,
        last_updated_utc=None,
        staleness_seconds=None,
    )


class FreshnessRegistry:
    def __init__(self) -> None:
        self._source_adapters: dict[str, Any] = {}

    def register_source(self, source_id: str, adapter: Any) -> None:
        self._source_adapters[str(source_id)] = adapter

    def get_freshness(self, source_id: str) -> FreshnessState:
        adapter = self._source_adapters.get(str(source_id))
        if adapter is None:
            return _unknown_freshness(str(source_id))
        try:
            freshness = adapter.freshness_state()
        except Exception:
            return _unknown_freshness(str(source_id))
        if isinstance(freshness, FreshnessState):
            return freshness
        return _unknown_freshness(str(source_id))

    def get_all_freshness(self) -> dict[str, FreshnessState]:
        return {
            source_id: self.get_freshness(source_id)
            for source_id in sorted(self._source_adapters)
        }


_DEFAULT_REGISTRY = FreshnessRegistry()


def register_source(source_id: str, adapter) -> None:
    _DEFAULT_REGISTRY.register_source(source_id, adapter)


def get_freshness(source_id: str) -> FreshnessState:
    return _DEFAULT_REGISTRY.get_freshness(source_id)


def get_all_freshness() -> dict[str, FreshnessState]:
    return _DEFAULT_REGISTRY.get_all_freshness()
