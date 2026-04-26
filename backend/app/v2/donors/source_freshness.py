from dataclasses import dataclass
from enum import Enum


class FreshnessClass(Enum):
    FRESH_FULL_REBUILD = "fresh_full_rebuild"
    FRESH_PARTIAL_REBUILD = "fresh_partial_rebuild"
    STORED_VALID_CONTEXT = "stored_valid_context"
    DEGRADED_MONITORING_MODE = "degraded_monitoring_mode"
    EXECUTION_FAILED_OR_INCOMPLETE = "execution_failed_or_incomplete"


@dataclass
class FreshnessState:
    source_id: str
    freshness_class: FreshnessClass
    last_updated_utc: str | None
    staleness_seconds: int | None


def get_freshness_state(source_id: str) -> FreshnessState:
    from app.v2.sources.freshness_registry import get_freshness

    return get_freshness(source_id)
