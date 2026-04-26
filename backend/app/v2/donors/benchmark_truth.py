from __future__ import annotations

from typing import TYPE_CHECKING

from app.v2.sources.benchmark_truth_adapter import fetch
from app.v2.translators.benchmark_truth_translator import translate

if TYPE_CHECKING:
    from app.v2.core.domain_objects import BenchmarkTruth


def get_benchmark_truth(
    benchmark_id: str,
    *,
    surface_name: str | None = None,
    allow_live_fetch: bool = True,
) -> "BenchmarkTruth":
    """Returns BenchmarkTruth. Wraps blueprint_benchmark_registry donor."""
    return translate(fetch(benchmark_id, surface_name=surface_name, allow_live_fetch=allow_live_fetch))
