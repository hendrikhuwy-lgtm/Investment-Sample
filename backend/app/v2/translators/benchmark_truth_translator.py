from __future__ import annotations

from typing import TYPE_CHECKING, Any

from app.v2.core.domain_objects import BenchmarkTruth, EvidenceCitation, EvidencePack
from app.v2.sources.freshness_registry import get_freshness

if TYPE_CHECKING:
    pass


def translate(benchmark_donor: Any) -> "BenchmarkTruth":
    """Translates benchmark mapping from blueprint_benchmark_registry → BenchmarkTruth."""
    raw = dict(benchmark_donor or {})
    freshness = get_freshness("benchmark_truth")
    source_id = "benchmark_truth_adapter"
    benchmark_id = str(raw.get("benchmark_id") or "").strip()
    name = str(raw.get("name") or benchmark_id.replace("_", " ").title() or "")
    evidence = EvidencePack(
        evidence_id=f"evidence_{benchmark_id.lower() or 'benchmark'}_registry",
        thesis=name or "Benchmark registry record",
        summary="Translated benchmark registry record for V2 benchmark truth.",
        freshness=freshness.freshness_class.value,
        citations=[
            EvidenceCitation(
                source_id=source_id,
                label="Benchmark truth adapter",
                note=str(raw.get("benchmark_source_type") or "registry"),
            )
        ],
        facts={
            "current_value": raw.get("current_value"),
            "ytd_return_pct": raw.get("ytd_return_pct"),
            "one_year_return_pct": raw.get("one_year_return_pct"),
            "proxy_symbol": raw.get("proxy_symbol"),
            "field_provenance": dict(raw.get("field_provenance") or {}),
            "freshness_state": {
                "source_id": freshness.source_id,
                "freshness_class": freshness.freshness_class.value,
                "last_updated_utc": freshness.last_updated_utc,
                "staleness_seconds": freshness.staleness_seconds,
            },
            "source_id": source_id,
        },
        observed_at=str(raw.get("updated_at") or freshness.last_updated_utc or ""),
    )
    return BenchmarkTruth(
        benchmark_id=benchmark_id,
        name=name,
        methodology=str(raw.get("benchmark_source_type") or "registry_proxy") or None,
        benchmark_authority_level="bounded",
        mapped_instruments=[str(item) for item in list(raw.get("mapped_tickers") or []) if str(item).strip()],
        evidence=[evidence],
        as_of=str(raw.get("updated_at") or freshness.last_updated_utc or ""),
    )
