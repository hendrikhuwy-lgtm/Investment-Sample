from __future__ import annotations

from typing import Any

from app.v2.core.domain_objects import EvidenceCitation, EvidencePack, MacroTruth
from app.v2.sources.freshness_registry import get_freshness
from app.v2.truth.envelopes import build_macro_truth_envelope


def _freshness_payload(source_id: str) -> dict[str, Any]:
    freshness = get_freshness(source_id)
    return {
        "source_id": freshness.source_id,
        "freshness_class": freshness.freshness_class.value,
        "last_updated_utc": freshness.last_updated_utc,
        "staleness_seconds": freshness.staleness_seconds,
    }


def translate(raw_signal: Any) -> MacroTruth:
    raw = dict(raw_signal or {})
    source_lookup_id = "macro"
    source_id = "macro_adapter"
    series_id = str(raw.get("series_id") or "unknown").strip() or "unknown"
    name = str(raw.get("name") or series_id.replace("_", " ").title()).strip()
    freshness_state = dict(raw.get("freshness_state") or {}) or _freshness_payload(source_lookup_id)
    observed_at = str(raw.get("date") or freshness_state.get("last_updated_utc") or "").strip()
    provider_execution = dict(raw.get("provider_execution") or {})
    truth_envelope = dict(raw.get("truth_envelope") or {}) or build_macro_truth_envelope(
        series_id=series_id,
        observation_date=observed_at or None,
        source_authority=str(raw.get("source_ref") or "").strip() or "macro",
        acquisition_mode="live" if str(raw.get("source_ref") or "").startswith("fred_api:") else "cached",
        retrieved_at_utc=str(raw.get("retrieved_utc") or freshness_state.get("last_updated_utc") or "").strip() or None,
        release_date=str(raw.get("release_date") or raw.get("realtime_start") or "").strip() or None,
        availability_date=str(raw.get("availability_date") or raw.get("realtime_start") or "").strip() or None,
        realtime_start=str(raw.get("realtime_start") or "").strip() or None,
        realtime_end=str(raw.get("realtime_end") or "").strip() or None,
        vintage_class=str(raw.get("vintage_class") or "").strip() or None,
        revision_state=str(raw.get("revision_state") or "").strip() or None,
        release_semantics_state=str(raw.get("release_semantics_state") or "").strip() or None,
        degradation_reason=str(raw.get("error") or "").strip() or None,
    )

    evidence = EvidencePack(
        evidence_id=f"evidence_macro_{series_id.lower()}",
        thesis=name or "Macro indicator",
        summary="Translated macro source payload for V2 macro truth.",
        freshness=str(freshness_state.get("freshness_class") or "unknown"),
        citations=[
            EvidenceCitation(
                source_id=source_id,
                label="Macro adapter",
                note=str(raw.get("source_ref") or "").strip() or None,
            )
        ],
        facts={
            "indicator_id": series_id,
            "name": name or None,
            "current_value": raw.get("value"),
            "previous_value": raw.get("previous_value"),
            "unit": raw.get("unit"),
            "regime_signal": None,
            "freshness_state": freshness_state,
            "source_id": source_id,
            "error": raw.get("error"),
            "reference_period": raw.get("reference_period"),
            "observation_date": raw.get("date"),
            "release_date": raw.get("release_date") or raw.get("realtime_start"),
            "availability_date": raw.get("availability_date") or raw.get("realtime_start"),
            "retrieved_utc": raw.get("retrieved_utc"),
            "realtime_start": raw.get("realtime_start"),
            "realtime_end": raw.get("realtime_end"),
            "vintage_class": raw.get("vintage_class"),
            "revision_state": raw.get("revision_state"),
            "release_semantics_state": raw.get("release_semantics_state"),
            "period_clock_class": raw.get("period_clock_class"),
            "provider_execution": provider_execution or None,
            "usable_truth": provider_execution.get("usable_truth"),
            "sufficiency_state": provider_execution.get("sufficiency_state"),
            "data_mode": provider_execution.get("data_mode"),
            "authority_level": provider_execution.get("authority_level"),
            "truth_envelope": truth_envelope,
        },
        observed_at=observed_at,
    )

    return MacroTruth(
        macro_id=f"macro:{series_id.lower()}",
        regime="unclassified",
        summary=f"{name or 'Macro indicator'} latest observation.",
        indicators={
            "indicator_id": series_id,
            "name": name or None,
            "current_value": raw.get("value"),
            "previous_value": raw.get("previous_value"),
            "unit": raw.get("unit"),
            "regime_signal": None,
            "freshness_state": freshness_state,
            "source_id": source_id,
            "reference_period": raw.get("reference_period"),
            "observation_date": raw.get("date"),
            "release_date": raw.get("release_date") or raw.get("realtime_start"),
            "availability_date": raw.get("availability_date") or raw.get("realtime_start"),
            "retrieved_utc": raw.get("retrieved_utc"),
            "realtime_start": raw.get("realtime_start"),
            "realtime_end": raw.get("realtime_end"),
            "vintage_class": raw.get("vintage_class"),
            "revision_state": raw.get("revision_state"),
            "release_semantics_state": raw.get("release_semantics_state"),
            "period_clock_class": raw.get("period_clock_class"),
            "provider_execution": provider_execution or None,
            "usable_truth": provider_execution.get("usable_truth"),
            "sufficiency_state": provider_execution.get("sufficiency_state"),
            "data_mode": provider_execution.get("data_mode"),
            "authority_level": provider_execution.get("authority_level"),
            "truth_envelope": truth_envelope,
        },
        evidence=[evidence],
        as_of=observed_at,
    )
