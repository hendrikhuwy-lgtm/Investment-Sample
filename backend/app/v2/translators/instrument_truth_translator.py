from __future__ import annotations

from typing import TYPE_CHECKING, Any

from app.v2.core.domain_objects import EvidenceCitation, EvidencePack, InstrumentTruth
from app.v2.sources.freshness_registry import get_freshness
from app.v2.truth.envelopes import build_truth_envelope

if TYPE_CHECKING:
    pass


def _normalize_vehicle_type(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return raw.lower().replace(" ", "_")


def _instrument_id(symbol: str) -> str:
    normalized = str(symbol or "").strip().lower().replace(".", "_").replace("-", "_")
    return f"instrument_{normalized or 'unknown'}"


def translate(candidate_truth_donor: Any) -> "InstrumentTruth":
    """Translates candidate_truth primitives from blueprint_candidate_truth → InstrumentTruth.
    Never passes raw donor object through to product contracts."""
    raw = dict(candidate_truth_donor or {})
    freshness = get_freshness("issuer_factsheet")
    source_id = "issuer_factsheet_adapter"
    symbol = str(raw.get("ticker") or raw.get("symbol") or "").strip()
    primary_documents = [dict(item) for item in list(raw.get("primary_documents") or []) if isinstance(item, dict)]
    citations = [
        EvidenceCitation(
            source_id=source_id,
            label="Issuer factsheet adapter",
            url=str(dict(raw.get("docs") or {}).get("factsheet_pdf_url") or "").strip() or None,
        )
    ]
    for document in primary_documents:
        doc_type = str(document.get("doc_type") or "document").strip() or "document"
        doc_url = str(document.get("doc_url") or "").strip() or None
        if not doc_url:
            continue
        citations.append(
            EvidenceCitation(
                source_id=f"{source_id}_{doc_type}",
                label=f"Issuer {doc_type}",
                url=doc_url,
                note=f"primary_document:{doc_type}",
            )
        )
    evidence = EvidencePack(
        evidence_id=f"evidence_{symbol.lower() or 'instrument'}_factsheet",
        thesis=str(raw.get("name") or symbol or "Instrument factsheet record"),
        summary="Translated issuer factsheet record for V2 instrument truth.",
        freshness=freshness.freshness_class.value,
        citations=citations,
        facts={
            "ticker": raw.get("ticker") or raw.get("symbol"),
            "issuer": raw.get("issuer"),
            "ter": raw.get("ter"),
            "aum_usd": raw.get("aum_usd"),
            "inception_date": raw.get("inception_date"),
            "verification_missing": list(raw.get("verification_missing") or []),
            "primary_documents": primary_documents,
            "freshness_state": {
                "source_id": freshness.source_id,
                "freshness_class": freshness.freshness_class.value,
                "last_updated_utc": freshness.last_updated_utc,
                "staleness_seconds": freshness.staleness_seconds,
            },
            "source_id": source_id,
            "truth_envelope": build_truth_envelope(
                as_of_utc=str(raw.get("factsheet_date") or freshness.last_updated_utc or ""),
                reference_period=str(raw.get("factsheet_date") or "")[:10] or None,
                source_authority="issuer_factsheet",
                acquisition_mode="donor",
                degradation_reason=None,
                recommendation_critical=True,
                retrieved_at_utc=freshness.last_updated_utc,
            ),
        },
        observed_at=str(raw.get("factsheet_date") or freshness.last_updated_utc or ""),
    )
    return InstrumentTruth(
        instrument_id=_instrument_id(symbol),
        symbol=symbol,
        name=str(raw.get("name") or ""),
        asset_class=str(raw.get("asset_class") or "unknown"),
        vehicle_type=_normalize_vehicle_type(raw.get("vehicle_type")),
        benchmark_id=str(raw.get("benchmark_id") or "").strip() or None,
        domicile=str(raw.get("domicile") or "").strip() or None,
        base_currency=str(raw.get("base_currency") or "").strip() or None,
        metrics={
            "issuer": raw.get("issuer"),
            "ter": raw.get("ter"),
            "aum_usd": raw.get("aum_usd"),
            "inception_date": raw.get("inception_date"),
            "factsheet_date": raw.get("factsheet_date"),
            "verification_missing": list(raw.get("verification_missing") or []),
            "primary_documents": primary_documents,
            "freshness_state": freshness.freshness_class.value,
            "freshness_last_updated_utc": freshness.last_updated_utc,
            "source_id": source_id,
        },
        evidence=[evidence],
        as_of=str(raw.get("factsheet_date") or freshness.last_updated_utc or ""),
    )
