from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING, Any

from app.config import get_db_path
from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, export_live_candidate_registry, seed_default_candidate_registry
from app.v2.core.domain_objects import EvidenceCitation, utc_now_iso
from app.v2.donors.blueprint import SQLiteBlueprintDonor
from app.v2.sources.freshness_registry import get_freshness

if TYPE_CHECKING:
    from app.v2.core.domain_objects import EvidencePack


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _normalize_identifier(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("candidate_instrument_"):
        return raw.removeprefix("candidate_instrument_").upper()
    if raw.startswith("candidate_"):
        return raw.removeprefix("candidate_").replace("instrument_", "").upper()
    if raw.startswith("instrument_"):
        return raw.removeprefix("instrument_").upper()
    return raw.upper()


def _pick_candidate(candidates: list[dict[str, Any]], identifier: str) -> dict[str, Any] | None:
    normalized = _normalize_identifier(identifier)
    if not normalized:
        return None

    matches = [
        candidate
        for candidate in candidates
        if str(candidate.get("symbol") or "").strip().upper() == normalized
        or str(candidate.get("registry_id") or "").strip() == identifier
    ]
    if not matches:
        return None

    preferred = next(
        (
            candidate
            for candidate in matches
            if str(candidate.get("symbol") or "").strip().upper() == normalized
            and str(candidate.get("sleeve_key") or "").strip()
        ),
        None,
    )
    return preferred or matches[0]


def _slug(value: str) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace(".", "_").replace("-", "_") or "unknown"


def _completeness_score(snapshot: dict[str, Any] | None) -> float:
    if not snapshot:
        return 0.0
    required_total = int(snapshot.get("required_fields_total") or 0)
    populated = int(snapshot.get("required_fields_populated") or 0)
    if required_total <= 0:
        return 0.0
    return round(populated / required_total, 3)


def _confidence_label(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.35:
        return "medium"
    return "low"


def _freshness_state(field_truth: dict[str, dict[str, Any]]) -> str:
    if any(str(item.get("value_type") or "") == "stale" for item in field_truth.values()):
        return "stored_valid_context"
    if any(str(item.get("missingness_reason") or "") != "populated" for item in field_truth.values()):
        return "fresh_partial_rebuild"
    if field_truth:
        return "fresh_full_rebuild"
    return get_freshness("issuer_factsheet").freshness_class.value


def _citation_rows(field_truth: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None]] = set()
    for field_name, item in sorted(field_truth.items()):
        source_name = str(item.get("source_name") or "").strip()
        source_url = str(item.get("source_url") or "").strip() or None
        if not source_name and source_url is None:
            continue

        source_id = _slug(source_name or field_name)
        key = (source_id, source_url)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "source_id": source_id,
                "label": source_name or field_name.replace("_", " ").title(),
                "url": source_url,
                "field_name": field_name,
                "observed_at": item.get("observed_at"),
                "confidence_label": str(item.get("confidence_label") or "").strip().lower() or "medium",
                "source_type": str(item.get("source_type") or "").strip().lower() or "unclassified",
            }
        )
    return rows


def build_evidence_pack(candidate_id: str) -> "EvidencePack":
    """Returns EvidencePack. Wraps citation health primitives donor."""
    from app.v2.core.domain_objects import EvidencePack

    normalized = _normalize_identifier(candidate_id)
    fallback_freshness = get_freshness("issuer_factsheet")
    empty_pack = EvidencePack(
        evidence_id=f"evidence_pack_{_slug(normalized)}",
        thesis=f"Evidence coverage for {normalized or 'unknown candidate'}",
        summary="No candidate-truth evidence rows are available for this candidate.",
        freshness=fallback_freshness.freshness_class.value,
        confidence="low",
        citations=[],
        facts={
            "candidate_symbol": normalized or None,
            "field_truths": [],
            "citation_rows": [],
            "completeness_score": 0.0,
        },
        observed_at=fallback_freshness.last_updated_utc or utc_now_iso(),
    )

    if not normalized:
        return empty_pack

    with _connection() as conn:
        ensure_candidate_registry_tables(conn)
        donor = SQLiteBlueprintDonor(conn)
        candidates = donor.list_candidates()
        if not candidates:
            seed_default_candidate_registry(conn)
            candidates = export_live_candidate_registry(conn)

        candidate = _pick_candidate(candidates, candidate_id)
        if candidate is None:
            return empty_pack

        sleeve_key = str(candidate.get("sleeve_key") or "").strip()
        if not sleeve_key:
            return empty_pack

        field_truth = donor.resolve_field_truth(candidate_symbol=normalized, sleeve_key=sleeve_key)
        completeness = donor.compute_candidate_completeness(candidate=candidate)

    completeness_score = _completeness_score(completeness)
    citation_rows = _citation_rows(field_truth)
    citations = [
        EvidenceCitation(
            source_id=row["source_id"],
            label=row["label"],
            url=row["url"],
            note=f"{row['field_name']}:{row['source_type']}",
        )
        for row in citation_rows
    ]
    field_truth_rows = [
        {
            "field_name": field_name,
            "resolved_value": item.get("resolved_value"),
            "value_type": item.get("value_type"),
            "source_name": item.get("source_name"),
            "source_url": item.get("source_url"),
            "observed_at": item.get("observed_at"),
            "confidence_label": item.get("confidence_label"),
            "completeness_state": item.get("completeness_state"),
            "source_type": item.get("source_type"),
            "missingness_reason": item.get("missingness_reason"),
        }
        for field_name, item in sorted(field_truth.items())
    ]

    if not citation_rows:
        return empty_pack.model_copy(
            update={
                "facts": {
                    **empty_pack.facts,
                    "candidate_symbol": normalized,
                    "sleeve_key": sleeve_key,
                    "completeness": completeness,
                    "completeness_score": completeness_score,
                }
            }
        )

    summary = (
        f"{len(citation_rows)} source citations support {normalized}. "
        f"Completeness score is {completeness_score:.2f} across {int(completeness.get('required_fields_total') or 0)} required fields."
    )
    return EvidencePack(
        evidence_id=f"evidence_pack_{_slug(normalized)}",
        thesis=f"Evidence coverage for {normalized}",
        summary=summary,
        freshness=_freshness_state(field_truth),
        confidence=_confidence_label(completeness_score),
        citations=citations,
        facts={
            "candidate_symbol": normalized,
            "sleeve_key": sleeve_key,
            "field_truths": field_truth_rows,
            "citation_rows": citation_rows,
            "completeness": completeness,
            "completeness_score": completeness_score,
        },
        observed_at=str(completeness.get("computed_at") or fallback_freshness.last_updated_utc or utc_now_iso()),
    )
