from __future__ import annotations

from typing import Any

from app.v2.core.domain_objects import EvidenceCitation, EvidencePack, utc_now_iso


def _slug(value: str) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace(".", "_").replace("-", "_") or "unknown"


def _citation_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    rows = list(raw.get("citation_rows") or [])
    if rows:
        return [dict(row or {}) for row in rows if isinstance(row, dict)]

    citations = list(raw.get("citations") or [])
    result: list[dict[str, Any]] = []
    for row in citations:
        item = dict(row or {})
        result.append(
            {
                "source_id": str(item.get("source_id") or item.get("label") or "source"),
                "label": str(item.get("label") or item.get("source_id") or "Source"),
                "url": item.get("url"),
                "observed_at": item.get("observed_at") or raw.get("observed_at"),
                "confidence_label": str(item.get("confidence_label") or "medium"),
                "source_type": str(item.get("source_type") or "unclassified"),
            }
        )
    return result


def translate(citation_health_data: Any) -> EvidencePack:
    """Translate citation-health primitives or donor rows into an EvidencePack."""
    raw = dict(citation_health_data or {})
    citation_rows = _citation_rows(raw)
    citations = [
        EvidenceCitation(
            source_id=str(row.get("source_id") or _slug(str(row.get("label") or "source"))),
            label=str(row.get("label") or row.get("source_id") or "Source"),
            url=str(row.get("url") or "").strip() or None,
            note=str(row.get("source_type") or "").strip() or None,
        )
        for row in citation_rows
    ]
    field_truths = list(raw.get("field_truths") or [])
    completeness_score = raw.get("completeness_score")
    if completeness_score is None and field_truths:
        populated = sum(1 for row in field_truths if str(dict(row or {}).get("missingness_reason") or "populated") == "populated")
        completeness_score = round(populated / len(field_truths), 3)
    if completeness_score is None:
        completeness_score = 0.0

    summary = str(raw.get("summary") or "").strip()
    if not summary:
        if citations:
            summary = (
                f"{len(citations)} source citation{'s' if len(citations) != 1 else ''} translated into "
                "an evidence pack."
            )
        else:
            summary = "No translated citation rows are available for this evidence pack."

    evidence_id = str(raw.get("evidence_id") or f"evidence_pack_{_slug(str(raw.get('candidate_symbol') or 'candidate'))}")
    observed_at = str(raw.get("observed_at") or utc_now_iso())
    freshness = str(raw.get("freshness") or raw.get("freshness_state") or "stored_valid_context")
    confidence = str(raw.get("confidence") or ("high" if completeness_score >= 0.75 else "medium" if completeness_score >= 0.35 else "low"))
    if confidence not in {"high", "medium", "low"}:
        confidence = "medium"

    return EvidencePack(
        evidence_id=evidence_id,
        thesis=str(raw.get("thesis") or raw.get("candidate_symbol") or "Evidence coverage"),
        summary=summary,
        freshness=freshness,
        confidence=confidence,
        citations=citations,
        facts={
            **{key: value for key, value in raw.items() if key not in {"citations"}},
            "citation_rows": citation_rows,
            "field_truths": field_truths,
            "completeness_score": float(completeness_score or 0.0),
        },
        observed_at=observed_at,
    )
