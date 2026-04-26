from __future__ import annotations

from typing import Any

from app.v2.core.change_ledger import record_change
from app.v2.core.holdings_overlay import apply_overlay
from app.v2.core.domain_objects import EvidencePack, InstrumentTruth, utc_now_iso
from app.v2.donors.evidence_pack import build_evidence_pack
from app.v2.donors.instrument_truth import get_instrument_truth
from app.v2.features.research_support import build_research_support_pack
from app.v2.forecasting.store import list_latest_runs, list_notebook_forecast_references
from app.v2.storage.notebook_store import ensure_seed_entry, list_entries, list_history
from app.v2.surfaces.common import degraded_section, empty_section, ready_section, surface_state


_CONTRACT_VERSION = "0.2.0"
_SURFACE_ID = "notebook"


def _candidate_contract_id(truth: InstrumentTruth) -> str:
    return f"candidate_{truth.instrument_id}"


def _slug(value: str) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace(".", "_").replace("-", "_") or "unknown"


def _title_from_field_name(field_name: str) -> str:
    return str(field_name or "").replace("_", " ").strip().title() or "Evidence"


def _format_value(value: Any) -> str:
    if value is None:
        return "not available"
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    if isinstance(value, list):
        return ", ".join(str(item) for item in value[:4]) or "not available"
    if isinstance(value, dict):
        visible = ", ".join(f"{key}={item}" for key, item in list(value.items())[:4])
        return visible or "not available"
    return str(value)


def _evidence_depth(truth: InstrumentTruth, evidence_pack: EvidencePack) -> str:
    citation_count = sum(len(pack.citations) for pack in truth.evidence) + len(evidence_pack.citations)
    completeness_score = float(dict(evidence_pack.facts or {}).get("completeness_score") or 0.0)
    if completeness_score >= 0.75 or citation_count >= 4:
        return "substantial"
    if completeness_score > 0.0 or citation_count >= 1:
        return "moderate"
    return "limited"


def _pack_sections(candidate_id: str, evidence_pack: EvidencePack) -> list[dict[str, Any]]:
    facts = dict(evidence_pack.facts or {})
    rows = list(facts.get("field_truths") or [])
    source_refs = [citation.source_id for citation in evidence_pack.citations]
    sections: list[dict[str, Any]] = []

    if evidence_pack.summary and (rows or source_refs):
        sections.append(
            {
                "section_id": f"section_{_slug(candidate_id)}_evidence_overview",
                "title": "Evidence Overview",
                "body": evidence_pack.summary,
                "source_refs": source_refs,
                "freshness_state": evidence_pack.freshness,
            }
        )

    for row in rows[:6]:
        field_name = str(row.get("field_name") or "")
        source_label = str(row.get("source_name") or "Unspecified source")
        source_url = str(row.get("source_url") or "").strip()
        refs = [ref for ref in [source_label, source_url] if ref]
        sections.append(
            {
                "section_id": f"section_{_slug(candidate_id)}_{_slug(field_name)}",
                "title": _title_from_field_name(field_name),
                "body": (
                    f"Observed value: {_format_value(row.get('resolved_value'))}. "
                    f"Source: {source_label}. "
                    f"Evidence class: {row.get('completeness_state') or 'unknown'}. "
                    f"As of: {row.get('observed_at') or 'unknown'}."
                ),
                "source_refs": refs,
                "freshness_state": "stored_valid_context"
                if str(row.get("value_type") or "") == "stale"
                else evidence_pack.freshness,
            }
        )
    return sections


def _truth_sections(candidate_id: str, truth: InstrumentTruth) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    for index, pack in enumerate(truth.evidence):
        source_refs = [citation.source_id for citation in pack.citations]
        sections.append(
            {
                "section_id": f"section_{_slug(candidate_id)}_truth_{index + 1}",
                "title": pack.thesis or f"{truth.symbol} evidence",
                "body": pack.summary,
                "source_refs": source_refs,
                "freshness_state": pack.freshness,
            }
        )
    return sections


def _investment_case(truth: InstrumentTruth, evidence_depth: str, evidence_pack: EvidencePack) -> str:
    source_count = len(evidence_pack.citations) + sum(len(pack.citations) for pack in truth.evidence)
    return (
        f"{truth.name} is under review as a {truth.asset_class} implementation candidate. "
        f"Current evidence depth is {evidence_depth}, supported by {source_count} cited source"
        f"{'' if source_count == 1 else 's'}."
    )


def _date_label(entry: dict[str, Any]) -> str:
    value = str(entry.get("updated_at") or entry.get("created_at") or "").strip()
    return value or utc_now_iso()


def _entry_payload(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "entry_id": entry["entry_id"],
        "candidate_id": entry["candidate_id"],
        "linked_object_type": entry["linked_object_type"],
        "linked_object_id": entry["linked_object_id"],
        "linked_object_label": entry["linked_object_label"],
        "status": entry["status"],
        "date_label": _date_label(entry),
        "title": entry["title"],
        "thesis": entry["thesis"],
        "assumptions": entry["assumptions"],
        "invalidation": entry["invalidation"],
        "watch_items": entry["watch_items"],
        "reflections": entry["reflections"],
        "next_review_date": entry.get("next_review_date"),
        "created_at": entry["created_at"],
        "updated_at": entry["updated_at"],
        "finalized_at": entry.get("finalized_at"),
        "archived_at": entry.get("archived_at"),
    }


def _fallback_active_draft(
    stable_candidate_id: str,
    truth: InstrumentTruth,
    evidence_pack: EvidencePack,
    evidence_sections: list[dict[str, Any]],
) -> dict[str, Any]:
    watch_items = " · ".join(section["title"] for section in evidence_sections[:4]) or "No evidence sections attached yet."
    return {
        "entry_id": f"draft_{_slug(stable_candidate_id)}",
        "candidate_id": stable_candidate_id,
        "linked_object_type": "candidate",
        "linked_object_id": stable_candidate_id,
        "linked_object_label": truth.name or truth.symbol,
        "status": "draft",
        "date_label": evidence_pack.observed_at or truth.as_of or utc_now_iso(),
        "title": truth.name or truth.symbol,
        "thesis": _investment_case(truth, _evidence_depth(truth, evidence_pack), evidence_pack),
        "assumptions": "Structured notebook assumptions are not persisted yet for this candidate.",
        "invalidation": "Use downgrade and kill conditions from the candidate report until notebook invalidation entries are captured.",
        "watch_items": watch_items,
        "reflections": (
            "This draft is derived from current evidence coverage because no persisted notebook draft exists yet."
        ),
        "next_review_date": None,
        "created_at": evidence_pack.observed_at or truth.as_of or utc_now_iso(),
        "updated_at": evidence_pack.observed_at or truth.as_of or utc_now_iso(),
        "finalized_at": None,
        "archived_at": None,
    }


def build(candidate_id: str) -> dict[str, object]:
    truth = get_instrument_truth(candidate_id)
    evidence_pack = build_evidence_pack(candidate_id)
    stable_candidate_id = _candidate_contract_id(truth)
    evidence_depth = _evidence_depth(truth, evidence_pack)
    evidence_sections = _pack_sections(stable_candidate_id, evidence_pack)
    if not evidence_sections:
        evidence_sections = _truth_sections(stable_candidate_id, truth)
    notebook_entries = list_entries(stable_candidate_id)
    notebook_history = list_history(candidate_id=stable_candidate_id, limit=24)
    forecast_refs = list_notebook_forecast_references(candidate_id=stable_candidate_id)
    refs_by_entry: dict[str, list[dict[str, Any]]] = {}
    for ref in forecast_refs:
        refs_by_entry.setdefault(str(ref.get("entry_id") or ""), []).append(
            {
                "note_forecast_ref_id": str(ref.get("note_forecast_ref_id") or ""),
                "forecast_run_id": str(ref.get("forecast_run_id") or ""),
                "reference_label": str(ref.get("reference_label") or "Forecast support"),
                "threshold_summary": ref.get("threshold_summary"),
                "created_at": str(ref.get("created_at") or ""),
            }
        )
    latest_runs = list_latest_runs(candidate_id=stable_candidate_id, limit=6)
    latest_run_refs = [
        {
            "forecast_run_id": str(item.get("forecast_run_id") or ""),
            "reference_label": f"{item.get('provider') or 'forecast'} · {item.get('model_name') or 'model'}",
            "threshold_summary": str(item.get("degraded_reason") or "Latest persisted scenario support"),
            "created_at": str(item.get("generated_at") or ""),
        }
        for item in latest_runs
    ]
    research_support = build_research_support_pack(
        truth=truth,
        target_surface="notebook",
        evidence_summary=evidence_pack.summary,
        decision_line=f"Notebook reasoning should stay aligned with {truth.name} evidence coverage and the current candidate review state.",
        drift_surface_id="candidate_report",
        drift_object_id=stable_candidate_id,
        drift_state={
            "gate_state": "review_only",
            "data_confidence": "mixed" if evidence_depth != "substantial" else "high",
            "reconciliation_status": "soft_drift" if evidence_depth == "moderate" else "verified" if evidence_depth == "substantial" else "weak_authority",
            "blocked_reason_count": 0,
            "critical_missing_count": 0,
        },
        sleeve_key=str(truth.metrics.get("sleeve_key") or truth.metrics.get("sleeve_affiliation") or ""),
    )
    active_draft = next((entry for entry in notebook_entries if entry["status"] == "draft"), None)
    memory_foundation_note = None
    if active_draft is None:
        drafting_support = dict(research_support.get("drafting_support") or {})
        market_context = dict(research_support.get("market_context") or {})
        seeded_entry, seed_created = ensure_seed_entry(
            stable_candidate_id,
            linked_object_type="candidate",
            linked_object_id=stable_candidate_id,
            linked_object_label=truth.name or truth.symbol,
            title=str(drafting_support.get("suggested_title") or truth.name or truth.symbol),
            thesis=str(drafting_support.get("summary") or _investment_case(truth, evidence_depth, evidence_pack)),
            assumptions=(
                " ".join(list(drafting_support.get("key_questions") or [])[:2]).strip()
                or "Use the current evidence map as the starting set of assumptions."
            ),
            invalidation=str((research_support.get("thesis_drift") or {}).get("watchlist_priority_delta") or "Invalidate this draft if recommendation-critical evidence weakens materially."),
            watch_items=" · ".join(list(drafting_support.get("next_steps") or [])[:3]).strip() or "No explicit next research steps were emitted yet.",
            reflections=" ".join(
                part
                for part in [
                    str(market_context.get("summary") or "").strip(),
                    str((research_support.get("sentiment_annotation") or {}).get("summary") or "").strip(),
                ]
                if part
            ).strip()
            or "Starter notebook draft seeded from current evidence and market context.",
            next_review_date=None,
        )
        if seed_created:
            memory_foundation_note = "A starter notebook draft was persisted from current evidence and report support. Edit and finalize it as research progresses."
        notebook_entries = list_entries(stable_candidate_id)
        notebook_history = list_history(candidate_id=stable_candidate_id, limit=24)
        active_draft = next((entry for entry in notebook_entries if entry["status"] == "draft"), seeded_entry)
    finalized_notes = []
    for entry in notebook_entries:
        if entry["status"] == "finalized":
            payload = _entry_payload(entry)
            payload["forecast_refs"] = refs_by_entry.get(entry["entry_id"], [])
            finalized_notes.append(payload)
    archived_notes = []
    for entry in notebook_entries:
        if entry["status"] == "archived":
            payload = _entry_payload(entry)
            payload["forecast_refs"] = refs_by_entry.get(entry["entry_id"], [])
            archived_notes.append(payload)
    active_draft_payload = _entry_payload(active_draft) if active_draft else _fallback_active_draft(
        stable_candidate_id,
        truth,
        evidence_pack,
        evidence_sections,
    )
    active_draft_payload["forecast_refs"] = refs_by_entry.get(str(active_draft_payload.get("entry_id") or ""), latest_run_refs if latest_runs else [])
    has_persisted_memory = bool(notebook_entries)

    base_contract = {
        "contract_version": _CONTRACT_VERSION,
        "surface_id": _SURFACE_ID,
        "generated_at": utc_now_iso(),
        "freshness_state": evidence_pack.freshness or next((pack.freshness for pack in truth.evidence if pack.freshness), "execution_failed_or_incomplete"),
        "surface_state": surface_state(
            "ready" if has_persisted_memory else "degraded",
            reason_codes=[] if has_persisted_memory else ["notebook_memory_not_persisted"],
            summary=(
                "Notebook is backed by persisted draft, finalized, and archive entries."
                if has_persisted_memory
                else "Notebook is preserving Cortex memory structure, but active content still falls back to derived evidence."
            ),
        ),
        "section_states": {
            "active_draft": ready_section() if active_draft else degraded_section("derived_draft", "Active draft is derived from evidence because no persisted draft exists."),
            "finalized_notes": ready_section() if finalized_notes else empty_section("no_finalized_notes", "No finalized notes have been stored yet."),
            "archive_notes": ready_section() if archived_notes else empty_section("no_archived_notes", "No archived notes have been stored yet."),
            "note_history": ready_section() if notebook_history else empty_section("no_note_history", "No notebook history has been recorded yet."),
            "evidence_sections": ready_section() if evidence_sections else degraded_section("no_evidence_sections", "No evidence sections were emitted."),
        },
        "candidate_id": stable_candidate_id,
        "name": truth.name or truth.symbol,
        "investment_case": _investment_case(truth, evidence_depth, evidence_pack),
        "evidence_sections": evidence_sections,
        "evidence_depth": evidence_depth,
        "last_updated_utc": evidence_pack.observed_at or truth.as_of or None,
        "active_draft": active_draft_payload,
        "finalized_notes": finalized_notes,
        "archived_notes": archived_notes,
        "forecast_refs": latest_run_refs,
        "memory_foundation_note": memory_foundation_note,
        "research_support": research_support,
        "note_history": [
            {
                "revision_id": str(item.get("revision_id") or ""),
                "entry_id": str(item.get("entry_id") or ""),
                "action": str(item.get("action") or ""),
                "created_at": str(item.get("created_at") or ""),
                "candidate_id": str(item.get("candidate_id") or stable_candidate_id),
                "status": str(item.get("status") or ""),
                "title": str(item.get("title") or ""),
            }
            for item in notebook_history
        ],
    }
    record_change(
        event_type="rebuild",
        surface_id="notebook",
        summary=f"Notebook contract rebuilt for {candidate_id}.",
        candidate_id=stable_candidate_id,
        change_trigger="notebook_surface_refresh",
        reason_summary="Notebook contract now merges persisted memory with donor evidence sections.",
        implication_summary=base_contract["investment_case"],
        report_tab="investment_case",
        impact_level="low",
        requires_review=False,
        deep_link_target={
            "target_type": "notebook",
            "target_id": stable_candidate_id,
        },
    )
    return apply_overlay(base_contract, holdings=None)
