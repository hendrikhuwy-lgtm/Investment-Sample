from __future__ import annotations

import sqlite3
from typing import Any

from app.config import get_db_path
from app.v2.core.change_ledger import record_change
from app.v2.core.holdings_overlay import apply_overlay
from app.v2.core.domain_objects import EvidenceCitation, EvidencePack, InstrumentTruth, utc_now_iso
from app.v2.donors.evidence_pack import build_evidence_pack
from app.v2.donors.instrument_truth import get_instrument_truth
from app.v2.features.research_support import build_research_support_pack
from app.v2.forecasting.store import list_forecast_evidence_refs
from app.v2.storage.evidence_store import read_workspace
from app.v2.surfaces.common import degraded_section, empty_section, ready_section, surface_state
from app.v2.truth.candidate_quality import build_candidate_truth_context


_CONTRACT_VERSION = "0.2.0"
_SURFACE_ID = "evidence_workspace"


def _candidate_contract_id(truth: InstrumentTruth) -> str:
    return f"candidate_{truth.instrument_id}"


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _source_rows(evidence_pack: EvidencePack) -> list[dict[str, Any]]:
    facts = dict(evidence_pack.facts or {})
    rows = list(facts.get("citation_rows") or [])
    if rows:
        return rows
    return [
        {
            "source_id": citation.source_id,
            "label": citation.label,
            "url": citation.url,
            "observed_at": evidence_pack.observed_at,
            "confidence_label": "medium",
            "source_type": "unclassified",
        }
        for citation in evidence_pack.citations
    ]


def _reliability(row: dict[str, Any], citation: EvidenceCitation | None = None) -> str:
    confidence = str(row.get("confidence_label") or "").strip().lower()
    source_type = str(row.get("source_type") or "").strip().lower()
    note = str(getattr(citation, "note", "") or "").strip().lower()
    if confidence == "high" or source_type in {"issuer_holdings_primary", "issuer_factsheet_secondary"} or "issuer_" in note:
        return "high"
    if confidence == "low" or source_type in {"proxy_only_last_resort", "internal_fallback"}:
        return "low"
    return "medium"


def _directness_from_row(row: dict[str, Any]) -> str:
    source_type = str(row.get("source_type") or "").strip().lower()
    if source_type in {"issuer_holdings_primary", "issuer_factsheet_secondary"}:
        return "direct"
    if "proxy" in source_type:
        return "proxy"
    return "direct"


def _claim_value_text(value: Any) -> str:
    if value is None or value == "":
        return "not available"
    if isinstance(value, dict):
        visible = ", ".join(f"{key}={item}" for key, item in list(value.items())[:4])
        return visible or "not available"
    if isinstance(value, list):
        visible = ", ".join(str(item) for item in value[:4])
        return visible or "not available"
    return str(value)


def _candidate_claims(truth: InstrumentTruth, evidence_pack: EvidencePack) -> list[dict[str, Any]]:
    facts = dict(evidence_pack.facts or {})
    field_truths = list(facts.get("field_truths") or [])
    claims: list[dict[str, Any]] = []
    for index, row in enumerate(field_truths[:8], start=1):
        item = dict(row or {})
        field_name = str(item.get("field_name") or f"field_{index}").replace("_", " ").title()
        value = item.get("resolved_value")
        claims.append(
            {
                "claim_id": f"derived_claim_{index}",
                "claim_text": f"{field_name}: {_claim_value_text(value)}",
                "claim_meta": str(item.get("source_name") or "Derived evidence row"),
                "directness": _directness_from_row(item),
                "freshness_state": str(item.get("value_type") or evidence_pack.freshness or "stored_valid_context"),
            }
        )
    if claims:
        return claims
    return [
        {
            "claim_id": "derived_claim_overview",
            "claim_text": evidence_pack.summary,
            "claim_meta": truth.name or truth.symbol,
            "directness": "proxy",
            "freshness_state": evidence_pack.freshness,
        }
    ]


def _authority_field_claims(source_authority_fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    for index, field in enumerate(
        [row for row in source_authority_fields if row.get("recommendation_critical")][:6],
        start=1,
    ):
        label = str(field.get("label") or field.get("field_name") or f"Field {index}")
        value = field.get("resolved_value")
        authority = str(field.get("authority_class") or "unknown").replace("_", " ")
        freshness = str(field.get("freshness_state") or "stored_valid_context")
        claims.append(
            {
                "claim_id": f"authority_claim_{index}",
                "claim_text": f"{label}: {_claim_value_text(value)}",
                "claim_meta": f"{authority} · {freshness.replace('_', ' ')}",
                "directness": "direct" if authority.startswith("issuer") or authority.startswith("verified") else "proxy",
                "freshness_state": freshness,
            }
        )
    return claims


def _derived_documents(evidence_pack: EvidencePack) -> list[dict[str, Any]]:
    rows = _source_rows(evidence_pack)
    documents = [
        {
            "document_id": f"derived_document_{index}",
            "title": str(row.get("label") or row.get("source_id") or "Source document"),
            "document_type": str(row.get("source_type") or "source_document"),
            "linked_object_label": str(evidence_pack.facts.get("candidate_symbol") or "Candidate"),
            "linked_object_type": "candidate",
            "retrieved_utc": row.get("observed_at") or evidence_pack.observed_at,
            "freshness_state": "stored_valid_context" if _reliability(row) == "low" else evidence_pack.freshness,
            "stale": not bool(row.get("observed_at")),
            "url": row.get("url"),
        }
        for index, row in enumerate(rows, start=1)
    ]
    for index, item in enumerate(list(evidence_pack.facts.get("primary_documents") or []), start=len(documents) + 1):
        document = dict(item or {})
        status = str(document.get("status") or "").strip().lower()
        documents.append(
            {
                "document_id": f"derived_document_primary_{index}",
                "title": (
                    f"Issuer {str(document.get('doc_type') or 'document').replace('_', ' ')}"
                    f" · {str(document.get('authority_class') or 'authority').replace('_', ' ')}"
                ),
                "document_type": str(document.get("doc_type") or "issuer_document"),
                "linked_object_label": str(evidence_pack.facts.get("candidate_symbol") or "Candidate"),
                "linked_object_type": "candidate",
                "retrieved_utc": document.get("retrieved_at"),
                "freshness_state": evidence_pack.freshness,
                "stale": status not in {"success", "verified"},
                "url": document.get("doc_url"),
            }
        )
    return documents


def _truth_source_citations(truth: InstrumentTruth) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pack in truth.evidence:
        for citation in pack.citations:
            rows.append(
                {
                    "source_id": citation.source_id,
                    "title": citation.label,
                    "url": citation.url,
                    "retrieved_utc": pack.observed_at,
                    "reliability": "high" if "issuer" in citation.source_id or "factsheet" in citation.source_id else "medium",
                }
            )
    return rows


def _derived_object_links(
    *,
    stable_candidate_id: str,
    truth: InstrumentTruth,
    benchmark_mappings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = [
        {
            "object_type": "candidate",
            "object_id": stable_candidate_id,
            "object_label": truth.name or truth.symbol,
        }
    ]
    seen: set[tuple[str, str]] = {("candidate", stable_candidate_id)}
    for mapping in benchmark_mappings:
        benchmark_label = str(mapping.get("benchmark_label") or "").strip()
        baseline_label = str(mapping.get("baseline_label") or "").strip()
        sleeve_label = str(mapping.get("sleeve_label") or "").strip()
        for object_type, object_label in (
            ("benchmark", benchmark_label),
            ("baseline", baseline_label),
            ("sleeve", sleeve_label),
        ):
            if not object_label:
                continue
            object_id = f"{object_type}_{object_label.lower().replace(' ', '_').replace('/', '_')}"
            key = (object_type, object_id)
            if key in seen:
                continue
            seen.add(key)
            links.append(
                {
                    "object_type": object_type,
                    "object_id": object_id,
                    "object_label": object_label,
                }
            )
    return links


def _derived_non_candidate_claims(
    *,
    object_type: str,
    object_label: str,
    benchmark_mappings: list[dict[str, Any]],
    truth: InstrumentTruth,
    freshness_state: str,
) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    for index, mapping in enumerate(benchmark_mappings, start=1):
        mapping_directness = str(mapping.get("directness") or "bounded")
        benchmark_label = str(mapping.get("benchmark_label") or "")
        baseline_label = str(mapping.get("baseline_label") or "")
        sleeve_label = str(mapping.get("sleeve_label") or "")
        if object_type == "benchmark" and benchmark_label == object_label:
            claims.append(
                {
                    "claim_id": f"derived_benchmark_claim_{index}",
                    "claim_text": f"{truth.symbol} is evaluated against {benchmark_label} for sleeve fit and replacement logic.",
                    "claim_meta": f"{mapping_directness} mapping",
                    "directness": "direct" if mapping_directness == "direct" else "proxy",
                    "freshness_state": freshness_state,
                }
            )
        elif object_type == "baseline" and baseline_label == object_label:
            claims.append(
                {
                    "claim_id": f"derived_baseline_claim_{index}",
                    "claim_text": f"{baseline_label} remains the default comparison anchor before replacing an incumbent or cash alternative.",
                    "claim_meta": f"{sleeve_label or 'Candidate sleeve'} baseline",
                    "directness": "proxy",
                    "freshness_state": freshness_state,
                }
            )
        elif object_type == "sleeve" and sleeve_label == object_label:
            claims.append(
                {
                    "claim_id": f"derived_sleeve_claim_{index}",
                    "claim_text": f"{truth.symbol} is currently linked to {sleeve_label} through the benchmark and evidence map.",
                    "claim_meta": f"{benchmark_label or 'Benchmark pending'} comparison route",
                    "directness": "proxy",
                    "freshness_state": freshness_state,
                }
            )
    return claims


def build(candidate_id: str) -> dict[str, object]:
    truth = get_instrument_truth(candidate_id)
    evidence_pack = build_evidence_pack(candidate_id)
    stable_candidate_id = _candidate_contract_id(truth)
    with _connection() as conn:
        truth_context = build_candidate_truth_context(
            conn,
            {
                "symbol": truth.symbol,
                "sleeve_key": str(truth.metrics.get("sleeve_key") or ""),
                "expense_ratio": truth.metrics.get("expense_ratio"),
                "domicile": truth.domicile,
                "primary_documents": truth.metrics.get("primary_documents"),
            },
        )
    workspace = read_workspace(stable_candidate_id)
    forecast_support_items = list_forecast_evidence_refs(stable_candidate_id)
    source_authority_fields = list(truth_context.get("source_authority_map") or [])
    reconciliation_report = list(truth_context.get("reconciliation_report") or [])
    data_quality_summary = dict(truth_context.get("data_quality") or {})
    primary_document_manifest = list(truth_context.get("primary_document_manifest") or [])
    research_support = build_research_support_pack(
        truth=truth,
        target_surface="evidence_workspace",
        source_authority_fields=source_authority_fields,
        reconciliation_report=reconciliation_report,
        primary_document_manifest=primary_document_manifest,
        recommendation_gate=dict(truth_context.get("recommendation_gate") or {}),
        data_quality_summary=data_quality_summary,
        implementation_profile=dict(truth_context.get("implementation_profile") or {}),
        evidence_summary=evidence_pack.summary,
        decision_line="Evidence support should strengthen recommendation-critical fields, benchmark links, and document backing before it changes the candidate view.",
        drift_surface_id="candidate_report",
        drift_object_id=stable_candidate_id,
        drift_state={
            "gate_state": str(dict(truth_context.get("recommendation_gate") or {}).get("gate_state") or "review_only"),
            "data_confidence": str(data_quality_summary.get("data_confidence") or "mixed"),
            "reconciliation_status": str(dict(truth_context.get("reconciliation") or {}).get("status") or "soft_drift"),
            "blocked_reason_count": len(list(dict(truth_context.get("recommendation_gate") or {}).get("blocked_reasons") or [])),
            "critical_missing_count": len(list(dict(truth_context.get("recommendation_gate") or {}).get("critical_missing_fields") or [])),
        },
        sleeve_key=str(truth.metrics.get("sleeve_key") or truth.metrics.get("sleeve_affiliation") or ""),
    )
    completeness_score = dict(evidence_pack.facts or {}).get("completeness_score")
    citations_by_id = {citation.source_id: citation for citation in evidence_pack.citations}
    source_citations = [
        {
            "source_id": str(row.get("source_id") or ""),
            "title": str(row.get("label") or row.get("source_id") or "Unnamed source"),
            "url": row.get("url"),
            "retrieved_utc": row.get("observed_at") or evidence_pack.observed_at or None,
            "reliability": _reliability(row, citations_by_id.get(str(row.get("source_id") or ""))),
        }
        for row in _source_rows(evidence_pack)
    ]
    if not source_citations:
        source_citations = _truth_source_citations(truth)
    workspace_documents = [
        {
            "document_id": str(row.get("document_id") or ""),
            "title": str(row.get("title") or "Document"),
            "document_type": str(row.get("document_type") or "document"),
            "linked_object_label": str(row.get("linked_object_label") or truth.name or truth.symbol),
            "linked_object_type": str(row.get("linked_object_type") or "candidate"),
            "retrieved_utc": row.get("retrieved_utc"),
            "freshness_state": str(row.get("freshness_state") or evidence_pack.freshness),
            "stale": bool(row.get("stale")),
            "url": row.get("url"),
        }
        for row in workspace["documents"]
    ]
    documents = workspace_documents or _derived_documents(evidence_pack)
    if not documents and source_citations:
        documents = [
            {
                "document_id": f"truth_document_{index}",
                "title": citation["title"],
                "document_type": "source_document",
                "linked_object_label": truth.name or truth.symbol,
                "linked_object_type": "candidate",
                "retrieved_utc": citation["retrieved_utc"],
                "freshness_state": evidence_pack.freshness,
                "stale": not bool(citation["retrieved_utc"]),
                "url": citation["url"],
            }
            for index, citation in enumerate(source_citations, start=1)
        ]
    benchmark_mappings = [
        {
            "mapping_id": str(row.get("mapping_id") or ""),
            "sleeve_label": str(row.get("sleeve_label") or ""),
            "instrument_label": str(row.get("instrument_label") or ""),
            "benchmark_label": str(row.get("benchmark_label") or ""),
            "baseline_label": str(row.get("baseline_label") or ""),
            "directness": str(row.get("directness") or ""),
        }
        for row in workspace["mappings"]
    ]
    if not benchmark_mappings:
        benchmark_mappings = [
            {
                "mapping_id": "derived_mapping_primary",
                "sleeve_label": str(truth.metrics.get("sleeve_key") or truth.metrics.get("sleeve_affiliation") or "Candidate sleeve").replace("_", " ").title(),
                "instrument_label": truth.symbol,
                "benchmark_label": str(truth.metrics.get("benchmark_label") or truth.benchmark_id or "Benchmark pending"),
                "baseline_label": "Cash / incumbent",
                "directness": str(truth.metrics.get("benchmark_authority_level") or "bounded"),
            }
        ]

    claims_by_object: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in workspace["claims"]:
        key = (str(row.get("object_type") or "candidate"), str(row.get("object_id") or stable_candidate_id))
        claims_by_object.setdefault(key, []).append(
            {
                "claim_id": str(row.get("claim_id") or ""),
                "claim_text": str(row.get("claim_text") or ""),
                "claim_meta": str(row.get("claim_meta") or ""),
                "directness": str(row.get("directness") or "proxy"),
                "freshness_state": str(row.get("freshness_state") or evidence_pack.freshness),
            }
        )

    object_groups: list[dict[str, Any]] = []
    object_links = workspace["object_links"] or _derived_object_links(
        stable_candidate_id=stable_candidate_id,
        truth=truth,
        benchmark_mappings=benchmark_mappings,
    )
    for link in object_links:
        object_type = str(link.get("object_type") or "candidate")
        object_id = str(link.get("object_id") or stable_candidate_id)
        object_label = str(link.get("object_label") or truth.name or truth.symbol)
        claims = claims_by_object.get((object_type, object_id)) or (
            _candidate_claims(truth, evidence_pack)
            if object_type == "candidate"
            else _derived_non_candidate_claims(
                object_type=object_type,
                object_label=object_label,
                benchmark_mappings=benchmark_mappings,
                truth=truth,
                freshness_state=evidence_pack.freshness,
            )
        )
        if object_type == "candidate":
            claims = claims + _authority_field_claims(source_authority_fields)
        direct_count = sum(1 for claim in claims if claim["directness"] == "direct")
        proxy_count = sum(1 for claim in claims if claim["directness"] != "direct")
        stale_count = sum(1 for claim in claims if claim["freshness_state"] in {"stored_valid_context", "execution_failed_or_incomplete"})
        gap_flag = not claims
        group_title = {
            "candidate": "Candidate",
            "benchmark": "Benchmarks",
            "baseline": "Baselines",
            "sleeve": "Sleeve links",
        }.get(object_type, object_type.replace("_", " ").title())
        existing_group = next((group for group in object_groups if group["title"] == group_title), None)
        item = {
            "object_type": object_type,
            "object_id": object_id,
            "object_label": object_label,
            "direct_count": direct_count,
            "proxy_count": proxy_count,
            "stale_count": stale_count,
            "gap_flag": gap_flag,
            "claims": claims,
        }
        if existing_group is None:
            object_groups.append({"title": group_title, "items": [item]})
        else:
            existing_group["items"].append(item)

    tax_assumptions = [
        {
            "assumption_id": str(row.get("assumption_id") or ""),
            "label": str(row.get("label") or ""),
            "value": str(row.get("value") or ""),
        }
        for row in workspace["tax_assumptions"]
    ]
    if not tax_assumptions:
        tax_assumptions = [
            {
                "assumption_id": "derived_tax_primary",
                "label": "Wrapper and domicile",
                "value": (
                    f"Current truth assumes domicile {truth.domicile or 'unknown'} and base currency {truth.base_currency or 'unknown'} "
                    "until explicit tax assumptions are stored."
                ),
            }
        ]

    gaps = [
        {
            "gap_id": str(row.get("gap_id") or ""),
            "object_label": str(row.get("object_label") or ""),
            "issue_text": str(row.get("issue_text") or ""),
        }
        for row in workspace["gaps"]
    ]
    if not gaps and not source_citations:
        gaps = [
            {
                "gap_id": "derived_gap_primary",
                "object_label": truth.symbol,
                "issue_text": "No source citations have been attached to this candidate workspace yet.",
            }
        ]
    for field in source_authority_fields:
        if field.get("freshness_state") == "missing":
            gaps.append(
                {
                    "gap_id": f"derived_gap_field_{field['field_name']}",
                    "object_label": truth.symbol,
                    "issue_text": f"{field['label']} is still missing from recommendation-critical evidence.",
                }
            )
    if not workspace["mappings"] and benchmark_mappings:
        gaps.append(
            {
                "gap_id": "derived_gap_mapping_support",
                "object_label": truth.symbol,
                "issue_text": "Benchmark mappings are still derived from current truth rather than confirmed workspace entries.",
            }
        )
    if not workspace["tax_assumptions"]:
        gaps.append(
            {
                "gap_id": "derived_gap_tax_support",
                "object_label": truth.symbol,
                "issue_text": "Tax and wrapper assumptions are still defaulted from instrument truth instead of explicit workspace notes.",
            }
        )
    for field in source_authority_fields:
        if field.get("recommendation_critical") and field.get("document_support_state") == "missing":
            gaps.append(
                {
                    "gap_id": f"derived_gap_doc_support_{field['field_name']}",
                    "object_label": truth.symbol,
                    "issue_text": f"{field['label']} does not yet have preferred issuer-document backing in the current evidence map.",
                }
            )

    direct_count = sum(len([claim for claim in item["claims"] if claim["directness"] == "direct"]) for group in object_groups for item in group["items"])
    proxy_count = sum(len([claim for claim in item["claims"] if claim["directness"] != "direct"]) for group in object_groups for item in group["items"])
    stale_count = sum(len([claim for claim in item["claims"] if claim["freshness_state"] in {"stored_valid_context", "execution_failed_or_incomplete"}]) for group in object_groups for item in group["items"])
    gap_count = sum(1 for group in object_groups for item in group["items"] if item["gap_flag"]) + len(gaps)

    base_contract = {
        "contract_version": _CONTRACT_VERSION,
        "surface_id": _SURFACE_ID,
        "generated_at": utc_now_iso(),
        "freshness_state": evidence_pack.freshness,
        "surface_state": surface_state(
            "ready" if (source_citations or workspace_documents or workspace["claims"]) else "degraded",
            reason_codes=[] if (source_citations or workspace_documents or workspace["claims"]) else ["partial_evidence"],
            summary=(
                "Evidence Workspace now merges donor citations with stored documents, claims, mappings, tax assumptions, and gaps."
                if (source_citations or workspace_documents or workspace["claims"])
                else "Evidence Workspace preserves structure but still has partial evidence coverage."
            ),
        ),
        "section_states": {
            "summary": ready_section(),
            "object_groups": ready_section() if object_groups else degraded_section("no_object_groups", "No object groups were emitted."),
            "documents": ready_section() if documents else empty_section("no_documents", "No evidence documents are attached yet."),
            "benchmark_mappings": ready_section() if benchmark_mappings else degraded_section("no_benchmark_mappings", "No benchmark mappings were emitted."),
            "tax_assumptions": ready_section() if tax_assumptions else degraded_section("no_tax_assumptions", "No tax assumptions were emitted."),
            "gaps": ready_section() if gaps else empty_section("no_gaps", "No explicit evidence gaps are currently stored."),
        },
        "candidate_id": stable_candidate_id,
        "name": truth.name or truth.symbol,
        "evidence_pack": {
            "source_count": len(source_citations),
            "freshness_state": evidence_pack.freshness,
            "completeness_score": completeness_score,
        },
        "source_citations": source_citations,
        "completeness_score": completeness_score,
        "source_authority_fields": source_authority_fields,
        "reconciliation_report": reconciliation_report,
        "data_quality_summary": data_quality_summary,
        "primary_document_manifest": primary_document_manifest,
        "summary": {
            "direct_count": direct_count,
            "proxy_count": proxy_count,
            "stale_count": stale_count,
            "gap_count": gap_count,
        },
        "object_groups": object_groups,
        "documents": documents,
        "benchmark_mappings": benchmark_mappings,
        "tax_assumptions": tax_assumptions,
        "gaps": gaps,
        "forecast_support_items": forecast_support_items,
        "research_support": research_support,
    }
    record_change(
        event_type="rebuild",
        surface_id="evidence_workspace",
        summary=f"Evidence workspace contract rebuilt for {candidate_id}.",
        candidate_id=stable_candidate_id,
        change_trigger="evidence_workspace_refresh",
        reason_summary="Evidence workspace merges donor coverage with persisted support-map content.",
        implication_summary=evidence_pack.summary,
        report_tab="evidence",
        impact_level="low",
        deep_link_target={
            "target_type": "evidence_workspace",
            "target_id": stable_candidate_id,
        },
    )
    return apply_overlay(base_contract, holdings=None)
