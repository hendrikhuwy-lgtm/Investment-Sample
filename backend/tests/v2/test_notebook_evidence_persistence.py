from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def isolated_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    db_path = tmp_path / "investment_agent_v2_test.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))
    return db_path


def _stable_candidate_id(identifier: str = "VWRA") -> str:
    from app.v2.donors.instrument_truth import get_instrument_truth

    truth = get_instrument_truth(identifier)
    return f"candidate_{truth.instrument_id}"


def test_notebook_builder_reads_persisted_entries(isolated_db: Path):
    from app.v2.storage.notebook_store import create_entry, set_status
    from app.v2.surfaces.notebook.contract_builder import build

    candidate_id = _stable_candidate_id("VWRA")
    draft = create_entry(
        candidate_id,
        linked_object_type="candidate",
        linked_object_id=candidate_id,
        linked_object_label="VWRA",
        title="VWRA thesis draft",
        thesis="Global equity core remains the default implementation.",
        assumptions="Macro stays supportive.",
        invalidation="Benchmark authority weakens.",
        watch_items="Fresh flows and valuation.",
        reflections="Initial persisted notebook draft.",
        next_review_date="2026-05-01",
    )
    finalized = create_entry(
        candidate_id,
        linked_object_type="candidate",
        linked_object_id=candidate_id,
        linked_object_label="VWRA",
        title="VWRA finalized note",
        thesis="Finalize current global-equity thesis.",
        assumptions="Evidence remains current.",
        invalidation="Cost edge disappears.",
        watch_items="Issuer refresh cadence.",
        reflections="Persisted finalized note.",
        next_review_date=None,
    )
    archived = create_entry(
        candidate_id,
        linked_object_type="candidate",
        linked_object_id=candidate_id,
        linked_object_label="VWRA",
        title="VWRA archived note",
        thesis="Older thesis memory.",
        assumptions="Past assumptions.",
        invalidation="Past invalidation.",
        watch_items="Past watch list.",
        reflections="Past reflections.",
        next_review_date=None,
    )
    set_status(finalized["entry_id"], "finalized")
    set_status(archived["entry_id"], "archived")

    contract = build("VWRA")
    assert contract["active_draft"] is not None
    assert contract["active_draft"]["title"] == draft["title"]
    assert contract["finalized_notes"]
    assert contract["archived_notes"]
    assert contract["note_history"]
    assert contract["research_support"] is not None
    assert contract["surface_state"]["state"] in {"ready", "degraded"}


def test_notebook_builder_seeds_persisted_memory_foundation_when_empty(isolated_db: Path):
    from app.v2.surfaces.notebook.contract_builder import build

    contract = build("VWRA")

    assert contract["active_draft"] is not None
    assert contract["memory_foundation_note"] is not None
    assert contract["research_support"] is not None
    assert contract["surface_state"]["state"] == "ready"


def test_evidence_builder_reads_persisted_workspace(isolated_db: Path):
    from app.v2.storage.evidence_store import add_claim, add_document, add_gap, add_mapping, add_tax_assumption
    from app.v2.surfaces.evidence_workspace.contract_builder import build

    candidate_id = _stable_candidate_id("VWRA")
    add_document(
        candidate_id,
        linked_object_type="candidate",
        linked_object_id=candidate_id,
        linked_object_label="VWRA",
        title="Issuer factsheet",
        document_type="factsheet",
        url="https://example.com/factsheet.pdf",
        retrieved_utc="2026-04-01T00:00:00Z",
        freshness_state="fresh_full_rebuild",
        stale=False,
    )
    add_claim(
        candidate_id,
        object_type="candidate",
        object_id=candidate_id,
        object_label="VWRA",
        claim_text="Expense ratio remains competitive.",
        claim_meta="Issuer factsheet",
        directness="direct",
        freshness_state="fresh_full_rebuild",
    )
    add_mapping(
        candidate_id,
        sleeve_label="Global Equity Core",
        instrument_label="VWRA",
        benchmark_label="ACWI",
        baseline_label="Cash / incumbent",
        directness="direct",
    )
    add_tax_assumption(candidate_id, label="Domicile", value="Assume Ireland wrapper treatment.")
    add_gap(candidate_id, object_label="VWRA", issue_text="No independent third-party valuation note yet.")

    contract = build("VWRA")
    assert contract["documents"]
    assert contract["object_groups"]
    assert contract["benchmark_mappings"]
    assert contract["tax_assumptions"]
    assert contract["gaps"]
    assert contract["source_authority_fields"]
    assert contract["reconciliation_report"]
    assert contract["data_quality_summary"]
    assert contract["research_support"] is not None
    assert contract["summary"]["direct_count"] >= 1


def test_evidence_builder_handles_structured_resolved_values_without_crashing(isolated_db: Path, monkeypatch: pytest.MonkeyPatch):
    import app.v2.surfaces.evidence_workspace.contract_builder as evidence_builder
    from app.v2.core.domain_objects import EvidenceCitation, EvidencePack
    from app.v2.donors.instrument_truth import get_instrument_truth

    truth = get_instrument_truth("VWRA")
    evidence_pack = EvidencePack(
        evidence_id="pack_vwra",
        thesis="VWRA evidence pack",
        summary="VWRA retains direct issuer support.",
        freshness="fresh_full_rebuild",
        observed_at="2026-04-01T00:00:00Z",
        citations=[EvidenceCitation(source_id="issuer_factsheet", label="Issuer factsheet")],
        facts={"candidate_symbol": truth.symbol, "field_truths": []},
    )

    monkeypatch.setattr(evidence_builder, "get_instrument_truth", lambda candidate_id: truth)
    monkeypatch.setattr(evidence_builder, "build_evidence_pack", lambda candidate_id: evidence_pack)
    monkeypatch.setattr(
        evidence_builder,
        "build_candidate_truth_context",
        lambda conn, candidate: {
            "source_authority_map": [
                {
                    "field_name": "benchmark_lineage",
                    "label": "Benchmark lineage",
                    "resolved_value": {"benchmark": "ACWI", "provider": "issuer"},
                    "authority_class": "issuer_primary",
                    "freshness_state": "current",
                    "recommendation_critical": True,
                    "document_support_state": "present",
                }
            ],
            "reconciliation_report": [],
            "data_quality": {
                "data_confidence": "high",
                "critical_fields_ready": 1,
                "critical_fields_total": 1,
                "summary": "Critical fields are ready.",
            },
            "primary_document_manifest": [],
        },
    )
    monkeypatch.setattr(
        evidence_builder,
        "read_workspace",
        lambda candidate_id: {
            "documents": [],
            "claims": [],
            "mappings": [],
            "tax_assumptions": [],
            "gaps": [],
            "object_links": [],
        },
    )
    monkeypatch.setattr(evidence_builder, "record_change", lambda **_: None)

    contract = evidence_builder.build("VWRA")

    assert contract["research_support"] is not None
    candidate_group = next(group for group in contract["object_groups"] if group["title"] == "Candidate")
    claim_text = " ".join(claim["claim_text"] for item in candidate_group["items"] for claim in item["claims"])
    assert "benchmark=ACWI" in claim_text
