from __future__ import annotations

from importlib import import_module
from pathlib import Path

import pytest

from app.services.portfolio_blueprint import build_portfolio_blueprint_payload


REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICES_ROOT = REPO_ROOT / "backend" / "app" / "services"
DEMO_ROOT = REPO_ROOT.parent / "investment-agent-demo"


def _find_candidate(payload: dict, symbol: str) -> dict:
    target = symbol.upper()
    for sleeve in payload.get("sleeves", []):
        for candidate in sleeve.get("candidates", []):
            if str(candidate.get("symbol") or "").upper() == target:
                return candidate
    raise AssertionError(f"Candidate {symbol} not found")


def test_portfolio_blueprint_entry_module_is_thin_wrapper() -> None:
    source = (SERVICES_ROOT / "portfolio_blueprint.py").read_text(encoding="utf-8")

    assert "from app.services.blueprint_payload_assembler import" in source
    assert "build_investment_thesis" not in source
    assert "build_thesis_sections" not in source
    assert "build_legacy_detail_explanation" not in source
    assert "build_canonical_decision_object" not in source


def test_deleted_legacy_explanation_modules_are_not_importable() -> None:
    with pytest.raises(ModuleNotFoundError):
        import_module("app.services.blueprint_candidate_explanation_writer")
    with pytest.raises(ModuleNotFoundError):
        import_module("app.services.blueprint_thesis_sections")
    with pytest.raises(ModuleNotFoundError):
        import_module("app.services.blueprint_legacy_adapter")


def test_official_payload_exposes_canonical_decision_only() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VWRA")

    assert dict(candidate.get("canonical_decision") or {})
    assert dict(candidate.get("lens_assessment") or {})
    assert dict(candidate.get("lens_fusion_result") or {})
    assert "lens_review_set" not in candidate
    assert "detail_explanation" not in candidate
    assert "thesis_sections" not in candidate


def test_official_frontend_source_has_no_mock_or_local_semantic_fallbacks() -> None:
    api_source = (DEMO_ROOT / "src" / "lib" / "api.ts").read_text(encoding="utf-8")
    transformer_source = (DEMO_ROOT / "src" / "lib" / "transformers.ts").read_text(encoding="utf-8")
    canonical_contract_source = (REPO_ROOT / "shared" / "canonical_frontend_contract.ts").read_text(encoding="utf-8")
    candidate_detail_source = (DEMO_ROOT / "src" / "components" / "blueprint" / "CandidateDetail.tsx").read_text(encoding="utf-8")
    candidate_row_source = (DEMO_ROOT / "src" / "components" / "blueprint" / "CandidateRow.tsx").read_text(encoding="utf-8")
    signal_card_source = (DEMO_ROOT / "src" / "components" / "daily-brief" / "SignalCard.tsx").read_text(encoding="utf-8")
    types_source = (DEMO_ROOT / "src" / "lib" / "types.ts").read_text(encoding="utf-8")

    assert "mockBlueprint" not in api_source
    assert "mockDailyBrief" not in api_source
    assert "useMockFallback" not in api_source
    assert "canonicalDecisionOrFallback" not in transformer_source
    assert "throw new Error(`Canonical decision missing" in transformer_source
    assert "throw new Error(`Canonical summary missing" in transformer_source
    assert "Unsupported canonical promotion_state" in transformer_source
    assert "Canonical Daily Brief action_state missing" in transformer_source
    assert "Unsupported canonical Daily Brief action_state" in transformer_source
    assert "detail_explanation?:" not in canonical_contract_source
    assert "thesis_sections?:" not in canonical_contract_source
    assert "lens_review_set" not in canonical_contract_source
    assert "canonical_decision: CanonicalCandidateDecision;" in canonical_contract_source
    assert "export type CanonicalPromotionState = 'research_only' | 'acceptable' | 'near_decision_ready' | 'buyable';" in canonical_contract_source
    assert "promotion_state?: CanonicalPromotionState | null;" in canonical_contract_source
    assert "export type CanonicalDailyBriefActionState = 'ignore' | 'monitor' | 'review';" in canonical_contract_source
    assert "action_state?: CanonicalDailyBriefActionState | null;" in canonical_contract_source
    assert "lens_assessment?:" in canonical_contract_source
    assert "lens_fusion_result?:" in canonical_contract_source
    assert "framework_judgment?:" in canonical_contract_source
    assert "signal.explanation?" not in signal_card_source
    assert "lensContext" in signal_card_source
    assert "card.action_tag" not in transformer_source
    assert "candidate.canonical_decision?.action_boundary" not in candidate_row_source
    assert "candidate.summary_line" not in candidate_row_source
    assert "candidate.promotion_state" not in candidate_row_source
    assert "buildFallbackDecision" not in candidate_detail_source
    assert "No canonical summary available." not in candidate_detail_source
    assert "candidate.summary_line" not in candidate_detail_source
    assert "candidate.promotion_state" not in candidate_detail_source
    assert "Framework Lens Judgment" in candidate_detail_source
    assert "summary_line:" not in types_source
    assert "promotion_state?:" not in types_source
    assert "review' | 'action" not in types_source


def test_contributor_guardrails_document_official_git_roots_and_retired_surfaces() -> None:
    guardrails_source = (REPO_ROOT / "docs" / "FRAMEWORK_CONTRIBUTOR_GUARDRAILS.md").read_text(encoding="utf-8")

    assert "/Users/huwenyihendrik/Projects/investment-agent" in guardrails_source
    assert "/Users/huwenyihendrik/Projects/investment-agent-demo" in guardrails_source
    assert "Do not reintroduce `detail_explanation`, `thesis_sections`, or `lens_review_set`" in guardrails_source
    assert "removed after Cortex/V2 became the active runtime" in guardrails_source
    assert "`investment-agent/frontend/`" in guardrails_source
    assert "`investment-agent/frontend_snapshots/`" in guardrails_source
