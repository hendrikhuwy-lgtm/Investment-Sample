"""
Surface contract shape parity tests.

Validates that each contract builder emits the fields declared in
v2_surface_contracts.ts. Serves as a guard against future drift between
backend output and frontend type expectations.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Compare contract
# ---------------------------------------------------------------------------

class TestCompareContractShape:
    def _contract(self):
        from app.v2.surfaces.compare.contract_builder import build
        return build("candidate_instrument_cmod", "candidate_instrument_sgln")

    def test_winner_name_present(self):
        contract = self._contract()
        assert "winner_name" in contract, "compare contract must emit winner_name"
        assert isinstance(contract["winner_name"], str)
        assert contract["winner_name"], "winner_name must be non-empty"

    def test_required_fields(self):
        contract = self._contract()
        required = {
            "surface_id", "contract_version", "generated_at", "freshness_state",
            "candidate_a_id", "candidate_b_id", "candidate_a_name", "candidate_b_name",
            "who_leads", "winner_name", "why_leads", "where_loser_wins",
            "what_would_change_comparison", "dimensions",
            "compare_ids", "sleeve_name", "candidates", "leader_candidate_id",
            "compare_readiness_state", "compare_readiness_note", "substitution_verdict",
            "substitution_rationale", "compare_dimensions", "discriminating_dimension_ids",
            "insufficient_dimensions",
        }
        for field in required:
            assert field in contract, f"Compare contract missing field: {field}"

    def test_compare_contract_emits_backend_owned_dimension_rows(self):
        contract = self._contract()
        assert isinstance(contract["compare_dimensions"], list)
        assert contract["compare_dimensions"], "compare_dimensions must be populated for a valid compare."
        first = contract["compare_dimensions"][0]
        assert "dimension_id" in first
        assert "values" in first


# ---------------------------------------------------------------------------
# Changes contract
# ---------------------------------------------------------------------------

class TestChangesContractShape:
    def _contract(self, surface_id: str = "blueprint_explorer"):
        from app.v2.surfaces.changes.contract_builder import build
        return build(surface_id)

    def test_required_top_level_fields(self):
        contract = self._contract()
        required = {
            "contract_version", "surface_id", "generated_at", "freshness_state",
            "change_events", "net_impact", "since_utc", "surface_state", "section_states",
            "window", "effective_since_utc", "summary", "available_sleeves",
            "available_categories", "feed_freshness_state", "latest_event_at",
            "latest_event_age_days", "filters_applied", "pagination",
        }
        for field in required:
            assert field in contract, f"Changes contract missing field: {field}"

    def test_change_events_is_list(self):
        contract = self._contract()
        assert isinstance(contract["change_events"], list)

    def test_change_event_has_semantic_fields(self):
        # Trigger a rebuild so there's at least one event
        from app.v2.core.change_ledger import record_change
        record_change(
            event_type="interpretation_change",
            surface_id="test_surface_shape_check",
            summary="Shape test event.",
        )
        contract = self._contract("test_surface_shape_check")
        events = contract["change_events"]
        assert events, "Expected at least one event after recording one."
        event = events[0]
        semantic_fields = {
            "event_id", "event_type", "summary", "changed_at_utc",
            "previous_state", "current_state", "implication_summary",
            "portfolio_consequence", "next_action", "what_would_reverse",
            "requires_review", "report_tab", "impact_level",
            "ui_category", "direction", "is_blocker_change",
            "category", "severity", "actionability", "scope", "confidence",
            "occurred_at", "effective_at", "title", "why_it_matters",
            "next_step", "evidence_refs",
        }
        for field in semantic_fields:
            assert field in event, f"Change event missing semantic field: {field}"

    def test_impact_level_valid(self):
        from app.v2.core.change_ledger import record_change
        record_change(
            event_type="truth_change",
            surface_id="test_impact_level_check",
            summary="High impact test event.",
        )
        contract = self._contract("test_impact_level_check")
        events = contract["change_events"]
        if not events:
            pytest.skip("No events recorded.")
        valid = {"high", "medium", "low"}
        for event in events:
            assert event["impact_level"] in valid, (
                f"Unexpected impact_level: {event['impact_level']}"
            )

    def test_report_tab_is_string(self):
        from app.v2.core.change_ledger import record_change
        record_change(
            event_type="rebuild",
            surface_id="test_report_tab_check",
            summary="Tab test event.",
        )
        contract = self._contract("test_report_tab_check")
        events = contract["change_events"]
        if not events:
            pytest.skip("No events.")
        for event in events:
            assert isinstance(event["report_tab"], str), "report_tab must be a string."


# ---------------------------------------------------------------------------
# Daily Brief contract
# ---------------------------------------------------------------------------

class TestDailyBriefContractShape:
    def _contract(self):
        from app.v2.surfaces.daily_brief.contract_builder import build
        return build()

    def test_required_top_level_fields(self):
        contract = self._contract()
        required = {
            "contract_version", "surface_id", "generated_at", "freshness_state",
            "what_changed", "why_it_matters_economically", "why_it_matters_here",
            "review_posture", "what_confirms_or_breaks", "evidence_and_trust",
            "surface_state", "section_states", "data_confidence", "decision_confidence",
            "surface_snapshot_id",
        }
        for field in required:
            assert field in contract, f"Daily brief missing field: {field}"

    def test_daily_brief_extended_sections_present(self):
        contract = self._contract()
        extended = {
            "market_state_cards",
            "signal_stack",
            "monitoring_conditions",
            "portfolio_impact_rows",
            "review_triggers",
            "scenario_blocks",
            "evidence_bars",
            "data_timeframes",
            "diagnostics",
        }
        for field in extended:
            assert field in contract, f"Daily brief missing enriched section field: {field}"

    def test_signal_card_has_enriched_fields(self):
        contract = self._contract()
        cards = contract.get("what_changed", [])
        if not cards:
            pytest.skip("No signal cards returned — macro inputs may be unavailable.")
        card = cards[0]
        enriched_fields = {
            "confirms", "breaks", "do_not_overread",
            "affected_sleeves", "affected_holdings",
            "mapping_directness", "trust_status", "related_work_id", "truth_envelope", "runtime_provenance",
        }
        for field in enriched_fields:
            assert field in card, f"Signal card missing enriched field: {field}"

    def test_signal_runtime_provenance_is_additively_explicit(self):
        contract = self._contract()
        cards = contract.get("what_changed", [])
        if not cards:
            pytest.skip("No signal cards returned.")
        provenance = cards[0].get("runtime_provenance")
        assert isinstance(provenance, dict)
        assert "source_family" in provenance
        assert "live_or_cache" in provenance
        assert "provenance_strength" in provenance
        assert "usable_truth" in provenance
        assert "sufficiency_state" in provenance
        assert "data_mode" in provenance
        assert "authority_level" in provenance

    def test_data_timeframes_are_structured(self):
        contract = self._contract()
        for row in contract.get("data_timeframes", []):
            assert "label" in row
            assert "summary" in row
            assert "truth_envelope" in row

    def test_market_state_cards_expose_runtime_provenance(self):
        contract = self._contract()
        cards = contract.get("market_state_cards", [])
        if not cards:
            pytest.skip("No market state cards returned.")
        provenance = cards[0].get("runtime_provenance")
        assert isinstance(provenance, dict)
        assert "source_family" in provenance
        assert "provenance_strength" in provenance
        assert "source_authority_tier" in provenance

    def test_market_state_cards_expose_validation_metadata(self):
        contract = self._contract()
        cards = contract.get("market_state_cards", [])
        if not cards:
            pytest.skip("No market state cards returned.")
        card = cards[0]
        assert "source_provider" in card
        assert "source_authority_tier" in card
        assert "validation_reason" in card
        assert "freshness_mode" in card
        assert "primary_provider" in card
        assert "cross_check_status" in card
        assert "authority_gap_reason" in card

    def test_affected_sleeves_is_list(self):
        contract = self._contract()
        cards = contract.get("what_changed", [])
        if not cards:
            pytest.skip("No signal cards.")
        for card in cards:
            assert isinstance(card["affected_sleeves"], list)

    def test_portfolio_impact_rows_use_exact_status_and_short_investor_copy(self):
        contract = self._contract()
        rows = contract.get("portfolio_impact_rows", [])
        if not rows:
            pytest.skip("No portfolio impact rows returned.")
        valid_statuses = {"Review", "Monitor", "Background"}
        for row in rows:
            assert row.get("status_label") in valid_statuses
            assert isinstance(row.get("consequence"), str) and row["consequence"].strip()
            assert "The main ETF choices are" not in row["consequence"]
            assert isinstance(row.get("next_step"), str) and row["next_step"].strip()
            assert row["next_step"] not in valid_statuses

    def test_mapping_directness_valid(self):
        contract = self._contract()
        cards = contract.get("what_changed", [])
        valid = {"direct", "sleeve-proxy", "macro-only"}
        for card in cards:
            assert card["mapping_directness"] in valid, (
                f"Unexpected mapping_directness: {card['mapping_directness']}"
            )


# ---------------------------------------------------------------------------
# Portfolio contract
# ---------------------------------------------------------------------------

class TestPortfolioContractShape:
    def _contract(self, account_id: str = "default"):
        from app.v2.surfaces.portfolio.contract_builder import build
        return build(account_id)

    def test_required_top_level_fields(self):
        contract = self._contract()
        required = {
            "contract_version", "surface_id", "generated_at", "freshness_state",
            "account_id", "mandate_state", "what_matters_now", "action_posture",
            "sleeve_drift_summary", "work_items", "holdings",
            "blueprint_consequence", "daily_brief_consequence", "surface_state", "section_states",
        }
        for field in required:
            assert field in contract, f"Portfolio contract missing field: {field}"

    def test_work_items_is_list(self):
        contract = self._contract()
        assert isinstance(contract["work_items"], list)

    def test_holdings_is_list(self):
        contract = self._contract()
        assert isinstance(contract["holdings"], list)

    def test_work_item_shape(self):
        contract = self._contract()
        work_items = contract["work_items"]
        if not work_items:
            pytest.skip("No work items — no off-target sleeves.")
        item = work_items[0]
        required = {
            "work_id", "title", "urgency", "affected_sleeves",
            "affected_holdings", "action_boundary", "what_invalidates_view",
        }
        for field in required:
            assert field in item, f"Work item missing field: {field}"

    def test_work_id_stable_format(self):
        contract = self._contract()
        for item in contract["work_items"]:
            assert str(item["work_id"]).startswith("work_"), (
                f"Unstable work_id format: {item['work_id']}"
            )

    def test_holding_row_shape(self):
        contract = self._contract()
        holdings = contract["holdings"]
        if not holdings:
            pytest.skip("No holdings available.")
        holding = holdings[0]
        required = {
            "holding_id", "symbol", "name", "sleeve_id",
            "review_status", "action_boundary", "next_review_reason",
        }
        for field in required:
            assert field in holding, f"Holding row missing field: {field}"

    def test_holding_id_stable_format(self):
        contract = self._contract()
        for holding in contract["holdings"]:
            assert str(holding["holding_id"]).startswith("holding_"), (
                f"Unstable holding_id format: {holding['holding_id']}"
            )

    def test_urgency_valid(self):
        contract = self._contract()
        valid = {"act", "review", "monitor"}
        for item in contract["work_items"]:
            assert item["urgency"] in valid, f"Unexpected urgency: {item['urgency']}"

    def test_sleeve_drift_rows_emit_canonical_target_fields(self):
        contract = self._contract()
        rows = contract["sleeve_drift_summary"]
        assert rows, "Portfolio contract must emit sleeve rows even before holdings are loaded."
        sample = rows[0]
        required = {
            "sleeve_id", "sleeve_name", "rank", "target_pct", "target_display",
            "min_pct", "max_pct", "sort_midpoint_pct", "is_nested", "parent_sleeve_id",
            "parent_sleeve_name", "counts_as_top_level_total", "target_label", "range_label",
            "current_pct", "drift_pct", "status", "band_status",
        }
        for field in required:
            assert field in sample, f"Sleeve drift row missing field: {field}"

    def test_portfolio_targets_no_longer_emit_zero_placeholder_targets(self):
        contract = self._contract()
        rows = contract["sleeve_drift_summary"]
        assert rows
        assert any(str(row.get("target_label") or "") == "50.0%" for row in rows)
        assert not any(
            str(row.get("target_label") or "") == "0.0%"
            for row in rows
        )

    def test_nested_equity_rows_do_not_count_as_top_level_total(self):
        contract = self._contract()
        nested = [row for row in contract["sleeve_drift_summary"] if row.get("is_nested")]
        assert nested
        for row in nested:
            assert row["counts_as_top_level_total"] is False
            assert row["parent_sleeve_name"] == "Global Equity Core"
