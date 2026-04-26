from __future__ import annotations

from typing import Any

from app.services.portfolio_blueprint import BLUEPRINT_PAYLOAD_INTEGRITY_VERSION


def blueprint_payload_supports_candidate_detail(payload: dict[str, Any]) -> tuple[bool, str | None]:
    blueprint_meta = dict(payload.get("blueprint_meta") or {})
    if int(blueprint_meta.get("payload_integrity_version") or 0) != BLUEPRINT_PAYLOAD_INTEGRITY_VERSION:
        return False, "payload_integrity_version_mismatch"
    truth_quality = dict(blueprint_meta.get("truth_quality") or {})
    if "candidate_truth_quality_state_counts" not in truth_quality:
        return False, "missing_truth_quality_summary"
    saw_candidate_detail = False
    for sleeve in list(payload.get("sleeves") or []):
        for candidate in list(dict(sleeve).get("candidates") or []):
            if not isinstance(candidate, dict):
                continue
            saw_candidate_detail = True
            decision_record = dict(candidate.get("decision_record") or {})
            benchmark_assignment = dict(candidate.get("benchmark_assignment") or {})
            investment_lens = dict(candidate.get("investment_lens") or {})
            liquidity_profile = dict(investment_lens.get("liquidity_profile") or {})
            usability_memo = dict(candidate.get("usability_memo") or {})
            action_readiness = str(candidate.get("action_readiness") or "")
            if not decision_record:
                return False, "missing_decision_record"
            if not all(
                [
                    decision_record.get("mandate_fit_state"),
                    decision_record.get("sleeve_fit_state"),
                    decision_record.get("policy_gate_state"),
                    decision_record.get("data_quality_state"),
                    decision_record.get("scoring_state"),
                ]
            ):
                return False, "missing_policy_stack_state"
            if benchmark_assignment and not all(
                [
                    benchmark_assignment.get("benchmark_fit_type"),
                    benchmark_assignment.get("benchmark_authority_level"),
                    benchmark_assignment.get("benchmark_effect_label"),
                    benchmark_assignment.get("benchmark_kind"),
                    benchmark_assignment.get("benchmark_role"),
                ]
            ):
                return False, "missing_benchmark_enrichment"
            if action_readiness not in {"usable_now", "usable_with_limits", "review_only", "not_usable_now"}:
                return False, "missing_operational_action_readiness"
            if not usability_memo.get("state") or not usability_memo.get("summary"):
                return False, "missing_operational_usability_fields"
            if not liquidity_profile.get("liquidity_status"):
                return False, "missing_liquidity_state"
            if "explanation" not in liquidity_profile:
                return False, "missing_liquidity_explanation"
            if not isinstance(candidate.get("truth_quality_summary"), dict):
                return False, "missing_candidate_truth_quality_summary"
            if any(
                str(decision_record.get(key) or "")
                in {
                    "partial",
                    "not_evaluated",
                    "blocked_by_missing_input",
                    "unknown_due_to_missing_inputs",
                    "unknown",
                }
                for key in ("sleeve_fit_state", "policy_gate_state", "data_quality_state", "scoring_state")
            ) and not dict(decision_record.get("explanations") or {}):
                return False, "missing_decision_explanations"
    if saw_candidate_detail:
        return True, None
    return False, "no_candidate_detail_rows"
