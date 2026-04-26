from __future__ import annotations

from typing import Any


def build_rejection_memo(
    candidate_record: dict[str, Any],
    winner_record: dict[str, Any] | None,
    evaluation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    decision = dict(candidate_record.get("decision_record") or {})
    quality = dict(candidate_record.get("investment_quality") or {})
    completeness = dict(candidate_record.get("data_completeness") or {})
    pressures = list(dict(candidate_record.get("eligibility") or {}).get("pressures") or [])
    winner_quality = dict((winner_record or {}).get("investment_quality") or {})
    rejection_type = str(decision.get("final_decision_state") or quality.get("recommendation_state") or "research_only")

    comparative_disadvantages: list[str] = []
    if winner_record:
        if float(quality.get("composite_score") or -1e9) < float(winner_quality.get("composite_score") or -1e9):
            comparative_disadvantages.append("Lower valid composite standing than the selected winner.")
        if str(candidate_record.get("benchmark_assignment", {}).get("benchmark_authority_level") or "") != str(winner_record.get("benchmark_assignment", {}).get("benchmark_authority_level") or ""):
            comparative_disadvantages.append("Weaker benchmark authority than the selected winner.")
        if str(completeness.get("readiness_level") or "") != str(dict(winner_record.get("data_completeness") or {}).get("readiness_level") or ""):
            comparative_disadvantages.append("Weaker readiness tier than the selected winner.")

    data_quality_concerns = list(dict.fromkeys(
        list(decision.get("rejection_reasons") or [])
        + [str(item.get("detail") or "") for item in pressures if str(item.get("pressure_type") or "") in {"data", "benchmark", "performance_evidence"}]
    ))[:6]
    failed_gates = [
        {
            "gate_name": str(item.get("gate_name") or ""),
            "reason": str(item.get("reason") or ""),
        }
        for item in list(dict(decision.get("policy_gates") or {}).get("failed_gates") or [])
    ]
    reconsideration = list(dict.fromkeys(
        list(quality.get("confidence_improvers") or [])
        + list(decision.get("caution_reasons") or [])
    ))[:4]
    if not reconsideration:
        reconsideration = ["Candidate would need stronger policy cleanliness, data completeness, or comparative standing before reconsideration."]

    return {
        "candidate_id": str(candidate_record.get("symbol") or ""),
        "sleeve": evaluation_context.get("sleeve_key") if isinstance(evaluation_context, dict) else candidate_record.get("sleeve_key"),
        "evaluation_mode": (evaluation_context or {}).get("evaluation_mode") if isinstance(evaluation_context, dict) else None,
        "rejection_type": rejection_type,
        "rejection_reasons": list(decision.get("rejection_reasons") or [])[:6],
        "failed_gates": failed_gates,
        "comparative_disadvantages": comparative_disadvantages[:4],
        "data_quality_concerns": data_quality_concerns,
        "what_would_need_to_change_before_reconsideration": reconsideration,
    }
