from __future__ import annotations

from typing import Any

from app.services.framework_lenses.buffett_munger_quality import evaluate_buffett_munger_quality
from app.services.framework_lenses.dalio_regime_transmission import evaluate_dalio_regime_transmission
from app.services.framework_lenses.fragility_red_team import evaluate_fragility_red_team
from app.services.framework_lenses.implementation_reality import evaluate_implementation_reality
from app.services.framework_lenses.marks_cycle_risk import evaluate_marks_cycle_risk


def build_lens_assessment(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    gate_summary: dict[str, Any],
    source_integrity_result: dict[str, Any],
    benchmark_support_status: dict[str, Any],
    tax_assumption_status: dict[str, Any],
    forecast_defensibility_status: dict[str, Any],
    portfolio_completeness_status: dict[str, Any],
    current_holding_record: dict[str, Any],
    recommendation_result: dict[str, Any],
    portfolio_consequence_summary: dict[str, Any],
    cost_realism_summary: dict[str, Any],
) -> dict[str, Any]:
    per_lens = {
        "marks_cycle_risk": evaluate_marks_cycle_risk(
            recommendation_result=recommendation_result,
            source_integrity_result=source_integrity_result,
            forecast_defensibility_status=forecast_defensibility_status,
            benchmark_support_status=benchmark_support_status,
            portfolio_consequence_summary=portfolio_consequence_summary,
        ),
        "buffett_munger_quality": evaluate_buffett_munger_quality(
            candidate=candidate,
            sleeve_key=sleeve_key,
            current_holding_record=current_holding_record,
            tax_assumption_status=tax_assumption_status,
            gate_summary=gate_summary,
        ),
        "dalio_regime_transmission": evaluate_dalio_regime_transmission(
            sleeve_key=sleeve_key,
            portfolio_consequence_summary=portfolio_consequence_summary,
            benchmark_support_status=benchmark_support_status,
            current_holding_record=current_holding_record,
        ),
        "implementation_reality": evaluate_implementation_reality(
            gate_summary=gate_summary,
            tax_assumption_status=tax_assumption_status,
            portfolio_completeness_status=portfolio_completeness_status,
            current_holding_record=current_holding_record,
            cost_realism_summary=cost_realism_summary,
        ),
        "fragility_red_team": evaluate_fragility_red_team(
            gate_summary=gate_summary,
            source_integrity_result=source_integrity_result,
            forecast_defensibility_status=forecast_defensibility_status,
            current_holding_record=current_holding_record,
        ),
    }
    return {"per_lens": per_lens}
