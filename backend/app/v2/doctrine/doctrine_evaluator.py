from __future__ import annotations

from app.v2.core.domain_objects import CandidateAssessment, FrameworkRestraint, InstrumentTruth, InterpretationCard
from app.v2.doctrine.doctrine_principle_registry import PRINCIPLE_REGISTRY


def _constraint_type_for(principle_id: str) -> str:
    if principle_id in {"risk_before_return", "avoid_overclaiming"}:
        return "conviction_adjustment"
    if principle_id in {"cycle_temperature", "uncertainty_acknowledged"}:
        return "risk_annotation"
    return "boundary_note"


def evaluate(
    candidate_assessment: CandidateAssessment,
    active_principles: list[str] | None = None,
) -> list[FrameworkRestraint]:
    """
    Evaluates a candidate assessment against doctrine principles.
    Returns a list of FrameworkRestraint objects to be passed to mandate_rubric.
    """
    principle_ids = list(active_principles) if active_principles is not None else list(PRINCIPLE_REGISTRY.keys())
    principle_ids = [principle_id for principle_id in principle_ids if principle_id in PRINCIPLE_REGISTRY]

    if not principle_ids and candidate_assessment.candidate_id:
        principle_ids = [next(iter(PRINCIPLE_REGISTRY))]

    restraints: list[FrameworkRestraint] = []
    for principle_id in principle_ids:
        principle = PRINCIPLE_REGISTRY[principle_id]
        constraint_type = _constraint_type_for(principle_id)
        restraints.append(
            FrameworkRestraint(
                restraint_id=f"{principle_id}:{candidate_assessment.candidate_id}",
                framework_id=principle_id,
                label=principle.title,
                principle_ref=principle_id,
                constraint_type=constraint_type,
                description=principle.objective,
                posture=principle.default_posture,
                rationale=principle.objective,
                supports=list(principle.evaluation_focus),
                cautions=[],
                claim_constraints=[],
                what_changes_view=[f"More direct evidence would change the {principle.title.lower()} read."],
            )
        )

    return restraints


def evaluate_doctrine(
    card: InterpretationCard,
    *,
    evidence: object | None = None,
) -> list[FrameworkRestraint]:
    dummy_instrument = InstrumentTruth(
        instrument_id=card.entity_id,
        symbol=card.title.split(" ", 1)[0] if card.title else card.entity_id,
        name=card.title or card.entity_id,
        asset_class="unknown",
    )
    assessment = CandidateAssessment(
        candidate_id=card.entity_id or card.card_id,
        sleeve_id="sleeve_unknown",
        instrument=dummy_instrument,
        interpretation=card,
    )
    return evaluate(assessment)
