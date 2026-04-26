from __future__ import annotations

from app.v2.core.domain_objects import (
    CandidateAssessment,
    ConstraintSummary,
    FrameworkRestraint,
    PolicyBoundary,
    PortfolioTruth,
    VisibleDecisionState,
)


def _build_visible_state(action_boundary: str | None, restraints: list[FrameworkRestraint]) -> VisibleDecisionState:
    normalized_boundary = str(action_boundary or "").strip().lower()
    has_caution = any(restraint.posture in {"cautious", "constraining", "blocking"} for restraint in restraints)
    if normalized_boundary == "blocked":
        return VisibleDecisionState(
            decision_id="decision_blocked",
            state="blocked",
            allowed_action="review",
            manual_approval_required=True,
            rationale="A mandate boundary is currently blocking promotion.",
        )
    if has_caution:
        return VisibleDecisionState(
            decision_id="decision_review",
            state="review",
            allowed_action="review",
            manual_approval_required=True,
            rationale="Doctrine restraints are present, so manual review remains required.",
        )
    return VisibleDecisionState(
        decision_id="decision_monitor",
        state="eligible",
        allowed_action="monitor",
        manual_approval_required=True,
        rationale="No blocking boundary is visible in the current rubric pass.",
    )


def apply_rubric(
    assessment: CandidateAssessment,
    boundary: PolicyBoundary,
    restraints: list[FrameworkRestraint],
) -> ConstraintSummary:
    """
    Applies mandate policy boundary and framework restraints to a candidate assessment.
    Returns ConstraintSummary with annotations.
    Must NOT import from blueprint_payload_assembler.
    """
    doctrine_annotations = [
        restraint.description or restraint.rationale
        for restraint in restraints
        if restraint.constraint_type == "conviction_adjustment" and (restraint.description or restraint.rationale)
    ]

    if boundary.action_boundary == "blocked":
        overclaim_risk = "high"
    elif restraints:
        overclaim_risk = "medium"
    else:
        overclaim_risk = "low"

    blocked_actions = [boundary.action_boundary] if boundary.action_boundary == "blocked" else []
    reviewer_notes = [boundary.summary]
    if boundary.required_action:
        reviewer_notes.append(boundary.required_action)
    reviewer_notes.extend(doctrine_annotations)

    return ConstraintSummary(
        summary_id=f"constraint_{assessment.candidate_id}",
        boundaries=[boundary],
        restraints=restraints,
        blocked_actions=blocked_actions,
        reviewer_notes=reviewer_notes,
        overclaim_risk=overclaim_risk,
        doctrine_annotations=doctrine_annotations,
        visible_decision_state=_build_visible_state(boundary.action_boundary, restraints),
    )


def build_policy_boundaries(
    candidate: CandidateAssessment,
    *,
    sleeve_purpose: str,
    portfolio: PortfolioTruth | None = None,
) -> list[PolicyBoundary]:
    benchmark_authority = str(candidate.instrument.metrics.get("benchmark_authority_level") or "bounded")
    benchmark_passes = benchmark_authority in {"direct", "strong", "bounded"}
    purpose_text = " ".join(
        [
            sleeve_purpose,
            candidate.interpretation.why_it_matters_here,
            candidate.instrument.asset_class,
            candidate.instrument.name,
        ]
    ).lower()
    purpose_passes = not sleeve_purpose.strip() or sleeve_purpose.lower() in purpose_text
    current_weight = float(candidate.holdings_context.get("current_weight") or 0.0)
    turnover_passes = current_weight <= 0.0 or candidate.conviction >= 0.65

    boundaries = [
        PolicyBoundary(
            boundary_id=f"boundary_benchmark_{candidate.candidate_id}",
            code="benchmark_authority_floor",
            action_boundary=None if benchmark_passes else "blocked",
            severity="warning" if benchmark_passes else "blocking",
            passes=benchmark_passes,
            summary="Benchmark authority must stay strong enough to support bounded comparison language.",
            required_action=None if benchmark_passes else "Gather stronger benchmark support before promotion.",
        ),
        PolicyBoundary(
            boundary_id=f"boundary_purpose_{candidate.candidate_id}",
            code="sleeve_purpose_alignment",
            action_boundary=None if purpose_passes else "blocked",
            severity="info" if purpose_passes else "blocking",
            passes=purpose_passes,
            summary="Candidate interpretation must still match the sleeve's intended role.",
            required_action=None if purpose_passes else "Restate or repair the sleeve-purpose match.",
        ),
        PolicyBoundary(
            boundary_id=f"boundary_turnover_{candidate.candidate_id}",
            code="low_turnover_discipline",
            action_boundary=None if turnover_passes else "review",
            severity="warning",
            passes=turnover_passes,
            summary="Replacing a live holding should clear a higher conviction bar.",
            required_action=None if turnover_passes else "Escalate to review before displacing an existing holding.",
        ),
    ]

    if portfolio is not None and portfolio.cash_weight > 0.15:
        boundaries.append(
            PolicyBoundary(
                boundary_id=f"boundary_cash_{candidate.candidate_id}",
                code="cash_context_visibility",
                action_boundary=None,
                scope="portfolio",
                severity="info",
                passes=True,
                summary="Elevated cash should remain visible when reading adds or replacements.",
            )
        )

    return boundaries


def derive_visible_decision_state(boundaries: list[PolicyBoundary], restraints: list[FrameworkRestraint]) -> VisibleDecisionState:
    blocked_boundary = next((boundary for boundary in boundaries if boundary.action_boundary == "blocked"), None)
    return _build_visible_state(blocked_boundary.action_boundary if blocked_boundary else None, restraints)


def summarize_constraints(
    *,
    candidate: CandidateAssessment,
    sleeve_purpose: str,
    restraints: list[FrameworkRestraint],
    portfolio: PortfolioTruth | None = None,
) -> ConstraintSummary:
    boundaries = build_policy_boundaries(candidate, sleeve_purpose=sleeve_purpose, portfolio=portfolio)
    dominant_boundary = next((boundary for boundary in boundaries if boundary.action_boundary == "blocked"), boundaries[0])
    summary = apply_rubric(candidate, dominant_boundary, restraints)
    blocked_actions = [boundary.code for boundary in boundaries if not boundary.passes]
    reviewer_notes = [boundary.required_action for boundary in boundaries if boundary.required_action]
    reviewer_notes.extend(summary.doctrine_annotations)
    return summary.model_copy(
        update={
            "boundaries": boundaries,
            "blocked_actions": blocked_actions,
            "reviewer_notes": reviewer_notes or summary.reviewer_notes,
            "visible_decision_state": derive_visible_decision_state(boundaries, restraints),
        }
    )
