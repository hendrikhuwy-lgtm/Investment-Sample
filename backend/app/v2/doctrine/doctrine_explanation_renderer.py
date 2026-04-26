from __future__ import annotations

from app.v2.core.domain_objects import FrameworkRestraint
from app.v2.doctrine.doctrine_corpus import corpus_entries_for_principle


def render_doctrine_explanations(restraints: list[FrameworkRestraint]) -> list[str]:
    explanations: list[str] = []
    for restraint in restraints:
        corpus = corpus_entries_for_principle(restraint.framework_id)
        source_note = ""
        if corpus:
            first = corpus[0]
            source_note = f" {first['author']} frames this as: {first['guidance']}"
        explanations.append(f"{restraint.label}: {restraint.rationale}{source_note}")
    return explanations


def render_doctrine_summary(restraints: list[FrameworkRestraint]) -> str:
    active = [restraint.label for restraint in restraints if restraint.posture in {"cautious", "constraining", "blocking"}]
    if not active:
        return "Doctrine currently moderates explanation tone more than decision scope."
    return f"Doctrine is actively constraining conviction through: {', '.join(active)}."

