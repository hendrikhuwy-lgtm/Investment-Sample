from __future__ import annotations

from typing import Any


def _text(value: Any) -> str | None:
    raw = str(value or "").strip()
    return raw or None


def _join_sentences(*parts: str | None) -> str:
    rows = [str(part).strip().rstrip(".") for part in parts if str(part or "").strip()]
    if not rows:
        return ""
    return ". ".join(rows) + "."


def _job_phrase(sleeve_key: str, sleeve_purpose: str) -> str:
    return {
        "global_equity_core": "anchor broad global equity exposure",
        "developed_ex_us_optional": "add developed-market breadth outside the US",
        "emerging_markets": "add emerging-market growth exposure",
        "china_satellite": "express a dedicated China view",
        "ig_bonds": "supply safer bond exposure",
        "cash_bills": "hold liquid capital with minimal risk",
        "real_assets": "add inflation-sensitive protection",
        "alternatives": "add a diversifier beyond the core stock-bond book",
        "convex": "provide downside protection when risk breaks sharply",
    }.get(sleeve_key, f"serve the {sleeve_purpose.lower()} role")


def _integrity_phrase(source_integrity_summary: dict[str, Any]) -> str:
    label = str(source_integrity_summary.get("integrity_label") or source_integrity_summary.get("state") or "mixed")
    return {
        "clean": "the evidence base is clean",
        "mixed": "the evidence base is usable but still mixed",
        "thin": "the evidence base is still thin",
        "weak": "the evidence base is still weak",
        "conflicted": "the evidence base still has conflicts",
        "missing": "critical evidence is still missing",
    }.get(label, "the evidence base is still mixed")


def _blocker_phrase(blocker_category: str | None, blocked_reasons: list[str]) -> str:
    category = str(blocker_category or "").strip()
    if category == "identity":
        return "identity support is still not strong enough for action"
    if category == "benchmark":
        return "benchmark identity is still too weak for a clean sleeve decision"
    if category == "implementation":
        return "implementation facts are still not clean enough for action"
    if category == "tax_wrapper":
        return "wrapper or tax facts still block action"
    if category == "evidence":
        return "evidence quality still needs to improve"
    if category == "integrity":
        return "source conflicts still need reconciliation"
    first_reason = _text(blocked_reasons[0] if blocked_reasons else None)
    if first_reason:
        return first_reason
    return "the case still needs more support before action"


def build_candidate_explorer_explanations(
    *,
    candidate_name: str,
    sleeve_key: str,
    sleeve_purpose: str,
    is_lead_candidate: bool,
    investor_decision_state: str,
    blocker_category: str | None,
    recommendation_gate: dict[str, Any],
    failure_class_summary: dict[str, Any] | None,
    implementation_profile: dict[str, Any],
    institutional_facts: dict[str, Any],
    source_integrity_summary: dict[str, Any],
    identity_state: dict[str, Any],
    reconciliation: dict[str, Any],
    overlay_summary: str | None,
) -> dict[str, str]:
    benchmark_full_name = _text(institutional_facts.get("benchmark_full_name"))
    blocked_reasons = [str(reason or "").strip() for reason in list(recommendation_gate.get("blocked_reasons") or []) if str(reason or "").strip()]
    primary_failure_summary = _text(dict(failure_class_summary or {}).get("summary"))
    job_phrase = _job_phrase(sleeve_key, sleeve_purpose)
    integrity_phrase = _integrity_phrase(source_integrity_summary)
    implementation_summary = _text(implementation_profile.get("summary"))
    identity_summary = _text(identity_state.get("summary"))

    if is_lead_candidate:
        why_now = _join_sentences(
            f"{candidate_name} is the current lead because it best matches the sleeve job to {job_phrase}",
            implementation_summary,
            f"Right now {integrity_phrase}",
        )
        winner_reason = _join_sentences(
            "It stays in front because the sleeve fit, implementation, and evidence stack are still cleaner than the alternatives",
            benchmark_full_name and f"The current benchmark anchor is {benchmark_full_name}",
        )
        loser_reason = "A challenger would need cleaner evidence or a clearer implementation edge to replace the lead."
    else:
        why_now = _join_sentences(
            f"{candidate_name} stays in scope because it can still {job_phrase}, but it is not yet the cleanest sleeve implementation",
            implementation_summary,
            f"Right now {integrity_phrase}",
        )
        winner_reason = "It stays relevant if the sleeve still needs a different implementation path or a different exposure mix."
        loser_reason = "It trails the lead because the current sleeve case is still cleaner elsewhere."

    what_blocks_action = primary_failure_summary or _join_sentences(
        _blocker_phrase(blocker_category, blocked_reasons),
        identity_summary if str(identity_state.get("state") or "") in {"thin", "review", "conflict", "missing"} else None,
        _text(reconciliation.get("summary")) if str(reconciliation.get("status") or "") in {"soft_drift", "hard_conflict"} else None,
    )

    if investor_decision_state == "actionable":
        action_boundary = (
            "This can move into sleeve review now."
            if not overlay_summary
            else "This can move into sleeve review now, but the final funding choice still depends on the live book."
        )
    elif investor_decision_state == "shortlisted":
        action_boundary = "Keep it in the shortlist, but do not move capital until the remaining blocker clears."
    elif investor_decision_state == "blocked":
        action_boundary = "Do not move capital until the blocked fields are resolved and the sleeve case is clean again."
    else:
        action_boundary = "Keep it in research only until the sleeve case becomes materially clearer."

    what_changes_view = {
        "identity": "The view improves if the fund identity resolves cleanly across name and ISIN support.",
        "benchmark": "The view improves if benchmark identity and benchmark support become explicit and clean.",
        "implementation": "The view improves if cost, liquidity, wrapper, and benchmark implementation facts tighten up.",
        "tax_wrapper": "The view improves if wrapper, domicile, and tax support become explicit enough for the sleeve.",
        "evidence": "The view improves if more recommendation-critical fields move onto stronger and cleaner sources.",
        "integrity": "The view improves if the current source conflicts reconcile cleanly.",
    }.get(str(blocker_category or "").strip(), "The view improves if the candidate keeps its sleeve fit and the remaining weak evidence clears.")
    if investor_decision_state == "actionable":
        what_changes_view = "The view weakens if a cleaner substitute appears or the current implementation edge starts to slip."

    implication_summary = _join_sentences(
        f"The sleeve question is whether {candidate_name} is the cleanest way to {job_phrase}",
        recommendation_gate.get("summary"),
        f"Right now {integrity_phrase}",
    )
    return {
        "why_now": why_now,
        "what_blocks_action": what_blocks_action,
        "what_changes_view": what_changes_view,
        "action_boundary": action_boundary,
        "implication_summary": implication_summary,
        "winner_reason": winner_reason,
        "loser_reason": loser_reason,
    }


def build_candidate_report_explanations(
    *,
    candidate_name: str,
    sleeve_key: str,
    sleeve_purpose: str,
    investor_decision_state: str,
    blocker_category: str | None,
    institutional_facts: dict[str, Any],
    recommendation_gate: dict[str, Any],
    failure_class_summary: dict[str, Any] | None,
    source_integrity_summary: dict[str, Any],
    implementation_profile: dict[str, Any],
    identity_state: dict[str, Any],
    overlay_context: dict[str, Any] | None,
    visible_rationale: str | None,
    what_changes_view: str | None,
) -> dict[str, Any]:
    job_phrase = _job_phrase(sleeve_key, sleeve_purpose)
    benchmark_full_name = _text(institutional_facts.get("benchmark_full_name"))
    exposure_summary = _text(institutional_facts.get("exposure_summary"))
    integrity_phrase = _integrity_phrase(source_integrity_summary)
    primary_failure_summary = _text(dict(failure_class_summary or {}).get("summary"))
    implementation_summary = _text(implementation_profile.get("summary"))
    overlay_summary = _text((overlay_context or {}).get("summary"))

    investment_case = _join_sentences(
        f"{candidate_name} is being judged on whether it can {job_phrase}",
        benchmark_full_name and f"It is being treated as a {benchmark_full_name} implementation",
        exposure_summary,
        implementation_summary,
    )
    current_implication = _join_sentences(
        visible_rationale,
        primary_failure_summary if primary_failure_summary != visible_rationale else None,
        recommendation_gate.get("summary") if recommendation_gate.get("summary") != primary_failure_summary else None,
        overlay_summary,
    )
    change_line = _join_sentences(
        what_changes_view,
        {
            "identity": "Identity support is the first thing that still has to improve.",
            "benchmark": "Benchmark support is the first thing that still has to improve.",
            "implementation": "Implementation quality is the first thing that still has to improve.",
            "tax_wrapper": "Wrapper facts are the first thing that still has to improve.",
            "evidence": "Evidence quality is the first thing that still has to improve.",
            "integrity": "Conflict resolution is the first thing that still has to improve.",
        }.get(str(blocker_category or "").strip(), None),
    )

    tradeoffs = [
        _join_sentences(f"It still has to prove that it is the cleanest way to {job_phrase}"),
        primary_failure_summary or _join_sentences(f"Right now {integrity_phrase}"),
        implementation_summary,
    ]
    if investor_decision_state == "actionable":
        tradeoffs.append("The remaining question is replacement and funding discipline, not whether the sleeve job exists.")
    return {
        "investment_case": investment_case,
        "current_implication": current_implication,
        "what_changes_view": change_line,
        "main_tradeoffs": [item for item in tradeoffs if item],
    }


def build_compare_explanations(
    *,
    substitution_verdict: str,
    sleeve_name: str | None,
    leader_name: str | None,
    leader_symbol: str | None,
    leader_reason: str | None,
    blocked_candidates: list[str],
    discriminating_labels: list[str],
) -> dict[str, str]:
    sleeve_text = str(sleeve_name or "the sleeve").strip()
    leader_text = str(leader_name or leader_symbol or "the current leader").strip()
    if substitution_verdict == "direct_substitutes":
        substitution_rationale = f"These candidates are close substitutes inside {sleeve_text}; the main question is which one delivers the cleaner implementation and evidence stack."
    elif substitution_verdict == "partial_substitutes":
        substitution_rationale = f"These candidates overlap inside {sleeve_text}, but they are not perfect replacements because the exposure or implementation tradeoff still changes the sleeve read."
    elif substitution_verdict == "different_jobs":
        substitution_rationale = f"These candidates sit in the same workflow, but they do different jobs inside {sleeve_text} and should not be treated as like-for-like substitutes."
    else:
        substitution_rationale = f"The compare is still bounded because the current candidate set does not yet support a clean substitute judgment inside {sleeve_text}."

    why_leads = _join_sentences(
        leader_reason or f"{leader_text} leads because the current recommendation stack is cleaner.",
        discriminating_labels and f"The main separating dimensions are {', '.join(discriminating_labels[:3])}",
        blocked_candidates and f"Blocked candidates still need to clear {', '.join(blocked_candidates[:2])}",
    )
    what_changes = (
        "The comparison changes if the trailing candidate clears its blocker and closes the implementation or evidence gap."
        if blocked_candidates or discriminating_labels
        else "The comparison changes if the current leader loses its implementation or evidence edge."
    )
    return {
        "substitution_rationale": substitution_rationale,
        "why_leads": why_leads,
        "what_would_change": what_changes,
    }
