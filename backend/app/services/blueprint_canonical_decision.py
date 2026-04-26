from __future__ import annotations

from typing import Any

from app.services.framework_constitution import (
    ACTION_BOUNDARY_STATES,
    NON_BUYABLE_BLOCKER_CODES,
    benchmark_performance_language_allowed,
    benchmark_structural_language_allowed,
    clamp_promotion_state,
    classify_complexity,
    decisive_tax_language_allowed,
    forecast_may_influence_main_path,
    is_core_sleeve,
    manual_approval_boundary_text,
    requires_manual_approval,
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _first_text(*values: Any) -> str:
    for value in values:
        if isinstance(value, list):
            for item in value:
                text = _text(item)
                if text:
                    return text
            continue
        text = _text(value)
        if text:
            return text
    return ""


def _with_period(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    return text if text.endswith((".", "!", "?")) else f"{text}."


def _bucket(snapshot: dict[str, Any], name: str) -> dict[str, Any]:
    return dict(snapshot.get(name) or {})


def build_canonical_gate_completeness_result(
    *,
    sleeve_key: str,
    gate_result: dict[str, Any],
    decision_completeness_status: dict[str, Any],
    portfolio_completeness_status: dict[str, Any],
    benchmark_support_status: dict[str, Any],
    tax_assumption_status: dict[str, Any],
    forecast_defensibility_status: dict[str, Any],
    source_integrity_result: dict[str, Any],
    evidence_pack: dict[str, Any],
    current_holding_record: dict[str, Any],
) -> dict[str, Any]:
    bucket_states = dict(decision_completeness_status.get("bucket_states") or {})
    support_class = str(benchmark_support_status.get("support_class") or "unavailable")
    tax_grade = str(tax_assumption_status.get("assumption_completeness_grade") or "INCOMPLETE")
    tax_confidence = str(tax_assumption_status.get("tax_confidence") or "low")
    holdings_bucket = str(bucket_states.get("holdings_exposure") or "missing")
    liquidity_bucket = str(bucket_states.get("liquidity_and_aum") or "missing")
    decision_grade = str(decision_completeness_status.get("data_completeness_grade") or "INCOMPLETE")
    portfolio_grade = str(portfolio_completeness_status.get("completeness_grade") or "INCOMPLETE")
    practical_edge = dict(current_holding_record.get("practical_edge") or {})
    complexity = classify_complexity(evidence_pack.get("candidate_record") or {}, sleeve_key)
    blockers: list[dict[str, Any]] = []
    limits: list[str] = []

    if is_core_sleeve(sleeve_key) and holdings_bucket in {"missing", "partial", "proxy_only"}:
        blockers.append(
            {
                "code": "core_holdings_incomplete",
                "reason": "Core-sleeve holdings truth is incomplete, so buyable promotion is blocked.",
            }
        )
    elif holdings_bucket in {"missing", "proxy_only"}:
        limits.append("Holdings support remains limited, so structural conclusions stay bounded.")

    if portfolio_grade != "SUFFICIENT":
        blockers.append(
            {
                "code": "current_holding_comparison_incomplete",
                "reason": "Current-holding comparison is incomplete, so no switch recommendation should be promoted.",
            }
        )

    if not bool(decision_completeness_status.get("runner_up_comparison_complete")):
        blockers.append(
            {
                "code": "nearest_rival_comparison_weak",
                "reason": "Nearest rival comparison remains too weak for a strong lead claim.",
            }
        )

    if not decisive_tax_language_allowed(tax_grade, tax_confidence):
        blockers.append(
            {
                "code": "significant_tax_uncertainty",
                "reason": "Tax authority is not strong enough for buyable promotion or decisive tax-edge language.",
            }
        )

    if support_class in {"weak_proxy", "unavailable"}:
        blockers.append(
            {
                "code": "benchmark_authority_too_weak",
                "reason": "Benchmark authority is too weak for strong performance-led promotion.",
            }
        )

    if liquidity_bucket in {"missing", "proxy_only"} or not bool(portfolio_completeness_status.get("switch_cost_estimated")):
        blockers.append(
            {
                "code": "implementation_friction_unclear",
                "reason": "Implementation friction or liquidity evidence is still too weak for buyable promotion.",
            }
        )

    if not complexity.get("simply_explainable"):
        blockers.append(
            {
                "code": "structure_too_complex",
                "reason": "Structure is not simple enough for clean promotion under the investor mandate.",
            }
        )

    if complexity.get("violates_mandate"):
        blockers.append(
            {
                "code": "mandate_complexity_violation",
                "reason": "Leverage or complex construction violates the governing mandate.",
            }
        )

    if str(current_holding_record.get("status") or "") == "unavailable" or str(practical_edge.get("status") or "") == "unavailable":
        blockers.append(
            {
                "code": "switch_cost_unclear",
                "reason": "Switch cost and practical edge are not cleanly known.",
            }
        )

    if not forecast_may_influence_main_path(str(forecast_defensibility_status.get("display_grade") or "HIDE")):
        limits.append("Forecast remains secondary and cannot upgrade the case.")

    if list(decision_completeness_status.get("critical_gaps") or []):
        limits.extend([str(item) for item in list(decision_completeness_status.get("critical_gaps") or [])[:4]])
    if list(source_integrity_result.get("material_issues") or []):
        limits.append("Source integrity still carries material caution.")

    blockers = [item for item in blockers if item.get("code") in NON_BUYABLE_BLOCKER_CODES]
    return {
        "gate_overall_status": str(gate_result.get("overall_status") or "not_evaluated"),
        "decision_completeness_grade": decision_grade,
        "portfolio_completeness_grade": portfolio_grade,
        "benchmark_support_class": support_class,
        "tax_assumption_grade": tax_grade,
        "tax_confidence": tax_confidence,
        "holdings_bucket_state": holdings_bucket,
        "liquidity_bucket_state": liquidity_bucket,
        "failed_blockers": blockers,
        "failed_blocker_codes": [str(item.get("code") or "") for item in blockers],
        "unresolved_limits": list(dict.fromkeys([item for item in limits if item]))[:10],
        "buyable_blocked": bool(blockers),
    }


def build_base_promotion_state(
    *,
    candidate: dict[str, Any],
    gate_summary: dict[str, Any],
    recommendation_result: dict[str, Any],
    investor_recommendation_status: dict[str, Any],
    current_holding_record: dict[str, Any],
) -> str:
    readiness = str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "research_visible")
    user_state = str(recommendation_result.get("candidate_status") or "")
    decision_type = str(recommendation_result.get("decision_type") or "RESEARCH")
    current_status = str(current_holding_record.get("status") or "")
    practical_edge_status = str(dict(current_holding_record.get("practical_edge") or {}).get("status") or "unavailable")

    if user_state in {"blocked_by_policy", "blocked_by_missing_required_evidence", "blocked_by_unresolved_gate"}:
        return "research_only"
    if readiness == "research_visible":
        return "research_only"
    if gate_summary.get("buyable_blocked"):
        if readiness in {"review_ready", "shortlist_ready", "recommendation_ready"}:
            return "acceptable"
        return "research_only"
    if readiness in {"review_ready", "shortlist_ready"}:
        return "near_decision_ready"
    if readiness == "recommendation_ready":
        if decision_type in {"ADD", "REPLACE"} and current_status == "different_from_current" and practical_edge_status == "sufficient":
            return "buyable"
        if current_status == "matched_to_current":
            return "buyable"
        return "near_decision_ready"
    if str(investor_recommendation_status.get("investor_status") or "") == "WATCHLIST_CANDIDATE":
        return "acceptable"
    return "acceptable"


def _action_boundary(
    *,
    promotion_state: str,
    recommendation_result: dict[str, Any],
    gate_summary: dict[str, Any],
    current_holding_record: dict[str, Any],
) -> dict[str, Any]:
    decision_type = str(recommendation_result.get("decision_type") or "RESEARCH")
    no_change = bool(recommendation_result.get("no_change_is_best")) or decision_type == "HOLD"
    manual_required = requires_manual_approval(action_type=decision_type, is_rebalance=False)
    blockers = [str(item.get("reason") or "") for item in list(gate_summary.get("failed_blockers") or [])]
    current_symbol = _text(current_holding_record.get("current_symbol"))
    if no_change and current_symbol:
        state = "no_change"
        do_now = f"Keep the current holding {current_symbol}. No change is the preferred conclusion for now."
        do_not = "Do not switch just because an alternate candidate looks interesting on rank."
    elif blockers:
        state = "blocked"
        do_now = "Do not change the portfolio. Keep this in research or secondary review only."
        do_not = "Do not treat this as switch-ready or allocation-ready."
    elif promotion_state == "buyable" and manual_required:
        state = "manual_review_required"
        do_now = "Escalate this as a manual approval case before any non-rebalance portfolio change."
        do_not = "Do not trade or switch automatically."
    elif promotion_state == "near_decision_ready":
        state = "compare_in_main_path"
        do_now = "Keep this in the main sleeve comparison path and test it directly against the incumbent and nearest rival."
        do_not = "Do not treat this as buyable yet."
    elif promotion_state == "acceptable":
        state = "secondary_review_only"
        do_now = "Keep this in secondary review only."
        do_not = "Do not elevate it into the main decision path yet."
    else:
        state = "monitor_only"
        do_now = "Keep this out of the main decision path and monitor only if needed."
        do_not = "Do not present this as an implementation candidate."
    assert state in ACTION_BOUNDARY_STATES
    return {
        "state": state,
        "decision_type": decision_type,
        "manual_approval_required": manual_required,
        "do_now": do_now,
        "do_not_do_now": do_not,
        "why_boundary_exists": _first_text(
            list(gate_summary.get("unresolved_limits") or []),
            list(gate_summary.get("failed_blockers") or []),
            recommendation_result.get("why_beats_runner_up"),
            recommendation_result.get("why_this_candidate"),
        ),
        "manual_approval_note": manual_approval_boundary_text(decision_type) if manual_required else "",
    }


def _framework_judgment(
    *,
    lens_assessment: dict[str, Any] | None,
    lens_fusion_result: dict[str, Any] | None,
    action_boundary: dict[str, Any],
    promotion_state: str,
) -> dict[str, Any]:
    fusion = dict(lens_fusion_result or {})
    if not lens_assessment:
        return {
            "summary": _with_period(
                f"Framework posture remains {promotion_state.replace('_', ' ')} with no additional lens constraints surfaced."
            ),
            "overall_lens_posture": "mixed_but_constructive",
            "dominant_supports": [],
            "dominant_cautions": [],
            "action_tone_constraint": "none",
        }
    supports = list(fusion.get("dominant_supports") or [])
    cautions = list(fusion.get("dominant_cautions") or [])
    posture = _text(fusion.get("overall_lens_posture") or "mixed_but_constructive").replace("_", " ")
    summary = (
        f"Framework lenses leave the case at {promotion_state.replace('_', ' ')} with a {posture} posture."
        if not cautions
        else f"Framework lenses keep the case at {promotion_state.replace('_', ' ')} because caution still dominates the action boundary."
    )
    return {
        "summary": _with_period(summary),
        "overall_lens_posture": fusion.get("overall_lens_posture"),
        "dominant_supports": supports[:4],
        "dominant_cautions": cautions[:4],
        "action_tone_constraint": fusion.get("action_tone_constraint") or "none",
        "action_boundary_state": action_boundary.get("state"),
    }


def _evidence_summary(
    *,
    evidence_pack: dict[str, Any],
    source_integrity_result: dict[str, Any],
    gate_summary: dict[str, Any],
) -> dict[str, Any]:
    bucket_support = dict(evidence_pack.get("bucket_support") or {})
    strongest_support: list[str] = []
    strongest_limits: list[str] = []
    appendix: list[dict[str, Any]] = []
    for bucket_name, raw in bucket_support.items():
        bucket = dict(raw or {})
        interp = dict(bucket.get("interpretation_summary") or {})
        supports = _text(interp.get("supports"))
        limits = _text(interp.get("does_not_support"))
        if supports:
            strongest_support.append(f"{bucket_name.replace('_', ' ')}: {supports}")
        if limits:
            strongest_limits.append(f"{bucket_name.replace('_', ' ')}: {limits}")
        appendix.append(
            {
                "bucket_name": bucket_name,
                "bucket_state": bucket.get("bucket_state"),
                "source_url": bucket.get("primary_source_url"),
                "source_name": bucket.get("primary_source_name"),
                "source_kind": bucket.get("primary_source_kind"),
                "observed_at": bucket.get("observed_at"),
                "retrieved_at": bucket.get("retrieved_at"),
                "extraction_methods": list(bucket.get("extraction_methods") or []),
                "supported_fields": list(bucket.get("supported_fields") or []),
                "missing_fields": list(bucket.get("missing_fields") or []),
                "failure_reasons": list(bucket.get("failure_reasons") or []),
                "claim_limits": list(bucket.get("claim_limits") or []),
                "supports": supports,
                "does_not_support": limits,
            }
        )
    if not strongest_limits:
        strongest_limits = [str(item) for item in list(gate_summary.get("unresolved_limits") or [])[:3]]
    return {
        "evidence_depth_class": evidence_pack.get("evidence_depth_class"),
        "confidence": source_integrity_result.get("overall_caution_level"),
        "support_depth": source_integrity_result.get("overall_source_status"),
        "strongest_support_points": strongest_support[:4],
        "strongest_limiting_points": strongest_limits[:4],
        "bucket_support_appendix": appendix,
    }


def build_canonical_report_sections(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    gate_summary: dict[str, Any],
    recommendation_result: dict[str, Any],
    evidence_summary: dict[str, Any],
    benchmark_support_status: dict[str, Any],
    tax_assumption_status: dict[str, Any],
    portfolio_completeness_status: dict[str, Any],
    current_holding_record: dict[str, Any],
    portfolio_consequence_summary: dict[str, Any],
    decision_change_set: dict[str, Any],
    action_boundary: dict[str, Any],
    promotion_state: str,
    lens_assessment: dict[str, Any] | None = None,
    lens_fusion_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    investment_quality = dict(candidate.get("investment_quality") or {})
    role = _first_text(
        investment_quality.get("role_in_portfolio"),
        dict(candidate.get("sleeve_expression") or {}).get("summary"),
        candidate.get("rationale"),
    )
    supports = list(evidence_summary.get("strongest_support_points") or [])
    limits = list(evidence_summary.get("strongest_limiting_points") or [])
    benchmark_class = _text(benchmark_support_status.get("support_class") or "unavailable")
    tax_confidence = _text(tax_assumption_status.get("tax_confidence") or "low")
    current_symbol = _text(current_holding_record.get("current_symbol"))
    switch = dict(current_holding_record.get("switching_friction") or {})
    practical_edge = dict(current_holding_record.get("practical_edge") or {})
    fusion = dict(lens_fusion_result or {})
    per_lens = dict(dict(lens_assessment or {}).get("per_lens") or {})
    dominant_supports = [str(item) for item in list(fusion.get("dominant_supports") or [])[:2]]
    dominant_cautions = [str(item) for item in list(fusion.get("dominant_cautions") or [])[:2]]

    problem = (
        "The system is testing whether anything should change in this sleeve at all."
        if current_symbol
        else "The system is testing whether this sleeve has a credible implementation candidate."
    )
    attractive = _first_text(
        list(investment_quality.get("key_advantages") or []),
        dominant_supports,
        recommendation_result.get("why_beats_runner_up"),
        recommendation_result.get("why_this_candidate"),
    )
    benefit = _first_text(
        dict(portfolio_consequence_summary.get("portfolio_effect") or {}).get("summary"),
        portfolio_consequence_summary.get("summary"),
        dict(candidate.get("investor_consequence_summary") or {}).get("implementation_quality_effect"),
    )
    main_tradeoff = _first_text(
        list(investment_quality.get("main_limitations") or []),
        list(investment_quality.get("key_risks") or []),
        dominant_cautions,
        limits,
    )
    current_view = (
        f"Promotion state is {promotion_state.replace('_', ' ')} because {action_boundary.get('why_boundary_exists') or 'the evidence and portfolio comparison are still being weighed'}."
    )
    not_now = action_boundary.get("do_not_do_now") or "Do not overread the current evidence."
    what_changes = _first_text(
        list(decision_change_set.get("invalidation_conditions") or []),
        list(dict(candidate.get("upgrade_path") or {}).get("missing_requirements") or []),
        list(dict(candidate.get("decision_readiness") or {}).get("what_must_change") or []),
    )
    benchmark_text = (
        "Benchmark authority is strong enough to support relative-performance language."
        if benchmark_performance_language_allowed(benchmark_class)
        else "Benchmark authority is not strong enough for strong performance language, so any historical comparison must stay cautious."
        if benchmark_structural_language_allowed(benchmark_class)
        else "Benchmark authority is too weak for anything beyond bounded structural comparison."
    )
    tax_text = (
        "Tax authority is strong enough to treat tax cleanliness as a real edge."
        if decisive_tax_language_allowed(
            str(tax_assumption_status.get("assumption_completeness_grade") or "INCOMPLETE"),
            tax_confidence,
        )
        else "Tax authority remains conditional, so tax should not be treated as a decisive edge."
    )
    portfolio_compare = (
        f"Current holding comparison is against {current_symbol}, with practical edge status {str(practical_edge.get('status') or 'unavailable').replace('_', ' ')}."
        if current_symbol
        else "No named current holding is available, so change language must stay constrained."
    )
    friction_text = _first_text(
        switch.get("summary"),
        practical_edge.get("reason"),
        "Switching friction is not yet fully clear.",
    )
    if dominant_cautions and str(fusion.get("action_tone_constraint") or "") in {"restrained", "monitoring_only"}:
        current_view = f"{current_view.rstrip('.')} Framework caution remains active, so action language stays restrained."

    lens_supports = []
    lens_cautions = []
    for lens_id, raw in per_lens.items():
        lens = dict(raw or {})
        label = lens_id.replace("_", " ")
        support = _first_text(lens.get("supports"), lens.get("investor_summary"))
        caution = _first_text(lens.get("cautions"), lens.get("blocker_flags"))
        if support:
            lens_supports.append(f"{label}: {support}")
        if caution:
            lens_cautions.append(f"{label}: {caution}")

    return {
        "problem_or_opportunity": _with_period(problem),
        "what_this_is": _with_period(role),
        "why_attractive": _with_period(attractive),
        "potential_benefit": _with_period(benefit),
        "evidence_support": [_with_period(item) for item in supports[:4]],
        "evidence_limits": [_with_period(item) for item in limits[:4]],
        "benchmark_authority": _with_period(benchmark_text),
        "tax_authority": _with_period(tax_text),
        "current_holding_comparison": _with_period(portfolio_compare),
        "switch_cost_or_friction": _with_period(friction_text),
        "main_tradeoff": _with_period(main_tradeoff),
        "current_view": _with_period(current_view),
        "what_to_do_now": _with_period(action_boundary.get("do_now")),
        "what_not_to_do_now": _with_period(not_now),
        "what_would_change_the_view": _with_period(what_changes or "Stronger direct evidence and a cleaner practical edge would make the conclusion more decisive."),
        "framework_judgment": _with_period(
            _first_text(
                list(dominant_cautions or []),
                list(dominant_supports or []),
                "Framework lenses are informative but remain subordinate to the base decision stack.",
            )
        ),
        "lens_supports": [_with_period(item) for item in lens_supports[:5]],
        "lens_cautions": [_with_period(item) for item in lens_cautions[:5]],
    }


def build_canonical_decision_object(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    evidence_pack: dict[str, Any],
    source_integrity_result: dict[str, Any],
    gate_result: dict[str, Any],
    decision_completeness_status: dict[str, Any],
    portfolio_completeness_status: dict[str, Any],
    benchmark_support_status: dict[str, Any],
    tax_assumption_status: dict[str, Any],
    forecast_defensibility_status: dict[str, Any],
    current_holding_record: dict[str, Any],
    recommendation_result: dict[str, Any],
    investor_recommendation_status: dict[str, Any],
    portfolio_consequence_summary: dict[str, Any],
    decision_change_set: dict[str, Any],
    gate_summary: dict[str, Any] | None = None,
    base_promotion_state: str | None = None,
    lens_assessment: dict[str, Any] | None = None,
    lens_fusion_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    gate_summary = dict(
        gate_summary
        or build_canonical_gate_completeness_result(
            sleeve_key=sleeve_key,
            gate_result=gate_result,
            decision_completeness_status=decision_completeness_status,
            portfolio_completeness_status=portfolio_completeness_status,
            benchmark_support_status=benchmark_support_status,
            tax_assumption_status=tax_assumption_status,
            forecast_defensibility_status=forecast_defensibility_status,
            source_integrity_result=source_integrity_result,
            evidence_pack=evidence_pack,
            current_holding_record=current_holding_record,
        )
    )
    base_promotion_state = base_promotion_state or build_base_promotion_state(
        candidate=candidate,
        gate_summary=gate_summary,
        recommendation_result=recommendation_result,
        investor_recommendation_status=investor_recommendation_status,
        current_holding_record=current_holding_record,
    )
    promotion_state = clamp_promotion_state(base_promotion_state, str(dict(lens_fusion_result or {}).get("promotion_cap") or "none"))
    action_boundary = _action_boundary(
        promotion_state=promotion_state,
        recommendation_result=recommendation_result,
        gate_summary=gate_summary,
        current_holding_record=current_holding_record,
    )
    evidence_summary = _evidence_summary(
        evidence_pack=evidence_pack,
        source_integrity_result=source_integrity_result,
        gate_summary=gate_summary,
    )
    report_sections = build_canonical_report_sections(
        candidate=candidate,
        sleeve_key=sleeve_key,
        gate_summary=gate_summary,
        recommendation_result=recommendation_result,
        evidence_summary=evidence_summary,
        benchmark_support_status=benchmark_support_status,
        tax_assumption_status=tax_assumption_status,
        portfolio_completeness_status=portfolio_completeness_status,
        current_holding_record=current_holding_record,
        portfolio_consequence_summary=portfolio_consequence_summary,
        decision_change_set=decision_change_set,
        action_boundary=action_boundary,
        promotion_state=promotion_state,
        lens_assessment=lens_assessment,
        lens_fusion_result=lens_fusion_result,
    )
    framework_judgment = _framework_judgment(
        lens_assessment=lens_assessment,
        lens_fusion_result=lens_fusion_result,
        action_boundary=action_boundary,
        promotion_state=promotion_state,
    )
    return {
        "candidate_symbol": candidate.get("symbol"),
        "candidate_name": candidate.get("name"),
        "sleeve_key": sleeve_key,
        "sleeve_role": _first_text(
            dict(candidate.get("sleeve_expression") or {}).get("summary"),
            dict(candidate.get("investment_quality") or {}).get("role_in_portfolio"),
        ),
        "readiness_state": str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "research_visible"),
        "promotion_state": promotion_state,
        "base_promotion_state": base_promotion_state,
        "eligibility_and_blockers": gate_summary,
        "evidence_summary": evidence_summary,
        "benchmark_authority": {
            "support_class": benchmark_support_status.get("support_class"),
            "authority_label": benchmark_support_status.get("authority_label"),
            "comparative_claim_boundary": benchmark_support_status.get("comparative_claim_boundary"),
            "performance_claims_allowed": benchmark_support_status.get("performance_claims_allowed"),
        },
        "tax_authority": {
            "assumption_grade": tax_assumption_status.get("assumption_completeness_grade"),
            "tax_confidence": tax_assumption_status.get("tax_confidence"),
            "advisory_boundary": tax_assumption_status.get("advisory_boundary"),
            "decisive_tax_use_allowed": tax_assumption_status.get("decisive_tax_use_allowed"),
        },
        "score_validity": {
            "valid": bool(dict(candidate.get("investment_quality") or {}).get("composite_score_valid", True)),
            "score": dict(candidate.get("investment_quality") or {}).get("composite_score"),
        },
        "portfolio_context": {
            "portfolio_completeness_grade": portfolio_completeness_status.get("completeness_grade"),
            "current_holding_record": current_holding_record,
            "portfolio_consequence_summary": portfolio_consequence_summary,
        },
        "incumbent_comparison_result": {
            "current_symbol": current_holding_record.get("current_symbol"),
            "practical_edge": current_holding_record.get("practical_edge"),
            "switching_friction": current_holding_record.get("switching_friction"),
            "no_change_is_best": recommendation_result.get("no_change_is_best"),
        },
        "recommendation_state": {
            "candidate_status": recommendation_result.get("candidate_status"),
            "decision_type": recommendation_result.get("decision_type"),
            "recommendation_tier": recommendation_result.get("recommendation_tier"),
            "current_holding_should_be_kept": recommendation_result.get("current_holding_should_be_kept"),
        },
        "action_boundary": action_boundary,
        "lens_assessment": lens_assessment or {"per_lens": {}},
        "lens_fusion_result": lens_fusion_result or {},
        "framework_judgment": framework_judgment,
        "report_sections": report_sections,
        "what_changes_the_view": {
            "summary": report_sections.get("what_would_change_the_view"),
            "conditions": list(dict(decision_change_set or {}).get("invalidation_conditions") or []),
        },
        "plain_english_summary": _first_text(
            report_sections.get("current_view"),
            report_sections.get("what_to_do_now"),
        ),
    }
