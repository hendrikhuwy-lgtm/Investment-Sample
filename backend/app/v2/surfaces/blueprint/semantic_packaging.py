from __future__ import annotations

from typing import Any


def source_confidence_label(source_integrity_summary: dict[str, Any] | None) -> str:
    state = str(dict(source_integrity_summary or {}).get("state") or "").strip().lower()
    if state == "strong":
        return "Source confidence strong"
    if state in {"mixed", "weak"}:
        return "Source confidence mixed"
    if state in {"conflicted", "missing"}:
        return "Source confidence weak"
    return "Source confidence bounded"


def _decision_phrase(investor_decision_state: str | None) -> str:
    normalized = str(investor_decision_state or "").strip().lower()
    if normalized == "actionable":
        return "actionable now"
    if normalized == "shortlisted":
        return "reviewable now"
    if normalized == "blocked":
        return "blocked for now"
    return "still in research scope"


def _dedupe(items: list[str], *, limit: int = 4) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        output.append(text)
        if len(output) >= limit:
            break
    return output


def build_candidate_summary_pack(
    *,
    candidate_name: str,
    investor_decision_state: str | None,
    recommendation_gate: dict[str, Any] | None,
    score_decomposition: dict[str, Any] | None,
    source_integrity_summary: dict[str, Any] | None,
    coverage_summary: dict[str, Any] | None,
    market_path_support: dict[str, Any] | None,
) -> dict[str, Any]:
    gate = dict(recommendation_gate or {})
    score = dict(score_decomposition or {})
    coverage = dict(coverage_summary or {})
    market_path = dict(market_path_support or {})
    score_components = [dict(component) for component in list(score.get("components") or [])]
    positives = [
        str(component.get("summary") or "")
        for component in score_components
        if str(component.get("tone") or "") == "good"
    ]
    negatives = [
        str(component.get("summary") or "")
        for component in score_components
        if str(component.get("tone") or "") in {"bad", "warn"}
    ]
    supporting_factors = _dedupe(
        positives
        + (
            [str(dict(coverage.get("coverage_workflow_summary") or {}).get("summary") or "")]
            if str(coverage.get("coverage_status") or "") in {"direct_ready", "proxy_ready"}
            else []
        )
        + (
            [str(market_path.get("market_path_case_note") or market_path.get("candidate_implication") or "")]
            if str(market_path.get("usefulness_label") or "") not in {"", "suppressed"}
            else []
        ),
        limit=3,
    )
    penalizing_factors = _dedupe(
        [str(reason) for reason in list(gate.get("blocked_reasons") or [])]
        + negatives
        + (
            [str(dict(coverage.get("coverage_workflow_summary") or {}).get("summary") or "")]
            if str(coverage.get("coverage_status") or "") not in {"", "direct_ready"}
            else []
        )
        + (
            [str(market_path.get("market_path_case_note") or market_path.get("candidate_implication") or "")]
            if str(market_path.get("usefulness_label") or "") in {"suppressed", "unstable", "usable_with_caution"}
            else []
        ),
        limit=4,
    )
    row_summary = (
        f"{candidate_name} is {_decision_phrase(investor_decision_state)}. "
        f"{str(gate.get('summary') or dict(source_integrity_summary or {}).get('summary') or '').strip()}"
    ).strip()
    row_summary = " ".join(part for part in row_summary.split() if part)
    strip = {
        "headline": row_summary,
        "stance": str(gate.get("summary") or dict(source_integrity_summary or {}).get("summary") or "").strip() or row_summary,
        "score_label": (
            f"Total score {int(score.get('total_score') or 0)}/100"
            if score
            else None
        ),
        "source_confidence_label": source_confidence_label(source_integrity_summary),
        "coverage_status": str(coverage.get("coverage_status") or "").strip() or None,
        "market_path_note": str(
            market_path.get("market_path_case_note")
            or market_path.get("candidate_implication")
            or ""
        ).strip() or None,
    }
    return {
        "candidate_row_summary": row_summary,
        "candidate_supporting_factors": supporting_factors,
        "candidate_penalizing_factors": penalizing_factors,
        "report_summary_strip": strip,
        "source_confidence_label": strip["source_confidence_label"],
    }


def build_market_path_summary_pack(market_path_support: dict[str, Any] | None) -> dict[str, Any]:
    support = dict(market_path_support or {})
    return {
        "market_path_objective": support.get("market_path_objective"),
        "market_path_case_note": support.get("market_path_case_note") or support.get("candidate_implication"),
    }


def build_default_route_cache_state(*, generated_at: str, max_age_seconds: int = 0) -> dict[str, Any]:
    return {
        "state": "live_build",
        "stale": False,
        "revalidating": False,
        "cached_at": generated_at,
        "max_age_seconds": max_age_seconds,
        "summary": "Contract was built directly from current backend truth.",
    }


def build_default_report_loading_hint(*, route_cache_state: str) -> dict[str, Any]:
    return {
        "strategy": "summary_then_sections",
        "summary": (
            "Render the summary strip, score rubric, and coverage workflow first, then hydrate charts, scenarios, "
            "competition, and evidence sections."
        ),
        "route_cache_state": route_cache_state,
        "staged_sections": ["report_summary_strip", "score_rubric", "coverage_workflow_summary", "charts", "evidence"],
    }
