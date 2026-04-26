from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from app.config import get_db_path
from app.services.blueprint_benchmark_registry import DEFAULT_SLEEVE_ASSIGNMENTS, DEFAULT_BENCHMARK_ASSIGNMENTS
from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, export_live_candidate_registry, seed_default_candidate_registry
from app.v2.core.domain_objects import CandidateAssessment, MarketSeriesTruth, PolicyBoundary, utc_now_iso
from app.v2.contracts.chart_contracts import (
    chart_marker,
    comparison_chart_panel,
)
from app.v2.blueprint_market import (
    build_candidate_market_path_support,
    compact_forecast_support_from_market_path,
)
from app.v2.blueprint_market.coverage import build_candidate_coverage_summary
from app.v2.blueprint_market.series_store import latest_forecast_artifact
from app.v2.core.holdings_overlay import apply_overlay
from app.v2.core.interpretation_engine import interpret
from app.v2.core.mandate_rubric import apply_rubric, build_policy_boundaries
from app.v2.donors.benchmark_truth import get_benchmark_truth
from app.v2.donors.instrument_truth import get_instrument_truth
from app.v2.donors.portfolio_truth import get_portfolio_truth
from app.v2.doctrine.doctrine_evaluator import evaluate
from app.v2.features.research_support import build_research_support_pack
from app.v2.storage.surface_snapshot_store import get_surface_snapshot, latest_surface_snapshot, previous_surface_snapshot, record_surface_snapshot
from app.v2.surfaces.blueprint.explanation_builders import build_candidate_report_explanations
from app.v2.surfaces.blueprint.index_scope_registry import resolve_index_scope_explainer
from app.v2.surfaces.changes.emitters import emit_candidate_report_changes
from app.v2.surfaces.blueprint.semantic_packaging import (
    build_candidate_summary_pack,
    build_default_report_loading_hint,
    build_default_route_cache_state,
    build_market_path_summary_pack,
)
from app.v2.surfaces.common import degraded_section, ready_section, runtime_provenance, surface_state
from app.v2.sources.freshness_registry import get_freshness
from app.v2.truth.candidate_quality import (
    build_candidate_truth_context,
    build_score_rubric,
    enrich_score_decomposition_with_market_path_support,
    market_path_support_component,
)
from app.v2.truth.envelopes import describe_truth_envelope
from app.v2.truth.replay_inputs import compact_replay_inputs
from app.v2.surfaces.market_truth_support import load_surface_market_truth


_REPORT_TABS = ["investment_case", "market_history", "scenarios", "risks", "competition", "evidence"]


class SourceSnapshotUnavailable(RuntimeError):
    pass


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _slug(value: str) -> str:
    return str(value or "").strip().lower().replace(".", "_").replace("-", "_")


def _market_path_support_component(market_path_support: dict[str, Any] | None) -> tuple[int, str, str]:
    component = market_path_support_component(market_path_support)
    return int(component.get("score") or 0), str(component.get("summary") or ""), str(component.get("tone") or "neutral")


def _enrich_score_decomposition(
    score_decomposition: dict[str, Any] | None,
    market_path_support: dict[str, Any] | None,
) -> dict[str, Any] | None:
    return enrich_score_decomposition_with_market_path_support(score_decomposition, market_path_support)


def _sanitize_surface_url(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    lowered = raw.lower()
    if (
        raw.startswith("/")
        or raw.startswith("file://")
        or raw.startswith("~")
        or "/Users/" in raw
        or "tests/fixtures" in lowered
        or "expected_fixture_path" in lowered
    ):
        return None
    return raw


def _sleeve_id(sleeve_key: str) -> str:
    return f"sleeve_{sleeve_key}"


def _sleeve_purpose(sleeve_key: str) -> str:
    purposes = {
        "global_equity_core": "Broad global equity exposure",
        "developed_ex_us_optional": "Optional developed-market complement",
        "emerging_markets": "Emerging-market diversification",
        "china_satellite": "China-specific satellite exposure",
        "ig_bonds": "Investment-grade ballast",
        "cash_bills": "Cash and T-bill liquidity reserve",
        "real_assets": "Inflation-sensitive real asset hedge",
        "alternatives": "Alternative diversifier",
        "convex": "Convex downside hedge",
    }
    return purposes.get(sleeve_key, str(sleeve_key or "").replace("_", " ").strip().title())


_SCORE_SUMMARY_COMPONENTS: tuple[tuple[str, str], ...] = (
    ("implementation", "Implementation"),
    ("source_integrity", "Source integrity"),
    ("benchmark_fidelity", "Benchmark fidelity"),
    ("sleeve_fit", "Sleeve fit"),
    ("long_horizon_quality", "Long-horizon quality"),
    ("market_path_support", "Market-path support"),
    ("instrument_quality", "Instrument quality"),
    ("portfolio_fit", "Portfolio fit"),
)
_USABLE_DOCUMENT_STATUSES = {"success", "verified", "usable", "cached_valid", "available", "cached", "ready", "ok"}


def _score_tone(score: int | float | None) -> str:
    if score is None:
        return "neutral"
    value = float(score)
    if value >= 75:
        return "good"
    if value >= 55:
        return "warn"
    return "bad"


def _candidate_score_summary(
    score_decomposition: dict[str, Any] | None,
    source_integrity_summary: dict[str, Any] | None,
    market_path_support: dict[str, Any] | None,
) -> dict[str, Any] | None:
    decomposition = dict(score_decomposition or {})
    component_map = {
        str(component.get("component_id") or ""): dict(component)
        for component in list(decomposition.get("components") or [])
        if str(component.get("component_id") or "").strip()
    }
    components: list[dict[str, Any]] = []
    scores: list[int] = []
    for component_id, fallback_label in _SCORE_SUMMARY_COMPONENTS:
        component = component_map.get(component_id)
        if not component:
            continue
        try:
            score = int(round(float(component.get("score"))))
        except (TypeError, ValueError):
            continue
        components.append(
            {
                "component_id": component_id,
                "label": str(component.get("label") or fallback_label),
                "score": score,
                "tone": _score_tone(score),
                "summary": str(component.get("summary") or ""),
            }
        )
        scores.append(score)
    if not scores:
        return None
    headline_score = decomposition.get("recommendation_score", decomposition.get("total_score"))
    try:
        average_score = int(round(float(headline_score)))
    except (TypeError, ValueError):
        average_score = int(round(sum(scores) / len(scores)))
    source_state = str(dict(source_integrity_summary or {}).get("state") or "").strip().lower()
    support = dict(market_path_support or {})
    market_usefulness = str(support.get("usefulness_label") or "").strip().lower()
    market_eligible = str(support.get("eligibility_state") or "").strip().lower() == "eligible"
    if len(scores) < 4 or source_state in {"weak", "conflicted", "missing"}:
        reliability_state = "weak"
    elif (
        len(scores) == len(_SCORE_SUMMARY_COMPONENTS)
        and source_state == "strong"
        and market_eligible
        and market_usefulness not in {"suppressed", "unstable", ""}
    ):
        reliability_state = "strong"
    else:
        reliability_state = "mixed"
    reliability_note: str | None = None
    if len(scores) < len(_SCORE_SUMMARY_COMPONENTS):
        reliability_note = f"Based on {len(scores)} of {len(_SCORE_SUMMARY_COMPONENTS)} score pillars."
    elif source_state in {"weak", "conflicted", "missing"}:
        reliability_note = "Recommendation score is still constrained by weak source support."
    elif source_state == "mixed":
        reliability_note = "Recommendation score is still constrained by mixed source support."
    elif not market_eligible or market_usefulness in {"suppressed", "unstable", ""}:
        reliability_note = "Market-path support is not strong enough to fully anchor the recommendation score."
    return {
        "average_score": average_score,
        "component_count_used": len(scores),
        "tone": _score_tone(average_score),
        "reliability_state": reliability_state,
        "reliability_note": reliability_note,
        "components": components,
    }


def _market_truth(symbol: str, endpoint_family: str = "ohlcv_history") -> MarketSeriesTruth:
    return load_surface_market_truth(
        symbol=symbol,
        surface_name="blueprint",
        endpoint_family=endpoint_family,
        lookback=120,
        allow_live_fetch=False,
    )


def _lightweight_forecast_support(*, label: str) -> dict[str, Any]:
    return {
        "provider": "deterministic_baseline",
        "model_name": "candidate_report_fast_path",
        "horizon": 21,
        "support_strength": "support_only",
        "confidence_summary": f"{label} keeps advisory deterministic scenario support in the report fast path.",
        "degraded_state": "report_fast_path",
        "generated_at": utc_now_iso(),
    }


def _lightweight_scenario_blocks(
    *,
    label: str,
    implication: str,
    upgrade_condition: str | None,
    downgrade_condition: str | None,
    kill_condition: str | None,
) -> list[dict[str, Any]]:
    return [
        {
            "scenario_type": "base_case",
            "title": "Base case",
            "summary": implication or f"{label} remains in the current base case while sleeve-fit and implementation support stay intact.",
            "what_confirms": [upgrade_condition or "Benchmark-relative support improves."],
            "what_breaks": [downgrade_condition or kill_condition or "Evidence weakens materially."],
            "monitoring_thresholds": [],
            "degraded_state": "report_fast_path",
        },
        {
            "scenario_type": "bear_case",
            "title": "Downside case",
            "summary": downgrade_condition or "Relative support weakens and the case slips toward watch or research-only.",
            "what_confirms": [kill_condition or "A blocker opens or evidence quality degrades."],
            "what_breaks": [upgrade_condition or "The current sleeve-fit thesis strengthens again."],
            "monitoring_thresholds": [],
            "degraded_state": "report_fast_path",
        },
    ]


def _path_delta_label(current_value: float | None, projected_value: float | None) -> str:
    if current_value in {None, 0} or projected_value is None:
        return "Path delta unavailable."
    delta_pct = ((float(projected_value) - float(current_value)) / abs(float(current_value))) * 100.0
    direction = "up" if delta_pct >= 0 else "down"
    return f"{abs(delta_pct):.2f}% {direction} from the current anchor."


def _scenario_blocks_from_market_path(
    market_path_support: dict[str, Any],
    *,
    label: str,
    forecast_support: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    scenarios = list(market_path_support.get("scenario_summary") or [])
    threshold_map = {str(item.get("threshold_id") or ""): dict(item) for item in list(market_path_support.get("threshold_map") or [])}
    observed = list(market_path_support.get("observed_series") or [])
    current_value = float(observed[-1]["value"]) if observed else None
    blocks: list[dict[str, Any]] = []
    type_map = {
        "base": ("base", "Base path", "base_case"),
        "downside": ("bear", "Downside path", "downside_case"),
        "stress": ("bear", "Stress path", "stress_case"),
    }
    for scenario in scenarios:
        scenario_type = str(scenario.get("scenario_type") or "")
        public_type, fallback_label, threshold_key = type_map.get(scenario_type, ("base", "Base path", "base_case"))
        path = list(scenario.get("path") or [])
        projected_value = float(path[-1]["value"]) if path else None
        threshold = threshold_map.get(threshold_key) or {}
        blocks.append(
            {
                "type": public_type,
                "label": str(scenario.get("label") or fallback_label),
                "trigger": str(threshold.get("note") or "Threshold context remains bounded."),
                "expected_return": _path_delta_label(current_value, projected_value),
                "portfolio_effect": str(scenario.get("summary") or f"{label} remains bounded in this scenario."),
                "short_term": str(market_path_support.get("candidate_implication") or ""),
                "long_term": str(threshold.get("note") or ""),
                "forecast_support": forecast_support,
                "degraded_state": str(market_path_support.get("suppression_reason") or "") or None,
            }
        )
    return blocks


def _try_load_cached_forecast_support(
    candidate_id: str,
    *,
    label: str,
    allow_refresh: bool = False,
) -> dict[str, Any] | None:
    try:
        if allow_refresh:
            market_path_support = build_candidate_market_path_support(candidate_id, allow_refresh=True)
        else:
            with _connection() as conn:
                cached = latest_forecast_artifact(conn, candidate_id=str(candidate_id))
            market_path_support = dict(cached.get("market_path_support") or {}) if cached and isinstance(cached.get("market_path_support"), dict) else None
        if not market_path_support:
            return None
        forecast_support = compact_forecast_support_from_market_path(market_path_support) or _lightweight_forecast_support(label=label)
        scenario_blocks = _scenario_blocks_from_market_path(
            market_path_support,
            label=label,
            forecast_support=forecast_support,
        )
        return {
            "forecast_support": forecast_support,
            "scenario_blocks": scenario_blocks,
            "market_path_support": market_path_support,
        }
    except Exception:
        return None


def _candidate_assessment(
    *,
    truth,
    interpretation_card,
    signal_strength: float,
    sleeve_id: str,
    sleeve_purpose: str,
) -> CandidateAssessment:
    supports = [signal.summary for signal in interpretation_card.signals if signal.direction in {"up", "positive"}]
    risks = [signal.summary for signal in interpretation_card.signals if signal.direction in {"down", "negative", "mixed"}]
    return CandidateAssessment(
        candidate_id=f"candidate_{truth.instrument_id}",
        sleeve_id=sleeve_id,
        instrument=truth,
        interpretation=interpretation_card,
        mandate_fit="aligned" if sleeve_purpose.lower() in interpretation_card.why_it_matters_here.lower() else "watch",
        conviction=0.55 if signal_strength >= 0.5 else 0.42,
        score_breakdown={"signal_strength": signal_strength},
        key_supports=supports[:3],
        key_risks=risks[:3],
    )


def _dominant_boundary(boundaries: list[PolicyBoundary]) -> PolicyBoundary:
    return next((boundary for boundary in boundaries if boundary.action_boundary == "blocked"), boundaries[0])


def _evidence_depth(truth) -> str:
    citation_count = sum(len(pack.citations) for pack in truth.evidence)
    if citation_count >= 3 or len(truth.evidence) >= 2:
        return "substantial"
    if citation_count >= 1 or truth.evidence:
        return "moderate"
    return "limited"


def _normalize_candidate_symbol(candidate_id: str) -> str:
    raw = str(candidate_id or "").strip()
    if raw.startswith("candidate_instrument_"):
        return raw.removeprefix("candidate_instrument_").upper()
    if raw.startswith("instrument_"):
        return raw.removeprefix("instrument_").upper()
    return raw.upper()


def _candidate_row(candidate_id: str) -> dict[str, Any] | None:
    symbol = _normalize_candidate_symbol(candidate_id)
    with _connection() as conn:
        ensure_candidate_registry_tables(conn)
        rows = export_live_candidate_registry(conn)
        if not rows:
            seed_default_candidate_registry(conn)
            rows = export_live_candidate_registry(conn)

    matches = [row for row in rows if str(row.get("symbol") or "").upper() == symbol]
    if not matches:
        return None

    preferred_sleeve = next(
        (
            sleeve_key
            for sleeve_key, default_symbol in DEFAULT_SLEEVE_ASSIGNMENTS.items()
            if str(default_symbol).upper() == symbol
        ),
        None,
    )
    if preferred_sleeve is not None:
        preferred = next((row for row in matches if str(row.get("sleeve_key") or "") == preferred_sleeve), None)
        if preferred is not None:
            return preferred
    return matches[0]


def _explorer_candidate_row_from_contract(contract: dict[str, Any], candidate_id: str, sleeve_key: str | None = None) -> dict[str, Any] | None:
    requested_sleeve = str(sleeve_key or "").strip()
    requested_sleeve_norm = requested_sleeve[7:] if requested_sleeve.startswith("sleeve_") else requested_sleeve
    fallback: dict[str, Any] | None = None
    for sleeve in list(contract.get("sleeves") or []):
        current_sleeve = str(dict(sleeve or {}).get("sleeve_key") or dict(sleeve or {}).get("sleeve_id") or "").strip()
        current_sleeve_norm = current_sleeve[7:] if current_sleeve.startswith("sleeve_") else current_sleeve
        for candidate in list(dict(sleeve).get("candidates") or []):
            candidate_row = dict(candidate or {})
            if str(candidate_row.get("candidate_id") or "") == candidate_id:
                candidate_sleeve = str(candidate_row.get("sleeve_key") or "").strip()
                candidate_sleeve_norm = candidate_sleeve[7:] if candidate_sleeve.startswith("sleeve_") else candidate_sleeve
                if not requested_sleeve_norm or current_sleeve_norm == requested_sleeve_norm or candidate_sleeve_norm == requested_sleeve_norm:
                    candidate_row.setdefault("sleeve_key", current_sleeve)
                    return candidate_row
                if fallback is None:
                    candidate_row.setdefault("sleeve_key", current_sleeve)
                    fallback = candidate_row
    return fallback


def _latest_explorer_candidate_row(candidate_id: str, sleeve_key: str | None = None) -> dict[str, Any] | None:
    snapshot = latest_surface_snapshot(surface_id="blueprint_explorer", object_id="blueprint_explorer")
    contract = dict(snapshot.get("contract") or {}) if snapshot else {}
    return _explorer_candidate_row_from_contract(contract, candidate_id, sleeve_key=sleeve_key)


def _bound_explorer_candidate_row(candidate_id: str, source_snapshot_id: str | None, sleeve_key: str | None = None) -> dict[str, Any] | None:
    snapshot_id = str(source_snapshot_id or "").strip()
    if not snapshot_id:
        return _latest_explorer_candidate_row(candidate_id, sleeve_key=sleeve_key)
    snapshot = get_surface_snapshot(snapshot_id)
    if snapshot is None:
        raise SourceSnapshotUnavailable(f"Explorer snapshot is unavailable: {snapshot_id}")
    contract = dict(snapshot.get("contract") or {})
    row = _explorer_candidate_row_from_contract(contract, candidate_id, sleeve_key=sleeve_key)
    if row is None:
        raise SourceSnapshotUnavailable(f"Candidate {candidate_id} is unavailable in Explorer snapshot {snapshot_id}")
    return row


def _overlay_bound_explorer_fields(report: dict[str, Any], bound_row: dict[str, Any] | None) -> dict[str, Any]:
    if not bound_row:
        return report
    direct_fields = [
        "benchmark_full_name",
        "exposure_summary",
        "ter_bps",
        "spread_proxy_bps",
        "aum_usd",
        "aum_state",
        "sg_tax_posture",
        "distribution_policy",
        "replication_risk_note",
        "current_weight_pct",
        "weight_state",
        "investor_decision_state",
        "source_integrity_summary",
        "failure_class_summary",
        "score_decomposition",
        "identity_state",
        "blocker_category",
        "candidate_row_summary",
        "candidate_supporting_factors",
        "candidate_penalizing_factors",
        "report_summary_strip",
        "source_confidence_label",
        "coverage_status",
        "coverage_workflow_summary",
        "score_rubric",
        "market_path_support",
        "market_path_objective",
        "market_path_case_note",
        "implementation_profile",
        "recommendation_gate",
        "reconciliation_status",
        "source_authority_fields",
        "reconciliation_report",
        "data_quality_summary",
    ]
    for field in direct_fields:
        if field in bound_row:
            report[field] = bound_row.get(field)
    if bound_row.get("quick_brief_snapshot"):
        report["source_quick_brief_snapshot"] = bound_row.get("quick_brief_snapshot")
    visible_decision_state = dict(bound_row.get("visible_decision_state") or {})
    if visible_decision_state:
        report["visible_decision_state"] = {
            "state": str(visible_decision_state.get("state") or "watch"),
            "allowed_action": str(visible_decision_state.get("allowed_action") or "monitor"),
            "rationale": str(visible_decision_state.get("rationale") or ""),
        }
    if bound_row.get("implication_summary"):
        report["current_implication"] = str(bound_row.get("implication_summary") or "")
    if bound_row.get("what_changes_view"):
        report["what_changes_view"] = str(bound_row.get("what_changes_view") or "")
    if bound_row.get("action_boundary"):
        report["action_boundary"] = bound_row.get("action_boundary")
        report["mandate_boundary"] = bound_row.get("action_boundary")
    if bound_row.get("funding_source"):
        report["funding_source"] = bound_row.get("funding_source")
    return report


def _baseline_benchmarks(truth, sleeve_key: str) -> list[str]:
    benchmark_ids: list[str] = []
    if truth.benchmark_id:
        benchmark_ids.append(truth.benchmark_id)

    default_symbol = DEFAULT_SLEEVE_ASSIGNMENTS.get(sleeve_key)
    default_assignment = DEFAULT_BENCHMARK_ASSIGNMENTS.get(str(default_symbol or "").upper(), {})
    default_benchmark_id = str(default_assignment.get("benchmark_key") or "").strip()
    if default_benchmark_id:
        benchmark_ids.append(default_benchmark_id)

    return list(dict.fromkeys([benchmark_id for benchmark_id in benchmark_ids if benchmark_id]))


def _what_changes_view_from_restraints(restraints: list) -> str:
    for restraint in restraints:
        if restraint.what_changes_view:
            return restraint.what_changes_view[0]
    return ""


def _join_phrases(items: list[str]) -> str:
    parts = [str(item or "").strip() for item in items if str(item or "").strip()]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return f"{', '.join(parts[:-1])}, and {parts[-1]}"


def _sentence_case(value: str | None) -> str:
    raw = _plain_sentence(value)
    if not raw:
        return ""
    return raw[:1].upper() + raw[1:]


def _condition_basis_labels(
    *,
    include_market_setup: bool = False,
    include_benchmark_proof: bool = False,
    include_source_support: bool = False,
    include_document_support: bool = False,
    include_peer_comparison: bool = False,
    include_sleeve_role_fit: bool = False,
    include_replacement_discipline: bool = False,
) -> list[str]:
    labels: list[str] = []
    if include_market_setup:
        labels.append("Market setup")
    if include_benchmark_proof:
        labels.append("Benchmark proof")
    if include_source_support:
        labels.append("Source support")
    if include_document_support:
        labels.append("Document support")
    if include_peer_comparison:
        labels.append("Peer comparison")
    if include_sleeve_role_fit:
        labels.append("Sleeve-role fit")
    if include_replacement_discipline:
        labels.append("Replacement discipline")
    return list(dict.fromkeys(labels))


def _condition_item(
    *,
    kind: str,
    label: str,
    text: str | None,
    confidence: str,
    basis_labels: list[str],
) -> dict[str, Any] | None:
    sentence = _plain_sentence(text)
    if not sentence:
        return None
    return {
        "kind": kind,
        "label": label,
        "text": sentence,
        "confidence": confidence,
        "basis_labels": basis_labels,
    }


def _active_boundary(boundaries: list[PolicyBoundary], *codes: str) -> PolicyBoundary | None:
    wanted = {str(code or "").strip() for code in codes if str(code or "").strip()}
    for boundary in boundaries:
        is_active = (not boundary.passes) or str(boundary.action_boundary or "").strip().lower() in {"blocked", "review"}
        if not is_active:
            continue
        if wanted and str(boundary.code or "").strip() not in wanted:
            continue
        return boundary
    return None


def _upgrade_market_clause(kronos_market_setup: dict[str, Any] | None) -> str | None:
    setup = dict(kronos_market_setup or {})
    scope_label = str(setup.get("scope_label") or "").strip()
    horizon = str(setup.get("horizon_label") or "").strip().lower()
    path_label = str(setup.get("path_support_label") or "").strip()
    drift_label = str(setup.get("drift_label") or "").strip()
    downside_label = str(setup.get("downside_risk_label") or "").strip()
    quality_gate = str(setup.get("quality_gate") or "").strip().lower()
    if not scope_label or quality_gate in {"", "unavailable"}:
        return None
    if path_label in {"Base path supportive", "Base path mildly supportive"}:
        parts = [f"{scope_label} support stays {path_label.lower()} over {horizon or 'the stated horizon'}"]
        if drift_label == "Strengthening":
            parts.append("continues strengthening")
        elif drift_label == "Stable":
            parts.append("stays stable")
        elif drift_label == "Weakening":
            parts.append("stops weakening")
        if downside_label == "Contained":
            parts.append("downside stays contained")
        elif downside_label == "Elevated":
            parts.append("downside pressure stops widening")
        return _join_phrases(parts)
    if path_label == "Base path fragile":
        return f"{scope_label} support stabilises again over {horizon or 'the stated horizon'}"
    if path_label == "Base path adverse":
        return f"{scope_label} conditions stop working against the exposure over {horizon or 'the stated horizon'}"
    return None


def _downgrade_market_clause(kronos_market_setup: dict[str, Any] | None) -> str | None:
    setup = dict(kronos_market_setup or {})
    scope_label = str(setup.get("scope_label") or "").strip()
    horizon = str(setup.get("horizon_label") or "").strip().lower()
    path_label = str(setup.get("path_support_label") or "").strip()
    drift_label = str(setup.get("drift_label") or "").strip()
    downside_label = str(setup.get("downside_risk_label") or "").strip()
    if not scope_label or path_label == "Market setup unavailable":
        return None
    if path_label in {"Base path fragile", "Base path adverse"} or drift_label == "Weakening" or downside_label == "Elevated":
        parts = [f"market support for {scope_label} weakens further over {horizon or 'the stated horizon'}"]
        if downside_label == "Elevated":
            parts.append("downside stays elevated")
        if drift_label == "Weakening":
            parts.append("the setup keeps drifting the wrong way")
        return _join_phrases(parts)
    return f"the current support for {scope_label} fades over {horizon or 'the stated horizon'}"


def _upgrade_proof_clause(
    *,
    benchmark_issue: bool,
    source_issue: bool,
    document_issue: bool,
    purpose_issue: bool,
    replacement_issue: bool,
) -> str:
    tasks: list[str] = []
    if purpose_issue:
        tasks.append("the ETF still fits the sleeve job it is supposed to do")
    if benchmark_issue:
        tasks.append("benchmark proof is strong enough to compare the ETF cleanly again")
    elif source_issue:
        tasks.append("source support is clean enough to trust the comparison")
    if document_issue:
        tasks.append("document support is strong enough to back a firm preference")
    if replacement_issue:
        tasks.append("the replacement case still clears the higher conviction bar")
    if not tasks:
        return "the implementation case is strong enough to prefer this ETF over cheaper same-job peers"
    tasks.append("the ETF still beats cheaper same-job peers on the actual implementation case")
    return _join_phrases(tasks)


def _downgrade_proof_clause(
    *,
    benchmark_issue: bool,
    source_issue: bool,
    document_issue: bool,
    purpose_issue: bool,
    replacement_issue: bool,
) -> str:
    if purpose_issue:
        return "the sleeve-role fit stays unresolved"
    if benchmark_issue and document_issue:
        return "benchmark proof and document support both stay too weak to back a clean comparison"
    if benchmark_issue:
        return "benchmark proof stays too weak to back a clean comparison"
    if document_issue:
        return "document support stays too thin for a firm preference"
    if source_issue:
        return "source support remains mixed while cheaper same-job peers stay good enough"
    if replacement_issue:
        return "the replacement case stops clearing the higher conviction bar"
    return "cheaper same-job peers stay good enough on cost and implementation"


def _kill_condition_text(boundaries: list[PolicyBoundary]) -> tuple[str, str, list[str]]:
    blocking = _active_boundary(boundaries, "benchmark_authority_floor", "sleeve_purpose_alignment", "low_turnover_discipline")
    if blocking is not None:
        code = str(blocking.code or "").strip()
        if code == "benchmark_authority_floor":
            return (
                "Remove only if benchmark proof cannot be repaired enough to compare this ETF cleanly again.",
                "high",
                _condition_basis_labels(include_benchmark_proof=True),
            )
        if code == "sleeve_purpose_alignment":
            return (
                "Remove only if the ETF no longer fits the sleeve job it is supposed to do.",
                "high",
                _condition_basis_labels(include_sleeve_role_fit=True),
            )
        if code == "low_turnover_discipline":
            return (
                "Remove only if the replacement case no longer clears the conviction bar needed to displace the current holding.",
                "medium",
                _condition_basis_labels(include_replacement_discipline=True, include_peer_comparison=True),
            )
    return (
        "Remove only if sleeve-role fit or benchmark proof actually breaks.",
        "medium",
        _condition_basis_labels(include_sleeve_role_fit=True, include_benchmark_proof=True),
    )


def _condition_scope_text(kronos_market_setup: dict[str, Any] | None) -> str:
    setup = dict(kronos_market_setup or {})
    scope = str(setup.get("scope_label") or "this exposure").strip() or "this exposure"
    if scope.lower() in {"unknown", "n/a", "na", "none"}:
        return "this exposure"
    return scope


def _condition_scope_phrase(kronos_market_setup: dict[str, Any] | None) -> str:
    scope = _plain_sentence(_condition_scope_text(kronos_market_setup))
    if not scope:
        return "this exposure"
    return scope[:1].lower() + scope[1:]


def _upgrade_condition_headline(
    *,
    kronos_market_setup: dict[str, Any] | None,
) -> str:
    setup = dict(kronos_market_setup or {})
    scope_text = _condition_scope_phrase(setup)
    path_label = str(setup.get("path_support_label") or "").strip()
    if path_label == "Base path supportive":
        return f"Upgrade only if the market keeps supporting {scope_text} and this ETF still keeps a real implementation edge after peer comparison."
    if path_label == "Base path mildly supportive":
        return f"Upgrade only if the market stays constructive for {scope_text} and this ETF still keeps a real implementation edge after peer comparison."
    if path_label == "Base path fragile":
        return f"Upgrade only if the market stops wobbling around {scope_text} and this ETF still keeps a real implementation edge after peer comparison."
    if path_label == "Base path adverse":
        return f"Upgrade only if the market stops leaning against {scope_text} and this ETF still keeps a real implementation edge after peer comparison."
    if scope_text != "this exposure":
        return f"Upgrade only if the market stops leaning against {scope_text} and this ETF still keeps a real implementation edge after peer comparison."
    return "Upgrade only if the market improves and this ETF still keeps a real implementation edge after peer comparison."


def _downgrade_condition_headline(
    *,
    kronos_market_setup: dict[str, Any] | None,
) -> str:
    setup = dict(kronos_market_setup or {})
    scope_text = _condition_scope_phrase(setup)
    path_label = str(setup.get("path_support_label") or "").strip()
    if path_label in {"Base path fragile", "Base path adverse"}:
        return f"Downgrade if the {scope_text} setup weakens further and cheaper peers continue to look good enough."
    if path_label in {"Base path supportive", "Base path mildly supportive"}:
        return f"Downgrade if support behind {scope_text} fades and cheaper peers continue to look good enough."
    if scope_text != "this exposure":
        return f"Downgrade if the {scope_text} setup weakens and cheaper peers continue to look good enough."
    return "Downgrade if the setup weakens and cheaper peers continue to look good enough."


def _upgrade_condition_support(
    *,
    benchmark_issue: bool,
    source_issue: bool,
    document_issue: bool,
    purpose_issue: bool,
    replacement_issue: bool,
) -> str:
    lines: list[str] = [
        "That would mean the current weakness was mostly timing pressure, not a broken case."
    ]
    if purpose_issue:
        lines.append("For a firmer preference, the ETF still has to prove that it cleanly fits the sleeve job it is meant to do.")
    elif replacement_issue:
        lines.append("For a firmer preference, the replacement case still has to clear the higher bar needed to displace the current holding.")
    elif benchmark_issue or source_issue or document_issue:
        lines.append(
            "For a firmer preference, the wrapper edge still has to survive against cheaper peers on benchmark fit, liquidity, execution quality, and document support."
        )
    else:
        lines.append(
            "For a firmer preference, the wrapper edge still has to hold up against cheaper peers on real execution quality, liquidity, and trust."
        )
    return " ".join(lines)


def _downgrade_condition_support(
    *,
    benchmark_issue: bool,
    source_issue: bool,
    document_issue: bool,
    purpose_issue: bool,
    replacement_issue: bool,
) -> str:
    if purpose_issue:
        return "At that point the role itself would still be too uncertain to support a cleaner preference."
    if replacement_issue:
        return "At that point the replacement case would no longer clear the higher bar needed to displace the current holding."
    if benchmark_issue and document_issue:
        return "At that point the market backdrop would be working against the idea while the benchmark case and document support still would not be strong enough for a clear preference."
    if benchmark_issue or source_issue or document_issue:
        return "At that point the problem is no longer just incomplete proof. The market backdrop would be working against the idea while the claimed wrapper edge still would not be strong enough to justify a clear preference."
    return "At that point the market backdrop would be weakening while the claimed wrapper edge still would not be strong enough to justify a clear preference."


def _condition_confirmation_label(kind: str) -> str:
    if kind == "upgrade":
        return "What would confirm the upgrade"
    if kind == "downgrade":
        return "What would confirm the downgrade"
    return "What would confirm the kill"


def _upgrade_confirmation_points(
    *,
    benchmark_issue: bool,
    document_issue: bool,
    purpose_issue: bool,
    replacement_issue: bool,
) -> list[str]:
    points = ["Market tone improves"]
    if purpose_issue:
        points.append("The ETF still fits the sleeve role it is meant to play")
    else:
        points.append("The benchmark case stays intact")
    if document_issue:
        points.append("Document support becomes strong enough for a firm comparison")
    if replacement_issue:
        points.append("The replacement case still clears the higher conviction bar")
    else:
        points.append("Cheaper peers still fail to close the implementation gap")
    return points[:4]


def _downgrade_confirmation_points(
    *,
    benchmark_issue: bool,
    source_issue: bool,
    document_issue: bool,
    purpose_issue: bool,
    replacement_issue: bool,
) -> list[str]:
    points = ["Market pressure deepens"]
    if purpose_issue:
        points.append("The sleeve role still does not read cleanly enough")
    else:
        points.append("Peer substitutes remain acceptable on real use")
    if benchmark_issue:
        points.append("The benchmark case still does not clean up")
    elif document_issue:
        points.append("Document support still is not strong enough for a firm preference")
    elif source_issue:
        points.append("The comparison evidence still does not clean up")
    if replacement_issue:
        points.append("The replacement case no longer clears the conviction bar")
    else:
        points.append("The evidence for a superior wrapper still does not strengthen")
    return points[:4]


def _kill_confirmation_points(
    *,
    purpose_issue: bool,
    replacement_issue: bool,
) -> list[str]:
    points = ["Benchmark alignment breaks"]
    if purpose_issue:
        points.append("The sleeve role is no longer valid")
    else:
        points.append("The sleeve role no longer holds together")
    if replacement_issue:
        points.append("The replacement case no longer clears the required conviction bar")
    else:
        points.append("The ETF no longer serves the intended portfolio function")
    return points[:4]


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _expense_ratio_label(value: Any) -> str | None:
    number = _safe_float(value)
    if number is None:
        return None
    return f"{number:.2%}"


def _bps_label(value: Any) -> str | None:
    number = _safe_float(value)
    if number is None:
        return None
    return f"{number:.1f} bps"


def _aum_label(value: Any) -> str | None:
    number = _safe_float(value)
    if number is None:
        return None
    if number >= 1_000_000_000:
        return f"${number / 1_000_000_000:.1f}B"
    if number >= 1_000_000:
        return f"${number / 1_000_000:.0f}M"
    return f"${number:,.0f}"


def _price_label(value: Any, currency: str | None = None) -> str | None:
    number = _safe_float(value)
    if number is None:
        return None
    base = f"{number:,.2f}"
    if currency:
        return f"{base} {currency}"
    return base


def _plain_sentence(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _quick_status(visible_decision_state: dict[str, Any], recommendation_gate: dict[str, Any]) -> tuple[str, str]:
    state = str(visible_decision_state.get("state") or "").strip().lower()
    gate_state = str(recommendation_gate.get("gate_state") or "").strip().lower()
    if state == "blocked" or gate_state == "blocked":
        return "blocked", "Blocked"
    if state == "eligible" or gate_state == "admissible":
        return "eligible", "Eligible"
    if state == "research_only":
        return "research_only", "Research only"
    return "watchlist", "Watchlist only"


def _quick_secondary_reasons(
    *,
    blocker_category: str | None,
    source_integrity_summary: dict[str, Any],
    market_path_support: dict[str, Any] | None,
    recommendation_gate: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    blocker = str(blocker_category or "").strip().lower()
    if blocker == "identity":
        reasons.append("Instrument identity still needs cleanup")
    elif blocker == "benchmark":
        reasons.append("Benchmark support still needs cleanup")
    elif blocker == "implementation":
        reasons.append("Implementation facts still need cleanup")
    elif blocker == "evidence":
        reasons.append("Source coverage is still thin")

    integrity = str(source_integrity_summary.get("integrity_label") or source_integrity_summary.get("state") or "").strip().lower()
    if integrity == "conflicted":
        reasons.append("Source confidence is still conflicted")
    elif integrity in {"thin", "mixed", "weak"}:
        reasons.append("Source confidence is still mixed")

    support = dict(market_path_support or {})
    usefulness = str(support.get("usefulness_label") or "").strip().lower()
    if usefulness == "unstable":
        reasons.append("Market backdrop is still fragile")
    elif usefulness == "usable_with_caution":
        reasons.append("Market backdrop is only bounded support")
    elif usefulness == "suppressed":
        reasons.append("Market backdrop is not available yet")

    if not reasons:
        blocked_reason = next(
            (
                _plain_sentence(reason)
                for reason in list(recommendation_gate.get("blocked_reasons") or [])
                if str(reason or "").strip()
            ),
            "",
        )
        if blocked_reason:
            reasons.append(blocked_reason)

    deduped: list[str] = []
    for item in reasons:
        if item and item not in deduped:
            deduped.append(item)
        if len(deduped) >= 3:
            break
    return deduped


def _quick_summary(
    *,
    name: str,
    benchmark_full_name: str | None,
    exposure_summary: str | None,
    implementation_profile: dict[str, Any],
    status_state: str,
    secondary_reasons: list[str],
) -> str:
    implementation_suitability = str(implementation_profile.get("execution_suitability") or "").strip().lower()
    implementation_phrase = (
        "strong implementation quality"
        if implementation_suitability == "execution_efficient"
        else "mixed implementation quality"
        if implementation_suitability == "execution_mixed"
        else "weaker implementation quality"
    )
    benchmark_or_exposure = str(benchmark_full_name or exposure_summary or name).strip()
    cost_bits = []
    if implementation_profile.get("spread_proxy"):
        cost_bits.append(str(implementation_profile.get("spread_proxy")))
    if implementation_profile.get("aum"):
        cost_bits.append(str(implementation_profile.get("aum")))
    cost_phrase = f" with {', '.join(cost_bits)}" if cost_bits else ""
    if status_state == "eligible":
        return _plain_sentence(
            f"{benchmark_or_exposure} exposure with {implementation_phrase}{cost_phrase}. The brief is currently action-ready."
        )
    if status_state == "blocked":
        reason = secondary_reasons[0] if secondary_reasons else "key recommendation facts are not clean enough yet"
        return _plain_sentence(
            f"{benchmark_or_exposure} exposure with {implementation_phrase}{cost_phrase}, but it is blocked for now because {reason.lower()}."
        )
    if status_state == "research_only":
        reason = secondary_reasons[0] if secondary_reasons else "the case still needs more work"
        return _plain_sentence(
            f"{benchmark_or_exposure} exposure with {implementation_phrase}{cost_phrase}. Keep it in research only while {reason.lower()}."
        )
    reason = secondary_reasons[0] if secondary_reasons else "the case is not clean enough yet"
    return _plain_sentence(
        f"{benchmark_or_exposure} exposure with {implementation_phrase}{cost_phrase}. Keep it on the watchlist while {reason.lower()}."
    )


def _authority_value(source_authority_fields: list[dict[str, Any]], field_name: str) -> Any:
    for row in source_authority_fields:
        if str(row.get("field_name") or "").strip() == field_name:
            return row.get("resolved_value")
    return None


def _count_label(value: Any) -> str | None:
    number = _safe_float(value)
    if number is None:
        return None
    rounded = int(round(number))
    if rounded <= 0:
        return None
    return f"{rounded:,}"


def _quick_exposure_label(
    *,
    benchmark_full_name: str | None,
    implementation_profile: dict[str, Any],
    exposure_summary: str | None,
    truth,
) -> str | None:
    benchmark_text = str(
        benchmark_full_name
        or implementation_profile.get("mandate_or_index")
        or exposure_summary
        or truth.asset_class
        or truth.name
        or ""
    ).strip()
    lowered = benchmark_text.lower()
    if "s&p 500" in lowered:
        return "U.S. large cap equity, S&P 500"
    if "msci world" in lowered:
        return "Developed-market equity, MSCI World"
    if "msci acwi" in lowered:
        return "Global equity, MSCI ACWI"
    if "emerging" in lowered:
        return "Emerging-market equity"
    if "treasury" in lowered or "t-bill" in lowered or "bill" in lowered:
        return "Short-duration Treasury bills"
    if "bond" in lowered or "aggregate" in lowered:
        return "Investment-grade bond exposure"
    if benchmark_text:
        return benchmark_text
    return None


def _quick_portfolio_role(*, sleeve_purpose: str, exposure_label: str | None) -> str | None:
    purpose = str(sleeve_purpose or "").strip()
    lowered = purpose.lower()
    if "global equity" in lowered:
        return "Possible core equity building block inside a broader global-equity allocation."
    if "developed-market complement" in lowered:
        return "Possible developed-markets building block when the portfolio wants ex-U.S. equity alongside a broader core."
    if "emerging-market diversification" in lowered:
        return "Possible emerging-markets building block when the portfolio needs dedicated higher-volatility growth exposure."
    if "investment-grade ballast" in lowered:
        return "Possible defensive bond building block when the portfolio needs ballast and rate-sensitive stability."
    if "cash and t-bill" in lowered or "liquidity reserve" in lowered:
        return "Possible liquidity reserve ETF when the portfolio needs very short-duration capital parking."
    if "real asset" in lowered:
        return "Possible real-asset sleeve building block when the portfolio needs inflation-sensitive diversification."
    if "alternative" in lowered:
        return "Possible diversifier when the portfolio needs non-core return drivers beyond the stock-bond mix."
    if "convex" in lowered:
        return "Possible hedge line when the portfolio needs explicit downside protection rather than broad beta."
    if exposure_label:
        return _plain_sentence(f"Possible {exposure_label.lower()} building block inside the active sleeve.")
    return purpose or None


_QUICK_COMPARE_OVERRIDES: dict[str, dict[str, str]] = {
    "IWDA": {
        "why_this_matters": (
            "The decision is whether IWDA is the cleanest way to anchor broad developed-markets equity exposure "
            "without paying extra for scale that may not improve the actual ETF outcome."
        ),
        "compare_first": (
            "Compare first against SPDR MSCI World UCITS ETF and Xtrackers MSCI World UCITS ETF 1C. "
            "They do the same MSCI World job at a lower stated annual fee than IWDA, so IWDA only wins if its "
            "scale, trading comfort, and execution quality justify paying more."
        ),
        "broader_alternative": (
            "Also check Vanguard FTSE Developed World UCITS ETF if the real question is broad developed-markets "
            "exposure rather than exact MSCI World lineage, and Vanguard FTSE All-World UCITS ETF if the investor "
            "may actually want one-fund global equity instead of developed markets only."
        ),
        "what_it_solves": (
            "Use as the developed-markets equity engine when the portfolio wants broad MSCI World exposure without "
            "emerging markets bundled in."
        ),
        "what_it_still_needs_to_prove": (
            "It still needs to prove that BlackRock's much larger scale is worth paying a higher annual fee than "
            "cheaper MSCI World peers that do essentially the same job."
        ),
        "decision_readiness": (
            "Watchlist only until the brief shows why IWDA deserves a fee premium over SPDR MSCI World, "
            "Xtrackers MSCI World, or Vanguard FTSE Developed World."
        ),
    }
}

_QUICK_PEER_COMPARE_SEEDS: dict[str, dict[str, Any]] = {
    "IWDA": {
        "primary_question": "Does IWDA earn its fee premium versus the closest same-job peers, or is a cheaper developed-markets line good enough?",
        "comparison_basis": "Same-job MSCI World peers first, then one broader developed-markets alternative and one all-world control case.",
        "peers": [
            {
                "role": "same_job_peer",
                "fund_name": "SPDR MSCI World UCITS ETF",
                "ticker_or_line": "SWRD / SWLD",
                "isin": "IE00BFY0GT14",
                "benchmark": "MSCI World Index",
                "benchmark_family": "MSCI World",
                "exposure_scope": "Developed markets only",
                "developed_only": True,
                "emerging_markets_included": False,
                "ter": "0.12%",
                "fund_assets": "$17.5B",
                "holdings_count": "1,311",
                "replication": "Optimized sampling",
                "distribution": "Accumulating",
                "domicile": "Ireland",
                "tracking_error_3y": "0.06%",
                "why_this_peer_matters": "Same MSCI World job at a lower stated annual fee, so it is the first direct fee-pressure test.",
                "same_index": True,
                "same_job": True,
                "same_distribution": True,
                "same_domicile": True,
            },
            {
                "role": "same_job_peer",
                "fund_name": "Xtrackers MSCI World UCITS ETF 1C",
                "ticker_or_line": "XDWD",
                "isin": "IE00BJ0KDQ92",
                "benchmark": "MSCI World Index",
                "benchmark_family": "MSCI World",
                "exposure_scope": "Developed markets only",
                "developed_only": True,
                "emerging_markets_included": False,
                "ter": "0.12%",
                "replication": "Physical",
                "distribution": "Accumulating",
                "domicile": "Ireland",
                "launch_date": "2014",
                "why_this_peer_matters": "Another direct MSCI World peer that pressures IWDA on the same exposure job with a lower headline fee.",
                "same_index": True,
                "same_job": True,
                "same_distribution": True,
                "same_domicile": True,
            },
            {
                "role": "broader_control",
                "fund_name": "Vanguard FTSE Developed World UCITS ETF",
                "ticker_or_line": "VHVG",
                "isin": "IE00BK5BQV03",
                "benchmark": "FTSE Developed Index",
                "benchmark_family": "FTSE Developed",
                "exposure_scope": "Developed markets only",
                "developed_only": True,
                "emerging_markets_included": False,
                "ter": "0.12%",
                "fund_assets": "$9.61B",
                "holdings_count": "1,992",
                "replication": "Physical",
                "distribution": "Accumulating",
                "domicile": "Ireland",
                "why_this_peer_matters": "Use this when the real question is broad developed-markets exposure rather than exact MSCI World lineage.",
                "same_index": False,
                "same_job": True,
                "same_distribution": True,
                "same_domicile": True,
            },
            {
                "role": "broader_control",
                "fund_name": "Vanguard FTSE All-World UCITS ETF",
                "ticker_or_line": "VWRP / VWCE",
                "isin": "IE00BK5BQT80",
                "benchmark": "FTSE All-World Index",
                "benchmark_family": "FTSE All-World",
                "exposure_scope": "Global equity incl. emerging markets",
                "developed_only": False,
                "emerging_markets_included": True,
                "ter": "0.19%",
                "fund_assets": "$57.48B",
                "holdings_count": "3,771",
                "replication": "Physical",
                "distribution": "Accumulating",
                "domicile": "Ireland",
                "why_this_peer_matters": "This is the control case for the more important asset-allocation question: developed markets only or one-fund global equity.",
                "same_index": False,
                "same_job": False,
                "same_distribution": True,
                "same_domicile": True,
            },
        ],
        "index_scope": {
            "index_name": "MSCI World Index",
            "coverage_statement": "Developed markets only.",
            "includes_statement": "Large and mid-cap companies across developed markets.",
            "excludes_statement": "Emerging markets are excluded.",
            "market_cap_scope": "Large and mid cap.",
            "emerging_markets_included": False,
        },
        "composition": {
            "number_of_stocks": "1,512",
            "country_weights": [
                {"label": "United States", "value": "71.17%"},
                {"label": "Non-U.S. developed markets", "value": "28.83%"},
            ],
            "top_10_weight": "25.39%",
            "us_weight": "71.17%",
            "non_us_weight": "28.83%",
            "em_weight": "0.00%",
        },
        "what_must_be_true_to_prefer_this": (
            "Prefer IWDA only if its larger scale, trading comfort, document support, or execution quality is good enough to justify paying more than the closest same-job peers."
        ),
    }
}


def _quick_fund_domicile(*, truth, implementation_profile: dict[str, Any]) -> str | None:
    return (
        str(getattr(truth, "domicile", "") or "").strip()
        or str(implementation_profile.get("fund_domicile") or "").strip()
        or str(implementation_profile.get("domicile") or "").strip()
        or None
    )


def _percent_value_label(value: Any, *, signed: bool = False) -> str | None:
    if isinstance(value, str):
        cleaned = _plain_sentence(value)
        return cleaned or None
    number = _safe_float(value)
    if number is None:
        return None
    if signed:
        return f"{number:+.2f}%"
    return f"{number:.2f}%"


def _doc_link_rows(primary_document_manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for document in list(primary_document_manifest or []):
        item = dict(document or {})
        if str(item.get("status") or "").strip().lower() not in _USABLE_DOCUMENT_STATUSES:
            continue
        doc_type = _plain_sentence(str(item.get("doc_type") or "").replace("_", " ").title()) or "Document"
        rows.append(
            {
                "label": doc_type,
                "url": _sanitize_surface_url(item.get("doc_url")),
            }
        )
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None]] = set()
    for row in rows:
        key = (str(row.get("label") or ""), row.get("url"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _peer_compare_delta(subject_value: Any, peer_value: Any, *, formatter) -> str | None:
    subject = _safe_float(subject_value)
    peer = _safe_float(peer_value)
    if subject is None or peer is None:
        return None
    return formatter(peer - subject)


def _quick_benchmark_family(benchmark_full_name: str | None, exposure_label: str | None, truth) -> str | None:
    benchmark = str(benchmark_full_name or truth.benchmark_id or "").strip()
    lowered = benchmark.lower()
    if "msci world" in lowered:
        return "MSCI World"
    if "all-world" in lowered:
        return "FTSE All-World"
    if "developed" in lowered:
        return "FTSE Developed"
    if "acwi" in lowered:
        return "MSCI ACWI"
    if "s&p 500" in lowered:
        return "S&P 500"
    exposure = str(exposure_label or "").strip()
    return exposure or None


def _quick_index_scope_explainer(
    *,
    symbol: str,
    benchmark_full_name: str | None,
    exposure_label: str | None,
    holdings_count: str | None,
    sleeve_key: str | None = None,
    benchmark_family: str | None = None,
    benchmark_key: str | None = None,
    asset_class: str | None = None,
) -> dict[str, Any]:
    seeded = dict(_QUICK_PEER_COMPARE_SEEDS.get(str(symbol or "").strip().upper(), {}).get("index_scope") or {})
    return resolve_index_scope_explainer(
        symbol=symbol,
        benchmark_full_name=benchmark_full_name,
        benchmark_family=benchmark_family,
        benchmark_key=benchmark_key,
        exposure_label=exposure_label,
        asset_class=asset_class,
        sleeve_key=sleeve_key,
        holdings_count=holdings_count,
        seeded_scope=seeded or None,
    )


def _quick_document_coverage(
    *,
    primary_document_manifest: list[dict[str, Any]],
    benchmark_full_name: str | None,
    truth,
) -> dict[str, Any]:
    usable_manifest = [
        dict(item or {})
        for item in list(primary_document_manifest or [])
        if str(dict(item or {}).get("status") or "").strip().lower() in _USABLE_DOCUMENT_STATUSES
    ]
    document_types = {
        str(dict(item or {}).get("doc_type") or "").strip().lower()
        for item in usable_manifest
        if str(dict(item or {}).get("doc_type") or "").strip()
    }
    timestamps = [
        str(dict(item or {}).get("retrieved_at") or "").strip()
        for item in usable_manifest
        if str(dict(item or {}).get("retrieved_at") or "").strip()
    ]
    missing_documents: list[str] = []
    for doc_type, label in (
        ("factsheet", "Factsheet"),
        ("kid", "KID"),
        ("prospectus", "Prospectus"),
        ("annual_report", "Annual report"),
    ):
        if doc_type not in document_types:
            missing_documents.append(label)
    benchmark_methodology_present = bool(benchmark_full_name) and ("prospectus" in document_types or "factsheet" in document_types)
    document_count = len(document_types)
    if "factsheet" in document_types and ("kid" in document_types or "prospectus" in document_types):
        confidence_grade = "A"
    elif "factsheet" in document_types:
        confidence_grade = "B"
    elif document_types:
        confidence_grade = "C"
    else:
        confidence_grade = "D"
    return {
        "factsheet_present": "factsheet" in document_types,
        "kid_present": "kid" in document_types,
        "prospectus_present": "prospectus" in document_types,
        "annual_report_present": "annual_report" in document_types,
        "benchmark_methodology_present": benchmark_methodology_present,
        "last_refreshed_at": max(timestamps) if timestamps else getattr(truth, "as_of", None),
        "document_count": document_count,
        "missing_documents": missing_documents,
        "document_confidence_grade": confidence_grade,
    }


def _quick_fund_profile(
    *,
    truth,
    benchmark_full_name: str | None,
    exposure_label: str | None,
    implementation_profile: dict[str, Any],
    source_authority_fields: list[dict[str, Any]],
    primary_document_manifest: list[dict[str, Any]],
    aum_usd: Any,
) -> dict[str, Any]:
    seeded = dict(_QUICK_PEER_COMPARE_SEEDS.get(str(getattr(truth, "symbol", "") or "").strip().upper(), {}) or {})
    holdings_count = _count_label(
        _authority_value(source_authority_fields, "holdings_count")
        or (truth.metrics or {}).get("holdings_count")
    ) or seeded.get("composition", {}).get("number_of_stocks")
    share_class_assets = _aum_label((truth.metrics or {}).get("aum_usd"))
    fund_assets = _plain_sentence(implementation_profile.get("aum") or _aum_label(aum_usd))
    if share_class_assets == fund_assets:
        share_class_assets = None
    return {
        "objective": _plain_sentence(
            f"Tracks {benchmark_full_name or implementation_profile.get('mandate_or_index') or exposure_label or truth.name}."
        ),
        "benchmark": _plain_sentence(benchmark_full_name or implementation_profile.get("mandate_or_index")),
        "benchmark_family": _quick_benchmark_family(benchmark_full_name, exposure_label, truth),
        "domicile": _quick_fund_domicile(truth=truth, implementation_profile=implementation_profile),
        "replication": _plain_sentence(implementation_profile.get("replication_method")),
        "distribution": _plain_sentence(implementation_profile.get("distribution_policy")),
        "fund_assets": fund_assets or None,
        "share_class_assets": share_class_assets,
        "holdings_count": holdings_count,
        "launch_date": _plain_sentence(implementation_profile.get("launch_date") or (truth.metrics or {}).get("launch_date")),
        "issuer": _plain_sentence(implementation_profile.get("issuer_name") or implementation_profile.get("issuer") or (truth.metrics or {}).get("issuer")),
        "documents": _doc_link_rows(primary_document_manifest),
    }


def _quick_listing_profile(
    *,
    truth,
    implementation_profile: dict[str, Any],
) -> dict[str, Any]:
    price = _safe_float((truth.metrics or {}).get("price"))
    return {
        "exchange": _plain_sentence(implementation_profile.get("primary_listing_exchange")),
        "trading_currency": _plain_sentence(implementation_profile.get("primary_trading_currency") or getattr(truth, "base_currency", None)),
        "ticker": _plain_sentence(getattr(truth, "symbol", None)),
        "market_price": _price_label(price, str(implementation_profile.get("primary_trading_currency") or getattr(truth, "base_currency", "")).strip() or None) if price and price > 0 else None,
        "nav": None,
        "spread_proxy": _plain_sentence(implementation_profile.get("spread_proxy")),
        "volume": None,
        "premium_discount": _plain_sentence(implementation_profile.get("premium_discount_behavior")),
        "as_of": getattr(truth, "as_of", None),
    }


def _quick_decision_proof_pack(
    *,
    why_this_matters: str,
    compare_first: str,
    broader_alternative: str | None,
    what_it_solves: str,
    what_it_does_not_solve: str,
    what_it_still_needs_to_prove: str,
    what_changes_view: str,
    symbol: str,
) -> dict[str, Any]:
    seeded = dict(_QUICK_PEER_COMPARE_SEEDS.get(str(symbol or "").strip().upper(), {}) or {})
    return {
        "why_candidate_exists": what_it_solves,
        "why_in_scope": why_this_matters,
        "why_not_complete_solution": what_it_does_not_solve,
        "best_same_job_peers": compare_first,
        "broader_control_peer": broader_alternative,
        "fee_premium_question": what_it_still_needs_to_prove,
        "what_must_be_true_to_prefer_this": _plain_sentence(
            seeded.get("what_must_be_true_to_prefer_this")
            or "Prefer this line only if the better implementation case survives the first peer comparison on cost, spread, tracking, wrapper, and document support."
        ),
        "what_would_change_verdict": _plain_sentence(what_changes_view),
    }


def _quick_performance_tracking_pack(
    *,
    truth,
    implementation_profile: dict[str, Any],
    source_authority_fields: list[dict[str, Any]],
) -> dict[str, Any]:
    metrics = dict(getattr(truth, "metrics", {}) or {})
    tracking_current = _plain_sentence(implementation_profile.get("tracking_difference"))
    tracking_1y = _percent_value_label(_authority_value(source_authority_fields, "tracking_difference_1y") or metrics.get("tracking_difference_1y"), signed=True)
    tracking_3y = _percent_value_label(_authority_value(source_authority_fields, "tracking_difference_3y") or metrics.get("tracking_difference_3y"), signed=True)
    tracking_5y = _percent_value_label(_authority_value(source_authority_fields, "tracking_difference_5y") or metrics.get("tracking_difference_5y"), signed=True)
    tracking_error_1y = _percent_value_label(_authority_value(source_authority_fields, "tracking_error_1y") or metrics.get("tracking_error_1y"))
    return {
        "return_1y": None,
        "return_3y": None,
        "return_5y": None,
        "benchmark_return_1y": None,
        "benchmark_return_3y": None,
        "benchmark_return_5y": None,
        "tracking_error_1y": tracking_error_1y,
        "tracking_error_3y": None,
        "tracking_error_5y": None,
        "tracking_difference_current_period": tracking_current or tracking_1y,
        "tracking_difference_1y": tracking_1y,
        "tracking_difference_3y": tracking_3y,
        "tracking_difference_5y": tracking_5y,
        "volatility": None,
        "max_drawdown": None,
        "as_of": getattr(truth, "as_of", None),
    }


def _quick_composition_pack(
    *,
    symbol: str,
    truth,
    source_authority_fields: list[dict[str, Any]],
) -> dict[str, Any]:
    seeded = dict(_QUICK_PEER_COMPARE_SEEDS.get(str(symbol or "").strip().upper(), {}).get("composition") or {})
    number_of_stocks = _count_label(
        _authority_value(source_authority_fields, "holdings_count")
        or (truth.metrics or {}).get("holdings_count")
    ) or seeded.get("number_of_stocks")
    non_us_weight = seeded.get("non_us_weight")
    if not non_us_weight and seeded.get("us_weight") and seeded.get("em_weight") is not None:
        us_weight_num = _safe_float(str(seeded.get("us_weight")).replace("%", ""))
        em_weight_num = _safe_float(str(seeded.get("em_weight")).replace("%", ""))
        if us_weight_num is not None and em_weight_num is not None:
            non_us_weight = f"{max(0.0, 100.0 - us_weight_num - em_weight_num):.2f}%"
    return {
        "number_of_stocks": number_of_stocks,
        "top_holdings": list(seeded.get("top_holdings") or []),
        "country_weights": list(seeded.get("country_weights") or []),
        "sector_weights": list(seeded.get("sector_weights") or []),
        "top_10_weight": seeded.get("top_10_weight"),
        "us_weight": seeded.get("us_weight"),
        "non_us_weight": non_us_weight,
        "em_weight": seeded.get("em_weight"),
    }


def _quick_peer_compare_pack(
    *,
    truth,
    sleeve_key: str,
    benchmark_full_name: str | None,
    exposure_label: str | None,
    implementation_profile: dict[str, Any],
    source_authority_fields: list[dict[str, Any]],
    primary_document_manifest: list[dict[str, Any]],
    aum_usd: Any,
    what_it_still_needs_to_prove: str,
) -> dict[str, Any]:
    symbol_upper = str(getattr(truth, "symbol", "") or "").strip().upper()
    benchmark = _plain_sentence(benchmark_full_name or implementation_profile.get("mandate_or_index"))
    holdings_count = _count_label(
        _authority_value(source_authority_fields, "holdings_count")
        or (truth.metrics or {}).get("holdings_count")
    ) or _QUICK_PEER_COMPARE_SEEDS.get(symbol_upper, {}).get("composition", {}).get("number_of_stocks")
    subject_ter_number = _safe_float(implementation_profile.get("expense_ratio"))
    if subject_ter_number is None:
        subject_ter_number = _safe_float(
            _authority_value(source_authority_fields, "expense_ratio")
            or (truth.metrics or {}).get("expense_ratio")
        )
    subject_row = {
        "role": "subject",
        "fund_name": getattr(truth, "name", None),
        "ticker_or_line": getattr(truth, "symbol", None),
        "isin": _QUICK_PEER_COMPARE_SEEDS.get(symbol_upper, {}).get("subject_isin")
        or ("IE00B4L5Y983" if symbol_upper == "IWDA" else None),
        "benchmark": benchmark,
        "benchmark_family": _quick_benchmark_family(benchmark_full_name, exposure_label, truth),
        "exposure_scope": _plain_sentence(exposure_label),
        "developed_only": True if symbol_upper == "IWDA" else None,
        "emerging_markets_included": False if symbol_upper == "IWDA" else None,
        "ter": _expense_ratio_label(subject_ter_number),
        "fund_assets": _plain_sentence(implementation_profile.get("aum") or _aum_label(aum_usd)),
        "share_class_assets": _aum_label((truth.metrics or {}).get("aum_usd")),
        "holdings_count": holdings_count,
        "replication": _plain_sentence(implementation_profile.get("replication_method")),
        "distribution": _plain_sentence(implementation_profile.get("distribution_policy")),
        "domicile": _quick_fund_domicile(truth=truth, implementation_profile=implementation_profile),
        "launch_date": _plain_sentence(implementation_profile.get("launch_date") or (truth.metrics or {}).get("launch_date")),
        "tracking_error_1y": _percent_value_label(_authority_value(source_authority_fields, "tracking_error_1y") or (truth.metrics or {}).get("tracking_error_1y")),
        "tracking_error_3y": None,
        "tracking_error_5y": None,
        "tracking_difference_1y": _percent_value_label(_authority_value(source_authority_fields, "tracking_difference_1y") or (truth.metrics or {}).get("tracking_difference_1y"), signed=True),
        "tracking_difference_3y": _percent_value_label(_authority_value(source_authority_fields, "tracking_difference_3y") or (truth.metrics or {}).get("tracking_difference_3y"), signed=True),
        "listing_exchange": _plain_sentence(implementation_profile.get("primary_listing_exchange")),
        "listing_currency": _plain_sentence(implementation_profile.get("primary_trading_currency") or getattr(truth, "base_currency", None)),
        "primary_document_links": _doc_link_rows(primary_document_manifest),
        "why_this_peer_matters": _plain_sentence(what_it_still_needs_to_prove),
        "ter_delta": None,
        "fund_assets_delta": None,
        "holdings_delta": None,
        "tracking_error_1y_delta": None,
        "same_index": True,
        "same_job": True,
        "same_distribution": True,
        "same_domicile": True,
    }
    seeded = dict(_QUICK_PEER_COMPARE_SEEDS.get(symbol_upper, {}) or {})
    if seeded:
        peer_rows: list[dict[str, Any]] = []
        subject_ter = subject_row.get("ter")
        subject_holdings_numeric = _safe_float(str(subject_row.get("holdings_count") or "").replace(",", ""))
        for peer in list(seeded.get("peers") or []):
            item = dict(peer)
            peer_ter_number = _safe_float(str(item.get("ter") or "").replace("%", "")) / 100.0 if str(item.get("ter") or "").strip() else None
            subject_ter_number = _safe_float(str(subject_ter or "").replace("%", "")) / 100.0 if str(subject_ter or "").strip() else None
            item["ter_delta"] = (
                f"{(peer_ter_number - subject_ter_number):+.2%}"
                if peer_ter_number is not None and subject_ter_number is not None
                else None
            )
            peer_holdings_numeric = _safe_float(str(item.get("holdings_count") or "").replace(",", ""))
            item["holdings_delta"] = (
                f"{int(round(peer_holdings_numeric - subject_holdings_numeric)):+,}"
                if peer_holdings_numeric is not None and subject_holdings_numeric is not None
                else None
            )
            peer_rows.append(item)
        return {
            "candidate_symbol": getattr(truth, "symbol", None),
            "candidate_label": getattr(truth, "name", None),
            "primary_question": seeded.get("primary_question") or what_it_still_needs_to_prove,
            "comparison_basis": seeded.get("comparison_basis") or "Same-job peers first, then broader control.",
            "rows": [subject_row, *peer_rows],
        }
    peer_rows = []
    for row in _quick_peer_rows(getattr(truth, "symbol", ""), sleeve_key)[:3]:
        peer_rows.append(
            {
                "role": "same_job_peer",
                "fund_name": row.get("name"),
                "ticker_or_line": row.get("symbol"),
                "isin": row.get("isin"),
                "benchmark": row.get("benchmark"),
                "benchmark_family": None,
                "exposure_scope": _plain_sentence(exposure_label),
                "developed_only": None,
                "emerging_markets_included": None,
                "ter": _expense_ratio_label(row.get("expense_ratio")),
                "fund_assets": _aum_label(row.get("aum_usd")),
                "share_class_assets": None,
                "holdings_count": _count_label(row.get("holdings_count")),
                "replication": _plain_sentence(row.get("replication_method")),
                "distribution": _plain_sentence(row.get("accumulation_or_distribution")),
                "domicile": _plain_sentence(row.get("domicile")),
                "launch_date": _plain_sentence(row.get("launch_date")),
                "tracking_error_1y": None,
                "tracking_error_3y": None,
                "tracking_error_5y": None,
                "tracking_difference_1y": None,
                "tracking_difference_3y": None,
                "listing_exchange": _plain_sentence(row.get("primary_listing_exchange")),
                "listing_currency": _plain_sentence(row.get("primary_trading_currency")),
                "primary_document_links": [],
                "why_this_peer_matters": _plain_sentence(
                    f"{_peer_label(row)} pressures the same sleeve job and is the first substitute check before preferring the subject line."
                ),
                "ter_delta": None,
                "fund_assets_delta": None,
                "holdings_delta": None,
                "tracking_error_1y_delta": None,
                "same_index": None,
                "same_job": True,
                "same_distribution": None,
                "same_domicile": None,
            }
        )
    return {
        "candidate_symbol": getattr(truth, "symbol", None),
        "candidate_label": getattr(truth, "name", None),
        "primary_question": what_it_still_needs_to_prove,
        "comparison_basis": "Closest same-sleeve substitutes first.",
        "rows": [subject_row, *peer_rows],
    }


def _quick_peer_rows(symbol: str, sleeve_key: str) -> list[dict[str, Any]]:
    with _connection() as conn:
        ensure_candidate_registry_tables(conn)
        rows = export_live_candidate_registry(conn)
        if not rows:
            seed_default_candidate_registry(conn)
            rows = export_live_candidate_registry(conn)
    return [
        row
        for row in rows
        if str(row.get("symbol") or "").strip().upper() != str(symbol or "").strip().upper()
        and str(row.get("sleeve_key") or "").strip() == str(sleeve_key or "").strip()
    ]


def _peer_label(row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol") or "").strip().upper()
    name = str(row.get("name") or "").strip()
    if symbol and name:
        return f"{name} ({symbol})"
    return name or symbol or "a close substitute"


def _quick_why_this_matters(
    *,
    symbol: str,
    exposure_label: str | None,
    portfolio_role: str | None,
) -> str:
    override = _QUICK_COMPARE_OVERRIDES.get(str(symbol or "").strip().upper(), {}).get("why_this_matters")
    if override:
        return _plain_sentence(override)
    exposure = str(exposure_label or "the current sleeve exposure").strip().lower()
    role = str(portfolio_role or "the current sleeve job").strip().rstrip(".")
    return _plain_sentence(
        f"The decision is whether this ETF is the cleanest way to buy {exposure} for {role.lower()}."
    )


def _quick_compare_first(
    *,
    symbol: str,
    sleeve_key: str,
    exposure_label: str | None,
) -> str:
    override = _QUICK_COMPARE_OVERRIDES.get(str(symbol or "").strip().upper(), {}).get("compare_first")
    if override:
        return _plain_sentence(override)
    peers = _quick_peer_rows(symbol, sleeve_key)
    named_peers = [_peer_label(row) for row in peers[:2] if _peer_label(row)]
    if len(named_peers) >= 2:
        return _plain_sentence(
            f"Compare first against {named_peers[0]} and {named_peers[1]}. "
            "They pressure the same sleeve job, so the question is whether this line is really the cleanest "
            "implementation once cost, spread, tracking, wrapper, and evidence quality are compared."
        )
    if named_peers:
        return _plain_sentence(
            f"Compare first against {named_peers[0]}. "
            "The question is whether this line wins on implementation quality and decision readiness rather than on exposure alone."
        )
    exposure = str(exposure_label or "the same exposure").strip().lower()
    return _plain_sentence(
        f"Compare first against the closest ETFs that do the same {exposure} job. "
        "The question is whether this line wins on cost, spread, tracking, wrapper, and source confidence rather than on label alone."
    )


def _quick_broader_alternative(
    *,
    symbol: str,
    exposure_label: str | None,
) -> str | None:
    override = _QUICK_COMPARE_OVERRIDES.get(str(symbol or "").strip().upper(), {}).get("broader_alternative")
    if override:
        return _plain_sentence(override)
    exposure = str(exposure_label or "").strip().lower()
    if "developed-market equity" in exposure or "msci world" in exposure:
        return _plain_sentence(
            "Also check a broader all-world fund if the real decision is one-fund global equity rather than developed markets only."
        )
    if "u.s. large cap" in exposure or "s&p 500" in exposure:
        return _plain_sentence(
            "Also check a broader all-world or global core line if the real decision is total equity exposure rather than U.S. large-cap only."
        )
    return None


def _quick_what_it_solves(
    *,
    symbol: str,
    exposure_label: str | None,
    portfolio_role: str | None,
) -> str:
    override = _QUICK_COMPARE_OVERRIDES.get(str(symbol or "").strip().upper(), {}).get("what_it_solves")
    if override:
        return _plain_sentence(override)
    exposure = str(exposure_label or "the sleeve exposure").strip().lower()
    role = str(portfolio_role or "the sleeve job").strip().rstrip(".")
    return _plain_sentence(f"Use when the portfolio needs {exposure} as {role.lower()}.")


def _quick_what_it_still_needs_to_prove(
    *,
    symbol: str,
    decision_reasons: list[str],
) -> str:
    override = _QUICK_COMPARE_OVERRIDES.get(str(symbol or "").strip().upper(), {}).get("what_it_still_needs_to_prove")
    if override:
        return _plain_sentence(override)
    reason = str(decision_reasons[0] or "").strip().lower() if decision_reasons else "its implementation is good enough to beat close substitutes"
    return _plain_sentence(
        f"It still needs to prove that {reason}, and that it wins the first comparison screen on cost, spread, tracking, wrapper, and evidence quality."
    )


def _quick_decision_readiness(
    *,
    symbol: str,
    status_label: str,
    compare_first: str,
    decision_reasons: list[str],
) -> str:
    override = _QUICK_COMPARE_OVERRIDES.get(str(symbol or "").strip().upper(), {}).get("decision_readiness")
    if override:
        return _plain_sentence(override)
    primary_reason = str(decision_reasons[0] or "").strip().lower() if decision_reasons else "the current read still needs more verification"
    compare_pressure = "the closest substitutes"
    lowered = compare_first.lower()
    if "against " in lowered:
        compare_pressure = compare_first.split("against ", 1)[1].split(".", 1)[0]
    return _plain_sentence(f"{status_label} until {primary_reason}, and the brief shows why it beats {compare_pressure}.")


def _quick_key_facts(
    *,
    truth,
    benchmark_full_name: str | None,
    ter_bps: Any,
    aum_usd: Any,
    implementation_profile: dict[str, Any],
    source_authority_fields: list[dict[str, Any]],
) -> list[dict[str, str]]:
    holdings_count = _count_label(_authority_value(source_authority_fields, "holdings_count"))
    fund_domicile = _quick_fund_domicile(truth=truth, implementation_profile=implementation_profile)
    rows = [
        ("Benchmark", benchmark_full_name or implementation_profile.get("mandate_or_index")),
        ("Expense ratio", _expense_ratio_label((float(ter_bps) / 10_000.0) if ter_bps is not None else None)),
        ("AUM", implementation_profile.get("aum") or _aum_label(aum_usd)),
        ("Replication", implementation_profile.get("replication_method")),
        ("Distribution", implementation_profile.get("distribution_policy")),
        ("Domicile", fund_domicile),
        ("Trading currency", implementation_profile.get("primary_trading_currency")),
        ("Holdings", holdings_count),
        ("Listing", implementation_profile.get("primary_listing_exchange")),
        ("Launch date", implementation_profile.get("launch_date")),
    ]
    return [
        {"label": str(label), "value": _plain_sentence(value)}
        for label, value in rows
        if str(value or "").strip()
    ][:10]


def _quick_should_i_use(
    *,
    exposure_label: str | None,
    portfolio_role: str | None,
    distribution_policy: str | None,
    domicile: str | None,
    status_state: str,
    decision_reasons: list[str],
) -> dict[str, str]:
    exposure = str(exposure_label or "this ETF").strip()
    exposure_lower = exposure.lower()
    distribution = str(distribution_policy or "").strip().lower()
    domicile_label = str(domicile or "").strip()
    role_sentence = str(portfolio_role or "a portfolio building block").strip().rstrip(".")
    accumulating = "accum" in distribution
    if "u.s. large cap" in exposure_lower or "s&p 500" in exposure_lower:
        not_ideal_for = "Investors who want all-world equity, ex-U.S. diversification, or cash distributions."
    elif "developed-market equity" in exposure_lower or "msci world" in exposure_lower:
        not_ideal_for = "Investors who need emerging-market exposure, income distributions, or a single all-world solution."
    elif "global equity" in exposure_lower or "acwi" in exposure_lower:
        not_ideal_for = "Investors who want cash distributions, a narrower regional allocation, or a concentrated active view."
    elif "treasury" in exposure_lower or "bill" in exposure_lower:
        not_ideal_for = "Investors who want longer-duration income, credit spread carry, or equity upside."
    else:
        not_ideal_for = "Investors whose mandate, payout needs, or regional exposure goal does not match this ETF."
    best_for_suffix = []
    if domicile_label:
        best_for_suffix.append(f"a {domicile_label}-domiciled wrapper")
    if accumulating:
        best_for_suffix.append("an accumulating share class")
    suffix = f" through {', '.join(best_for_suffix)}" if best_for_suffix else ""
    wait_reason = decision_reasons[0].lower() if decision_reasons else "the remaining evidence is not clean enough yet"
    compare_against = "Close substitutes that do the same exposure job, then pressure this line on fee, spread, tracking, wrapper, and trading line."
    return {
        "best_for": _plain_sentence(f"Long-term investors who specifically want {exposure} exposure{suffix}."),
        "not_ideal_for": _plain_sentence(not_ideal_for),
        "use_it_when": _plain_sentence(
            f"Use it when you want {exposure} as {role_sentence.lower()}."
        ),
        "wait_if": _plain_sentence(
            f"Wait if {wait_reason}, or if a close substitute offers clearly better implementation terms."
            if status_state != "eligible"
            else f"Wait only if a close substitute offers clearly better implementation terms for the same exposure."
        ),
        "compare_against": _plain_sentence(compare_against),
    }


def _quick_performance_checks(
    *,
    symbol: str,
    status_label: str,
    decision_reasons: list[str],
    implementation_profile: dict[str, Any],
    fund_domicile: str | None,
    ter_bps: Any,
    aum_usd: Any,
    portfolio_role: str | None,
) -> list[dict[str, Any]]:
    expense_ratio = _expense_ratio_label((float(ter_bps) / 10_000.0) if ter_bps is not None else None)
    spread_proxy = str(implementation_profile.get("spread_proxy") or "").strip() or None
    tracking_difference = str(implementation_profile.get("tracking_difference") or "").strip() or None
    aum_value = str(implementation_profile.get("aum") or _aum_label(aum_usd) or "").strip() or None
    structure_bits = [
        str(fund_domicile or "").strip() or None,
        str(implementation_profile.get("replication_method") or "").strip() or None,
        str(implementation_profile.get("distribution_policy") or "").strip() or None,
        "UCITS" if "ucits" in str(implementation_profile.get("mandate_or_index") or "").strip().lower() else None,
    ]
    readiness_reason = decision_reasons[0] if decision_reasons else "the current read still needs more verification"
    symbol_upper = str(symbol or "").strip().upper()
    cost_summary = (
        "0.20% TER; the key question is whether IWDA's scale and trading comfort justify paying more than cheaper MSCI World peers."
        if symbol_upper == "IWDA"
        else (
            f"{expense_ratio} TER, but fee only matters if the line also holds up on spread, tracking, and wrapper fit."
            if expense_ratio
            else "Expense ratio is not surfaced strongly enough yet, so cost still needs direct comparison against peers."
        )
    )
    rows = [
        {
            "check_id": "cost",
            "label": "Cost",
            "summary": _plain_sentence(cost_summary),
            "metric": expense_ratio,
        },
        {
            "check_id": "liquidity",
            "label": "Liquidity",
            "summary": _plain_sentence(
                f"{spread_proxy} spread proxy; use it to judge whether the cheaper line stays cheaper after trading costs."
                if spread_proxy
                else "Spread and trading-line liquidity still need a cleaner read before this ETF can win the implementation case."
            ),
            "metric": spread_proxy,
        },
        {
            "check_id": "tracking",
            "label": "Tracking",
            "summary": _plain_sentence(
                f"{tracking_difference} tracking difference on the currently surfaced lookback; fee advantage only matters if tracking quality also holds up."
                if tracking_difference
                else "Tracking difference is not surfaced strongly enough yet, so the fee comparison is still incomplete."
            ),
            "metric": tracking_difference,
        },
        {
            "check_id": "size_and_survivability",
            "label": "Size and survivability",
            "summary": _plain_sentence(
                f"AUM reads {aum_value}, which supports survivability and trading depth but does not settle the fee-vs-implementation tradeoff on its own."
                if aum_value
                else "AUM is not surfaced strongly enough yet."
            ),
            "metric": aum_value,
        },
        {
            "check_id": "structure",
            "label": "Structure",
            "summary": _plain_sentence(
                f"{', '.join([bit for bit in structure_bits if bit])}. The wrapper only matters if it fits the account, mandate, and payout needs better than close substitutes."
                if any(structure_bits)
                else "Wrapper and implementation structure still need a cleaner read."
            ),
            "metric": None,
        },
        {
            "check_id": "portfolio_fit",
            "label": "Portfolio fit",
            "summary": _plain_sentence(portfolio_role or "Portfolio fit still needs to be judged against the active sleeve objective."),
            "metric": None,
        },
        {
            "check_id": "decision_readiness",
            "label": "Decision readiness",
            "summary": _plain_sentence(f"{status_label} while {readiness_reason.lower()}."),
            "metric": status_label,
        },
    ]
    return rows


def _quick_what_you_are_buying(
    *,
    truth,
    benchmark_full_name: str | None,
    implementation_profile: dict[str, Any],
    source_authority_fields: list[dict[str, Any]],
) -> list[dict[str, str]]:
    holdings_count = _count_label(_authority_value(source_authority_fields, "holdings_count"))
    fund_domicile = _quick_fund_domicile(truth=truth, implementation_profile=implementation_profile)
    rows = [
        ("Index tracked", benchmark_full_name or implementation_profile.get("mandate_or_index")),
        ("Holdings count", holdings_count),
        ("Replication method", implementation_profile.get("replication_method")),
        ("Distribution policy", implementation_profile.get("distribution_policy")),
        ("Domicile", fund_domicile),
        ("Trading currency", implementation_profile.get("primary_trading_currency")),
        ("Listing", implementation_profile.get("primary_listing_exchange")),
        ("Launch date", implementation_profile.get("launch_date")),
    ]
    return [
        {"label": str(label), "value": _plain_sentence(value)}
        for label, value in rows
        if str(value or "").strip()
    ]


def _quick_portfolio_fit(
    *,
    portfolio_role: str | None,
    exposure_label: str | None,
    overlay_context: dict[str, Any] | None,
) -> dict[str, str]:
    overlay_state = str(dict(overlay_context or {}).get("state") or "").strip().lower()
    exposure = str(exposure_label or "this exposure").strip().lower()
    if "u.s. large cap" in exposure or "s&p 500" in exposure:
        does_not_solve = "Does not replace ex-U.S. or all-world exposure on its own."
    elif "developed-market equity" in exposure or "msci world" in exposure:
        does_not_solve = "Does not add emerging-market exposure or solve income needs on its own."
    elif "global equity" in exposure or "acwi" in exposure:
        does_not_solve = "Does not replace a dedicated income, factor, or region-specific allocation."
    elif "treasury" in exposure or "bill" in exposure:
        does_not_solve = "Does not replace longer-duration income or broader bond diversification."
    else:
        does_not_solve = "Does not solve every portfolio need on its own."
    current_need = (
        "Portfolio overlay is unavailable, so current need is still judged at the fund and sleeve level."
        if overlay_state in {"overlay_absent", ""}
        else _plain_sentence(dict(overlay_context or {}).get("summary") or "Current need depends on the live portfolio overlay.")
    )
    return {
        "role_in_portfolio": _plain_sentence(portfolio_role or "Portfolio role still needs a cleaner sleeve-level read."),
        "what_it_does_not_solve": _plain_sentence(does_not_solve),
        "current_need": _plain_sentence(current_need),
    }


def _quick_how_to_decide(
    *,
    exposure_label: str | None,
    distribution_policy: str | None,
    domicile: str | None,
) -> list[str]:
    exposure = str(exposure_label or "this exposure").strip()
    distribution = str(distribution_policy or "the current payout structure").strip()
    domicile_label = str(domicile or "this wrapper").strip()
    return [
        _plain_sentence(f"Do I want {exposure.lower()} specifically, or do I need a broader fund?"),
        _plain_sentence(f"Do I want {distribution.lower()}, or a different payout structure?"),
        _plain_sentence(f"Is {domicile_label} the right wrapper for my account and jurisdiction?"),
        "Are cost, spread, and tracking competitive against close substitutes?",
        "Does my current portfolio actually need more of this exposure right now?",
    ]


def _quick_evidence_footer(
    *,
    source_integrity_summary: dict[str, Any],
    primary_document_manifest: list[dict[str, Any]],
    freshness_state: str | None,
) -> dict[str, str]:
    freshness_raw = str(freshness_state or "").strip().lower()
    integrity = str(source_integrity_summary.get("integrity_label") or source_integrity_summary.get("state") or "").strip().lower()
    if integrity == "strong":
        evidence_quality = "Strong enough to support a live preference."
    elif integrity in {"mixed", "thin", "weak"}:
        evidence_quality = "Adequate for watchlist work, but not yet strong enough for a firm preference."
    elif integrity == "conflicted":
        evidence_quality = "Not strong enough for a firm preference while important source conflicts remain."
    else:
        evidence_quality = "Evidence quality is not yet surfaced strongly enough."
    docs = [
        _plain_sentence(str(doc.get("doc_type") or "").replace("_", " "))
        for doc in primary_document_manifest
        if str(doc.get("status") or "").strip().lower() in _USABLE_DOCUMENT_STATUSES
    ]
    docs = [doc for doc in docs if doc]
    document_support = (
        f"{', '.join(dict.fromkeys(docs[:3]))} available."
        if docs
        else "Document support is still thin."
    )
    monitoring_status = (
        "Current stored context remains usable."
        if freshness_raw == "stored_valid_context"
        else "Running in degraded monitoring mode."
        if freshness_raw == "degraded_monitoring_mode"
        else "Freshness is not surfaced strongly enough yet."
    )
    return {
        "evidence_quality": evidence_quality,
        "data_completeness": f"{int(source_integrity_summary.get('critical_fields_ready') or 0)} of {int(source_integrity_summary.get('critical_fields_total') or 0)} decision-critical fields currently populated.",
        "document_support": document_support,
        "monitoring_status": monitoring_status,
    }


def _support_state_entry(state: str, reason: str) -> dict[str, str]:
    normalized = str(state or "").strip().lower()
    if normalized not in {
        "direct",
        "derived",
        "proxy",
        "partial",
        "stale",
        "verified_not_applicable",
        "unavailable_with_verified_reason",
    }:
        normalized = "unavailable_with_verified_reason"
    return {"state": normalized, "reason": _plain_sentence(reason) or "Support state was resolved by the report contract."}


def _value_count(mapping: dict[str, Any], keys: list[str] | tuple[str, ...]) -> int:
    count = 0
    for key in keys:
        value = mapping.get(key)
        if value is None or value == "":
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        count += 1
    return count


def _build_deep_report_support_state(
    *,
    primary_document_manifest: list[dict[str, Any]],
    quick_brief: dict[str, Any],
    implementation_profile: dict[str, Any],
    source_authority_fields: list[dict[str, Any]],
    evidence_sources: list[dict[str, Any]],
    market_path_support: dict[str, Any] | None,
    coverage_summary: dict[str, Any],
    forecast_support: dict[str, Any] | None,
    scenario_blocks: list[dict[str, Any]],
) -> dict[str, Any]:
    usable_docs = [
        dict(item)
        for item in list(primary_document_manifest or [])
        if str(dict(item).get("status") or "").strip().lower() in _USABLE_DOCUMENT_STATUSES
    ]
    failed_docs = [
        dict(item)
        for item in list(primary_document_manifest or [])
        if str(dict(item).get("status") or "").strip().lower()
        and str(dict(item).get("status") or "").strip().lower() not in _USABLE_DOCUMENT_STATUSES
    ]
    doc_state = (
        _support_state_entry("direct", f"{len(usable_docs)} usable primary document{'s' if len(usable_docs) != 1 else ''} carried into the report.")
        if usable_docs
        else _support_state_entry("partial", "Only failed or unusable document references are present.")
        if failed_docs
        else _support_state_entry("unavailable_with_verified_reason", "No primary document manifest is available for this candidate report.")
    )

    performance_pack = dict(dict(quick_brief or {}).get("performance_tracking_pack") or {})
    performance_keys = (
        "return_1y",
        "return_3y",
        "return_5y",
        "benchmark_return_1y",
        "tracking_error_1y",
        "tracking_difference_current_period",
        "tracking_difference_1y",
        "tracking_difference_3y",
        "tracking_difference_5y",
        "volatility",
        "max_drawdown",
    )
    tracking_authority = [
        item for item in list(source_authority_fields or [])
        if str(dict(item).get("field_name") or "").startswith(("tracking_", "return_", "volatility", "max_drawdown"))
        and dict(item).get("resolved_value") not in {None, ""}
    ]
    performance_count = _value_count(performance_pack, performance_keys)
    if performance_count >= 3:
        performance_state = _support_state_entry("direct", "Performance and tracking pack has multiple populated measures.")
    elif performance_count or tracking_authority:
        performance_state = _support_state_entry("derived", "Performance support is present through available tracking or derived market evidence.")
    else:
        performance_state = _support_state_entry("partial", "Performance section is present but still lacks full return and tracking history.")

    composition_pack = dict(dict(quick_brief or {}).get("composition_pack") or {})
    composition_count = _value_count(
        composition_pack,
        (
            "number_of_stocks",
            "top_holdings",
            "country_weights",
            "sector_weights",
            "top_10_weight",
            "us_weight",
            "non_us_weight",
            "em_weight",
        ),
    )
    composition_state = (
        _support_state_entry("direct", "Composition support is populated from holdings or seeded official exposure context.")
        if composition_count >= 2
        else _support_state_entry("partial", "Composition support is product-aware but still incomplete.")
        if composition_count
        else _support_state_entry("verified_not_applicable", "Composition detail is not applicable or not decision-critical for this structure-first report section.")
    )

    listing_profile = dict(dict(quick_brief or {}).get("listing_profile") or {})
    listing_count = _value_count(listing_profile, ("exchange", "trading_currency", "ticker", "market_price", "spread_proxy", "volume", "premium_discount"))
    listing_state = (
        _support_state_entry("direct", "Listing profile has exchange, currency, ticker, and execution-cost support.")
        if listing_profile.get("exchange") and listing_profile.get("trading_currency") and listing_profile.get("ticker") and listing_profile.get("spread_proxy")
        else _support_state_entry("partial", "Listing profile has core identifiers but lacks some live quote, NAV, volume, or premium-discount detail.")
        if listing_count
        else _support_state_entry("unavailable_with_verified_reason", "Listing profile fields are not available in persisted truth.")
    )

    missing_impl = list(implementation_profile.get("missing_fields") or [])
    execution_confidence = str(implementation_profile.get("execution_confidence") or "").strip().lower()
    implementation_state = (
        _support_state_entry("direct", "Implementation profile is usable with no unresolved implementation fields.")
        if not missing_impl and execution_confidence in {"strong", "usable"}
        else _support_state_entry("partial", "Implementation profile is usable but still carries missing or degraded execution fields.")
    )

    support = dict(market_path_support or {})
    coverage_status = str(dict(coverage_summary or {}).get("coverage_status") or "").strip().lower()
    route_state = str(support.get("route_state") or support.get("market_setup_state") or "").strip().lower()
    suppression_reason = str(support.get("suppression_reason") or "").strip()
    uses_proxy = bool(support.get("uses_proxy_series") or dict(support.get("series_quality_summary") or {}).get("uses_proxy_series"))
    served_last_good = bool(dict(support.get("model_metadata") or {}).get("last_good_artifact_served"))
    if support and uses_proxy:
        market_state = _support_state_entry("proxy", "Market-path route is available through a disclosed proxy series.")
    elif support and route_state in {"direct_usable", "direct", "ready", "usable"}:
        market_state = _support_state_entry("direct", "Market-path artifact is available for the direct route.")
    elif support and served_last_good:
        market_state = _support_state_entry("stale", "Market-path support is served from last-good artifact, not a fresh run.")
    elif support:
        market_state = _support_state_entry("partial", f"Market-path route exists, but forecast support is constrained by {suppression_reason or route_state or 'artifact quality'}.")
    elif coverage_status == "proxy_ready":
        market_state = _support_state_entry("proxy", "Route/history coverage is proxy-ready, but no current forecast artifact is available.")
    elif coverage_status == "direct_ready":
        market_state = _support_state_entry("partial", "Direct route/history coverage is ready, but no current forecast artifact is available.")
    else:
        market_state = _support_state_entry("unavailable_with_verified_reason", f"Market-path coverage is {coverage_status or 'unavailable'} and no forecast artifact is available.")

    evidence_state = (
        _support_state_entry("direct", "Evidence sources include primary issuer documents or direct citations.")
        if evidence_sources and any(str(dict(item).get("directness") or "").strip() == "direct" for item in evidence_sources)
        else _support_state_entry("partial", "Evidence sources are present but mostly indirect.")
        if evidence_sources
        else _support_state_entry("unavailable_with_verified_reason", "No evidence sources were emitted for this report.")
    )

    section_states = {
        "documents": doc_state,
        "performance": performance_state,
        "composition": composition_state,
        "listing": listing_state,
        "implementation": implementation_state,
        "market_path": market_state,
        "evidence_sources": evidence_state,
    }
    stale_sections = [key for key, value in section_states.items() if value["state"] == "stale"]
    proxy_sections = [key for key, value in section_states.items() if value["state"] == "proxy"]
    verified_not_applicable_sections = [key for key, value in section_states.items() if value["state"] == "verified_not_applicable"]
    unavailable_sections = [key for key, value in section_states.items() if value["state"] == "unavailable_with_verified_reason"]
    partial_sections = [key for key, value in section_states.items() if value["state"] == "partial"]
    if unavailable_sections:
        overall_state = "unavailable_with_verified_reason"
    elif stale_sections:
        overall_state = "stale"
    elif partial_sections:
        overall_state = "partial"
    else:
        overall_state = "complete"
    forecast_runtime_state = (
        "served_last_good"
        if served_last_good
        else "failed"
        if suppression_reason == "model_execution_failed"
        else "unavailable_with_verified_reason"
        if not support
        else "ready"
        if not suppression_reason
        else "constrained"
    )
    forecast_artifact_state = "current" if support and not served_last_good and not suppression_reason else "stale" if served_last_good else "unavailable_with_verified_reason" if not support else "partial"
    scenario_support_state = "partial" if scenario_blocks else "unavailable_with_verified_reason"
    return {
        "overall_state": overall_state,
        "documents_support_state": doc_state["state"],
        "performance_support_state": performance_state["state"],
        "composition_support_state": composition_state["state"],
        "listing_support_state": listing_state["state"],
        "implementation_support_state": implementation_state["state"],
        "market_path_support_state": market_state["state"],
        "evidence_sources_support_state": evidence_state["state"],
        "section_states": section_states,
        "unresolved_sections": [],
        "stale_sections": stale_sections,
        "proxy_sections": proxy_sections,
        "verified_not_applicable_sections": verified_not_applicable_sections,
        "unavailable_sections": unavailable_sections,
        "partial_sections": partial_sections,
        "support_reasons": [value["reason"] for value in section_states.values()],
        "source_completion_is_report_support": False,
        "coverage_status": coverage_status or None,
        "forecast_runtime_state": forecast_runtime_state,
        "forecast_artifact_state": forecast_artifact_state,
        "scenario_support_state": scenario_support_state,
        "market_path_unavailable_reason": suppression_reason or None,
    }


def _quick_scenario_entry(market_path_support: dict[str, Any] | None) -> dict[str, str]:
    support = dict(market_path_support or {})
    usefulness = str(support.get("usefulness_label") or "").strip().lower()
    if usefulness == "strong":
        backdrop = "Backdrop supportive."
    elif usefulness == "usable":
        backdrop = "Backdrop mildly supportive."
    elif usefulness == "usable_with_caution":
        backdrop = "Backdrop only bounded support."
    elif usefulness == "unstable":
        backdrop = "Backdrop fragile."
    else:
        backdrop = "Backdrop unavailable."
    implication = str(support.get("candidate_implication") or "").strip()
    summary = _plain_sentence(f"{backdrop} {implication}" if implication else backdrop)
    return {
        "backdrop_summary": summary,
        "disclosure_label": "Open scenario view",
    }


def _kronos_scope(
    *,
    sleeve_key: str,
    benchmark_full_name: str | None,
    exposure_label: str | None,
    market_path_support: dict[str, Any] | None,
) -> tuple[str, str]:
    support = dict(market_path_support or {})
    emitted_key = str(support.get("scope_key") or "").strip()
    emitted_label = str(support.get("scope_label") or "").strip()
    if emitted_key and emitted_label:
        return emitted_key, emitted_label
    raw = " ".join(
        part for part in [str(benchmark_full_name or "").strip(), str(exposure_label or "").strip(), sleeve_key.replace("_", " ").strip()] if part
    ).lower()
    if any(token in raw for token in ["gold", "bullion"]):
        return "gold", "gold"
    if any(token in raw for token in ["s&p 500", "sp 500", "u.s. large", "us large", "large cap us"]):
        return "us_large_cap", "broad U.S. large-cap equity"
    if "all world" in raw or "all-world" in raw:
        return "all_world_equity", "all-world equity"
    if any(token in raw for token in ["world", "developed"]) or sleeve_key in {"global_equity_core", "developed_ex_us_optional"}:
        return "developed_markets_equity", "broad developed markets"
    if any(token in raw for token in ["treasury", "t-bill", "bill", "cash"]) or sleeve_key == "cash_bills":
        return "short_duration_treasury", "short-duration cash alternatives"
    if "bond" in raw or sleeve_key == "ig_bonds":
        return "investment_grade_bonds", "investment-grade bonds"
    label = _plain_sentence(exposure_label or benchmark_full_name or sleeve_key.replace("_", " "))
    return _slug(label), label.lower()


def _kronos_horizon_label(market_path_support: dict[str, Any] | None) -> str:
    support = dict(market_path_support or {})
    timestamps = list(support.get("output_timestamps") or [])
    horizon = len(timestamps) or int(support.get("forecast_horizon") or 0)
    if horizon <= 0:
        return "Horizon unavailable"
    return f"{horizon} trading day{'s' if horizon != 1 else ''}"


def _kronos_market_setup_state(market_path_support: dict[str, Any] | None) -> str:
    support = dict(market_path_support or {})
    state = str(support.get("market_setup_state") or "").strip().lower()
    if state:
        return state
    usefulness = str(support.get("usefulness_label") or "").strip().lower()
    if usefulness == "suppressed":
        return "unavailable"
    quality = dict(support.get("series_quality_summary") or {})
    if bool(quality.get("uses_proxy_series")):
        return "proxy_usable"
    return "direct_usable"


def _kronos_base_endpoint_delta_pct(market_path_support: dict[str, Any] | None) -> float | None:
    support = dict(market_path_support or {})
    endpoints = list(support.get("scenario_endpoint_summary") or [])
    base = next((item for item in endpoints if str(item.get("scenario_type") or "").strip().lower() == "base"), None)
    value = (base or {}).get("endpoint_delta_pct")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _kronos_confidence_label(market_path_support: dict[str, Any] | None) -> str:
    support = dict(market_path_support or {})
    state = _kronos_market_setup_state(support)
    metadata = dict(support.get("model_metadata") or {})
    emitted = str(metadata.get("confidence_label") or "").strip().lower()
    if emitted in {"high", "medium", "low", "unavailable"}:
        return emitted.title() if emitted != "unavailable" else "Unavailable"
    if state == "unavailable":
        return "Unavailable"
    if state in {"stale", "degraded"}:
        return "Low"
    if state == "proxy_usable":
        return "Medium"
    return "Medium"


def _kronos_freshness_label(market_path_support: dict[str, Any] | None) -> str:
    support = dict(market_path_support or {})
    freshness_state = str(support.get("freshness_state") or "").strip().lower()
    quality = dict(support.get("series_quality_summary") or {})
    stale_days = int(quality.get("stale_days") or 0)
    generated_at = str(support.get("generated_at") or "").strip()
    if freshness_state == "last_good":
        return "Showing last good run"
    if freshness_state == "stale" or stale_days > 1:
        return f"Stale by {max(stale_days, 1)} day{'s' if max(stale_days, 1) != 1 else ''}"
    if freshness_state == "untrusted":
        return "Freshness unresolved"
    if not generated_at:
        return "Freshness unavailable"
    try:
        parsed = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError:
        return _plain_sentence(f"Live run {generated_at}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    age = max(datetime.now(UTC) - parsed, timedelta(0))
    hours = int(age.total_seconds() // 3600)
    if hours < 1:
        minutes = max(1, int(age.total_seconds() // 60))
        return f"Live run {minutes}m ago"
    if hours < 48:
        return f"Live run {hours}h ago"
    days = max(1, age.days)
    return f"Live run {days}d ago"


def _kronos_route_truth_label(*, market_path_support: dict[str, Any] | None, scope_label: str) -> str:
    support = dict(market_path_support or {})
    state = _kronos_market_setup_state(support)
    driving_symbol = str(support.get("driving_symbol") or "").strip().upper()
    proxy_symbol = str(support.get("proxy_symbol") or "").strip().upper()
    if state == "unavailable":
        return "No usable market setup"
    if state == "proxy_usable":
        proxy = proxy_symbol or driving_symbol or "approved proxy"
        return f"Using {proxy} proxy for {scope_label} market setup"
    if state == "stale":
        return f"{driving_symbol or 'Stored'} stored market setup"
    return f"Direct {driving_symbol} market setup" if driving_symbol else "Direct market setup"


def _kronos_object_label(market_path_support: dict[str, Any] | None) -> str | None:
    support = dict(market_path_support or {})
    driving_symbol = str(support.get("driving_symbol") or "").strip().upper()
    if not driving_symbol:
        return None
    if str(support.get("driving_series_role") or "").strip() == "approved_proxy":
        return f"{driving_symbol} proxy"
    return driving_symbol


def _kronos_quality_gate(market_path_support: dict[str, Any] | None) -> str:
    support = dict(market_path_support or {})
    state = _kronos_market_setup_state(support)
    confidence = _kronos_confidence_label(support).lower()
    if state == "unavailable":
        return "unavailable"
    if state == "direct_usable" and confidence == "high":
        return "high"
    if state in {"direct_usable", "proxy_usable"}:
        return "medium"
    return "low"


def _kronos_path_support_label(market_path_support: dict[str, Any] | None) -> str:
    support = dict(market_path_support or {})
    state = _kronos_market_setup_state(support)
    usefulness = str(support.get("usefulness_label") or "").strip().lower()
    fragility = str(support.get("candidate_fragility_label") or "").strip().lower()
    drift = str(support.get("threshold_drift_direction") or "").strip().lower()
    takeaways = dict(support.get("scenario_takeaways") or {})
    base_delta_pct = _kronos_base_endpoint_delta_pct(support)
    if state == "unavailable":
        return "No usable market setup"
    if base_delta_pct is not None and base_delta_pct <= -4.0:
        return "Base path adverse"
    if usefulness == "unstable" and (fragility == "acute" or bool(takeaways.get("stress_breaks_candidate_support"))):
        return "Base path adverse"
    if usefulness in {"unstable", "usable_with_caution"} or drift == "toward_weakening":
        return "Base path fragile"
    if base_delta_pct is not None and base_delta_pct >= 2.5 and drift != "toward_weakening":
        return "Base path supportive"
    if base_delta_pct is not None and base_delta_pct > 0:
        return "Base path mildly supportive"
    return "Base path bounded"


def _kronos_downside_risk_label(market_path_support: dict[str, Any] | None) -> str:
    support = dict(market_path_support or {})
    state = _kronos_market_setup_state(support)
    usefulness = str(support.get("usefulness_label") or "").strip().lower()
    quality = dict(support.get("series_quality_summary") or {})
    takeaways = dict(support.get("scenario_takeaways") or {})
    fragility = str(support.get("candidate_fragility_label") or "").strip().lower()
    if state == "unavailable" or usefulness == "suppressed":
        return "Unavailable"
    if fragility in {"fragile", "acute"} or bool(takeaways.get("stress_breaks_candidate_support")):
        return "Elevated"
    if bool(takeaways.get("downside_damage_is_contained")):
        return "Contained"
    if bool(quality.get("has_corporate_action_uncertainty")):
        return "Watch"
    return "Bounded"


def _kronos_drift_label(market_path_support: dict[str, Any] | None) -> str:
    support = dict(market_path_support or {})
    raw = str(support.get("threshold_drift_direction") or "").strip().lower()
    if raw == "toward_strengthening":
        return "Strengthening"
    if raw == "toward_weakening":
        return "Weakening"
    return "Stable"


def _kronos_volatility_regime_label(market_path_support: dict[str, Any] | None) -> str | None:
    support = dict(market_path_support or {})
    raw = str(support.get("volatility_outlook") or "").strip()
    if not raw:
        return None
    return _plain_sentence(raw)


def _kronos_provider_label(market_path_support: dict[str, Any] | None) -> str | None:
    support = dict(market_path_support or {})
    raw = str(support.get("provider_source") or "").strip()
    if not raw:
        return None
    token_map = {
        "twelve_data": "Twelve Data",
        "approved_proxy": "approved proxy",
        "kronos": "Kronos",
        "polygon": "Polygon",
        "fmp": "FMP",
    }
    return " + ".join(token_map.get(token.strip(), token.strip().replace("_", " ").title()) for token in raw.split("+") if token.strip())


def _kronos_decision_impact_text(
    *,
    scope_label: str,
    market_path_support: dict[str, Any] | None,
    status_state: str,
    decision_reasons: list[str],
) -> str:
    support = dict(market_path_support or {})
    path_label = _kronos_path_support_label(support)
    quality_gate = _kronos_quality_gate(support)
    if path_label == "Market setup unavailable":
        return "Does not override wrapper-level decision."
    if path_label == "Base path adverse":
        return f"Does not strengthen the case for preferring this ETF over cheaper same-job peers in {scope_label}."
    if path_label == "Base path fragile":
        return f"Does not strengthen the case for preferring this ETF over cheaper same-job peers in {scope_label}."
    if quality_gate == "high" and path_label == "Base path supportive" and status_state == "eligible" and not decision_reasons:
        return f"Supports {scope_label}, but only as bounded timing context rather than a replacement for wrapper proof."
    if path_label in {"Base path supportive", "Base path mildly supportive"}:
        return f"Supports {scope_label}, but not strongly enough to justify paying more for this wrapper."
    return "Bounded context only. Does not override wrapper-level decision."


def _kronos_market_setup(
    *,
    sleeve_key: str,
    benchmark_full_name: str | None,
    exposure_label: str | None,
    market_path_support: dict[str, Any] | None,
    status_state: str,
    decision_reasons: list[str],
) -> dict[str, Any] | None:
    support = dict(market_path_support or {})
    if not support:
        return None
    scope_key, scope_label = _kronos_scope(
        sleeve_key=sleeve_key,
        benchmark_full_name=benchmark_full_name,
        exposure_label=exposure_label,
        market_path_support=support,
    )
    return {
        "scope_key": scope_key,
        "scope_label": scope_label,
        "market_setup_state": _kronos_market_setup_state(support),
        "route_label": _kronos_route_truth_label(market_path_support=support, scope_label=scope_label),
        "forecast_object_label": _kronos_object_label(support),
        "horizon_label": _kronos_horizon_label(support),
        "path_support_label": _kronos_path_support_label(support),
        "confidence_label": _kronos_confidence_label(support),
        "freshness_label": _kronos_freshness_label(support),
        "downside_risk_label": _kronos_downside_risk_label(support),
        "drift_label": _kronos_drift_label(support),
        "volatility_regime_label": _kronos_volatility_regime_label(support),
        "decision_impact_text": _kronos_decision_impact_text(
            scope_label=scope_label,
            market_path_support=support,
            status_state=status_state,
            decision_reasons=decision_reasons,
        ),
        "quality_gate": _kronos_quality_gate(support),
        "as_of": str(support.get("generated_at") or "").strip() or None,
        "scenario_available": bool(
            support.get("observed_series")
            or support.get("projected_series")
            or support.get("scenario_summary")
        ),
        "open_scenario_cta": "Open scenario view",
    }


def _kronos_decision_bridge(
    *,
    kronos_market_setup: dict[str, Any] | None,
) -> dict[str, Any] | None:
    setup = dict(kronos_market_setup or {})
    scope_label = str(setup.get("scope_label") or "").strip()
    horizon = str(setup.get("horizon_label") or "").strip()
    path_label = str(setup.get("path_support_label") or "").strip()
    decision_impact = str(setup.get("decision_impact_text") or "").strip()
    if not scope_label or not path_label:
        return None
    supports_exposure = path_label not in {"Market setup unavailable", "Base path adverse"}
    supports_wrapper = False
    if path_label == "Base path supportive" and str(setup.get("quality_gate") or "").strip().lower() == "high":
        supports_wrapper = False
    strength = "No decision change" if not supports_exposure else "Exposure reinforcement only" if not supports_wrapper else "Material but bounded"
    return {
        "selection_context": scope_label,
        "regime_summary": _plain_sentence(
            f"{str(setup.get('route_label') or 'Kronos market setup').strip()} shows {path_label.lower()} conditions for {scope_label} over {horizon}."
        ),
        "selection_consequence": decision_impact,
        "wrapper_boundary_text": "The support changes the exposure read first and the ETF wrapper read second.",
        "supports_exposure_choice": supports_exposure,
        "supports_wrapper_choice": supports_wrapper,
        "decision_strength_label": strength,
    }


def _kronos_compare_check(
    *,
    kronos_market_setup: dict[str, Any] | None,
) -> dict[str, Any] | None:
    setup = dict(kronos_market_setup or {})
    path_label = str(setup.get("path_support_label") or "").strip()
    if not path_label or path_label == "Market setup unavailable":
        return None
    scope_label = str(setup.get("scope_label") or "this exposure").strip()
    if path_label in {"Base path fragile", "Base path adverse"}:
        regime_text = f"Current Kronos conditions do not create a strong enough regime case to prefer this ETF over cheaper same-job peers in {scope_label}."
    else:
        regime_text = f"Current Kronos conditions support {scope_label}, but not strongly enough to erase fee pressure from same-job peers."
    return {
        "compare_context": scope_label,
        "regime_check_text": regime_text,
        "affects_peer_preference": False,
        "affects_exposure_preference": path_label in {"Base path supportive", "Base path mildly supportive", "Base path fragile"},
    }


def _kronos_scenario_pack(
    *,
    market_path_support: dict[str, Any] | None,
    forecast_support: dict[str, Any] | None,
) -> dict[str, Any] | None:
    support = dict(market_path_support or {})
    if not support:
        return None
    scenarios = {str(item.get("scenario_type") or "").strip().lower(): dict(item) for item in list(support.get("scenario_summary") or [])}
    threshold_flags = [
        _plain_sentence(
            " · ".join(
                part
                for part in [str(item.get("label") or "").strip(), str(item.get("relation") or "").strip(), str(item.get("note") or "").strip()]
                if part
            )
        )
        for item in list(support.get("threshold_map") or [])[:3]
        if str(item.get("label") or "").strip()
    ]
    return {
        "observed_path": "Observed path available." if list(support.get("observed_series") or []) else "Observed path unavailable.",
        "base_path": _plain_sentence((scenarios.get("base") or {}).get("summary") or "Base path not surfaced."),
        "downside_path": _plain_sentence((scenarios.get("downside") or {}).get("summary") or "Downside path not surfaced."),
        "stress_path": _plain_sentence((scenarios.get("stress") or {}).get("summary") or "Stress path not surfaced."),
        "uncertainty_band": "Available" if dict(support.get("uncertainty_band") or {}).get("lower_points") else "Not surfaced",
        "drift_state": _kronos_drift_label(support),
        "fragility_state": _plain_sentence(str(support.get("candidate_fragility_label") or "").replace("_", " ")) or "Unrated",
        "threshold_flags": threshold_flags,
        "quality_gate": _kronos_quality_gate(support),
        "provenance": _kronos_route_truth_label(
            market_path_support=support,
            scope_label=str(support.get("scope_label") or "this exposure").strip() or "this exposure",
        ),
        "refresh_status": _kronos_freshness_label(support),
        "last_run_at": str(support.get("generated_at") or "").strip() or None,
    }


def _kronos_optional_metrics(
    *,
    market_path_support: dict[str, Any] | None,
) -> dict[str, Any] | None:
    support = dict(market_path_support or {})
    metadata = dict(support.get("model_metadata") or {})
    if _kronos_quality_gate(support) != "high":
        return None
    if not any(key in metadata for key in ("upside_probability", "downside_breach_probability", "volatility_elevation_probability", "change_vs_prior_run")):
        return None
    def _as_text(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return f"{float(value) * 100:.0f}%"
        return _plain_sentence(value)
    return {
        "upside_probability": _as_text(metadata.get("upside_probability")),
        "downside_breach_probability": _as_text(metadata.get("downside_breach_probability")),
        "volatility_elevation_probability": _as_text(metadata.get("volatility_elevation_probability")),
        "change_vs_prior_run": _as_text(metadata.get("change_vs_prior_run")),
    }


def _decision_condition_pack(
    *,
    truth,
    sleeve_key: str,
    benchmark_full_name: str | None,
    exposure_label: str | None,
    visible_decision_state: dict[str, Any],
    blocker_category: str | None,
    recommendation_gate: dict[str, Any],
    source_integrity_summary: dict[str, Any],
    market_path_support: dict[str, Any] | None,
    boundaries: list[PolicyBoundary],
    primary_document_manifest: list[dict[str, Any]],
) -> dict[str, Any]:
    status_state, _ = _quick_status(visible_decision_state, recommendation_gate)
    decision_reasons = _quick_secondary_reasons(
        blocker_category=blocker_category,
        source_integrity_summary=source_integrity_summary,
        market_path_support=market_path_support,
        recommendation_gate=recommendation_gate,
    )
    kronos_market_setup = _kronos_market_setup(
        sleeve_key=sleeve_key,
        benchmark_full_name=benchmark_full_name,
        exposure_label=exposure_label,
        market_path_support=market_path_support,
        status_state=status_state,
        decision_reasons=decision_reasons,
    )
    kronos_market_setup = dict(kronos_market_setup or {})
    if not str(kronos_market_setup.get("scope_label") or "").strip():
        fallback_scope = _plain_sentence(exposure_label or benchmark_full_name or "")
        if fallback_scope.lower() in {"", "unknown", "n/a", "na", "none"}:
            fallback_scope = "this exposure"
        kronos_market_setup["scope_label"] = fallback_scope
    document_coverage = _quick_document_coverage(
        primary_document_manifest=primary_document_manifest,
        benchmark_full_name=benchmark_full_name,
        truth=truth,
    )
    integrity = str(source_integrity_summary.get("integrity_label") or source_integrity_summary.get("state") or "").strip().lower()
    gate_state = str(recommendation_gate.get("gate_state") or "").strip().lower()
    benchmark_issue = _active_boundary(boundaries, "benchmark_authority_floor") is not None
    purpose_issue = _active_boundary(boundaries, "sleeve_purpose_alignment") is not None
    replacement_issue = _active_boundary(boundaries, "low_turnover_discipline") is not None
    source_issue = integrity in {"thin", "mixed", "weak", "conflicted", "missing"}
    document_issue = bool(document_coverage.get("missing_documents")) or str(document_coverage.get("document_confidence_grade") or "").strip().upper() in {"C", "D"}
    upgrade_confidence = (
        "high"
        if str(dict(kronos_market_setup or {}).get("quality_gate") or "").strip().lower() == "high" and not benchmark_issue and not purpose_issue
        else "medium"
        if kronos_market_setup
        else "low"
    )
    downgrade_confidence = (
        "high"
        if (
            str(dict(kronos_market_setup or {}).get("path_support_label") or "").strip() in {"Base path fragile", "Base path adverse"}
            or str(dict(kronos_market_setup or {}).get("drift_label") or "").strip() == "Weakening"
        )
        else "medium"
    )
    proof_clause = _upgrade_condition_support(
        benchmark_issue=benchmark_issue,
        source_issue=source_issue,
        document_issue=document_issue,
        purpose_issue=purpose_issue,
        replacement_issue=replacement_issue,
    )
    downgrade_proof_clause = _downgrade_condition_support(
        benchmark_issue=benchmark_issue,
        source_issue=source_issue,
        document_issue=document_issue,
        purpose_issue=purpose_issue,
        replacement_issue=replacement_issue,
    )
    upgrade_text = _plain_sentence(_upgrade_condition_headline(kronos_market_setup=kronos_market_setup))
    downgrade_text = _plain_sentence(_downgrade_condition_headline(kronos_market_setup=kronos_market_setup))
    _, kill_confidence, kill_basis = _kill_condition_text(boundaries)
    kill_text = "Kill the idea only if the role breaks, not because timing stays weak."
    upgrade_item = _condition_item(
        kind="upgrade",
        label="Upgrade if",
        text=upgrade_text,
        confidence=upgrade_confidence,
        basis_labels=_condition_basis_labels(
            include_market_setup=bool(kronos_market_setup),
            include_benchmark_proof=benchmark_issue or gate_state != "admissible",
            include_source_support=source_issue,
            include_document_support=document_issue,
            include_peer_comparison=True,
            include_sleeve_role_fit=purpose_issue,
            include_replacement_discipline=replacement_issue,
        ),
    )
    if upgrade_item is not None:
        upgrade_item["support_text"] = _sentence_case(proof_clause)
        upgrade_item["confirmation_label"] = _condition_confirmation_label("upgrade")
        upgrade_item["confirmation_points"] = _upgrade_confirmation_points(
            benchmark_issue=benchmark_issue,
            document_issue=document_issue,
            purpose_issue=purpose_issue,
            replacement_issue=replacement_issue,
        )
    downgrade_item = _condition_item(
        kind="downgrade",
        label="Downgrade if",
        text=downgrade_text,
        confidence=downgrade_confidence,
        basis_labels=_condition_basis_labels(
            include_market_setup=bool(kronos_market_setup),
            include_benchmark_proof=benchmark_issue or gate_state != "admissible",
            include_source_support=source_issue,
            include_document_support=document_issue,
            include_peer_comparison=True,
            include_sleeve_role_fit=purpose_issue,
            include_replacement_discipline=replacement_issue,
        ),
    )
    if downgrade_item is not None:
        downgrade_item["support_text"] = _sentence_case(downgrade_proof_clause)
        downgrade_item["confirmation_label"] = _condition_confirmation_label("downgrade")
        downgrade_item["confirmation_points"] = _downgrade_confirmation_points(
            benchmark_issue=benchmark_issue,
            source_issue=source_issue,
            document_issue=document_issue,
            purpose_issue=purpose_issue,
            replacement_issue=replacement_issue,
        )
    kill_item = _condition_item(
        kind="kill",
        label="Kill if",
        text=kill_text,
        confidence=kill_confidence,
        basis_labels=kill_basis,
    )
    if kill_item is not None:
        kill_item["support_text"] = "This is the hard boundary. Weak timing can delay action, but it should not break the thesis on its own."
        kill_item["confirmation_label"] = _condition_confirmation_label("kill")
        kill_item["confirmation_points"] = _kill_confirmation_points(
            purpose_issue=purpose_issue,
            replacement_issue=replacement_issue,
        )
    return {
        "intro": "These are the conditions that would materially change the current verdict.",
        "upgrade": upgrade_item,
        "downgrade": downgrade_item,
        "kill": kill_item,
    }


def _quick_brief(
    *,
    truth,
    sleeve_key: str,
    sleeve_purpose: str,
    visible_decision_state: dict[str, Any],
    blocker_category: str | None,
    recommendation_gate: dict[str, Any],
    source_integrity_summary: dict[str, Any],
    market_path_support: dict[str, Any] | None,
    implementation_profile: dict[str, Any],
    source_authority_fields: list[dict[str, Any]],
    benchmark_full_name: str | None,
    exposure_summary: str | None,
    ter_bps: Any,
    aum_usd: Any,
    action_boundary: str | None,
    what_blocks_action: str,
    what_changes_view: str,
    upgrade_condition: str | None,
    kill_condition: str | None,
    overlay_context: dict[str, Any] | None,
    primary_document_manifest: list[dict[str, Any]],
    freshness_state: str | None,
    market: MarketSeriesTruth,
    forecast_support: dict[str, Any] | None,
) -> dict[str, Any]:
    status_state, status_label = _quick_status(visible_decision_state, recommendation_gate)
    decision_reasons = _quick_secondary_reasons(
        blocker_category=blocker_category,
        source_integrity_summary=source_integrity_summary,
        market_path_support=market_path_support,
        recommendation_gate=recommendation_gate,
    )
    exposure_label = _quick_exposure_label(
        benchmark_full_name=benchmark_full_name,
        implementation_profile=implementation_profile,
        exposure_summary=exposure_summary,
        truth=truth,
    )
    portfolio_role = _quick_portfolio_role(
        sleeve_purpose=sleeve_purpose,
        exposure_label=exposure_label,
    )
    fund_domicile = _quick_fund_domicile(truth=truth, implementation_profile=implementation_profile)
    why_this_matters = _quick_why_this_matters(
        symbol=truth.symbol,
        exposure_label=exposure_label,
        portfolio_role=portfolio_role,
    )
    compare_first = _quick_compare_first(
        symbol=truth.symbol,
        sleeve_key=sleeve_key,
        exposure_label=exposure_label,
    )
    broader_alternative = _quick_broader_alternative(
        symbol=truth.symbol,
        exposure_label=exposure_label,
    )
    what_it_solves = _quick_what_it_solves(
        symbol=truth.symbol,
        exposure_label=exposure_label,
        portfolio_role=portfolio_role,
    )
    portfolio_fit = _quick_portfolio_fit(
        portfolio_role=portfolio_role,
        exposure_label=exposure_label,
        overlay_context=overlay_context,
    )
    what_it_does_not_solve = str(portfolio_fit.get("what_it_does_not_solve") or "").strip() or None
    what_it_still_needs_to_prove = _quick_what_it_still_needs_to_prove(
        symbol=truth.symbol,
        decision_reasons=decision_reasons,
    )
    decision_readiness = _quick_decision_readiness(
        symbol=truth.symbol,
        status_label=status_label,
        compare_first=compare_first,
        decision_reasons=decision_reasons,
    )
    overlay_state = str(dict(overlay_context or {}).get("state") or "").strip().lower()
    overlay_note = (
        "Portfolio overlay unavailable. Fund-level view shown."
        if overlay_state in {"overlay_absent", ""}
        else _plain_sentence(dict(overlay_context or {}).get("summary"))
    )
    evidence_footer = _quick_evidence_footer(
        source_integrity_summary=source_integrity_summary,
        primary_document_manifest=primary_document_manifest,
        freshness_state=freshness_state,
    )
    kronos_market_setup = _kronos_market_setup(
        sleeve_key=sleeve_key,
        benchmark_full_name=benchmark_full_name,
        exposure_label=exposure_label,
        market_path_support=market_path_support,
        status_state=status_state,
        decision_reasons=decision_reasons,
    )
    kronos_decision_bridge = _kronos_decision_bridge(
        kronos_market_setup=kronos_market_setup,
    )
    kronos_compare_check = _kronos_compare_check(
        kronos_market_setup=kronos_market_setup,
    )
    kronos_scenario_pack = _kronos_scenario_pack(
        market_path_support=market_path_support,
        forecast_support=forecast_support,
    )
    kronos_optional_metrics = _kronos_optional_metrics(
        market_path_support=market_path_support,
    )
    scenario_entry = _quick_scenario_entry(market_path_support)
    document_coverage = _quick_document_coverage(
        primary_document_manifest=primary_document_manifest,
        benchmark_full_name=benchmark_full_name,
        truth=truth,
    )
    fund_profile = _quick_fund_profile(
        truth=truth,
        benchmark_full_name=benchmark_full_name,
        exposure_label=exposure_label,
        implementation_profile=implementation_profile,
        source_authority_fields=source_authority_fields,
        primary_document_manifest=primary_document_manifest,
        aum_usd=aum_usd,
    )
    listing_profile = _quick_listing_profile(
        truth=truth,
        implementation_profile=implementation_profile,
    )
    composition_pack = _quick_composition_pack(
        symbol=truth.symbol,
        truth=truth,
        source_authority_fields=source_authority_fields,
    )
    index_scope_explainer = _quick_index_scope_explainer(
        symbol=truth.symbol,
        benchmark_full_name=benchmark_full_name,
        exposure_label=exposure_label,
        holdings_count=str(fund_profile.get("holdings_count") or "").strip() or None,
        sleeve_key=sleeve_key,
        benchmark_family=str(fund_profile.get("benchmark_family") or "").strip() or None,
        benchmark_key=str(getattr(truth, "benchmark_id", "") or "").strip() or None,
        asset_class=str(getattr(truth, "asset_class", "") or "").strip() or None,
    )
    decision_proof_pack = _quick_decision_proof_pack(
        why_this_matters=why_this_matters,
        compare_first=compare_first,
        broader_alternative=broader_alternative,
        what_it_solves=what_it_solves,
        what_it_does_not_solve=what_it_does_not_solve or "",
        what_it_still_needs_to_prove=what_it_still_needs_to_prove,
        what_changes_view=_plain_sentence(what_changes_view or upgrade_condition or "No upgrade trigger is surfaced."),
        symbol=truth.symbol,
    )
    performance_tracking_pack = _quick_performance_tracking_pack(
        truth=truth,
        implementation_profile=implementation_profile,
        source_authority_fields=source_authority_fields,
    )
    peer_compare_pack = _quick_peer_compare_pack(
        truth=truth,
        sleeve_key=sleeve_key,
        benchmark_full_name=benchmark_full_name,
        exposure_label=exposure_label,
        implementation_profile=implementation_profile,
        source_authority_fields=source_authority_fields,
        primary_document_manifest=primary_document_manifest,
        aum_usd=aum_usd,
        what_it_still_needs_to_prove=what_it_still_needs_to_prove,
    )
    what_you_are_buying = _quick_what_you_are_buying(
        truth=truth,
        benchmark_full_name=benchmark_full_name,
        implementation_profile=implementation_profile,
        source_authority_fields=source_authority_fields,
    )
    return {
        "status_state": status_state,
        "status_label": status_label,
        "fund_identity": {
            "ticker": truth.symbol,
            "name": truth.name,
            "issuer": _plain_sentence(implementation_profile.get("issuer_name") or implementation_profile.get("issuer")),
            "exposure_label": exposure_label,
        },
        "portfolio_role": portfolio_role,
        "role_label": portfolio_role,
        "summary": _quick_summary(
            name=truth.name,
            benchmark_full_name=benchmark_full_name,
            exposure_summary=exposure_summary,
            implementation_profile=implementation_profile,
            status_state=status_state,
            secondary_reasons=decision_reasons,
        ),
        "decision_reasons": decision_reasons,
        "secondary_reasons": decision_reasons,
        "key_facts": _quick_key_facts(
            truth=truth,
            benchmark_full_name=benchmark_full_name,
            ter_bps=ter_bps,
            aum_usd=aum_usd,
            implementation_profile=implementation_profile,
            source_authority_fields=source_authority_fields,
        ),
        "should_i_use": _quick_should_i_use(
            exposure_label=exposure_label,
            portfolio_role=portfolio_role,
            distribution_policy=str(implementation_profile.get("distribution_policy") or "").strip() or None,
            domicile=fund_domicile,
            status_state=status_state,
            decision_reasons=decision_reasons,
        ),
        "performance_checks": _quick_performance_checks(
            symbol=truth.symbol,
            status_label=status_label,
            decision_reasons=decision_reasons,
            implementation_profile=implementation_profile,
            fund_domicile=fund_domicile,
            ter_bps=ter_bps,
            aum_usd=aum_usd,
            portfolio_role=portfolio_role,
        ),
        "what_you_are_buying": what_you_are_buying,
        "portfolio_fit": portfolio_fit,
        "why_this_matters": why_this_matters,
        "compare_first": compare_first,
        "broader_alternative": broader_alternative,
        "what_it_solves": what_it_solves,
        "what_it_still_needs_to_prove": what_it_still_needs_to_prove,
        "decision_readiness": decision_readiness,
        "peer_compare_pack": peer_compare_pack,
        "fund_profile": fund_profile,
        "listing_profile": listing_profile,
        "index_scope_explainer": index_scope_explainer,
        "decision_proof_pack": decision_proof_pack,
        "performance_tracking_pack": performance_tracking_pack,
        "composition_pack": composition_pack,
        "document_coverage": document_coverage,
        "how_to_decide": _quick_how_to_decide(
            exposure_label=exposure_label,
            distribution_policy=str(implementation_profile.get("distribution_policy") or "").strip() or None,
            domicile=fund_domicile,
        ),
        "evidence_footer_detail": evidence_footer,
        "scenario_entry": scenario_entry,
        "kronos_market_setup": kronos_market_setup,
        "kronos_decision_bridge": kronos_decision_bridge,
        "kronos_compare_check": kronos_compare_check,
        "kronos_scenario_pack": kronos_scenario_pack,
        "kronos_optional_metrics": kronos_optional_metrics,
        "why_it_matters": [
            {"label": "Why this matters", "value": why_this_matters},
            {"label": "Compare first", "value": compare_first},
            *([{"label": "Broader alternative", "value": broader_alternative}] if broader_alternative else []),
            {"label": "What it solves", "value": what_it_solves},
            {
                "label": "What it does not solve",
                "value": what_it_does_not_solve,
            },
            {"label": "What it still needs to prove", "value": what_it_still_needs_to_prove},
            {"label": "What changes the verdict", "value": _plain_sentence(what_changes_view or upgrade_condition or "No upgrade trigger is surfaced.")},
            {"label": "Decision readiness", "value": decision_readiness},
        ],
        "performance_and_implementation": [
            {"label": row["label"], "value": _plain_sentence(row["summary"])}
            for row in _quick_performance_checks(
                symbol=truth.symbol,
                status_label=status_label,
                decision_reasons=decision_reasons,
                implementation_profile=implementation_profile,
                fund_domicile=fund_domicile,
                ter_bps=ter_bps,
                aum_usd=aum_usd,
                portfolio_role=portfolio_role,
            )
        ],
        "overlay_note": overlay_note,
        "backdrop_note": scenario_entry["backdrop_summary"],
        "evidence_footer": [
            {"label": "Evidence quality", "value": evidence_footer["evidence_quality"]},
            {"label": "Data completeness", "value": evidence_footer["data_completeness"]},
            {"label": "Document support", "value": evidence_footer["document_support"]},
            {"label": "Monitoring status", "value": evidence_footer["monitoring_status"]},
        ],
    }


def _sparkline_points(market: MarketSeriesTruth) -> list[int]:
    values = [int(round(point.value)) for point in market.points[:12]]
    return values or [100, 100]


def _window_return(points: list[MarketDataPoint], lookback: int) -> float | None:
    if len(points) <= lookback:
        return None
    latest = points[-1].value
    base = points[-(lookback + 1)].value
    if base in {None, 0}:
        return None
    return ((latest - base) / abs(base)) * 100.0


def _regime_windows(market: MarketSeriesTruth, baseline_comparisons: list[dict[str, Any]]) -> list[dict[str, Any]]:
    benchmark_note = baseline_comparisons[2]["summary"] if len(baseline_comparisons) > 2 else "Benchmark anchor remains bounded."
    rows: list[dict[str, Any]] = []
    for label, lookback in (("1 month", 21), ("3 months", 63), ("6 months", 126)):
        window_return = _window_return(market.points, lookback)
        if window_return is None:
            continue
        rows.append(
            {
                "label": label,
                "period": f"{lookback} trading days",
                "fund_return": f"{window_return:.2f}%",
                "benchmark_return": "Bounded baseline",
                "note": benchmark_note,
            }
        )
    return rows


def _coerce_freshness_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    return {"freshness_class": text}


def _benchmark_field_provenance(benchmark_truth_obj) -> dict[str, Any]:
    if benchmark_truth_obj is None or not getattr(benchmark_truth_obj, "evidence", None):
        return {}
    facts = dict((benchmark_truth_obj.evidence[0].facts or {}))
    freshness_state = _coerce_freshness_payload(facts.get("freshness_state"))
    field_provenance = dict(facts.get("field_provenance") or {})
    mapped: dict[str, Any] = {}
    for field_name, entry in field_provenance.items():
        if not isinstance(entry, dict):
            continue
        mapped[f"benchmark.{field_name}"] = runtime_provenance(
            source_family=str(entry.get("source_family") or "").strip() or None,
            provider_used=str(entry.get("provider") or "").strip() or None,
            path_used=str(entry.get("path") or "").strip() or None,
            freshness=str(freshness_state.get("freshness_class") or "").strip() or None,
            authority_kind=str(entry.get("authority_kind") or "").strip() or None,
            provider_execution={
                "provider_name": entry.get("provider"),
                "path_used": entry.get("path"),
                "live_or_cache": entry.get("live_or_cache"),
                "usable_truth": entry.get("usable_truth"),
                "sufficiency_state": entry.get("sufficiency_state"),
                "data_mode": entry.get("data_mode"),
                "authority_level": entry.get("authority_level"),
                "observed_at": entry.get("observed_at"),
                "provenance_strength": entry.get("provenance_strength"),
                "insufficiency_reason": entry.get("insufficiency_reason"),
                "freshness_class": entry.get("freshness") or freshness_state.get("freshness_class"),
            },
            provenance_strength=str(entry.get("provenance_strength") or "").strip() or None,
            insufficiency_reason=str(entry.get("insufficiency_reason") or "").strip() or None,
        )
    return mapped


def _instrument_market_provenance(truth, market: MarketSeriesTruth) -> dict[str, Any]:
    source_provenance = dict((truth.metrics or {}).get("source_provenance") or {})
    freshness = None
    truth_envelope = {}
    provider_execution = {}
    if market.evidence:
        facts = dict(market.evidence[0].facts or {})
        freshness = str((_coerce_freshness_payload(facts.get("freshness_state"))).get("freshness_class") or "").strip() or None
        truth_envelope = dict(facts.get("truth_envelope") or {})
        provider_execution = dict(facts.get("provider_execution") or {})
    mapped: dict[str, Any] = {}
    for key in ("price", "change_pct_1d"):
        entry = dict(source_provenance.get(key) or {})
        entry_execution = {
            "provider_name": entry.get("provider"),
            "path_used": entry.get("path"),
            "live_or_cache": entry.get("live_or_cache"),
            "usable_truth": entry.get("usable_truth"),
            "sufficiency_state": entry.get("sufficiency_state"),
            "data_mode": entry.get("data_mode"),
            "authority_level": entry.get("authority_level"),
            "observed_at": entry.get("observed_at"),
            "provenance_strength": entry.get("provenance_strength"),
            "insufficiency_reason": entry.get("insufficiency_reason"),
            "freshness_class": freshness,
        }
        merged_execution = dict(provider_execution or {})
        merged_execution.update({name: value for name, value in entry_execution.items() if value is not None})
        mapped[f"instrument.{key}"] = runtime_provenance(
            source_family=str(entry.get("source_family") or "").strip() or None,
            provider_used=str(entry.get("provider") or "").strip() or None,
            path_used=str(entry.get("path") or "").strip() or None,
            provider_execution=merged_execution or None,
            truth_envelope=dict(entry.get("truth_envelope") or {}) or truth_envelope,
            freshness=freshness,
            authority_kind=str(entry.get("authority_kind") or "").strip() or None,
            provenance_strength=str(entry.get("provenance_strength") or "").strip() or None,
            insufficiency_reason=str(entry.get("insufficiency_reason") or "").strip() or None,
        )
    return mapped


def _market_history_block(
    truth,
    market: MarketSeriesTruth,
    baseline_comparisons: list[dict[str, Any]],
    *,
    benchmark_truth_obj=None,
) -> dict[str, Any]:
    latest = market.points[-1].value if market.points else None
    previous = market.points[-2].value if len(market.points) >= 2 else latest
    one_day = None
    if latest is not None and previous not in {None, 0}:
        one_day = ((latest - previous) / abs(previous)) * 100.0
    regime_windows = _regime_windows(market, baseline_comparisons)
    field_provenance = {
        **_instrument_market_provenance(truth, market),
        **_benchmark_field_provenance(benchmark_truth_obj),
    }
    return {
        "summary": (
            f"{truth.name} is moving {one_day:.2f}% on the latest session and should be read against its benchmark and sleeve baseline rather than in isolation."
            if one_day is not None
            else f"{truth.name} market history is available, but the current read should stay anchored to benchmark and sleeve context."
        ),
        "sparkline_points": _sparkline_points(market),
        "benchmark_note": baseline_comparisons[2]["summary"] if len(baseline_comparisons) > 2 else None,
        "regime_windows": regime_windows
        or [
            {
                "label": "Current snapshot",
                "period": "latest",
                "fund_return": f"{one_day:.2f}%" if one_day is not None else "n/a",
                "benchmark_return": baseline_comparisons[2]["verdict"] if len(baseline_comparisons) > 2 else "n/a",
                "note": "Current market history is bounded to the routed window, so use it as context rather than full-cycle evidence.",
            }
        ],
        "field_provenance": field_provenance,
    }


def _benchmark_proxy_symbol(truth, sleeve_key: str) -> str | None:
    assignment = DEFAULT_BENCHMARK_ASSIGNMENTS.get(str(truth.symbol or "").upper(), {})
    proxy = str(assignment.get("benchmark_proxy_symbol") or "").strip().upper()
    if proxy:
        return proxy
    for benchmark_id in _baseline_benchmarks(truth, sleeve_key):
        fallback = str(DEFAULT_BENCHMARK_ASSIGNMENTS.get(str(benchmark_id or "").upper(), {}).get("benchmark_proxy_symbol") or "").strip().upper()
        if fallback:
            return fallback
    return None


def _risk_blocks(assessment: CandidateAssessment, boundaries: list[PolicyBoundary]) -> list[dict[str, Any]]:
    blocks = [
        {
            "category": "implementation",
            "title": "Implementation risk",
            "detail": risk,
        }
        for risk in assessment.key_risks[:2]
    ]
    for boundary in boundaries[:2]:
        blocks.append(
            {
                "category": "boundary",
                "title": boundary.code.replace("_", " ").title(),
                "detail": boundary.required_action or boundary.summary,
            }
        )
    return blocks or [
        {
            "category": "coverage",
            "title": "Coverage risk",
            "detail": "Current V2 report did not emit explicit risks, so this remains a bounded placeholder.",
        }
    ]


def _competition_blocks(baseline_comparisons: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "label": comparison["label"],
            "summary": comparison["summary"],
            "verdict": comparison.get("verdict"),
        }
        for comparison in baseline_comparisons
    ]


def _evidence_sources(truth, primary_document_manifest: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, document in enumerate(list(primary_document_manifest or []), start=1):
        item = dict(document or {})
        doc_type = str(item.get("doc_type") or "issuer_document")
        authority_class = str(item.get("authority_class") or "issuer_primary")
        rows.append(
            {
                "source_id": str(item.get("document_id") or f"primary_document_{index}"),
                "label": f"{doc_type.replace('_', ' ').title()} · {authority_class.replace('_', ' ')}",
                "url": _sanitize_surface_url(item.get("doc_url")),
                "freshness_state": "fresh_full_rebuild" if str(item.get("status") or "").strip().lower() in _USABLE_DOCUMENT_STATUSES else "unavailable_with_verified_reason",
                "directness": "direct" if authority_class.startswith("issuer") else "sleeve-proxy",
            }
        )
    for pack in truth.evidence:
        for citation in pack.citations:
            rows.append(
                {
                    "source_id": citation.source_id,
                    "label": citation.label,
                    "url": _sanitize_surface_url(citation.url),
                    "freshness_state": pack.freshness,
                    "directness": "direct" if "issuer" in citation.source_id or "factsheet" in citation.source_id else "sleeve-proxy",
                }
            )
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (str(row.get("label") or ""), str(row.get("url") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _latest_timestamp(market: MarketSeriesTruth | None) -> str:
    if market and market.points:
        return str(market.points[-1].at)
    return utc_now_iso()


def _market_history_charts(
    *,
    truth,
    market: MarketSeriesTruth,
    benchmark_truth: MarketSeriesTruth | None,
    benchmark_proxy_symbol: str | None,
) -> list[dict[str, Any]]:
    if not market.points:
        return []
    return [
        comparison_chart_panel(
            panel_id=f"candidate_report_market_history_{truth.symbol.lower()}",
            title="Market history",
            primary_truth=market,
            comparison_truth=benchmark_truth,
            summary=(
                f"{truth.name} is shown against its benchmark proxy so recent market movement keeps a baseline frame."
            ),
            what_to_notice="Read the latest move against the benchmark proxy and use the regime windows for the longer context.",
            primary_label=truth.symbol,
            comparison_label=benchmark_proxy_symbol,
            source_family="quote_latest",
        )
    ]


def _scenario_charts(
    *,
    truth,
    market: MarketSeriesTruth,
    benchmark_truth: MarketSeriesTruth | None,
    benchmark_proxy_symbol: str | None,
    upgrade_condition: str | None,
    downgrade_condition: str | None,
    kill_condition: str | None,
) -> list[dict[str, Any]]:
    if not market.points:
        return []
    panel = comparison_chart_panel(
        panel_id=f"candidate_report_scenarios_{truth.symbol.lower()}",
        title="Scenario context",
        primary_truth=market,
        comparison_truth=benchmark_truth,
        summary="Scenario markers stay secondary to the sleeve and benchmark read.",
        what_to_notice="Use the markers as action thresholds layered onto the same market context rather than as independent forecast authority.",
        primary_label=truth.symbol,
        comparison_label=benchmark_proxy_symbol,
        source_family="quote_latest",
    )
    timestamp = _latest_timestamp(market)
    panel["markers"] = [
        chart_marker(
            marker_id=f"candidate_report_scenarios_{truth.symbol.lower()}_upgrade",
            timestamp=timestamp,
            label="Upgrade",
            marker_type="threshold",
            summary=upgrade_condition or "More direct support would upgrade the view.",
        ),
        chart_marker(
            marker_id=f"candidate_report_scenarios_{truth.symbol.lower()}_downgrade",
            timestamp=timestamp,
            label="Downgrade",
            marker_type="threshold",
            summary=downgrade_condition or "A weaker sleeve or benchmark read would downgrade the view.",
        ),
        chart_marker(
            marker_id=f"candidate_report_scenarios_{truth.symbol.lower()}_kill",
            timestamp=timestamp,
            label="Kill",
            marker_type="threshold",
            summary=kill_condition or "A mandate or admissibility failure would remove the candidate from action.",
        ),
    ]
    return [panel]


def _competition_charts(
    *,
    truth,
    market: MarketSeriesTruth,
    benchmark_truth: MarketSeriesTruth | None,
    benchmark_proxy_symbol: str | None,
) -> list[dict[str, Any]]:
    if not market.points:
        return []
    panel = comparison_chart_panel(
        panel_id=f"candidate_report_competition_{truth.symbol.lower()}",
        title="Competition context",
        primary_truth=market,
        comparison_truth=benchmark_truth,
        summary="Competition stays benchmark-aware first; holdings overlay only refines the incumbent angle when present.",
        what_to_notice="Use this panel to anchor the candidate against its sleeve benchmark before making incumbent-specific comparisons.",
        primary_label=truth.symbol,
        comparison_label=benchmark_proxy_symbol,
        source_family="quote_latest",
    )
    panel["markers"] = [
        chart_marker(
            marker_id=f"candidate_report_competition_{truth.symbol.lower()}_baseline",
            timestamp=_latest_timestamp(market),
            label="Baseline",
            marker_type="context",
            summary="Benchmark-relative competition is the primary comparison frame in the report.",
        )
    ]
    return [panel]


def _decision_thresholds(
    upgrade_condition: str | None,
    downgrade_condition: str | None,
    kill_condition: str | None,
    recommendation_gate: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rows = [
        ("Upgrade if", upgrade_condition),
        ("Downgrade if", downgrade_condition),
        ("Kill if", kill_condition),
    ]
    thresholds = [{"label": label, "value": str(value)} for label, value in rows if value]
    gate = dict(recommendation_gate or {})
    if gate.get("blocked_reasons"):
        thresholds.append(
            {
                "label": "Admissibility gate",
                "value": " ".join(str(reason) for reason in list(gate.get("blocked_reasons") or [])[:2]),
            }
        )
    return thresholds


def _portfolio_overlay_context(sleeve_key: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    try:
        portfolio = get_portfolio_truth("default")
    except Exception:
        portfolio = None

    holdings = list(getattr(portfolio, "holdings", []) or []) if portfolio is not None else []
    if not holdings:
        return None, {
            "state": "overlay_absent",
            "summary": (
                "No holdings overlay is active. Candidate Report remains sleeve-first and market-first; "
                "incumbent displacement and funding specificity will appear once a portfolio overlay is loaded."
            ),
        }

    portfolio_sleeve = {
        "global_equity_core": "global_equity",
        "developed_ex_us_optional": "global_equity",
        "emerging_markets": "global_equity",
        "china_satellite": "global_equity",
        "ig_bonds": "ig_bond",
        "cash_bills": "cash",
        "real_assets": "real_asset",
        "alternatives": "alt",
        "convex": "convex",
    }.get(sleeve_key, sleeve_key)
    sleeve_holdings = [dict(holding) for holding in holdings if str(holding.get("sleeve") or "").strip().lower() == portfolio_sleeve]
    incumbent = sleeve_holdings[0] if sleeve_holdings else None
    incumbent_label = str(incumbent.get("symbol") or incumbent.get("name") or "").strip() if incumbent else None
    overlay_summary = {
        "portfolio_id": getattr(portfolio, "portfolio_id", None),
        "summary": (
            f"{len(holdings)} holding{'s' if len(holdings) != 1 else ''} are active in the current overlay. "
            "Report tabs remain valid without holdings and now gain incumbent/funding specificity where mapped."
        ),
    }
    if incumbent_label:
        overlay_context = {
            "state": "ready",
            "summary": f"Current portfolio overlay identifies {incumbent_label} as the incumbent comparison anchor for this sleeve.",
        }
    else:
        overlay_context = {
            "state": "incumbent_unavailable",
            "summary": (
                "Portfolio overlay is active, but no current holding is mapped to this sleeve. "
                "Use sleeve fit, benchmark support, and implementation quality as the primary comparison."
            ),
        }
    return {
        **overlay_summary,
        "incumbent_label": incumbent_label,
    }, overlay_context


def _current_holding_weight_pct(symbol: str) -> float | None:
    try:
        portfolio = get_portfolio_truth("default")
    except Exception:
        return None
    holdings = list(getattr(portfolio, "holdings", []) or [])
    matched = next(
        (
            holding
            for holding in holdings
            if str(holding.get("symbol") or "").strip().upper() == str(symbol or "").strip().upper()
        ),
        None,
    )
    if not matched:
        return None
    try:
        weight = float(matched.get("weight") or matched.get("weight_pct") or 0.0)
    except (TypeError, ValueError):
        return None
    if weight <= 1.0:
        return round(weight * 100.0, 2)
    return round(weight, 2)


def _report_weight_state(current_weight_pct: float | None, *, holdings_overlay_present: bool) -> str:
    if not holdings_overlay_present:
        return "overlay_absent"
    if current_weight_pct is None:
        return "not_held"
    if current_weight_pct <= 0:
        return "not_held"
    return "held"


def _visible_state_rank(state: str | None) -> int:
    return {
        "blocked": 0,
        "research_only": 1,
        "watch": 2,
        "review": 3,
        "eligible": 4,
    }.get(str(state or "").strip().lower(), 2)


def _failure_summary_with_doctrine(
    failure_class_summary: dict[str, Any],
    *,
    canonical_visible_state: dict[str, Any],
    doctrine_visible_state: Any,
) -> dict[str, Any]:
    summary = dict(failure_class_summary or {})
    items = [dict(item) for item in list(summary.get("items") or [])]
    doctrine_state = str(getattr(doctrine_visible_state, "state", "") or "").strip().lower()
    canonical_state = str(dict(canonical_visible_state or {}).get("state") or "").strip().lower()
    if doctrine_state and _visible_state_rank(doctrine_state) < _visible_state_rank(canonical_state):
        doctrine_item = {
            "class_id": "doctrine_restraint",
            "label": "Doctrine restraint",
            "severity": "review",
            "summary": str(getattr(doctrine_visible_state, "rationale", "") or "Process doctrine still keeps this candidate in review.").strip(),
            "fields": [],
        }
        if not any(str(item.get("class_id") or "") == "doctrine_restraint" for item in items):
            items.append(doctrine_item)
        summary["items"] = items
        summary["review_classes"] = sorted(
            dict.fromkeys([str(item.get("class_id") or "") for item in items if str(item.get("severity") or "") == "review"])
        )
        if not summary.get("primary_class") and doctrine_item["summary"]:
            summary["primary_class"] = "doctrine_restraint"
            summary["primary_label"] = "Doctrine restraint"
            summary["summary"] = doctrine_item["summary"]
    return summary


def build(
    candidate_id: str,
    *,
    source_snapshot_id: str | None = None,
    source_generated_at: str | None = None,
    source_contract_version: str | None = None,
    sleeve_key: str | None = None,
    report_build_mode: str = "snapshot_only",
) -> dict[str, object]:
    stable_candidate_id = f"candidate_instrument_{_normalize_candidate_symbol(candidate_id).lower()}"
    bound_explorer_row = _bound_explorer_candidate_row(stable_candidate_id, source_snapshot_id, sleeve_key=sleeve_key) if source_snapshot_id else _latest_explorer_candidate_row(stable_candidate_id, sleeve_key=sleeve_key)
    row = bound_explorer_row or _candidate_row(candidate_id)
    allow_live_fetch = str(report_build_mode or "snapshot_only") == "refresh"
    truth = get_instrument_truth(candidate_id, allow_live_fetch=allow_live_fetch)
    sleeve_key = str((row or {}).get("sleeve_key") or truth.metrics.get("sleeve_key") or "unknown")
    sleeve_id = _sleeve_id(sleeve_key)
    sleeve_purpose = _sleeve_purpose(sleeve_key)
    candidate_payload: dict[str, Any] = {
        **(row or {}),
        "symbol": truth.symbol,
        "sleeve_key": sleeve_key,
    }
    if not candidate_payload.get("expense_ratio") and truth.metrics.get("expense_ratio") is not None:
        candidate_payload["expense_ratio"] = truth.metrics.get("expense_ratio")
    if not candidate_payload.get("domicile") and truth.domicile:
        candidate_payload["domicile"] = truth.domicile
    if not candidate_payload.get("primary_documents") and truth.metrics.get("primary_documents"):
        candidate_payload["primary_documents"] = truth.metrics.get("primary_documents")
    with _connection() as conn:
        truth_context = build_candidate_truth_context(conn, candidate_payload)
    implementation_profile = dict(truth_context.get("implementation_profile") or {})
    recommendation_gate = dict(truth_context.get("recommendation_gate") or {})
    reconciliation = dict(truth_context.get("reconciliation") or {})
    source_authority_fields = list(truth_context.get("source_authority_map") or [])
    reconciliation_report = list(truth_context.get("reconciliation_report") or [])
    data_quality_summary = dict(truth_context.get("data_quality") or {})
    primary_document_manifest = list(truth_context.get("primary_document_manifest") or [])
    institutional_facts = dict(truth_context.get("institutional_facts") or {})
    source_integrity_summary = dict(truth_context.get("source_integrity_summary") or {})
    investor_decision_state = str(truth_context.get("investor_decision_state") or "research_only")
    visible_decision_state = dict(truth_context.get("visible_decision_state") or {})
    failure_class_summary = dict(truth_context.get("failure_class_summary") or {})
    score_decomposition = dict(truth_context.get("score_decomposition") or {})
    identity_state = dict(truth_context.get("identity_state") or {})
    blocker_category = truth_context.get("blocker_category")
    if bound_explorer_row:
        bound_visible_state = dict(bound_explorer_row.get("visible_decision_state") or {})
        if bound_visible_state:
            visible_decision_state = bound_visible_state

    market = _market_truth(truth.symbol)
    signal, interpretation_card = interpret(truth, market)
    assessment = _candidate_assessment(
        truth=truth,
        interpretation_card=interpretation_card,
        signal_strength=signal.strength,
        sleeve_id=sleeve_id,
        sleeve_purpose=sleeve_purpose,
    )
    restraints = evaluate(assessment)
    boundaries = build_policy_boundaries(assessment, sleeve_purpose=sleeve_purpose)
    dominant_boundary = _dominant_boundary(boundaries)
    constraint_summary = apply_rubric(assessment, dominant_boundary, restraints).model_copy(update={"boundaries": boundaries})
    failure_class_summary = _failure_summary_with_doctrine(
        failure_class_summary,
        canonical_visible_state=visible_decision_state,
        doctrine_visible_state=constraint_summary.visible_decision_state,
    )

    baseline_comparisons: list[dict[str, Any]] = [
        {
            "label": "Against cash (do nothing)",
            "summary": (
                f"Holding cash instead of {truth.name} means accepting the opportunity cost of "
                f"{truth.asset_class} exposure for this sleeve."
            ),
            "verdict": "cash_alternative",
        },
    ]
    holdings_overlay, overlay_context = _portfolio_overlay_context(sleeve_key)
    current_weight_pct = _current_holding_weight_pct(truth.symbol)
    weight_state = _report_weight_state(current_weight_pct, holdings_overlay_present=holdings_overlay is not None)
    incumbent_label = str((holdings_overlay or {}).get("incumbent_label") or "").strip()
    if incumbent_label:
        baseline_comparisons.append(
            {
                "label": f"Against incumbent ({incumbent_label})",
                "summary": (
                    f"Replacing {incumbent_label} with {truth.name} requires clearing a higher conviction bar "
                    "and justifying the turnover cost."
                ),
                "verdict": "displacement_cost",
            }
        )
    else:
        baseline_comparisons.append(
            {
                "label": "Against incumbent (overlay unresolved)",
                "summary": overlay_context["summary"] if overlay_context else (
                    "Incumbent comparison is unresolved, so use sleeve fit and benchmark support as the primary frame."
                ),
                "verdict": "overlay_absent" if (overlay_context or {}).get("state") == "overlay_absent" else "incumbent_unavailable",
            }
        )
    primary_benchmark_truth_obj = None
    for benchmark_id in _baseline_benchmarks(truth, sleeve_key):
        benchmark_truth = get_benchmark_truth(
            benchmark_id,
            surface_name="candidate_report",
            allow_live_fetch=allow_live_fetch,
        )
        if primary_benchmark_truth_obj is None:
            primary_benchmark_truth_obj = benchmark_truth
        baseline_comparisons.append(
            {
                "label": f"Against {benchmark_truth.name}",
                "summary": f"{benchmark_truth.name} remains a bounded comparison anchor for {truth.name}.",
                "verdict": "bounded_support"
                if benchmark_truth.benchmark_authority_level != "direct"
                else "direct_support",
            }
        )

    action_boundary_str = dominant_boundary.required_action or dominant_boundary.summary
    what_changes_view = _what_changes_view_from_restraints(restraints)

    available_tabs = list(_REPORT_TABS)
    market_history_block = _market_history_block(
        truth,
        market,
        baseline_comparisons,
        benchmark_truth_obj=primary_benchmark_truth_obj,
    )
    cached_forecast = _try_load_cached_forecast_support(assessment.candidate_id, label=truth.name, allow_refresh=False)
    market_path_support = dict(cached_forecast.get("market_path_support") or {}) if cached_forecast else None
    score_decomposition = _enrich_score_decomposition(score_decomposition, market_path_support)
    score_summary = _candidate_score_summary(
        score_decomposition=score_decomposition,
        source_integrity_summary=source_integrity_summary,
        market_path_support=market_path_support,
    )
    decision_condition_pack = _decision_condition_pack(
        truth=truth,
        sleeve_key=sleeve_key,
        benchmark_full_name=institutional_facts.get("benchmark_full_name"),
        exposure_label=institutional_facts.get("exposure_summary"),
        visible_decision_state=visible_decision_state,
        blocker_category=str(blocker_category) if blocker_category is not None else None,
        recommendation_gate=recommendation_gate,
        source_integrity_summary=source_integrity_summary,
        market_path_support=market_path_support,
        boundaries=boundaries,
        primary_document_manifest=primary_document_manifest,
    )
    upgrade_condition = str(((decision_condition_pack.get("upgrade") or {}).get("text")) or "").strip() or None
    downgrade_condition = str(((decision_condition_pack.get("downgrade") or {}).get("text")) or "").strip() or None
    kill_condition = str(((decision_condition_pack.get("kill") or {}).get("text")) or "").strip() or None
    if cached_forecast is not None:
        forecast_support = cached_forecast["forecast_support"]
        scenario_blocks = cached_forecast["scenario_blocks"]
    else:
        forecast_support = _lightweight_forecast_support(label=truth.name)
        scenario_blocks = _lightweight_scenario_blocks(
            label=truth.name,
            implication=interpretation_card.why_it_matters_economically,
            upgrade_condition=upgrade_condition,
            downgrade_condition=downgrade_condition,
            kill_condition=kill_condition,
        )
    risk_blocks = _risk_blocks(assessment, boundaries)
    if implementation_profile.get("summary"):
        risk_blocks.append(
            {
                "category": "implementation",
                "title": "Implementation suitability",
                "detail": str(implementation_profile.get("summary")),
            }
        )
    if reconciliation.get("status") in {"soft_drift", "hard_conflict"}:
        risk_blocks.append(
            {
                "category": "reconciliation",
                "title": "Cross-source reconciliation",
                "detail": str(reconciliation.get("summary")),
            }
        )
    competition_blocks = _competition_blocks(baseline_comparisons)
    evidence_sources = _evidence_sources(truth, primary_document_manifest)
    if market_path_support and not scenario_blocks:
        scenario_blocks = _lightweight_scenario_blocks(
            label=truth.name,
            implication=str(market_path_support.get("candidate_implication") or ""),
            upgrade_condition=upgrade_condition,
            downgrade_condition=downgrade_condition,
            kill_condition=kill_condition,
        )
    report_explanations = build_candidate_report_explanations(
        candidate_name=truth.name,
        sleeve_key=sleeve_key,
        sleeve_purpose=sleeve_purpose,
        investor_decision_state=investor_decision_state,
        blocker_category=str(blocker_category) if blocker_category is not None else None,
        institutional_facts=institutional_facts,
        recommendation_gate=recommendation_gate,
        source_integrity_summary=source_integrity_summary,
        implementation_profile=implementation_profile,
        identity_state=identity_state,
        overlay_context=overlay_context,
        visible_rationale=str(visible_decision_state.get("rationale") or constraint_summary.visible_decision_state.rationale or ""),
        what_changes_view=what_changes_view,
        failure_class_summary=failure_class_summary,
    )
    with _connection() as coverage_conn:
        coverage_summary = build_candidate_coverage_summary(
            coverage_conn,
            {**(row or {}), "symbol": truth.symbol, "sleeve_key": sleeve_key, "name": truth.name},
            truth_context,
            candidate_id=str(assessment.candidate_id),
            market_path_support=market_path_support,
        )
    score_rubric = build_score_rubric(
        sleeve_key=sleeve_key,
        score_decomposition=score_decomposition,
    )
    semantic_summary = build_candidate_summary_pack(
        candidate_name=truth.name,
        investor_decision_state=investor_decision_state,
        recommendation_gate=recommendation_gate,
        score_decomposition=score_decomposition,
        source_integrity_summary=source_integrity_summary,
        coverage_summary=coverage_summary,
        market_path_support=market_path_support,
    )
    market_path_summary = build_market_path_summary_pack(market_path_support)
    decision_thresholds = _decision_thresholds(
        upgrade_condition,
        downgrade_condition,
        kill_condition,
        recommendation_gate,
    )
    quick_brief = _quick_brief(
        truth=truth,
        sleeve_key=sleeve_key,
        sleeve_purpose=sleeve_purpose,
        visible_decision_state=visible_decision_state,
        blocker_category=str(blocker_category) if blocker_category is not None else None,
        recommendation_gate=recommendation_gate,
        source_integrity_summary=source_integrity_summary,
        market_path_support=market_path_support,
        implementation_profile=implementation_profile,
        source_authority_fields=source_authority_fields,
        benchmark_full_name=institutional_facts.get("benchmark_full_name"),
        exposure_summary=institutional_facts.get("exposure_summary"),
        ter_bps=institutional_facts.get("ter_bps"),
        aum_usd=institutional_facts.get("aum_usd"),
        action_boundary=action_boundary_str,
        what_blocks_action=report_explanations["main_tradeoffs"][0] if report_explanations["main_tradeoffs"] else recommendation_gate.get("summary") or "",
        what_changes_view=report_explanations["what_changes_view"],
        upgrade_condition=upgrade_condition,
        kill_condition=kill_condition,
        overlay_context=overlay_context,
        primary_document_manifest=primary_document_manifest,
        freshness_state=get_freshness("market_price").freshness_class.value,
        market=market,
        forecast_support=forecast_support,
    )
    for source in evidence_sources:
        source["truth_envelope"] = dict(
            (
                truth.evidence[0].facts.get("truth_envelope")
                if truth.evidence
                else {}
            )
            or {}
        )
        envelope_note = describe_truth_envelope(dict(source.get("truth_envelope") or {}))
        if envelope_note:
            source["label"] = f"{source['label']} · {envelope_note}"
    if market_path_support:
        series_quality_summary = dict(market_path_support.get("series_quality_summary") or {})
        market_history_block["summary"] = " ".join(
            part
            for part in [
                str(market_history_block.get("summary") or "").strip(),
                str(market_path_support.get("candidate_implication") or "").strip(),
            ]
            if part
        ).strip()
        threshold_labels = [
            f"{str(item.get('label') or 'Threshold')}: {str(item.get('relation') or 'bounded')}"
            for item in list(market_path_support.get("threshold_map") or [])[:3]
        ]
        market_history_block["benchmark_note"] = " ".join(
            part
            for part in [
                str(market_history_block.get("benchmark_note") or "").strip(),
                "Threshold context:",
                " · ".join(threshold_labels) if threshold_labels else "",
            ]
            if part
        ).strip()
        evidence_sources.append(
            {
                "source_id": "market_path_support",
                "label": (
                    f"Market path support · {str(market_path_support.get('provider_source') or 'twelve_data+kronos')}"
                    f" · {str(series_quality_summary.get('quality_label') or 'quality_unrated')}"
                ),
                "url": None,
                "freshness_state": (
                    "current"
                    if int(series_quality_summary.get("stale_days") or 999) <= 5
                    else "degraded_monitoring_mode"
                ),
                "directness": (
                    "proxy"
                    if bool(series_quality_summary.get("uses_proxy_series"))
                    else "direct"
                ),
                "truth_envelope": None,
            }
        )
    market_history_block["benchmark_note"] = (
        f"{market_history_block.get('benchmark_note') or 'Current benchmark note remains bounded.'} "
        "Use forecast support as a secondary scenario aid, not as the primary benchmark frame."
    )
    benchmark_proxy_symbol = _benchmark_proxy_symbol(truth, sleeve_key)
    benchmark_truth = None
    if benchmark_proxy_symbol:
        try:
            benchmark_truth = _market_truth(benchmark_proxy_symbol, "benchmark_proxy")
        except TypeError:
            benchmark_truth = _market_truth(benchmark_proxy_symbol)
    market_history_charts = _market_history_charts(
        truth=truth,
        market=market,
        benchmark_truth=benchmark_truth,
        benchmark_proxy_symbol=benchmark_proxy_symbol,
    )
    scenario_charts = _scenario_charts(
        truth=truth,
        market=market,
        benchmark_truth=benchmark_truth,
        benchmark_proxy_symbol=benchmark_proxy_symbol,
        upgrade_condition=upgrade_condition,
        downgrade_condition=downgrade_condition,
        kill_condition=kill_condition,
    )
    competition_charts = _competition_charts(
        truth=truth,
        market=market,
        benchmark_truth=benchmark_truth,
        benchmark_proxy_symbol=benchmark_proxy_symbol,
    )
    research_support = build_research_support_pack(
        truth=truth,
        target_surface="candidate_report",
        source_authority_fields=source_authority_fields,
        reconciliation_report=reconciliation_report,
        primary_document_manifest=primary_document_manifest,
        recommendation_gate=recommendation_gate,
        data_quality_summary=data_quality_summary,
        implementation_profile=implementation_profile,
        market_context={
            "title": "Market context",
            "summary": str(market_history_block.get("summary") or f"{truth.symbol} market context remains bounded."),
            "instrument_line": (
                f"{truth.symbol} current market data is available through {str(dict(market_history_block.get('field_provenance') or {}).get('instrument.price', {}).get('provider') or 'current quote support')}."
            ),
            "benchmark_line": str(market_history_block.get("benchmark_note") or ""),
            "freshness_note": "Use market context as bounded support for the report rather than as the sole recommendation input.",
        },
        evidence_summary=" ".join(
            part
            for part in [
                str(dict(failure_class_summary or {}).get("summary") or recommendation_gate.get("summary") or "").strip(),
                str(data_quality_summary.get("summary") or "").strip(),
            ]
            if part
        ).strip(),
        decision_line=str(action_boundary_str or visible_decision_state.get("rationale") or constraint_summary.visible_decision_state.rationale or ""),
        drift_surface_id="candidate_report",
        drift_object_id=str(assessment.candidate_id),
        drift_state={
            "gate_state": str(recommendation_gate.get("gate_state") or "review_only"),
            "data_confidence": str(data_quality_summary.get("data_confidence") or recommendation_gate.get("data_confidence") or "mixed"),
            "reconciliation_status": str(reconciliation.get("status") or "verified"),
            "blocked_reason_count": len(list(recommendation_gate.get("blocked_reasons") or [])),
            "critical_missing_count": len(list(recommendation_gate.get("critical_missing_fields") or [])),
        },
        sleeve_key=sleeve_key,
    )
    deep_report_support_state = _build_deep_report_support_state(
        primary_document_manifest=primary_document_manifest,
        quick_brief=quick_brief,
        implementation_profile=implementation_profile,
        source_authority_fields=source_authority_fields,
        evidence_sources=evidence_sources,
        market_path_support=market_path_support,
        coverage_summary=coverage_summary,
        forecast_support=forecast_support,
        scenario_blocks=scenario_blocks,
    )

    generated_at = utc_now_iso()
    base_contract = {
        "contract_version": "0.3.1",
        "surface_id": "candidate_report",
        "generated_at": generated_at,
        "freshness_state": get_freshness("market_price").freshness_class.value,
        "route_cache_state": build_default_route_cache_state(generated_at=generated_at, max_age_seconds=300),
        "surface_state": surface_state(
            "ready",
            reason_codes=[],
            summary="Candidate report is hydrated from instrument truth, market context, rubric, and evidence sources.",
        ),
        "section_states": {
            "investment_case": ready_section(),
            "market_history": ready_section() if market_history_block else degraded_section("no_market_history", "No market-history block was emitted."),
            "scenarios": ready_section() if scenario_blocks else degraded_section("no_scenarios", "No scenario blocks were emitted."),
            "risks": ready_section() if risk_blocks else degraded_section("no_risks", "No risk blocks were emitted."),
            "competition": ready_section() if competition_blocks else degraded_section("no_competition", "No competition blocks were emitted."),
            "evidence": ready_section() if evidence_sources else degraded_section("no_evidence_sources", "No evidence sources were emitted."),
        },
        "candidate_id": assessment.candidate_id,
        "sleeve_id": sleeve_id,
        "sleeve_key": sleeve_key,
        "name": truth.name,
        "benchmark_full_name": institutional_facts.get("benchmark_full_name"),
        "exposure_summary": institutional_facts.get("exposure_summary"),
        "ter_bps": institutional_facts.get("ter_bps"),
        "spread_proxy_bps": institutional_facts.get("spread_proxy_bps"),
        "aum_usd": institutional_facts.get("aum_usd"),
        "aum_state": institutional_facts.get("aum_state"),
        "sg_tax_posture": institutional_facts.get("sg_tax_posture"),
        "distribution_policy": institutional_facts.get("distribution_policy"),
        "replication_risk_note": institutional_facts.get("replication_risk_note"),
        "current_weight_pct": current_weight_pct,
        "weight_state": weight_state,
        "investor_decision_state": investor_decision_state,
        "source_integrity_summary": source_integrity_summary or None,
        "score_decomposition": {
            **score_decomposition,
            "total_score": int(dict(score_decomposition).get("total_score") or 0),
        } if score_decomposition else None,
        "score_summary": score_summary,
        "identity_state": identity_state or None,
        "blocker_category": blocker_category,
        "failure_class_summary": failure_class_summary or None,
        "candidate_row_summary": semantic_summary["candidate_row_summary"],
        "candidate_supporting_factors": semantic_summary["candidate_supporting_factors"],
        "candidate_penalizing_factors": semantic_summary["candidate_penalizing_factors"],
        "report_summary_strip": semantic_summary["report_summary_strip"],
        "source_confidence_label": semantic_summary["source_confidence_label"],
        "coverage_status": coverage_summary["coverage_status"],
        "coverage_workflow_summary": coverage_summary["coverage_workflow_summary"],
        "score_rubric": score_rubric,
        "investment_case": report_explanations["investment_case"],
        "current_implication": report_explanations["current_implication"],
        "action_boundary": action_boundary_str,
        "what_changes_view": report_explanations["what_changes_view"],
        "visible_decision_state": {
            "state": str(visible_decision_state.get("state") or "watch"),
            "allowed_action": str(visible_decision_state.get("allowed_action") or "monitor"),
            "rationale": str(visible_decision_state.get("rationale") or constraint_summary.visible_decision_state.rationale or ""),
        },
        "upgrade_condition": upgrade_condition,
        "downgrade_condition": downgrade_condition,
        "kill_condition": kill_condition,
        "decision_condition_pack": decision_condition_pack,
        "main_tradeoffs": report_explanations["main_tradeoffs"],
        "baseline_comparisons": baseline_comparisons,
        "evidence_depth": _evidence_depth(truth),
        "mandate_boundary": action_boundary_str,
        "doctrine_annotations": constraint_summary.doctrine_annotations,
        "report_tabs": available_tabs,
        "quick_brief": quick_brief,
        "market_history_block": market_history_block,
        "market_history_charts": market_history_charts,
        "scenario_blocks": scenario_blocks,
        "scenario_charts": scenario_charts,
        "risk_blocks": risk_blocks,
        "competition_blocks": competition_blocks,
        "competition_charts": competition_charts,
        "evidence_sources": evidence_sources,
        "decision_thresholds": decision_thresholds,
        "forecast_support": forecast_support,
        "market_path_support": market_path_support,
        "market_path_support_state": deep_report_support_state["market_path_support_state"],
        "forecast_runtime_state": deep_report_support_state["forecast_runtime_state"],
        "forecast_artifact_state": deep_report_support_state["forecast_artifact_state"],
        "scenario_support_state": deep_report_support_state["scenario_support_state"],
        "market_path_objective": market_path_summary["market_path_objective"],
        "market_path_case_note": market_path_summary["market_path_case_note"],
        "holdings_overlay": holdings_overlay,
        "overlay_context": overlay_context,
        "report_cache_state": "live_build",
        "report_generated_at": generated_at,
        "report_source_snapshot_at": generated_at,
        "report_loading_hint": build_default_report_loading_hint(route_cache_state="live_build"),
        "bound_source_snapshot_id": str(source_snapshot_id or "").strip() or None,
        "bound_source_generated_at": str(source_generated_at or "").strip() or None,
        "source_contract_version": str(source_contract_version or "").strip() or None,
        "binding_state": "aligned" if source_snapshot_id or source_generated_at else "unbound_direct",
        "report_build_mode": str(report_build_mode or "snapshot_only"),
        "data_confidence": str(data_quality_summary.get("data_confidence") or recommendation_gate.get("data_confidence") or "mixed"),
        "decision_confidence": str(assessment.interpretation.confidence or "medium"),
        "implementation_profile": implementation_profile,
        "recommendation_gate": recommendation_gate,
        "reconciliation_status": reconciliation,
        "source_authority_fields": source_authority_fields,
        "reconciliation_report": reconciliation_report,
        "data_quality_summary": data_quality_summary,
        "primary_document_manifest": primary_document_manifest,
        "deep_report_support_state": deep_report_support_state,
        "research_support": research_support,
    }
    base_contract = _overlay_bound_explorer_fields(base_contract, bound_explorer_row if source_snapshot_id else None)
    final_contract = apply_overlay(base_contract, holdings=holdings_overlay)
    previous_snapshot = latest_surface_snapshot(
        surface_id="candidate_report",
        object_id=str(assessment.candidate_id),
    )
    prior_snapshot = previous_surface_snapshot(
        surface_id="candidate_report",
        object_id=str(assessment.candidate_id),
    )
    emit_candidate_report_changes(
        str(assessment.candidate_id),
        dict(prior_snapshot.get("contract") or {}) if prior_snapshot else dict(previous_snapshot.get("contract") or {}) if previous_snapshot else None,
        final_contract,
    )
    snapshot_id = record_surface_snapshot(
        surface_id="candidate_report",
        object_id=str(assessment.candidate_id),
        snapshot_kind="recommendation_state",
        state_label=str(visible_decision_state.get("state") or constraint_summary.visible_decision_state.state),
        data_confidence=str(data_quality_summary.get("data_confidence") or recommendation_gate.get("data_confidence") or "mixed"),
        decision_confidence=str(assessment.interpretation.confidence or "medium"),
        generated_at=str(final_contract.get("generated_at") or ""),
        contract=final_contract,
        input_summary={
            "candidate_symbol": truth.symbol,
            "baseline_count": len(baseline_comparisons),
            "evidence_source_count": len(evidence_sources),
            "blocked_reason_count": len(recommendation_gate.get("blocked_reasons") or []),
            "reconciliation_status": reconciliation.get("status"),
        },
        decision_inputs=compact_replay_inputs(
            source_authority_fields=source_authority_fields,
            reconciliation_report=reconciliation_report,
            implementation_profile=implementation_profile,
            recommendation_gate=recommendation_gate,
            truth_envelopes={
                "market_truth": dict(
                    (((market.evidence or [None])[0].facts if market.evidence else {}) or {}).get("truth_envelope") or {}
                )
            },
            primary_document_manifest=primary_document_manifest,
            extra={
                "candidate_symbol": truth.symbol,
                "reconciliation_status": reconciliation,
                "data_quality_summary": data_quality_summary,
                "overlay_context": dict(overlay_context or {}),
            },
        ),
    )
    final_contract["surface_snapshot_id"] = snapshot_id
    return final_contract
