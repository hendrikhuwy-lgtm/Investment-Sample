from __future__ import annotations

from typing import Any

from app.services.blueprint_rebalance import build_rebalance_policy
from app.services.brief_benchmark import build_comparison_context, get_current_benchmark
from app.services.brief_dca import build_dca_guidance, get_current_dca_policy
from app.services.brief_delivery_state import latest_brief_versions
from app.services.brief_fund_rationale import build_fund_selection_table
from app.services.brief_history_compare import build_history_compare
from app.services.brief_tax_location import build_tax_location_guidance
from app.services.cma_engine import build_expected_return_range_section, current_cma_version
from app.services.exposure_aggregator import build_exposure_snapshot
from app.services.ips import get_ips, policy_weights_from_ips
from app.services.ips_generator import build_ips_snapshot
from app.services.policy_assumptions import classify_policy_trust_banner, policy_render_labels
from app.services.portfolio_ingest import latest_upload_run_id
from app.services.portfolio_blueprint import build_portfolio_blueprint_payload
from app.services.scenario_registry import compare_scenarios, get_active_scenario_definitions
from app.services.stress_engine import run_stress_suite


POLICY_PACK_VERSION = "2026.03.08"


def _normalize_policy_weights(raw: dict[str, float]) -> dict[str, float]:
    return {
        "global_equity": float(raw.get("global_equity") or raw.get("global_equities") or 0.0),
        "ig_bond": float(raw.get("ig_bond") or raw.get("ig_bonds") or 0.0),
        "cash": float(raw.get("cash") or raw.get("cash_bills") or 0.0),
        "real_asset": float(raw.get("real_asset") or raw.get("real_assets") or 0.0),
        "alt": float(raw.get("alt") or raw.get("alternatives") or 0.0),
        "convex": float(raw.get("convex") or 0.0),
    }


def _build_rebalancing_section(ips_profile: Any) -> dict[str, Any]:
    rules: list[dict[str, Any]] = []
    for allocation in ips_profile.allocations:
        policy = build_rebalance_policy(
            sleeve_key=str(allocation.sleeve),
            target_weight=float(allocation.target_weight),
            min_band=float(allocation.min_band),
            max_band=float(allocation.max_band),
        )
        rules.append(policy.model_dump(mode="json"))
    return {
        "calendar_review_cadence": str(ips_profile.rebalance_frequency),
        "small_trade_threshold": "Only review small trade actions once multiple sleeves can be netted efficiently.",
        "tax_aware_note": "When turnover is optional, prefer the path with lower expected tax drag and lower implementation friction.",
        "rules": rules,
    }


def _load_prior_policy_weights(conn, current_run_id: str) -> dict[str, float] | None:
    row = conn.execute(
        """
        SELECT brief_run_id
        FROM daily_brief_runs
        WHERE brief_run_id != ?
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        (current_run_id,),
    ).fetchone()
    if row is None:
        return None
    previous_run_id = str(row["brief_run_id"])
    version_payload = latest_brief_versions(conn, previous_run_id)
    if version_payload is None:
        return None
    payload = dict(version_payload.get("payload") or {})
    previous_weights = payload.get("policy_weights")
    if not isinstance(previous_weights, dict):
        return None
    return _normalize_policy_weights(previous_weights)


def _build_core_satellite_summary(blueprint_payload: dict[str, Any]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for sleeve in list(blueprint_payload.get("sleeves") or []):
        range_payload = dict(sleeve.get("policy_weight_range") or {})
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        classification = "core"
        if any(token in sleeve_key for token in ("satellite", "optional", "convex", "alternative")):
            classification = "satellite"
        if sleeve_key in {"ig_bonds", "cash_bills"}:
            classification = "defensive"
        summary.append(
            {
                "sleeve_key": sleeve_key,
                "sleeve_name": str(sleeve.get("name") or sleeve_key.replace("_", " ").title()),
                "classification": classification,
                "min_weight": float(range_payload.get("min") or 0.0) / 100.0,
                "target_weight": float(range_payload.get("target") or 0.0) / 100.0,
                "max_weight": float(range_payload.get("max") or 0.0) / 100.0,
            }
        )
    return summary


def _build_sub_sleeve_breakdown(blueprint_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sleeve in list(blueprint_payload.get("sleeves") or []):
        rows.append(
            {
                "sleeve_key": str(sleeve.get("sleeve_key") or ""),
                "sleeve_name": str(sleeve.get("name") or ""),
                "purpose": str(sleeve.get("purpose") or ""),
                "constraints": list(sleeve.get("constraints") or []),
            }
        )
    return rows


def _policy_relevance_tags(
    *,
    blueprint_payload: dict[str, Any],
    exposures: dict[str, Any],
    long_state: str,
    short_state: str,
) -> list[dict[str, Any]]:
    exposure_rows = list(exposures.get("sleeve_concentration") or [])
    exposure_weight = {str(item.get("sleeve") or ""): float(item.get("weight") or 0.0) for item in exposure_rows}
    sleeve_aliases = {
        "global_equity_core": {"global_equity", "global_equities", "developed_market_equity"},
        "emerging_markets": {"emerging_markets", "em_equity", "em"},
        "china_satellite": {"china", "china_satellite"},
        "IG_bonds": {"ig_bond", "ig_bonds"},
        "cash_bills": {"cash", "cash_bills"},
        "real_assets": {"real_asset", "real_assets"},
        "alternatives": {"alt", "alternatives"},
        "convex": {"convex"},
    }
    blueprint_sleeves = list(blueprint_payload.get("sleeves") or [])
    tags: list[dict[str, Any]] = []
    for label, aliases in sleeve_aliases.items():
        blueprint_hit = any(str(item.get("sleeve_key") or "") in aliases for item in blueprint_sleeves)
        holding_weight = sum(weight for sleeve, weight in exposure_weight.items() if sleeve in aliases)
        if not blueprint_hit and holding_weight <= 0:
            continue
        relevance = "low relevance"
        affects = "benchmark watch only"
        if holding_weight >= 0.1:
            relevance = "high relevance to current holdings"
            affects = "current holdings"
        elif blueprint_hit:
            relevance = "high relevance to target sleeves"
            affects = "policy target"
        elif holding_weight > 0:
            relevance = "medium relevance"
            affects = "implementation watchlist"
        tags.append(
            {
                "sleeve_tag": label,
                "holding_weight": round(holding_weight, 4),
                "blueprint_present": blueprint_hit,
                "relevance": relevance,
                "affects": affects,
            }
        )
    tags.append(
        {
            "sleeve_tag": "regime_watch",
            "holding_weight": 0.0,
            "blueprint_present": True,
            "relevance": "medium relevance",
            "affects": "benchmark watch only" if short_state == "Normal" else "policy target",
            "note": f"Long regime {long_state}; short regime {short_state}.",
        }
    )
    return tags


def build_policy_pack(
    conn,
    *,
    brief_run_id: str,
    default_allocation: dict[str, float] | None,
    long_state: str,
    short_state: str,
) -> dict[str, Any]:
    ips_profile = get_ips(conn)
    policy_weights = _normalize_policy_weights(policy_weights_from_ips(ips_profile))
    if default_allocation:
        for key, value in _normalize_policy_weights(default_allocation).items():
            if value > 0:
                policy_weights[key] = value
    expected_returns = build_expected_return_range_section(conn)
    benchmark = get_current_benchmark(conn)
    benchmark_context = build_comparison_context(benchmark, expected_returns)
    blueprint_payload = build_portfolio_blueprint_payload()
    rebalancing = _build_rebalancing_section(ips_profile)
    ips_snapshot = build_ips_snapshot(
        conn,
        benchmark=benchmark,
        rebalancing_policy=rebalancing,
        cma_version=current_cma_version(conn),
    )
    scenario_definitions = get_active_scenario_definitions(conn)
    stress = run_stress_suite(policy_weights, scenario_definitions=scenario_definitions, conn=conn)
    dca_guidance = build_dca_guidance(get_current_dca_policy(conn))
    history_compare = build_history_compare(conn, current_run_id=brief_run_id)
    prior_policy_weights = _load_prior_policy_weights(conn, brief_run_id)
    scenario_compare = compare_scenarios(
        conn,
        current_weights=policy_weights,
        prior_weights=prior_policy_weights,
    )
    worst_years = [
        float(item.get("worst_year_loss_min") or 0.0)
        for item in list(expected_returns.get("items") or [])
    ] + [
        float(item.get("worst_year_loss_max") or 0.0)
        for item in list(expected_returns.get("items") or [])
    ]
    aggregate_drawdown = {
        "expected_worst_year_loss_min": min(worst_years or [0.0]),
        "expected_worst_year_loss_max": max(worst_years or [0.0]),
        "policy_truth_state": str(stress.get("policy_truth_state") or "developer_seed"),
        "policy_labels": list(stress.get("policy_labels") or []),
        "citations": list(stress.get("citations") or []),
        "source_records": list(stress.get("source_records") or []),
        "scenario_summary": dict(stress.get("summary") or {}),
        "historical_analogs": [
            {
                "label": str(item.get("name")),
                "estimated_impact_pct": float(item.get("estimated_impact_pct") or 0.0),
                "confidence_rating": str(item.get("confidence_rating") or "medium"),
                "policy_truth_state": str(item.get("policy_truth_state") or "developer_seed"),
                "methodology_source_name": item.get("methodology_source_name"),
            }
            for item in list(stress.get("scenarios") or [])[:4]
        ],
        "caveat": (
            "Reference-only drawdown framing based on analog scenarios. Not for allocation decisions."
            if str(stress.get("policy_truth_state") or "developer_seed") in {"developer_seed", "provisional", "stale", "blocked"}
            else "Drawdown framing is aggregate policy context based on analog scenarios, not a position-level loss forecast."
        ),
    }
    active_run_id = latest_upload_run_id(conn)
    exposures = build_exposure_snapshot(conn, run_id=active_run_id) if active_run_id else {}
    policy_states = [
        str(expected_returns.get("policy_truth_state") or "blocked"),
        str(benchmark_context.get("policy_truth_state") or "blocked"),
        str(stress.get("policy_truth_state") or "blocked"),
    ]
    trust_banner = classify_policy_trust_banner(policy_states)
    policy_section_sources = [
        *list(expected_returns.get("source_records") or []),
        *list(benchmark_context.get("source_records") or []),
        *list(stress.get("source_records") or []),
    ]
    review_queue = [
        {
            "item": f"Regime watch: long={long_state}, short={short_state}",
            "action_tag": "scenario_watch",
            "policy_relevance": "monitoring",
        },
        {
            "item": "Benchmark dislocation review: compare current benchmark weights and drawdown analogs.",
            "action_tag": "benchmark_watch",
            "policy_relevance": "blueprint_review",
        },
        {
            "item": "Allocation drift trigger review: confirm whether calendar review cadence remains appropriate.",
            "action_tag": "rebalance_candidate",
            "policy_relevance": "blueprint_review",
        },
        {
            "item": "Scenario deterioration review: inspect whether current analog impacts are broadening versus prior runs.",
            "action_tag": "review",
            "policy_relevance": "dashboard_alerting",
        },
    ]
    return {
        "version": POLICY_PACK_VERSION,
        "trust_banner": trust_banner,
        "policy_truth_state": trust_banner.get("trust_level"),
        "policy_labels": policy_render_labels(
            "sourced" if trust_banner.get("guidance_ready") else "developer_seed"
        ),
        "expected_returns": expected_returns,
        "benchmark": benchmark_context,
        "aggregate_drawdown": aggregate_drawdown,
        "rebalancing_policy": rebalancing,
        "ips_snapshot": ips_snapshot,
        "core_satellite_summary": _build_core_satellite_summary(blueprint_payload),
        "sub_sleeve_breakdown": _build_sub_sleeve_breakdown(blueprint_payload),
        "fund_selection": build_fund_selection_table(blueprint_payload),
        "tax_location_guidance": build_tax_location_guidance(blueprint_payload),
        "dca_guidance": dca_guidance,
        "review_queue": review_queue,
        "history_compare": history_compare,
        "scenario_compare": scenario_compare,
        "scenario_registry": scenario_definitions,
        "stress": stress,
        "policy_source_records": policy_section_sources,
        "portfolio_relevance": _policy_relevance_tags(
            blueprint_payload=blueprint_payload,
            exposures=exposures,
            long_state=long_state,
            short_state=short_state,
        ),
        "current_exposures": exposures,
        "policy_weights": policy_weights,
        "blueprint_payload": blueprint_payload,
    }
