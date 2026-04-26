from __future__ import annotations

from app.services.blueprint_liquidity import evaluate_liquidity_profile
from app.services.blueprint_rebalance import build_rebalance_policy, evaluate_rebalance_diagnostics
from app.services.blueprint_risk_controls import evaluate_concentration_controls, get_concentration_policy, summarize_concentration_status
from app.services.portfolio_blueprint import build_portfolio_blueprint_payload


def test_concentration_evaluator_returns_unknown_with_blockers_when_inputs_missing() -> None:
    controls = evaluate_concentration_controls(
        candidate={},
        sleeve_key="global_equity_core",
        target_weight_pct=45.0,
        policy=get_concentration_policy(),
        warning_buffer=0.9,
    )
    by_metric = {str(item["metric_name"]): item for item in controls}
    assert by_metric["Dominant country weight proxy"]["status"] == "unknown"
    assert "US weight missing" in list(by_metric["Dominant country weight proxy"]["blockers"] or [])
    assert by_metric["Top-10 concentration"]["status"] == "unknown"
    assert "issuer concentration unavailable" in list(by_metric["Top-10 concentration"]["blockers"] or [])


def test_concentration_evaluator_warns_when_candidate_is_near_policy_limit() -> None:
    controls = evaluate_concentration_controls(
        candidate={
            "us_weight_pct": 64.0,
            "tech_weight_pct": 33.0,
            "top10_concentration_pct": 27.5,
            "em_weight_pct": 18.5,
        },
        sleeve_key="global_equity_core",
        target_weight_pct=52.0,
        policy=get_concentration_policy(),
        warning_buffer=0.9,
    )
    summary = summarize_concentration_status(controls)
    assert summary["status"] in {"warn", "fail"}
    assert summary["warn"] >= 1


def test_liquidity_profile_marks_missing_spread_as_unknown_without_fabricating_liquidation_days() -> None:
    profile = evaluate_liquidity_profile(
        candidate={
            "instrument_type": "etf_ucits",
            "liquidity_score": 0.82,
            "bid_ask_spread_proxy": None,
        },
        sleeve_key="ig_bonds",
    )
    assert profile["liquidity_status"] == "adequate"
    assert profile["spread_status"] == "unknown"
    assert "spread proxy unavailable" in list(profile["blockers"] or [])
    assert profile["days_to_liquidate_estimate"] is None


def test_liquidity_profile_uses_limited_evidence_when_proxy_fields_exist_without_score() -> None:
    profile = evaluate_liquidity_profile(
        candidate={
            "instrument_type": "etf_ucits",
            "liquidity_score": None,
            "bid_ask_spread_proxy": 7.0,
            "liquidity_proxy": "LSE primary listing with broad secondary market participation.",
        },
        sleeve_key="global_equity_core",
    )
    assert profile["liquidity_status"] == "limited_evidence"
    assert profile["spread_status"] == "tight"
    assert profile["explanation"]
    assert "liquidity_score" in list(profile["missing_inputs"] or [])


def test_rebalance_diagnostics_emit_interdependency_warning_for_undercovered_convex() -> None:
    policy = build_rebalance_policy(sleeve_key="convex", target_weight=0.03, min_band=0.03, max_band=0.03)
    diagnostics = evaluate_rebalance_diagnostics(
        policy=policy,
        actual_weight=0.01,
        related_weights={"global_equity_core": 0.50, "convex": 0.01},
    )
    assert diagnostics["status"] == "interdependency_warning"
    assert "convex_vs_equity_ratio" in list(diagnostics["triggered_rules"] or [])


def test_blueprint_payload_exposes_release1_risk_and_rebalance_fields() -> None:
    payload = build_portfolio_blueprint_payload()
    assert payload["blueprint_meta"]["profile_type"] == "hnwi_sg"
    assert isinstance(payload["blueprint_meta"]["rebalance_summary"], dict)
    assert int(payload["blueprint_meta"]["payload_integrity_version"]) >= 2
    assert "decision_record.mandate_fit_state" in list(payload["blueprint_meta"]["required_detail_fields"] or [])

    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    assert sleeve.get("rebalance_policy")
    assert sleeve.get("rebalance_diagnostics")

    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    lens = dict(candidate.get("investment_lens") or {})
    assert isinstance(list(lens.get("risk_controls") or []), list)
    assert dict(lens.get("risk_control_summary") or {}).get("status") in {"pass", "warn", "fail", "unknown"}
    assert dict(lens.get("liquidity_profile") or {}).get("liquidity_status") in {"strong", "adequate", "weak", "limited_evidence", "unknown"}
    assert "explanation" in dict(lens.get("liquidity_profile") or {})
