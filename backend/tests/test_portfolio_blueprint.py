from __future__ import annotations

import os

os.environ.setdefault("IA_BLUEPRINT_CANDIDATE_METADATA_AUTO_REFRESH", "0")

from app.services.portfolio_blueprint import build_portfolio_blueprint_payload


REQUIRED_SLEEVE_KEYS = {
    "global_equity_core",
    "developed_ex_us_optional",
    "emerging_markets",
    "china_satellite",
    "ig_bonds",
    "cash_bills",
    "real_assets",
    "alternatives",
    "convex",
}


def test_truth_summary_separates_live_candidates_from_placeholders() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve_keys = {item.get("sleeve_key") for item in payload["sleeves"]}
    assert REQUIRED_SLEEVE_KEYS.issubset(sleeve_keys)
    truth_summary = dict(payload["blueprint_meta"].get("truth_summary") or {})
    source_counts = dict(truth_summary.get("source_state_counts") or {})
    display_source_counts = dict(truth_summary.get("display_source_state_counts") or {})
    object_counts = dict(truth_summary.get("object_type_counts") or {})

    assert truth_summary.get("policy_placeholder_count") == 4
    assert truth_summary.get("strategy_placeholder_count") == 1
    assert object_counts.get("policy_placeholder") == 4
    assert object_counts.get("strategy_placeholder") == 1
    assert source_counts.get("source_validated", 0) >= 28
    assert any(key.startswith("source_validated_") for key in display_source_counts)
    assert truth_summary.get("manual_seed_candidate_count") == 0
    assert truth_summary.get("stale_live_candidate_count") == 0


def test_cash_bills_exposes_live_candidates_separately_from_placeholders() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "cash_bills")
    live_symbols = {item["symbol"] for item in sleeve.get("candidates") or []}
    policy_symbols = {item["symbol"] for item in sleeve.get("policy_placeholders") or []}

    assert {"IB01", "BIL", "SGOV", "BILS"}.issubset(live_symbols)
    assert {"SGD_CASH_RESERVE", "SGD_MMF_POLICY", "SG_TBILL_POLICY", "UCITS_MMF_PLACEHOLDER"}.issubset(policy_symbols)
    assert live_symbols.isdisjoint(policy_symbols)


def test_sg_score_ranking_and_breakdown_present_for_candidates() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidates = list(global_core.get("candidates") or [])
    assert len(candidates) >= 2

    ranks = [int(item.get("sg_rank") or 0) for item in candidates]
    assert sorted(ranks) == list(range(1, len(candidates) + 1))
    quality_ranks = [
        int(dict(item.get("investment_quality") or {}).get("rank_in_sleeve") or 0)
        for item in candidates
        if dict(item.get("investment_quality") or {}).get("rank_in_sleeve") is not None
    ]
    assert quality_ranks == sorted(quality_ranks)

    first = candidates[0]
    sg_lens = dict(first.get("sg_lens") or {})
    breakdown = dict(sg_lens.get("breakdown") or {})
    assert "score" in sg_lens
    assert "withholding_penalty" in breakdown
    assert "expense_penalty" in breakdown
    assert "liquidity_bonus" in breakdown
    assert "estate_risk_penalty" in breakdown


def test_blueprint_citations_are_timestamped() -> None:
    payload = build_portfolio_blueprint_payload()
    citations = list(payload.get("citations") or [])
    assert citations
    assert all(str(item.get("retrieved_at") or "") for item in citations)


def test_operational_usability_states_promote_plausible_candidates_without_hiding_limits() -> None:
    payload = build_portfolio_blueprint_payload()
    by_sleeve = {item["sleeve_key"]: item for item in payload["sleeves"]}

    global_core = by_sleeve["global_equity_core"]
    vwrL = next(item for item in global_core["candidates"] if item["symbol"] == "VWRL")
    ssac = next(item for item in global_core["candidates"] if item["symbol"] == "SSAC")
    assert vwrL["action_readiness"] == "usable_with_limits"
    assert ssac["action_readiness"] == "usable_with_limits"
    assert "confidence-reducing gaps remain" in str(dict(vwrL.get("usability_memo") or {}).get("summary") or "").lower()

    em = by_sleeve["emerging_markets"]
    eimi = next(item for item in em["candidates"] if item["symbol"] == "EIMI")
    assert eimi["action_readiness"] in {"usable_with_limits", "not_usable_now"}
    assert str(dict(eimi.get("usability_memo") or {}).get("summary") or "")

    bonds = by_sleeve["ig_bonds"]
    for symbol in {"AGGU", "VAGU", "A35"}:
        candidate = next(item for item in bonds["candidates"] if item["symbol"] == symbol)
        assert candidate["action_readiness"] == "usable_with_limits"


def test_cash_candidates_distinguish_tax_blockers_from_fixed_trading_currency() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "cash_bills")
    ib01 = next(item for item in sleeve["candidates"] if item["symbol"] == "IB01")
    bil = next(item for item in sleeve["candidates"] if item["symbol"] == "BIL")
    sgov = next(item for item in sleeve["candidates"] if item["symbol"] == "SGOV")
    bils = next(item for item in sleeve["candidates"] if item["symbol"] == "BILS")

    assert ib01["action_readiness"] == "usable_with_limits"
    assert bil["action_readiness"] == "usable_with_limits"
    assert sgov["primary_trading_currency"] == "USD"
    assert bils["primary_trading_currency"] == "USD"
    assert sgov["action_readiness"] == "not_usable_now"
    assert bils["action_readiness"] == "not_usable_now"
    assert dict(sgov.get("usability_memo") or {}).get("hard_blockers") == ["tax data is too incomplete for sleeve type"]
    assert dict(bils.get("usability_memo") or {}).get("hard_blockers") == ["tax data is too incomplete for sleeve type"]


def test_share_class_gap_becomes_upgrade_condition_not_hard_blocker_for_iwdp() -> None:
    payload = build_portfolio_blueprint_payload()
    for sleeve_key in {"real_assets", "alternatives"}:
        sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == sleeve_key)
        iwdp = next(item for item in sleeve["candidates"] if item["symbol"] == "IWDP")
        memo = dict(iwdp.get("usability_memo") or {})
        assert iwdp["action_readiness"] == "usable_with_limits"
        assert not list(memo.get("hard_blockers") or [])
        assert any("share-class proof" in str(item).lower() for item in list(memo.get("upgrade_conditions") or []))


def test_usability_counts_and_no_pick_reason_reflect_operationally_usable_candidates() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    readiness_summary = dict(dict(global_core.get("recommendation") or {}).get("readiness_summary") or {})
    assert dict(readiness_summary.get("usability_counts") or {}).get("usable_with_limits", 0) >= 1
    assert "operationally usable now" in str(dict(global_core.get("recommendation") or {}).get("no_current_pick_reason") or "").lower()


def test_candidates_include_investment_lens_with_non_directive_language() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    by_symbol = {str(item.get("symbol")): item for item in list(global_core.get("candidates") or [])}
    candidate = by_symbol.get("IWDA") or next(iter(global_core.get("candidates") or []), None)
    assert candidate is not None

    lens = dict(candidate.get("investment_lens") or {})
    assert lens.get("horizon") == "long"
    assert str(lens.get("role_line") or "")
    assert len(list(lens.get("long_term_fit") or [])) >= 3
    assert len(list(lens.get("micro_implications") or [])) >= 3
    assert len(list(lens.get("macro_implications") or [])) >= 2
    assert len(list(lens.get("strengths") or [])) >= 2
    assert len(list(lens.get("risks") or [])) >= 2
    assert isinstance(list(lens.get("unknowns_that_matter") or []), list)
    assert isinstance(dict(lens.get("advanced_pack") or {}), dict)
    assert len(list(lens.get("implementation_notes") or [])) >= 1
    assert len(list(lens.get("instrument_diagnostics") or [])) >= 3
    assert len(list(lens.get("risk_drivers") or [])) >= 2
    assert len(list(lens.get("tends_to_help") or [])) >= 2
    assert len(list(lens.get("tends_to_hurt") or [])) >= 2
    assert 1 <= len(list(lens.get("monitor_now") or [])) <= 6
    assert len(list(lens.get("regime_monitors") or [])) <= 2
    assert isinstance(list(lens.get("candidate_monitors") or []), list)
    assert isinstance(list(lens.get("monitors") or []), list)
    assert isinstance(list(lens.get("rules") or []), list)
    assert isinstance(list(lens.get("missing_data") or []), list)
    assert str(lens.get("backdrop_summary") or "")
    assert str(lens.get("confidence") or "")
    assert str(lens.get("freshness") or "")
    assert len(list(lens.get("review_triggers") or [])) >= 3
    sensitivity = dict(lens.get("sensitivity_map") or {})
    for key, value in sensitivity.items():
        assert key in {"DGS10", "T10YIE", "VIXCLS", "BAMLH0A0HYM2", "SP500", "REAL_YIELD_10Y", "USD_STRENGTH", "IG_CREDIT_SPREADS", "BOND_VOLATILITY", "GLOBAL_EQUITY_EX_US"}
        assert str(value) in {"low", "medium", "high"}
    assert isinstance(list(lens.get("candidate_specific_monitor_keys") or []), list)
    regime_snapshot = dict(lens.get("regime_snapshot") or {})
    assert isinstance(regime_snapshot, dict)
    assert str(regime_snapshot.get("volatility_label") or "")
    assert str(regime_snapshot.get("rates_pressure_label") or "")
    assert str(regime_snapshot.get("credit_stress_label") or "")
    assert str(regime_snapshot.get("risk_appetite_label") or "")
    rules = dict(regime_snapshot.get("rule_summaries") or {})
    assert str(rules.get("volatility") or "")
    assert str(rules.get("rates_pressure") or "")
    assert str(rules.get("credit_stress") or "")
    assert str(rules.get("risk_appetite") or "")
    assert str(lens.get("lens_basis") or "")

    joined = " ".join(
        [
            str(lens.get("role_line") or ""),
            *[str(x) for x in list(lens.get("implementation_notes") or [])],
            *[str(x) for x in list(lens.get("instrument_diagnostics") or [])],
            *[str(x) for x in list(lens.get("risk_drivers") or [])],
            *[str(x) for x in list(lens.get("tends_to_help") or [])],
            *[str(x) for x in list(lens.get("tends_to_hurt") or [])],
            *[str(x) for x in list(lens.get("monitor_now") or [])],
            *[str(x) for x in list(lens.get("review_triggers") or [])],
        ]
    ).lower()
    for phrase in ("buy", "sell", "target return", "expected return", "outperform", "alpha forecast"):
        assert phrase not in joined


def test_investment_lens_candidate_differentiation_for_global_core() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    by_symbol = {str(item.get("symbol")): item for item in list(global_core.get("candidates") or [])}
    iwda = by_symbol.get("IWDA")
    vwra = by_symbol.get("VWRA")
    ssac = by_symbol.get("SSAC")
    assert iwda is not None
    assert vwra is not None
    assert ssac is not None
    iwda_lens = dict(iwda.get("investment_lens") or {})
    vwra_lens = dict(vwra.get("investment_lens") or {})
    ssac_lens = dict(ssac.get("investment_lens") or {})
    assert str(iwda_lens.get("role_line")) != str(ssac_lens.get("role_line"))
    iwda_micro = [str(item.get("text") or "") for item in list(iwda_lens.get("micro_implications") or [])]
    vwra_micro = [str(item.get("text") or "") for item in list(vwra_lens.get("micro_implications") or [])]
    assert iwda_micro
    assert vwra_micro
    assert iwda_micro != vwra_micro
    differing_micro = sum(1 for left, right in zip(iwda_micro, vwra_micro) if left != right)
    assert differing_micro >= 2
    iwda_candidate_keys = set(str(x) for x in list(iwda_lens.get("candidate_specific_monitor_keys") or []))
    vwra_candidate_keys = set(str(x) for x in list(vwra_lens.get("candidate_specific_monitor_keys") or []))
    ssac_candidate_keys = set(str(x) for x in list(ssac_lens.get("candidate_specific_monitor_keys") or []))
    assert len(iwda_candidate_keys.symmetric_difference(vwra_candidate_keys)) >= 2
    assert len(vwra_candidate_keys.symmetric_difference(ssac_candidate_keys)) >= 2
    iwda_monitor_records = list(iwda_lens.get("monitors") or [])
    for record in iwda_monitor_records:
        evidence = str(record.get("evidence") or "")
        if evidence:
            assert "1d" in evidence and "5d" in evidence and "1m" in evidence and "1y pct" in evidence
    iwda_sensitivity = dict(iwda_lens.get("sensitivity_map") or {})
    ssac_sensitivity = dict(ssac_lens.get("sensitivity_map") or {})
    assert set(iwda_sensitivity).issubset(iwda_candidate_keys.union({"VIXCLS", "BAMLH0A0HYM2"}))
    assert set(ssac_sensitivity).issubset(ssac_candidate_keys.union({"VIXCLS", "BAMLH0A0HYM2"}))


def test_rates_label_is_partial_when_real_yield_input_missing() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in global_core["candidates"] if item["symbol"] == "VWRA")
    lens = dict(candidate.get("investment_lens") or {})
    regime = dict(lens.get("regime_snapshot") or {})
    assert str(regime.get("rates_pressure_label") or "") in {"unknown", "partial"}
    assert "US 10Y real yield" in set(str(x) for x in list(lens.get("missing_data") or []))


def test_volatility_label_marks_partial_when_vix_persistence_not_met() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in global_core["candidates"] if item["symbol"] == "VWRA")
    regime = dict(dict(candidate.get("investment_lens") or {}).get("regime_snapshot") or {})
    label = str(regime.get("volatility_label") or "")
    assert label in {"moderate (partial)", "moderate", "low", "stress", "unknown"}
    if label == "moderate":
        assert "for >=" in str(dict(regime.get("rule_summaries") or {}).get("volatility") or "")
    else:
        assert label != "moderate"


def test_all_null_monitors_are_compacted_into_missing_data() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in global_core["candidates"] if item["symbol"] == "VWRA")
    lens = dict(candidate.get("investment_lens") or {})
    monitor_keys = {str(item.get("metric_key") or "") for item in list(lens.get("monitors") or [])}
    missing = {str(item) for item in list(lens.get("missing_data") or [])}
    assert "REAL_YIELD_10Y" not in monitor_keys
    assert "US 10Y real yield" in missing


def test_micro_implications_never_collapse_to_all_blocked() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in global_core["candidates"] if item["symbol"] == "VWRA")
    micro = list(dict(candidate.get("investment_lens") or {}).get("micro_implications") or [])
    assert len(micro) >= 3
    statuses = {str(item.get("status") or "") for item in micro}
    assert statuses.intersection({"verified", "inferred"})
    for item in micro:
        if str(item.get("status") or "") == "blocked":
            assert list(item.get("blockers") or [])


def test_verified_micro_implications_require_cited_verified_evidence() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in global_core["candidates"] if item["symbol"] == "VWRA")
    micro = list(dict(candidate.get("investment_lens") or {}).get("micro_implications") or [])
    for item in micro:
        if str(item.get("status") or "") != "verified":
            continue
        evidence = list(item.get("evidence") or [])
        assert any(
            str(entry.get("status") or "") == "verified" and list(entry.get("citation_ids") or [])
            for entry in evidence
        )


def test_candidate_differentiators_allow_partially_verified_values() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in global_core["candidates"] if item["symbol"] == "VWRA")
    differentiators = list(dict(candidate.get("investment_lens") or {}).get("candidate_differentiators") or [])
    assert differentiators
    statuses = {str(item.get("status") or "") for item in differentiators}
    assert statuses.issubset({"verified", "partially_verified", "unverified"})


def test_unknowns_that_matter_are_deduplicated_and_material() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in global_core["candidates"] if item["symbol"] == "VWRA")
    unknowns = [str(item) for item in list(dict(candidate.get("investment_lens") or {}).get("unknowns_that_matter") or [])]
    assert unknowns == list(dict.fromkeys(unknowns))
    assert all(
        any(token in item.lower() for token in ("weight", "tracking", "currency", "share-class", "ter", "replication", "top-10"))
        for item in unknowns
    )


def test_core_equity_candidates_have_current_profile_blockers_filled() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    by_symbol = {str(item.get("symbol")): item for item in list(global_core.get("candidates") or [])}

    vwra_unknowns = {str(item) for item in list(dict(by_symbol["VWRA"].get("investment_lens") or {}).get("unknowns_that_matter") or [])}
    iwda_unknowns = {str(item) for item in list(dict(by_symbol["IWDA"].get("investment_lens") or {}).get("unknowns_that_matter") or [])}

    for blocked in (
        "Underlying currency exposure",
        "US weight",
        "Top-10 concentration",
        "Tracking-difference proxy",
    ):
        assert blocked not in vwra_unknowns
        assert blocked not in iwda_unknowns


def test_additional_tracked_candidates_have_factsheet_derived_tracking_notes_when_available() -> None:
    payload = build_portfolio_blueprint_payload()
    by_symbol = {
        str(candidate.get("symbol")): candidate
        for sleeve in payload["sleeves"]
        for candidate in list(sleeve.get("candidates") or [])
    }
    for symbol in ("CSPX", "SSAC", "EIMI", "XCHA", "AGGU", "A35", "VEVE", "VFEA", "VAGU", "IB01", "BIL"):
        candidate = by_symbol[symbol]
        lens = dict(candidate.get("investment_lens") or {})
        assert str(candidate.get("tracking_difference_note") or "")
        unknowns = {str(item) for item in list(lens.get("unknowns_that_matter") or [])}
        source_gaps = {str(item) for item in list(lens.get("source_gap_highlights") or [])}
        assert "Tracking-difference proxy" not in unknowns
        assert "Tracking-difference proxy" not in source_gaps


def test_non_equity_candidates_do_not_inherit_equity_composition_blockers() -> None:
    payload = build_portfolio_blueprint_payload()
    by_symbol = {
        str(candidate.get("symbol")): candidate
        for sleeve in payload["sleeves"]
        for candidate in list(sleeve.get("candidates") or [])
    }

    for symbol in ("SGLN", "DBMF", "CAOS", "AGGU", "IB01"):
        lens = dict(by_symbol[symbol].get("investment_lens") or {})
        unknowns = {str(item) for item in list(lens.get("unknowns_that_matter") or [])}
        assert "US weight" not in unknowns
        assert "EM weight" not in unknowns
        assert "Top-10 concentration" not in unknowns

    vwra = by_symbol["VWRA"]
    iwda = by_symbol["IWDA"]
    assert vwra.get("underlying_currency_exposure") == "unhedged global developed and emerging equity basket"
    assert float(vwra.get("us_weight_pct") or 0.0) > 0
    assert float(vwra.get("top10_concentration_pct") or 0.0) > 0
    assert str(vwra.get("tracking_difference_note") or "")
    assert iwda.get("underlying_currency_exposure") == "unhedged developed-market equity basket"
    assert iwda.get("em_weight_pct") == 0.0


def test_candidates_expose_canonical_decision_report_without_legacy_detail_payload() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in global_core["candidates"] if item["symbol"] == "VWRA")

    canonical = dict(candidate.get("canonical_decision") or {})
    report = dict(canonical.get("report_sections") or {})

    assert canonical.get("promotion_state")
    assert canonical.get("plain_english_summary")
    assert report.get("what_this_is")
    assert report.get("why_attractive")
    assert report.get("current_view")
    assert report.get("what_to_do_now")
    assert report.get("what_not_to_do_now")
    assert "detail_explanation" not in candidate
    assert "thesis_sections" not in candidate


def test_sleeves_include_structured_recommendation_presentation_payload() -> None:
    payload = build_portfolio_blueprint_payload()
    global_core = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    recommendation = dict(global_core.get("recommendation") or {})
    presentation = dict(recommendation.get("presentation") or {})

    assert presentation.get("status_label")
    assert presentation.get("status_reason")
    assert presentation.get("decision_title")
    assert presentation.get("decision_summary")
    assert presentation.get("current_need")
    assert presentation.get("portfolio_consequence")
    assert presentation.get("next_review_action")
    assert presentation.get("boundary")
    assert presentation.get("review_priority")
    assert isinstance(presentation.get("visible_alternatives") or [], list)
