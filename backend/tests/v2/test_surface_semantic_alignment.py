from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest


def _sample_market_series(*, label: str, series_id: str) -> object:
    from app.v2.core.domain_objects import EvidenceCitation, EvidencePack, MarketDataPoint, MarketSeriesTruth

    return MarketSeriesTruth(
        series_id=series_id,
        label=label,
        frequency="daily",
        units="price",
        points=[
            MarketDataPoint(at="2026-04-01T00:00:00+00:00", value=100.0),
            MarketDataPoint(at="2026-04-02T00:00:00+00:00", value=103.0),
            MarketDataPoint(at="2026-04-03T00:00:00+00:00", value=104.5),
        ],
        evidence=[
            EvidencePack(
                evidence_id=f"evidence_{series_id}",
                thesis=f"{label} context remains live.",
                summary=f"{label} context remains live.",
                freshness="fresh_full_rebuild",
                observed_at="2026-04-03T00:00:00+00:00",
                citations=[EvidenceCitation(source_id="test_source", label="Test source")],
            )
        ],
        as_of="2026-04-03T00:00:00+00:00",
    )


def _sample_macro_truth() -> object:
    from app.v2.core.domain_objects import EvidenceCitation, EvidencePack, MacroTruth

    return MacroTruth(
        macro_id="macro_growth",
        regime="Growth holding up",
        summary="Growth and risk appetite remain intact.",
        indicators={"change_pct_1d": 0.6},
        evidence=[
            EvidencePack(
                evidence_id="macro_evidence",
                thesis="Macro remains constructive.",
                summary="Macro remains constructive.",
                freshness="fresh_full_rebuild",
                observed_at="2026-04-03T00:00:00+00:00",
                citations=[EvidenceCitation(source_id="macro_test", label="Macro test source")],
            )
        ],
        as_of="2026-04-03T00:00:00+00:00",
    )


def _sample_instrument(symbol: str, sleeve_key: str = "global_equity_core") -> object:
    from app.v2.core.domain_objects import EvidenceCitation, EvidencePack, InstrumentTruth

    return InstrumentTruth(
        instrument_id=f"instrument_{symbol.lower()}",
        symbol=symbol,
        name=f"{symbol} ETF",
        asset_class="equity",
        benchmark_id="benchmark_global_equity",
        metrics={
            "sleeve_key": sleeve_key,
            "sleeve_affiliation": sleeve_key,
            "benchmark_authority_level": "direct",
            "expense_ratio": 0.002,
            "liquidity_score": 0.9,
            "issuer": "Test Issuer",
            "aum_usd": 5_000_000_000,
            "primary_documents": [
                {
                    "doc_type": "factsheet",
                    "doc_url": "https://example.com/factsheet.pdf",
                    "status": "verified",
                    "authority_class": "issuer_primary",
                    "retrieved_at": "2026-04-03T00:00:00+00:00",
                }
            ],
        },
        evidence=[
            EvidencePack(
                evidence_id=f"evidence_{symbol.lower()}",
                thesis=f"{symbol} remains a valid implementation candidate.",
                summary=f"{symbol} remains a valid implementation candidate.",
                freshness="fresh_full_rebuild",
                observed_at="2026-04-03T00:00:00+00:00",
                citations=[EvidenceCitation(source_id="issuer_factsheet", label="Issuer factsheet")],
                facts={
                    "primary_documents": [
                        {
                            "doc_type": "factsheet",
                            "doc_url": "https://example.com/factsheet.pdf",
                            "status": "verified",
                            "authority_class": "issuer_primary",
                            "retrieved_at": "2026-04-03T00:00:00+00:00",
                        }
                    ]
                },
            )
        ],
        as_of="2026-04-03T00:00:00+00:00",
    )


def _empty_portfolio_truth() -> object:
    from app.v2.core.domain_objects import PortfolioTruth

    return PortfolioTruth(
        portfolio_id="portfolio_default",
        name="Default Portfolio",
        base_currency="USD",
        holdings=[],
        exposures={},
    )


def _signal_support_payload() -> dict[str, object]:
    support = {
        "provider": "deterministic",
        "model_name": "rule_based",
        "horizon": 21,
        "support_strength": "support_only",
        "confidence_summary": "Deterministic support",
        "degraded_state": None,
        "generated_at": "2026-04-04T00:00:00+00:00",
    }
    return {
        "bundle": SimpleNamespace(support=SimpleNamespace(**support)),
        "monitoring_condition": {
            "condition_id": "cond_demo",
            "label": "Watch the current threshold",
            "why_now": "The signal remains directionally important and changes the sleeve review map.",
            "near_term_trigger": "Escalate review if the latest strength holds.",
            "thesis_trigger": "Reduce conviction if the macro read softens.",
            "break_condition": "A reversal would break the current read.",
            "portfolio_consequence": "Portfolio consequence is sleeve-level until overlay specifics are present.",
            "next_action": "Monitor",
            "forecast_support": support,
            "trigger_support": {
                "object_id": "demo",
                "trigger_type": "review_trigger",
                "threshold": "105",
                "source_family": "benchmark_proxy",
                "provider": "deterministic",
                "current_distance_to_trigger": "2.0%",
                "next_action_if_hit": "Review now if the level is cleared.",
                "next_action_if_broken": "Stand down if the level fails.",
                "threshold_state": "watch",
                "support_strength": "support_only",
                "confidence_summary": "Deterministic threshold",
                "degraded_state": None,
                "generated_at": "2026-04-04T00:00:00+00:00",
            },
        },
        "scenario_block": {
            "signal_id": "signal_demo",
            "label": "Base case",
            "summary": "Scenario support remains available.",
            "scenarios": [
                {
                    "scenario_id": "bull",
                    "type": "bull",
                    "label": "Bull",
                    "portfolio_effect": "Adds upside pressure.",
                    "macro": None,
                    "micro": None,
                    "short_term": "Supportive",
                    "long_term": "Constructive",
                }
            ],
            "forecast_support": support,
            "what_confirms": "Trend persistence confirms the read.",
            "what_breaks": "Trend reversal breaks the read.",
            "threshold_summary": "Watch the 105 threshold.",
            "degraded_state": None,
        },
    }


def test_daily_brief_semantics_stay_market_first_without_holdings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "brief_semantics.sqlite3"))
    import app.v2.surfaces.daily_brief.contract_builder as brief_builder

    payload = _signal_support_payload()
    monkeypatch.setattr(brief_builder, "_load_macro_truths", lambda: ([_sample_macro_truth()], "macro_test"))
    monkeypatch.setattr(brief_builder, "_load_market_truths", lambda: [_sample_market_series(label="ACWI", series_id="series_acwi")])
    monkeypatch.setattr(brief_builder, "_load_news_truths", lambda: [])
    monkeypatch.setattr(brief_builder, "get_portfolio_truth", lambda account_id: _empty_portfolio_truth())
    monkeypatch.setattr(brief_builder, "record_change", lambda **_: None)
    monkeypatch.setattr(brief_builder, "build_signal_support_bundle", lambda *args, **kwargs: payload)
    monkeypatch.setattr(
        brief_builder,
        "forecast_panel_from_bundle",
        lambda **kwargs: {
            "panel_id": kwargs["panel_id"],
            "title": kwargs["title"],
            "chart_type": "scenario_support",
            "primary_series": None,
            "summary": kwargs["summary"],
            "what_to_notice": kwargs["what_to_notice"],
            "degraded_state": "no_series_available",
            "freshness_state": "fresh_full_rebuild",
            "trust_state": "proxy_support",
        },
    )

    contract = brief_builder.build()

    assert contract["what_changed"]
    assert all("market context is" not in signal["summary"].lower() for signal in contract["what_changed"])
    assert all("no measurable 1-day change" not in signal["summary"].lower() for signal in contract["what_changed"])
    assert contract["why_it_matters_here"]
    assert "rather than gating on current holdings" in contract["why_it_matters_here"].lower()
    assert contract["data_timeframes"]
    assert all("no explicit truth clock emitted" not in row["summary"].lower() for row in contract["data_timeframes"])


def test_blueprint_semantics_keep_overlay_secondary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "blueprint_semantics.sqlite3"))
    import app.v2.surfaces.blueprint.explorer_contract_builder as explorer_builder

    candidate_rows = [
        {"symbol": "VWRA", "sleeve_key": "global_equity_core", "aum": 5_000_000_000, "aum_usd": 5_000_000_000, "liquidity_score": 0.9, "expense_ratio": 0.002},
        {"symbol": "CSPX", "sleeve_key": "global_equity_core", "aum": 3_000_000_000, "aum_usd": 3_000_000_000, "liquidity_score": 0.8, "expense_ratio": 0.0025},
    ]
    monkeypatch.setattr(explorer_builder, "export_live_candidate_registry", lambda conn: candidate_rows)
    monkeypatch.setattr(explorer_builder, "seed_default_candidate_registry", lambda conn: None)
    monkeypatch.setattr(explorer_builder, "record_change", lambda **_: None)
    monkeypatch.setattr(explorer_builder, "get_portfolio_truth", lambda account_id: _empty_portfolio_truth())
    monkeypatch.setattr(explorer_builder, "_market_truth", lambda symbol, endpoint_family="ohlcv_history": _sample_market_series(label=symbol, series_id=f"series_{symbol.lower()}"))
    monkeypatch.setattr(explorer_builder, "get_instrument_truth", lambda symbol: _sample_instrument(symbol))
    monkeypatch.setattr(explorer_builder, "get_freshness", lambda source_id: SimpleNamespace(freshness_class=SimpleNamespace(value="fresh_full_rebuild")))

    contract = explorer_builder.build()
    sleeve = contract["sleeves"][0]
    candidate = sleeve["candidates"][0]

    assert candidate["portfolio_fit_now"] in {"Highest", "Good", "Weak today"}
    assert candidate["capital_priority_now"] in {"First call on next dollar", "Second choice", "No new capital"}
    assert "overlay-dependent" not in candidate["what_blocks_action"].lower()
    assert "refines incumbent and funding detail" in sleeve["capital_memo"].lower()
    assert candidate["action_boundary"] != "Review sleeve evidence and benchmark context before moving capital."
    assert isinstance(candidate["recommendation_gate"], dict)
    assert isinstance(candidate["data_quality_summary"], dict)


def test_candidate_report_keeps_forecast_support_out_of_primary_evidence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "report_semantics.sqlite3"))
    import app.v2.surfaces.blueprint.report_contract_builder as report_builder

    monkeypatch.setattr(report_builder, "get_portfolio_truth", lambda account_id: _empty_portfolio_truth())
    monkeypatch.setattr(report_builder, "_market_truth", lambda symbol, endpoint_family="ohlcv_history": _sample_market_series(label=symbol, series_id=f"series_{symbol.lower()}"))
    monkeypatch.setattr(report_builder, "get_instrument_truth", lambda candidate_id: _sample_instrument("VWRA"))
    monkeypatch.setattr(
        report_builder,
        "get_benchmark_truth",
        lambda benchmark_id: SimpleNamespace(name="ACWI benchmark", benchmark_authority_level="direct"),
    )

    contract = report_builder.build("VWRA")

    assert contract["evidence_sources"]
    assert all("forecast support" not in str(source["label"]).lower() for source in contract["evidence_sources"])
    assert contract["primary_document_manifest"] is not None


def test_evidence_workspace_keeps_forecast_support_out_of_primary_documents(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "evidence_semantics.sqlite3"))
    import app.v2.surfaces.evidence_workspace.contract_builder as evidence_builder
    from app.v2.core.domain_objects import EvidenceCitation, EvidencePack

    truth = _sample_instrument("VWRA")
    evidence_pack = EvidencePack(
        evidence_id="pack_vwra",
        thesis="VWRA evidence pack",
        summary="VWRA retains direct issuer support.",
        freshness="fresh_full_rebuild",
        observed_at="2026-04-03T00:00:00+00:00",
        citations=[EvidenceCitation(source_id="issuer_factsheet", label="Issuer factsheet")],
        facts={
            "field_truths": [
                {
                    "field_name": "expense_ratio",
                    "resolved_value": "0.20%",
                    "source_name": "Issuer factsheet",
                    "source_type": "issuer_factsheet_secondary",
                    "value_type": "fresh_full_rebuild",
                }
            ],
            "primary_documents": truth.metrics.get("primary_documents"),
            "candidate_symbol": truth.symbol,
        },
    )
    monkeypatch.setattr(evidence_builder, "get_instrument_truth", lambda candidate_id: truth)
    monkeypatch.setattr(evidence_builder, "build_evidence_pack", lambda candidate_id: evidence_pack)
    monkeypatch.setattr(
        evidence_builder,
        "build_candidate_truth_context",
        lambda conn, candidate: {
            "source_authority_map": [
                {
                    "field_name": "expense_ratio",
                    "label": "Expense ratio",
                    "resolved_value": "0.20%",
                    "authority_class": "issuer_primary",
                    "freshness_state": "current",
                    "recommendation_critical": True,
                    "document_support_state": "present",
                }
            ],
            "reconciliation_report": [],
            "data_quality": {"data_confidence": "high", "critical_fields_ready": 1, "critical_fields_total": 1, "summary": "Critical fields are ready."},
            "primary_document_manifest": truth.metrics.get("primary_documents"),
        },
    )
    monkeypatch.setattr(
        evidence_builder,
        "read_workspace",
        lambda candidate_id: {
            "documents": [],
            "claims": [],
            "mappings": [],
            "tax_assumptions": [],
            "gaps": [],
            "object_links": [],
        },
    )
    monkeypatch.setattr(
        evidence_builder,
        "list_forecast_evidence_refs",
        lambda candidate_id: [
            {
                "evidence_ref_id": "forecast_ref_1",
                "evidence_label": "Forecast support",
                "summary": "Forecast support item",
                "provider": "chronos",
                "model_name": "chronos",
                "support_strength": "support_only",
                "freshness_state": "fresh_full_rebuild",
                "object_type": "candidate",
                "object_id": "candidate_instrument_vwra",
                "object_label": "VWRA ETF",
                "created_at": "2026-04-03T00:00:00+00:00",
            }
        ],
    )
    monkeypatch.setattr(evidence_builder, "record_change", lambda **_: None)

    contract = evidence_builder.build("VWRA")

    assert contract["forecast_support_items"]
    assert all(document["document_type"] != "forecast_support" for document in contract["documents"])
    candidate_group = next(group for group in contract["object_groups"] if group["title"] == "Candidate")
    candidate_claim_text = " ".join(claim["claim_text"] for item in candidate_group["items"] for claim in item["claims"])
    assert "Forecast support item" not in candidate_claim_text
