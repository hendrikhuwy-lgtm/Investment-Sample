from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest


def _forecast_bundle_dict() -> tuple[object, dict[str, object]]:
    class _Support:
        provider = "deterministic"
        model_name = "rule_based"
        horizon = 21
        support_strength = "support_only"
        confidence_summary = "Deterministic support"
        degraded_state = None
        generated_at = "2026-04-04T00:00:00+00:00"

        def to_dict(self) -> dict[str, object]:
            return {
                "provider": self.provider,
                "model_name": self.model_name,
                "horizon": self.horizon,
                "support_strength": self.support_strength,
                "confidence_summary": self.confidence_summary,
                "degraded_state": self.degraded_state,
                "generated_at": self.generated_at,
            }

    bundle = SimpleNamespace(
        support=_Support(),
        result=SimpleNamespace(freshness_state="fresh_full_rebuild"),
        forecast_run_id="forecast_run_demo",
    )
    payload = {
        "bundle": bundle,
        "monitoring_condition": {
            "condition_id": "cond_demo",
            "label": "Watch the current threshold",
            "why_now": "The signal remains directionally important.",
            "near_term_trigger": "If the path continues higher, escalate review.",
            "thesis_trigger": "If the regime weakens, trim conviction.",
            "break_condition": "A reversal would break the current read.",
            "portfolio_consequence": "Portfolio consequence is sleeve-level until overlay specifics are present.",
            "next_action": "Monitor",
            "forecast_support": bundle.support.to_dict(),
            "trigger_support": {
                "object_id": "demo",
                "trigger_type": "review_trigger",
                "threshold": "105",
                "source_family": "benchmark_proxy",
                "provider": "deterministic",
                "current_distance_to_trigger": "2.0%",
                "next_action_if_hit": "Review now",
                "next_action_if_broken": "Stand down",
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
                },
                {
                    "scenario_id": "base",
                    "type": "base",
                    "label": "Base",
                    "portfolio_effect": "Keeps posture stable.",
                    "macro": None,
                    "micro": None,
                    "short_term": "Stable",
                    "long_term": "Bounded",
                },
                {
                    "scenario_id": "bear",
                    "type": "bear",
                    "label": "Bear",
                    "portfolio_effect": "Raises review intensity.",
                    "macro": None,
                    "micro": None,
                    "short_term": "Risky",
                    "long_term": "Weaker",
                },
            ],
            "forecast_support": bundle.support.to_dict(),
            "what_confirms": "Trend persistence confirms the read.",
            "what_breaks": "Trend reversal breaks the read.",
            "threshold_summary": "Watch the 105 threshold.",
            "degraded_state": None,
        },
        "scenario_blocks": [
            {
                "type": "base",
                "label": "Base case",
                "trigger": "Trend persists",
                "expected_return": "Mid-single digits",
                "portfolio_effect": "Supports current posture.",
                "short_term": "Stable",
                "long_term": "Constructive",
                "forecast_support": bundle.support.to_dict(),
                "what_confirms": "Trend persistence confirms the read.",
                "what_breaks": "Trend reversal breaks the read.",
                "degraded_state": None,
            }
        ],
        "decision_thresholds": [
            {
                "label": "Upgrade if",
                "value": "Relative strength improves",
                "forecast_support": bundle.support.to_dict(),
                "trigger_type": "upgrade",
                "threshold_state": "watch",
            }
        ],
        "flip_risk_note": "Flip risk remains bounded.",
    }
    return bundle, payload


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
        },
        evidence=[
            EvidencePack(
                evidence_id=f"evidence_{symbol.lower()}",
                thesis=f"{symbol} remains a valid implementation candidate.",
                summary=f"{symbol} remains a valid implementation candidate.",
                freshness="fresh_full_rebuild",
                citations=[EvidenceCitation(source_id="issuer_factsheet", label="Issuer factsheet")],
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


def _portfolio_truth_with_holdings() -> object:
    from app.v2.core.domain_objects import PortfolioTruth

    return PortfolioTruth(
        portfolio_id="portfolio_default",
        name="Default Portfolio",
        base_currency="USD",
        holdings=[
            {
                "symbol": "SPY",
                "name": "SPY ETF",
                "sleeve": "global_equity",
                "weight": 0.4,
            },
            {
                "symbol": "AGG",
                "name": "AGG ETF",
                "sleeve": "ig_bond",
                "weight": 0.2,
            },
        ],
        exposures={"global_equity": 0.4, "ig_bond": 0.2},
    )


def test_daily_brief_no_holdings_mode_remains_market_first(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "wave4b_brief.sqlite3"))
    import app.v2.surfaces.daily_brief.contract_builder as brief_builder

    bundle, payload = _forecast_bundle_dict()
    monkeypatch.setattr(brief_builder, "_load_macro_truths", lambda: ([_sample_macro_truth()], "macro_test"))
    monkeypatch.setattr(
        brief_builder,
        "_load_market_truths",
        lambda: [_sample_market_series(label="ACWI", series_id="series_acwi")],
    )
    monkeypatch.setattr(brief_builder, "_load_news_truths", lambda: [])
    monkeypatch.setattr(brief_builder, "get_portfolio_truth", lambda account_id: _empty_portfolio_truth())
    monkeypatch.setattr(brief_builder, "record_change", lambda **_: None)
    monkeypatch.setattr(
        brief_builder,
        "build_signal_support_bundle",
        lambda *args, **kwargs: payload,
    )
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

    assert contract["surface_state"]["state"] in {"ready", "degraded"}
    assert contract["what_changed"], "Daily Brief must still emit signals without holdings."
    assert contract["why_it_matters_economically"]
    assert contract["what_confirms_or_breaks"]
    assert contract["scenario_blocks"], "Daily Brief must still emit scenarios or explicit degraded scenario content."
    assert contract["portfolio_overlay_context"]["state"] == "overlay_absent"
    assert contract["holdings_overlay_present"] is False
    assert any(signal["affected_sleeves"] for signal in contract["what_changed"])
    assert all(not signal["affected_holdings"] for signal in contract["what_changed"])


def test_daily_brief_holdings_overlay_enriches_without_replacing_primary_logic(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "wave4b_brief_overlay.sqlite3"))
    import app.v2.surfaces.daily_brief.contract_builder as brief_builder

    _, payload = _forecast_bundle_dict()
    monkeypatch.setattr(brief_builder, "_load_macro_truths", lambda: ([_sample_macro_truth()], "macro_test"))
    monkeypatch.setattr(
        brief_builder,
        "_load_market_truths",
        lambda: [_sample_market_series(label="ACWI", series_id="series_acwi")],
    )
    monkeypatch.setattr(brief_builder, "_load_news_truths", lambda: [])
    monkeypatch.setattr(brief_builder, "get_portfolio_truth", lambda account_id: _portfolio_truth_with_holdings())
    monkeypatch.setattr(brief_builder, "record_change", lambda **_: None)
    monkeypatch.setattr(
        brief_builder,
        "build_signal_support_bundle",
        lambda *args, **kwargs: payload,
    )
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

    assert contract["what_changed"], "Primary Daily Brief signals must still exist with holdings."
    assert contract["holdings_overlay_present"] is True
    assert contract["portfolio_overlay_context"]["state"] == "ready"
    assert any(signal["affected_holdings"] for signal in contract["what_changed"]), "Holdings overlay should enrich affected holdings."


def test_blueprint_and_report_no_holdings_mode_keep_core_reasoning(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "wave4b_blueprint.sqlite3"))
    import app.v2.surfaces.blueprint.explorer_contract_builder as explorer_builder
    import app.v2.surfaces.blueprint.report_contract_builder as report_builder

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

    monkeypatch.setattr(report_builder, "export_live_candidate_registry", lambda conn: candidate_rows)
    monkeypatch.setattr(report_builder, "seed_default_candidate_registry", lambda conn: None)
    monkeypatch.setattr(report_builder, "get_portfolio_truth", lambda account_id: _empty_portfolio_truth())
    monkeypatch.setattr(report_builder, "_market_truth", lambda symbol, endpoint_family="ohlcv_history": _sample_market_series(label=symbol, series_id=f"series_{symbol.lower()}"))
    monkeypatch.setattr(report_builder, "get_instrument_truth", lambda candidate_id: _sample_instrument("VWRA"))
    monkeypatch.setattr(
        report_builder,
        "get_benchmark_truth",
        lambda benchmark_id: SimpleNamespace(name="ACWI benchmark", benchmark_authority_level="direct"),
    )
    monkeypatch.setattr(report_builder, "get_freshness", lambda source_id: SimpleNamespace(freshness_class=SimpleNamespace(value="fresh_full_rebuild")))

    explorer_contract = explorer_builder.build()
    report_contract = report_builder.build("VWRA")

    assert explorer_contract["sleeves"], "Blueprint must still emit sleeve priorities without holdings."
    sleeve = explorer_contract["sleeves"][0]
    assert sleeve["candidates"], "Blueprint must still emit candidate competition without holdings."
    assert sleeve["funding_path"]["degraded_state"] == "overlay_absent"
    assert "portfolio overlay" in sleeve["funding_path"]["summary"].lower()
    assert report_contract["report_tabs"], "Candidate Report tabs must remain available without holdings."
    assert report_contract["baseline_comparisons"][1]["verdict"] == "overlay_absent"
    assert report_contract["overlay_context"]["state"] == "overlay_absent"
    assert report_contract["holdings_overlay_present"] is False


def test_blueprint_holdings_overlay_enriches_incumbent_without_replacing_core_logic(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "wave4b_blueprint_overlay.sqlite3"))
    import app.v2.surfaces.blueprint.explorer_contract_builder as explorer_builder

    _, payload = _forecast_bundle_dict()
    candidate_rows = [
        {"symbol": "VWRA", "sleeve_key": "global_equity_core", "aum": 5_000_000_000, "aum_usd": 5_000_000_000, "liquidity_score": 0.9, "expense_ratio": 0.002},
        {"symbol": "CSPX", "sleeve_key": "global_equity_core", "aum": 3_000_000_000, "aum_usd": 3_000_000_000, "liquidity_score": 0.8, "expense_ratio": 0.0025},
    ]
    monkeypatch.setattr(explorer_builder, "export_live_candidate_registry", lambda conn: candidate_rows)
    monkeypatch.setattr(explorer_builder, "seed_default_candidate_registry", lambda conn: None)
    monkeypatch.setattr(explorer_builder, "record_change", lambda **_: None)
    monkeypatch.setattr(explorer_builder, "get_portfolio_truth", lambda account_id: _portfolio_truth_with_holdings())
    monkeypatch.setattr(explorer_builder, "_market_truth", lambda symbol, endpoint_family="ohlcv_history": _sample_market_series(label=symbol, series_id=f"series_{symbol.lower()}"))
    monkeypatch.setattr(explorer_builder, "get_instrument_truth", lambda symbol: _sample_instrument(symbol))
    monkeypatch.setattr(explorer_builder, "get_freshness", lambda source_id: SimpleNamespace(freshness_class=SimpleNamespace(value="fresh_full_rebuild")))

    contract = explorer_builder.build()
    sleeve = contract["sleeves"][0]

    assert sleeve["candidates"], "Blueprint candidate roster must remain intact with holdings."
    assert sleeve["funding_path"]["degraded_state"] is None
    assert sleeve["funding_path"]["incumbent_label"] == "SPY"
    assert "SPY" in sleeve["funding_path"]["funding_source"]
    assert contract["holdings_overlay_present"] is True
