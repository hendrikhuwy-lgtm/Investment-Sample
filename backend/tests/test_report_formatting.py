from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

from app.models.types import Citation, SourceRecord
from app.services import reporting
from app.services.reporting import build_narrated_email_brief


def _citation(url: str, source_id: str, importance: str) -> Citation:
    return Citation(
        url=url,
        source_id=source_id,
        retrieved_at=datetime.now(UTC),
        importance=importance,
    )


def test_executive_snapshot_has_no_raw_urls_and_uses_markers() -> None:
    c1 = _citation("https://fred.stlouisfed.org/series/DGS10", "fred_dgs10", "primary official rates source")
    c2 = _citation("https://fred.stlouisfed.org/series/SP500", "fred_sp500", "primary official equity source")

    rendered = build_narrated_email_brief(
        subject="SG Macro and Markets Brief, 2026-02-17 12:00 SGT, Signals Watch",
        generated_at_sgt="2026-02-17T12:00:00+08:00",
        executive_snapshot=[
            {
                "text": "Rates are 4.09 and equity index is 6836.17 in this run.",
                "citations": [c1, c2],
            }
        ],
        graph_rows=[
            {
                "metric": "US 10Y Treasury Yield (2026-02-12)",
                "latest": "4.09",
                "delta_5": "-0.12",
                "trajectory": "downward",
                "pattern": "stable variability",
                "sparkline": "▁▂▃▄",
                "range_bar": "████░░░░",
                "citation": c1,
            }
        ],
        graph_quality_audit={
            "up_to_date": True,
            "retrieval_fresh": True,
            "data_fresh": True,
            "cited": True,
            "easy_to_comprehend": True,
            "note": "Audit ok",
            "citations": [c1],
        },
        lenses=[
            {
                "title": "Market implied regime lens",
                "body": "Signals indicate a watch regime.",
                "citations": [c1],
            }
        ],
        big_players=[
            {
                "text": "SEC 13F proxy: recent filing cadence remains active.",
                "citations": [c2],
            }
        ],
        portfolio_mapping=[
            {
                "text": "Target return context remains 6%-10% across a multi-year horizon.",
                "citations": [c1],
            }
        ],
        convex_report={
            "total_weight": 0.03,
            "valid": True,
            "errors": [],
            "target_breakdown": [
                {"component": "Managed Futures (2.0%)", "target": 0.02, "actual": 0.02, "within_target": True},
                {"component": "Tail Hedge (0.7%)", "target": 0.007, "actual": 0.007, "within_target": True},
                {"component": "Long Puts (0.3%)", "target": 0.003, "actual": 0.003, "within_target": True},
            ],
            "margin_required_any": False,
            "max_loss_known_all": True,
        },
        source_appendix=[
            SourceRecord(
                source_id="fred_dgs10",
                url="https://fred.stlouisfed.org/series/DGS10",
                publisher="FRED",
                retrieved_at=datetime.now(UTC),
                topic="rates",
                credibility_tier="primary",
                raw_hash="abc",
                source_type="web",
            ),
            SourceRecord(
                source_id="fred_sp500",
                url="https://fred.stlouisfed.org/series/SP500",
                publisher="FRED",
                retrieved_at=datetime.now(UTC),
                topic="equities",
                credibility_tier="primary",
                raw_hash="def",
                source_type="web",
            ),
        ],
        allocation={"global_equities": 0.35, "ig_bonds": 0.20, "convex": 0.03},
        policy_pack={
            "expected_returns": {
                "assumption_date": "2026-03-01",
                "version": "2026.03",
                "items": [
                    {
                        "sleeve_key": "global_equity",
                        "sleeve_name": "Global Equity",
                        "expected_return_min": 0.06,
                        "expected_return_max": 0.09,
                        "confidence_label": "medium",
                        "scenario_notes": "Policy-level range.",
                    }
                ],
                "caveat": "Policy assumptions only.",
            },
            "benchmark": {
                "benchmark_definition_id": "bench_1",
                "benchmark_name": "Institutional Policy Composite Benchmark",
                "version": "2026.03",
                "assumption_date": "2026-03-01",
                "components": [
                    {
                        "component_key": "global_equity",
                        "component_name": "Global Equity",
                        "weight": 0.5,
                        "rationale": "Growth anchor.",
                    }
                ],
                "expected_return_min": 0.05,
                "expected_return_max": 0.08,
            },
            "aggregate_drawdown": {
                "expected_worst_year_loss_min": -0.30,
                "expected_worst_year_loss_max": -0.10,
                "historical_analogs": [
                    {"label": "2008-style analog", "estimated_impact_pct": -18.0, "confidence_rating": "medium"}
                ],
                "caveat": "Aggregate analog framing.",
            },
            "rebalancing_policy": {
                "calendar_review_cadence": "monthly",
                "small_trade_threshold": "Net trades where possible.",
                "tax_aware_note": "Prefer lower drag when optional.",
                "rules": [
                    {
                        "sleeve": "global_equity",
                        "target_weight": 0.50,
                        "min_band": 0.45,
                        "max_band": 0.55,
                        "calendar_rebalance_frequency": "monthly",
                        "rebalance_priority": "core",
                    }
                ],
            },
            "ips_snapshot": {
                "objectives": "6%-10% policy range.",
                "risk_tolerance": "moderate growth",
                "time_horizon_years": 10,
                "liquidity_needs": "Keep reserve sleeve.",
                "tax_context": "SG policy lens.",
                "benchmark": {"benchmark_name": "Institutional Policy Composite Benchmark"},
                "caveat": "Policy summary only.",
            },
            "core_satellite_summary": [
                {
                    "sleeve_key": "global_equity_core",
                    "sleeve_name": "Global Equity Core",
                    "classification": "core",
                    "min_weight": 0.40,
                    "target_weight": 0.45,
                    "max_weight": 0.55,
                }
            ],
            "review_queue": [{"item": "Review drift and benchmark context.", "action_tag": "review"}],
            "dca_guidance": {
                "cadence": "monthly",
                "routing_mode": "drift_correcting",
                "neutral_conditions": [{"sleeve_key": "global_equity", "weight": 0.45}],
                "drift_conditions": [{"sleeve_key": "underweight_sleeves", "weight": 0.70}],
                "stress_conditions": [{"sleeve_key": "cash", "weight": 0.35}],
                "distribution_logic": "Accumulation default.",
            },
            "fund_selection": [
                {
                    "ticker": "VWRA",
                    "sleeve_name": "Global Equity Core",
                    "ter": 0.0022,
                    "aum": "not available",
                    "liquidity_proxy": "LSE",
                    "singapore_tax_efficiency_score": 87.0,
                    "rationale": "Global one-fund core.",
                }
            ],
            "sub_sleeve_breakdown": [
                {
                    "sleeve_key": "global_equity_core",
                    "sleeve_name": "Global Equity Core",
                    "purpose": "Growth anchor.",
                    "constraints": ["Keep diversified."],
                }
            ],
            "tax_location_guidance": [
                {
                    "sleeve_key": "global_equity_core",
                    "sleeve_name": "Global Equity Core",
                    "preferred_account_type": "taxable_guidance_only",
                    "guidance": "Prefer IE UCITS wrappers.",
                }
            ],
        },
        chart_payloads=[
            {
                "chart_key": "allocation_pie",
                "title": "Allocation Pie Chart",
                "svg": "<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10'></svg>",
                "source_as_of": "2026-03-01",
                "freshness_note": "fresh",
            }
        ],
        approval_record={"approval_status": "generated"},
    )

    markdown = rendered["markdown"]
    html = rendered["html"]

    start = markdown.index("## Executive Snapshot")
    end = markdown.index("## Data Graphs")
    exec_section = markdown[start:end]
    assert "http://" not in exec_section
    assert "https://" not in exec_section
    assert re.search(r"\[\d+\]", exec_section)

    assert "## Sources" in markdown
    assert "## Policy Layer" in markdown
    assert "### Expected Return Range" in markdown
    assert "## Execution Layer" in markdown
    assert "### Fund Selection Table" in markdown
    assert re.search(r"\n1\. .*Retrieved .*Type:", markdown)

    assert "<sup class='cite'><a href='#cite-" in html
    assert "id='sources'" in html
    assert "id='policy-layer'" in html
    assert "id='execution-layer'" in html

    graph_line = next(line for line in markdown.splitlines() if "US 10Y Treasury Yield" in line)
    assert re.search(r"\[\d+\]", graph_line)


def test_write_narrated_email_files_outputs_html_and_pdf_with_charts(monkeypatch, tmp_path: Path) -> None:
    c1 = _citation("https://fred.stlouisfed.org/series/DGS10", "fred_dgs10", "primary official rates source")
    rendered = build_narrated_email_brief(
        subject="Daily Brief Artifact Test",
        generated_at_sgt="2026-03-07T12:00:00+08:00",
        executive_snapshot=[{"text": "Rates remain in watch regime.", "citations": [c1]}],
        graph_rows=[
            {
                "metric": "US 10Y Treasury Yield (2026-03-07)",
                "latest": "4.10",
                "delta_5": "0.05",
                "trajectory": "stable",
                "pattern": "contained volatility",
                "sparkline": "▁▂▃▄",
                "range_bar": "████░░░░",
                "citation": c1,
            }
        ],
        graph_quality_audit={
            "up_to_date": True,
            "retrieval_fresh": True,
            "data_fresh": True,
            "cited": True,
            "easy_to_comprehend": True,
            "note": "Audit ok",
            "citations": [c1],
        },
        lenses=[{"title": "Macro lens", "body": "Watch state remains in place.", "citations": [c1]}],
        big_players=[{"text": "Large allocators remain cautious.", "citations": [c1]}],
        portfolio_mapping=[{"text": "Policy mapping remains diversified.", "citations": [c1]}],
        convex_report={"total_weight": 0.02, "valid": True, "errors": [], "target_breakdown": [], "margin_required_any": False, "max_loss_known_all": True},
        source_appendix=[
            SourceRecord(
                source_id="fred_dgs10",
                url="https://fred.stlouisfed.org/series/DGS10",
                publisher="FRED",
                retrieved_at=datetime.now(UTC),
                topic="rates",
                credibility_tier="primary",
                raw_hash="abc",
                source_type="web",
            )
        ],
        allocation={"global_equities": 0.45, "ig_bonds": 0.25},
        policy_pack={
            "expected_returns": {
                "assumption_date": "2026-03-01",
                "version": "2026.03",
                "items": [
                    {
                        "sleeve_key": "global_equity",
                        "sleeve_name": "Global Equity",
                        "expected_return_min": 0.06,
                        "expected_return_max": 0.09,
                        "confidence_label": "medium",
                    }
                ],
            }
        },
        chart_payloads=[
            {
                "chart_key": "allocation_pie",
                "title": "Allocation Pie Chart",
                "svg": "<svg xmlns='http://www.w3.org/2000/svg' width='32' height='32'><circle cx='16' cy='16' r='12' fill='#1f5f8b' /></svg>",
                "source_as_of": "2026-03-07",
                "freshness_note": "fresh",
            }
        ],
        approval_record={"approval_status": "approved"},
    )

    class _FakeProc:
        returncode = 1
        stderr = b""

    monkeypatch.setattr(reporting, "OUTBOX_DIR", tmp_path)
    monkeypatch.setattr(reporting.subprocess, "run", lambda *args, **kwargs: _FakeProc())

    files = reporting.write_narrated_email_files("brief_artifact_test", rendered["markdown"], rendered["html"])
    html = Path(files["html_path"]).read_text(encoding="utf-8")
    pdf = Path(files["pdf_path"])

    assert "Allocation Pie Chart" in html
    assert "<svg" in html
    assert pdf.exists()
    assert pdf.stat().st_size > 0
