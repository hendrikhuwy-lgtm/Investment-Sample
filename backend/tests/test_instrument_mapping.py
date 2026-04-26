from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.models.types import Citation, SourceRecord
from app.services.instrument_mapping import build_implementation_mapping
from app.services.normalize import CitationPolicyError
from app.services.reporting import build_narrated_email_brief


def _base_report_payload() -> dict:
    now = datetime.now(UTC)
    c1 = Citation(
        url="https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10",
        source_id="fred_dgs10",
        retrieved_at=now,
        importance="Primary official rates source",
    )
    c2 = Citation(
        url="https://fred.stlouisfed.org/graph/fredgraph.csv?id=SP500",
        source_id="fred_sp500",
        retrieved_at=now,
        importance="Primary official equity source",
    )
    source_appendix = [
        SourceRecord(
            source_id="fred_dgs10",
            url="https://fred.stlouisfed.org/series/DGS10",
            publisher="FRED",
            retrieved_at=now,
            topic="rates",
            credibility_tier="primary",
            raw_hash="hash1",
            source_type="web",
        ),
        SourceRecord(
            source_id="fred_sp500",
            url="https://fred.stlouisfed.org/series/SP500",
            publisher="FRED",
            retrieved_at=now,
            topic="equities",
            credibility_tier="primary",
            raw_hash="hash2",
            source_type="web",
        ),
    ]
    return {
        "subject": "SG Macro and Markets Brief, 2026-02-17 12:00 SGT, Signals {Long: Watch} {Short: Watch}",
        "generated_at_sgt": "2026-02-17T12:00:00+08:00",
        "executive_snapshot": [{"text": "Rates and equities monitored.", "citations": [c1, c2]}],
        "graph_rows": [
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
        "graph_quality_audit": {"up_to_date": True, "retrieval_fresh": True, "data_fresh": True, "cited": True, "easy_to_comprehend": True, "note": "ok", "citations": [c1]},
        "lenses": [{"title": "Market implied regime lens", "body": "Signals indicate watch.", "citations": [c1], "source_title": "FRED", "source_date_available": False}],
        "big_players": [{"text": "Policy proxy observed.", "citations": [c2]}],
        "portfolio_mapping": [{"text": "Policy mapping retained.", "citations": [c1, c2]}],
        "convex_report": {
            "total_weight": 0.03,
            "valid": True,
            "errors": [],
            "target_breakdown": [],
            "margin_required_any": False,
            "max_loss_known_all": True,
        },
        "source_appendix": source_appendix,
        "allocation": {"global_equities": 0.35, "ig_bonds": 0.20, "convex": 0.03},
        "long_state": "Watch",
        "short_state": "Watch",
    }


def test_ie_ucits_ranks_above_us_under_sg_profile_assumptions() -> None:
    mapping = build_implementation_mapping(retrieved_at=datetime.now(UTC))
    candidates = mapping["sleeves"]["global_equity"]["candidates"]

    ie_scores = [float(item.tax_score or 0.0) for item in candidates if item.domicile == "IE"]
    us_scores = [float(item.tax_score or 0.0) for item in candidates if item.domicile == "US"]
    assert ie_scores and us_scores
    assert max(ie_scores) > max(us_scores)
    assert candidates[0].domicile == "IE"


def test_convex_options_have_defined_loss_and_no_margin() -> None:
    mapping = build_implementation_mapping(retrieved_at=datetime.now(UTC))
    convex = mapping["sleeves"]["convex"]["candidates"]
    option_candidates = [item for item in convex if item.option_position == "long_put"]
    assert option_candidates
    for item in option_candidates:
        assert item.margin_required is False
        assert item.max_loss_known is True


def test_no_short_options_allowed_in_convex_candidates() -> None:
    mapping = build_implementation_mapping(retrieved_at=datetime.now(UTC))
    convex = mapping["sleeves"]["convex"]["candidates"]
    for item in convex:
        assert item.option_position in {None, "long_put"}
        assert "short option" not in (item.notes or "").lower()
        assert "naked call" not in (item.notes or "").lower()


def test_implementation_section_includes_citation_markers() -> None:
    payload = _base_report_payload()
    payload["implementation_mapping"] = build_implementation_mapping(retrieved_at=datetime.now(UTC))
    # merge source appendix with mapping sources for citation registry
    payload["source_appendix"] = [*payload["source_appendix"], *payload["implementation_mapping"]["source_records"]]
    rendered = build_narrated_email_brief(**payload)
    markdown = rendered["markdown"]
    assert "## Implementation Layer – Illustrative Products" in markdown
    assert "### Global Equity" in markdown
    # symbol row should include numbered citation marker.
    assert "CSPX [" in markdown or "SPY [" in markdown


def test_report_fails_if_instrument_data_lacks_citation() -> None:
    payload = _base_report_payload()
    mapping = build_implementation_mapping(retrieved_at=datetime.now(UTC))
    mapping["sleeves"]["global_equity"]["candidates"][0] = mapping["sleeves"]["global_equity"]["candidates"][0].model_copy(
        update={"citations": []}
    )
    payload["implementation_mapping"] = mapping
    payload["source_appendix"] = [*payload["source_appendix"], *mapping["source_records"]]
    with pytest.raises(CitationPolicyError):
        build_narrated_email_brief(**payload)
