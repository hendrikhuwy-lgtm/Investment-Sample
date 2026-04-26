from __future__ import annotations

from pathlib import Path

from app.v2.core.domain_objects import (
    CandidateAssessment,
    ConstraintSummary,
    FrameworkRestraint,
    InstrumentTruth,
    InterpretationCard,
    MarketDataPoint,
    MarketSeriesTruth,
    PolicyBoundary,
    SignalPacket,
)
from app.v2.core.holdings_overlay import apply_overlay
from app.v2.core.interpretation_engine import interpret
from app.v2.core.mandate_rubric import apply_rubric
from app.v2.doctrine.doctrine_evaluator import evaluate


def _instrument_truth() -> InstrumentTruth:
    return InstrumentTruth(
        instrument_id="instrument_vwra",
        symbol="VWRA",
        name="Vanguard FTSE All-World UCITS ETF",
        asset_class="equity",
        metrics={"sleeve_affiliation": "global_equity_core"},
    )


def _market_truth() -> MarketSeriesTruth:
    return MarketSeriesTruth(
        series_id="series_vwra",
        label="VWRA daily move",
        frequency="daily",
        units="percent",
        points=[
            MarketDataPoint(at="2026-03-31T00:00:00+00:00", value=100.0),
            MarketDataPoint(at="2026-04-01T00:00:00+00:00", value=101.2),
        ],
    )


def _assessment() -> CandidateAssessment:
    _, card = interpret(_instrument_truth(), _market_truth())
    return CandidateAssessment(
        candidate_id="candidate_instrument_vwra",
        sleeve_id="sleeve_global_equity_core",
        instrument=_instrument_truth(),
        interpretation=card,
    )


def test_interpret_returns_signal_packet_and_interpretation_card() -> None:
    signal, card = interpret(_instrument_truth(), _market_truth())

    assert isinstance(signal, SignalPacket)
    assert isinstance(card, InterpretationCard)
    assert signal.magnitude == "moderate"
    assert card.implication_horizon == "near_term"


def test_apply_overlay_with_none_keeps_base_unmutated() -> None:
    base = {"surface_id": "candidate_report", "candidate_id": "candidate_instrument_vwra"}

    result = apply_overlay(base, None)

    assert base == {"surface_id": "candidate_report", "candidate_id": "candidate_instrument_vwra"}
    assert result is not base
    assert result["holdings_overlay_present"] is False


def test_apply_overlay_with_holdings_returns_new_dict_with_overlay_present() -> None:
    base = {"surface_id": "candidate_report"}

    result = apply_overlay(base, {"symbol": "VWRA", "weight": 0.25})

    assert base == {"surface_id": "candidate_report"}
    assert result is not base
    assert result["holdings_overlay_present"] is True
    assert result["holdings_overlay"] == {"symbol": "VWRA", "weight": 0.25}


def test_doctrine_evaluator_returns_at_least_one_framework_restraint() -> None:
    restraints = evaluate(_assessment())

    assert len(restraints) >= 1
    assert all(isinstance(restraint, FrameworkRestraint) for restraint in restraints)


def test_apply_rubric_returns_constraint_summary() -> None:
    assessment = _assessment()
    boundary = PolicyBoundary(
        boundary_id="boundary_candidate_instrument_vwra",
        code="review_boundary",
        action_boundary="review",
        summary="Policy review remains required.",
    )

    summary = apply_rubric(assessment, boundary, evaluate(assessment))

    assert isinstance(summary, ConstraintSummary)
    assert summary.summary_id == "constraint_candidate_instrument_vwra"


def test_tests_v2_do_not_import_blueprint_payload_assembler() -> None:
    tests_dir = Path(__file__).resolve().parent
    forbidden_module = "blueprint" + "_payload_assembler"

    for path in tests_dir.rglob("*.py"):
        content = path.read_text(encoding="utf-8")
        assert f"import {forbidden_module}" not in content
        assert f"from app.services.{forbidden_module} import" not in content
