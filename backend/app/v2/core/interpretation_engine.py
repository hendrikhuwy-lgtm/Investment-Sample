from __future__ import annotations

from app.v2.core.domain_objects import (
    BenchmarkTruth,
    CandidateAssessment,
    CompareAssessment,
    InstrumentTruth,
    InterpretationCard,
    MacroTruth,
    MarketSeriesTruth,
    PortfolioPressure,
    PortfolioTruth,
    SignalPacket,
    SleeveAssessment,
)
from app.v2.core.holdings_overlay import apply_holdings_overlay
from app.v2.core.mandate_rubric import summarize_constraints
from app.v2.doctrine.doctrine_evaluator import evaluate_doctrine


def _coerce_float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _market_change_pct_1d(market: MarketSeriesTruth) -> float | None:
    if len(market.points) >= 2:
        previous = _coerce_float(market.points[-2].value)
        latest = _coerce_float(market.points[-1].value)
        if previous is not None and latest is not None and previous != 0:
            return ((latest - previous) / abs(previous)) * 100.0

    for pack in market.evidence:
        for key in ("change_pct_1d", "pct_change_1d", "daily_change_pct"):
            value = _coerce_float(pack.facts.get(key))
            if value is not None:
                return value

    return None


def _signal_direction(change_pct_1d: float | None) -> str:
    if change_pct_1d is None:
        return "unknown"
    if change_pct_1d == 0:
        return "neutral"
    return "up" if change_pct_1d > 0 else "down"


# Per-symbol magnitude thresholds: (significant_pct, moderate_pct)
# Calibrated to each instrument's typical daily volatility so that
# "significant" means a genuine tail event, not a routine session.
_MAGNITUDE_THRESHOLDS: dict[str, tuple[float, float]] = {
    "^GSPC":            (1.5,  0.75),   # S&P 500 — ~1% typical daily
    "^DJI":             (1.5,  0.75),   # Dow
    "^IXIC":            (2.0,  1.0),    # Nasdaq — higher beta
    "^RUT":             (2.0,  1.0),    # Russell 2K — higher beta
    "^SPXEW":           (1.5,  0.75),   # S&P Equal Weight
    "^990100-USD-STRD": (1.5,  0.75),   # MSCI World
    "^VIX":             (10.0, 4.0),    # VIX moves 5-15% routinely
    "BAMLH0A0HYM2":     (4.0,  1.5),    # HY spreads — % change in spread level
    "DXY":              (0.8,  0.35),   # USD Index — low vol; 1% is a macro event
    "^KRX":             (2.0,  1.0),    # KBW Regional Banks
    "FEDFUNDS":         (5.0,  1.0),    # Policy rate: 25bp hike ≈ 6.9% of level
    "SOFR":             (5.0,  1.0),    # Overnight rate: 1bp ≈ 0.27%, 25bp ≈ 6.9%
    "DGS2":             (2.0,  0.75),   # UST 2Y yield % change
    "^TNX":             (2.5,  1.0),    # 10Y yield — 10bp ≈ 2.3% of level
    "DFII10":           (3.0,  1.0),    # Real yield 10Y — wider range
    "^TYX":             (2.5,  1.0),    # 30Y yield
    "MORTGAGE30US":     (0.5,  0.2),    # Weekly release, very slow-moving
    "NASDAQNCPAG":      (0.7,  0.3),    # Bond index — low daily vol
    "CPI_YOY":          (3.0,  1.0),    # Monthly YoY — large Δ = regime shift
    "GC=F":             (2.0,  1.0),    # Gold — moderate vol
    "BZ=F":             (4.0,  1.5),    # Brent crude — high daily vol
    "CL=F":             (4.0,  1.5),    # WTI crude — high daily vol
    "BTC-USD":          (5.0,  2.0),    # Bitcoin — very high vol
}
_MAGNITUDE_FALLBACK: tuple[float, float] = (2.0, 0.5)


def _signal_magnitude(change_pct_1d: float | None, symbol: str | None = None) -> str:
    if change_pct_1d is None:
        return "unknown"
    sig_thresh, mod_thresh = _MAGNITUDE_THRESHOLDS.get(
        str(symbol or "").upper(), _MAGNITUDE_FALLBACK
    )
    absolute_change = abs(change_pct_1d)
    if absolute_change > sig_thresh:
        return "significant"
    if absolute_change > mod_thresh:
        return "moderate"
    return "minor"


def _implication_horizon(magnitude: str) -> str:
    return {
        "significant": "immediate",
        "moderate": "near_term",
        "minor": "long_term",
        "unknown": "near_term",
    }.get(magnitude, "long_term")


def _economic_template(asset_class: str | None) -> str:
    asset_key = str(asset_class or "").strip().lower()
    if asset_key == "equity":
        return "Equity moves matter because they can change expected return capture and the path of portfolio volatility."
    if asset_key in {"fixed_income", "bond", "bonds"}:
        return "Bond moves matter because they can change income, duration sensitivity, and diversification support."
    if asset_key in {"cash", "cash_equivalent"}:
        return "Cash-like moves matter because they can change liquidity value, reinvestment optionality, and downside ballast."
    if asset_key in {"commodity", "commodities", "real_assets"}:
        return "Real asset moves matter because they can change inflation sensitivity and diversification behavior."
    return "This move matters economically because it can change expected portfolio role, risk transfer, and implementation timing."


def _resolve_sleeve_affiliation(truth: InstrumentTruth) -> str:
    for key in ("sleeve_affiliation", "sleeve_name", "sleeve_id", "sleeve"):
        value = truth.metrics.get(key)
        if value:
            return str(value)
    return "current sleeve context"


def _sleeve_template(sleeve_affiliation: str) -> str:
    label = sleeve_affiliation.replace("_", " ").strip() or "current sleeve context"
    return (
        f"For the {label} sleeve, this changes which instrument best does the job now, "
        "even before any holdings overlay is attached."
    )


def _empty_market_context() -> MarketSeriesTruth:
    return MarketSeriesTruth(
        series_id="series_unavailable",
        label="Unavailable market context",
        frequency="daily",
        units="percent",
        points=[],
    )


def interpret(
    truth: InstrumentTruth,
    market: MarketSeriesTruth,
) -> tuple[SignalPacket, InterpretationCard]:
    """
    Produces a SignalPacket and InterpretationCard from truth + market data.
    Must NOT import from blueprint_payload_assembler or cortex_blueprint_presentation.
    """
    change_pct_1d = _market_change_pct_1d(market)
    direction = _signal_direction(change_pct_1d)
    magnitude = _signal_magnitude(change_pct_1d, symbol=market.label or market.series_id)
    implication_horizon = _implication_horizon(magnitude)
    asset_class = truth.asset_class or "asset"
    sleeve_affiliation = _resolve_sleeve_affiliation(truth)
    if change_pct_1d is None:
        move_text = "the latest move is not established because the current input is insufficient"
    elif change_pct_1d > 0:
        move_text = f"the latest session is firmer by {change_pct_1d:.2f}%"
    elif change_pct_1d < 0:
        move_text = f"the latest session is softer by {abs(change_pct_1d):.2f}%"
    else:
        move_text = "the latest session is broadly unchanged"
    evidence_ids = [pack.evidence_id for pack in truth.evidence]
    signal = SignalPacket(
        signal_id=f"signal_{truth.instrument_id}_{market.series_id}",
        source_truth_id=truth.instrument_id,
        signal_kind="market_context",
        direction=direction,
        magnitude=magnitude,
        strength=abs(change_pct_1d or 0.0),
        horizon=implication_horizon,
        summary=(
            f"{truth.symbol} does not yet have an established directional move in the current read; {move_text}."
            if direction == "unknown"
            else f"{truth.symbol} is {direction} in the current read; {move_text}."
        ),
        evidence_ids=evidence_ids,
        metadata={
            "change_pct_1d": change_pct_1d,
            "movement_state": "known" if change_pct_1d is not None else "input_constrained",
        },
    )
    card = InterpretationCard(
        card_id=f"card_{truth.instrument_id}",
        entity_id=truth.instrument_id,
        title=f"{truth.symbol} interpretation",
        thesis=f"{truth.name} is currently being read as a {asset_class} implementation candidate.",
        confidence="medium",
        conviction="medium",
        implication_horizon=implication_horizon,
        why_it_matters_economically=_economic_template(asset_class),
        why_it_matters_here=_sleeve_template(sleeve_affiliation),
        signals=[signal],
        evidence_ids=evidence_ids,
        notes=["Interpretation remains generic until doctrine and policy layers add sharper constraints."],
    )
    return signal, card


def build_signal_packet(
    *,
    signal_id: str,
    source_truth_id: str,
    signal_kind: str,
    direction: str,
    strength: float,
    summary: str,
    evidence_ids: list[str] | None = None,
    horizon: str = "strategic",
) -> SignalPacket:
    return SignalPacket(
        signal_id=signal_id,
        source_truth_id=source_truth_id,
        signal_kind=signal_kind,
        direction=direction,
        magnitude=_signal_magnitude(strength),
        strength=strength,
        summary=summary,
        evidence_ids=list(evidence_ids or []),
        horizon=horizon,
    )


def interpret_instrument_truth(
    instrument: InstrumentTruth,
    *,
    benchmark: BenchmarkTruth | None = None,
    market_series: MarketSeriesTruth | None = None,
    macro: MacroTruth | None = None,
) -> InterpretationCard:
    market_context = market_series or _empty_market_context()
    primary_signal, card = interpret(instrument, market_context)
    signals = [primary_signal]
    notes = list(card.notes)

    if benchmark is not None:
        authority = str(benchmark.benchmark_authority_level or "bounded")
        signals.append(
            build_signal_packet(
                signal_id=f"signal_benchmark_{instrument.instrument_id}",
                source_truth_id=benchmark.benchmark_id,
                signal_kind="benchmark",
                direction="positive" if authority in {"direct", "strong"} else "mixed",
                strength=0.7 if authority in {"direct", "strong"} else 0.35,
                summary=f"Benchmark authority is {authority}, which {'supports' if authority in {'direct', 'strong'} else 'limits'} comparison language.",
                evidence_ids=[pack.evidence_id for pack in benchmark.evidence],
            )
        )

    if macro is not None:
        signals.append(
            build_signal_packet(
                signal_id=f"signal_macro_{instrument.instrument_id}",
                source_truth_id=macro.macro_id,
                signal_kind="macro",
                direction="mixed",
                strength=0.3,
                summary=macro.summary,
                evidence_ids=[pack.evidence_id for pack in macro.evidence],
            )
        )
        notes.append("Macro context is included as framing, not as a stand-alone trading trigger.")

    evidence_ids = list(dict.fromkeys([pack.evidence_id for pack in instrument.evidence]))
    return card.model_copy(
        update={
            "signals": signals,
            "evidence_ids": evidence_ids,
            "notes": notes,
        }
    )


def assess_candidate(
    instrument: InstrumentTruth,
    *,
    sleeve_id: str,
    sleeve_purpose: str,
    portfolio: PortfolioTruth | None = None,
    benchmark: BenchmarkTruth | None = None,
    market_series: MarketSeriesTruth | None = None,
    macro: MacroTruth | None = None,
) -> tuple[CandidateAssessment, object]:
    interpretation = interpret_instrument_truth(
        instrument,
        benchmark=benchmark,
        market_series=market_series,
        macro=macro,
    )
    raw_candidate = CandidateAssessment(
        candidate_id=f"candidate_{instrument.instrument_id}",
        sleeve_id=sleeve_id,
        instrument=instrument,
        interpretation=interpretation,
        mandate_fit="aligned" if sleeve_purpose.lower() in interpretation.why_it_matters_here.lower() else "watch",
        conviction=0.55 if interpretation.implication_horizon != "long_term" else 0.42,
        score_breakdown={
            "evidence": 0.5,
            "implementation": 0.6 if instrument.metrics.get("expense_ratio", 1.0) <= 0.25 else 0.4,
        },
        key_supports=[signal.summary for signal in interpretation.signals if signal.direction in {"up", "positive"}][:3],
        key_risks=[signal.summary for signal in interpretation.signals if signal.direction in {"down", "negative", "mixed"}][:3],
    )
    candidate = apply_holdings_overlay(raw_candidate, portfolio) if portfolio is not None else raw_candidate
    restraints = evaluate_doctrine(candidate)
    constraint_summary = summarize_constraints(
        candidate=candidate,
        sleeve_purpose=sleeve_purpose,
        restraints=restraints,
        portfolio=portfolio,
    )
    return candidate, constraint_summary


def build_sleeve_assessment(
    *,
    sleeve_id: str,
    label: str,
    purpose: str,
    candidates: list[CandidateAssessment],
) -> SleeveAssessment:
    preferred = max(candidates, key=lambda candidate: candidate.conviction) if candidates else None
    pressure_level = "high" if any(candidate.mandate_fit == "outside" for candidate in candidates) else "medium" if len(candidates) > 1 else "low"
    summary = (
        f"{preferred.instrument.symbol} currently leads because its interpretation is cleaner."
        if preferred is not None
        else "No candidate assessments are available."
    )
    return SleeveAssessment(
        sleeve_id=sleeve_id,
        label=label,
        purpose=purpose,
        candidates=candidates,
        preferred_candidate_id=preferred.candidate_id if preferred is not None else None,
        pressure_level=pressure_level,
        summary=summary,
    )


def build_portfolio_pressure(portfolio: PortfolioTruth, sleeves: list[SleeveAssessment]) -> PortfolioPressure:
    high_pressure_sleeves = [sleeve.sleeve_id for sleeve in sleeves if sleeve.pressure_level == "high"]
    medium_pressure_sleeves = [sleeve.sleeve_id for sleeve in sleeves if sleeve.pressure_level == "medium"]
    level = "acute" if high_pressure_sleeves else "elevated" if medium_pressure_sleeves else "calm"
    drivers = [
        "Multiple sleeves need comparison before a clean recommendation can be shown."
        if high_pressure_sleeves or medium_pressure_sleeves
        else "No immediate portfolio pressure is visible in the current V2 scaffold."
    ]
    return PortfolioPressure(
        portfolio_id=portfolio.portfolio_id,
        pressure_id=f"pressure_{portfolio.portfolio_id}",
        level=level,
        drivers=drivers,
        affected_sleeves=high_pressure_sleeves or medium_pressure_sleeves,
        summary=drivers[0],
    )


def build_compare_assessment(left: CandidateAssessment, right: CandidateAssessment) -> CompareAssessment:
    winner = left if left.conviction >= right.conviction else right
    loser = right if winner is left else left
    return CompareAssessment(
        compare_id=f"compare_{left.candidate_id}_{right.candidate_id}",
        left_candidate_id=left.candidate_id,
        right_candidate_id=right.candidate_id,
        winner_candidate_id=winner.candidate_id,
        confidence="medium" if abs(left.conviction - right.conviction) < 0.15 else "high",
        rationale=[
            f"{winner.instrument.symbol} leads on current conviction.",
            f"{loser.instrument.symbol} still needs a cleaner edge to justify displacement.",
        ],
        key_deltas={
            "conviction_gap": round(winner.conviction - loser.conviction, 3),
            "support_count_gap": len(winner.key_supports) - len(loser.key_supports),
        },
        decision_state_hint="compare",
    )
