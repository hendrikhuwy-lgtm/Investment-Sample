from __future__ import annotations

from datetime import UTC, date, datetime

from app.v2.core.domain_objects import EvidencePack, MarketDataPoint, MarketSeriesTruth


def test_vix_authority_probe_cross_checked(monkeypatch) -> None:
    from app.v2.core.market_authority_probes import evaluate_market_authority_probe

    class _Response:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:
            return None

    csv_text = "\n".join(
        [
            "observation_date,VIXCLS",
            "2026-04-06,24.01",
            "2026-04-07,24.17",
        ]
    )

    monkeypatch.setattr(
        "app.v2.core.market_authority_probes.requests.get",
        lambda _url, *, params=None, headers=None, timeout=6: _Response(csv_text),
    )

    probe = evaluate_market_authority_probe(
        "^VIX",
        primary_provider="yahoo_finance",
        primary_value=24.17,
        as_of=date(2026, 4, 7),
    )

    assert probe.primary_provider == "yahoo_finance"
    assert probe.cross_check_provider == "fred_public"
    assert probe.cross_check_status == "cross_checked"
    assert probe.authority_gap_reason is None
    assert probe.cross_check_as_of == "2026-04-07"


def test_bitcoin_authority_probe_detects_mismatch(monkeypatch) -> None:
    from app.v2.core.market_authority_probes import evaluate_market_authority_probe

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[list[float]]:
            return [
                [1775520000, 68000.0, 71000.0, 69000.0, 70500.0, 1000.0],
                [1775433600, 67000.0, 69000.0, 68000.0, 68860.0, 900.0],
            ]

    monkeypatch.setattr(
        "app.v2.core.market_authority_probes.requests.get",
        lambda _url, *, params=None, headers=None, timeout=6: _Response(),
    )

    probe = evaluate_market_authority_probe(
        "BTC-USD",
        primary_provider="yahoo_finance",
        primary_value=68860.0,
        as_of=date(2026, 4, 7),
    )

    assert probe.cross_check_provider == "coinbase_exchange"
    assert probe.cross_check_status == "authority_mismatch"
    assert probe.authority_gap_reason == "cross_check_value_mismatch"
    assert probe.cross_check_as_of == "2026-04-07"


def test_crude_authority_probe_surfaces_gap() -> None:
    from app.v2.core.market_authority_probes import evaluate_market_authority_probe

    probe = evaluate_market_authority_probe(
        "CL=F",
        primary_provider="yahoo_finance",
        primary_value=112.41,
        as_of=date(2026, 4, 7),
    )

    assert probe.cross_check_status == "validated_by_primary_only"
    assert probe.cross_check_provider is None
    assert probe.authority_gap_reason == "licensed_settlement_cross_check_unavailable"


def test_market_state_card_exposes_authority_probe_metadata(monkeypatch) -> None:
    from app.v2.core.market_authority_probes import MarketAuthorityProbeResult
    from app.v2.core.market_strip_registry import market_strip_spec
    from app.v2.surfaces.daily_brief.contract_builder import _annotate_market_truth, _market_state_card_from_truth

    truth = MarketSeriesTruth(
        series_id="market:^vix",
        label="^VIX",
        frequency="daily",
        units="index",
        points=[
            MarketDataPoint(at="2026-04-06T00:00:00+00:00", value=22.25),
            MarketDataPoint(at="2026-04-07T00:00:00+00:00", value=24.17),
        ],
        evidence=[
            EvidencePack(
                evidence_id="evidence_market_^vix",
                thesis="VIX close",
                summary="Close",
                facts={
                    "source_family": "market_close",
                    "source_provider": "yahoo_finance",
                    "movement_state": "known",
                    "provider_execution": {
                        "provider_name": "yahoo_finance",
                        "source_family": "market_close",
                        "authority_level": "direct",
                        "semantic_grade": "movement_capable",
                        "usable_truth": True,
                        "observed_at": "2026-04-07T00:00:00+00:00",
                        "fetched_at": "2026-04-08T00:05:00+00:00",
                        "provenance_strength": "public_verified_close",
                    },
                    "truth_envelope": {
                        "as_of_utc": "2026-04-07",
                        "observation_date": "2026-04-07",
                        "reference_period": "2026-04-07",
                        "source_authority": "yahoo_finance",
                        "acquisition_mode": "live",
                        "retrieved_at_utc": "2026-04-08T00:05:00+00:00",
                    },
                },
            )
        ],
    )

    monkeypatch.setattr(
        "app.v2.surfaces.daily_brief.contract_builder.evaluate_market_authority_probe",
        lambda symbol, *, primary_provider, primary_value, as_of: MarketAuthorityProbeResult(
            primary_provider=primary_provider,
            cross_check_provider="fred_public",
            cross_check_status="cross_checked",
            authority_gap_reason=None,
            cross_check_value=24.17,
            cross_check_as_of="2026-04-07",
            cross_check_authority_tier="public_benchmark_mirror",
        ),
    )
    monkeypatch.setattr(
        "app.v2.surfaces.daily_brief.contract_builder.summarize_market_authority_probe",
        lambda probe: "Cross-check fred public matched (24.17) on 2026-04-07.",
    )
    monkeypatch.setattr(
        "app.v2.surfaces.daily_brief.contract_builder._expected_market_close_date",
        lambda: date(2026, 4, 7),
    )
    monkeypatch.setattr(
        "app.v2.surfaces.daily_brief.contract_builder._current_surface_slot_boundary",
        lambda now=None: datetime(2026, 4, 8, 0, 0, tzinfo=UTC),
    )
    monkeypatch.setattr(
        "app.v2.surfaces.daily_brief.contract_builder._previous_surface_slot_boundary",
        lambda now=None: datetime(2026, 4, 7, 12, 0, tzinfo=UTC),
    )

    card = _market_state_card_from_truth(_annotate_market_truth(truth, market_strip_spec("^VIX")))

    assert card["primary_provider"] == "yahoo_finance"
    assert card["cross_check_provider"] == "fred_public"
    assert card["cross_check_status"] == "cross_checked"
    assert card["authority_gap_reason"] is None
    assert "Cross-check fred public matched" in card["note"]
