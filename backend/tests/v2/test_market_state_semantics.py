from __future__ import annotations

from datetime import UTC, date, datetime

from app.v2.core.domain_objects import EvidencePack, MarketDataPoint, MarketSeriesTruth


def test_market_strip_registry_uses_actual_indices_and_skips_macro_quote_targets() -> None:
    from app.v2.core.market_strip_registry import daily_brief_targets, market_strip_spec

    assert market_strip_spec("^GSPC")["label"] == "S&P 500"
    assert market_strip_spec("^GSPC")["routed_family"] == "market_close"
    assert market_strip_spec("^IXIC")["label"] == "Nasdaq"
    assert market_strip_spec("GC=F")["label"] == "Gold"
    assert market_strip_spec("^990100-USD-STRD")["label"] == "World Equity"
    assert market_strip_spec("CPI_YOY")["data_source"] == "public_fred"
    assert market_strip_spec("SOFR")["series_id"] == "SOFR"
    assert market_strip_spec("^SPXEW")["label"] == "S&P Equal Weight"

    targets = daily_brief_targets()
    assert "^GSPC" in targets["market_close"]
    assert "^IXIC" in targets["market_close"]
    assert "GC=F" in targets["market_close"]
    assert "^GSPC" not in targets["quote_latest"]
    assert "SOFR" not in targets["quote_latest"]
    assert "CPI_YOY" not in targets["quote_latest"]


def test_market_close_prefers_yahoo_for_indices_and_futures() -> None:
    from app.services.provider_registry import routed_provider_candidates

    assert routed_provider_candidates("market_close", identifier="^GSPC")[0] == "yahoo_finance"
    assert routed_provider_candidates("market_close", identifier="GC=F")[0] == "yahoo_finance"


def test_public_fred_market_state_truth_builds_daily_series(monkeypatch) -> None:
    from app.v2.core.market_state_sources import load_public_fred_market_state_truth

    class _Response:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:
            return None

    def _fake_get(_url: str, *, params: dict[str, str], headers: dict[str, str], timeout: int) -> _Response:
        assert headers["User-Agent"].startswith("investment-agent-v2")
        assert timeout == 6
        series_id = params["id"]
        if series_id == "SOFR":
            return _Response("observation_date,SOFR\n2026-04-02,3.66\n2026-04-06,3.65\n")
        raise AssertionError(series_id)

    monkeypatch.setattr("app.v2.core.market_state_sources.requests.get", _fake_get)

    truth = load_public_fred_market_state_truth(
        symbol="SOFR",
        label="SOFR",
        series_id="SOFR",
        transform="identity",
        units="percent",
        cadence="daily",
    )

    assert len(truth.points) == 2
    assert truth.points[-1].value == 3.65
    facts = dict(truth.evidence[0].facts)
    assert facts["source_family"] == "macro_market_state"
    assert facts["source_provider"] == "fred_public"
    assert facts["provider_execution"]["authority_level"] == "direct"
    assert facts["provider_execution"]["provider_symbol"] == "SOFR"


def test_public_fred_market_state_truth_derives_cpi_yoy(monkeypatch) -> None:
    from app.v2.core.market_state_sources import load_public_fred_market_state_truth

    class _Response:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:
            return None

    csv_text = "\n".join(
        [
            "observation_date,CPIAUCSL",
            "2025-01-01,99.8",
            "2025-02-01,100.0",
            "2025-03-01,100.2",
            "2025-04-01,100.4",
            "2025-05-01,100.6",
            "2025-06-01,100.8",
            "2025-07-01,101.0",
            "2025-08-01,101.2",
            "2025-09-01,101.4",
            "2025-10-01,101.6",
            "2025-11-01,101.8",
            "2025-12-01,102.0",
            "2026-01-01,102.2",
            "2026-02-01,102.4",
        ]
    )

    monkeypatch.setattr(
        "app.v2.core.market_state_sources.requests.get",
        lambda _url, *, params, headers, timeout: _Response(csv_text),
    )

    truth = load_public_fred_market_state_truth(
        symbol="CPI_YOY",
        label="Inflation",
        series_id="CPIAUCSL",
        transform="cpi_yoy",
        units="percent",
        cadence="monthly",
    )

    assert round(truth.points[-1].value, 2) == 2.40


def test_market_truth_validation_rejects_stale_market_close(monkeypatch) -> None:
    from app.v2.core.market_strip_registry import market_strip_spec
    from app.v2.surfaces.daily_brief.contract_builder import _annotate_market_truth, _market_truth_validation

    truth = MarketSeriesTruth(
        series_id="market:^spxew",
        label="^GSPC",
        frequency="daily",
        units="index",
        points=[
            MarketDataPoint(at="2026-04-06T00:00:00+00:00", value=7859.95),
            MarketDataPoint(at="2026-04-07T00:00:00+00:00", value=7837.71),
        ],
        evidence=[
            EvidencePack(
                evidence_id="evidence_market_^gspc",
                thesis="Index close",
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
                        "observed_at": "2026-04-06T00:00:00+00:00",
                    },
                },
            )
        ],
    )

    monkeypatch.setattr(
        "app.v2.surfaces.daily_brief.contract_builder._expected_market_close_date",
        lambda: date(2026, 4, 7),
    )

    applied = _annotate_market_truth(truth, market_strip_spec("^GSPC"))
    is_exact, validation_status, validation_reason = _market_truth_validation(applied, market_strip_spec("^GSPC"))
    facts = dict(applied.evidence[0].facts)

    assert is_exact is False
    assert validation_status == "stale"
    assert "Expected 2026-04-07 close" in str(validation_reason)
    assert facts["validation_status"] == "stale"


def test_expected_market_close_date_uses_latest_completed_us_close() -> None:
    from app.v2.surfaces.daily_brief.contract_builder import _expected_market_close_date

    assert _expected_market_close_date(datetime(2026, 4, 9, 16, 14, tzinfo=UTC)) == date(2026, 4, 8)
    assert _expected_market_close_date(datetime(2026, 4, 9, 23, 30, tzinfo=UTC)) == date(2026, 4, 9)


def test_market_state_card_exposes_declared_close_metadata(monkeypatch) -> None:
    from app.v2.core.market_strip_registry import market_strip_spec
    from app.v2.surfaces.daily_brief.contract_builder import _annotate_market_truth, _market_state_card_from_truth

    truth = MarketSeriesTruth(
        series_id="market:^gspc",
        label="^GSPC",
        frequency="daily",
        units="index",
        points=[
            MarketDataPoint(at="2026-04-06T00:00:00+00:00", value=6611.83),
            MarketDataPoint(at="2026-04-07T00:00:00+00:00", value=6550.29),
        ],
        evidence=[
            EvidencePack(
                evidence_id="evidence_market_^gspc",
                thesis="Index close",
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

    card = _market_state_card_from_truth(_annotate_market_truth(truth, market_strip_spec("^GSPC")))

    assert card["source_provider"] == "yahoo_finance"
    assert card["source_authority_tier"] == "public_verified_close"
    assert card["metric_definition"] == "S&P 500 index official daily close"
    assert card["metric_polarity"] == "higher_is_better"
    assert card["validation_status"] == "valid"
    assert "Validated declared daily close" in str(card["validation_reason"])
    assert card["freshness_mode"] == "fresh_current_slot"
    assert card["session_relevance_state"] == "fresh_session_move"


def test_credit_card_tone_uses_metric_polarity(monkeypatch) -> None:
    from app.v2.core.market_strip_registry import market_strip_spec
    from app.v2.surfaces.daily_brief.contract_builder import _annotate_market_truth, _market_state_card_from_truth

    truth = MarketSeriesTruth(
        series_id="market:bamlh0a0hym2",
        label="BAMLH0A0HYM2",
        frequency="daily",
        units="spread",
        points=[
            MarketDataPoint(at="2026-04-05T00:00:00+00:00", value=3.13),
            MarketDataPoint(at="2026-04-06T00:00:00+00:00", value=3.05),
        ],
        evidence=[
            EvidencePack(
                evidence_id="evidence_market_credit",
                thesis="Credit spread",
                summary="Spread",
                facts={
                    "source_family": "macro_market_state",
                    "source_provider": "fred_public",
                    "movement_state": "known",
                    "provider_execution": {
                        "provider_name": "fred_public",
                        "source_family": "macro_market_state",
                        "authority_level": "direct",
                        "semantic_grade": "movement_capable",
                        "usable_truth": True,
                        "observed_at": "2026-04-06T00:00:00+00:00",
                        "fetched_at": "2026-04-08T00:05:00+00:00",
                        "provenance_strength": "official_macro_reference",
                    },
                    "truth_envelope": {
                        "as_of_utc": "2026-04-06",
                        "observation_date": "2026-04-06",
                        "reference_period": "2026-04-06",
                        "source_authority": "fred_public",
                        "acquisition_mode": "live",
                        "retrieved_at_utc": "2026-04-08T00:05:00+00:00",
                    },
                },
            )
        ],
    )

    monkeypatch.setattr(
        "app.v2.surfaces.daily_brief.contract_builder._current_surface_slot_boundary",
        lambda now=None: datetime(2026, 4, 6, 0, 0, tzinfo=UTC),
    )

    card = _market_state_card_from_truth(_annotate_market_truth(truth, market_strip_spec("BAMLH0A0HYM2")))

    assert card["caption"] == "Credit tone firmer"
    assert card["tone"] == "good"


def test_weekly_official_release_card_downgrades_to_backdrop_when_outside_session_window(monkeypatch) -> None:
    from app.v2.core.market_strip_registry import market_strip_spec
    from app.v2.surfaces.daily_brief.contract_builder import _annotate_market_truth, _market_state_card_from_truth

    truth = MarketSeriesTruth(
        series_id="market:mortgage30us",
        label="MORTGAGE30US",
        frequency="weekly",
        units="percent",
        points=[
            MarketDataPoint(at="2026-03-27T00:00:00+00:00", value=6.38),
            MarketDataPoint(at="2026-04-02T00:00:00+00:00", value=6.46),
        ],
        evidence=[
            EvidencePack(
                evidence_id="evidence_market_mortgage30us",
                thesis="Mortgage rate",
                summary="Mortgage rate",
                facts={
                    "source_family": "macro_market_state",
                    "source_provider": "fred_public",
                    "movement_state": "known",
                    "provider_execution": {
                        "provider_name": "fred_public",
                        "source_family": "macro_market_state",
                        "authority_level": "direct",
                        "semantic_grade": "movement_capable",
                        "usable_truth": True,
                        "observed_at": "2026-04-02T00:00:00+00:00",
                        "fetched_at": "2026-04-08T00:05:00+00:00",
                        "provenance_strength": "official_macro_reference",
                    },
                    "truth_envelope": {
                        "as_of_utc": "2026-04-02",
                        "observation_date": "2026-04-02",
                        "reference_period": "2026-04-02",
                        "source_authority": "fred_public",
                        "acquisition_mode": "live",
                        "retrieved_at_utc": "2026-04-08T00:05:00+00:00",
                    },
                },
            )
        ],
    )

    monkeypatch.setattr(
        "app.v2.surfaces.daily_brief.contract_builder._current_surface_slot_boundary",
        lambda now=None: datetime(2026, 4, 8, 0, 0, tzinfo=UTC),
    )
    monkeypatch.setattr(
        "app.v2.surfaces.daily_brief.contract_builder._previous_surface_slot_boundary",
        lambda now=None: datetime(2026, 4, 7, 12, 0, tzinfo=UTC),
    )

    card = _market_state_card_from_truth(_annotate_market_truth(truth, market_strip_spec("MORTGAGE30US")))

    assert card["validation_status"] == "valid"
    assert card["freshness_mode"] == "fresh_current_slot"
    assert card["session_relevance_state"] == "cadence_valid_backdrop"
    assert card["value"] == "neutral · minor"
    assert card["caption"] == "Latest weekly reading"
    assert card["sub_caption"] == "Backdrop input; no fresh session update"
    assert card["tone"] == "neutral"
    assert round(card["change_pct_1d"], 2) == 1.25
