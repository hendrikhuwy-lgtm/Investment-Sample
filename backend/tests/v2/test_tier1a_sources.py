from __future__ import annotations

import pytest

from app.v2.sources.registry import get_issuer_adapter
from app.v2.translators.instrument_truth_translator import translate as translate_instrument_truth
from app.v2.translators.market_signal_translator import translate as translate_market_signal


def test_e2e_issuer_to_instrument_truth() -> None:
    """issuer factsheet fetch -> translator -> InstrumentTruth object"""
    adapter = get_issuer_adapter()
    raw = adapter.fetch_all()[0]

    truth = translate_instrument_truth(raw)

    assert truth.instrument_id or truth.symbol
    assert truth.metrics["source_id"] == "issuer_factsheet_adapter"
    for document in truth.metrics.get("primary_documents", []):
        assert document.get("authority_class")
        assert document.get("document_fingerprint")


def test_market_signal_translator_maps_market_adapter_payload() -> None:
    truth = translate_market_signal(
        {
            "ticker": "VWRA",
            "price": 101.2,
            "open": 100.0,
            "currency": "USD",
        }
    )

    assert truth.series_id == "market:VWRA"
    assert truth.evidence[0].facts["current_value"] == 101.2
    assert truth.evidence[0].facts["one_day_change_pct"] == pytest.approx(1.2)
    assert truth.evidence[0].facts["source_id"] == "market_price_adapter"
    assert "truth_envelope" in truth.evidence[0].facts
