from __future__ import annotations

from app.v2.core.domain_objects import MacroTruth
from app.v2.sources import macro_adapter, news_adapter
from app.v2.translators import macro_signal_translator, news_signal_translator


def test_tier1b_adapters_import() -> None:
    assert macro_adapter is not None
    assert news_adapter is not None


def test_macro_adapter_fetch_all_returns_cached_series() -> None:
    rows = macro_adapter.fetch_all()

    assert rows
    assert any(str(row.get("series_id") or "").upper() == "DGS10" for row in rows)


def test_macro_signal_translator_handles_empty_payload() -> None:
    result = macro_signal_translator.translate({})

    assert isinstance(result, MacroTruth)
    assert result.macro_id == "macro:unknown"


def test_news_signal_translator_handles_empty_list() -> None:
    assert news_signal_translator.translate([]) == []
