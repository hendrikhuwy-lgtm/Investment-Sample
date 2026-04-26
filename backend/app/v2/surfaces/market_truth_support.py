from __future__ import annotations

from app.v2.contracts.chart_contracts import load_routed_market_series
from app.v2.core.domain_objects import MarketSeriesTruth
from app.v2.sources.registry import get_market_adapter, get_source_definition
from app.v2.translators.market_signal_translator import translate as translate_market_signal


def load_surface_market_truth(
    *,
    symbol: str,
    surface_name: str,
    endpoint_family: str = "ohlcv_history",
    lookback: int = 120,
    allow_live_fetch: bool = True,
) -> MarketSeriesTruth:
    try:
        truth, _payload = load_routed_market_series(
            surface_name=surface_name,
            endpoint_family=endpoint_family,
            identifier=symbol,
            label=symbol,
            lookback=lookback,
            allow_live_fetch=allow_live_fetch,
        )
        if len(truth.points) >= 2:
            return truth
    except Exception:
        pass

    if not allow_live_fetch:
        return translate_market_signal({})

    get_source_definition("etf_market_state")
    adapter = get_market_adapter()
    try:
        try:
            payload = adapter.fetch(symbol, surface_name=surface_name)
        except TypeError:
            payload = adapter.fetch(symbol)
    except Exception:
        payload = {}
    return translate_market_signal(payload)
