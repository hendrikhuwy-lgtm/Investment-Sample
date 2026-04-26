from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from app.models.types import PortfolioSignal


SIGNAL_METHODOLOGY_VERSION = "2026.03.08"
SIGNAL_THRESHOLD_REGISTRY: dict[str, dict[str, Any]] = {
    "vix_level": {
        "watch_threshold": 16.0,
        "alert_threshold": 22.0,
        "methodology_note": "Observational percentile-style threshold for volatility regime monitoring, not a policy trigger.",
    },
    "hy_oas_level": {
        "watch_threshold": 3.5,
        "alert_threshold": 4.0,
        "methodology_note": "Credit spread stress threshold used to detect tightening financial conditions.",
    },
    "dgs10_level": {
        "watch_threshold": 4.0,
        "alert_threshold": 4.5,
        "methodology_note": "Rate-level observation threshold for discount-rate sensitivity monitoring.",
    },
    "dgs10_5obs_change": {
        "watch_threshold": 0.1,
        "alert_threshold": 0.2,
        "methodology_note": "Momentum threshold for abrupt rates repricing.",
    },
    "t10yie_level": {
        "watch_threshold": 2.4,
        "alert_threshold": 2.7,
        "methodology_note": "Inflation-expectation regime threshold for monitoring inflation-sensitive sleeves.",
    },
}


def merged_signal_threshold_registry(
    override_thresholds: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    merged = deepcopy(SIGNAL_THRESHOLD_REGISTRY)
    for metric_key, value in (override_thresholds or {}).items():
        base = merged.get(metric_key, {})
        if isinstance(value, dict):
            merged[metric_key] = {**base, **value}
    return merged


def signal_methodology_registry(
    override_thresholds: dict[str, dict[str, Any]] | None = None,
    *,
    methodology_version: str | None = None,
) -> dict[str, Any]:
    return {
        "version": methodology_version or SIGNAL_METHODOLOGY_VERSION,
        "thresholds": merged_signal_threshold_registry(override_thresholds),
    }


def _state_from_threshold(value: float, watch_threshold: float, alert_threshold: float) -> str:
    if value >= alert_threshold:
        return "alert"
    if value >= watch_threshold:
        return "watch"
    return "normal"


def _abs_state_from_threshold(value: float, watch_threshold: float, alert_threshold: float) -> str:
    absolute = abs(value)
    if absolute >= alert_threshold:
        return "alert"
    if absolute >= watch_threshold:
        return "watch"
    return "normal"


def rate_shock_signal(metric_value: float, threshold: float = 0.35) -> PortfolioSignal:
    state = "normal"
    if metric_value >= threshold:
        state = "alert"
    elif metric_value >= threshold * 0.7:
        state = "watch"

    return PortfolioSignal(
        signal_id=f"rate_shock_{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}",
        metric="dgs10_5d_change",
        value=metric_value,
        threshold=threshold,
        state=state,
        portfolio_impact={
            "equities": "higher discount rates pressure valuations",
            "bonds": "duration sensitivity increases",
            "convex": "may imply increased demand for convex protection",
        },
    )


def inflation_expectation_signal(metric_value: float, threshold: float = 0.2) -> PortfolioSignal:
    state = "normal"
    if abs(metric_value) >= threshold:
        state = "alert"
    elif abs(metric_value) >= threshold * 0.7:
        state = "watch"

    return PortfolioSignal(
        signal_id=f"inflation_shift_{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}",
        metric="t10yie_5d_change",
        value=metric_value,
        threshold=threshold,
        state=state,
        portfolio_impact={
            "equities": "sector/style rotation risk",
            "bonds": "real yield regime shifts",
            "real_assets": "inflation hedge relevance changes",
        },
    )


def extended_market_signals(
    series: dict[str, dict[str, float]],
    threshold_registry: dict[str, dict[str, Any]] | None = None,
) -> list[PortfolioSignal]:
    now = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    thresholds = merged_signal_threshold_registry(threshold_registry)

    dgs10 = series.get("DGS10", {})
    t10y2y = series.get("T10Y2Y", {})
    t10yie = series.get("T10YIE", {})
    sp500 = series.get("SP500", {})
    vix = series.get("VIXCLS", {})
    hy = series.get("BAMLH0A0HYM2", {})
    oil = series.get("DCOILWTICO", {})
    usd = series.get("DTWEXBGS", {})
    em_vol = series.get("VXEEMCLS", {})
    sgd = series.get("DEXSIUS", {})
    sg10 = series.get("IRLTLT01SGM156N", {})

    signals = [
        PortfolioSignal(
            signal_id=f"volatility_regime_{now}",
            metric="vix_level",
            value=float(vix.get("latest_value", 0.0)),
            threshold=float(thresholds["vix_level"]["alert_threshold"]),
            state=_state_from_threshold(
                float(vix.get("latest_value", 0.0)),
                float(thresholds["vix_level"]["watch_threshold"]),
                float(thresholds["vix_level"]["alert_threshold"]),
            ),
            portfolio_impact={
                "equities": "higher implied volatility may imply larger short-term valuation swings",
                "convex": "supports relevance of protection sleeve",
            },
        ),
        PortfolioSignal(
            signal_id=f"credit_stress_{now}",
            metric="hy_oas_level",
            value=float(hy.get("latest_value", 0.0)),
            threshold=float(thresholds["hy_oas_level"]["alert_threshold"]),
            state=_state_from_threshold(
                float(hy.get("latest_value", 0.0)),
                float(thresholds["hy_oas_level"]["watch_threshold"]),
                float(thresholds["hy_oas_level"]["alert_threshold"]),
            ),
            portfolio_impact={
                "credit": "wider spreads may indicate higher perceived default/liquidity risk",
                "equities": "credit stress is consistent with tighter risk conditions",
            },
        ),
        PortfolioSignal(
            signal_id=f"equity_trend_{now}",
            metric="sp500_5obs_change",
            value=float(sp500.get("change_5obs", 0.0)),
            threshold=100.0,
            state=_abs_state_from_threshold(float(sp500.get("change_5obs", 0.0)), 60.0, 100.0),
            portfolio_impact={
                "equities": "directional momentum may imply regime persistence or reversal risk",
                "alternatives": "trend-sensitive strategies may react faster to moves",
            },
        ),
        PortfolioSignal(
            signal_id=f"rates_level_{now}",
            metric="dgs10_level",
            value=float(dgs10.get("latest_value", 0.0)),
            threshold=float(thresholds["dgs10_level"]["alert_threshold"]),
            state=_state_from_threshold(
                float(dgs10.get("latest_value", 0.0)),
                float(thresholds["dgs10_level"]["watch_threshold"]),
                float(thresholds["dgs10_level"]["alert_threshold"]),
            ),
            portfolio_impact={
                "equities": "higher discount-rate backdrop may compress valuation multiples",
                "bonds": "duration sensitivity remains material",
            },
        ),
        PortfolioSignal(
            signal_id=f"rates_momentum_{now}",
            metric="dgs10_5obs_change",
            value=float(dgs10.get("change_5obs", 0.0)),
            threshold=float(thresholds["dgs10_5obs_change"]["alert_threshold"]),
            state=_abs_state_from_threshold(
                float(dgs10.get("change_5obs", 0.0)),
                float(thresholds["dgs10_5obs_change"]["watch_threshold"]),
                float(thresholds["dgs10_5obs_change"]["alert_threshold"]),
            ),
            portfolio_impact={
                "equities": "fast rate moves may imply repricing pressure",
                "bonds": "momentum in yields can shift duration outcomes quickly",
            },
        ),
        PortfolioSignal(
            signal_id=f"inflation_level_{now}",
            metric="t10yie_level",
            value=float(t10yie.get("latest_value", 0.0)),
            threshold=float(thresholds["t10yie_level"]["alert_threshold"]),
            state=_state_from_threshold(
                float(t10yie.get("latest_value", 0.0)),
                float(thresholds["t10yie_level"]["watch_threshold"]),
                float(thresholds["t10yie_level"]["alert_threshold"]),
            ),
            portfolio_impact={
                "real_assets": "inflation expectation level can alter hedge usefulness",
                "bonds": "real-yield and inflation-break-even dynamics matter",
            },
        ),
        PortfolioSignal(
            signal_id=f"inflation_momentum_{now}",
            metric="t10yie_5obs_change",
            value=float(t10yie.get("change_5obs", 0.0)),
            threshold=0.12,
            state=_abs_state_from_threshold(float(t10yie.get("change_5obs", 0.0)), 0.07, 0.12),
            portfolio_impact={
                "equities": "changes in inflation expectations may imply style and duration rotation",
                "bonds": "break-even momentum can influence rate path assumptions",
            },
        ),
        PortfolioSignal(
            signal_id=f"curve_shape_{now}",
            metric="t10y2y_level",
            value=float(t10y2y.get("latest_value", 0.0)),
            threshold=0.0,
            state=_state_from_threshold(float(t10y2y.get("latest_value", 0.0)), 0.5, 1.0) if float(t10y2y.get("latest_value", 0.0)) >= 0 else "watch",
            portfolio_impact={
                "bonds": "curve shape can alter duration and carry assumptions",
                "benchmark": "duration-sensitive benchmark assumptions may need review",
            },
        ),
        PortfolioSignal(
            signal_id=f"oil_regime_{now}",
            metric="oil_5obs_change",
            value=float(oil.get("change_5obs", 0.0)),
            threshold=5.0,
            state=_abs_state_from_threshold(float(oil.get("change_5obs", 0.0)), 3.0, 5.0),
            portfolio_impact={
                "real_assets": "commodity repricing can change inflation-hedge relevance",
                "equities": "energy-sensitive sectors and input-cost assumptions may shift",
            },
        ),
        PortfolioSignal(
            signal_id=f"usd_regime_{now}",
            metric="usd_broad_5obs_change",
            value=float(usd.get("change_5obs", 0.0)),
            threshold=1.0,
            state=_abs_state_from_threshold(float(usd.get("change_5obs", 0.0)), 0.5, 1.0),
            portfolio_impact={
                "emerging_markets": "stronger USD can tighten EM financial conditions",
                "singapore": "USD/SGD moves affect SGD-based investor translation on USD assets",
            },
        ),
        PortfolioSignal(
            signal_id=f"em_volatility_{now}",
            metric="vxeem_level",
            value=float(em_vol.get("latest_value", 0.0)),
            threshold=28.0,
            state=_state_from_threshold(float(em_vol.get("latest_value", 0.0)), 22.0, 28.0),
            portfolio_impact={
                "emerging_markets": "higher EM volatility raises watch-level relevance for EM sleeves",
                "convex": "cross-asset volatility spillover can increase convex sleeve relevance",
            },
        ),
        PortfolioSignal(
            signal_id=f"sgd_translation_{now}",
            metric="sgd_per_usd_5obs_change",
            value=float(sgd.get("change_5obs", 0.0)),
            threshold=0.02,
            state=_abs_state_from_threshold(float(sgd.get("change_5obs", 0.0)), 0.01, 0.02),
            portfolio_impact={
                "cash_bills": "SGD opportunity cost versus USD assets may shift",
                "portfolio": "USD asset translation into SGD should be monitored",
            },
        ),
        PortfolioSignal(
            signal_id=f"sg_rates_{now}",
            metric="sg10y_level",
            value=float(sg10.get("latest_value", 0.0)),
            threshold=3.0,
            state=_state_from_threshold(float(sg10.get("latest_value", 0.0)), 2.5, 3.0),
            portfolio_impact={
                "cash_bills": "local opportunity cost for SGD investors changes with SG rates",
                "bonds": "local yield context matters for SGD-based allocators",
            },
        ),
    ]

    return signals


def summarize_signal_state(signals: list[PortfolioSignal]) -> str:
    if any(signal.state == "alert" for signal in signals):
        return "Alert"
    if any(signal.state == "watch" for signal in signals):
        return "Watch"
    return "Normal"
