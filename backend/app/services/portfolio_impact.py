from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ImpactRule:
    sleeve_sensitivity: dict[str, float]
    primary: str
    secondary: str
    convex_relevance: str


_DEFAULT_RULE = ImpactRule(
    sleeve_sensitivity={
        "global_equity": 0.35,
        "ig_bond": 0.25,
        "real_asset": 0.20,
        "alt": 0.15,
        "convex": 0.05,
        "cash": 0.00,
    },
    primary="Broad multi-asset monitoring implications.",
    secondary="Cross-asset context should be monitored for persistence.",
    convex_relevance="Convex sleeve relevance unchanged.",
)

_IMPACT_RULES: dict[str, ImpactRule] = {
    "DGS10": ImpactRule(
        sleeve_sensitivity={
            "ig_bond": 0.55,
            "global_equity": 0.25,
            "real_asset": 0.10,
            "alt": 0.05,
            "convex": 0.05,
            "cash": 0.00,
        },
        primary="Rates sensitivity is concentrated in IG bonds and duration-sensitive equities.",
        secondary="Higher rate momentum can shift discount-rate assumptions and financing conditions.",
        convex_relevance="Convex sleeve relevance is modestly elevated when rate volatility rises.",
    ),
    "T10YIE": ImpactRule(
        sleeve_sensitivity={
            "real_asset": 0.40,
            "ig_bond": 0.25,
            "global_equity": 0.20,
            "alt": 0.10,
            "convex": 0.05,
            "cash": 0.00,
        },
        primary="Inflation expectation moves are most relevant for real assets and inflation-sensitive sleeves.",
        secondary="Breakeven changes can affect real-yield assumptions and diversification behavior.",
        convex_relevance="Convex sleeve relevance typically increases if inflation repricing raises uncertainty.",
    ),
    "SP500": ImpactRule(
        sleeve_sensitivity={
            "global_equity": 0.65,
            "alt": 0.15,
            "real_asset": 0.10,
            "convex": 0.10,
            "ig_bond": 0.00,
            "cash": 0.00,
        },
        primary="Equity market direction primarily affects the global equity sleeve and risk budget.",
        secondary="Cross-asset diversification assumptions should be monitored when equity momentum shifts.",
        convex_relevance="Convex sleeve can matter more during equity drawdown acceleration.",
    ),
    "VIXCLS": ImpactRule(
        sleeve_sensitivity={
            "global_equity": 0.45,
            "convex": 0.35,
            "alt": 0.10,
            "real_asset": 0.05,
            "ig_bond": 0.05,
            "cash": 0.00,
        },
        primary="Volatility regime changes are most relevant to equity dispersion and convex overlays.",
        secondary="Elevated implied volatility can alter cross-asset risk premia behavior.",
        convex_relevance="Convex sleeve relevance is elevated under higher implied volatility conditions.",
    ),
    "BAMLH0A0HYM2": ImpactRule(
        sleeve_sensitivity={
            "global_equity": 0.35,
            "ig_bond": 0.30,
            "alt": 0.15,
            "real_asset": 0.10,
            "convex": 0.10,
            "cash": 0.00,
        },
        primary="Credit spread widening is linked to broader risk-asset stress and financing conditions.",
        secondary="High-yield spread shifts can influence both equity risk appetite and bond spread risk.",
        convex_relevance="Convex relevance is elevated when credit stress indicators accelerate.",
    ),
}


def quantify_portfolio_impact(
    metric_id: str,
    sleeve_weights: dict[str, float],
    *,
    holdings_available: bool = True,
) -> dict[str, object]:
    rule = _IMPACT_RULES.get(metric_id, _DEFAULT_RULE)
    raw_score = 0.0
    exposures: list[tuple[str, float]] = []
    for sleeve, weight in sleeve_weights.items():
        sensitivity = rule.sleeve_sensitivity.get(sleeve, 0.0)
        weighted_exposure = max(0.0, float(weight)) * sensitivity
        raw_score += weighted_exposure
        exposures.append((sleeve, max(0.0, float(weight))))
    impact_score = max(0.0, min(100.0, raw_score * 100.0))
    ranked_sleeves = sorted(
        (
            {
                "sleeve": sleeve,
                "weight": round(weight, 6),
                "weight_pct_nav": round(weight * 100.0, 2),
                "sensitivity": round(rule.sleeve_sensitivity.get(sleeve, 0.0), 4),
            }
            for sleeve, weight in exposures
        ),
        key=lambda item: item["weight"] * item["sensitivity"],
        reverse=True,
    )
    primary_sleeves = ranked_sleeves[:2]
    secondary_sleeves = ranked_sleeves[2:4]
    if holdings_available and ranked_sleeves:
        transmission = ", ".join(
            f"{item['sleeve']} {item['weight_pct_nav']:.1f}% NAV"
            for item in ranked_sleeves[:3]
        )
        portfolio_transmission_line = f"Portfolio transmission: primary {transmission}."
    else:
        portfolio_transmission_line = "Portfolio transmission unavailable, holdings not ingested."

    return {
        "impact_score": round(impact_score, 2),
        "primary": rule.primary,
        "secondary": rule.secondary,
        "convex_relevance": rule.convex_relevance,
        "sleeve_sensitivity": rule.sleeve_sensitivity,
        "primary_sleeves": primary_sleeves,
        "secondary_sleeves": secondary_sleeves,
        "portfolio_transmission_line": portfolio_transmission_line,
        "holdings_available": holdings_available,
    }
