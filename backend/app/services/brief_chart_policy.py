from __future__ import annotations

from typing import Any


def annotate_reader_chart(
    chart_key: str,
    chart: dict[str, Any],
    *,
    delta_candidate_count: int = 0,
) -> dict[str, Any]:
    annotated = dict(chart)
    key = str(chart_key or "")

    if key == "metric_delta_chart":
        annotated["chart_class"] = "change-over-time chart"
        annotated["interpretive_value"] = "medium"
        annotated["usefulness_reason"] = (
            "Shows which monitored moves changed the most across the current brief, but it is secondary if the top developments already make the same point clearly."
        )
        annotated["hidden_if_redundant"] = delta_candidate_count < 3
        return annotated

    if key == "cross_asset_stress_panel":
        annotated["chart_class"] = "cross-asset stress comparison chart"
        annotated["interpretive_value"] = "high"
        annotated["usefulness_reason"] = (
            "Shows whether stress is isolated or broadening across volatility, credit, EM, and USD conditions faster than prose alone."
        )
        annotated["hidden_if_redundant"] = False
        return annotated

    if key == "prior_versus_current_implication_comparison":
        annotated["chart_class"] = "prior-vs-current implication comparison chart"
        annotated["interpretive_value"] = "high"
        annotated["usefulness_reason"] = (
            "Shows whether the brief is dealing with a new issue, a fading issue, or a broadening one, which is hard to retain from text alone."
        )
        annotated["hidden_if_redundant"] = False
        return annotated

    if key == "regime_timeline":
        annotated["chart_class"] = "regime context chart"
        annotated["interpretive_value"] = "high"
        annotated["usefulness_reason"] = (
            "Shows how the current brief fits into the recent regime path rather than treating today as an isolated snapshot."
        )
        annotated["hidden_if_redundant"] = False
        return annotated

    annotated["chart_class"] = annotated.get("chart_class") or "supporting chart"
    annotated["interpretive_value"] = annotated.get("interpretive_value") or "low"
    annotated["usefulness_reason"] = annotated.get("usefulness_reason") or "Supports the brief, but does not add first-read value."
    annotated["hidden_if_redundant"] = annotated.get("hidden_if_redundant", True)
    return annotated
