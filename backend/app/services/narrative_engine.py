from __future__ import annotations

import re
from typing import Any


METRIC_FRIENDLY_NAMES: dict[str, str] = {
    "DGS10": "US 10Y Treasury Yield",
    "T10Y2Y": "US 10Y-2Y Treasury Curve",
    "T10YIE": "US 10Y Breakeven Inflation",
    "VIXCLS": "CBOE VIX",
    "BAMLH0A0HYM2": "US High Yield OAS",
    "SP500": "SP 500 Index",
    "DCOILWTICO": "WTI Crude Oil",
    "DTWEXBGS": "US Dollar Broad Index",
    "VXEEMCLS": "EM Volatility Index",
    "DEXSIUS": "SGD per USD",
    "IRLTLT01SGM156N": "Singapore 10Y Government Yield",
}


def _friendly_metric_name(metric_id: str) -> str:
    code = str(metric_id or "").upper()
    return METRIC_FRIENDLY_NAMES.get(code, code or "Metric")


def _sanitize_text(text: str) -> str:
    cleaned = re.sub(r"\bcitations?\s+\d+\b", "", str(text or ""), flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = cleaned.replace("..", ".")
    cleaned = cleaned.replace(" .", ".")
    return cleaned


def build_alert_narrative(alert: dict[str, Any]) -> str:
    metric_id = str(alert.get("metric_id", "Metric"))
    metric_name = _friendly_metric_name(metric_id)
    delta_value = float(alert.get("delta_value", 0.0) or 0.0)
    delta_window = str(alert.get("delta_window", "1d"))
    threshold_name = str(alert.get("threshold_name", "threshold"))
    threshold_value = float(alert.get("threshold_value", 0.0) or 0.0)
    current_value = float(alert.get("current_value", 0.0) or 0.0)
    percentile_60 = alert.get("percentile_60")
    prev_state = str(alert.get("prev_state", "stable"))
    curr_state = str(alert.get("curr_state", "stable"))
    days_in_state = int(alert.get("days_in_state", 1) or 1)
    since_when = str(alert.get("since_when", "latest snapshot"))
    impact_map = dict(alert.get("impact_map") or {})
    primary = _sanitize_text(str(impact_map.get("primary") or "No portfolio mapping metadata."))
    secondary = _sanitize_text(str(impact_map.get("secondary") or "No secondary mapping metadata."))
    convex = _sanitize_text(str(impact_map.get("convex_relevance") or "Convex relevance not specified."))

    percentile_text = "n/a" if percentile_60 is None else f"{float(percentile_60):.1f}"
    distance_to_revert = float(alert.get("distance_to_revert", 0.0) or 0.0)
    distance_unit = str(alert.get("distance_unit") or "pct pts")
    if distance_to_revert >= 0:
        distance_phrase = (
            f"distance to exit state {distance_to_revert:.2f} {distance_unit} "
            "(additional decline needed to exit)."
        )
    else:
        distance_phrase = (
            f"distance to exit state {distance_to_revert:.2f} {distance_unit} "
            f"(currently above threshold by {abs(distance_to_revert):.2f} {distance_unit}; "
            "that decline is needed to exit)."
        )
    threshold_line = (
        f"Trigger transparency: {threshold_name} {threshold_value:.1f}, current {current_value:.2f}, {distance_phrase}"
    )
    portfolio_transmission_line = _sanitize_text(str(impact_map.get("portfolio_transmission_line") or ""))
    monitor_next = _sanitize_text(
        str(
            alert.get("what_to_monitor_next")
            or impact_map.get("monitor_next")
            or "Monitor persistence, cross-asset confirmation, and any spillover into Blueprint or live holdings."
        )
    )

    narrative = (
        f"[{metric_name} ({metric_id})] moved {delta_value:+.2f} over {delta_window}, crossing {threshold_name} at {threshold_value:.1f}. "
        f"Current: {current_value:.2f}, percentile: {percentile_text}. "
        f"State changed from {prev_state} to {curr_state}. Duration: {days_in_state} day(s) since {since_when}. "
        f"Why it matters: {primary} Secondary: {secondary} Convex relevance: {convex}. "
        f"{portfolio_transmission_line} What to monitor next: {monitor_next}. {threshold_line}"
    )
    return _sanitize_text(narrative)


def no_material_change_text(state: str, confidence: float) -> str:
    return (
        "No material threshold crossings observed. "
        f"Macro regime remains {state} with confidence {confidence:.2f}. "
        "No new stress accelerations detected."
    )
