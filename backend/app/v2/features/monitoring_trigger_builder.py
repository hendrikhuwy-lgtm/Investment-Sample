from __future__ import annotations

from typing import Any


def build_monitoring_condition(
    *,
    condition_id: str,
    label: str,
    why_now: str,
    confirms: str,
    breaks: str,
    implication: str,
    portfolio_consequence: str,
    next_action: str,
) -> dict[str, Any]:
    return {
        "condition_id": condition_id,
        "label": label,
        "why_now": why_now,
        "near_term_trigger": confirms,
        "thesis_trigger": implication,
        "break_condition": breaks,
        "portfolio_consequence": portfolio_consequence,
        "next_action": next_action,
    }

