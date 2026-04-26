from __future__ import annotations

from typing import Any


_MAGNITUDE_WEIGHT = {"significant": 16, "moderate": 10, "minor": 5}


def _normalize_direction(value: str) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"up", "positive"}:
        return "positive"
    if raw in {"down", "negative"}:
        return "negative"
    return "mixed"


def build_scenarios(
    *,
    signal_id: str,
    label: str,
    direction: str,
    magnitude: str,
    implication: str,
    why_here: str,
) -> dict[str, Any]:
    normalized_direction = _normalize_direction(direction)
    amplitude = _MAGNITUDE_WEIGHT.get(str(magnitude or "").strip().lower(), 8)

    if normalized_direction == "positive":
        bull_return, base_return, bear_return = f"+{amplitude + 6}%", f"+{amplitude - 1}%", f"-{max(3, amplitude // 2)}%"
        bull_effect = f"{label} keeps supporting current positioning and widens room for action."
        base_effect = f"{label} stays supportive enough to keep the current posture intact."
        bear_effect = f"{label} reverses and removes support from the current read."
    elif normalized_direction == "negative":
        bull_return, base_return, bear_return = f"+{max(3, amplitude // 2)}%", f"-{max(2, amplitude - 4)}%", f"-{amplitude + 6}%"
        bull_effect = f"{label} stabilizes and softens the current downside read."
        base_effect = f"{label} keeps pressure on the current posture."
        bear_effect = f"{label} deteriorates further and forces tighter review discipline."
    else:
        bull_return, base_return, bear_return = f"+{max(2, amplitude // 2)}%", "0%", f"-{max(2, amplitude // 2)}%"
        bull_effect = f"{label} resolves in a supportive direction."
        base_effect = f"{label} stays mixed, so the current posture remains bounded."
        bear_effect = f"{label} resolves against the current read."

    return {
        "signal_id": signal_id,
        "label": label,
        "summary": implication or why_here,
        "scenarios": [
            {
                "scenario_id": f"{signal_id}_bull",
                "type": "bull",
                "label": f"{label} improves",
                "portfolio_effect": bull_effect,
                "macro": implication or None,
                "micro": why_here or None,
                "short_term": f"1-4 weeks: {bull_effect}",
                "long_term": "3-12 months: support broadens if the signal remains durable.",
            },
            {
                "scenario_id": f"{signal_id}_base",
                "type": "base",
                "label": f"{label} holds current path",
                "portfolio_effect": base_effect,
                "macro": implication or None,
                "micro": why_here or None,
                "short_term": f"1-4 weeks: expected direction {base_return}.",
                "long_term": "3-12 months: current positioning remains justified unless evidence weakens.",
            },
            {
                "scenario_id": f"{signal_id}_bear",
                "type": "bear",
                "label": f"{label} breaks against the read",
                "portfolio_effect": bear_effect,
                "macro": implication or None,
                "micro": why_here or None,
                "short_term": f"1-4 weeks: expected direction {bear_return}.",
                "long_term": "3-12 months: thesis confidence should be reset if the break persists.",
            },
        ],
    }

