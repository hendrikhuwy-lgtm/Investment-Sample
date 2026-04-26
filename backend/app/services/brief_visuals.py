from __future__ import annotations

from typing import Any


def _bar_svg(title: str, items: list[tuple[str, float]], *, color: str = "#0f766e") -> str:
    width = 760
    height = 48 + (len(items) * 34)
    max_value = max([abs(value) for _label, value in items] or [1.0])
    lines = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>",
        "<style>text{font-family:Helvetica,Arial,sans-serif;font-size:12px;fill:#16324f}.muted{fill:#577590}.label{font-weight:700}</style>",
        f"<text x='20' y='24' class='label'>{title}</text>",
    ]
    y = 46
    for label, value in items:
        bar_width = 420 * (abs(value) / max_value if max_value else 0.0)
        x = 210 if value >= 0 else 210 - bar_width
        lines.append(f"<text x='20' y='{y}'>{label}</text>")
        lines.append("<line x1='210' y1='{0}' x2='210' y2='{1}' stroke='#94a3b8' stroke-width='1'/>".format(y - 12, y + 12))
        lines.append(
            f"<rect x='{x:.1f}' y='{y - 11}' width='{bar_width:.1f}' height='16' fill='{color if value >= 0 else '#c2410c'}' rx='3'/>"
        )
        lines.append(f"<text x='650' y='{y}' class='muted'>{value:+.1f}%</text>")
        y += 32
    lines.append("</svg>")
    return "".join(lines)


def _pie_svg(title: str, items: list[tuple[str, float]]) -> str:
    total = sum([max(weight, 0.0) for _label, weight in items]) or 1.0
    colors = ["#0f766e", "#1d4ed8", "#b45309", "#7c3aed", "#be123c", "#475569", "#16a34a"]
    cx = 130
    cy = 140
    r = 90
    start = 0.0
    legend_y = 50
    parts = [
        "<svg xmlns='http://www.w3.org/2000/svg' width='760' height='280' viewBox='0 0 760 280'>",
        "<style>text{font-family:Helvetica,Arial,sans-serif;font-size:12px;fill:#16324f}.label{font-weight:700}</style>",
        f"<text x='20' y='24' class='label'>{title}</text>",
    ]
    import math

    for idx, (label, weight) in enumerate(items):
        angle = (weight / total) * math.tau
        end = start + angle
        x1 = cx + r * math.cos(start - math.pi / 2)
        y1 = cy + r * math.sin(start - math.pi / 2)
        x2 = cx + r * math.cos(end - math.pi / 2)
        y2 = cy + r * math.sin(end - math.pi / 2)
        large = 1 if angle > math.pi else 0
        color = colors[idx % len(colors)]
        parts.append(
            f"<path d='M {cx} {cy} L {x1:.2f} {y1:.2f} A {r} {r} 0 {large} 1 {x2:.2f} {y2:.2f} Z' fill='{color}' opacity='0.92'/>"
        )
        parts.append(f"<rect x='320' y='{legend_y - 10}' width='12' height='12' fill='{color}' rx='2'/>")
        parts.append(f"<text x='340' y='{legend_y}'>{label}: {weight:.1%}</text>")
        legend_y += 24
        start = end
    parts.append("</svg>")
    return "".join(parts)


def build_brief_charts(
    *,
    allocation_items: list[tuple[str, float]],
    benchmark_items: list[tuple[str, float]],
    stress_items: list[tuple[str, float]],
    regime_history: list[dict[str, Any]],
) -> list[dict[str, str]]:
    drift_items: list[tuple[str, float]] = []
    benchmark_map = {key: value for key, value in benchmark_items}
    for key, weight in allocation_items:
        drift_items.append((key, (weight - float(benchmark_map.get(key, 0.0))) * 100.0))
    history_items = []
    for item in regime_history[-6:]:
        label = str(item.get("as_of_ts") or "")[:10]
        state_score = 0.0
        if str(item.get("long_state") or "").lower() not in {"normal", "neutral"}:
            state_score += 1.0
        if str(item.get("short_state") or "").lower() not in {"normal", "neutral"}:
            state_score += 1.0
        history_items.append((label, state_score))
    return [
        {"chart_key": "allocation_pie", "title": "Allocation Pie Chart", "svg": _pie_svg("Allocation Pie Chart", allocation_items)},
        {"chart_key": "sleeve_drift_bar", "title": "Sleeve Drift Bar Chart", "svg": _bar_svg("Sleeve Drift Bar Chart", drift_items, color="#1d4ed8")},
        {"chart_key": "drawdown_chart", "title": "Drawdown Scenario Chart", "svg": _bar_svg("Drawdown Scenario Chart", stress_items, color="#b45309")},
        {"chart_key": "benchmark_chart", "title": "Composite Benchmark Chart", "svg": _bar_svg("Composite Benchmark Weights", [(key, value * 100.0) for key, value in benchmark_items])},
        {"chart_key": "regime_timeline", "title": "Regime Timeline Chart", "svg": _bar_svg("Regime Timeline Chart", history_items, color="#7c3aed")},
        {"chart_key": "scenario_history_chart", "title": "Historical Scenario Comparison Chart", "svg": _bar_svg("Historical Scenario Comparison Chart", stress_items, color="#be123c")},
    ]
