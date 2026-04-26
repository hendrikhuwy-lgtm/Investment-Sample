from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

from app.models.types import AttributionSleeveEffect, AttributionSummary, PortfolioSnapshot


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CACHE_DIR = PROJECT_ROOT / "outbox" / "live_cache"


def _latest_monthly_proxy_returns() -> dict[str, float]:
    sp500_path = CACHE_DIR / "SP500.csv"
    vix_path = CACHE_DIR / "VIXCLS.csv"
    dgs10_path = CACHE_DIR / "DGS10.csv"

    def _read_series(path: Path, column: str) -> list[float]:
        if not path.exists():
            return []
        rows = list(csv.DictReader(path.read_text(encoding="utf-8", errors="replace").splitlines()))
        out: list[float] = []
        for row in rows:
            value = row.get(column)
            if value in {None, "", "."}:
                continue
            try:
                out.append(float(value))
            except ValueError:
                continue
        return out

    sp = _read_series(sp500_path, "SP500")
    vx = _read_series(vix_path, "VIXCLS")
    y10 = _read_series(dgs10_path, "DGS10")

    sp_month = ((sp[-1] / sp[-22]) - 1.0) if len(sp) > 22 and sp[-22] > 0 else 0.0
    vix_chg = ((vx[-1] / vx[-22]) - 1.0) if len(vx) > 22 and vx[-22] > 0 else 0.0
    y10_chg = (y10[-1] - y10[-22]) if len(y10) > 22 else 0.0

    # Diagnostic proxies only; no directional recommendation.
    ig_bond = max(-0.04, min(0.04, -4.5 * (y10_chg / 100.0)))
    real_asset = 0.45 * sp_month
    alt = 0.30 * sp_month
    convex = max(-0.02, min(0.06, (abs(vix_chg) * 0.04)))
    cash = 0.002
    return {
        "global_equity": sp_month,
        "ig_bond": ig_bond,
        "real_asset": real_asset,
        "alt": alt,
        "convex": convex,
        "cash": cash,
    }


def _monthly_windows(snapshots: list[PortfolioSnapshot]) -> dict[str, list[PortfolioSnapshot]]:
    out: dict[str, list[PortfolioSnapshot]] = defaultdict(list)
    for item in sorted(snapshots, key=lambda snap: snap.created_at):
        key = item.created_at.strftime("%Y-%m")
        out[key].append(item)
    return out


def _portfolio_monthly_return(window: list[PortfolioSnapshot]) -> float:
    if len(window) < 2:
        return 0.0
    start = float(window[0].total_value)
    end = float(window[-1].total_value)
    if start <= 0:
        return 0.0
    return (end / start) - 1.0


def _average_weights(window: list[PortfolioSnapshot]) -> dict[str, float]:
    sleeves = ["global_equity", "ig_bond", "real_asset", "alt", "convex", "cash"]
    out: dict[str, float] = {}
    for sleeve in sleeves:
        values = [float(item.sleeve_weights.get(sleeve, 0.0)) for item in window]
        out[sleeve] = mean(values) if values else 0.0
    return out


def compute_monthly_attribution(
    snapshots: list[PortfolioSnapshot],
    policy_weights: dict[str, float],
) -> AttributionSummary:
    windows = _monthly_windows(snapshots)
    if not windows:
        return AttributionSummary(
            period="n/a",
            portfolio_return=0.0,
            benchmark_return=0.0,
            active_return=0.0,
            sleeve_effects=[],
            notes="No snapshots available for attribution.",
        )

    period = sorted(windows.keys())[-1]
    window = windows[period]
    actual_weights = _average_weights(window)
    bench_returns = _latest_monthly_proxy_returns()
    portfolio_return = _portfolio_monthly_return(window)
    benchmark_return = sum(float(policy_weights.get(k, 0.0)) * float(v) for k, v in bench_returns.items())
    active_return = portfolio_return - benchmark_return

    sleeve_effects: list[AttributionSleeveEffect] = []
    for sleeve in policy_weights.keys():
        w_p = float(actual_weights.get(sleeve, 0.0))
        w_b = float(policy_weights.get(sleeve, 0.0))
        r_b = float(bench_returns.get(sleeve, 0.0))

        # Approximate sleeve realized return from benchmark adjusted by weight differential.
        r_p = r_b + ((w_p - w_b) * 0.05)
        allocation = (w_p - w_b) * (r_b - benchmark_return)
        selection = w_b * (r_p - r_b)
        interaction = (w_p - w_b) * (r_p - r_b)
        total = allocation + selection + interaction
        sleeve_effects.append(
            AttributionSleeveEffect(
                sleeve=sleeve,
                allocation_effect=round(allocation, 6),
                selection_effect=round(selection, 6),
                interaction_effect=round(interaction, 6),
                total_effect=round(total, 6),
            )
        )

    return AttributionSummary(
        period=period,
        portfolio_return=round(portfolio_return, 6),
        benchmark_return=round(benchmark_return, 6),
        active_return=round(active_return, 6),
        sleeve_effects=sleeve_effects,
        notes=(
            "Brinson-style sleeve attribution using diagnostic benchmark proxies and observed sleeve-weight profile; "
            "used for monitoring and post-hoc analysis."
        ),
    )
