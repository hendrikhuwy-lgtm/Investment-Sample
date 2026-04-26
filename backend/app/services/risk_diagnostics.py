from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

from app.models.types import PortfolioSnapshot


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CACHE_DIR = PROJECT_ROOT / "outbox" / "live_cache"


def _safe_returns(values: list[float]) -> list[float]:
    out: list[float] = []
    for i in range(1, len(values)):
        prev = values[i - 1]
        curr = values[i]
        if prev <= 0:
            continue
        out.append((curr / prev) - 1.0)
    return out


def _portfolio_returns_from_snapshots(snapshots: list[PortfolioSnapshot]) -> list[float]:
    ordered = sorted(snapshots, key=lambda item: item.created_at)
    values = [float(item.total_value) for item in ordered if float(item.total_value) > 0]
    return _safe_returns(values)


def _load_benchmark_returns() -> list[float]:
    path = CACHE_DIR / "SP500.csv"
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []

    points: list[float] = []
    header = []
    for idx, line in enumerate(lines):
        if idx == 0:
            header = line.split(",")
            continue
        parts = line.split(",")
        if len(parts) != len(header):
            continue
        row = dict(zip(header, parts))
        value = row.get("SP500")
        if value in {None, ".", ""}:
            continue
        try:
            points.append(float(value))
        except ValueError:
            continue
    return _safe_returns(points[-300:])


def _covariance(x: list[float], y: list[float]) -> float:
    n = min(len(x), len(y))
    if n < 2:
        return 0.0
    x_vals = x[-n:]
    y_vals = y[-n:]
    mx = mean(x_vals)
    my = mean(y_vals)
    return sum((x_vals[i] - mx) * (y_vals[i] - my) for i in range(n)) / n


def _variance(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = mean(values)
    return sum((item - m) ** 2 for item in values) / len(values)


def _correlation(x: list[float], y: list[float]) -> float:
    cov = _covariance(x, y)
    sx = math.sqrt(max(0.0, _variance(x)))
    sy = math.sqrt(max(0.0, _variance(y)))
    if sx == 0 or sy == 0:
        return 0.0
    return cov / (sx * sy)


def _max_drawdown(values: list[float]) -> float:
    if not values:
        return 0.0
    peak = values[0]
    max_dd = 0.0
    for value in values:
        if value > peak:
            peak = value
        if peak <= 0:
            continue
        dd = (value / peak) - 1.0
        if dd < max_dd:
            max_dd = dd
    return max_dd


def _historical_var_cvar(returns: list[float], confidence: float = 0.95) -> tuple[float, float]:
    if len(returns) < 20:
        return 0.0, 0.0
    sorted_ret = sorted(returns)
    idx = max(0, min(len(sorted_ret) - 1, int((1.0 - confidence) * len(sorted_ret))))
    var = sorted_ret[idx]
    tail = [item for item in sorted_ret if item <= var]
    cvar = mean(tail) if tail else var
    return var, cvar


def _sleeve_correlation_view(snapshots: list[PortfolioSnapshot]) -> dict[str, dict[str, float]]:
    ordered = sorted(snapshots, key=lambda item: item.created_at)
    sleeves = ["global_equity", "ig_bond", "real_asset", "alt", "convex", "cash"]
    series: dict[str, list[float]] = {sleeve: [] for sleeve in sleeves}
    for snap in ordered:
        weights = snap.sleeve_weights
        for sleeve in sleeves:
            series[sleeve].append(float(weights.get(sleeve, 0.0)))

    corr: dict[str, dict[str, float]] = {}
    for lhs in sleeves:
        corr[lhs] = {}
        for rhs in sleeves:
            corr[lhs][rhs] = round(_correlation(series[lhs], series[rhs]), 3)
    return corr


def _concentration_alerts(snapshot: PortfolioSnapshot | None) -> list[str]:
    if snapshot is None:
        return ["Concentration metrics unavailable: no snapshot."]
    metrics = snapshot.concentration_metrics or {}
    alerts: list[str] = []
    single = float(metrics.get("single_name_risk_pct", 0.0))
    top5 = float(metrics.get("top5_positions_pct", 0.0))
    if single >= 20:
        alerts.append("Single-name concentration exceeds 20% of portfolio value.")
    if top5 >= 65:
        alerts.append("Top-5 concentration exceeds 65% of portfolio value.")
    if not alerts:
        alerts.append("No concentration threshold breach in current snapshot.")
    return alerts


def compute_risk_diagnostics(snapshots: list[PortfolioSnapshot]) -> dict[str, Any]:
    ordered = sorted(snapshots, key=lambda item: item.created_at)
    returns = _portfolio_returns_from_snapshots(ordered)
    benchmark = _load_benchmark_returns()
    values = [float(item.total_value) for item in ordered]

    volatility = pstdev(returns) * math.sqrt(252) if len(returns) >= 2 else 0.0
    beta = 0.0
    if returns and benchmark:
        n = min(len(returns), len(benchmark))
        cov = _covariance(returns[-n:], benchmark[-n:])
        var_m = _variance(benchmark[-n:])
        beta = cov / var_m if var_m > 0 else 0.0

    max_drawdown = _max_drawdown(values)
    var95, cvar95 = _historical_var_cvar(returns, confidence=0.95)
    latest = ordered[-1] if ordered else None
    missing_inputs = []
    if len(ordered) < 2:
        missing_inputs.append("insufficient_snapshot_history")
    if len(returns) < 20:
        missing_inputs.append("limited_returns_history_for_var")
    if not benchmark:
        missing_inputs.append("benchmark_series_missing")

    return {
        "as_of": datetime.now().isoformat(),
        "volatility_annualized": round(volatility, 4),
        "beta_to_sp500": round(beta, 4),
        "max_drawdown": round(max_drawdown, 4),
        "var_model_based_95": {
            "label": "model_based_historical",
            "value": round(var95, 4),
        },
        "cvar_model_based_95": {
            "label": "model_based_historical",
            "value": round(cvar95, 4),
        },
        "concentration_alerts": _concentration_alerts(latest),
        "correlation_view": _sleeve_correlation_view(ordered) if len(ordered) >= 5 else {},
        "missing_inputs": missing_inputs,
        "blocked_sections": [
            "correlation_view"
        ] if len(ordered) < 5 else [],
    }
