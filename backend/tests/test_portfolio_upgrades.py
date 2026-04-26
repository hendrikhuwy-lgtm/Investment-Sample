from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.config import Settings
from app.models.db import connect, init_db
from app.models.types import PortfolioSnapshot
from app.services.ips import get_ips, put_ips
from app.services.language_safety import PERSISTENT_DISCLAIMER, assert_no_directive_language
from app.services.performance_attribution import compute_monthly_attribution
from app.services.portfolio_state import ensure_portfolio_tables, upsert_holding
from app.services.rebalance_cues import evaluate_drift_alerts
from app.services.stress_engine import run_stress_suite


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "app" / "storage" / "schema.sql"


def _conn(tmp_path: Path):
    settings = Settings(db_path=str(tmp_path / "upgrades.sqlite"), mcp_live_required=False)
    db_path = settings.resolved_db_path(Path(__file__).resolve().parents[2])
    conn = connect(db_path)
    init_db(conn, SCHEMA_PATH)
    ensure_portfolio_tables(conn)
    return conn


def test_ips_stored_and_loaded(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    try:
        original = get_ips(conn)
        updated = original.model_copy(update={"owner_label": "Dad Portfolio", "horizon_years": 15})
        put_ips(conn, updated)
        loaded = get_ips(conn)
    finally:
        conn.close()

    assert loaded.owner_label == "Dad Portfolio"
    assert loaded.horizon_years == 15
    assert loaded.allocations


def test_drift_alerts_trigger_correctly() -> None:
    alerts = evaluate_drift_alerts(
        actual_weights={"global_equity": 0.62, "ig_bond": 0.10, "cash": 0.05, "real_asset": 0.08, "alt": 0.10, "convex": 0.05},
        policy_weights={"global_equity": 0.50, "ig_bond": 0.20, "cash": 0.10, "real_asset": 0.10, "alt": 0.07, "convex": 0.03},
        policy_bands={
            "global_equity": (0.45, 0.55),
            "ig_bond": (0.15, 0.25),
            "cash": (0.05, 0.15),
            "real_asset": (0.05, 0.15),
            "alt": (0.04, 0.10),
            "convex": (0.02, 0.04),
        },
    )
    sleeves = {item["sleeve"] for item in alerts}
    assert "global_equity" in sleeves
    assert "ig_bond" in sleeves
    assert "convex" in sleeves


def test_stress_engine_returns_expected_shape() -> None:
    result = run_stress_suite(
        {"global_equity": 0.50, "ig_bond": 0.20, "cash": 0.10, "real_asset": 0.10, "alt": 0.07, "convex": 0.03},
        convex_carry_estimate_pct=0.0025,
    )
    assert "scenarios" in result
    assert "summary" in result
    assert result["scenarios"]
    first = result["scenarios"][0]
    assert {"scenario_id", "estimated_impact_pct", "convex_contribution_pct", "ex_convex_impact_pct"} <= set(first.keys())


def test_attribution_returns_allocation_and_selection_effects() -> None:
    snapshots = [
        PortfolioSnapshot(
            snapshot_id="s1",
            created_at=datetime(2026, 2, 1, tzinfo=UTC),
            total_value=100000.0,
            sleeve_weights={"global_equity": 0.50, "ig_bond": 0.20, "cash": 0.10, "real_asset": 0.10, "alt": 0.07, "convex": 0.03},
            concentration_metrics={},
            convex_coverage_ratio=0.03,
            tax_drag_estimate=0.004,
            notes=None,
        ),
        PortfolioSnapshot(
            snapshot_id="s2",
            created_at=datetime(2026, 2, 28, tzinfo=UTC),
            total_value=102000.0,
            sleeve_weights={"global_equity": 0.52, "ig_bond": 0.18, "cash": 0.08, "real_asset": 0.10, "alt": 0.09, "convex": 0.03},
            concentration_metrics={},
            convex_coverage_ratio=0.03,
            tax_drag_estimate=0.004,
            notes=None,
        ),
    ]
    summary = compute_monthly_attribution(
        snapshots,
        policy_weights={"global_equity": 0.50, "ig_bond": 0.20, "cash": 0.10, "real_asset": 0.10, "alt": 0.07, "convex": 0.03},
    )
    assert summary.sleeve_effects
    first = summary.sleeve_effects[0]
    assert isinstance(first.allocation_effect, float)
    assert isinstance(first.selection_effect, float)


def test_prohibited_language_lint_test_passes() -> None:
    assert_no_directive_language([PERSISTENT_DISCLAIMER, "Diagnostic monitoring view."])
    with pytest.raises(ValueError):
        assert_no_directive_language(["Buy now for upside."])
