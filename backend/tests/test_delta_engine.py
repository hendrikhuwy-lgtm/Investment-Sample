from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta

from app.services.delta_engine import (
    _bound_pct_shift,
    _days_in_state,
    _lifecycle_state,
    _material_change,
    _threshold_crossing_detector,
    build_and_persist_delta_state,
    compute_state,
    compute_metric_deltas,
    ensure_delta_tables,
    load_latest_delta_payload,
)
from app.services.language_safety import assert_no_directive_language
from app.services.narrative_engine import build_alert_narrative


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_delta_tables(conn)
    return conn


def _graph_row(metric_id: str, latest: float) -> dict:
    return {
        "series_code": metric_id,
        "long_horizon": {"latest": latest, "percentile_5y": 72.0},
        "short_horizon": {"change_5obs": 0.0, "momentum_20obs": 0.0},
        "citation": {
            "url": "https://fred.stlouisfed.org/series/DGS10",
            "source_id": "fred_dgs10",
            "retrieved_at": "2026-02-19T01:00:00+00:00",
            "importance": "primary official rates series",
        },
    }


def _graph_row_with_observed(metric_id: str, latest: float, observed_at: str, retrieved_at: str) -> dict:
    return {
        "series_code": metric_id,
        "long_horizon": {"latest": latest, "percentile_5y": 72.0},
        "short_horizon": {"change_5obs": 0.0, "momentum_20obs": 0.0},
        "citation": {
            "url": f"https://fred.stlouisfed.org/series/{metric_id}",
            "source_id": f"fred_{metric_id.lower()}",
            "retrieved_at": retrieved_at,
            "observed_at": observed_at,
            "importance": "primary official rates series",
        },
    }


def test_delta_computation_windows_5_and_60() -> None:
    conn = _conn()
    base = datetime(2026, 2, 1, tzinfo=UTC)
    for idx in range(1, 61):
        conn.execute(
            """
            INSERT INTO metric_snapshots (
              snapshot_id, asof_ts, metric_id, value, window_5_change, window_20_change,
              window_60_range_low, window_60_range_high, percentile_60, stddev_60, citations_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"seed_{idx}",
                (base + timedelta(days=idx)).isoformat(),
                "DGS10",
                float(idx),
                0.0,
                0.0,
                0.0,
                0.0,
                50.0,
                1.0,
                json.dumps([]),
            ),
        )
    conn.commit()

    rows = compute_metric_deltas(conn, [_graph_row("DGS10", 61.0)], datetime(2026, 4, 10, tzinfo=UTC))
    assert len(rows) == 1
    row = rows[0]
    assert round(float(row["window_5_change"]), 4) == 5.0
    assert round(float(row["window_20_change"]), 4) == 20.0
    assert float(row["window_60_range_low"]) == 2.0
    assert float(row["window_60_range_high"]) == 61.0


def test_threshold_crossing_detector_up_and_down() -> None:
    up = _threshold_crossing_detector(79.0, 82.0, "p60_stress_emerging", 80.0, "up")
    down = _threshold_crossing_detector(62.0, 58.0, "p60_revert_below_elevated", 60.0, "down")
    assert up["crossed_up"] is True
    assert up["crossed_down"] is False
    assert down["crossed_down"] is True
    assert down["crossed_up"] is False


def test_materiality_filter_minor_change_suppressed_unless_crossing() -> None:
    assert _material_change(delta_1d=0.1, stddev_60=1.0, threshold_crossed=False) is False
    assert _material_change(delta_1d=0.1, stddev_60=1.0, threshold_crossed=True) is True


def test_alert_lifecycle_classification_paths() -> None:
    conn = _conn()
    now = datetime(2026, 2, 19, tzinfo=UTC)
    assert _lifecycle_state(conn, metric_id="VIXCLS", horizon="short", prev_state="stable", curr_state="elevated", asof_ts=now) == "new"
    conn.execute(
        """
        INSERT INTO alert_events_v2 (
          alert_id, asof_ts, horizon, metric_id, prev_state, curr_state, lifecycle, severity,
          threshold_name, threshold_value, current_value, delta_value, delta_window, percentile_60,
          days_in_state, impact_score, impact_map_json, narrative_md, citations_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "seed_a",
            (now - timedelta(days=1)).isoformat(),
            "short",
            "VIXCLS",
            "elevated",
            "elevated",
            "persisting",
            3,
            "p60_elevated",
            60.0,
            20.0,
            0.2,
            "1d",
            71.0,
            2,
            50.0,
            json.dumps({}),
            "seed",
            json.dumps([{"url": "https://fred.stlouisfed.org/series/VIXCLS", "source_id": "fred_vix", "retrieved_at": now.isoformat(), "importance": "primary"}]),
        ),
    )
    conn.commit()
    assert _lifecycle_state(conn, metric_id="VIXCLS", horizon="short", prev_state="elevated", curr_state="stress_emerging", asof_ts=now) == "escalated"
    assert _lifecycle_state(conn, metric_id="VIXCLS", horizon="short", prev_state="elevated", curr_state="elevated", asof_ts=now) == "persisting"
    assert _lifecycle_state(conn, metric_id="VIXCLS", horizon="short", prev_state="elevated", curr_state="stable", asof_ts=now) == "de_escalated"


def test_days_in_state_counter_increments() -> None:
    conn = _conn()
    now = datetime(2026, 2, 19, tzinfo=UTC)
    for idx in range(2):
        conn.execute(
            """
            INSERT INTO alert_events_v2 (
              alert_id, asof_ts, horizon, metric_id, prev_state, curr_state, lifecycle, severity,
              threshold_name, threshold_value, current_value, delta_value, delta_window, percentile_60,
              days_in_state, impact_score, impact_map_json, narrative_md, citations_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"seed_{idx}",
                (now - timedelta(days=idx + 1)).isoformat(),
                "short",
                "BAMLH0A0HYM2",
                "elevated",
                "elevated",
                "persisting",
                3,
                "p60_elevated",
                60.0,
                3.0,
                0.1,
                "1d",
                65.0,
                idx + 1,
                44.0,
                json.dumps({}),
                "seed",
                json.dumps([{"url": "https://fred.stlouisfed.org/series/BAMLH0A0HYM2", "source_id": "fred_hy", "retrieved_at": now.isoformat(), "importance": "primary"}]),
            ),
        )
    conn.commit()
    assert _days_in_state(conn, metric_id="BAMLH0A0HYM2", horizon="short", curr_state="elevated") == 3


def test_narrative_template_contains_required_fields_and_no_directive_verbs() -> None:
    narrative = build_alert_narrative(
        {
            "metric_id": "VIXCLS",
            "delta_value": 1.2,
            "delta_window": "1d",
            "threshold_name": "p60_stress_emerging",
            "threshold_value": 80.0,
            "current_value": 22.4,
            "percentile_60": 84.0,
            "prev_state": "elevated",
            "curr_state": "stress_emerging",
            "days_in_state": 3,
            "since_when": "2026-02-17",
            "distance_to_revert": -4.0,
            "impact_map": {
                "primary": "Volatility regime changes are most relevant to equity dispersion and convex overlays.",
                "secondary": "Elevated implied volatility can alter cross-asset risk premia behavior.",
                "convex_relevance": "Convex sleeve relevance is elevated under higher implied volatility conditions.",
            },
        }
    )
    assert "crossing" in narrative
    assert "State changed from" in narrative
    assert "Duration:" in narrative
    assert "Why it matters:" in narrative
    assert_no_directive_language([narrative])


def test_build_and_persist_delta_state_returns_summary_payload() -> None:
    conn = _conn()
    payload = build_and_persist_delta_state(
        conn,
        macro_result={
            "signals_summary": {
                "graph_metadata": [
                    _graph_row("DGS10", 4.2),
                    _graph_row("VIXCLS", 20.0),
                ]
            }
        },
        sleeve_weights={"global_equity": 0.5, "ig_bond": 0.2, "real_asset": 0.1, "alt": 0.07, "convex": 0.03, "cash": 0.1},
    )
    assert "daily_state_change_summary" in payload
    assert "alerts_v2" in payload


def test_percentile_shift_is_current_minus_previous_pct_points_and_bounded() -> None:
    conn = _conn()
    conn.execute(
        """
        INSERT INTO metric_snapshots (
          snapshot_id, asof_ts, metric_id, metric_name, value, delta_1d, window_5_change, window_20_change,
          window_60_range_low, window_60_range_high, percentile_60, prev_percentile_60, percentile_shift,
          stddev_60, state_short, days_in_state_short, citations_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "seed_shift",
            "2026-02-18T00:00:00+00:00",
            "DGS10",
            "US 10Y Treasury Yield",
            100.0,
            0.0,
            0.0,
            0.0,
            80.0,
            100.0,
            95.0,
            None,
            0.0,
            1.0,
            "stress_regime",
            4,
            json.dumps([{"url": "https://fred.stlouisfed.org/series/DGS10", "source_id": "fred_dgs10", "retrieved_at": "2026-02-18T00:00:00+00:00", "importance": "primary"}]),
        ),
    )
    conn.commit()
    rows = compute_metric_deltas(conn, [_graph_row("DGS10", 50.0)], datetime(2026, 2, 19, tzinfo=UTC))
    assert len(rows) == 1
    row = rows[0]
    expected = _bound_pct_shift(float(row["percentile_60"]) - 95.0)
    assert float(row["percentile_shift"]) == expected
    assert _bound_pct_shift(240.0) == 100.0
    assert _bound_pct_shift(-240.0) == -100.0


def test_state_transition_labels_are_consistent_across_sections() -> None:
    conn = _conn()
    build_and_persist_delta_state(
        conn,
        macro_result={
            "signals_summary": {
                "graph_metadata": [
                    _graph_row("DGS10", 4.2),
                    _graph_row("VIXCLS", 24.0),
                ]
            }
        },
        sleeve_weights={"global_equity": 0.5, "ig_bond": 0.2, "real_asset": 0.1, "alt": 0.07, "convex": 0.03, "cash": 0.1},
        holdings_available=True,
    )
    loaded = load_latest_delta_payload(conn)
    short_transition = dict(loaded["daily_state_change_summary"]["regime_transitions"]["short"])
    long_transition = dict(loaded["daily_state_change_summary"]["regime_transitions"]["long"])
    assert short_transition["curr_state"] == loaded["regimes"]["short"]["state"]
    assert long_transition["curr_state"] == loaded["regimes"]["long"]["state"]
    for alert in loaded["alerts_v2"]:
        expected = compute_state({"percentile_60": alert.get("percentile_60")}, "short", str(alert.get("prev_state")))
        assert str(alert.get("curr_state")) == expected


def test_new_observation_since_prior_run_false_when_observed_at_is_unchanged() -> None:
    conn = _conn()
    build_and_persist_delta_state(
        conn,
        macro_result={
            "signals_summary": {
                "graph_metadata": [
                    _graph_row_with_observed(
                        "DGS10",
                        4.2,
                        observed_at="2026-02-18",
                        retrieved_at="2026-02-19T01:00:00+00:00",
                    )
                ]
            }
        },
        sleeve_weights={"global_equity": 0.5, "ig_bond": 0.2, "real_asset": 0.1, "alt": 0.07, "convex": 0.03, "cash": 0.1},
        asof_ts=datetime(2026, 2, 19, 1, 0, tzinfo=UTC),
    )
    build_and_persist_delta_state(
        conn,
        macro_result={
            "signals_summary": {
                "graph_metadata": [
                    _graph_row_with_observed(
                        "DGS10",
                        4.35,
                        observed_at="2026-02-18",
                        retrieved_at="2026-02-19T02:00:00+00:00",
                    )
                ]
            }
        },
        sleeve_weights={"global_equity": 0.5, "ig_bond": 0.2, "real_asset": 0.1, "alt": 0.07, "convex": 0.03, "cash": 0.1},
        asof_ts=datetime(2026, 2, 19, 2, 0, tzinfo=UTC),
    )
    loaded = load_latest_delta_payload(conn)
    metric = next(item for item in loaded["metrics"] if item["metric_id"] == "DGS10")
    assert metric["new_observation_since_prior_run"] is False
    assert metric["change_type"] in {"no_new_observation", "cached_fallback"}
