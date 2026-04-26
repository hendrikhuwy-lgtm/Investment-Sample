from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from app.models.db import connect, init_db
from app.services.dashboard_monitoring import (
    build_dashboard_monitoring,
    build_near_threshold_watchlist,
    build_top_movers_summary,
)
from app.services.data_lag import build_data_trust_badge, classify_lag_cause


def test_lag_cause_expected_publication_lag_on_weekend() -> None:
    cause = classify_lag_cause(
        series_key="DGS10",
        observed_at="2026-02-27",
        retrieved_at="2026-03-01T08:00:00+08:00",
        lag_days=2,
        retrieval_succeeded=True,
        cache_fallback_used=False,
        latest_available_matches_observed=True,
        previous_observed_at="2026-02-26",
    )
    assert cause == "expected_publication_lag"


def test_data_trust_badge_low_with_many_unexpected() -> None:
    badge = build_data_trust_badge(
        [
            {"metric_key": "DGS10", "lag_days": 6, "lag_class": "stale", "lag_cause": "unexpected_ingestion_lag"},
            {"metric_key": "T10YIE", "lag_days": 4, "lag_class": "lagged", "lag_cause": "unexpected_ingestion_lag"},
            {"metric_key": "SP500", "lag_days": 5, "lag_class": "stale", "lag_cause": "unexpected_ingestion_lag"},
            {"metric_key": "VIXCLS", "lag_days": 2, "lag_class": "lagged", "lag_cause": "expected_publication_lag"},
            {"metric_key": "BAMLH0A0HYM2", "lag_days": 2, "lag_class": "lagged", "lag_cause": "expected_publication_lag"},
        ]
    )
    assert badge["level"] == "low"


def test_watchlist_and_movers_include_lag_fields() -> None:
    metrics = [
        {
            "metric_id": "VIXCLS",
            "metric_name": "CBOE VIX",
            "percentile_60": 78.0,
            "prev_percentile_60": 74.0,
            "curr_state": "elevated",
            "days_in_state_short": 4,
            "delta_1d": 1.2,
            "citations": [{"url": "https://fred.stlouisfed.org/series/VIXCLS", "source_id": "fred_vixcls", "retrieved_at": datetime.now(UTC).isoformat(), "importance": "test"}],
        }
    ]
    lag_map = {
        "VIXCLS": {
            "observed_at": "2026-02-28",
            "retrieved_at": "2026-03-01T08:00:00+08:00",
            "lag_days": 1,
            "lag_class": "fresh",
            "lag_cause": "expected_publication_lag",
        }
    }
    watchlist = build_near_threshold_watchlist(
        metrics,
        lag_map,
        {"VIXCLS": "2026-02-27"},
        cache_fallback_used=False,
        top_n=3,
    )
    movers = build_top_movers_summary(
        metrics,
        lag_map,
        {"VIXCLS": "2026-02-27"},
        cache_fallback_used=False,
        top_n=3,
    )
    assert watchlist and watchlist[0]["lag_cause"] == "expected_publication_lag"
    assert movers and movers[0]["lag_cause"] == "expected_publication_lag"
    assert bool(watchlist[0]["new_observation_since_prior_run"]) is True
    assert movers[0]["change_type"] == "new_observation"


def test_movers_mark_no_new_observation_when_observed_at_is_unchanged() -> None:
    metrics = [
        {
            "metric_id": "SP500",
            "metric_name": "S&P 500 Index",
            "percentile_60": 62.0,
            "prev_percentile_60": 61.0,
            "curr_state": "elevated",
            "prev_state": "elevated",
            "days_in_state_short": 2,
            "delta_1d": 4.2,
            "citations": [],
        }
    ]
    lag_map = {
        "SP500": {
            "observed_at": "2026-02-28",
            "retrieved_at": "2026-03-01T08:00:00+08:00",
            "lag_days": 1,
            "lag_class": "fresh",
            "lag_cause": "expected_publication_lag",
        }
    }
    movers = build_top_movers_summary(
        metrics,
        lag_map,
        {"SP500": "2026-02-28"},
        cache_fallback_used=False,
        top_n=3,
    )
    assert movers
    assert movers[0]["new_observation_since_prior_run"] is False
    assert movers[0]["change_type"] == "no_new_observation"
    assert "No new observation since observed_at 2026-02-28." in str(movers[0]["observation_status_text"])


def test_build_dashboard_monitoring_payload_shape(tmp_path: Path) -> None:
    db_path = tmp_path / "monitoring.sqlite3"
    conn = connect(db_path)
    try:
        schema_path = Path(__file__).resolve().parents[1] / "app" / "storage" / "schema.sql"
        init_db(conn, schema_path)
        conn.execute(
            """
            INSERT INTO metric_snapshots (
              snapshot_id, asof_ts, metric_id, metric_name, value, percentile_60, prev_percentile_60,
              state_short, days_in_state_short, observed_at, retrieved_at, lag_days, lag_class, lag_cause, citations_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "ms_1",
                datetime.now(UTC).isoformat(),
                "SP500",
                "S&P 500 Index",
                5200.0,
                62.0,
                59.0,
                "elevated",
                2,
                "2026-02-28",
                datetime.now(UTC).isoformat(),
                1,
                "fresh",
                "expected_publication_lag",
                "[]",
            ),
        )
        conn.commit()

        payload = build_dashboard_monitoring(
            conn,
            delta_payload={
                "alerts_v2": [],
                "metrics": [
                    {
                        "metric_id": "SP500",
                        "metric_name": "S&P 500 Index",
                        "percentile_60": 62.0,
                        "prev_percentile_60": 59.0,
                        "curr_state": "elevated",
                        "days_in_state_short": 2,
                        "delta_1d": 12.0,
                        "citations": [],
                    }
                ],
            },
            today_upload={"cached_used": False},
            holdings=[],
            valuations=[],
            diagnostic={},
            policy_weights={"global_equity": 0.35},
        )
    finally:
        conn.close()

    assert "why_today_matters" in payload
    assert "near_threshold_watchlist" in payload
    assert "top_movers_summary" in payload
    assert "data_trust_badge" in payload
    assert "data_recency" in payload
    assert "portfolio_monitoring" in payload
